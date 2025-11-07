from __future__ import annotations
"""Telegram 发送队列与后台线程。"""

import json
import logging
import sqlite3
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Sequence

from .config import Config
from .gallery import DownloadedMedia
from .http_client import HttpClient, HttpResponse


LOGGER = logging.getLogger(__name__)


class TelegramSendError(RuntimeError):
    """Telegram 发送相关的基类异常。"""


class TelegramRateLimitError(TelegramSendError):
    """表示触发限流，需要等待 ``retry_after`` 秒后重试。"""

    def __init__(self, message: str, *, retry_after: Optional[float] = None) -> None:
        super().__init__(message)
        self.retry_after = retry_after


class TelegramFileTooLargeError(TelegramSendError):
    """媒体文件体积过大导致的错误。"""


class TelegramRecoverableError(TelegramSendError):
    """可通过重试恢复的临时性错误。"""


class TelegramFatalError(TelegramSendError):
    """不可恢复的致命错误。"""


@dataclass
class QueueTask:
    """临时任务数据结构，在入队时使用。"""

    chat_id: str
    file_path: Path
    media_type: str
    caption: Optional[str]
    work_id: Optional[str]
    page: Optional[int]
    metadata: Dict[str, object] = field(default_factory=dict)
    group_id: str = ""
    created_at: float = field(default_factory=lambda: time.time())


@dataclass
class QueueRow:
    """队列表中实际存储的一行记录。"""

    id: int
    chat_id: str
    file_path: Path
    media_type: str
    caption: Optional[str]
    work_id: Optional[str]
    page: Optional[int]
    metadata: Dict[str, object]
    attempts: int
    group_id: str
    available_at: float
    last_error: Optional[str]


@dataclass
class QueueBatch:
    """按作品聚合后的批量任务。"""

    group_id: str
    chat_id: str
    rows: List[QueueRow]


class TelegramQueue:
    """使用 SQLite 实现的持久化任务队列。"""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(str(path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._init_db()

    def _init_db(self) -> None:
        """初始化数据表结构。"""

        with self._conn:
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id TEXT NOT NULL,
                    file_path TEXT NOT NULL,
                    media_type TEXT NOT NULL,
                    caption TEXT,
                    work_id TEXT,
                    page INTEGER,
                    metadata TEXT,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    group_id TEXT NOT NULL,
                    last_error TEXT,
                    available_at REAL NOT NULL DEFAULT 0,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS ledger (
                    key TEXT PRIMARY KEY,
                    sent_at REAL NOT NULL
                )
                """
            )

    def enqueue(self, tasks: Sequence[QueueTask]) -> int:
        """批量入队，并以文件路径与 ledger 控制幂等。"""

        if not tasks:
            return 0
        inserted = 0
        now = time.time()
        with self._lock:
            for task in tasks:
                ledger_key = self._ledger_key(task.chat_id, task.file_path)
                if self._ledger_exists(ledger_key):
                    continue
                if self._task_exists(task.chat_id, task.file_path):
                    continue
                payload = json.dumps(task.metadata, ensure_ascii=False)
                self._conn.execute(
                    """
                    INSERT INTO tasks (chat_id, file_path, media_type, caption, work_id, page, metadata, group_id, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        task.chat_id,
                        str(task.file_path),
                        task.media_type,
                        task.caption,
                        task.work_id,
                        task.page,
                        payload,
                        task.group_id,
                        now,
                        now,
                    ),
                )
                inserted += 1
            if inserted:
                self._conn.commit()
        return inserted

    def fetch_next_batch(self) -> Optional[QueueBatch]:
        """提取一个可执行批次，优先同一作品。"""

        with self._lock:
            now = time.time()
            row = self._conn.execute(
                """
                SELECT id, chat_id, group_id
                FROM tasks
                WHERE available_at <= ?
                ORDER BY created_at
                LIMIT 1
                """,
                (now,),
            ).fetchone()
            if row is None:
                return None
            group_id = row["group_id"]
            chat_id = row["chat_id"]
            rows = self._conn.execute(
                """
                SELECT * FROM tasks
                WHERE group_id = ? AND available_at <= ?
                ORDER BY page IS NULL, page, id
                """,
                (group_id, now),
            ).fetchall()
            return QueueBatch(
                group_id=group_id,
                chat_id=chat_id,
                rows=[self._to_row(record) for record in rows],
            )

    def remove(self, rows: Sequence[QueueRow]) -> None:
        """删除指定任务，主要供测试使用。"""

        if not rows:
            return
        with self._lock:
            ids = [row.id for row in rows]
            placeholders = ",".join("?" for _ in ids)
            self._conn.execute(
                f"DELETE FROM tasks WHERE id IN ({placeholders})",
                ids,
            )
            self._conn.commit()

    def mark_sent(self, rows: Sequence[QueueRow]) -> None:
        """记录任务已发送并写入幂等账本。"""

        if not rows:
            return
        now = time.time()
        with self._lock:
            ledger_rows = [
                (self._ledger_key(row.chat_id, row.file_path), now) for row in rows
            ]
            self._conn.executemany(
                "INSERT OR IGNORE INTO ledger (key, sent_at) VALUES (?, ?)",
                ledger_rows,
            )
            ids = [row.id for row in rows]
            placeholders = ",".join("?" for _ in ids)
            self._conn.execute(
                f"DELETE FROM tasks WHERE id IN ({placeholders})",
                ids,
            )
            self._conn.commit()

    def reschedule(self, rows: Sequence[QueueRow], delay: float, error: str) -> None:
        """将任务延后重试并累计尝试次数。"""

        if not rows:
            return
        now = time.time()
        available_at = now + max(delay, 0.0)
        with self._lock:
            for row in rows:
                self._conn.execute(
                    """
                    UPDATE tasks
                    SET attempts = attempts + 1,
                        available_at = ?,
                        last_error = ?,
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (available_at, error, now, row.id),
                )
            self._conn.commit()

    def mark_failed(self, rows: Sequence[QueueRow], error: str) -> None:
        """标记为永久失败并阻止再次调度。"""

        if not rows:
            return
        now = time.time()
        with self._lock:
            for row in rows:
                self._conn.execute(
                    """
                    UPDATE tasks
                    SET attempts = attempts + 1,
                        last_error = ?,
                        updated_at = ?,
                        available_at = 1e18
                    WHERE id = ?
                    """,
                    (error, now, row.id),
                )
            self._conn.commit()

    def update_media_type(self, rows: Sequence[QueueRow], media_type: str) -> None:
        """修改媒体类型，例如降级为 document。"""

        if not rows:
            return
        now = time.time()
        with self._lock:
            for row in rows:
                self._conn.execute(
                    """
                    UPDATE tasks
                    SET media_type = ?,
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (media_type, now, row.id),
                )
            self._conn.commit()

    def pending_count(self) -> int:
        """获取当前可发送的任务数量。"""

        return self._scalar("SELECT COUNT(*) FROM tasks WHERE available_at <= ?", (time.time(),))

    def total_count(self) -> int:
        """获取队列总任务数量。"""

        return self._scalar("SELECT COUNT(*) FROM tasks", ())

    def failed_count(self) -> int:
        """统计永久失败的任务数量。"""

        return self._scalar("SELECT COUNT(*) FROM tasks WHERE available_at > 1e17", ())

    def close(self) -> None:
        """关闭数据库连接。"""

        self._conn.close()

    def _scalar(self, query: str, params: Sequence[object]) -> int:
        """执行返回单个整数值的查询。"""

        with self._lock:
            row = self._conn.execute(query, params).fetchone()
            if row is None:
                return 0
            return int(row[0])

    def _ledger_exists(self, key: str) -> bool:
        """检查指定键是否已在幂等账本中存在。"""

        row = self._conn.execute("SELECT 1 FROM ledger WHERE key = ?", (key,)).fetchone()
        return row is not None

    def _task_exists(self, chat_id: str, file_path: Path) -> bool:
        """判断相同 chat_id 与文件路径的任务是否已入队。"""

        row = self._conn.execute(
            "SELECT 1 FROM tasks WHERE chat_id = ? AND file_path = ?",
            (chat_id, str(file_path)),
        ).fetchone()
        return row is not None

    def _ledger_key(self, chat_id: str, file_path: Path) -> str:
        """生成 ledger 唯一键，使用绝对路径确保幂等。"""

        return f"{chat_id}:{file_path}"  # absolute path string

    def _to_row(self, record: sqlite3.Row) -> QueueRow:
        """将 SQLite 行转换为 :class:`QueueRow` 对象。"""

        metadata: Dict[str, object] = {}
        raw = record["metadata"]
        if isinstance(raw, str) and raw:
            try:
                metadata = json.loads(raw)
            except json.JSONDecodeError:
                metadata = {}
        return QueueRow(
            id=int(record["id"]),
            chat_id=str(record["chat_id"]),
            file_path=Path(record["file_path"]),
            media_type=str(record["media_type"]),
            caption=record["caption"],
            work_id=record["work_id"],
            page=record["page"],
            metadata=metadata,
            attempts=int(record["attempts"]),
            group_id=str(record["group_id"]),
            available_at=float(record["available_at"]),
            last_error=record["last_error"],
        )


class TelegramMetrics:
    """聚合 Telegram 发送相关的计数信息。"""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.sent = 0
        self.failed = 0
        self.retries = 0
        self.rate_limited = 0
        self.enqueued = 0
        self.last_sent: Optional[float] = None
        self.skipped = 0

    def record_sent(self, count: int) -> None:
        """记录成功发送的数量并更新时间戳。"""

        with self._lock:
            self.sent += count
            self.last_sent = time.time()

    def record_failed(self, count: int) -> None:
        """记录永久失败的数量。"""

        with self._lock:
            self.failed += count

    def record_retry(self, count: int = 1) -> None:
        """记录由于暂时性错误触发的重试次数。"""

        with self._lock:
            self.retries += count

    def record_rate_limit(self) -> None:
        """记录一次限流事件。"""

        with self._lock:
            self.rate_limited += 1

    def record_enqueued(self, count: int) -> None:
        """记录入队任务数量。"""

        with self._lock:
            self.enqueued += count

    def record_skipped(self, count: int) -> None:
        """记录因超出尝试次数而主动跳过的数量。"""

        with self._lock:
            self.skipped += count

    def snapshot(self) -> Dict[str, object]:
        """返回当前指标快照。"""

        with self._lock:
            return {
                "tg_sent": self.sent,
                "tg_failed": self.failed,
                "tg_retries": self.retries,
                "tg_rate_limited": self.rate_limited,
                "tg_enqueued": self.enqueued,
                "tg_last_sent": self.last_sent,
                "tg_skipped": self.skipped,
            }


class TelegramAPI:
    """封装 Telegram Bot API 的调用。"""

    def __init__(self, token: str, *, proxy: Optional[str] = None) -> None:
        self._base_url = f"https://api.telegram.org/bot{token}"
        self._client = HttpClient(proxy=proxy)

    def close(self) -> None:
        """占位关闭方法，HttpClient 无持久连接。"""

        return None

    def send_media_group(self, chat_id: str, rows: Sequence[QueueRow], *, disable_notifications: bool) -> None:
        """调用 ``sendMediaGroup`` 接口批量发送图片/视频。"""

        media_payload: List[Dict[str, object]] = []
        files_payload: Dict[str, tuple[str, bytes]] = {}
        for idx, row in enumerate(rows):
            attach_key = f"file{idx}"
            payload: Dict[str, object] = {
                "type": self._input_media_type(row.media_type),
                "media": f"attach://{attach_key}",
            }
            if row.caption and idx == 0:
                payload["caption"] = row.caption
            media_payload.append(payload)
            content = row.file_path.read_bytes()
            files_payload[attach_key] = (row.file_path.name, content)
        data = {
            "chat_id": chat_id,
            "media": json.dumps(media_payload, ensure_ascii=False),
            "disable_notification": disable_notifications,
        }
        response = self._client.post(
            f"{self._base_url}/sendMediaGroup",
            data=data,
            files=files_payload,
        )
        self._handle_response(response)

    def send_single(self, row: QueueRow, *, disable_notifications: bool) -> None:
        """发送单条媒体，自动匹配合适的接口。"""

        method, field_name = self._method_for_media(row.media_type)
        data = {"chat_id": row.chat_id, "disable_notification": disable_notifications}
        if row.caption:
            data["caption"] = row.caption
        files = {field_name: (row.file_path.name, row.file_path.read_bytes())}
        response = self._client.post(
            f"{self._base_url}/{method}",
            data=data,
            files=files,
        )
        self._handle_response(response)

    def _handle_response(self, response: HttpResponse) -> None:
        """解析响应并转换为自定义异常。"""

        try:
            payload = response.json()
        except json.JSONDecodeError as exc:
            raise TelegramRecoverableError("invalid response from Telegram") from exc
        if response.is_success and payload.get("ok") is True:
            return
        description = payload.get("description") or response.text
        parameters = payload.get("parameters") or {}
        retry_after = parameters.get("retry_after")
        if response.status_code == 429 or "too many requests" in str(description).lower():
            raise TelegramRateLimitError(str(description), retry_after=retry_after)
        if response.status_code in {500, 502, 503, 504}:
            raise TelegramRecoverableError(str(description))
        if response.status_code == 413 or "file is too big" in str(description).lower():
            raise TelegramFileTooLargeError(str(description))
        if response.status_code >= 400:
            raise TelegramFatalError(str(description))
        raise TelegramSendError(str(description))

    def _method_for_media(self, media_type: str) -> tuple[str, str]:
        """根据媒体类型返回 API 方法和字段名。"""

        if media_type == "photo":
            return "sendPhoto", "photo"
        if media_type == "animation":
            return "sendAnimation", "animation"
        if media_type == "video":
            return "sendVideo", "video"
        return "sendDocument", "document"

    def _input_media_type(self, media_type: str) -> str:
        """转换 media group 所需的 type 字段。"""

        if media_type in {"photo", "video", "animation", "document"}:
            return "animation" if media_type == "animation" else media_type
        return "document"


class TelegramDispatcher:
    """后台线程，负责从队列读取任务并调用 Telegram API。"""

    def __init__(self, config: Config) -> None:
        if not config.telegram_active:
            raise ValueError("TelegramDispatcher requires telegram_active configuration")
        queue_path = config.telegram_queue_file
        if queue_path is None:
            raise ValueError("telegram queue path unavailable")
        self.config = config
        self.queue = TelegramQueue(queue_path)
        self.metrics = TelegramMetrics()
        proxy = config.telegram_proxy or config.proxy
        self.api = TelegramAPI(config.telegram_bot_token or "", proxy=proxy)
        self.stop_event = threading.Event()
        self.thread = threading.Thread(target=self._run, name="telegram-sender", daemon=True)

    def _partition_skippable(self, rows: Sequence[QueueRow]) -> tuple[list[QueueRow], list[QueueRow]]:
        """区分需要跳过与可以继续重试的任务。"""

        threshold = max(0, self.config.telegram_max_attempts)
        if threshold <= 0:
            return [], list(rows)
        to_skip: list[QueueRow] = []
        to_retry: list[QueueRow] = []
        for row in rows:
            next_attempt = row.attempts + 1
            if next_attempt >= threshold:
                to_skip.append(row)
            else:
                to_retry.append(row)
        return to_skip, to_retry

    def _mark_skipped(self, rows: Sequence[QueueRow], reason: str) -> None:
        """将任务视为已发送并记录日志。"""

        if not rows:
            return
        for row in rows:
            LOGGER.warning(
                "telegram skip after repeated failures",
                extra={
                    "file": str(row.file_path),
                    "attempts": row.attempts,
                    "reason": reason,
                },
            )
        self.queue.mark_sent(rows)
        self.metrics.record_skipped(len(rows))

    def start(self) -> None:
        """启动发送线程。"""

        self.thread.start()

    def shutdown(self, *, wait: bool = True) -> None:
        """请求线程停止，并在需要时阻塞等待。"""

        self.stop_event.set()
        if wait and self.thread.is_alive():
            self.thread.join()
        self.api.close()
        self.queue.close()

    def enqueue_downloads(self, downloads: Sequence[DownloadedMedia], *, phase: str) -> int:
        """将新下载的媒体转换为队列任务，并返回成功入队数量。"""

        tasks: List[QueueTask] = []
        for download in downloads:
            metadata = self._build_metadata(download)
            caption = self._build_caption(metadata)
            media_type = download.media_type
            for chat_id in self.config.telegram_chat_ids:
                group_key = metadata.get("work_id") or download.path.stem
                group_id = f"{chat_id}:{group_key}"
                task = QueueTask(
                    chat_id=chat_id,
                    file_path=download.path,
                    media_type=media_type,
                    caption=caption,
                    work_id=str(metadata.get("work_id")) if metadata.get("work_id") else None,
                    page=metadata.get("page"),
                    metadata=metadata,
                    group_id=group_id,
                )
                tasks.append(task)
        inserted = self.queue.enqueue(tasks)
        if inserted:
            LOGGER.info(
                "queued telegram notifications",
                extra={"phase": phase, "count": inserted},
            )
            self.metrics.record_enqueued(inserted)
        return inserted

    def metrics_snapshot(self) -> Dict[str, object]:
        """返回指标快照，包含队列长度与失败计数。"""

        snapshot = self.metrics.snapshot()
        snapshot["tg_queue"] = self.queue.pending_count()
        snapshot["tg_queue_total"] = self.queue.total_count()
        snapshot["tg_failed_total"] = self.queue.failed_count()
        return snapshot

    def drain(self, timeout: float = 30.0) -> None:
        """在单次模式下等待队列清空或超时。"""

        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.queue.pending_count() == 0:
                return
            time.sleep(min(1.0, deadline - time.time()))

    def _run(self) -> None:
        """线程主循环：不断取批次并发送。"""

        while not self.stop_event.is_set():
            batch = self.queue.fetch_next_batch()
            if batch is None:
                self.stop_event.wait(self.config.telegram_worker_poll_seconds)
                continue
            self._process_batch(batch)

    def _process_batch(self, batch: QueueBatch) -> None:
        """根据配置选择 media group 或单条发送。"""

        remaining = list(batch.rows)
        while remaining:
            chunk = remaining[: self._media_group_limit()]
            remaining = remaining[self._media_group_limit():]
            if len(chunk) > 1 and self.config.telegram_send_media_group:
                try:
                    self._send_media_group(chunk)
                except TelegramFileTooLargeError as exc:
                    LOGGER.warning("telegram media group too large, falling back", extra={"group": batch.group_id, "error": str(exc)})
                    if self.config.telegram_document_fallback:
                        to_skip, to_retry = self._partition_skippable(chunk)
                        if to_skip:
                            self._mark_skipped(to_skip, str(exc))
                        if to_retry:
                            self.queue.update_media_type(to_retry, "document")
                            delay = self._next_delay(max(row.attempts for row in to_retry) + 1)
                            self.queue.reschedule(to_retry, delay, str(exc))
                            self.metrics.record_retry(len(to_retry))
                        return
                    self.queue.mark_failed(chunk, str(exc))
                    self.metrics.record_failed(len(chunk))
                    return
                except TelegramRateLimitError as exc:
                    LOGGER.warning("telegram rate limited", extra={"group": batch.group_id, "retry_after": exc.retry_after})
                    to_skip, to_retry = self._partition_skippable(chunk)
                    if to_skip:
                        self._mark_skipped(to_skip, str(exc))
                    if to_retry:
                        delay = (exc.retry_after or self.config.telegram_retry_backoff_initial) + self.config.telegram_rate_limit_padding_seconds
                        self.queue.reschedule(to_retry, delay, str(exc))
                        self.metrics.record_rate_limit()
                    else:
                        self.metrics.record_rate_limit()
                    return
                except TelegramRecoverableError as exc:
                    LOGGER.warning("telegram recoverable error", extra={"group": batch.group_id, "error": str(exc)})
                    to_skip, to_retry = self._partition_skippable(chunk)
                    if to_skip:
                        self._mark_skipped(to_skip, str(exc))
                    if to_retry:
                        delay = self._next_delay(max(row.attempts for row in to_retry) + 1)
                        self.queue.reschedule(to_retry, delay, str(exc))
                        self.metrics.record_retry(len(to_retry))
                    return
                except TelegramFatalError as exc:
                    LOGGER.error("telegram fatal error", extra={"group": batch.group_id, "error": str(exc)})
                    self.queue.mark_failed(chunk, str(exc))
                    self.metrics.record_failed(len(chunk))
                    return
                else:
                    self.queue.mark_sent(chunk)
                    self.metrics.record_sent(len(chunk))
            else:
                for row in chunk:
                    try:
                        self._send_single(row)
                    except TelegramFileTooLargeError as exc:
                        LOGGER.warning("telegram file too large", extra={"file": str(row.file_path)})
                        if self.config.telegram_document_fallback and row.media_type != "document":
                            to_skip, to_retry = self._partition_skippable([row])
                            if to_skip:
                                self._mark_skipped(to_skip, str(exc))
                                continue
                            if not to_retry:
                                continue
                            self.queue.update_media_type(to_retry, "document")
                            delay = self._next_delay(max(r.attempts for r in to_retry) + 1)
                            self.queue.reschedule(to_retry, delay, str(exc))
                            self.metrics.record_retry(len(to_retry))
                        else:
                            self.queue.mark_failed([row], str(exc))
                            self.metrics.record_failed(1)
                        continue
                    except TelegramRateLimitError as exc:
                        LOGGER.warning("telegram rate limited", extra={"file": str(row.file_path), "retry_after": exc.retry_after})
                        to_skip, to_retry = self._partition_skippable([row])
                        if to_skip:
                            self._mark_skipped(to_skip, str(exc))
                            continue
                        if not to_retry:
                            continue
                        delay = (exc.retry_after or self.config.telegram_retry_backoff_initial) + self.config.telegram_rate_limit_padding_seconds
                        self.queue.reschedule(to_retry, delay, str(exc))
                        self.metrics.record_rate_limit()
                        return
                    except TelegramRecoverableError as exc:
                        LOGGER.warning("telegram recoverable error", extra={"file": str(row.file_path), "error": str(exc)})
                        to_skip, to_retry = self._partition_skippable([row])
                        if to_skip:
                            self._mark_skipped(to_skip, str(exc))
                            continue
                        if not to_retry:
                            continue
                        delay = self._next_delay(max(r.attempts for r in to_retry) + 1)
                        self.queue.reschedule(to_retry, delay, str(exc))
                        self.metrics.record_retry(len(to_retry))
                        continue
                    except TelegramFatalError as exc:
                        LOGGER.error("telegram fatal error", extra={"file": str(row.file_path), "error": str(exc)})
                        self.queue.mark_failed([row], str(exc))
                        self.metrics.record_failed(1)
                        continue
                    except FileNotFoundError:
                        LOGGER.error("media file missing for telegram", extra={"file": str(row.file_path)})
                        self.queue.mark_failed([row], "file missing")
                        self.metrics.record_failed(1)
                        continue
                    else:
                        self.queue.mark_sent([row])
                        self.metrics.record_sent(1)

    def _send_media_group(self, rows: Sequence[QueueRow]) -> None:
        """调用 Telegram API 发送媒体组。"""

        if not rows:
            return
        self.api.send_media_group(rows[0].chat_id, rows, disable_notifications=self.config.telegram_disable_notifications)

    def _send_single(self, row: QueueRow) -> None:
        """调用 Telegram API 发送单个媒体文件。"""

        self.api.send_single(row, disable_notifications=self.config.telegram_disable_notifications)

    def _media_group_limit(self) -> int:
        """返回 Telegram 官方限制的媒体组上限。"""

        return 10

    def _next_delay(self, attempt: int) -> float:
        """根据尝试次数计算指数退避延迟。"""

        delay = self.config.telegram_retry_backoff_initial * (
            self.config.telegram_retry_backoff_multiplier ** max(0, attempt - 1)
        )
        return min(delay, self.config.telegram_retry_backoff_max)

    def _build_metadata(self, download: DownloadedMedia) -> Dict[str, object]:
        """从 ``DownloadedMedia`` 中提取 caption 渲染所需字段。"""

        metadata = {
            "work_id": download.metadata.work_id,
            "title": download.metadata.title,
            "author": download.metadata.author_name,
            "author_id": download.metadata.author_id,
            "author_url": download.metadata.author_url,
            "work_url": download.metadata.work_url,
            "page": download.metadata.page,
            "tags": list(download.metadata.tags),
            "file_name": download.path.name,
            "file_path": str(download.path),
            "timestamp": download.created_at,
        }
        return metadata

    def _build_caption(self, metadata: Dict[str, object]) -> str:
        """渲染消息文本并控制长度、可选追加标签。"""

        template = self.config.telegram_caption_template or "{title}"
        context = _SafeDict(metadata)
        caption = template.format_map(context)
        if self.config.telegram_include_tags:
            tags = metadata.get("tags") or []
            if tags:
                tags_text = ", ".join(str(tag) for tag in tags[:10])
                caption = f"{caption}\nTags: {tags_text}" if caption else f"Tags: {tags_text}"
        max_len = max(1, self.config.telegram_caption_max_length)
        if len(caption) > max_len:
            caption = caption[: max_len - 1] + "…"
        return caption


class _SafeDict(dict):
    """格式化模板时遇到缺失键返回空字符串。"""

    def __missing__(self, key: str) -> str:  # type: ignore[override]
        return ""
