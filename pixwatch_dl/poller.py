from __future__ import annotations

"""轮询与健康检查管理模块。"""

import json
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from threading import Event
from typing import Dict, Optional, TextIO

import fcntl
import shutil

from .config import Config
from .gallery import GalleryDlError, GalleryDlRunner, GalleryResult
from .telegram import TelegramDispatcher


class FileLock:
    """基于 fcntl 的简单文件锁，实现跨进程互斥。"""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._fh: Optional[TextIO] = None

    def acquire(self) -> None:
        """以非阻塞方式申请独占锁，失败时抛出 ``BlockingIOError``。"""

        fh = open(self.path, "a+")
        fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        self._fh = fh

    def release(self) -> None:
        """释放锁并关闭文件句柄。"""

        if self._fh is not None:
            fcntl.flock(self._fh, fcntl.LOCK_UN)
            self._fh.close()
            self._fh = None

    def __enter__(self) -> "FileLock":
        self.acquire()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.release()


@dataclass
class HealthRecord:
    """记录最近一次任务的运行状态。"""

    phase: str
    status: str
    exit_code: int
    added: int
    skipped: int
    failed: int
    attempts: int
    duration: float
    timestamp: float
    telegram: Optional[Dict[str, object]] = None


@dataclass
class CycleOutcome:
    """保存单轮同步的结果，供上层逻辑判断重试或告警。"""

    status: str
    result: Optional[GalleryResult]
    enqueued: int = 0


class Poller:
    """封装周期性同步流程与健康检查写入。"""

    def __init__(self, config: Config, runner: GalleryDlRunner, dispatcher: Optional[TelegramDispatcher] = None) -> None:
        self.config = config
        self.runner = runner
        self.logger = logging.getLogger(__name__)
        self.lock = FileLock(config.lock_path)
        self.dispatcher = dispatcher

    def run_periodic_cycle(self) -> CycleOutcome:
        """执行一次增量同步周期。"""

        return self._run_cycle(phase="periodic")

    def _run_cycle(self, *, phase: str) -> CycleOutcome:
        """执行具体的下载逻辑，并处理异常、写入健康状态。"""

        start = time.monotonic()
        try:
            with self.lock:
                if not self._has_disk_capacity():
                    self.logger.warning(
                        "insufficient disk space, skipping",
                        extra={"phase": phase, "reason": "disk_low"},
                    )
                    record = HealthRecord(
                        phase=phase,
                        status="skipped",
                        exit_code=-1,
                        added=0,
                        skipped=0,
                        failed=0,
                        attempts=0,
                        duration=0.0,
                        timestamp=time.time(),
                    )
                    metrics = self._telegram_metrics()
                    record.telegram = metrics
                    self._write_health(record, metrics)
                    return CycleOutcome(status="skipped", result=None)
                self.logger.info("starting gallery sync", extra={"phase": phase, "url": self.config.bookmarks_url})
                result = self.runner.run(self.config.bookmarks_url)
        except BlockingIOError:
            self.logger.info("previous run still active, skipping", extra={"phase": phase})
            return CycleOutcome(status="skipped", result=None)
        except GalleryDlError as exc:
            duration = time.monotonic() - start
            self.logger.error(
                "gallery-dl failed",
                extra={
                    "phase": phase,
                    "duration": duration,
                    "error": str(exc),
                },
            )
            record = HealthRecord(
                phase=phase,
                status="failed",
                exit_code=getattr(exc, "exit_code", -1),
                added=0,
                skipped=0,
                failed=0,
                attempts=getattr(exc, "attempts", 0),
                duration=duration,
                timestamp=time.time(),
            )
            metrics = self._telegram_metrics()
            record.telegram = metrics
            self._write_health(record, metrics)
            return CycleOutcome(status="failed", result=None)
        else:
            self.logger.info(
                "gallery sync completed",
                extra={
                    "phase": phase,
                    "exit_code": result.exit_code,
                    "duration": result.duration,
                    "added": result.stats.added,
                    "skipped": result.stats.skipped,
                    "failed": result.stats.failed,
                    "attempts": result.attempts,
                },
            )
            record = HealthRecord(
                phase=phase,
                status="success" if result.exit_code == 0 else "failed",
                exit_code=result.exit_code,
                added=result.stats.added,
                skipped=result.stats.skipped,
                failed=result.stats.failed,
                attempts=result.attempts,
                duration=result.duration,
                timestamp=time.time(),
            )
            enqueued = 0
            if self.dispatcher and result.exit_code == 0 and result.downloads:
                try:
                    enqueued = self.dispatcher.enqueue_downloads(result.downloads, phase=phase)
                except Exception:
                    self.logger.exception("failed to enqueue telegram notifications", extra={"phase": phase})
            metrics = self._telegram_metrics()
            record.telegram = metrics
            self._write_health(record, metrics)
            if enqueued:
                self.logger.info(
                    "telegram tasks enqueued",
                    extra={"phase": phase, "count": enqueued},
                )
            return CycleOutcome(status=record.status, result=result, enqueued=enqueued)

    def run_forever(self, stop_event: Event) -> None:
        """在守护模式下持续执行，每轮之间休眠配置的间隔。"""

        while not stop_event.is_set():
            cycle_start = time.monotonic()
            self.run_periodic_cycle()
            elapsed = time.monotonic() - cycle_start
            remaining = max(0, self.config.poll_interval_seconds - elapsed)
            stop_event.wait(remaining)

    def _has_disk_capacity(self) -> bool:
        """判断下载目录所在磁盘是否满足剩余空间阈值。"""

        _, _, free = shutil.disk_usage(self.config.download_directory)
        return free >= self.config.disk_free_threshold_bytes

    def _write_health(self, record: HealthRecord, metrics: Optional[Dict[str, object]] = None) -> None:
        """将健康状态写入配置指定的 JSON 文件。"""

        payload = {
            "phase": record.phase,
            "status": record.status,
            "exit_code": record.exit_code,
            "added": record.added,
            "skipped": record.skipped,
            "failed": record.failed,
            "attempts": record.attempts,
            "duration": record.duration,
            "timestamp": record.timestamp,
        }
        if metrics:
            payload.update(metrics)
        tmp_path = self.config.health_path.with_suffix(".tmp")
        with tmp_path.open("w", encoding="utf-8") as fh:
            json.dump(payload, fh)
        tmp_path.replace(self.config.health_path)

    def _telegram_metrics(self) -> Optional[Dict[str, object]]:
        """从 Telegram dispatcher 获取指标快照，失败时记录错误并返回 ``None``。"""

        if not self.dispatcher:
            return None
        try:
            return self.dispatcher.metrics_snapshot()
        except Exception:
            self.logger.exception("failed to collect telegram metrics")
            return None
