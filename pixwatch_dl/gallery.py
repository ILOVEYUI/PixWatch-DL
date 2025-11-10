"""gallery-dl 调用封装。

该模块承担以下职责：

1. 根据配置构建 ``gallery-dl`` 命令行，并提供指数退避、限时终止等保护；
2. 解析 ``gallery-dl`` 输出的统计信息，提取新增/跳过/失败数量；
3. 扫描下载目录，收集本轮新增的媒体文件及其元数据，供 Telegram 通知模块使用。

所有关键函数和数据结构都附带中文注释，以便快速理解行为。
"""

from __future__ import annotations

import json
import logging
import os
import shlex
import subprocess
import time
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Set

from .config import Config


@dataclass
class DownloadMetadata:
    """描述单个下载媒体的元数据。"""

    work_id: Optional[str] = None
    title: Optional[str] = None
    author_name: Optional[str] = None
    author_id: Optional[str] = None
    work_url: Optional[str] = None
    author_url: Optional[str] = None
    page: Optional[int] = None
    tags: Sequence[str] = field(default_factory=tuple)
    raw: dict[str, object] = field(default_factory=dict)


@dataclass
class DownloadedMedia:
    """表示一次新下载的文件。"""

    path: Path
    media_type: str
    created_at: float
    metadata: DownloadMetadata = field(default_factory=DownloadMetadata)


@dataclass
class GalleryStats:
    """存放 gallery-dl 输出的统计数量。"""

    added: int = 0
    skipped: int = 0
    failed: int = 0


@dataclass
class GalleryResult:
    """封装一次 gallery-dl 执行的结果。"""

    url: str
    exit_code: int
    duration: float
    attempts: int
    stdout: str
    stderr: str
    stats: GalleryStats
    downloads: List[DownloadedMedia] = field(default_factory=list)

    @property
    def success(self) -> bool:
        """返回命令是否成功退出。"""

        return self.exit_code == 0


class GalleryDlError(RuntimeError):
    """在所有重试均失败时抛出的异常。"""

    def __init__(self, message: str, *, attempts: int, exit_code: int, stdout: str, stderr: str) -> None:
        super().__init__(message)
        self.attempts = attempts
        self.exit_code = exit_code
        self.stdout = stdout
        self.stderr = stderr


class GalleryDlRunner:
    """包装 ``gallery-dl`` 的执行逻辑。"""

    def __init__(self, config: Config) -> None:
        self.config = config
        self.logger = logging.getLogger(__name__)

    def build_command(self, url: str, *, extra: Optional[Iterable[str]] = None) -> List[str]:
        """构建 ``gallery-dl`` 命令行参数列表。"""

        command: List[str] = [
            self.config.gallery_dl_path,
            "--download-archive",
            str(self.config.archive_path),
            "-d",
            str(self.config.download_directory),
            "--write-metadata",
            "--sleep",
            str(self.config.sleep),
            "--sleep-request",
            str(self.config.sleep_request),
            "--retries",
            str(self.config.retries),
            "-j",
            str(self.config.concurrency),
        ]
        command.extend(self.config.extra_gallery_args)
        if extra:
            command.extend(extra)
        # ``argparse`` 在解析 ``gallery-dl`` 的 CLI 时允许使用 ``--`` 显式结束选项段。
        # 这里提前补上一段 ``--``，可以避免某些旧版本在带有自定义参数时把 URL 误判为
        # 额外选项，导致 ``unrecognized arguments`` 错误。
        command.append("--")
        command.append(url)
        return command

    def run(self, url: str, *, timeout: Optional[int] = None, extra: Optional[Iterable[str]] = None) -> GalleryResult:
        """执行命令并在必要时重试。

        - ``timeout``：单次命令的超时时间；
        - ``max_attempts``：从配置读取，控制最大重试次数；
        - 在每次成功执行后，会比对下载目录快照以收集新增文件。
        """

        timeout = timeout or self.config.timeout_seconds
        attempts = 0
        delay = self.config.backoff_initial_seconds
        deadline = time.monotonic() + self.config.max_run_seconds
        last_stdout = ""
        last_stderr = ""
        last_exit = -1
        max_attempts = max(1, self.config.max_attempts)
        baseline = self._snapshot_files()
        while attempts <= max_attempts:
            attempts += 1
            start = time.monotonic()
            try:
                command = self.build_command(url, extra=extra)
                self.logger.debug(
                    "invoking gallery-dl",
                    extra={
                        "url": url,
                        "attempt": attempts,
                        "timeout": timeout,
                    },
                )
                stdout, stderr, exit_code = self._run_once(command, timeout)
                duration = time.monotonic() - start
                stats = self._parse_stats(stdout)
                downloads: List[DownloadedMedia] = []
                result = GalleryResult(
                    url=url,
                    exit_code=exit_code,
                    duration=duration,
                    attempts=attempts,
                    stdout=stdout,
                    stderr=stderr,
                    stats=stats,
                    downloads=downloads,
                )
                if exit_code == 0:
                    downloads.extend(self._collect_downloads(baseline))
                    result.downloads = downloads
                    return result
                if not self._should_retry(exit_code, stderr):
                    return result
                self.logger.warning(
                    "gallery-dl attempt failed",
                    extra={
                        "url": url,
                        "attempt": attempts,
                        "exit_code": exit_code,
                    },
                )
                last_stdout, last_stderr, last_exit = stdout, stderr, exit_code
                if attempts >= max_attempts or time.monotonic() + delay > deadline:
                    break
                time.sleep(delay)
                delay = min(delay * self.config.backoff_multiplier, self.config.backoff_max_seconds)
            except TimeoutError:
                self.logger.warning(
                    "gallery-dl attempt timed out",
                    extra={"url": url, "attempt": attempts, "timeout": timeout},
                )
                last_stdout = last_stdout or ""
                last_stderr = (last_stderr + "\n" if last_stderr else "") + "timeout"
                last_exit = -1
                if attempts >= max_attempts or time.monotonic() > deadline:
                    break
                time.sleep(delay)
                delay = min(delay * self.config.backoff_multiplier, self.config.backoff_max_seconds)
        raise GalleryDlError(
            f"gallery-dl failed after {attempts} attempts (exit={last_exit})",
            attempts=attempts,
            exit_code=last_exit,
            stdout=last_stdout,
            stderr=last_stderr,
        )

    def _should_retry(self, exit_code: int, stderr: str) -> bool:
        if exit_code == 0:
            return False
        if exit_code in {1, 2}:
            haystack = (stderr or "").lower()
            for token in ("http 429", "timed out", "temporarily unavailable", "timeout", "connection reset"):
                if token in haystack:
                    return True
        return False

    def _run_once(self, command: List[str], timeout: int) -> tuple[str, str, int]:
        """执行一次 ``gallery-dl`` 子进程。"""

        env = os.environ.copy()
        if self.config.proxy:
            env["https_proxy"] = self.config.proxy
            env["http_proxy"] = self.config.proxy
        cmd_str = " ".join(shlex.quote(part) for part in command)
        try:
            completed = subprocess.run(
                command,
                capture_output=True,
                text=True,
                check=False,
                timeout=timeout,
                env=env,
            )
        except subprocess.TimeoutExpired as exc:
            self.logger.debug("gallery-dl cmd=%s timeout", cmd_str)
            raise TimeoutError("gallery-dl timed out") from exc
        self.logger.debug(
            "gallery-dl cmd=%s returncode=%s",
            cmd_str,
            completed.returncode,
        )
        return completed.stdout or "", completed.stderr or "", completed.returncode

    def _parse_stats(self, stdout: str) -> GalleryStats:
        """解析 ``gallery-dl`` 的标准输出以统计下载数量。"""

        added = skipped = failed = 0
        for line in stdout.splitlines():
            lower = line.lower()
            if "downloaded" in lower and "skipped" not in lower:
                added = self._extract_number(line, default=added)
            if "skipped" in lower:
                skipped = self._extract_number(line, default=skipped)
            if "failed" in lower:
                failed = self._extract_number(line, default=failed)
        return GalleryStats(added=added, skipped=skipped, failed=failed)

    @staticmethod
    def _extract_number(line: str, default: int = 0) -> int:
        """从一行文本中提取数字，找不到则返回默认值。"""

        for token in line.split():
            if token.isdigit():
                return int(token)
        return default

    def _snapshot_files(self) -> Set[str]:
        """遍历下载目录并记录文件快照。"""

        files: Set[str] = set()
        base = self.config.download_directory
        if not base.exists():
            return files
        for path in base.rglob("*"):
            if path.is_file():
                files.add(str(path.resolve()))
        return files

    def _collect_downloads(self, baseline: Set[str]) -> List[DownloadedMedia]:
        """根据前后快照比对找出新增文件并补充元数据。"""

        after = self._snapshot_files()
        new_entries = after - baseline
        downloads: List[DownloadedMedia] = []
        for entry in new_entries:
            path = Path(entry)
            if not path.exists() or not path.is_file():
                continue
            name_lower = path.name.lower()
            if name_lower.endswith(".part"):
                continue
            suffix = path.suffix.lower()
            if suffix in {".json", ".txt", ".yml", ".yaml", ".log", ".md", ".nfo"}:
                continue
            media_type = self._classify_media_type(path)
            metadata = self._load_metadata(path)
            try:
                created_at = path.stat().st_mtime
            except OSError:
                created_at = time.time()
            downloads.append(
                DownloadedMedia(
                    path=path,
                    media_type=media_type,
                    created_at=created_at,
                    metadata=metadata,
                )
            )
        downloads.sort(
            key=lambda item: (
                item.metadata.work_id or item.path.stem,
                item.metadata.page or 0,
                item.path.name,
            )
        )
        return downloads

    def _classify_media_type(self, path: Path) -> str:
        """根据扩展名推断媒体类型，以匹配 Telegram 的接口。"""

        suffix = path.suffix.lower()
        if suffix in {".jpg", ".jpeg", ".png", ".bmp", ".webp", ".jfif"}:
            return "photo"
        if suffix in {".gif", ".apng"}:
            return "animation"
        if suffix in {".mp4", ".webm", ".mkv", ".mov", ".avi"}:
            return "video"
        return "document"

    def _load_metadata(self, media_path: Path) -> DownloadMetadata:
        """加载与媒体文件同名的 JSON 元数据。"""

        candidates = [
            media_path.with_suffix(media_path.suffix + ".json"),
            media_path.with_suffix(media_path.suffix + ".metadata"),
            media_path.with_suffix(".json"),
        ]
        data: Optional[dict[str, object]] = None
        for candidate in candidates:
            if candidate.exists():
                try:
                    with candidate.open("r", encoding="utf-8") as fh:
                        data = json.load(fh)
                    break
                except (OSError, json.JSONDecodeError):
                    continue
        metadata = DownloadMetadata(raw=data or {})
        if data:
            work_id = data.get("id") or data.get("illust_id") or data.get("work_id")
            if work_id is not None:
                metadata.work_id = str(work_id)
            title = data.get("title")
            if isinstance(title, str):
                metadata.title = title
            user = data.get("user")
            if isinstance(user, dict):
                name = user.get("name")
                if isinstance(name, str):
                    metadata.author_name = name
                author_id = user.get("id")
                if author_id is not None:
                    metadata.author_id = str(author_id)
            author_name = data.get("user_name")
            if isinstance(author_name, str) and not metadata.author_name:
                metadata.author_name = author_name
            author_id = data.get("user_id")
            if author_id is not None and not metadata.author_id:
                metadata.author_id = str(author_id)
            page = data.get("page") or data.get("page_index") or data.get("page_number")
            if isinstance(page, int):
                metadata.page = page
            elif isinstance(page, str) and page.isdigit():
                metadata.page = int(page)
            tags: Sequence[str] = ()
            raw_tags = data.get("tags")
            if isinstance(raw_tags, list):
                tags_list: List[str] = []
                for tag in raw_tags:
                    if isinstance(tag, str):
                        tags_list.append(tag)
                    elif isinstance(tag, dict):
                        name = tag.get("name") or tag.get("tag")
                        if isinstance(name, str):
                            tags_list.append(name)
                tags = tuple(tags_list)
            metadata.tags = tags
            url = data.get("page_url") or data.get("url") or data.get("work_url")
            if isinstance(url, str):
                metadata.work_url = url
            if metadata.work_url is None and metadata.work_id:
                metadata.work_url = f"https://www.pixiv.net/artworks/{metadata.work_id}"
            if metadata.author_id:
                metadata.author_url = f"https://www.pixiv.net/users/{metadata.author_id}"
        if metadata.work_id is None:
            inferred = self._infer_from_filename(media_path)
            metadata.work_id = metadata.work_id or inferred.get("work_id")
            metadata.page = metadata.page or inferred.get("page")
        if metadata.title is None:
            metadata.title = media_path.stem
        if metadata.work_url is None and metadata.work_id:
            metadata.work_url = f"https://www.pixiv.net/artworks/{metadata.work_id}"
        return metadata

    def _infer_from_filename(self, media_path: Path) -> dict[str, Optional[int | str]]:
        """从文件名中提取作品 ID 与页码等信息。"""

        match = re.search(r"(?P<id>\d+)_p(?P<page>\d+)", media_path.stem)
        info: dict[str, Optional[int | str]] = {"work_id": None, "page": None}
        if match:
            info["work_id"] = match.group("id")
            info["page"] = int(match.group("page"))
        return info
