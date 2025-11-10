from __future__ import annotations

import json
from pathlib import Path

import fcntl
import pytest
"""核心流程的单元测试，覆盖命令构建、回灌判定与队列逻辑。"""

from pixwatch_dl import backfill
from pixwatch_dl.config import Config
from pixwatch_dl.gallery import GalleryDlRunner, GalleryResult, GalleryStats
from pixwatch_dl.poller import Poller, CycleOutcome
from pixwatch_dl.telegram import (
    QueueTask,
    TelegramDispatcher,
    TelegramQueue,
    TelegramRecoverableError,
)


@pytest.fixture()
def sample_config(tmp_path: Path) -> Config:
    """构造带有临时目录的默认配置。"""

    download_dir = tmp_path / "downloads"
    archive = tmp_path / "archive.txt"
    lock = tmp_path / "lock"
    health = tmp_path / "health.json"
    download_dir.mkdir(parents=True, exist_ok=True)
    archive.parent.mkdir(parents=True, exist_ok=True)
    health.parent.mkdir(parents=True, exist_ok=True)
    return Config(
        user_id=123,
        download_directory=download_dir,
        archive_path=archive,
        lock_path=lock,
        health_path=health,
        log_level="DEBUG",
        gallery_dl_path="gallery-dl",
        concurrency=2,
        sleep=0.1,
        sleep_request=0.2,
        retries=3,
        timeout_seconds=5,
        poll_interval_seconds=1,
        max_run_seconds=10,
        max_attempts=3,
        disk_free_threshold_bytes=1024,
        backoff_initial_seconds=0.01,
        backoff_multiplier=2,
        backoff_max_seconds=0.1,
        proxy=None,
        extra_gallery_args=(),
    )


def test_build_command_includes_archive_and_url(sample_config: Config) -> None:
    runner = GalleryDlRunner(sample_config)
    command = runner.build_command(sample_config.bookmarks_url)
    assert command[0] == sample_config.gallery_dl_path
    assert "--download-archive" in command
    assert "--write-metadata" in command
    # ``-d`` 与下载目录参数必须成对出现，确保写入指定目录。
    assert "-d" in command
    assert command[command.index("-d") + 1] == str(sample_config.download_directory)
    assert command[-1] == sample_config.bookmarks_url
    assert command[-2] == "--"
    assert str(sample_config.archive_path) in command


def test_parse_stats_from_gallery_output(sample_config: Config) -> None:
    runner = GalleryDlRunner(sample_config)
    output = """[gallery-dl][download] Downloaded 4 images\n[gallery-dl][download] Skipped 2 \n[gallery-dl][error] Failed 1"""
    stats = runner._parse_stats(output)
    assert stats.added == 4
    assert stats.skipped == 2
    assert stats.failed == 1


def test_backfill_detection(tmp_path: Path, sample_config: Config) -> None:
    archive = sample_config.archive_path
    if archive.exists():
        archive.unlink()
    assert backfill.needs_backfill(sample_config) is True
    archive.write_text("existing")
    assert backfill.needs_backfill(sample_config) is False


class DummyRunner:
    """简化的 Runner，用于模拟成功或失败的执行结果。"""

    def __init__(self, result: GalleryResult) -> None:
        self.result = result
        self.calls = 0
        self.last_kwargs: dict[str, object] = {}

    def run(self, url: str, **kwargs) -> GalleryResult:
        self.calls += 1
        self.last_kwargs = kwargs
        return self.result


def test_poller_skip_when_locked(sample_config: Config) -> None:
    result = GalleryResult(
        url="http://example.com",
        exit_code=0,
        duration=1.0,
        attempts=1,
        stdout="",
        stderr="",
        stats=GalleryStats(),
    )
    runner = DummyRunner(result)
    poller = Poller(sample_config, runner)  # type: ignore[arg-type]
    with open(sample_config.lock_path, "w") as fh:
        fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        outcome = poller.run_periodic_cycle()
    assert outcome.status == "skipped"
    assert runner.calls == 0


def test_poller_records_success(sample_config: Config, monkeypatch: pytest.MonkeyPatch) -> None:
    result = GalleryResult(
        url="http://example.com",
        exit_code=0,
        duration=1.0,
        attempts=1,
        stdout="",
        stderr="",
        stats=GalleryStats(added=1, skipped=0, failed=0),
    )
    runner = DummyRunner(result)
    poller = Poller(sample_config, runner)  # type: ignore[arg-type]

    outcome = poller.run_periodic_cycle()
    assert outcome.status == "success"
    assert outcome.result is result
    with sample_config.health_path.open() as fh:
        payload = json.load(fh)
    assert payload["status"] == "success"
    assert payload["added"] == 1


def test_poller_records_failure(sample_config: Config) -> None:
    result = GalleryResult(
        url="http://example.com",
        exit_code=2,
        duration=1.0,
        attempts=1,
        stdout="",
        stderr="Invalid range provided",
        stats=GalleryStats(added=0, skipped=0, failed=0),
    )
    runner = DummyRunner(result)
    poller = Poller(sample_config, runner)  # type: ignore[arg-type]

    outcome = poller.run_periodic_cycle()
    assert outcome.status == "failed"
    with sample_config.health_path.open() as fh:
        payload = json.load(fh)
    assert payload["status"] == "failed"
    assert payload["exit_code"] == 2
    assert payload["error"] == "Invalid range provided"


def test_poller_limited_cycle_uses_range(sample_config: Config) -> None:
    result = GalleryResult(
        url="http://example.com",
        exit_code=0,
        duration=1.0,
        attempts=1,
        stdout="",
        stderr="",
        stats=GalleryStats(added=2, skipped=0, failed=0),
    )
    runner = DummyRunner(result)
    poller = Poller(sample_config, runner)  # type: ignore[arg-type]

    outcome = poller.run_limited_cycle(5)
    assert outcome.status == "success"
    assert runner.last_kwargs.get("extra") == ["--range", ":5"]

def test_poller_disk_guard(sample_config: Config, monkeypatch: pytest.MonkeyPatch) -> None:
    runner = DummyRunner(
        GalleryResult(
            url="http://example.com",
            exit_code=0,
            duration=1.0,
            attempts=1,
            stdout="",
            stderr="",
            stats=GalleryStats(),
        )
    )
    poller = Poller(sample_config, runner)  # type: ignore[arg-type]

    monkeypatch.setattr("pixwatch_dl.poller.shutil.disk_usage", lambda _: (100, 99, 0))
    outcome = poller.run_periodic_cycle()
    assert outcome.status == "skipped"
    assert runner.calls == 0


def test_telegram_queue_deduplicates(tmp_path: Path) -> None:
    queue_path = tmp_path / "queue.sqlite"
    queue = TelegramQueue(queue_path)
    media_path = tmp_path / "image.jpg"
    media_path.write_text("placeholder")
    task = QueueTask(
        chat_id="123",
        file_path=media_path,
        media_type="photo",
        caption="caption",
        work_id="999",
        page=0,
        metadata={"work_id": "999"},
        group_id="123:999",
    )
    first = queue.enqueue([task])
    assert first == 1
    batch = queue.fetch_next_batch()
    assert batch is not None
    assert batch.rows
    queue.mark_sent(batch.rows)
    second = queue.enqueue([task])
    assert second == 0
    queue.close()


def test_telegram_skip_after_attempts(tmp_path: Path) -> None:
    download_dir = tmp_path / "downloads"
    download_dir.mkdir()
    media_path = download_dir / "image.jpg"
    media_path.write_bytes(b"data")
    archive = tmp_path / "archive.txt"
    lock = tmp_path / "lock"
    health = tmp_path / "health.json"
    queue_path = tmp_path / "queue.sqlite"
    queue_path.parent.mkdir(parents=True, exist_ok=True)

    config = Config(
        user_id=456,
        download_directory=download_dir,
        archive_path=archive,
        lock_path=lock,
        health_path=health,
        telegram_enabled=True,
        telegram_bot_token="dummy",
        telegram_chat_ids=("123",),
        telegram_queue_path=queue_path,
        telegram_max_attempts=2,
        telegram_retry_backoff_initial=0.0,
        telegram_retry_backoff_multiplier=1.0,
        telegram_retry_backoff_max=0.0,
    )

    dispatcher = TelegramDispatcher(config)

    class DummyAPI:
        def send_single(self, row, disable_notifications: bool) -> None:
            raise TelegramRecoverableError("temp")

        def send_media_group(self, chat_id: str, rows, *, disable_notifications: bool) -> None:
            raise TelegramRecoverableError("temp")

        def close(self) -> None:
            return None

    dispatcher.api = DummyAPI()  # type: ignore[assignment]

    task = QueueTask(
        chat_id="123",
        file_path=media_path,
        media_type="photo",
        caption="",
        work_id="w1",
        page=0,
        metadata={"work_id": "w1"},
        group_id="123:w1",
    )
    inserted = dispatcher.queue.enqueue([task])
    assert inserted == 1

    batch = dispatcher.queue.fetch_next_batch()
    assert batch is not None
    dispatcher._process_batch(batch)

    batch = dispatcher.queue.fetch_next_batch()
    assert batch is not None
    dispatcher._process_batch(batch)

    assert dispatcher.queue.pending_count() == 0
    snapshot = dispatcher.metrics.snapshot()
    assert snapshot["tg_skipped"] == 1

    dispatcher.shutdown(wait=False)
