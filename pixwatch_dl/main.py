from __future__ import annotations

import logging
"""守护进程入口。"""

import logging
import signal
import sys
import threading

from . import backfill
from .config import load_config
from .gallery import GalleryDlRunner, GalleryDlError
from .logging_setup import setup_logging
from .poller import Poller
from .telegram import TelegramDispatcher


LOGGER = logging.getLogger(__name__)


def _install_signal_handlers(stop_event: threading.Event) -> None:
    """注册 SIGINT/SIGTERM 处理器，实现优雅退出。"""

    def handler(signum, frame):  # type: ignore[override]
        LOGGER.info("received signal, shutting down", extra={"signal": signum})
        stop_event.set()

    signal.signal(signal.SIGTERM, handler)
    signal.signal(signal.SIGINT, handler)


def main() -> int:
    """守护模式主流程。"""

    config = load_config()
    setup_logging(config.log_level)

    runner = GalleryDlRunner(config)
    dispatcher = None
    if config.telegram_active:
        dispatcher = TelegramDispatcher(config)
        dispatcher.start()
    poller = Poller(config, runner, dispatcher)

    try:
        if backfill.needs_backfill(config):
            result = backfill.run_backfill(config, runner)
            if dispatcher and result.exit_code == 0 and result.downloads:
                dispatcher.enqueue_downloads(result.downloads, phase="full")
    except GalleryDlError as exc:
        LOGGER.error(
            "backfill failed",
            extra={"exit_code": exc.exit_code, "attempts": exc.attempts, "error": str(exc)},
        )
        if dispatcher:
            dispatcher.shutdown()
        return 1

    stop_event = threading.Event()
    _install_signal_handlers(stop_event)

    LOGGER.info("starting poller", extra={"interval": config.poll_interval_seconds})
    try:
        poller.run_forever(stop_event)
    except Exception:
        LOGGER.exception("unexpected error in poller loop")
        return 1
    finally:
        LOGGER.info("shutdown complete")
        if dispatcher:
            dispatcher.shutdown()

    return 0


if __name__ == "__main__":
    sys.exit(main())
