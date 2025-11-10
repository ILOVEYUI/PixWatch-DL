"""限量守护模式入口。"""

from __future__ import annotations

import logging
import signal
import sys
import threading

from .config import load_config
from .gallery import GalleryDlRunner
from .logging_setup import setup_logging
from .poller import Poller
from .telegram import TelegramDispatcher


LOGGER = logging.getLogger(__name__)


def _install_signal_handlers(stop_event: threading.Event) -> None:
    """注册信号处理器以便优雅退出。"""

    def handler(signum, frame):  # type: ignore[override]
        LOGGER.info("received signal, shutting down", extra={"signal": signum})
        stop_event.set()

    signal.signal(signal.SIGTERM, handler)
    signal.signal(signal.SIGINT, handler)


def main() -> int:
    """仅扫描前 ``limited_bookmark_limit`` 条收藏的常驻模式。"""

    config = load_config()
    setup_logging(config.log_level)

    limit = config.limited_bookmark_limit or 0
    if limit <= 0:
        LOGGER.error("limited mode requires a positive limited_bookmark_limit")
        return 2
    interval = float(config.limited_poll_interval_seconds or config.poll_interval_seconds)

    runner = GalleryDlRunner(config)
    dispatcher = None
    if config.telegram_active:
        dispatcher = TelegramDispatcher(config)
        dispatcher.start()

    poller = Poller(config, runner, dispatcher)
    stop_event = threading.Event()
    _install_signal_handlers(stop_event)

    LOGGER.info(
        "starting limited poller",
        extra={"interval": interval, "limit": limit},
    )

    try:
        poller.run_limited_forever(stop_event, interval=interval, limit=limit)
    except Exception:
        LOGGER.exception("unexpected error in limited poller")
        return 1
    finally:
        LOGGER.info("limited mode shutdown complete")
        if dispatcher:
            dispatcher.shutdown()

    return 0


if __name__ == "__main__":
    sys.exit(main())
