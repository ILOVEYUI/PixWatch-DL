from __future__ import annotations

"""单次执行入口。"""

import logging
import sys

from . import backfill
from .config import load_config
from .gallery import GalleryDlError, GalleryDlRunner
from .logging_setup import setup_logging
from .poller import Poller
from .telegram import TelegramDispatcher

LOGGER = logging.getLogger(__name__)


def main() -> int:
    """执行一次同步任务，适用于 systemd timer。"""

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

    outcome = poller.run_periodic_cycle()
    if dispatcher:
        dispatcher.drain()
        dispatcher.shutdown()
    if outcome.status == "success" and outcome.result and outcome.result.exit_code == 0:
        return 0
    if outcome.status == "skipped":
        LOGGER.info("sync skipped", extra={"status": outcome.status})
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())
