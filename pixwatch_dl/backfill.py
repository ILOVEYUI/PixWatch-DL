"""全量回灌逻辑。

本模块判断是否需要执行初次全量同步，并在必要时调用 ``gallery-dl`` 执行公共收藏下载。
所有日志字段遵循结构化 JSON 规范，方便集中收集与分析。
"""

from __future__ import annotations

import logging

from .config import Config
from .gallery import GalleryDlRunner, GalleryResult


def needs_backfill(config: Config) -> bool:
    """判断是否需要执行全量回灌。

    触发条件：
    - 归档文件不存在；
    - 归档文件存在但为空（意味着尚未记录任何作品 ID）。

    若在读取归档文件属性时出现 ``OSError``，视为损坏或不可访问，同样要求重新回灌。
    """

    archive = config.archive_path
    if not archive.exists():
        return True
    try:
        return archive.stat().st_size == 0
    except OSError:
        return True


def run_backfill(config: Config, runner: GalleryDlRunner) -> GalleryResult:
    """执行一次全量回灌。

    在日志中标记 ``phase=full``，以便与周期性任务做区分。
    返回 :class:`GalleryResult`，供调用方进一步处理 Telegram 队列或健康状态。
    """

    logger = logging.getLogger(__name__)
    logger.info("starting backfill", extra={"phase": "full", "url": config.bookmarks_url})
    result = runner.run(config.bookmarks_url)
    logger.info(
        "finished backfill",
        extra={
            "phase": "full",
            "url": config.bookmarks_url,
            "exit_code": result.exit_code,
            "added": result.stats.added,
            "skipped": result.stats.skipped,
            "failed": result.stats.failed,
            "duration": result.duration,
            "attempts": result.attempts,
        },
    )
    return result
