"""简易向导运行入口。

本模块提供一个面向非技术用户的交互式命令行：

1. 引导用户填写最少量的必填配置，并生成 TOML 配置文件；
2. 自动设置 ``PIXWATCH_CONFIG_FILE`` 环境变量，调用现有的守护模式或单次执行逻辑；
3. 输出清晰的中文提示，帮助用户理解每一步的意义与可能出现的错误。

整个实现仅依赖 Python 标准库，方便在 Linux 服务器上直接执行。
"""

from __future__ import annotations

import os
import sys
import textwrap
from pathlib import Path
from typing import Callable

from . import main as daemon_main
from . import once as once_main
from . import limited as limited_main


def _prompt(text: str, *, validator: Callable[[str], bool], error_message: str) -> str:
    """通用输入函数，确保用户输入符合要求。

    :param text: 提示语句（包含输入说明）。
    :param validator: 验证函数，返回 ``True`` 表示输入有效。
    :param error_message: 当验证失败时向用户展示的中文错误说明。
    :return: 通过验证的原始字符串。
    """

    while True:
        value = input(text).strip()
        if validator(value):
            return value
        print(error_message)


def _prompt_int(text: str) -> int:
    """专用整数输入函数，重复提示直到获得合法整数。"""

    while True:
        raw = input(text).strip()
        try:
            return int(raw)
        except ValueError:
            print("请输入数字。")


def _prompt_positive_int(text: str) -> int:
    """获取大于 0 的整数，适用于限量模式参数。"""

    while True:
        value = _prompt_int(text)
        if value > 0:
            return value
        print("请输入大于 0 的数字。")


def _ensure_directory(path: Path) -> None:
    """确保目录存在，若不存在则创建。"""

    if not path.exists():
        path.mkdir(parents=True, exist_ok=True)


def _write_config_file(config_path: Path) -> None:
    """根据用户交互输入写入最基础的 TOML 配置。"""

    print("\n=== 配置向导 ===")
    user_id = _prompt_int("请输入 Pixiv 用户 ID（仅公共收藏）：")

    download_directory = Path(
        _prompt(
            "请输入下载保存目录（例如 /data/pixiv）：",
            validator=lambda value: bool(value),
            error_message="下载目录不能为空。",
        )
    ).expanduser().resolve()
    _ensure_directory(download_directory)

    archive_path = Path(
        _prompt(
            "请输入 gallery-dl archive 文件路径（例如 /data/pixiv/archive.txt）：",
            validator=lambda value: bool(value),
            error_message="archive 路径不能为空。",
        )
    ).expanduser().resolve()
    _ensure_directory(archive_path.parent)

    lock_path = Path(
        _prompt(
            "请输入锁文件路径（例如 /var/run/pixwatch.lock）：",
            validator=lambda value: bool(value),
            error_message="锁文件路径不能为空。",
        )
    ).expanduser().resolve()
    _ensure_directory(lock_path.parent)

    health_path = Path(
        _prompt(
            "请输入健康状态文件路径（例如 /var/lib/pixwatch/health.json）：",
            validator=lambda value: bool(value),
            error_message="健康文件路径不能为空。",
        )
    ).expanduser().resolve()
    _ensure_directory(health_path.parent)

    telegram_enabled = input("是否启用 Telegram 推送？(y/N)：").strip().lower() == "y"
    telegram_block = ""
    if telegram_enabled:
        bot_token = _prompt(
            "请输入 Telegram Bot Token：",
            validator=lambda value: bool(value),
            error_message="Bot Token 不能为空。",
        )
        chat_ids_raw = _prompt(
            "请输入目标 Chat ID（多个以英文逗号分隔）：",
            validator=lambda value: bool(value),
            error_message="Chat ID 不能为空。",
        )
        chat_ids = ",".join(part.strip() for part in chat_ids_raw.split(",") if part.strip())
        queue_path = download_directory / "telegram_queue.sqlite3"
        telegram_block = textwrap.dedent(
            f"""
            telegram_enabled = true
            telegram_bot_token = "{bot_token}"
            telegram_chat_ids = [{", ".join(f'\"{cid}\"' for cid in chat_ids.split(','))}]
            telegram_queue_path = "{queue_path}"
            """
        ).strip()
    else:
        telegram_block = "telegram_enabled = false"

    config_content = textwrap.dedent(
        f"""
        # 由 pixwatch-easy 向导生成的最小配置
        user_id = {user_id}
        download_directory = "{download_directory}"
        archive_path = "{archive_path}"
        lock_path = "{lock_path}"
        health_path = "{health_path}"
        {telegram_block}
        """
    ).strip() + "\n"

    config_path.write_text(config_content, encoding="utf-8")
    print(f"\n已生成配置文件：{config_path}")


def _choose_action() -> str:
    """让用户选择运行模式。"""

    print("\n=== 运行模式 ===")
    print("1. 持续守护运行（每 10 分钟同步一次）")
    print("2. 仅执行一次同步（适合定时任务测试）")
    print("3. 限量守护模式（每轮仅扫描前 Y 条收藏）")
    print("0. 退出")
    while True:
        choice = input("请选择：").strip()
        if choice in {"0", "1", "2", "3"}:
            return choice
        print("请输入 0、1、2 或 3。")


def _run_with_config(config_path: Path, choice: str, limited_params: tuple[int, int] | None = None) -> int:
    """根据用户选择调用具体的执行入口。"""

    os.environ["PIXWATCH_CONFIG_FILE"] = str(config_path)
    if limited_params is not None:
        interval_minutes, limit = limited_params
        os.environ["PIXWATCH_LIMITED_POLL_INTERVAL_SECONDS"] = str(interval_minutes * 60)
        os.environ["PIXWATCH_LIMITED_BOOKMARK_LIMIT"] = str(limit)
    if choice == "1":
        return daemon_main.main()
    if choice == "2":
        return once_main.main()
    if choice == "3":
        return limited_main.main()
    return 0


def main() -> int:
    """模块主入口，负责协调全部交互步骤。"""

    print("欢迎使用 PixWatch 简易向导。")
    config_path_input = input(
        "请输入配置文件路径（默认 ./pixwatch.toml）："
    ).strip()
    if not config_path_input:
        config_path_input = "./pixwatch.toml"
    config_path = Path(config_path_input).expanduser().resolve()

    if config_path.exists():
        print(f"检测到已有配置文件：{config_path}")
        regenerate = input("是否覆盖重建？(y/N)：").strip().lower() == "y"
        if regenerate:
            _write_config_file(config_path)
    else:
        _write_config_file(config_path)

    choice = _choose_action()
    if choice == "0":
        print("已取消执行。")
        return 0

    limited_params: tuple[int, int] | None = None
    if choice == "3":
        print("\n=== 限量模式参数 ===")
        interval_minutes = _prompt_positive_int("请输入轮询间隔（分钟）：")
        limit = _prompt_positive_int("请输入每轮扫描的收藏数量上限：")
        limited_params = (interval_minutes, limit)

    try:
        exit_code = _run_with_config(config_path, choice, limited_params)
    except KeyboardInterrupt:
        print("\n已收到中断信号，正在退出。")
        return 1

    print("执行完毕。")
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
