"""配置加载模块。

本模块负责：
1. 读取外部配置文件与环境变量，并进行数据类型转换；
2. 通过 :class:`Config` 数据类约束所有可用配置项及其默认值；
3. 自动创建下载目录、归档文件、健康检查文件与 Telegram 队列文件所在的目录结构。

所有注释均使用中文，便于后续运维人员快速理解配置的作用与加载流程。
"""

from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from types import UnionType
from typing import Dict, Optional, get_origin, get_args, get_type_hints

import typing


_ENV_PREFIX = "PIXWATCH_"


def _expand_path(value: str) -> Path:
    """展开并规范化路径字符串。

    - 支持 ``~`` 家目录写法；
    - 返回绝对路径，确保后续读写操作不依赖当前工作目录。
    """

    return Path(os.path.expanduser(value)).resolve()


@dataclass(frozen=True)
class Config:
    """程序全局配置。

    字段含义说明（全部为中文，便于查阅）：

    - ``user_id``：需要同步的 Pixiv 用户 ID，仅抓取公共收藏；
    - ``download_directory``：所有文件写入的根目录；
    - ``archive_path``：供 gallery-dl 使用的去重归档文件；
    - ``lock_path``：轮询任务互斥锁文件路径；
    - ``health_path``：写入健康检查 JSON 的路径；
    - ``log_level``：日志级别；
    - ``gallery_dl_path``：可执行文件路径，允许自定义安装位置；
    - ``concurrency``、``sleep``、``sleep_request``、``retries``：控制 gallery-dl 的并发、限速与重试参数；
    - ``timeout_seconds``：单次 gallery-dl 调用的超时时间；
    - ``poll_interval_seconds``：常驻模式下的轮询间隔；
    - ``max_run_seconds``：一次轮询可执行的最大时长，超过则超时终止；
    - ``max_attempts``：单轮指数退避重试次数；
    - ``disk_free_threshold_bytes``：磁盘剩余空间低于该值时跳过任务；
    - ``backoff_initial_seconds``、``backoff_multiplier``、``backoff_max_seconds``：网络错误重试的退避策略；
    - ``proxy``：为 gallery-dl 设置的代理地址（可选）；
    - ``extra_gallery_args``：附加给 gallery-dl 的命令行参数元组；
    - ``telegram_*``：Telegram 发送相关配置，覆盖启用开关、机器人凭据、队列路径、文案模板及重试策略。
    """

    user_id: int
    download_directory: Path
    archive_path: Path
    lock_path: Path
    health_path: Path
    log_level: str = "INFO"
    gallery_dl_path: str = "gallery-dl"
    concurrency: int = 2
    sleep: float = 1.5
    sleep_request: float = 1.0
    retries: int = 5
    timeout_seconds: int = 1200
    poll_interval_seconds: int = 600
    max_run_seconds: int = 900
    max_attempts: int = 3
    disk_free_threshold_bytes: int = 10 * 1024 * 1024 * 1024
    backoff_initial_seconds: float = 5.0
    backoff_multiplier: float = 2.0
    backoff_max_seconds: float = 300.0
    proxy: Optional[str] = None
    extra_gallery_args: tuple[str, ...] = field(default_factory=tuple)
    telegram_enabled: bool = True
    telegram_bot_token: Optional[str] = None
    telegram_chat_ids: tuple[str, ...] = field(default_factory=tuple)
    telegram_queue_path: Optional[Path] = None
    telegram_caption_template: str = "{title} — {author}\n{work_url}"
    telegram_caption_max_length: int = 1024
    telegram_include_tags: bool = False
    telegram_send_media_group: bool = True
    telegram_disable_notifications: bool = False
    telegram_document_fallback: bool = True
    telegram_worker_poll_seconds: float = 5.0
    telegram_max_attempts: int = 5
    telegram_retry_backoff_initial: float = 5.0
    telegram_retry_backoff_multiplier: float = 2.0
    telegram_retry_backoff_max: float = 600.0
    telegram_rate_limit_padding_seconds: float = 1.0
    telegram_proxy: Optional[str] = None

    @property
    def bookmarks_url(self) -> str:
        """生成 Pixiv 公共收藏页的地址。"""

        return f"https://www.pixiv.net/users/{self.user_id}/bookmarks/artworks"

    @property
    def telegram_active(self) -> bool:
        """判断 Telegram 通知是否满足启用条件。"""

        return bool(
            self.telegram_enabled
            and self.telegram_bot_token
            and self.telegram_chat_ids
        )

    @property
    def telegram_queue_file(self) -> Optional[Path]:
        """返回 Telegram 队列文件路径。

        若未启用 Telegram，则返回 ``None``，以便调用方跳过队列初始化。
        """

        if not self.telegram_active:
            return None
        return self.telegram_queue_path or (self.download_directory / "telegram_queue.sqlite3")


def load_config() -> Config:
    """加载配置并执行必要的目录初始化。

    处理流程：
    1. 若设置 ``PIXWATCH_CONFIG_FILE`` 环境变量，则读取 TOML 配置文件；
    2. 所有 ``PIXWATCH_*`` 环境变量会覆盖文件中的同名字段；
    3. 进行数据类型转换与合法性校验；
    4. 创建必要的目录/文件后返回 :class:`Config` 实例。
    """

    config_file = os.environ.get(f"{_ENV_PREFIX}CONFIG_FILE")
    file_data: Dict[str, object] = {}

    if config_file:
        path = Path(config_file)
        if not path.exists():
            raise FileNotFoundError(f"Config file '{config_file}' not found")
        with path.open("rb") as fh:
            file_data = tomllib.load(fh)

    merged: Dict[str, object] = dict(file_data)

    for field_name in Config.__dataclass_fields__:
        env_key = f"{_ENV_PREFIX}{field_name.upper()}"
        if env_key in os.environ:
            merged[field_name] = os.environ[env_key]

    # 必填字段列表：缺失任意一个都会立即抛出异常，避免运行时才发现问题
    required_fields = ["user_id", "download_directory", "archive_path", "lock_path", "health_path"]
    for field_name in required_fields:
        if field_name not in merged:
            raise ValueError(f"Missing required configuration field '{field_name}'")

    # `from __future__ import annotations` 会把类型注解延迟成字符串，因此这里
    # 使用 `get_type_hints` 取得真实类型，确保后续转换能够识别 `Path` 等对象。
    type_hints = get_type_hints(Config, include_extras=True)

    cast: Dict[str, object] = {}
    for field_name, field_def in Config.__dataclass_fields__.items():
        if field_name not in merged:
            continue
        value = merged[field_name]
        field_type = type_hints.get(field_name, field_def.type)
        origin = get_origin(field_type)
        args = get_args(field_type)
        target_type = field_type
        is_optional = False
        if origin in (UnionType, typing.Union):
            non_none_args = [a for a in args if a is not type(None)]  # noqa: E721
            if len(non_none_args) == 1 and len(args) == 2:
                target_type = non_none_args[0]
                is_optional = True
        elif origin is Optional:
            target_type = args[0]
            is_optional = True

        if is_optional:
            origin = get_origin(target_type)
            args = get_args(target_type)

        if is_optional and value is None:
            cast[field_name] = None
            continue

        if target_type is Path:
            # 路径类型统一展开为绝对路径，避免后续切换工作目录导致定位失败
            cast[field_name] = _expand_path(str(value))
        elif target_type is int:
            cast[field_name] = int(value)
        elif target_type is float:
            cast[field_name] = float(value)
        elif target_type is bool:
            if isinstance(value, str):
                cast[field_name] = value.strip().lower() in {
                    "1",
                    "true",
                    "yes",
                    "on",
                    "y",
                }
            else:
                cast[field_name] = bool(value)
        elif origin is tuple and args and args[0] is str:
            if isinstance(value, str):
                # 字符串支持多种分隔符，统一转为空格再切分
                separators = [",", " ", "\n", "\t"]
                processed = value
                for sep in separators:
                    processed = processed.replace(sep, " ")
                cast[field_name] = tuple(s for s in processed.split(" ") if s)
            elif isinstance(value, (list, tuple)):
                cast[field_name] = tuple(str(v) for v in value)
            else:
                raise TypeError(f"Invalid value for {field_name}: {value!r}")
        else:
            cast[field_name] = value

    # 生成最终配置对象，mypy 在此处会因动态构造而给出类型提示，忽略即可
    config = Config(**cast)  # type: ignore[arg-type]

    _ensure_directories(config)
    return config


def _ensure_directories(config: Config) -> None:
    """确保关键目录与文件存在。

    - 下载目录、归档、锁文件、健康检查文件所在路径全部提前创建；
    - 归档文件若不存在则创建空文件，保证首次运行也能共享同一去重文件；
    - 若启用了 Telegram 队列，则确保 SQLite 文件所在目录可用。
    """

    config.download_directory.mkdir(parents=True, exist_ok=True)
    config.archive_path.parent.mkdir(parents=True, exist_ok=True)
    config.lock_path.parent.mkdir(parents=True, exist_ok=True)
    config.health_path.parent.mkdir(parents=True, exist_ok=True)
    if not config.archive_path.exists():
        config.archive_path.touch()
    queue_path = config.telegram_queue_file
    if queue_path is not None:
        queue_path.parent.mkdir(parents=True, exist_ok=True)
