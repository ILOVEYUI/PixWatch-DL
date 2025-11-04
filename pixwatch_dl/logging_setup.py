"""日志初始化模块，输出结构化 JSON。"""

from __future__ import annotations

import json
import logging
import sys
from typing import Any, Dict


class JsonFormatter(logging.Formatter):
    """将日志记录格式化为 JSON 文本。"""

    def format(self, record: logging.LogRecord) -> str:
        """构建包含标准字段与额外上下文的 JSON 字符串。"""

        payload: Dict[str, Any] = {
            "level": record.levelname,
            "message": record.getMessage(),
            "logger": record.name,
            "time": self.formatTime(record, self.datefmt),
        }
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        if hasattr(record, "extra") and isinstance(record.extra, dict):
            payload.update(record.extra)
        for key, value in record.__dict__.items():
            if key.startswith("_") or key in payload or key in ("args", "msg"):
                continue
            if key in ("levelname", "name", "pathname", "lineno", "funcName", "created", "msecs", "relativeCreated", "exc_info", "exc_text", "stack_info"):
                continue
            payload[key] = value
        return json.dumps(payload, ensure_ascii=False)


def setup_logging(level: str = "INFO") -> None:
    """配置根日志记录器为结构化输出。"""

    logging.captureWarnings(True)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.setLevel(level.upper())
    root.handlers.clear()
    root.addHandler(handler)
