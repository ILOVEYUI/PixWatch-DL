from __future__ import annotations

"""轻量级 HTTP 客户端。

由于只需与 Telegram Bot API 交互，因此实现了极简的 multipart/form-data POST 客户端。
所有函数均配有中文注释，强调编码细节与异常处理策略。
"""

import json
import os
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Dict, Optional, Tuple


@dataclass
class HttpResponse:
    """HTTP 响应对象，封装状态码与内容。"""

    status_code: int
    content: bytes

    @property
    def is_success(self) -> bool:
        """返回是否为 2xx 响应。"""

        return 200 <= self.status_code < 300

    @property
    def text(self) -> str:
        """按 UTF-8 解码响应体。"""

        return self.content.decode("utf-8", errors="replace")

    def json(self) -> dict:
        """将响应体视为 JSON 并解析。"""

        return json.loads(self.text)


class HttpClient:
    """用于向 Telegram Bot API 发送 multipart/form-data 请求。"""

    def __init__(self, *, proxy: Optional[str] = None, timeout: float = 60.0) -> None:
        """构建带可选代理的 opener。"""

        handlers = []
        if proxy:
            handlers.append(
                urllib.request.ProxyHandler(
                    {
                        "http": proxy,
                        "https": proxy,
                    }
                )
            )
        self._opener = urllib.request.build_opener(*handlers)
        self._timeout = timeout

    def post(
        self,
        url: str,
        *,
        data: Optional[Dict[str, object]] = None,
        files: Optional[Dict[str, Tuple[str, bytes]]] = None,
    ) -> HttpResponse:
        boundary = self._boundary()
        body_chunks = []
        for key, value in (data or {}).items():
            encoded_value = self._encode_field(value)
            body_chunks.append(
                (
                    f"--{boundary}\r\n"
                    f"Content-Disposition: form-data; name=\"{key}\"\r\n\r\n"
                ).encode("utf-8")
            )
            body_chunks.append(encoded_value)
            body_chunks.append(b"\r\n")
        for key, (filename, content) in (files or {}).items():
            body_chunks.append(
                (
                    f"--{boundary}\r\n"
                    f"Content-Disposition: form-data; name=\"{key}\"; filename=\"{filename}\"\r\n"
                    "Content-Type: application/octet-stream\r\n\r\n"
                ).encode("utf-8")
            )
            body_chunks.append(content)
            body_chunks.append(b"\r\n")
        body_chunks.append(f"--{boundary}--\r\n".encode("utf-8"))
        payload = b"".join(body_chunks)
        request = urllib.request.Request(
            url,
            data=payload,
            method="POST",
            headers={
                "Content-Type": f"multipart/form-data; boundary={boundary}",
            },
        )
        try:
            with self._opener.open(request, timeout=self._timeout) as response:
                status = int(response.getcode())
                content = response.read()
        except urllib.error.HTTPError as exc:
            status = exc.code
            content = exc.read()
        return HttpResponse(status_code=status, content=content)

    def _boundary(self) -> str:
        """生成 multipart 边界字符串，确保足够随机。"""

        stamp = int(time.time() * 1000)
        return f"----PixWatch{stamp}{os.getpid()}"

    def _encode_field(self, value: object) -> bytes:
        """将表单字段统一编码为字节串。"""

        if isinstance(value, bool):
            return ("true" if value else "false").encode("utf-8")
        if value is None:
            return b""
        if isinstance(value, (bytes, bytearray)):
            return bytes(value)
        return str(value).encode("utf-8")
