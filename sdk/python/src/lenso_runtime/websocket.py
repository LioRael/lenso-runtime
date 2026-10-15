"""Optional WebSocket framing; Runtime session semantics are transport-independent."""
from __future__ import annotations

import asyncio
import socket

from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed, WebSocketException

from .transport import MAX_FRAME, websocket_address


class WebSocketTransport:
    is_stdio = False

    def __init__(self, connection: ClientConnection) -> None:
        self.connection = connection
        self.writing = False
        self.failed_write = False

    @property
    def idle(self) -> bool:
        return not self.writing and not self.failed_write and self.connection.transport.get_write_buffer_size() == 0

    async def receive(self) -> bytes:
        try:
            message = await self.connection.recv()
        except ConnectionClosed as exc:
            if exc.sent is not None and exc.sent.code in (1002, 1003, 1007, 1009):
                raise ValueError("invalid WebSocket frame") from exc
            raise ConnectionError("WebSocket disconnected") from exc
        if not isinstance(message, str):
            raise ValueError("Runtime requires WebSocket text messages")
        data = message.encode("utf-8")
        if len(data) > MAX_FRAME - 1:
            raise ValueError("WebSocket payload too large")
        return data

    async def send(self, data: bytes) -> None:
        if self.writing:
            raise RuntimeError("concurrent transport writers")
        self.writing = True
        try:
            # Session encodes NDJSON once; WebSocket has message framing instead.
            await self.connection.send(data[:-1].decode("utf-8"))
        except BaseException as exc:
            self.failed_write = True
            if isinstance(exc, ConnectionClosed):
                raise ConnectionError("WebSocket disconnected") from exc
            raise
        finally:
            self.writing = False

    def abort(self) -> None:
        self.connection.transport.abort()

    async def wait_closed(self) -> None:
        await self.connection.wait_closed()


async def open_connection(url: str) -> WebSocketTransport:
    host, port = websocket_address(url)
    sock = socket.socket(socket.AF_INET6 if ":" in host else socket.AF_INET, socket.SOCK_STREAM)
    sock.setblocking(False)
    try:
        await asyncio.get_running_loop().sock_connect(sock, (host, port))
        # A preconnected socket prevents following redirects. No DNS, proxy,
        # browser Origin, compression, auto-reconnect or library ping timer.
        connection = await connect(url, sock=sock, proxy=None, origin=None,
            compression=None, ping_interval=None, open_timeout=5, close_timeout=0.1,
            max_size=MAX_FRAME - 1, max_queue=1, write_limit=(MAX_FRAME, 0))
        return WebSocketTransport(connection)
    except BaseException as exc:
        sock.close()
        if isinstance(exc, (WebSocketException, ValueError)):
            raise ConnectionError("WebSocket handshake failed") from exc
        raise
