"""Owned asyncio transports. Framing only; routing and lifecycle stay in session.

Unix stdio owns non-inheritable duplicates of fd 0/1. asyncio sets O_NONBLOCK on
those shared open-file descriptions; stdout must remain protocol-only. No worker
thread may remain blocked reading stdin during shutdown.
"""
from __future__ import annotations

import asyncio
import ipaddress
import os
from urllib.parse import urlsplit
from typing import Protocol, cast

MAX_FRAME = 256 * 1024
_stdio_claimed = False


class MessageTransport(Protocol):
    is_stdio: bool
    writing: bool
    failed_write: bool

    @property
    def idle(self) -> bool: ...
    async def receive(self) -> bytes: ...
    async def send(self, data: bytes) -> None: ...
    def abort(self) -> None: ...
    async def wait_closed(self) -> None: ...


class _PipeWriter(asyncio.Protocol):
    def __init__(self) -> None:
        self.transport: asyncio.WriteTransport
        self.resumed = asyncio.Event()
        self.resumed.set()
        self.closed = asyncio.Event()
        self.error: Exception | None = None

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self.transport = cast(asyncio.WriteTransport, transport)
        self.transport.set_write_buffer_limits(high=MAX_FRAME, low=0)

    def pause_writing(self) -> None:
        self.resumed.clear()

    def resume_writing(self) -> None:
        self.resumed.set()

    def connection_lost(self, exc: Exception | None) -> None:
        self.error = exc
        self.closed.set()
        self.resumed.set()

    def write(self, data: bytes) -> None:
        if self.closed.is_set() or self.transport.is_closing():
            raise ConnectionError("stdio writer closed")
        self.transport.write(data)

    async def drain(self) -> None:
        await self.resumed.wait()
        if self.closed.is_set():
            raise ConnectionError("stdio writer closed") from self.error

    async def wait_closed(self) -> None:
        await self.closed.wait()


class _PipeReader(asyncio.StreamReaderProtocol):
    def __init__(self, reader: asyncio.StreamReader) -> None:
        super().__init__(reader)
        self.closed = asyncio.Event()

    def connection_lost(self, exc: Exception | None) -> None:
        super().connection_lost(exc)
        self.closed.set()


class LineTransport:
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter | _PipeWriter,
                 read_transport: asyncio.ReadTransport | None = None,
                 read_protocol: _PipeReader | None = None) -> None:
        self.reader, self.writer = reader, writer
        self.read_transport, self.read_protocol = read_transport, read_protocol
        self.is_stdio = read_transport is not None
        self.writing = False
        self.failed_write = False

    @property
    def idle(self) -> bool:
        return not self.writing and not self.failed_write and self.writer.transport.get_write_buffer_size() == 0

    async def receive(self) -> bytes:
        data = await self.reader.readuntil(b"\n")
        if len(data) > MAX_FRAME:
            raise ValueError("frame too large")
        return data

    async def send(self, data: bytes) -> None:
        if self.writing:
            raise RuntimeError("concurrent transport writers")
        self.writing = True
        try:
            self.writer.write(data)
            await self.writer.drain()
        except BaseException:
            self.failed_write = True
            raise
        finally:
            self.writing = False

    def abort(self) -> None:
        if not self.writer.transport.is_closing():
            self.writer.transport.abort()
        if self.read_transport is not None and not self.read_transport.is_closing():
            self.read_transport.close()

    async def wait_closed(self) -> None:
        await self.writer.wait_closed()
        if self.read_protocol is not None:
            await self.read_protocol.closed.wait()


async def open_tcp(host: str, port: int) -> LineTransport:
    reader, writer = await asyncio.open_connection(host, port, limit=MAX_FRAME)
    writer.transport.set_write_buffer_limits(high=MAX_FRAME, low=0)
    return LineTransport(reader, writer)


def websocket_address(url: str | None) -> tuple[str, int]:
    if type(url) is not str or not url.startswith("ws://") or any(c in url for c in "@?#") or any(ord(c) <= 32 for c in url):
        raise ValueError("literal loopback ws:// URL without credentials, query or fragment required")
    parsed = urlsplit(url)
    host = parsed.hostname or ""
    if "%" in host or not ipaddress.ip_address(host).is_loopback:
        raise ValueError("literal loopback WebSocket address required")
    port = parsed.port if parsed.port is not None else 80
    if not 1 <= port <= 65535:
        raise ValueError("invalid WebSocket port")
    return host, port


async def open_websocket(url: str) -> MessageTransport:
    websocket_address(url)
    try:
        from .websocket import open_connection
    except ImportError as exc:
        raise ValueError("WebSocket requires lenso-runtime-sdk[websocket]") from exc
    return await open_connection(url)


async def open_stdio() -> LineTransport:
    global _stdio_claimed
    if os.name != "posix":
        raise ValueError("stdio requires a Unix asyncio pipe event loop")
    if _stdio_claimed:
        raise ValueError("stdio permits one RuntimeHost per process")
    _stdio_claimed = True
    loop = asyncio.get_running_loop()
    reader = asyncio.StreamReader(limit=MAX_FRAME)
    protocol = _PipeReader(reader)
    read_transport = None
    input_pipe = output_pipe = None
    try:
        input_pipe = os.fdopen(os.dup(0), "rb", buffering=0)
        output_pipe = os.fdopen(os.dup(1), "wb", buffering=0)
        read_transport, _ = await loop.connect_read_pipe(lambda: protocol, input_pipe)
        write_protocol = _PipeWriter()
        await loop.connect_write_pipe(lambda: write_protocol, output_pipe)
        return LineTransport(reader, write_protocol, read_transport, protocol)
    except BaseException:
        if read_transport is not None:
            read_transport.close()
        elif input_pipe is not None:
            input_pipe.close()
        if output_pipe is not None:
            output_pipe.close()
        raise
