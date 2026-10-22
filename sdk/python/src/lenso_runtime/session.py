"""Asyncio session over bounded Runtime v1 transports. Never reconnects or replays."""
from __future__ import annotations

import asyncio
import inspect
import ipaddress
import json
import math
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass, field
from typing import Any, Generic, TypeVar

from .schema import Contract, Schema, validate_contract, validate_schema, validate_value, validate_wire
from .transport import MessageTransport, open_stdio, open_tcp, open_websocket, websocket_address

MAX_FRAME = 256 * 1024
ERROR_CODES = frozenset({"service_not_found", "not_ready", "overloaded", "deadline_exceeded",
    "connection_lost", "cancelled", "contract_mismatch", "invalid_input", "invalid_output",
    "business", "protocol_error"})


class RuntimeError(Exception):
    def __init__(self, code: str, message: str | None = None, details: Any = None):
        super().__init__(message or code)
        self.code, self.details = code, details


class BusinessError(RuntimeError):
    def __init__(self, details: Any, message: str = "Business operation failed"):
        super().__init__("business", message, details)


@dataclass(frozen=True)
class Target:
    namespace: str
    service: str
    contract_version: str = "1"


@dataclass(frozen=True)
class HandlerContext:
    call_id: str
    trace_id: str
    parent_id: str | None
    caller: dict[str, str]
    deadline: float
    cancelled: asyncio.Event

    def call_options(self) -> dict[str, Any]:
        """Explicit child-call inheritance; the deadline uses the local loop clock."""
        return {"deadline": self.deadline, "cancelled": self.cancelled,
                "trace_id": self.trace_id, "parent_id": self.call_id}


Handler = Callable[[Any, HandlerContext], Any]


@dataclass
class _Pending:
    mode: str
    method: Schema | None
    future: asyncio.Future[Any] | None = None
    timer: asyncio.TimerHandle | None = None
    cancellation: asyncio.Task[None] | None = None
    ended: bool = False
    error: RuntimeError | None = None
    waiting: bool = False


@dataclass
class _Invocation:
    stream: bool
    deadline: float
    cancelled: asyncio.Event = field(default_factory=asyncio.Event)
    credit_ready: asyncio.Event = field(default_factory=asyncio.Event)
    credits: int = 0
    task: asyncio.Task[None] | None = None


def _integer(value: Any, low: int, high: int) -> bool:
    return type(value) is int and low <= value <= high


def _identifier(value: Any) -> bool:
    return type(value) is str and 1 <= len(value.encode("utf-8")) <= 256


def _route(value: Any) -> bool:
    return type(value) is str and value.isascii() and value.isdecimal() and not value.startswith("0") and len(value) <= 20 and int(value) <= 2**64 - 1


def _encode(frame: dict[str, Any], code: str = "invalid_input") -> bytes:
    try:
        validate_wire(frame, budget=[MAX_FRAME - 1])
        data = json.dumps(frame, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode() + b"\n"
        if len(data) > MAX_FRAME:
            raise ValueError("frame too large")
        return data
    except (ValueError, TypeError, OverflowError, RecursionError) as exc:
        raise RuntimeError(code) from exc


T = TypeVar("T")


class ResponseStream(AsyncIterator[T], Generic[T]):
    """One outstanding next(), no prefetch; use async with for early-break cleanup."""
    def __init__(self, host: RuntimeHost, ident: str, pending: _Pending):
        self._host, self._id, self._pending = host, ident, pending

    def __aiter__(self) -> ResponseStream:
        return self

    async def __anext__(self) -> T:
        p = self._pending
        if p.error:
            raise p.error
        if p.ended:
            raise StopAsyncIteration
        if p.waiting:
            raise RuntimeError("overloaded", "Only one concurrent stream next() is supported")
        p.waiting = True
        p.future = asyncio.get_running_loop().create_future()
        try:
            self._host._send({"type": "credit", "id": self._id, "credits": 1})
            result = await p.future
            if p.error:
                raise p.error
            if result is _END:
                raise StopAsyncIteration
            return result
        except asyncio.CancelledError:
            self._host._retire(self._id, RuntimeError("cancelled"), cancel=True)
            raise
        except RuntimeError as exc:
            self._host._retire(self._id, exc, cancel=True)
            raise
        finally:
            p.waiting = False
            if p.future.done() and not p.future.cancelled():
                p.future.exception()
            p.future = None

    async def aclose(self) -> None:
        self._host._retire(self._id, cancel=True)

    async def __aenter__(self) -> ResponseStream:
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.aclose()


_END = object()
_CONNECT = object()


class RuntimeHost:
    def __init__(self, *, _token: object | None = None) -> None:
        if _token is not _CONNECT:
            raise TypeError("Use await RuntimeHost.connect(...) or connect_registered(...)")
        self._contract: Contract = {"version": "1", "methods": {}}
        self._handlers: dict[str, Handler] = {}
        self._max_concurrency = 8
        self._pending: dict[str, _Pending] = {}
        self._invocations: dict[str, _Invocation] = {}
        self._sequence = 0
        self._closed = asyncio.Event()
        self._accepting = False
        self._ready = False
        self._queue: asyncio.Queue[tuple[float, bytes]] = asyncio.Queue(maxsize=64)
        self._tasks: set[asyncio.Task[Any]] = set()
        self._activation: asyncio.Future[None] | None = None
        self._activation_task: asyncio.Task[None] | None = None
        self._drained: asyncio.Future[None] | None = None
        self._close_task: asyncio.Task[None] | None = None
        self._drain_task: asyncio.Task[None] | None = None
        self._transport: MessageTransport

    @classmethod
    async def connect_registered(cls, *, namespace: str, service: str, instance: str,
                      contract: Contract | None = None, handlers: dict[str, Handler] | None = None,
                      host: str = "127.0.0.1", port: int = 7331,
                      max_concurrency: int = 8, transport: str = "tcp", url: str | None = None) -> RuntimeHost:
        """Register and start I/O without ready; bind exports then activate after setup."""
        contract = contract if contract is not None else {"version": "1", "methods": {}}
        handlers = handlers if handlers is not None else {}
        try:
            validate_contract(contract)
            if not all(_identifier(v) for v in (namespace, service, instance)):
                raise ValueError("invalid identity")
            if transport not in ("tcp", "stdio", "websocket"):
                raise ValueError("unsupported transport")
            if transport == "tcp" and (not ipaddress.ip_address(host).is_loopback or not _integer(port, 1, 65535)):
                raise ValueError("numeric loopback TCP required")
            if transport == "websocket":
                websocket_address(url)
            if not _integer(max_concurrency, 1, 64):
                raise ValueError("invalid concurrency")
            if handlers and (set(handlers) != set(contract["methods"]) or not all(callable(h) for h in handlers.values())):
                raise ValueError("bindings must exactly match contract")
        except (ValueError, TypeError, UnicodeError) as exc:
            raise RuntimeError("invalid_input", str(exc)) from exc
        self = cls(_token=_CONNECT)
        self._contract, self._handlers = json.loads(json.dumps(contract)), dict(handlers)
        self._max_concurrency = max_concurrency
        try:
            async with asyncio.timeout(5):
                if transport == "stdio":
                    self._transport = await open_stdio()
                elif transport == "websocket":
                    assert url is not None
                    self._transport = await open_websocket(url)
                else:
                    self._transport = await open_tcp(host, port)
                await self._send_direct({"type": "hello", "version": 1, "namespace": namespace, "service": service, "instance": instance})
                welcome = await self._read_frame()
                self._expect(welcome, "welcome")
                if not _integer(welcome.get("version"), 1, 1) or not _route(welcome.get("generation")) or not _integer(welcome.get("heartbeat_ms"), 1, 5000):
                    raise RuntimeError("protocol_error")
                await self._send_direct({"type": "register", "contract": self._contract, "max_concurrency": max_concurrency})
                self._expect(await self._read_frame(), "registered")
            self._accepting = True
            self._spawn(self._read_loop())
            self._spawn(self._write_loop())
            self._spawn(self._heartbeat(welcome["heartbeat_ms"] / 1000))
            return self
        except BaseException as exc:
            if hasattr(self, "_transport"):
                self._transport.abort()
            if isinstance(exc, TimeoutError):
                raise RuntimeError("deadline_exceeded", "Handshake timed out") from exc
            if isinstance(exc, ValueError):
                raise RuntimeError("invalid_input", str(exc)) from exc
            if isinstance(exc, OSError):
                raise RuntimeError("connection_lost") from exc
            raise

    @classmethod
    async def connect(cls, *, namespace: str, service: str, instance: str,
                      contract: Contract | None = None, handlers: dict[str, Handler] | None = None,
                      host: str = "127.0.0.1", port: int = 7331,
                      max_concurrency: int = 8, transport: str = "tcp", url: str | None = None) -> RuntimeHost:
        """Register and activate immediately; native Apps use connect_registered."""
        # Keep immediate-connect validation resource-free and backwards compatible.
        try:
            validate_contract(contract if contract is not None else {"version": "1", "methods": {}})
        except (ValueError, TypeError) as exc:
            raise RuntimeError("invalid_input", str(exc)) from exc
        if set(handlers or {}) != set((contract if contract is not None else {"methods": {}})["methods"]):
            raise RuntimeError("invalid_input", "bindings must exactly match contract")
        self: RuntimeHost | None = None
        try:
            async with asyncio.timeout(5):
                self = await cls.connect_registered(namespace=namespace, service=service, instance=instance,
                    contract=contract, handlers=handlers, host=host, port=port,
                    max_concurrency=max_concurrency, transport=transport, url=url)
                await self.activate()
                return self
        except BaseException as exc:
            if self is not None:
                await self.close()
            if isinstance(exc, TimeoutError):
                raise RuntimeError("deadline_exceeded", "Handshake timed out") from exc
            raise

    def bind_exports(self, handlers: dict[str, Handler]) -> None:
        """Bind exactly the registered methods before activation, without setup."""
        if self._closed.is_set() or self._activation_task is not None or self._drain_task is not None:
            raise RuntimeError("not_ready", "Export binding is closed")
        if set(handlers) != set(self._contract["methods"]) or not all(callable(h) for h in handlers.values()):
            raise RuntimeError("invalid_input", "bindings must exactly match contract")
        self._handlers = dict(handlers)

    async def activate(self) -> None:
        if self._closed.is_set() or self._drain_task is not None:
            raise RuntimeError("not_ready")
        if self._ready:
            return
        if self._activation_task is None:
            self.bind_exports(self._handlers)
            self._activation_task = asyncio.create_task(self._perform_activation())
        try:
            await asyncio.shield(self._activation_task)
        except asyncio.CancelledError:
            # A sent ready has an uncertain outcome: retire this session, never replay.
            await self.close()
            raise

    async def _perform_activation(self) -> None:
        self._activation = asyncio.get_running_loop().create_future()
        try:
            if not self._accepting or self._drain_task is not None:
                raise RuntimeError("not_ready")
            async with asyncio.timeout(5):
                self._send({"type": "ready"})
                await self._activation
        except TimeoutError as exc:
            self._fail(RuntimeError("deadline_exceeded"))
            raise RuntimeError("deadline_exceeded", "Activation timed out") from exc
        except BaseException:
            self._fail(RuntimeError("connection_lost"))
            raise
        finally:
            if self._activation.done() and not self._activation.cancelled():
                self._activation.exception()

    def _spawn(self, coroutine: Any) -> asyncio.Task[Any]:
        task = asyncio.create_task(coroutine)
        self._tasks.add(task)
        def finished(done: asyncio.Task[Any]) -> None:
            self._tasks.discard(done)
            if not done.cancelled() and done.exception() is not None:
                self._fail(RuntimeError("protocol_error"))
        task.add_done_callback(finished)
        return task

    @staticmethod
    def _expect(frame: dict[str, Any], kind: str) -> None:
        if frame.get("type") == "error" and "id" not in frame:
            raise RuntimeError(frame["code"], frame["message"], frame.get("details"))
        if frame.get("type") != kind:
            raise RuntimeError("protocol_error")

    async def _read_frame(self) -> dict[str, Any]:
        try:
            data = await self._transport.receive()
            frame = json.loads(data.decode("utf-8"), parse_constant=lambda _: (_ for _ in ()).throw(ValueError("nonfinite")))
            if type(frame) is not dict or type(frame.get("type")) is not str:
                raise ValueError("invalid frame")
            validate_wire(frame)
            self._validate_frame(frame)
            return frame
        except asyncio.IncompleteReadError as exc:
            raise RuntimeError("protocol_error" if exc.partial else "connection_lost") from exc
        except (ValueError, UnicodeError, RecursionError, asyncio.LimitOverrunError) as exc:
            raise RuntimeError("protocol_error") from exc
        except OSError as exc:
            raise RuntimeError("connection_lost") from exc

    @staticmethod
    def _validate_frame(f: dict[str, Any]) -> None:
        kind = f["type"]
        fields = {
            "welcome": ({"version", "generation", "heartbeat_ms"}, set()),
            "registered": (set(), set()), "ready": (set(), set()), "drained": (set(), set()),
            "invoke": ({"id", "method", "input", "timeout_ms", "trace_id", "caller"}, {"parent_id"}),
            "credit": ({"id", "credits"}, set()), "cancel": ({"id"}, set()),
            "result": ({"id", "output"}, set()), "chunk": ({"id", "output"}, set()),
            "end": ({"id"}, set()), "error": ({"code", "message"}, {"id", "details"}),
            "status": ({"id", "services", "calls", "limits"}, {"recent"}),
        }
        if kind not in fields:
            raise ValueError("unknown frame")
        required, optional = fields[kind]
        if required - f.keys() or f.keys() - (required | optional | {"type"}):
            raise ValueError("invalid frame fields")
        if kind in {"invoke", "credit", "cancel", "result", "chunk", "end", "status"} or kind == "error" and "id" in f:
            if not _route(f.get("id")):
                raise ValueError("invalid route")
        if kind in {"result", "chunk"} and "output" not in f:
            raise ValueError("missing output")
        if kind == "error" and (f.get("code") not in ERROR_CODES or type(f.get("message")) is not str
                                or len(f["message"].encode("utf-8")) > 1024):
            raise ValueError("invalid error")
        if kind == "credit" and not _integer(f.get("credits"), 1, 16):
            raise ValueError("invalid credit")
        if kind == "invoke":
            caller = f.get("caller")
            if (not _identifier(f.get("method")) or "input" not in f or not _integer(f.get("timeout_ms"), 1, 60000)
                    or not _identifier(f.get("trace_id")) or type(caller) is not dict
                    or set(caller) != {"namespace", "service", "instance"}
                    or not all(_identifier(caller.get(k)) for k in ("namespace", "service", "instance"))
                    or "parent_id" in f and not _identifier(f["parent_id"])):
                raise ValueError("invalid invoke")
        if kind == "status" and (type(f.get("services")) is not list or type(f.get("calls")) is not list or type(f.get("limits")) is not dict):
            raise ValueError("invalid status")

    async def _send_direct(self, frame: dict[str, Any]) -> None:
        await self._transport.send(_encode(frame))

    def _send(self, frame: dict[str, Any], code: str = "invalid_input") -> None:
        if self._closed.is_set():
            raise RuntimeError("connection_lost")
        data = _encode(frame, code)
        try:
            self._queue.put_nowait((asyncio.get_running_loop().time() + 5, data))
        except asyncio.QueueFull as exc:
            raise RuntimeError("overloaded") from exc

    async def _write_loop(self) -> None:
        try:
            while True:
                deadline, data = await self._queue.get()
                if asyncio.get_running_loop().time() >= deadline:
                    raise TimeoutError
                async with asyncio.timeout_at(deadline):
                    await self._transport.send(data)
        except asyncio.CancelledError:
            raise
        except Exception:
            self._fail(RuntimeError("connection_lost"))

    async def _heartbeat(self, interval: float) -> None:
        try:
            while True:
                await asyncio.sleep(interval)
                self._send({"type": "heartbeat"})
        except asyncio.CancelledError:
            raise
        except Exception:
            self._fail(RuntimeError("connection_lost"))

    async def _read_loop(self) -> None:
        try:
            while True:
                self._receive(await self._read_frame())
        except asyncio.CancelledError:
            raise
        except RuntimeError as exc:
            self._fail(exc)
        except Exception:
            self._fail(RuntimeError("protocol_error"))

    def _fail(self, error: RuntimeError, *, abort: bool = True) -> None:
        if self._closed.is_set():
            return
        self._closed.set()
        self._accepting = False
        if abort:
            self._transport.abort()
        if self._activation is not None and not self._activation.done():
            self._activation.set_exception(error)
        for ident in list(self._pending):
            self._retire(ident, error)
        if self._drained is not None and not self._drained.done():
            self._drained.set_exception(error)
        for inv in self._invocations.values():
            inv.cancelled.set()
        current = asyncio.current_task()
        for task in list(self._tasks):
            if task is not current:
                task.cancel()
        while not self._queue.empty():
            self._queue.get_nowait()

    def _retire(self, ident: str, error: RuntimeError | None = None, *, cancel: bool = False) -> None:
        p = self._pending.pop(ident, None)
        if p is None:
            return
        p.ended, p.error = True, error
        if p.timer:
            p.timer.cancel()
        if p.cancellation is not None and p.cancellation is not asyncio.current_task():
            p.cancellation.cancel()
        if p.future is not None and not p.future.done():
            if error:
                p.future.set_exception(error)
            else:
                p.future.set_result(_END)
        if cancel and not self._closed.is_set():
            try:
                self._send({"type": "cancel", "id": ident})
            except RuntimeError:
                self._fail(RuntimeError("connection_lost"))

    def _receive(self, f: dict[str, Any]) -> None:
        kind = f["type"]
        ident: str = f.get("id", "")
        if kind == "error" and "id" not in f:
            self._fail(RuntimeError(f["code"], f["message"], f.get("details")))
        elif kind == "ready":
            if self._activation is None or self._activation.done() or self._drain_task is not None:
                raise RuntimeError("protocol_error")
            self._ready = True
            self._activation.set_result(None)
        elif kind == "invoke":
            if not self._ready:
                raise RuntimeError("protocol_error")
            if not self._accepting or len(self._invocations) >= self._max_concurrency:
                self._send({"type": "error", "id": ident, "code": "overloaded", "message": "Host is busy"})
                return
            if ident in self._invocations:
                raise RuntimeError("protocol_error")
            method = self._contract["methods"].get(f["method"])
            inv = _Invocation(stream=method is not None and method["mode"] == "stream",
                              deadline=asyncio.get_running_loop().time() + f["timeout_ms"] / 1000)
            self._invocations[ident] = inv
            inv.task = self._spawn(self._invoke(f, inv))
            inv.task.add_done_callback(lambda _: self._invocations.pop(ident, None))
        elif kind in ("credit", "cancel"):
            inv = self._invocations.get(ident)
            if inv is None:
                return
            if kind == "cancel":
                inv.cancelled.set()
                if inv.task is not None:
                    inv.task.cancel()
            else:
                if not inv.stream or inv.credits + f["credits"] > 16:
                    raise RuntimeError("protocol_error")
                inv.credits += f["credits"]
                inv.credit_ready.set()
        elif kind == "drained":
            if self._drained is None or self._drained.done():
                raise RuntimeError("protocol_error")
            self._drained.set_result(None)
        elif kind in ("result", "chunk", "end", "error", "status"):
            p = self._pending.get(ident)
            if p is None:
                return  # Retired routes cannot be resurrected by late replies.
            if kind == "error":
                error = RuntimeError(f["code"], f["message"], f.get("details"))
                if f["code"] == "business" and p.method:
                    try:
                        validate_value(p.method["error"], f.get("details"))
                    except ValueError:
                        error = RuntimeError("invalid_output")
                self._retire(ident, error, cancel=p.mode == "stream")
            elif kind == "status" and p.mode == "status":
                assert p.future is not None
                p.future.set_result(f)
                self._retire(ident)
            elif kind == "result" and p.mode == "unary" or kind == "chunk" and p.mode == "stream":
                try:
                    if p.method:
                        validate_value(p.method["output"], f["output"])
                except ValueError:
                    self._retire(ident, RuntimeError("invalid_output"), cancel=p.mode == "stream")
                    return
                if p.future is None or p.future.done():
                    raise RuntimeError("protocol_error")
                p.future.set_result(f["output"])
                if kind == "result":
                    self._retire(ident)
            elif kind == "end" and p.mode == "stream":
                self._retire(ident)
            else:
                raise RuntimeError("protocol_error")
        else:
            raise RuntimeError("protocol_error")

    def _start(self, target: Target | None, method: str, input: Any, mode: str,
               timeout_ms: int, contract: Schema | None, trace_id: str | None, parent_id: str | None,
               deadline: float | None = None, cancelled: asyncio.Event | None = None) -> tuple[str, _Pending]:
        if self._closed.is_set():
            raise RuntimeError("connection_lost")
        if not self._accepting:
            raise RuntimeError("not_ready")
        if len(self._pending) >= 192 or self._sequence >= 2**64 - 1:
            raise RuntimeError("overloaded")
        target = target or Target("", "", "")
        try:
            if not _integer(timeout_ms, 1, 60000):
                raise ValueError("invalid timeout")
            if deadline is not None and (type(deadline) not in (int, float) or not math.isfinite(deadline)):
                raise ValueError("invalid deadline")
            if cancelled is not None and not isinstance(cancelled, asyncio.Event):
                raise ValueError("invalid cancellation event")
            if mode != "status":
                if not all(_identifier(v) for v in (target.namespace, target.service, target.contract_version, method)):
                    raise ValueError("invalid target or method")
                validate_wire(input, budget=[MAX_FRAME - 1])
            if any(v is not None and not _identifier(v) for v in (trace_id, parent_id)):
                raise ValueError("invalid trace identifier")
            if contract:
                if contract.get("mode") != mode:
                    raise ValueError("wrong method mode")
                for slot in ("input", "output", "error"):
                    validate_schema(contract[slot])
                validate_value(contract["input"], input)
        except (ValueError, TypeError, AttributeError, KeyError, OverflowError, RecursionError) as exc:
            raise RuntimeError("invalid_input") from exc
        loop = asyncio.get_running_loop()
        if cancelled is not None and cancelled.is_set():
            raise RuntimeError("cancelled")
        if deadline is not None:
            remaining = deadline - loop.time()
            if remaining < 0.001:
                raise RuntimeError("deadline_exceeded")
            timeout_ms = math.floor(min(timeout_ms, remaining * 1000))
        self._sequence += 1
        ident = str(self._sequence)
        frame: dict[str, Any] = {"type": "status", "id": ident} if mode == "status" else {
            "type": "call", "id": ident, "namespace": target.namespace, "service": target.service,
            "contract_version": target.contract_version, "method": method, "input": input, "timeout_ms": timeout_ms}
        if trace_id is not None:
            frame["trace_id"] = trace_id
        if parent_id is not None:
            frame["parent_id"] = parent_id
        self._send(frame)
        p = _Pending(mode, contract, future=loop.create_future() if mode != "stream" else None)
        p.timer = loop.call_later(timeout_ms / 1000, lambda: self._retire(ident, RuntimeError("deadline_exceeded"), cancel=mode != "status"))
        self._pending[ident] = p
        if cancelled is not None:
            async def watch_cancel() -> None:
                await cancelled.wait()
                self._retire(ident, RuntimeError("cancelled"), cancel=True)
            p.cancellation = self._spawn(watch_cancel())
        return ident, p

    async def call(self, target: Target, method: str, input: Any, *, timeout_ms: int = 5000,
                   contract: Schema | None = None, trace_id: str | None = None, parent_id: str | None = None,
                   deadline: float | None = None, cancelled: asyncio.Event | None = None) -> Any:
        ident, p = self._start(target, method, input, "unary", timeout_ms, contract, trace_id, parent_id, deadline, cancelled)
        try:
            assert p.future is not None
            return await p.future
        except asyncio.CancelledError:
            self._retire(ident, cancel=True)
            raise

    def stream(self, target: Target, method: str, input: Any, *, timeout_ms: int = 5000,
               contract: Schema | None = None, trace_id: str | None = None, parent_id: str | None = None,
               deadline: float | None = None, cancelled: asyncio.Event | None = None) -> ResponseStream:
        ident, p = self._start(target, method, input, "stream", timeout_ms, contract, trace_id, parent_id, deadline, cancelled)
        return ResponseStream(self, ident, p)

    async def status(self, *, timeout_ms: int = 5000) -> dict[str, Any]:
        ident, p = self._start(None, "", None, "status", timeout_ms, None, None, None)
        try:
            assert p.future is not None
            return await p.future
        except asyncio.CancelledError:
            self._retire(ident)
            raise

    async def _invoke(self, f: dict[str, Any], inv: _Invocation) -> None:
        ident, iterator = f["id"], None
        method = self._contract["methods"].get(f["method"])
        context = HandlerContext(ident, f["trace_id"], f.get("parent_id"), f["caller"],
                                 inv.deadline, inv.cancelled)
        try:
            async with asyncio.timeout_at(context.deadline):
                if method is None:
                    raise RuntimeError("not_ready")
                try:
                    validate_value(method["input"], f["input"])
                except ValueError as exc:
                    raise RuntimeError("invalid_input") from exc
                output = self._handlers[f["method"]](f["input"], context)
                if inspect.isawaitable(output):
                    output = await output
                if method["mode"] == "unary":
                    self._check_output(method, output)
                    if inv.cancelled.is_set():
                        raise RuntimeError("cancelled")
                    self._send({"type": "result", "id": ident, "output": output}, "invalid_output")
                else:
                    if not hasattr(output, "__aiter__"):
                        raise RuntimeError("invalid_output")
                    iterator = aiter(output)
                    while True:
                        while inv.credits == 0:
                            inv.credit_ready.clear()
                            await inv.credit_ready.wait()
                        if inv.cancelled.is_set():
                            raise RuntimeError("cancelled")
                        try:
                            item = await anext(iterator)
                        except StopAsyncIteration:
                            self._send({"type": "end", "id": ident})
                            break
                        self._check_output(method, item)
                        if inv.cancelled.is_set():
                            raise RuntimeError("cancelled")
                        inv.credits -= 1
                        self._send({"type": "chunk", "id": ident, "output": item}, "invalid_output")
        except BaseException as exc:
            if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                raise
            if not self._closed.is_set():
                error = (RuntimeError("cancelled") if isinstance(exc, asyncio.CancelledError)
                         else RuntimeError("deadline_exceeded") if isinstance(exc, TimeoutError)
                         else exc if isinstance(exc, RuntimeError) else RuntimeError("invalid_output"))
                if error.code not in ERROR_CODES or len(str(error).encode("utf-8", errors="replace")) > 1024:
                    error = RuntimeError("invalid_output")
                if error.code == "business":
                    try:
                        validate_wire(error.details)
                        if method is None:
                            raise ValueError("missing method")
                        validate_value(method["error"], error.details)
                    except (ValueError, TypeError, OverflowError, RecursionError):
                        error = RuntimeError("invalid_output")
                frame = {"type": "error", "id": ident, "code": error.code, "message": str(error)}
                if error.code == "business":
                    frame["details"] = error.details
                try:
                    self._send(frame, "invalid_output")
                except RuntimeError as send_error:
                    if send_error.code == "invalid_output":
                        try:
                            self._send({"type": "error", "id": ident, "code": "invalid_output", "message": "invalid_output"})
                        except RuntimeError:
                            self._fail(RuntimeError("connection_lost"))
                    else:
                        self._fail(RuntimeError("connection_lost"))
        finally:
            inv.cancelled.set()
            try:
                if iterator is not None and hasattr(iterator, "aclose"):
                    async with asyncio.timeout(0.1):
                        await iterator.aclose()
            finally:
                self._invocations.pop(ident, None)

    @staticmethod
    def _check_output(method: Schema, output: Any) -> None:
        try:
            validate_wire(output, budget=[MAX_FRAME - 1])
            validate_value(method["output"], output)
        except (ValueError, TypeError, OverflowError, RecursionError) as exc:
            raise RuntimeError("invalid_output") from exc

    async def drain(self, timeout_ms: int = 5000) -> None:
        if not _integer(timeout_ms, 1, 60000):
            raise RuntimeError("invalid_input")
        if self._drain_task is None:
            self._accepting = False
            self._drain_task = asyncio.create_task(self._perform_drain(timeout_ms))
        await asyncio.shield(self._drain_task)

    async def _perform_drain(self, timeout_ms: int) -> None:
        if self._closed.is_set():
            return
        self._accepting = False
        self._drained = asyncio.get_running_loop().create_future()
        try:
            async with asyncio.timeout(timeout_ms / 1000 + 0.1):
                self._send({"type": "drain", "timeout_ms": timeout_ms})
                await self._drained
                tasks = [inv.task for inv in self._invocations.values() if inv.task is not None]
                if tasks:
                    _, unfinished = await asyncio.wait(tasks, timeout=timeout_ms / 1000)
                    if unfinished:
                        raise TimeoutError
        except TimeoutError as exc:
            self._fail(RuntimeError("deadline_exceeded"))
            raise RuntimeError("deadline_exceeded") from exc
        except RuntimeError as exc:
            self._fail(exc)
            raise
        finally:
            if self._drained is not None:
                if self._drained.done() and not self._drained.cancelled():
                    self._drained.exception()
                elif not self._drained.done():
                    self._drained.cancel()

    async def wait_closed(self) -> None:
        await self._closed.wait()

    async def close(self) -> None:
        """Abort transport, retire routes, cancel and await owned tasks (one second)."""
        if self._close_task is None:
            self._close_task = asyncio.create_task(self._perform_close())
        await asyncio.shield(self._close_task)

    async def _perform_close(self) -> None:
        # Duplicated stdout remains open in the process after closing our fd.
        # Only an idle pipe may send unregister; never flush abandoned writes.
        unregister = not self._closed.is_set() and self._transport.is_stdio and self._transport.idle
        self._fail(RuntimeError("connection_lost"), abort=False)
        try:
            if unregister:
                async with asyncio.timeout(0.1):
                    await self._transport.send(_encode({"type": "unregister"}))
        except (OSError, TimeoutError):
            pass  # Heartbeat expiry and the process owner bound kernel retirement.
        finally:
            self._transport.abort()
        tasks = list(self._tasks)
        if self._activation_task is not None and not self._activation_task.done():
            self._activation_task.cancel()
            tasks.append(self._activation_task)
        if self._drain_task is not None and not self._drain_task.done():
            self._drain_task.cancel()
            tasks.append(self._drain_task)
        if tasks:
            _, unfinished = await asyncio.wait(tasks, timeout=1)
            if unfinished:
                raise RuntimeError("deadline_exceeded", "Handlers ignored cancellation during close")
            await asyncio.gather(*tasks, return_exceptions=True)
        try:
            async with asyncio.timeout(1):
                await self._transport.wait_closed()
        except (OSError, TimeoutError):
            pass

    async def __aenter__(self) -> RuntimeHost:
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.close()
