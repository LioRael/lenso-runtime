"""Adversarial peer tests for framing, correlation retirement, and bounded resources."""
from __future__ import annotations

import asyncio
import json
import unittest
from unittest.mock import patch

from lenso_runtime import RuntimeError, RuntimeHost, Target
from lenso_runtime.session import MAX_FRAME, _encode

NULL_METHOD = {"mode": "unary", "input": {"type": "null"}, "output": {"type": "null"}, "error": {"type": "null"}}
TARGET = Target("demo", "other")


class PeerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.peers = []
        self.work = set()
        self.connected = asyncio.get_running_loop().create_future()
        self.server = await asyncio.start_server(self.peer, "127.0.0.1", 0, limit=MAX_FRAME)
        self.port = self.server.sockets[0].getsockname()[1]
        self.host = await RuntimeHost.connect(namespace="demo", service="client", instance="one", port=self.port)
        self.reader, self.writer = await self.connected
        self.failures = []
        self.old_exception_handler = asyncio.get_running_loop().get_exception_handler()
        asyncio.get_running_loop().set_exception_handler(lambda loop, context: self.failures.append(context))

    async def asyncTearDown(self):
        await self.host.close()
        for writer in self.peers:
            writer.close()
            await writer.wait_closed()
        self.server.close()
        await self.server.wait_closed()
        await asyncio.gather(*self.work)
        await asyncio.sleep(0)
        asyncio.get_running_loop().set_exception_handler(self.old_exception_handler)
        self.assertEqual(self.failures, [])

    async def peer(self, reader, writer):
        task = asyncio.current_task()
        self.work.add(task)
        self.peers.append(writer)
        try:
            for reply in ({"type": "welcome", "version": 1, "generation": "1", "heartbeat_ms": 1000},
                          {"type": "registered"}, {"type": "ready"}):
                await reader.readline()
                writer.write(_encode(reply))
                await writer.drain()
            self.connected.set_result((reader, writer))
            await writer.wait_closed()
        finally:
            self.work.discard(task)

    async def read(self):
        while True:
            frame = json.loads(await asyncio.wait_for(self.reader.readline(), 1))
            if frame["type"] != "heartbeat":
                return frame

    async def send(self, frame):
        self.writer.write(_encode(frame))
        await self.writer.drain()
        await asyncio.sleep(0.01)

    async def call(self):
        task = asyncio.create_task(self.host.call(TARGET, "method", None, contract=NULL_METHOD))
        frame = await self.read()
        return task, frame["id"]

    async def test_late_terminals_do_not_reopen_retired_routes(self):
        task, ident = await self.call()
        await self.send({"type": "result", "id": ident, "output": None})
        self.assertIsNone(await task)
        await self.send({"type": "result", "id": ident, "output": None})
        await self.send({"type": "error", "id": ident, "code": "business", "message": "late", "details": None})
        self.assertFalse(self.host._closed.is_set())
        task, ident2 = await self.call()
        self.assertGreater(int(ident2), int(ident))
        await self.send({"type": "result", "id": ident2, "output": None})
        self.assertIsNone(await task)

    async def test_invalid_frame_retires_every_route(self):
        task, ident = await self.call()
        self.writer.write(b'{"type":"result","id":"' + ident.encode() + b'","output":null,"extra":true}\n')
        await self.writer.drain()
        with self.assertRaises(RuntimeError) as error:
            await task
        self.assertEqual(error.exception.code, "protocol_error")
        await self.host.wait_closed()
        self.assertEqual(self.host._pending, {})

    async def test_oversized_error_message_retires_all_routes_without_details(self):
        first, ident = await self.call()
        second, _ = await self.call()
        await self.send({"type": "error", "id": ident, "code": "business",
                         "message": "é" * 513, "details": {"secret": "private-details"}})
        failures = await asyncio.gather(first, second, return_exceptions=True)
        for failure in failures:
            self.assertIsInstance(failure, RuntimeError)
            self.assertEqual(failure.code, "protocol_error")
            self.assertEqual(str(failure), "protocol_error")
            self.assertIsNone(failure.details)
        await self.host.wait_closed()
        self.assertEqual(self.host._pending, {})

    async def test_chunk_without_demand_is_protocol_error(self):
        stream = self.host.stream(TARGET, "stream", None)
        frame = await self.read()
        await self.send({"type": "chunk", "id": frame["id"], "output": None})
        await self.host.wait_closed()
        with self.assertRaises(RuntimeError) as error:
            await anext(stream)
        self.assertEqual(error.exception.code, "protocol_error")

    async def test_eof_half_close_fails_session(self):
        task, ident = await self.call()
        self.writer.write_eof()
        with self.assertRaises(RuntimeError) as error:
            await task
        self.assertEqual(error.exception.code, "connection_lost")
        await self.host.wait_closed()

    async def test_truncated_eof_is_protocol_error(self):
        task, ident = await self.call()
        self.writer.write(b'{"type":"result"')
        self.writer.write_eof()
        with self.assertRaises(RuntimeError) as error:
            await task
        self.assertEqual(error.exception.code, "protocol_error")

    async def test_oversized_line_is_rejected_before_json_parse(self):
        task, ident = await self.call()
        self.writer.write(b" " * MAX_FRAME + b"\n")
        await self.writer.drain()
        with self.assertRaises(RuntimeError) as error:
            await task
        self.assertEqual(error.exception.code, "protocol_error")

    async def test_invalid_utf8_is_protocol_error(self):
        task, ident = await self.call()
        self.writer.write(b'{"type":"result","output":"\xff"}\n')
        await self.writer.drain()
        with self.assertRaises(RuntimeError) as error:
            await task
        self.assertEqual(error.exception.code, "protocol_error")

    async def test_cancel_queue_overflow_closes_without_losing_original_error(self):
        # Freeze the owned writer to deterministically model a saturated socket.
        writer_task = next(t for t in self.host._tasks if t.get_coro().__name__ == "_write_loop")
        writer_task.cancel()
        await asyncio.gather(writer_task, return_exceptions=True)
        stream_method = {**NULL_METHOD, "mode": "stream"}
        stream = self.host.stream(TARGET, "stream", None, contract=stream_method)
        for _ in range(63):
            self.host.stream(TARGET, "stream", None)
        self.assertEqual(self.host._queue.qsize(), 64)
        with self.assertRaises(RuntimeError) as error:
            self.host.stream(TARGET, "stream", None)
        self.assertEqual(error.exception.code, "overloaded")
        self.assertFalse(self.host._closed.is_set())
        await self.send({"type": "error", "id": stream._id, "code": "business", "message": "bad details", "details": 1})
        await self.host.wait_closed()
        with self.assertRaises(RuntimeError) as error:
            await anext(stream)
        self.assertEqual(error.exception.code, "invalid_output")
        self.assertEqual(self.host._queue.qsize(), 0)
        self.assertEqual(self.host._pending, {})

    async def test_expired_queued_write_is_never_transmitted(self):
        writer_task = next(t for t in self.host._tasks if t.get_coro().__name__ == "_write_loop")
        writer_task.cancel()
        await asyncio.gather(writer_task, return_exceptions=True)
        stream = self.host.stream(TARGET, "stream", None)
        _, data = self.host._queue.get_nowait()
        self.host._queue.put_nowait((asyncio.get_running_loop().time() - 1, data))
        self.host._spawn(self.host._write_loop())
        await asyncio.wait_for(self.host.wait_closed(), 1)
        self.assertEqual(await self.reader.read(), b"")
        with self.assertRaises(RuntimeError) as error:
            await anext(stream)
        self.assertEqual(error.exception.code, "connection_lost")

    async def test_task_cancel_has_no_unretrieved_future_error(self):
        task, ident = await self.call()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(await self.read(), {"type": "cancel", "id": ident})
        await self.send({"type": "error", "id": ident, "code": "cancelled", "message": "late"})
        self.assertEqual(self.host._pending, {})


class FrameTests(unittest.TestCase):
    def test_exact_utf8_frame_budget(self):
        base = {"type": "chunk", "id": "1", "output": ""}
        available = MAX_FRAME - len(_encode(base))
        base["output"] = "x" * available
        self.assertEqual(len(_encode(base)), MAX_FRAME)
        base["output"] += "x"
        with self.assertRaises(RuntimeError):
            _encode(base)
        base["output"] = "🙂" * (available // 4 + 1)
        with self.assertRaises(RuntimeError):
            _encode(base)

        # Preflight rejects over-budget content before json.dumps allocates a copy.
        with patch("lenso_runtime.session.json.dumps", side_effect=AssertionError("must not encode")):
            with self.assertRaises(RuntimeError):
                _encode({"type": "chunk", "id": "1", "output": "x" * (MAX_FRAME + 1)})

    def test_error_message_utf8_boundary(self):
        for ident in ({}, {"id": "1"}):
            frame = {"type": "error", "code": "cancelled", "message": "é" * 512, **ident}
            RuntimeHost._validate_frame(frame)
            frame["message"] += "é"
            with self.assertRaises(ValueError):
                RuntimeHost._validate_frame(frame)

    def test_unknown_and_malformed_envelopes(self):
        for frame in ({"type": "bogus"}, {"type": "ready", "extra": 1},
                      {"type": "result", "id": "01", "output": None},
                      {"type": "end", "id": "18446744073709551616"},
                      {"type": "credit", "id": "1", "credits": True},
                      {"type": "error", "code": "invented", "message": "bad"}):
            with self.subTest(frame=frame), self.assertRaises(ValueError):
                RuntimeHost._validate_frame(frame)


if __name__ == "__main__":
    unittest.main()
