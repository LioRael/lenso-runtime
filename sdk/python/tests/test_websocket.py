from __future__ import annotations

import asyncio
import importlib.util
import json
import os
import sys
import unittest
from unittest.mock import patch

from lenso_runtime import RuntimeError, RuntimeHost
from lenso_runtime.transport import MAX_FRAME, websocket_address
import test_runtime

HAS_WS = importlib.util.find_spec("websockets") is not None


class WebSocketAddressTests(unittest.TestCase):
    def test_literal_loopback_only(self):
        self.assertEqual(websocket_address("ws://127.0.0.1:7332/runtime"), ("127.0.0.1", 7332))
        self.assertEqual(websocket_address("ws://[::1]:7332"), ("::1", 7332))
        for url in (None, "wss://127.0.0.1", "ws://localhost", "ws://192.0.2.1", "ws://127.0.0.1:0",
                    "ws://127.0.0.1:65536", "ws://user@127.0.0.1", "ws://127.0.0.1?x", "ws://127.0.0.1#x",
                    "ws://[::1%lo0]", "ws://127.0.0.1/\n"):
            with self.subTest(url=url), self.assertRaises(ValueError):
                websocket_address(url)


@unittest.skipUnless(HAS_WS, "install lenso-runtime-sdk[websocket] for WebSocket checks")
class WebSocketKernelTests(test_runtime.KernelTests):
    """Run the full session/TS/Rust suite with both Python endpoints on WebSocket."""

    async def asyncSetUp(self):
        # Preserve the real kernel fixture and all inherited semantic assertions.
        create = asyncio.create_subprocess_exec
        async def spawn(*args, **kwargs):
            if args[0] == str(test_runtime.ROOT / "target/debug/lenso-runtime"):
                args = (*args, "--websocket", "127.0.0.1:0")
            return await create(*args, **kwargs)
        self.ws_url = None
        with patch("asyncio.create_subprocess_exec", spawn):
            await super().asyncSetUp()

    async def connect(self, service, **options):
        if self.ws_url is None:
            event = json.loads(await asyncio.wait_for(self.kernel.stderr.readline(), 5))
            self.assertEqual(event["event"], "runtime_websocket_listening")
            self.ws_url = "ws://" + event["address"]
        return await super().connect(service, transport="websocket", url=self.ws_url, **options)

    async def test_actual_websocket_example_service_and_client(self):
        env = {**os.environ, "LENSO_RUNTIME_TRANSPORT": "websocket", "LENSO_RUNTIME_URL": self.ws_url,
               "LENSO_RUNTIME_SERVICE": "python-example", "LENSO_RUNTIME_PORT": str(self.port)}
        service = await asyncio.create_subprocess_exec(sys.executable,
            str(test_runtime.ROOT / "examples/python/service.py"), env=env,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        self.children.append(service)
        async with asyncio.timeout(3):
            while not any(s["service"] == "python-example" and s["state"] == "ready"
                          for s in (await self.client.status())["services"]):
                await asyncio.sleep(0.01)
        client = await asyncio.create_subprocess_exec(sys.executable,
            str(test_runtime.ROOT / "examples/python/client.py"), env=env,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        self.children.append(client)
        out, err = await asyncio.wait_for(client.communicate(), 3)
        self.assertEqual(client.returncode, 0, err.decode())
        self.assertEqual(out, b"")
        for expected in (b"total_duration_ms", b"processed", b"business", b"deadline_exceeded"):
            self.assertIn(expected, err)
        await test_runtime.stop(self.kernel)
        self.assertEqual(await asyncio.wait_for(service.wait(), 2), 0)


@unittest.skipUnless(HAS_WS, "install lenso-runtime-sdk[websocket] for WebSocket checks")
class WebSocketPeerTests(unittest.IsolatedAsyncioTestCase):
    async def peer(self, action):
        from websockets.asyncio.server import serve
        async def handler(connection):
            try:
                await action(connection)
            finally:
                await connection.wait_closed()
        server = await serve(handler, "127.0.0.1", 0, compression=None)
        self.addAsyncCleanup(self.close_server, server)
        return "ws://127.0.0.1:" + str(server.sockets[0].getsockname()[1])

    async def close_server(self, server):
        server.close()
        await server.wait_closed()

    async def test_fragmentation_ping_and_text_framing(self):
        from lenso_runtime.transport import open_websocket
        seen = asyncio.Future()
        async def action(ws):
            seen.set_result(await ws.recv())
            pong = await ws.ping(b"alive")
            await asyncio.wait_for(pong, 1)
            await ws.send(['{"type":', '"heartbeat"}'])
        transport = await open_websocket(await self.peer(action))
        try:
            await transport.send(b'{"type":"heartbeat"}\n')
            self.assertEqual(await asyncio.wait_for(transport.receive(), 2), b'{"type":"heartbeat"}')
            self.assertEqual(await seen, '{"type":"heartbeat"}')
        finally:
            transport.abort()
            await transport.wait_closed()

    async def test_binary_oversize_and_invalid_utf8_rejected(self):
        from lenso_runtime.transport import open_websocket
        async def binary(ws):
            await ws.send(b'{}')
        async def oversized(ws):
            await ws.send('x' * MAX_FRAME)
        async def invalid_utf8(ws):
            await ws.send(b'\xff', text=True)
        for action in (binary, oversized, invalid_utf8):
            with self.subTest(action=action.__name__):
                transport = await open_websocket(await self.peer(action))
                try:
                    with self.assertRaises(ValueError):
                        await asyncio.wait_for(transport.receive(), 2)
                finally:
                    transport.abort()
                    await transport.wait_closed()

    async def test_malformed_runtime_message_fails_handshake(self):
        async def action(ws):
            await ws.recv()
            await ws.send('{"type":"unknown"}')
        with self.assertRaises(RuntimeError) as failure:
            await RuntimeHost.connect(namespace="demo", service="bad", instance="one",
                                      transport="websocket", url=await self.peer(action))
        self.assertEqual(failure.exception.code, "protocol_error")

    async def test_redirect_is_not_followed(self):
        from websockets.asyncio.server import serve
        from websockets.http11 import Response
        from websockets.datastructures import Headers
        from lenso_runtime.transport import open_websocket
        hits = []
        async def handler(ws):
            self.fail("redirect must not establish a WebSocket")
        def redirect(connection, request):
            hits.append(request.path)
            return Response(302, "Found", Headers({"Location": "ws://192.0.2.1:80/"}), b"")
        async with serve(handler, "127.0.0.1", 0, process_request=redirect) as server:
            url = "ws://127.0.0.1:" + str(server.sockets[0].getsockname()[1])
            with self.assertRaises(ConnectionError):
                await asyncio.wait_for(open_websocket(url), 2)
        self.assertEqual(hits, ["/"])
