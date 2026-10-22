"""Session lifecycle with a bounded scripted transport; no listener proof implied."""
import asyncio
import copy
import json
import unittest
from unittest.mock import patch

from lenso_runtime import RuntimeError, RuntimeHost, Target

METHOD = {"mode": "unary", "input": {"type": "null"}, "output": {"type": "null"}, "error": {"type": "null"}}
CONTRACT = {"version": "1", "methods": {"run": METHOD}}


class Peer:
    is_stdio = False
    idle = True

    def __init__(self):
        self.incoming = asyncio.Queue()
        self.frames = []
        self.aborted = False
        self.ready_sent = asyncio.Event()
        self.reply_ready = True

    async def send(self, data):
        f = json.loads(data)
        self.frames.append(f)
        kind = f["type"]
        reply = None
        if kind == "hello":
            reply = {"type": "welcome", "version": 1, "generation": "1", "heartbeat_ms": 1000}
        elif kind == "register":
            reply = {"type": "registered"}
        elif kind == "ready":
            self.ready_sent.set()
            if self.reply_ready:
                reply = {"type": "ready"}
        elif kind == "drain":
            reply = {"type": "drained"}
        elif kind == "status":
            reply = {"type": "status", "id": f["id"], "services": [], "calls": [], "limits": {}}
        if reply:
            await self.incoming.put(json.dumps(reply).encode())

    async def receive(self):
        return await self.incoming.get()

    def abort(self):
        self.aborted = True

    async def wait_closed(self):
        pass


class LifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.peer = Peer()
        self.patch = patch("lenso_runtime.session.open_tcp", return_value=self.peer)
        self.patch.start()
        self.addCleanup(self.patch.stop)
        self.hosts = []

    async def registered(self, **kwargs):
        host = await RuntimeHost.connect_registered(namespace="demo", service="python", instance="one", **kwargs)
        self.hosts.append(host)
        self.addAsyncCleanup(host.close)
        return host

    async def test_registered_supports_outbound_without_ready(self):
        host = await self.registered(contract=CONTRACT)
        self.assertEqual([f["type"] for f in self.peer.frames], ["hello", "register"])
        self.assertEqual((await host.status())["services"], [])
        self.assertNotIn("ready", [f["type"] for f in self.peer.frames])

    async def test_binding_exact_and_activation_shared(self):
        host = await self.registered(contract=CONTRACT)
        with self.assertRaises(RuntimeError):
            host.bind_exports({})
        host.bind_exports({"run": lambda *_: None})
        await asyncio.gather(host.activate(), host.activate())
        self.assertEqual(sum(f["type"] == "ready" for f in self.peer.frames), 1)
        with self.assertRaises(RuntimeError):
            host.bind_exports({"run": lambda *_: None})

    async def test_missing_bindings_never_sends_ready(self):
        host = await self.registered(contract=CONTRACT)
        with self.assertRaises(RuntimeError) as failure:
            await host.activate()
        self.assertEqual(failure.exception.code, "invalid_input")
        self.assertFalse(self.peer.ready_sent.is_set())

    async def test_cancel_activation_closes_uncertain_session(self):
        host = await self.registered()
        self.peer.reply_ready = False
        task = asyncio.create_task(host.activate())
        await self.peer.ready_sent.wait()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertTrue(self.peer.aborted)
        await host.wait_closed()

    async def test_disconnect_during_activation_fails_waiter(self):
        host = await self.registered()
        self.peer.reply_ready = False
        task = asyncio.create_task(host.activate())
        await self.peer.ready_sent.wait()
        await self.peer.incoming.put(b'{"type":"unknown"}')
        with self.assertRaises(RuntimeError) as failure:
            await task
        self.assertEqual(failure.exception.code, "protocol_error")
        self.assertTrue(self.peer.aborted)

    async def test_drain_registered_prevents_activation_and_outbound(self):
        host = await self.registered()
        await host.drain(100)
        with self.assertRaises(RuntimeError):
            await host.activate()
        with self.assertRaises(RuntimeError):
            await host.status()
        self.assertFalse(self.peer.ready_sent.is_set())

    async def test_immediate_connect_keeps_ready_behavior(self):
        host = await RuntimeHost.connect(namespace="demo", service="client", instance="one")
        self.addAsyncCleanup(host.close)
        self.assertTrue(self.peer.ready_sent.is_set())

    async def test_immediate_missing_bindings_rejected_before_io(self):
        with self.assertRaises(RuntimeError):
            await RuntimeHost.connect(namespace="demo", service="client", instance="one", contract=CONTRACT)
        self.assertEqual(self.peer.frames, [])

    async def test_registration_snapshots_contract_and_handlers(self):
        contract = copy.deepcopy(CONTRACT)
        handlers = {"run": lambda *_: None}
        host = await self.registered(contract=contract, handlers=handlers)
        contract["methods"].clear()
        handlers.clear()
        await host.activate()
        self.assertTrue(self.peer.ready_sent.is_set())
        self.assertEqual(set(self.peer.frames[1]["contract"]["methods"]), {"run"})
