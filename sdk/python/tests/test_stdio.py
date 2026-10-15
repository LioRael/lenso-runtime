from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
import signal
import sys
import tempfile
import unittest

import lenso_runtime
from lenso_runtime import RuntimeError, RuntimeHost, Target
from test_runtime import CONTRACT, EXPECTED, LOGS, ROOT, stop

PEER = ROOT / "sdk/python/tests/stdio_peer.py"
PACKAGE_ROOT = str(Path(lenso_runtime.__file__).resolve().parents[1])
ENV = {**os.environ, "PYTHONPATH": PACKAGE_ROOT}


@unittest.skipUnless(os.name == "posix", "stdio intentionally requires Unix")
class StdioPeerTests(unittest.IsolatedAsyncioTestCase):
    async def start(self, mode="service"):
        self.child = await asyncio.create_subprocess_exec(sys.executable, str(PEER),
            env={**ENV, "TEST_MODE": mode}, stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, limit=262144)
        self.addAsyncCleanup(stop, self.child)
        for expected, reply in (("hello", {"type": "welcome", "version": 1, "generation": "1", "heartbeat_ms": 1000}),
                                ("register", {"type": "registered"}), ("ready", {"type": "ready"})):
            self.assertEqual((await self.read())["type"], expected)
            await self.send(reply)
        self.assertEqual(json.loads(await self.child.stderr.readline())["event"], "ready")

    async def read(self):
        while True:
            frame = json.loads(await asyncio.wait_for(self.child.stdout.readline(), 2))
            if frame["type"] != "heartbeat":
                return frame

    async def send(self, value):
        self.child.stdin.write(json.dumps(value).encode() + b"\n")
        await self.child.stdin.drain()

    def invoke(self, ident, method, input):
        return {"type": "invoke", "id": ident, "method": method, "input": input,
                "timeout_ms": 5000, "trace_id": "stdio-test", "caller": {"namespace": "demo", "service": "peer", "instance": "one"}}

    async def test_stdio_framing_zero_prefetch_cancel_and_eof_exit(self):
        await self.start()
        await self.send(self.invoke("1", "aggregate", {"logs": LOGS}))
        self.assertEqual(await self.read(), {"type": "result", "id": "1", "output": EXPECTED})
        await self.send(self.invoke("2", "progress", {"logs": LOGS * 3}))
        with self.assertRaises(TimeoutError):
            await asyncio.wait_for(self.child.stdout.readline(), 0.03)
        await self.send({"type": "credit", "id": "2", "credits": 1})
        self.assertEqual((await self.read())["output"], {"processed": 1, "total": 3})
        self.assertEqual(json.loads(await self.child.stderr.readline())["event"], "produced")
        await self.send({"type": "cancel", "id": "2"})
        self.assertEqual((await self.read())["code"], "cancelled")
        self.assertEqual(json.loads(await self.child.stderr.readline())["event"], "stream_cleanup")
        self.child.stdin.close()
        code = await asyncio.wait_for(self.child.wait(), 2)
        if code != 0:
            self.fail((await self.child.stderr.read()).decode())
        self.assertEqual(await self.child.stdout.read(), b"")

    async def test_idle_close_sends_unregister_while_process_stays_alive(self):
        await self.start("close-idle")
        self.assertEqual(await self.read(), {"type": "unregister"})
        self.assertEqual(json.loads(await self.child.stderr.readline())["event"], "closed")
        self.assertIsNone(self.child.returncode)

    async def test_blocked_stdout_does_not_block_close_or_process_exit(self):
        await self.start("stalled")
        # Stop consuming stdout until the child reports close on stderr.
        self.assertEqual(json.loads(await asyncio.wait_for(self.child.stderr.readline(), 2))["event"], "closed")
        buffered = await self.child.stdout.read()
        self.assertLess(len(buffered), 32 * 200000)
        self.assertEqual(await asyncio.wait_for(self.child.wait(), 2), 0)

    async def test_malformed_and_truncated_input_exit(self):
        for data in (b'{"type":"unknown"}\n', b'{"type":"result"'):
            with self.subTest(data=data):
                await self.start()
                self.child.stdin.write(data)
                await self.child.stdin.drain()
                self.child.stdin.close()
                self.assertEqual(await asyncio.wait_for(self.child.wait(), 2), 0)
                self.assertEqual(json.loads(await self.child.stderr.readline())["event"], "closed")


@unittest.skipUnless(os.name == "posix", "stdio intentionally requires Unix")
class MixedTransportTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.pid_path = Path(self.directory.name) / "python.pid"
        configs = [
            {"command": sys.executable, "args": [str(PEER)], "env": {"PYTHONPATH": PACKAGE_ROOT, "TEST_PID_FILE": str(self.pid_path)},
             "namespace": "demo", "service": "python-logs", "instance": "one", "transport": "stdio"},
            {"command": str(ROOT / "target/debug/logs-batch"), "namespace": "demo", "service": "rust-stdio", "instance": "one", "transport": "stdio"},
            {"command": sys.executable, "args": [str(PEER)], "env": {"PYTHONPATH": PACKAGE_ROOT, "TEST_UPSTREAM": "rust-stdio"},
             "namespace": "demo", "service": "python-bridge", "instance": "one", "transport": "stdio"},
            {"command": sys.executable, "args": [str(ROOT / "examples/python/service.py")], "env": {"PYTHONPATH": PACKAGE_ROOT},
             "namespace": "demo", "service": "python-example", "instance": "one", "transport": "stdio"},
        ]
        config = Path(self.directory.name) / "processes.json"
        config.write_text(json.dumps(configs))
        self.kernel = await asyncio.create_subprocess_exec(str(ROOT / "target/debug/lenso-runtime"),
            "--listen", "127.0.0.1:0", "--processes", str(config), stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE)
        self.addAsyncCleanup(stop, self.kernel)
        self.port = int(json.loads(await asyncio.wait_for(self.kernel.stderr.readline(), 5))["address"].rsplit(":", 1)[1])
        self.client = await RuntimeHost.connect(namespace="demo", service="tcp-client", instance="one", port=self.port)
        self.addAsyncCleanup(self.client.close)
        async with asyncio.timeout(5):
            while True:
                status = await self.client.status()
                ready = {s["service"] for s in status["services"] if s["state"] == "ready" and s["transport"] == "stdio"}
                if ready == {"python-logs", "rust-stdio", "python-bridge", "python-example"}:
                    break
                await asyncio.sleep(0.01)

    async def test_tcp_to_python_stdio_and_python_stdio_to_rust_stdio(self):
        for service in ("python-logs", "python-bridge", "python-example"):
            target = Target("demo", service)
            self.assertEqual(await self.client.call(target, "aggregate", {"logs": LOGS}, contract=CONTRACT["methods"]["aggregate"]), EXPECTED)
            with self.assertRaises(RuntimeError) as error:
                await self.client.call(target, "aggregate", {"logs": []}, contract=CONTRACT["methods"]["aggregate"])
            self.assertEqual(error.exception.code, "business")
            async with self.client.stream(target, "progress", {"logs": LOGS * 2}, contract=CONTRACT["methods"]["progress"]) as stream:
                self.assertEqual([item["processed"] async for item in stream], [1, 2])
            timed = self.client.stream(target, "progress", {"logs": LOGS, "delay_ms": 500}, timeout_ms=30)
            with self.assertRaises(RuntimeError) as error:
                await anext(timed)
            self.assertEqual(error.exception.code, "deadline_exceeded")
        await asyncio.sleep(0.03)
        self.assertEqual((await self.client.status())["calls"], [])
        rust = await asyncio.create_subprocess_exec(str(ROOT / "target/debug/logs-client"),
            env={**os.environ, "LENSO_RUNTIME_PORT": str(self.port), "LENSO_RUNTIME_SERVICE": "python-logs"},
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        self.addAsyncCleanup(stop, rust)
        out, err = await asyncio.wait_for(rust.communicate(), 5)
        self.assertEqual(rust.returncode, 0, err.decode())
        self.assertEqual(json.loads(out)["total_duration_ms"], 10)

    async def test_ts_tcp_calls_python_stdio(self):
        bun = os.getenv("BUN", "bun")
        ts = await asyncio.create_subprocess_exec(bun, str(ROOT / "examples/python/interop.ts"), cwd=ROOT,
            env={**os.environ, "LENSO_RUNTIME_PORT": str(self.port)}, stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        self.addAsyncCleanup(stop, ts)
        self.assertEqual(json.loads(await asyncio.wait_for(ts.stdout.readline(), 5))["event"], "ts_python_verified")
        ts.stdin.close()
        self.assertEqual(await asyncio.wait_for(ts.wait(), 5), 0, (await ts.stderr.read()).decode())

    async def test_stdio_child_crash_fails_route_without_replay(self):
        stream = self.client.stream(Target("demo", "python-logs"), "progress", {"logs": LOGS, "delay_ms": 500})
        task = asyncio.create_task(anext(stream))
        await asyncio.sleep(0.03)
        os.kill(int(self.pid_path.read_text()), signal.SIGKILL)
        with self.assertRaises(RuntimeError) as error:
            await task
        self.assertEqual(error.exception.code, "connection_lost")
        with self.assertRaises(RuntimeError) as error:
            await self.client.call(Target("demo", "python-logs"), "aggregate", {"logs": LOGS})
        self.assertEqual(error.exception.code, "service_not_found")
        self.assertEqual(await self.client.call(Target("demo", "python-bridge"), "aggregate", {"logs": LOGS}), EXPECTED)


if __name__ == "__main__":
    unittest.main()
