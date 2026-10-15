from __future__ import annotations

import asyncio
from contextlib import suppress
import copy
import importlib.util
import json
import os
from pathlib import Path
import shutil
import unittest

from lenso_runtime import BusinessError, RuntimeError, RuntimeHost, Target

ROOT = Path(__file__).resolve().parents[3]
CONTRACT = json.loads((ROOT / "examples/contracts/logs.json").read_text())
LOGS = [{"level": "error", "message": "test", "duration_ms": 7}]
EXPECTED = {"count": 1, "error_count": 1, "total_duration_ms": 7}
TARGET = Target("demo", "python-logs")


async def stop(process):
    if process.returncode is None:
        process.terminate()
        try:
            await asyncio.wait_for(process.wait(), 3)
        except TimeoutError:
            process.kill()
            await process.wait()


class KernelTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.kernel = await asyncio.create_subprocess_exec(
            str(ROOT / "target/debug/lenso-runtime"), "--listen", "127.0.0.1:0",
            stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE)
        self.addAsyncCleanup(stop, self.kernel)
        line = await asyncio.wait_for(self.kernel.stderr.readline(), 5)
        if not line:
            self.fail("Kernel did not open loopback TCP; run cargo build --workspace --locked and allow local sockets")
        self.port = int(json.loads(line)["address"].rsplit(":", 1)[1])
        self.hosts = []
        self.children = []
        self.cancelled = asyncio.Event()
        self.produced = 0

        def aggregate(input, context):
            if not input["logs"]:
                raise BusinessError({"reason": "At least one log is required"})
            return {"count": len(input["logs"]), "error_count": sum(log["level"] == "error" for log in input["logs"]),
                    "total_duration_ms": sum(log["duration_ms"] for log in input["logs"])}

        async def progress(input, context):
            try:
                for i in range(len(input["logs"])):
                    await asyncio.sleep(input.get("delay_ms", 0) / 1000)
                    self.produced += 1
                    yield {"processed": i + 1, "total": len(input["logs"])}
            finally:
                self.cancelled.set()

        self.service = await self.connect("python-logs", contract=copy.deepcopy(CONTRACT),
                                          handlers={"aggregate": aggregate, "progress": progress})
        self.client = await self.connect("client")

    async def connect(self, service, **options):
        host = await RuntimeHost.connect(namespace="demo", service=service, instance="one", port=self.port, **options)
        self.hosts.append(host)
        return host

    async def asyncTearDown(self):
        for host in self.hosts:
            await host.close()
        for child in self.children:
            await stop(child)
        await stop(self.kernel)

    async def assertCode(self, awaitable, code):
        with self.assertRaises(RuntimeError) as failure:
            await awaitable
        self.assertEqual(failure.exception.code, code)
        return failure.exception

    def stream(self, **kwargs):
        return self.client.stream(TARGET, "progress", {"logs": LOGS * 3, **kwargs}, contract=CONTRACT["methods"]["progress"])

    async def test_correlation_concurrent_calls_and_business_error(self):
        results = await asyncio.gather(*(self.client.call(TARGET, "aggregate", {"logs": LOGS * i},
            contract=CONTRACT["methods"]["aggregate"]) for i in range(1, 20)))
        self.assertEqual([r["count"] for r in results], list(range(1, 20)))
        error = await self.assertCode(self.client.call(TARGET, "aggregate", {"logs": []},
            contract=CONTRACT["methods"]["aggregate"]), "business")
        self.assertIn("reason", error.details)
        self.assertEqual(self.client._pending, {})

    async def test_stream_zero_prefetch_cancel_and_cleanup(self):
        async with self.stream() as stream:
            await asyncio.sleep(0.03)
            self.assertEqual(self.produced, 0)
            self.assertEqual(await anext(stream), {"processed": 1, "total": 3})
            await asyncio.sleep(0.03)
            self.assertEqual(self.produced, 1)
        await asyncio.wait_for(self.cancelled.wait(), 1)
        self.assertEqual(self.client._pending, {})
        await asyncio.sleep(0.01)
        self.assertEqual(self.service._invocations, {})

    async def test_stream_end_and_bounded_next(self):
        async with self.stream(delay_ms=20) as stream:
            first = asyncio.create_task(anext(stream))
            await asyncio.sleep(0)
            await self.assertCode(anext(stream), "overloaded")
            self.assertEqual((await first)["processed"], 1)
            self.assertEqual([i["processed"] async for i in stream], [2, 3])
        self.assertEqual(self.produced, 3)

    async def test_task_cancel_and_deadline_reach_handler(self):
        stream = self.stream(delay_ms=500)
        next_item = asyncio.create_task(anext(stream))
        await asyncio.sleep(0.03)
        next_item.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await next_item
        await asyncio.wait_for(self.cancelled.wait(), 1)
        self.cancelled.clear()
        timed = self.client.stream(TARGET, "progress", {"logs": LOGS, "delay_ms": 500}, timeout_ms=30)
        await self.assertCode(anext(timed), "deadline_exceeded")
        await asyncio.wait_for(self.cancelled.wait(), 1)
        self.assertEqual(self.client._pending, {})

    async def test_caller_contract_checks_and_live_mutation(self):
        method = copy.deepcopy(CONTRACT["methods"]["aggregate"])
        await self.assertCode(self.client.call(TARGET, "aggregate", {"logs": [{"level": "info", "message": "bad", "duration_ms": 1.0}]}, contract=method), "invalid_input")
        method["output"]["properties"]["count"]["maximum"] = 0
        await self.assertCode(self.client.call(TARGET, "aggregate", {"logs": LOGS}, contract=method), "invalid_output")
        stream_method = copy.deepcopy(CONTRACT["methods"]["progress"])
        stream = self.client.stream(TARGET, "progress", {"logs": LOGS * 3}, contract=stream_method)
        self.assertEqual((await anext(stream))["processed"], 1)
        stream_method["output"]["properties"]["processed"]["maximum"] = 1
        await self.assertCode(anext(stream), "invalid_output")
        await self.assertCode(anext(stream), "invalid_output")
        await asyncio.wait_for(self.cancelled.wait(), 1)
        self.assertEqual(self.client._pending, {})

    async def test_service_boundary_errors_are_safe(self):
        for name, handler, expected in (
            ("invalid", lambda input, context: {"count": True}, "invalid_output"),
            ("exception", lambda input, context: (_ for _ in ()).throw(ValueError("private-secret")), "invalid_output"),
            ("details", lambda input, context: (_ for _ in ()).throw(BusinessError({"wrong": True})), "invalid_output"),
        ):
            await self.connect(name, contract=CONTRACT, handlers={"aggregate": handler, "progress": lambda i, c: None})
            failure = await self.assertCode(self.client.call(Target("demo", name), "aggregate", {"logs": LOGS}), expected)
            self.assertNotIn("private-secret", str(failure))
        await self.assertCode(self.client.call(TARGET, "aggregate", {"logs": LOGS}, timeout_ms=0), "invalid_input")
        await self.assertCode(self.client.call(Target("demo", "missing"), "aggregate", {"logs": LOGS}), "service_not_found")
        await self.assertCode(self.client.call(Target("demo", "python-logs", "other"), "aggregate", {"logs": LOGS}), "contract_mismatch")

    async def test_oversized_business_response_preserves_other_routes(self):
        contract = copy.deepcopy(CONTRACT)
        contract["methods"]["aggregate"]["error"]["properties"]["reason"]["maxLength"] = 300000
        def huge_error(input, context):
            raise BusinessError({"reason": "x" * 262144})
        service = await self.connect("huge-error", contract=contract,
            handlers={"aggregate": huge_error, "progress": lambda i, c: None})
        await self.assertCode(self.client.call(Target("demo", "huge-error"), "aggregate", {"logs": LOGS}), "invalid_output")
        self.assertFalse(service._closed.is_set())
        self.assertEqual(await self.client.call(TARGET, "aggregate", {"logs": LOGS}), EXPECTED)

    async def test_drain_and_idempotent_close(self):
        task = asyncio.create_task(self.client.call(TARGET, "aggregate", {"logs": LOGS}))
        await asyncio.sleep(0)
        await self.client.drain()
        self.assertEqual(await task, EXPECTED)
        await self.assertCode(self.client.call(TARGET, "aggregate", {"logs": LOGS}), "not_ready")
        await asyncio.gather(self.client.close(), self.client.close())
        await self.client.wait_closed()
        self.assertEqual(self.client._pending, {})
        self.assertEqual(self.client._tasks, set())

    async def test_disconnect_fails_inflight_and_never_replays(self):
        stream = self.stream(delay_ms=500)
        task = asyncio.create_task(anext(stream))
        await asyncio.sleep(0.02)
        await self.service.close()
        await self.assertCode(task, "connection_lost")
        await self.assertCode(self.client.call(TARGET, "aggregate", {"logs": LOGS}), "service_not_found")
        await stop(self.kernel)
        await asyncio.wait_for(self.client.wait_closed(), 1)
        await self.assertCode(self.client.call(TARGET, "aggregate", {"logs": LOGS}), "connection_lost")

    async def test_rust_calls_python_and_python_calls_rust(self):
        env = {**os.environ, "LENSO_RUNTIME_PORT": str(self.port), "LENSO_RUNTIME_SERVICE": "python-logs"}
        rust = await asyncio.create_subprocess_exec(str(ROOT / "target/debug/logs-client"), env=env,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        self.children.append(rust)
        out, err = await asyncio.wait_for(rust.communicate(), 5)
        self.assertEqual(rust.returncode, 0, err.decode())
        self.assertEqual(json.loads(out), {"count": 2, "error_count": 1, "total_duration_ms": 10})
        env["LENSO_RUNTIME_SERVICE"] = "rust-logs"
        rust_service = await asyncio.create_subprocess_exec(str(ROOT / "target/debug/logs-batch"), env=env,
            stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE)
        self.children.append(rust_service)
        ready = await asyncio.wait_for(rust_service.stderr.readline(), 5)
        self.assertEqual(json.loads(ready)["event"], "service_ready")
        target = Target("demo", "rust-logs")
        self.assertEqual(await self.client.call(target, "aggregate", {"logs": LOGS}, contract=CONTRACT["methods"]["aggregate"]), EXPECTED)
        await self.assertCode(self.client.call(target, "aggregate", {"logs": []}, contract=CONTRACT["methods"]["aggregate"]), "business")
        async with self.client.stream(target, "progress", {"logs": LOGS}, contract=CONTRACT["methods"]["progress"]) as stream:
            self.assertEqual([item async for item in stream], [{"processed": 1, "total": 1}])
        timed = self.client.stream(target, "progress", {"logs": LOGS, "delay_ms": 500}, timeout_ms=20)
        await self.assertCode(anext(timed), "deadline_exceeded")
        cancelled = self.client.stream(target, "progress", {"logs": LOGS, "delay_ms": 500})
        task = asyncio.create_task(anext(cancelled))
        await asyncio.sleep(0.02)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        await asyncio.sleep(0.02)
        self.assertEqual((await self.client.status())["calls"], [])

    async def test_close_is_bounded_when_handler_suppresses_cancellation(self):
        entered, release = asyncio.Event(), asyncio.Event()

        async def stubborn(input, context):
            entered.set()
            try:
                await release.wait()
            except asyncio.CancelledError:
                await release.wait()
            return EXPECTED

        service = await self.connect("stubborn", contract=CONTRACT,
                                     handlers={"aggregate": stubborn, "progress": lambda i, c: None})
        self.hosts.remove(service)
        task = asyncio.create_task(self.client.call(Target("demo", "stubborn"), "aggregate", {"logs": LOGS}))
        await asyncio.wait_for(entered.wait(), 1)
        started = asyncio.get_running_loop().time()
        try:
            await self.assertCode(service.close(), "deadline_exceeded")
            self.assertLess(asyncio.get_running_loop().time() - started, 1.5)
            await self.assertCode(task, "connection_lost")
        finally:
            release.set()
            await asyncio.gather(*service._tasks, return_exceptions=True)
        self.assertEqual(service._invocations, {})

    async def test_typescript_bidirectional(self):
        bun = os.getenv("BUN", shutil.which("bun"))
        self.assertIsNotNone(bun, "bun is needed for TS/Python interoperability")
        ts = await asyncio.create_subprocess_exec(bun, str(ROOT / "examples/python/interop.ts"), cwd=ROOT,
            env={**os.environ, "LENSO_RUNTIME_PORT": str(self.port)}, stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        self.children.append(ts)
        line = await asyncio.wait_for(ts.stdout.readline(), 10)
        if not line:
            self.fail((await ts.stderr.read()).decode())
        self.assertEqual(json.loads(line)["event"], "ts_python_verified")
        target = Target("demo", "ts-logs")
        self.assertEqual(await self.client.call(target, "aggregate", {"logs": LOGS}, contract=CONTRACT["methods"]["aggregate"]), EXPECTED)
        await self.assertCode(self.client.call(target, "aggregate", {"logs": []}, contract=CONTRACT["methods"]["aggregate"]), "business")
        async with self.client.stream(target, "progress", {"logs": LOGS}, contract=CONTRACT["methods"]["progress"]) as stream:
            self.assertEqual([item async for item in stream], [{"processed": 1, "total": 1}])
        timed = self.client.stream(target, "progress", {"logs": LOGS, "delay_ms": 500}, timeout_ms=20)
        await self.assertCode(anext(timed), "deadline_exceeded")
        ts.stdin.close()
        await asyncio.wait_for(ts.wait(), 5)
        self.assertEqual(ts.returncode, 0, (await ts.stderr.read()).decode())


if __name__ == "__main__":
    unittest.main()
