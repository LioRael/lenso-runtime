"""Owned test child. Stdout is exclusively Runtime protocol; events go to stderr."""
from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
import signal
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "examples/python"))
from service import CONTRACT, aggregate, progress
from lenso_runtime import RuntimeHost, Target


def event(name, **values):
    print(json.dumps({"event": name, **values}), file=sys.stderr, flush=True)


async def main():
    upstream = os.getenv("TEST_UPSTREAM")
    host = None
    async def unary(input, context):
        if upstream:
            return await host.call(Target("demo", upstream), "aggregate", input,
                contract=CONTRACT["methods"]["aggregate"], trace_id=context.trace_id, parent_id=context.call_id)
        return aggregate(input, context)
    async def stream(input, context):
        try:
            if upstream:
                async with host.stream(Target("demo", upstream), "progress", input,
                        contract=CONTRACT["methods"]["progress"], trace_id=context.trace_id, parent_id=context.call_id) as items:
                    async for item in items:
                        yield item
            else:
                async for item in progress(input, context):
                    event("produced")
                    yield item
        finally:
            event("stream_cleanup")
    host = await RuntimeHost.connect(namespace="demo", service=os.getenv("LENSO_RUNTIME_SERVICE", "python-logs"),
        instance=os.getenv("LENSO_RUNTIME_INSTANCE", "one"), contract=CONTRACT,
        handlers={"aggregate": unary, "progress": stream}, transport="stdio")
    if os.getenv("TEST_PID_FILE"):
        Path(os.environ["TEST_PID_FILE"]).write_text(str(os.getpid()))
    event("ready")
    mode = os.getenv("TEST_MODE", "service")
    if mode == "close-idle":
        await host.close()
        event("closed")
        await asyncio.Event().wait()  # Parent verifies unregister before terminating this child.
    elif mode == "stalled":
        calls = [asyncio.create_task(host.call(Target("demo", "other"), "echo", "x" * 200000)) for _ in range(32)]
        await asyncio.sleep(0.1)
        await host.close()
        await asyncio.gather(*calls, return_exceptions=True)
        event("closed")
    else:
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGTERM, stop.set)
        waits = [asyncio.create_task(host.wait_closed()), asyncio.create_task(stop.wait())]
        try:
            await asyncio.wait(waits, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for wait in waits:
                wait.cancel()
            await asyncio.gather(*waits, return_exceptions=True)
            await host.close()
            event("closed")


if __name__ == "__main__":
    asyncio.run(main())
