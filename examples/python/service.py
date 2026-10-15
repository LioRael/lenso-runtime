"""A real Python log service using the shared JSON contract and Rust kernel."""
from __future__ import annotations

import asyncio
import os
from pathlib import Path
import signal
import sys
from collections.abc import AsyncIterator

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "generated"))
from logs import (CONTRACT, AggregateInput, AggregateOutput, ProgressInput, ProgressOutput)
from lenso_runtime import BusinessError, HandlerContext, RuntimeHost


def aggregate(input: AggregateInput, context: HandlerContext) -> AggregateOutput:
    """Short ordinary functions work directly; long work must yield to asyncio."""
    logs = input["logs"]
    if not logs:
        raise BusinessError({"reason": "At least one log is required"})
    return {"count": len(logs), "error_count": sum(log["level"] == "error" for log in logs),
            "total_duration_ms": sum(log["duration_ms"] for log in logs)}


async def progress(input: ProgressInput, context: HandlerContext) -> AsyncIterator[ProgressOutput]:
    logs = input["logs"]
    if not logs:
        raise BusinessError({"reason": "At least one log is required"})
    for index in range(len(logs)):
        await asyncio.sleep(input.get("delay_ms", 0) / 1000)
        yield {"processed": index + 1, "total": len(logs)}


async def main() -> None:
    host = await RuntimeHost.connect(namespace=os.getenv("LENSO_RUNTIME_NAMESPACE", "demo"), service=os.getenv("LENSO_RUNTIME_SERVICE", "python-logs"),
        instance=os.getenv("LENSO_RUNTIME_INSTANCE", "python-one"),
        transport=os.getenv("LENSO_RUNTIME_TRANSPORT", "tcp"), url=os.getenv("LENSO_RUNTIME_URL"),
        port=int(os.getenv("LENSO_RUNTIME_PORT", "7331")), contract=CONTRACT,
        handlers={"aggregate": aggregate, "progress": progress})
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    print("Python log service ready", file=sys.stderr, flush=True)
    waiters = [asyncio.create_task(stop.wait()), asyncio.create_task(host.wait_closed())]
    try:
        await asyncio.wait(waiters, return_when=asyncio.FIRST_COMPLETED)
        await host.drain()
    finally:
        for waiter in waiters:
            waiter.cancel()
        await asyncio.gather(*waiters, return_exceptions=True)
        await host.close()


if __name__ == "__main__":
    asyncio.run(main())
