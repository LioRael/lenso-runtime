"""Call either the Rust, TS, or Python log service using generated Python types."""
from __future__ import annotations

import asyncio
import os
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "generated"))
from logs import AggregateInputLogsItem, LogsClient
from lenso_runtime import RuntimeError, RuntimeHost


async def main() -> None:
    transport = os.getenv("LENSO_RUNTIME_TRANSPORT", "tcp")
    identity = os.getenv("LENSO_RUNTIME_SERVICE", "python-client") if transport == "stdio" else "python-client"
    target = os.getenv("LENSO_RUNTIME_TARGET_SERVICE", "batch" if transport == "stdio" else os.getenv("LENSO_RUNTIME_SERVICE", "batch"))
    async with await RuntimeHost.connect(namespace=os.getenv("LENSO_RUNTIME_NAMESPACE", "demo"), service=identity,
            instance=os.getenv("LENSO_RUNTIME_INSTANCE", "one"), transport=transport, url=os.getenv("LENSO_RUNTIME_URL"),
            port=int(os.getenv("LENSO_RUNTIME_PORT", "7331"))) as host:
        client = LogsClient(host, namespace="demo", service=target)
        logs: list[AggregateInputLogsItem] = [{"level": "error", "message": "synthetic failure", "duration_ms": 7}]
        print(await client.aggregate({"logs": logs}), file=sys.stderr)
        async with client.progress({"logs": logs}) as stream:
            async for item in stream:
                print(item, file=sys.stderr)
        try:
            await client.aggregate({"logs": []})
        except RuntimeError as error:
            assert error.code == "business"
            print(error.code, error.details, file=sys.stderr)
        # Task.cancel() sends v1 cancel. Timeouts send cancel and expose a code.
        try:
            async with client.progress({"logs": logs, "delay_ms": 500}, timeout_ms=20) as stream:
                await anext(stream)
        except RuntimeError as error:
            assert error.code == "deadline_exceeded"
            print(error.code, file=sys.stderr)
        await host.drain()


if __name__ == "__main__":
    asyncio.run(main())
