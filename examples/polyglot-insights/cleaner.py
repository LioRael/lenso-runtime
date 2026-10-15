"""Real asyncio CSV processing; the SDK is imported, never reimplemented here."""
from __future__ import annotations

import asyncio
import csv
import json
import os
from pathlib import Path
import signal
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent / "generated"))
from cleaner import CONTRACT, PrepareInput, PrepareOutput, RenderInput, RenderOutput
from gateway import GatewayClient
from metrics import MetricsClient
from lenso_runtime import BusinessError, HandlerContext, RuntimeHost


def event(name: str, context: HandlerContext | None = None, **fields: object) -> None:
    if context:
        fields.update(trace_id=context.trace_id, call_id=context.call_id,
                      parent_id=context.parent_id, caller=context.caller)
    print(json.dumps({"event": name, **fields}), file=sys.stderr, flush=True)


async def main() -> None:
    namespace = os.environ["INSIGHTS_NAMESPACE"]
    host: RuntimeHost

    async def prepare(input: PrepareInput, context: HandlerContext) -> PrepareOutput:
        event("prepare_started", context)
        completed = False
        try:
            # Task cancellation propagates into the SDK's nested call future.
            def options() -> dict:
                remaining = int((context.deadline - asyncio.get_running_loop().time()) * 1000)
                return dict(timeout_ms=max(1, remaining), trace_id=context.trace_id, parent_id=context.call_id)
            policy = await GatewayClient(host, namespace=namespace, service="gateway").policy({}, **options())
            limits = await MetricsClient(host, namespace=namespace, service="metrics").limits({}, **options())
            if not input["lines"]:
                raise BusinessError({"reason": "No CSV lines supplied"})
            result: PrepareOutput = {"records": [], "rejected": 0, "reasons": []}
            for number, line in enumerate(input["lines"], 1):
                await asyncio.sleep(input.get("delay_ms", 0) / 1000)
                try:
                    fields = next(csv.reader([line], strict=True))
                    if len(fields) != 3:
                        raise ValueError("expected service,status,duration_ms")
                    service, raw_status, raw_duration = (field.strip() for field in fields)
                    service = service.lower()
                    if service not in policy["services"]:
                        raise ValueError("unknown service")
                    # Reject ambiguous numeric formats; no float truncation.
                    if not raw_status.isascii() or not raw_status.isdecimal() or not raw_duration.isascii() or not raw_duration.isdecimal():
                        raise ValueError("status and duration must be nonnegative integers")
                    status, duration = int(raw_status), int(raw_duration)
                    if not 100 <= status <= 599 or not 0 <= duration <= limits["max_duration_ms"]:
                        raise ValueError("status or duration outside limits")
                    result["records"].append({"service": service, "status": status, "duration_ms": duration})
                except (ValueError, csv.Error) as error:
                    result["rejected"] += 1
                    result["reasons"].append(f"line {number}: {error}"[:128])
            if len(result["records"]) > limits["max_records"]:
                raise BusinessError({"reason": "Too many valid records"})
            completed = True
            return result
        finally:
            event("prepare_finished", context, completed=completed)

    def render(input: RenderInput, context: HandlerContext) -> RenderOutput:
        event("render", context)
        if input["processed"] != input["total"] or input["total"] == 0:
            raise BusinessError({"reason": "A final nonempty summary is required"})
        return {"text": (f"{input['total']} requests; {input['error_count']} server errors; "
            f"{input['slow_count']} slow requests; p95 {input['p95_ms']} ms; "
            f"mean {input['total_duration_ms'] / input['total']:.1f} ms.")}

    host = await RuntimeHost.connect(namespace=namespace, service="cleaner", instance="python-one",
        port=int(os.environ["LENSO_RUNTIME_PORT"]), contract=CONTRACT,
        handlers={"prepare": prepare, "render": render})
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    event("cleaner_ready", sdk_source=__import__("lenso_runtime").__file__)
    waiters = [asyncio.create_task(stop.wait()), asyncio.create_task(host.wait_closed())]
    try:
        done, _ = await asyncio.wait(waiters, return_when=asyncio.FIRST_COMPLETED)
        if waiters[0] in done:
            await host.drain(1000)
    finally:
        for waiter in waiters:
            waiter.cancel()
        await asyncio.gather(*waiters, return_exceptions=True)
        await host.close()
        event("cleaner_closed")


if __name__ == "__main__":
    asyncio.run(main())
