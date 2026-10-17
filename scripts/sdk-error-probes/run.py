"""Owned real-kernel error probes. Build kernel/SDK first; pass --go-root explicitly."""
import argparse
import asyncio
import os
import json
from pathlib import Path

from lenso_runtime import RuntimeHost, RuntimeError, Target

ROOT = Path(__file__).resolve().parents[2]
METHOD = {"mode": "unary", "input": {"type": "integer"}, "output": {"type": "null"}, "error": {"type": "null"}}


async def stop(process):
    if process.returncode is None:
        process.terminate()
        try:
            await asyncio.wait_for(process.wait(), 3)
        except TimeoutError:
            process.kill()
            await process.wait()


async def run(command, env, cwd=ROOT):
    child = await asyncio.create_subprocess_exec(*map(str, command), env=env, cwd=cwd)
    try:
        assert await asyncio.wait_for(child.wait(), 90) == 0, command
    finally:
        await stop(child)


async def check(work, code):
    try:
        await work
    except RuntimeError as error:
        assert error.code == code and error.details is None, (error.code, code)
        print(f"Python {code}: passed", flush=True)
    else:
        raise AssertionError(code)


async def incompatible_hello(reader, writer, port):
    """Inject only a version mismatch; forward the real kernel's error verbatim."""
    upstream = None
    try:
        async with asyncio.timeout(5):
            hello = json.loads(await reader.readline())
            hello["version"] = 2
            response, upstream = await asyncio.open_connection("127.0.0.1", port)
            upstream.write((json.dumps(hello) + "\n").encode())
            await upstream.drain()
            writer.write(await response.readline())
            await writer.drain()
    finally:
        writer.close()
        await writer.wait_closed()
        if upstream is not None:
            upstream.close()
            await upstream.wait_closed()


async def main(go_root):
    kernel = await asyncio.create_subprocess_exec(str(ROOT / "target/debug/lenso-runtime"),
        "--listen", "127.0.0.1:0", stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE)
    hosts = []
    drain = None
    proxy = None
    proxy_tasks = []
    try:
        line = await asyncio.wait_for(kernel.stderr.readline(), 5)
        port = int(json.loads(line)["address"].rsplit(":", 1)[1])
        async def discard_logs():
            while await kernel.stderr.read(4096):
                pass
        drain = asyncio.create_task(discard_logs())
        def accept(reader, writer):
            proxy_tasks.append(asyncio.create_task(incompatible_hello(reader, writer, port)))
        proxy = await asyncio.start_server(accept, "127.0.0.1", 0)
        protocol_port = proxy.sockets[0].getsockname()[1]
        env = {**os.environ, "LENSO_PROTOCOL_PORT": str(protocol_port), "LENSO_RUNTIME_PORT": str(port), "LENSO_RUNTIME_ADDRESS": f"127.0.0.1:{port}"}
        async def wait(delay, context):
            await asyncio.sleep(delay / 1000)
        service = await RuntimeHost.connect(namespace="errors", service="server", instance="one", port=port,
            contract={"version": "1", "methods": {"wait": METHOD}}, handlers={"wait": wait})
        hosts.append(service)
        client = await RuntimeHost.connect(namespace="errors", service="python", instance="one", port=port)
        hosts.append(client)
        await check(RuntimeHost.connect(namespace="errors", service="bad-python", instance="one", port=protocol_port), "protocol_error")
        target = Target("errors", "server")
        await check(client.call(Target("errors", "server", "2"), "wait", 0), "contract_mismatch")
        await check(client.call(target, "missing", 0), "contract_mismatch")
        await check(client.call(target, "wait", 500, timeout_ms=50), "deadline_exceeded")
        event = asyncio.Event()
        timer = asyncio.get_running_loop().call_later(0.05, event.set)
        try:
            await check(client.call(target, "wait", 500, cancelled=event), "cancelled")
        finally:
            timer.cancel()
        assert await client.call(target, "wait", 0) is None
        await run(["bun", "scripts/sdk-error-probes/client.ts"], env)
        await run(["cargo", "run", "--locked", "--offline", "-p", "runtime-sdk", "--example", "error_probe"], env)
        await run(["go", "run", ROOT / "scripts/sdk-error-probes/main.go"], env, go_root)
        await run(["go", "test", "./sdk/runtime", "-count=1"], env, go_root)
    finally:
        for host in reversed(hosts):
            await host.close()
        if proxy is not None:
            proxy.close()
            await proxy.wait_closed()
        if proxy_tasks:
            await asyncio.gather(*proxy_tasks, return_exceptions=True)
        await stop(kernel)
        if drain is not None:
            await drain


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--go-root", type=Path, required=True)
    args = parser.parse_args()
    asyncio.run(main(args.go_root.resolve()))
