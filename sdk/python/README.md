# Lenso Runtime Python SDK

An asyncio SDK with a dependency-free TCP/stdio core for the existing trusted-local Runtime v1 kernel.
Python services export ordinary short functions, coroutines, or async generators;
TS and Rust services keep their native plugin composition. This package does not
port the Lenso framework or introduce another protocol or identity mechanism.

Requires Python 3.11+. Supported transports are **numeric loopback TCP**
(`127.0.0.1` or `::1`), **owned Unix stdio**, and optional **loopback WebSocket**.
Remote connections, automatic reconnect/replay, promotion and deferred
ready/activation are not implemented.
No production networking, credentials, publication or deployment is involved.

## Install and generate

From the repository root:

```sh
python3 -m venv .venv
. .venv/bin/activate
python -m pip install ./sdk/python
python -m lenso_runtime.generate examples/contracts/logs.json examples/generated/logs.py logs
```

The same business JSON generates TS/Rust with `bun run generate`, and Python with
the command above. Generated Python uses `TypedDict`, `NotRequired`, scalar types,
nullable unions and `Literal` (float enums remain `float` with runtime membership
validation). Required nullable values differ from missing optional fields. The
runtime always checks input, result/chunks and business details against the
caller's supplied schema, including live schema mutation. Generated clients
supply that schema automatically. Low-level `host.call/stream` without a method
contract retain wire checks and callee/kernel validation only.

## Run real services

Build the existing Rust kernel and examples first (`cargo build --workspace --locked`).
Run these commands in separate terminals, from the repository root:

```sh
# Terminal 1
./target/debug/lenso-runtime --listen 127.0.0.1:7331

# Terminal 2: Python exports the existing log contract
python examples/python/service.py

# Terminal 3: the existing Rust client calls Python
LENSO_RUNTIME_SERVICE=python-logs ./target/debug/logs-client

# Terminal 4: Rust exports the same contract under service "batch"
./target/debug/logs-batch

# Terminal 5: Python calls Rust, including stream/business error/deadline
python examples/python/client.py
# Or call Python itself:
LENSO_RUNTIME_SERVICE=python-logs python examples/python/client.py
```

The example service handles SIGINT/SIGTERM on Unix and exits on kernel loss via
`wait_closed()`. Python communicates through Runtime across service boundaries;
there is no Python plugin engine inside the TS/Rust process.

For owned stdio, activate the installed Python environment and run from the root:

```sh
./target/debug/lenso-runtime --listen 127.0.0.1:7331 --processes examples/python/processes.json
# In another terminal, use the same Rust/TS/Python TCP clients as above.
LENSO_RUNTIME_SERVICE=python-logs python examples/python/client.py
```

The kernel supplies identity and `LENSO_RUNTIME_TRANSPORT=stdio` to the example.
Direct SDK callers choose `transport="stdio"` explicitly. One stdio host may be
opened per process lifetime; its stdout is exclusively protocol, and logs belong
on stderr. The SDK duplicates stdin/stdout descriptors and uses nonblocking Unix
asyncio pipes. Their original descriptors remain open; pipe flags are shared.
Windows stdio and non-pipe stdin/stdout are unsupported.

## Optional WebSocket

Install the pinned extra, then start the existing kernel with a WebSocket listener:

```sh
python -m pip install './sdk/python[websocket]'
./target/debug/lenso-runtime --listen 127.0.0.1:7331 --websocket 127.0.0.1:7332
# Separate terminals:
LENSO_RUNTIME_TRANSPORT=websocket LENSO_RUNTIME_URL=ws://127.0.0.1:7332 python examples/python/service.py
LENSO_RUNTIME_TRANSPORT=websocket LENSO_RUNTIME_URL=ws://127.0.0.1:7332 LENSO_RUNTIME_SERVICE=python-logs python examples/python/client.py
```

Direct callers pass `transport="websocket", url="ws://127.0.0.1:7332"` to
`RuntimeHost.connect`. Only literal loopback `ws://` addresses are accepted;
credentials, query strings, fragments, scoped IPv6 and redirects are rejected.
No DNS, proxy, compression, browser Origin or library ping timer is used. Runtime
heartbeats remain active. This is a trusted-local connection, without TLS or
remote authentication. Each text message carries one v1 JSON envelope without
an NDJSON newline; binary messages are rejected. Fragmented text is reassembled
under a 256 KiB minus one byte message limit, with a one-frame receive high-water
mark. Ping/pong control handling is provided by `websockets==17.1`. Close aborts
the owned connection, without reconnect or replay. The extra is imported lazily;
TCP/stdio do not require it. A missing extra yields `invalid_input` with install
instructions.

## Async API

```python
from lenso_runtime import RuntimeHost, Target

async with await RuntimeHost.connect(
    namespace="demo", service="frontend", instance="one"
) as host:
    result = await host.call(Target("demo", "batch"), "aggregate", {"logs": []})
```

Use the generated `examples/generated/logs.py` `LogsClient` for typed methods;
see `examples/python/client.py` and `service.py`. Bindings must exactly match the
contract's method names before connection. Unary functions return a JSON value;
stream functions return an async iterator. Async handler tasks receive normal
`asyncio.CancelledError` on cancel/deadline/session close. Context carries the
route ID, trace/parent IDs, caller, a monotonic deadline and a cancellation event.
Propagate trace IDs explicitly for nested calls. `BusinessError(details)` validates
its details and preserves them for the caller; arbitrary handler exceptions expose
only `invalid_output`, without exception text. `RuntimeError.code` identifies
protocol/business failure; cancelling the caller's Task raises `CancelledError`
and sends v1 cancel. Deadlines include time before stream consumption starts.

Use `async with host.stream(...) as stream` or `await stream.aclose()` when
breaking early. Python `async for` alone does not guarantee iterator close on
`break`. One concurrent `anext()` per stream is supported; another fails
`overloaded`. No item is prefetched: each `anext()` grants one credit and the
callee advances its async generator only after credit. Discovering generator
completion may require the next consumer demand, as in the existing SDKs.

`drain(timeout_ms)` stops local admission and asks the kernel to drain work, then
waits for local handler cleanup within its bound. `close()` is awaited and
idempotent; it retires every request, cancels owned tasks and waits up to one
second for them, plus a bounded transport close. An idle stdio writer attempts
`unregister` within 100 ms before aborting its owned pipe descriptors. Stalled or
failed writers abort immediately, without flushing abandoned writes. Kernel
heartbeat expiry/process ownership bound retirement if unregister cannot be sent.
`wait_closed()` observes the terminal session transition; `close()` awaits
cleanup. Disconnect never reconnects or replays requests. Side effects already
sent can have an uncertain outcome, and cancellation does not roll them back.

Handlers must cooperate with asyncio: short sync functions run on the event loop;
blocking I/O or CPU work must use an application-owned executor/process. Python
cannot forcibly stop code that blocks the loop or suppresses cancellation. Close
reports `deadline_exceeded` if a handler ignores cancellation, retains ownership
of the unfinished task, and never claims that work has stopped. Cancellation
suppression can retain a concurrency permit until the handler exits.

## Bounds and checks

TCP and stdio use strict UTF-8 NDJSON, 256 KiB including newline, a 64-frame outbound queue,
a five-second lifetime for queued/blocked writes, 192 local pending operations,
and 1–64 configured inbound concurrency. There is one active frame write and
bounded asyncio socket/pipe buffering in addition to that queue. No stream-item queue
exists; one pending next consumes one credit. If a required cancel cannot enter
the queue, the connection closes and the current route keeps its original error.
Malformed/truncated input, half-close and failed writes retire the whole session.
Late valid terminal frames for retired calls are ignored. All state belongs to
one asyncio event loop; Hosts are not thread-safe.

Run from the root after building Rust and the TS SDK:

```sh
bun install --frozen-lockfile --ignore-scripts
bun run build:sdk
cargo build --workspace --locked
python -m pip install "./sdk/python[websocket]"
PYTHONPATH=sdk/python/src python3 -W error -m unittest discover -s sdk/python/tests -v
# Optional static analysis with pyright installed:
pyright -p sdk/python/pyrightconfig.json
```

Tests start/stop their own kernel on port zero, plus Rust and TS peers. `BUN` may
specify the Bun executable. They verify both directions of TS/Python and
Rust/Python calls; streaming/error/cancel/deadline checks run through real kernel
routes. Adversarial peer tests cover framing, bounds, late replies, overflow and
cleanup. The shared Rust schema corpus is reused. Local execution evidence is
recorded in `examples/python/VERIFICATION.md`; untested platforms/transports are
not certified by these results.

## CLI adapter capability boundary

All commands below use the repository root as their working directory. The
package does **not** expose a generic Runtime CLI: static `inspect`, arbitrary
`call`, and a managed `dev` command are **unsupported**. The Python example
client has fixed inputs and targets; it must not be advertised as arbitrary call.
The service is a verified application entrypoint, without a development watcher
or supervisor. An adapter may explicitly delegate that entrypoint only for this
example. Native package build is:

```sh
python -m pip wheel --no-deps --wheel-dir sdk/python/dist ./sdk/python
```

Contract generation is `python -m lenso_runtime.generate <contract.json>
<output.py> <contract-name>`; this is code generation, not service inspection.
No generic CLI JSON output or exit-code interface is promised.
