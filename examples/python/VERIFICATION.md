# Python SDK verification — TCP baseline

Local evidence collected on 2026-10-15 UTC in an isolated checkout of
`LioRael/lenso-runtime`, based on upstream commit
`765db7a60f0020c1ecce99ef75ff7f7f99e4a385`. No Python SDK existed at that base.
Kernel/protocol/transport/Rust SDK/TS SDK source files are unchanged by this work.
The implementation reuses their v1 frames and business JSON contract over TCP.

Environment: macOS arm64, Python 3.14.8, Bun 1.4.3, Cargo 1.101.0-nightly,
Rust 1.101.0-nightly, pyright 1.1.414, setuptools 84.0.0. Python 3.11 is the declared
minimum and the configured CI version; it was statically checked as 3.11 but not
executed locally. CI was configured, not run remotely. Windows is unverified.

| Check | Result |
| --- | --- |
| `cargo build --workspace --locked` | Pass |
| `bun install --frozen-lockfile --ignore-scripts` | Pass; lockfiles unchanged |
| `bun run build:sdk` and `bun run typecheck` | Pass |
| `cargo test --workspace --locked` | Pass |
| `bun test sdk/typescript tools scripts/local.test.ts` | 58 pass, 0 fail |
| `bun run smoke` | Pass; existing TS/Rust real-kernel routes |
| `PYTHONPATH=sdk/python/src python -W error -m unittest discover -s sdk/python/tests -v` | 29 pass, 0 fail |
| Same Python suite from an installed wheel, without `PYTHONPATH` | Pass |
| `pyright --pythonpath <python3.14> -p sdk/python/pyrightconfig.json` | 0 errors, 0 warnings |
| Wheel build, isolated installation, package import and `py.typed` inclusion | Pass |
| Actual `examples/python/service.py` + existing Rust client | Pass; count 2, error count 1, duration 10 |
| Actual `examples/python/client.py` against Rust and Python services | Pass; result, stream, business details, deadline |
| Kernel shutdown with actual Python/Rust example services running | Both services observed disconnect and exited successfully |

The Python tests own a fresh Rust kernel on port zero and cover:

- TS → Python: unary, stream, business details, cancel and deadline.
- Python → TS: unary, stream, business details and deadline.
- Rust → Python: existing generated unary client.
- Python → Rust: unary, stream, business details, task cancellation and deadline.
- Concurrent call correlation, caller-held schema bounds/live mutation, server
  input/output/error validation, zero stream prefetch, one outstanding next,
  generator cancellation cleanup, drain and idempotent close.
- Malformed/unknown frames, strict UTF-8, truncated input, oversized lines,
  half-close, ignored valid late terminals, bounded outbound queue, cancel enqueue
  failure, expired queued writes, frame preflight before serialization, and
  oversized business errors preserving healthy connections.
- An intentionally cancellation-suppressing handler: close reports a bounded
  `deadline_exceeded`; the test then releases and awaits the handler explicitly.

The local wheel is at `sdk/python/dist/lenso_runtime_sdk-0.1.0-py3-none-any.whl`
(build output is ignored by Git). SHA-256:
`1f2ed7aa388e40040e1d0794a4cfa98359643a8b6454859d3c6481fe34be85a8`.
Wheel timestamps can differ on a rebuild; this identifies the verified artifact.

Original lockfile SHA-256 values remain:

- Cargo.lock: `9402e640171c6caf8871ba315c6e44a42284f08d42729482a4754e92016a3913`
- bun.lock: `40768c0456eacd51990635070539c064a0deb9f5415677177728c54cfd58f2fb`

Python supports only literal loopback TCP in this delivery. Stdio/WebSocket,
remote/production identity, automatic reconnect/replay, promotion, staged ready,
raw-data channels, CPU/blocking-work preemption and full framework/plugin parity
are not implemented. No all-transport or production-readiness claim is made.
Cancellation is cooperative and does not roll back side effects. No runtime core
change is needed for coordination with another language SDK effort.

## Owned Unix stdio follow-up

The TCP baseline above is preserved in commit
`e3c557dc92d962128961f49e3612f6718d33bf2f`. This follow-up adds an SDK transport
abstraction and owned Unix pipes, without changing kernel, protocol, shared
generator, Rust SDK or TS SDK. Python supports TCP and Unix stdio at this stage;
WebSocket remains unsupported. The baseline wheel/hash above predates stdio.

New verification on the same macOS/Python environment:

- Full Python suite: 36 pass, 0 fail with warnings treated as errors. Pyright:
  0 errors, 0 warnings.

- Strict stdout NDJSON and stderr logging, zero-credit stream suspension,
  cancellation cleanup, EOF and malformed/truncated-input process exit.
- Idle explicit close emits unregister while the process stays alive. A child
  with blocked stdout closes within its bound and exits without flushing all
  queued calls; no stdin worker thread is left blocking interpreter exit.
- Real kernel routes TCP Python → stdio Python, TCP Python → stdio Python bridge
  → stdio Rust, Rust TCP → Python stdio, and TS TCP → Python stdio.
- The actual `examples/python/service.py` is also launched by kernel process
  configuration and verified for unary/stream/business error/deadline.
- Killing an owned Python child fails its active route, does not restart or
  replay it, and leaves a separate Rust-backed route healthy.

Generic Python CLI inspect/call/dev remain unsupported. Native wheel build and
the explicitly documented fixed application entrypoints are the integration
surface available to CLI adapters. Unix stdio was run locally on macOS; Linux
CI is configured but not yet observed. Windows stdio is explicitly unsupported.

## Optional loopback WebSocket follow-up

This follow-up builds on TCP `e3c557d` and Unix stdio `7188a5c`. Current supported
Python transports are literal loopback TCP, owned Unix stdio, and optional
literal loopback WebSocket. The historical limitations above describe those
prior commits. Shared kernel, Rust/TS SDKs, `tools/generate.ts` and lockfiles
remain unchanged.

Verified locally on 2026-10-15 with the same Python 3.14.8/macOS environment and
`websockets==17.1`, using the existing Rust/Bun build artifacts:

- Source suite before the example-entrypoint addition: 53 tests passed.
- Final installed wheel suite, without `PYTHONPATH`: **54 tests passed**, with
  warnings treated as errors (6.186 seconds). No tests skipped.
- Pyright with the optional dependency installed: **0 errors, 0 warnings**.
- WebSocket Python client and service run the existing full session suite:
  TS TCP ↔ Python WebSocket and Rust TCP ↔ Python WebSocket real-kernel calls,
  correlation, schema checks, stream credit/backpressure, cancellation/deadline,
  business errors, drain, bounded close, disconnect and no replay.
- Actual Python WebSocket example service/client: unary, stream, business error,
  deadline, clean client exit and service exit after kernel shutdown passed.
- Adversarial WebSocket peers: fragmented text, ping/pong, text output without
  newline, binary/oversized/invalid UTF-8 rejection, malformed Runtime handshake,
  and redirect rejection passed. URL tests reject nonliteral/nonloopback hosts,
  credentials, queries, fragments, scoped IPv6 and invalid ports.
- A separate Python interpreter without websockets imported the core successfully
  and returned the documented `invalid_input` install hint for WebSocket.
- Native wheel built offline with `pip wheel --no-index --no-build-isolation
  --no-deps --wheel-dir sdk/python/dist ./sdk/python`, then installed with
  `pip install --no-index --no-deps --force-reinstall <wheel>` in the existing
  isolated package-check environment. The optional dependency was already present.

Final wheel: `sdk/python/dist/lenso_runtime_sdk-0.1.0-py3-none-any.whl`.
SHA-256: `08dec12f9d1ed2ca898655f3ebce37ad922edd1017ff98262b3dee8b932e7a27`.
Reproduce the installed suite from the repository root:

```sh
BUN=<bun-executable> <installed-wheel-python> -W error -m unittest discover -s sdk/python/tests -v
pyright --pythonpath <python-with-websocket-extra> -p sdk/python/pyrightconfig.json
```

CI now installs the WebSocket extra; remote CI and Python 3.11 execution were
not run locally. WebSocket peer tests skip when the optional dependency is absent.
Generic inspect/call/dev, remote authentication/TLS, reconnect/replay, promotion,
deferred ready and Windows stdio remain unsupported. No publish, push, merge or
deployment was performed.
