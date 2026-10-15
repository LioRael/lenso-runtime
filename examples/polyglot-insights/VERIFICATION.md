# Local verification — 2026-10-15 UTC

Base Runtime: `765db7a60f0020c1ecce99ef75ff7f7f99e4a385`.
Consumed Python SDK: `e3c557dc92d962128961f49e3612f6718d33bf2f`.
The application commit changes only `examples/polyglot-insights/` and
`scripts/polyglot-insights.ts`. No kernel, protocol, transport or SDK source
change was required. `tools/generate.ts` was read and run, never edited.

Environment: macOS arm64; Bun 1.4.3; Python 3.14.8; Rust
1.101.0-nightly (2026-10-12). Python 3.11 is the SDK minimum, but was not run for
this application. All application networking was numeric loopback TCP. The
execution sandbox initially denied binding a TCP listener; the authorized local
loopback run succeeded after the tool's automatic approval. No credentials or
permissions were changed.

| Command | Result |
| --- | --- |
| `bun scripts/polyglot-insights.ts --verify` | 12 integration checks passed; every owned child exited |
| `bun scripts/polyglot-insights.ts` | Expected CSV report; all three owned processes exited normally |
| `bun test examples/polyglot-insights/lifecycle.test.ts` | 3 passed, 0 failed |
| `bunx --no-install tsc --noEmit -p examples/polyglot-insights/tsconfig.json` | Pass |
| `cargo clippy --locked --manifest-path examples/polyglot-insights/rust/Cargo.toml --target-dir target -- -D warnings` | Pass |
| `cargo fmt --manifest-path examples/polyglot-insights/rust/Cargo.toml --check` | Pass |
| Re-run existing TS/Rust and delivered Python generators against the three contracts | Byte-identical checked-in bindings |

The checked-in `evidence/` directory contains representative successful normal
and fault-suite runs. It includes unmodified JSON results and bounded actual
process logs, plus trace IDs/callers showing all six directed language pairs.
Source SHA-256 manifests and binary hashes identify the code that ran even when
the example was not committed yet. `runtime_commit` is the dependency commit
at that moment; use `application_source_sha256` for the example itself.
Documentation/evidence can be added after execution without changing binaries.

The fixed fixture contains 14 rows: 10 accepted, 4 rejected; 2 server errors,
4 requests >=250 ms, duration sum 2350 ms, p95 800 ms and mean 235.0 ms.
Rust emits five real chunks. Backpressure checks observe zero emitted chunks
without credit and one after one credit. Cancellation/deadline checks observe
Python `finally`, Rust handler drop, and no remaining routes. The deliberate
SIGKILL entries in the fault-run process list are the failure injections; the
normal run shows graceful zero exits instead.

Go, stdio, WebSocket, remote operation, production load, arbitrary blocking work
preemption and side-effect rollback are not verified. Python's app methods are
unary; the stream is produced by Rust. No mocked Python result is included.
