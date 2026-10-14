# Lenso Runtime

A local cross-language execution kernel, not a replacement for TypeScript
Lenso. Plugins inside a service keep native calls, dependency injection and
cleanup. Only explicitly exported capabilities cross the kernel.

## Build and run

Requirements for this source checkout: Rust/Cargo and Bun.

```sh
bun install
bun run build
bun run test
bun run smoke
cargo build --workspace --release
bun run perf
```

`smoke` starts and stops its own loopback kernel and Rust service on an available
port. It exercises generated TypeScript calls, stream progress, cancellation,
deadlines and a lost callee connection. It uses synthetic structured logs only.
The brief ephemeral-port reservation has a bind/release race; startup failure is
reported rather than attaching to an unknown process.

To keep the processes running separately:

```sh
target/debug/lenso-runtime --listen 127.0.0.1:7331
LENSO_RUNTIME_PORT=7331 target/debug/logs-batch
```

The application/dev command or process manager can still start Hosts and the kernel.
Optionally the kernel owns a small set of persistent service commands; it does not
replace the native App or development build/hot-replacement supervisor.
One Host process represents one scoped Service instance. Multiple instances may
register the same Service contract.

## Mixed local transports

TCP remains the SDK default. Only connection/start configuration chooses a
transport; contracts, generated business clients and native plugin composition
are unchanged. A single kernel routes between all connected transports.

```sh
target/debug/lenso-runtime --listen 127.0.0.1:7331 \
  --websocket 127.0.0.1:7332 --processes services.json
bun scripts/transport-smoke.ts
```

`--websocket` is optional. `--processes` reads a bounded JSON array, for example:

```json
[{
  "command": "target/debug/logs-batch",
  "args": [],
  "namespace": "demo",
  "service": "batch",
  "instance": "rust-one",
  "transport": "stdio",
  "ready_timeout_ms": 5000,
  "shutdown_timeout_ms": 2000
}]
```

Commands are executed directly, not shell-joined. Relative command/cwd paths are
relative to the launch context; use an absolute executable path when setting cwd.
Optional `cwd` and `env` overrides are supported; environment is inherited, and
Runtime supplies its actual TCP port and enforced transport selector. Do not put
secrets in checked-in configuration. Environment values are not logged.

TS: `RuntimeHost.connect({ ...options, transport: { type: "stdio" } })` or
`transport: { type: "websocket", url: "ws://127.0.0.1:7332" }`.
Rust retains the existing `HostOptions` struct and default `connect` /
`connect_registered`; select `Transport::Stdio` or
`Transport::WebSocket { url }` with `connect_with_transport` /
`connect_registered_with_transport`. Native Apps should use the latter and
activate only after their existing setup/export binding succeeds.

Stdio services are **persistent child processes**, never one process per request.
Stdout is reserved for protocol, including before ready and during cleanup:
use `console.error` / `eprintln!` for business logs. Runtime reports process
started, connected and ready separately and awaits shutdown/reaping.
TCP-managed commands use the same process owner; independently launched TCP
and WebSocket services remain caller/process-manager owned.
See [PROTOCOL.md](PROTOCOL.md#owned-processes-and-local-websocket-policy) for
ready failure, drain/force-close bounds and platform limitations.

WebSocket is trusted local development only, with explicit loopback listeners,
bounded JSON text messages, no newline wrapping and no raw binary channel.
Browser Origin-bearing handshakes and remote/WSS mode are rejected.
TLS termination, authenticated service identity and capability authorization
remain prerequisites for any future remote production mode, not features here.

## Developer-owned code

1. Keep ordinary Lenso plugins and async business functions.
2. Add one Runtime plugin to the TS App, supply explicit exports and inject
   generated clients into business plugins through normal typed dependencies.
3. Put a restricted JSON Schema contract beside the exported business feature.
4. Generate both languages from that file:

```sh
bun tools/generate.ts examples/contracts/logs.json examples/generated logs
```

The SDK supplies framing, correlation, bounded buffering, heartbeat, stream
credit and cancellation. Generated clients supply method names and input/output
types; the kernel still validates every service boundary. Host code supplies
local setup, async handlers and cleanup. Rust handlers are statically linked
into their service executable, with no dynamic ABI or source execution.
Optional non-null fields and required nullable fields are supported. Optional
nullable fields are deliberately rejected in v1, at registration and generation.
Integers use canonical JSON integer tokens in the safe range; optional values
must be omitted rather than passed as JavaScript `undefined`.

Both generated clients obtain their version and validation schemas from the
contract source. TS and Rust check the caller's input, output, business error
and stream-item constraints, not only language types or the callee's contract.
Rust's generated `LogsServer` builder infers handler input/output types and
checks complete bindings before connect:

```rust
let handlers = LogsServer::new()
    .aggregate(aggregate)
    .progress(progress)
    .build()?;
let host = RuntimeHost::connect(options, handlers).await?;
// Stop admission and await bounded work before releasing native resources.
host.drain(5000).await?;
host.close();
```

See `examples/batch/src/main.rs` for the async handlers and host options.
Manual `Handlers::unary` / `Handlers::stream` remain available and use the same
exact method/mode/duplicate validation. Failed initialization never sends ready
or leaves background connection tasks running.
For native assembly requiring outbound clients during setup, Rust additionally
provides `RuntimeHost::connect_registered(options)`. It validates local options
and registers without ready. After setup, check all explicit bindings with
`host.validate_handlers(&handlers)`, then await `host.activate(handlers)`.
Activation validates again, binds once and waits for the ready acknowledgment;
failure or cancellation closes the connection. `connect` remains the convenience
register-and-activate API for SDK-only services.
Rust's low-level generic `call` / `stream` only deserialize; use the generated
client or `call_contract` / `stream_contract` for caller-side schema validation.
For nested calls, explicitly forward trace and cancellation context:
`{traceId: context.traceId, parentId: context.callId, signal: context.signal}`.
Rust exposes the equivalent `trace_id`, `call_id` and cancellation token.
The SDK does not install an implicit global business container.

The companion `lenso` repository already contains `packages/runtime` and
`examples/runtime` for Core Plugin/Manage/Engine integration. The `lenso-console`
repository contains `packages/console-runtime`, the optional read-only page.
These remain separately owned repositories, not source copied into this one.
The standalone build, tests, smoke example and performance entry above need
only this repository. `@lenso/runtime-sdk` is an ordinary package, but the
checkout-local SDK and adapter version strings do not imply npm publication.
Use `startRuntimeApp` for a serving App: it binds native exports after Core setup,
then publishes readiness, and pre-drains before Core cleanup. Plain `startApp`
can create an outbound-only caller without publishing server capabilities.
The Console checkout supplies the independent optional `@lenso/console-runtime`
page. Its example host selects it only with `LENSO_RUNTIME_PORT`, using the
existing authenticated administrator session and generic plugin route.
For source integration, use Bun 1.4.3 in each consuming checkout. In `lenso`,
run `bun run prepare:runtime`, then `bun install --frozen-lockfile`, build the
framework packages and Runtime adapter as directed by `examples/runtime/README.md`,
and run that example's existing `generate` command. Preparation downloads and
checksums a pinned Runtime source snapshot, builds only the TS SDK into
`.runtime-deps/sdk`, and retains its generator/contract in `.runtime-deps/source`.
It is a reproducible source dependency, not an npm release. Build this checkout's
native binaries independently with Cargo; set `LENSO_RUNTIME_ROOT` or explicit
binary overrides when running the example. No machine-specific path is stored.
Console has its own pinned `prepare:runtime` workflow and
`packages/console-runtime/README.md` for its existing Auth-bound host.

For this checkout's current, unpublished candidate, run
`bun run prepare:runtime --runtime-root ../lenso-runtime` in each consuming
repository before its frozen install (adjust the checkout-relative path).
This explicitly selects trusted local source bytes, including current edits;
preparation records source hashes as local-candidate provenance rather than
claiming they are the checksum-pinned remote snapshot. The default command
continues to prepare the existing immutable snapshot. Native binaries must
still be built separately from the same selected source. Do not confuse either
preparation mode with npm/crates publication or precompiled distribution.

## Native Rust application integration

Rust plugin Core, business plugins, the optional App adapter and native App
examples belong to [lenso-rust](https://github.com/LioRael/lenso-rust), not this
Runtime workspace. Runtime owns only the kernel, protocol, SDK and SDK examples;
it does not depend on Rust plugin Core.

The existing smoke defaults to this repository's SDK-only examples. To verify
the separate native plugin App, first build both repositories, then explicitly
select its prebuilt binaries:

```sh
LENSO_RUST_ROOT=/path/to/lenso-rust bun run smoke
```

The selected directory must contain `target/debug/logs-batch` and `logs-client`.
The kernel and TypeScript SDK remain this repository's own builds. Smoke uses
the same generated contract for TS→Rust and Rust→TS calls, waits for actual
readiness and owns its process/port cleanup. No sibling directory is discovered,
and no missing selected App is silently replaced with the SDK-only example.

From this repository, set `LENSO_ROOT` and `LENSO_RUST_ROOT` to the prepared native
checkouts and run `bun run verify:lenso`. Build Rust's `logs-client` with its staged
builder first. Verification uses the actual `lenso dev --workspace` CLI, inspect/call,
duplex trace linkage, TS-only invalidation, a failing Rust build retaining the old
instance, compatible candidate cutover, an old surviving stream, incompatible
promotion rejection and owned-port cleanup. It copies Rust sources into an owned
temporary test workspace before injecting failures or changing an implementation.
Only the TS source timestamp is touched and restored if its contents remain
unchanged; developer business source bytes are not edited.

## Execution and security

See [PROTOCOL.md](PROTOCOL.md) for the complete v1 frame and contract boundary.
Unary responses, business errors and stream items are schema-validated. Integers
are restricted to the JavaScript safe range. Optional and nullable are distinct.
No closures, class instances, database handles or transactions cross services.
Binary values are unsupported; use bounded streams or owner-controlled storage
references for files.

- Cross-Service calls always pass through the kernel. Native internal functions
  do not acquire transport authorization, encoding or cancellation semantics.
- Timeout does not mean remote side effects did not finish.
- Cancellation is cooperative, not transaction rollback.
- A TS handler that ignores abort retains its execution slot until its Promise
  and iterator cleanup actually finish. Drain waits for that local work within
  its bound; it does not pretend a cancelled correlation stopped JS execution.
- No automatic retry, reconnect replay or write idempotency is supplied.
- Disconnect and kernel restart fail in-flight work. Re-registration creates a
  new connection generation and does not replay those calls.
- Drain stops admission and bounds remaining execution time.
- Service not found, unready, overloaded, expired, disconnected, invalid
  contract/data and business failure remain distinct.

**Development trust model:** loopback only; every local process is trusted.
Service identity is connection-bound but not credential-verified. An untrusted
local process can impersonate a service. No end-user `principal`, roles or Auth
Actor are trusted from business JSON, and no production identity federation is
implemented. Existing Lenso Auth proofs stay inside their owning TS process.
Business resource authorization must execute at that resource's owner.

Registration is not Console/MCP/agent exposure. Read-only Manage operations must
be explicitly selected, bound to verified request evidence and authorized again
at the server service. There is no generic management invocation bypass.

## Performance

The current three-transport comparison is in
[`examples/multitransport/RUN.md`](examples/multitransport/RUN.md), with real
pre-refactor TCP samples, three alternating original/current TCP runs and all
small/large, serial/concurrent transport samples. Run `bun run perf:transports`
after the release build. Every target uses the same Rust handler and payload;
the caller remains TCP. Source hashes, first-call/startup measurements, separate
stream first/final/EOF times and machine load are recorded.

These are non-isolated local measurements, not a transport ranking or production
SLO. Small-message tail variation and measured microsecond-level overhead remain
visible in the evidence. No iii or legacy ProcessAdapter performance is claimed.

For the three-path source comparison, prepare Lenso's framework packages and build
Runtime's release kernel plus the native Rust release App:

```sh
cargo build --locked -p kernel --release
# From lenso-rust, selecting this Runtime checkout:
sh scripts/build-runtime.sh --runtime-root "$LENSO_RUNTIME_ROOT" --release --out target/release/logs-batch
# From lenso-runtime, with LENSO_ROOT and LENSO_RUST_ROOT explicitly set:
PERF_REQUESTS=1000 PERF_RECORDS=1000 PERF_CONCURRENCY=8 bun run perf
```

`perf` uses the same 1000-record bounded batch and checks every output across
native TS, TS→kernel→TS native App, and TS→kernel→Rust native App. Both remote
implementations use one owned kernel. Output includes per-request p50/p95/p99,
throughput, error rate, measured business time, all process RSS/CPU samples,
32-item slow-consumer streams and overload/settlement diagnostics. Missing
business timings are `null`, not invented zeros. Without `LENSO_ROOT`, the
standalone SDK comparison remains available, but remote TS is explicitly unmeasured.
The contract caps batches at 1000; this aggregation is still cheap and transport
cost dominates. It is **not evidence of a general Rust speed advantage**.
Pure business timers exclude transport and Rust input conversion; their structured
stderr logging still contributes to remote end-to-end cost. Output equality checks
are outside latency samples but inside throughput. No side-effectful replay or
production data upload is performed.

Historical TCP-only source-checkout run before the multi-transport boundary:
Apple M2 Pro, macOS/Darwin 27.0.0 arm64, Bun 1.4.3,
Rust 1.101.0-nightly, optimized native binaries, no external telemetry. No other
agent build/test was running during this sample; the machine was not isolated.
Each path completed 1000 requests with 1000 records (60915 input bytes), concurrency
8, identical output and zero errors:

| Path | Business p50 (ms) | End-to-end p50 / p95 (ms) | Requests/s |
| --- | ---: | ---: | ---: |
| Native TS | 0.001834 | 0.015958 / 0.037791 | 332157 |
| Runtime → TS | 0.004875 | 8.520958 / 9.045000 | 931 |
| Runtime → Rust | 0.013250 | 8.691875 / 9.214709 | 916 |

Business telemetry contained all 1000 measured samples for each remote path.
RSS snapshots with all processes alive: TS service 63664 KiB after its phase;
Rust service 5168 KiB, kernel 8528 KiB and caller 60896 KiB after the Rust phase.
These are not independent per-path peaks. A 1000-item stream completed in
100.46 ms; 32 items consumed with 10 ms pauses took 394.12 ms. All 192 burst
stream requests returned `overloaded`; that admission result is not a kernel
queue-throughput measurement. Cleanup settled the Runtime registry at zero calls.

The actual CLI workspace verification observed cached startup 1.78 s, TS source
invalidation 0.37 s, failed Rust build notification 0.41 s with the old instance
still callable, and compatible Rust rebuild/ready/cutover 1.48 s. An earlier
cold/staging run took about 10 s to start. These are single-run feedback evidence,
not service-level promises; Cargo cache state changes them substantially.

Samples are evidence, not a mathematical memory proof; protocol bounds and
fault tests provide the other part of that argument. Native measurements include
an async `await` harness. CPU% from `ps` is lifetime sampling, not isolated CPU
utilization. Do not read batch time divided by count as request latency.

Cross-machine execution is not measured. iii is not installed or benchmarked in
this checkout, and no speed comparison is claimed. For a fair external run:

1. Pin iii and Lenso commits, Rust/Bun versions, hardware and telemetry settings.
2. Use the same 32 records, aggregation, concurrency and duration.
3. Allow iii to put the whole app in one Worker and call internal functions;
   compare that with Lenso native calls separately from transport paths.
4. For transport comparison use one TS caller, one Rust service and one engine
   for each project; measure from client invocation to decoded result.
5. Record latency distributions, throughput, errors, all process CPU/RSS, then
   stress overload and a slow consumer without an unbounded producer queue.

The official iii
[`ws_roundtrip_bench.rs`](https://github.com/iii-hq/iii/blob/main/engine/benches/ws_roundtrip_bench.rs)
times WebSocket send through matching decoded response, including JSON encoding
and decoding. Its engine and echo worker are Tokio tasks in one benchmark process,
not independently running TS/Rust SDK services. Engine startup, function
registration and readiness probing happen outside Criterion's timed iteration.
That benchmark and internal engine microbenchmarks cannot be substituted for a
cross-language end-to-end result.

## Not delivered as production capability

No high availability, persistent queues/workflows, exactly-once, distributed
transactions, TLS, remote listening or production service/user credentials.
No Wasm kernel. Pure TS Cloudflare Workers remain independent; connecting a
Worker to an external native runtime would add network and deployment costs.

The kernel is designed as an independent executable, but signed/precompiled
platform artifacts, an automatic binary installer, npm/crates publication and
platform certification are **not completed**. TS-only consumers should eventually
use matching precompiled binaries without Cargo; this source delivery still
requires Cargo to create them. No deployment or online resources are changed.
