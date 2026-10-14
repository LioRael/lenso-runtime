# Transport example and measurements

From the repository root, after the transport kernel and SDK changes are integrated:

```sh
bun install --frozen-lockfile
TMPDIR=/tmp cargo build --release --workspace
bun run build:sdk
bun scripts/transport-smoke.ts
bun scripts/transport-perf.ts --output examples/multitransport/results.json
```

The benchmark uses the same Rust `logs-batch` aggregation and progress handlers for all targets. The caller is always TCP. Targets are TCP, kernel-owned stdio, and WebSocket. Each target gets 50 warmup requests per payload, then 300 measured requests at concurrency 1 and 8 for 2 and 1000 log records. `TRANSPORT_REQUESTS` overrides the measured count; the output marks when it differs from the baseline.

Every aggregate result and stream chunk is checked. Raw latency samples and stream arrival times are retained in JSON. Startup, first call per payload, first stream chunk, final expected chunk, and iterator EOF are separate fields. The first larger-payload call is not a cold-process measurement. Startup uses different launch orchestration from the old baseline, so it is not an isolated transport comparison.

`baseline.json` is actual pre-refactor TCP evidence from revision `cc4a5af938e534b34371e4a9771190cade601a51`, captured on Darwin 27 arm64, Apple M2 Pro, Bun 1.4.3 and Rust 1.101.0-nightly. It includes source SHA-256 hashes, all 1200 unary latency samples, and all 1000 stream arrival times. All measured unary requests succeeded. The original build command required `TMPDIR=/tmp` because the default temporary path exceeded the macOS Unix socket path limit in the local compiler cache wrapper. The original SDK build initially failed due to missing installed Bun types; `bun install --frozen-lockfile` fixed dependency availability. The baseline was then run using the unmodified release kernel and Rust service.

The current script rejects `--baseline` to prevent replacing that evidence with post-refactor timings. No iii or legacy runtime was benchmarked.

The independent Bun service entry points are `tcp-service.ts` and `websocket-service.ts`. Smoke starts a kernel-owned Rust stdio service and an additional owned TCP fixture, plus standalone TCP/WebSocket Bun services. The TCP Bun service uses the same generated client to call both Rust stdio and Bun WebSocket, with trace/parent linkage checked in real kernel records. No transport selection enters those business calls. Smoke also checks stream cancellation, deadline, slow consumption, overload, disconnect/no replay, spawn failure, early exit, ready timeout, group cleanup, and Rust stdio exit after abrupt Runtime loss.

The Rust WebSocket benchmark target is a standalone process started by the benchmark, with `LENSO_RUNTIME_TRANSPORT=websocket` and `LENSO_RUNTIME_URL`. Its shutdown is owned by the script; TCP and stdio targets are owned by the kernel configuration. The benchmark checks that the target's status transport matches the measured label before running requests.

## Observed results and limits

The integrated build, Rust workspace tests, Clippy with warnings denied, TypeScript typecheck, SDK tests and both smoke commands passed on Darwin 27 arm64. Rust workspace tests cover 64 cases plus a real stdio subprocess harness; TS covers 31 cases, including a real 5-second stalled-writer deadline and oversized fragmented WebSocket input. The separate native Rust adapter's five unchanged lifecycle tests also passed against the current SDK without editing its checkout. The available external TS checkout had no Runtime adapter, so native TS integration was not run.

`results.json` holds all 12 conditions, startup/first-call/stream fields, source hashes and load averages. The caller is TCP and the target is the same release Rust handler for every row. A representative 1000-record (60915-byte input), concurrency-8 sample:

| Target | p50 / p95 / p99 (ms) | Requests/s | Errors |
| --- | ---: | ---: | ---: |
| Original TCP, initial baseline | 8.505 / 8.972 / 9.088 | 928 | 0 |
| Current TCP | 8.404 / 8.803 / 9.001 | 943 | 0 |
| Current stdio | 8.469 / 8.816 / 9.531 | 939 | 0 |
| Current WebSocket | 8.961 / 11.213 / 13.186 | 865 | 0 |

This sample's 1-minute load was about 6.0 on 10 logical CPUs; the machine was not isolated. An earlier round had strongly inflated tails across all transports while a subsequent system check reported load 11.7. Its raw samples are retained as `previous_unisolated_run`, not silently replaced. Do not infer a general transport ranking from sequential runs.

TCP diagnosis retained single-frame writes and the original Nagle policy, removed complete-frame copying in TS, and replaced per-write JS timers with one bounded deadline watcher. Rust sends the codec's owned serialized buffer without copying/reframing it; WebSocket removes the compatibility delimiter before sending one text message. Kernel read/write arbitration is again frame-level with the original outbound priority, rather than letting a buffered read loop monopolize the task. The transport owns the write deadline, avoiding nested kernel timeouts.

`tcp-regression-check.json` preserves three alternating original/current runs and the pre-optimization rounds. Final paired-run median ratios (current/original) are: small serial p50 0.939, small concurrent p50 0.971 and throughput 1.069, large concurrent p50 0.993 and throughput 1.016. Small concurrent p99 is 1.115 and tails remain variable. No sustained median/throughput regression was observed after the targeted fixes, but these measurements are not a no-regression guarantee or production SLO.

To reproduce the actual original comparison, first build the archived revision in the ignored fixture:

```sh
mkdir -p target/tcp-before-source
git archive cc4a5af938e534b34371e4a9771190cade601a51 | tar -x -C target/tcp-before-source
TMPDIR=/tmp cargo build --manifest-path target/tcp-before-source/Cargo.toml --workspace --release --locked
bun scripts/transport-tcp-check.ts
```

The reference runner verifies the preserved source hashes and uses the original Rust kernel, Rust SDK/service and TS RuntimeHost, not a native plugin labelled “process”. `tcp-policy-check.json` is an earlier exploratory socket-policy sample, not the final build.

Linux and Windows were not run. Non-Unix Rust stdio still has the documented blocking-stream limitation; Unix process groups cannot contain descendants that escape their group. Static `--processes` launch is delivered, but dynamic stdio candidate creation/attachment to the external dev supervisor is not. Connected instances use the existing promotion/generation semantics. Remote production/WSS and raw-data channels remain unsupported/design-only as described in `PROTOCOL.md`. The legacy checkout was unavailable; neither its ProcessAdapter nor iii was run.
