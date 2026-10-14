# Transport example and measurements

From the repository root, after the transport kernel and SDK changes are integrated:

```sh
bun install --frozen-lockfile
bun run build:sdk
TMPDIR=/tmp bun scripts/transport-build.ts
bun scripts/transport-smoke.ts
# Only after all other builds/tests stop; use a new output:
bun scripts/transport-perf.ts --repeat 3 --output examples/multitransport/current-v2.json
```

The benchmark uses the same Rust `logs-batch` aggregation and progress handlers for all targets. The caller is always TCP. Targets are TCP, kernel-owned stdio, and WebSocket. New methodology `transport-v2-independent-warmup` gives each payload/concurrency condition its own 50 warmup requests, then 300 measured requests at concurrency 1 and 8 for 2 and 1000 log records. Warmup runs at the concurrency being measured. `TRANSPORT_REQUESTS` overrides the measured count (1..100000). `--repeat` accepts 2..6 rounds, default 3. Orders rotate; with three targets each takes each position once per complete three-round cycle. `--transport tcp|stdio|websocket` restricts targets.

Every aggregate result and stream chunk is checked. Latency excludes only its own request's trailing assertion; another request's assertions can delay work on the shared JavaScript event loop. Throughput includes validation. There is no validation-disabled canonical run. Raw latency samples and stream arrival times are retained in content-addressed JSON under ignored `.perf-artifacts/`. No prior report is embedded in a new raw report. `--output` optionally writes a compact <=32KiB summary inside `examples/multitransport/`; traversal and symlink redirection are rejected, and `baseline.json` is protected. Without `--output`, only ignored raw data and a stdout summary are produced.

If a larger repeat set exceeds the repository summary size cap, its raw artifact is still preserved and the runner reports its location without overwriting the requested summary. Use fewer rounds or a single target for a smaller repository summary; stdout-only summaries are not subject to this file-size cap.

Startup, first call per payload, first stream chunk, final expected chunk, and iterator EOF are separate fields. The first larger-payload call is not a cold-process measurement. Startup uses different launch orchestration from the old baseline, so it is not an isolated transport comparison. Load averages are recorded per condition and run. p99 from 300 requests is descriptive, not a stable SLO.

`baseline.json` is actual pre-refactor TCP evidence from revision `cc4a5af938e534b34371e4a9771190cade601a51`, captured on Darwin 27 arm64, Apple M2 Pro, Bun 1.4.3 and Rust 1.101.0-nightly. It includes source SHA-256 hashes and counts for all 1200 unary latency samples and 1000 stream arrival times; raw observations are archived separately. All measured unary requests succeeded. The original build command required `TMPDIR=/tmp` because the default temporary path exceeded the macOS Unix socket path limit in the local compiler cache wrapper. The original SDK build initially failed due to missing installed Bun types; `bun install --frozen-lockfile` fixed dependency availability. The baseline was then run using the unmodified release kernel and Rust service.

The four historical reports now retain compact metrics and observation counts, not raw arrays. Their numbers were not remeasured or changed. Historical v1 warmed serial requests only, then measured serial/concurrent conditions in fixed target order. Do not pool it with v2 or use the old baseline for new-methodology speed ratios. Prior experiments are compact `historical_cohorts`; exact originals, including their nested reports, remain recoverable separately.

The current script rejects `--baseline` to prevent replacing that evidence with post-refactor timings. No iii or legacy runtime was benchmarked.

The independent Bun service entry points are `tcp-service.ts` and `websocket-service.ts`. Smoke starts a kernel-owned Rust stdio service and an additional owned TCP fixture, plus standalone TCP/WebSocket Bun services. The TCP Bun service uses the same generated client to call both Rust stdio and Bun WebSocket, with trace/parent linkage checked in real kernel records. No transport selection enters those business calls. Smoke also checks stream cancellation, deadline, slow consumption, overload, disconnect/no replay, spawn failure, early exit, ready timeout, group cleanup, and Rust stdio exit after abrupt Runtime loss.

The Rust WebSocket benchmark target is a standalone process started by the benchmark, with `LENSO_RUNTIME_TRANSPORT=websocket` and `LENSO_RUNTIME_URL`. Its shutdown is owned by the script; TCP and stdio targets are owned by the kernel configuration. The benchmark checks that the target's status transport matches the measured label before running requests.

## Observed results and limits

Final evidence was captured on 2026-10-14 after implementations 001–009 were independently approved. Reviewers reported the full functional gate passed: 91 Rust workspace tests plus the real stdio subprocess harness, Clippy with warnings denied, 55 SDK tests, TypeScript typecheck, original smoke (17) and mixed smoke (12). The final evidence executor separately passed both release proof builds, 15 evidence-helper tests, all 55 SDK tests, typecheck and migration `--verify`, then stopped builds/tests. It did not repeat the reviewers' Rust/Clippy/smoke gates or run external native applications.

`final-transports-v2.json` (25,558 bytes) records exactly three balanced target orders: TCP/stdio/WebSocket, stdio/WebSocket/TCP, WebSocket/TCP/stdio. Each condition has 50 independent warmup requests and 300 measured requests per round, with every result checked. Rows below are medians of the three per-round statistics, **not pooled request percentiles**. The two payloads are 125 and 60,915 input bytes. All 10,800 measured unary requests and 9,000 stream chunks passed validation.

| Target | Records | Concurrency | p50 / p95 / p99 (ms) | Requests/s | Errors |
| --- | ---: | ---: | ---: | ---: | ---: |
| TCP | 2 | 1 | 0.126 / 0.239 / 0.271 | 6,778 | 0 |
| TCP | 2 | 8 | 0.244 / 0.388 / 0.786 | 29,273 | 0 |
| TCP | 1000 | 1 | 2.851 / 3.324 / 3.504 | 343 | 0 |
| TCP | 1000 | 8 | 8.596 / 9.334 / 9.829 | 921 | 0 |
| stdio | 2 | 1 | 0.090 / 0.134 / 0.175 | 10,369 | 0 |
| stdio | 2 | 8 | 0.259 / 0.407 / 0.753 | 28,009 | 0 |
| stdio | 1000 | 1 | 2.903 / 3.409 / 3.951 | 333 | 0 |
| stdio | 1000 | 8 | 8.938 / 10.053 / 10.855 | 870 | 0 |
| WebSocket | 2 | 1 | 0.118 / 0.159 / 0.196 | 7,952 | 0 |
| WebSocket | 2 | 8 | 0.245 / 0.374 / 0.739 | 28,701 | 0 |
| WebSocket | 1000 | 1 | 3.009 / 3.735 / 4.837 | 313 | 0 |
| WebSocket | 1000 | 8 | 8.701 / 9.266 / 9.470 | 911 | 0 |

Separate startup/first-call and stream times, also medians across rounds:

| Target | Startup (ms) | First call, 2 / 1000 records (ms) | Stream first / final chunk / EOF (ms) |
| --- | ---: | ---: | ---: |
| TCP | 13.124 | 0.208 / 3.425 | 3.307 / 98.831 / 98.982 |
| stdio | 13.176 | 0.194 / 3.391 | 2.915 / 96.552 / 96.741 |
| WebSocket | 24.292 | 0.234 / 3.104 | 2.982 / 116.401 / 116.540 |

`final-tcp-v2.json` (24,847 bytes) is the separate three-round paired comparison against **`9d0ff1d0a433c18fbc3e791149e8c3f29a3b0b11`**, not the historical baseline. Orders are reference/current, current/reference, reference/current; with three rounds their positions are not perfectly balanced. All 7,200 unary requests and 6,000 stream chunks passed. Each cell below is the current/reference paired-ratio median [minimum, maximum]. `I` means inconclusive under the actual rule `abs(median - 1) <= maximum - minimum`; `D` means descriptive difference only, not statistical significance.

| Records / concurrency | p50 ratio | p95 ratio | p99 ratio | Throughput ratio |
| --- | ---: | ---: | ---: | ---: |
| 2 / 1 | 0.855 [0.759, 0.858] D | 0.816 [0.657, 0.926] I | 0.749 [0.659, 0.947] I | 1.172 [1.129, 1.359] I |
| 2 / 8 | 0.896 [0.828, 0.930] D | 0.892 [0.874, 0.915] D | 0.936 [0.885, 0.979] I | 1.108 [1.103, 1.175] D |
| 1000 / 1 | 0.927 [0.925, 0.935] D | 0.928 [0.901, 0.955] D | 0.874 [0.804, 1.100] I | 1.079 [1.065, 1.090] D |
| 1000 / 8 | 1.035 [0.965, 1.041] I | 1.100 [1.078, 1.195] I | 1.579 [0.856, 1.923] I | 0.964 [0.926, 1.017] I |

Paired reference/current startup medians were 24.279/12.855 ms; first calls were 0.276/0.242 ms (2 records) and 3.634/3.188 ms (1000). Stream first/final/EOF medians were 3.184/107.279/107.342 ms for reference and 2.935/106.187/106.334 ms for current. Startup orchestration differs and is not compared as a speed ratio. Full per-round metrics, counts and load snapshots remain in both summaries.

Environment: Apple M2 Pro, 10 logical CPUs, Darwin 27.0.0 arm64, Bun 1.4.3, Cargo 1.101.0-nightly (`29c5daa1a`), Rustc 1.101.0-nightly (`e9bbadf68`), native release target `aarch64-apple-darwin`. Owned ordinary targets were used, with Cargo's existing local compiler cache (reference 81 hits; current 74 hits/7 misses); these are not cold build times. No competing agent build/test was authorized, but the machine was **not isolated**. Condition-level 1-minute loads ranged 4.241–4.745 in the transport cohort and 4.745–5.018 in the paired cohort.

The same validated handler/payload and inherited stderr policy were used throughout; caller transport was always TCP. Cleanup of each standalone WebSocket target logged a reset without closing handshake after successful measurement; measured request/stream errors were zero. Tails were variable: stdio large/concurrent per-round p99 ranged 9.267–75.967 ms; paired current large/concurrent p99 ranged 11.743–17.274 ms. The large/concurrent TCP ratios do not establish either a gain or no-regression guarantee. Three rounds and 300 correlated requests per condition cannot establish statistical certainty or a production SLO. No iii, legacy adapter, Linux, Windows or remote/WSS comparison was run.

The earlier `results.json`, `tcp-policy-check.json` and `tcp-regression-check.json` remain unchanged historical v1 cohorts; they are not used for these ratios.

To reproduce the final comparison, select the pre-optimization landed commit `9d0ff1d0a433c18fbc3e791149e8c3f29a3b0b11`. Run only after all source changes/builds/tests have stopped, in a disposable checkout with an ordinary owned `target` directory:

```sh
# Refuse shared target symlinks or an existing archive; never overlay revisions.
test ! -L target && test ! -e target/tcp-before-source || exit 1
mkdir -p target
mkdir target/tcp-before-source
git archive --output=target/tcp-reference.tar 9d0ff1d0a433c18fbc3e791149e8c3f29a3b0b11
tar -xf target/tcp-reference.tar -C target/tcp-before-source
bun install --frozen-lockfile
(cd target/tcp-before-source && bun install --frozen-lockfile --ignore-scripts)
bun --no-install -e 'const { RuntimeHost } = await import("./target/tcp-before-source/sdk/typescript/index.ts"); if (typeof RuntimeHost.connect !== "function") throw new Error("Missing archived RuntimeHost"); console.log("Archived RuntimeHost import passed");'
bun run build:sdk
TMPDIR=/tmp bun scripts/transport-build.ts target/tcp-before-source
TMPDIR=/tmp bun scripts/transport-build.ts
# Final lightweight gates (full Rust/Clippy/smokes were independently reviewed):
bun test ./scripts/benchmark-evidence.test.ts
bun test ./sdk/typescript
bun run typecheck
bun scripts/benchmark-migration.ts --verify
# Only after all builds/tests stop:
TRANSPORT_REQUESTS=300 bun scripts/transport-perf.ts \
  --repeat 3 --output examples/multitransport/final-transports-v2.json
TRANSPORT_REQUESTS=300 bun --no-install scripts/transport-tcp-check.ts \
  --reference-revision 9d0ff1d0a433c18fbc3e791149e8c3f29a3b0b11 \
  --repeat 3 --output examples/multitransport/final-tcp-v2.json
```

`--reference-revision` requires one full lowercase 40-character commit SHA. Without it, the reference remains `cc4a5af938e534b34371e4a9771190cade601a51` from `baseline.json`: to rerun that historical cohort, archive that SHA into a fresh `target/tcp-before-source`, build both roots as above, and omit the flag. The allowlisted fixture path is unchanged. Missing Git objects, a wrong-revision fixture, or absent/stale build proofs block the comparison; they are not repaired by the runner.

Install dependencies in both roots with their own frozen lockfiles, without changing manifests, lockfiles or source. Reference installation uses `--ignore-scripts`; no archived lifecycle script needs to execute. The archived `9d0ff1d…` TS RuntimeHost imports `websocket.ts`, which resolves `ws/package.json` relative to the archived SDK. A current-root install alone does not provide that archive-local dependency under isolated workspace linking. The source-import preflight above checks dependency resolution without connecting a host, starting services or bypassing build proofs. Use `--no-install` for the preflight and comparison so Bun cannot satisfy missing packages through auto-install/cache resolution. The runner imports the archived source entry directly; missing reference dependencies fail the import, with no fallback to the current SDK and no proof bypass. No reference SDK distribution build is required for this source import.

The reference runner compares the entire source/application inventory used by the proof helpers with actual Git blobs from the selected commit, including the reference TS RuntimeHost and Rust service. Missing, extra or modified inventoried source is rejected. For the historical cohort it additionally checks the preserved baseline source hashes; `baseline.json` is never changed. Reports label `reference_revision` and resolved `reference_commit` with the selected commit and carry that fixture's verified source hashes, not hashes borrowed from another cohort. The per-run path `original` means the selected reference, not necessarily `cc4a5af`.

Both sides require equivalent release source-to-build proof verification before launch and after measurement. Only `transport-build.ts` produces real proofs after a successful locked Cargo build reporting both expected executables and unchanged inventories; do not post-hoc certify existing binaries. The runner uses the selected reference Rust kernel, Rust SDK/service and TS RuntimeHost, not a native plugin labelled “process”. Both sides use the same current measurement harness and generated client. It reports individual paired current/reference ratios, median and range dispersion for each condition. A median's distance from 1 no greater than its observed ratio range is labelled `inconclusive`; a larger difference is only descriptive, not statistical significance. Startup is not compared. `tcp-policy-check.json` is an earlier exploratory socket-policy sample, not the final build.

## Evidence preservation and recovery

Before replacement, all four raw files were copied byte-for-byte to `.perf-artifacts/migration/<sha256>.json`. Byte counts and SHA-256 hashes were verified against those copies and committed `9d0ff1d0a433c18fbc3e791149e8c3f29a3b0b11` Git blobs. Local `manifest.json` is also ignored. Ignored files are not guaranteed to replicate between worktrees; this document does not claim archives exist in another participant's checkout.

Each tracked summary's `artifact` records original byte/line counts, SHA-256, Git revision/blob and exact `git cat-file blob <blob>` recovery command. The same four identities are in `scripts/benchmark-migration.ts`. For example, recover the original baseline without replacing its summary:

```sh
mkdir -p .perf-artifacts/migration
git cat-file blob df347492d572f9b7442b2d4d98679a9acb4746ba > .perf-artifacts/migration/0e8c44ab4bd31f3646e65bffb582373af2a7b37dacbba9b02c84b5ada1195136.json
shasum -a 256 .perf-artifacts/migration/0e8c44ab4bd31f3646e65bffb582373af2a7b37dacbba9b02c84b5ada1195136.json
bun scripts/benchmark-migration.ts --verify
```

Verification retrieves all four Git originals, verifies hashes/sizes, checks every retained metric/count, and verifies local archives when present. It does not run services or change summaries. Acceptance in another worktree requires verifying these Git objects there; private local archives alone are not a durable handoff. A shallow checkout lacking the objects must obtain the historical commit before recovery. Unit tests use small synthetic fixtures and do not require archives or Git history.

## Source and build provenance

`transport-build.ts` runs `cargo build --workspace --release --locked` and records proofs after a successful build and unchanged source manifests. It reuses landed `candidateFiles`, `recordBinary` and `verifyBinary` definitions/format from `scripts/local.ts`, not a parallel binary-proof format. Optional root/application-source parameters allow archived-reference verification. Existing local launch behavior is unchanged.

Both current and reference kernel and `logs-batch` require source/binary proofs. Runtime/application manifests include Rust service source, workspace inputs and lockfiles. The harness manifest includes scripts, generated client, example/config authoring files, baseline and generated SDK distribution. Before/after verification rejects stale binaries, changed source/harness/client or altered owned-process configuration. Raw reports retain full manifests; summaries retain manifest hashes, counts where available, executable hashes and build times. Executable hashes alone identify artifacts, not source correspondence.

The final current candidate is uncommitted source over checkout HEAD `cc4a5af938e534b34371e4a9771190cade601a51`; the report's `revision` is that HEAD, **not** a claim that its Git tree equals the optimized candidate. Its verified source-manifest SHA-256 is `a312987012d62b415dbd0ca9ea4c07fdf2aa0381d91ac6ba48de51b9678e10c9` and application-source SHA-256 is `ca51ae7420347eb2eb2500af4bffa649415f587643fda68ad8ca09e00b82b509`. Reference source/application hashes are `5667cd1916d7edf5f51e9e149a86c5bcf5cb2515306254d5c005ed1128cbed7e` / `b2bcb95973c11072f0a05a54ddd387c431d0188843e8f2c1a1e1028362e6d158`. The reference inventory's 37 unique files matched actual `9d0ff1d…` Git blobs exactly; archived/current `RuntimeHost` classes were distinct. The unchanged Rust business service source hash on both sides was `491a0212a9134904da7cfa909fd8cb3e702f61921b4f2b4621e30480bd84bca4`.

| Release executable | Current SHA-256 | Reference SHA-256 |
| --- | --- | --- |
| `lenso-runtime` | `41237a03b2ce958937bba242d76650c448dab7c17f958edb020a6df3afa0c906` | `87ba477d16bf7f87da47c17c2800f7b50b5a06fd7f13a43ede4d316e31c6fdcf` |
| `logs-batch` | `e0eb3c300d38141fee9ca5f5dd45defc1eed2202dff6d9fecf6d1a5efd329e38` | `e2ee51c68a05a5ac901ef945ae450a101c1f1ae2736b0240dd1f957b229cc454` |

Both exact documented build-helper commands completed locked native release builds, validated Cargo's JSON executable paths and recorded proofs, without post-hoc certification. Both measurement commands verified source/application/binary proofs before and after. Transport and paired harness hashes differ because the newly written transport summary was included in the paired harness inventory; the measurement source/client/SDK distribution did not change between commands. Documentation was updated only after measurement.

### Final raw handoff

The two complete reports (including full manifests, launch configurations and observations) are hash-linked from the summaries:

| Report | Raw bytes | SHA-256 / ignored destination filename |
| --- | ---: | --- |
| Three transports | 667,482 | `095495c501ef8ec8747b1d9d8a480f6e6c01aa42323f9596969ea4656e8069d3.json` |
| Paired TCP | 466,277 | `2db393fef8cc58ea9b19c96176c8cab7bf9790fb12cbdb3d7d96b3b1a3a82062.json` |

The parent workspace accepted the exact-byte reports, preflight proof inventories and stderr logs after verifying SHA-256 and byte counts before and after copying to ignored `.perf-artifacts/`. Its archive manifest is `.perf-artifacts/final-2026-10-14/handoff-manifest.json`. The temporary handoff directories and implementation plans were then removed. These archives remain local to that accepted checkout, not automatically VFS-replicated or included in a fresh clone. Do not commit raw artifacts or assume another checkout contains them.

Synthetic helper validation is separate from live measurement:

```sh
bun run build:sdk && bun run typecheck
bun test ./scripts/benchmark-evidence.test.ts
bun test ./sdk/typescript
```

CI could later retain ignored `.perf-artifacts/` as an explicitly requested, access-controlled artifact with a documented expiry. Keep hash-linked summaries and recovery metadata with the review. This change does not modify the existing check workflow, launch CI, upload data or configure retention.

Linux and Windows were not run. Non-Unix Rust stdio still has the documented blocking-stream limitation; Unix process groups cannot contain descendants that escape their group. Static `--processes` launch is delivered, but dynamic stdio candidate creation/attachment to the external dev supervisor is not. Connected instances use the existing promotion/generation semantics. Remote production/WSS and raw-data channels remain unsupported/design-only as described in `PROTOCOL.md`. The legacy checkout was unavailable; neither its ProcessAdapter nor iii was run.
