# Run the smallest real proof

From this checkout, use the manifest's toolchain and frozen dependency state:

```sh
bun install --frozen-lockfile
bun run build:sdk
bun run typecheck
cargo build --workspace --locked
bun run smoke
```

`smoke` owns a loopback kernel and SDK example processes and checks generated calls, stream progress, cancellation, deadlines and connection loss. For a transport change, additionally run `bun run smoke:transports`; existing mixed transport smoke covers TCP, stdio, WebSocket and cooperative cancellation/backpressure. For a native-only edit, these transport checks are unnecessary.

Generator changes should generate into an owned temporary directory first, compare intended outputs and compile them. Use the existing schema cases instead of inventing a second validator. Never overwrite developer sources or edit generated output as the source of truth.

## Explicit native candidate integration

For a TS consumer source checkout, its `scripts/prepare-runtime.ts` chooses an immutable snapshot by default. `--runtime-root /absolute/trusted/runtime` deliberately selects local bytes and records candidate provenance. Prepare before frozen install; package exports resolve to built `dist`. A candidate adding SDK dependencies can require a root-lock update by the consumer integration owner. If frozen install rejects it, report that gap or return to the pinned preparation; do not bypass the lock for a documentation example. Do not use paths into `sdk/typescript/index.ts` as consumer imports.

Runtime owns `scripts/local.ts debug|release` for kernel source/binary proofs; Rust owns `scripts/build-runtime.sh --runtime-root ...` for staged native binaries without rewriting its root Cargo.lock. Select `LENSO_ROOT`, `LENSO_RUST_ROOT` and `LENSO_RUNTIME_ROOT` explicitly when the corresponding cross-repo script requires them. Read the README's matching command sequence before `verify:lenso`; SDK-only smoke is not proof of native App integration.

A source proof checks the exact selected source, SDK and binary bytes. It is not a release, signed attestation, install permission, benchmark or production SLO. Report toolchain, revision, commands, observed results and unrun boundaries. Do not rerun performance suites for a documentation/example change.

Baseline `765db7a` includes all three transports, cancellation and bounded stream credit. Go/Python SDK work and source-first authoring may arrive in separate commits; refresh this evidence only after inspecting their actual delivery and running its specific example. A branch name, README promise or shared version string is insufficient.
