# SDK error consistency candidate

Verified on Darwin arm64 with Rust/Cargo 1.99.0, Bun 1.4.3, Node 26.10.0,
Python 3.14.8 and Go 1.27.1. Runtime base and remotely rechecked main:
`0b45e1342ab38cc71a03de978c4a98e46d01472a`. Read-only Go SDK reference:
`LioRael/lenso-go@beba2222f7b73828c060b9aaee5c9439230e3c34`.
No source or CLI files in that checkout were changed.

## Reproduced differences and fix

Go already rejects unknown error codes and messages over 1024 UTF-8 bytes.
Python already rejects unknown codes but accepted oversized messages. TS and
Rust accepted both. Against synthetic TCP peers before the fix, TS exposed
`unknown`/`business`; Rust left the other pending route alive (the bounded test
expired); Python reported `invalid_output` after business-schema validation
instead of rejecting the malformed envelope as `protocol_error`.

The fix validates incoming errors before delivering any message/details. Rust
reuses the existing protocol validator; TS reuses its existing error-code set;
Python adds the missing byte bound. No public signature, valid business error,
wire field, retry, deadline or cancellation behavior changes. Invalid peer
messages/details are replaced with a generic `protocol_error`. Regression tests
cover both concurrent pending calls, 1024/1026-byte Unicode boundaries, and
ID-less validation (Rust/Python). Existing handshake/receive paths share the
same validators. Healthy calls remain usable after ordinary terminal errors.

## Actual real-kernel evidence

`scripts/sdk-error-probes/run.py` owns a kernel bound to port zero and a synthetic
Python service, then invokes each installed SDK through its public exports. Go
is selected by an explicit checkout path; it is not copied into Runtime.

| Case | TS | Rust | Go | Python |
| --- | --- | --- | --- | --- |
| Requested contract version absent | contract_mismatch | contract_mismatch | contract_mismatch | contract_mismatch |
| Requested method absent on registered service | contract_mismatch | contract_mismatch | contract_mismatch | contract_mismatch |
| Protocol version rejected at handshake | protocol_error | protocol_error | protocol_error | protocol_error |
| Slow call exceeds timeout | deadline_exceeded | deadline_exceeded | deadline_exceeded | deadline_exceeded |
| In-flight explicit cancellation | cancelled | cancelled | cancelled | cancelled |
| Successful call after timeout/cancellation | passed | passed | passed | passed |

Protocol rejection uses an owned loopback proxy that changes only the hello
version to 2 and forwards the **actual kernel error** verbatim. Malformed error
code/message regression tests use adversarial peers separately: a healthy kernel
already prevents these envelopes. All data is synthetic. Go also preserves
`errors.Is` for context cancellation/deadline. Python explicit cancellation uses
its existing event option; native asyncio task cancellation remains
`asyncio.CancelledError`, as covered by its existing suite.

## Reproduction and results

With the listed toolchains on PATH, from this Runtime checkout:

```sh
bun install --frozen-lockfile
bun run build:sdk
bun run typecheck
CARGO_BUILD_JOBS=2 cargo build --workspace --locked --offline
CARGO_BUILD_JOBS=2 cargo test --workspace --locked --offline
bun test ./sdk/typescript
PYTHONPATH=sdk/python/src python3 -m unittest discover -s sdk/python/tests
PYTHONPATH=sdk/python/src CARGO_BUILD_JOBS=2 python3 scripts/sdk-error-probes/run.py --go-root /absolute/path/to/lenso-go
```

All executed successfully. Cargo: 96 tests passed, plus the stdio process harness
and doc tests. TS: 60 passed. Python: 41 passed, 19 optional WebSocket tests skipped
because `websockets` was unavailable. Four-language probe: all table entries
passed. The probe additionally runs `go test ./sdk/runtime -count=1` with its
owned kernel address, so the existing Go real-kernel test does not skip.
`git diff --check` and TypeScript typecheck passed. Rust build/test used two jobs
and the existing local dependency cache; no lockfile changes were required.

Scope is SDK receive validation, tests, probe fixtures and protocol/evidence docs.
No kernel routing changes, Go implementation changes, other task workspace edits,
account/credential operations, paid model calls, push/merge, publication or
release occurred. Four-language probes certify local TCP only. Python WebSocket,
other OS/architectures, registry artifacts and native application/CLI integration
were not verified. Timeout/cancellation still do not imply rollback of effects.
