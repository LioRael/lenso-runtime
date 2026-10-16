# Precompiled kernel distribution

Ordinary TS, Go and Python applications need their own language tools and SDK,
plus the precompiled kernel. They do not need Cargo or rustc. Developers changing
the kernel build this repository; developers changing a Rust service build that
service. SDK releases remain independent of kernel releases.

The matching Rust CLI is `LioRael/lenso-cli` (`lenso-unified`), not
`lenso-cli-legacy`. [Runtime v0.1.0](https://github.com/LioRael/lenso-runtime/releases/tag/v0.1.0)
is public; the matching [CLI v0.1.0](https://github.com/LioRael/lenso-cli/releases/tag/v0.1.0)
is published in the private CLI repository and requires repository access. With
the official CLI installed:

```sh
lenso-unified runtime use v0.1.0
lenso-unified runtime current --json
lenso-unified runtime run -- --listen 127.0.0.1:7331
```

`runtime install VERSION` caches without selecting; `use VERSION` installs if
needed and atomically selects. `list` shows cache receipts; `path` prints the
resolved executable for application/process-manager configuration. Current/path/
run resolve `LENSO_RUNTIME_BIN`, then `LENSO_RUNTIME_VERSION`, then selected cache,
then the pinned default v0.1.0. A missing default asset reports failure: it never
invokes Cargo. Cached binaries are checked locally on resolution, not downloaded
again at startup. Existing processes continue using their selected executable
when the default changes. No automatic update, retry, or process restart occurs.

For source development, explicitly set `LENSO_RUNTIME_BIN` to the binary you
built (for example this checkout's `target/debug/lenso-runtime`). Local overrides
are labelled unverified and do not mutate the selected release.

## Release contract and platform boundaries

Each repository independently versions its CLI or kernel. Runtime assets are raw
executables named `lenso-runtime-vVERSION-TARGET` (`.exe` on Windows) plus one
`SHA256SUMS` containing `hex-digest`, two spaces, and each asset's exact filename.
The CLI uses the same format with `lenso-unified`. No archive extraction or global
installation is involved. `--version` and the package script enforce Cargo/tag
version agreement. A release is immutable: issue a new version for new bytes.

The released baseline on 2026-10-16 is Runtime tag `v0.1.0` at
`d2539a05f39b89aae733893a8c8b75ce55882666` and CLI tag `v0.1.0` at
`5e44331b8ba3e6ad4037c0a96be63ec69c78f31a`. All five native jobs passed in
[Runtime run 38880149851](https://github.com/LioRael/lenso-runtime/actions/runs/38880149851)
and [CLI run 38880153014](https://github.com/LioRael/lenso-cli/actions/runs/38880153014).
Each release contains five executables and `SHA256SUMS`; all twelve published
assets were downloaded and the ten executables matched their release checksum
manifests during acceptance.
These receipts describe the released commits, not later local CI/land candidates.

| Target | Native CI runner | v0.1.0 validation |
| --- | --- | --- |
| aarch64-apple-darwin | macos-14 | Native build and binary/manager smoke passed; official CLI download, no-Rust use, offline reuse and Python call accepted on macOS 27 |
| x86_64-apple-darwin | macos-15-intel | Native build and binary/manager smoke passed |
| x86_64-unknown-linux-gnu | ubuntu-22.04 | Native build and binary/manager smoke passed |
| aarch64-unknown-linux-gnu | ubuntu-22.04-arm | Native build and binary/manager smoke passed |
| x86_64-pc-windows-msvc | windows-2022 | Native build and binary/manager smoke passed |

Runner labels were checked against the [GitHub runner reference](https://docs.github.com/en/actions/reference/runners/github-hosted-runners).
Linux assets are glibc builds on Ubuntu 22.04, not Alpine/musl binaries. Older
OS/glibc compatibility has not been tested. macOS 14/15 are CI build environments,
not yet proven minimum OS versions. Windows ARM, 32-bit, musl and other targets
are explicitly unsupported by the manager. Windows kernel TCP/manager smoke
passed in native CI; Unix stdio cancellation and process-group semantics are not
claimed on Windows. CLI language adapters remain Unix only; its runtime manager
compiles separately on Windows. End-user installation and Python interoperability
outside the accepted Apple Silicon macOS environment remain unverified.

The workflows do not configure macOS Developer ID signing/notarization or
Windows Authenticode signing. Rust/macOS linker ad-hoc signing is not a trusted
publisher identity. SHA-256 detects corruption relative to the release manifest;
it is not a publisher signature. OS trust prompts may require manual handling.

## Local reproduction and release steps

```sh
cargo build --release --locked -p kernel
python3 scripts/smoke-binary.py target/release/lenso-runtime
python3 scripts/package-release.py --binary target/release/lenso-runtime \
  --target aarch64-apple-darwin --version v0.1.0 --output dist
cat dist/*.sha256 > dist/SHA256SUMS
```

The CLI repository has the HTTP-fixture acceptance suite, including the real
kernel and Python SDK/service call. It needs Python >=3.11, this Runtime checkout
and its release binary; it does not install any SDK or invoke a compiler.

1. Obtain separate authorization to push/integrate source, create tags and run
   remote workflows for each future release.
2. Run source checks and `.github/workflows/binaries.yml` with `publish=false`.
   All five native jobs must pass; inspect downloaded candidate assets and sums.
3. Update each repository's Cargo version independently when needed, and update
   CLI's pinned kernel default only to a reviewed published compatible version.
4. After tag authorization, dispatch from that exact existing version tag with
   `publish=true`. The workflow rejects a branch ref or mismatched Cargo version
   and creates a **draft** only after the full matrix and checksum fan-in pass.
5. Review/sign assets if credentials become available (regenerate sums after
   signing), then explicitly approve publishing the draft. Runtime and CLI
   repositories require separate releases. SDK npm/crates/pip publication is not
   part of either binary workflow.

The public Runtime v0.1.0 GitHub download and cached offline reuse were accepted
with the official CLI on macOS, including a Python service call without Rust.
The HTTP-fixture suite remains useful for deterministic failure-path checks; it
is separate from this published-asset acceptance. For future private Runtime
releases, set `LENSO_RUNTIME_TOKEN` to an existing token with
read access to repository contents. The manager resolves asset IDs via GitHub's
release API, follows the asset download, and never saves the token in receipts.
It does not create credentials or alter permissions. Private Runtime CDN access
remains unverified because the accepted Runtime release is public; the CLI
repository's private visibility does not make Runtime downloads private.
