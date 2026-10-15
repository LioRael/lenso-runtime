---
name: lenso-runtime
description: Implement or troubleshoot explicit cross-process or cross-language Lenso Runtime services, generated clients, host readiness, streaming, cancellation, and TCP/stdio/WebSocket connections. Use for Runtime SDK/kernel boundaries; native TS/Rust/Go plugin composition alone does not need Runtime.
---

# Lenso Runtime boundaries

Keep native functions and dependency injection inside each service. Export only the capabilities that actually cross a process/language boundary; Runtime registration does not expose CLI, MCP, Manage or Console operations.

1. Identify the owner: kernel/protocol/SDK/generator in this repo; TS App adapter in `lenso/packages/runtime`; Rust App adapter in `lenso-rust/crates/plugin-runtime`; Console page in its own repo. Use installed public exports, not copied protocol implementations.
2. Pin the selected SDK and generator source before choosing APIs. This skill's evidence is Runtime `765db7a` (2026-10-15), a source delivery. Matching `0.1.0` version strings do not establish matching builds or registry publication.
3. Start with the smallest existing contract and native handler. Export explicitly, generate bindings, typecheck/build, then verify one call through the real kernel. Local plugin methods need no wire contract.
4. Keep connection registration, successful native setup/binding, and ready activation distinct. Drain incoming work before releasing business resources. Register cleanup immediately and preserve both operation and cleanup errors.
5. Carry cancellation/trace context explicitly; bound stream production and await SDK backpressure. Do not add implicit retries or assume cancellation undoes effects.

Read by boundary:

- Contract, generated methods, errors and native values: [Contracts](references/contracts.md).
- SDK versus native adapters, readiness and transport: [Hosts](references/hosts.md).
- Reproducible source integration and verification: [Validation](references/validation.md).

The current Runtime supports **TCP, persistent stdio and local WebSocket**, including cancellation and stream credit/backpressure. It is a trusted local execution system, not authenticated remote production service infrastructure. A protocol schema is not authorization; business resource checks remain at their owner.
