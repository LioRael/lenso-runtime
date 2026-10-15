# Choose the host boundary

| Host | Existing entry | Ownership |
| --- | --- | --- |
| SDK-only TS | `RuntimeHost.connect(options)`; `ready: false` then `activate()` for late readiness | App owns connection, native setup, drain/close |
| SDK-only Rust | `RuntimeHost::connect` or `connect_registered`, then `validate_handlers` / `activate` | App owns native lifetime; generated `LogsServer::build()` checks complete bindings |
| Native TS App | `createRuntimePlugin`, `startRuntimeApp` in `@lenso/runtime` | Exact installed plugin; `exports(running, clients)` after Core setup; starter pre-drains before Core cleanup |
| Native Rust App | `RuntimePlugin`, `RuntimeExport`, `start_runtime_app` in `lenso-plugin-runtime` | Exact handle and post-setup factory; entrance cleanup before resources |

These are separate layers. `@lenso/runtime-sdk` is not the TS Core adapter. Rust adapter pins an older SDK and currently has no transport argument; verify that repo before using newer SDK options through it. Go core/Runtime SDK is a separate local delivery; do not fabricate Go connection APIs from this TS/Rust table.

An outbound-only native TS app can use Core `startApp` without activation. A serving app uses `startRuntimeApp`. `activateRuntimeApp` borrows a running app and leaves failure cleanup/pre-drain to its caller. Neither installing a plugin nor scanning native methods publishes capabilities.

## Three implemented transports

| Transport | Configuration / real example | Required behavior |
| --- | --- | --- |
| TCP | Default; kernel `--listen 127.0.0.1:7331` | Existing framed protocol; trusted loopback |
| stdio | TS `transport: { type: "stdio" }`; Rust `Transport::Stdio`; kernel `--processes` config | Persistent child, never one child per call; protocol-only stdout during startup/ready/cleanup |
| WebSocket | TS `transport: { type: "websocket", url }`; Rust `Transport::WebSocket { url }`; kernel `--websocket 127.0.0.1:7332` | Bounded JSON text messages, no newline wrapping/binary payload; browser Origin and remote/WSS mode rejected |

Rust SDK uses `connect_with_transport` or `connect_registered_with_transport` with existing `HostOptions`. TS keeps the transport inside connect options. Read `examples/multitransport/bun-service.ts`, `examples/batch/src/main.rs` and `scripts/transport-smoke.ts` for exact options.

Kernel-owned commands are direct executables with optional args/cwd/env, not shell strings. Relative paths follow launch context; use an absolute executable with a changed cwd. Environment is inherited; keep secrets out of checked-in process JSON. Kernel distinguishes started/connected/ready and owns shutdown/reaping for its children. Independently launched processes remain the launcher's responsibility.

Logs belong on stderr (`console.error`, `eprintln!`). Business errors must not dump credentials or raw payloads. Avoid another listener/container when the existing App/Engine already owns the process. TS Engine's ordinary dev worker restart also restarts Engine-owned companions; independent lifetimes require the existing outer workspace launcher, not an unsupported hot-reload promise.
