# Runtime protocol v1

Transport: loopback TCP, UTF-8 newline-delimited JSON, 256 KiB maximum frame
including newline. Bun uses `node:net`, Rust uses Tokio. All local processes are
trusted; service names are not cryptographically authenticated. No end-user
principal is accepted or propagated. Production identity federation is absent.
No public listener in v1.

Every frame has `type`. Unknown/malformed frames close the connection.
Handshake completes within 5 s. Call, status and promotion IDs share a strictly increasing
positive canonical decimal `u64` sequence per connection (encoded as strings).
The kernel retains only its high-water mark, not a history set. Cancel and
credit refer to the earlier call ID and do not advance that sequence.
Kernel route IDs are globally increasing strings, never reused within a kernel
lifetime. Only the assigned connection may respond. Disconnect fails involved
calls; new connections cannot deliver old responses.

## Frames

- Host → kernel: `{"type":"hello","version":1,"namespace":"demo","service":"frontend","instance":"one"}`
- Kernel → host: `{"type":"welcome","version":1,"generation":"1","heartbeat_ms":1000}`
- Host → kernel: `{"type":"register","contract":<contract>,"max_concurrency":8}`
- Kernel → host: `{"type":"registered"}`
- Host → kernel: `{"type":"ready"}`; kernel → host: `{"type":"ready"}`
  Registration permits outbound calls/status, but not inbound assignment.
  TS App integration publishes ready only after all native setup and explicit
  export binding succeed. Its owned starter pre-drains before native cleanup.
- A caller may stop the handshake after `welcome`: `hello` alone permits
  outbound call/status/credit/cancel, heartbeat and drain/unregister. It cannot
  receive invocations or send ready without registration. Such a connection
  remains `connected` in status; it has no exported contract or concurrency.
  Current TS and Rust SDK Hosts instead register a contract (an empty method
  set for caller-only use). Registration and ready are not prerequisites for
  outbound calls in a future SDK. Repeated hello/register/ready is a protocol
  error; drain prevents subsequent calls, registration and ready.
- Host → kernel: `{"type":"heartbeat"}` (no response).
- Host → kernel: `{"type":"call","id":"1","namespace":"demo","service":"batch","method":"aggregate","contract_version":"1","input":{},"timeout_ms":5000,"trace_id":"optional","parent_id":"optional"}`
- Kernel → callee: `{"type":"invoke","id":"route-id","method":"aggregate","input":{},"timeout_ms":5000,"trace_id":"assigned-or-inherited","parent_id":"optional","caller":{"namespace":"demo","service":"frontend","instance":"one"}}`
- Callee → kernel → caller: `{"type":"result","id":"...","output":{}}`
- Either direction: `{"type":"error","id":"...","code":"business","message":"safe message","details":{}}`
- Callee → kernel → caller: `{"type":"chunk","id":"...","output":{}}`
- Callee → kernel → caller: `{"type":"end","id":"..."}`
- Caller → kernel → callee: `{"type":"credit","id":"...","credits":1}`
- Caller → kernel → callee: `{"type":"cancel","id":"..."}`
- Host → kernel: `{"type":"drain","timeout_ms":5000}`; reject new assignments.
  Kernel → host: `{"type":"drained"}` once work terminates or deadline fails it.
- Host → kernel: `{"type":"unregister"}` closes connection and fails calls.
- Trusted local operator → kernel:
  `{"type":"promote","id":"...","namespace":"demo","service":"batch","instance":"next","timeout_ms":5000}`;
  kernel → operator: `{"type":"promoted","id":"..."}` at routing commit.
  TS: `await host.promote({ namespace, service, instance, timeoutMs? })`.
  `timeoutMs` defaults to 5000 and bounds old-instance drain, not ACK latency.
  The SDK waits at most 5 s for ACK; an ACK timeout/disconnect is an uncertain
  outcome, not proof that no commit occurred. Inspect status, never replay blindly.
- Host → kernel: `{"type":"status","id":"..."}`; kernel →
  host: `{"type":"status","id":"...","services":[...],"calls":[...],"limits":{...}}`.
  Local trusted diagnostics, not automatic Manage exposure.
  Services include capability names/modes, contract version, lifecycle state,
  generation, and additive `selected` boolean; calls include trace/parent IDs.
  `recent` retains at most 64 terminal outcome/code/duration records, never input
  or output payloads. A snapshot exceeding the frame budget fails with
  `overloaded`; the diagnostic connection is not disconnected.
  Additive `queue_ms` measures kernel admission to assignment (or terminal
  failure when never assigned); `execution_ms` measures assignment to terminal
  response/failure, or is null for never-assigned work. Execution includes wire,
  SDK validation/scheduling and stream-consumer backpressure, not just business
  execution. Business handler timing must be measured by the host itself.

Error codes: `service_not_found`, `not_ready`, `overloaded`, `deadline_exceeded`,
`connection_lost`, `cancelled`, `contract_mismatch`, `invalid_input`,
`invalid_output`, `business`, `protocol_error`. Handshake/registration errors
can omit `id`, and close the connection. Business details validate against the
method error schema.

## Bounds and semantics

Defaults: 128 connections, 64 active calls, 128 waiting calls, 64 outbound frames
per connection, 256 KiB/frame, 60 s maximum deadline, 5 s heartbeat expiry.
Registered concurrency is 1–64. Choose least-busy ready matching instance.
Deadlines begin at admission, including queue time. Same scoped service/version
must register structurally equal contracts. No automatic retries or replay.

## Trusted local cutover

Promotion is an explicit LOCAL development operator action, not an authenticated
management API. Any trusted loopback connection may request it after hello.
It runs no arbitrary code and adds no CLI, MCP or Manage tool exposure.

Before selection, same-service ready instances remain least-busy. For staged
replacement, promote the initial instance before starting the next candidate:
later candidates may register/ready but cannot receive calls until promoted.
Promotion requires exactly one ready, non-draining target instance. Missing,
unready or ambiguous targets fail `not_ready`. It compares the entire contract,
including version, schemas, modes and exported method set, against all serving
same-scope instances, admitted calls, and any retained selected contract.
Any difference, including a version bump, fails `contract_mismatch`. Registration
failure or rejected promotion never changes the previous routing selection.
Validation and routing commit happen atomically in the kernel actor.

Only new assignment changes: admitted but unassigned queued calls use the
selected candidate. Already assigned old calls/streams keep their old route,
credits and deadline. Other existing scoped instances stop new admission and
enter bounded drain. At its deadline remaining old assigned work receives
`deadline_exceeded`, with cancellation sent to the old callee and outcome
reported in recent status. Kernel-driven drain emits no unsolicited lifecycle
frame to old SDKs. Explicit SDK drain still works and may shorten, never extend,
the existing bound; it also drains outbound work. Promotion ACK does not wait
for old work to finish. Use status or old SDK drain before process cleanup.

Selection is pinned to connection generation. Selected disconnect fails active
work and fails closed for new work (`not_ready`); no fallback to drained old
instances or a reconnect with the same name. Explicitly promote a compatible
ready replacement. Selection and its contract survive disconnect until kernel
restart, bounded to 128 distinct promoted scopes; further scopes fail
`overloaded`. No global retry, rollback, or version negotiation occurs.

Streams begin with zero credit. SDK grants one on consumer `next()`. At most
16 outstanding credits; each chunk spends one. Chunk without credit fails the
route. Unary accepts result, stream accepts chunk/end. Terminal frames need no
credit. Iterator `return()` cancels. No unbounded consumer buffering.

Timeout does not prove side effects did not finish. Cancellation is cooperative,
not rollback. Drain gives in-flight work bounded time. Kernel restart fails
in-flight calls; Hosts may reconnect/register/ready without replay.

## Single contract source

Business-local JSON: `{"version":"1","methods":{"aggregate":{"mode":"unary",
"input":<schema>,"output":<schema>,"error":<schema>}}}`.

Restricted JSON Schema keywords: `type` (string, boolean, number, integer, null,
object, array), `properties`, `required`, `additionalProperties:false`, `items`,
`enum`, `minimum`, `maximum`, `minLength`, `maxLength`, `minItems`, `maxItems`,
`anyOf` only for nullable values. Reject unsupported keywords at registration
and generation. No references, recursion, arbitrary maps, coercion or formats.
Enums have at most 64 scalar members; object/array enums are unsupported to keep
validation and generated types bounded and explicit. Client-only Hosts may
register an empty method set.
Objects declare `required` explicitly, including an empty list. A field may be
optional and non-null, or required and nullable; optional nullable fields are
rejected at registration and generation rather than conflating absent with null.
Integers are within ±9007199254740991 and use JSON integer tokens (not `1.0` or
`1e0`), matching generated serde `i64`; numbers are finite; strings UTF-8.
`number` uses IEEE-754 binary64, not arbitrary-precision decimal arithmetic.
Missing optional fields differ from explicit null; Rust generation preserves this.
Binary is unsupported. Files use bounded streams or owner-controlled references.
Runtime validates input/output/chunks/business errors, not just generated types.
Generate natural TypeScript client methods and serde Rust input/output/error
types. Internal Plugin methods do not require contracts.
Both generated clients validate the caller-held method schema: input before
sending, output and business details before exposing a result, and each stream
item before delivering it. This remains necessary even when the callee's
registered contract uses the same version but weaker bounds. A caller schema
failure is `invalid_input` or `invalid_output`; a requested version absent from
the target service is `contract_mismatch`. No compatibility negotiation or
coercion occurs. Stream validation/deserialization failure cancels and retires
the current call, clears local wait/credit resources, and never delivers another
item from that stream. The TS iterator keeps rejecting with its terminal error;
Rust emits one error and then returns `None`.
When a Rust terminal error and buffered chunk are simultaneously ready, the
terminal error wins; a successful end still validates any already-buffered item.
If either SDK cannot enqueue cancellation because its bounded writer is full,
it closes the connection so the kernel retires the route rather than silently
leaving it alive. The current call retains its original error; other in-flight
calls on that connection fail. Ordinary overload rejecting new admission does
not close a healthy connection.

Rust manual bindings must exactly match the declared method set and modes.
Missing, extra, repeated or wrong-mode bindings fail before opening a connection
or publishing ready. The generated typed server builder delegates to that same
validated binding layer; it does not define a second registry or contract.
The 256 KiB limit also applies after adding the trusted invocation envelope and
after translating response IDs. Requests that do not fit fail `invalid_input`,
responses `invalid_output`, without dropping healthy peers or unrelated calls.
