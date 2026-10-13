# Runtime protocol v1

Transport: loopback TCP, UTF-8 newline-delimited JSON, 256 KiB maximum frame
including newline. Bun uses `node:net`, Rust uses Tokio. All local processes are
trusted; service names are not cryptographically authenticated. No end-user
principal is accepted or propagated. Production identity federation is absent.
No public listener in v1.

Every frame has `type`. Unknown/malformed frames close the connection.
Handshake completes within 5 s. Call and status IDs share a strictly increasing
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
- Host → kernel: `{"type":"status","id":"..."}`; kernel →
  host: `{"type":"status","id":"...","services":[...],"calls":[...],"limits":{...}}`.
  Local trusted diagnostics, not automatic Manage exposure.
  Services include capability names/modes; calls include trace/parent IDs.
  `recent` retains at most 64 terminal outcome/code/duration records, never input
  or output payloads. A snapshot exceeding the frame budget fails with
  `overloaded`; the diagnostic connection is not disconnected.

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
The 256 KiB limit also applies after adding the trusted invocation envelope and
after translating response IDs. Requests that do not fit fail `invalid_input`,
responses `invalid_output`, without dropping healthy peers or unrelated calls.
