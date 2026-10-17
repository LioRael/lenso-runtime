import { afterEach, expect, test } from "bun:test";
import { createServer, type Socket } from "node:net";
import { BusinessError, RuntimeError, RuntimeHost, type Handler } from "@lenso/runtime-sdk";
import { type Contract, type Schema, validateValue } from "./schema";
import { generate } from "../../tools/generate";
import { LogsClient, logsContract, isAggregateError, isProgressError } from "../../examples/generated/logs";
import schemaCases from "../../crates/sdk/tests/schema-cases.json";

test("shared restricted schema fixtures agree on constraints and presence", () => {
  for (const fixture of schemaCases) {
    for (const value of fixture.accept)
      expect(() => validateValue(fixture.schema as Schema, value)).not.toThrow();
    for (const value of fixture.reject)
      expect(() => validateValue(fixture.schema as Schema, value)).toThrow();
  }
});

const object = {
  type: "object",
  properties: { value: { type: "integer" } },
  required: ["value"],
  additionalProperties: false,
} as const;
const contract: Contract = {
  version: "1",
  methods: {
    unary: { mode: "unary", input: object as any, output: object as any, error: object as any },
    stream: { mode: "stream", input: object as any, output: object as any, error: object as any },
  },
};
const target = { namespace: "demo", service: "worker", contractVersion: "1" };
function copyMethod(method: Contract["methods"][string]): Contract["methods"][string] {
  return {
    mode: method.mode,
    input: structuredClone(method.input),
    output: structuredClone(method.output),
    error: structuredClone(method.error),
  };
}
const cleanup: (() => void)[] = [];
afterEach(() => {
  for (const close of cleanup.splice(0)) close();
});
async function fixture(handlers: Record<string, Handler> = {}, maxConcurrency = 8, ready = true, transport: "tcp" | "websocket" = "tcp") {
  let peer!: Socket,
    buffer = "",
    wsPeer: any;
  const frames: any[] = [];
  const waiters: { type: string; resolve(frame: any): void }[] = [];
  const server = createServer((socket) => {
    peer = socket;
    socket.on("data", (data) => {
      buffer += data;
      let index: number;
      while ((index = buffer.indexOf("\n")) >= 0) {
        const frame = JSON.parse(buffer.slice(0, index));
        buffer = buffer.slice(index + 1);
        accept(frame);
      }
    });
  });
  const accept = (frame: any) => {
    if (frame.type === "hello")
      send({ type: "welcome", version: 1, generation: "1", heartbeat_ms: 20 });
    else if (frame.type === "register") send({ type: "registered" });
    else if (frame.type === "ready") send({ type: "ready" });
    else if (frame.type !== "heartbeat") {
      const waiter = waiters.findIndex((w) => w.type === frame.type);
      if (waiter >= 0) waiters.splice(waiter, 1)[0]!.resolve(frame);
      else frames.push(frame);
    }
  };
  const raw = (text: string) => transport === "tcp" ? peer.write(text) : wsPeer.send(text);
  const send = (frame: any) => raw(JSON.stringify(frame) + (transport === "tcp" ? "\n" : ""));
  const websocket = transport === "websocket" ? Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    fetch(request, server) {
      if (server.upgrade(request)) return;
      return new Response("upgrade required", { status: 400 });
    },
    websocket: {
      open(socket) { wsPeer = socket; },
      message(_socket, message) { accept(JSON.parse(String(message))); },
    },
  }) : undefined;
  if (transport === "tcp")
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  cleanup.push(() => {
    peer?.destroy();
    server.close();
    websocket?.stop(true);
  });
  const exported = {
    version: "1",
    methods: Object.fromEntries(
      Object.keys(handlers).map((name) => [name, copyMethod(contract.methods[name]!)]),
    ),
  };
  const host = await RuntimeHost.connect({
    namespace: "demo",
    service: "sdk",
    instance: "one",
    contract: exported,
    handlers,
    maxConcurrency,
    ready,
    port: transport === "tcp" ? (server.address() as any).port : undefined,
    transport: websocket ? { type: "websocket", url: `ws://127.0.0.1:${websocket.port}` } : { type: "tcp" },
  });
  cleanup.push(() => host.close());
  const next = (type: string): Promise<any> => {
    const index = frames.findIndex((f) => f.type === type);
    if (index >= 0) return Promise.resolve(frames.splice(index, 1)[0]);
    return new Promise((resolve) => waiters.push({ type, resolve }));
  };
  const invoke = (id: string, method: string, timeout_ms = 1000) =>
    send({
      type: "invoke",
      id,
      method,
      input: { value: 1 },
      timeout_ms,
      trace_id: "trace",
      caller: { namespace: "demo", service: "caller", instance: "one" },
    });
  return { host, next, send, raw, invoke, frames, exported, disconnect: () => transport === "tcp" ? peer.destroy() : wsPeer.close(), halfClose: () => peer.end() };
}
test("promotion uses scoped operator frame and commit acknowledgement, with no retry", async () => {
  const f = await fixture();
  const promoted = f.host.promote({ namespace: "demo", service: "worker", instance: "new", timeoutMs: 40 });
  const request = await f.next("promote");
  expect(request).toEqual({
    type: "promote", id: "1", namespace: "demo", service: "worker", instance: "new", timeout_ms: 40,
  });
  f.send({ type: "promoted", id: request.id });
  expect(await promoted).toBeUndefined();
  const incompatible = f.host.promote({ namespace: "demo", service: "worker", instance: "bad" });
  const rejected = await f.next("promote");
  expect(rejected.timeout_ms).toBe(5000);
  f.send({ type: "error", id: rejected.id, code: "contract_mismatch", message: "contract_mismatch" });
  await expect(incompatible).rejects.toMatchObject({ code: "contract_mismatch" });
  await expect(f.host.promote({ namespace: "", service: "worker", instance: "new" })).rejects.toMatchObject({ code: "invalid_input" });
  await expect(f.host.promote({ namespace: "demo", service: "worker", instance: "new", timeoutMs: 60001 })).rejects.toMatchObject({ code: "invalid_input" });
  const pending = f.host.promote({ namespace: "demo", service: "worker", instance: "lost" });
  await f.next("promote");
  f.disconnect();
  await expect(pending).rejects.toMatchObject({ code: "connection_lost" });
  expect(f.frames.filter((frame) => frame.type === "cancel")).toHaveLength(0);
});

test("unary, status and disconnect fail pending work without retry", async () => {
  const f = await fixture();
  const call = f.host.call(target, "unary", { value: 3 });
  const frame = await f.next("call");
  f.send({ type: "result", id: frame.id, output: { value: 4 } });
  expect(await call).toEqual({ value: 4 });
  const status = f.host.status();
  const request = await f.next("status");
  f.send({ type: "status", id: request.id, services: [], calls: [], limits: {} });
  expect(await status).toEqual({ services: [], calls: [], limits: {} });
  const expiredStatus = f.host.status({ timeoutMs: 20 });
  const late = await f.next("status");
  await expect(expiredStatus).rejects.toMatchObject({ code: "deadline_exceeded" });
  f.send({ type: "status", id: late.id, services: [], calls: [], limits: {} });
  const afterLate = f.host.call(target, "unary", {});
  const afterLateRequest = await f.next("call");
  f.send({ type: "result", id: afterLateRequest.id, output: { value: 5 } });
  expect(await afterLate).toEqual({ value: 5 });
  const pending = f.host.call(target, "unary", {});
  await f.next("call");
  f.disconnect();
  await expect(pending).rejects.toMatchObject({ code: "connection_lost" });
  await expect(f.host.call(target, "unary", {})).rejects.toMatchObject({ code: "connection_lost" });
  await f.host.drain(100);
});
test("stream credits follow next and iterator return cancels", async () => {
  const f = await fixture();
  const stream = f.host.stream(target, "stream", {});
  const call = await f.next("call");
  expect(f.frames.filter((f) => f.type === "credit")).toHaveLength(0);
  const next = stream.next();
  expect(await f.next("credit")).toMatchObject({ id: call.id, credits: 1 });
  f.send({ type: "chunk", id: call.id, output: { value: 2 } });
  expect(await next).toEqual({ done: false, value: { value: 2 } });
  await stream.return!();
  expect(await f.next("cancel")).toMatchObject({ id: call.id });
  expect(await stream.next()).toEqual({ done: true, value: undefined });
});
test("caller abort and deadlines send cancellation", async () => {
  const f = await fixture(),
    controller = new AbortController();
  const call = f.host.call(target, "unary", {}, { signal: controller.signal });
  await f.next("call");
  controller.abort();
  await expect(call).rejects.toMatchObject({ code: "cancelled" });
  await f.next("cancel");
  const expired = f.host.call(target, "unary", {}, { timeoutMs: 20 });
  await f.next("call");
  await expect(expired).rejects.toMatchObject({ code: "deadline_exceeded" });
  await f.next("cancel");
});
test("host validates output and business details and bounds noncooperative handlers", async () => {
  const f = await fixture({
    unary: () => {
      throw new BusinessError({ value: 9 });
    },
  });
  f.invoke("route1", "unary");
  expect(await f.next("error")).toMatchObject({ code: "business", details: { value: 9 } });
  let release!: (value: { value: number }) => void;
  let starts = 0;
  const slow = await fixture(
    {
      unary: () => {
        starts++;
        return starts === 1
          ? new Promise((resolve) => {
              release = resolve;
            })
          : { value: 2 };
      },
    },
    1,
  );
  slow.invoke("route2", "unary", 20);
  expect(await slow.next("error")).toMatchObject({ code: "deadline_exceeded" });
  slow.invoke("route4", "unary");
  expect(await slow.next("error")).toMatchObject({ code: "overloaded" });
  expect(starts).toBe(1);
  release({ value: 1 });
  await Bun.sleep(10);
  slow.invoke("route5", "unary");
  expect(await slow.next("result")).toMatchObject({ output: { value: 2 } });
  const invalid = await fixture({ unary: () => ({ unexpected: true }) });
  invalid.invoke("route3", "unary");
  expect(await invalid.next("error")).toMatchObject({ code: "invalid_output" });
});
test("host stream does not advance its iterator before credit and cancels waiting producer", async () => {
  let advances = 0,
    cancelled = false;
  const f = await fixture({
    stream: async function* (_: unknown, context: any) {
      context.signal.addEventListener("abort", () => {
        cancelled = true;
      });
      advances++;
      yield { value: 1 };
      advances++;
      yield { value: 2 };
    },
  });
  f.invoke("route1", "stream");
  await Bun.sleep(10);
  expect(advances).toBe(0);
  f.send({ type: "credit", id: "route1", credits: 1 });
  expect(await f.next("chunk")).toMatchObject({ output: { value: 1 } });
  await Bun.sleep(10);
  expect(advances).toBe(1);
  f.send({ type: "cancel", id: "route1" });
  expect(await f.next("error")).toMatchObject({ code: "cancelled" });
  expect(cancelled).toBe(true);
});
test("many-item producer keeps only one scoped abort subscription and removes it at completion", async () => {
  const active = new Set<unknown>();
  let peak = 0, subscriptions = 0;
  const count = 512;
  const f = await fixture({
    stream: (_input: unknown, { signal }: { signal: AbortSignal }) => {
      const add = signal.addEventListener.bind(signal);
      const remove = signal.removeEventListener.bind(signal);
      signal.addEventListener = (type: string, listener: EventListenerOrEventListenerObject, options?: boolean | AddEventListenerOptions) => {
        if (type === "abort") {
          active.add(listener);
          subscriptions++;
          peak = Math.max(peak, active.size);
        }
        add(type, listener, options);
      };
      signal.removeEventListener = (type: string, listener: EventListenerOrEventListenerObject, options?: boolean | EventListenerOptions) => {
        if (type === "abort") active.delete(listener);
        remove(type, listener, options);
      };
      return (async function* () {
        for (let value = 0; value < count; value++) yield { value };
      })();
    },
  });
  f.invoke("many", "stream", 10000);
  for (let value = 0; value < count; value++) {
    f.send({ type: "credit", id: "many", credits: 1 });
    expect(await f.next("chunk")).toMatchObject({ output: { value } });
    expect(active.size).toBeLessThanOrEqual(1);
  }
  f.send({ type: "credit", id: "many", credits: 1 });
  expect(await f.next("end")).toMatchObject({ id: "many" });
  expect(subscriptions).toBeGreaterThanOrEqual(count + 2);
  expect(peak).toBe(1);
  expect(active.size).toBe(0);
});
for (const stage of ["handler resolve", "handler reject", "credit", "next resolve", "next reject"]) {
  test(`producer cancellation during ${stage} retains local work and iterator cleanup`, async () => {
    let entered!: () => void;
    const started = new Promise<void>((resolve) => { entered = resolve; });
    let settle!: () => void;
    let returned!: () => void;
    const returning = new Promise<void>((resolve) => { returned = resolve; });
    let finishReturn!: () => void;
    const returnWork = new Promise<void>((resolve) => { finishReturn = resolve; });
    let advances = 0;
    const iterator = {
      [Symbol.asyncIterator]() { return this; },
      next() {
        advances++;
        entered();
        return new Promise<IteratorResult<{ value: number }>>((resolve, reject) => {
          settle = () => stage.endsWith("reject")
            ? reject(new Error("late next failure"))
            : resolve({ done: false, value: { value: 1 } });
        });
      },
      async return() {
        returned();
        await returnWork;
        return { done: true as const, value: undefined };
      },
    };
    const f = await fixture({
      stream: () => {
        if (!stage.startsWith("handler")) {
          if (stage === "credit") entered();
          return iterator;
        }
        entered();
        return new Promise((resolve, reject) => {
          settle = () => stage.endsWith("reject")
            ? reject(new Error("late handler failure"))
            : resolve(iterator);
        });
      },
    }, 1);
    f.invoke("cancelled", "stream");
    if (stage.startsWith("next"))
      f.send({ type: "credit", id: "cancelled", credits: 1 });
    await started;
    f.send({ type: "cancel", id: "cancelled" });
    expect(await f.next("error")).toMatchObject({ id: "cancelled", code: "cancelled" });
    expect(advances).toBe(stage.startsWith("next") ? 1 : 0);
    f.invoke("blocked", "stream");
    expect(await f.next("error")).toMatchObject({ id: "blocked", code: "overloaded" });
    let drained = false;
    const drain = f.host.drain(1000).then(() => { drained = true; });
    await f.next("drain");
    f.send({ type: "drained" });
    if (stage !== "credit") settle();
    if (stage !== "handler reject") {
      await returning;
      await Bun.sleep(0);
      expect(drained).toBe(false);
    }
    finishReturn();
    await drain;
    expect(f.frames.filter((frame) => ["chunk", "end", "error"].includes(frame.type))).toEqual([]);
  });
}
test("drain is explicit, bounded and rejects new calls", async () => {
  const f = await fixture();
  const drain = f.host.drain(100);
  expect(await f.next("drain")).toMatchObject({ timeout_ms: 100 });
  await expect(f.host.call(target, "unary", {})).rejects.toMatchObject({ code: "not_ready" });
  f.send({ type: "drained" });
  await drain;
});
test("drain waits for cancelled local work, not just the kernel acknowledgement", async () => {
  let release!: (value: { value: number }) => void;
  const f = await fixture(
    {
      unary: () =>
        new Promise((resolve) => {
          release = resolve;
        }),
    },
    1,
  );
  f.invoke("route1", "unary", 20);
  expect(await f.next("error")).toMatchObject({ code: "deadline_exceeded" });
  let complete = false,
    repeatedComplete = false;
  const drain = f.host.drain(200).then(() => {
    complete = true;
  });
  const repeated = f.host.drain(200).then(() => {
    repeatedComplete = true;
  });
  await f.next("drain");
  f.send({ type: "drained" });
  await Bun.sleep(10);
  expect(complete).toBe(false);
  expect(repeatedComplete).toBe(false);
  release({ value: 1 });
  await drain;
  await repeated;
  expect(complete).toBe(true);
  expect(repeatedComplete).toBe(true);
});
test("burst overload rejects new work without disconnecting an unrelated call", async () => {
  const f = await fixture();
  const first = f.host.call(target, "unary", {});
  const outcomes = Array.from({ length: 1000 }, () =>
    f.host.call(target, "unary", {}, { timeoutMs: 50 }).then(
      () => "success",
      (e) => e.code,
    ),
  );
  const request = await f.next("call");
  f.send({ type: "result", id: request.id, output: { value: 7 } });
  expect(await first).toEqual({ value: 7 });
  const codes = await Promise.all(outcomes);
  expect(codes).toContain("overloaded");
  expect(codes.every((code) => code === "overloaded" || code === "deadline_exceeded")).toBe(true);
});
test("malformed frames and chunks without credit fail pending work", async () => {
  const f = await fixture();
  class Resource {
    value = 1;
  }
  await expect(f.host.call(target, "unary", new Resource())).rejects.toMatchObject({
    code: "invalid_input",
  });
  await expect(f.host.call(target, "unary", "x".repeat(256 * 1024))).rejects.toMatchObject({
    code: "invalid_input",
  });
  await expect(f.host.call(target, "unary", "\ud800")).rejects.toMatchObject({
    code: "invalid_input",
  });
  const cyclic: Record<string, unknown> = {};
  cyclic.self = cyclic;
  await expect(f.host.call(target, "unary", cyclic)).rejects.toMatchObject({
    code: "invalid_input",
  });
  const pending = f.host.call(target, "unary", {});
  const call = await f.next("call");
  f.send({ type: "result", id: call.id, output: {}, unknown: true });
  await expect(pending).rejects.toMatchObject({ code: "protocol_error" });
  const g = await fixture();
  const stream = g.host.stream(target, "stream", {});
  const request = await g.next("call");
  g.send({ type: "chunk", id: request.id, output: {} });
  await Bun.sleep(10);
  await expect(stream.next()).rejects.toMatchObject({ code: "protocol_error" });
});
test("generator preserves optional non-null and required nullable, rejecting ambiguous optional null", async () => {
  const schema = {
    type: "object",
    additionalProperties: false,
    required: ["nullable"],
    properties: {
      optional: { type: "integer" },
      nullable: { anyOf: [{ type: "string" }, { type: "null" }] },
    },
  } as any;
  const c: Contract = {
    version: "1",
    methods: { sample: { mode: "unary", input: schema, output: schema, error: schema } },
  };
  const output = generate(c, "sample");
  expect(output.typescript).toContain('"optional"?: number');
  expect(output.rust).toContain('deserialize_with = "present_non_null"');
  expect(output.rust).toContain('deserialize_with = "required_value"');
  const nullableObject = { anyOf: [schema, { type: "null" }] } as any;
  const nullable = generate(
    { ...c, methods: { sample: { ...c.methods.sample!, output: nullableObject } } },
    "sample",
  );
  expect(nullable.typescript).toContain("export type SampleOutput = SampleOutputValue | null");
  expect(nullable.rust).toContain("pub type SampleOutput = Option<SampleOutputValue>");
  schema.required = [];
  expect(() => generate(c, "sample")).toThrow("optional nullable");
  expect(() =>
    generate(
      {
        ...c,
        methods: {
          ...c.methods,
          sample: { ...c.methods.sample!, input: { type: "string", format: "date" } as any },
        },
      },
      "sample",
    ),
  ).toThrow("unsupported schema keyword");
  expect(() =>
    generate(
      {
        ...c,
        methods: {
          ...c.methods,
          sample: {
            ...c.methods.sample!,
            input: { type: "integer", enum: Array.from({ length: 65 }, (_, i) => i) },
          },
        },
      },
      "sample",
    ),
  ).toThrow("64 scalar");
});
test("generator rejects schema names colliding with its client or server", () => {
  const empty: Schema = { type: "object", properties: {}, required: [], additionalProperties: false };
  for (const field of ["client", "server"]) {
    const input: Schema = {
      type: "object",
      properties: { [field]: empty },
      required: [field],
      additionalProperties: false,
    };
    expect(() =>
      generate(
        { version: "1", methods: { foo: { mode: "unary", input, output: empty, error: empty } } },
        "foo_input",
      ),
    ).toThrow("generated type name collision");
  }
});
test("generated artifacts reproduce from the source contract", async () => {
  const contract = await Bun.file(
    new URL("../../examples/contracts/logs.json", import.meta.url),
  ).json();
  const output = generate(contract, "logs");
  expect(await Bun.file(new URL("../../examples/generated/logs.ts", import.meta.url)).text()).toBe(
    output.typescript,
  );
  expect(await Bun.file(new URL("../../examples/generated/logs.rs", import.meta.url)).text()).toBe(
    output.rust,
  );
});
test("generated clients validate their own response contract and cancel invalid streams", async () => {
  const f = await fixture();
  const client = new LogsClient(f.host, target);
  const input = { logs: [{ level: "info", message: "synthetic", duration_ms: 1 }] };
  const result = client.aggregate(input);
  const call = await f.next("call");
  f.send({ type: "result", id: call.id, output: { unexpected: true } });
  await expect(result).rejects.toMatchObject({ code: "invalid_output" });
  const business = client.aggregate(input);
  const businessCall = await f.next("call");
  f.send({
    type: "error",
    id: businessCall.id,
    code: "business",
    message: "safe",
    details: { unexpected: true },
  });
  await expect(business).rejects.toMatchObject({ code: "invalid_output" });
  const stream = client.progress(input);
  const streamCall = await f.next("call");
  const next = stream.next();
  await f.next("credit");
  f.send({ type: "chunk", id: streamCall.id, output: { unexpected: true } });
  await expect(next).rejects.toMatchObject({ code: "invalid_output" });
  expect(await f.next("cancel")).toMatchObject({ id: streamCall.id });
});
// Successful cancel framing does not prove safety when the bounded writer is full.
test.each(["tcp", "websocket"] as const)("%s: failed stream cancellation closes the connection rather than orphaning a route", async (transport) => {
  const f = await fixture({}, 8, true, transport);
  const unrelated = f.host.call(target, "unary", {});
  await f.next("call");
  const client = new LogsClient(f.host, target);
  const stream = client.progress({ logs: [{ level: "info", message: "synthetic", duration_ms: 1 }] });
  const call = await f.next("call");
  const next = stream.next();
  await f.next("credit");
  (f.host as any).transport.send = () => { throw new (class extends Error { code = "overloaded"; })("overloaded"); };
  f.send({ type: "chunk", id: call.id, output: { unexpected: true } });
  await expect(next).rejects.toMatchObject({ code: "invalid_output" });
  await expect(stream.next()).rejects.toMatchObject({ code: "invalid_output" });
  await expect(unrelated).rejects.toMatchObject({ code: "overloaded" });
  await expect(f.host.call(target, "unary", {})).rejects.toMatchObject({ code: "connection_lost" });
});
test("registered-only hosts can call before explicitly publishing readiness", async () => {
  const f = await fixture({}, 8, false);
  const result = f.host.call(target, "unary", {});
  const call = await f.next("call");
  f.send({ type: "result", id: call.id, output: { value: 1 } });
  expect(await result).toEqual({ value: 1 });
  await Promise.all([f.host.activate(), f.host.activate()]);
  await f.host.activate();
});

for (const transport of ["tcp", "websocket"] as const) {
  test(`${transport}: shared correlation, invocation, stream credits, cancellation and drain`, async () => {
    const f = await fixture({ unary: (input) => input }, 8, false, transport);
    const result = f.host.call(target, "unary", { value: 8 });
    const call = await f.next("call");
    f.send({ type: "result", id: call.id, output: { value: 9 } });
    expect(await result).toEqual({ value: 9 });
    await f.host.activate();
    f.invoke("incoming", "unary");
    expect(await f.next("result")).toMatchObject({ id: "incoming", output: { value: 1 } });
    const stream = f.host.stream(target, "stream", {});
    const request = await f.next("call");
    const next = stream.next();
    expect(await f.next("credit")).toMatchObject({ id: request.id, credits: 1 });
    f.send({ type: "chunk", id: request.id, output: { value: 3 } });
    expect(await next).toEqual({ done: false, value: { value: 3 } });
    await stream.return!();
    expect(await f.next("cancel")).toMatchObject({ id: request.id });
    const drain = f.host.drain(100);
    await f.next("drain");
    f.send({ type: "drained" });
    await drain;
  });
  test(`${transport}: envelope budget, oversized inbound, disconnect without replay`, async () => {
    const f = await fixture({}, 8, true, transport);
    await expect(f.host.call(target, "unary", "x".repeat(256 * 1024 - 1))).rejects.toMatchObject({ code: "invalid_input" });
    const pending = f.host.call(target, "unary", {});
    await f.next("call");
    f.raw(" ".repeat(256 * 1024));
    await expect(pending).rejects.toMatchObject({ code: "protocol_error" });
    await expect(f.host.status()).rejects.toMatchObject({ code: "connection_lost" });
    expect(f.frames.filter((frame) => frame.type === "call")).toHaveLength(0);
    const g = await fixture({}, 8, true, transport);
    const lost = g.host.call(target, "unary", {});
    await g.next("call");
    g.disconnect();
    await expect(lost).rejects.toMatchObject({ code: "connection_lost" });
    await expect(g.host.call(target, "unary", {})).rejects.toMatchObject({ code: "connection_lost" });
  });
}
test("TCP handles split UTF-8, coalesced lines and peer half-close", async () => {
  const f = await fixture();
  const first = f.host.call(target, "unary", {});
  const second = f.host.call(target, "unary", {});
  const a = await f.next("call"), b = await f.next("call");
  const text = JSON.stringify({ type: "result", id: a.id, output: "é" });
  f.raw(text.slice(0, 5));
  f.raw(text.slice(5) + "\n" + JSON.stringify({ type: "result", id: b.id, output: 2 }) + "\n");
  expect(await first).toBe("é");
  expect(await second).toBe(2);
  const lost = f.host.status();
  await f.next("status");
  f.halfClose();
  await expect(lost).rejects.toMatchObject({ code: "connection_lost" });
});
test("WebSocket accepts only literal numeric loopback without credentials or tokens", async () => {
  for (const url of ["wss://127.0.0.1", "ws://localhost", "ws://192.168.1.1", "ws://127.1", "ws://2130706433", "ws://user@127.0.0.1", "ws://127.0.0.1?token=x", "ws://127.0.0.1/#x"])
    await expect(RuntimeHost.connect({ namespace: "demo", service: "sdk", instance: "one", contract: { version: "1", methods: {} }, transport: { type: "websocket", url } })).rejects.toMatchObject({ code: "invalid_input" });
});

test("raw caller definitions observe nested input/output/error mutation between calls", async () => {
  const f = await fixture();
  const definition = copyMethod(contract.methods.unary!);
  definition.input.properties!.value!.maximum = 0;
  await expect(f.host.call(target, "unary", { value: 1 }, { contract: definition })).rejects.toMatchObject({ code: "invalid_input" });
  definition.input.properties!.value!.maximum = 1;
  const accepted = f.host.call(target, "unary", { value: 1 }, { contract: definition });
  const first = await f.next("call");
  definition.output.properties!.value!.maximum = 0;
  f.send({ type: "result", id: first.id, output: { value: 1 } });
  await expect(accepted).rejects.toMatchObject({ code: "invalid_output" });
  definition.output.properties!.value!.maximum = 1;
  const business = f.host.call(target, "unary", { value: 1 }, { contract: definition });
  const second = await f.next("call");
  definition.error.properties!.value!.maximum = 0;
  f.send({ type: "error", id: second.id, code: "business", message: "safe", details: { value: 1 } });
  await expect(business).rejects.toMatchObject({ code: "invalid_output" });
});

test("raw host contract input/output/error mutations remain live between invocations", async () => {
  let business = false;
  const f = await fixture({ unary: () => {
    if (business) throw new BusinessError({ value: 1 });
    return { value: 1 };
  } });
  const definition = f.exported.methods.unary!;
  definition.input.properties!.value!.maximum = 0;
  f.invoke("input", "unary");
  expect(await f.next("error")).toMatchObject({ id: "input", code: "invalid_input" });
  definition.input.properties!.value!.maximum = 1;
  definition.output.properties!.value!.maximum = 0;
  f.invoke("output", "unary");
  expect(await f.next("error")).toMatchObject({ id: "output", code: "invalid_output" });
  definition.output.properties!.value!.maximum = 1;
  f.invoke("accepted", "unary");
  expect(await f.next("result")).toMatchObject({ id: "accepted", output: { value: 1 } });
  business = true;
  definition.error.properties!.value!.maximum = 0;
  f.invoke("business", "unary");
  expect(await f.next("error")).toMatchObject({ id: "business", code: "invalid_output" });
});

test.each(["output", "error"] as const)("raw host stream keeps its captured method but observes nested %s mutation", async (field) => {
  const f = await fixture({ stream: async function* () {
    yield { value: 1 };
    if (field === "error") throw new BusinessError({ value: 1 });
    yield { value: 1 };
    yield { value: 1 };
  } });
  const captured = f.exported.methods.stream!;
  f.invoke("live", "stream");
  f.send({ type: "credit", id: "live", credits: 1 });
  expect(await f.next("chunk")).toMatchObject({ output: { value: 1 } });
  const replacement = structuredClone(captured);
  replacement[field].properties!.value!.maximum = 0;
  f.exported.methods.stream = replacement;
  if (field === "output") {
    f.send({ type: "credit", id: "live", credits: 1 });
    expect(await f.next("chunk")).toMatchObject({ output: { value: 1 } });
    // A fresh invocation sees the replacement, unlike the existing one.
    f.invoke("replacement", "stream");
    f.send({ type: "credit", id: "replacement", credits: 1 });
    expect(await f.next("error")).toMatchObject({ id: "replacement", code: "invalid_output" });
    // Use field replacement on the captured object, not only nested mutation.
    captured.output = { ...captured.output, properties: { value: { type: "integer", maximum: 0 } } };
    f.send({ type: "credit", id: "live", credits: 1 });
    expect(await f.next("error")).toMatchObject({ id: "live", code: "invalid_output" });
  } else {
    captured.error.properties!.value!.maximum = 0;
    f.send({ type: "credit", id: "live", credits: 1 });
    expect(await f.next("error")).toMatchObject({ id: "live", code: "invalid_output" });
  }
});

test.each(["output", "error"] as const)("raw caller streams retain captured definitions and observe nested %s mutation", async (field) => {
  const f = await fixture();
  const captured = copyMethod(contract.methods.stream!);
  const options = { contract: captured };
  const stream = f.host.stream(target, "stream", { value: 1 }, options);
  const call = await f.next("call");
  options.contract = structuredClone(captured);
  options.contract.output.properties!.value!.maximum = 0;
  const first = stream.next();
  await f.next("credit");
  f.send({ type: "chunk", id: call.id, output: { value: 1 } });
  expect(await first).toEqual({ done: false, value: { value: 1 } });
  captured.input.properties!.value!.maximum = 0;
  expect(() => f.host.stream(target, "stream", { value: 1 }, { contract: captured })).toThrow();
  captured[field].properties!.value!.maximum = 0;
  const next = stream.next();
  await f.next("credit");
  f.send(field === "output"
    ? { type: "chunk", id: call.id, output: { value: 1 } }
    : { type: "error", id: call.id, code: "business", message: "safe", details: { value: 1 } });
  await expect(next).rejects.toMatchObject({ code: "invalid_output" });
  expect(await f.next("cancel")).toMatchObject({ id: call.id });
});

test("generated clients and error guards keep mutable exported method lookup semantics", async () => {
  const f = await fixture();
  const original = logsContract.methods.aggregate!;
  const replacement = structuredClone(original);
  const input = { logs: [{ level: "info", message: "synthetic", duration_ms: 1 }] };
  const failure = new RuntimeError("business", "safe", { reason: "long" });
  try {
    logsContract.methods.aggregate = replacement;
    replacement.input.properties!.logs!.maxItems = 0;
    const client = new LogsClient(f.host, target);
    await expect(client.aggregate(input)).rejects.toMatchObject({ code: "invalid_input" });
    replacement.input.properties!.logs!.maxItems = 1;
    const call = client.aggregate(input);
    const request = await f.next("call");
    replacement.output.properties!.count!.maximum = 0;
    f.send({ type: "result", id: request.id, output: { count: 1, error_count: 0, total_duration_ms: 1 } });
    await expect(call).rejects.toMatchObject({ code: "invalid_output" });
    expect(isAggregateError(failure)).toBe(true);
    replacement.error.properties!.reason!.maxLength = 1;
    expect(isAggregateError(failure)).toBe(false);
    logsContract.methods.aggregate = original;
    expect(isAggregateError(failure)).toBe(true);
  } finally {
    logsContract.methods.aggregate = original;
  }
});

test("generated streams distinguish method replacement from captured nested mutation", async () => {
  const f = await fixture();
  const original = logsContract.methods.progress!;
  const captured = copyMethod(original);
  const input = { logs: [{ level: "info", message: "synthetic", duration_ms: 1 }] };
  try {
    logsContract.methods.progress = captured;
    const client = new LogsClient(f.host, target);
    const stream = client.progress(input);
    const call = await f.next("call");
    const replacement = copyMethod(captured);
    replacement.output.properties!.processed!.maximum = 0;
    replacement.error.properties!.reason!.maxLength = 1;
    logsContract.methods.progress = replacement;
    expect(isProgressError(new RuntimeError("business", "safe", { reason: "long" }))).toBe(false);
    const first = stream.next();
    await f.next("credit");
    f.send({ type: "chunk", id: call.id, output: { processed: 1, total: 1 } });
    expect(await first).toEqual({ done: false, value: { processed: 1, total: 1 } });
    captured.output.properties!.processed!.maximum = 0;
    const second = stream.next();
    await f.next("credit");
    f.send({ type: "chunk", id: call.id, output: { processed: 1, total: 1 } });
    await expect(second).rejects.toMatchObject({ code: "invalid_output" });
    expect(await f.next("cancel")).toMatchObject({ id: call.id });
  } finally {
    logsContract.methods.progress = original;
  }
});

test("child options retain an absolute budget and cancellation after handler entry", async () => {
  let childOptions: import("@lenso/runtime-sdk").CallOptions | undefined;
  let entered!: () => void;
  const started = new Promise<void>((resolve) => { entered = resolve; });
  const f = await fixture({ unary: async (_input, context) => {
    childOptions = context.callOptions();
    expect(context.deadlineMs).toBe(childOptions.deadlineMs!);
    entered();
    await new Promise<void>((resolve) => context.signal.addEventListener("abort", () => resolve(), { once: true }));
    return { value: 1 };
  } });
  f.invoke("42", "unary", 2000);
  await started;
  await Bun.sleep(30);
  const child = f.host.call(target, "unary", { value: 1 }, childOptions);
  const frame = await f.next("call");
  expect(frame.timeout_ms).toBeLessThan(1980);
  expect(frame.timeout_ms).toBeGreaterThan(0);
  expect(frame.parent_id).toBe("42");
  expect(frame.trace_id).toBe(childOptions!.traceId!);
  expect(frame.deadlineMs).toBeUndefined();
  f.send({ type: "cancel", id: "42" });
  await expect(child).rejects.toMatchObject({ code: "cancelled" });
  expect((await f.next("cancel")).id).toBe(frame.id);
});

test("expired child budget sends no work; explicit deadline bounds unary and idle stream", async () => {
  const f = await fixture();
  await expect(f.host.call(target, "unary", { value: 1 }, { deadlineMs: performance.now() - 1 }))
    .rejects.toMatchObject({ code: "deadline_exceeded" });
  for (const deadlineMs of [NaN, Infinity])
    await expect(f.host.call(target, "unary", { value: 1 }, { deadlineMs }))
      .rejects.toMatchObject({ code: "invalid_input" });
  const call = f.host.call(target, "unary", { value: 1 }, { deadlineMs: performance.now() + 100 });
  const request = await f.next("call");
  expect(request.id).toBe("1");
  expect(request.timeout_ms).toBeLessThanOrEqual(100);
  await expect(call).rejects.toMatchObject({ code: "deadline_exceeded" });
  expect((await f.next("cancel")).id).toBe(request.id);
  const stream = f.host.stream(target, "stream", { value: 1 }, { deadlineMs: performance.now() + 100 });
  const streamRequest = await f.next("call");
  expect((await f.next("cancel")).id).toBe(streamRequest.id);
  await expect(stream.next()).rejects.toMatchObject({ code: "deadline_exceeded" });
});

test("invalid structured errors retire only their invocation and completion cancels child context", async () => {
  let failure: RuntimeError | undefined;
  let signal: AbortSignal | undefined;
  const f = await fixture({ unary: (_input, context) => {
    signal = context.signal;
    if (failure) throw failure;
    return { value: 1 };
  } });
  for (const reason of [new RuntimeError("made_up", "bad"), new RuntimeError("business", "界".repeat(400), { value: 1 }),
    new RuntimeError("invalid_output", "bad", "x".repeat(262144))]) {
    failure = reason;
    f.invoke("42", "unary");
    const response = await f.next("error");
    expect(response.code).toBe("invalid_output");
    expect(response.details).toBeUndefined();
    expect(signal!.aborted).toBe(true);
  }
  failure = undefined;
  f.invoke("43", "unary");
  expect((await f.next("result")).output).toEqual({ value: 1 });
  expect(signal!.aborted).toBe(true);
});
