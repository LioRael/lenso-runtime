import { afterEach, expect, test } from "bun:test";
import { createServer, type Socket } from "node:net";
import { BusinessError, RuntimeHost, type Handler } from "@lenso/runtime-sdk";
import { type Contract, type Schema, validateValue } from "./schema";
import { generate } from "../../tools/generate";
import { LogsClient } from "../../examples/generated/logs";
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
const cleanup: (() => void)[] = [];
afterEach(() => {
  for (const close of cleanup.splice(0)) close();
});
async function fixture(handlers: Record<string, Handler> = {}, maxConcurrency = 8, ready = true) {
  let peer!: Socket,
    buffer = "";
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
        if (frame.type === "hello")
          send({ type: "welcome", version: 1, generation: "1", heartbeat_ms: 20 });
        else if (frame.type === "register") send({ type: "registered" });
        else if (frame.type === "ready") send({ type: "ready" });
        else if (frame.type === "heartbeat") continue;
        else {
          const waiter = waiters.findIndex((w) => w.type === frame.type);
          if (waiter >= 0) waiters.splice(waiter, 1)[0]!.resolve(frame);
          else frames.push(frame);
        }
      }
    });
  });
  const send = (frame: any) => peer.write(JSON.stringify(frame) + "\n");
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  cleanup.push(() => {
    peer?.destroy();
    server.close();
  });
  const exported = {
    version: "1",
    methods: Object.fromEntries(
      Object.keys(handlers).map((name) => [name, contract.methods[name]!]),
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
    port: (server.address() as any).port,
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
  return { host, next, send, invoke, frames, disconnect: () => peer.destroy() };
}
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
test("failed stream cancellation closes the connection rather than orphaning a route", async () => {
  const f = await fixture();
  const unrelated = f.host.call(target, "unary", {});
  await f.next("call");
  const client = new LogsClient(f.host, target);
  const stream = client.progress({ logs: [{ level: "info", message: "synthetic", duration_ms: 1 }] });
  const call = await f.next("call");
  const next = stream.next();
  await f.next("credit");
  (f.host as unknown as { writes: number }).writes = 64;
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
