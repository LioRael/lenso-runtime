import { expect, test } from "bun:test";
import { PassThrough, Writable } from "node:stream";
import { connectTransport, lineTransport, MAX_PAYLOAD, websocketTransport } from "./transport";
import { EventEmitter } from "node:events";
import { WebSocketServer } from "./websocket";

test("line transport bounds split frames and outstanding writes, fails on write errors", async () => {
  const input = new PassThrough();
  const callbacks: ((reason?: Error | null) => void)[] = [];
  const output = new Writable({ write(_bytes, _encoding, callback) { callbacks.push(callback); } });
  const messages: string[] = [], failures: Error[] = [];
  const events = {
    open() {},
    message(text: string) { messages.push(text); },
    fail(reason: Error) { failures.push(reason); transport.close(); },
    error(code: string) { return new Error(code); },
  };
  const transport = lineTransport(input, output, events);
  const bytes = Buffer.from('"é"\n{}\n');
  input.write(bytes.subarray(0, 2));
  input.write(bytes.subarray(2));
  expect(messages).toEqual(['"é"', "{}"]);
  for (let i = 0; i < 64; i++) transport.send("{}");
  expect(() => transport.send("{}")).toThrow("overloaded");
  callbacks[0]!(new Error("broken pipe"));
  await Bun.sleep(1);
  expect(failures[0]?.message).toBe("connection_lost");
  expect(() => transport.send("{}")).toThrow("connection_lost");
});

test("line transport accepts exact payload budget and rejects larger incomplete frames", () => {
  const input = new PassThrough(), output = new PassThrough();
  const messages: string[] = [];
  let failure: Error | undefined;
  const transport = lineTransport(input, output, {
    open() {}, message(text) { messages.push(text); },
    fail(reason) { failure = reason; transport.close(); },
    error(code) { return new Error(code); },
  });
  input.write("x".repeat(MAX_PAYLOAD));
  input.write("\n");
  expect(messages[0]?.length).toBe(MAX_PAYLOAD);
  input.write("x".repeat(MAX_PAYLOAD));
  input.write("x");
  expect(failure?.message).toBe("protocol_error");
});

test("line transport assembles byte fragments and preserves empty records and frame boundaries", () => {
  const input = new PassThrough(), output = new PassThrough();
  const messages: string[] = [];
  const failures: Error[] = [];
  const transport = lineTransport(input, output, {
    open() {}, message(text) { messages.push(text); },
    fail(reason) { failures.push(reason); transport.close(); },
    error(code) { return new Error(code); },
  });
  const bytes = Buffer.from("😀\n\n{}\n");
  for (const byte of bytes) input.write(Buffer.from([byte]));
  expect(messages).toEqual(["😀", "", "{}"]);
  expect(failures).toEqual([]);
  input.write(Buffer.from([0xff, 0x0a]));
  input.write("{}\n");
  expect(failures.map(error => error.message)).toEqual(["protocol_error"]);
  expect(messages).toEqual(["😀", "", "{}"]);
});

test("fragment assembly grows geometrically with bounded copy work and reuses only initialized bytes", () => {
  const input = Object.assign(new EventEmitter(), { pause() {} }) as unknown as PassThrough;
  const output = new PassThrough();
  const messages: string[] = [];
  const capacities: number[] = [];
  let copied = 0;
  const originalAllocUnsafe = Buffer.allocUnsafe;
  const originalCopy = Buffer.prototype.copy;
  (Buffer as any).allocUnsafe = (size: number) => {
    capacities.push(size);
    return originalAllocUnsafe(size).fill(0x78);
  };
  Buffer.prototype.copy = function(this: Buffer, target: Uint8Array, targetStart?: number, sourceStart?: number, sourceEnd?: number) {
    copied += (sourceEnd ?? this.length) - (sourceStart ?? 0);
    return originalCopy.call(this, target, targetStart, sourceStart, sourceEnd);
  };
  let transport = lineTransport(input, output, {
    open() {}, message(text) { messages.push(text); },
    fail(reason) { throw reason; },
    error(code) { return new Error(code); },
  });
  try {
    input.emit("data", "a".repeat(100));
    input.emit("data", "\n");
    expect(messages).toEqual(["a".repeat(100)]);
    input.emit("data", "y");
    input.emit("data", "\n");
    expect(messages.at(-1)).toBe("y");
    transport.close();
    transport = lineTransport(input, output, {
      open() {}, message(text) { messages.push(text); },
      fail(reason) { throw reason; },
      error(code) { return new Error(code); },
    });
    capacities.length = 0;
    copied = 0;
    const byte = Buffer.from([0x61]);
    for (let i = 0; i < MAX_PAYLOAD; i++) input.emit("data", byte);
    input.emit("data", "\n");
    expect(messages.at(-1)).toBe("a".repeat(MAX_PAYLOAD));
    expect(capacities).toEqual([64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536, 131072, MAX_PAYLOAD]);
    expect(copied).toBeLessThan(2 * MAX_PAYLOAD);
    const allocationsAfterFragmentedFrame = capacities.length;
    const copiesAfterFragmentedFrame = copied;
    input.emit("data", "z\n");
    expect(messages.at(-1)).toBe("z");
    expect(capacities).toHaveLength(allocationsAfterFragmentedFrame);
    expect(copied).toBe(copiesAfterFragmentedFrame);
  } finally {
    transport.close();
    Buffer.prototype.copy = originalCopy;
    (Buffer as any).allocUnsafe = originalAllocUnsafe;
  }
});

test("line framing failure closes once even when the failure callback does not close", () => {
  const input = new PassThrough(), output = new PassThrough();
  const failures: Error[] = [];
  const transport = lineTransport(input, output, {
    open() {}, message() {},
    fail(reason) { failures.push(reason); },
    error(code) { return new Error(code); },
  });
  input.write(Buffer.from([0xff, 0x0a]));
  input.write("{}\n");
  input.end();
  expect(failures.map(error => error.message)).toEqual(["protocol_error"]);
  expect(input.listenerCount("data")).toBe(0);
  expect(() => transport.send("{}")).toThrow("connection_lost");
});

test("line transport closes a stalled writer at the bounded write deadline", async () => {
  const input = new PassThrough();
  const output = new Writable({ write(_chunk, _encoding, _callback) {} });
  let transport: ReturnType<typeof lineTransport>;
  let failed!: (reason: Error) => void;
  const failure = new Promise<Error>(resolve => { failed = resolve; });
  transport = lineTransport(input, output, {
    open() {}, message() {},
    fail(reason) { transport.close(); failed(reason); },
    error: code => new Error(code),
  });
  const started = performance.now();
  try {
    transport.send("{}");
    expect((await failure).message).toBe("connection_lost");
    expect(performance.now() - started).toBeGreaterThanOrEqual(4900);
  } finally {
    transport.close();
    input.destroy();
    output.destroy();
  }
}, 7000);

test("WebSocket parser rejects oversized fragmented text before delivering a message", async () => {
  const server = new WebSocketServer({ host: "127.0.0.1", port: 0, perMessageDeflate: false });
  await new Promise<void>((resolve, reject) => {
    server.once("listening", resolve);
    server.once("error", reject);
  });
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("Missing server port");
  server.on("connection", peer => {
    peer.send("x".repeat(200 * 1024), { binary: false, fin: false });
    peer.send("x".repeat(70 * 1024), { binary: false, fin: true });
  });
  let messages = 0, fail!: (reason: Error) => void;
  const failed = new Promise<Error>(resolve => { fail = resolve; });
  const transport = connectTransport({ type: "websocket", url: `ws://127.0.0.1:${address.port}` }, undefined, {
    open() {},
    message() { messages++; },
    fail,
    error: code => new Error(code),
  });
  try {
    expect((await failed).message).toBe("protocol_error");
    expect(messages).toBe(0);
  } finally {
    transport.close();
    for (const peer of server.clients) peer.terminate();
    await new Promise<void>((resolve, reject) => server.close(reason => reason ? reject(reason) : resolve()));
  }
});

test("WebSocket boundary sends text without newline, bounds buffered sends and fails closed", () => {
  class FakeWebSocket extends EventEmitter {
    readyState = 1;
    texts: string[] = [];
    callbacks: ((reason?: Error) => void)[] = [];
    terminated = false;
    send(text: string, callback: (reason?: Error) => void) { this.texts.push(text); this.callbacks.push(callback); }
    close() { this.readyState = 3; }
    terminate() { this.terminated = true; this.readyState = 3; }
  }
  const socket = new FakeWebSocket();
  let transport: ReturnType<typeof connectTransport>;
  const failures: Error[] = [];
  const events = {
    open() {}, message(_text: string) {},
    fail(reason: Error) { failures.push(reason); transport.close(); },
    error(code: string) { return new Error(code); },
  };
  transport = websocketTransport(socket as any, events);
  try {
    transport.send("{}");
    expect(socket.texts).toEqual(["{}"]);
    for (let i = 1; i < 64; i++) transport.send("{}");
    expect(() => transport.send("{}")).toThrow("overloaded");
    expect(socket.texts).toHaveLength(64);
    for (const callback of socket.callbacks) callback();
    socket.callbacks = [];
    transport.send("x".repeat(MAX_PAYLOAD));
    expect(() => transport.send("x".repeat(MAX_PAYLOAD + 1))).toThrow("invalid_input");
    socket.callbacks.at(-1)!();
    for (let i = 0; i < 64; i++) transport.send("x".repeat(MAX_PAYLOAD));
    expect(() => transport.send("{}")).toThrow("overloaded");
    socket.emit("message", Buffer.from([0xff]), false);
    expect(failures[0]?.message).toBe("protocol_error");
    expect(socket.terminated).toBe(true);
    expect(() => transport.send("{}")).toThrow("connection_lost");
    const failedWriter = new FakeWebSocket();
    transport = websocketTransport(failedWriter as any, events);
    transport.send("{}");
    failedWriter.callbacks[0]!(new Error("write failed"));
    expect(failures[1]?.message).toBe("connection_lost");
    expect(failedWriter.terminated).toBe(true);
  } finally { transport.close(); }
});

for (const mode of ["eof", "oversized"]) {
  test(`persistent stdio protocol: framing, invocation, correlation and ${mode} fail closed`, async () => {
    const child = Bun.spawn([process.execPath, new URL("./stdio-peer.ts", import.meta.url).pathname, mode], { stdin: "pipe", stdout: "pipe", stderr: "pipe" });
    const reader = child.stdout.getReader();
    const decoder = new TextDecoder();
    let buffer = "", calls: any[] = [], seen = 0;
    const send = (frame: any) => { child.stdin.write(JSON.stringify(frame) + "\n"); child.stdin.flush(); };
    try {
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });
        let newline: number;
        while ((newline = buffer.indexOf("\n")) >= 0) {
          const frame = JSON.parse(buffer.slice(0, newline));
          buffer = buffer.slice(newline + 1);
          if (frame.type === "hello") {
            const welcome = JSON.stringify({ type: "welcome", version: 1, generation: "one", heartbeat_ms: 1000 });
            child.stdin.write(welcome.slice(0, 7));
            child.stdin.write(welcome.slice(7) + "\n");
            child.stdin.flush();
          } else if (frame.type === "register") send({ type: "registered" });
          else if (frame.type === "ready") {
            send({ type: "ready" });
            send({ type: "invoke", id: "incoming", method: "echo", input: 7, timeout_ms: 1000, trace_id: "trace", caller: { namespace: "test", service: "peer", instance: "one" } });
          } else if (frame.type === "result") {
            expect(frame).toMatchObject({ id: "incoming", output: 7 });
            seen++;
          } else if (frame.type === "call") {
            calls.push(frame);
            if (calls.length === 2) {
              const bytes = Buffer.from(JSON.stringify({ type: "result", id: calls[1].id, output: 2 }) + "\n" + JSON.stringify({ type: "result", id: calls[0].id, output: "é" }) + "\n");
              const split = bytes.indexOf(Buffer.from("é")) + 1;
              child.stdin.write(bytes.subarray(0, split));
              child.stdin.write(bytes.subarray(split));
              child.stdin.flush();
            }
          } else if (frame.type === "status") {
            if (mode === "oversized") {
              child.stdin.write(" ".repeat(MAX_PAYLOAD + 1));
              child.stdin.flush();
            } else child.stdin.end();
          } else expect(frame.type).toBe("heartbeat");
        }
      }
      expect(buffer).toBe("");
      expect(calls).toHaveLength(2);
      expect(seen).toBe(1);
      expect(await child.exited).toBe(0);
      expect(await new Response(child.stderr).text()).toBe("");
    } finally {
      child.kill();
    }
  });
}
