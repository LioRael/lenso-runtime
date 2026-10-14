import { createConnection } from "node:net";
import type { Readable, Writable } from "node:stream";
import type Ws from "ws";
import { WebSocket } from "./websocket";

export type TransportOptions =
  | { type: "tcp" }
  | { type: "stdio" }
  | { type: "websocket"; url: string };
export const MAX_PAYLOAD = 256 * 1024 - 1;
const MAX_SENDS = 64;
const MAX_BUFFERED = MAX_SENDS * (MAX_PAYLOAD + 1);
export interface Transport {
  send(text: string): void;
  close(): void;
}
type Events = {
  open(): void;
  message(text: string): void;
  fail(reason: Error): void;
  error(code: string): Error;
};

/** Incremental NDJSON framing: retain at most one bounded, incomplete frame. */
export function lineTransport(input: Readable, output: Writable, events: Events, owned = false): Transport {
  const empty = Buffer.alloc(0);
  const decoder = new TextDecoder("utf-8", { fatal: true });
  let buffer = empty, used = 0, writes = 0, closed = false, framingFailed = false;
  type Write = { expires: number; done: boolean };
  const pendingWrites: Write[] = [];
  let deadline: ReturnType<typeof setTimeout> | undefined;
  const fail = (reason: Error) => {
    if (!closed) events.fail(reason);
  };
  const watchWrite = () => {
    deadline = undefined;
    const oldest = pendingWrites[0];
    if (!oldest || closed) return;
    const remaining = oldest.expires - performance.now();
    if (remaining <= 0) fail(events.error("connection_lost"));
    else deadline = setTimeout(watchWrite, remaining);
  };
  const lost = () => fail(events.error("connection_lost"));
  const closeTransport = () => {
    if (closed) return;
    closed = true;
    buffer = empty;
    used = 0;
    clearTimeout(deadline);
    deadline = undefined;
    pendingWrites.length = 0;
    input.off("data", data);
    input.off("end", lost);
    input.off("close", lost);
    input.off("error", lost);
    // Writable emits error after invoking failed write callbacks.
    if (writes === 0) setImmediate(() => output.off("error", lost));
    output.off("close", lost);
    output.off("finish", lost);
    if (owned) {
      input.destroy();
      output.destroy();
    } else input.pause();
  };
  const data = (chunk: Buffer | string) => {
    if (framingFailed) return;
    try {
      const bytes = typeof chunk === "string" ? Buffer.from(chunk) : chunk;
      let start = 0;
      while (start < bytes.length && !closed) {
        const newline = bytes.indexOf(10, start);
        const end = newline < 0 ? bytes.length : newline;
        const part = bytes.subarray(start, end);
        if (used + part.length > MAX_PAYLOAD) throw events.error("protocol_error");
        if (newline < 0) {
          if (part.length) {
            const required = used + part.length;
            if (required > buffer.length) {
              const capacity = Math.min(MAX_PAYLOAD, Math.max(64, buffer.length * 2, required));
              const grown = Buffer.allocUnsafe(capacity);
              buffer.copy(grown, 0, 0, used);
              buffer = grown;
            }
            part.copy(buffer, used);
            used = required;
          }
          break;
        }
        let text: string;
        if (used === 0) {
          text = decoder.decode(part);
        } else {
          if (part.length) {
            const required = used + part.length;
            if (required > buffer.length) {
              const capacity = Math.min(MAX_PAYLOAD, Math.max(64, buffer.length * 2, required));
              const grown = Buffer.allocUnsafe(capacity);
              buffer.copy(grown, 0, 0, used);
              buffer = grown;
            }
            part.copy(buffer, used);
            used = required;
          }
          text = decoder.decode(buffer.subarray(0, used));
        }
        used = 0;
        events.message(text);
        start = newline + 1;
      }
    } catch (reason) {
      framingFailed = true;
      buffer = empty;
      used = 0;
      const failure = reason instanceof Error && reason.message === "protocol_error" ? reason : events.error("protocol_error");
      closeTransport();
      events.fail(failure);
    }
  };
  input.on("data", data);
  input.on("end", lost);
  input.on("close", lost);
  input.on("error", lost);
  output.on("error", lost);
  output.on("close", lost);
  output.on("finish", lost);
  return {
    send(text) {
      if (closed) throw events.error("connection_lost");
      if (Buffer.byteLength(text) > MAX_PAYLOAD) throw events.error("invalid_input");
      if (writes >= MAX_SENDS) throw events.error("overloaded");
      writes++;
      const record = { expires: performance.now() + 5000, done: false };
      pendingWrites.push(record);
      deadline ??= setTimeout(watchWrite, 5000);
      const release = () => {
        if (record.done) return;
        record.done = true;
        writes--;
        const index = pendingWrites.indexOf(record);
        if (index >= 0) pendingWrites.splice(index, 1);
        if (!pendingWrites.length) {
          clearTimeout(deadline);
          deadline = undefined;
        }
      };
      try {
        output.write(text + "\n", (reason) => {
          release();
          if (reason) fail(events.error("connection_lost"));
          if (closed && writes === 0) setImmediate(() => output.off("error", lost));
        });
      } catch {
        release();
        const reason = events.error("connection_lost");
        fail(reason);
        throw reason;
      }
    },
    close: closeTransport,
  };
}

function websocketUrl(value: string, events: Events): string {
  // Validate the literal authority too: URL parsing normalizes nonnumeric/short IPv4 aliases.
  const match = /^ws:\/\/(\[::1\]|127\.(?:0|[1-9]\d{0,2})\.(?:0|[1-9]\d{0,2})\.(?:0|[1-9]\d{0,2}))(?::\d+)?(?:\/[^?#]*)?$/.exec(value);
  if (!match || (match[1] !== "[::1]" && match[1]!.split(".").some((part) => +part > 255)))
    throw events.error("invalid_input");
  try {
    return new URL(value).href;
  } catch {
    throw events.error("invalid_input");
  }
}

export function connectTransport(options: TransportOptions | undefined, port: number | undefined, events: Events): Transport {
  if (!options || options.type === "tcp") {
    const socket = createConnection({ host: "127.0.0.1", port: port ?? 7331 });
    socket.setNoDelay(false);
    const transport = lineTransport(socket, socket, events, true);
    socket.once("connect", events.open);
    return transport;
  }
  if (options.type === "stdio") {
    const transport = lineTransport(process.stdin, process.stdout, events);
    queueMicrotask(events.open);
    return transport;
  }
  if (options.type !== "websocket") throw events.error("invalid_input");
  const socket = new WebSocket(websocketUrl(options.url, events), { maxPayload: MAX_PAYLOAD });
  return websocketTransport(socket, events);
}

type WebSocketLike = Pick<Ws, "readyState" | "send" | "close" | "terminate" | "on" | "removeAllListeners">;

export function websocketTransport(socket: WebSocketLike, events: Events): Transport {
  let closed = false, writes = 0, buffered = 0;
  const deadlines = new Set<ReturnType<typeof setTimeout>>();
  const close = () => {
    if (closed) return;
    closed = true;
    for (const deadline of deadlines) clearTimeout(deadline);
    deadlines.clear();
    socket.removeAllListeners("open");
    socket.removeAllListeners("message");
    // Keep error/close listeners until termination: ws may emit a late error
    // when a connection is aborted before its handshake completes.
    socket.terminate();
  };
  const fail = (reason: Error) => {
    if (closed) return;
    events.fail(reason);
    close();
  };
  socket.on("open", events.open);
  socket.on("error", (reason: Error & { code?: string }) => fail(events.error(
    ["WS_ERR_UNSUPPORTED_MESSAGE_LENGTH", "WS_ERR_INVALID_UTF8"].includes(reason.code ?? "")
      ? "protocol_error" : "connection_lost",
  )));
  socket.on("close", () => fail(events.error("connection_lost")));
  socket.on("message", (data, isBinary) => {
    try {
      if (isBinary || (typeof data !== "string" && !Buffer.isBuffer(data)))
        throw events.error("protocol_error");
      const bytes = typeof data === "string" ? Buffer.from(data) : data;
      if (bytes.length > MAX_PAYLOAD) throw events.error("protocol_error");
      events.message(new TextDecoder("utf-8", { fatal: true }).decode(bytes));
    } catch (reason) {
      fail(reason instanceof Error && reason.message === "protocol_error" ? reason : events.error("protocol_error"));
    }
  });
  return {
    send(text) {
      if (closed || socket.readyState !== WebSocket.OPEN) {
        const reason = events.error("connection_lost");
        fail(reason);
        throw reason;
      }
      const bytes = Buffer.byteLength(text);
      if (bytes > MAX_PAYLOAD) throw events.error("invalid_input");
      if (writes >= MAX_SENDS || buffered + bytes > MAX_BUFFERED)
        throw events.error("overloaded");
      writes++;
      buffered += bytes;
      let done = false;
      const release = (reason?: Error) => {
        if (done) return;
        done = true;
        writes--;
        buffered -= bytes;
        clearTimeout(deadline);
        deadlines.delete(deadline);
        if (reason) fail(events.error("connection_lost"));
      };
      const deadline = setTimeout(() => release(events.error("connection_lost")), 5000);
      deadlines.add(deadline);
      deadline.unref();
      try {
        socket.send(text, (reason) => release(reason ?? undefined));
      } catch {
        release();
        const reason = events.error("connection_lost");
        fail(reason);
        throw reason;
      }
    },
    close,
  };
}
