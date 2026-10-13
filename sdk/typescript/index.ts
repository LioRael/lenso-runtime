import { createConnection, type Socket } from "node:net";
import { types } from "node:util";
import { type Contract, validateContract, validateSchema, validateValue } from "./schema";
export { type Contract, type Schema } from "./schema";
export class RuntimeError<E = unknown> extends Error {
  constructor(
    public readonly code: string,
    message: string,
    public readonly details?: E,
  ) {
    super(message);
    this.name = "RuntimeError";
  }
}
export class BusinessError<E> extends RuntimeError<E> {
  constructor(details: E, message = "Business operation failed") {
    super("business", message, details);
  }
}
export type Target = { namespace: string; service: string; contractVersion: string };
export type CallOptions = {
  timeoutMs?: number;
  signal?: AbortSignal;
  traceId?: string;
  parentId?: string;
  contract?: Contract["methods"][string];
};
export type HandlerContext = {
  signal: AbortSignal;
  callId: string;
  traceId: string;
  parentId?: string;
  caller: { namespace: string; service: string; instance: string };
};
export type Handler = (
  input: any,
  context: HandlerContext,
) => unknown | Promise<unknown> | AsyncIterable<unknown>;
export type HostOptions = {
  namespace: string;
  service: string;
  instance: string;
  port?: number;
  contract: Contract;
  maxConcurrency?: number;
  handlers?: Record<string, Handler>;
  ready?: boolean;
};
export type Status = {
  services: unknown[];
  calls: unknown[];
  limits: Record<string, unknown>;
  recent?: unknown[];
};
type Frame = Record<string, any>;
type Deferred<T> = { promise: Promise<T>; resolve(value: T): void; reject(error: unknown): void };
function deferred<T>(): Deferred<T> {
  let resolve!: Deferred<T>["resolve"], reject!: Deferred<T>["reject"];
  const promise = new Promise<T>((a, b) => {
    resolve = a;
    reject = b;
  });
  // Internal transport failure can reject before the caller receives its promise.
  void promise.catch(() => {});
  return { promise, resolve, reject };
}
type Pending = {
  mode: "unary" | "stream" | "status";
  contract?: Contract["methods"][string];
  result?: Deferred<any>;
  next: Deferred<IteratorResult<any>>[];
  error?: Error;
  ended: boolean;
  cleanup(): void;
};
type Invocation = {
  controller: AbortController;
  credits: number;
  stream: boolean;
  wake?: () => void;
};
const MAX_FRAME = 256 * 1024;
const error = (code: string) => new RuntimeError(code, code);
function spend(budget: { remaining: number }, bytes: number): void {
  budget.remaining -= bytes;
  if (budget.remaining < 0) throw error("invalid_input");
}
function spendString(value: string, budget: { remaining: number }): void {
  if (value.length + 2 > budget.remaining) throw error("invalid_input");
  let bytes = Buffer.byteLength(value) + 2;
  for (let i = 0; i < value.length; i++) {
    const unit = value.charCodeAt(i);
    if (unit >= 0xd800 && unit <= 0xdbff) {
      const next = value.charCodeAt(++i);
      if (!(next >= 0xdc00 && next <= 0xdfff)) throw error("invalid_input");
    } else if (unit >= 0xdc00 && unit <= 0xdfff) throw error("invalid_input");
    else if (unit === 34 || unit === 92 || [8, 9, 10, 12, 13].includes(unit)) bytes++;
    else if (unit < 32) bytes += 5;
  }
  spend(budget, bytes);
}
function validateWireValue(
  value: unknown,
  depth = 0,
  budget = { remaining: MAX_FRAME - 1 },
  omitUndefined = false,
): void {
  if (depth > 64) throw error("invalid_input");
  if (value === null) {
    spend(budget, 4);
    return;
  }
  if (typeof value === "string") {
    spendString(value, budget);
    return;
  }
  if (typeof value === "boolean") {
    spend(budget, value ? 4 : 5);
    return;
  }
  if (typeof value === "number" && Number.isFinite(value)) {
    spend(budget, JSON.stringify(value).length);
    return;
  }
  if (typeof value !== "object" || value === null || types.isProxy(value))
    throw error("invalid_input");
  if (Array.isArray(value)) {
    spend(budget, 2 + Math.max(0, value.length - 1));
    for (let i = 0; i < value.length; i++) {
      const field = Object.getOwnPropertyDescriptor(value, String(i));
      if (!field || field.get || field.set) throw error("invalid_input");
      validateWireValue(field.value, depth + 1, budget, omitUndefined);
    }
    return;
  }
  if (Object.getPrototypeOf(value) !== Object.prototype && Object.getPrototypeOf(value) !== null)
    throw error("invalid_input");
  if (Object.getOwnPropertySymbols(value).length) throw error("invalid_input");
  spend(budget, 2);
  let first = true;
  for (const key in value) {
    if (!Object.hasOwn(value, key)) continue;
    const field = Object.getOwnPropertyDescriptor(value, key)!;
    if (field.get || field.set) throw error("invalid_input");
    if (omitUndefined && field.value === undefined) continue;
    if (!first) spend(budget, 1);
    first = false;
    spendString(key, budget);
    spend(budget, 1);
    validateWireValue(field.value, depth + 1, budget, omitUndefined);
  }
}
const frameFields: Record<string, [string[], string[]]> = {
  welcome: [["version", "generation", "heartbeat_ms"], []],
  registered: [[], []],
  ready: [[], []],
  drained: [[], []],
  invoke: [["id", "method", "input", "timeout_ms", "trace_id", "caller"], ["parent_id"]],
  credit: [["id", "credits"], []],
  cancel: [["id"], []],
  result: [["id", "output"], []],
  chunk: [["id", "output"], []],
  end: [["id"], []],
  error: [
    ["code", "message"],
    ["id", "details"],
  ],
  status: [["id", "services", "calls", "limits"], ["recent"]],
};
function validateFrame(frame: Frame): void {
  if (!frame || typeof frame !== "object" || Array.isArray(frame) || typeof frame.type !== "string")
    throw error("protocol_error");
  const fields = frameFields[frame.type];
  if (
    !fields ||
    fields[0].some((key) => !Object.hasOwn(frame, key)) ||
    Object.keys(frame).some((key) => !["type", ...fields[0], ...fields[1]].includes(key))
  )
    throw error("protocol_error");
  for (const key of ["id", "method", "trace_id", "parent_id", "code", "message", "generation"])
    if (Object.hasOwn(frame, key) && typeof frame[key] !== "string") throw error("protocol_error");
  if (
    frame.type === "invoke" &&
    (!frame.caller ||
      typeof frame.caller !== "object" ||
      Object.keys(frame.caller).length !== 3 ||
      ["namespace", "service", "instance"].some((key) => typeof frame.caller[key] !== "string"))
  )
    throw error("protocol_error");
  if (
    frame.type === "status" &&
    (!frame.limits ||
      typeof frame.limits !== "object" ||
      Array.isArray(frame.limits) ||
      (frame.recent !== undefined && !Array.isArray(frame.recent)))
  )
    throw error("protocol_error");
}

/** One connection is one service instance. Disconnect never replays calls. */
export class RuntimeHost {
  private socket: Socket;
  private buffer = Buffer.alloc(0);
  private writes = 0;
  private sequence = 0;
  private pending = new Map<string, Pending>();
  private invocations = new Map<string, Invocation>();
  private executions = 0;
  private executionsIdle?: Deferred<void>;
  private handshake = deferred<void>();
  private activation?: Deferred<void>;
  private activated = false;
  private phase: "welcome" | "registered" | "ready" | "active" | "closed" = "welcome";
  private heartbeat?: ReturnType<typeof setInterval>;
  private draining?: Deferred<void>;
  private drainPromise?: Promise<void>;
  private accepting = true;
  private constructor(private options: HostOptions) {
    this.socket = createConnection({ host: "127.0.0.1", port: options.port ?? 7331 });
    this.socket.on("connect", () => {
      try {
        this.send({
          type: "hello",
          version: 1,
          namespace: options.namespace,
          service: options.service,
          instance: options.instance,
        });
      } catch (e) {
        this.fail(e as Error);
      }
    });
    this.socket.on("data", (data) => {
      try {
        this.buffer = Buffer.concat([
          this.buffer,
          typeof data === "string" ? Buffer.from(data) : data,
        ]);
        let newline: number;
        while ((newline = this.buffer.indexOf(10)) !== -1) {
          if (newline + 1 > MAX_FRAME) throw error("protocol_error");
          const text = new TextDecoder("utf-8", { fatal: true }).decode(
            this.buffer.subarray(0, newline),
          );
          this.buffer = this.buffer.subarray(newline + 1);
          this.receive(JSON.parse(text));
        }
        if (this.buffer.length >= MAX_FRAME) throw error("protocol_error");
      } catch (e) {
        this.fail(e instanceof Error ? e : error("protocol_error"));
      }
    });
    this.socket.on("error", () => this.fail(error("connection_lost")));
    this.socket.on("close", () => this.fail(error("connection_lost")));
  }
  static async connect(options: HostOptions): Promise<RuntimeHost> {
    validateWireValue(options.contract, 0, undefined, true);
    validateContract(options.contract);
    const concurrency = options.maxConcurrency ?? 8;
    if (!Number.isInteger(concurrency) || concurrency < 1 || concurrency > 64)
      throw error("invalid_input");
    const host = new RuntimeHost(options);
    const timer = setTimeout(() => host.fail(error("deadline_exceeded")), 5000);
    try {
      await host.handshake.promise;
      return host;
    } finally {
      clearTimeout(timer);
    }
  }
  private send(frame: Frame): void {
    if (this.phase === "closed") throw error("connection_lost");
    validateWireValue(frame, 0, undefined, true);
    const bytes = Buffer.from(JSON.stringify(frame) + "\n");
    if (bytes.length > MAX_FRAME) throw error("invalid_input");
    if (this.writes >= 64) throw error("overloaded");
    this.writes++;
    this.socket.write(bytes, () => {
      this.writes--;
    });
  }
  private fail(reason: Error): void {
    if (this.phase === "closed") return;
    this.phase = "closed";
    clearInterval(this.heartbeat);
    this.socket.destroy();
    this.handshake.reject(reason);
    this.activation?.reject(reason);
    this.draining?.reject(reason);
    for (const [id] of this.pending) this.finish(id, reason);
    for (const invocation of this.invocations.values()) {
      invocation.controller.abort(reason);
      invocation.wake?.();
    }
    this.invocations.clear();
  }
  private receive(frame: Frame): void {
    validateFrame(frame);
    if (frame.type === "error" && frame.id === undefined) {
      this.fail(new RuntimeError(frame.code, frame.message, frame.details));
      return;
    }
    if (this.phase !== "active") {
      if (frame.type !== this.phase) throw error("protocol_error");
      if (this.phase === "welcome") {
        if (
          frame.version !== 1 ||
          !Number.isInteger(frame.heartbeat_ms) ||
          frame.heartbeat_ms < 1 ||
          frame.heartbeat_ms > 5000
        )
          throw error("protocol_error");
        this.heartbeat = setInterval(() => {
          try {
            this.send({ type: "heartbeat" });
          } catch {}
        }, frame.heartbeat_ms);
        this.phase = "registered";
        this.send({
          type: "register",
          contract: this.options.contract,
          max_concurrency: this.options.maxConcurrency ?? 8,
        });
      } else if (this.phase === "registered") {
        if (this.options.ready === false) {
          this.phase = "active";
          this.handshake.resolve();
        } else {
          this.checkHandlers(this.options.handlers ?? {});
          this.phase = "ready";
          this.send({ type: "ready" });
        }
      } else {
        this.phase = "active";
        this.activated = true;
        this.handshake.resolve();
      }
      return;
    }
    switch (frame.type) {
      case "ready":
        if (!this.activation || this.activated) throw error("protocol_error");
        this.activated = true;
        this.activation.resolve();
        break;
      case "invoke": {
        if (
          typeof frame.id !== "string" ||
          typeof frame.method !== "string" ||
          !Number.isInteger(frame.timeout_ms) ||
          frame.timeout_ms < 1 ||
          frame.timeout_ms > 60000 ||
          typeof frame.trace_id !== "string" ||
          !frame.caller
        )
          throw error("protocol_error");
        if (this.invocations.has(frame.id)) throw error("protocol_error");
        void this.invoke(frame).catch((e) =>
          this.fail(e instanceof Error ? e : error("protocol_error")),
        );
        break;
      }
      case "credit": {
        const invocation = this.invocations.get(frame.id);
        if (!Number.isInteger(frame.credits) || frame.credits < 1 || frame.credits > 16)
          throw error("protocol_error");
        if (invocation) {
          if (!invocation.stream) throw error("protocol_error");
          invocation.credits += frame.credits;
          if (invocation.credits > 16) throw error("protocol_error");
          invocation.wake?.();
        }
        break;
      }
      case "cancel": {
        const invocation = this.invocations.get(frame.id);
        invocation?.controller.abort(error("cancelled"));
        invocation?.wake?.();
        break;
      }
      case "status": {
        const p = this.pending.get(frame.id);
        if (!p) return;
        if (
          p.mode !== "status" ||
          !Array.isArray(frame.services) ||
          !Array.isArray(frame.calls) ||
          !frame.limits
        )
          throw error("protocol_error");
        p.result!.resolve({
          services: frame.services,
          calls: frame.calls,
          limits: frame.limits,
          ...(frame.recent === undefined ? {} : { recent: frame.recent }),
        });
        this.finish(frame.id);
        break;
      }
      case "result":
      case "chunk":
      case "end":
      case "error": {
        if (typeof frame.id !== "string") throw error("protocol_error");
        const p = this.pending.get(frame.id);
        if (!p) return;
        const schema =
          frame.type === "error" && frame.code === "business"
            ? p.contract?.error
            : frame.type === "result" || frame.type === "chunk"
              ? p.contract?.output
              : undefined;
        if (schema) {
          try {
            validateValue(schema, frame.type === "error" ? frame.details : frame.output);
          } catch {
            this.finish(frame.id, error("invalid_output"));
            if (p.mode === "stream") {
              try {
                this.send({ type: "cancel", id: frame.id });
              } catch {}
            }
            break;
          }
        }
        if (frame.type === "error") {
          this.finish(frame.id, new RuntimeError(frame.code, frame.message, frame.details));
          break;
        }
        if (frame.type === "result" && p.mode === "unary" && Object.hasOwn(frame, "output")) {
          p.result!.resolve(frame.output);
          this.finish(frame.id);
        } else if (
          frame.type === "chunk" &&
          p.mode === "stream" &&
          Object.hasOwn(frame, "output")
        ) {
          const waiter = p.next.shift();
          if (!waiter) throw error("protocol_error");
          waiter.resolve({ done: false, value: frame.output });
        } else if (frame.type === "end" && p.mode === "stream") this.finish(frame.id);
        else throw error("protocol_error");
        break;
      }
      case "drained":
        if (!this.draining) throw error("protocol_error");
        this.draining.resolve();
        break;
      default:
        throw error("protocol_error");
    }
  }
  private finish(id: string, reason?: Error): void {
    const p = this.pending.get(id);
    if (!p) return;
    p.error = reason;
    p.ended = true;
    p.cleanup();
    this.pending.delete(id);
    if (reason) p.result?.reject(reason);
    for (const waiter of p.next.splice(0))
      reason ? waiter.reject(reason) : waiter.resolve({ done: true, value: undefined });
  }
  private start(
    target: Target | undefined,
    method: string,
    input: unknown,
    mode: Pending["mode"],
    options: CallOptions,
  ): [string, Pending] {
    if (this.phase === "closed") throw error("connection_lost");
    if (this.phase !== "active" || !this.accepting) throw error("not_ready");
    if (this.pending.size >= 192) throw error("overloaded");
    if (options.signal?.aborted) throw error("cancelled");
    if (mode !== "status") validateWireValue(input);
    if (options.contract) {
      if (options.contract.mode !== mode) throw error("invalid_input");
      try {
        for (const schema of [
          options.contract.input,
          options.contract.output,
          options.contract.error,
        ])
          validateSchema(schema);
        validateValue(options.contract.input, input);
      } catch {
        throw error("invalid_input");
      }
    }
    const timeout = options.timeoutMs ?? 5000;
    if (!Number.isInteger(timeout) || timeout < 1 || timeout > 60000) throw error("invalid_input");
    if (!Number.isSafeInteger(this.sequence + 1)) throw error("overloaded");
    const id = String(++this.sequence);
    const cancel = (code: string) => {
      if (!this.pending.has(id)) return;
      try {
        if (mode !== "status") this.send({ type: "cancel", id });
      } catch {
      } finally {
        this.finish(id, error(code));
      }
    };
    const abort = () => cancel("cancelled");
    const timer = setTimeout(() => cancel("deadline_exceeded"), timeout);
    const p: Pending = {
      mode,
      contract: options.contract,
      result: mode !== "stream" ? deferred() : undefined,
      next: [],
      ended: false,
      cleanup: () => {
        clearTimeout(timer);
        options.signal?.removeEventListener("abort", abort);
      },
    };
    this.pending.set(id, p);
    options.signal?.addEventListener("abort", abort, { once: true });
    try {
      this.send(
        mode === "status"
          ? { type: "status", id }
          : {
              type: "call",
              id,
              namespace: target!.namespace,
              service: target!.service,
              contract_version: target!.contractVersion,
              method,
              input,
              timeout_ms: timeout,
              trace_id: options.traceId,
              parent_id: options.parentId,
            },
      );
    } catch (e) {
      p.cleanup();
      this.pending.delete(id);
      throw e;
    }
    return [id, p];
  }
  private checkHandlers(handlers: Record<string, Handler>): void {
    const methods = this.options.contract.methods;
    if (
      Object.keys(methods).some((name) => typeof handlers[name] !== "function") ||
      Object.keys(handlers).some((name) => !Object.hasOwn(methods, name))
    )
      throw error("invalid_input");
  }
  activate(handlers?: Record<string, Handler>): Promise<void> {
    if (this.activated) return Promise.resolve();
    if (this.activation) return this.activation.promise;
    if (this.phase !== "active" || !this.accepting) return Promise.reject(error("not_ready"));
    try {
      const exports = handlers ?? this.options.handlers ?? {};
      this.checkHandlers(exports);
      this.options.handlers = exports;
      this.activation = deferred();
      this.send({ type: "ready" });
      return this.activation.promise;
    } catch (e) {
      this.activation?.reject(e);
      return Promise.reject(e);
    }
  }
  status(options: CallOptions = {}): Promise<Status> {
    try {
      return this.start(undefined, "", undefined, "status", options)[1].result!.promise;
    } catch (e) {
      return Promise.reject(e);
    }
  }
  call<T = unknown>(
    target: Target,
    method: string,
    input: unknown,
    options: CallOptions = {},
  ): Promise<T> {
    try {
      return this.start(target, method, input, "unary", options)[1].result!.promise;
    } catch (e) {
      return Promise.reject(e);
    }
  }
  stream<T = unknown>(
    target: Target,
    method: string,
    input: unknown,
    options: CallOptions = {},
  ): AsyncIterableIterator<T> {
    const [id, p] = this.start(target, method, input, "stream", options);
    return {
      [Symbol.asyncIterator]() {
        return this;
      },
      next: () => {
        if (p.error) return Promise.reject(p.error);
        if (p.ended) return Promise.resolve({ done: true, value: undefined });
        if (p.next.length >= 16) return Promise.reject(error("overloaded"));
        const waiter = deferred<IteratorResult<T>>();
        p.next.push(waiter);
        try {
          this.send({ type: "credit", id, credits: 1 });
        } catch (e) {
          this.finish(id, e as Error);
          try {
            this.send({ type: "cancel", id });
          } catch {}
        }
        return waiter.promise;
      },
      return: async () => {
        if (!p.ended) {
          try {
            this.send({ type: "cancel", id });
          } finally {
            this.finish(id);
          }
        }
        return { done: true, value: undefined };
      },
      throw: async (e) => {
        if (!p.ended) {
          try {
            this.send({ type: "cancel", id });
          } finally {
            this.finish(id, e instanceof Error ? e : error("cancelled"));
          }
        }
        throw e;
      },
    };
  }
  private async invoke(frame: Frame): Promise<void> {
    const method = this.options.contract.methods[frame.method],
      handler = this.options.handlers?.[frame.method];
    if (!this.accepting || this.executions >= (this.options.maxConcurrency ?? 8)) {
      this.send({ type: "error", id: frame.id, code: "overloaded", message: "Host is busy" });
      return;
    }
    this.executions++;
    const invocation: Invocation = {
      controller: new AbortController(),
      credits: 0,
      stream: method?.mode === "stream",
    };
    this.invocations.set(frame.id, invocation);
    const timer = setTimeout(() => {
      invocation.controller.abort(error("deadline_exceeded"));
      invocation.wake?.();
    }, frame.timeout_ms);
    const signal = invocation.controller.signal;
    const aborted = deferred<never>();
    const onAbort = () => aborted.reject(signal.reason);
    signal.addEventListener("abort", onAbort, { once: true });
    // A handler may ignore cancellation; race it so transport resources still terminate.
    const race = <T>(promise: Promise<T>) => Promise.race([promise, aborted.promise]);
    let iterator: AsyncIterator<unknown> | undefined;
    let handlerWork: Promise<unknown> | undefined;
    let localWork: Promise<unknown> = Promise.resolve();
    try {
      if (!method || !handler) throw error("not_ready");
      try {
        validateValue(method.input, frame.input);
      } catch {
        throw error("invalid_input");
      }
      handlerWork = Promise.resolve(
        handler(frame.input, {
          signal,
          callId: frame.id,
          traceId: frame.trace_id,
          parentId: frame.parent_id,
          caller: frame.caller,
        }),
      );
      localWork = handlerWork;
      const output = await race(handlerWork);
      if (method.mode === "unary") {
        try {
          validateWireValue(output);
          validateValue(method.output, output);
        } catch {
          throw error("invalid_output");
        }
        this.send({ type: "result", id: frame.id, output });
      } else {
        if (
          !output ||
          typeof (output as AsyncIterable<unknown>)[Symbol.asyncIterator] !== "function"
        )
          throw error("invalid_output");
        iterator = (output as AsyncIterable<unknown>)[Symbol.asyncIterator]();
        while (true) {
          if (signal.aborted) throw signal.reason;
          if (!invocation.credits)
            await race(
              new Promise<void>((resolve) => {
                invocation.wake = resolve;
              }),
            );
          invocation.wake = undefined;
          if (signal.aborted) throw signal.reason;
          const next = Promise.resolve(iterator.next());
          localWork = next;
          const item = await race(next);
          if (item.done) {
            this.send({ type: "end", id: frame.id });
            break;
          }
          try {
            validateWireValue(item.value);
            validateValue(method.output, item.value);
          } catch {
            throw error("invalid_output");
          }
          invocation.credits--;
          this.send({ type: "chunk", id: frame.id, output: item.value });
        }
      }
    } catch (e) {
      if (this.phase !== "closed") {
        let failure = e instanceof RuntimeError ? e : error("invalid_output");
        if (failure.code === "business") {
          try {
            validateWireValue(failure.details);
            validateValue(method.error, failure.details);
          } catch {
            failure = error("invalid_output");
          }
        }
        this.send({
          type: "error",
          id: frame.id,
          code: failure.code,
          message: failure.message,
          ...(failure.details === undefined ? {} : { details: failure.details }),
        });
      }
    } finally {
      clearTimeout(timer);
      signal.removeEventListener("abort", onAbort);
      this.invocations.delete(frame.id);
      // Transport cancellation cannot release the permit of a still-running JS Promise.
      const cleanup = Promise.resolve().then(async () => {
        let owner = iterator;
        if (!owner && method?.mode === "stream" && handlerWork) {
          const lateOutput = await handlerWork;
          if (
            lateOutput &&
            typeof (lateOutput as AsyncIterable<unknown>)[Symbol.asyncIterator] === "function"
          ) {
            owner = (lateOutput as AsyncIterable<unknown>)[Symbol.asyncIterator]();
          }
        }
        await owner?.return?.();
      });
      void Promise.allSettled([localWork, cleanup]).then(() => {
        this.executions--;
        if (this.executions === 0) this.executionsIdle?.resolve();
      });
    }
  }
  drain(timeoutMs = 5000): Promise<void> {
    return (this.drainPromise ??= this.performDrain(timeoutMs));
  }
  private async performDrain(timeoutMs: number): Promise<void> {
    if (!Number.isInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > 60000)
      throw error("invalid_input");
    if (this.phase === "closed" && this.executions === 0) return;
    if (this.phase !== "active" && this.phase !== "closed") throw error("not_ready");
    this.accepting = false;
    let expired = false,
      failure: unknown;
    const timer = setTimeout(() => {
      expired = true;
      this.executionsIdle?.reject(error("deadline_exceeded"));
      this.fail(error("deadline_exceeded"));
    }, timeoutMs + 100);
    try {
      if (this.phase === "active") {
        this.draining = deferred();
        try {
          this.send({ type: "drain", timeout_ms: timeoutMs });
          await this.draining.promise;
        } catch (e) {
          failure = e;
        }
      }
      if (this.executions > 0) {
        if (expired) throw error("deadline_exceeded");
        this.executionsIdle ??= deferred();
        await this.executionsIdle.promise;
      }
      if (failure) throw failure;
    } finally {
      clearTimeout(timer);
    }
  }
  close(): void {
    if (this.phase === "closed") return;
    try {
      this.send({ type: "unregister" });
    } finally {
      this.fail(error("connection_lost"));
    }
  }
}
