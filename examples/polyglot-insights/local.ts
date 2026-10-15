import { RuntimeHost, RuntimeError, type HandlerContext } from "@lenso/runtime-sdk";
import { mkdir } from "node:fs/promises";
import { resolve } from "node:path";
import { stopProcess } from "../../scripts/local";
import { gatewayContract } from "./generated/gateway";

export const root = resolve(import.meta.dir, "../..");
export const example = import.meta.dir;
export type Event = Record<string, any>;

export class Child {
  process: Bun.Subprocess;
  tail = "";
  finished: Promise<void>;
  constructor(public name: string, command: string[], env = process.env) {
    this.process = Bun.spawn(command, { cwd: root, env, stdout: "pipe", stderr: "pipe" });
    const read = async (stream: ReadableStream<Uint8Array>) => {
      const decoder = new TextDecoder();
      for await (const bytes of stream) this.tail = (this.tail + decoder.decode(bytes, { stream: true })).slice(-65536);
      this.tail = (this.tail + decoder.decode()).slice(-65536);
    };
    this.finished = Promise.all([read(this.process.stdout as ReadableStream<Uint8Array>),
      read(this.process.stderr as ReadableStream<Uint8Array>), this.process.exited]).then(() => {});
  }
  events(): Event[] {
    return this.tail.split("\n").flatMap(line => { try { return [JSON.parse(line)]; } catch { return []; } });
  }
  check() {
    if (this.process.exitCode !== null || this.process.signalCode !== null)
      throw new Error(`${this.name} exited (${this.process.exitCode ?? this.process.signalCode})\n${this.tail}`);
  }
  async stop() {
    await stopProcess(this.process);
    await this.finished;
  }
}

export async function until(label: string, check: () => boolean | Promise<boolean>, ms = 5000) {
  const deadline = Date.now() + ms;
  do {
    if (await check()) return;
    await Bun.sleep(15);
  } while (Date.now() < deadline);
  throw new Error(`Timed out: ${label}`);
}

export class LocalInsights {
  namespace = `insights-${crypto.randomUUID()}`;
  children: Child[] = [];
  gateway!: RuntimeHost;
  kernel!: Child;
  metrics!: Child;
  cleaner!: Child;
  port = 0;
  callbacks: Event[] = [];
  policyDelayMs = 0;
  private closePromise?: Promise<void>;
  directory: string;
  constructor(public python: string, public pythonPath: string) {
    this.directory = resolve(root, "target/polyglot-insights", this.namespace);
  }
  spawn(name: string, command: string[], env = process.env) {
    if (this.closePromise) throw new Error("Application is closing");
    const child = new Child(name, command, env);
    this.children.push(child);
    return child;
  }
  async start() {
    if (this.closePromise) throw new Error("Cannot start a closed application");
    await mkdir(this.directory, { recursive: true });
    try {
      this.kernel = this.spawn("kernel", [resolve(root, "target/debug/lenso-runtime"), "--listen", "127.0.0.1:0"]);
      await until("kernel bound loopback", () => {
        this.kernel.check();
        const event = this.kernel.events().find(event => event.event === "runtime_listening");
        if (!event) return false;
        this.port = Number(event.address.split(":").at(-1));
        return this.port > 0;
      });
      this.gateway = await RuntimeHost.connect({ namespace: this.namespace, service: "gateway", instance: "ts-one",
        port: this.port, contract: gatewayContract, handlers: {
          policy: async (_input: unknown, context: HandlerContext) => {
            this.callbacks.push({ event: "policy_started", caller: context.caller, call_id: context.callId,
              trace_id: context.traceId, parent_id: context.parentId });
            try {
              if (this.policyDelayMs) await new Promise<void>((done, reject) => {
                const cancel = () => { clearTimeout(timer); reject(new RuntimeError("cancelled", "Policy cancelled")); };
                const timer = setTimeout(() => { context.signal.removeEventListener("abort", cancel); done(); }, this.policyDelayMs);
                context.signal.addEventListener("abort", cancel, { once: true });
                if (context.signal.aborted) cancel();
              });
              return { slow_ms: 250, services: ["api", "web", "worker"] };
            } finally {
              this.callbacks.push({ event: "policy_finished", trace_id: context.traceId, cancelled: context.signal.aborted });
            }
          },
        } });
      if (this.closePromise) { this.gateway.close(); throw new Error("Application interrupted during gateway startup"); }
      const env = { ...process.env, LENSO_RUNTIME_PORT: String(this.port), INSIGHTS_NAMESPACE: this.namespace,
        PYTHONPATH: this.pythonPath, PYTHONUNBUFFERED: "1", PYTHONDONTWRITEBYTECODE: "1" };
      this.metrics = this.spawn("metrics", [resolve(root, "target/debug/polyglot-metrics")], env);
      this.cleaner = this.spawn("cleaner", [this.python, "-W", "error", resolve(example, "cleaner.py")], env);
      await until("all service capabilities ready", async () => {
        this.children.forEach(child => child.check());
        const status = await this.gateway.status({ timeoutMs: 500 });
        return [["gateway", ["policy"]], ["metrics", ["limits", "analyze"]], ["cleaner", ["prepare", "render"]]]
          .every(([name, methods]) => status.services.some(service => service.namespace === this.namespace &&
            service.service === name && service.state === "ready" && service.transport === "tcp" &&
            (methods as string[]).every(method => service.capabilities.some(capability => capability.method === method))));
      });
      console.log(JSON.stringify({ event: "application_ready", namespace: this.namespace, port: this.port,
        languages: ["typescript", "rust", "python"], transport: "loopback-tcp" }));
      return this;
    } catch (error) {
      this.printTails();
      await this.close();
      throw error;
    }
  }
  printTails() {
    for (const child of this.children) console.error(`--- ${child.name} log tail ---\n${child.tail.slice(-12000)}`);
  }
  async idle() {
    await until("all routes and active calls retired", async () => {
      const status = await this.gateway.status({ timeoutMs: 500 });
      return status.calls.length === 0 && status.services.every(service => service.active_calls === 0);
    });
  }
  async close() {
    this.closePromise ??= this.stopAll();
    return this.closePromise;
  }
  private async stopAll() {
    // The processes own their cleanup. Await each stop; escalate only after its bound.
    const failures: unknown[] = [];
    for (const child of this.children.filter(child => child !== this.kernel).reverse()) {
      try { await child.stop(); } catch (error) { failures.push(error); }
    }
    this.gateway?.close();
    if (this.kernel) { try { await this.kernel.stop(); } catch (error) { failures.push(error); } }
    await mkdir(this.directory, { recursive: true });
    for (const child of this.children) await Bun.write(resolve(this.directory, `${child.name}.log`), child.tail);
    await Bun.write(resolve(this.directory, "callbacks.json"), JSON.stringify(this.callbacks, null, 2) + "\n");
    if (failures.length) throw new AggregateError(failures, "Child cleanup failed");
  }
}
