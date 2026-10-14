import assert from "node:assert/strict";
import { mkdir, mkdtemp, rm } from "node:fs/promises";
import { join } from "node:path";
import { RuntimeHost } from "@lenso/runtime-sdk";
import { LogsClient } from "../examples/generated/logs";
import { emptyContract } from "./local";
import { processConfig, root, startTransport } from "../examples/multitransport/local";

const logs = [
  { level: "info", message: "synthetic startup", duration_ms: 3 },
  { level: "error", message: "synthetic failure", duration_ms: 7 },
];
const expected = { count: 2, error_count: 1, total_duration_ms: 10 };
async function expectCode(run: () => Promise<unknown>, code: string) {
  await assert.rejects(run, (error: unknown) =>
    typeof error === "object" && error !== null && "code" in error && error.code === code);
}
async function settled(host: RuntimeHost) {
  const deadline = performance.now() + 2000;
  while ((await host.status()).calls.length) {
    if (performance.now() > deadline) throw new Error("Routes did not retire");
    await Bun.sleep(10);
  }
}
function alive(pid: number) {
  try { process.kill(pid, 0); return true; }
  catch (error) {
    if (typeof error === "object" && error !== null && "code" in error && error.code === "ESRCH")
      return false;
    throw error;
  }
}
async function readPid(path: string) {
  const deadline = performance.now() + 1000;
  while (!(await Bun.file(path).exists())) {
    if (performance.now() > deadline) throw new Error(`PID file missing: ${path}`);
    await Bun.sleep(10);
  }
  const pid = Number(await Bun.file(path).text());
  assert.ok(Number.isInteger(pid) && pid > 0);
  return pid;
}
await mkdir(join(root, "target"), { recursive: true });
const directory = await mkdtemp(join(root, "target", "lenso-smoke-"));
const rustPidFile = join(directory, "rust.pid");
const managedPidFile = join(directory, "managed.pid");
const tcpPidFile = join(directory, "tcp.pid");
const wsPidFile = join(directory, "ws.pid");
const extra = [
  processConfig(process.execPath, ["examples/multitransport/tcp-service.ts"], "managed-tcp", "tcp",
    { TRANSPORT_PID_FILE: managedPidFile }),
];
const independent = [
  processConfig(process.execPath, ["examples/multitransport/tcp-service.ts"], "bun-tcp", "tcp",
    { TRANSPORT_PID_FILE: tcpPidFile, LENSO_RUNTIME_FANOUT_SERVICES: "batch,bun-websocket" }),
  processConfig(process.execPath, ["examples/multitransport/websocket-service.ts"], "bun-websocket", "tcp",
    { TRANSPORT_PID_FILE: wsPidFile }),
];
let ws: RuntimeHost | undefined;
let runtime: Awaited<ReturnType<typeof startTransport>> | undefined;
const checks: string[] = [];
try {
  runtime = await startTransport("stdio", extra, independent, { TRANSPORT_PID_FILE: rustPidFile });
  const rustPid = await readPid(rustPidFile);
  const managedPid = await readPid(managedPidFile);
  const tcpPid = await readPid(tcpPidFile);
  const wsPid = await readPid(wsPidFile);
  ws = await RuntimeHost.connect({
    namespace: "demo", service: "ws-caller", instance: "one",
    contract: emptyContract, transport: { type: "websocket", url: runtime.url },
  });
  const rust = new LogsClient(ws, { namespace: "demo", service: "batch" });
  const websocket = new LogsClient(runtime.host, { namespace: "demo", service: "bun-websocket" });
  const tcp = new LogsClient(ws, { namespace: "demo", service: "bun-tcp" });
  assert.deepEqual(await rust.aggregate({ logs }), expected);
  checks.push("WebSocket caller → kernel → owned stdio Rust generated handler");
  assert.deepEqual(await websocket.aggregate({ logs }), expected);
  checks.push("TCP caller → kernel → independent Bun WebSocket service");
  assert.deepEqual(await tcp.aggregate({ logs }, { traceId: "mixed-trio-fanout" }), expected);
  checks.push("WebSocket caller → kernel → independent Bun TCP service");
  const recent = (await runtime.host.status()).recent!.filter(call => call.trace_id === "mixed-trio-fanout");
  const parent = recent.find(call => call.service === "bun-tcp")!;
  for (const service of ["batch", "bun-websocket"]) {
    const nested = recent.find(call => call.service === service);
    assert.ok(nested, `Missing real nested call to ${service}`);
    assert.equal(nested.parent_id, parent.id);
  }
  checks.push("Bun TCP service → stdio Rust and WebSocket Bun with linked trace/parent IDs");
  await expectCode(() => rust.aggregate({ logs: [] }), "business");
  const delayed = rust.progress({ logs, delay_ms: 100 }, { timeoutMs: 20 });
  await expectCode(() => delayed.next(), "deadline_exceeded");
  await settled(runtime.host);
  checks.push("stdio stream deadline and real route retirement");
  const abandoned = websocket.progress({ logs, delay_ms: 10 });
  assert.equal((await abandoned.next()).value?.processed, 1);
  await abandoned.return?.();
  await settled(runtime.host);
  checks.push("WebSocket iterator cancellation and real route retirement");
  const progress = [];
  for await (const item of rust.progress({ logs })) {
    progress.push(item);
    await Bun.sleep(30);
  }
  assert.deepEqual(progress, [{ processed: 1, total: 2 }, { processed: 2, total: 2 }]);
  checks.push("slow consumer preserves ordered chunks and EOF");
  const held = [];
  const outcomes: Promise<string>[] = [];
  for (let i = 0; i < 160; i++) {
    const iterator = rust.progress({ logs, delay_ms: 1000 }, { timeoutMs: 5000 });
    held.push(iterator);
    outcomes.push(iterator.next().then(() => "settled", reason => reason.code));
    await Bun.sleep(1);
  }
  await Bun.sleep(50);
  const overloaded = await runtime.host.status();
  assert.ok(overloaded.calls.length <= 136, "one 8-slot callee plus 128 waiting routes");
  for (const iterator of held) await iterator.return?.();
  assert.ok((await Promise.all(outcomes)).includes("overloaded"));
  await settled(runtime.host);
  checks.push("stdio overload rejects excess work and cancellation settles bounded queues");
  const interrupted = websocket.progress({ logs, delay_ms: 1000 });
  const pending = interrupted.next();
  const admissionDeadline = performance.now() + 1000;
  while (!(await runtime.host.status()).calls.length) {
    if (performance.now() > admissionDeadline) throw new Error("Disconnect route was not admitted");
    await Bun.sleep(10);
  }
  process.kill(wsPid, "SIGKILL");
  await expectCode(() => pending, "connection_lost");
  await settled(runtime.host);
  await Bun.sleep(50);
  await expectCode(() => websocket.aggregate({ logs }), "service_not_found");
  checks.push("callee disconnect terminates route, no reconnect/replay");
  await ws.close();
  ws = undefined;
  await runtime.close();
  runtime = undefined;
  const deadline = performance.now() + 2000;
  while (alive(rustPid) || alive(managedPid) || alive(tcpPid) || alive(wsPid)) {
    if (performance.now() > deadline) throw new Error("Kernel-owned children survived shutdown");
    await Bun.sleep(10);
  }
  checks.push("kernel reaps owned stdio/TCP children; script reaps standalone TCP/WebSocket children");
  const failed = processConfig(process.execPath, ["-e", "process.exit(17)"], "startup-failure", "tcp");
  await assert.rejects(() => startTransport("stdio", [failed]));
  const absent = processConfig(join(directory, "nonexistent-executable"), [], "spawn-failure", "stdio");
  await assert.rejects(() => startTransport("stdio", [absent]));
  const unreadyPidFile = join(directory, "unready.pid");
  const unready = processConfig(process.execPath, ["-e",
    'await Bun.write(process.env.TRANSPORT_PID_FILE, String(process.pid)); setInterval(() => {}, 1000);'],
    "unready", "stdio", { TRANSPORT_PID_FILE: unreadyPidFile });
  unready.ready_timeout_ms = 150;
  await assert.rejects(() => startTransport("stdio", [unready]));
  assert.equal(alive(await readPid(unreadyPidFile)), false);
  checks.push("spawn failure, pre-ready exit and ready timeout abort startup and reap children");
  const crashPidFile = join(directory, "crash-rust.pid");
  runtime = await startTransport("stdio", [], [], { TRANSPORT_PID_FILE: crashPidFile });
  const crashPid = await readPid(crashPidFile);
  runtime.kernel.kill("SIGKILL");
  await runtime.kernel.exited;
  const crashDeadline = performance.now() + 2000;
  while (alive(crashPid)) {
    if (performance.now() > crashDeadline) throw new Error("Rust service survived its Runtime session");
    await Bun.sleep(10);
  }
  await runtime.close();
  runtime = undefined;
  checks.push("owned Rust stdio service observes abrupt Runtime loss and exits without replay");
  console.log(JSON.stringify({ smoke: "pass", platform: process.platform, arch: process.arch, bun: Bun.version, checks }, null, 2));
} finally {
  try { await ws?.close(); }
  finally {
    try { await runtime?.close(); }
    finally { await rm(directory, { recursive: true, force: true }); }
  }
}
