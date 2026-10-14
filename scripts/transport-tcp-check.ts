import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { resolve } from "node:path";
import { RuntimeHost } from "@lenso/runtime-sdk";
import { emptyContract, reservePort, stopProcess } from "./local";
import { measure } from "../examples/multitransport/measure";
import { root, startTransport } from "../examples/multitransport/local";

const baseline = await Bun.file(resolve(root, "examples/multitransport/baseline.json")).json();
const referenceRoot = resolve(root, "target/tcp-before-source");
for (const [path, hash] of Object.entries(baseline.source_sha256)) {
  const bytes = Buffer.from(await Bun.file(resolve(referenceRoot, path)).arrayBuffer());
  assert.equal(createHash("sha256").update(bytes).digest("hex"), hash, `Original source mismatch: ${path}`);
}
const original: { RuntimeHost: typeof RuntimeHost } = await import(resolve(referenceRoot, "sdk/typescript/index.ts"));

async function startReference() {
  const started = performance.now(), port = await reservePort();
  const binary = (name: string) => resolve(referenceRoot, "target/release", name);
  const kernel = Bun.spawn([binary("lenso-runtime"), "--listen", `127.0.0.1:${port}`], { stderr: "inherit", stdout: "ignore" });
  let batch: Bun.Subprocess | undefined;
  let host: RuntimeHost | undefined;
  const close = async () => {
    try { host?.close(); } finally {
      try { if (batch) await stopProcess(batch); } finally { await stopProcess(kernel); }
    }
  };
  try {
    const deadline = performance.now() + 5000;
    while (!host) {
      if (kernel.exitCode !== null) throw new Error("Original kernel startup failed");
      try {
        host = await original.RuntimeHost.connect({ namespace: "demo", service: "check", instance: "original", port, contract: emptyContract });
      } catch (error) {
        if (performance.now() >= deadline) throw error;
        await Bun.sleep(10);
      }
    }
    batch = Bun.spawn([binary("logs-batch")], {
      env: { ...process.env, LENSO_RUNTIME_PORT: String(port) }, stderr: "inherit", stdout: "ignore",
    });
    while (!(await host.status()).services.some(s => s.service === "batch" && s.state === "ready")) {
      if (batch.exitCode !== null) throw new Error("Original service startup failed");
      if (performance.now() >= deadline) throw new Error("Original service not ready");
      await Bun.sleep(10);
    }
    return { host, startup_ms: performance.now() - started, close };
  } catch (error) { await close(); throw error; }
}

const runs = [];
for (let round = 0; round < 3; round++) {
  for (const path of round % 2 ? ["current", "original"] : ["original", "current"]) {
    const runtime = path === "original" ? await startReference() : await startTransport("tcp");
    try { runs.push({ round, path, ...await measure(runtime.host, runtime.startup_ms) }); }
    finally { await runtime.close(); }
  }
}
const output = resolve(root, "examples/multitransport/tcp-regression-check.json");
const previous = await Bun.file(output).exists() ? await Bun.file(output).json() : undefined;
const report = {
  captured_at: new Date().toISOString(),
  reference_revision: baseline.revision,
  reference_source_sha256: baseline.source_sha256,
  note: "Three alternating original/current TCP runs. Original uses archived kernel, Rust SDK, Rust service and TypeScript RuntimeHost, not a native/process-labelled proxy. Same measure() payloads, generated client checks, 50 warmups and 300 requests per condition. Startup methods differ and are not a speed comparison. No concurrent builds/tests.",
  runs,
  before_optimization: previous?.before_optimization ?? previous?.runs,
};
await Bun.write(output, JSON.stringify(report, null, 2) + "\n");
console.log(JSON.stringify(runs.map(({ round, path, conditions }) => ({
  round, path, conditions: conditions.map(({ records, concurrency, latency_ms, throughput_per_second, errors }) =>
    ({ records, concurrency, latency_ms, throughput_per_second, errors })),
})), null, 2));
