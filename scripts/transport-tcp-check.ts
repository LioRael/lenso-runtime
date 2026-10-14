import assert from "node:assert/strict";
import { resolve } from "node:path";
import { RuntimeHost } from "@lenso/runtime-sdk";
import { emptyContract, reservePort, stopProcess } from "./local";
import { measure } from "../examples/multitransport/measure";
import { root, startTransport } from "../examples/multitransport/local";
import { balancedOrders, benchmarkEnvironment, containedPath, loadReferenceRuntimeHost, pairedRatios, provenance, referenceRevision, rejectDrift, repeatCount, sha256, verifyReferenceSource, writeEvidence, type Json } from "./benchmark-evidence";

const baseline = await Bun.file(resolve(root, "examples/multitransport/baseline.json")).json();
const referenceRoot = resolve(root, "target/tcp-before-source");
const outputIndex = process.argv.indexOf("--output");
const output = outputIndex < 0 ? undefined : process.argv[outputIndex + 1];
if (outputIndex >= 0 && !output) throw new Error("Missing --output path");
if (output) containedPath(resolve(root, "examples/multitransport"), output);
if (process.argv.includes("--baseline")) throw new Error("The historical baseline is protected");
const repeatIndex = process.argv.indexOf("--repeat");
if (repeatIndex >= 0 && !process.argv[repeatIndex + 1]) throw new Error("Missing --repeat count");
const count = repeatCount(repeatIndex < 0 ? undefined : process.argv[repeatIndex + 1]);
const revision = referenceRevision(process.argv, baseline.revision);
const reference = await verifyReferenceSource(root, referenceRoot, revision, baseline);
const before = { current: await provenance(root), original: await provenance(referenceRoot, root) };
const environment = benchmarkEnvironment();
const original = await loadReferenceRuntimeHost(referenceRoot);

async function startReference() {
  const started = performance.now(), port = await reservePort();
  const binary = (name: string) => resolve(referenceRoot, "target/release", name);
  const kernel = Bun.spawn([binary("lenso-runtime"), "--listen", `127.0.0.1:${port}`], { stderr: "inherit", stdout: "ignore" });
  let batch: Bun.Subprocess | undefined;
  let host: RuntimeHost | undefined;
  const close = async () => {
    try { await host?.close(); } finally {
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
    const launch_config = {
      kernel: { binary: binary("lenso-runtime"), args: ["--listen", `127.0.0.1:${port}`] },
      batch: { binary: binary("logs-batch"), runtime_port: port },
      caller: { namespace: "demo", service: "check", instance: "original", port },
    };
    const config_sha256 = sha256(Buffer.from(JSON.stringify(launch_config)));
    return { host, startup_ms: performance.now() - started, config_sha256, launch_config, close };
  } catch (error) { await close(); throw error; }
}

const runs: ({ round: number; path: string; config_sha256: string; launch_config: Json } & Awaited<ReturnType<typeof measure>>)[] = [];
for (const [round, order] of balancedOrders(["original", "current"], count).entries()) {
  for (const path of order) {
    const runtime = path === "original" ? await startReference() : await startTransport("tcp");
    try {
      runs.push({ round, path, config_sha256: runtime.config_sha256, launch_config: runtime.launch_config, ...await measure(runtime.host, runtime.startup_ms) });
      if ("verifyConfig" in runtime && typeof runtime.verifyConfig === "function") await runtime.verifyConfig();
    }
    finally { await runtime.close(); }
  }
}
rejectDrift(before, { current: await provenance(root), original: await provenance(referenceRoot, root) });
rejectDrift(reference, await verifyReferenceSource(root, referenceRoot, revision, baseline));
const comparisons = runs[0]!.conditions.map((condition, index) => {
  const pairs = Array.from({ length: count }, (_, round) => {
    const current = runs.find(run => run.round === round && run.path === "current")!.conditions[index]!;
    const reference = runs.find(run => run.round === round && run.path === "original")!.conditions[index]!;
    assert.equal(current.requests, reference.requests);
    assert.equal(current.errors, 0);
    assert.equal(reference.errors, 0);
    return { current, reference };
  });
  return {
    records: condition.records, concurrency: condition.concurrency,
    throughput: pairedRatios(pairs.map(p => ({ current: p.current.throughput_per_second, reference: p.reference.throughput_per_second }))),
    p50: pairedRatios(pairs.map(p => ({ current: p.current.latency_ms.p50!, reference: p.reference.latency_ms.p50! }))),
    p95: pairedRatios(pairs.map(p => ({ current: p.current.latency_ms.p95!, reference: p.reference.latency_ms.p95! }))),
    p99: pairedRatios(pairs.map(p => ({ current: p.current.latency_ms.p99!, reference: p.reference.latency_ms.p99! }))),
  };
});
const report = {
  captured_at: new Date().toISOString(),
  ...reference,
  environment, environment_after: benchmarkEnvironment(),
  note: "Alternating paired v2 original/current TCP runs with equivalent build proof verification. Original uses archived kernel, Rust SDK/service and TypeScript RuntimeHost. Independent concurrency warmups; validation enabled. Startup differs and is not a speed comparison. p99 descriptive; differences below paired ratio range are inconclusive. Historical v1 results are a separate cohort. Run only after other builds/tests stop.",
  runs, comparisons,
  provenance_identity: Object.fromEntries(Object.entries(before).map(([name, proof]) => [name, {
    source_sha256: sha256(Buffer.from(JSON.stringify(proof.source))),
    application_source_sha256: sha256(Buffer.from(JSON.stringify(proof.application_source))),
    harness_sha256: sha256(Buffer.from(JSON.stringify(proof.harness))), binaries: proof.binaries,
  }])),
};
await writeEvidence(root, { ...report, manifests: before }, output);
console.log(JSON.stringify(runs.map(({ round, path, conditions }) => ({
  round, path, conditions: conditions.map(({ records, concurrency, latency_ms, throughput_per_second, errors }) =>
    ({ records, concurrency, latency_ms, throughput_per_second, errors })),
})), null, 2));
