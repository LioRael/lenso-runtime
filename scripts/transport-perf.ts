import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { resolve, relative, isAbsolute } from "node:path";
import { cpus, platform, release, arch, loadavg } from "node:os";
import { measure } from "../examples/multitransport/measure";
import { startTransport, type TargetTransport } from "../examples/multitransport/local";

if (process.argv.includes("--baseline"))
  throw new Error("Never overwrite the preserved pre-refactor baseline with a new kernel.");
const output = process.argv.indexOf("--output");
const path = output < 0 ? undefined : process.argv[output + 1];
if (output >= 0) {
  const local = path && relative(resolve("examples/multitransport"), resolve(path));
  if (!path || !local || local.startsWith("..") || isAbsolute(local) ||
    !path.endsWith(".json") || local === "baseline.json")
    throw new Error("--output must name a non-baseline JSON file in examples/multitransport/");
}
const previous = path && await Bun.file(path).exists() ? await Bun.file(path).json() : undefined;
const baseline = await Bun.file("examples/multitransport/baseline.json").json();
const results: Record<string, Awaited<ReturnType<typeof measure>>> = {};
const select = process.argv.indexOf("--transport");
const transports = select < 0 ? ["tcp", "stdio", "websocket"] : [process.argv[select + 1]];
if (transports.some(value => !["tcp", "stdio", "websocket"].includes(value ?? "")))
  throw new Error("--transport must be tcp, stdio or websocket");
for (const transport of transports as TargetTransport[]) {
  const runtime = await startTransport(transport);
  try { results[transport] = await measure(runtime.host, runtime.startup_ms); }
  finally { await runtime.close(); }
}
const comparisons = Object.fromEntries(Object.entries(results).map(([transport, result]) => [
  transport, result.conditions.map(condition => {
    const old = baseline.old_tcp.conditions.find((c: typeof condition) =>
      c.records === condition.records && c.concurrency === condition.concurrency);
    return {
      records: condition.records, concurrency: condition.concurrency,
      requests_match: old.requests === condition.requests,
      baseline_requests: old.requests,
      throughput_ratio: condition.throughput_per_second / old.throughput_per_second,
      p50_ratio: condition.latency_ms.p50! / old.latency_ms.p50,
      p95_ratio: condition.latency_ms.p95! / old.latency_ms.p95,
      p99_ratio: condition.latency_ms.p99! / old.latency_ms.p99,
      errors: condition.errors, baseline_errors: old.errors,
    };
  }),
]));
const result = {
  captured_at: new Date().toISOString(),
  revision: execFileSync("git", ["rev-parse", "HEAD"], { encoding: "utf8" }).trim(),
  working_tree_modified: execFileSync("git", ["status", "--porcelain"], { encoding: "utf8" }).trim().length > 0,
  source_hashes: Object.fromEntries(await Promise.all([
    "Cargo.lock", "bun.lock", "crates/kernel/src/lib.rs", "crates/kernel/src/process.rs",
    "crates/transport/src/lib.rs", "crates/transport/src/stdio.rs", "crates/transport/src/diagnostics.rs",
    "crates/sdk/src/lib.rs", "sdk/typescript/index.ts", "sdk/typescript/package.json",
    "sdk/typescript/transport.ts", "sdk/typescript/websocket.ts", "examples/batch/src/main.rs",
    "examples/multitransport/measure.ts",
  ].map(async path => [path, createHash("sha256").update(Buffer.from(await Bun.file(path).arrayBuffer())).digest("hex")]))),
  environment: {
    platform: platform(), os: release(), arch: arch(), cpu: cpus()[0]?.model,
    bun: Bun.version,
    rustc: execFileSync("rustc", ["--version"], { encoding: "utf8" }).trim(),
    build: "release", caller_transport: "tcp",
    logical_cpus: cpus().length, load_average_after: loadavg(), machine_isolated: false,
    validation: baseline.environment.validation,
    logging: baseline.environment.logging,
  },
  results, comparisons,
  previous_unisolated_run: previous?.previous_unisolated_run ?? previous,
  note: "Caller is always TCP; target alone varies. Same release Rust logs-batch handler, generated client, payloads, validation and logging as baseline. Raw local samples, not iii/legacy comparisons. Latency excludes assertions; throughput includes them. Startup includes owned-process config creation, kernel/service launch, caller handshake and readiness polling, unlike baseline's separate service spawn. Final expected chunk and EOF are separate. First call per payload is outside warmed samples; larger-payload first call is not a cold process. Single local runs do not establish general transport superiority.",
};
if (path) {
  await Bun.write(path, JSON.stringify(result, null, 2) + "\n");
}
console.log(JSON.stringify(result, null, 2));
