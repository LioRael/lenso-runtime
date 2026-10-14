import { execFileSync } from "node:child_process";
import { measure } from "../examples/multitransport/measure";
import { root, startTransport, type TargetTransport } from "../examples/multitransport/local";
import { balancedOrders, benchmarkEnvironment, containedPath, provenance, rejectDrift, repeatCount, writeEvidence, sha256 } from "./benchmark-evidence";

function option(name: string) {
  const index = process.argv.indexOf(name);
  if (index < 0) return undefined;
  const value = process.argv[index + 1];
  if (!value || value.startsWith("--")) throw new Error(`Missing value for ${name}`);
  return value;
}
if (process.argv.includes("--baseline")) throw new Error("The historical baseline is protected");
const output = option("--output");
if (output) containedPath(`${root}/examples/multitransport`, output);
const selected = option("--transport");
if (selected && !["tcp", "stdio", "websocket"].includes(selected)) throw new Error("Unknown transport");
const transports: TargetTransport[] = selected ? [selected as TargetTransport] : ["tcp", "stdio", "websocket"];
const count = repeatCount(option("--repeat"));
const before = await provenance(root);
const environment = benchmarkEnvironment();
const runs = [];
for (const [round, order] of balancedOrders(transports, count).entries()) {
  for (const transport of order) {
    const runtime = await startTransport(transport);
    try {
      runs.push({ round, transport, config_sha256: runtime.config_sha256, launch_config: runtime.launch_config, ...await measure(runtime.host, runtime.startup_ms) });
      await runtime.verifyConfig();
    }
    finally { await runtime.close(); }
  }
}
rejectDrift(before, await provenance(root));
const manifestIdentity = (files: Record<string, string>) => ({
  sha256: sha256(Buffer.from(JSON.stringify(files))), files: Object.keys(files).length,
});
const report = {
  captured_at: new Date().toISOString(),
  revision: execFileSync("git", ["rev-parse", "HEAD"], { cwd: root, encoding: "utf8" }).trim(),
  environment, environment_after: benchmarkEnvironment(),
  provenance_identity: { source: manifestIdentity(before.source), application_source: manifestIdentity(before.application_source), harness: manifestIdentity(before.harness), binaries: before.binaries },
  runs,
  note: "Rotating target orders; independent warmup for every concurrency. Historical v1 baseline is a different methodology and is not used for v2 ratios. p99 remains descriptive. No superiority or production SLO claim.",
};
// Full manifests stay with the raw measurements; compact output carries their identities.
const result = await writeEvidence(root, { ...report, manifests: before }, output);
console.log(JSON.stringify(result, null, 2));
