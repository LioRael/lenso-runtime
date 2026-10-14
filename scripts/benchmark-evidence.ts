import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import { lstat, readdir } from "node:fs/promises";
import { isAbsolute, relative, resolve } from "node:path";
import { cpus, platform, release, arch, loadavg } from "node:os";
import { candidateFiles, verifyBinary } from "./local";

export const methodology = "transport-v2-independent-warmup";
export type Json = null | boolean | number | string | Json[] | { [key: string]: Json };
export const sha256 = (bytes: Uint8Array) => createHash("sha256").update(bytes).digest("hex");

export function referenceRevision(args: readonly string[], historicalRevision: string) {
  const index = args.indexOf("--reference-revision");
  if (index < 0) return historicalRevision;
  const value = args[index + 1];
  if (!value || !/^[0-9a-f]{40}$/.test(value) || args.lastIndexOf("--reference-revision") !== index)
    throw new Error("--reference-revision requires one full lowercase 40-character commit SHA");
  return value;
}

export async function loadReferenceRuntimeHost(referenceRoot: string) {
  const original: { RuntimeHost: typeof import("@lenso/runtime-sdk").RuntimeHost } =
    await import(resolve(referenceRoot, "sdk/typescript/index.ts"));
  return original;
}

export async function verifyReferenceSource(
  repository: string, referenceRoot: string, revision: string,
  historical?: { revision: string; source_sha256: Record<string, string> },
) {
  const git = (args: string[]) => execFileSync("git", args, { cwd: repository });
  const commit = git(["rev-parse", "--verify", `${revision}^{commit}`]).toString().trim();
  if (/^[0-9a-f]{40}$/.test(revision) && commit !== revision)
    throw new Error("Reference revision must identify a commit, not a tag");
  const paths = ["crates", "sdk/typescript", "tools", "examples/contracts", "examples/batch",
    "Cargo.toml", "Cargo.lock", "bun.lock", "package.json", "tsconfig.json"];
  const expected: Record<string, string> = {};
  const entries = git(["ls-tree", "-r", "-z", "--full-tree", commit, "--", ...paths]).toString().split("\0");
  for (const entry of entries.filter(Boolean)) {
    const tab = entry.indexOf("\t");
    const path = entry.slice(tab + 1);
    if (path.split("/").some(part => ["dist", "node_modules", "target"].includes(part))) continue;
    if (!paths.includes(path) && !/\.(rs|ts|json|toml|md)$/.test(path)) continue;
    const [mode, type, blob] = entry.slice(0, tab).split(" ");
    if (type !== "blob" || mode === "120000") throw new Error(`Reference source is not an ordinary Git file: ${path}`);
    expected[path] = sha256(git(["cat-file", "blob", blob!]));
  }
  const sorted = (files: Record<string, string>) =>
    Object.fromEntries(Object.entries(files).sort(([a], [b]) => a.localeCompare(b)));
  const actual = sorted({ ...await candidateFiles(referenceRoot), ...await candidateFiles(referenceRoot, true) });
  if (JSON.stringify(actual) !== JSON.stringify(sorted(expected)))
    throw new Error(`Reference source does not match Git commit ${commit}`);
  // Keep the preserved historical hashes as an additional check for that cohort.
  if (historical && commit === historical.revision) {
    for (const [path, hash] of Object.entries(historical.source_sha256)) {
      if (sha256(new Uint8Array(await Bun.file(resolve(referenceRoot, path)).arrayBuffer())) !== hash)
        throw new Error(`Original source mismatch: ${path}`);
    }
  }
  return { reference_revision: revision, reference_commit: commit, reference_source_sha256: actual };
}

// Historical metrics stay unchanged; only observations become counts.
export function compact(value: Json): Json {
  const cohorts: Json[] = [];
  function visit(node: Json): Json {
    if (Array.isArray(node)) return node.map(visit);
    if (node === null || typeof node !== "object") return node;
    const result: Record<string, Json> = {};
    for (const [key, item] of Object.entries(node)) {
      if (key === "manifests" || key === "launch_config") {
        result[`${key}_sha256`] = sha256(Buffer.from(JSON.stringify(item)));
      } else if (key === "raw_ms" || key === "arrivals_ms") {
        if (!Array.isArray(item)) throw new Error(`Invalid observations: ${key}`);
        result[key === "raw_ms" ? "latency_sample_count" : "arrival_count"] = item.length;
      } else if (key === "previous_unisolated_run" || key === "before_optimization") {
        // Collect history at the root, including older chains, never recursively.
        const reduced = visit(item);
        cohorts.push({ label: key, summary: reduced });
      } else result[key] = visit(item);
    }
    if (typeof node.errors === "number" && typeof node.requests === "number")
      result.error_rate = node.errors / node.requests;
    return result;
  }
  const result = visit(value);
  if (cohorts.length) {
    if (!result || Array.isArray(result) || typeof result !== "object") throw new Error("Historical cohorts require an object report");
    result.historical_cohorts = cohorts;
  }
  return result;
}

export function summary(raw: Json, artifact: Json, version = methodology): Record<string, Json> {
  const report = compact(raw);
  if (!report || Array.isArray(report) || typeof report !== "object") throw new Error("Report must be an object");
  return {
    ...report, schema_version: 1, methodology_version: version, artifact,
  };
}

export function repeatCount(value = "3") {
  const count = Number(value);
  if (!Number.isInteger(count) || count < 2 || count > 6)
    throw new Error("Repeat count must be 2..6");
  return count;
}

export function benchmarkEnvironment() {
  return {
    platform: platform(), os: release(), arch: arch(), cpu: cpus()[0]?.model ?? "unknown",
    bun: Bun.version, rustc: execFileSync("rustc", ["--version"], { encoding: "utf8" }).trim(),
    build: "release", caller_transport: "tcp", logical_cpus: cpus().length,
    load_average: loadavg(), machine_isolated: false, validation: "enabled",
    logging: "Kernel/service stderr inherited; stdout ignored",
  };
}

export function balancedOrders<T>(targets: readonly T[], count: number): T[][] {
  repeatCount(String(count));
  if (!targets.length) throw new Error("No benchmark targets");
  return Array.from({ length: count }, (_, round) =>
    Array.from({ length: targets.length }, (_, offset) => targets[(round + offset) % targets.length]!));
}

export function pairedRatios(pairs: { current: number; reference: number }[]) {
  if (pairs.length < 2 || pairs.some(p => !Number.isFinite(p.current) ||
    !Number.isFinite(p.reference) || p.current <= 0 || p.reference <= 0))
    throw new Error("At least two finite positive pairs are required");
  const ratios = pairs.map(p => p.current / p.reference);
  const sorted = [...ratios].sort((a, b) => a - b);
  const median = (sorted[Math.floor((sorted.length - 1) / 2)]! + sorted[Math.ceil((sorted.length - 1) / 2)]!) / 2;
  const dispersion = Math.max(...ratios) - Math.min(...ratios);
  return { ratios, median, dispersion, status: Math.abs(median - 1) <= dispersion ? "inconclusive" : "descriptive-difference" };
}

export async function warmCondition(count: number, concurrency: number, operation: () => Promise<void>) {
  let next = 0;
  await Promise.all(Array.from({ length: concurrency }, async () => {
    while (next++ < count) await operation();
  }));
}

export function containedPath(root: string, path: string) {
  const destination = resolve(path);
  const local = relative(resolve(root), destination);
  if (!local || local === ".." || local.startsWith("../") || isAbsolute(local) || !local.endsWith(".json"))
    throw new Error(`JSON path must remain inside ${root}`);
  if (local.split(/[\\/]/).some(component => component.toLowerCase() === "baseline.json"))
    throw new Error("The historical baseline is protected");
  return destination;
}

export async function writeEvidence(root: string, raw: Json, output?: string) {
  const bytes = Buffer.from(JSON.stringify(raw, null, 2) + "\n");
  const hash = sha256(bytes);
  const artifactRoot = resolve(root, ".perf-artifacts");
  const archive = resolve(artifactRoot, `${hash}.json`);
  const destination = output && containedPath(resolve(root, "examples/multitransport"), output);
  // Check every existing component, including the destination itself.
  for (const path of [archive, ...(destination ? [destination] : [])]) {
    let component = resolve(root);
    for (const part of relative(component, path).split(/[\\/]/)) {
      component = resolve(component, part);
      try {
        if ((await lstat(component)).isSymbolicLink()) throw new Error(`Symlinked output: ${component}`);
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code === "ENOENT") break;
        throw error;
      }
    }
  }
  await Bun.write(archive, bytes);
  if (sha256(new Uint8Array(await Bun.file(archive).arrayBuffer())) !== hash) throw new Error("Artifact write mismatch");
  const result = summary(raw, { path: relative(root, archive), sha256: hash, bytes: bytes.length });
  const text = JSON.stringify(result) + "\n";
  if (destination && Buffer.byteLength(text) > 32768)
    throw new Error(`Compact summary exceeds 32KiB; raw evidence preserved at ${archive}. Use fewer rounds or a single target.`);
  if (destination) await Bun.write(destination, text);
  return result;
}

export async function harnessManifest(root: string) {
  const files: Record<string, string> = {};
  async function visit(path: string) {
    for (const entry of await readdir(resolve(root, path), { withFileTypes: true })) {
      const name = `${path}/${entry.name}`;
      if (entry.isSymbolicLink()) throw new Error(`Harness symlink: ${name}`);
      if (entry.isDirectory()) await visit(name);
      else files[name] = sha256(new Uint8Array(await Bun.file(resolve(root, name)).arrayBuffer()));
    }
  }
  for (const path of ["scripts", "examples/multitransport", "examples/generated", "sdk/typescript/dist"]) await visit(path);
  // Reports are outputs, not harness inputs.
  for (const path of Object.keys(files))
    if (/^examples\/multitransport\/(results|tcp-policy-check|tcp-regression-check)\.json$/.test(path)) delete files[path];
  return Object.fromEntries(Object.entries(files).sort(([a], [b]) => a.localeCompare(b)));
}

export function rejectDrift(before: unknown, after: unknown) {
  if (JSON.stringify(before) !== JSON.stringify(after)) throw new Error("Benchmark input changed during run");
}

export async function provenance(root: string, harnessRoot = root) {
  const binaries: Record<string, Json> = {};
  for (const name of ["lenso-runtime", "logs-batch"]) {
    const binary = resolve(root, "target/release", name);
    const compile_ms = await verifyBinary(binary, root, "release", root);
    binaries[name] = { sha256: sha256(new Uint8Array(await Bun.file(binary).arrayBuffer())), compile_ms };
  }
  return { source: await candidateFiles(root), application_source: await candidateFiles(root, true), harness: await harnessManifest(harnessRoot), binaries };
}
