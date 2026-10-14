import { expect, test } from "bun:test";
import { execFileSync } from "node:child_process";
import { mkdir, mkdtemp, rm, symlink } from "node:fs/promises";
import { resolve } from "node:path";
import { balancedOrders, compact, containedPath, harnessManifest, pairedRatios, provenance, referenceRevision, rejectDrift, repeatCount, sha256, summary, verifyReferenceSource, warmCondition, writeEvidence, type Json } from "./benchmark-evidence";
import { candidateFiles, recordBinary, verifyBinary } from "./local";
import { emittedExecutables, transportBuildCommand } from "./transport-build";

const root = resolve(import.meta.dir, "..");
async function fixture(operation: (directory: string) => Promise<void>) {
  await mkdir(resolve(root, ".perf-artifacts"), { recursive: true });
  const directory = await mkdtemp(resolve(root, ".perf-artifacts/test-"));
  try { await operation(directory); }
  finally { await rm(directory, { recursive: true, force: true }); }
}

function equalMetrics(raw: Json, reduced: Json): void {
  if (Array.isArray(raw)) {
    expect(Array.isArray(reduced)).toBe(true);
    expect((reduced as Json[]).length).toBe(raw.length);
    raw.forEach((value, index) => equalMetrics(value, (reduced as Json[])[index]!));
  } else if (raw !== null && typeof raw === "object") {
    const object = reduced as Record<string, Json>;
    for (const [key, value] of Object.entries(raw)) {
      if (key === "raw_ms" || key === "arrivals_ms") {
        expect(object[key]).toBeUndefined();
        expect(object[key === "raw_ms" ? "latency_sample_count" : "arrival_count"]).toBe((value as Json[]).length);
      } else if (key === "previous_unisolated_run" || key === "before_optimization") {
        expect(object[key]).toBeUndefined();
        const cohort = (object.historical_cohorts as Record<string, Json>[]).find(c => c.label === key)!;
        equalMetrics(value, cohort.summary!);
      } else equalMetrics(value, object[key]!);
    }
  } else expect(reduced).toEqual(raw);
}

test("small synthetic historical fixture preserves all metrics and comparison shapes", () => {
  const raw = {
    source_sha256: { "kernel.rs": "synthetic-hash" },
    old_tcp: { conditions: [{ records: 2, concurrency: 8, requests: 2, completed: 2, errors: 0, latency_ms: { p50: 1, p95: 2, p99: 2 }, raw_ms: [1, 2], throughput_per_second: 100 }] },
    previous_unisolated_run: { note: "Synthetic prior experiment", stream: { records: 2, arrivals_ms: [1, 2], first_chunk_ms: 1, completion_ms: 2, eof_ms: 3 } },
    before_optimization: [{ round: 0, startup_ms: 3 }],
  };
  const reduced = summary(raw, { sha256: "synthetic-artifact" }, "historical-v1");
  equalMetrics(raw, reduced);
  expect((reduced.old_tcp as typeof raw.old_tcp).conditions[0]!.requests).toBe(2);
  expect(reduced.source_sha256).toEqual(raw.source_sha256);
});

test("summary preserves metrics, counts observations, and removes historical report keys", () => {
  expect(compact({ conditions: [{ requests: 2, errors: 0, completed: 2, raw_ms: [1, 2], latency_ms: { p50: 1 } }], stream: { arrivals_ms: [1] } }))
    .toEqual({ conditions: [{ requests: 2, errors: 0, completed: 2, latency_sample_count: 2, latency_ms: { p50: 1 }, error_rate: 0 }], stream: { arrival_count: 1 } });
  expect(() => summary([], {})).toThrow("Report must be an object");
});

test("older report chains become flat historical cohorts without recursive embedding", () => {
  const reduced = summary({
    note: "current",
    previous_unisolated_run: {
      note: "previous",
      previous_unisolated_run: { note: "oldest", stream: { arrivals_ms: [1, 2] } },
    },
  }, {});
  const cohorts = reduced.historical_cohorts as Record<string, Json>[];
  expect(cohorts.length).toBe(2);
  expect(cohorts.map(c => (c.summary as Record<string, Json>).note)).toEqual(["oldest", "previous"]);
  for (const cohort of cohorts) {
    const previous = cohort.summary as Record<string, Json>;
    expect(previous.historical_cohorts).toBeUndefined();
    expect(previous.previous_unisolated_run).toBeUndefined();
  }
});

test("orders rotate; each target takes each position in a complete cycle", () => {
  expect(balancedOrders(["tcp", "stdio", "websocket"], 3)).toEqual([
    ["tcp", "stdio", "websocket"], ["stdio", "websocket", "tcp"], ["websocket", "tcp", "stdio"],
  ]);
  expect(balancedOrders(["old", "new"], 2)).toEqual([["old", "new"], ["new", "old"]]);
  for (const value of ["1", "7", "NaN", "2.5"]) expect(() => repeatCount(value)).toThrow();
});

test("every concurrency has its own exact, parallel warmup", async () => {
  for (const concurrency of [1, 8]) {
    let calls = 0, active = 0, peak = 0;
    await warmCondition(50, concurrency, async () => {
      calls++; active++; peak = Math.max(peak, active);
      await Promise.resolve();
      active--;
    });
    expect(calls).toBe(50);
    expect(peak).toBe(concurrency);
  }
});

test("paired dispersion marks small differences inconclusive", () => {
  expect(pairedRatios([{ current: 99, reference: 100 }, { current: 103, reference: 100 }]).status).toBe("inconclusive");
  expect(pairedRatios([{ current: 80, reference: 100 }, { current: 81, reference: 100 }]).status).toBe("descriptive-difference");
  expect(() => pairedRatios([{ current: 0, reference: 1 }, { current: 1, reference: 1 }])).toThrow();
});

test("output containment and baseline protection", async () => {
  const base = resolve(root, "examples/multitransport");
  for (const path of [resolve(root, "outside.json"), resolve(base, "../escape.json"),
    resolve(base, "baseline.json"), resolve(base, "BASELINE.json"), resolve(base, "Baseline.json"), base])
    expect(() => containedPath(base, path)).toThrow();
  await fixture(async directory => {
    const destination = resolve(directory, "examples/multitransport/summary.json");
    const result = await writeEvidence(directory, { conditions: [{ requests: 1, errors: 0, raw_ms: [1] }] }, destination);
    expect(await Bun.file(destination).json()).toEqual(result);
    const identity = result.artifact as Record<string, Json>;
    const bytes = Buffer.from(await Bun.file(resolve(directory, identity.path as string)).arrayBuffer());
    expect(sha256(bytes)).toBe(identity.sha256 as string);
    expect(bytes.length).toBe(identity.bytes as number);
    await symlink(resolve(directory, "examples/multitransport"), resolve(directory, ".perf-artifacts/link"));
    await symlink(destination, resolve(directory, "examples/multitransport/alias.json"));
    await expect(writeEvidence(directory, {}, resolve(directory, "examples/multitransport/alias.json"))).rejects.toThrow("Symlinked output");
  });
});

test("case-variant baseline paths are rejected without touching synthetic evidence", async () => {
  await fixture(async directory => {
    const baseline = resolve(directory, "examples/multitransport/baseline.json");
    await Bun.write(baseline, "synthetic-preserved-baseline");
    for (const name of ["BASELINE.json", "Baseline.json", "bAsElInE.json"])
      await expect(writeEvidence(directory, {}, resolve(directory, "examples/multitransport", name)))
        .rejects.toThrow("historical baseline is protected");
    expect(await Bun.file(baseline).text()).toBe("synthetic-preserved-baseline");
  });
});

test("Cargo target directory is explicit and proofs require exact emitted native artifacts", async () => {
  await fixture(async directory => {
    const command = transportBuildCommand(directory);
    expect(command[command.indexOf("--target-dir") + 1]).toBe(resolve(directory, "target"));
    expect(command).toContain("--message-format=json");
    const messages = (target: string) => ["lenso-runtime", "logs-batch"].map(name =>
      JSON.stringify({ reason: "compiler-artifact", target: { name }, executable: resolve(target, name) })).join("\n");
    const native = resolve(directory, "target/release");
    // Stale native binaries must not be decorated by an alternate-target build.
    for (const name of ["lenso-runtime", "logs-batch"]) await Bun.write(resolve(native, name), "stale-test-binary");
    expect(emittedExecutables(directory, messages(native))).toEqual([
      resolve(native, "lenso-runtime"), resolve(native, "logs-batch"),
    ]);
    for (const alternate of [resolve(directory, "env-target/release"),
      resolve(directory, "configured-target/release"), resolve(directory, "target/aarch64-other/release")])
      expect(() => emittedExecutables(directory, messages(alternate))).toThrow("expected native layout");
    expect(() => emittedExecutables(directory, JSON.stringify({ reason: "build-finished", success: true }))).toThrow("both benchmark executables");
    for (const name of ["lenso-runtime", "logs-batch"])
      expect(await Bun.file(resolve(native, `${name}.source.json`)).exists()).toBe(false);
  });
});

test("oversized summary never replaces its destination; full raw data survives", async () => {
  await fixture(async directory => {
    const destination = resolve(directory, "examples/multitransport/summary.json");
    await Bun.write(destination, "preserved-summary");
    const raw = { note: "x".repeat(40000) };
    const bytes = Buffer.from(JSON.stringify(raw, null, 2) + "\n");
    const archive = resolve(directory, `.perf-artifacts/${sha256(bytes)}.json`);
    await expect(writeEvidence(directory, raw, destination)).rejects.toThrow("raw evidence preserved");
    expect(await Bun.file(destination).text()).toBe("preserved-summary");
    expect(Buffer.from(await Bun.file(archive).arrayBuffer())).toEqual(bytes);
    expect((await writeEvidence(directory, raw)).note).toBe(raw.note);
  });
});

test("full config and provenance manifests become hash identities only in summaries", () => {
  const raw = { launch_config: { owned: [{ service: "synthetic" }] }, manifests: { source: { "x.ts": "synthetic" } } };
  const reduced = summary(raw, {});
  expect(reduced.launch_config).toBeUndefined();
  expect(reduced.manifests).toBeUndefined();
  expect(reduced.launch_config_sha256).toBe(sha256(Buffer.from(JSON.stringify(raw.launch_config))));
  expect(() => rejectDrift(reduced.launch_config_sha256, sha256(Buffer.from("{}")))).toThrow();
});

test("existing proof verifier rejects stale binary/source/application and harness drift", async () => {
  await fixture(async directory => {
    for (const path of ["crates/a/src", "sdk/typescript/dist", "tools", "examples/contracts", "examples/batch/src", "examples/generated", "examples/multitransport", "scripts"])
      await mkdir(resolve(directory, path), { recursive: true });
    for (const path of ["Cargo.toml", "Cargo.lock", "bun.lock", "package.json", "tsconfig.json", "crates/a/src/lib.rs", "examples/batch/src/main.rs", "scripts/harness.ts", "examples/generated/logs.ts", "sdk/typescript/dist/index.js"])
      await Bun.write(resolve(directory, path), "{}");
    const nativeFiles = await candidateFiles(directory, true);
    for (const name of ["lenso-runtime", "logs-batch"]) {
      const binary = resolve(directory, "target/release", name);
      await Bun.write(binary, "unit-test-only-binary");
      await recordBinary(binary, 1, await candidateFiles(directory), "release", nativeFiles);
    }
    const before = await provenance(directory);
    expect(before.binaries["logs-batch"]).toBeDefined();
    const binary = resolve(directory, "target/release/logs-batch");
    await expect(verifyBinary(binary, directory, "debug", directory)).rejects.toThrow("Binary candidate mismatch");
    await Bun.write(binary, "changed-test-binary");
    await expect(verifyBinary(binary, directory, "release", directory)).rejects.toThrow("Binary candidate mismatch");
    await Bun.write(binary, "unit-test-only-binary");
    await Bun.write(resolve(directory, "crates/a/src/lib.rs"), "changed");
    await expect(verifyBinary(binary, directory, "release", directory)).rejects.toThrow("Binary candidate mismatch");
    await Bun.write(resolve(directory, "crates/a/src/lib.rs"), "{}");
    await Bun.write(resolve(directory, "examples/batch/src/main.rs"), "changed");
    await expect(verifyBinary(binary, directory, "release", directory)).rejects.toThrow("Native application source mismatch");
    await Bun.write(resolve(directory, "scripts/harness.ts"), "changed");
    expect(() => rejectDrift(before.harness, {})).toThrow();
    const afterHarness = await harnessManifest(directory);
    expect(() => rejectDrift(before.harness, afterHarness)).toThrow();
    await Bun.write(resolve(directory, "examples/generated/logs.ts"), "changed");
    const changedClient = await harnessManifest(directory);
    expect(() => rejectDrift(afterHarness, changedClient)).toThrow();
  });
});

test("reference revision CLI keeps the historical default and requires an unambiguous full SHA", () => {
  const historical = "a".repeat(40), selected = "b".repeat(40);
  expect(referenceRevision([], historical)).toBe(historical);
  expect(referenceRevision(["--reference-revision", selected], historical)).toBe(selected);
  for (const args of [["--reference-revision"], ["--reference-revision", "--repeat"],
    ["--reference-revision", "9d0ff1d"], ["--reference-revision", "main"],
    ["--reference-revision", selected.toUpperCase()],
    ["--reference-revision", selected, "--reference-revision", historical]])
    expect(() => referenceRevision(args, historical)).toThrow("full lowercase");
});

test("archived host requires its own dependency resolution and never falls back to the current SDK", async () => {
  await fixture(async directory => {
    const archived = resolve(directory, "archive");
    const entry = resolve(archived, "sdk/typescript/index.ts");
    const dependency = "lenso-reference-fixture-dependency";
    await Bun.write(entry, `
      import { createRequire } from "node:module";
      const require = createRequire(import.meta.url);
      const library = require(require.resolve("${dependency}/package.json"));
      export class RuntimeHost { static identity = library.identity; }
    `);
    const check = async () => {
      const child = Bun.spawn([process.execPath, "--no-install", "-e", `
        const { loadReferenceRuntimeHost } = await import(${JSON.stringify(resolve(import.meta.dir, "benchmark-evidence.ts"))});
        const { RuntimeHost } = await loadReferenceRuntimeHost(${JSON.stringify(archived)});
        console.log(RuntimeHost.identity);
      `], { cwd: root, stdout: "pipe", stderr: "pipe" });
      const [exit, stdout, stderr] = await Promise.all([
        child.exited, new Response(child.stdout).text(), new Response(child.stderr).text(),
      ]);
      return { exit, stdout: stdout.trim(), stderr };
    };
    const missing = await check();
    expect(missing.exit).not.toBe(0);
    expect(missing.stderr).toContain(dependency);
    // Mirror a workspace-local installed dependency without installing test packages.
    await Bun.write(resolve(archived, "sdk/typescript/node_modules", dependency, "package.json"),
      JSON.stringify({ name: dependency, identity: "archived-host-local-dependency" }));
    const installed = await check();
    expect(installed.exit).toBe(0);
    expect(installed.stdout).toBe("archived-host-local-dependency");
    expect(installed.stderr).toBe("");
    await rm(entry);
    expect((await check()).exit).not.toBe(0);
  });
});

test("reference labels and full source inventory come from selected Git blobs, not historical metadata", async () => {
  await fixture(async directory => {
    const repository = resolve(directory, "repository"), archived = resolve(directory, "archive");
    await mkdir(repository);
    const git = (args: string[], input?: string) =>
      execFileSync("git", args, { cwd: repository, input, encoding: "utf8" }).trim();
    git(["init", "--quiet"]);
    const files: Record<string, string> = {
      "Cargo.toml": "synthetic-workspace", "Cargo.lock": "synthetic-lock",
      "bun.lock": "synthetic-bun-lock", "package.json": "{}", "tsconfig.json": "{}",
      "crates/a/src/lib.rs": "synthetic-runtime", "examples/batch/src/main.rs": "synthetic-service",
      "sdk/typescript/index.ts": "synthetic-reference-host",
      "sdk/typescript/schema.ts": "synthetic-schema",
      "tools/generate.ts": "synthetic-generator", "examples/contracts/logs.json": "{}",
    };
    const snapshot = (source: Record<string, string>) => {
      git(["read-tree", "--empty"]);
      for (const [path, bytes] of Object.entries(source)) {
        const blob = git(["hash-object", "-w", "--stdin"], bytes);
        git(["update-index", "--add", "--cacheinfo", `100644,${blob},${path}`]);
      }
      const tree = git(["write-tree"]);
      // Unreachable deterministic objects only: no branch, checkout commit, or hooks.
      return git(["hash-object", "-t", "commit", "-w", "--stdin"],
        `tree ${tree}\nauthor Synthetic <synthetic@example.invalid> 0 +0000\ncommitter Synthetic <synthetic@example.invalid> 0 +0000\n\nfixture\n`);
    };
    const historicalRevision = snapshot(files);
    const selectedFiles: Record<string, string> = { ...files, "sdk/typescript/index.ts": "synthetic-selected-host" };
    const selectedRevision = snapshot(selectedFiles);
    for (const [path, bytes] of Object.entries(selectedFiles)) await Bun.write(resolve(archived, path), bytes);
    const historical = { revision: historicalRevision, source_sha256: { "sdk/typescript/index.ts": sha256(Buffer.from(files["sdk/typescript/index.ts"]!)) } };
    const reference = await verifyReferenceSource(repository, archived, selectedRevision, historical);
    expect(reference.reference_revision).toBe(selectedRevision);
    expect(reference.reference_commit).toBe(selectedRevision);
    expect(reference.reference_source_sha256["sdk/typescript/index.ts"]).toBe(sha256(Buffer.from(selectedFiles["sdk/typescript/index.ts"])));
    expect(summary(reference, {}).reference_revision).toBe(selectedRevision);
    await expect(verifyReferenceSource(repository, archived, historicalRevision, historical)).rejects.toThrow("does not match Git commit");
    await expect(verifyReferenceSource(repository, archived, "f".repeat(40))).rejects.toThrow();
    for (const path of ["sdk/typescript/schema.ts", "examples/batch/src/main.rs", "Cargo.lock"]) {
      await Bun.write(resolve(archived, path), "wrong-source");
      await expect(verifyReferenceSource(repository, archived, selectedRevision, historical)).rejects.toThrow("does not match Git commit");
      await Bun.write(resolve(archived, path), selectedFiles[path]!);
    }
    await rm(resolve(archived, "sdk/typescript/schema.ts"));
    await expect(verifyReferenceSource(repository, archived, selectedRevision, historical)).rejects.toThrow("does not match Git commit");
    await Bun.write(resolve(archived, "sdk/typescript/schema.ts"), selectedFiles["sdk/typescript/schema.ts"]!);
    const extra = resolve(archived, "sdk/typescript/extra.ts");
    await Bun.write(extra, "uncommitted-host-input");
    await expect(verifyReferenceSource(repository, archived, selectedRevision, historical)).rejects.toThrow("does not match Git commit");
    await rm(extra);
    await Bun.write(resolve(archived, "sdk/typescript/index.ts"), files["sdk/typescript/index.ts"]!);
    expect((await verifyReferenceSource(repository, archived, historicalRevision, historical)).reference_revision).toBe(historicalRevision);
    await expect(verifyReferenceSource(repository, archived, historicalRevision, {
      ...historical, source_sha256: { "sdk/typescript/index.ts": "wrong-preserved-hash" },
    })).rejects.toThrow("Original source mismatch");

    for (const path of ["scripts", "examples/multitransport", "examples/generated", "sdk/typescript/dist"])
      await mkdir(resolve(archived, path), { recursive: true });
    await Bun.write(resolve(archived, "scripts/harness.ts"), "same-current-measurement-harness");
    const source = await candidateFiles(archived), native = await candidateFiles(archived, true);
    for (const name of ["lenso-runtime", "logs-batch"]) {
      const binary = resolve(archived, "target/release", name);
      await Bun.write(binary, "synthetic-not-executable");
      await expect(provenance(archived)).rejects.toThrow();
      await recordBinary(binary, 1, source, "release", native);
    }
    const before = await provenance(archived);
    for (const name of ["lenso-runtime", "logs-batch"]) {
      const binary = resolve(archived, "target/release", name);
      await Bun.write(binary, "stale-synthetic-binary");
      await expect(provenance(archived)).rejects.toThrow("Binary candidate mismatch");
      await Bun.write(binary, "synthetic-not-executable");
    }
    expect(await provenance(archived)).toEqual(before);
    await Bun.write(resolve(archived, "sdk/typescript/index.ts"), selectedFiles["sdk/typescript/index.ts"]!);
    await expect(provenance(archived)).rejects.toThrow("Binary candidate mismatch");
  });
});
