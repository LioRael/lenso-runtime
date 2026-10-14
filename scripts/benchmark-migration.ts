import { execFileSync } from "node:child_process";
import assert from "node:assert/strict";
import { resolve } from "node:path";
import { sha256, summary, type Json } from "./benchmark-evidence";

export const migrationRevision = "9d0ff1d0a433c18fbc3e791149e8c3f29a3b0b11";
export const migrationFiles = [
  { name: "baseline", bytes: 66470, lines: 2316, sha256: "0e8c44ab4bd31f3646e65bffb582373af2a7b37dacbba9b02c84b5ada1195136", blob: "df347492d572f9b7442b2d4d98679a9acb4746ba" },
  { name: "results", bytes: 451015, lines: 14291, sha256: "9508c5542fd28cd8031b21524d3f0cc7cae966c7b6d403bc4eb3df4596103780", blob: "62cd8720383846072e91acb950a4620049cbc996" },
  { name: "tcp-policy-check", bytes: 73171, lines: 2376, sha256: "d7fff59fde05f82ea64f09e2eb7bc426d257f958ae7f81c14f7f085acc759f9c", blob: "516d1e6e4c2e887b2f92824e32b4c124a266fa35" },
  { name: "tcp-regression-check", bytes: 846448, lines: 27855, sha256: "f6d1b4eb66f5639cfdbcfd2b18ae7ff92e69696e1bc6cf234e6a462a573468ec", blob: "a3e4af726c533b18ae2aee7c56e002ee4c8344f9" },
];

export function historicalBytes(blob: string, root: string) {
  return execFileSync("git", ["cat-file", "blob", blob], { cwd: root, maxBuffer: 2 * 1024 * 1024 });
}

function verifyMetrics(raw: Json, reduced: Json): number {
  let counts = 0;
  if (Array.isArray(raw)) {
    assert(Array.isArray(reduced));
    assert.equal(raw.length, reduced.length);
    raw.forEach((value, index) => { counts += verifyMetrics(value, reduced[index]!); });
  } else if (raw !== null && typeof raw === "object") {
    assert(reduced !== null && typeof reduced === "object" && !Array.isArray(reduced));
    for (const [key, value] of Object.entries(raw)) {
      if (key === "raw_ms" || key === "arrivals_ms") {
        assert(Array.isArray(value));
        assert.equal(reduced[key], undefined);
        assert.equal(reduced[key === "raw_ms" ? "latency_sample_count" : "arrival_count"], value.length);
        counts += value.length;
      } else if (key === "previous_unisolated_run" || key === "before_optimization") {
        assert.equal(reduced[key], undefined);
        const cohort: Record<string, Json> | undefined = (reduced.historical_cohorts as Record<string, Json>[]).find(c => c.label === key);
        assert(cohort);
        counts += verifyMetrics(value, cohort.summary!);
      } else counts += verifyMetrics(value, reduced[key]!);
    }
  } else assert.deepEqual(reduced, raw);
  return counts;
}

if (import.meta.main) {
  const root = resolve(import.meta.dir, "..");
  const verify = process.argv.includes("--verify");
  const prepared = [];
  // Verify ALL archives and Git identities before replacing ANY canonical report.
  for (const entry of migrationFiles) {
    const path = `examples/multitransport/${entry.name}.json`;
    const archive = `.perf-artifacts/migration/${entry.sha256}.json`;
    const gitBytes = historicalBytes(entry.blob, root);
    const current = new Uint8Array(await Bun.file(resolve(root, path)).arrayBuffer());
    if (verify) {
      assert.equal(gitBytes.length, entry.bytes);
      assert.equal(sha256(gitBytes), entry.sha256);
      const archived = Bun.file(resolve(root, archive));
      if (await archived.exists()) assert.deepEqual(Buffer.from(await archived.arrayBuffer()), gitBytes);
      assert(current.length <= 32768);
      const reduced = JSON.parse(Buffer.from(current).toString());
      assert.equal(reduced.artifact.git_blob, entry.blob);
      assert.equal(reduced.artifact.sha256, entry.sha256);
      const counts = verifyMetrics(JSON.parse(gitBytes.toString()), reduced);
      console.log(`${path}: ${current.length} bytes; all metrics equal; ${counts} observations accounted for; Git blob SHA-256 verified`);
      continue;
    }
    const bytes = new Uint8Array(await Bun.file(resolve(root, archive)).arrayBuffer());
    if (bytes.length !== entry.bytes || sha256(bytes) !== entry.sha256 ||
        sha256(gitBytes) !== entry.sha256 || sha256(current) !== entry.sha256)
      throw new Error(`Raw preservation mismatch: ${path}`);
    const report = summary(JSON.parse(Buffer.from(bytes).toString()), {
      path: archive, sha256: entry.sha256, bytes: entry.bytes, lines: entry.lines,
      git_blob: entry.blob, git_revision: migrationRevision,
      retrieve: `git cat-file blob ${entry.blob}`,
    }, "transport-v1-serial-only-warmup-historical");
    const text = JSON.stringify(report) + "\n";
    if (Buffer.byteLength(text) > 32768) throw new Error(`Summary too large: ${path}`);
    prepared.push({ path, text });
  }
  for (const { path, text } of prepared) {
    await Bun.write(resolve(root, path), text);
    console.log(`${path}: ${Buffer.byteLength(text)} bytes`);
  }
}
