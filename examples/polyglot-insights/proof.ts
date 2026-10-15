import { readdir } from "node:fs/promises";
import { resolve } from "node:path";
import { candidateFiles } from "../../scripts/local";
import { root, example } from "./local";

async function hash(path: string) {
  return new Bun.CryptoHasher("sha256").update(await Bun.file(path).arrayBuffer()).digest("hex");
}
async function files(directory: string): Promise<Record<string, string>> {
  const result: Record<string, string> = {};
  async function visit(relative: string) {
    for (const entry of await readdir(resolve(directory, relative), { withFileTypes: true })) {
      if (["target", "__pycache__", "evidence"].includes(entry.name)) continue;
      const path = relative ? `${relative}/${entry.name}` : entry.name;
      if (entry.isDirectory()) await visit(path);
      else if (entry.isFile()) result[path] = await hash(resolve(directory, path));
      else throw new Error(`Source symlinks are not supported: ${path}`);
    }
  }
  await visit("");
  return Object.fromEntries(Object.entries(result).sort(([a], [b]) => a.localeCompare(b)));
}
async function output(command: string[], cwd: string) {
  const child = Bun.spawn(command, { cwd, stdout: "pipe", stderr: "pipe" });
  const [stdout, stderr, code] = await Promise.all([new Response(child.stdout).text(), new Response(child.stderr).text(), child.exited]);
  if (code !== 0) throw new Error(`Provenance command failed: ${command.join(" ")}\n${stderr}`);
  return stdout.trim();
}

export async function sourceProof(python: string, pythonPath: string) {
  return {
    runtime_commit: await output(["git", "rev-parse", "HEAD"], root),
    runtime_source_sha256: await candidateFiles(root),
    application_source_sha256: await files(example),
    launcher_sha256: await hash(resolve(root, "scripts/polyglot-insights.ts")),
    python_source_sha256: await files(resolve(pythonPath, "lenso_runtime")),
    binaries: {
      kernel_sha256: await hash(resolve(root, "target/debug/lenso-runtime")),
      metrics_sha256: await hash(resolve(root, "target/debug/polyglot-metrics")),
    },
    environment: { platform: process.platform, arch: process.arch, bun: Bun.version,
      python: await output([python, "--version"], root), rust: await output(["rustc", "--version"], root) },
  };
}
