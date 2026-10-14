import { resolve } from "node:path";
import { candidateFiles, recordBinary } from "./local";
import { rejectDrift } from "./benchmark-evidence";

export function transportBuildCommand(root: string) {
  return ["cargo", "build", "--workspace", "--release", "--locked",
    "--target-dir", resolve(root, "target"), "--message-format=json"];
}

export function emittedExecutables(root: string, messages: string) {
  const names = ["lenso-runtime", "logs-batch"];
  const found = new Map<string, string>();
  for (const line of messages.split("\n").filter(line => line.trim())) {
    const message = JSON.parse(line);
    if (message.reason !== "compiler-artifact" || !message.executable ||
        !names.includes(message.target?.name)) continue;
    const expected = resolve(root, "target/release", message.target.name);
    if (typeof message.executable !== "string" || resolve(message.executable) !== expected)
      throw new Error(`Cargo emitted ${message.target.name} outside the expected native layout; remove build.target/CARGO_BUILD_TARGET before recording proofs`);
    found.set(message.target.name, expected);
  }
  if (names.some(name => !found.has(name))) throw new Error("Cargo did not report both benchmark executables");
  return names.map(name => found.get(name)!);
}

// Both current and archived reference builds use the existing source-proof format.
if (import.meta.main) {
  const runtimeRoot = resolve(import.meta.dir, "..");
  const root = resolve(process.argv[2] ?? runtimeRoot);
  if (root !== runtimeRoot && root !== resolve(runtimeRoot, "target/tcp-before-source"))
    throw new Error("Build only this worktree or its ignored target/tcp-before-source reference");
  const files = await candidateFiles(root);
  const nativeFiles = await candidateFiles(root, true);
  const start = performance.now();
  const build = Bun.spawn(transportBuildCommand(root), {
    cwd: root, stdout: "pipe", stderr: "inherit",
  });
  const [messages, exitCode] = await Promise.all([new Response(build.stdout).text(), build.exited]);
  if (exitCode !== 0) {
    console.error(messages);
    throw new Error("Transport build failed");
  }
  // Validate every emitted path before writing ANY proof, even if old native binaries exist.
  const binaries = emittedExecutables(root, messages);
  rejectDrift(files, await candidateFiles(root));
  rejectDrift(nativeFiles, await candidateFiles(root, true));
  for (const binary of binaries)
    await recordBinary(binary, performance.now() - start, files, "release", nativeFiles);
}
