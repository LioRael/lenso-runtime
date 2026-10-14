import { createConnection, createServer } from "node:net";
import { dirname, resolve } from "node:path";
import { readdir } from "node:fs/promises";
import { RuntimeHost } from "@lenso/runtime-sdk";

export const emptyContract = { version: "1", methods: {} };

const runtimeRoot = resolve(import.meta.dir, "..");
export async function candidateFiles(root = runtimeRoot, native = false) {
  const files: Record<string, string> = {};
  async function visit(path: string) {
    for (const entry of await readdir(resolve(root, path), { withFileTypes: true })) {
      const name = `${path}/${entry.name}`;
      if (["dist", "node_modules", "target"].includes(entry.name)) continue;
      if (entry.isDirectory()) await visit(name);
      else if (entry.isFile() && /\.(rs|ts|json|toml|md)$/.test(name))
        files[name] = await hashFile(resolve(root, name));
      else if (entry.isSymbolicLink()) throw new Error(`Candidate source cannot be a symlink: ${name}`);
    }
  }
  for (const path of native ? ["crates", "examples/batch"] : ["crates", "sdk/typescript", "tools", "examples/contracts"]) await visit(path);
  for (const path of native ? ["Cargo.toml", "Cargo.lock"] : ["Cargo.toml", "Cargo.lock", "bun.lock", "package.json", "tsconfig.json"])
    files[path] = await hashFile(resolve(root, path));
  return Object.fromEntries(Object.entries(files).sort(([a], [b]) => a.localeCompare(b)));
}
async function hashFile(path: string) {
  return new Bun.CryptoHasher("sha256").update(await Bun.file(path).arrayBuffer()).digest("hex");
}
export async function recordBinary(binary: string, compileMs: number, files?: Record<string, string>, profile = "debug", nativeFiles?: Record<string, string>) {
  if (profile !== "debug" && profile !== "release") throw new Error("Unknown binary build profile");
  await Bun.write(`${binary}.source.json`, JSON.stringify({
    files: files ?? await candidateFiles(), binary_sha256: await hashFile(binary), compile_ms: compileMs,
    build_profile: profile,
    ...(nativeFiles ? { native_files: nativeFiles } : {}),
  }, null, 2) + "\n");
}
export async function verifyBinary(binary: string, nativeRoot?: string, expectedProfile?: string, sourceRoot = runtimeRoot) {
  const proof = await Bun.file(`${binary}.source.json`).json();
  if (JSON.stringify(proof.files) !== JSON.stringify(await candidateFiles(sourceRoot)) ||
      proof.binary_sha256 !== await hashFile(binary) ||
      !["debug", "release"].includes(proof.build_profile) ||
      (expectedProfile !== undefined && proof.build_profile !== expectedProfile) ||
      !Number.isFinite(proof.compile_ms) || proof.compile_ms < 0)
    throw new Error(`Binary candidate mismatch: ${binary}. Rebuild the selected source.`);
  if (nativeRoot && JSON.stringify(proof.native_files) !== JSON.stringify(await candidateFiles(nativeRoot, true)))
    throw new Error(`Native application source mismatch: ${binary}. Rebuild the selected application.`);
  return proof.compile_ms as number;
}
export async function verifyPrepared(root: string) {
  const proof = await Bun.file(resolve(root, ".runtime-deps/provenance.json")).json();
  if (proof.source?.kind !== "local" || resolve(proof.source.root) !== runtimeRoot)
    throw new Error("Prepare the selected candidate with explicit --runtime-root; pinned fallback is not allowed.");
  for (const [path, hash] of Object.entries(proof.source.files)) {
    if (await hashFile(resolve(runtimeRoot, path)) !== hash ||
        await hashFile(resolve(root, ".runtime-deps/source", path)) !== hash)
      throw new Error(`Prepared Runtime source mismatch: ${path}`);
  }
  const installedEntry = Bun.resolveSync("@lenso/runtime-sdk", resolve(root, "examples/runtime"));
  const installedDist = dirname(installedEntry);
  if (resolve(installedEntry) !== resolve(installedDist, "index.js") ||
      await hashFile(resolve(installedDist, "../package.json")) !== proof.files["package.json"])
    throw new Error("Installed Runtime SDK manifest or entry does not match the prepared candidate.");
  for (const [path, hash] of Object.entries(proof.files))
    if (await hashFile(resolve(root, ".runtime-deps/sdk", path)) !== hash ||
        (path.startsWith("dist/") && (
          await hashFile(resolve(runtimeRoot, "sdk/typescript", path)) !== hash ||
          await hashFile(resolve(installedDist, path.slice("dist/".length))) !== hash)))
      throw new Error(`Prepared Runtime SDK mismatch: ${path}`);
  for (const path of Object.keys(await candidateFiles()))
    if (/^(crates\/[^/]+\/(src\/.*\.rs|Cargo\.toml)|sdk\/typescript\/[^/]+\.(ts|json)|tools\/generate\.ts|examples\/contracts\/logs\.json)$/.test(path) &&
        !proof.source.files[path])
      throw new Error(`Prepared Runtime candidate is missing source: ${path}`);
}

export async function reservePort(): Promise<number> {
  const server = createServer();
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("No TCP port");
  const port = address.port;
  await new Promise<void>((resolve, reject) => server.close((e) => (e ? reject(e) : resolve())));
  return port;
}

async function waitForPort(port: number, process: Bun.Subprocess): Promise<void> {
  const deadline = Date.now() + 5000;
  while (Date.now() < deadline) {
    if (process.exitCode !== null) throw new Error(`Kernel exited: ${process.exitCode}`);
    const open = await new Promise<boolean>((resolve) => {
      const socket = createConnection({ host: "127.0.0.1", port });
      socket.once("connect", () => {
        socket.destroy();
        resolve(true);
      });
      socket.once("error", () => {
        socket.destroy();
        resolve(false);
      });
      socket.setTimeout(100, () => {
        socket.destroy();
        resolve(false);
      });
    });
    if (open) return;
    await Bun.sleep(25);
  }
  throw new Error("Kernel did not listen within 5s");
}

export async function stopProcess(child: Bun.Subprocess, timeoutMs = 2000): Promise<void> {
  if (child.exitCode !== null) return;
  child.kill("SIGTERM");
  const timer = setTimeout(() => child.kill("SIGKILL"), timeoutMs);
  try {
    await child.exited;
  } finally {
    clearTimeout(timer);
  }
}

export async function startLocal(
  profile = "debug",
  options: {
    batchEnv?: Record<string, string>;
    batchStderr?: "inherit" | "pipe";
  } = {},
) {
  const port = await reservePort();
  const directory = resolve(import.meta.dir, "..", "target", profile);
  const kernelPath = resolve(directory, "lenso-runtime");
  const appDirectory = process.env.LENSO_RUST_ROOT
    ? resolve(process.env.LENSO_RUST_ROOT, "target", profile)
    : directory;
  const batchPath = resolve(appDirectory, "logs-batch");
  const clientPath = resolve(appDirectory, "logs-client");
  if (!(await Bun.file(kernelPath).exists()) || !(await Bun.file(batchPath).exists())) {
    throw new Error(`Build Runtime and selected Rust binaries first: ${kernelPath}, ${batchPath}`);
  }
  const compileMs = process.env.LENSO_RUST_ROOT
    ? { kernel: await verifyBinary(kernelPath, undefined, profile), rust: await verifyBinary(batchPath, process.env.LENSO_RUST_ROOT, profile) }
    : null;
  if (process.env.LENSO_ROOT) await verifyPrepared(resolve(process.env.LENSO_ROOT));
  const kernel = Bun.spawn([kernelPath, "--listen", `127.0.0.1:${port}`], {
    stdout: "inherit",
    stderr: "inherit",
  });
  let batch: Bun.Subprocess | undefined;
  let host: RuntimeHost | undefined;
  try {
    await waitForPort(port, kernel);
    batch = Bun.spawn([batchPath], {
      env: { ...process.env, LENSO_RUNTIME_PORT: String(port), ...options.batchEnv },
      stdout: "inherit",
      stderr: options.batchStderr ?? "inherit",
    });
    host = await RuntimeHost.connect({
      namespace: "demo",
      service: "verification",
      instance: "bun-one",
      port,
      contract: emptyContract,
    });
    const deadline = Date.now() + 5000;
    while (true) {
      const snapshot = await host.status();
      if (
        snapshot.services.some(
          (service) =>
            typeof service === "object" &&
            service !== null &&
            "namespace" in service &&
            service.namespace === "demo" &&
            "service" in service &&
            service.service === "batch" &&
            "state" in service &&
            service.state === "ready",
        )
      )
        break;
      if (batch.exitCode !== null) throw new Error(`Batch exited: ${batch.exitCode}`);
      if (Date.now() > deadline) throw new Error("Batch did not become ready within 5s");
      await Bun.sleep(25);
    }
    return {
      port,
      host,
      kernel,
      batch,
      batchPath,
      compileMs,
      clientPath,
      async close() {
        try {
          await host!.close();
        } finally {
          try {
            await stopProcess(batch!);
          } finally {
            await stopProcess(kernel);
          }
        }
      },
    };
  } catch (error) {
    try {
      await host?.close();
    } finally {
      try {
        if (batch) await stopProcess(batch);
      } finally {
        await stopProcess(kernel);
      }
    }
    throw error;
  }
}

if (import.meta.main) {
  const profile = process.argv[2];
  if (profile !== "debug" && profile !== "release")
    throw new Error("Usage: bun scripts/local.ts <debug|release> (builds and records kernel)");
  const files = await candidateFiles();
  const before = performance.now();
  const child = Bun.spawn(["cargo", "build", "--locked", "-p", "kernel",
    ...(profile === "release" ? ["--release"] : [])], { cwd: runtimeRoot, stdout: "inherit", stderr: "inherit" });
  if (await child.exited !== 0) throw new Error("Kernel build failed");
  if (JSON.stringify(files) !== JSON.stringify(await candidateFiles()))
    throw new Error("Runtime source changed during kernel build");
  await recordBinary(resolve(runtimeRoot, "target", profile, "lenso-runtime"), performance.now() - before, files, profile);
}
