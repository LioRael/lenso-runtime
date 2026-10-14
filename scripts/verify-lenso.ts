import assert from "node:assert/strict";
import { cp, mkdir, mkdtemp, rm, stat, utimes } from "node:fs/promises";
import { createServer } from "node:net";
import { resolve } from "node:path";
import { RuntimeHost } from "@lenso/runtime-sdk";
import { LogsClient, logsContract } from "../examples/generated/logs";
import { emptyContract, reservePort, stopProcess } from "./local";

const lensoRoot = process.env.LENSO_ROOT;
if (!lensoRoot) throw new Error("Set LENSO_ROOT to the prepared Lenso checkout.");
const root = resolve(lensoRoot);
const runtimeRoot = resolve(import.meta.dir, "..");
const rustRoot = process.env.LENSO_RUST_ROOT;
if (!rustRoot) throw new Error("Set LENSO_RUST_ROOT to the native Rust application checkout.");
await mkdir(resolve(runtimeRoot, "target"), { recursive: true });
const fixture = await mkdtemp(resolve(runtimeRoot, "target/workspace-verification-"));
try {
  for (const path of ["Cargo.toml", "Cargo.lock", "crates", "examples", "scripts"])
    await cp(resolve(rustRoot, path), resolve(fixture, path), {
      recursive: true,
      preserveTimestamps: true,
    });
  const port = await reservePort();
  const environment = {
    ...process.env,
    LENSO_RUNTIME_ROOT: runtimeRoot,
    LENSO_RUST_ROOT: fixture,
    LENSO_RUNTIME_PORT: String(port),
    LENSO_RUNTIME_EXTERNAL: "1",
  };
  async function cli(args: string[], input?: unknown) {
    const child = Bun.spawn(
      ["bun", "packages/cli/src/bin.ts", ...args, "--root", "examples/runtime", "--json"],
      {
        cwd: root,
        env: environment,
        stdin: "pipe",
        stdout: "pipe",
        stderr: "inherit",
      },
    );
    if (input !== undefined) child.stdin.write(JSON.stringify(input) + "\n");
    child.stdin.end();
    const timeout = setTimeout(() => child.kill("SIGKILL"), 10000);
    let text = "";
    try {
      for await (const chunk of child.stdout) {
        text += new TextDecoder().decode(chunk);
        if (text.length > 256 * 1024) throw new Error("CLI output exceeded verification bound");
      }
      return { code: await child.exited, output: JSON.parse(text) };
    } finally {
      clearTimeout(timeout);
      await stopProcess(child);
    }
  }
  const inspection = await cli(["inspect", "logs", "aggregate"]);
  assert.equal(inspection.code, 0);
  assert.equal(inspection.output.data.inspection, "static");

  const source = resolve(root, "examples/runtime/src/plugins.ts");
  const originalStat = await stat(source);
  const originalText = await Bun.file(source).text();
  const dev = Bun.spawn(
    ["bun", "packages/cli/src/bin.ts", "dev", "--workspace", "examples/runtime/lenso.workspace.ts"],
    {
      cwd: root,
      env: environment,
      stdin: "pipe",
      stdout: "pipe",
      stderr: "pipe",
    },
  );
  let failure: unknown;
  const events: Record<string, unknown>[] = [];
  const feedback: Record<string, number> = {};
  async function consume(stream: ReadableStream<Uint8Array>) {
    const decoder = new TextDecoder();
    let buffer = "";
    for await (const chunk of stream) {
      buffer += decoder.decode(chunk, { stream: true });
      if (buffer.length > 65536) throw new Error("Development diagnostic exceeded line bound");
      let newline: number;
      while ((newline = buffer.indexOf("\n")) !== -1) {
        const line = buffer.slice(0, newline);
        buffer = buffer.slice(newline + 1);
        console.error(line);
        try {
          const event = JSON.parse(
            line.startsWith("[lenso:workspace] ") ? line.slice("[lenso:workspace] ".length) : line,
          );
          events.push(event);
          if (event.status === "failed" && !event.retained) failure = event.error;
        } catch {}
      }
    }
  }
  const readers = [consume(dev.stdout), consume(dev.stderr)].map((p) =>
    p.catch((e) => {
      failure = e;
    }),
  );
  async function until(check: () => boolean | Promise<boolean>) {
    const deadline = Date.now() + 120000;
    while (Date.now() < deadline) {
      if (failure) throw new Error(`Development failed: ${JSON.stringify(failure)}`);
      if (dev.exitCode !== null) throw new Error(`Development exited: ${dev.exitCode}`);
      if (await check()) return;
      await Bun.sleep(25);
    }
    throw new Error("Development verification timed out");
  }
  let host: RuntimeHost | undefined;
  let oldStream: ReturnType<LogsClient["progress"]> | undefined;
  let touched = false;
  try {
    const startup = performance.now();
    await until(async () => {
      try {
        host ??= await RuntimeHost.connect({
          namespace: "verification",
          service: "engine-proof",
          instance: "one",
          port,
          contract: emptyContract,
        });
        const snapshot = await host.status();
        return ["batch", "frontend"].every((name) =>
          snapshot.services.some(
            (row) => row.namespace === "demo" && row.service === name && row.state === "ready",
          ),
        );
      } catch {
        return false;
      }
    });
    feedback.startup_ms = performance.now() - startup;
    assert.ok(host);
    const generation = async (service: string) => {
      const snapshot = await host!.status();
      return (snapshot.services as Record<string, unknown>[]).find(
        (row) =>
          row.namespace === "demo" &&
          row.service === service &&
          row.state === "ready" &&
          (service !== "batch" || row.selected === true),
      )?.generation;
    };
    const batchBefore = await generation("batch");
    const frontendBefore = await generation("frontend");
    assert.ok(batchBefore);
    assert.ok(frontendBefore);
    const rust = Bun.spawn([resolve(rustRoot, "target/debug/logs-client")], {
      env: { ...environment, LENSO_RUNTIME_SERVICE: "frontend" },
      stdout: "pipe",
      stderr: "inherit",
    });
    const rustTimer = setTimeout(() => rust.kill("SIGKILL"), 5000);
    try {
      assert.deepEqual(await new Response(rust.stdout).json(), {
        count: 2,
        error_count: 1,
        total_duration_ms: 10,
      });
      assert.equal(await rust.exited, 0);
    } finally {
      clearTimeout(rustTimer);
      if (rust.exitCode === null) {
        rust.kill("SIGKILL");
        await rust.exited;
      }
    }
    const duplex = await host.status();
    assert.ok(
      (duplex.recent as Record<string, unknown>[]).some(
        (row) => row.service === "batch" && typeof row.parent_id === "string",
      ),
    );
    const called = await cli(["call", "logs", "aggregate", "--stdin"], {
      logs: [{ level: "info", message: "synthetic request", duration_ms: 12 }],
    });
    assert.equal(called.code, 0);
    assert.deepEqual(called.output.data, {
      batch: { count: 1, error_count: 0, total_duration_ms: 12 },
      local: { batches: 1 },
    });
    const invalid = await cli(["call", "logs", "aggregate", "--stdin"], {
      logs: [],
      principal: "admin",
    });
    assert.equal(invalid.code, 2);
    assert.equal(await generation("frontend"), frontendBefore);

    // Trigger the real Engine source watcher without modifying business source bytes.
    const tsUpdate = performance.now();
    await utimes(source, new Date(), new Date());
    touched = true;
    await until(async () => {
      const next = await generation("frontend");
      return next !== undefined && next !== frontendBefore;
    });
    feedback.ts_update_ms = performance.now() - tsUpdate;
    assert.equal(await generation("batch"), batchBefore);
    const frontendAfter = await generation("frontend");
    assert.notEqual(frontendAfter, frontendBefore);
    const client = new LogsClient(host, { namespace: "demo", service: "batch" });
    const sample = { logs: [{ level: "info", message: "synthetic request", duration_ms: 12 }] };
    const rustSource = resolve(fixture, "examples/batch/src/main.rs");
    const rustText = await Bun.file(rustSource).text();
    const failedBefore = events.filter((event) => event.status === "failed").length;
    const failedUpdate = performance.now();
    await Bun.write(rustSource, rustText + "\nthis_is_a_deliberate_build_failure\n");
    await until(() => {
      if (events.filter((event) => event.status === "failed").length > failedBefore) {
        failure = undefined;
        return true;
      }
      return false;
    });
    feedback.failed_build_ms = performance.now() - failedUpdate;
    assert.equal(await generation("batch"), batchBefore);
    assert.deepEqual(await client.aggregate(sample), {
      count: 1,
      error_count: 0,
      total_duration_ms: 12,
    });

    oldStream = client.progress(
      {
        logs: Array.from({ length: 4 }, () => sample.logs[0]!),
        delay_ms: 500,
      },
      { timeoutMs: 15000 },
    );
    assert.equal((await oldStream.next()).value?.processed, 1);
    const rustUpdate = performance.now();
    assert.ok(rustText.includes("total_duration_ms: result.total_duration_ms,"));
    await Bun.write(
      rustSource,
      rustText.replace(
        "total_duration_ms: result.total_duration_ms,",
        "total_duration_ms: result.total_duration_ms + 1,",
      ),
    );
    await until(async () => {
      const next = await generation("batch");
      return next !== undefined && next !== batchBefore;
    });
    feedback.rust_update_ms = performance.now() - rustUpdate;
    const cutover = await host.status();
    assert.ok(
      (cutover.calls as Record<string, unknown>[]).some(
        (call) =>
          call.service === "batch" &&
          call.method === "progress" &&
          call.callee_generation === batchBefore &&
          call.state === "active",
      ),
    );
    assert.equal((await oldStream.next()).value?.processed, 2);
    // Only one consumer credit is outstanding; item 3 must execute after cutover.
    assert.equal((await oldStream.next()).value?.processed, 3);
    await oldStream.return?.();
    assert.deepEqual(await client.aggregate(sample), {
      count: 1,
      error_count: 0,
      total_duration_ms: 13,
    });
    assert.equal(await generation("frontend"), frontendAfter);
    const selectedBefore = await generation("batch");
    const incompatible = await RuntimeHost.connect({
      namespace: "demo",
      service: "batch",
      instance: "incompatible",
      port,
      contract: { ...logsContract, version: "incompatible" },
      handlers: {
        aggregate: () => ({ count: 0, error_count: 0, total_duration_ms: 0 }),
        progress: async function* () {},
      },
    });
    try {
      await assert.rejects(
        host.promote({
          namespace: "demo",
          service: "batch",
          instance: "incompatible",
          timeoutMs: 5000,
        }),
        (error: unknown) =>
          error instanceof Error && "code" in error && error.code === "contract_mismatch",
      );
      assert.equal(await generation("batch"), selectedBefore);
      assert.equal((await client.aggregate(sample)).total_duration_ms, 13);
    } finally {
      await incompatible.close();
    }
    console.log(
      JSON.stringify({
        verification: "pass",
        checks: [
          "Rust→kernel→TS Lenso→kernel→Rust duplex",
          "linked parent trace",
          "exact CLI inspect/call",
          "invalid input before setup",
          "real Engine source invalidation",
          "TS reload retains kernel connection and Rust instance",
          "failed Rust build retains callable old instance",
          "compatible Rust cutover retains TS instance",
          "old assigned stream survives candidate cutover",
          "incompatible promotion rejected without changing routing",
        ],
        feedback,
      }),
    );
  } finally {
    try {
      await oldStream?.return?.().catch(() => {});
      host?.close();
    } finally {
      dev.stdin.end();
      await stopProcess(dev, 40000);
      await Promise.all(readers);
      if (touched && (await Bun.file(source).text()) === originalText) {
        await utimes(source, originalStat.atime, originalStat.mtime);
      }
      assert.equal(dev.exitCode, 0, "Workspace CLI must finish normal shutdown successfully.");
    }
  }
  const released = createServer();
  await new Promise<void>((resolve, reject) => {
    released.once("error", reject);
    released.listen(port, "127.0.0.1", resolve);
  });
  await new Promise<void>((resolve, reject) => released.close((e) => (e ? reject(e) : resolve())));
  console.log(JSON.stringify({ cleanup: "pass", owned_kernel_port: "released" }));
} finally {
  await rm(fixture, { recursive: true, force: true });
}
