import assert from "node:assert/strict";
import { stat, utimes } from "node:fs/promises";
import { createServer } from "node:net";
import { resolve } from "node:path";
import { RuntimeHost } from "@lenso/runtime-sdk";
import { emptyContract, reservePort, stopProcess } from "./local";

const lensoRoot = process.env.LENSO_ROOT;
if (!lensoRoot) throw new Error("Set LENSO_ROOT to the prepared Lenso checkout.");
const root = resolve(lensoRoot);
const runtimeRoot = resolve(import.meta.dir, "..");
const port = await reservePort();
const environment = {
  ...process.env,
  LENSO_RUNTIME_ROOT: runtimeRoot,
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
const dev = Bun.spawn(["bun", "src/dev.ts"], {
  cwd: resolve(root, "examples/runtime"),
  env: environment,
  stdin: "pipe",
  stdout: "pipe",
  stderr: "pipe",
});
let ready = 0,
  failure: unknown;
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
        const event = JSON.parse(line);
        if (event.type === "ready") ready++;
        if (event.type === "failed") failure = event.diagnostic;
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
  const deadline = Date.now() + 15000;
  while (Date.now() < deadline) {
    if (failure) throw new Error(`Development failed: ${JSON.stringify(failure)}`);
    if (dev.exitCode !== null) throw new Error(`Development exited: ${dev.exitCode}`);
    if (await check()) return;
    await Bun.sleep(25);
  }
  throw new Error("Development verification timed out");
}
let host: RuntimeHost | undefined;
let touched = false;
try {
  await until(() => ready >= 1);
  host = await RuntimeHost.connect({
    namespace: "verification",
    service: "engine-proof",
    instance: "one",
    port,
    contract: emptyContract,
  });
  const generation = async (service: string) => {
    const snapshot = await host!.status();
    return (snapshot.services as Record<string, unknown>[]).find(
      (row) => row.namespace === "demo" && row.service === service && row.state === "ready",
    )?.generation;
  };
  const batchBefore = await generation("batch");
  const frontendBefore = await generation("frontend");
  assert.ok(batchBefore);
  assert.ok(frontendBefore);
  const rust = Bun.spawn([resolve(runtimeRoot, "target/debug/logs-client")], {
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
  await utimes(source, new Date(), new Date());
  touched = true;
  await until(() => ready >= 2);
  assert.equal(await generation("batch"), batchBefore);
  const frontendAfter = await generation("frontend");
  assert.notEqual(frontendAfter, frontendBefore);
  dev.stdin.write("restart-batch\n");
  await until(async () => {
    const next = await generation("batch");
    return next !== undefined && next !== batchBefore;
  });
  assert.equal(await generation("frontend"), frontendAfter);
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
        "Rust-only restart retains TS instance",
      ],
    }),
  );
} finally {
  try {
    host?.close();
  } finally {
    dev.stdin.end();
    await stopProcess(dev);
    await Promise.all(readers);
    if (touched && (await Bun.file(source).text()) === originalText) {
      await utimes(source, originalStat.atime, originalStat.mtime);
    }
  }
}
const released = createServer();
await new Promise<void>((resolve, reject) => {
  released.once("error", reject);
  released.listen(port, "127.0.0.1", resolve);
});
await new Promise<void>((resolve, reject) => released.close((e) => (e ? reject(e) : resolve())));
console.log(JSON.stringify({ cleanup: "pass", owned_kernel_port: "released" }));
