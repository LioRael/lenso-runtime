import assert from "node:assert/strict";
import { LogsClient, logsContract } from "../examples/generated/logs";
import { RuntimeHost } from "@lenso/runtime-sdk";
import { startLocal } from "./local";
import { resolve } from "node:path";

const runtime = await startLocal();
let tsService: RuntimeHost | undefined;
const logs = [
  { level: "info", message: "synthetic startup", duration_ms: 3 },
  { level: "error", message: "synthetic failure", duration_ms: 7 },
];
const client = new LogsClient(runtime.host, {
  namespace: "demo",
  service: "batch",
});
async function expectCode(run: () => Promise<unknown>, code: string) {
  try {
    await run();
  } catch (error) {
    assert.equal((error as { code: string }).code, code);
    return;
  }
  throw new Error(`Expected ${code}`);
}
try {
  const rust = Bun.spawn([resolve(import.meta.dir, "../target/debug/logs-client")], {
    env: { ...process.env, LENSO_RUNTIME_PORT: String(runtime.port) },
    stdout: "pipe",
    stderr: "inherit",
  });
  const timer = setTimeout(() => rust.kill("SIGKILL"), 5000);
  try {
    const output = await new Response(rust.stdout).json();
    assert.equal(await rust.exited, 0);
    assert.deepEqual(output, { count: 2, error_count: 1, total_duration_ms: 10 });
  } finally {
    clearTimeout(timer);
    if (rust.exitCode === null) {
      rust.kill("SIGKILL");
      await rust.exited;
    }
  }
  assert.deepEqual(await client.aggregate({ logs }), {
    count: 2,
    error_count: 1,
    total_duration_ms: 10,
  });
  await expectCode(() => client.aggregate({ logs: [] }), "business");
  const progress = [];
  for await (const item of client.progress({ logs })) progress.push(item);
  assert.deepEqual(progress, [
    { processed: 1, total: 2 },
    { processed: 2, total: 2 },
  ]);
  // Mock SDK tests prove cancel framing, not retirement of a real kernel route.
  const invalidStream = runtime.host.stream(
    { namespace: "demo", service: "batch", contractVersion: logsContract.version },
    "progress",
    { logs },
    {
      contract: {
        ...logsContract.methods.progress!,
        output: {
          ...logsContract.methods.progress!.output,
          properties: {
            ...logsContract.methods.progress!.output.properties!,
            total: { type: "integer", maximum: 1 },
          },
        },
      },
    },
  );
  await expectCode(() => invalidStream.next(), "invalid_output");
  await expectCode(() => invalidStream.next(), "invalid_output");
  assert.equal((await runtime.host.status()).calls.length, 0);
  await expectCode(
    () =>
      runtime.host.call(
        {
          namespace: "demo",
          service: "absent",
          contractVersion: "1",
        },
        "aggregate",
        { logs },
      ),
    "service_not_found",
  );
  await expectCode(
    () =>
      runtime.host.call(
        {
          namespace: "demo",
          service: "batch",
          contractVersion: "wrong",
        },
        "aggregate",
        { logs },
      ),
    "contract_mismatch",
  );
  await expectCode(
    () =>
      runtime.host.call(
        {
          namespace: "demo",
          service: "batch",
          contractVersion: "1",
        },
        "aggregate",
        { logs, principal: "admin" },
      ),
    "invalid_input",
  );

  tsService = await RuntimeHost.connect({
    namespace: "demo",
    service: "echo",
    instance: "ts-one",
    port: runtime.port,
    contract: {
      version: "1",
      methods: {
        echo: {
          mode: "unary",
          input: { type: "string", maxLength: 64 },
          output: { type: "string", maxLength: 64 },
          error: { type: "object", properties: {}, required: [], additionalProperties: false },
        },
      },
    },
    handlers: {
      echo: async (input, context) => {
        assert.equal(context.caller.service, "verification");
        assert.ok(context.traceId);
        return input;
      },
    },
  });
  assert.equal(
    await runtime.host.call(
      {
        namespace: "demo",
        service: "echo",
        contractVersion: "1",
      },
      "echo",
      "synthetic echo",
    ),
    "synthetic echo",
  );
  await tsService.drain(100);
  await expectCode(
    () =>
      runtime.host.call(
        {
          namespace: "demo",
          service: "echo",
          contractVersion: "1",
        },
        "echo",
        "drained",
      ),
    "not_ready",
  );
  await tsService.close();
  tsService = undefined;

  const slow = client.progress({ logs, delay_ms: 100 }, { timeoutMs: 20 });
  await expectCode(() => slow.next(), "deadline_exceeded");
  const abort = new AbortController();
  const cancelled = client.progress({ logs, delay_ms: 100 }, { signal: abort.signal });
  const next = cancelled.next();
  abort.abort();
  await expectCode(() => next, "cancelled");
  const abandoned = client.progress({ logs });
  await abandoned.next();
  await abandoned.return?.();
  await Bun.sleep(50);
  const snapshot = await runtime.host.status();
  assert.equal(snapshot.calls.length, 0);

  const interrupted = client.progress({ logs, delay_ms: 1000 });
  const pending = interrupted.next();
  runtime.batch.kill("SIGKILL");
  await runtime.batch.exited;
  await expectCode(() => pending, "connection_lost");
  console.log(
    JSON.stringify({
      smoke: "pass",
      checks: [
        "TS→kernel→Rust",
        "Rust generated client",
        "generated clients",
        "stream",
        "caller stream validation terminates real route",
        "typed business error",
        "missing service",
        "version mismatch",
        "boundary validation",
        "deadline",
        "abort",
        "iterator cancellation",
        "callee disconnect",
        "TS export",
        "trusted caller and trace context",
        "drained admission",
      ],
    }),
  );
} finally {
  try {
    await tsService?.close();
  } finally {
    await runtime.close();
  }
}
