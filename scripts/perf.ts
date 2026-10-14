import { cpus, platform, release, arch } from "node:os";
import { execFileSync } from "node:child_process";
import assert from "node:assert/strict";
import { resolve } from "node:path";
import { LogsClient } from "../examples/generated/logs";
import { RuntimeHost } from "@lenso/runtime-sdk";
import { emptyContract, startLocal, stopProcess } from "./local";

const count = Number(process.env.PERF_REQUESTS ?? 1000);
const recordCount = Number(process.env.PERF_RECORDS ?? 1000);
const concurrency = Number(process.env.PERF_CONCURRENCY ?? 8);
if (
  !Number.isInteger(count) ||
  count < 1 ||
  count > 100000 ||
  !Number.isInteger(recordCount) ||
  recordCount < 1 ||
  recordCount > 1000 ||
  !Number.isInteger(concurrency) ||
  concurrency < 1 ||
  concurrency > 64
) {
  throw new Error(
    "PERF_REQUESTS must be 1–100000; PERF_RECORDS must be 1–1000; PERF_CONCURRENCY must be 1–64",
  );
}
const runtime = await startLocal("release", {
  batchStderr: "pipe",
  batchEnv: { LENSO_PERF_TIMING: "1" },
});
const client = new LogsClient(runtime.host, {
  namespace: "demo",
  service: "batch",
});
const logs = Array.from({ length: recordCount }, (_, i) => ({
  level: i % 8 === 0 ? "error" : "info",
  message: `synthetic-${i}`,
  duration_ms: i,
}));
function nativeAggregate() {
  let error_count = 0,
    total_duration_ms = 0;
  for (const log of logs) {
    if (log.level === "error") error_count++;
    total_duration_ms += log.duration_ms;
  }
  return { count: logs.length, error_count, total_duration_ms };
}
const expected = nativeAggregate();
const businessSamples: Record<string, number[]> = { rust: [], ts: [], native: [] };
function captureBusiness(child: Bun.Subprocess, key: "rust" | "ts") {
  if (!child.stderr || typeof child.stderr === "number") return;
  const stderr = child.stderr;
  void (async () => {
    const reader = stderr.getReader();
    const decoder = new TextDecoder();
    let pending = "";
    try {
      while (true) {
        const { value, done } = await reader.read();
        if (done) break;
        pending += decoder.decode(value, { stream: true });
        if (pending.length > 65536) throw new Error("Business timing line exceeds limit");
        const lines = pending.split("\n");
        pending = lines.pop() ?? "";
        for (const line of lines)
          try {
            const event = JSON.parse(line);
            if (
              event.event === "aggregate_completed" &&
              Number.isFinite(event.business_ms) &&
              businessSamples[key].length < count + 100
            )
              businessSamples[key].push(event.business_ms);
          } catch {
            /* Ignore ordinary stderr output. */
          }
      }
    } catch {
      /* Child shutdown closes the stream. */
    }
  })();
}
function distribution(samples: number[]) {
  if (!samples.length) return null;
  const sorted = [...samples].sort((a, b) => a - b);
  const at = (q: number) =>
    sorted[Math.min(sorted.length - 1, Math.floor((sorted.length - 1) * q))] ?? null;
  return { samples: sorted.length, p50_ms: at(0.5), p95_ms: at(0.95), p99_ms: at(0.99) };
}
async function collected(key: "rust" | "ts", expectedCount: number) {
  const deadline = performance.now() + 2000;
  while (businessSamples[key].length < expectedCount && performance.now() < deadline)
    await Bun.sleep(2);
  return businessSamples[key].length === expectedCount;
}
const pids = [process.pid, runtime.kernel.pid, runtime.batch.pid];
function usage() {
  try {
    return execFileSync("ps", ["-p", pids.join(","), "-o", "pid=,pcpu=,rss=,time="], {
      encoding: "utf8",
    }).trim();
  } catch {
    return "unavailable";
  }
}
function summary(samples: number[], seconds: number, errors: number) {
  samples.sort((a, b) => a - b);
  const percentile = (q: number) =>
    samples[Math.min(samples.length - 1, Math.floor((samples.length - 1) * q))] ?? null;
  return {
    completed: samples.length,
    errors,
    error_rate: errors / count,
    throughput_per_second: samples.length / seconds,
    latency_ms: { p50: percentile(0.5), p95: percentile(0.95), p99: percentile(0.99) },
  };
}
async function measure(run: () => unknown | Promise<unknown>) {
  const samples: number[] = [];
  let index = 0,
    errors = 0;
  const start = performance.now();
  await Promise.all(
    Array.from({ length: concurrency }, async () => {
      while (index++ < count) {
        const before = performance.now();
        try {
          const output = await run();
          const elapsed = performance.now() - before;
          assert.deepEqual(output, expected);
          samples.push(elapsed);
        } catch (error) {
          if (error instanceof assert.AssertionError) throw error;
          errors++;
        }
      }
    }),
  );
  return summary(samples, (performance.now() - start) / 1000, errors);
}
let tsProcess: Bun.Subprocess | undefined;
try {
  captureBusiness(runtime.batch, "rust");
  const root = process.env.LENSO_ROOT;
  let nativeRun: () => unknown = nativeAggregate;
  let remote_ts: unknown = "not measured (LENSO_ROOT unset)";
  let tsMemory: string | null = null;
  if (root) {
    const nativeModule = await import(resolve(root, "examples/runtime/src/aggregate.ts"));
    nativeRun = () => nativeModule.aggregate({ logs });
    assert.deepEqual(nativeRun(), expected);
    tsProcess = Bun.spawn([process.execPath, resolve(root, "examples/runtime/src/ts-batch.ts")], {
      cwd: resolve(root, "examples/runtime"),
      env: {
        ...process.env,
        LENSO_RUNTIME_PORT: String(runtime.port),
        LENSO_RUNTIME_SERVICE: "batch-ts",
        LENSO_RUNTIME_INSTANCE: "perf-ts",
        LENSO_PERF_TIMING: "1",
      },
      stdout: "ignore",
      stderr: "pipe",
    });
    pids.push(tsProcess.pid);
    captureBusiness(tsProcess, "ts");
    const deadline = performance.now() + 10000;
    while (true) {
      if (tsProcess.exitCode !== null)
        throw new Error(`TS benchmark service exited: ${tsProcess.exitCode}`);
      const snapshot = await runtime.host.status();
      if (
        snapshot.services.some(
          (service) =>
            service.namespace === "demo" &&
            service.service === "batch-ts" &&
            service.instance === "perf-ts" &&
            service.state === "ready",
        )
      )
        break;
      if (performance.now() > deadline) throw new Error("TS benchmark service readiness timed out");
      await Bun.sleep(25);
    }
    const tsClient = new LogsClient(runtime.host, { namespace: "demo", service: "batch-ts" });
    assert.deepEqual(await tsClient.aggregate({ logs }), expected);
    for (let i = 0; i < 100; i++) await tsClient.aggregate({ logs });
    if (!(await collected("ts", 101)))
      throw new Error("TS warmup business telemetry is incomplete.");
    businessSamples.ts.length = 0;
    remote_ts = await measure(() => tsClient.aggregate({ logs }));
    await collected("ts", (remote_ts as ReturnType<typeof summary>).completed);
    tsMemory = usage();
  }
  assert.deepEqual(await client.aggregate({ logs }), expected);
  for (let i = 0; i < 100; i++) await client.aggregate({ logs });
  if (process.env.LENSO_RUST_ROOT && !(await collected("rust", 101)))
    throw new Error("Rust warmup business telemetry is incomplete.");
  businessSamples.rust.length = 0;
  for (let i = 0; i < 100; i++) nativeRun();
  const native = await measure(() => {
    const start = performance.now();
    const output = nativeRun();
    businessSamples.native.push(performance.now() - start);
    return output;
  });
  const before = usage();
  const cross_service = await measure(() => client.aggregate({ logs }));
  if (process.env.LENSO_RUST_ROOT) await collected("rust", cross_service.completed);
  const rustMemory = usage();
  const streamStart = performance.now();
  let chunks = 0,
    streamBytes = 0,
    previousChunk = streamStart;
  const gaps: number[] = [];
  const streamLogs = Array.from({ length: 1000 }, (_, i) => logs[i % logs.length]!);
  for await (const item of client.progress({ logs: streamLogs })) {
    const now = performance.now();
    gaps.push(now - previousChunk);
    previousChunk = now;
    streamBytes += Buffer.byteLength(JSON.stringify(item));
    chunks++;
  }
  const stream_ms = performance.now() - streamStart;
  gaps.sort((a, b) => a - b);
  const slow = client.progress({ logs: logs.slice(0, 32) }, { timeoutMs: 10000 });
  const memorySamples = [usage()];
  const slowStart = performance.now();
  let slowChunks = 0;
  for await (const _ of slow) {
    slowChunks++;
    await Bun.sleep(10);
    if (slowChunks % 8 === 0) memorySamples.push(usage());
  }
  const slowDuration = performance.now() - slowStart;
  const loadHosts: RuntimeHost[] = [];
  const outcomes: Record<string, number> = {};
  const loadStreams: AsyncIterableIterator<unknown>[] = [];
  let saturatedStatus: unknown;
  try {
    for (let i = 0; i < 4; i++) {
      loadHosts.push(
        await RuntimeHost.connect({
          namespace: "demo",
          service: "load",
          instance: `load-${i}`,
          port: runtime.port,
          contract: emptyContract,
        }),
      );
    }
    for (const host of loadHosts) {
      const loadClient = new LogsClient(host, {
        namespace: "demo",
        service: "batch",
      });
      for (let i = 0; i < 48; i++) {
        loadStreams.push(
          loadClient.progress({ logs: logs.slice(0, 32), delay_ms: 1000 }, { timeoutMs: 500 }),
        );
      }
    }
    const pending = loadStreams.map((stream) =>
      stream.next().then(
        () => {
          outcomes.success = (outcomes.success ?? 0) + 1;
        },
        (e) => {
          const code = typeof e?.code === "string" ? e.code : "unknown";
          outcomes[code] = (outcomes[code] ?? 0) + 1;
        },
      ),
    );
    await Bun.sleep(100);
    saturatedStatus = await runtime.host.status();
    await Promise.all(pending);
  } finally {
    await Promise.all(loadStreams.map((stream) => stream.return?.().catch(() => {})));
    await Promise.all(loadHosts.map((host) => host.close()));
  }
  let status = await runtime.host.status();
  const settleDeadline = performance.now() + 2000;
  while (status.calls.length && performance.now() < settleDeadline) {
    await Bun.sleep(25);
    status = await runtime.host.status();
  }
  if (status.calls.length) throw new Error("Load calls did not retire after caller cleanup.");
  console.log(
    JSON.stringify(
      {
        environment: {
          platform: platform(),
          os: release(),
          arch: arch(),
          cpu: cpus()[0]?.model,
          bun: Bun.version,
          rustc: execFileSync("rustc", ["--version"], { encoding: "utf8" }).trim(),
          build: "release",
          requests: count,
          records: recordCount,
          concurrency,
          input_bytes: Buffer.byteLength(JSON.stringify({ logs })),
          observability: "no external telemetry; kernel structured stderr enabled",
        },
        native,
        remote_ts,
        business_compute_ms: {
          native: distribution(businessSamples.native),
          rust: distribution(businessSamples.rust),
          ts: distribution(businessSamples.ts),
        },
        business_collection: {
          rust_complete: businessSamples.rust.length === cross_service.completed,
          ts_complete:
            typeof remote_ts === "object" &&
            businessSamples.ts.length === (remote_ts as ReturnType<typeof summary>).completed,
        },
        cross_service,
        processes: {
          pids,
          roles: {
            caller: process.pid,
            kernel: runtime.kernel.pid,
            rust: runtime.batch.pid,
            ts: tsProcess?.pid,
          },
          ps_columns: "PID CPU% RSS_KiB cumulative_CPU_time",
          after_native: before,
          after_remote_ts: tsMemory,
          after_remote_rust: rustMemory,
          before,
          after: usage(),
        },
        stream: {
          chunks,
          duration_ms: stream_ms,
          chunks_per_second: (chunks * 1000) / stream_ms,
          output_payload_bytes: streamBytes,
          inter_chunk_ms: { p50: gaps[499], p95: gaps[949], p99: gaps[989] },
        },
        slow_consumer: {
          chunks: slowChunks,
          delay_ms: 10,
          duration_ms: slowDuration,
          process_samples: memorySamples,
        },
        overload: {
          attempted: 192,
          caller_instances: 4,
          records_per_stream: Math.min(32, logs.length),
          timeout_ms: 500,
          outcomes,
          saturated_status: saturatedStatus,
          note: "Overloaded outcomes include SDK write admission and kernel admission; inspect the kernel snapshot for its actual active/waiting bounds.",
        },
        settled_status: status,
        cross_machine: "not measured",
        iii_comparison: "not run; no comparative speed claim",
        note: "Remote timings include scheduling, schema validation, encoding, kernel routing and decoding. Every measured output is checked; that check is outside latency but inside throughput. Business timing excludes transport and Rust input conversion; stderr logging still contributes to end-to-end cost. Native async measurement includes await/timer overhead. ps CPU% is a lifetime sample, not isolated benchmark CPU utilization. This bounded log aggregation is cheap even at 1000 records; it does not establish general Rust superiority.",
      },
      null,
      2,
    ),
  );
} finally {
  try {
    if (tsProcess) await stopProcess(tsProcess);
  } finally {
    await runtime.close();
  }
}
