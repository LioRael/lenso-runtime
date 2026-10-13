import { cpus, platform, release, arch } from "node:os";
import { execFileSync } from "node:child_process";
import { LogsClient } from "../examples/generated/logs";
import { RuntimeHost } from "@lenso/runtime-sdk";
import { emptyContract, startLocal } from "./local";

const count = Number(process.env.PERF_REQUESTS ?? 1000);
const concurrency = Number(process.env.PERF_CONCURRENCY ?? 8);
if (
  !Number.isInteger(count) ||
  count < 1 ||
  count > 100000 ||
  !Number.isInteger(concurrency) ||
  concurrency < 1 ||
  concurrency > 64
) {
  throw new Error("PERF_REQUESTS must be 1–100000; PERF_CONCURRENCY must be 1–64");
}
const runtime = await startLocal("release");
const client = new LogsClient(runtime.host, {
  namespace: "demo",
  service: "batch",
});
const logs = Array.from({ length: 32 }, (_, i) => ({
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
          await run();
          samples.push(performance.now() - before);
        } catch {
          errors++;
        }
      }
    }),
  );
  return summary(samples, (performance.now() - start) / 1000, errors);
}
try {
  for (let i = 0; i < 100; i++) await client.aggregate({ logs });
  const native = await measure(nativeAggregate);
  const before = usage();
  const cross_service = await measure(() => client.aggregate({ logs }));
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
  const slow = client.progress({ logs }, { timeoutMs: 10000 });
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
        loadStreams.push(loadClient.progress({ logs, delay_ms: 1000 }, { timeoutMs: 500 }));
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
  const status = await runtime.host.status();
  console.log(
    JSON.stringify(
      {
        environment: {
          platform: platform(),
          os: release(),
          arch: arch(),
          cpu: cpus()[0]?.model,
          bun: Bun.version,
          build: "release",
          requests: count,
          concurrency,
          input_bytes: Buffer.byteLength(JSON.stringify({ logs })),
          observability: "no external telemetry; kernel structured stderr enabled",
        },
        native,
        cross_service,
        processes: {
          pids,
          ps_columns: "PID CPU% RSS_KiB cumulative_CPU_time",
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
          timeout_ms: 500,
          outcomes,
          saturated_status: saturatedStatus,
          note: "Overloaded outcomes include SDK write admission and kernel admission; inspect the kernel snapshot for its actual active/waiting bounds.",
        },
        settled_status: status,
        cross_machine: "not measured",
        iii_comparison: "not run; no comparative speed claim",
        note: "Per-request timings include scheduling, validation, encoding, kernel routing and decoding. Native async measurement includes await overhead. ps CPU% is a lifetime sample, not isolated benchmark CPU utilization.",
      },
      null,
      2,
    ),
  );
} finally {
  await runtime.close();
}
