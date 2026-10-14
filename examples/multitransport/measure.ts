import assert from "node:assert/strict";
import { loadavg } from "node:os";
import { LogsClient } from "../generated/logs";
import type { RuntimeHost } from "@lenso/runtime-sdk";

export const requests = Number(process.env.TRANSPORT_REQUESTS ?? 300);
export const warmup = 50;
export const concurrent = 8;
if (!Number.isInteger(requests) || requests < 1 || requests > 100000)
  throw new Error("TRANSPORT_REQUESTS must be 1..100000");

function distribution(raw: number[]) {
  const sorted = [...raw].sort((a, b) => a - b);
  const at = (q: number) => sorted[Math.floor((sorted.length - 1) * q)] ?? null;
  return { p50: at(.5), p95: at(.95), p99: at(.99) };
}

export async function measure(host: RuntimeHost, startup_ms: number) {
  const load_average_before = loadavg();
  const client = new LogsClient(host, { namespace: "demo", service: "batch" });
  const conditions = [];
  for (const records of [2, 1000]) {
    const logs = Array.from({ length: records }, (_, i) => ({
      level: i % 8 === 0 ? "error" : "info",
      message: `synthetic-${i}`,
      duration_ms: i,
    }));
    const expected = {
      count: records,
      error_count: Math.ceil(records / 8),
      total_duration_ms: records * (records - 1) / 2,
    };
    const first = performance.now();
    assert.deepEqual(await client.aggregate({ logs }), expected);
    const first_call_ms = performance.now() - first;
    for (let i = 0; i < warmup; i++)
      assert.deepEqual(await client.aggregate({ logs }), expected);
    for (const concurrency of [1, concurrent]) {
      const raw_ms: number[] = [];
      const failures: string[] = [];
      const load_before = loadavg();
      let index = 0;
      const start = performance.now();
      await Promise.all(Array.from({ length: concurrency }, async () => {
        while (index++ < requests) {
          const before = performance.now();
          try {
            const output = await client.aggregate({ logs });
            const elapsed = performance.now() - before;
            assert.deepEqual(output, expected);
            raw_ms.push(elapsed);
          } catch (error) {
            if (error instanceof assert.AssertionError) throw error;
            failures.push(String(error));
          }
        }
      }));
      const elapsed_ms = performance.now() - start;
      conditions.push({
        records, input_bytes: Buffer.byteLength(JSON.stringify({ logs })),
        concurrency, requests, warmup, first_call_ms,
        completed: raw_ms.length, errors: failures.length, failures,
        elapsed_ms, throughput_per_second: raw_ms.length * 1000 / elapsed_ms,
        latency_ms: distribution(raw_ms), raw_ms,
        load_before, load_after: loadavg(),
      });
    }
  }
  const logs = Array.from({ length: 1000 }, (_, i) => ({
    level: "info", message: `synthetic-${i}`, duration_ms: i,
  }));
  const start = performance.now();
  let first_chunk_ms: number | null = null;
  let completion_ms: number | null = null;
  const arrivals_ms: number[] = [];
  for await (const item of client.progress({ logs })) {
    const elapsed = performance.now() - start;
    first_chunk_ms ??= elapsed;
    arrivals_ms.push(elapsed);
    assert.equal(item.processed, arrivals_ms.length);
    assert.equal(item.total, 1000);
    if (item.processed === item.total) completion_ms = elapsed;
  }
  const eof_ms = performance.now() - start;
  assert.equal(arrivals_ms.length, 1000);
  return {
    startup_ms, conditions, load_average_before, load_average_after: loadavg(),
    stream: { records: 1000, first_chunk_ms, completion_ms, eof_ms, arrivals_ms },
  };
}
