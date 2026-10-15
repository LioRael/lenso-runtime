/** TS -> Python calls, plus a TS service for the Python -> TS half of the check. */
import assert from "node:assert/strict";
import { BusinessError, RuntimeHost } from "@lenso/runtime-sdk";
import { LogsClient, logsContract } from "../generated/logs";

const host = await RuntimeHost.connect({
  namespace: "demo", service: "ts-logs", instance: "one",
  port: Number(process.env.LENSO_RUNTIME_PORT), contract: logsContract,
  handlers: {
    aggregate(input) {
      if (!input.logs.length) throw new BusinessError({ reason: "At least one log is required" });
      return { count: input.logs.length, error_count: input.logs.filter((log: any) => log.level === "error").length,
        total_duration_ms: input.logs.reduce((sum: number, log: any) => sum + log.duration_ms, 0) };
    },
    async *progress(input, context) {
      for (let index = 0; index < input.logs.length; index++) {
        await Bun.sleep(input.delay_ms ?? 0);
        if (context.signal.aborted) return;
        yield { processed: index + 1, total: input.logs.length };
      }
    },
  },
});
try {
  const client = new LogsClient(host, { namespace: "demo", service: "python-logs" });
  const logs = [{ level: "error", message: "interop", duration_ms: 7 }];
  assert.deepEqual(await client.aggregate({ logs }), { count: 1, error_count: 1, total_duration_ms: 7 });
  await assert.rejects(client.aggregate({ logs: [] }), (e: any) => e.code === "business" && e.details.reason.length > 0);
  const items = [];
  for await (const item of client.progress({ logs })) items.push(item);
  assert.deepEqual(items, [{ processed: 1, total: 1 }]);
  const abort = new AbortController();
  const cancelled = client.progress({ logs, delay_ms: 500 }, { signal: abort.signal });
  const next = cancelled.next();
  abort.abort();
  await assert.rejects(next, (e: any) => e.code === "cancelled");
  const timed = client.progress({ logs, delay_ms: 500 }, { timeoutMs: 20 });
  await assert.rejects(timed.next(), (e: any) => e.code === "deadline_exceeded");
  console.log(JSON.stringify({ event: "ts_python_verified", unary: true, stream: true, business: true, cancel: true, deadline: true }));
  // The test now calls the TS service from Python, then closes stdin.
  await Bun.stdin.text();
  await host.drain(1000);
} finally {
  host.close();
}
