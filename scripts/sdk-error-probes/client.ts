import assert from "node:assert/strict";
import { RuntimeHost, RuntimeError } from "@lenso/runtime-sdk";
const host = await RuntimeHost.connect({ namespace: "errors", service: "ts", instance: "one",
  port: Number(process.env.LENSO_RUNTIME_PORT), contract: {version: "1", methods: {}} });
const target = {namespace: "errors", service: "server", contractVersion: "1"};
async function check(work: Promise<unknown>, code: string) {
  await assert.rejects(work, (e: unknown) => e instanceof RuntimeError && e.code === code && e.details === undefined);
  console.log(`TS ${code}: passed`);
}
try {
  await check(RuntimeHost.connect({namespace: "errors", service: "bad-ts", instance: "one",
    port: Number(process.env.LENSO_PROTOCOL_PORT), contract: {version: "1", methods: {}}}), "protocol_error");
  await check(host.call({...target, contractVersion: "2"}, "wait", 0), "contract_mismatch");
  await check(host.call(target, "missing", 0), "contract_mismatch");
  await check(host.call(target, "wait", 500, {timeoutMs: 50}), "deadline_exceeded");
  const controller = new AbortController();
  const pending = host.call(target, "wait", 500, {signal: controller.signal});
  const timer = setTimeout(() => controller.abort(), 50);
  try { await check(pending, "cancelled"); } finally { clearTimeout(timer); }
  assert.equal(await host.call(target, "wait", 0), null);
} finally { host.close(); }
