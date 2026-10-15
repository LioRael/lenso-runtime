import assert from "node:assert/strict";
import { resolve } from "node:path";
import { RuntimeError } from "@lenso/runtime-sdk";
import { CleanerClient } from "./generated/cleaner";
import { MetricsClient, type AnalyzeOutput } from "./generated/metrics";
import { LocalInsights, until, type Event } from "./local";
import { sourceProof } from "./proof";

type Options = { python: string; pythonPath: string; verify: boolean; input: string };

async function expectCode(run: () => Promise<unknown>, code: string) {
  try { await run(); } catch (error) {
    assert.ok(error instanceof RuntimeError, String(error));
    assert.equal(error.code, code);
    return { code: error.code, details: error.details };
  }
  throw new Error(`Expected ${code}, but operation succeeded`);
}

async function verify(app: LocalInsights, checks: Event[]) {
  const cleaner = new CleanerClient(app.gateway, { namespace: app.namespace, service: "cleaner" });
  const metrics = new MetricsClient(app.gateway, { namespace: app.namespace, service: "metrics" });
  const sample = [{ service: "api", status: 200, duration_ms: 40 }, { service: "worker", status: 503, duration_ms: 800 }];
  const check = async (name: string, fn: () => Promise<unknown>) => {
    const begin = performance.now();
    const detail = await fn();
    await app.idle();
    checks.push({ name, passed: true, duration_ms: Math.round(performance.now() - begin), detail });
    console.log(JSON.stringify({ event: "check_passed", name }));
  };
  await check("typed business errors and invalid input", async () => {
    const python = await expectCode(() => cleaner.prepare({ lines: [] }), "business");
    assert.deepEqual(python.details, { reason: "No CSV lines supplied" });
    const rust = await expectCode(() => metrics.analyze({ records: [] }).next(), "business");
    assert.deepEqual(rust.details, { reason: "No valid requests to analyze" });
    const invalid = await expectCode(async () => metrics.analyze({ records: [{ ...sample[0]!, duration_ms: -1 }] }).next(), "invalid_input");
    return { python, rust, invalid };
  });
  await check("concurrent response correlation", async () => {
    const results = await Promise.all(Array.from({ length: 6 }, (_, i) =>
      cleaner.prepare({ lines: [`api,200,${i + 1}`] }, { traceId: `concurrent-${i}` })));
    assert.deepEqual(results.map(result => result.records[0]!.duration_ms), [1, 2, 3, 4, 5, 6]);
    return { correlated: results.length };
  });
  await check("Rust stream demand and early return cleanup", async () => {
    const traceId = "backpressure";
    const stream = metrics.analyze({ records: [...sample, ...sample], batch_size: 1 }, { traceId });
    try {
      await until("Rust handler entered", () => app.metrics.events().some(e => e.event === "analyze_started" && e.trace_id === traceId));
      await Bun.sleep(80);
      const sent = () => app.metrics.events().filter(e => e.event === "chunk_sent" && e.trace_id === traceId).length;
      assert.equal(sent(), 0, "No emitted chunk without consumer demand");
      const first = await stream.next();
      assert.equal(first.value.processed, 1);
      await until("one emitted chunk", () => sent() === 1);
      await Bun.sleep(80);
      assert.equal(sent(), 1, "No second chunk without the second credit");
      await stream.return?.();
      await until("Rust handler released", () => app.metrics.events().some(e => e.event === "analyze_finished" && e.trace_id === traceId && !e.completed));
      return { chunks_before_demand: 0, chunks_after_one_credit: 1, handler_released: true };
    } finally { await stream.return?.(); }
  });
  for (const language of ["python", "rust"] as const) {
    await check(`${language} nested callback cancellation`, async () => {
      const traceId = `${language}-nested-cancel`;
      const abort = new AbortController();
      app.policyDelayMs = 1000;
      const pending = language === "python"
        ? cleaner.prepare({ lines: ["api,200,10"] }, { traceId, signal: abort.signal })
        : metrics.analyze({ records: sample }, { traceId, signal: abort.signal }).next();
      const observed = expectCode(() => pending, "cancelled");
      try {
        await until("nested TS callback entered", () => app.callbacks.some(e => e.event === "policy_started" && e.trace_id === traceId));
        abort.abort();
        const result = await observed;
        await until("nested callback cancelled", () => app.callbacks.some(e => e.event === "policy_finished" && e.trace_id === traceId && e.cancelled));
        const child = language === "python" ? app.cleaner : app.metrics;
        await until("application handler cleanup", () => child.events().some(e => e.event === (language === "python" ? "prepare_finished" : "analyze_finished") && e.trace_id === traceId && !e.completed));
        return { ...result, nested_callback_cancelled: true, handler_released: true };
      } finally { app.policyDelayMs = 0; abort.abort(); await observed.catch(() => {}); }
    });
    await check(`${language} deadline and handler cleanup`, async () => {
      const traceId = `${language}-deadline`;
      const result = await expectCode(() => language === "python"
        ? cleaner.prepare({ lines: ["api,200,10"], delay_ms: 500 }, { traceId, timeoutMs: 80 })
        : metrics.analyze({ records: sample, delay_ms: 500 }, { traceId, timeoutMs: 80 }).next(), "deadline_exceeded");
      const child = language === "python" ? app.cleaner : app.metrics;
      await until("deadline handler cleanup", () => child.events().some(e => e.event === (language === "python" ? "prepare_finished" : "analyze_finished") && e.trace_id === traceId && !e.completed));
      return result;
    });
  }
  await check("Python process loss fails in-flight work without replay", async () => {
    const traceId = "python-disconnect";
    const pending = cleaner.prepare({ lines: ["api,200,10"], delay_ms: 1000 }, { traceId });
    const observed = expectCode(() => pending, "connection_lost");
    await until("Python handler entered before kill", () => app.cleaner.events().some(e => e.event === "prepare_started" && e.trace_id === traceId));
    app.cleaner.process.kill("SIGKILL");
    await app.cleaner.finished;
    const result = await observed;
    const restarted = app.spawn("cleaner-restarted", [app.python, "-W", "error", resolve(import.meta.dir, "cleaner.py")],
      { ...process.env, LENSO_RUNTIME_PORT: String(app.port), INSIGHTS_NAMESPACE: app.namespace,
        PYTHONPATH: app.pythonPath, PYTHONUNBUFFERED: "1", PYTHONDONTWRITEBYTECODE: "1" });
    app.cleaner = restarted;
    await until("replacement Python ready", async () => {
      restarted.check();
      return (await app.gateway.status()).services.some(s => s.service === "cleaner" && s.state === "ready");
    });
    await Bun.sleep(80);
    assert.equal(restarted.events().some(e => e.trace_id === traceId), false);
    assert.equal((await cleaner.prepare({ lines: ["web,200,17"] }, { traceId: "new-explicit-call" })).records[0]!.duration_ms, 17);
    return { ...result, replayed: false, explicit_new_call_succeeded: true };
  });
  await check("Rust process loss fails pending stream", async () => {
    const traceId = "rust-disconnect";
    const stream = metrics.analyze({ records: sample, delay_ms: 1000 }, { traceId });
    const observed = expectCode(() => stream.next(), "connection_lost");
    await until("Rust handler entered before kill", () => app.metrics.events().some(e => e.event === "analyze_started" && e.trace_id === traceId));
    app.metrics.process.kill("SIGKILL");
    await app.metrics.finished;
    return await observed;
  });
  // Last scenario closes the real kernel and proves owned Python exits itself.
  const begin = performance.now();
  const traceId = "kernel-disconnect";
  app.policyDelayMs = 1000;
  const observed = expectCode(() => cleaner.prepare({ lines: ["api,200,10"] }, { traceId }), "connection_lost");
  await until("Python to TS nested call in-flight", () => app.callbacks.some(e => e.event === "policy_started" && e.trace_id === traceId));
  app.kernel.process.kill("SIGKILL");
  await app.kernel.finished;
  await observed;
  await until("Python exits after kernel loss", () => app.cleaner.process.exitCode !== null);
  await app.cleaner.finished;
  assert.equal(app.cleaner.process.exitCode, 0);
  checks.push({ name: "kernel loss closes pending calls and Python process", passed: true,
    duration_ms: Math.round(performance.now() - begin), detail: { code: "connection_lost", python_exit: 0 } });
}

export async function main(options: Options) {
  if (options.verify && options.input !== resolve(import.meta.dir, "fixtures/requests.csv"))
    throw new Error("--verify uses the bundled fixture; run --input separately");
  // Bound local input before splitting or transferring it through Runtime.
  const file = Bun.file(options.input);
  if (file.size > 256000) throw new Error("CSV input exceeds 256000 bytes");
  const content = await file.text();
  const lines = content.replace(/\r\n/g, "\n").split("\n");
  if (lines.at(-1) === "") lines.pop();
  if (lines.length > 1000 || lines.some(line => line.length > 256)) throw new Error("CSV is limited to 1000 lines of at most 256 characters");
  const proof = await sourceProof(options.python, options.pythonPath);
  const app = new LocalInsights(options.python, options.pythonPath);
  const checks: Event[] = [];
  const evidence: Event = { started_at: new Date().toISOString(), transport: "loopback-tcp", languages: ["typescript", "rust", "python"],
    not_covered: ["go", "stdio", "websocket", "production networking", "side-effect rollback"],
    input: options.input, python_sdk_source: options.pythonPath, proof, checks };
  let interrupted = false;
  const stop = () => { interrupted = true; void app.close().catch(error => console.error(error)); };
  process.once("SIGINT", stop); process.once("SIGTERM", stop);
  let failed: unknown;
  try {
    await app.start();
    const cleaner = new CleanerClient(app.gateway, { namespace: app.namespace, service: "cleaner" });
    const metrics = new MetricsClient(app.gateway, { namespace: app.namespace, service: "metrics" });
    const traceId = "application-report";
    const prepared = await cleaner.prepare({ lines }, { traceId });
    console.log(JSON.stringify({ event: "prepared", accepted: prepared.records.length, rejected: prepared.rejected, reasons: prepared.reasons }));
    const progress: AnalyzeOutput[] = [];
    for await (const chunk of metrics.analyze({ records: prepared.records, batch_size: 2 }, { traceId })) {
      progress.push(chunk);
      console.log(JSON.stringify({ event: "progress", ...chunk }));
    }
    await app.idle();
    evidence.result = { ...prepared, progress, summary: progress.at(-1) };
    const callbacks = app.callbacks.filter(event => event.event === "policy_started" && event.trace_id === traceId);
    assert.deepEqual(new Set(callbacks.map(event => event.caller.service)), new Set(["cleaner", "metrics"]));
    assert.ok(callbacks.every(event => event.parent_id));
    await until("Python to Rust trace evidence", () => app.metrics.events().some(event => event.event === "limits" && event.caller.service === "cleaner" && event.trace_id === traceId && event.parent_id));
    await until("Rust to Python trace evidence", () => app.cleaner.events().some(event => event.event === "render" && event.caller.service === "metrics" && event.trace_id === traceId && event.parent_id));
    checks.push({ name: "real business pipeline and all six directed language pairs", passed: true,
      detail: ["TS→Python", "Python→TS", "Python→Rust", "TS→Rust", "Rust→TS", "Rust→Python"] });
    if (options.verify) {
      // The bundled fixture is the fixed oracle for the integration suite.
      assert.equal(prepared.rejected, 4);
      assert.deepEqual(progress.map(chunk => chunk.processed), [2, 4, 6, 8, 10]);
      assert.deepEqual(progress.at(-1), { processed: 10, total: 10, error_count: 2, slow_count: 4,
        total_duration_ms: 2350, p95_ms: 800,
        report: "10 requests; 2 server errors; 4 slow requests; p95 800 ms; mean 235.0 ms." });
      checks.push({ name: "fixture numerical oracle", passed: true });
      await verify(app, checks);
    }
  } catch (error) {
    failed = error;
    evidence.error = String(error);
    app.printTails();
  } finally {
    try { await app.close(); } catch (error) { failed ??= error; evidence.cleanup_error = String(error); }
    process.off("SIGINT", stop); process.off("SIGTERM", stop);
    evidence.finished_at = new Date().toISOString();
    evidence.interrupted = interrupted;
    evidence.processes = app.children.map(child => ({ name: child.name, pid: child.process.pid,
      exit_code: child.process.exitCode, signal: child.process.signalCode }));
    evidence.all_children_exited = app.children.every(child => child.process.exitCode !== null || child.process.signalCode !== null);
    evidence.passed = !failed && !interrupted && evidence.all_children_exited;
    const path = resolve(app.directory, "evidence.json");
    await Bun.write(path, JSON.stringify(evidence, null, 2) + "\n");
    console.log(JSON.stringify({ event: "application_closed", passed: evidence.passed, evidence: path,
      all_children_exited: evidence.all_children_exited }));
  }
  if (interrupted) { process.exitCode = 130; return; }
  if (failed) throw failed;
}
