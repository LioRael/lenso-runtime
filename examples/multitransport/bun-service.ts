import { RuntimeError, RuntimeHost } from "@lenso/runtime-sdk";
import { LogsClient, logsContract, type AggregateInput, type ProgressInput } from "../generated/logs";

export async function serve(type: "tcp" | "websocket") {
  let upstreams: LogsClient[] = [];
  const host = await RuntimeHost.connect({
    namespace: process.env.LENSO_RUNTIME_NAMESPACE ?? "demo",
    service: process.env.LENSO_RUNTIME_SERVICE ?? `bun-${type}`,
    instance: process.env.LENSO_RUNTIME_INSTANCE ?? `${type}-one`,
    port: Number(process.env.LENSO_RUNTIME_PORT ?? 7331),
    transport: type === "tcp" ? { type } : {
      type, url: process.env.LENSO_RUNTIME_URL ?? "ws://127.0.0.1:7332",
    },
    contract: logsContract,
    ready: false,
    handlers: {
      aggregate: async (value, context) => {
        if (upstreams.length) {
          const outputs = await Promise.all(upstreams.map(client => client.aggregate(value, {
            signal: context.signal, traceId: context.traceId, parentId: context.callId,
          })));
          const first = outputs[0]!;
          if (outputs.some(output => output.count !== first.count ||
            output.error_count !== first.error_count || output.total_duration_ms !== first.total_duration_ms))
            throw new RuntimeError("invalid_output", "Upstream results differ");
          return first;
        }
        const { logs } = value as AggregateInput;
        return {
          count: logs.length,
          error_count: logs.filter(log => log.level === "error").length,
          total_duration_ms: logs.reduce((sum, log) => sum + log.duration_ms, 0),
        };
      },
      progress: async function* (value, context) {
        const { logs, delay_ms } = value as ProgressInput;
        for (let index = 0; index < logs.length; index++) {
          if (delay_ms) await Bun.sleep(delay_ms);
          if (context.signal.aborted) return;
          yield { processed: index + 1, total: logs.length };
        }
      },
    },
  });
  upstreams = (process.env.LENSO_RUNTIME_FANOUT_SERVICES ?? "").split(",").filter(Boolean)
    .map(service => new LogsClient(host, {
      namespace: process.env.LENSO_RUNTIME_NAMESPACE ?? "demo", service,
    }));
  await host.activate();
  if (process.env.TRANSPORT_PID_FILE)
    await Bun.write(process.env.TRANSPORT_PID_FILE, String(process.pid));
  console.error(JSON.stringify({ event: "bun_service_ready", transport: type, pid: process.pid }));
  let stopping = false;
  const stop = async () => {
    if (stopping) return;
    stopping = true;
    let exitCode = 0;
    try { await host.drain(500); }
    catch (reason) {
      exitCode = 1;
      console.error(JSON.stringify({
        event: "service_shutdown_failed",
        code: reason instanceof RuntimeError ? reason.code : "cleanup_failed",
      }));
    } finally {
      try { host.close(); }
      catch {
        exitCode = 1;
        console.error(JSON.stringify({ event: "service_close_failed" }));
      }
      process.exit(exitCode);
    }
  };
  process.on("SIGTERM", stop);
  process.on("SIGINT", stop);
  void host.whenClosed().then(stop);
}
