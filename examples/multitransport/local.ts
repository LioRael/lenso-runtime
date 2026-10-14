import { mkdir, mkdtemp, rm } from "node:fs/promises";
import { resolve, join } from "node:path";
import { RuntimeHost } from "@lenso/runtime-sdk";
import { emptyContract, reservePort, stopProcess } from "../../scripts/local";
import { sha256, rejectDrift, type Json } from "../../scripts/benchmark-evidence";

export type TargetTransport = "tcp" | "stdio" | "websocket";
export interface ProcessConfig {
  command: string;
  args: string[];
  cwd: string;
  env: Record<string, string>;
  namespace: string;
  service: string;
  instance: string;
  transport: "tcp" | "stdio";
  ready_timeout_ms: number;
  shutdown_timeout_ms: number;
}
export const root = resolve(import.meta.dir, "../..");

export function processConfig(
  command: string, args: string[], service: string, transport: "tcp" | "stdio",
  env: Record<string, string> = {},
): ProcessConfig {
  return {
    command, args, cwd: root, env, namespace: "demo", service,
    instance: `${service}-one`, transport, ready_timeout_ms: 5000,
    shutdown_timeout_ms: 1000,
  };
}

export async function startTransport(
  target: TargetTransport, extra: ProcessConfig[] = [], independent: ProcessConfig[] = [],
  batchEnv: Record<string, string> = {},
) {
  const start = performance.now();
  const port = await reservePort();
  const wsPort = await reservePort();
  const url = `ws://127.0.0.1:${wsPort}`;
  await mkdir(join(root, "target"), { recursive: true });
  const directory = await mkdtemp(join(root, "target", "lenso-transports-"));
  const batch = processConfig(
    resolve(root, "target/release/logs-batch"), [], "batch",
    target === "stdio" ? "stdio" : "tcp",
    { ...batchEnv, LENSO_RUNTIME_TRANSPORT: target, LENSO_RUNTIME_URL: url },
  );
  const config = join(directory, "processes.json");
  await Bun.write(config, JSON.stringify([...(target === "websocket" ? [] : [batch]), ...extra.map(child => ({
    ...child,
    env: { LENSO_RUNTIME_URL: url, ...child.env },
  }))]));
  const config_sha256 = sha256(new Uint8Array(await Bun.file(config).arrayBuffer()));
  const launch_config: Json = JSON.parse(JSON.stringify({
    owned: await Bun.file(config).json(),
    standalone: [...(target === "websocket" ? [batch] : []), ...independent],
    kernel_args: ["--listen", `127.0.0.1:${port}`, "--websocket", `127.0.0.1:${wsPort}`, "--processes", config],
    caller: { namespace: "demo", service: "verification", instance: "transport-one", port, transport: "tcp" },
    port, url,
  }));
  const kernel = Bun.spawn([
    resolve(root, "target/release/lenso-runtime"),
    "--listen", `127.0.0.1:${port}`, "--websocket", `127.0.0.1:${wsPort}`,
    "--processes", config,
  ], { cwd: root, stdout: "ignore", stderr: "inherit" });
  let host: RuntimeHost | undefined;
  const standalone: Bun.Subprocess[] = [];
  try {
    const deadline = performance.now() + 7000;
    while (!host) {
      if (kernel.exitCode !== null) throw new Error(`Kernel exited: ${kernel.exitCode}`);
      try {
        host = await RuntimeHost.connect({
          namespace: "demo", service: "verification", instance: "transport-one",
          port, contract: emptyContract, transport: { type: "tcp" },
        });
      } catch (error) {
        if (performance.now() >= deadline) throw error;
        await Bun.sleep(10);
      }
    }
    for (const child of [...(target === "websocket" ? [batch] : []), ...independent])
      standalone.push(Bun.spawn([child.command, ...child.args], {
        cwd: root,
        env: {
          ...process.env, LENSO_RUNTIME_URL: url, ...child.env,
          LENSO_RUNTIME_PORT: String(port),
          LENSO_RUNTIME_NAMESPACE: child.namespace, LENSO_RUNTIME_SERVICE: child.service,
          LENSO_RUNTIME_INSTANCE: child.instance,
        },
        stdout: "ignore", stderr: "inherit",
      }));
    while (true) {
      const snapshot = await host.status();
      const names = ["batch", ...extra.map(p => p.service), ...independent.map(p => p.service)];
      if (names.every(name => snapshot.services.some(s =>
        s.namespace === "demo" && s.service === name && s.state === "ready",
      ))) {
        const service = snapshot.services.find(s => s.namespace === "demo" && s.service === "batch");
        if (!service || !("transport" in service) || service.transport !== target)
          throw new Error(`Measured target ${target} does not match status transport: ${JSON.stringify(service)}`);
        break;
      }
      if (kernel.exitCode !== null) throw new Error(`Kernel exited: ${kernel.exitCode}`);
      const exited = standalone.find(child => child.exitCode !== null);
      if (exited) throw new Error(`Independent service exited: ${exited.exitCode}`);
      if (performance.now() >= deadline) throw new Error("Services did not become ready");
      await Bun.sleep(10);
    }
    return {
      host, kernel, port, url, startup_ms: performance.now() - start,
      config_sha256, launch_config,
      async verifyConfig() {
        rejectDrift(config_sha256, sha256(new Uint8Array(await Bun.file(config).arrayBuffer())));
      },
      async close() {
        try { await host!.close(); }
        finally {
          try { await Promise.all(standalone.map(child => stopProcess(child))); }
          finally {
            try { await stopProcess(kernel, 7000); }
            finally { await rm(directory, { recursive: true, force: true }); }
          }
        }
      },
    };
  } catch (error) {
    try { await host?.close(); }
    finally {
      try { await Promise.all(standalone.map(child => stopProcess(child))); }
      finally {
        try { await stopProcess(kernel, 7000); }
        finally { await rm(directory, { recursive: true, force: true }); }
      }
    }
    throw error;
  }
}
