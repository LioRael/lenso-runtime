import { createConnection, createServer } from "node:net";
import { resolve } from "node:path";
import { RuntimeHost } from "@lenso/runtime-sdk";

export const emptyContract = { version: "1", methods: {} };

export async function reservePort(): Promise<number> {
  const server = createServer();
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("No TCP port");
  const port = address.port;
  await new Promise<void>((resolve, reject) => server.close((e) => (e ? reject(e) : resolve())));
  return port;
}

async function waitForPort(port: number, process: Bun.Subprocess): Promise<void> {
  const deadline = Date.now() + 5000;
  while (Date.now() < deadline) {
    if (process.exitCode !== null) throw new Error(`Kernel exited: ${process.exitCode}`);
    const open = await new Promise<boolean>((resolve) => {
      const socket = createConnection({ host: "127.0.0.1", port });
      socket.once("connect", () => {
        socket.destroy();
        resolve(true);
      });
      socket.once("error", () => {
        socket.destroy();
        resolve(false);
      });
      socket.setTimeout(100, () => {
        socket.destroy();
        resolve(false);
      });
    });
    if (open) return;
    await Bun.sleep(25);
  }
  throw new Error("Kernel did not listen within 5s");
}

export async function stopProcess(child: Bun.Subprocess): Promise<void> {
  if (child.exitCode !== null) return;
  child.kill("SIGTERM");
  const timer = setTimeout(() => child.kill("SIGKILL"), 2000);
  try {
    await child.exited;
  } finally {
    clearTimeout(timer);
  }
}

export async function startLocal(profile = "debug") {
  const port = await reservePort();
  const directory = resolve(import.meta.dir, "..", "target", profile);
  const kernelPath = resolve(directory, "lenso-runtime");
  const appDirectory = process.env.LENSO_RUST_ROOT
    ? resolve(process.env.LENSO_RUST_ROOT, "target", profile)
    : directory;
  const batchPath = resolve(appDirectory, "logs-batch");
  const clientPath = resolve(appDirectory, "logs-client");
  if (!(await Bun.file(kernelPath).exists()) || !(await Bun.file(batchPath).exists())) {
    throw new Error(
      `Build Runtime and selected Rust binaries first: ${kernelPath}, ${batchPath}`,
    );
  }
  const kernel = Bun.spawn([kernelPath, "--listen", `127.0.0.1:${port}`], {
    stdout: "inherit",
    stderr: "inherit",
  });
  let batch: Bun.Subprocess | undefined;
  let host: RuntimeHost | undefined;
  try {
    await waitForPort(port, kernel);
    batch = Bun.spawn([batchPath], {
      env: { ...process.env, LENSO_RUNTIME_PORT: String(port) },
      stdout: "inherit",
      stderr: "inherit",
    });
    host = await RuntimeHost.connect({
      namespace: "demo",
      service: "verification",
      instance: "bun-one",
      port,
      contract: emptyContract,
    });
    const deadline = Date.now() + 5000;
    while (true) {
      const snapshot = await host.status();
      if (snapshot.services.some((service) =>
        typeof service === "object" && service !== null &&
        "namespace" in service && service.namespace === "demo" &&
        "service" in service && service.service === "batch" &&
        "state" in service && service.state === "ready"
      )) break;
      if (batch.exitCode !== null) throw new Error(`Batch exited: ${batch.exitCode}`);
      if (Date.now() > deadline) throw new Error("Batch did not become ready within 5s");
      await Bun.sleep(25);
    }
    return {
      port,
      host,
      kernel,
      batch,
      clientPath,
      async close() {
        try {
          await host!.close();
        } finally {
          try {
            await stopProcess(batch!);
          } finally {
            await stopProcess(kernel);
          }
        }
      },
    };
  } catch (error) {
    try {
      await host?.close();
    } finally {
      try {
        if (batch) await stopProcess(batch);
      } finally {
        await stopProcess(kernel);
      }
    }
    throw error;
  }
}
