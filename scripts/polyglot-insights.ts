// Bootstrap before importing the installed SDK, so a fresh checkout has one command.
import { resolve } from "node:path";

const root = resolve(import.meta.dir, "..");
const example = resolve(root, "examples/polyglot-insights");
const args = process.argv.slice(2);
if (args.includes("--help")) {
  console.log("bun scripts/polyglot-insights.ts [--verify] [--input path.csv]\n" +
    "Requires Bun, Node, Cargo/Rust and Python 3.11+. Uses sdk/python; LENSO_PYTHON_SDK_ROOT can select another checkout.\n" +
    "Builds locally, processes synthetic CSV, prints progress/report, and cleans up all owned services.");
  process.exit(0);
}
for (let i = 0; i < args.length; i++) {
  if (args[i] === "--verify") continue;
  if (args[i] === "--input" && args[i + 1]) { i++; continue; }
  throw new Error(`Unknown or incomplete argument: ${args[i]}`);
}
const python = process.env.PYTHON ?? "python3";
const sdkRoot = resolve(process.env.LENSO_PYTHON_SDK_ROOT ?? resolve(root, "sdk/python"));
const pythonPath = resolve(sdkRoot, "src");
if (!await Bun.file(resolve(pythonPath, "lenso_runtime/__init__.py")).exists())
  throw new Error(`Python SDK missing: ${sdkRoot}. Integrate the Python SDK commit or set LENSO_PYTHON_SDK_ROOT to its sdk/python directory. No Python mock fallback exists.`);

async function run(command: string[], env = process.env) {
  console.error(`Build: ${command.join(" ")}`);
  const child = Bun.spawn(command, { cwd: root, env, stdout: "pipe", stderr: "pipe", detached: process.platform !== "win32" });
  let tail = "";
  const pump = async (stream: ReadableStream<Uint8Array>) => {
    const decoder = new TextDecoder();
    for await (const bytes of stream) tail = (tail + decoder.decode(bytes, { stream: true })).slice(-16000);
  };
  let interrupted = false;
  let escalation: ReturnType<typeof setTimeout> | undefined;
  const killTree = (signal: "SIGTERM" | "SIGKILL") => {
    try {
      if (process.platform === "win32") child.kill(signal);
      else process.kill(-child.pid, signal);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ESRCH") throw error;
    }
  };
  const stop = () => {
    interrupted = true;
    killTree("SIGTERM");
    escalation ??= setTimeout(() => killTree("SIGKILL"), 2000);
  };
  process.once("SIGINT", stop); process.once("SIGTERM", stop);
  const timer = setTimeout(() => killTree("SIGKILL"), 180000);
  try {
    await Promise.all([pump(child.stdout), pump(child.stderr), child.exited]);
    if (interrupted || child.exitCode !== 0) throw new Error(`Command ${interrupted ? "interrupted" : "failed"} (${child.exitCode ?? child.signalCode}): ${command.join(" ")}\n${tail}`);
  } finally {
    clearTimeout(timer); clearTimeout(escalation); process.off("SIGINT", stop); process.off("SIGTERM", stop);
  }
}

await run([python, "-c", "import sys; assert sys.version_info >= (3,11); import lenso_runtime; print(lenso_runtime.__file__)"],
  { ...process.env, PYTHONPATH: pythonPath, PYTHONDONTWRITEBYTECODE: "1" });
await run([process.execPath, "install", "--frozen-lockfile", "--ignore-scripts"]);
await run([process.execPath, "run", "build:sdk"]);
await run(["cargo", "build", "--locked", "-p", "kernel", "--target-dir", resolve(root, "target")]);
await run(["cargo", "build", "--locked", "--manifest-path", resolve(example, "rust/Cargo.toml"), "--target-dir", resolve(root, "target")]);

const { main } = await import("../examples/polyglot-insights/main");
await main({ python, pythonPath, verify: args.includes("--verify"),
  input: args.includes("--input") ? resolve(args[args.indexOf("--input") + 1]!) : resolve(example, "fixtures/requests.csv") });
