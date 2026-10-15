import { open } from "node:fs/promises";
import { constants } from "node:fs";
import { checkContracts, exitCode, formatHuman } from "./contract-check/index";

const usage = `Usage: bun tools/contract-check.ts [--json] [--name <client-name>] <before.json> <after.json>

Compare restricted v1 contracts using existing strict structural matching.
--json    Emit one JSON report (including file/usage errors).
--name    Generator client name; defaults to logs, like tools/generate.ts.
--        End options, for paths beginning with a dash.
--help    Show this help.

Exit codes: 0 strict match and generator accepted; 1 mismatch or generator rejection;
2 invalid input, unknown analysis, or usage/I/O error.
Migration safety is always unknown. This command does not negotiate or promote contracts.
`;
const maxBytes = 1024 * 1024;

// Read at most the advertised limit even if the file grows after opening.
async function readContract(path: string): Promise<unknown> {
  // A named pipe must not block open() before the regular-file check.
  const file = await open(path, constants.O_RDONLY | constants.O_NONBLOCK);
  try {
    if (!(await file.stat()).isFile()) throw new Error("Expected a regular JSON file.");
    const bytes = Buffer.alloc(maxBytes + 1);
    let length = 0;
    while (length < bytes.length) {
      const result = await file.read(bytes, length, bytes.length - length, null);
      if (result.bytesRead === 0) break;
      length += result.bytesRead;
    }
    if (length > maxBytes) throw new Error("Analyzer file limit is 1 MiB; result is unknown.");
    const text = new TextDecoder("utf-8", { fatal: true }).decode(bytes.subarray(0, length));
    try { return JSON.parse(text); } catch { throw new Error("Invalid JSON document."); }
  } finally { await file.close(); }
}

export async function main(args = process.argv.slice(2)): Promise<number> {
  // Determine output mode even if parsing subsequently discovers a usage error.
  const optionEnd = args.indexOf("--");
  const json = args.slice(0, optionEnd < 0 ? args.length : optionEnd).includes("--json");
  let source = "options";
  try {
    const paths: string[] = [];
    let clientName = "logs", positional = false, named = false;
    for (let index = 0; index < args.length; index++) {
      const arg = args[index]!;
      if (!positional && arg === "--") { positional = true; continue; }
      if (!positional && arg === "--json") continue;
      if (!positional && arg === "--help") { process.stdout.write(usage); return 0; }
      if (!positional && arg === "--name") {
        if (named || !args[index + 1] || args[index + 1]!.startsWith("--")) throw new Error("--name requires one client name and may appear only once.");
        clientName = args[++index]!;
        named = true;
        continue;
      }
      if (!positional && arg.startsWith("-")) throw new Error(`Unknown option: ${JSON.stringify(arg)}`);
      paths.push(arg);
    }
    if (paths.length !== 2) throw new Error("Expected before.json and after.json.");
    source = "before";
    const before = await readContract(paths[0]!);
    source = "after";
    const after = await readContract(paths[1]!);
    const report = checkContracts(before, after, { clientName });
    process.stdout.write(json ? JSON.stringify(report, null, 2) + "\n" : formatHuman(report));
    return exitCode(report);
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown checker error.";
    const report = {
      formatVersion: 1, strictMatch: "unknown", migrationSafety: "unknown",
      diagnostics: [{ source, category: "analysis", code: source === "options" ? "cli.usage" : "cli.input", path: "$", pointer: "", message }],
    };
    if (json) process.stdout.write(JSON.stringify(report, null, 2) + "\n");
    else process.stderr.write(`contract-check: ${JSON.stringify(message)}\n${source === "options" ? usage : ""}`);
    return 2;
  }
}

if (import.meta.main) process.exitCode = await main();
