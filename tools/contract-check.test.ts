import { afterAll, beforeAll, expect, test } from "bun:test";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { checkContracts, exitCode, formatHuman, type Diagnostic } from "./contract-check/index";
import { generate } from "./generate";
import { type Contract, type Schema, validateContract } from "../sdk/typescript/schema";
import logs from "../examples/contracts/logs.json";

const scalar: Schema = { type: "string" };
function contract(input: Schema = scalar): Contract {
  return { version: "1", methods: { run: { mode: "unary", input, output: { type: "null" }, error: { type: "null" } } } };
}
function object(properties: Record<string, Schema>, required = Object.keys(properties)): Schema {
  return { type: "object", properties, required, additionalProperties: false };
}
const difference = (before: unknown, after: unknown) => checkContracts(before, after).diagnostics.filter((d) => d.category === "change");
function at(diagnostics: Diagnostic[], pointer: string): Diagnostic {
  const result = diagnostics.find((diagnostic) => diagnostic.pointer === pointer);
  expect(result).toBeDefined();
  return result!;
}

test("real example is accepted without mutating inputs or claiming safety", () => {
  const original = JSON.stringify(logs);
  const report = checkContracts(logs, structuredClone(logs));
  expect(report).toMatchObject({
    formatVersion: 1, strictMatch: "match", migrationSafety: "unknown",
    before: { validation: "valid", generator: "accepted" },
    after: { validation: "valid", generator: "accepted" }, diagnostics: [],
  });
  expect(JSON.stringify(logs)).toBe(original);
  expect(exitCode(report)).toBe(0);
  expect(formatHuman(report)).toContain("Migration safety: unknown");
});

test("object key order and equivalent binary64 tokens match, arrays remain ordered", () => {
  const before = contract(object({ first: scalar, second: { type: "number", minimum: 0 } }));
  const after = JSON.parse(JSON.stringify(before));
  after.methods.run.input.properties = { second: { minimum: -0, type: "number" }, first: scalar };
  expect(checkContracts(before, after).strictMatch).toBe("match");
  after.methods.run.input.required.reverse();
  const report = checkContracts(before, after);
  expect(report.strictMatch).toBe("mismatch");
  expect(at(report.diagnostics, "/methods/run/input/required").code).toBe("fields.required_changed");
  const numeric = (token: string) => JSON.parse(`{"version":"1","methods":{"run":{"mode":"unary","input":{"type":"integer","enum":[${token}]},"output":{"type":"null"},"error":{"type":"null"}}}}`);
  expect(checkContracts(numeric("1"), numeric("1.0")).strictMatch).toBe("match");
  expect(checkContracts(numeric("1e0"), numeric("1")).strictMatch).toBe("match");
  expect(checkContracts(contract({ type: "integer", enum: [1, 2] }), contract({ type: "integer", enum: [2, 1] })).strictMatch).toBe("mismatch");
});

test("version, mode, method addition and removal have separate paths", () => {
  const before = contract(), after = structuredClone(before);
  after.version = "2026-10";
  after.methods.run!.mode = "stream";
  after.methods.extra = structuredClone(before.methods.run!);
  let result = difference(before, after);
  expect(at(result, "/version").code).toBe("version.changed");
  expect(at(result, "/methods/run/mode").code).toBe("mode.changed");
  expect(at(result, "/methods/extra").code).toBe("method.added");
  result = difference(after, before);
  expect(at(result, "/methods/extra").code).toBe("method.removed");
  expect(checkContracts({ version: "2", methods: {} }, { version: "2", methods: {} }).strictMatch).toBe("match");
});

test("nested fields, required presence, array items and all constraint families are diagnosed", () => {
  const before = contract(object({
    text: { type: "string", minLength: 1, maxLength: 10, enum: ["a", "bb"] },
    count: { type: "integer", minimum: 0, maximum: 10 },
    rows: { type: "array", minItems: 1, maxItems: 10, items: object({ value: scalar }) },
    removed: scalar,
  }));
  const after = structuredClone(before);
  const input = after.methods.run!.input, props = input.properties!;
  props.text = { type: "string", minLength: 2, maxLength: 20, enum: ["bb", "ccc"] };
  props.count = { type: "number", minimum: -1, maximum: 20 };
  props.rows!.minItems = 0;
  delete props.rows!.maxItems;
  props.rows!.items!.properties!.value = { type: "boolean" };
  delete props.removed;
  props.added = scalar;
  input.required = ["text", "rows"];
  const result = checkContracts(before, after);
  expect(result.strictMatch).toBe("mismatch");
  expect(exitCode(result)).toBe(1);
  const base = "/methods/run/input";
  for (const suffix of ["properties/text/minLength", "properties/text/maxLength", "properties/count/minimum", "properties/count/maximum", "properties/rows/minItems", "properties/rows/maxItems"])
    expect(at(result.diagnostics, `${base}/${suffix}`).code).toBe("constraint.changed");
  expect(at(result.diagnostics, `${base}/properties/text/enum`).code).toBe("constraint.enum_changed");
  expect(at(result.diagnostics, `${base}/properties/rows/items/properties/value/type`).code).toBe("type.changed");
  expect(at(result.diagnostics, `${base}/properties/removed`).code).toBe("field.removed");
  expect(at(result.diagnostics, `${base}/properties/added`).code).toBe("field.added");
  expect(at(result.diagnostics, `${base}/required`).after).toEqual(["text", "rows"]);
});

test("output and error paths are checked, including nullable nested constraints", () => {
  const before = contract();
  before.methods.run!.output = { anyOf: [{ type: "null" }, object({ label: { type: "string", maxLength: 8 } })] };
  const after = structuredClone(before);
  after.methods.run!.output.anyOf![1]!.properties!.label!.maxLength = 9;
  after.methods.run!.error = { type: "string" };
  expect(at(difference(before, after), "/methods/run/output/anyOf/1/properties/label/maxLength").code).toBe("constraint.changed");
  expect(at(difference(before, after), "/methods/run/error/type").code).toBe("type.changed");
  after.methods.run!.output.anyOf!.reverse();
  expect(at(difference(before, after), "/methods/run/output/anyOf").code).toBe("nullability.changed");
});

test("optional input additions and widened constraints still fail strict matching", () => {
  const before = contract(object({ value: { type: "integer", minimum: 0 } }));
  const after = structuredClone(before);
  after.methods.run!.input.properties!.extra = scalar;
  after.methods.run!.input.properties!.value!.minimum = -1;
  const report = checkContracts(before, after);
  expect(report.strictMatch).toBe("mismatch");
  expect(report.migrationSafety).toBe("unknown");
  expect(exitCode(report)).toBe(1);
});

test("validation diagnoses unsupported features and malformed nodes at their actual path", () => {
  const cases: [unknown, string][] = [
    [null, ""],
    [{ ...contract(), version: 1 }, "/version"],
    [{ ...contract(), extra: true }, "/extra"],
    [{ version: "1", methods: [] }, "/methods"],
    [{ version: "1", methods: { run: null } }, "/methods/run"],
    [{ version: "1", methods: { run: { ...contract().methods.run, mode: "duplex" } } }, "/methods/run/mode"],
    [{ version: "1", methods: { run: { ...contract().methods.run, mode: { toString: 0 } } } }, "/methods/run/mode"],
    [{ version: "1", methods: { run: { ...contract().methods.run, mode: ["unary"] } } }, "/methods/run/mode"],
    [contract({ type: "string", format: "email" } as Schema), "/methods/run/input/format"],
    [contract({ type: "object", properties: {}, required: [] }), "/methods/run/input/additionalProperties"],
    [contract({ type: "array" }), "/methods/run/input/items"],
    [contract({ type: "string", maxLength: -1 }), "/methods/run/input/maxLength"],
    [contract({ type: "integer", enum: [1, 1] }), "/methods/run/input/enum"],
    [contract(object({ optional: { anyOf: [scalar, { type: "null" }] } }, [])), "/methods/run/input/properties/optional"],
    [contract(object({ optional: { type: "null" } }, [])), "/methods/run/input/properties/optional"],
    [contract(object({ child: null as unknown as Schema })), "/methods/run/input/properties/child"],
    [contract({ anyOf: [scalar, { type: "integer" }] }), "/methods/run/input/anyOf"],
    [contract({ type: "array", items: { type: "string", pattern: "a" } as Schema }), "/methods/run/input/items/pattern"],
    [contract(object({ value: scalar }, ["missing"])), "/methods/run/input/required"],
  ];
  for (const [candidate, pointer] of cases) {
    expect(() => validateContract(candidate as Contract)).toThrow();
    const report = checkContracts(contract(), candidate);
    expect(report.after).toEqual({ validation: "invalid", generator: "unknown" });
    expect(report.strictMatch).toBe("unknown");
    expect(exitCode(report)).toBe(2);
    expect(at(report.diagnostics, pointer).category).toBe("validation");
  }
});

test("unknown JSON boundaries never claim equal even for identical inputs", () => {
  const cycle: Record<string, unknown> = {};
  cycle.self = cycle;
  const sparse = contract();
  sparse.methods.run!.input = { type: "string", enum: Array(1) };
  for (const value of [cycle, undefined, { ...contract(), version: "\ud800" }, contract({ type: "number", minimum: Infinity }), sparse, new Date()]) {
    const report = checkContracts(value, value);
    expect(report.strictMatch).toBe("unknown");
    expect(report.before.validation).toBe("unknown");
    expect(exitCode(report)).toBe(2);
  }
});

test("schema depth limit is reported without overflowing the checker", () => {
  let schema: Schema = scalar;
  for (let i = 0; i < 66; i++) schema = { type: "array", items: schema };
  const report = checkContracts(contract(schema), contract());
  expect(report.before.validation).toBe("invalid");
  expect(report.diagnostics[0]!.message).toBe("schema too deep");
});

test("generator accepts mapped wire names and locates mapped collisions", () => {
  for (const method of ["new", "constructor", "a-b", "a/b~c", 'a"b', "a_", "a__b"]) {
    const candidate = { version: "1", methods: { [method]: contract().methods.run! } };
    expect(checkContracts(candidate, candidate).before.generator).toBe("accepted");
  }
  for (const property of ["self", "Self", "crate", "has-space ", "a_", "a__b"]) {
    const candidate = contract({ type: "array", items: object({ [property]: scalar }) });
    expect(checkContracts(candidate, candidate).before.generator).toBe("accepted");
  }
  const candidate = contract(object({ "a-b": scalar, a_b: scalar }));
  expect(checkContracts(candidate, candidate).diagnostics[0]).toMatchObject({
    code: "generator.property_identifier", pointer: "/methods/run/input/properties/a_b",
    relatedPath: '$["methods"]["run"]["input"]["properties"]["a-b"]',
  });
});

test("generator collisions include both schema paths and respect selected client name", () => {
  const candidate = contract(object({ foo_bar: object({}), fooBar: object({}) }));
  expect(() => generate(candidate, "logs")).toThrow("generated type name collision");
  const report = checkContracts(candidate, candidate);
  expect(report.diagnostics[0]).toMatchObject({
    code: "generator.type_name_collision",
    path: '$["methods"]["run"]["input"]["properties"]["fooBar"]',
    relatedPath: '$["methods"]["run"]["input"]["properties"]["foo_bar"]',
  });
  const named = contract(object({ client: object({}) }));
  expect(checkContracts(named, named).before.generator).toBe("accepted");
  expect(checkContracts(named, named, { clientName: "run_input" }).diagnostics[0]).toMatchObject({
    code: "generator.type_name_collision", relatedPath: "$",
  });
  expect(checkContracts(contract(), contract(), { clientName: "bad-name" }).diagnostics[0]!.code).toBe("generator.client_name");
});

test("prototype-looking names remain own fields and terminal controls are escaped", () => {
  const candidate = JSON.parse('{"version":"1","methods":{"__proto__":{"mode":"unary","input":{"type":"string"},"output":{"type":"null"},"error":{"type":"null"}}}}');
  expect(checkContracts(candidate, candidate).strictMatch).toBe("match");
  const after = JSON.parse(JSON.stringify(candidate));
  after.methods.__proto__.input = { type: "boolean" };
  expect(at(difference(candidate, after), "/methods/__proto__/input/type").code).toBe("type.changed");
  const control = { version: "1", methods: { ["a\n\x1b[31m\u009b31m"]: contract().methods.run! } };
  expect(formatHuman(checkContracts(control, contract()))).not.toContain("\x1b");
  expect(formatHuman(checkContracts(control, contract()))).not.toContain("\u009b");
  expect(formatHuman(checkContracts(control, contract()))).toContain("\\n");
});

let directory: string;
const cli = new URL("./contract-check.ts", import.meta.url).pathname;
beforeAll(async () => {
  directory = await mkdtemp(join(tmpdir(), "lenso-contract-check-"));
  await writeFile(join(directory, "before.json"), JSON.stringify(contract()));
  await writeFile(join(directory, "same.json"), JSON.stringify(contract()));
  await writeFile(join(directory, "changed.json"), JSON.stringify({ ...contract(), version: "2" }));
  await writeFile(join(directory, "invalid.json"), "not json");
  await writeFile(join(directory, "large.json"), " ".repeat(1024 * 1024 + 1));
  await writeFile(join(directory, "bad-utf8.json"), Buffer.from([0xff]));
  await writeFile(join(directory, "-dash.json"), JSON.stringify(contract()));
});
afterAll(async () => { if (directory) await rm(directory, { recursive: true, force: true }); });
async function run(...args: string[]) {
  const process = Bun.spawn([Bun.argv[0]!, cli, ...args], { cwd: directory, stdout: "pipe", stderr: "pipe" });
  const [stdout, stderr, code] = await Promise.all([new Response(process.stdout).text(), new Response(process.stderr).text(), process.exited]);
  return { stdout, stderr, code };
}

test("CLI emits one machine-readable report and stable status for match and change", async () => {
  const same = await run("--json", "before.json", "same.json");
  expect(same.code).toBe(0);
  expect(same.stderr).toBe("");
  expect(JSON.parse(same.stdout).strictMatch).toBe("match");
  const changed = await run("before.json", "changed.json", "--json");
  expect(changed.code).toBe(1);
  expect(JSON.parse(changed.stdout).diagnostics[0].path).toBe('$["version"]');
  const human = await run("before.json", "changed.json");
  expect(human.stdout).toContain("Strict match: mismatch");
  expect(human.stdout).toContain('$["version"]');
  expect(await readFile(join(directory, "before.json"), "utf8")).toBe(JSON.stringify(contract()));
  expect(await readFile(join(directory, "changed.json"), "utf8")).toBe(JSON.stringify({ ...contract(), version: "2" }));
});

test("CLI file/JSON/usage errors remain structured unknown results", async () => {
  for (const args of [
    ["before.json", "missing.json"], ["invalid.json", "same.json"],
    ["large.json", "same.json"], ["bad-utf8.json", "same.json"], [".", "same.json"],
    ["before.json"], ["--wat", "before.json", "same.json"],
    ["--name"], ["--name", "logs", "--name", "logs", "before.json", "same.json"],
  ]) {
    const result = await run("--json", ...args);
    expect(result.code).toBe(2);
    expect(result.stderr).toBe("");
    expect(JSON.parse(result.stdout).strictMatch).toBe("unknown");
    expect(JSON.parse(result.stdout).diagnostics[0].path).toBe("$");
  }
  const invalid = await run("invalid.json", "same.json");
  expect(invalid.code).toBe(2);
  expect(invalid.stdout).toBe("");
  expect(invalid.stderr).toContain("Invalid JSON");
});

test("CLI supports explicit generator names, dash paths and help", async () => {
  expect((await run("--json", "--", "-dash.json", "same.json")).code).toBe(0);
  const bad = await run("--json", "--name", "bad-name", "before.json", "same.json");
  expect(bad.code).toBe(1);
  expect(JSON.parse(bad.stdout).diagnostics[0].code).toBe("generator.client_name");
  const help = await run("--help");
  expect(help.code).toBe(0);
  expect(help.stdout).toContain("Usage:");
});

test.skipIf(process.platform === "win32")("CLI rejects a named pipe without waiting for a writer", async () => {
  const pipe = join(directory, "contract.fifo");
  const created = Bun.spawnSync(["mkfifo", pipe]);
  expect(created.exitCode).toBe(0);
  const result = await run("--json", pipe, "same.json");
  expect(result.code).toBe(2);
  expect(JSON.parse(result.stdout).diagnostics[0].message).toBe("Expected a regular JSON file.");
});
