import { expect, test } from "bun:test";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { join, resolve } from "node:path";
import { generate } from "./generate";
import { type Contract, type Schema, validateValue } from "../sdk/typescript/schema";

const empty: Schema = { type: "object", properties: {}, required: [], additionalProperties: false };
const object = (properties: Record<string, Schema>, optional: string[] = []): Schema => ({
  type: "object", properties, required: Object.keys(properties).filter((key) => !optional.includes(key)),
  additionalProperties: false,
});
const definition = (input: Schema = empty): Contract["methods"][string] => ({
  mode: "unary", input, output: input, error: empty,
});
const contract = (input: Schema = empty): Contract => ({ version: "1", methods: { sample: definition(input) } });

test("field mapping preserves TS wire keys and explicitly renames Rust identifiers", () => {
  const output = generate(contract(object(Object.fromEntries([
    "first-name", "0value", "", "_", "self", "Self", "super", "crate", "type", "try", "gen", "abstract",
  ].map((key) => [key, { type: "string" }])))), "names");
  for (const [wire, source] of [
    ["first-name", "first_name"], ["0value", "_0value"], ["", "field"], ["_", "field_"],
    ["self", "self_"], ["Self", "Self_"], ["super", "super_"], ["crate", "crate_"],
    ["type", "r#type"], ["try", "r#try"], ["gen", "r#gen"], ["abstract", "r#abstract"],
  ]) {
    expect(output.typescript).toContain(`${JSON.stringify(wire)}: string`);
    expect(output.rust).toContain(`#[serde(rename = ${JSON.stringify(wire)})]\n    pub ${source}: String`);
  }
});

test.each([
  ["a-b", "a_b", "a_b"], ["self", "self_", "self_"], ["", "field", "field"],
  ["1value", "_1value", "_1value"], ["a b", "a-b", "a_b"], ["名", "字", "field_"],
])("field collisions report both wire keys: %s / %s", (first, second, source) => {
  expect(() => generate(contract(object({ [first]: { type: "string" }, [second]: { type: "string" } })), "names"))
    .toThrow(`generated Rust field name collision in SampleInput: ${JSON.stringify(first)} and ${JSON.stringify(second)} map to ${JSON.stringify(source)}`);
});

test("nested type names handle leading, trailing and repeated underscores", () => {
  const output = generate(contract(object({ "_a__b_": empty, "9-lives": empty })), "client__name_");
  expect(output.rust).toContain("pub struct SampleInputAB");
  expect(output.rust).toContain("pub struct SampleInputField9Lives");
  expect(output.typescript).toContain("export class ClientNameClient");
  expect(() => generate(contract(object({ "a_b": empty, "a__b": empty })), "names"))
    .toThrow("generated type name collision: SampleInputAB");
});

test.each([
  ["echo-name", "echo_name", "TypeScript"], ["new", "new_", "TypeScript"],
  ["constructor", "constructor_", "TypeScript"], ["self", "self_", "Rust"],
])("method collisions report the language and wire names: %s / %s", (first, second, language) => {
  expect(() => generate({ version: "1", methods: { [first]: definition(), [second]: definition() } }, "names"))
    .toThrow(`generated ${language} method name collision: ${JSON.stringify(first)} and ${JSON.stringify(second)}`);
});

test("unrepresentable Rust strings fail with a diagnostic", () => {
  expect(() => generate(contract(object({ ["bad\ud800key"]: { type: "string" } })), "names"))
    .toThrow("unsupported unpaired surrogate");
});

test.each(["runtime", "runtime-"])("generated types cannot shadow SDK imports: %s", (method) => {
  expect(() => generate({ version: "1", methods: { [method]: definition({ type: "string" }) } }, "names"))
    .toThrow("generated type name collision: RuntimeError");
});

test.each(["constructor", "host", "target", "new", "build", "then", "__proto__"])(
  "reserved generated members receive a stable suffix: %s", (method) => {
    const output = generate({ version: "1", methods: { [method]: definition() } }, "names");
    expect(output.typescript).toContain(`  ${method}_(input:`);
    expect(output.rust).toContain(`fn ${method}_(`);
    expect(output.rust).toContain(`self.handlers.unary(${JSON.stringify(method)}, handler)`);
  },
);

// Compile every strict/reserved Rust keyword in edition 2024, including path
// keywords that cannot use raw identifiers. These remain ordinary TS keys.
const keywords = "as break const continue crate else enum extern false fn for if impl in let loop match mod move mut pub ref return self Self static struct super trait true type unsafe use where while async await dyn abstract become box do final macro override priv typeof unsized virtual yield try gen".split(" ");
const weirdKeys = ["first-name", "123value", "_leading", "repeat__separator", "", "_", "space key", "__proto__", "quote\"key", "slash\\key", "literal\\bkey", "line\r\nkey", "tab\tkey", "nul\0key", "back\bkey", "form\fkey", "名前", "emoji🙂key"];
const payload = Object.fromEntries([...keywords, ...weirdKeys].map((key) => [key, `value:${key}`]));
Object.assign(payload, {
  "nested-value": { match: "ok", "child-name": 7 },
  "items-list": [{ "item-code": "one" }],
  "nullable-value": null,
});
const payloadSchema = object({
  ...Object.fromEntries([...keywords, ...weirdKeys].map((key) => [key, { type: "string" } as Schema])),
  "nested-value": object({ match: { type: "string", enum: ["ok"] }, "child-name": { type: "integer" } }),
  "items-list": { type: "array", items: object({ "item-code": { type: "string" } }) },
  "optional-value": { type: "integer" },
  "nullable-value": { anyOf: [object({ enum: { type: "string" } }), { type: "null" }] },
}, ["optional-value"]);
const errorSchema = object({ "error-code": { type: "string" }, self: { type: "string" } });
const fixtureContract: Contract = {
  version: "identifiers\r\n1",
  methods: {
    "echo-names": { ...definition(payloadSchema), error: errorSchema },
    "try": { mode: "unary", input: { type: "boolean" }, output: { type: "string" }, error: errorSchema },
    "stream-items": { ...definition(payloadSchema), mode: "stream", error: errorSchema },
    "constructor": definition({ type: "string" }),
    ["__proto__"]: definition({ type: "string" }),
    "new": definition({ type: "string" }),
    "build": definition({ type: "string" }),
    "control\b\f\0\r\n\\b": definition({ type: "string" }),
  },
};
// The method is exactly the existing 256-byte wire limit. The property forces
// a delimiter beyond Rust's 255-hash raw-string limit in embedded contract JSON.
const delimiterContract: Contract = {
  version: "1",
  methods: { ['"' + "#".repeat(255)]: definition(object({ ['"' + "#".repeat(260)]: { type: "string" } })) },
};

const root = resolve(import.meta.dir, "..");
async function run(command: string[], cwd = root) {
  const child = Bun.spawn(command, { cwd, stdout: "pipe", stderr: "pipe" });
  const timer = setTimeout(() => child.kill("SIGKILL"), 120_000);
  try {
    const [code, stdout, stderr] = await Promise.all([
      child.exited, new Response(child.stdout).text(), new Response(child.stderr).text(),
    ]);
    if (code !== 0) throw new Error(`${command.join(" ")} exited ${code}\n${stdout}\n${stderr}`);
  } finally {
    clearTimeout(timer);
  }
}

test("generated Rust and TS compile and roundtrip unchanged wire names through the real kernel", async () => {
  const scratch = join(root, ".native-check");
  await mkdir(scratch, { recursive: true });
  const directory = await mkdtemp(join(scratch, "generator-"));
  let child: ReturnType<typeof Bun.spawn> | undefined;
  let service: { close(): void } | undefined;
  let stderr: Promise<string> | undefined;
  let deadline: ReturnType<typeof setTimeout> | undefined;
  try {
    const output = generate(fixtureContract, "names");
    await mkdir(join(directory, "src"));
    await Promise.all([
      writeFile(join(directory, "names.ts"), output.typescript),
      writeFile(join(directory, "src/names.rs"), output.rust),
      writeFile(join(directory, "src/raw_delimiters.rs"), generate(delimiterContract, "delimiters").rust),
      writeFile(join(directory, "src/payload.json"), JSON.stringify(payload)),
      writeFile(join(directory, "src/main.rs"), await readFile(join(import.meta.dir, "fixtures/identifiers.rs"), "utf8")),
      writeFile(join(directory, "check.ts"), await readFile(join(import.meta.dir, "fixtures/identifiers.ts.txt"), "utf8")),
      writeFile(join(directory, "Cargo.lock"), await readFile(join(root, "Cargo.lock"))),
      writeFile(join(directory, "Cargo.toml"), `[workspace]\n[package]\nname = "generator-identifiers"\nversion = "0.0.0"\nedition = "2024"\n[dependencies]\nruntime-sdk = { path = ${JSON.stringify(join(root, "crates/sdk"))} }\nkernel = { path = ${JSON.stringify(join(root, "crates/kernel"))} }\nserde = { version = "1", features = ["derive"] }\nserde_json = "1"\ntokio = { version = "1.53.3", features = ["rt-multi-thread", "macros", "net", "io-util", "io-std", "sync", "time"] }\n`),
    ]);
    // Test prototype keys independently: __proto__ must not mask constructor's
    // and toString's distinct contextual-typing failure in generated TS.
    const prototypeFiles: string[] = [];
    for (const key of Object.getOwnPropertyNames(Object.prototype)) {
      const value = object({ [key]: { type: "string" } });
      const fixture: Contract = { version: "1", methods: { [key]: definition(value) } };
      const file = join(directory, `prototype-${key}.ts`);
      await writeFile(file, generate(fixture, "prototype").typescript);
      expect((await import(file)).prototypeContract).toEqual(fixture);
      prototypeFiles.push(file);
    }
    await run(["bun", "x", "--no-install", "tsc", "--ignoreConfig", "--noEmit", "--strict", "--target", "ES2022", "--module", "Preserve", "--moduleResolution", "Bundler", "--types", "bun", "--skipLibCheck", join(directory, "check.ts"), ...prototypeFiles]);
    // Share dependency artifacts with the sequential workspace checks; the fixture
    // remains an independent package and still compiles its generated Rust.
    await run(["cargo", "build", "--manifest-path", join(directory, "Cargo.toml"), "--target-dir", join(root, "target")]);
    const generated = await import(join(directory, "names.ts"));
    expect(generated.namesContract).toEqual(fixtureContract);
    expect(Object.hasOwn(generated.namesContract.methods, "__proto__")).toBe(true);
    expect(Object.hasOwn(generated.namesContract.methods["echo-names"].input.properties, "__proto__")).toBe(true);
    validateValue(generated.namesContract.methods["echo-names"].input, payload);
    const check = await import(join(directory, "check.ts"));
    child = Bun.spawn([join(root, "target/debug/generator-identifiers")], { stdin: "pipe", stdout: "pipe", stderr: "pipe" });
    stderr = new Response(child.stderr as ReadableStream).text();
    deadline = setTimeout(() => child?.kill("SIGKILL"), 15_000);
    const reader = (child.stdout as ReadableStream<Uint8Array>).getReader();
    let buffer = "";
    const decoder = new TextDecoder();
    async function nextLine() {
      while (!buffer.includes("\n")) {
        const next = await reader.read();
        if (next.done) throw new Error(`Rust fixture exited early: ${await stderr}`);
        buffer += decoder.decode(next.value, { stream: true });
      }
      const end = buffer.indexOf("\n"), line = buffer.slice(0, end);
      buffer = buffer.slice(end + 1);
      return JSON.parse(line);
    }
    const { port } = await nextLine();
    service = await check.startService(port);
    await check.exercise(port, payload);
    (child.stdin as import("bun").FileSink).write("go\n");
    await (child.stdin as import("bun").FileSink).flush();
    expect(await nextLine()).toEqual({ rust_client: "pass", serde: "pass" });
    expect(await child.exited).toBe(0);
    reader.releaseLock();
  } finally {
    if (deadline) clearTimeout(deadline);
    await service?.close();
    if (child && child.exitCode === null) {
      child.kill("SIGKILL");
      await child.exited;
    }
    await stderr;
    await rm(directory, { recursive: true, force: true });
  }
}, 180_000);
