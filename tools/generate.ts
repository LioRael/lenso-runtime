import { mkdir, writeFile } from "node:fs/promises";
import { type Contract, type Schema, validateContract } from "../sdk/typescript/schema";

const identifier = /^[A-Za-z][A-Za-z0-9_]*$/;
function pascal(name: string): string {
  return name
    .split("_")
    .map((s) => s[0]!.toUpperCase() + s.slice(1))
    .join("");
}
function rustJson(value: unknown): string {
  const json = JSON.stringify(value);
  let hashes = "#";
  while (json.includes('"' + hashes)) hashes += "#";
  return `serde_json::from_str::<serde_json::Value>(r${hashes}"${json}"${hashes}).map_err(serde::de::Error::custom)?`;
}
const rustKeywords = new Set([
  "self",
  "Self",
  "super",
  "crate",
  "type",
  "match",
  "ref",
  "mod",
  "pub",
  "fn",
  "use",
  "let",
  "move",
  "async",
  "await",
  "loop",
  "in",
  "where",
  "enum",
  "struct",
  "impl",
  "trait",
  "const",
  "static",
  "return",
  "break",
  "continue",
  "if",
  "else",
  "for",
  "while",
  "as",
  "dyn",
  "unsafe",
  "extern",
  "true",
  "false",
]);

/** Generation rejects optional nullable fields rather than conflating absence and null. */
export function generate(contract: Contract, name: string): { typescript: string; rust: string } {
  validateContract(contract);
  if (!identifier.test(name)) throw new Error("invalid client name");
  const ts: string[] = [],
    rs: string[] = [],
    names = new Set<string>();
  let optionalHelper = false,
    requiredHelper = false;
  const contractName = name + "Contract";
  function render(schema: Schema, typeName: string): [string, string] {
    if (schema.enum && (schema.type === "object" || schema.type === "array"))
      throw new Error("compound enums unsupported");
    if (schema.anyOf) {
      const branch = schema.anyOf.find((s) => s.type !== "null")!;
      const [t, r] = render(branch, typeName + "Value");
      return [`${t} | null`, `Option<${r}>`];
    }
    if (schema.type === "object") {
      if (names.has(typeName)) throw new Error(`generated type name collision: ${typeName}`);
      names.add(typeName);
      const tf: string[] = [],
        rf: string[] = [];
      for (const [key, value] of Object.entries(schema.properties!)) {
        if (!identifier.test(key) || ["self", "Self", "super", "crate"].includes(key))
          throw new Error(`unsupported property identifier: ${key}`);
        const optional = !schema.required!.includes(key);
        if (optional && (value.anyOf || value.type === "null"))
          throw new Error(
            `optional nullable field ${key} is unsupported: absence must differ from null`,
          );
        const [t, r] = render(value, typeName + pascal(key));
        tf.push(`  ${JSON.stringify(key)}${optional ? "?" : ""}: ${t};`);
        if (optional) {
          optionalHelper = true;
          rf.push(
            `    #[serde(default, skip_serializing_if = "Option::is_none", deserialize_with = "present_non_null")]`,
          );
        }
        // Required nullable fields must be present even when their value is null.
        else if (value.anyOf || value.type === "null") {
          requiredHelper = true;
          rf.push(`    #[serde(deserialize_with = "required_value")]`);
        }
        rf.push(
          `    pub ${rustKeywords.has(key) ? "r#" : ""}${key}: ${optional ? `Option<${r}>` : r},`,
        );
      }
      ts.push(`export type ${typeName} = {\n${tf.join("\n")}\n};`);
      rs.push(
        `#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]\n#[serde(deny_unknown_fields)]\npub struct ${typeName} {\n${rf.join("\n")}\n}`,
      );
      return [typeName, typeName];
    }
    if (schema.type === "array") {
      const [t, r] = render(schema.items!, typeName + "Item");
      return [`Array<${t}>`, `Vec<${r}>`];
    }
    const types: Record<string, [string, string]> = {
      string: ["string", "String"],
      integer: ["number", "i64"],
      number: ["number", "f64"],
      boolean: ["boolean", "bool"],
      null: ["null", "()"],
    };
    const result = types[schema.type!]!;
    if (schema.enum) {
      if (names.has(typeName)) throw new Error(`generated type name collision: ${typeName}`);
      names.add(typeName);
      const membership =
        schema.type === "number"
          ? `.iter().any(|candidate| candidate.as_f64() == serialized.as_f64())`
          : `.contains(&serialized)`;
      rs.push(
        `#[derive(Debug, Clone, serde::Serialize)]\n#[serde(transparent)]\npub struct ${typeName}(pub ${result[1]});\nimpl<'de> serde::Deserialize<'de> for ${typeName} {\n    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {\n        let value = <${result[1]} as serde::Deserialize>::deserialize(deserializer)?;\n        let serialized = serde_json::to_value(&value).map_err(serde::de::Error::custom)?;\n        if [${schema.enum.map(rustJson).join(", ")}]${membership} { Ok(Self(value)) }\n        else { Err(serde::de::Error::custom("value outside enum")) }\n    }\n}`,
      );
      return [schema.enum.map((v) => JSON.stringify(v)).join(" | "), typeName];
    }
    return result;
  }
  const methods: string[] = [],
    rustMethods: string[] = [];
  for (const [method, definition] of Object.entries(contract.methods)) {
    if (
      !identifier.test(method) ||
      ["constructor", "host", "target", "self", "Self", "super", "crate"].includes(method)
    )
      throw new Error(`unsupported method identifier: ${method}`);
    const prefix = pascal(method);
    for (const slot of ["input", "output", "error"] as const) {
      const typeName = prefix + pascal(slot),
        [t, r] = render(definition[slot], typeName);
      if (t !== typeName && r !== typeName) {
        if (names.has(typeName)) throw new Error(`generated type name collision: ${typeName}`);
        names.add(typeName);
      }
      if (t !== typeName) ts.push(`export type ${typeName} = ${t};`);
      if (r !== typeName) rs.push(`pub type ${typeName} = ${r};`);
    }
    ts.push(
      `export function is${prefix}Error(error: unknown): error is RuntimeError<${prefix}Error> & { code: "business"; details: ${prefix}Error } {\n  if (!(error instanceof RuntimeError) || error.code !== "business") return false;\n  try { validateValue(${contractName}.methods[${JSON.stringify(method)}]!.error, error.details); return true; } catch { return false; }\n}`,
    );
    const mode = definition.mode === "unary" ? "call" : "stream";
    methods.push(
      `  ${method}(input: ${prefix}Input, options: CallOptions = {}): ${mode === "call" ? `Promise<${prefix}Output>` : `AsyncIterableIterator<${prefix}Output>`} {\n    return this.host.${mode}<${prefix}Output>(this.target, ${JSON.stringify(method)}, input, { ...options, contract: ${contractName}.methods[${JSON.stringify(method)}]! });\n  }`,
    );
    rustMethods.push(
      `    pub ${mode === "call" ? "async " : ""}fn ${rustKeywords.has(method) ? "r#" : ""}${method}(&self, input: ${prefix}Input, options: runtime_sdk::CallOptions) -> Result<${mode === "call" ? `${prefix}Output` : `runtime_sdk::ResponseStream<${prefix}Output>`}, runtime_sdk::RuntimeError> {\n        self.host.${mode}(&self.target, ${JSON.stringify(method)}, input, options)${mode === "call" ? ".await" : ""}\n    }`,
    );
  }
  return {
    typescript: `// Generated by tools/generate.ts. Do not edit.\nimport { RuntimeHost, RuntimeError, type CallOptions, type Target, type Contract } from "@lenso/runtime-sdk";\nimport { validateValue } from "@lenso/runtime-sdk/schema";\nexport const ${contractName}: Contract = ${JSON.stringify(contract, null, 2)};\n\n${ts.join("\n\n")}\n\nexport class ${pascal(name)}Client {\n  private target: Target;\n  constructor(private host: RuntimeHost, target: Pick<Target, "namespace" | "service">) {\n    this.target = { ...target, contractVersion: ${contractName}.version };\n  }\n${methods.join("\n")}\n}\n`,
    rust: `// Generated by tools/generate.ts. Do not edit.\n${optionalHelper ? `fn present_non_null<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>\nwhere D: serde::Deserializer<'de>, T: serde::Deserialize<'de> {\n    T::deserialize(deserializer).map(Some)\n}\n` : ""}${requiredHelper ? `fn required_value<'de, D, T>(deserializer: D) -> Result<T, D::Error>\nwhere D: serde::Deserializer<'de>, T: serde::Deserialize<'de> {\n    T::deserialize(deserializer)\n}\n` : ""}\n${rs.join("\n\n")}\n\npub struct ${pascal(name)}Client {\n    host: runtime_sdk::RuntimeHost,\n    target: runtime_sdk::Target,\n}\nimpl ${pascal(name)}Client {\n    pub fn new(host: runtime_sdk::RuntimeHost, namespace: impl Into<String>, service: impl Into<String>) -> Self {\n        Self { host, target: runtime_sdk::Target { namespace: namespace.into(), service: service.into(), contract_version: ${JSON.stringify(contract.version)}.into() } }\n    }\n${rustMethods.join("\n")}\n}\n`,
  };
}
if (import.meta.main) {
  const [source, directory, name = "logs"] = process.argv.slice(2);
  if (!source || !directory)
    throw new Error("usage: bun tools/generate.ts <contract.json> <output-dir> [client-name]");
  const output = generate(await Bun.file(source).json(), name);
  await mkdir(directory, { recursive: true });
  await writeFile(`${directory}/${name}.ts`, output.typescript);
  await writeFile(`${directory}/${name}.rs`, output.rust);
}
