import { type Contract, type Schema, validateContract } from "./schema";

const identifier = /^[A-Za-z][A-Za-z0-9_]*$/;
// Keep existing ASCII identifiers stable. Wire names are never normalized;
// only identifiers in generated source use this spelling.
function sourceIdentifier(name: string): string {
  let result = name.replace(/[^A-Za-z0-9_]/gu, "_");
  if (!result) return "field";
  if (/^[0-9]/.test(result)) result = "_" + result;
  return result === "_" ? "field_" : result;
}
export function pascal(name: string): string {
  const result = sourceIdentifier(name)
    .split("_")
    .filter(Boolean)
    .map((s) => s[0]!.toUpperCase() + s.slice(1))
    .join("");
  return !result ? "Field" : /^[0-9]/.test(result) ? "Field" + result : result;
}
function rustJson(value: unknown): string {
  return `serde_json::from_str::<serde_json::Value>(${rustString(JSON.stringify(value))}).map_err(serde::de::Error::custom)?`;
}
function rustString(value: string): string {
  let hashes = "#";
  while (value.includes('"' + hashes)) {
    // Rust permits at most 255 raw-string delimiter hashes.
    if (hashes.length === 255) return rustLiteral(value);
    hashes += "#";
  }
  return `r${hashes}"${value}"${hashes}`;
}
// JSON and Rust have different escape syntax for control characters. In
// particular, raw Rust strings normalize CRLF and cannot preserve wire names.
function rustLiteral(value: string): string {
  let result = '"';
  for (const character of value) {
    const code = character.codePointAt(0)!;
    if (code >= 0xD800 && code <= 0xDFFF)
      throw new Error("unsupported unpaired surrogate in Rust string");
    if (character === '"' || character === "\\") result += "\\" + character;
    else if (character === "\n") result += "\\n";
    else if (character === "\r") result += "\\r";
    else if (character === "\t") result += "\\t";
    else if (code < 0x20 || code === 0x7F) result += `\\u{${code.toString(16)}}`;
    else result += character;
  }
  return result + '"';
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
  "mut",
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
  "abstract",
  "become",
  "box",
  "do",
  "final",
  "macro",
  "override",
  "priv",
  "typeof",
  "unsized",
  "virtual",
  "yield",
  "try",
  "gen",
]);
const rustPathKeywords = new Set(["self", "Self", "super", "crate"]);
export function rustIdentifier(name: string): string {
  const result = sourceIdentifier(name);
  if (rustPathKeywords.has(result)) return result + "_";
  return rustKeywords.has(result) ? "r#" + result : result;
}
// These names belong to generated client/server machinery. Map both languages
// consistently, then check collisions with user-provided spellings (e.g. new_).
const reservedMethods = new Set(["constructor", "host", "target", "new", "build", "__proto__", "then"]);
export function methodIdentifier(name: string): string {
  const result = sourceIdentifier(name);
  return reservedMethods.has(result) ? result + "_" : result;
}
function claimName(names: Map<string, string>, name: string, wireName: string, context: string): void {
  const previous = names.get(name);
  if (previous !== undefined)
    throw new Error(`${context}: ${JSON.stringify(previous)} and ${JSON.stringify(wireName)} map to ${JSON.stringify(name)}`);
  names.set(name, wireName);
}

/**
 * TS properties retain their quoted wire keys. Rust fields and client/server
 * methods replace non-ASCII identifier characters with underscores, prefix
 * leading digits, and escape keywords/reserved members. Collisions are errors,
 * never order-dependent numeric suffixes. Serde renames keep JSON unchanged.
 * Optional nullable fields remain unsupported (absence must differ from null).
 */
export function generate(contract: Contract, name: string): { typescript: string; rust: string } {
  validateContract(contract);
  if (!identifier.test(name)) throw new Error("invalid client name");
  const ts: string[] = [],
    rs: string[] = [],
    names = new Set([
      pascal(name) + "Client", pascal(name) + "Server",
      "RuntimeHost", "RuntimeError", "CallOptions", "Target", "Contract", "validateValue",
    ]);
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
        rf: string[] = [],
        fields = new Map<string, string>();
      for (const [key, value] of Object.entries(schema.properties!)) {
        const field = rustIdentifier(key);
        claimName(fields, field, key, `generated Rust field name collision in ${typeName}`);
        const optional = !schema.required!.includes(key);
        if (optional && (value.anyOf || value.type === "null"))
          throw new Error(
            `optional nullable field ${key} is unsupported: absence must differ from null`,
          );
        const [t, r] = render(value, typeName + pascal(key));
        tf.push(`  ${JSON.stringify(key)}${optional ? "?" : ""}: ${t};`);
        if (field !== key) rf.push(`    #[serde(rename = ${rustLiteral(key)})]`);
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
          `    pub ${field}: ${optional ? `Option<${r}>` : r},`,
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
    rustMethods: string[] = [],
    serverMethods: string[] = [],
    tsMethodNames = new Map<string, string>(),
    rustMethodNames = new Map<string, string>();
  for (const [method, definition] of Object.entries(contract.methods)) {
    const tsMethod = methodIdentifier(method),
      rustMethod = rustIdentifier(tsMethod),
      wireMethod = rustLiteral(method);
    claimName(tsMethodNames, tsMethod, method, "generated TypeScript method name collision");
    claimName(rustMethodNames, rustMethod, method, "generated Rust method name collision");
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
      `  ${tsMethod}(input: ${prefix}Input, options: CallOptions = {}): ${mode === "call" ? `Promise<${prefix}Output>` : `AsyncIterableIterator<${prefix}Output>`} {\n    return this.host.${mode}<${prefix}Output>(this.target, ${JSON.stringify(method)}, input, { ...options, contract: ${contractName}.methods[${JSON.stringify(method)}]! });\n  }`,
    );
    rustMethods.push(
      `    pub ${mode === "call" ? "async " : ""}fn ${rustMethod}(&self, input: ${prefix}Input, options: runtime_sdk::CallOptions) -> Result<${mode === "call" ? `${prefix}Output` : `runtime_sdk::ResponseStream<${prefix}Output>`}, runtime_sdk::RuntimeError> {\n        self.host.${mode}_contract(&self.target, ${wireMethod}, input, options, &contract()["methods"][${wireMethod}])${mode === "call" ? ".await" : ""}\n    }`,
    );
    const params = `${prefix}Input, runtime_sdk::HandlerContext${mode === "stream" ? `, runtime_sdk::StreamSender<${prefix}Output>` : ""}`;
    const result = mode === "call"
      ? `Result<${prefix}Output, runtime_sdk::BusinessError<${prefix}Error>>`
      : "Result<(), runtime_sdk::RuntimeError>";
    serverMethods.push(
      `    pub fn ${rustMethod}<F, Fut>(mut self, handler: F) -> Self\n    where F: Fn(${params}) -> Fut + Send + Sync + 'static,\n          Fut: std::future::Future<Output = ${result}> + Send + 'static {\n        self.handlers.${definition.mode}(${wireMethod}, handler);\n        self\n    }`,
    );
  }
  rs.push(`pub fn contract() -> &'static serde_json::Value {\n    static CONTRACT: std::sync::LazyLock<serde_json::Value> = std::sync::LazyLock::new(|| serde_json::from_str(${rustString(JSON.stringify(contract))}).expect("generated contract"));\n    &CONTRACT\n}\n\n#[derive(Default)]\npub struct ${pascal(name)}Server { handlers: runtime_sdk::Handlers }\nimpl ${pascal(name)}Server {\n    pub fn new() -> Self { <Self as Default>::default() }\n${serverMethods.join("\n")}\n    pub fn build(self) -> Result<runtime_sdk::Handlers, runtime_sdk::RuntimeError> {\n        self.handlers.validate(contract())?;\n        Ok(self.handlers)\n    }\n}`);
  // __proto__ changes object-literal prototypes, and inherited Object members
  // (e.g. constructor/toString) defeat TS contextual typing of schema records.
  // Parse these contracts as JSON to preserve own keys at every nesting level.
  const serialized = JSON.stringify(contract, null, 2);
  const contractSource = Object.getOwnPropertyNames(Object.prototype).some((key) =>
    serialized.includes(`${JSON.stringify(key)}:`))
    ? `JSON.parse(${JSON.stringify(serialized)})`
    : serialized;
  return {
    typescript: `// Generated by tools/generate.ts. Do not edit.\nimport { RuntimeHost, RuntimeError, type CallOptions, type Target, type Contract } from "@lenso/runtime-sdk";\nimport { validateValue } from "@lenso/runtime-sdk/schema";\nexport const ${contractName}: Contract = ${contractSource};\n\n${ts.join("\n\n")}\n\nexport class ${pascal(name)}Client {\n  private target: Target;\n  constructor(private host: RuntimeHost, target: Pick<Target, "namespace" | "service">) {\n    this.target = { ...target, contractVersion: ${contractName}.version };\n  }\n${methods.join("\n")}\n}\n`,
    rust: `// Generated by tools/generate.ts. Do not edit.\n${optionalHelper ? `fn present_non_null<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>\nwhere D: serde::Deserializer<'de>, T: serde::Deserialize<'de> {\n    T::deserialize(deserializer).map(Some)\n}\n` : ""}${requiredHelper ? `fn required_value<'de, D, T>(deserializer: D) -> Result<T, D::Error>\nwhere D: serde::Deserializer<'de>, T: serde::Deserialize<'de> {\n    T::deserialize(deserializer)\n}\n` : ""}\n${rs.join("\n\n")}\n\npub struct ${pascal(name)}Client {\n    host: runtime_sdk::RuntimeHost,\n    target: runtime_sdk::Target,\n}\nimpl ${pascal(name)}Client {\n    pub fn new(host: runtime_sdk::RuntimeHost, namespace: impl Into<String>, service: impl Into<String>) -> Self {\n        Self { host, target: runtime_sdk::Target { namespace: namespace.into(), service: service.into(), contract_version: ${rustLiteral(contract.version)}.into() } }\n    }\n${rustMethods.join("\n")}\n}\n`,
  };
}
