import { generate, pascal, rustIdentifier, methodIdentifier } from "../generate";
import { type Contract, type Schema, validateContract, validateSchema } from "../../sdk/typescript/schema";

type Path = (string | number)[];
type ObjectValue = Record<string, unknown>;
export type Source = "before" | "after" | "comparison" | "options";
export type Diagnostic = {
  source: Source;
  category: "validation" | "generator" | "change" | "analysis";
  code: string;
  path: string;
  pointer: string;
  message: string;
  before?: unknown;
  after?: unknown;
  relatedPath?: string;
};
export type DocumentCheck = {
  validation: "valid" | "invalid" | "unknown";
  generator: "accepted" | "rejected" | "unknown";
};
export type ContractCheck = {
  formatVersion: 1;
  clientName: string;
  strictMatch: "match" | "mismatch" | "unknown";
  migrationSafety: "unknown";
  before: DocumentCheck;
  after: DocumentCheck;
  diagnostics: Diagnostic[];
  note: string;
};

const slots = ["input", "output", "error"] as const;
const identifier = /^[A-Za-z][A-Za-z0-9_]*$/;
const note = "Offline v1 structural comparison only. Runtime strict matching is unchanged. Migration safety is unknown; generator acceptance is not a compile or deployment guarantee.";

function object(value: unknown): value is ObjectValue {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}
export function jsonPath(path: Path): string {
  return "$" + path.map((key) => `[${JSON.stringify(key)}]`).join("");
}
function diagnostic(source: Source, category: Diagnostic["category"], code: string, path: Path, message: string): Diagnostic {
  return {
    source, category, code, path: jsonPath(path),
    pointer: path.map((key) => "/" + String(key).replaceAll("~", "~0").replaceAll("/", "~1")).join(""),
    message,
  };
}
function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : "Unrecognized check failure";
}

// Bound the standalone analyzer and refuse values which cannot round-trip as
// ordinary cross-language JSON. These limits are not runtime protocol rules.
function inputBoundary(value: unknown): { path: Path; message: string } | undefined {
  const ancestors = new Set<object>();
  let nodes = 0;
  function unicode(value: string): boolean {
    return [...value].some((scalar) => {
      const point = scalar.codePointAt(0)!;
      return point >= 0xd800 && point <= 0xdfff;
    });
  }
  function visit(value: unknown, path: Path): ReturnType<typeof inputBoundary> {
    if (++nodes > 100_000 || path.length > 256)
      return { path, message: "Analyzer JSON depth or node limit exceeded; result is unknown." };
    if (typeof value === "string")
      return unicode(value) ? { path, message: "Unpaired Unicode surrogate has an unknown cross-language JSON boundary." } : undefined;
    if (value === null || typeof value === "boolean") return;
    if (typeof value === "number")
      return Number.isFinite(value) ? undefined : { path, message: "Non-finite number is outside ordinary JSON." };
    if (typeof value !== "object") return { path, message: "Expected a JSON value; analysis is unknown." };
    if (ancestors.has(value)) return { path, message: "Cyclic data is outside ordinary JSON." };
    if (Array.isArray(value) && (Object.keys(value).length !== value.length || Object.keys(value).some((key, index) => key !== String(index))))
      return { path, message: "Sparse or augmented arrays are outside ordinary JSON." };
    if (!Array.isArray(value) && ![Object.prototype, null].includes(Object.getPrototypeOf(value)))
      return { path, message: "Non-plain object is outside ordinary JSON." };
    ancestors.add(value);
    for (const [key, child] of Object.entries(value)) {
      const childPath = [...path, Array.isArray(value) ? Number(key) : key];
      if (unicode(key)) return { path: childPath, message: "Unpaired Unicode surrogate in a key has an unknown cross-language JSON boundary." };
      const failure = visit(child, childPath);
      if (failure) return failure;
    }
    ancestors.delete(value);
  }
  return visit(value, []);
}

// The existing SDK validator decides validity. This walk only locates its first
// failure; it neither changes validation nor invents a broader schema subset.
function schemaFailure(value: unknown, path: Path, depth = 0): { path: Path; message: string } | undefined {
  try { validateSchema(value as Schema, depth); return; } catch (error) {
    const message = errorMessage(error);
    if (!object(value) || depth > 64) return { path, message };
    if (Array.isArray(value.anyOf)) {
      for (let index = 0; index < value.anyOf.length; index++) {
        const failure = schemaFailure(value.anyOf[index], [...path, "anyOf", index], depth + 1);
        if (failure) return failure;
      }
    }
    if (object(value.properties)) {
      const fields = new Map<string, Path>();
      for (const [key, child] of Object.entries(value.properties)) {
        const childPath = [...path, "properties", key];
        const failure = schemaFailure(child, childPath, depth + 1);
        if (failure) return failure;
        if (Array.isArray(value.required) && !value.required.includes(key) && object(child) && (child.anyOf || child.type === "null"))
          return { path: childPath, message: "Optional nullable fields are unsupported in v1." };
      }
    }
    if (value.type === "array") {
      const failure = schemaFailure(value.items, [...path, "items"], depth + 1);
      if (failure) return failure;
    }
    if (Object.hasOwn(value, "anyOf")) return { path: [...path, "anyOf"], message };
    const unsupported = /^unsupported schema keyword: (.*)$/.exec(message)?.[1];
    if (unsupported !== undefined && Object.hasOwn(value, unsupported)) return { path: [...path, unsupported], message };
    const keyword = /^keyword (.*) incompatible/.exec(message)?.[1];
    if (keyword !== undefined) return { path: [...path, keyword], message };
    if (message === "invalid schema type") return { path: [...path, "type"], message };
    if (value.type === "object") {
      if (value.additionalProperties !== false) return { path: [...path, "additionalProperties"], message };
      if (!object(value.properties)) return { path: [...path, "properties"], message };
      if (message === "invalid required" || !Array.isArray(value.required)) return { path: [...path, "required"], message };
    }
    for (const key of ["minimum", "maximum", "minLength", "maxLength", "minItems", "maxItems"])
      if (message === `invalid ${key}`) return { path: [...path, key], message };
    if (message.includes("enum") || (value.enum !== undefined && message === "value does not satisfy schema"))
      return { path: [...path, "enum"], message };
    return { path, message };
  }
}
function validationFailure(value: unknown, message: string): { path: Path; message: string } {
  if (!object(value)) return { path: [], message };
  const extra = Object.keys(value).find((key) => !["version", "methods"].includes(key));
  if (extra !== undefined) return { path: [extra], message: "Unsupported contract field." };
  if (typeof value.version !== "string" || !value.version || Buffer.byteLength(value.version) > 256)
    return { path: ["version"], message: "Version must be a nonempty string of at most 256 UTF-8 bytes." };
  if (!object(value.methods) || Object.keys(value.methods).length > 256) return { path: ["methods"], message };
  for (const [name, method] of Object.entries(value.methods)) {
    const path = ["methods", name];
    if (!name || Buffer.byteLength(name) > 256 || !object(method)) return { path, message };
    const extra = Object.keys(method).find((key) => !["mode", ...slots].includes(key));
    if (extra !== undefined) return { path: [...path, extra], message: "Unsupported method field." };
    if (method.mode !== "unary" && method.mode !== "stream") return { path: [...path, "mode"], message: "Mode must be unary or stream." };
    for (const slot of slots) {
      const failure = schemaFailure(method[slot], [...path, slot]);
      if (failure) return failure;
    }
  }
  return { path: [], message };
}

// A locator for the generator's existing identifier/name failures. generate()
// itself remains authoritative and runs in memory, without writing artifacts.
function generatorFailure(contract: Contract, name: string): { path: Path; code: string; relatedPath?: string } | undefined {
  if (!identifier.test(name)) return { path: [], code: "generator.client_name" };
  const names = new Map<string, Path>([[pascal(name) + "Client", []], [pascal(name) + "Server", []]]);
  type Failure = NonNullable<ReturnType<typeof generatorFailure>>;
  function reserve(name: string, path: Path): void {
    const previous = names.get(name);
    if (previous) throw { path, code: "generator.type_name_collision", relatedPath: jsonPath(previous) } satisfies Failure;
    names.set(name, path);
  }
  function render(schema: Schema, typeName: string, path: Path): boolean {
    if (schema.anyOf) {
      const index = schema.anyOf.findIndex((branch) => branch.type !== "null");
      render(schema.anyOf[index]!, typeName + "Value", [...path, "anyOf", index]);
      return false;
    }
    if (schema.type === "object") {
      reserve(typeName, path);
      const fields = new Map<string, Path>();
      for (const [key, child] of Object.entries(schema.properties!)) {
        const childPath = [...path, "properties", key];
        const field = rustIdentifier(key), previous = fields.get(field);
        if (previous) throw { path: childPath, code: "generator.property_identifier", relatedPath: jsonPath(previous) } satisfies Failure;
        fields.set(field, childPath);
        render(child, typeName + pascal(key), childPath);
      }
      return true;
    }
    if (schema.type === "array") {
      render(schema.items!, typeName + "Item", [...path, "items"]);
      return false;
    }
    if (schema.enum) { reserve(typeName, path); return true; }
    return false;
  }
  try {
    const tsMethods = new Map<string, Path>(), rustMethods = new Map<string, Path>();
    for (const [method, definition] of Object.entries(contract.methods)) {
      const path = ["methods", method];
      for (const [seen, spelling] of [[tsMethods, methodIdentifier(method)], [rustMethods, rustIdentifier(methodIdentifier(method))]] as const) {
        const previous = seen.get(spelling);
        if (previous) return { path, code: "generator.method_identifier", relatedPath: jsonPath(previous) };
        seen.set(spelling, path);
      }
      for (const slot of slots) {
        const typeName = pascal(method) + pascal(slot), schemaPath = [...path, slot];
        if (!render(definition[slot], typeName, schemaPath)) reserve(typeName, schemaPath);
      }
    }
  } catch (failure) { return failure as Failure; }
}

function inspect(value: unknown, source: "before" | "after", clientName: string, diagnostics: Diagnostic[]): DocumentCheck {
  const boundary = inputBoundary(value);
  if (boundary) {
    diagnostics.push(diagnostic(source, "analysis", "analysis.unknown", boundary.path, boundary.message));
    return { validation: "unknown", generator: "unknown" };
  }
  try { validateContract(value as Contract); } catch (error) {
    const failure = validationFailure(value, errorMessage(error));
    diagnostics.push(diagnostic(source, "validation", "contract.invalid", failure.path, failure.message));
    return { validation: "invalid", generator: "unknown" };
  }
  try {
    generate(value as Contract, clientName);
    return { validation: "valid", generator: "accepted" };
  } catch (error) {
    const failure = generatorFailure(value as Contract, clientName);
    diagnostics.push({
      ...diagnostic(source, "generator", failure?.code ?? "generator.unknown", failure?.path ?? [], errorMessage(error)),
      ...(failure?.relatedPath !== undefined ? { relatedPath: failure.relatedPath } : {}),
    });
    return { validation: "valid", generator: failure ? "rejected" : "unknown" };
  }
}

// Mirrors protocol::json_equivalent for validated ordinary JSON: object order
// does not matter; arrays keep their order; all numbers use binary64 semantics.
function equivalent(left: unknown, right: unknown): boolean {
  if (left === right) return true;
  if (Array.isArray(left) && Array.isArray(right))
    return left.length === right.length && left.every((value, index) => equivalent(value, right[index]));
  if (object(left) && object(right))
    return Object.keys(left).length === Object.keys(right).length && Object.keys(left).every((key) => Object.hasOwn(right, key) && equivalent(left[key], right[key]));
  return false;
}

function changeCode(path: Path, before: unknown, after: unknown): string {
  const key = path.at(-1);
  if (path.length === 1 && key === "version") return "version.changed";
  if (path.length === 2 && path[0] === "methods") return before === undefined ? "method.added" : "method.removed";
  if (path.length === 3 && key === "mode") return "mode.changed";
  if (path.at(-2) === "properties") return before === undefined ? "field.added" : "field.removed";
  if (key === "required") return "fields.required_changed";
  if (key === "enum") return "constraint.enum_changed";
  if (["minimum", "maximum", "minLength", "maxLength", "minItems", "maxItems"].includes(String(key))) return "constraint.changed";
  if (key === "type") return "type.changed";
  if (key === "anyOf") return "nullability.changed";
  return "schema.changed";
}
function changes(before: unknown, after: unknown, path: Path, diagnostics: Diagnostic[]): void {
  if (equivalent(before, after)) return;
  if (object(before) && object(after)) {
    for (const key of [...new Set([...Object.keys(before), ...Object.keys(after)])].sort())
      changes(Object.hasOwn(before, key) ? before[key] : undefined, Object.hasOwn(after, key) ? after[key] : undefined, [...path, key], diagnostics);
    return;
  }
  // Keep nullable-branch positions and nested field paths; all other arrays are
  // reported as a whole so membership/presence changes stay easy to review.
  if (path.at(-1) === "anyOf" && Array.isArray(before) && Array.isArray(after) && before.every((branch, i) => object(branch) && object(after[i]) && branch.type === after[i].type)) {
    before.forEach((branch, index) => changes(branch, after[index], [...path, index], diagnostics));
    return;
  }
  const code = changeCode(path, before, after);
  const message = code === "version.changed"
    ? "Contract version changed; v1 selects versions exactly and performs no version negotiation."
    : code === "mode.changed"
      ? "Method mode changed between unary and stream."
      : "Contract structure changed; strict matching differs. Migration impact is unknown.";
  diagnostics.push({
    ...diagnostic("comparison", "change", code, path, message),
    ...(before !== undefined ? { before } : {}),
    ...(after !== undefined ? { after } : {}),
  });
}

/** Compare old and proposed contracts without changing either definition. */
export function checkContracts(before: unknown, after: unknown, options: { clientName?: string } = {}): ContractCheck {
  const clientName = options.clientName ?? "logs", diagnostics: Diagnostic[] = [];
  const previous = inspect(before, "before", clientName, diagnostics);
  const proposed = inspect(after, "after", clientName, diagnostics);
  let strictMatch: ContractCheck["strictMatch"] = "unknown";
  if (previous.validation === "valid" && proposed.validation === "valid") {
    strictMatch = equivalent(before, after) ? "match" : "mismatch";
    if (strictMatch === "mismatch") changes(before, after, [], diagnostics);
  }
  return { formatVersion: 1, clientName, strictMatch, migrationSafety: "unknown", before: previous, after: proposed, diagnostics, note };
}

/** 0 = strict match + generator accepted; 1 = differences/rejection; 2 = invalid/unknown. */
export function exitCode(report: ContractCheck): 0 | 1 | 2 {
  if (report.strictMatch === "unknown" || [report.before, report.after].some((doc) => doc.validation !== "valid" || doc.generator === "unknown")) return 2;
  return report.strictMatch === "match" && report.before.generator === "accepted" && report.after.generator === "accepted" ? 0 : 1;
}

export function formatHuman(report: ContractCheck): string {
  const escape = (value: string) => value.replace(/[\u0000-\u001f\u007f-\u009f]/g, (char) => "\\u" + char.charCodeAt(0).toString(16).padStart(4, "0"));
  const summarize = (value: unknown) => {
    const text = escape(JSON.stringify(value));
    return text.length > 180 ? text.slice(0, 177) + "..." : text;
  };
  const lines = [
    `Strict match: ${report.strictMatch}`,
    `Migration safety: ${report.migrationSafety}`,
    `Before: ${report.before.validation}; generator: ${report.before.generator}`,
    `After: ${report.after.validation}; generator: ${report.after.generator}`,
  ];
  for (const item of report.diagnostics) {
    lines.push(`[${item.source}] ${item.code} ${escape(item.path)}: ${escape(item.message)}`);
    if (Object.hasOwn(item, "before")) lines.push(`  before: ${summarize(item.before)}`);
    if (Object.hasOwn(item, "after")) lines.push(`  after: ${summarize(item.after)}`);
    if (item.relatedPath !== undefined) lines.push(`  related: ${escape(item.relatedPath)}`);
  }
  lines.push(report.note);
  return lines.join("\n") + "\n";
}
