export type Schema = {
  type?: "string" | "boolean" | "number" | "integer" | "null" | "object" | "array";
  properties?: Record<string, Schema>;
  required?: string[];
  additionalProperties?: false;
  items?: Schema;
  enum?: unknown[];
  minimum?: number;
  maximum?: number;
  minLength?: number;
  maxLength?: number;
  minItems?: number;
  maxItems?: number;
  anyOf?: Schema[];
};
// Contracts are live mutable definitions. Validation must not identity-cache,
// freeze, or snapshot raw schemas, including generated exported contracts.
export type Contract = {
  version: string;
  methods: Record<
    string,
    {
      mode: "unary" | "stream";
      input: Schema;
      output: Schema;
      error: Schema;
    }
  >;
};
const keys = new Set([
  "type",
  "properties",
  "required",
  "additionalProperties",
  "items",
  "enum",
  "minimum",
  "maximum",
  "minLength",
  "maxLength",
  "minItems",
  "maxItems",
  "anyOf",
]);
export function validateSchema(schema: Schema, depth = 0): void {
  if (depth > 64) throw new Error("schema too deep");
  if (!schema || typeof schema !== "object" || Array.isArray(schema))
    throw new Error("schema must be an object");
  for (const key of Object.keys(schema))
    if (!keys.has(key)) throw new Error(`unsupported schema keyword: ${key}`);
  if (schema.anyOf) {
    if (
      Object.keys(schema).length !== 1 ||
      schema.anyOf.length !== 2 ||
      schema.anyOf.filter((s) => s.type === "null").length !== 1 ||
      schema.anyOf.some((s) => s.anyOf || (s.type === "null" && Object.keys(s).length !== 1))
    )
      throw new Error("anyOf is only supported for nullable values");
    schema.anyOf.forEach((s) => validateSchema(s, depth + 1));
    return;
  }
  if (!["string", "boolean", "number", "integer", "null", "object", "array"].includes(schema.type!))
    throw new Error("invalid schema type");
  const allowed: Record<string, string[]> = {
    object: ["properties", "required", "additionalProperties"],
    array: ["items", "minItems", "maxItems"],
    string: ["minLength", "maxLength"],
    integer: ["minimum", "maximum"],
    number: ["minimum", "maximum"],
    boolean: [],
    null: [],
  };
  for (const key of Object.keys(schema))
    if (!["type", "enum", ...allowed[schema.type!]].includes(key))
      throw new Error(`keyword ${key} incompatible with ${schema.type}`);
  for (const key of [
    "minimum",
    "maximum",
    "minLength",
    "maxLength",
    "minItems",
    "maxItems",
  ] as const) {
    const n = schema[key];
    if (
      n !== undefined &&
      (!Number.isFinite(n) ||
        (!["minimum", "maximum"].includes(key) && (!Number.isSafeInteger(n) || n < 0)))
    )
      throw new Error(`invalid ${key}`);
  }
  for (const [min, max] of [
    ["minimum", "maximum"],
    ["minLength", "maxLength"],
    ["minItems", "maxItems"],
  ] as const)
    if (schema[min] !== undefined && schema[max] !== undefined && schema[min]! > schema[max]!)
      throw new Error("inverted bounds");
  if (schema.type === "object") {
    if (
      schema.additionalProperties !== false ||
      !schema.properties ||
      typeof schema.properties !== "object" ||
      Array.isArray(schema.properties) ||
      !Array.isArray(schema.required)
    )
      throw new Error("objects require properties, required and additionalProperties:false");
    if (
      new Set(schema.required).size !== schema.required.length ||
      schema.required.some((k) => typeof k !== "string" || !Object.hasOwn(schema.properties!, k))
    )
      throw new Error("invalid required");
    for (const [key, child] of Object.entries(schema.properties)) {
      if (!schema.required.includes(key) && (child.anyOf || child.type === "null"))
        throw new Error("optional nullable fields are unsupported");
      validateSchema(child, depth + 1);
    }
  }
  if (schema.type === "array") {
    if (!schema.items) throw new Error("array requires items");
    validateSchema(schema.items, depth + 1);
  }
  if (schema.enum !== undefined) {
    if (
      !Array.isArray(schema.enum) ||
      schema.enum.length === 0 ||
      schema.enum.length > 64 ||
      schema.type === "object" ||
      schema.type === "array"
    )
      throw new Error("enum requires at most 64 scalar members");
    if (new Set(schema.enum.map((v) => JSON.stringify(v))).size !== schema.enum.length)
      throw new Error("duplicate enum member");
    for (const value of schema.enum) validateValue({ ...schema, enum: undefined }, value);
  }
}
export function validateContract(contract: Contract): void {
  if (
    !contract ||
    Array.isArray(contract) ||
    typeof contract.version !== "string" ||
    !contract.version ||
    !contract.methods ||
    typeof contract.methods !== "object" ||
    Array.isArray(contract.methods) ||
    Object.keys(contract).some((k) => !["version", "methods"].includes(k))
  )
    throw new Error("invalid contract");
  if (
    Buffer.byteLength(contract.version) > 256 ||
    Object.keys(contract.methods).length > 256 ||
    Object.keys(contract.methods).some((name) => !name || Buffer.byteLength(name) > 256)
  )
    throw new Error("invalid contract identifiers or method count");
  for (const method of Object.values(contract.methods)) {
    if (
      !method ||
      typeof method !== "object" ||
      Array.isArray(method) ||
      !["unary", "stream"].includes(method.mode) ||
      Object.keys(method).some((k) => !["mode", "input", "output", "error"].includes(k))
    )
      throw new Error("invalid method");
    validateSchema(method.input);
    validateSchema(method.output);
    validateSchema(method.error);
  }
}
export function validateValue(schema: Schema, value: unknown, depth = 0): void {
  const fail = () => {
    throw new Error("value does not satisfy schema");
  };
  if (depth > 64) return fail();
  if (schema.anyOf) {
    for (const branch of schema.anyOf) {
      try {
        validateValue(branch, value, depth + 1);
        return;
      } catch {}
    }
    return fail();
  }
  if (schema.enum && !schema.enum.some((v) => JSON.stringify(v) === JSON.stringify(value))) fail();
  switch (schema.type) {
    case "null":
      if (value !== null) fail();
      break;
    case "boolean":
      if (typeof value !== "boolean") fail();
      break;
    case "number":
    case "integer":
      if (
        typeof value !== "number" ||
        !Number.isFinite(value) ||
        (schema.type === "integer" && !Number.isSafeInteger(value)) ||
        (schema.minimum !== undefined && value < schema.minimum) ||
        (schema.maximum !== undefined && value > schema.maximum)
      )
        fail();
      break;
    case "string":
      if (typeof value !== "string") return fail();
      // Schema length counts Unicode scalar values, not UTF-16 code units.
      if (schema.minLength !== undefined || schema.maxLength !== undefined) {
        let length = 0;
        for (const _scalar of value) {
          length++;
          if (schema.maxLength !== undefined && length > schema.maxLength) fail();
        }
        if (
          (schema.minLength !== undefined && length < schema.minLength) ||
          (schema.maxLength !== undefined && length > schema.maxLength)
        )
          fail();
      }
      break;
    case "array":
      if (!Array.isArray(value)) return fail();
      if (
        (schema.minItems !== undefined && value.length < schema.minItems) ||
        (schema.maxItems !== undefined && value.length > schema.maxItems)
      )
        fail();
      for (const item of value) validateValue(schema.items!, item, depth + 1);
      break;
    case "object":
      if (!value || typeof value !== "object" || Array.isArray(value)) return fail();
      if (
        Object.getPrototypeOf(value) !== Object.prototype &&
        Object.getPrototypeOf(value) !== null
      )
        return fail();
      for (const key of schema.required!) if (!Object.hasOwn(value, key)) fail();
      for (const [key, item] of Object.entries(value)) {
        if (!Object.hasOwn(schema.properties!, key)) fail();
        validateValue(schema.properties![key], item, depth + 1);
      }
      break;
    default:
      fail();
  }
}
