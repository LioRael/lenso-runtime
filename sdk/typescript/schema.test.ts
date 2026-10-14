import { expect, test } from "bun:test";
import { validateValue, type Schema } from "./schema";
import schemaCases from "../../crates/sdk/tests/schema-cases.json";

test("shared restricted schema fixtures agree on string length constraints", () => {
  for (const fixture of schemaCases) {
    for (const value of fixture.accept)
      expect(() => validateValue(fixture.schema as Schema, value)).not.toThrow();
    for (const value of fixture.reject)
      expect(() => validateValue(fixture.schema as Schema, value)).toThrow();
  }
});

test("string bounds accept exact limits and reject overflow", () => {
  expect(() => validateValue({ type: "string", minLength: 1, maxLength: 1 }, "🙂")).not.toThrow();
  expect(() => validateValue({ type: "string", maxLength: 1 }, "🙂x")).toThrow();
  expect(() => validateValue({ type: "string", minLength: 2 }, "🙂")).toThrow();
});

test("raw schemas still enforce negative maximums and enum-only strings", () => {
  expect(() => validateValue({ type: "string", maxLength: -1 }, "")).toThrow();
  expect(() => validateValue({ type: "string", maxLength: -0.5 }, "")).toThrow();
  expect(() => validateValue({ type: "string", maxLength: -Infinity }, "")).toThrow();
  expect(() => validateValue({ type: "string", enum: ["allowed"] }, "allowed")).not.toThrow();
  expect(() => validateValue({ type: "string", enum: ["allowed"] }, "rejected")).toThrow();
  expect(() => validateValue({ type: "string", maxLength: 1 }, "\uD800")).not.toThrow();
  expect(() => validateValue({ type: "string", maxLength: 1 }, "\uDC00")).not.toThrow();
  expect(() => validateValue({ type: "string", maxLength: 1 }, "\uD800\uD800")).toThrow();
});

test("bounded string counting skips unbounded values and stops at max overflow", () => {
  const originalIterator = String.prototype[Symbol.iterator];
  let iterations = 0;
  String.prototype[Symbol.iterator] = function* (this: string) {
    for (const scalar of originalIterator.call(this)) {
      iterations++;
      yield scalar;
    }
    return undefined;
  };
  try {
    validateValue({ type: "string" }, "unbounded");
    expect(iterations).toBe(0);

    expect(() => validateValue({ type: "string", maxLength: 1 }, "🙂long")).toThrow();
    expect(iterations).toBe(2);
  } finally {
    String.prototype[Symbol.iterator] = originalIterator;
  }
});
