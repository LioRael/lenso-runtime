"""The restricted v1 contract schema; no coercion or third-party validator."""
from __future__ import annotations

import json
import math
from typing import Any

Schema = dict[str, Any]
Contract = dict[str, Any]
SAFE_INTEGER = 9007199254740991
_ALLOWED = {
    "object": {"properties", "required", "additionalProperties"},
    "array": {"items", "minItems", "maxItems"},
    "string": {"minLength", "maxLength"},
    "integer": {"minimum", "maximum"},
    "number": {"minimum", "maximum"},
    "boolean": set(), "null": set(),
}


def _finite(value: Any) -> bool:
    try:
        return type(value) in (int, float) and math.isfinite(value)
    except OverflowError:
        return False


def _spend(budget: list[int] | None, count: int) -> None:
    if budget is not None:
        budget[0] -= count
        if budget[0] < 0:
            raise ValueError("JSON exceeds frame budget")


def validate_wire(value: Any, depth: int = 0, *, budget: list[int] | None = None) -> None:
    if depth > 64:
        raise ValueError("value too deep")
    if value is None or type(value) is bool:
        _spend(budget, 4 if value is None or value is True else 5)
        return
    if type(value) is str:
        # Reject huge strings before allocating an escaped UTF-8 copy.
        if budget is not None and len(value) + 2 > budget[0]:
            raise ValueError("string exceeds frame budget")
        size = len(value.encode("utf-8", errors="strict")) + 2
        size += sum(1 if char in '\"\\\b\f\n\r\t' else 5 if ord(char) < 32 else 0 for char in value)
        _spend(budget, size)
        return
    if type(value) in (int, float):
        if not _finite(value) or (type(value) is int and abs(value) > SAFE_INTEGER):
            raise ValueError("nonfinite number or unsafe integer")
        _spend(budget, len(json.dumps(value, allow_nan=False)))
        return
    if type(value) is list:
        _spend(budget, 2 + max(0, len(value) - 1))
        for item in value:
            validate_wire(item, depth + 1, budget=budget)
        return
    if type(value) is dict:
        _spend(budget, 2 + max(0, len(value) - 1))
        for key, item in value.items():
            if type(key) is not str:
                raise ValueError("object keys must be strings")
            validate_wire(key, depth + 1, budget=budget)
            _spend(budget, 1)
            validate_wire(item, depth + 1, budget=budget)
        return
    raise ValueError("not a JSON value")


def validate_schema(schema: Any, depth: int = 0) -> None:
    if type(schema) is not dict or depth > 64:
        raise ValueError("invalid schema or depth")
    if "anyOf" in schema:
        branches = schema["anyOf"]
        if (set(schema) != {"anyOf"} or type(branches) is not list or len(branches) != 2
                or any(type(b) is not dict or "anyOf" in b for b in branches)
                or sum(b.get("type") == "null" for b in branches) != 1
                or any(b.get("type") == "null" and b != {"type": "null"} for b in branches)):
            raise ValueError("anyOf only supports nullable values")
        for branch in branches:
            validate_schema(branch, depth + 1)
        return
    kind = schema.get("type")
    if type(kind) is not str or kind not in _ALLOWED:
        raise ValueError("invalid schema type")
    if set(schema) - ({"type", "enum"} | _ALLOWED[kind]):
        raise ValueError("unsupported or incompatible schema keyword")
    for lower, upper in (("minimum", "maximum"), ("minLength", "maxLength"), ("minItems", "maxItems")):
        for key in (lower, upper):
            if key in schema:
                n = schema[key]
                if not _finite(n):
                    raise ValueError("invalid bound")
                if key not in ("minimum", "maximum") and (type(n) is not int or not 0 <= n <= SAFE_INTEGER):
                    raise ValueError("invalid length bound")
        if lower in schema and upper in schema and schema[lower] > schema[upper]:
            raise ValueError("inverted bounds")
    if kind == "object":
        props, required = schema.get("properties"), schema.get("required")
        if (schema.get("additionalProperties") is not False or type(props) is not dict
                or type(required) is not list or any(type(k) is not str for k in required)
                or len(set(required)) != len(required) or any(k not in props for k in required)):
            raise ValueError("objects require properties, required, additionalProperties:false")
        for key, child in props.items():
            if type(key) is not str or type(child) is not dict:
                raise ValueError("invalid property")
            if key not in required and ("anyOf" in child or child.get("type") == "null"):
                raise ValueError("optional nullable fields unsupported")
            validate_schema(child, depth + 1)
    if kind == "array":
        validate_schema(schema.get("items"), depth + 1)
    if "enum" in schema:
        values = schema["enum"]
        if type(values) is not list or not 1 <= len(values) <= 64 or kind in ("object", "array"):
            raise ValueError("enum requires 1-64 scalar members")
        base = {k: v for k, v in schema.items() if k != "enum"}
        for i, value in enumerate(values):
            validate_value(base, value)
            if any(_equal(value, prior) for prior in values[:i]):
                raise ValueError("duplicate enum member")


def _equal(a: Any, b: Any) -> bool:
    # bool is an int subclass in Python, but not in JSON.
    return (type(a) is type(b) or type(a) in (int, float) and type(b) in (int, float)) and a == b


def validate_value(schema: Schema, value: Any, depth: int = 0) -> None:
    if depth > 64:
        raise ValueError("value too deep")
    if "anyOf" in schema:
        for branch in schema["anyOf"]:
            try:
                validate_value(branch, value, depth + 1)
                return
            except ValueError:
                pass
        raise ValueError("value outside nullable schema")
    kind = schema["type"]
    valid = {
        "null": value is None, "boolean": type(value) is bool,
        "integer": type(value) is int, "number": type(value) in (int, float),
        "string": type(value) is str, "object": type(value) is dict, "array": type(value) is list,
    }[kind]
    if not valid:
        raise ValueError("value has wrong type")
    if "enum" in schema and not any(_equal(value, v) for v in schema["enum"]):
        raise ValueError("value outside enum")
    if kind in ("integer", "number"):
        if not _finite(value) or (kind == "integer" and abs(value) > SAFE_INTEGER):
            raise ValueError("invalid number")
        if value < schema.get("minimum", -math.inf) or value > schema.get("maximum", math.inf):
            raise ValueError("number outside bounds")
    if kind in ("string", "array"):
        lower, upper = ("minLength", "maxLength") if kind == "string" else ("minItems", "maxItems")
        if not schema.get(lower, 0) <= len(value) <= schema.get(upper, math.inf):
            raise ValueError("length outside bounds")
        if kind == "string":
            value.encode("utf-8", errors="strict")
        else:
            for item in value:
                validate_value(schema["items"], item, depth + 1)
    if kind == "object":
        if any(k not in value for k in schema["required"]) or set(value) - set(schema["properties"]):
            raise ValueError("missing or extra property")
        for key, item in value.items():
            validate_value(schema["properties"][key], item, depth + 1)


def validate_contract(contract: Any) -> None:
    if (type(contract) is not dict or set(contract) != {"version", "methods"}
            or type(contract["version"]) is not str or not 1 <= len(contract["version"].encode()) <= 256
            or type(contract["methods"]) is not dict or len(contract["methods"]) > 256):
        raise ValueError("invalid contract")
    for name, method in contract["methods"].items():
        if (type(name) is not str or not 1 <= len(name.encode()) <= 256
                or type(method) is not dict or set(method) != {"mode", "input", "output", "error"}
                or method["mode"] not in ("unary", "stream")):
            raise ValueError("invalid method")
        for slot in ("input", "output", "error"):
            validate_schema(method[slot])
