import importlib.util
import json
from pathlib import Path
import unittest

from lenso_runtime.generate import generate
from lenso_runtime.schema import validate_contract, validate_schema, validate_value, validate_wire

ROOT = Path(__file__).resolve().parents[3]


class SchemaTests(unittest.TestCase):
    def test_shared_rust_schema_corpus(self):
        cases = json.loads((ROOT / "crates/sdk/tests/schema-cases.json").read_text())
        for case in cases:
            with self.subTest(case=case["name"]):
                validate_schema(case["schema"])
                for value in case["accept"]:
                    validate_value(case["schema"], value)
                for value in case["reject"]:
                    with self.assertRaises(ValueError):
                        validate_value(case["schema"], value)

    def test_python_json_distinctions(self):
        for bad in (True, 1.0, 2**53):
            with self.assertRaises(ValueError):
                validate_value({"type": "integer"}, bad)
        for bad in (float("inf"), float("nan"), 10**1000, {1: "bad"}, object(), "\ud800"):
            with self.assertRaises((ValueError, UnicodeError)):
                validate_wire(bad)
        for bad in ({"type": "string", "format": "email"}, {"type": "string", "minimum": 1},
                    {"type": "array"}, {"type": "integer", "enum": [True]},
                    {"type": "boolean", "enum": [True, True]},
                    {"anyOf": [{"type": "null", "enum": [None]}, {"type": "string"}]},
                    {"type": "string", "maxLength": -1}, {"type": "number", "maximum": float("inf")},
                    {"type": "object", "properties": {"a": {"type": "null"}}, "required": [], "additionalProperties": False}):
            with self.subTest(schema=bad), self.assertRaises(ValueError):
                validate_schema(bad)

    def test_generated_file_is_current_and_importable(self):
        contract = json.loads((ROOT / "examples/contracts/logs.json").read_text())
        path = ROOT / "examples/generated/logs.py"
        self.assertEqual(generate(contract), path.read_text())
        spec = importlib.util.spec_from_file_location("generated_logs", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        self.assertEqual(module.CONTRACT, contract)
        self.assertEqual(module.ProgressInput.__required_keys__, frozenset({"logs"}))
        self.assertEqual(module.ProgressInput.__optional_keys__, frozenset({"delay_ms"}))

    def test_generation_rejects_name_collisions(self):
        schema = {"type": "null"}
        method = {"mode": "unary", "input": schema, "output": schema, "error": schema}
        for methods in ({"class": method}, {"a_b": method, "aB": method}, {"_hidden": method}):
            with self.assertRaises(ValueError):
                generate({"version": "1", "methods": methods})

    def test_mutation_is_observable(self):
        schema = {"type": "string", "maxLength": 3}
        validate_value(schema, "abc")
        schema["maxLength"] = 2
        with self.assertRaises(ValueError):
            validate_value(schema, "abc")


if __name__ == "__main__":
    unittest.main()
