use super::*;
use serde_json::json;

fn assert_parity(schema: &Value, values: &[Value]) {
    let prepared = PreparedSchema::new(schema).unwrap();
    for value in values {
        assert_eq!(
            prepared.validate_value(value),
            validate_value(schema, value),
            "prepared/raw mismatch for schema {schema}"
        );
    }
}

#[test]
fn prepared_matches_raw_scalar_acceptance_and_safe_diagnostics() {
    let values = [
        Value::Null,
        json!(true),
        json!(false),
        json!(""),
        json!("a"),
        json!("🌱"),
        json!("e\u{301}"),
        json!("🌱🌱"),
        json!(-1),
        json!(0),
        json!(-0.0),
        json!(1),
        json!(1.0),
        json!(1.5),
        json!(-9_007_199_254_740_992_i64),
        json!(-9_007_199_254_740_991_i64),
        json!(9_007_199_254_740_991_u64),
        json!(9_007_199_254_740_992_u64),
        json!(u64::MAX),
        json!(f64::MAX),
        json!([]),
        json!({}),
    ];
    for schema in [
        json!({"type":"null"}),
        json!({"type":"boolean"}),
        json!({"type":"boolean","enum":[true]}),
        json!({"type":"string"}),
        json!({"type":"string","minLength":1,"maxLength":1}),
        json!({"type":"string","minLength":1.0,"maxLength":1.0}),
        json!({"type":"string","maxLength":9007199254740991.0}),
        json!({"type":"string","enum":["🌱","a"]}),
        json!({"type":"number"}),
        json!({"type":"number","minimum":-1.5,"maximum":1.5}),
        json!({"type":"number","enum":[0,1.0]}),
        json!({"type":"number","enum":[-0.0]}),
        json!({"type":"integer"}),
        json!({"type":"integer","minimum":-1.0,"maximum":1.0}),
        json!({"type":"integer","enum":[1.0]}),
        json!({"type":"integer","enum":[-0.0]}),
        json!({"type":"integer","enum":[-9007199254740991.0,9007199254740991.0]}),
        json!({"anyOf":[{"type":"null"},{"type":"string","minLength":1.0,"maxLength":1.0}]}),
        json!({"anyOf":[{"type":"integer","enum":[1.0]},{"type":"null"}]}),
    ] {
        assert_parity(&schema, &values);
    }

    let integer = PreparedSchema::new(&json!({"type":"integer","enum":[1.0]})).unwrap();
    assert_eq!(integer.validate_value(&json!(1)), Ok(()));
    assert_eq!(
        integer.validate_value(&json!(1.0)),
        fail("value has wrong type")
    );
    let zero = PreparedSchema::new(&json!({"type":"number","enum":[0]})).unwrap();
    assert_eq!(zero.validate_value(&json!(-0.0)), Ok(()));
}

#[test]
fn prepared_matches_raw_arrays_closed_objects_and_nullable_presence() {
    let schemas = [
        json!({"type":"array","items":{"type":"integer"},"minItems":1.0,"maxItems":2.0}),
        json!({"type":"array","items":{"anyOf":[{"type":"string"},{"type":"null"}]}}),
        json!({"type":"object","properties":{"x":{"type":"string"}},"required":[],"additionalProperties":false}),
        json!({"type":"object","properties":{"x":{"anyOf":[{"type":"null"},{"type":"string"}]},"y":{"type":"integer"}},"required":["x"],"additionalProperties":false}),
        json!({"type":"object","properties":{"x":{"type":"array","items":{"type":"number","enum":[0,1.0]}}},"required":["x"],"additionalProperties":false}),
    ];
    let values = [
        Value::Null,
        json!([]),
        json!([1]),
        json!([1, 2]),
        json!([1, 2, 3]),
        json!([1.0]),
        json!([null, "a"]),
        json!({}),
        json!({"x":null}),
        json!({"x":"a"}),
        json!({"x":"a","y":1}),
        json!({"x":"a","y":null}),
        json!({"x":"a","unknown":"private-business-value"}),
        json!({"x":[-0.0,1]}),
        json!({"x":[2]}),
    ];
    for schema in schemas {
        assert_parity(&schema, &values);
    }
}

fn nested_arrays(depth: usize) -> (Value, Value) {
    let mut schema = json!({"type":"integer"});
    let mut value = json!(1);
    for _ in 0..depth {
        schema = json!({"type":"array","items":schema});
        value = json!([value]);
    }
    (schema, value)
}

#[test]
fn prepared_preserves_depth_boundary() {
    for depth in [0, 63, 64] {
        let (schema, value) = nested_arrays(depth);
        let prepared = PreparedSchema::new(&schema).unwrap();
        assert_eq!(prepared.validate_value(&value), Ok(()));
        assert_parity(&schema, &[value, nested_arrays(depth + 1).1]);
    }
    let (schema, _) = nested_arrays(65);
    assert_eq!(
        PreparedSchema::new(&schema).err(),
        Some(ValidationError("schema too deep".into()))
    );
    let (schema, _) = nested_arrays(64);
    let prepared = PreparedSchema::new(&schema).unwrap();
    assert_eq!(
        prepared.validate_value(&nested_arrays(65).1),
        fail("value has wrong type")
    );
}

#[test]
fn preparation_rejects_invalid_schemas_with_raw_diagnostics() {
    for schema in [
        Value::Null,
        json!({}),
        json!({"type":"string","format":"date"}),
        json!({"type":"object","properties":{}}),
        json!({"type":"object","properties":{},"required":[],"additionalProperties":true}),
        json!({"type":"object","properties":{},"required":["ghost"],"additionalProperties":false}),
        json!({"type":"object","properties":{"x":{"type":"integer"}},"required":["x","x"],"additionalProperties":false}),
        json!({"type":"object","properties":{"x":{"type":"null"}},"required":[],"additionalProperties":false}),
        json!({"type":"object","properties":{"x":{"anyOf":[{"type":"null"},{"type":"string"}]}},"required":[],"additionalProperties":false}),
        json!({"type":"array"}),
        json!({"type":"array","items":{"type":"unknown"}}),
        json!({"type":"array","items":{"type":"integer"},"enum":[[1]]}),
        json!({"anyOf":[{"type":"string"},{"type":"number"}]}),
        json!({"anyOf":[{"type":"null"},{"type":"null"}]}),
        json!({"anyOf":[{"type":"null"},{"anyOf":[{"type":"null"},{"type":"string"}]}]}),
        json!({"type":"string","minLength":3,"maxLength":2}),
        json!({"type":"string","maxLength":-1}),
        json!({"type":"string","maxLength":1.5}),
        json!({"type":"string","maxLength":9007199254740992.0}),
        json!({"type":"number","minimum":2.0,"maximum":1.0}),
        json!({"type":"number","minimum":"1"}),
        json!({"type":"number","enum":[0,-0.0]}),
        json!({"type":"number","enum":[1,1.0]}),
        json!({"type":"integer","enum":[1.5]}),
        json!({"type":"integer","enum":[9007199254740992.0]}),
        json!({"type":"integer","enum":[]}),
        json!({"type":"integer","enum":(0..65).collect::<Vec<_>>()}),
        json!({"type":"string","maxLength":1,"enum":["too-long-private-value"]}),
    ] {
        let error = PreparedSchema::new(&schema).err();
        assert!(error.is_some());
        assert_eq!(error, validate_value(&schema, &Value::Null).err());
    }
}

#[test]
fn prepared_owns_snapshot_and_raw_validation_remains_defensive() {
    let mut schema = json!({"type":"object","properties":{"x":{"type":"integer","enum":[1.0]}},"required":["x"],"additionalProperties":false});
    let prepared = PreparedSchema::new(&schema).unwrap();
    schema["properties"]["x"]["enum"] = json!([2]);
    assert_eq!(prepared.validate_value(&json!({"x":1})), Ok(()));
    assert!(validate_value(&schema, &json!({"x":1})).is_err());
    assert!(prepared.validate_value(&json!({"x":2})).is_err());
    schema["required"] = json!(["ghost"]);
    assert!(validate_value(&schema, &json!({"x":2})).is_err());
    drop(schema);
    assert_eq!(prepared.validate_value(&json!({"x":1})), Ok(()));
}

#[test]
fn preparing_once_skips_all_structural_work_for_repeated_payloads() {
    let schema = json!({"type":"object","properties":{"x":{"type":"array","items":{"type":"integer","enum":[1.0]}}},"required":["x"],"additionalProperties":false});
    SCHEMA_VISITS.with(|visits| visits.set(0));
    let prepared = PreparedSchema::new(&schema).unwrap();
    let preparation_visits = SCHEMA_VISITS.with(std::cell::Cell::get);
    assert_eq!(preparation_visits, 3);
    for _ in 0..32 {
        assert_eq!(prepared.validate_value(&json!({"x":[1]})), Ok(()));
        assert_eq!(
            prepared.validate_value(&json!({"x":[2]})),
            fail("value is not an enum member")
        );
    }
    assert_eq!(SCHEMA_VISITS.with(std::cell::Cell::get), preparation_visits);
    for _ in 0..32 {
        assert_eq!(validate_value(&schema, &json!({"x":[1]})), Ok(()));
    }
    assert_eq!(
        SCHEMA_VISITS.with(std::cell::Cell::get),
        preparation_visits * 33
    );
}

#[test]
fn prepared_contract_validates_once_and_shares_methods_across_calls_and_chunks() {
    let original = json!({"version":"1","methods":{"run":{
        "mode":"stream","input":{"type":"integer"},
        "output":{"type":"integer","enum":[1.0]},"error":{"type":"string","maxLength":1}
    }}});
    SCHEMA_VISITS.with(|visits| visits.set(0));
    PAYLOAD_CHECKS.with(|checks| checks.set(0));
    let contract = PreparedContract::new(&original).unwrap();
    assert_eq!(SCHEMA_VISITS.with(std::cell::Cell::get), 3);
    let method = contract.method("run").unwrap().clone();
    assert!(std::sync::Arc::ptr_eq(
        &method,
        contract.method("run").unwrap()
    ));
    for _ in 0..32 {
        assert_eq!(method.input().validate_value(&json!(1)), Ok(()));
        assert_eq!(method.output().validate_value(&json!(1)), Ok(()));
        assert_eq!(
            method.output().validate_value(&json!(2)),
            fail("value is not an enum member")
        );
    }
    assert_eq!(SCHEMA_VISITS.with(std::cell::Cell::get), 3);
    assert_eq!(PAYLOAD_CHECKS.with(std::cell::Cell::get), 96);
    assert_eq!(contract.original(), &original);
    drop(contract);
    assert_eq!(method.error().validate_value(&json!("🌱")), Ok(()));
}

#[test]
fn prepared_contract_matches_raw_structural_diagnostics_and_order() {
    let valid = json!({"version":"1","methods":{"run":{
        "mode":"unary","input":{"type":"integer"},
        "output":{"type":"string"},"error":{"type":"string"}
    }}});
    let mut cases = vec![Value::Null, json!({}), json!({"version":"","methods":{}})];
    for field in ["input", "output", "error"] {
        let mut invalid = valid.clone();
        invalid["methods"]["run"]
            .as_object_mut()
            .unwrap()
            .remove(field);
        cases.push(invalid);
        let mut invalid = valid.clone();
        invalid["methods"]["run"][field] = json!({"type":"number","enum":[0,-0.0]});
        cases.push(invalid);
    }
    let mut invalid = valid.clone();
    invalid["methods"]["run"]["mode"] = json!("unknown");
    cases.push(invalid);
    let mut invalid = valid.clone();
    invalid["methods"]["run"]["extra"] = json!(true);
    cases.push(invalid);
    let mut invalid = valid;
    invalid["methods"]["run"]["input"] = json!({"type":"string","format":"date"});
    invalid["methods"]["zzz"] = json!({"mode":"unknown"});
    cases.push(invalid);
    for contract in cases {
        assert_eq!(
            PreparedContract::new(&contract).err(),
            validate_contract(&contract).err()
        );
    }
}
