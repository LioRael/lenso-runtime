//! Runtime v1 contract and host-frame validation. Errors never include business values.
use serde_json::{Map, Value};

/// Maximum NDJSON frame size, including the newline.
pub const MAX_FRAME_BYTES: usize = 256 * 1024;
const SAFE_INTEGER: f64 = 9_007_199_254_740_991.0;
const MAX_DEPTH: usize = 64;

/// A safe validation diagnostic, without the rejected payload.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct ValidationError(pub String);

fn fail<T>(message: &str) -> Result<T, ValidationError> {
    Err(ValidationError(message.to_owned()))
}

fn object(value: &Value) -> Result<&Map<String, Value>, ValidationError> {
    value
        .as_object()
        .ok_or_else(|| ValidationError("expected object".into()))
}

fn keys(obj: &Map<String, Value>, allowed: &[&str]) -> Result<(), ValidationError> {
    if obj.keys().any(|key| !allowed.contains(&key.as_str())) {
        return fail("unsupported field");
    }
    Ok(())
}

fn text(value: Option<&Value>) -> bool {
    value
        .and_then(Value::as_str)
        .is_some_and(|s| !s.is_empty() && s.len() <= 256)
}

/// Compare contract JSON using binary64 number semantics across language encoders.
pub fn json_equivalent(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Number(a), Value::Number(b)) => a.as_f64() == b.as_f64(),
        (Value::Array(a), Value::Array(b)) => {
            a.len() == b.len() && a.iter().zip(b).all(|(a, b)| json_equivalent(a, b))
        }
        (Value::Object(a), Value::Object(b)) => {
            a.len() == b.len()
                && a.iter().all(|(key, value)| {
                    b.get(key)
                        .is_some_and(|other| json_equivalent(value, other))
                })
        }
        _ => left == right,
    }
}

/// Reject malformed contracts and every schema feature outside the v1 subset.
///
/// # Errors
/// Returns a safe diagnostic for unsupported keywords, invalid shapes or bounds.
pub fn validate_contract(contract: &Value) -> Result<(), ValidationError> {
    let obj = object(contract)?;
    keys(obj, &["version", "methods"])?;
    if !text(obj.get("version")) {
        return fail("invalid contract version");
    }
    let methods = object(
        obj.get("methods")
            .ok_or_else(|| ValidationError("missing methods".into()))?,
    )?;
    if methods.len() > 256 {
        return fail("invalid method count");
    }
    for (name, method) in methods {
        if name.is_empty() || name.len() > 256 {
            return fail("invalid method name");
        }
        let method = object(method)?;
        keys(method, &["mode", "input", "output", "error"])?;
        if !matches!(
            method.get("mode").and_then(Value::as_str),
            Some("unary" | "stream")
        ) {
            return fail("invalid method mode");
        }
        for field in ["input", "output", "error"] {
            schema(
                method
                    .get(field)
                    .ok_or_else(|| ValidationError("missing method schema".into()))?,
                0,
            )?;
        }
    }
    Ok(())
}

fn schema(value: &Value, depth: usize) -> Result<(), ValidationError> {
    if depth > MAX_DEPTH {
        return fail("schema too deep");
    }
    let obj = object(value)?;
    if let Some(branches) = obj.get("anyOf") {
        keys(obj, &["anyOf"])?;
        let branches = branches
            .as_array()
            .ok_or_else(|| ValidationError("invalid nullable schema".into()))?;
        if branches.len() != 2
            || branches
                .iter()
                .filter(|b| **b == serde_json::json!({"type":"null"}))
                .count()
                != 1
            || branches
                .iter()
                .filter(|b| b.get("type").and_then(Value::as_str) == Some("null"))
                .count()
                != 1
            || branches.iter().any(|b| b.get("anyOf").is_some())
        {
            return fail("anyOf supports only a nullable pair");
        }
        for branch in branches {
            schema(branch, depth + 1)?;
        }
        return Ok(());
    }
    let kind = obj
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(|| ValidationError("schema needs type".into()))?;
    let allowed: &[&str] = match kind {
        "object" => &[
            "type",
            "properties",
            "required",
            "additionalProperties",
            "enum",
        ],
        "array" => &["type", "items", "minItems", "maxItems", "enum"],
        "string" => &["type", "minLength", "maxLength", "enum"],
        "number" | "integer" => &["type", "minimum", "maximum", "enum"],
        "null" | "boolean" => &["type", "enum"],
        _ => return fail("unsupported schema type"),
    };
    keys(obj, allowed)?;
    match kind {
        "object" => {
            if obj.get("additionalProperties") != Some(&Value::Bool(false)) {
                return fail("objects must forbid additional properties");
            }
            let props = object(
                obj.get("properties")
                    .ok_or_else(|| ValidationError("missing properties".into()))?,
            )?;
            for child in props.values() {
                schema(child, depth + 1)?;
            }
            {
                let required = obj
                    .get("required")
                    .ok_or_else(|| ValidationError("objects must declare required".into()))?;
                let required = required
                    .as_array()
                    .ok_or_else(|| ValidationError("invalid required".into()))?;
                let mut seen = std::collections::HashSet::new();
                for key in required {
                    let key = key
                        .as_str()
                        .ok_or_else(|| ValidationError("invalid required".into()))?;
                    if !props.contains_key(key) || !seen.insert(key) {
                        return fail("required must name distinct properties");
                    }
                }
                for (key, child) in props {
                    if !seen.contains(key.as_str())
                        && (child.get("anyOf").is_some() || child["type"] == "null")
                    {
                        return fail("optional nullable fields are unsupported");
                    }
                }
            }
        }
        "array" => schema(
            obj.get("items")
                .ok_or_else(|| ValidationError("missing items".into()))?,
            depth + 1,
        )?,
        _ => {}
    }
    for (min, max) in [("minLength", "maxLength"), ("minItems", "maxItems")] {
        for key in [min, max] {
            if obj.get(key).is_some_and(|v| {
                v.as_f64().is_none_or(|n| {
                    !n.is_finite() || n < 0.0 || n.fract() != 0.0 || n > SAFE_INTEGER
                })
            }) {
                return fail("invalid size bound");
            }
        }
        if let (Some(a), Some(b)) = (
            obj.get(min).and_then(Value::as_f64),
            obj.get(max).and_then(Value::as_f64),
        ) {
            if a > b {
                return fail("inverted size bounds");
            }
        }
    }
    for key in ["minimum", "maximum"] {
        if obj
            .get(key)
            .is_some_and(|v| v.as_f64().is_none_or(|n| !n.is_finite()))
        {
            return fail("invalid numeric bound");
        }
    }
    if let (Some(a), Some(b)) = (
        obj.get("minimum").and_then(Value::as_f64),
        obj.get("maximum").and_then(Value::as_f64),
    ) {
        if a > b {
            return fail("inverted numeric bounds");
        }
    }
    if let Some(variants) = obj.get("enum") {
        let variants = variants
            .as_array()
            .ok_or_else(|| ValidationError("invalid enum".into()))?;
        if variants.is_empty() || variants.len() > 64 || matches!(kind, "object" | "array") {
            return fail("enum supports at most 64 scalar members");
        }
        let mut without_enum = obj.clone();
        without_enum.remove("enum");
        let without_enum = Value::Object(without_enum);
        let mut seen = std::collections::HashSet::new();
        for variant in variants {
            let normalized;
            let candidate = if kind == "integer" {
                if let Some(n) = variant
                    .as_f64()
                    .filter(|n| n.is_finite() && n.fract() == 0.0 && n.abs() <= SAFE_INTEGER)
                {
                    normalized = serde_json::json!(n as i64);
                    &normalized
                } else {
                    variant
                }
            } else {
                variant
            };
            check(&without_enum, candidate, depth + 1)?;
            let encoded = if let Some(number) = candidate.as_f64() {
                if number == 0.0 {
                    "0".to_owned()
                } else {
                    number.to_string()
                }
            } else {
                serde_json::to_string(candidate)
                    .map_err(|_| ValidationError("invalid enum member".into()))?
            };
            if !seen.insert(encoded) {
                return fail("duplicate enum member");
            }
        }
    }
    Ok(())
}

/// Validate a JSON value without coercion, including safe-integer and closed-object rules.
///
/// # Errors
/// Returns an error if the schema is unsupported or the value violates it.
pub fn validate_value(schema_value: &Value, value: &Value) -> Result<(), ValidationError> {
    schema(schema_value, 0)?;
    check(schema_value, value, 0)
}

fn check(schema: &Value, value: &Value, depth: usize) -> Result<(), ValidationError> {
    if depth > MAX_DEPTH {
        return fail("value too deep");
    }
    if let Some(branches) = schema.get("anyOf").and_then(Value::as_array) {
        return if branches
            .iter()
            .any(|branch| check(branch, value, depth + 1).is_ok())
        {
            Ok(())
        } else {
            fail("value does not match nullable schema")
        };
    }
    let kind = schema["type"].as_str().unwrap_or("");
    let valid = match kind {
        "null" => value.is_null(),
        "boolean" => value.is_boolean(),
        "string" => value.is_string(),
        "object" => value.is_object(),
        "array" => value.is_array(),
        "number" => value.as_f64().is_some_and(f64::is_finite),
        "integer" => {
            (value.as_i64().is_some() || value.as_u64().is_some())
                && value.as_f64().is_some_and(|n| n.abs() <= SAFE_INTEGER)
        }
        _ => false,
    };
    if !valid {
        return fail("value has wrong type");
    }
    if schema
        .get("enum")
        .and_then(Value::as_array)
        .is_some_and(|variants| {
            !variants.iter().any(|variant| {
                if matches!(kind, "number" | "integer") {
                    variant.as_f64() == value.as_f64()
                } else {
                    variant == value
                }
            })
        })
    {
        return fail("value is not an enum member");
    }
    if let Some(obj) = value.as_object() {
        let props = object(&schema["properties"])?;
        if obj.keys().any(|key| !props.contains_key(key)) {
            return fail("unknown property");
        }
        if schema
            .get("required")
            .and_then(Value::as_array)
            .is_some_and(|required| {
                required
                    .iter()
                    .any(|key| !obj.contains_key(key.as_str().unwrap_or("")))
            })
        {
            return fail("missing required property");
        }
        for (key, child) in obj {
            check(&props[key], child, depth + 1)?;
        }
    }
    if let Some(values) = value.as_array() {
        size(schema, values.len(), "minItems", "maxItems")?;
        for child in values {
            check(&schema["items"], child, depth + 1)?;
        }
    }
    if let Some(text) = value.as_str() {
        size(schema, text.chars().count(), "minLength", "maxLength")?;
    }
    if let Some(number) = value.as_f64() {
        if schema
            .get("minimum")
            .and_then(Value::as_f64)
            .is_some_and(|min| number < min)
            || schema
                .get("maximum")
                .and_then(Value::as_f64)
                .is_some_and(|max| number > max)
        {
            return fail("number outside bounds");
        }
    }
    Ok(())
}

fn size(schema: &Value, count: usize, min: &str, max: &str) -> Result<(), ValidationError> {
    if schema
        .get(min)
        .and_then(Value::as_f64)
        .is_some_and(|n| (count as f64) < n)
        || schema
            .get(max)
            .and_then(Value::as_f64)
            .is_some_and(|n| (count as f64) > n)
    {
        return fail("size outside bounds");
    }
    Ok(())
}

/// Validate host-to-kernel frame shapes, rejecting unknown types and fields.
/// Lifecycle, route ownership and contract validation belong to the kernel.
pub fn validate_host_frame(value: &Value) -> Result<(), ValidationError> {
    let obj = object(value)?;
    let kind = obj.get("type").and_then(Value::as_str).unwrap_or("");
    let (required, optional): (&[&str], &[&str]) = match kind {
        "hello" => (&["version", "namespace", "service", "instance"], &[]),
        "register" => (&["contract", "max_concurrency"], &[]),
        "ready" | "heartbeat" | "unregister" => (&[], &[]),
        "call" => (
            &[
                "id",
                "namespace",
                "service",
                "method",
                "contract_version",
                "input",
                "timeout_ms",
            ],
            &["trace_id", "parent_id"],
        ),
        "result" | "chunk" => (&["id", "output"], &[]),
        "error" => (&["code", "message"], &["id", "details"]),
        "end" | "cancel" | "status" => (&["id"], &[]),
        "credit" => (&["id", "credits"], &[]),
        "drain" => (&["timeout_ms"], &[]),
        "promote" => (
            &["id", "namespace", "service", "instance", "timeout_ms"],
            &[],
        ),
        _ => return fail("unknown frame type"),
    };
    if required.iter().any(|key| !obj.contains_key(*key))
        || obj.keys().any(|key| {
            key != "type" && !required.contains(&key.as_str()) && !optional.contains(&key.as_str())
        })
    {
        return fail("invalid frame fields");
    }
    for key in [
        "id",
        "namespace",
        "service",
        "instance",
        "method",
        "contract_version",
        "trace_id",
        "parent_id",
    ] {
        if obj.contains_key(key) && !text(obj.get(key)) {
            return fail("invalid frame identifier");
        }
    }
    for key in ["timeout_ms", "max_concurrency", "credits"] {
        if obj.contains_key(key) && obj[key].as_u64().is_none() {
            return fail("invalid frame number");
        }
    }
    if kind == "hello" && obj["version"] != 1 {
        return fail("unsupported protocol version");
    }
    if kind == "error"
        && (!matches!(
            obj["code"].as_str(),
            Some(
                "service_not_found"
                    | "not_ready"
                    | "overloaded"
                    | "deadline_exceeded"
                    | "connection_lost"
                    | "cancelled"
                    | "contract_mismatch"
                    | "invalid_input"
                    | "invalid_output"
                    | "business"
                    | "protocol_error"
            )
        ) || obj["message"].as_str().is_none_or(|s| s.len() > 1024))
    {
        return fail("invalid error frame");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn validates_only_scoped_promotion_shape() {
        let frame = json!({"type":"promote","id":"1","namespace":"demo","service":"worker","instance":"new","timeout_ms":5000});
        assert!(validate_host_frame(&frame).is_ok());
        let mut unknown = frame.clone();
        unknown["execute"] = json!("anything");
        assert!(validate_host_frame(&unknown).is_err());
        let mut missing = frame.clone();
        missing.as_object_mut().unwrap().remove("instance");
        assert!(validate_host_frame(&missing).is_err());
        let mut invalid = frame;
        invalid["timeout_ms"] = json!(-1);
        assert!(validate_host_frame(&invalid).is_err());
    }

    #[test]
    fn rejects_unsupported_and_malformed_contracts() {
        for schema in [
            json!({"type":"string","format":"date"}),
            json!({"type":"object","properties":{}}),
            json!({"type":"array"}),
            json!({"type":"integer","enum":(0..65).collect::<Vec<_>>()}),
            json!({"type":"array","items":{"type":"integer"},"enum":[[1]]}),
            json!({"anyOf":[{"type":"string"},{"type":"number"}]}),
            json!({"type":"string","minLength":3,"maxLength":2}),
            json!({"type":"object","properties":{},"required":["ghost"],"additionalProperties":false}),
        ] {
            let contract = json!({"version":"1","methods":{"run":{"mode":"unary","input":schema,"output":{"type":"null"},"error":{"type":"null"}}}});
            assert!(validate_contract(&contract).is_err());
        }
        assert!(validate_contract(&json!({"version":"1","methods":{}})).is_ok());
    }

    #[test]
    fn optional_is_distinct_from_null_and_objects_are_closed() {
        let s = json!({"type":"object","properties":{"x":{"type":"string"}},"required":[],"additionalProperties":false});
        assert!(validate_value(&s, &json!({})).is_ok());
        assert!(validate_value(&s, &json!({"x":null})).is_err());
        assert!(validate_value(&s, &json!({"other":1})).is_err());
    }

    #[test]
    fn validates_nullable_unicode_lengths_and_safe_integers() {
        let s = json!({"anyOf":[{"type":"null"},{"type":"string","minLength":1,"maxLength":1}]});
        assert!(validate_value(&s, &json!("🌱")).is_ok());
        assert!(validate_value(&s, &Value::Null).is_ok());
        assert!(validate_value(
            &json!({"type":"integer"}),
            &json!(9_007_199_254_740_992_u64)
        )
        .is_err());
        assert!(validate_value(&json!({"type":"integer"}), &json!(1.5)).is_err());
        assert!(validate_value(&json!({"type":"integer"}), &json!(1.0)).is_err());
        assert!(validate_value(&json!({"type":"integer","enum":[1.0]}), &json!(1)).is_ok());
        assert!(validate_value(&json!({"type":"string","maxLength":1.0}), &json!("a")).is_ok());
        assert!(json_equivalent(
            &json!({"minimum":0.0}),
            &json!({"minimum":0})
        ));
    }
}
