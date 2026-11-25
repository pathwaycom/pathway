// Copyright © 2026 Pathway

//! JSON decoding shared by input connectors and typed JSON field expressions.

use base64::Engine;
use itertools::Itertools;
use ndarray::ArrayD;
use serde_json::{Map as JsonMap, Value as JsonValue};

use super::value::parse_pathway_pointer;
use super::{DateTimeNaive, DateTimeUtc, Duration as EngineDuration, Type, Value};

pub const NDARRAY_ELEMENTS_FIELD_NAME: &str = "elements";
pub const NDARRAY_SHAPE_FIELD_NAME: &str = "shape";

fn parse_list_from_json(values: &[JsonValue], dtype: &Type) -> Option<Value> {
    let mut list = Vec::with_capacity(values.len());
    for value in values {
        list.push(parse_value_from_json(value, dtype)?);
    }
    Some(Value::from(list))
}

fn parse_tuple_from_json(values: &[JsonValue], dtypes: &[Type]) -> Option<Value> {
    if values.len() != dtypes.len() {
        return None;
    }
    let mut tuple = Vec::with_capacity(values.len());
    for (value, dtype) in values.iter().zip_eq(dtypes.iter()) {
        tuple.push(parse_value_from_json(value, dtype)?);
    }
    Some(Value::from(tuple))
}

fn parse_ndarray_from_json(
    value: &JsonMap<String, JsonValue>,
    n_dim: Option<usize>,
    dtype: &Type,
) -> Option<Value> {
    let JsonValue::Array(ref elements) = *value.get(NDARRAY_ELEMENTS_FIELD_NAME)? else {
        return None;
    };
    let JsonValue::Array(ref json_field_shape) = *value.get(NDARRAY_SHAPE_FIELD_NAME)? else {
        return None;
    };
    if n_dim.is_some_and(|n_dim| n_dim != json_field_shape.len()) {
        return None;
    }
    let mut shape = Vec::new();
    for shape_element in json_field_shape {
        let JsonValue::Number(shape_element) = shape_element else {
            return None;
        };
        let shape_element = shape_element.as_u64()?;
        let Ok(shape_element) = TryInto::<usize>::try_into(shape_element) else {
            return None;
        };
        shape.push(shape_element);
    }

    match dtype {
        Type::Int => {
            let mut flat_elements = Vec::new();
            for element in elements {
                let JsonValue::Number(number_element) = element else {
                    return None;
                };
                let int_element = number_element.as_i64()?;
                flat_elements.push(int_element);
            }
            let array_impl = ArrayD::<i64>::from_shape_vec(shape, flat_elements);
            if let Ok(array_impl) = array_impl {
                Some(Value::from(array_impl))
            } else {
                None
            }
        }
        Type::Float => {
            let mut flat_elements = Vec::new();
            for element in elements {
                let JsonValue::Number(number_element) = element else {
                    return None;
                };
                let float_element = number_element.as_f64()?;
                flat_elements.push(float_element);
            }
            let array_impl = ArrayD::<f64>::from_shape_vec(shape, flat_elements);
            if let Ok(array_impl) = array_impl {
                Some(Value::from(array_impl))
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Parses a value of type `dtype` from JSON variable `value`.
/// If the field is not present or has incorrect format in the
/// given payload, returns `None`. This `None` is further converted
/// into `ParseError::FailedToParseFromJson` containing verbose
/// information about parsing problem.
pub fn parse_value_from_json(value: &JsonValue, dtype: &Type) -> Option<Value> {
    if value.is_null() {
        if dtype.is_optional() {
            return Some(Value::None);
        }
        return None;
    }
    match (dtype.unoptionalize(), value) {
        (Type::Bool | Type::Any, JsonValue::Bool(v)) => Some(Value::Bool(*v)),
        // Numbers parsing
        (Type::Int, JsonValue::Number(v)) => {
            let i64_field = v.as_i64()?;
            Some(Value::from(i64_field))
        }
        (Type::Float, JsonValue::Number(v)) => {
            let f64_field = v.as_f64()?;
            Some(Value::from(f64_field))
        }
        (Type::Duration, JsonValue::Number(v)) => {
            let duration_ns = v.as_i64()?;
            let engine_duration = EngineDuration::new_with_unit(duration_ns, "ns")
                .expect("new_with_unit can't fail when 'ns' is used as a unit");
            Some(Value::Duration(engine_duration))
        }
        (Type::Any, JsonValue::Number(v)) => {
            if let Some(parsed_i64) = v.as_i64() {
                Some(Value::from(parsed_i64))
            } else {
                v.as_f64().map(Value::from)
            }
        }
        // Strings parsing
        (Type::String | Type::Any, JsonValue::String(s)) => Some(Value::from(s.as_str())),
        (Type::Bytes, JsonValue::String(s)) => {
            let decoded = base64::engine::general_purpose::STANDARD.decode(s);
            if let Ok(decoded) = decoded {
                Some(Value::Bytes(decoded.into()))
            } else {
                None
            }
        }
        (Type::PyObjectWrapper, JsonValue::String(s)) => {
            let bytes = base64::engine::general_purpose::STANDARD.decode(s).ok()?;
            let value = bincode::deserialize::<Value>(&bytes).ok()?;
            matches!(value, Value::PyObjectWrapper(_)).then_some(value)
        }
        (Type::Pointer, JsonValue::String(s)) => parse_pathway_pointer(s).ok(),
        (Type::DateTimeUtc, JsonValue::String(s)) => {
            let engine_datetime = DateTimeUtc::strptime(s, "%Y-%m-%dT%H:%M:%S%.f%z");
            if let Ok(engine_datetime) = engine_datetime {
                Some(Value::DateTimeUtc(engine_datetime))
            } else {
                None
            }
        }
        (Type::DateTimeNaive, JsonValue::String(s)) => {
            let engine_datetime = DateTimeNaive::strptime(s, "%Y-%m-%dT%H:%M:%S%.f");
            if let Ok(engine_datetime) = engine_datetime {
                Some(Value::DateTimeNaive(engine_datetime))
            } else {
                None
            }
        }
        (Type::Json, value) => Some(Value::from(value.clone())),
        (Type::Tuple(dtypes), JsonValue::Array(v)) => parse_tuple_from_json(v, dtypes),
        (Type::List(arg), JsonValue::Array(v)) => parse_list_from_json(v, arg),
        (Type::Array(n_dim, nested_type), JsonValue::Object(v)) => {
            parse_ndarray_from_json(v, *n_dim, nested_type.as_ref())
        }
        _ => None,
    }
}
