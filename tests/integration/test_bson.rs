// Copyright © 2024 Pathway

use std::sync::Arc;

use ndarray::{arr1, ArrayD};
use ordered_float::OrderedFloat;
use serde_json::json;

use pathway_engine::connectors::data_format::{BsonFormatter, Formatter};
use pathway_engine::engine::time::DateTime;
use pathway_engine::engine::{DateTimeNaive, DateTimeUtc, Duration, Key, Timestamp, Type, Value};

const TEST_FIELD: &str = "field";

fn test_type_formatting(type_: Type, values: &[Value]) -> eyre::Result<()> {
    let value_fields = vec![TEST_FIELD.to_string()];
    let mut formatter = BsonFormatter::new(value_fields);

    for value in values {
        let context = formatter
            .format(&Key::random(), &[value.clone()], Timestamp(0), 1)
            .expect("formatter failed");
        assert_eq!(context.payloads.len(), 1);
        let document = context.payloads[0].clone().into_bson_document().unwrap();
        match (&type_, value) {
            (&Type::Bool, Value::Bool(b)) => assert_eq!(*b, document.get_bool(TEST_FIELD).unwrap()),
            (&Type::Int, Value::Int(i)) => assert_eq!(*i, document.get_i64(TEST_FIELD).unwrap()),
            (&Type::Float, Value::Float(f)) => {
                assert_eq!(*f, document.get_f64(TEST_FIELD).unwrap())
            }
            (&Type::String, Value::String(s)) => {
                assert_eq!(*s, document.get_str(TEST_FIELD).unwrap())
            }
            (&Type::Bytes, Value::Bytes(b)) => assert_eq!(
                &b.to_vec(),
                document.get_binary_generic(TEST_FIELD).unwrap()
            ),
            (&Type::Pointer, Value::Pointer(p)) => {
                assert_eq!(&p.to_string(), document.get_str(TEST_FIELD).unwrap())
            }
            (&Type::DateTimeNaive, Value::DateTimeNaive(dt)) => assert_eq!(
                dt.timestamp_milliseconds(),
                document
                    .get_datetime(TEST_FIELD)
                    .unwrap()
                    .timestamp_millis()
            ),
            (&Type::DateTimeUtc, Value::DateTimeUtc(dt)) => assert_eq!(
                dt.timestamp_milliseconds(),
                document
                    .get_datetime(TEST_FIELD)
                    .unwrap()
                    .timestamp_millis()
            ),
            (&Type::Duration, Value::Duration(d)) => {
                assert_eq!(d.milliseconds(), document.get_i64(TEST_FIELD).unwrap())
            }
            (&Type::Json, Value::Json(_)) => {
                let raw_str = document.get_str(TEST_FIELD).unwrap();
                let parsed_json: serde_json::Value = serde_json::from_str(raw_str).unwrap();
                assert_eq!(Value::from(parsed_json), value.clone());
            }
            (&Type::Tuple(_), Value::Tuple(t)) => {
                let document_contents: Vec<bool> = document
                    .get_array(TEST_FIELD)
                    .expect("only boolean arrays are supported in the array test")
                    .iter()
                    .map(|x| x.as_bool().unwrap())
                    .collect();
                let original_contents: Vec<bool> = t
                    .iter()
                    .cloned()
                    .map(|x| match x {
                        Value::Bool(b) => b,
                        _ => unreachable!("only boolean arrays are supported in the array test"),
                    })
                    .collect();
                assert_eq!(original_contents, document_contents);
            }
            (&Type::Array(_, _), Value::IntArray(a)) => {
                let document_contents: Vec<i64> = document
                    .get_array(TEST_FIELD)
                    .unwrap()
                    .iter()
                    .map(|x| x.as_i64().unwrap())
                    .collect();
                let original_contents: Vec<i64> = a.iter().copied().collect();
                assert_eq!(original_contents, document_contents);
            }
            (&Type::Array(_, _), Value::FloatArray(a)) => {
                let document_contents: Vec<f64> = document
                    .get_array(TEST_FIELD)
                    .unwrap()
                    .iter()
                    .map(|x| x.as_f64().unwrap())
                    .collect();
                let original_contents: Vec<f64> = a.iter().copied().collect();
                assert_eq!(original_contents, document_contents);
            }
            _ => unreachable!(),
        }
    }

    Ok(())
}

#[test]
fn test_format_bool() -> eyre::Result<()> {
    test_type_formatting(Type::Bool, &[Value::Bool(true), Value::Bool(false)])
}

#[test]
fn test_format_int() -> eyre::Result<()> {
    test_type_formatting(Type::Int, &[Value::Int(-1), Value::Int(0), Value::Int(1)])
}

#[test]
fn test_format_float() -> eyre::Result<()> {
    test_type_formatting(
        Type::Float,
        &[
            Value::Float(OrderedFloat(-1.0)),
            Value::Float(OrderedFloat(0.0)),
            Value::Float(OrderedFloat(1e50)),
        ],
    )
}

#[test]
fn test_format_pointer() -> eyre::Result<()> {
    let test_key = Key::random();
    test_type_formatting(Type::Pointer, &[Value::Pointer(test_key)])
}

#[test]
fn test_format_string() -> eyre::Result<()> {
    test_type_formatting(
        Type::String,
        &[Value::String("abc".into()), Value::String("".into())],
    )
}

#[test]
fn test_format_bytes() -> eyre::Result<()> {
    let test_bytes: &[u8] = &[1, 10, 5, 19, 55, 67, 9, 87, 28];
    test_type_formatting(
        Type::Bytes,
        &[Value::Bytes([].into()), Value::Bytes(test_bytes.into())],
    )
}

#[test]
fn test_format_datetimenaive() -> eyre::Result<()> {
    test_type_formatting(
        Type::DateTimeNaive,
        &[
            Value::DateTimeNaive(DateTimeNaive::from_timestamp(0, "s")?),
            Value::DateTimeNaive(DateTimeNaive::from_timestamp(10000, "s")?),
            Value::DateTimeNaive(DateTimeNaive::from_timestamp(-10000, "s")?),
        ],
    )
}

#[test]
fn test_format_datetimeutc() -> eyre::Result<()> {
    test_type_formatting(
        Type::DateTimeUtc,
        &[
            Value::DateTimeUtc(DateTimeUtc::new(0)),
            Value::DateTimeUtc(DateTimeUtc::new(10_000_000_000_000)),
            Value::DateTimeUtc(DateTimeUtc::new(-10_000_000_000_000)),
        ],
    )
}

#[test]
fn test_format_duration() -> eyre::Result<()> {
    test_type_formatting(
        Type::Duration,
        &[
            Value::Duration(Duration::new(0)),
            Value::Duration(Duration::new(10_000_000_000_000)),
            Value::Duration(Duration::new(-10_000_000_000_000)),
        ],
    )
}

#[test]
fn test_format_json() -> eyre::Result<()> {
    test_type_formatting(Type::Json, &[Value::from(json!({"A": 100}))])
}

#[test]
fn test_format_tuple() -> eyre::Result<()> {
    let test_array = Value::Tuple(vec![Value::Bool(true), Value::Bool(false)].into());
    test_type_formatting(Type::Tuple(Arc::new([Type::Bool])), &[test_array])
}

#[test]
fn test_format_int_array() -> eyre::Result<()> {
    let test_array: ArrayD<i64> = arr1(&[1_i64, 2_i64, 3_i64]).into_dyn();
    let test_value = Value::from(test_array);
    test_type_formatting(Type::Array(None, Arc::new(Type::Int)), &[test_value])
}

#[test]
fn test_format_float_array() -> eyre::Result<()> {
    let test_array: ArrayD<f64> = arr1(&[1.0_f64, 2.0_f64, 3.0_f64]).into_dyn();
    let test_value = Value::from(test_array);
    test_type_formatting(Type::Array(None, Arc::new(Type::Float)), &[test_value])
}

#[test]
fn test_unsupported_type() -> eyre::Result<()> {
    let value_fields = vec![TEST_FIELD.to_string()];
    let mut formatter = BsonFormatter::new(value_fields);

    let context = formatter.format(&Key::random(), &[Value::Error], Timestamp(0), 1);
    assert!(context.is_err());

    Ok(())
}
