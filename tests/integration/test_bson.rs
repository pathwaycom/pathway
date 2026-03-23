// Copyright © 2026 Pathway

use std::collections::HashMap;
use std::sync::Arc;

use mongodb::bson::{doc, Document as BsonDocument};
use ndarray::{arr1, ArrayD};
use ordered_float::OrderedFloat;
use serde_json::json;

use pathway_engine::connectors::bson::BsonParser;
use pathway_engine::connectors::data_format::{
    BsonFormatter, Formatter, InnerSchemaField, ParsedEventWithErrors, Parser,
};
use pathway_engine::connectors::data_storage::{DataEventType, ReaderContext};
use pathway_engine::connectors::SessionType;
use pathway_engine::engine::time::DateTime;
use pathway_engine::engine::{DateTimeNaive, DateTimeUtc, Duration, Key, Timestamp, Type, Value};

const TEST_FIELD: &str = "field";

fn check_formatted_value(type_: &Type, value: &Value, document: &BsonDocument) {
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
                .map(|x| match x {
                    Value::Bool(b) => *b,
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

fn test_type_formatting(type_: Type, values: &[Value]) -> eyre::Result<()> {
    let value_fields = vec![TEST_FIELD.to_string()];
    let formatter_with_specials = BsonFormatter::new(value_fields.clone(), true);
    let formatter_without_specials = BsonFormatter::new(value_fields, false);
    let mut formatters = [
        (formatter_with_specials, true),
        (formatter_without_specials, false),
    ];

    for value in values {
        for (ref mut formatter, is_with_specials) in formatters.iter_mut() {
            let context = formatter
                .format(&Key::random(), std::slice::from_ref(value), Timestamp(0), 1)
                .expect("formatter failed");
            assert_eq!(context.payloads.len(), 1);
            let document = context.payloads[0].clone().into_bson_document().unwrap();
            check_formatted_value(&type_, value, &document);
            if *is_with_specials {
                assert_eq!(document.len(), 3); // value, time, diff
            } else {
                assert_eq!(document.len(), 1); // only value
            }
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
    let mut formatter = BsonFormatter::new(value_fields, true);

    let context = formatter.format(&Key::random(), &[Value::Error], Timestamp(0), 1);
    assert!(context.is_err());

    Ok(())
}

#[test]
fn test_format_without_specials() -> eyre::Result<()> {
    test_type_formatting(
        Type::String,
        &[Value::String("abc".into()), Value::String("".into())],
    )
}

fn make_schema(fields: &[(&str, Type)]) -> HashMap<String, InnerSchemaField> {
    fields
        .iter()
        .map(|(name, type_)| {
            (
                (*name).to_string(),
                InnerSchemaField::new(type_.clone(), None),
            )
        })
        .collect()
}

fn parse_bson_context(
    parser: &mut BsonParser,
    event_type: DataEventType,
    key: &str,
    document: BsonDocument,
) -> ParsedEventWithErrors {
    let context = ReaderContext::Bson((event_type, key.to_string(), document));
    let mut events = parser.parse(&context).expect("parse failed");
    assert_eq!(events.len(), 1);
    events.remove(0)
}

fn extract_key(event: &ParsedEventWithErrors) -> Vec<Value> {
    match event {
        ParsedEventWithErrors::Insert((Some(Ok(key_values)), _)) => key_values.clone(),
        other => panic!("expected Insert with Ok key, got: {other:?}"),
    }
}

fn extract_values(event: &ParsedEventWithErrors) -> Vec<Value> {
    match event {
        ParsedEventWithErrors::Insert((_, value_fields)) => value_fields
            .iter()
            .map(|r| r.as_ref().expect("value field error").clone())
            .collect(),
        other => panic!("expected Insert, got: {other:?}"),
    }
}

/// The key is always the raw MongoDB _id string passed in the ReaderContext.
#[test]
fn test_bson_parser_key_from_raw_id() -> eyre::Result<()> {
    let value_fields = vec!["name".to_string(), "count".to_string()];
    let schema = make_schema(&[("name", Type::String), ("count", Type::Int)]);
    let mut parser = BsonParser::new(value_fields, schema, SessionType::Native)?;

    let doc = doc! { "name": "Alice", "count": 42_i32 };
    let event = parse_bson_context(&mut parser, DataEventType::Insert, "doc-key-123", doc);

    assert_eq!(
        extract_key(&event),
        vec![Value::String("doc-key-123".into())]
    );
    assert_eq!(
        extract_values(&event),
        vec![Value::String("Alice".into()), Value::Int(42)]
    );

    Ok(())
}

fn test_type_parsing(type_: Type, values: &[Value]) -> eyre::Result<()> {
    let value_fields = vec![TEST_FIELD.to_string()];
    let schema = make_schema(&[(TEST_FIELD, type_.clone())]);
    let mut parser = BsonParser::new(value_fields, schema, SessionType::Native)?;
    let mut formatter = BsonFormatter::new(vec![TEST_FIELD.to_string()], false);

    for value in values {
        let context = formatter
            .format(&Key::random(), std::slice::from_ref(value), Timestamp(0), 1)
            .expect("formatter failed");
        let document = context.payloads[0].clone().into_bson_document().unwrap();

        let event = parse_bson_context(&mut parser, DataEventType::Insert, "test-key", document);
        assert_eq!(extract_values(&event), std::slice::from_ref(value));
    }

    Ok(())
}

#[test]
fn test_parse_bool() -> eyre::Result<()> {
    test_type_parsing(Type::Bool, &[Value::Bool(true), Value::Bool(false)])
}

#[test]
fn test_parse_int() -> eyre::Result<()> {
    test_type_parsing(Type::Int, &[Value::Int(-1), Value::Int(0), Value::Int(1)])
}

#[test]
fn test_parse_float() -> eyre::Result<()> {
    test_type_parsing(
        Type::Float,
        &[
            Value::Float(OrderedFloat(-1.0)),
            Value::Float(OrderedFloat(0.0)),
            Value::Float(OrderedFloat(1e50)),
        ],
    )
}

#[test]
fn test_parse_pointer() -> eyre::Result<()> {
    let test_key = Key::random();
    test_type_parsing(Type::Pointer, &[Value::Pointer(test_key)])
}

#[test]
fn test_parse_string() -> eyre::Result<()> {
    test_type_parsing(
        Type::String,
        &[Value::String("abc".into()), Value::String("".into())],
    )
}

#[test]
fn test_parse_bytes() -> eyre::Result<()> {
    let test_bytes: &[u8] = &[1, 10, 5, 19, 55, 67, 9, 87, 28];
    test_type_parsing(
        Type::Bytes,
        &[Value::Bytes([].into()), Value::Bytes(test_bytes.into())],
    )
}

#[test]
fn test_parse_datetimenaive() -> eyre::Result<()> {
    test_type_parsing(
        Type::DateTimeNaive,
        &[
            Value::DateTimeNaive(DateTimeNaive::from_timestamp(0, "s")?),
            Value::DateTimeNaive(DateTimeNaive::from_timestamp(10000, "s")?),
            Value::DateTimeNaive(DateTimeNaive::from_timestamp(-10000, "s")?),
        ],
    )
}

#[test]
fn test_parse_datetimeutc() -> eyre::Result<()> {
    test_type_parsing(
        Type::DateTimeUtc,
        &[
            Value::DateTimeUtc(DateTimeUtc::new(0)),
            Value::DateTimeUtc(DateTimeUtc::new(10_000_000_000_000)),
            Value::DateTimeUtc(DateTimeUtc::new(-10_000_000_000_000)),
        ],
    )
}

#[test]
fn test_parse_duration() -> eyre::Result<()> {
    test_type_parsing(
        Type::Duration,
        &[
            Value::Duration(Duration::new(0)),
            Value::Duration(Duration::new(10_000_000_000_000)),
            Value::Duration(Duration::new(-10_000_000_000_000)),
        ],
    )
}

#[test]
fn test_parse_json() -> eyre::Result<()> {
    test_type_parsing(Type::Json, &[Value::from(json!({"A": 100}))])
}

#[test]
fn test_parse_tuple() -> eyre::Result<()> {
    // The formatter ignores inner-type count but the parser validates array length,
    // so the type must list one element type per value element.
    let test_array = Value::Tuple(vec![Value::Bool(true), Value::Bool(false)].into());
    test_type_parsing(
        Type::Tuple(Arc::new([Type::Bool, Type::Bool])),
        &[test_array],
    )
}

#[test]
fn test_parse_unsupported_type() -> eyre::Result<()> {
    // Type::Array is not supported by the parser (mirrors test_unsupported_type for
    // the formatter, which rejects Value::Error).
    let value_fields = vec![TEST_FIELD.to_string()];
    let schema = make_schema(&[(TEST_FIELD, Type::Array(None, Arc::new(Type::Int)))]);
    let mut parser = BsonParser::new(value_fields, schema, SessionType::Native)?;

    let doc = doc! { TEST_FIELD: [1_i64, 2_i64, 3_i64] };
    let event = parse_bson_context(&mut parser, DataEventType::Insert, "key", doc);
    assert_parse_field_error(event);

    Ok(())
}

fn assert_parse_field_error(event: ParsedEventWithErrors) {
    match event {
        ParsedEventWithErrors::Insert((_, fields)) => {
            assert_eq!(fields.len(), 1);
            assert!(fields[0].is_err());
        }
        other => panic!("expected Insert event with error field, got: {other:?}"),
    }
}

#[test]
fn test_parse_optional() -> eyre::Result<()> {
    // Value::None (null) and a non-null value both round-trip through Optional.
    test_type_parsing(
        Type::Optional(Arc::new(Type::Bool)),
        &[Value::None, Value::Bool(true), Value::Bool(false)],
    )
}

#[test]
fn test_parse_list() -> eyre::Result<()> {
    // List is like Tuple but validated element-by-element against a single inner type.
    let list_value = Value::Tuple(vec![Value::Int(1), Value::Int(2), Value::Int(3)].into());
    test_type_parsing(Type::List(Arc::new(Type::Int)), &[list_value])
}

#[test]
fn test_parse_any() -> eyre::Result<()> {
    // Type::Any infers the Value kind from the BSON type.  Only values whose
    // formatter→parser round-trip produces the same Value type are included here;
    // DateTimeNaive, Duration, Pointer, and Json all come back as a different type
    // when parsed as Any (e.g. DateTimeNaive → DateTimeUtc).
    test_type_parsing(
        Type::Any,
        &[
            Value::None,
            Value::Bool(true),
            Value::Int(42),
            Value::Float(OrderedFloat(1.5)),
            Value::String("hello".into()),
            Value::Bytes([1u8, 2, 3].as_slice().into()),
            Value::DateTimeUtc(DateTimeUtc::new(0)),
            Value::Tuple(vec![Value::Bool(true), Value::Bool(false)].into()),
        ],
    )
}

#[test]
fn test_parse_future_unsupported() -> eyre::Result<()> {
    // Type::Future is not supported by the parser, just like Type::Array.
    let value_fields = vec![TEST_FIELD.to_string()];
    let schema = make_schema(&[(TEST_FIELD, Type::Future(Arc::new(Type::Int)))]);
    let mut parser = BsonParser::new(value_fields, schema, SessionType::Native)?;

    let doc = doc! { TEST_FIELD: 42_i64 };
    let event = parse_bson_context(&mut parser, DataEventType::Insert, "key", doc);
    assert_parse_field_error(event);

    Ok(())
}

#[test]
fn test_format_pending() -> eyre::Result<()> {
    // Value::Pending is non-serializable, just like Value::Error.
    let value_fields = vec![TEST_FIELD.to_string()];
    let mut formatter = BsonFormatter::new(value_fields, true);

    let context = formatter.format(&Key::random(), &[Value::Pending], Timestamp(0), 1);
    assert!(context.is_err());

    Ok(())
}

/// Delete events carry only the key; value columns are not parsed.
#[test]
fn test_bson_parser_delete_has_no_values() -> eyre::Result<()> {
    let value_fields = vec!["name".to_string(), "count".to_string()];
    let schema = make_schema(&[("name", Type::String), ("count", Type::Int)]);
    let mut parser = BsonParser::new(value_fields, schema, SessionType::Native)?;

    // MongoDB sends an empty document for deletes; the parser must not attempt
    // to read value columns from it.
    let event = parse_bson_context(
        &mut parser,
        DataEventType::Delete,
        "doc-key-456",
        BsonDocument::new(),
    );

    match event {
        ParsedEventWithErrors::Delete((Some(Ok(key_values)), value_fields)) => {
            assert_eq!(key_values, vec![Value::String("doc-key-456".into())]);
            assert!(value_fields.is_empty());
        }
        other => panic!("expected Delete with Ok key and empty values, got: {other:?}"),
    }

    Ok(())
}
