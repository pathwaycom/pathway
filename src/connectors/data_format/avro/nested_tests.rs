// Copyright © 2026 Pathway

use super::*;
use crate::connectors::data_format::json_schema::JsonSchema;
use crate::connectors::data_format::{FieldSource, ParsedEventWithErrors, Parser};
use crate::connectors::{DataEventType, ReaderContext};

fn object(name: &str, schema: JsonValue) -> JsonValue {
    let mut field = json!({"name": name});
    field["schema"] = schema;
    json!({"type": "object", "fields": [field]})
}

fn normalize(schema: JsonValue, value: AvroValue) -> Result<JsonValue, String> {
    let schema: JsonSchema = serde_json::from_value(schema).unwrap();
    let mut value = schema
        .normalize_avro(value, "/one/two/three")
        .map_err(|error| error.to_string())?;
    schema
        .validate(&mut value, "/one/two/three")
        .map_err(|error| error.to_string())?;
    Ok(value)
}

fn field(schema: JsonValue, optional: bool, default: Option<Value>) -> ValueField {
    let dtype = if optional {
        Type::Optional(Type::Json.into())
    } else {
        Type::Json
    };
    let mut field = ValueField::new("one".to_owned(), dtype, FieldSource::Payload);
    field.json_schema = Some(serde_json::from_value(schema).unwrap());
    field.default = default;
    field
}

fn parse(schema: &JsonValue, value: AvroValue, field: ValueField) -> Result<Value, String> {
    let schema = Arc::new(AvroSchema::parse(schema).unwrap());
    let bytes = GenericDatumWriter::builder(&schema)
        .build()
        .unwrap()
        .write_value_to_vec(value)
        .unwrap();
    let mut parser = AvroParser::new(vec![field], None, None, true).unwrap();
    // Exercise the parser with a foreign producer's schema without a broker.
    parser.reader_schema = schema;
    let mut events = parser
        .parse(&ReaderContext::RawBytes(DataEventType::Insert, bytes))
        .unwrap();
    let ParsedEventWithErrors::Insert((_, mut values)) = events.pop().unwrap() else {
        panic!("expected an insert");
    };
    values.pop().unwrap().map_err(|error| error.to_string())
}

#[test]
fn native_avro_nested_leaves() {
    let leaves = [
        ("bool", AvroValue::Boolean(true), json!(true)),
        ("int", AvroValue::Long(i64::MAX), json!(i64::MAX)),
        ("float", AvroValue::Int(42), json!(42.0)),
        ("float", AvroValue::Double(1.5), json!(1.5)),
        ("string", AvroValue::Enum(0, "ready".into()), json!("ready")),
        ("bytes", AvroValue::Bytes(vec![0, 1, 255]), json!("AAH/")),
        ("bytes", AvroValue::Fixed(0, vec![]), json!("")),
        (
            "date_time_utc",
            AvroValue::TimestampMillis(123),
            json!("1970-01-01T00:00:00.123000000+0000"),
        ),
        (
            "date_time_utc",
            AvroValue::TimestampMicros(123_000),
            json!("1970-01-01T00:00:00.123000000+0000"),
        ),
        (
            "date_time_utc",
            AvroValue::TimestampNanos(123_000_000),
            json!("1970-01-01T00:00:00.123000000+0000"),
        ),
        (
            "date_time_naive",
            AvroValue::Date(1),
            json!("1970-01-02T00:00:00.000000000"),
        ),
        (
            "date_time_naive",
            AvroValue::LocalTimestampMillis(123),
            json!("1970-01-01T00:00:00.123000000"),
        ),
        (
            "date_time_naive",
            AvroValue::LocalTimestampMicros(123_000),
            json!("1970-01-01T00:00:00.123000000"),
        ),
        (
            "date_time_naive",
            AvroValue::LocalTimestampNanos(123_000_000),
            json!("1970-01-01T00:00:00.123000000"),
        ),
        ("duration", AvroValue::Long(-123), json!(-123_000)),
        ("duration", AvroValue::TimeMicros(123), json!(123_000)),
        ("duration", AvroValue::TimeMillis(123), json!(123_000_000)),
        ("null", AvroValue::Null, JsonValue::Null),
        (
            "json",
            AvroValue::String("{\"x\":1}".into()),
            json!("{\"x\":1}"),
        ),
    ];
    for (dtype, value, expected) in leaves {
        assert_eq!(
            normalize(json!({"type": dtype}), value).unwrap(),
            expected,
            "{dtype}"
        );
    }
}

#[test]
fn nested_avro_arrays_and_errors() {
    let schema = json!({"type": "array", "n_dim": null, "items": {"type": "float"}});
    let value = AvroValue::Array(vec![AvroValue::Array(vec![
        AvroValue::Int(1),
        AvroValue::Double(2.5),
    ])]);
    assert_eq!(
        normalize(schema.clone(), value).unwrap(),
        json!({"shape": [1,2], "elements": [1,2.5]})
    );
    assert_eq!(
        normalize(schema.clone(), AvroValue::Double(2.5)).unwrap(),
        json!({"shape": [], "elements": [2.5]})
    );
    assert_eq!(
        normalize(schema.clone(), AvroValue::Array(vec![])).unwrap(),
        json!({"shape": [0], "elements": []})
    );
    for value in [
        AvroValue::Array(vec![
            AvroValue::Int(1),
            AvroValue::Array(vec![AvroValue::Int(2)]),
        ]),
        AvroValue::Array(vec![
            AvroValue::Array(vec![]),
            AvroValue::Array(vec![AvroValue::Int(1)]),
        ]),
        AvroValue::Array(vec![AvroValue::TimestampMillis(1)]),
        AvroValue::Array(vec![AvroValue::Boolean(true)]),
        AvroValue::Array(vec![AvroValue::Double(f64::INFINITY)]),
    ] {
        assert!(normalize(schema.clone(), value)
            .unwrap_err()
            .contains("/one/two/three"));
    }
    let schema = json!({"type": "array", "n_dim": 2, "items": {"type": "int"}});
    assert!(normalize(schema, AvroValue::Array(vec![AvroValue::Int(1)])).is_err());
    let schema = json!({"type": "array", "n_dim": 1, "items": {"type": "int"}});
    assert!(normalize(schema, AvroValue::Array(vec![AvroValue::Double(1.5)])).is_err());
}

#[test]
fn nested_avro_records_maps_unions_and_defaults() {
    let schema = json!({"type": "object", "fields": [
        {"name": "payload", "schema": {"type": "list", "items": {"type": "optional", "wrapped": object("data", json!({"type":"bytes"}))}}},
        {"name": "fallback", "schema": {"type": "bytes"}, "default": {"value": "AQI="}}
    ]});
    let values = vec![
        (
            "payload".into(),
            AvroValue::Array(vec![
                AvroValue::Union(0, Box::new(AvroValue::Null)),
                AvroValue::Union(
                    1,
                    Box::new(AvroValue::Record(vec![(
                        "data".into(),
                        AvroValue::Bytes(vec![3]),
                    )])),
                ),
            ]),
        ),
        ("extra".into(), AvroValue::TimestampMillis(123)),
    ];
    let expected = json!({"payload": [null, {"data":"Aw=="}], "extra":123, "fallback":"AQI="});
    for value in [
        AvroValue::Record(values.clone()),
        AvroValue::Map(values.into_iter().collect()),
    ] {
        assert_eq!(normalize(schema.clone(), value).unwrap(), expected);
    }
    let schema = json!({"type":"tuple","items":[{"type":"bytes"},{"type":"int"}]});
    assert_eq!(
        normalize(
            schema.clone(),
            AvroValue::Array(vec![AvroValue::Bytes(vec![1]), AvroValue::Int(2)])
        )
        .unwrap(),
        json!(["AQ==", 2])
    );
    assert!(normalize(schema, AvroValue::Array(vec![]))
        .unwrap_err()
        .contains("length 2"));
}

#[test]
fn nested_avro_errors_name_paths_and_reject_invalid_encodings() {
    let schema = object(
        "t/wo~",
        json!({"type": "list", "items": object("three", json!({"type":"float"}))}),
    );
    let value = AvroValue::Record(vec![(
        "t/wo~".into(),
        AvroValue::Array(vec![AvroValue::Record(vec![(
            "three".into(),
            AvroValue::String("bad".into()),
        )])]),
    )]);
    assert!(normalize(schema, value)
        .unwrap_err()
        .contains("/one/two/three/t~1wo~0/0/three"));
    let optional = json!({"type":"optional", "wrapped":{"type":"float"}});
    for number in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        assert!(normalize(optional.clone(), AvroValue::Double(number)).is_err());
    }
    for (dtype, value) in [
        ("date_time_utc", AvroValue::TimestampMillis(i64::MAX)),
        ("duration", AvroValue::Long(i64::MAX)),
        ("bytes", AvroValue::String("AQI=".into())),
        ("py_object_wrapper", AvroValue::Bytes(vec![255])),
        ("int", AvroValue::Boolean(true)),
    ] {
        assert!(normalize(json!({"type": dtype}), value)
            .unwrap_err()
            .contains("/one/two/three"));
    }
    let schema = object("missing", optional);
    assert!(normalize(schema, AvroValue::Record(vec![]))
        .unwrap_err()
        .contains("/missing"));
}

#[test]
fn nested_avro_parser_distinguishes_json_text_from_union_strings() {
    let nested = object("two", json!({"type":"bytes"}));
    for avro_type in [json!("string"), json!(["null", "string"])] {
        let value = AvroValue::String("{\"two\":\"AQI=\"}".into());
        let value = if avro_type.is_array() {
            AvroValue::Union(1, Box::new(value))
        } else {
            value
        };
        let schema =
            json!({"type":"record", "name":"Row", "fields":[{"name":"one", "type":avro_type}]});
        assert_eq!(
            parse(
                &schema,
                AvroValue::Record(vec![("one".into(), value)]),
                field(nested.clone(), false, None)
            )
            .unwrap(),
            Value::from(json!({"two":"AQI="}))
        );
    }
    let schema =
        json!({"type":"record", "name":"Row", "fields":[{"name":"one", "type":["string","long"]}]});
    let value = AvroValue::Record(vec![(
        "one".into(),
        AvroValue::Union(0, Box::new(AvroValue::String("{\"two\":\"AQI=\"}".into()))),
    )]);
    assert!(parse(&schema, value, field(nested, false, None))
        .unwrap_err()
        .contains("/one"));
}

#[test]
fn nested_avro_parser_missing_columns_nulls_and_native_records() {
    let nested = object("two", json!({"type":"bytes"}));
    let absent = json!({"type":"record","name":"Row","fields":[]});
    let default = Value::from(json!({"two":"AQI="}));
    for optional in [false, true] {
        let validation = if optional {
            json!({"type":"optional","wrapped":nested})
        } else {
            nested.clone()
        };
        assert_eq!(
            parse(
                &absent,
                AvroValue::Record(vec![]),
                field(validation.clone(), optional, Some(default.clone()))
            )
            .unwrap(),
            default
        );
        assert!(parse(
            &absent,
            AvroValue::Record(vec![]),
            field(validation.clone(), optional, None)
        )
        .unwrap_err()
        .contains("/one"));
        let schema = json!({"type":"record","name":"Row","fields":[{"name":"one","type":"null"}]});
        let null = parse(
            &schema,
            AvroValue::Record(vec![("one".into(), AvroValue::Null)]),
            field(validation, optional, Some(default.clone())),
        );
        if optional {
            assert_eq!(null.unwrap(), Value::None);
        } else {
            assert!(null.is_err());
        }
    }
    let schema = json!({"type":"record","name":"Row","fields":[{"name":"one","type":{"type":"record","name":"Inner","fields":[{"name":"two","type":"bytes"}]}}]});
    assert_eq!(
        parse(
            &schema,
            AvroValue::Record(vec![(
                "one".into(),
                AvroValue::Record(vec![("two".into(), AvroValue::Bytes(vec![1, 2]))])
            )]),
            field(nested, false, None)
        )
        .unwrap(),
        default
    );
}

/// A parser-only load check; invoke explicitly with --ignored --nocapture.
#[test]
#[ignore = "processes over 1 GiB per mode"]
fn nested_avro_parser_load() {
    let schema = Arc::new(AvroSchema::parse(&json!({"type":"record","name":"Row","fields":[
        {"name":"one","type":{"type":"record","name":"Inner","fields":[{"name":"two","type":"bytes"}]}}
    ]})).unwrap());
    let expected = vec![42_u8; 1024];
    let payload = GenericDatumWriter::builder(&schema)
        .build()
        .unwrap()
        .write_value_to_vec(AvroValue::Record(vec![(
            "one".into(),
            AvroValue::Record(vec![("two".into(), AvroValue::Bytes(expected.clone()))]),
        )]))
        .unwrap();
    let rows = (1_usize << 30).div_ceil(payload.len());
    let expected = Value::Bytes(expected.into());
    for nested in [false, true] {
        let mut field = field(object("two", json!({"type":"bytes"})), false, None);
        if !nested {
            field.json_schema = None;
        }
        let mut parser = AvroParser::new(vec![field], None, None, true).unwrap();
        parser.reader_schema = schema.clone();
        let context = ReaderContext::RawBytes(DataEventType::Insert, payload.clone());
        let started = std::time::Instant::now();
        for _ in 0..rows {
            let mut events = parser.parse(&context).unwrap();
            let ParsedEventWithErrors::Insert((_, mut values)) = events.pop().unwrap() else {
                panic!("expected insert")
            };
            let Value::Json(json) = values.pop().unwrap().unwrap() else {
                panic!("expected JSON")
            };
            assert_eq!(
                crate::connectors::data_format::parse_value_from_json(&json["two"], &Type::Bytes)
                    .unwrap(),
                expected
            );
        }
        eprintln!(
            "nested={nested}, rows={rows}, input_bytes={}, elapsed={:?}",
            rows * payload.len(),
            started.elapsed()
        );
    }
}
