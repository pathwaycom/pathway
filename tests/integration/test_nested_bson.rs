// Copyright © 2026 Pathway

use std::sync::Arc;

use mongodb::bson::{doc, Bson, Document};
use ndarray::{arr1, arr2, ArrayD, IxDyn};
use serde_json::{json, Value as JsonValue};

use pathway_engine::connectors::data_format::bson::serialize_value_to_bson;
use pathway_engine::connectors::data_format::{
    serialize_value_to_json, BsonParser, InnerSchemaField, ParsedEventWithErrors, Parser,
};
use pathway_engine::connectors::data_storage::{DataEventType, ReaderContext};
use pathway_engine::connectors::SessionType;
use pathway_engine::engine::{DateTimeNaive, DateTimeUtc, Duration, Key, Type, Value};

fn object(name: &str, schema: JsonValue) -> JsonValue {
    json!({"type": "object", "fields": [{"name": name, "schema": schema}]})
}

fn field(schema: JsonValue, optional: bool, default: Option<Value>) -> InnerSchemaField {
    let mut field = InnerSchemaField::new(
        if optional {
            Type::Optional(Arc::new(Type::Json))
        } else {
            Type::Json
        },
        default,
    );
    field.json_schema = Some(serde_json::from_value(schema).unwrap());
    field
}

fn parse(field: InnerSchemaField, document: Document) -> Result<Value, String> {
    let mut parser = BsonParser::new(
        vec!["one".to_owned()],
        [("one".to_owned(), field)].into(),
        SessionType::Native,
    )
    .unwrap();
    let context = ReaderContext::Bson((DataEventType::Insert, "key".to_owned(), document));
    let mut events = parser.parse(&context).unwrap();
    let ParsedEventWithErrors::Insert((_, values)) = events.pop().unwrap() else {
        panic!("expected one insert");
    };
    values
        .into_iter()
        .next()
        .unwrap()
        .map_err(|error| error.to_string())
}

#[test]
fn test_nested_bson_native_and_json_text_leaves() {
    let leaves = [
        (json!({"type": "bool"}), Value::Bool(true)),
        (json!({"type": "int"}), Value::Int(i64::MAX)),
        (json!({"type": "float"}), Value::Float(1.5.into())),
        (json!({"type": "string"}), Value::from("hello π")),
        (
            json!({"type": "bytes"}),
            Value::Bytes(vec![0, 1, 255].into()),
        ),
        (json!({"type": "bytes"}), Value::Bytes(vec![].into())),
        (
            json!({"type": "date_time_naive"}),
            Value::DateTimeNaive(DateTimeNaive::new(-123_000_000)),
        ),
        (
            json!({"type": "date_time_utc"}),
            Value::DateTimeUtc(DateTimeUtc::new(123_000_000)),
        ),
        (
            json!({"type": "duration"}),
            Value::Duration(Duration::new(-123_000_000)),
        ),
        (json!({"type": "pointer"}), Value::Pointer(Key::random())),
        (
            json!({"type": "array", "n_dim": 1, "items": {"type": "int"}}),
            Value::from(arr1(&[1, -2, 3]).into_dyn()),
        ),
        (
            json!({"type": "array", "n_dim": 2, "items": {"type": "float"}}),
            Value::from(arr2(&[[1.0, 2.5], [-3.0, 4.0]]).into_dyn()),
        ),
        (
            json!({"type": "array", "n_dim": 1, "items": {"type": "int"}}),
            Value::from(ArrayD::<i64>::zeros(IxDyn(&[0]))),
        ),
        (
            json!({"type": "array", "n_dim": 0, "items": {"type": "float"}}),
            Value::from(ArrayD::from_elem(IxDyn(&[]), 1.5)),
        ),
        (
            json!({"type": "array", "n_dim": null, "items": {"type": "int"}}),
            Value::from(ArrayD::from_elem(IxDyn(&[1, 2, 3]), 42_i64)),
        ),
    ];
    for (leaf, value) in leaves {
        let schema = object("two", object("three", leaf));
        let expected = json!({"two": {"three": serialize_value_to_json(&value).unwrap()}});
        for input in [
            Bson::Document(doc! {"two": {"three": serialize_value_to_bson(&value).unwrap()}}),
            Bson::String(expected.to_string()),
        ] {
            let actual = parse(field(schema.clone(), false, None), doc! {"one": input}).unwrap();
            assert_eq!(actual, Value::from(expected.clone()), "{schema}");
        }
    }
}

#[test]
fn test_nested_bson_defaults_nulls_and_extra_fields() {
    let schema = json!({"type": "object", "fields": [
        {"name": "two", "schema": {"type": "bytes"}, "default": {"value": "AQI="}},
        {"name": "nullable", "schema": {"type": "optional", "wrapped": {"type": "int"}}}
    ]});
    let actual = parse(
        field(schema.clone(), false, None),
        doc! {"one": {"nullable": null, "extra": [1, "x"]}},
    )
    .unwrap();
    let Value::Json(actual) = actual else {
        panic!("expected JSON");
    };
    assert_eq!(
        *actual,
        json!({"two": "AQI=", "nullable": null, "extra": [1, "x"]})
    );
    assert!(parse(field(schema.clone(), false, None), doc! {"one": {}})
        .unwrap_err()
        .contains("/one/nullable"));
    assert!(parse(
        field(schema.clone(), false, None),
        doc! {"one": {"two": null, "nullable": null}}
    )
    .unwrap_err()
    .contains("/one/two"));
    let default = Value::from(json!({"two": "AQI=", "nullable": null}));
    for optional in [false, true] {
        let schema = if optional {
            json!({"type": "optional", "wrapped": schema})
        } else {
            schema.clone()
        };
        assert_eq!(
            parse(
                field(schema.clone(), optional, Some(default.clone())),
                doc! {}
            )
            .unwrap(),
            default
        );
        assert!(parse(field(schema.clone(), optional, None), doc! {})
            .unwrap_err()
            .contains("/one"));
        let null = parse(
            field(schema, optional, Some(default.clone())),
            doc! {"one": null},
        );
        if optional {
            assert_eq!(null.unwrap(), Value::None);
        } else {
            assert!(null.is_err());
        }
    }
}

#[test]
fn test_nested_bson_lists_tuples_and_error_paths() {
    let schema = object(
        "two",
        json!({"type": "list", "items": object("three", json!({"type": "float"}))}),
    );
    let error = parse(
        field(schema, false, None),
        doc! {"one": {"two": [{"three": "bad"}]}},
    )
    .unwrap_err();
    assert!(error.contains("/one/two/0/three"), "{error}");
    assert!(error.contains("expected float"), "{error}");
    let schema = json!({"type": "tuple", "items": [object("three", json!({"type": "float"})), {"type": "optional", "wrapped": {"type": "string"}}]});
    assert_eq!(
        parse(
            field(schema.clone(), false, None),
            doc! {"one": [{"three": 1}, null]}
        )
        .unwrap(),
        Value::from(json!([{"three": 1.0}, null]))
    );
    assert!(parse(field(schema, false, None), doc! {"one": []})
        .unwrap_err()
        .contains("length 2"));
    let schema = object("t/wo~", json!({"type": "int"}));
    assert!(
        parse(field(schema, false, None), doc! {"one": {"t/wo~": "bad"}})
            .unwrap_err()
            .contains("/one/t~1wo~0")
    );
}

#[test]
fn test_nested_bson_invalid_arrays_and_encodings() {
    let optional_float = object(
        "two",
        json!({"type": "optional", "wrapped": {"type": "float"}}),
    );
    let float_array = object(
        "two",
        json!({"type": "array", "n_dim": 1, "items": {"type": "float"}}),
    );
    for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        assert!(parse(
            field(optional_float.clone(), false, None),
            doc! {"one": {"two": value}}
        )
        .unwrap_err()
        .contains("/one/two"));
        assert!(parse(
            field(float_array.clone(), false, None),
            doc! {"one": {"two": [value]}}
        )
        .unwrap_err()
        .contains("/one/two"));
    }
    let schema = object(
        "two",
        json!({"type": "array", "n_dim": null, "items": {"type": "int"}}),
    );
    for array in [
        mongodb::bson::bson!([[1], [2, 3]]),
        mongodb::bson::bson!([1, [2]]),
        mongodb::bson::bson!([[1], 2]),
        mongodb::bson::bson!([1, "bad"]),
    ] {
        let error = parse(
            field(schema.clone(), false, None),
            doc! {"one": {"two": array}},
        )
        .unwrap_err();
        assert!(error.contains("/one/two"), "{error}");
    }
    let schema = object(
        "two",
        json!({"type": "array", "n_dim": 2, "items": {"type": "int"}}),
    );
    assert!(parse(field(schema, false, None), doc! {"one": {"two": [1, 2]}}).is_err());
    for (leaf, input) in [
        ("bytes", Bson::String("AQI=".into())),
        ("float", Bson::Double(f64::INFINITY)),
        ("int", Bson::Boolean(true)),
        (
            "py_object_wrapper",
            Bson::Binary(mongodb::bson::Binary {
                subtype: mongodb::bson::spec::BinarySubtype::Generic,
                bytes: vec![255],
            }),
        ),
    ] {
        let schema = object("two", json!({"type": leaf}));
        assert!(
            parse(field(schema, false, None), doc! {"one": {"two": input}})
                .unwrap_err()
                .contains("/one/two")
        );
    }
    let schema = object("two", json!({"type": "bytes"}));
    for text in ["{", r#"{"two":"%%%"}"#] {
        assert!(
            parse(field(schema.clone(), false, None), doc! {"one": text})
                .unwrap_err()
                .contains("/one")
        );
    }
}
