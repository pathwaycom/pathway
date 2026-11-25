// Copyright © 2026 Pathway

use super::helpers::{
    assert_error_shown, assert_error_shown_for_reader_context, new_filesystem_reader,
    read_data_from_reader, value_field, ErrorPlacement,
};

use std::collections::HashMap;
use std::sync::Arc;

use pathway_engine::connectors::data_format::{
    json_schema::JsonSchema, FieldSource, InnerSchemaField, JsonLinesParser, ParsedEvent,
    ParsedEventWithErrors, Parser,
};
use pathway_engine::connectors::data_storage::{
    ConnectorMode, DataEventType, ReadMethod, ReaderContext,
};
use pathway_engine::connectors::SessionType;
use pathway_engine::engine::{DateTimeNaive, DateTimeUtc, Type, Value};
use pathway_engine::python_api::ValueField;

#[test]
fn test_nested_json_validation_in_key_payload_and_pointer_paths() -> eyre::Result<()> {
    let validation: JsonSchema = serde_json::from_value(serde_json::json!({
        "type": "object", "fields": [
            {"name": "value", "schema": {"type": "float"}},
            {"name": "currency", "schema": {"type": "string"},
             "default": {"value": "USD"}}
        ]
    }))?;
    for from_key in [false, true] {
        for use_path in [false, true] {
            let mut field = InnerSchemaField::new(Type::Json, None);
            field.json_schema = Some(validation.clone());
            let paths = if use_path {
                HashMap::from([("price".to_owned(), "/wrapper/price".to_owned())])
            } else {
                HashMap::new()
            };
            let mut parser = JsonLinesParser::new(
                None,
                vec![ValueField::new(
                    "price".to_owned(),
                    Type::Json,
                    if from_key {
                        FieldSource::Key
                    } else {
                        FieldSource::Payload
                    },
                )],
                paths,
                true,
                HashMap::from([("price".to_owned(), field)]),
                SessionType::Native,
                None,
            )?;
            for (input, valid) in [
                (serde_json::json!({"value": 12, "extra": true}), true),
                (serde_json::json!({"value": "12"}), false),
                (serde_json::json!({"value": true}), false),
                (serde_json::json!({"value": 12, "currency": null}), false),
                (serde_json::json!({}), false),
                (serde_json::json!(null), false),
                (serde_json::json!([]), false),
            ] {
                let mut record = serde_json::json!({"price": input});
                if use_path {
                    record = serde_json::json!({"wrapper": record});
                }
                let data = serde_json::to_vec(&record)?;
                let context = if from_key {
                    ReaderContext::KeyValue((Some(data), None))
                } else {
                    ReaderContext::from_raw_bytes(DataEventType::Insert, data)
                };
                let events = parser
                    .parse(&context)
                    .expect("valid JSON should produce an insert event");
                let [ParsedEventWithErrors::Insert((_, values))] = events.as_slice() else {
                    panic!("expected one insert event");
                };
                if valid {
                    let Value::Json(actual) = values[0].as_ref().unwrap() else {
                        panic!("expected a JSON value");
                    };
                    assert_eq!(
                        **actual,
                        serde_json::json!({"value": 12, "currency": "USD", "extra": true})
                    );
                } else {
                    let error = values[0].as_ref().unwrap_err().to_string();
                    assert!(error.contains("/price"), "{error}");
                }
            }
        }
    }
    Ok(())
}

#[test]
fn test_nested_json_validation_reports_array_index_and_escaped_field() -> eyre::Result<()> {
    let schema: JsonSchema = serde_json::from_value(serde_json::json!({
        "type": "object", "fields": [
            {"name": "a/b~c", "schema": {"type": "list", "items": {
                "type": "object", "fields": [
                    {"name": "value", "schema": {"type": "int"}}
                ]
            }}}
        ]
    }))?;
    let mut input = serde_json::json!({"a/b~c": [{"value": 1}, {"value": "bad"}]});
    let error = schema
        .validate(&mut input, "/data")
        .unwrap_err()
        .to_string();
    assert!(error.contains("/data/a~1b~0c/1/value"), "{error}");
    Ok(())
}

#[test]
fn test_nested_json_encoded_leaves_validate_without_replacing_json() -> eyre::Result<()> {
    use serde_json::json;

    for (leaf, valid, invalid) in [
        (json!({"type": "bytes"}), json!("AQI="), json!("%%%")),
        (
            json!({"type": "date_time_naive"}),
            json!("2026-11-04T12:00:00.123456789"),
            json!("bad"),
        ),
        (
            json!({"type": "date_time_utc"}),
            json!("2026-11-04T12:00:00+00:00"),
            json!("2026-11-04T12:00:00"),
        ),
        (json!({"type": "duration"}), json!(123456789), json!(1.5)),
        (
            json!({"type": "pointer"}),
            json!(pathway_engine::engine::Key::from_impl(42).to_string()),
            json!("bad"),
        ),
        (
            json!({"type": "array", "n_dim": 1, "items": {"type": "int"}}),
            json!({"shape": [2], "elements": [1, 2]}),
            json!({"shape": [2], "elements": [1]}),
        ),
        (
            json!({"type": "array", "n_dim": 2, "items": {"type": "float"}}),
            json!({"shape": [1, 2], "elements": [1, 2.5]}),
            json!({"shape": [2], "elements": [1, 2.5]}),
        ),
    ] {
        let schema: JsonSchema = serde_json::from_value(json!({
            "type": "object", "fields": [{"name": "two", "schema": {
                "type": "list", "items": {"type": "object", "fields": [
                    {"name": "three", "schema": leaf}
                ]}
            }}]
        }))?;
        let mut value = json!({"two": [{"three": valid}], "extra": true});
        let original = value.clone();
        schema.validate(&mut value, "/one")?;
        assert_eq!(value, original);
        value["two"][0]["three"] = invalid;
        let error = schema.validate(&mut value, "/one").unwrap_err().to_string();
        assert!(error.contains("/one/two/0/three"), "{error}");
    }
    Ok(())
}

#[test]
fn test_json_array_decoder_rejects_malformed_arrays() {
    use pathway_engine::connectors::data_format::parse_value_from_json;
    use serde_json::json;

    let dtype = Type::Array(Some(1), Arc::new(Type::Int));
    for value in [
        json!({}),
        json!({"shape": [1]}),
        json!({"elements": [1]}),
        json!({"shape": [1], "elements": null}),
        json!({"shape": null, "elements": [1]}),
        json!({"shape": [-1], "elements": []}),
        json!({"shape": [1, 1], "elements": [1]}),
        json!({"shape": [2], "elements": [1]}),
        json!({"shape": [1], "elements": [1.5]}),
    ] {
        assert!(parse_value_from_json(&value, &dtype).is_none(), "{value}");
    }
    assert!(parse_value_from_json(
        &json!({"shape": [1], "elements": [1]}),
        &Type::Array(None, Arc::new(Type::Any)),
    )
    .is_none());
}

fn generate_deeply_nested_json(depth: usize) -> String {
    let mut raw_nested_json = String::with_capacity(depth * 8);
    for _ in 0..depth {
        raw_nested_json.push_str("{\"a\":");
    }
    raw_nested_json.push('0');
    for _ in 0..depth {
        raw_nested_json.push('}');
    }
    raw_nested_json
}

#[test]
fn test_jsonlines_ok() -> eyre::Result<()> {
    let reader = new_filesystem_reader(
        "tests/data/jsonlines.txt",
        ConnectorMode::Static,
        ReadMethod::ByLine,
        "*",
        false,
    )?;
    let schema = [
        ("a".to_string(), InnerSchemaField::new(Type::String, None)),
        ("b".to_string(), InnerSchemaField::new(Type::Int, None)),
        ("c".to_string(), InnerSchemaField::new(Type::Int, None)),
    ];
    let parser = JsonLinesParser::new(
        Some(&["a".to_string()]),
        vec![value_field("b"), value_field("c")],
        HashMap::new(),
        true,
        schema.into(),
        SessionType::Native,
        None,
    )?;

    let entries = read_data_from_reader(Box::new(reader), Box::new(parser))?;

    let expected_values = vec![
        ParsedEvent::Insert((
            Some(vec![Value::from("abc")]),
            vec![Value::Int(7), Value::Int(15)],
        )),
        ParsedEvent::Insert((
            Some(vec![Value::from("def")]),
            vec![Value::Int(1), Value::Int(3)],
        )),
        ParsedEvent::Insert((
            Some(vec![Value::from("ghi")]),
            vec![Value::Int(2), Value::Int(4)],
        )),
        ParsedEvent::AdvanceTime,
    ];
    assert_eq!(entries, expected_values);

    Ok(())
}

#[test]
fn test_jsonlines_incorrect_key() -> eyre::Result<()> {
    let reader = new_filesystem_reader(
        "tests/data/jsonlines.txt",
        ConnectorMode::Static,
        ReadMethod::ByLine,
        "*",
        false,
    )?;
    let schema = [
        ("a".to_string(), InnerSchemaField::new(Type::String, None)),
        ("b".to_string(), InnerSchemaField::new(Type::Int, None)),
        ("c".to_string(), InnerSchemaField::new(Type::Int, None)),
        ("d".to_string(), InnerSchemaField::new(Type::Int, None)),
    ];
    let parser = JsonLinesParser::new(
        Some(&["a".to_string(), "d".to_string()]),
        vec![value_field("b"), value_field("c")],
        HashMap::new(),
        true,
        schema.into(),
        SessionType::Native,
        None,
    )?;

    assert_error_shown(
        Box::new(reader),
        Box::new(parser),
        r#"field d with no JsonPointer path specified is absent in {"a":"abc","b":7,"c":15}"#,
        ErrorPlacement::Key,
    );

    Ok(())
}

#[test]
fn test_jsonlines_incomplete_key_to_null() -> eyre::Result<()> {
    let reader = new_filesystem_reader(
        "tests/data/jsonlines.txt",
        ConnectorMode::Static,
        ReadMethod::ByLine,
        "*",
        false,
    )?;
    let schema = [
        ("a".to_string(), InnerSchemaField::new(Type::String, None)),
        ("b".to_string(), InnerSchemaField::new(Type::Int, None)),
        ("c".to_string(), InnerSchemaField::new(Type::Int, None)),
        ("d".to_string(), InnerSchemaField::new(Type::Int, None)),
    ];
    let parser = JsonLinesParser::new(
        Some(&["a".to_string(), "d".to_string()]),
        vec![value_field("b"), value_field("c")],
        HashMap::new(),
        false,
        schema.into(),
        SessionType::Native,
        None,
    )?;

    let entries = read_data_from_reader(Box::new(reader), Box::new(parser))?;
    assert_eq!(entries.len(), 4);

    Ok(())
}

#[test]
fn test_jsonlines_incorrect_values() -> eyre::Result<()> {
    let reader = new_filesystem_reader(
        "tests/data/jsonlines.txt",
        ConnectorMode::Static,
        ReadMethod::ByLine,
        "*",
        false,
    )?;
    let schema = [
        ("a".to_string(), InnerSchemaField::new(Type::String, None)),
        ("b".to_string(), InnerSchemaField::new(Type::Int, None)),
        ("qqq".to_string(), InnerSchemaField::new(Type::Int, None)),
    ];
    let parser = JsonLinesParser::new(
        Some(&["a".to_string()]),
        vec![value_field("b"), value_field("qqq")],
        HashMap::new(),
        true,
        schema.into(),
        SessionType::Native,
        None,
    )?;

    assert_error_shown(
        Box::new(reader),
        Box::new(parser),
        r#"field qqq with no JsonPointer path specified is absent in {"a":"abc","b":7,"c":15}"#,
        ErrorPlacement::Value(1),
    );

    Ok(())
}

#[test]
fn test_jsonlines_types_parsing() -> eyre::Result<()> {
    let reader = new_filesystem_reader(
        "tests/data/jsonlines_types.txt",
        ConnectorMode::Static,
        ReadMethod::ByLine,
        "*",
        false,
    )?;
    let schema = [
        ("a".to_string(), InnerSchemaField::new(Type::String, None)),
        (
            "float".to_string(),
            InnerSchemaField::new(Type::Float, None),
        ),
        (
            "int_positive".to_string(),
            InnerSchemaField::new(Type::Int, None),
        ),
        (
            "int_negative".to_string(),
            InnerSchemaField::new(Type::Int, None),
        ),
        (
            "string".to_string(),
            InnerSchemaField::new(Type::String, None),
        ),
        (
            "array".to_string(),
            InnerSchemaField::new(
                Type::Tuple(
                    [
                        Type::String,
                        Type::Int,
                        Type::Int,
                        Type::Float,
                        Type::Tuple([].into()),
                    ]
                    .into(),
                ),
                None,
            ),
        ),
        (
            "bool_true".to_string(),
            InnerSchemaField::new(Type::Bool, None),
        ),
        (
            "bool_false".to_string(),
            InnerSchemaField::new(Type::Bool, None),
        ),
    ];
    let parser = JsonLinesParser::new(
        Some(&["a".to_string()]),
        vec![
            value_field("float"),
            value_field("int_positive"),
            value_field("int_negative"),
            value_field("string"),
            value_field("array"),
            value_field("bool_true"),
            value_field("bool_false"),
        ],
        HashMap::new(),
        true,
        schema.into(),
        SessionType::Native,
        None,
    )?;

    let entries = read_data_from_reader(Box::new(reader), Box::new(parser))?;

    let expected_values = vec![ParsedEvent::Insert((
        Some(vec![Value::from("abc")]),
        vec![
            Value::Float(1.23.into()),
            Value::Int(50),
            Value::Int(-60),
            Value::from("hello"),
            Value::Tuple(Arc::new([
                Value::from("world"),
                Value::Int(1),
                Value::Int(-4),
                Value::Float(7.38.into()),
                Value::Tuple(Arc::new([])),
            ])),
            Value::Bool(true),
            Value::Bool(false),
        ],
    ))];
    assert_eq!(entries, expected_values);

    Ok(())
}

#[test]
fn test_jsonlines_complex_paths() -> eyre::Result<()> {
    let reader = new_filesystem_reader(
        "tests/data/json_complex_paths.txt",
        ConnectorMode::Static,
        ReadMethod::ByLine,
        "*",
        false,
    )?;

    let mut routes = HashMap::new();
    routes.insert("owner".to_string(), "/name".to_string());
    routes.insert("pet_kind".to_string(), "/pet/animal".to_string());
    routes.insert("pet_name".to_string(), "/pet/name".to_string());
    routes.insert("pet_height".to_string(), "/pet/measurements/1".to_string());

    let schema = [
        (
            "owner".to_string(),
            InnerSchemaField::new(Type::String, None),
        ),
        (
            "pet_kind".to_string(),
            InnerSchemaField::new(Type::String, None),
        ),
        (
            "pet_name".to_string(),
            InnerSchemaField::new(Type::String, None),
        ),
        (
            "pet_height".to_string(),
            InnerSchemaField::new(Type::Int, None),
        ),
    ];
    let parser = JsonLinesParser::new(
        None,
        vec![
            value_field("owner"),
            value_field("pet_kind"),
            value_field("pet_name"),
            value_field("pet_height"),
        ],
        routes,
        true,
        schema.into(),
        SessionType::Native,
        None,
    )?;

    let entries = read_data_from_reader(Box::new(reader), Box::new(parser))?;

    let expected_values = vec![
        ParsedEvent::Insert((
            None,
            vec![
                Value::String("John".into()),
                Value::String("dog".into()),
                Value::String("Alice".into()),
                Value::Int(400),
            ],
        )),
        ParsedEvent::Insert((
            None,
            vec![
                Value::String("Jack".into()),
                Value::String("cat".into()),
                Value::String("Bob".into()),
                Value::Int(200),
            ],
        )),
    ];
    assert_eq!(entries, expected_values);

    Ok(())
}

#[test]
fn test_jsonlines_complex_paths_error() -> eyre::Result<()> {
    let reader = new_filesystem_reader(
        "tests/data/json_complex_paths.txt",
        ConnectorMode::Static,
        ReadMethod::ByLine,
        "*",
        false,
    )?;

    let schema = [
        (
            "owner".to_string(),
            InnerSchemaField::new(Type::String, None),
        ),
        (
            "pet_kind".to_string(),
            InnerSchemaField::new(Type::String, None),
        ),
        (
            "pet_name".to_string(),
            InnerSchemaField::new(Type::String, None),
        ),
        (
            "pet_height".to_string(),
            InnerSchemaField::new(Type::Int, None),
        ),
    ];
    let mut routes = HashMap::new();
    routes.insert("owner".to_string(), "/name".to_string());
    routes.insert("pet_kind".to_string(), "/pet/animal".to_string());
    routes.insert("pet_name".to_string(), "/pet/name".to_string());
    routes.insert(
        "pet_height".to_string(),
        "/pet/measurements/height".to_string(),
    );

    let parser = JsonLinesParser::new(
        None,
        vec![
            value_field("owner"),
            value_field("pet_kind"),
            value_field("pet_name"),
            value_field("pet_height"),
        ],
        routes,
        true,
        schema.into(),
        SessionType::Native,
        None,
    )?;

    assert_error_shown(
        Box::new(reader),
        Box::new(parser),
        r#"field pet_height with path /pet/measurements/height is absent in {"name":"John","pet":{"animal":"dog","name":"Alice","measurements":[200,400,600]}}"#,
        ErrorPlacement::Value(3),
    );

    Ok(())
}

#[test]
fn test_jsonlines_complex_path_ignore_errors() -> eyre::Result<()> {
    let reader = new_filesystem_reader(
        "tests/data/json_complex_paths.txt",
        ConnectorMode::Static,
        ReadMethod::ByLine,
        "*",
        false,
    )?;

    let schema = [
        (
            "owner".to_string(),
            InnerSchemaField::new(Type::String, None),
        ),
        (
            "pet_kind".to_string(),
            InnerSchemaField::new(Type::String, None),
        ),
        (
            "pet_name".to_string(),
            InnerSchemaField::new(Type::String, None),
        ),
        (
            "pet_height".to_string(),
            InnerSchemaField::new(Type::Int, None),
        ),
    ];
    let mut routes = HashMap::new();
    routes.insert("owner".to_string(), "/name".to_string());
    routes.insert("pet_kind".to_string(), "/pet/animal".to_string());
    routes.insert("pet_name".to_string(), "/pet/name".to_string());
    routes.insert(
        "pet_height".to_string(),
        "/pet/measurements/height".to_string(),
    );

    let parser = JsonLinesParser::new(
        None,
        vec![
            value_field("owner"),
            value_field("pet_kind"),
            value_field("pet_name"),
            value_field("pet_height"),
        ],
        routes,
        false,
        schema.into(),
        SessionType::Native,
        None,
    )?;

    let entries = read_data_from_reader(Box::new(reader), Box::new(parser))?;
    assert_eq!(entries.len(), 2);

    Ok(())
}

#[test]
fn test_jsonlines_incorrect_key_verbose_error() -> eyre::Result<()> {
    let reader = new_filesystem_reader(
        "tests/data/jsonlines.txt",
        ConnectorMode::Static,
        ReadMethod::ByLine,
        "*",
        false,
    )?;
    let schema = [
        ("a".to_string(), InnerSchemaField::new(Type::String, None)),
        ("b".to_string(), InnerSchemaField::new(Type::Int, None)),
        ("c".to_string(), InnerSchemaField::new(Type::Int, None)),
        ("d".to_string(), InnerSchemaField::new(Type::Int, None)),
    ];
    let parser = JsonLinesParser::new(
        Some(&["a".to_string(), "d".to_string()]),
        vec![value_field("b"), value_field("c")],
        HashMap::new(),
        true,
        schema.into(),
        SessionType::Native,
        None,
    )?;

    assert_error_shown(
        Box::new(reader),
        Box::new(parser),
        r#"field d with no JsonPointer path specified is absent in {"a":"abc","b":7,"c":15}"#,
        ErrorPlacement::Key,
    );

    Ok(())
}

#[test]
fn test_jsonlines_incorrect_jsonpointer_verbose_error() -> eyre::Result<()> {
    let mut routes = HashMap::new();
    routes.insert("d".to_string(), "/non/existent/path".to_string());

    let reader = new_filesystem_reader(
        "tests/data/jsonlines.txt",
        ConnectorMode::Static,
        ReadMethod::ByLine,
        "*",
        false,
    )?;
    let schema = [
        ("a".to_string(), InnerSchemaField::new(Type::String, None)),
        ("b".to_string(), InnerSchemaField::new(Type::Int, None)),
        ("c".to_string(), InnerSchemaField::new(Type::Int, None)),
        ("d".to_string(), InnerSchemaField::new(Type::Int, None)),
    ];
    let parser = JsonLinesParser::new(
        Some(&["a".to_string(), "d".to_string()]),
        vec![value_field("b"), value_field("c")],
        routes,
        true,
        schema.into(),
        SessionType::Native,
        None,
    )?;

    assert_error_shown(
        Box::new(reader),
        Box::new(parser),
        r#"field d with path /non/existent/path is absent in {"a":"abc","b":7,"c":15}"#,
        ErrorPlacement::Key,
    );

    Ok(())
}

#[test]
fn test_jsonlines_failed_to_parse_field() -> eyre::Result<()> {
    let reader = new_filesystem_reader(
        "tests/data/json_complex_paths.txt",
        ConnectorMode::Static,
        ReadMethod::ByLine,
        "*",
        false,
    )?;
    let schema = [("pet".to_string(), InnerSchemaField::new(Type::Any, None))];
    let parser = JsonLinesParser::new(
        None,
        vec![value_field("pet")],
        HashMap::new(),
        true,
        schema.into(),
        SessionType::Native,
        None,
    )?;

    assert_error_shown(
        Box::new(reader),
        Box::new(parser),
        r#"failed to create a field "pet" with type Any from json payload: {"animal":"dog","name":"Alice","measurements":[200,400,600]}"#,
        ErrorPlacement::Value(0),
    );

    Ok(())
}

#[test]
fn test_jsonlines_timestamp() -> eyre::Result<()> {
    let reader = new_filesystem_reader(
        "tests/data/jsonlines_timestamp.txt",
        ConnectorMode::Static,
        ReadMethod::ByLine,
        "*",
        false,
    )?;
    let schema = [
        (
            "utc".to_string(),
            InnerSchemaField::new(Type::DateTimeUtc, None),
        ),
        (
            "naive".to_string(),
            InnerSchemaField::new(Type::DateTimeNaive, None),
        ),
    ];
    let parser = JsonLinesParser::new(
        None,
        vec![value_field("utc"), value_field("naive")],
        HashMap::new(),
        true,
        schema.into(),
        SessionType::Native,
        None,
    )?;

    let entries = read_data_from_reader(Box::new(reader), Box::new(parser))?;

    let expected_values = vec![
        ParsedEvent::Insert((
            None,
            vec![
                Value::DateTimeUtc(DateTimeUtc::from_timestamp(1738630923123456789, "ns")?),
                Value::DateTimeNaive(DateTimeNaive::from_timestamp(1738660087987654321, "ns")?),
            ],
        )),
        ParsedEvent::Insert((
            None,
            vec![
                Value::DateTimeUtc(DateTimeUtc::from_timestamp(1738630923123, "ms")?),
                Value::DateTimeNaive(DateTimeNaive::from_timestamp(1738660087987, "ms")?),
            ],
        )),
        ParsedEvent::Insert((
            None,
            vec![
                Value::DateTimeUtc(DateTimeUtc::from_timestamp(1738630923, "s")?),
                Value::DateTimeNaive(DateTimeNaive::from_timestamp(1738660087, "s")?),
            ],
        )),
    ];
    assert_eq!(entries, expected_values);

    Ok(())
}

#[test]
fn test_nested_levels_limit() -> eyre::Result<()> {
    let schema = [("a".to_string(), InnerSchemaField::new(Type::Json, None))];
    let mut parser = Box::new(JsonLinesParser::new(
        None,
        vec![value_field("a")],
        HashMap::new(),
        true,
        schema.into(),
        SessionType::Native,
        None,
    )?);

    let raw_json = generate_deeply_nested_json(127);
    let input_context =
        ReaderContext::from_raw_bytes(DataEventType::Insert, raw_json.as_bytes().to_vec());
    let parsed = parser.parse(&input_context);
    assert!(parsed.is_ok());

    let raw_json = generate_deeply_nested_json(128);
    let input_context =
        ReaderContext::from_raw_bytes(DataEventType::Insert, raw_json.as_bytes().to_vec());
    assert_error_shown_for_reader_context(
        &input_context,
        parser,
        "recursion limit exceeded at line 1 column 636",
        ErrorPlacement::Message,
    );

    Ok(())
}
