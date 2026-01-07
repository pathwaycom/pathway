// Copyright © 2026 Pathway

use super::helpers::{
    assert_error_shown, assert_error_shown_for_reader_context, new_filesystem_reader,
    read_data_from_reader, value_field, ErrorPlacement,
};

use std::collections::HashMap;
use std::sync::Arc;

use pathway_engine::connectors::data_format::{
    InnerSchemaField, JsonLinesParser, ParsedEvent, Parser,
};
use pathway_engine::connectors::data_storage::{
    ConnectorMode, DataEventType, ReadMethod, ReaderContext,
};
use pathway_engine::connectors::SessionType;
use pathway_engine::engine::{DateTimeNaive, DateTimeUtc, Type, Value};

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
