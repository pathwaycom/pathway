// Copyright © 2024 Pathway

use crate::helpers::ErrorPlacement;

use super::helpers::{assert_error_shown, read_data_from_reader};

use std::collections::HashMap;

use std::sync::Arc;

use pathway_engine::connectors::data_format::{JsonLinesParser, ParsedEvent};
use pathway_engine::connectors::data_storage::{ConnectorMode, FilesystemReader, ReadMethod};
use pathway_engine::connectors::SessionType;
use pathway_engine::engine::Value;

#[test]
fn test_jsonlines_ok() -> eyre::Result<()> {
    let reader = FilesystemReader::new(
        "tests/data/jsonlines.txt",
        ConnectorMode::Static,
        None,
        ReadMethod::ByLine,
        "*",
    )?;
    let parser = JsonLinesParser::new(
        Some(vec!["a".to_string()]),
        vec!["b".to_string(), "c".to_string()],
        HashMap::new(),
        true,
        HashMap::new(),
        SessionType::Native,
    );

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
    let reader = FilesystemReader::new(
        "tests/data/jsonlines.txt",
        ConnectorMode::Static,
        None,
        ReadMethod::ByLine,
        "*",
    )?;
    let parser = JsonLinesParser::new(
        Some(vec!["a".to_string(), "d".to_string()]),
        vec!["b".to_string(), "c".to_string()],
        HashMap::new(),
        true,
        HashMap::new(),
        SessionType::Native,
    );

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
    let reader = FilesystemReader::new(
        "tests/data/jsonlines.txt",
        ConnectorMode::Static,
        None,
        ReadMethod::ByLine,
        "*",
    )?;
    let parser = JsonLinesParser::new(
        Some(vec!["a".to_string(), "d".to_string()]),
        vec!["b".to_string(), "c".to_string()],
        HashMap::new(),
        false,
        HashMap::new(),
        SessionType::Native,
    );

    let entries = read_data_from_reader(Box::new(reader), Box::new(parser))?;
    assert_eq!(entries.len(), 4);

    Ok(())
}

#[test]
fn test_jsonlines_incorrect_values() -> eyre::Result<()> {
    let reader = FilesystemReader::new(
        "tests/data/jsonlines.txt",
        ConnectorMode::Static,
        None,
        ReadMethod::ByLine,
        "*",
    )?;
    let parser = JsonLinesParser::new(
        Some(vec!["a".to_string()]),
        vec!["b".to_string(), "qqq".to_string()],
        HashMap::new(),
        true,
        HashMap::new(),
        SessionType::Native,
    );

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
    let reader = FilesystemReader::new(
        "tests/data/jsonlines_types.txt",
        ConnectorMode::Static,
        None,
        ReadMethod::ByLine,
        "*",
    )?;
    let parser = JsonLinesParser::new(
        Some(vec!["a".to_string()]),
        vec![
            "float".to_string(),
            "int_positive".to_string(),
            "int_negative".to_string(),
            "string".to_string(),
            "array".to_string(),
            "bool_true".to_string(),
            "bool_false".to_string(),
        ],
        HashMap::new(),
        true,
        HashMap::new(),
        SessionType::Native,
    );

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
    let reader = FilesystemReader::new(
        "tests/data/json_complex_paths.txt",
        ConnectorMode::Static,
        None,
        ReadMethod::ByLine,
        "*",
    )?;

    let mut routes = HashMap::new();
    routes.insert("owner".to_string(), "/name".to_string());
    routes.insert("pet_kind".to_string(), "/pet/animal".to_string());
    routes.insert("pet_name".to_string(), "/pet/name".to_string());
    routes.insert("pet_height".to_string(), "/pet/measurements/1".to_string());

    let parser = JsonLinesParser::new(
        None,
        vec![
            "owner".to_string(),
            "pet_kind".to_string(),
            "pet_name".to_string(),
            "pet_height".to_string(),
        ],
        routes,
        true,
        HashMap::new(),
        SessionType::Native,
    );

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
    let reader = FilesystemReader::new(
        "tests/data/json_complex_paths.txt",
        ConnectorMode::Static,
        None,
        ReadMethod::ByLine,
        "*",
    )?;

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
            "owner".to_string(),
            "pet_kind".to_string(),
            "pet_name".to_string(),
            "pet_height".to_string(),
        ],
        routes,
        true,
        HashMap::new(),
        SessionType::Native,
    );

    assert_error_shown(
        Box::new(reader),
        Box::new(parser),
        r#"field pet_height with path /pet/measurements/height is absent in {"name":"John","pet":{"animal":"dog","measurements":[200,400,600],"name":"Alice"}}"#,
        ErrorPlacement::Value(3),
    );

    Ok(())
}

#[test]
fn test_jsonlines_complex_path_ignore_errors() -> eyre::Result<()> {
    let reader = FilesystemReader::new(
        "tests/data/json_complex_paths.txt",
        ConnectorMode::Static,
        None,
        ReadMethod::ByLine,
        "*",
    )?;

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
            "owner".to_string(),
            "pet_kind".to_string(),
            "pet_name".to_string(),
            "pet_height".to_string(),
        ],
        routes,
        false,
        HashMap::new(),
        SessionType::Native,
    );

    let entries = read_data_from_reader(Box::new(reader), Box::new(parser))?;
    assert_eq!(entries.len(), 2);

    Ok(())
}

#[test]
fn test_jsonlines_incorrect_key_verbose_error() -> eyre::Result<()> {
    let reader = FilesystemReader::new(
        "tests/data/jsonlines.txt",
        ConnectorMode::Static,
        None,
        ReadMethod::ByLine,
        "*",
    )?;
    let parser = JsonLinesParser::new(
        Some(vec!["a".to_string(), "d".to_string()]),
        vec!["b".to_string(), "c".to_string()],
        HashMap::new(),
        true,
        HashMap::new(),
        SessionType::Native,
    );

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

    let reader = FilesystemReader::new(
        "tests/data/jsonlines.txt",
        ConnectorMode::Static,
        None,
        ReadMethod::ByLine,
        "*",
    )?;
    let parser = JsonLinesParser::new(
        Some(vec!["a".to_string(), "d".to_string()]),
        vec!["b".to_string(), "c".to_string()],
        routes,
        true,
        HashMap::new(),
        SessionType::Native,
    );

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
    let reader = FilesystemReader::new(
        "tests/data/json_complex_paths.txt",
        ConnectorMode::Static,
        None,
        ReadMethod::ByLine,
        "*",
    )?;
    let parser = JsonLinesParser::new(
        None,
        vec!["pet".to_string()],
        HashMap::new(),
        true,
        HashMap::new(),
        SessionType::Native,
    );

    assert_error_shown(
        Box::new(reader),
        Box::new(parser),
        r#"failed to create a field "pet" with type Any from the following json payload: {"animal":"dog","measurements":[200,400,600],"name":"Alice"}"#,
        ErrorPlacement::Value(0),
    );

    Ok(())
}
