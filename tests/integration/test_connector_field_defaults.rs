// Copyright © 2024 Pathway

use super::helpers::{new_csv_filesystem_reader, new_filesystem_reader, read_data_from_reader};

use std::collections::HashMap;

use pathway_engine::connectors::data_format::{
    DsvParser, DsvSettings, InnerSchemaField, JsonLinesParser, ParsedEvent,
};
use pathway_engine::connectors::data_storage::{ConnectorMode, ReadMethod};
use pathway_engine::connectors::SessionType;
use pathway_engine::engine::{Type, Value};

fn get_schema_with_common_parts() -> HashMap<String, InnerSchemaField> {
    [
        (
            "seq_id".to_string(),
            InnerSchemaField::new(Type::String, None),
        ),
        ("key".to_string(), InnerSchemaField::new(Type::String, None)),
        (
            "value".to_string(),
            InnerSchemaField::new(Type::String, None),
        ),
    ]
    .into()
}

#[test]
fn test_dsv_with_default_end_of_line() -> eyre::Result<()> {
    let mut builder = csv::ReaderBuilder::new();
    builder.has_headers(false);

    let mut schema = get_schema_with_common_parts();

    schema.insert(
        "number".to_string(),
        InnerSchemaField::new(Type::Int, Some(Value::Int(42))),
    );

    let reader = new_csv_filesystem_reader(
        "tests/data/dsv_with_skips.txt",
        builder,
        ConnectorMode::Static,
        "*",
        false,
    )?;
    let parser = DsvParser::new(
        DsvSettings::new(
            Some(vec!["seq_id".to_string()]),
            vec!["key".to_string(), "value".to_string(), "number".to_string()],
            ',',
        ),
        schema,
    )?;

    let read_lines = read_data_from_reader(Box::new(reader), Box::new(parser))?;
    assert_eq!(
        read_lines,
        vec![
            ParsedEvent::Insert((
                Some(vec![Value::String("1".into())]),
                vec![
                    Value::String("some_key".into()),
                    Value::String("some_value".into()),
                    Value::Int(42)
                ]
            )),
            ParsedEvent::Insert((
                Some(vec![Value::String("2".into())]),
                vec![
                    Value::String("".into()),
                    Value::String("some_value".into()),
                    Value::Int(1)
                ]
            ))
        ]
    );

    Ok(())
}

#[test]
fn test_dsv_with_default_middle_of_line() -> eyre::Result<()> {
    let mut builder = csv::ReaderBuilder::new();
    builder.has_headers(false);

    let mut schema = get_schema_with_common_parts();

    schema.insert(
        "number".to_string(),
        InnerSchemaField::new(Type::Int, Some(Value::Int(42))),
    );

    let reader = new_csv_filesystem_reader(
        "tests/data/dsv_with_skips2.txt",
        builder,
        ConnectorMode::Static,
        "*",
        false,
    )?;
    let parser = DsvParser::new(
        DsvSettings::new(
            Some(vec!["seq_id".to_string()]),
            vec!["key".to_string(), "value".to_string(), "number".to_string()],
            ',',
        ),
        schema,
    )?;

    let read_lines = read_data_from_reader(Box::new(reader), Box::new(parser))?;
    assert_eq!(
        read_lines,
        vec![
            ParsedEvent::Insert((
                Some(vec![Value::String("1".into())]),
                vec![
                    Value::String("some_key".into()),
                    Value::String("some_value".into()),
                    Value::Int(42)
                ]
            )),
            ParsedEvent::Insert((
                Some(vec![Value::String("2".into())]),
                vec![
                    Value::String("".into()),
                    Value::String("some_value".into()),
                    Value::Int(1)
                ]
            ))
        ]
    );

    Ok(())
}

#[test]
fn test_dsv_fails_without_default() -> eyre::Result<()> {
    let mut builder = csv::ReaderBuilder::new();
    builder.has_headers(false);

    let mut schema = get_schema_with_common_parts();

    schema.insert("number".to_string(), InnerSchemaField::new(Type::Int, None));

    let reader = new_csv_filesystem_reader(
        "tests/data/dsv_with_skips.txt",
        builder,
        ConnectorMode::Static,
        "*",
        false,
    )?;
    let parser = DsvParser::new(
        DsvSettings::new(
            Some(vec!["seq_id".to_string()]),
            vec!["key".to_string(), "value".to_string(), "number".to_string()],
            ',',
        ),
        schema,
    )?;

    let read_lines = read_data_from_reader(Box::new(reader), Box::new(parser))?;
    assert_eq!(
        read_lines,
        vec![
            ParsedEvent::Insert((
                Some(vec![Value::String("1".into())]),
                vec![
                    Value::String("some_key".into()),
                    Value::String("some_value".into()),
                    Value::Error,
                ]
            )),
            ParsedEvent::Insert((
                Some(vec![Value::String("2".into())]),
                vec![
                    Value::String("".into()),
                    Value::String("some_value".into()),
                    Value::Int(1)
                ]
            ))
        ]
    );

    Ok(())
}

#[test]
fn test_dsv_with_default_nullable() -> eyre::Result<()> {
    let mut builder = csv::ReaderBuilder::new();
    builder.has_headers(false);

    let mut schema = get_schema_with_common_parts();

    schema.insert(
        "number".to_string(),
        InnerSchemaField::new(Type::Optional(Type::Int.into()), Some(Value::None)),
    );

    let reader = new_csv_filesystem_reader(
        "tests/data/dsv_with_skips.txt",
        builder,
        ConnectorMode::Static,
        "*",
        false,
    )?;
    let parser = DsvParser::new(
        DsvSettings::new(
            Some(vec!["seq_id".to_string()]),
            vec!["key".to_string(), "value".to_string(), "number".to_string()],
            ',',
        ),
        schema,
    )?;

    let read_lines = read_data_from_reader(Box::new(reader), Box::new(parser))?;
    assert_eq!(
        read_lines,
        vec![
            ParsedEvent::Insert((
                Some(vec![Value::String("1".into())]),
                vec![
                    Value::String("some_key".into()),
                    Value::String("some_value".into()),
                    Value::None
                ]
            )),
            ParsedEvent::Insert((
                Some(vec![Value::String("2".into())]),
                vec![
                    Value::String("".into()),
                    Value::String("some_value".into()),
                    Value::Int(1)
                ]
            ))
        ]
    );

    Ok(())
}

fn get_schema_abc() -> HashMap<String, InnerSchemaField> {
    [
        ("a".to_string(), InnerSchemaField::new(Type::String, None)),
        ("b".to_string(), InnerSchemaField::new(Type::Int, None)),
        ("c".to_string(), InnerSchemaField::new(Type::Int, None)),
    ]
    .into()
}

#[test]
fn test_jsonlines_fails_without_default() -> eyre::Result<()> {
    let mut schema = get_schema_abc();
    schema.insert("d".to_string(), InnerSchemaField::new(Type::Int, None));
    let reader = new_filesystem_reader(
        "tests/data/jsonlines.txt",
        ConnectorMode::Static,
        ReadMethod::ByLine,
        "*",
        false,
    )?;
    let parser = JsonLinesParser::new(
        Some(vec!["a".to_string()]),
        vec!["b".to_string(), "c".to_string(), "d".to_string()],
        HashMap::new(),
        true,
        schema,
        SessionType::Native,
        None,
    )?;

    let read_lines = read_data_from_reader(Box::new(reader), Box::new(parser))?;
    assert_eq!(
        read_lines,
        vec![
            ParsedEvent::Insert((
                Some(vec![Value::String("abc".into())]),
                vec![Value::Int(7), Value::Int(15), Value::Error]
            )),
            ParsedEvent::Insert((
                Some(vec![Value::String("def".into())]),
                vec![Value::Int(1), Value::Int(3), Value::Error]
            )),
            ParsedEvent::Insert((
                Some(vec![Value::String("ghi".into())]),
                vec![Value::Int(2), Value::Int(4), Value::Error]
            )),
            ParsedEvent::AdvanceTime
        ]
    );

    Ok(())
}

#[test]
fn test_jsonlines_with_default() -> eyre::Result<()> {
    let mut schema = get_schema_abc();
    schema.insert(
        "d".to_string(),
        InnerSchemaField::new(Type::Int, Some(Value::Int(42))),
    );

    let reader = new_filesystem_reader(
        "tests/data/jsonlines_with_skips.txt",
        ConnectorMode::Static,
        ReadMethod::ByLine,
        "*",
        false,
    )?;
    let parser = JsonLinesParser::new(
        Some(vec!["a".to_string()]),
        vec!["b".to_string(), "c".to_string(), "d".to_string()],
        HashMap::new(),
        true,
        schema,
        SessionType::Native,
        None,
    )?;

    let read_lines = read_data_from_reader(Box::new(reader), Box::new(parser))?;
    assert_eq!(
        read_lines,
        vec![
            ParsedEvent::Insert((
                Some(vec![Value::String("abc".into())]),
                vec![Value::Int(7), Value::Int(15), Value::Int(42)]
            )),
            ParsedEvent::Insert((
                Some(vec![Value::String("def".into())]),
                vec![Value::Int(1), Value::Int(3), Value::Int(42)]
            )),
            ParsedEvent::Insert((
                Some(vec![Value::String("ghi".into())]),
                vec![Value::Int(2), Value::Int(4), Value::Int(54)]
            )),
            ParsedEvent::AdvanceTime
        ]
    );

    Ok(())
}

#[test]
fn test_jsonlines_with_default_at_jsonpath() -> eyre::Result<()> {
    let mut schema = get_schema_abc();
    schema.insert(
        "d".to_string(),
        InnerSchemaField::new(Type::Int, Some(Value::Int(42))),
    );

    let mut routes = HashMap::new();
    routes.insert(
        "d".to_string(),
        "/some/path/to/a/field/that/does/not/exist".to_string(),
    );

    let reader = new_filesystem_reader(
        "tests/data/jsonlines_with_skips.txt",
        ConnectorMode::Static,
        ReadMethod::ByLine,
        "*",
        false,
    )?;
    let parser = JsonLinesParser::new(
        Some(vec!["a".to_string()]),
        vec!["b".to_string(), "c".to_string(), "d".to_string()],
        routes,
        true,
        schema,
        SessionType::Native,
        None,
    )?;

    let read_lines = read_data_from_reader(Box::new(reader), Box::new(parser))?;
    assert_eq!(
        read_lines,
        vec![
            ParsedEvent::Insert((
                Some(vec![Value::String("abc".into())]),
                vec![Value::Int(7), Value::Int(15), Value::Int(42)]
            )),
            ParsedEvent::Insert((
                Some(vec![Value::String("def".into())]),
                vec![Value::Int(1), Value::Int(3), Value::Int(42)]
            )),
            ParsedEvent::Insert((
                Some(vec![Value::String("ghi".into())]),
                vec![Value::Int(2), Value::Int(4), Value::Int(42)]
            )),
            ParsedEvent::AdvanceTime
        ]
    );

    Ok(())
}

#[test]
fn test_jsonlines_explicit_null_not_overridden() -> eyre::Result<()> {
    let mut schema = get_schema_abc();
    schema.insert(
        "d".to_string(),
        InnerSchemaField::new(Type::Optional(Type::Int.into()), Some(Value::Int(42))),
    );

    let reader = new_filesystem_reader(
        "tests/data/jsonlines_with_skips_and_nulls.txt",
        ConnectorMode::Static,
        ReadMethod::ByLine,
        "*",
        false,
    )?;
    let parser = JsonLinesParser::new(
        Some(vec!["a".to_string()]),
        vec!["b".to_string(), "c".to_string(), "d".to_string()],
        HashMap::new(),
        true,
        schema,
        SessionType::Native,
        None,
    )?;

    let read_lines = read_data_from_reader(Box::new(reader), Box::new(parser))?;
    assert_eq!(
        read_lines,
        vec![
            ParsedEvent::Insert((
                Some(vec![Value::String("abc".into())]),
                vec![Value::Int(7), Value::Int(15), Value::None]
            )),
            ParsedEvent::Insert((
                Some(vec![Value::String("def".into())]),
                vec![Value::Int(1), Value::Int(3), Value::Int(42)]
            )),
            ParsedEvent::Insert((
                Some(vec![Value::String("ghi".into())]),
                vec![Value::Int(2), Value::Int(4), Value::Int(54)]
            )),
            ParsedEvent::AdvanceTime
        ]
    );

    Ok(())
}
