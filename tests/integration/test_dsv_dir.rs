// Copyright © 2024 Pathway

use super::helpers::read_data_from_reader;

use pathway_engine::connectors::data_format::{DsvParser, DsvSettings};
use pathway_engine::connectors::data_format::{InnerSchemaField, ParsedEvent};
use pathway_engine::connectors::data_storage::{new_csv_filesystem_reader, ConnectorMode};
use pathway_engine::engine::{Type, Value};

#[test]
fn test_dsv_dir_ok() -> eyre::Result<()> {
    let mut builder = csv::ReaderBuilder::new();
    builder.has_headers(false);

    let schema = [
        ("key".to_string(), InnerSchemaField::new(Type::String, None)),
        ("foo".to_string(), InnerSchemaField::new(Type::String, None)),
    ];

    let reader = new_csv_filesystem_reader(
        "tests/data/csvdir",
        builder,
        ConnectorMode::Static,
        "*",
        false,
    )?;
    let parser = DsvParser::new(
        DsvSettings::new(Some(vec!["key".to_string()]), vec!["foo".to_string()], ','),
        schema.into(),
    )?;

    let read_lines = read_data_from_reader(Box::new(reader), Box::new(parser))?;

    assert_eq!(read_lines.len(), 6);
    let expected_values = vec![
        ParsedEvent::Insert((Some(vec![Value::from("1")]), vec![Value::from("abc")])),
        ParsedEvent::Insert((Some(vec![Value::from("2")]), vec![Value::from("def")])),
        ParsedEvent::Insert((Some(vec![Value::from("3")]), vec![Value::from("ghi")])),
        ParsedEvent::Insert((Some(vec![Value::from("4")]), vec![Value::from("jkl")])),
        ParsedEvent::Insert((Some(vec![Value::from("5")]), vec![Value::from("mno")])),
        ParsedEvent::Insert((Some(vec![Value::from("6")]), vec![Value::from("pqr")])),
    ];
    assert_eq!(read_lines, expected_values);

    Ok(())
}

#[test]
fn test_single_file_ok() -> eyre::Result<()> {
    let mut builder = csv::ReaderBuilder::new();
    builder.has_headers(false);

    let schema = [
        ("a".to_string(), InnerSchemaField::new(Type::String, None)),
        ("b".to_string(), InnerSchemaField::new(Type::String, None)),
    ];

    let reader = new_csv_filesystem_reader(
        "tests/data/sample.txt",
        builder,
        ConnectorMode::Static,
        "*",
        false,
    )?;
    let parser = DsvParser::new(
        DsvSettings::new(Some(vec!["a".to_string()]), vec!["b".to_string()], ','),
        schema.into(),
    )?;

    let read_lines = read_data_from_reader(Box::new(reader), Box::new(parser))?;

    assert_eq!(read_lines.len(), 10);

    Ok(())
}

#[test]
fn test_custom_delimiter() -> eyre::Result<()> {
    let mut builder = csv::ReaderBuilder::new();
    builder.delimiter(b'+');
    builder.has_headers(false);

    let schema = [
        ("key".to_string(), InnerSchemaField::new(Type::String, None)),
        ("foo".to_string(), InnerSchemaField::new(Type::String, None)),
        (
            "foofoo".to_string(),
            InnerSchemaField::new(Type::String, None),
        ),
    ];

    let reader = new_csv_filesystem_reader(
        "tests/data/sql_injection.txt",
        builder,
        ConnectorMode::Static,
        "*",
        false,
    )?;
    let parser = DsvParser::new(
        DsvSettings::new(
            Some(vec!["key".to_string()]),
            vec!["foo".to_string(), "foofoo".to_string()],
            '+',
        ),
        schema.into(),
    )?;

    let read_lines = read_data_from_reader(Box::new(reader), Box::new(parser))?;
    assert_eq!(read_lines.len(), 2);

    Ok(())
}

#[test]
fn test_escape_fields() -> eyre::Result<()> {
    let mut builder = csv::ReaderBuilder::new();
    builder.has_headers(false);

    let schema = [
        ("key".to_string(), InnerSchemaField::new(Type::String, None)),
        (
            "value,with,comma".to_string(),
            InnerSchemaField::new(Type::String, None),
        ),
        (
            "some other value".to_string(),
            InnerSchemaField::new(Type::String, None),
        ),
    ];

    let reader = new_csv_filesystem_reader(
        "tests/data/csv_fields_escaped.txt",
        builder,
        ConnectorMode::Static,
        "*",
        false,
    )?;
    let parser = DsvParser::new(
        DsvSettings::new(
            Some(vec!["key".to_string()]),
            vec![
                "value,with,comma".to_string(),
                "some other value".to_string(),
            ],
            ',',
        ),
        schema.into(),
    )?;

    let read_lines = read_data_from_reader(Box::new(reader), Box::new(parser))?;

    assert_eq!(read_lines.len(), 3);
    let expected_values = vec![
        ParsedEvent::Insert((
            Some(vec![Value::from("1")]),
            vec![Value::from("2"), Value::from("3")],
        )),
        ParsedEvent::Insert((
            Some(vec![Value::from("4")]),
            vec![Value::from("5"), Value::from("6")],
        )),
        ParsedEvent::Insert((
            Some(vec![Value::from("7")]),
            vec![Value::from("8"), Value::from("9")],
        )),
    ];
    assert_eq!(read_lines, expected_values);

    Ok(())
}

#[test]
fn test_escape_newlines() -> eyre::Result<()> {
    let mut builder = csv::ReaderBuilder::new();
    builder.has_headers(false);

    let schema = [
        ("key".to_string(), InnerSchemaField::new(Type::String, None)),
        (
            "value".to_string(),
            InnerSchemaField::new(Type::String, None),
        ),
    ];

    let reader = new_csv_filesystem_reader(
        "tests/data/csv_escaped_newlines.txt",
        builder,
        ConnectorMode::Static,
        "*",
        false,
    )?;
    let parser = DsvParser::new(
        DsvSettings::new(
            Some(vec!["key".to_string()]),
            vec!["value".to_string()],
            ',',
        ),
        schema.into(),
    )?;

    let read_lines = read_data_from_reader(Box::new(reader), Box::new(parser))?;

    assert_eq!(read_lines.len(), 2);
    let expected_values = vec![
        ParsedEvent::Insert((
            Some(vec![Value::from("1")]),
            vec![Value::from("2\n3 4\n5 6")],
        )),
        ParsedEvent::Insert((Some(vec![Value::from("a")]), vec![Value::from("b")])),
    ];
    assert_eq!(read_lines, expected_values);

    Ok(())
}

#[test]
fn test_nonexistent_file() -> eyre::Result<()> {
    let mut builder = csv::ReaderBuilder::new();
    builder.has_headers(false);

    let reader = new_csv_filesystem_reader(
        "tests/data/nonexistent_file.txt",
        builder,
        ConnectorMode::Static,
        "*",
        false,
    );

    // We treat this path as a glob pattern, so the situation is normal:
    // the scanner will just scan the contents on an empty set. If a file
    // will be added at this path, it will be scanned and read.
    assert!(reader.is_ok());

    Ok(())
}

#[test]
fn test_special_fields() -> eyre::Result<()> {
    let mut builder = csv::ReaderBuilder::new();
    builder.has_headers(false);

    let schema = [
        ("key".to_string(), InnerSchemaField::new(Type::String, None)),
        (
            "value".to_string(),
            InnerSchemaField::new(Type::String, None),
        ),
        (
            "data".to_string(),
            InnerSchemaField::new(Type::String, None),
        ),
    ];

    let reader = new_csv_filesystem_reader(
        "tests/data/csv_special_fields.txt",
        builder,
        ConnectorMode::Static,
        "*",
        false,
    )?;
    let parser = DsvParser::new(
        DsvSettings::new(
            Some(vec!["key".to_string()]),
            vec!["value".to_string(), "data".to_string()],
            ',',
        ),
        schema.into(),
    )?;

    let read_lines = read_data_from_reader(Box::new(reader), Box::new(parser))?;

    let expected_values = vec![
        ParsedEvent::Insert((
            Some(vec![Value::from("1")]),
            vec![Value::from("abc"), Value::from("def")],
        )),
        ParsedEvent::AdvanceTime,
    ];
    assert_eq!(read_lines, expected_values);

    Ok(())
}
