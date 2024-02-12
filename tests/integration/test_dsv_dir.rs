// Copyright © 2024 Pathway

use super::helpers::read_data_from_reader;

use std::collections::HashMap;

use pathway_engine::connectors::data_format::ParsedEvent;
use pathway_engine::connectors::data_format::{DsvParser, DsvSettings};
use pathway_engine::connectors::data_storage::{ConnectorMode, CsvFilesystemReader};
use pathway_engine::engine::Value;

#[test]
fn test_dsv_dir_ok() -> eyre::Result<()> {
    let mut builder = csv::ReaderBuilder::new();
    builder.has_headers(false);

    let reader = CsvFilesystemReader::new(
        "tests/data/csvdir",
        builder,
        ConnectorMode::Static,
        None,
        "*",
    )?;
    let parser = DsvParser::new(
        DsvSettings::new(Some(vec!["key".to_string()]), vec!["foo".to_string()], ','),
        HashMap::new(),
    );

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

    let reader = CsvFilesystemReader::new(
        "tests/data/sample.txt",
        builder,
        ConnectorMode::Static,
        None,
        "*",
    )?;
    let parser = DsvParser::new(
        DsvSettings::new(Some(vec!["a".to_string()]), vec!["b".to_string()], ','),
        HashMap::new(),
    );

    let read_lines = read_data_from_reader(Box::new(reader), Box::new(parser))?;

    assert_eq!(read_lines.len(), 10);

    Ok(())
}

#[test]
fn test_custom_delimiter() -> eyre::Result<()> {
    let mut builder = csv::ReaderBuilder::new();
    builder.delimiter(b'+');
    builder.has_headers(false);

    let reader = CsvFilesystemReader::new(
        "tests/data/sql_injection.txt",
        builder,
        ConnectorMode::Static,
        None,
        "*",
    )?;
    let parser = DsvParser::new(
        DsvSettings::new(
            Some(vec!["key".to_string()]),
            vec!["foo".to_string(), "foofoo".to_string()],
            '+',
        ),
        HashMap::new(),
    );

    let read_lines = read_data_from_reader(Box::new(reader), Box::new(parser))?;
    assert_eq!(read_lines.len(), 2);

    Ok(())
}

#[test]
fn test_escape_fields() -> eyre::Result<()> {
    let mut builder = csv::ReaderBuilder::new();
    builder.has_headers(false);

    let reader = CsvFilesystemReader::new(
        "tests/data/csv_fields_escaped.txt",
        builder,
        ConnectorMode::Static,
        None,
        "*",
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
        HashMap::new(),
    );

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

    let reader = CsvFilesystemReader::new(
        "tests/data/csv_escaped_newlines.txt",
        builder,
        ConnectorMode::Static,
        None,
        "*",
    )?;
    let parser = DsvParser::new(
        DsvSettings::new(
            Some(vec!["key".to_string()]),
            vec!["value".to_string()],
            ',',
        ),
        HashMap::new(),
    );

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

    let reader = CsvFilesystemReader::new(
        "tests/data/nonexistent_file.txt",
        builder,
        ConnectorMode::Static,
        None,
        "*",
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

    let reader = CsvFilesystemReader::new(
        "tests/data/csv_special_fields.txt",
        builder,
        ConnectorMode::Static,
        None,
        "*",
    )?;
    let parser = DsvParser::new(
        DsvSettings::new(
            Some(vec!["key".to_string()]),
            vec!["value".to_string(), "data".to_string()],
            ',',
        ),
        HashMap::new(),
    );

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
