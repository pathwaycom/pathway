mod helpers;
use helpers::read_data_from_reader;

use std::collections::HashMap;
use std::io;
use std::path::PathBuf;
use std::str::FromStr;

use assert_matches::assert_matches;

use pathway_engine::connectors::data_format::ParsedEvent;
use pathway_engine::connectors::data_format::{DsvParser, DsvSettings};
use pathway_engine::connectors::data_storage::CsvFilesystemReader;
use pathway_engine::engine::Value;

#[test]
fn test_dsv_dir_ok() -> eyre::Result<()> {
    let mut builder = csv::ReaderBuilder::new();
    builder.has_headers(false);

    let reader =
        CsvFilesystemReader::new(PathBuf::from("tests/data/csvdir"), builder, false, None)?;
    let parser = DsvParser::new(
        DsvSettings::new(Some(vec!["key".to_string()]), vec!["foo".to_string()], ','),
        HashMap::new(),
    );

    let read_lines = read_data_from_reader(Box::new(reader), Box::new(parser))?;

    assert_eq!(read_lines.len(), 6);
    let expected_values = vec![
        ParsedEvent::Insert((
            Some(vec![Value::from_str("1")?]),
            vec![Value::from_str("abc")?],
        )),
        ParsedEvent::Insert((
            Some(vec![Value::from_str("2")?]),
            vec![Value::from_str("def")?],
        )),
        ParsedEvent::Insert((
            Some(vec![Value::from_str("3")?]),
            vec![Value::from_str("ghi")?],
        )),
        ParsedEvent::Insert((
            Some(vec![Value::from_str("4")?]),
            vec![Value::from_str("jkl")?],
        )),
        ParsedEvent::Insert((
            Some(vec![Value::from_str("5")?]),
            vec![Value::from_str("mno")?],
        )),
        ParsedEvent::Insert((
            Some(vec![Value::from_str("6")?]),
            vec![Value::from_str("pqr")?],
        )),
    ];
    assert_eq!(read_lines, expected_values);

    Ok(())
}

#[test]
fn test_single_file_ok() -> eyre::Result<()> {
    let mut builder = csv::ReaderBuilder::new();
    builder.has_headers(false);

    let reader =
        CsvFilesystemReader::new(PathBuf::from("tests/data/sample.txt"), builder, false, None)?;
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
        PathBuf::from("tests/data/sql_injection.txt"),
        builder,
        false,
        None,
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
        PathBuf::from("tests/data/csv_fields_escaped.txt"),
        builder,
        false,
        None,
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
            Some(vec![Value::from_str("1")?]),
            vec![Value::from_str("2")?, Value::from_str("3")?],
        )),
        ParsedEvent::Insert((
            Some(vec![Value::from_str("4")?]),
            vec![Value::from_str("5")?, Value::from_str("6")?],
        )),
        ParsedEvent::Insert((
            Some(vec![Value::from_str("7")?]),
            vec![Value::from_str("8")?, Value::from_str("9")?],
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
        PathBuf::from("tests/data/csv_escaped_newlines.txt"),
        builder,
        false,
        None,
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
            Some(vec![Value::from_str("1")?]),
            vec![Value::from_str("2\n3 4\n5 6")?],
        )),
        ParsedEvent::Insert((
            Some(vec![Value::from_str("a")?]),
            vec![Value::from_str("b")?],
        )),
    ];
    assert_eq!(read_lines, expected_values);

    Ok(())
}

#[test]
fn test_nonexistent_file() -> eyre::Result<()> {
    let mut builder = csv::ReaderBuilder::new();
    builder.has_headers(false);

    let reader = CsvFilesystemReader::new(
        PathBuf::from("tests/data/nonexistent_file.txt"),
        builder,
        false,
        None,
    );
    assert_matches!(reader.unwrap_err().kind(), io::ErrorKind::NotFound);

    Ok(())
}

#[test]
fn test_special_fields() -> eyre::Result<()> {
    let mut builder = csv::ReaderBuilder::new();
    builder.has_headers(false);

    let reader = CsvFilesystemReader::new(
        PathBuf::from("tests/data/csv_special_fields.txt"),
        builder,
        false,
        None,
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
            Some(vec![Value::from_str("1")?]),
            vec![Value::from_str("abc")?, Value::from_str("def")?],
        )),
        ParsedEvent::AdvanceTime,
    ];
    assert_eq!(read_lines, expected_values);

    Ok(())
}
