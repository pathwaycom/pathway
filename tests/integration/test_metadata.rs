// Copyright © 2024 Pathway

use super::helpers::read_data_from_reader;

use std::collections::HashMap;

use pathway_engine::connectors::data_format::{
    DsvParser, DsvSettings, IdentityParser, JsonLinesParser, ParsedEvent,
};
use pathway_engine::connectors::data_storage::{
    ConnectorMode, CsvFilesystemReader, FilesystemReader, ReadMethod,
};
use pathway_engine::connectors::SessionType;
use pathway_engine::engine::Value;

/// This function requires that _metadata field is the last in the `value_names_list`
fn check_file_name_in_metadata(data_read: &ParsedEvent, name: &str) {
    if let ParsedEvent::Insert((_, values)) = data_read {
        if let Value::Json(meta) = &values[values.len() - 1] {
            let path: String = meta["path"].to_string();
            assert!(path.ends_with(name), "{data_read:?}");
        } else {
            panic!("wrong type of metadata field");
        }
    } else {
        panic!("wrong type of event");
    }
}

#[test]
fn test_metadata_fs_dir() -> eyre::Result<()> {
    let reader = FilesystemReader::new(
        "tests/data/csvdir/",
        ConnectorMode::Static,
        None,
        ReadMethod::ByLine,
        "*",
    )?;
    let parser = DsvParser::new(
        DsvSettings::new(
            Some(vec!["key".to_string()]),
            vec![
                "key".to_string(),
                "foo".to_string(),
                "_metadata".to_string(),
            ],
            ',',
        ),
        HashMap::new(),
    );

    let data_read = read_data_from_reader(Box::new(reader), Box::new(parser))?;
    check_file_name_in_metadata(&data_read[0], "tests/data/csvdir/a.txt\"");
    check_file_name_in_metadata(&data_read[2], "tests/data/csvdir/b.txt\"");
    check_file_name_in_metadata(&data_read[4], "tests/data/csvdir/c.txt\"");

    Ok(())
}

#[test]
fn test_metadata_fs_file() -> eyre::Result<()> {
    let reader = FilesystemReader::new(
        "tests/data/minimal.txt",
        ConnectorMode::Static,
        None,
        ReadMethod::ByLine,
        "*",
    )?;
    let parser = DsvParser::new(
        DsvSettings::new(
            Some(vec!["key".to_string()]),
            vec![
                "key".to_string(),
                "foo".to_string(),
                "_metadata".to_string(),
            ],
            ',',
        ),
        HashMap::new(),
    );

    let data_read = read_data_from_reader(Box::new(reader), Box::new(parser))?;
    check_file_name_in_metadata(&data_read[0], "tests/data/minimal.txt\"");

    Ok(())
}

#[test]
fn test_metadata_csv_dir() -> eyre::Result<()> {
    let mut builder = csv::ReaderBuilder::new();
    builder.has_headers(false);

    let reader = CsvFilesystemReader::new(
        "tests/data/csvdir/",
        builder,
        ConnectorMode::Static,
        None,
        "*",
    )?;
    let parser = DsvParser::new(
        DsvSettings::new(
            Some(vec!["key".to_string()]),
            vec![
                "key".to_string(),
                "foo".to_string(),
                "_metadata".to_string(),
            ],
            ',',
        ),
        HashMap::new(),
    );

    let data_read = read_data_from_reader(Box::new(reader), Box::new(parser))?;
    check_file_name_in_metadata(&data_read[0], "tests/data/csvdir/a.txt\"");
    check_file_name_in_metadata(&data_read[2], "tests/data/csvdir/b.txt\"");
    check_file_name_in_metadata(&data_read[4], "tests/data/csvdir/c.txt\"");

    Ok(())
}

#[test]
fn test_metadata_csv_file() -> eyre::Result<()> {
    let mut builder = csv::ReaderBuilder::new();
    builder.has_headers(false);

    let reader = CsvFilesystemReader::new(
        "tests/data/minimal.txt",
        builder,
        ConnectorMode::Static,
        None,
        "*",
    )?;
    let parser = DsvParser::new(
        DsvSettings::new(
            Some(vec!["key".to_string()]),
            vec![
                "key".to_string(),
                "foo".to_string(),
                "_metadata".to_string(),
            ],
            ',',
        ),
        HashMap::new(),
    );

    let data_read = read_data_from_reader(Box::new(reader), Box::new(parser))?;
    check_file_name_in_metadata(&data_read[0], "tests/data/minimal.txt\"");

    Ok(())
}

#[test]
fn test_metadata_json_file() -> eyre::Result<()> {
    let reader = FilesystemReader::new(
        "tests/data/jsonlines.txt",
        ConnectorMode::Static,
        None,
        ReadMethod::ByLine,
        "*",
    )?;
    let parser = JsonLinesParser::new(
        None,
        vec!["a".to_string(), "_metadata".to_string()],
        HashMap::new(),
        false,
        HashMap::new(),
        SessionType::Native,
    );

    let data_read = read_data_from_reader(Box::new(reader), Box::new(parser))?;
    check_file_name_in_metadata(&data_read[0], "tests/data/jsonlines.txt\"");

    Ok(())
}

#[test]
fn test_metadata_json_dir() -> eyre::Result<()> {
    let reader = FilesystemReader::new(
        "tests/data/jsonlines/",
        ConnectorMode::Static,
        None,
        ReadMethod::ByLine,
        "*",
    )?;
    let parser = JsonLinesParser::new(
        None,
        vec!["a".to_string(), "_metadata".to_string()],
        HashMap::new(),
        false,
        HashMap::new(),
        SessionType::Native,
    );

    let data_read = read_data_from_reader(Box::new(reader), Box::new(parser))?;
    check_file_name_in_metadata(&data_read[0], "tests/data/jsonlines/one.jsonlines\"");
    check_file_name_in_metadata(&data_read[1], "tests/data/jsonlines/two.jsonlines\"");

    Ok(())
}

#[test]
fn test_metadata_identity_file() -> eyre::Result<()> {
    let reader = FilesystemReader::new(
        "tests/data/jsonlines.txt",
        ConnectorMode::Static,
        None,
        ReadMethod::ByLine,
        "*",
    )?;
    let parser = IdentityParser::new(
        vec!["data".to_string(), "_metadata".to_string()],
        false,
        SessionType::Native,
    );

    let data_read = read_data_from_reader(Box::new(reader), Box::new(parser))?;
    check_file_name_in_metadata(&data_read[0], "tests/data/jsonlines.txt\"");

    Ok(())
}

#[test]
fn test_metadata_identity_dir() -> eyre::Result<()> {
    let reader = FilesystemReader::new(
        "tests/data/jsonlines/",
        ConnectorMode::Static,
        None,
        ReadMethod::ByLine,
        "*",
    )?;
    let parser = IdentityParser::new(
        vec!["data".to_string(), "_metadata".to_string()],
        false,
        SessionType::Native,
    );

    let data_read = read_data_from_reader(Box::new(reader), Box::new(parser))?;
    check_file_name_in_metadata(&data_read[0], "tests/data/jsonlines/one.jsonlines\"");
    check_file_name_in_metadata(&data_read[1], "tests/data/jsonlines/two.jsonlines\"");

    Ok(())
}
