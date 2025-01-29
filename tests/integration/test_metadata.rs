// Copyright © 2024 Pathway

use super::helpers::read_data_from_reader;

use std::collections::HashMap;

use pathway_engine::connectors::data_format::{
    DsvParser, DsvSettings, IdentityParser, InnerSchemaField, JsonLinesParser, KeyGenerationPolicy,
    ParsedEvent,
};
use pathway_engine::connectors::data_storage::{
    new_csv_filesystem_reader, new_filesystem_reader, ConnectorMode, ReadMethod,
};
use pathway_engine::connectors::SessionType;
use pathway_engine::engine::{Type, Value};

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
    let reader = new_filesystem_reader(
        "tests/data/csvdir/",
        ConnectorMode::Static,
        ReadMethod::ByLine,
        "*",
        false,
    )?;
    let schema = [
        ("key".to_string(), InnerSchemaField::new(Type::Int, None)),
        ("foo".to_string(), InnerSchemaField::new(Type::String, None)),
        (
            "_metadata".to_string(),
            InnerSchemaField::new(Type::Json, None),
        ),
    ];
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
        schema.into(),
    )?;

    let data_read = read_data_from_reader(Box::new(reader), Box::new(parser))?;
    check_file_name_in_metadata(&data_read[0], "tests/data/csvdir/a.txt\"");
    check_file_name_in_metadata(&data_read[2], "tests/data/csvdir/b.txt\"");
    check_file_name_in_metadata(&data_read[4], "tests/data/csvdir/c.txt\"");

    Ok(())
}

#[test]
fn test_metadata_fs_file() -> eyre::Result<()> {
    let reader = new_filesystem_reader(
        "tests/data/minimal.txt",
        ConnectorMode::Static,
        ReadMethod::ByLine,
        "*",
        false,
    )?;
    let schema = [
        ("key".to_string(), InnerSchemaField::new(Type::Int, None)),
        ("foo".to_string(), InnerSchemaField::new(Type::String, None)),
        (
            "_metadata".to_string(),
            InnerSchemaField::new(Type::Json, None),
        ),
    ];
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
        schema.into(),
    )?;

    let data_read = read_data_from_reader(Box::new(reader), Box::new(parser))?;
    check_file_name_in_metadata(&data_read[0], "tests/data/minimal.txt\"");

    Ok(())
}

#[test]
fn test_metadata_csv_dir() -> eyre::Result<()> {
    let mut builder = csv::ReaderBuilder::new();
    builder.has_headers(false);

    let reader = new_csv_filesystem_reader(
        "tests/data/csvdir/",
        builder,
        ConnectorMode::Static,
        "*",
        false,
    )?;
    let schema = [
        ("key".to_string(), InnerSchemaField::new(Type::Int, None)),
        ("foo".to_string(), InnerSchemaField::new(Type::String, None)),
        (
            "_metadata".to_string(),
            InnerSchemaField::new(Type::Json, None),
        ),
    ];
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
        schema.into(),
    )?;

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

    let reader = new_csv_filesystem_reader(
        "tests/data/minimal.txt",
        builder,
        ConnectorMode::Static,
        "*",
        false,
    )?;
    let schema = [
        ("key".to_string(), InnerSchemaField::new(Type::Int, None)),
        ("foo".to_string(), InnerSchemaField::new(Type::String, None)),
        (
            "_metadata".to_string(),
            InnerSchemaField::new(Type::Json, None),
        ),
    ];
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
        schema.into(),
    )?;

    let data_read = read_data_from_reader(Box::new(reader), Box::new(parser))?;
    check_file_name_in_metadata(&data_read[0], "tests/data/minimal.txt\"");

    Ok(())
}

#[test]
fn test_metadata_json_file() -> eyre::Result<()> {
    let reader = new_filesystem_reader(
        "tests/data/jsonlines.txt",
        ConnectorMode::Static,
        ReadMethod::ByLine,
        "*",
        false,
    )?;
    let schema = [
        ("a".to_string(), InnerSchemaField::new(Type::String, None)),
        (
            "_metadata".to_string(),
            InnerSchemaField::new(Type::Json, None),
        ),
    ];
    let parser = JsonLinesParser::new(
        None,
        vec!["a".to_string(), "_metadata".to_string()],
        HashMap::new(),
        false,
        schema.into(),
        SessionType::Native,
    )?;

    let data_read = read_data_from_reader(Box::new(reader), Box::new(parser))?;
    check_file_name_in_metadata(&data_read[0], "tests/data/jsonlines.txt\"");

    Ok(())
}

#[test]
fn test_metadata_json_dir() -> eyre::Result<()> {
    let reader = new_filesystem_reader(
        "tests/data/jsonlines/",
        ConnectorMode::Static,
        ReadMethod::ByLine,
        "*",
        false,
    )?;
    let schema = [
        ("a".to_string(), InnerSchemaField::new(Type::String, None)),
        (
            "_metadata".to_string(),
            InnerSchemaField::new(Type::Json, None),
        ),
    ];
    let parser = JsonLinesParser::new(
        None,
        vec!["a".to_string(), "_metadata".to_string()],
        HashMap::new(),
        false,
        schema.into(),
        SessionType::Native,
    )?;

    let data_read = read_data_from_reader(Box::new(reader), Box::new(parser))?;
    check_file_name_in_metadata(&data_read[0], "tests/data/jsonlines/one.jsonlines\"");
    check_file_name_in_metadata(&data_read[1], "tests/data/jsonlines/two.jsonlines\"");

    Ok(())
}

#[test]
fn test_metadata_identity_file() -> eyre::Result<()> {
    let reader = new_filesystem_reader(
        "tests/data/jsonlines.txt",
        ConnectorMode::Static,
        ReadMethod::ByLine,
        "*",
        false,
    )?;
    let parser = IdentityParser::new(
        vec!["data".to_string(), "_metadata".to_string()],
        false,
        KeyGenerationPolicy::PreferMessageKey,
        SessionType::Native,
    );

    let data_read = read_data_from_reader(Box::new(reader), Box::new(parser))?;
    check_file_name_in_metadata(&data_read[0], "tests/data/jsonlines.txt\"");

    Ok(())
}

#[test]
fn test_metadata_identity_dir() -> eyre::Result<()> {
    let reader = new_filesystem_reader(
        "tests/data/jsonlines/",
        ConnectorMode::Static,
        ReadMethod::ByLine,
        "*",
        false,
    )?;
    let parser = IdentityParser::new(
        vec!["data".to_string(), "_metadata".to_string()],
        false,
        KeyGenerationPolicy::PreferMessageKey,
        SessionType::Native,
    );

    let data_read = read_data_from_reader(Box::new(reader), Box::new(parser))?;
    check_file_name_in_metadata(&data_read[0], "tests/data/jsonlines/one.jsonlines\"");
    check_file_name_in_metadata(&data_read[1], "tests/data/jsonlines/two.jsonlines\"");

    Ok(())
}
