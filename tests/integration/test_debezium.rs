// Copyright © 2024 Pathway

use crate::helpers::{ErrorPlacement, ReplaceErrors};

use super::helpers::{assert_error_shown_for_raw_data, read_data_from_reader};

use std::fs::File;
use std::io::{BufRead, BufReader};

use assert_matches::assert_matches;

use pathway_engine::connectors::data_format::{
    DebeziumDBType, DebeziumMessageParser, ParseError, ParsedEvent, Parser,
};
use pathway_engine::connectors::data_storage::{
    ConnectorMode, FilesystemReader, ReadMethod, ReaderContext,
};
use pathway_engine::connectors::SessionType;
use pathway_engine::engine::Value;

#[test]
fn test_debezium_reads_ok() -> eyre::Result<()> {
    let reader = FilesystemReader::new(
        "tests/data/sample_debezium.txt",
        ConnectorMode::Static,
        None,
        ReadMethod::ByLine,
        "*",
    )?;
    let parser = DebeziumMessageParser::new(
        Some(vec!["id".to_string()]),
        vec!["first_name".to_string()],
        "        ".to_string(),
        DebeziumDBType::Postgres,
    );

    assert_matches!(parser.session_type(), SessionType::Native);

    let changelog = read_data_from_reader(Box::new(reader), Box::new(parser))?;

    let expected_values = vec![
        ParsedEvent::Insert((Some(vec![Value::Int(1001)]), vec![Value::from("Sally")])),
        ParsedEvent::Insert((Some(vec![Value::Int(1002)]), vec![Value::from("George")])),
        ParsedEvent::Insert((Some(vec![Value::Int(1003)]), vec![Value::from("Edward")])),
        ParsedEvent::Insert((Some(vec![Value::Int(1004)]), vec![Value::from("Anne")])),
        ParsedEvent::Insert((Some(vec![Value::Int(1005)]), vec![Value::from("Sergey")])),
        ParsedEvent::Delete((Some(vec![Value::Int(1005)]), vec![Value::from("Sergey")])),
        ParsedEvent::Insert((Some(vec![Value::Int(1005)]), vec![Value::from("Siarhei")])),
        ParsedEvent::Delete((Some(vec![Value::Int(1005)]), vec![Value::from("Siarhei")])),
    ];
    assert_eq!(changelog, expected_values);

    Ok(())
}

#[test]
fn test_debezium_unparsable_json() -> eyre::Result<()> {
    let incorrect_json_pair = b"a        b";
    let parser = DebeziumMessageParser::new(
        Some(vec!["id".to_string()]),
        vec!["first_name".to_string()],
        "        ".to_string(),
        DebeziumDBType::Postgres,
    );

    assert_error_shown_for_raw_data(
        incorrect_json_pair,
        Box::new(parser),
        r#"received message is not json: "b""#,
        ErrorPlacement::Message,
    );

    Ok(())
}

#[test]
fn test_debezium_json_format_incorrect() -> eyre::Result<()> {
    let incorrect_json_pair = br#"{"payload": {}}        {"c": "d"}"#;
    let parser = DebeziumMessageParser::new(
        Some(vec!["id".to_string()]),
        vec!["first_name".to_string()],
        "        ".to_string(),
        DebeziumDBType::Postgres,
    );
    assert_error_shown_for_raw_data(incorrect_json_pair, Box::new(parser), "received message doesn't comply with debezium format: there is no payload at the top level of value json", ErrorPlacement::Message);
    Ok(())
}

#[test]
fn test_debezium_json_no_operation_specified() -> eyre::Result<()> {
    let incorrect_json_pair = br#"{"payload": {}}        {"payload": "d"}"#;
    let parser = DebeziumMessageParser::new(
        Some(vec!["id".to_string()]),
        vec!["first_name".to_string()],
        "        ".to_string(),
        DebeziumDBType::Postgres,
    );
    assert_error_shown_for_raw_data(incorrect_json_pair, Box::new(parser), "received message doesn't comply with debezium format: incorrect type of payload.op field or it is missing", ErrorPlacement::Message);
    Ok(())
}

#[test]
fn test_debezium_json_unsupported_operation() -> eyre::Result<()> {
    let incorrect_json_pair = br#"{"payload": {}}        {"payload": {"op": "a"}}"#;
    let parser = DebeziumMessageParser::new(
        Some(vec!["id".to_string()]),
        vec!["first_name".to_string()],
        "        ".to_string(),
        DebeziumDBType::Postgres,
    );
    assert_error_shown_for_raw_data(
        incorrect_json_pair,
        Box::new(parser),
        r#"unknown debezium operation "a""#,
        ErrorPlacement::Message,
    );
    Ok(())
}

#[test]
fn test_debezium_json_incomplete_data() -> eyre::Result<()> {
    let incorrect_json_pair = br#"{"payload": null}        {"payload": {"op": "u"}}"#;
    let parser = DebeziumMessageParser::new(
        Some(vec!["id".to_string()]),
        vec!["first_name".to_string()],
        "        ".to_string(),
        DebeziumDBType::Postgres,
    );
    assert_error_shown_for_raw_data(
        incorrect_json_pair,
        Box::new(parser),
        "field id with no JsonPointer path specified is absent in null",
        ErrorPlacement::Key,
    );
    Ok(())
}

#[test]
fn test_debezium_tokens_amt_mismatch() -> eyre::Result<()> {
    let incorrect_json_pair = b"a b";
    let parser = DebeziumMessageParser::new(
        Some(vec!["id".to_string()]),
        vec!["first_name".to_string()],
        "        ".to_string(),
        DebeziumDBType::Postgres,
    );
    assert_error_shown_for_raw_data(
        incorrect_json_pair,
        Box::new(parser),
        "key-value pair has unexpected number of tokens: 1 instead of 2",
        ErrorPlacement::Message,
    );

    let incorrect_json_pair = b"a        b        c";
    let parser = DebeziumMessageParser::new(
        Some(vec!["id".to_string()]),
        vec!["first_name".to_string()],
        "        ".to_string(),
        DebeziumDBType::Postgres,
    );
    assert_error_shown_for_raw_data(
        incorrect_json_pair,
        Box::new(parser),
        "key-value pair has unexpected number of tokens: 3 instead of 2",
        ErrorPlacement::Message,
    );

    Ok(())
}

#[test]
fn test_debezium_mongodb_format() -> eyre::Result<()> {
    let reader = FilesystemReader::new(
        "tests/data/sample_debezium_mongodb.txt",
        ConnectorMode::Static,
        None,
        ReadMethod::ByLine,
        "*",
    )?;
    let parser = DebeziumMessageParser::new(
        Some(vec!["id".to_string()]),
        vec!["first_name".to_string()],
        "        ".to_string(),
        DebeziumDBType::MongoDB,
    );

    assert_matches!(parser.session_type(), SessionType::Upsert);

    let changelog = read_data_from_reader(Box::new(reader), Box::new(parser))?;

    let expected_values = vec![
        ParsedEvent::Upsert((
            Some(vec![Value::from("1001")]),
            Some(vec![Value::from("Sally")]),
        )),
        ParsedEvent::Upsert((
            Some(vec![Value::from("1002")]),
            Some(vec![Value::from("George")]),
        )),
        ParsedEvent::Upsert((
            Some(vec![Value::from("1003")]),
            Some(vec![Value::from("Edward")]),
        )),
        ParsedEvent::Upsert((
            Some(vec![Value::from("1004")]),
            Some(vec![Value::from("Anne")]),
        )),
        ParsedEvent::Upsert((
            Some(vec![Value::from("1005")]),
            Some(vec![Value::from("Bob")]),
        )),
        ParsedEvent::Upsert((
            Some(vec![Value::from("1003")]),
            Some(vec![Value::from("Sergey")]),
        )),
        ParsedEvent::Upsert((Some(vec![Value::from("1004")]), None)),
    ];
    assert_eq!(changelog, expected_values);

    Ok(())
}

#[test]
fn test_postgres_with_empty_pkey() -> eyre::Result<()> {
    let mut parser = DebeziumMessageParser::new(
        None,
        vec!["first_name".to_string()],
        "        ".to_string(),
        DebeziumDBType::Postgres,
    );

    let reader = BufReader::new(File::open("tests/data/sample_debezium.txt")?);
    let line: String = reader.lines().next().unwrap()?;
    let key_value: Vec<&str> = line.split("        ").collect();
    let value = key_value[1];

    let context = ReaderContext::KeyValue((None, Some(value.into())));
    let parse_result: Vec<_> = parser
        .parse(&context)
        .map_err(ParseError::from)?
        .into_iter()
        .map(|entry| entry.replace_errors())
        .collect();
    assert_eq!(
        parse_result,
        vec![ParsedEvent::Insert((
            None,
            vec![Value::String("Sally".into())]
        ))]
    );

    Ok(())
}
