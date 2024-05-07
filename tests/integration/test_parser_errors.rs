// Copyright © 2024 Pathway

use crate::helpers::ErrorPlacement;

use super::helpers::assert_error_shown_for_reader_context;

use pathway_engine::connectors::data_format::{DebeziumDBType, DebeziumMessageParser};
use pathway_engine::connectors::data_storage::{DataEventType, ReaderContext};

#[test]
fn test_utf8_decode_error() -> eyre::Result<()> {
    let parser = DebeziumMessageParser::new(
        Some(vec!["id".to_string()]),
        vec!["first_name".to_string()],
        "        ".to_string(),
        DebeziumDBType::Postgres,
    );

    let invalid_utf8_bytes: &[u8] = &[0xC0, 0x80, 0xE0, 0x80, 0x80];
    assert_error_shown_for_reader_context(
        &ReaderContext::KeyValue((Some(invalid_utf8_bytes.to_vec()), Some(invalid_utf8_bytes.to_vec()))),
        Box::new(parser),
        "received plaintext message is not in utf-8 format: invalid utf-8 sequence of 1 bytes from index 0",
        ErrorPlacement::Message
    );

    Ok(())
}

#[test]
fn test_empty_payload() -> eyre::Result<()> {
    let parser = DebeziumMessageParser::new(
        Some(vec!["id".to_string()]),
        vec!["first_name".to_string()],
        "        ".to_string(),
        DebeziumDBType::Postgres,
    );

    assert_error_shown_for_reader_context(
        &ReaderContext::KeyValue((None, None)),
        Box::new(parser),
        "received message doesn't have payload",
        ErrorPlacement::Message,
    );

    Ok(())
}

#[test]
fn test_unsupported_context() -> eyre::Result<()> {
    let parser = DebeziumMessageParser::new(
        Some(vec!["id".to_string()]),
        vec!["first_name".to_string()],
        "        ".to_string(),
        DebeziumDBType::Postgres,
    );

    assert_error_shown_for_reader_context(
        &ReaderContext::TokenizedEntries(DataEventType::Insert, vec!["a".to_string()]),
        Box::new(parser),
        "internal error, reader context is not supported in this parser",
        ErrorPlacement::Message,
    );

    Ok(())
}
