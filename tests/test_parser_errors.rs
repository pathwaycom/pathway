mod helpers;
use helpers::assert_error_shown_for_reader_context;

use pathway_engine::connectors::data_format::{DebeziumMessageParser, IdentityParser};
use pathway_engine::connectors::data_storage::ReaderContext;

#[test]
fn test_utf8_decode_error() -> eyre::Result<()> {
    let parser = DebeziumMessageParser::new(
        Some(vec!["id".to_string()]),
        vec!["first_name".to_string()],
        "        ".to_string(),
    );

    let invalid_utf8_bytes: &[u8] = &[0xC0, 0x80, 0xE0, 0x80, 0x80];
    assert_error_shown_for_reader_context(
        &ReaderContext::KeyValue((Some(invalid_utf8_bytes.to_vec()), Some(invalid_utf8_bytes.to_vec()))),
        Box::new(parser),
        "received plaintext message is not in utf-8 format: invalid utf-8 sequence of 1 bytes from index 0",
    );

    Ok(())
}

#[test]
fn test_empty_payload() -> eyre::Result<()> {
    let parser = DebeziumMessageParser::new(
        Some(vec!["id".to_string()]),
        vec!["first_name".to_string()],
        "        ".to_string(),
    );

    assert_error_shown_for_reader_context(
        &ReaderContext::KeyValue((None, None)),
        Box::new(parser),
        "received message doesn't have payload",
    );

    Ok(())
}

#[test]
fn test_unsupported_context() -> eyre::Result<()> {
    let parser = DebeziumMessageParser::new(
        Some(vec!["id".to_string()]),
        vec!["first_name".to_string()],
        "        ".to_string(),
    );

    assert_error_shown_for_reader_context(
        &ReaderContext::TokenizedEntries(vec!["a".to_string()]),
        Box::new(parser),
        "internal error, reader context is not supported in this parser",
    );

    Ok(())
}

#[test]
fn test_incomplete_removal() -> eyre::Result<()> {
    let parser = IdentityParser::new();

    assert_error_shown_for_reader_context(
        &ReaderContext::Diff((false, None, Vec::new())),
        Box::new(parser),
        "received removal event without a key",
    );

    Ok(())
}
