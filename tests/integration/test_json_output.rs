// Copyright © 2024 Pathway

use pathway_engine::connectors::data_format::{Formatter, JsonLinesFormatter};
use pathway_engine::engine::DateTimeUtc;
use pathway_engine::engine::Duration;
use pathway_engine::engine::{DateTimeNaive, Timestamp};
use pathway_engine::engine::{Key, Value};

use super::helpers::assert_document_raw_byte_contents;

#[test]
fn test_json_format_ok() -> eyre::Result<()> {
    let mut formatter = JsonLinesFormatter::new(vec!["a".to_string()], None);

    let result = formatter.format(
        &Key::for_value(&Value::from("1")),
        &[Value::from("b")],
        Timestamp(0),
        1,
    )?;
    assert_eq!(result.payloads.len(), 1);
    assert_document_raw_byte_contents(
        &result.payloads[0],
        r#"{"a":"b","diff":1,"time":0}"#.as_bytes(),
    );
    assert_eq!(result.values, &[Value::from("b")]);

    Ok(())
}

#[test]
fn test_json_int_serialization() -> eyre::Result<()> {
    let mut formatter = JsonLinesFormatter::new(vec!["a".to_string()], None);

    let result = formatter.format(
        &Key::for_value(&Value::from("1")),
        &[Value::Int(555)],
        Timestamp(0),
        1,
    )?;
    assert_eq!(result.payloads.len(), 1);
    assert_document_raw_byte_contents(
        &result.payloads[0],
        r#"{"a":555,"diff":1,"time":0}"#.as_bytes(),
    );
    assert_eq!(result.values.len(), 1);

    Ok(())
}

#[test]
fn test_json_float_serialization() -> eyre::Result<()> {
    let mut formatter = JsonLinesFormatter::new(vec!["a".to_string()], None);

    let result = formatter.format(
        &Key::for_value(&Value::from("1")),
        &[Value::Float(5.55.into())],
        Timestamp(0),
        1,
    )?;
    assert_eq!(result.payloads.len(), 1);
    assert_document_raw_byte_contents(
        &result.payloads[0],
        r#"{"a":5.55,"diff":1,"time":0}"#.as_bytes(),
    );
    assert_eq!(result.values.len(), 1);

    Ok(())
}

#[test]
fn test_json_bool_serialization() -> eyre::Result<()> {
    let mut formatter = JsonLinesFormatter::new(vec!["a".to_string()], None);

    let result = formatter.format(
        &Key::for_value(&Value::from("1")),
        &[Value::Bool(true)],
        Timestamp(0),
        1,
    )?;
    assert_eq!(result.payloads.len(), 1);
    assert_document_raw_byte_contents(
        &result.payloads[0],
        r#"{"a":true,"diff":1,"time":0}"#.as_bytes(),
    );
    assert_eq!(result.values.len(), 1);

    Ok(())
}

#[test]
fn test_json_null_serialization() -> eyre::Result<()> {
    let mut formatter = JsonLinesFormatter::new(vec!["a".to_string()], None);

    let result = formatter.format(
        &Key::for_value(&Value::from("1")),
        &[Value::None],
        Timestamp(0),
        1,
    )?;
    assert_eq!(result.payloads.len(), 1);
    assert_document_raw_byte_contents(
        &result.payloads[0],
        r#"{"a":null,"diff":1,"time":0}"#.as_bytes(),
    );
    assert_eq!(result.values.len(), 1);

    Ok(())
}

#[test]
fn test_json_pointer_serialization() -> eyre::Result<()> {
    let mut formatter = JsonLinesFormatter::new(vec!["a".to_string()], None);

    let key = pathway_engine::engine::Key(1);
    let result = formatter.format(
        &Key::for_value(&Value::from("1")),
        &[Value::Pointer(key)],
        Timestamp(0),
        1,
    )?;
    assert_eq!(result.payloads.len(), 1);
    assert_document_raw_byte_contents(
        &result.payloads[0],
        r#"{"a":"^04000000000000000000000000","diff":1,"time":0}"#.as_bytes(),
    );
    assert_eq!(result.values.len(), 1);

    Ok(())
}

#[test]
fn test_json_tuple_serialization() -> eyre::Result<()> {
    let mut formatter = JsonLinesFormatter::new(vec!["a".to_string()], None);

    let value1 = Value::Bool(true);
    let value2 = Value::None;
    let values = [value1, value2];
    let tuple_value = Value::from(values.as_slice());

    let result = formatter.format(
        &Key::for_value(&Value::from("1")),
        &[tuple_value],
        Timestamp(0),
        1,
    )?;
    assert_eq!(result.payloads.len(), 1);
    assert_document_raw_byte_contents(
        &result.payloads[0],
        r#"{"a":[true,null],"diff":1,"time":0}"#.as_bytes(),
    );
    assert_eq!(result.values.len(), 1);

    Ok(())
}

#[test]
fn test_json_date_time_naive_serialization() -> eyre::Result<()> {
    let mut formatter = JsonLinesFormatter::new(vec!["a".to_string()], None);

    let result = formatter.format(
        &Key::for_value(&Value::from("1")),
        &[Value::DateTimeNaive(DateTimeNaive::new(
            1684147860000000000,
        ))],
        Timestamp(0),
        1,
    )?;
    assert_eq!(result.payloads.len(), 1);
    assert_document_raw_byte_contents(
        &result.payloads[0],
        r#"{"a":"2023-05-15T10:51:00.000000000","diff":1,"time":0}"#.as_bytes(),
    );
    assert_eq!(result.values.len(), 1);

    Ok(())
}

#[test]
fn test_json_date_time_utc_serialization() -> eyre::Result<()> {
    let mut formatter = JsonLinesFormatter::new(vec!["a".to_string()], None);

    let result = formatter.format(
        &Key::for_value(&Value::from("1")),
        &[Value::DateTimeUtc(DateTimeUtc::new(1684147860000000000))],
        Timestamp(0),
        1,
    )?;
    assert_eq!(result.payloads.len(), 1);
    assert_document_raw_byte_contents(
        &result.payloads[0],
        r#"{"a":"2023-05-15T10:51:00.000000000+0000","diff":1,"time":0}"#.as_bytes(),
    );
    assert_eq!(result.values.len(), 1);

    Ok(())
}

#[test]
fn test_json_duration_serialization() -> eyre::Result<()> {
    //The test won't work if the server summer time is different than UTC+2
    let mut formatter = JsonLinesFormatter::new(vec!["a".to_string()], None);

    let result = formatter.format(
        &Key::for_value(&Value::from("1")),
        &[Value::Duration(Duration::new(1197780000000000))],
        Timestamp(0),
        1,
    )?;
    assert_eq!(result.payloads.len(), 1);
    assert_document_raw_byte_contents(
        &result.payloads[0],
        r#"{"a":1197780000000000,"diff":1,"time":0}"#.as_bytes(),
    );
    assert_eq!(result.values.len(), 1);

    Ok(())
}

#[test]
fn test_json_format_timestamps() -> eyre::Result<()> {
    let mut formatter = JsonLinesFormatter::new(vec!["utc".to_string(), "naive".to_string()], None);

    let result = formatter.format(
        &Key::for_value(&Value::from("1")),
        &[
            Value::DateTimeUtc(DateTimeUtc::from_timestamp(1738686506812, "ms")?),
            Value::DateTimeNaive(DateTimeNaive::from_timestamp(1738686506812, "ms")?),
        ],
        Timestamp(0),
        1,
    )?;
    assert_eq!(result.payloads.len(), 1);
    assert_document_raw_byte_contents(
        &result.payloads[0],
        r#"{"utc":"2025-02-04T16:28:26.812000000+0000","naive":"2025-02-04T16:28:26.812000000","diff":1,"time":0}"#.as_bytes(),
    );

    Ok(())
}
