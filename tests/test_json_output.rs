use std::str::from_utf8;
use std::str::FromStr;

use pathway_engine::connectors::data_format::{Formatter, JsonLinesFormatter};
use pathway_engine::engine::DateTimeNaive;
use pathway_engine::engine::DateTimeUtc;
use pathway_engine::engine::Duration;
use pathway_engine::engine::{Key, Value};

#[test]
fn test_json_format_ok() -> eyre::Result<()> {
    let mut formatter = JsonLinesFormatter::new(vec!["a".to_string()]);

    let result = formatter.format(&Key::from_str("1")?, &[Value::from_str("b")?], 0, 1)?;
    assert_eq!(result.payloads.len(), 1);
    assert_eq!(
        from_utf8(&result.payloads[0])?,
        r#"{"a":"b","diff":1,"time":0}"#
    );
    assert_eq!(result.values.len(), 0);

    Ok(())
}

#[test]
fn test_json_int_serialization() -> eyre::Result<()> {
    let mut formatter = JsonLinesFormatter::new(vec!["a".to_string()]);

    let result = formatter.format(&Key::from_str("1")?, &[Value::Int(555)], 0, 1)?;
    assert_eq!(result.payloads.len(), 1);
    assert_eq!(
        from_utf8(&result.payloads[0])?,
        r#"{"a":555,"diff":1,"time":0}"#
    );
    assert_eq!(result.values.len(), 0);

    Ok(())
}

#[test]
fn test_json_float_serialization() -> eyre::Result<()> {
    let mut formatter = JsonLinesFormatter::new(vec!["a".to_string()]);

    let result = formatter.format(&Key::from_str("1")?, &[Value::Float(5.55.into())], 0, 1)?;
    assert_eq!(result.payloads.len(), 1);
    assert_eq!(
        from_utf8(&result.payloads[0])?,
        r#"{"a":5.55,"diff":1,"time":0}"#
    );
    assert_eq!(result.values.len(), 0);

    Ok(())
}

#[test]
fn test_json_bool_serialization() -> eyre::Result<()> {
    let mut formatter = JsonLinesFormatter::new(vec!["a".to_string()]);

    let result = formatter.format(&Key::from_str("1")?, &[Value::Bool(true)], 0, 1)?;
    assert_eq!(result.payloads.len(), 1);
    assert_eq!(
        from_utf8(&result.payloads[0])?,
        r#"{"a":true,"diff":1,"time":0}"#
    );
    assert_eq!(result.values.len(), 0);

    Ok(())
}

#[test]
fn test_json_null_serialization() -> eyre::Result<()> {
    let mut formatter = JsonLinesFormatter::new(vec!["a".to_string()]);

    let result = formatter.format(&Key::from_str("1")?, &[Value::None], 0, 1)?;
    assert_eq!(result.payloads.len(), 1);
    assert_eq!(
        from_utf8(&result.payloads[0])?,
        r#"{"a":null,"diff":1,"time":0}"#
    );
    assert_eq!(result.values.len(), 0);

    Ok(())
}

#[test]
fn test_json_pointer_serialization() -> eyre::Result<()> {
    let mut formatter = JsonLinesFormatter::new(vec!["a".to_string()]);

    let key = pathway_engine::engine::Key(1);
    let result = formatter.format(&Key::from_str("1")?, &[Value::Pointer(key)], 0, 1)?;
    assert_eq!(result.payloads.len(), 1);
    assert_eq!(
        from_utf8(&result.payloads[0])?,
        r#"{"a":"Key(1)","diff":1,"time":0}"#
    );
    assert_eq!(result.values.len(), 0);

    Ok(())
}

#[test]
fn test_json_tuple_serialization() -> eyre::Result<()> {
    let mut formatter = JsonLinesFormatter::new(vec!["a".to_string()]);

    let value1 = Value::Bool(true);
    let value2 = Value::None;
    let values = [value1, value2];
    let tuple_value = Value::from(values.as_slice());

    let result = formatter.format(&Key::from_str("1")?, &[tuple_value], 0, 1)?;
    assert_eq!(result.payloads.len(), 1);
    assert_eq!(
        from_utf8(&result.payloads[0])?,
        r#"{"a":[true,null],"diff":1,"time":0}"#
    );
    assert_eq!(result.values.len(), 0);

    Ok(())
}

#[test]
fn test_json_date_time_naive_serialization() -> eyre::Result<()> {
    let mut formatter = JsonLinesFormatter::new(vec!["a".to_string()]);

    let result = formatter.format(
        &Key::from_str("1")?,
        &[Value::DateTimeNaive(DateTimeNaive::new(
            1684147860000000000,
        ))],
        0,
        1,
    )?;
    assert_eq!(result.payloads.len(), 1);
    assert_eq!(
        from_utf8(&result.payloads[0])?,
        r#"{"a":"2023-05-15T10:51:00","diff":1,"time":0}"#
    );
    assert_eq!(result.values.len(), 0);

    Ok(())
}

#[test]
fn test_json_date_time_utc_serialization() -> eyre::Result<()> {
    let mut formatter = JsonLinesFormatter::new(vec!["a".to_string()]);

    let result = formatter.format(
        &Key::from_str("1")?,
        &[Value::DateTimeUtc(DateTimeUtc::new(1684147860000000000))],
        0,
        1,
    )?;
    assert_eq!(result.payloads.len(), 1);
    assert_eq!(
        from_utf8(&result.payloads[0])?,
        r#"{"a":"2023-05-15T10:51:00+0000","diff":1,"time":0}"#
    );
    assert_eq!(result.values.len(), 0);

    Ok(())
}

#[test]
fn test_json_duration_serialization() -> eyre::Result<()> {
    //The test won't work if the server summer time is different than UTC+2
    let mut formatter = JsonLinesFormatter::new(vec!["a".to_string()]);

    let result = formatter.format(
        &Key::from_str("1")?,
        &[Value::Duration(Duration::new(1197780000000000))],
        0,
        1,
    )?;
    assert_eq!(result.payloads.len(), 1);
    assert_eq!(
        from_utf8(&result.payloads[0])?,
        r#"{"a":1197780000000000,"diff":1,"time":0}"#
    );
    assert_eq!(result.values.len(), 0);

    Ok(())
}
