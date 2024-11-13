// Copyright © 2024 Pathway

use assert_matches::assert_matches;

use pathway_engine::connectors::data_format::{
    Formatter, PsqlSnapshotFormatter, PsqlSnapshotFormatterError,
};
use pathway_engine::engine::{Key, Timestamp, Value};

use super::helpers::assert_document_raw_byte_contents;

#[test]
fn test_psql_format_snapshot_commands() -> eyre::Result<()> {
    let mut formatter = PsqlSnapshotFormatter::new(
        "table_name".to_string(),
        vec!["key".to_string()],
        vec![
            "key".to_string(),
            "value_string".to_string(),
            "value_bool".to_string(),
            "value_float".to_string(),
        ],
    )?;

    let result = formatter.format(
        &Key::for_value(&Value::from("1")),
        &[
            Value::from("k"),
            Value::from("string"),
            Value::Bool(true),
            Value::from(1.23),
        ],
        Timestamp(5),
        1,
    )?;

    assert_eq!(result.payloads.len(), 1);
    assert_document_raw_byte_contents(
        &result.payloads[0],
        b"INSERT INTO table_name (key,value_string,value_bool,value_float,time,diff) VALUES ($1,$2,$3,$4,5,1) ON CONFLICT (key) DO UPDATE SET value_string=$2,value_bool=$3,value_float=$4,time=5,diff=1 WHERE table_name.key=$1\n"
    );
    assert_eq!(result.values.len(), 4);

    let result = formatter.format(
        &Key::for_value(&Value::from("1")),
        &[
            Value::from("k"),
            Value::from("string"),
            Value::Bool(true),
            Value::from(1.23),
        ],
        Timestamp(5),
        -1,
    )?;

    assert_eq!(result.payloads.len(), 1);
    assert_document_raw_byte_contents(
        &result.payloads[0],
        b"DELETE FROM table_name WHERE key=$1\n",
    );
    assert_eq!(result.values.len(), 1);

    Ok(())
}

#[test]
fn test_psql_primary_key_unspecified() -> eyre::Result<()> {
    let formatter = PsqlSnapshotFormatter::new(
        "table_name".to_string(),
        vec!["key".to_string()],
        vec![
            "value_string".to_string(),
            "value_bool".to_string(),
            "value_float".to_string(),
        ],
    );
    assert_matches!(formatter, Err(PsqlSnapshotFormatterError::UnknownKey(key)) if key == "key");
    Ok(())
}

#[test]
fn test_psql_format_snapshot_composite() -> eyre::Result<()> {
    let formatter = PsqlSnapshotFormatter::new(
        "table_name".to_string(),
        vec!["key".to_string(), "value_float".to_string()],
        vec![
            "key".to_string(),
            "value_string".to_string(),
            "value_bool".to_string(),
            "value_float".to_string(),
        ],
    );

    let result = formatter?.format(
        &Key::for_value(&Value::from("1")),
        &[
            Value::from("k"),
            Value::from("string"),
            Value::Bool(true),
            Value::from(1.23),
        ],
        Timestamp(5),
        1,
    )?;

    assert_eq!(result.payloads.len(), 1);
    assert_document_raw_byte_contents(
        &result.payloads[0],
        b"INSERT INTO table_name (key,value_string,value_bool,value_float,time,diff) VALUES ($1,$2,$3,$4,5,1) ON CONFLICT (key,value_float) DO UPDATE SET value_string=$2,value_bool=$3,time=5,diff=1 WHERE table_name.key=$1 AND table_name.value_float=$4\n"
    );
    assert_eq!(result.values.len(), 4);

    Ok(())
}
