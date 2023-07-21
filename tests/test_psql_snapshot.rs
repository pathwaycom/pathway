use std::str::from_utf8;
use std::str::FromStr;

use assert_matches::assert_matches;

use pathway_engine::connectors::data_format::{
    Formatter, PsqlSnapshotFormatter, PsqlSnapshotFormatterError,
};
use pathway_engine::engine::{Key, Value};

#[test]
fn test_psql_format_snapshot_command() -> eyre::Result<()> {
    let formatter = PsqlSnapshotFormatter::new(
        "table_name".to_string(),
        vec!["key".to_string()],
        vec![
            "key".to_string(),
            "value_string".to_string(),
            "value_bool".to_string(),
            "value_float".to_string(),
        ],
    );

    let result = formatter?.format(
        &Key::from_str("1")?,
        &[
            Value::from_str("k")?,
            Value::from_str("string")?,
            Value::Bool(true),
            Value::from(1.23),
        ],
        5,
        -1,
    )?;

    assert_eq!(result.payloads.len(), 1);
    assert_eq!(
        "INSERT INTO table_name (key,value_string,value_bool,value_float,time,diff) VALUES ($1,$2,$3,$4,5,-1) ON CONFLICT (key) DO UPDATE SET value_string=$2,value_bool=$3,value_float=$4,time=5,diff=-1 WHERE table_name.key=$1 AND (table_name.time<5 OR (table_name.time=5 AND table_name.diff=-1))\n",
        from_utf8(&result.payloads[0])?
    );
    assert_eq!(result.values.len(), 4);

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
        &Key::from_str("1")?,
        &[
            Value::from_str("k")?,
            Value::from_str("string")?,
            Value::Bool(true),
            Value::from(1.23),
        ],
        5,
        1,
    )?;

    assert_eq!(result.payloads.len(), 1);
    assert_eq!(
        "INSERT INTO table_name (key,value_string,value_bool,value_float,time,diff) VALUES ($1,$2,$3,$4,5,1) ON CONFLICT (key,value_float) DO UPDATE SET value_string=$2,value_bool=$3,time=5,diff=1 WHERE table_name.key=$1 AND table_name.value_float=$4 AND (table_name.time<5 OR (table_name.time=5 AND table_name.diff=-1))\n",
        from_utf8(&result.payloads[0])?
    );
    assert_eq!(result.values.len(), 4);

    Ok(())
}
