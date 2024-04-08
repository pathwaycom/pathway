// Copyright © 2024 Pathway

use assert_matches::assert_matches;

use pathway_engine::connectors::data_format::{
    DsvFormatter, DsvSettings, Formatter, FormatterError,
};
use pathway_engine::engine::Value;
use pathway_engine::engine::{Key, Timestamp};

#[test]
fn test_dsv_format_ok() -> eyre::Result<()> {
    let mut formatter = DsvFormatter::new(DsvSettings::new(
        Some(vec!["a".to_string()]),
        vec!["b".to_string(), "c".to_string()],
        ';',
    ));

    let result = formatter.format(
        &Key::for_value(&Value::from("1")),
        &[Value::from("x"), Value::from("y")],
        Timestamp(0),
        1,
    )?;

    let target_payloads = vec![b"b;c;time;diff".to_vec(), b"\"x\";\"y\";0;1".to_vec()];

    assert_eq!(result.payloads, target_payloads);
    assert_eq!(result.values.len(), 0);

    Ok(())
}

#[test]
fn test_dsv_columns_mismatch() -> eyre::Result<()> {
    let mut formatter = DsvFormatter::new(DsvSettings::new(
        Some(vec!["a".to_string()]),
        vec!["b".to_string(), "c".to_string(), "d".to_string()],
        ';',
    ));

    let result = formatter.format(
        &Key::for_value(&Value::from("1")),
        &[Value::from("x"), Value::from("y")],
        Timestamp(0),
        1,
    );
    assert_matches!(result, Err(FormatterError::ColumnsValuesCountMismatch));

    Ok(())
}
