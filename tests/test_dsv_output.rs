use std::str::FromStr;

use assert_matches::assert_matches;

use pathway_engine::connectors::data_format::{
    DsvFormatter, DsvSettings, Formatter, FormatterError,
};
use pathway_engine::engine::Key;
use pathway_engine::engine::Value;

#[test]
fn test_dsv_format_ok() -> eyre::Result<()> {
    let mut formatter = DsvFormatter::new(DsvSettings::new(
        Some(vec!["a".to_string()]),
        vec!["b".to_string(), "c".to_string()],
        ';',
    ));

    let result = formatter.format(
        &Key::from_str("1")?,
        &[Value::from_str("x")?, Value::from_str("y")?],
        0,
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
        &Key::from_str("1")?,
        &[Value::from_str("x")?, Value::from_str("y")?],
        0,
        1,
    );
    assert_matches!(result, Err(FormatterError::ColumnsValuesCountMismatch));

    Ok(())
}
