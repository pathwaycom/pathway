use std::str::FromStr;

use pathway_engine::connectors::data_format::{Formatter, FormatterError, PsqlUpdatesFormatter};
use pathway_engine::connectors::data_storage::PsqlSerializer;
use pathway_engine::engine::{DateTimeNaive, DateTimeUtc, Duration, Key, Value};

#[test]
fn test_psql_columns_mismatch() -> eyre::Result<()> {
    let mut formatter = PsqlUpdatesFormatter::new(
        "table_name".to_string(),
        vec!["b".to_string(), "c".to_string(), "d".to_string()],
    );

    let result = formatter.format(
        &Key::from_str("1")?,
        &[Value::from_str("x")?, Value::from_str("y")?],
        0,
        1,
    );
    match result {
        Ok(_) => panic!("This formatting should have had ended with an error"),
        Err(err_type) => match err_type {
            FormatterError::ColumnsValuesCountMismatch => Ok(()),
            _ => panic!("Unexpected type of error"),
        },
    }
}

#[test]
fn test_psql_format_strings() -> eyre::Result<()> {
    let mut formatter = PsqlUpdatesFormatter::new(
        "table_name".to_string(),
        vec!["b".to_string(), "c".to_string()],
    );

    assert_eq!(Value::from_str("x")?.to_postgres_output(), "x".to_string());

    let result = formatter.format(
        &Key::from_str("1")?,
        &[Value::from_str("x")?, Value::from_str("y")?],
        0,
        1,
    )?;
    assert_eq!(
        result.payloads,
        vec![b"INSERT INTO table_name (b,c,time,diff) VALUES ($1,$2,0,1)\n"]
    );
    assert_eq!(result.values.len(), 2);

    Ok(())
}

#[test]
fn test_psql_format_null() -> eyre::Result<()> {
    let mut formatter =
        PsqlUpdatesFormatter::new("table_name".to_string(), vec!["column".to_string()]);

    assert_eq!(Value::None.to_postgres_output(), "null");
    {
        let result = formatter.format(&Key::from_str("1")?, &[Value::None], 0, 1)?;
        assert_eq!(
            result.payloads,
            vec![b"INSERT INTO table_name (column,time,diff) VALUES ($1,0,1)\n"]
        );
        assert_eq!(result.values.len(), 1);
    }

    Ok(())
}

#[test]
fn test_psql_format_bool() -> eyre::Result<()> {
    let mut formatter =
        PsqlUpdatesFormatter::new("table_name".to_string(), vec!["column".to_string()]);

    {
        assert_eq!(Value::Bool(true).to_postgres_output(), "t".to_string());
        let result = formatter.format(&Key::from_str("1")?, &[Value::Bool(true)], 0, 1)?;
        assert_eq!(
            result.payloads,
            vec![b"INSERT INTO table_name (column,time,diff) VALUES ($1,0,1)\n"]
        );
        assert_eq!(result.values.len(), 1);
    }
    {
        assert_eq!(Value::Bool(false).to_postgres_output(), "f".to_string());
        let result = formatter.format(&Key::from_str("1")?, &[Value::Bool(false)], 0, 1)?;
        assert_eq!(
            result.payloads,
            vec![b"INSERT INTO table_name (column,time,diff) VALUES ($1,0,1)\n"]
        );
        assert_eq!(result.values.len(), 1);
    }

    Ok(())
}

#[test]
fn test_psql_format_int() -> eyre::Result<()> {
    let mut formatter =
        PsqlUpdatesFormatter::new("table_name".to_string(), vec!["column".to_string()]);

    {
        assert_eq!(Value::Int(123).to_postgres_output(), "123".to_string());

        let result = formatter.format(&Key::from_str("1")?, &[Value::Int(123)], 0, 1)?;
        assert_eq!(
            result.payloads,
            vec![b"INSERT INTO table_name (column,time,diff) VALUES ($1,0,1)\n"]
        );
        assert_eq!(result.values.len(), 1);
    }
    {
        assert_eq!(Value::Int(-2).to_postgres_output(), "-2".to_string());

        let result = formatter.format(&Key::from_str("1")?, &[Value::Int(-2)], 0, 1)?;
        assert_eq!(
            result.payloads,
            vec![b"INSERT INTO table_name (column,time,diff) VALUES ($1,0,1)\n"]
        );
        assert_eq!(result.values.len(), 1);
    }
    {
        assert_eq!(Value::Int(0).to_postgres_output(), "0".to_string());

        let result = formatter.format(&Key::from_str("1")?, &[Value::Int(0)], 0, 1)?;
        assert_eq!(
            result.payloads,
            vec![b"INSERT INTO table_name (column,time,diff) VALUES ($1,0,1)\n"]
        );
        assert_eq!(result.values.len(), 1);
    }

    Ok(())
}

#[test]
fn test_psql_format_floats() -> eyre::Result<()> {
    let mut formatter =
        PsqlUpdatesFormatter::new("table_name".to_string(), vec!["column".to_string()]);

    {
        assert_eq!(Value::from(5.5).to_postgres_output(), "5.5".to_string());

        let result = formatter.format(&Key::from_str("1")?, &[Value::from(5.5)], 0, 1)?;
        assert_eq!(
            result.payloads,
            vec![b"INSERT INTO table_name (column,time,diff) VALUES ($1,0,1)\n"]
        );
        assert_eq!(result.values.len(), 1);
    }

    Ok(())
}

#[test]
fn test_psql_format_pointers() -> eyre::Result<()> {
    let mut formatter =
        PsqlUpdatesFormatter::new("table_name".to_string(), vec!["column".to_string()]);

    {
        let key = pathway_engine::engine::Key(1);

        assert_eq!(
            Value::Pointer(key).to_postgres_output(),
            "Key(1)".to_string()
        );

        let result = formatter.format(&Key::from_str("1")?, &[Value::Pointer(key)], 0, 1)?;
        assert_eq!(
            result.payloads,
            vec![b"INSERT INTO table_name (column,time,diff) VALUES ($1,0,1)\n"]
        );
        assert_eq!(result.values.len(), 1);
    }

    Ok(())
}

#[test]
fn test_psql_format_tuple() -> eyre::Result<()> {
    let mut formatter =
        PsqlUpdatesFormatter::new("table_name".to_string(), vec!["column".to_string()]);

    {
        let value1 = Value::Bool(true);
        let value2 = Value::None;
        let values = [value1, value2];

        let tuple_value = Value::from(values.as_slice());

        assert_eq!(tuple_value.to_postgres_output(), "{t,null}".to_string());
        let result = formatter.format(&Key::from_str("1")?, &[tuple_value], 0, 1)?;
        assert_eq!(
            result.payloads,
            vec![b"INSERT INTO table_name (column,time,diff) VALUES ($1,0,1)\n"]
        );
        assert_eq!(result.values.len(), 1);
    }

    Ok(())
}

#[test]
fn test_psql_format_date_time_naive() -> eyre::Result<()> {
    let mut formatter =
        PsqlUpdatesFormatter::new("table_name".to_string(), vec!["column".to_string()]);

    {
        assert_eq!(
            Value::DateTimeNaive(DateTimeNaive::new(1684147860000000000)).to_postgres_output(),
            "2023-05-15T10:51:00".to_string()
        );

        let result = formatter.format(
            &Key::from_str("1")?,
            &[Value::DateTimeNaive(DateTimeNaive::new(
                1684147860000000000,
            ))],
            0,
            1,
        )?;
        assert_eq!(
            result.payloads,
            vec![b"INSERT INTO table_name (column,time,diff) VALUES ($1,0,1)\n"]
        );
        assert_eq!(result.values.len(), 1);
    }
    {
        assert_eq!(
            Value::DateTimeNaive(DateTimeNaive::new(1684147883546378921)).to_postgres_output(),
            "2023-05-15T10:51:23.546378921".to_string()
        );

        let result = formatter.format(
            &Key::from_str("1")?,
            &[Value::DateTimeNaive(DateTimeNaive::new(
                1684147883546378921,
            ))],
            0,
            1,
        )?;
        assert_eq!(
            result.payloads,
            vec![b"INSERT INTO table_name (column,time,diff) VALUES ($1,0,1)\n"]
        );
        assert_eq!(result.values.len(), 1);
    }
    {
        assert_eq!(
            Value::DateTimeNaive(DateTimeNaive::new(0)).to_postgres_output(),
            "1970-01-01T00:00:00".to_string()
        );

        let result = formatter.format(
            &Key::from_str("1")?,
            &[Value::DateTimeNaive(DateTimeNaive::new(0))],
            0,
            1,
        )?;
        assert_eq!(
            result.payloads,
            vec![b"INSERT INTO table_name (column,time,diff) VALUES ($1,0,1)\n"]
        );
        assert_eq!(result.values.len(), 1);
    }

    Ok(())
}

#[test]
fn test_psql_format_date_time_utc() -> eyre::Result<()> {
    let mut formatter =
        PsqlUpdatesFormatter::new("table_name".to_string(), vec!["column".to_string()]);

    {
        assert_eq!(
            Value::DateTimeUtc(DateTimeUtc::new(1684147860000000000)).to_postgres_output(),
            "2023-05-15T10:51:00+0000".to_string()
        );

        let result = formatter.format(
            &Key::from_str("1")?,
            &[Value::DateTimeUtc(DateTimeUtc::new(1684147860000000000))],
            0,
            1,
        )?;
        assert_eq!(
            result.payloads,
            vec![b"INSERT INTO table_name (column,time,diff) VALUES ($1,0,1)\n"]
        );
        assert_eq!(result.values.len(), 1);
    }
    {
        assert_eq!(
            Value::DateTimeUtc(DateTimeUtc::new(1684147883546378921)).to_postgres_output(),
            "2023-05-15T10:51:23.546378921+0000".to_string()
        );

        let result = formatter.format(
            &Key::from_str("1")?,
            &[Value::DateTimeUtc(DateTimeUtc::new(1684147883546378921))],
            0,
            1,
        )?;
        assert_eq!(
            result.payloads,
            vec![b"INSERT INTO table_name (column,time,diff) VALUES ($1,0,1)\n"]
        );
        assert_eq!(result.values.len(), 1);
    }
    {
        assert_eq!(
            Value::DateTimeUtc(DateTimeUtc::new(0)).to_postgres_output(),
            "1970-01-01T00:00:00+0000".to_string()
        );

        let result = formatter.format(
            &Key::from_str("1")?,
            &[Value::DateTimeUtc(DateTimeUtc::new(0))],
            0,
            1,
        )?;
        assert_eq!(
            result.payloads,
            vec![b"INSERT INTO table_name (column,time,diff) VALUES ($1,0,1)\n"]
        );
        assert_eq!(result.values.len(), 1);
    }

    Ok(())
}

#[test]
fn test_psql_format_duration() -> eyre::Result<()> {
    let mut formatter =
        PsqlUpdatesFormatter::new("table_name".to_string(), vec!["column".to_string()]);

    {
        assert_eq!(
            Value::Duration(Duration::new(1197780000000000)).to_postgres_output(),
            "1197780000000000".to_string()
        );

        let result = formatter.format(
            &Key::from_str("1")?,
            &[Value::Duration(Duration::new(1197780000000000))],
            0,
            1,
        )?;
        assert_eq!(
            result.payloads,
            vec![b"INSERT INTO table_name (column,time,diff) VALUES ($1,0,1)\n"]
        );
        assert_eq!(result.values.len(), 1);
    }
    {
        assert_eq!(
            Value::Duration(Duration::new(-1197780000000000)).to_postgres_output(),
            "-1197780000000000".to_string()
        );

        let result = formatter.format(
            &Key::from_str("1")?,
            &[Value::Duration(Duration::new(-1197780000000000))],
            0,
            1,
        )?;
        assert_eq!(
            result.payloads,
            vec![b"INSERT INTO table_name (column,time,diff) VALUES ($1,0,1)\n"]
        );
        assert_eq!(result.values.len(), 1);
    }
    {
        assert_eq!(
            Value::Duration(Duration::new(0)).to_postgres_output(),
            "0".to_string()
        );

        let result = formatter.format(
            &Key::from_str("1")?,
            &[Value::Duration(Duration::new(0))],
            0,
            1,
        )?;
        assert_eq!(
            result.payloads,
            vec![b"INSERT INTO table_name (column,time,diff) VALUES ($1,0,1)\n"]
        );
        assert_eq!(result.values.len(), 1);
    }

    Ok(())
}
