// Copyright © 2024 Pathway

use pathway_engine::connectors::data_format::{Formatter, FormatterError, PsqlUpdatesFormatter};
use pathway_engine::engine::{DateTimeNaive, DateTimeUtc, Duration, Key, Timestamp, Value};

#[test]
fn test_psql_columns_mismatch() -> eyre::Result<()> {
    let mut formatter = PsqlUpdatesFormatter::new(
        "table_name".to_string(),
        vec!["b".to_string(), "c".to_string(), "d".to_string()],
    );

    let result = formatter.format(
        &Key::for_value(&Value::from("1")),
        &[Value::from("x"), Value::from("y")],
        Timestamp(0),
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

    let result = formatter.format(
        &Key::for_value(&Value::from("1")),
        &[Value::from("x"), Value::from("y")],
        Timestamp(0),
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

    {
        let result = formatter.format(
            &Key::for_value(&Value::from("1")),
            &[Value::None],
            Timestamp(0),
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
fn test_psql_format_bool() -> eyre::Result<()> {
    let mut formatter =
        PsqlUpdatesFormatter::new("table_name".to_string(), vec!["column".to_string()]);

    {
        let result = formatter.format(
            &Key::for_value(&Value::from("1")),
            &[Value::Bool(true)],
            Timestamp(0),
            1,
        )?;
        assert_eq!(
            result.payloads,
            vec![b"INSERT INTO table_name (column,time,diff) VALUES ($1,0,1)\n"]
        );
        assert_eq!(result.values.len(), 1);
    }
    {
        let result = formatter.format(
            &Key::for_value(&Value::from("1")),
            &[Value::Bool(false)],
            Timestamp(0),
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
fn test_psql_format_int() -> eyre::Result<()> {
    let mut formatter =
        PsqlUpdatesFormatter::new("table_name".to_string(), vec!["column".to_string()]);

    {
        let result = formatter.format(
            &Key::for_value(&Value::from("1")),
            &[Value::Int(123)],
            Timestamp(0),
            1,
        )?;
        assert_eq!(
            result.payloads,
            vec![b"INSERT INTO table_name (column,time,diff) VALUES ($1,0,1)\n"]
        );
        assert_eq!(result.values.len(), 1);
    }
    {
        let result = formatter.format(
            &Key::for_value(&Value::from("1")),
            &[Value::Int(-2)],
            Timestamp(0),
            1,
        )?;
        assert_eq!(
            result.payloads,
            vec![b"INSERT INTO table_name (column,time,diff) VALUES ($1,0,1)\n"]
        );
        assert_eq!(result.values.len(), 1);
    }
    {
        let result = formatter.format(
            &Key::for_value(&Value::from("1")),
            &[Value::Int(0)],
            Timestamp(0),
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
fn test_psql_format_floats() -> eyre::Result<()> {
    let mut formatter =
        PsqlUpdatesFormatter::new("table_name".to_string(), vec!["column".to_string()]);

    {
        let result = formatter.format(
            &Key::for_value(&Value::from("1")),
            &[Value::from(5.5)],
            Timestamp(0),
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
fn test_psql_format_pointers() -> eyre::Result<()> {
    let mut formatter =
        PsqlUpdatesFormatter::new("table_name".to_string(), vec!["column".to_string()]);

    {
        let key = pathway_engine::engine::Key(1);

        let result = formatter.format(
            &Key::for_value(&Value::from("1")),
            &[Value::Pointer(key)],
            Timestamp(0),
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
fn test_psql_format_tuple() -> eyre::Result<()> {
    let mut formatter =
        PsqlUpdatesFormatter::new("table_name".to_string(), vec!["column".to_string()]);

    {
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
        let result = formatter.format(
            &Key::for_value(&Value::from("1")),
            &[Value::DateTimeNaive(DateTimeNaive::new(
                1684147860000000000,
            ))],
            Timestamp(0),
            1,
        )?;
        assert_eq!(
            result.payloads,
            vec![b"INSERT INTO table_name (column,time,diff) VALUES ($1,0,1)\n"]
        );
        assert_eq!(result.values.len(), 1);
    }
    {
        let result = formatter.format(
            &Key::for_value(&Value::from("1")),
            &[Value::DateTimeNaive(DateTimeNaive::new(
                1684147883546378921,
            ))],
            Timestamp(0),
            1,
        )?;
        assert_eq!(
            result.payloads,
            vec![b"INSERT INTO table_name (column,time,diff) VALUES ($1,0,1)\n"]
        );
        assert_eq!(result.values.len(), 1);
    }
    {
        let result = formatter.format(
            &Key::for_value(&Value::from("1")),
            &[Value::DateTimeNaive(DateTimeNaive::new(0))],
            Timestamp(0),
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
        let result = formatter.format(
            &Key::for_value(&Value::from("1")),
            &[Value::DateTimeUtc(DateTimeUtc::new(1684147860000000000))],
            Timestamp(0),
            1,
        )?;
        assert_eq!(
            result.payloads,
            vec![b"INSERT INTO table_name (column,time,diff) VALUES ($1,0,1)\n"]
        );
        assert_eq!(result.values.len(), 1);
    }
    {
        let result = formatter.format(
            &Key::for_value(&Value::from("1")),
            &[Value::DateTimeUtc(DateTimeUtc::new(1684147883546378921))],
            Timestamp(0),
            1,
        )?;
        assert_eq!(
            result.payloads,
            vec![b"INSERT INTO table_name (column,time,diff) VALUES ($1,0,1)\n"]
        );
        assert_eq!(result.values.len(), 1);
    }
    {
        let result = formatter.format(
            &Key::for_value(&Value::from("1")),
            &[Value::DateTimeUtc(DateTimeUtc::new(0))],
            Timestamp(0),
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
        let result = formatter.format(
            &Key::for_value(&Value::from("1")),
            &[Value::Duration(Duration::new(1197780000000000))],
            Timestamp(0),
            1,
        )?;
        assert_eq!(
            result.payloads,
            vec![b"INSERT INTO table_name (column,time,diff) VALUES ($1,0,1)\n"]
        );
        assert_eq!(result.values.len(), 1);
    }
    {
        let result = formatter.format(
            &Key::for_value(&Value::from("1")),
            &[Value::Duration(Duration::new(-1197780000000000))],
            Timestamp(0),
            1,
        )?;
        assert_eq!(
            result.payloads,
            vec![b"INSERT INTO table_name (column,time,diff) VALUES ($1,0,1)\n"]
        );
        assert_eq!(result.values.len(), 1);
    }
    {
        let result = formatter.format(
            &Key::for_value(&Value::from("1")),
            &[Value::Duration(Duration::new(0))],
            Timestamp(0),
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
