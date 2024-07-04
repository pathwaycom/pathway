// Copyright © 2024 Pathway

use std::collections::HashMap;
use std::path::Path;

use assert_matches::assert_matches;
use deltalake::datafusion::parquet::file::reader::SerializedFileReader;
use deltalake::datafusion::parquet::record::Field as ParquetField;
use serde_json::json;
use tempfile::tempdir;

use pathway_engine::connectors::data_format::{
    Formatter, IdentityFormatter, InnerSchemaField, ParsedEvent, TransparentParser,
};
use pathway_engine::connectors::data_storage::{
    ConnectorMode, DeltaTableReader, DeltaTableWriter, ObjectDownloader, WriteError, Writer,
};
use pathway_engine::connectors::SessionType;
use pathway_engine::engine::{DateTimeNaive, DateTimeUtc, Duration, Key, Timestamp, Type, Value};
use pathway_engine::python_api::ValueField;

use crate::helpers::read_data_from_reader;

fn run_single_column_save(type_: Type, values: &[Value]) -> Result<(), WriteError> {
    let test_storage = tempdir().expect("tempdir creation failed");
    let test_storage_path = test_storage.path();

    let value_fields = vec![ValueField {
        name: "field".to_string(),
        type_,
        is_optional: true,
        default: None,
    }];

    let mut writer = DeltaTableWriter::new(
        test_storage_path.to_str().unwrap(),
        &value_fields,
        HashMap::new(),
        None,
    )?;
    let mut formatter = IdentityFormatter::new();

    for value in values {
        let context = formatter
            .format(&Key::random(), &[value.clone()], Timestamp(0), 1)
            .expect("formatter failed");
        writer.write(context)?;
    }
    writer.flush(true)?;
    let rows_present = read_from_deltalake(test_storage_path.to_str().unwrap(), type_);
    assert_eq!(rows_present, values);

    let rows_roundtrip = read_with_connector(test_storage_path.to_str().unwrap(), type_);
    assert_eq!(rows_roundtrip, values);

    Ok(())
}

fn read_with_connector(path: &str, type_: Type) -> Vec<Value> {
    let mut schema = HashMap::new();
    schema.insert(
        "field".to_string(),
        InnerSchemaField::new(type_, true, None),
    );
    let mut type_map = HashMap::new();
    type_map.insert("field".to_string(), type_);
    let reader = DeltaTableReader::new(
        path,
        ObjectDownloader::Local,
        HashMap::new(),
        type_map,
        ConnectorMode::Static,
        None,
    )
    .unwrap();
    let parser =
        TransparentParser::new(None, vec!["field".to_string()], schema, SessionType::Native);
    let values_read = read_data_from_reader(Box::new(reader), Box::new(parser)).unwrap();
    let mut result = Vec::new();
    for event in values_read {
        let ParsedEvent::Insert((_key, values)) = event else {
            panic!("Unexpected event type: {event:?}")
        };
        assert_eq!(values.len(), 1);
        let value = values[0].clone();
        result.push(value);
    }
    result
}

fn read_from_deltalake(path: &str, type_: Type) -> Vec<Value> {
    let mut reread_values = Vec::new();
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let table = deltalake::open_table(path).await.unwrap();
            let mut parquet_files = Vec::<String>::new();
            for file_name in table.get_file_uris().unwrap() {
                let full_path = Path::new(path).join(file_name);
                parquet_files.push(full_path.to_string_lossy().to_string());
            }
            let rows = parquet_files
                .iter()
                .map(|p| SerializedFileReader::try_from(Path::new(p)).unwrap())
                .flat_map(|r| r.into_iter());
            for row in rows {
                let mut has_time_column = false;
                let mut has_diff_column = false;
                for (name, field) in row.expect("row reading failed").get_column_iter() {
                    if name == "time" {
                        has_time_column = true;
                    }
                    if name == "diff" {
                        has_diff_column = true;
                    }
                    if name != "field" {
                        continue;
                    }
                    let parsed_value = match (field, type_) {
                        (ParquetField::Null, _) => Value::None,
                        (ParquetField::Bool(b), Type::Bool) => Value::from(*b),
                        (ParquetField::Long(i), Type::Int) => Value::from(*i),
                        (ParquetField::Long(i), Type::Duration) => Value::from(Duration::new_with_unit(*i, "us").unwrap()),
                        (ParquetField::Double(f), Type::Float) => Value::Float((*f).into()),
                        (ParquetField::Str(s), Type::String) => Value::String(s.into()),
                        (ParquetField::Str(s), Type::Json) => {
                            let json: serde_json::Value = serde_json::from_str(s).unwrap();
                            Value::from(json)
                        },
                        (ParquetField::TimestampMicros(us), Type::DateTimeNaive) => Value::from(DateTimeNaive::from_timestamp(*us, "us").unwrap()),
                        (ParquetField::TimestampMicros(us), Type::DateTimeUtc) => Value::from(DateTimeUtc::from_timestamp(*us, "us").unwrap()),
                        (ParquetField::Bytes(b), Type::Bytes) => Value::Bytes(b.data().into()),
                        _ => panic!("Pathway shouldn't have serialized field of type {type_:?} as {field:?}"),
                    };
                    reread_values.push(parsed_value);
                }
                assert!(has_time_column && has_diff_column);
            }
        });

    reread_values
}

#[test]
fn test_save_bool() -> eyre::Result<()> {
    Ok(run_single_column_save(
        Type::Bool,
        &[Value::Bool(true), Value::Bool(false)],
    )?)
}

#[test]
fn test_save_int() -> eyre::Result<()> {
    Ok(run_single_column_save(Type::Int, &[Value::Int(10)])?)
}

#[test]
fn test_save_float() -> eyre::Result<()> {
    Ok(run_single_column_save(
        Type::Float,
        &[Value::Float(0.01.into())],
    )?)
}

#[test]
fn test_save_string() -> eyre::Result<()> {
    Ok(run_single_column_save(
        Type::String,
        &[Value::String("abc".into()), Value::String("".into())],
    )?)
}

#[test]
fn test_save_bytes() -> eyre::Result<()> {
    let test_bytes: &[u8] = &[1, 10, 5, 19, 55, 67, 9, 87, 28];
    Ok(run_single_column_save(
        Type::Bytes,
        &[Value::Bytes([].into()), Value::Bytes(test_bytes.into())],
    )?)
}

#[test]
fn test_save_datetimenaive() -> eyre::Result<()> {
    Ok(run_single_column_save(
        Type::DateTimeNaive,
        &[
            Value::DateTimeNaive(DateTimeNaive::from_timestamp(0, "s")?),
            Value::DateTimeNaive(DateTimeNaive::from_timestamp(10000, "s")?),
            Value::DateTimeNaive(DateTimeNaive::from_timestamp(-10000, "s")?),
        ],
    )?)
}

#[test]
fn test_save_datetimeutc() -> eyre::Result<()> {
    Ok(run_single_column_save(
        Type::DateTimeUtc,
        &[
            Value::DateTimeUtc(DateTimeUtc::new(0)),
            Value::DateTimeUtc(DateTimeUtc::new(10_000_000_000_000)),
            Value::DateTimeUtc(DateTimeUtc::new(-10_000_000_000_000)),
        ],
    )?)
}

#[test]
fn test_save_duration() -> eyre::Result<()> {
    Ok(run_single_column_save(
        Type::Duration,
        &[
            Value::Duration(Duration::new(0)),
            Value::Duration(Duration::new(10_000_000_000_000)),
            Value::Duration(Duration::new(-10_000_000_000_000)),
        ],
    )?)
}

#[test]
fn test_save_json() -> eyre::Result<()> {
    Ok(run_single_column_save(
        Type::Json,
        &[Value::from(json!({"A": 100}))],
    )?)
}

#[test]
fn test_unsupported_types_fail_as_expected() -> eyre::Result<()> {
    let unsupported_types = &[
        Type::Any,
        Type::Array,
        Type::PyObjectWrapper,
        Type::Tuple,
        Type::Pointer,
    ];
    for t in unsupported_types {
        let save_result = run_single_column_save(*t, &[]);
        assert_matches!(save_result, Err(WriteError::UnsupportedType(_)));
    }
    Ok(())
}
