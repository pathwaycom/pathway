use std::collections::HashMap;
use std::sync::mpsc;

use deltalake::arrow::array::RecordBatch as ArrowRecordBatch;
use serde_json::json;

use pathway_engine::connectors::data_format::FormatterContext;
use pathway_engine::connectors::data_lake::columns_into_pathway_values;
use pathway_engine::connectors::data_lake::LakeBatchWriter;
use pathway_engine::connectors::data_storage::{LakeWriter, WriteError, Writer};
use pathway_engine::engine::{
    DateTimeNaive, DateTimeUtc, Duration as EngineDuration, Key, Timestamp, Type, Value,
};
use pathway_engine::python_api::ValueField;

struct ArrowBatchWriter {
    sender: mpsc::Sender<ArrowRecordBatch>,
}

impl ArrowBatchWriter {
    fn new(sender: mpsc::Sender<ArrowRecordBatch>) -> Self {
        Self { sender }
    }
}

impl LakeBatchWriter for ArrowBatchWriter {
    fn write_batch(&mut self, batch: ArrowRecordBatch) -> Result<(), WriteError> {
        self.sender.send(batch).unwrap();
        Ok(())
    }
}

fn run_arrow_roadtrip(type_: Type, values: Vec<Value>) -> eyre::Result<()> {
    assert!(!values.is_empty());
    let value_field = ValueField {
        name: "value".to_string(),
        type_: type_.clone(),
        default: None,
    };
    let (sender, receiver) = mpsc::channel();
    let batch_writer = ArrowBatchWriter::new(sender);
    let value_fields = vec![value_field];
    let mut writer = LakeWriter::new(Box::new(batch_writer), &value_fields, None)?;

    for value in &values {
        writer.write(FormatterContext::new_single_payload(
            vec![],
            Key::random(),
            vec![value.clone()],
            Timestamp(0),
            0,
        ))?;
    }
    writer.flush(true)?;
    let formatted_batch = receiver.recv().unwrap();

    let mut column_types = HashMap::new();
    column_types.insert("value".to_string(), type_.clone());
    let values_maps_roundtrip = columns_into_pathway_values(&formatted_batch, &column_types);
    let mut values_roundtrip = Vec::new();
    for value_map in values_maps_roundtrip {
        let value = value_map.get("value").unwrap().clone().unwrap();
        values_roundtrip.push(value);
    }

    assert_eq!(values_roundtrip, values);

    if !matches!(type_, Type::Optional(_)) {
        // Check that the same process works when the type is optional.
        let mut values_with_nulls = values.clone();
        values_with_nulls.push(Value::None);
        run_arrow_roadtrip(Type::Optional(type_.clone().into()), values_with_nulls)?;
    }

    Ok(())
}

#[test]
fn test_format_int() -> eyre::Result<()> {
    run_arrow_roadtrip(
        Type::Int,
        vec![Value::Int(-1), Value::Int(0), Value::Int(123)],
    )
}

#[test]
fn test_format_string() -> eyre::Result<()> {
    run_arrow_roadtrip(
        Type::String,
        vec![
            Value::String("foo".to_string().into()),
            Value::String("bar".to_string().into()),
            Value::String("".to_string().into()),
        ],
    )
}

#[test]
fn test_format_bool() -> eyre::Result<()> {
    run_arrow_roadtrip(
        Type::Bool,
        vec![Value::Bool(true), Value::Bool(false), Value::Bool(true)],
    )
}

#[test]
fn test_format_bytes() -> eyre::Result<()> {
    run_arrow_roadtrip(
        Type::Bytes,
        vec![
            Value::Bytes("foo".as_bytes().to_vec().into()),
            Value::Bytes("bar".as_bytes().to_vec().into()),
            Value::Bytes("".as_bytes().to_vec().into()),
        ],
    )
}

#[test]
fn test_format_float() -> eyre::Result<()> {
    run_arrow_roadtrip(
        Type::Float,
        vec![
            Value::Float((-1.0).into()),
            Value::Float(0.0.into()),
            Value::Float(3.0.into()),
        ],
    )
}

#[test]
fn test_format_duration() -> eyre::Result<()> {
    run_arrow_roadtrip(
        Type::Duration,
        vec![
            Value::Duration(EngineDuration::new_with_unit(-1, "s")?),
            Value::Duration(EngineDuration::new_with_unit(2, "ms")?),
            Value::Duration(EngineDuration::new_with_unit(0, "ns")?),
        ],
    )
}

#[test]
fn test_save_datetimenaive() -> eyre::Result<()> {
    run_arrow_roadtrip(
        Type::DateTimeNaive,
        vec![
            Value::DateTimeNaive(DateTimeNaive::from_timestamp(0, "s")?),
            Value::DateTimeNaive(DateTimeNaive::from_timestamp(10000, "s")?),
            Value::DateTimeNaive(DateTimeNaive::from_timestamp(-10000, "s")?),
        ],
    )
}

#[test]
fn test_save_datetimeutc() -> eyre::Result<()> {
    run_arrow_roadtrip(
        Type::DateTimeUtc,
        vec![
            Value::DateTimeUtc(DateTimeUtc::new(0)),
            Value::DateTimeUtc(DateTimeUtc::new(10_000_000_000_000)),
            Value::DateTimeUtc(DateTimeUtc::new(-10_000_000_000_000)),
        ],
    )
}

#[test]
fn test_save_json() -> eyre::Result<()> {
    run_arrow_roadtrip(Type::Json, vec![Value::from(json!({"A": 100}))])
}
