use std::collections::HashMap;
use std::sync::Arc;

use deltalake::arrow::array::types::{
    DurationMicrosecondType, DurationMillisecondType, DurationNanosecondType, DurationSecondType,
    Float16Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type,
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType, UInt16Type, UInt32Type, UInt8Type,
};
use deltalake::arrow::array::{
    Array as ArrowArray, ArrowPrimitiveType, AsArray, RecordBatch as ArrowRecordBatch,
};
use deltalake::arrow::datatypes::{DataType as ArrowDataType, TimeUnit as ArrowTimeUnit};
use deltalake::datafusion::parquet::record::Field as ParquetValue;
use deltalake::parquet::record::Row as ParquetRow;
use half::f16;

use crate::connectors::data_storage::ConversionError;
use crate::connectors::data_storage::ValuesMap;
use crate::connectors::WriteError;
use crate::engine::error::{limit_length, STANDARD_OBJECT_LENGTH_LIMIT};
use crate::engine::{DateTimeNaive, DateTimeUtc, Duration as EngineDuration, Type, Value};

pub mod delta;
pub mod iceberg;
pub mod writer;

pub use delta::DeltaBatchWriter;
pub use iceberg::IcebergBatchWriter;
pub use writer::LakeWriter;

const SPECIAL_OUTPUT_FIELDS: [(&str, Type); 2] = [("time", Type::Int), ("diff", Type::Int)];

pub trait LakeBatchWriter: Send {
    fn write_batch(&mut self, batch: ArrowRecordBatch) -> Result<(), WriteError>;
}

// Commonly used routines for converting Parquet and Arrow data into Pathway values.

pub fn parquet_row_into_values_map<S: ::std::hash::BuildHasher>(
    parquet_row: &ParquetRow,
    column_types: &HashMap<String, Type, S>,
) -> ValuesMap {
    let mut row_map = HashMap::new();
    for (name, parquet_value) in parquet_row.get_column_iter() {
        let Some(expected_type) = column_types.get(name) else {
            // Column outside of the user-provided schema
            continue;
        };

        let value = match (parquet_value, expected_type) {
            (ParquetValue::Null, _) => Some(Value::None),
            (ParquetValue::Bool(b), Type::Bool | Type::Any) => Some(Value::from(*b)),
            (ParquetValue::Long(i), Type::Int | Type::Any) => Some(Value::from(*i)),
            (ParquetValue::Long(i), Type::Duration) => Some(Value::from(
                EngineDuration::new_with_unit(*i, "us").unwrap(),
            )),
            (ParquetValue::Double(f), Type::Float | Type::Any) => Some(Value::Float((*f).into())),
            (ParquetValue::Str(s), Type::String | Type::Any) => Some(Value::String(s.into())),
            (ParquetValue::Str(s), Type::Json) => serde_json::from_str::<serde_json::Value>(s)
                .ok()
                .map(Value::from),
            (ParquetValue::TimestampMicros(us), Type::DateTimeNaive | Type::Any) => Some(
                Value::from(DateTimeNaive::from_timestamp(*us, "us").unwrap()),
            ),
            (ParquetValue::TimestampMicros(us), Type::DateTimeUtc) => {
                Some(Value::from(DateTimeUtc::from_timestamp(*us, "us").unwrap()))
            }
            (ParquetValue::Bytes(b), Type::Bytes | Type::Any) => {
                Some(Value::Bytes(b.data().into()))
            }
            _ => None,
        };
        let value = if let Some(value) = value {
            Ok(value)
        } else {
            let value_repr =
                limit_length(format!("{parquet_value:?}"), STANDARD_OBJECT_LENGTH_LIMIT);
            Err(Box::new(ConversionError {
                value_repr,
                field_name: name.clone(),
                type_: expected_type.clone(),
            }))
        };
        row_map.insert(name.clone(), value);
    }

    row_map.into()
}

pub fn columns_into_pathway_values<S: ::std::hash::BuildHasher>(
    entry: &ArrowRecordBatch,
    column_types: &HashMap<String, Type, S>,
) -> Vec<ValuesMap> {
    let rows_count = entry.num_rows();
    let mut result = vec![HashMap::new(); rows_count];

    for (column_name, expected_type) in column_types {
        let Some(column) = entry.column_by_name(column_name) else {
            continue;
        };
        let arrow_type = column.data_type();
        let values_vector = match (arrow_type, expected_type.unoptionalize()) {
            (ArrowDataType::Null, _) => vec![Ok(Value::None); rows_count],
            (ArrowDataType::Int64, Type::Int | Type::Any) => {
                convert_arrow_array::<i64, Int64Type>(column, |v| Ok(Value::Int(v)))
            }
            (ArrowDataType::Int32, Type::Int | Type::Any) => {
                convert_arrow_array::<i32, Int32Type>(column, |v| Ok(Value::Int(v.into())))
            }
            (ArrowDataType::Int16, Type::Int | Type::Any) => {
                convert_arrow_array::<i16, Int16Type>(column, |v| Ok(Value::Int(v.into())))
            }
            (ArrowDataType::Int8, Type::Int | Type::Any) => {
                convert_arrow_array::<i8, Int8Type>(column, |v| Ok(Value::Int(v.into())))
            }
            (ArrowDataType::UInt32, Type::Int | Type::Any) => {
                convert_arrow_array::<u32, UInt32Type>(column, |v| Ok(Value::Int(v.into())))
            }
            (ArrowDataType::UInt16, Type::Int | Type::Any) => {
                convert_arrow_array::<u16, UInt16Type>(column, |v| Ok(Value::Int(v.into())))
            }
            (ArrowDataType::UInt8, Type::Int | Type::Any) => {
                convert_arrow_array::<u8, UInt8Type>(column, |v| Ok(Value::Int(v.into())))
            }
            (ArrowDataType::Float64, Type::Float | Type::Any) => {
                convert_arrow_array::<f64, Float64Type>(column, |v| Ok(Value::Float(v.into())))
            }
            (ArrowDataType::Float32, Type::Float | Type::Any) => {
                convert_arrow_array::<f32, Float32Type>(column, |v| {
                    Ok(Value::Float(Into::<f64>::into(v).into()))
                })
            }
            (ArrowDataType::Float16, Type::Float | Type::Any) => {
                convert_arrow_array::<f16, Float16Type>(column, |v| {
                    Ok(Value::Float(Into::<f64>::into(v).into()))
                })
            }
            (ArrowDataType::Boolean, Type::Bool | Type::Any) => convert_arrow_boolean_array(column),
            (ArrowDataType::Utf8, Type::String | Type::Any) => convert_arrow_string_array(column),
            (ArrowDataType::Utf8, Type::Json) => {
                convert_arrow_json_array(column, column_name, expected_type)
            }
            (ArrowDataType::Binary, Type::Bytes | Type::Any) => convert_arrow_bytes_array(column),
            (ArrowDataType::Duration(time_unit), Type::Duration | Type::Any) => {
                convert_arrow_duration_array(column, *time_unit)
            }
            (ArrowDataType::Int64, Type::Duration) => {
                // Compatibility clause: there is no duration type in Delta Lake,
                // so int64 is used to store duration.
                // Since the timestamp types in DeltaLake are stored in microseconds,
                // we need to convert the duration to microseconds.
                convert_arrow_array::<i64, Int64Type>(column, |v| {
                    Ok(Value::Duration(
                        EngineDuration::new_with_unit(v, "us").unwrap(),
                    ))
                })
            }
            (ArrowDataType::Timestamp(time_unit, None), Type::DateTimeNaive | Type::Any) => {
                convert_arrow_timestamp_array_naive(column, *time_unit)
            }
            (
                ArrowDataType::Timestamp(time_unit, Some(timezone)),
                Type::DateTimeUtc | Type::Any,
            ) => convert_arrow_timestamp_array_utc(column, *time_unit, timezone.as_ref()),
            (arrow_type, expected_type) => {
                vec![
                    Err(Box::new(ConversionError {
                        value_repr: format!("{arrow_type:?}"),
                        field_name: column_name.clone(),
                        type_: expected_type.clone(),
                    }));
                    rows_count
                ]
            }
        };
        let is_optional = matches!(expected_type, Type::Optional(_));
        for (index, value) in values_vector.into_iter().enumerate() {
            let prepared_value = if value == Ok(Value::None) && !is_optional {
                Err(Box::new(ConversionError {
                    value_repr: "null".to_string(),
                    field_name: column_name.clone(),
                    type_: expected_type.clone(),
                }))
            } else {
                value
            };
            result[index].insert(column_name.clone(), prepared_value);
        }
    }

    result.into_iter().map(std::convert::Into::into).collect()
}

type ParsedValue = Result<Value, Box<ConversionError>>;

fn convert_arrow_array<N, T: ArrowPrimitiveType<Native = N>>(
    column: &Arc<dyn ArrowArray>,
    mut to_value: impl FnMut(N) -> ParsedValue,
) -> Vec<ParsedValue> {
    let values = column.as_primitive::<T>();
    values
        .into_iter()
        .map(|v| match v {
            Some(v) => to_value(v),
            None => Ok(Value::None),
        })
        .collect()
}

fn convert_arrow_json_array(
    column: &Arc<dyn ArrowArray>,
    name: &str,
    expected_type: &Type,
) -> Vec<ParsedValue> {
    column
        .as_string::<i32>()
        .into_iter()
        .map(|v| match v {
            Some(v) => serde_json::from_str::<serde_json::Value>(v)
                .map(Value::from)
                .map_err(|_| {
                    Box::new(ConversionError {
                        value_repr: limit_length(v.to_string(), STANDARD_OBJECT_LENGTH_LIMIT),
                        field_name: name.to_string(),
                        type_: expected_type.clone(),
                    })
                }),
            None => Ok(Value::None),
        })
        .collect()
}

fn convert_arrow_string_array(column: &Arc<dyn ArrowArray>) -> Vec<ParsedValue> {
    column
        .as_string::<i32>()
        .into_iter()
        .map(|v| match v {
            Some(v) => Ok(Value::String(v.into())),
            None => Ok(Value::None),
        })
        .collect()
}

fn convert_arrow_boolean_array(column: &Arc<dyn ArrowArray>) -> Vec<ParsedValue> {
    column
        .as_boolean()
        .into_iter()
        .map(|v| match v {
            Some(v) => Ok(Value::Bool(v)),
            None => Ok(Value::None),
        })
        .collect()
}

fn convert_arrow_bytes_array(column: &Arc<dyn ArrowArray>) -> Vec<ParsedValue> {
    column
        .as_binary::<i32>()
        .into_iter()
        .map(|v| match v {
            Some(v) => Ok(Value::Bytes(v.into())),
            None => Ok(Value::None),
        })
        .collect()
}

fn convert_arrow_duration_array(
    column: &Arc<dyn ArrowArray>,
    time_unit: ArrowTimeUnit,
) -> Vec<ParsedValue> {
    match time_unit {
        ArrowTimeUnit::Second => convert_arrow_array::<i64, DurationSecondType>(column, |v| {
            Ok(Value::from(EngineDuration::new_with_unit(v, "s").unwrap()))
        }),
        ArrowTimeUnit::Millisecond => {
            convert_arrow_array::<i64, DurationMillisecondType>(column, |v| {
                Ok(Value::from(EngineDuration::new_with_unit(v, "ms").unwrap()))
            })
        }
        ArrowTimeUnit::Microsecond => {
            convert_arrow_array::<i64, DurationMicrosecondType>(column, |v| {
                Ok(Value::from(EngineDuration::new_with_unit(v, "us").unwrap()))
            })
        }
        ArrowTimeUnit::Nanosecond => {
            convert_arrow_array::<i64, DurationNanosecondType>(column, |v| {
                Ok(Value::from(EngineDuration::new_with_unit(v, "ns").unwrap()))
            })
        }
    }
}

fn convert_arrow_timestamp_array_naive(
    column: &Arc<dyn ArrowArray>,
    time_unit: ArrowTimeUnit,
) -> Vec<ParsedValue> {
    match time_unit {
        ArrowTimeUnit::Second => convert_arrow_array::<i64, TimestampSecondType>(column, |v| {
            Ok(Value::from(DateTimeNaive::from_timestamp(v, "s").unwrap()))
        }),
        ArrowTimeUnit::Millisecond => {
            convert_arrow_array::<i64, TimestampMillisecondType>(column, |v| {
                Ok(Value::from(DateTimeNaive::from_timestamp(v, "ms").unwrap()))
            })
        }
        ArrowTimeUnit::Microsecond => {
            convert_arrow_array::<i64, TimestampMicrosecondType>(column, |v| {
                Ok(Value::from(DateTimeNaive::from_timestamp(v, "us").unwrap()))
            })
        }
        ArrowTimeUnit::Nanosecond => {
            convert_arrow_array::<i64, TimestampNanosecondType>(column, |v| {
                Ok(Value::from(DateTimeNaive::from_timestamp(v, "ns").unwrap()))
            })
        }
    }
}

fn convert_arrow_timestamp_array_utc(
    column: &Arc<dyn ArrowArray>,
    time_unit: ArrowTimeUnit,
    timezone: &str,
) -> Vec<ParsedValue> {
    let values_naive = convert_arrow_timestamp_array_naive(column, time_unit);
    values_naive
        .into_iter()
        .map(|v| match v {
            Ok(Value::DateTimeNaive(v)) => {
                Ok(Value::from(v.to_utc_from_timezone(timezone).unwrap()))
            }
            Ok(value) => Ok(value),
            Err(e) => Err(e),
        })
        .collect()
}
