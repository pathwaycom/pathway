use std::collections::HashMap;
use std::sync::Arc;

use arcstr::ArcStr;
use deltalake::arrow::array::types::{
    DurationMicrosecondType, DurationMillisecondType, DurationNanosecondType, DurationSecondType,
    Float16Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type,
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType, UInt16Type, UInt32Type, UInt8Type,
};
use deltalake::arrow::array::{
    Array as ArrowArray, ArrowPrimitiveType, AsArray, OffsetSizeTrait,
    RecordBatch as ArrowRecordBatch,
};
use deltalake::arrow::datatypes::{DataType as ArrowDataType, TimeUnit as ArrowTimeUnit};
use deltalake::datafusion::parquet::record::Field as ParquetValue;
use deltalake::datafusion::parquet::record::List as ParquetList;
use deltalake::parquet::record::Row as ParquetRow;
use half::f16;
use ndarray::ArrayD;
use once_cell::sync::Lazy;

use crate::connectors::data_storage::ConversionError;
use crate::connectors::data_storage::ValuesMap;
use crate::connectors::WriteError;
use crate::engine::error::{limit_length, STANDARD_OBJECT_LENGTH_LIMIT};
use crate::engine::{
    value::parse_pathway_pointer, value::Kind, DateTimeNaive, DateTimeUtc,
    Duration as EngineDuration, Type, Value,
};

pub mod delta;
pub mod iceberg;
pub mod writer;

pub use delta::DeltaBatchWriter;
pub use iceberg::IcebergBatchWriter;
pub use writer::LakeWriter;

const SPECIAL_FIELD_TIME: &str = "time";
const SPECIAL_FIELD_DIFF: &str = "diff";
const SPECIAL_OUTPUT_FIELDS: [(&str, Type); 2] = [
    (SPECIAL_FIELD_TIME, Type::Int),
    (SPECIAL_FIELD_DIFF, Type::Int),
];
const PATHWAY_COLUMN_META_FIELD: &str = "pathway.column.metadata";

pub struct LakeWriterSettings {
    pub use_64bit_size_type: bool,
    pub utc_timezone_name: ArcStr,
}

pub type ArrowMetadata = HashMap<String, String>;
pub type MetadataPerColumn = HashMap<String, ArrowMetadata>;
static EMPTY_METADATA_PER_COLUMN: Lazy<MetadataPerColumn> = Lazy::new(HashMap::new);

pub trait LakeBatchWriter: Send {
    fn write_batch(&mut self, batch: ArrowRecordBatch) -> Result<(), WriteError>;

    fn metadata_per_column(&self) -> &MetadataPerColumn {
        &EMPTY_METADATA_PER_COLUMN
    }

    fn settings(&self) -> LakeWriterSettings;

    fn name(&self) -> String;
}

type ParsedValue = Result<Value, Box<ConversionError>>;

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
        let value = parquet_value_into_pathway_value(parquet_value, expected_type, name);
        row_map.insert(name.clone(), value);
    }

    row_map.into()
}

pub fn parquet_value_into_pathway_value(
    parquet_value: &ParquetValue,
    expected_type: &Type,
    name: &str,
) -> ParsedValue {
    let expected_type_unopt = expected_type.unoptionalize();
    let unchecked_value = match (parquet_value, expected_type_unopt) {
        (ParquetValue::Null, _) => Some(Value::None),
        (ParquetValue::Bool(b), Type::Bool | Type::Any) => Some(Value::from(*b)),
        (ParquetValue::Long(i), Type::Int | Type::Any) => Some(Value::from(*i)),
        (ParquetValue::Long(i), Type::Duration) => Some(Value::from(
            EngineDuration::new_with_unit(*i, "us").unwrap(),
        )),
        (ParquetValue::Double(f), Type::Float | Type::Any) => Some(Value::Float((*f).into())),
        (ParquetValue::Str(s), Type::String | Type::Any) => Some(Value::String(s.into())),
        (ParquetValue::Str(s), Type::Pointer) => parse_pathway_pointer(s).ok(),
        (ParquetValue::Str(s), Type::Json) => serde_json::from_str::<serde_json::Value>(s)
            .ok()
            .map(Value::from),
        (ParquetValue::TimestampMicros(us), Type::DateTimeNaive | Type::Any) => Some(Value::from(
            DateTimeNaive::from_timestamp(*us, "us").unwrap(),
        )),
        (ParquetValue::TimestampMicros(us), Type::DateTimeUtc) => {
            Some(Value::from(DateTimeUtc::from_timestamp(*us, "us").unwrap()))
        }
        (ParquetValue::Bytes(b), Type::Bytes | Type::Any) => Some(Value::Bytes(b.data().into())),
        (ParquetValue::Bytes(b), Type::PyObjectWrapper) => {
            if let Ok(value) = bincode::deserialize::<Value>(b.data()) {
                Some(value)
            } else {
                None
            }
        }
        (ParquetValue::ListInternal(parquet_list), Type::List(nested_type)) => {
            let mut values = Vec::new();
            for element in parquet_list.elements() {
                values.push(parquet_value_into_pathway_value(
                    element,
                    nested_type,
                    name,
                )?);
            }
            Some(Value::Tuple(values.into()))
        }
        (ParquetValue::Group(row), Type::Array(_, array_type)) => {
            parse_pathway_array_from_parquet_row(row, array_type)
        }
        (ParquetValue::Group(row), Type::Tuple(nested_types)) => {
            parse_pathway_tuple_from_row(row, nested_types)
        }
        _ => None,
    };

    let expected_type_is_optional = expected_type.is_optional();
    let unchecked_value_is_none = unchecked_value == Some(Value::None);
    let value = if unchecked_value_is_none && !expected_type_is_optional {
        None
    } else {
        unchecked_value
    };

    if let Some(value) = value {
        Ok(value)
    } else {
        let value_repr = limit_length(format!("{parquet_value:?}"), STANDARD_OBJECT_LENGTH_LIMIT);
        Err(Box::new(conversion_error(&value_repr, name, expected_type)))
    }
}

pub fn parse_pathway_tuple_from_row(row: &ParquetRow, nested_types: &[Type]) -> Option<Value> {
    let mut tuple_contents: Vec<Option<Value>> = vec![None; nested_types.len()];
    for (column_name, parquet_value) in row.get_column_iter() {
        // Column name has format [index], so we need to skip the first and the last
        // character to obtain the sequential index
        let str_index = &column_name[1..(column_name.len() - 1)];
        let index: usize = str_index.parse().ok()?;
        if index >= nested_types.len() {
            return None;
        }
        tuple_contents[index] =
            parquet_value_into_pathway_value(parquet_value, &nested_types[index], "").ok();
    }
    let mut tuple_values = Vec::new();
    for tuple_value in tuple_contents {
        tuple_values.push(tuple_value?);
    }
    Some(Value::Tuple(tuple_values.into()))
}

pub fn parse_pathway_array_from_parquet_row(row: &ParquetRow, array_type: &Type) -> Option<Value> {
    let shape_i64 = parse_int_array_from_parquet_row(row, "shape")?;
    let mut shape: Vec<usize> = Vec::new();
    for element in shape_i64 {
        shape.push(element.try_into().ok()?);
    }
    match array_type {
        Type::Int => {
            let values = parse_int_array_from_parquet_row(row, "elements")?;
            let array_impl = ArrayD::<i64>::from_shape_vec(shape, values).ok()?;
            Some(Value::from(array_impl))
        }
        Type::Float => {
            let values = parse_float_array_from_parquet_row(row, "elements")?;
            let array_impl = ArrayD::<f64>::from_shape_vec(shape, values).ok()?;
            Some(Value::from(array_impl))
        }
        _ => panic!("this method should not be used for types other than Int or Float"),
    }
}

fn parse_int_array_from_parquet_row(row: &ParquetRow, name: &str) -> Option<Vec<i64>> {
    let mut result = Vec::new();
    let list_field = parse_list_field_from_parquet_row(row, name)?;
    for element in list_field.elements() {
        if let ParquetValue::Long(v) = element {
            result.push(*v);
        } else {
            return None;
        }
    }
    Some(result)
}

fn parse_float_array_from_parquet_row(row: &ParquetRow, name: &str) -> Option<Vec<f64>> {
    let mut result = Vec::new();
    let list_field = parse_list_field_from_parquet_row(row, name)?;
    for element in list_field.elements() {
        if let ParquetValue::Double(v) = element {
            result.push(*v);
        } else {
            return None;
        }
    }
    Some(result)
}

fn parse_list_field_from_parquet_row<'a>(
    row: &'a ParquetRow,
    name: &str,
) -> Option<&'a ParquetList> {
    for (column_name, parquet_value) in row.get_column_iter() {
        if column_name != name {
            continue;
        }
        if let ParquetValue::ListInternal(list_field) = parquet_value {
            return Some(list_field);
        }
        break;
    }
    None
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
        let values_vector =
            column_into_pathway_values(column, expected_type, column_name, rows_count);
        for (index, value) in values_vector.into_iter().enumerate() {
            result[index].insert(column_name.clone(), value);
        }
    }

    result.into_iter().map(std::convert::Into::into).collect()
}

#[allow(clippy::too_many_lines)]
fn column_into_pathway_values(
    column: &Arc<dyn ArrowArray>,
    expected_type: &Type,
    column_name: &str,
    rows_count: usize,
) -> Vec<ParsedValue> {
    let arrow_type = column.data_type();
    let expected_type_unopt = expected_type.unoptionalize();
    let mut values_vector = match (arrow_type, expected_type_unopt) {
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
        (ArrowDataType::Utf8, Type::String | Type::Json | Type::Pointer | Type::Any) => {
            convert_arrow_string_array::<i32>(column, column_name, expected_type_unopt)
        }
        (ArrowDataType::LargeUtf8, Type::String | Type::Json | Type::Pointer | Type::Any) => {
            convert_arrow_string_array::<i64>(column, column_name, expected_type_unopt)
        }
        (ArrowDataType::Binary, Type::Bytes | Type::PyObjectWrapper | Type::Any) => {
            convert_arrow_bytes_array::<i32>(column, column_name, expected_type_unopt)
        }
        (ArrowDataType::LargeBinary, Type::Bytes | Type::PyObjectWrapper | Type::Any) => {
            convert_arrow_bytes_array::<i64>(column, column_name, expected_type_unopt)
        }
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
        (ArrowDataType::Timestamp(time_unit, Some(timezone)), Type::DateTimeUtc | Type::Any) => {
            convert_arrow_timestamp_array_utc(column, *time_unit, timezone.as_ref())
        }
        (ArrowDataType::List(_), Type::List(_) | Type::Any) => {
            convert_arrow_list_array::<i32>(column, expected_type, column_name, column.len())
        }
        (ArrowDataType::LargeList(_), Type::List(_) | Type::Any) => {
            convert_arrow_list_array::<i64>(column, expected_type, column_name, column.len())
        }
        (arrow_type, expected_type) => {
            vec![
                Err(Box::new(conversion_error(
                    &format!("{arrow_type:?}"),
                    column_name,
                    expected_type
                )));
                rows_count
            ]
        }
    };

    let is_optional = expected_type.is_optional();
    if !is_optional {
        for value in &mut values_vector {
            if value == &Ok(Value::None) {
                *value = Err(Box::new(conversion_error(
                    "null",
                    column_name,
                    expected_type,
                )));
            }
        }
    }

    values_vector
}

fn pathway_tuple_from_parsed_values(nested_list_contents: Vec<ParsedValue>) -> ParsedValue {
    let mut prepared_values = Vec::new();
    for value in nested_list_contents {
        prepared_values.push(value?);
    }
    Ok(Value::Tuple(prepared_values.into()))
}

fn convert_arrow_list_array<OffsetType: OffsetSizeTrait>(
    column: &Arc<dyn ArrowArray>,
    expected_type: &Type,
    column_name: &str,
    rows_count: usize,
) -> Vec<ParsedValue> {
    let nested_type = match expected_type.unoptionalize() {
        Type::Any => Type::Any,
        Type::List(nested_type) => nested_type.as_ref().clone(),
        _ => unreachable!(),
    };
    let mut result = Vec::new();
    for element in column.as_list::<OffsetType>().iter() {
        let parsed_value = match element {
            Some(element) => {
                let nested_list_contents =
                    column_into_pathway_values(&element, &nested_type, column_name, rows_count);
                pathway_tuple_from_parsed_values(nested_list_contents)
            }
            None => Ok(Value::None),
        };
        result.push(parsed_value);
    }
    result
}

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

fn convert_arrow_string_array<OffsetType: OffsetSizeTrait>(
    column: &Arc<dyn ArrowArray>,
    name: &str,
    expected_type: &Type,
) -> Vec<ParsedValue> {
    column
        .as_string::<OffsetType>()
        .into_iter()
        .map(|v| match v {
            Some(v) => match expected_type {
                Type::String | Type::Any => Ok(Value::String(v.into())),
                Type::Json => serde_json::from_str::<serde_json::Value>(v)
                    .map(Value::from)
                    .map_err(|_| {
                        Box::new(conversion_error(
                            &limit_length(v.to_string(), STANDARD_OBJECT_LENGTH_LIMIT),
                            name,
                            expected_type,
                        ))
                    }),
                Type::Pointer => parse_pathway_pointer(v).map_err(|_| {
                    Box::new(conversion_error(
                        &limit_length(v.to_string(), STANDARD_OBJECT_LENGTH_LIMIT),
                        name,
                        expected_type,
                    ))
                }),
                _ => unreachable!("must not be used for type {expected_type}"),
            },
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

fn convert_arrow_bytes_array<OffsetType: OffsetSizeTrait>(
    column: &Arc<dyn ArrowArray>,
    field_name: &str,
    expected_type: &Type,
) -> Vec<ParsedValue> {
    column
        .as_binary::<OffsetType>()
        .into_iter()
        .map(|v| match v {
            Some(v) => {
                if expected_type == &Type::Bytes {
                    Ok(Value::Bytes(v.into()))
                } else {
                    let maybe_value = bincode::deserialize::<Value>(v);
                    if let Ok(value) = maybe_value {
                        match (value.kind(), expected_type) {
                            (Kind::PyObjectWrapper, Type::PyObjectWrapper) => Ok(value),
                            _ => Err(Box::new(conversion_error(
                                &format!("{value}"),
                                field_name,
                                expected_type,
                            ))),
                        }
                    } else {
                        Err(Box::new(conversion_error(
                            &format!("{maybe_value:?}"),
                            field_name,
                            expected_type,
                        )))
                    }
                }
            }
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

fn conversion_error(v: &str, name: &str, expected_type: &Type) -> ConversionError {
    ConversionError {
        value_repr: limit_length(v.to_string(), STANDARD_OBJECT_LENGTH_LIMIT),
        field_name: name.to_string(),
        type_: expected_type.clone(),
    }
}
