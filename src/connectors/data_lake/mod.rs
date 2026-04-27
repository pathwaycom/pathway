use log::error;
use std::collections::HashMap;
use std::sync::Arc;

use arcstr::ArcStr;
use deltalake::arrow::array::types::{
    ArrowDictionaryKeyType, Decimal128Type, Decimal256Type, DurationMicrosecondType,
    DurationMillisecondType, DurationNanosecondType, DurationSecondType, Float16Type, Float32Type,
    Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType, UInt16Type, UInt32Type,
    UInt64Type, UInt8Type,
};
use deltalake::arrow::array::{
    Array as ArrowArray, ArrowPrimitiveType, AsArray, OffsetSizeTrait,
    RecordBatch as ArrowRecordBatch,
};
use deltalake::arrow::datatypes::{
    ArrowNativeType, DataType as ArrowDataType, TimeUnit as ArrowTimeUnit,
};
use deltalake::datafusion::parquet::record::Field as ParquetValue;
use deltalake::datafusion::parquet::record::List as ParquetList;
use deltalake::parquet::record::Row as ParquetRow;
use half::f16;
use ndarray::ArrayD;
use once_cell::sync::Lazy;

use crate::connectors::data_format::{NDARRAY_ELEMENTS_FIELD_NAME, NDARRAY_SHAPE_FIELD_NAME};
use crate::connectors::data_lake::buffering::PayloadType;
use crate::connectors::data_storage::ConversionError;
use crate::connectors::data_storage::ValuesMap;
use crate::connectors::{WriteError, SPECIAL_FIELD_DIFF, SPECIAL_FIELD_TIME};
use crate::engine::error::{limit_length, STANDARD_OBJECT_LENGTH_LIMIT};
use crate::engine::{
    value::parse_pathway_pointer, value::Kind, DateTimeNaive, DateTimeUtc,
    Duration as EngineDuration, Type, Value,
};
use crate::python_api::ValueField;

pub mod arrow;
pub mod buffering;
pub mod delta;
pub mod iceberg;
pub mod writer;

pub use delta::DeltaBatchWriter;
pub use iceberg::IcebergBatchWriter;
pub use writer::LakeWriter;

const SPECIAL_FIELD_ID: &str = "_id";
const SPECIAL_OUTPUT_FIELDS: [(&str, Type); 2] = [
    (SPECIAL_FIELD_TIME, Type::Int),
    (SPECIAL_FIELD_DIFF, Type::Int),
];
const SNAPSHOT_OUTPUT_FIELDS: [(&str, Type); 1] = [(SPECIAL_FIELD_ID, Type::Pointer)];
const PATHWAY_COLUMN_META_FIELD: &str = "pathway.column.metadata";

#[derive(Clone, Copy, Debug)]
pub enum MaintenanceMode {
    StreamOfChanges,
    Snapshot,
}

impl MaintenanceMode {
    fn additional_output_fields(&self) -> Vec<(&str, Type)> {
        match self {
            Self::StreamOfChanges => SPECIAL_OUTPUT_FIELDS.to_vec(),
            Self::Snapshot => SNAPSHOT_OUTPUT_FIELDS.to_vec(),
        }
    }

    fn is_append_only(self) -> bool {
        match self {
            Self::StreamOfChanges => true,
            Self::Snapshot => false,
        }
    }
}

pub struct LakeWriterSettings {
    pub use_64bit_size_type: bool,
    pub utc_timezone_name: ArcStr,
    pub timestamp_unit: ArrowTimeUnit,
}

pub type ArrowMetadata = HashMap<String, String>;
pub type MetadataPerColumn = HashMap<String, ArrowMetadata>;
static EMPTY_METADATA_PER_COLUMN: Lazy<MetadataPerColumn> = Lazy::new(HashMap::new);

pub trait LakeBatchWriter: Send {
    fn write_batch(
        &mut self,
        batch: ArrowRecordBatch,
        payload_type: PayloadType,
    ) -> Result<(), WriteError>;

    fn metadata_per_column(&self) -> &MetadataPerColumn {
        &EMPTY_METADATA_PER_COLUMN
    }

    fn settings(&self) -> LakeWriterSettings;

    fn name(&self) -> String;

    /// Override the Arrow storage type for individual user columns. Returned by writers
    /// that want to coerce a Pathway column onto an existing destination column whose
    /// type is more specific than what `arrow_data_type` would derive from the Pathway
    /// type alone — for example, writing a Pathway `str` column into an existing Delta
    /// `decimal(p, s)` column. The conversion at row time is performed by the matching
    /// arm of `array_for_type`. Default: empty (use the type derived from the Pathway
    /// schema).
    fn arrow_type_overrides(&self) -> HashMap<String, ArrowDataType> {
        HashMap::new()
    }
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
        (ParquetValue::Int(i), Type::Int | Type::Any) => Some(Value::from(i64::from(*i))),
        (ParquetValue::Short(i), Type::Int | Type::Any) => Some(Value::from(i64::from(*i))),
        (ParquetValue::Byte(i), Type::Int | Type::Any) => Some(Value::from(i64::from(*i))),
        (ParquetValue::UInt(i), Type::Int | Type::Any) => Some(Value::from(i64::from(*i))),
        (ParquetValue::UShort(i), Type::Int | Type::Any) => Some(Value::from(i64::from(*i))),
        (ParquetValue::UByte(i), Type::Int | Type::Any) => Some(Value::from(i64::from(*i))),
        (ParquetValue::ULong(i), Type::Int | Type::Any) => i64::try_from(*i).ok().map(Value::from),
        (ParquetValue::Long(i), Type::Duration) => Some(Value::from(
            EngineDuration::new_with_unit(*i, "us").unwrap(),
        )),
        (ParquetValue::Double(f), Type::Float | Type::Any) => Some(Value::Float((*f).into())),
        (ParquetValue::Float(f), Type::Float | Type::Any) => {
            Some(Value::Float(f64::from(*f).into()))
        }
        (ParquetValue::Float16(f), Type::Float | Type::Any) => {
            Some(Value::Float(f64::from(*f).into()))
        }
        // Pathway has no Decimal type. Delta `decimal(p,s)` columns are commonly produced
        // by Spark / pandas / DuckDB. Two read mappings are supported, depending on the
        // Pathway type the user declared in their schema:
        //   - Type::Float | Type::Any: convert through f64. Lossy in general (binary
        //     representation, ~15-17 significant decimal digits of mantissa). The reader
        //     emits a one-shot warning at startup naming the affected columns.
        //   - Type::String: format the unscaled integer with the column's scale and pass
        //     the resulting decimal text through unchanged. Lossless for the full
        //     precision range supported by Delta (up to 38 digits).
        (ParquetValue::Decimal(d), Type::Float | Type::Any) => {
            decimal_field_to_f64(d).map(|f| Value::Float(f.into()))
        }
        (ParquetValue::Decimal(d), Type::String) => {
            decimal_field_to_string(d).map(|s| Value::String(s.into()))
        }
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
        // Delta Lake's spec says timestamps are microsecond precision; Pathway always
        // writes them that way. External tools sometimes produce parquet files with
        // millisecond-precision timestamps — accept those too on read.
        (ParquetValue::TimestampMillis(ms), Type::DateTimeNaive | Type::Any) => Some(Value::from(
            DateTimeNaive::from_timestamp(*ms, "ms").unwrap(),
        )),
        (ParquetValue::TimestampMillis(ms), Type::DateTimeUtc) => {
            Some(Value::from(DateTimeUtc::from_timestamp(*ms, "ms").unwrap()))
        }
        // Pathway has no native Date type; Delta `date` columns are days since the Unix epoch.
        // Materialize them at midnight so they fit DateTimeNaive / DateTimeUtc — the only
        // sensible mapping that preserves the calendar day.
        (ParquetValue::Date(days), Type::DateTimeNaive | Type::Any) => {
            DateTimeNaive::from_timestamp(i64::from(*days), "D")
                .ok()
                .map(Value::from)
        }
        (ParquetValue::Date(days), Type::DateTimeUtc) => {
            DateTimeUtc::from_timestamp(i64::from(*days), "D")
                .ok()
                .map(Value::from)
        }
        (ParquetValue::Bytes(b), Type::Bytes | Type::Any) => Some(Value::Bytes(b.data().into())),
        (ParquetValue::Bytes(b), Type::PyObjectWrapper) => {
            bincode::deserialize::<Value>(b.data()).ok()
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
    let shape_i64 = parse_int_array_from_parquet_row(row, NDARRAY_SHAPE_FIELD_NAME)?;
    let mut shape: Vec<usize> = Vec::new();
    for element in shape_i64 {
        shape.push(element.try_into().ok()?);
    }
    match array_type {
        Type::Int => {
            let values = parse_int_array_from_parquet_row(row, NDARRAY_ELEMENTS_FIELD_NAME)?;
            let array_impl = ArrayD::<i64>::from_shape_vec(shape, values).ok()?;
            Some(Value::from(array_impl))
        }
        Type::Float => {
            let values = parse_float_array_from_parquet_row(row, NDARRAY_ELEMENTS_FIELD_NAME)?;
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
    let create_error_array = || {
        vec![
            Err(Box::new(conversion_error(
                &format!("{arrow_type:?}"),
                column_name,
                expected_type
            )));
            rows_count
        ]
    };
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
        (ArrowDataType::Decimal128(_, scale), Type::Float | Type::Any) => {
            let divisor = 10f64.powi(i32::from(*scale));
            convert_arrow_array::<i128, Decimal128Type>(column, |v| {
                // The cast is documented-lossy: this whole arm is the "decimal as
                // float" path that warns at startup. Arm for Type::String just below
                // is the lossless route.
                #[allow(clippy::cast_precision_loss)]
                let unscaled = v as f64;
                Ok(Value::Float((unscaled / divisor).into()))
            })
        }
        (ArrowDataType::Decimal128(_, scale), Type::String) => {
            let scale = i32::from(*scale);
            convert_arrow_array::<i128, Decimal128Type>(column, |v| {
                Ok(Value::String(
                    format_decimal_str(&v.to_string(), scale).into(),
                ))
            })
        }
        (ArrowDataType::Decimal256(_, scale), Type::Float | Type::Any) => {
            // arrow's i256 doesn't expose a native `as f64` — go through its `to_string`
            // and re-parse, which is exact for values fitting f64 and a graceful approximation
            // for the (rare) larger ones.
            let scale_div = 10f64.powi(i32::from(*scale));
            let arr = column.as_primitive::<Decimal256Type>();
            arr.into_iter()
                .map(|v| match v {
                    Some(v) => match v.to_string().parse::<f64>() {
                        Ok(f) => Ok(Value::Float((f / scale_div).into())),
                        Err(_) => Err(Box::new(conversion_error(
                            &v.to_string(),
                            column_name,
                            expected_type,
                        ))),
                    },
                    None => Ok(Value::None),
                })
                .collect()
        }
        (ArrowDataType::Decimal256(_, scale), Type::String) => {
            let scale = i32::from(*scale);
            let arr = column.as_primitive::<Decimal256Type>();
            arr.into_iter()
                .map(|v| match v {
                    Some(v) => Ok(Value::String(
                        format_decimal_str(&v.to_string(), scale).into(),
                    )),
                    None => Ok(Value::None),
                })
                .collect()
        }
        (ArrowDataType::Boolean, Type::Bool | Type::Any) => convert_arrow_boolean_array(column),
        (ArrowDataType::Utf8, Type::String | Type::Json | Type::Pointer | Type::Any) => {
            convert_arrow_string_array::<i32>(column, column_name, expected_type_unopt)
        }
        (ArrowDataType::LargeUtf8, Type::String | Type::Json | Type::Pointer | Type::Any) => {
            convert_arrow_string_array::<i64>(column, column_name, expected_type_unopt)
        }
        (ArrowDataType::Utf8View, Type::String | Type::Json | Type::Pointer | Type::Any) => {
            convert_arrow_string_view_array(column, column_name, expected_type_unopt)
        }
        (ArrowDataType::Binary, Type::Bytes | Type::PyObjectWrapper | Type::Any) => {
            convert_arrow_bytes_array::<i32>(column, column_name, expected_type_unopt)
        }
        (ArrowDataType::LargeBinary, Type::Bytes | Type::PyObjectWrapper | Type::Any) => {
            convert_arrow_bytes_array::<i64>(column, column_name, expected_type_unopt)
        }
        (ArrowDataType::BinaryView, Type::Bytes | Type::PyObjectWrapper | Type::Any) => {
            convert_arrow_binary_view_array(column, column_name, expected_type_unopt)
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
        (ArrowDataType::Dictionary(key_type, _), _) => {
            // There are two ways to represent a column of any structure T:
            // - The straightforward way is by defining a type T as the type of this column;
            // - The optimized way is by defining a column as a ArrowDataType::Dictionary(size_type, T).
            //
            // When a table is written and read by Pathway, the straightforward way is used to represent the data blocks.
            // However, when a query with DataFusion is made, and it heuristically detects that:
            // 1. The observed type is heavy;
            // 2. The cardinality of the set of values of this array is low.
            // It uses the optimized representation for the values of the column.
            //
            // This code decodes the optimized representation of a column.

            let result = match key_type.as_ref() {
                ArrowDataType::UInt8 => convert_arrow_dictionary_array::<u8, UInt8Type>(
                    column,
                    column_name,
                    expected_type_unopt,
                ),
                ArrowDataType::UInt16 => convert_arrow_dictionary_array::<u16, UInt16Type>(
                    column,
                    column_name,
                    expected_type_unopt,
                ),
                ArrowDataType::UInt32 => convert_arrow_dictionary_array::<u32, UInt32Type>(
                    column,
                    column_name,
                    expected_type_unopt,
                ),
                ArrowDataType::UInt64 => convert_arrow_dictionary_array::<u64, UInt64Type>(
                    column,
                    column_name,
                    expected_type_unopt,
                ),
                _ => None,
            };
            result.unwrap_or_else(create_error_array)
        }
        _ => create_error_array(),
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

fn convert_arrow_dictionary_array<
    N: ArrowNativeType,
    T: ArrowPrimitiveType<Native = N> + ArrowDictionaryKeyType,
>(
    column: &Arc<dyn ArrowArray>,
    column_name: &str,
    expected_type_unopt: &Type,
) -> Option<Vec<ParsedValue>> {
    // Dictionary arrays are used in DataFusion to efficiently represent arrays with low-cardinality values.
    // They consist of two arrays: a values array, which holds all unique values,
    // and a keys array, which holds indices pointing to entries in the values array.
    // Each element in the keys array refers to a corresponding value by index.

    let impl_ = column.as_dictionary::<T>();
    let keys = impl_.keys();
    let values = impl_.values();
    let parsed_keys: Vec<_> = keys.into_iter().collect();

    let parsed_values =
        column_into_pathway_values(values, expected_type_unopt, column_name, values.len());
    let mut result = Vec::with_capacity(parsed_keys.len());
    for index in parsed_keys {
        let Some(index) = index else {
            result.push(Ok(Value::None));
            continue;
        };
        let index: usize = index.as_usize();
        if index >= parsed_values.len() {
            error!(
                "Broken dictionary object: key points to an object that is out of the given set."
            );
            return None;
        }
        let value = parsed_values[index].clone();
        result.push(value);
    }

    Some(result)
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

fn convert_arrow_string_view_array(
    column: &Arc<dyn ArrowArray>,
    name: &str,
    expected_type: &Type,
) -> Vec<ParsedValue> {
    column
        .as_string_view()
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

fn convert_arrow_binary_view_array(
    column: &Arc<dyn ArrowArray>,
    field_name: &str,
    expected_type: &Type,
) -> Vec<ParsedValue> {
    column
        .as_binary_view()
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

/// Insert the implicit decimal point into the textual unscaled value, padding with
/// leading zeros when the magnitude is below 1. Lossless for any precision: the
/// caller hands us the unscaled integer formatted as decimal text (e.g. via
/// `i128::to_string()` or `arrow::i256::to_string()`), so this routine never goes
/// through floating-point.
fn format_decimal_str(unscaled_str: &str, scale: i32) -> String {
    if scale <= 0 {
        return unscaled_str.to_string();
    }
    // Bounded by Delta's spec maximum precision of 38, so safe to widen.
    let scale_u = usize::try_from(scale).expect("scale is non-negative above");
    let (sign, abs) = unscaled_str
        .strip_prefix('-')
        .map_or(("", unscaled_str), |s| ("-", s));
    if abs.len() > scale_u {
        let split = abs.len() - scale_u;
        format!("{}{}.{}", sign, &abs[..split], &abs[split..])
    } else {
        let zeros = "0".repeat(scale_u - abs.len());
        format!("{sign}0.{zeros}{abs}")
    }
}

fn decimal_field_to_string(d: &deltalake::parquet::data_type::Decimal) -> Option<String> {
    let bytes = d.data();
    let unscaled = match bytes.len() {
        n @ 1..=16 => {
            let mut buf = if (bytes[0] & 0x80) != 0 {
                [0xffu8; 16]
            } else {
                [0u8; 16]
            };
            buf[16 - n..].copy_from_slice(bytes);
            i128::from_be_bytes(buf)
        }
        // Larger fixed-len-byte-array encodings (Decimal up to 38 digits fit in 16
        // bytes; anything wider is non-spec for Delta). Fall through and let the
        // caller surface a conversion error.
        _ => return None,
    };
    Some(format_decimal_str(&unscaled.to_string(), d.scale()))
}

// Cast to f64 is documented-lossy: Pathway warns at startup that values exceeding
// f64's mantissa lose precision through this path, and the lossless alternative
// (read the column as Pathway `str`) is right above this function.
#[allow(clippy::cast_precision_loss)]
fn decimal_field_to_f64(d: &deltalake::parquet::data_type::Decimal) -> Option<f64> {
    // Delta `decimal(p, s)` is encoded by the parquet writer as INT32 / INT64 /
    // FIXED_LEN_BYTE_ARRAY of the unscaled big-endian two's-complement value, with
    // `precision` and `scale` carried as logical-type metadata. Reconstruct the
    // unscaled integer in the widest representation we can fit and divide by 10^scale.
    let bytes = d.data();
    let unscaled = match bytes.len() {
        n @ 1..=8 => {
            // Sign-extend big-endian two's-complement into i64.
            let mut buf = if (bytes[0] & 0x80) != 0 {
                [0xffu8; 8]
            } else {
                [0u8; 8]
            };
            buf[8 - n..].copy_from_slice(bytes);
            i64::from_be_bytes(buf) as f64
        }
        n @ 9..=16 => {
            let mut buf = if (bytes[0] & 0x80) != 0 {
                [0xffu8; 16]
            } else {
                [0u8; 16]
            };
            buf[16 - n..].copy_from_slice(bytes);
            i128::from_be_bytes(buf) as f64
        }
        _ => return None,
    };
    let divisor = 10f64.powi(d.scale());
    Some(unscaled / divisor)
}

fn conversion_error(v: &str, name: &str, expected_type: &Type) -> ConversionError {
    ConversionError::new(
        limit_length(v.to_string(), STANDARD_OBJECT_LENGTH_LIMIT),
        name.to_string(),
        expected_type.clone(),
        None,
    )
}

pub fn construct_column_types_map(
    value_fields: &[ValueField],
    mode: MaintenanceMode,
) -> HashMap<String, Type> {
    value_fields
        .iter()
        .map(|field| (field.name.clone(), field.type_.clone()))
        .chain(
            mode.additional_output_fields()
                .into_iter()
                .map(|(f, t)| (f.to_string(), t)),
        )
        .collect()
}

pub fn construct_column_order(value_fields: &[ValueField], mode: MaintenanceMode) -> Vec<String> {
    value_fields
        .iter()
        .map(|field| field.name.clone())
        .chain(
            mode.additional_output_fields()
                .into_iter()
                .map(|(f, _)| f.to_string()),
        )
        .collect()
}
