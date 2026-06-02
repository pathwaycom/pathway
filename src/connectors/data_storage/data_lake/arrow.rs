use std::collections::HashMap;
use std::sync::Arc;

use deltalake::arrow::array::Array as ArrowArray;
use deltalake::arrow::array::{
    BinaryArray as ArrowBinaryArray, BooleanArray as ArrowBooleanArray, BooleanBufferBuilder,
    Date32Array as ArrowDate32Array, Decimal128Array as ArrowDecimal128Array,
    FixedSizeBinaryArray as ArrowFixedSizeBinaryArray, Float32Array as ArrowFloat32Array,
    Float64Array as ArrowFloat64Array, Int32Array as ArrowInt32Array,
    Int64Array as ArrowInt64Array, LargeBinaryArray as ArrowLargeBinaryArray,
    LargeListArray as ArrowLargeListArray, ListArray as ArrowListArray,
    StringArray as ArrowStringArray, StructArray as ArrowStructArray,
    Time64MicrosecondArray as ArrowTime64MicrosecondArray,
    TimestampMicrosecondArray as ArrowTimestampMsArray,
    TimestampNanosecondArray as ArrowTimestampNsArray,
};
use deltalake::arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use deltalake::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Fields as ArrowFields, Schema as ArrowSchema,
    TimeUnit as ArrowTimeUnit,
};
use ndarray::ArrayD;
use uuid::Uuid;

use super::{LakeWriterSettings, MaintenanceMode};
use crate::connectors::data_format::{
    NDARRAY_ELEMENTS_FIELD_NAME, NDARRAY_SHAPE_FIELD_NAME, NDARRAY_SINGLE_ELEMENT_FIELD_NAME,
};
use crate::connectors::data_storage::data_lake::iceberg::IcebergError;
use crate::connectors::data_storage::data_lake::LakeBatchWriter;
use crate::connectors::WriteError;
use crate::engine::time::DateTime as EngineDateTime;
use crate::engine::value::Handle;
use crate::engine::{Type, Value};
use crate::python_api::ValueField;

#[allow(clippy::too_many_lines)] // one match arm per supported Arrow type — reads naturally as a long table.
pub fn array_for_type(
    type_: &ArrowDataType,
    values: &[Value],
) -> Result<Arc<dyn ArrowArray>, WriteError> {
    match type_ {
        ArrowDataType::Boolean => {
            let v = array_of_simple_type::<bool>(values, |v| match v {
                Value::Bool(b) => Ok(*b),
                _ => Err(WriteError::TypeMismatchWithSchema(v.clone(), type_.clone())),
            })?;
            Ok(Arc::new(ArrowBooleanArray::from(v)))
        }
        ArrowDataType::Int64 => {
            let v = array_of_simple_type::<i64>(values, |v| match v {
                Value::Int(i) => Ok(*i),
                Value::Duration(d) => Ok(d.microseconds()),
                _ => Err(WriteError::TypeMismatchWithSchema(v.clone(), type_.clone())),
            })?;
            Ok(Arc::new(ArrowInt64Array::from(v)))
        }
        // Pathway `int` is i64; an existing destination column declared as Iceberg
        // `int` (32-bit) opts into a narrowing write. Out-of-range values fail the
        // batch — silent truncation would corrupt downstream queries.
        ArrowDataType::Int32 => {
            let v = array_of_simple_type::<i32>(values, |v| match v {
                Value::Int(i) => i32::try_from(*i).map_err(|_| {
                    WriteError::from(IcebergError::IntegerNarrowingOverflow {
                        value: *i,
                        target: ArrowDataType::Int32,
                    })
                }),
                Value::Duration(d) => i32::try_from(d.microseconds()).map_err(|_| {
                    WriteError::from(IcebergError::IntegerNarrowingOverflow {
                        value: d.microseconds(),
                        target: ArrowDataType::Int32,
                    })
                }),
                _ => Err(WriteError::TypeMismatchWithSchema(v.clone(), type_.clone())),
            })?;
            Ok(Arc::new(ArrowInt32Array::from(v)))
        }
        ArrowDataType::Float64 => {
            let v = array_of_simple_type::<f64>(values, |v| match v {
                Value::Float(f) => Ok((*f).into()),
                _ => Err(WriteError::TypeMismatchWithSchema(v.clone(), type_.clone())),
            })?;
            Ok(Arc::new(ArrowFloat64Array::from(v)))
        }
        // Existing destination column declared as Iceberg `float` (32-bit). The
        // f64 → f32 cast is always defined and never traps on out-of-range; values
        // exceeding f32 range materialize as ±∞, matching IEEE 754 semantics.
        ArrowDataType::Float32 => {
            let v = array_of_simple_type::<f32>(values, |v| match v {
                Value::Float(f) =>
                {
                    #[allow(clippy::cast_possible_truncation)]
                    Ok(f64::from(*f) as f32)
                }
                _ => Err(WriteError::TypeMismatchWithSchema(v.clone(), type_.clone())),
            })?;
            Ok(Arc::new(ArrowFloat32Array::from(v)))
        }
        ArrowDataType::Utf8 => {
            let v = array_of_simple_type::<String>(values, |v| match v {
                Value::String(s) => Ok(s.to_string()),
                Value::Pointer(p) => Ok(p.to_string()),
                Value::Json(j) => Ok(j.to_string()),
                _ => Err(WriteError::TypeMismatchWithSchema(v.clone(), type_.clone())),
            })?;
            Ok(Arc::new(ArrowStringArray::from(v)))
        }
        ArrowDataType::Binary | ArrowDataType::LargeBinary => {
            let mut vec_owned = array_of_simple_type::<Vec<u8>>(values, |v| match v {
                Value::Bytes(b) => Ok(b.to_vec()),
                Value::PyObjectWrapper(_) | Value::Pointer(_) => {
                    Ok(bincode::serialize(v).map_err(|e| *e)?)
                }
                _ => Err(WriteError::TypeMismatchWithSchema(v.clone(), type_.clone())),
            })?;
            let mut vec_refs = Vec::new();
            for item in &mut vec_owned {
                vec_refs.push(item.as_mut().map(|v| v.as_slice()));
            }
            if *type_ == ArrowDataType::Binary {
                Ok(Arc::new(ArrowBinaryArray::from(vec_refs)))
            } else {
                Ok(Arc::new(ArrowLargeBinaryArray::from(vec_refs)))
            }
        }
        ArrowDataType::Timestamp(ArrowTimeUnit::Microsecond, None) => {
            let v = array_of_simple_type::<i64>(values, |v| match v {
                #[allow(clippy::cast_possible_truncation)]
                Value::DateTimeNaive(dt) => Ok(dt.timestamp_microseconds()),
                _ => Err(WriteError::TypeMismatchWithSchema(v.clone(), type_.clone())),
            })?;
            Ok(Arc::new(ArrowTimestampMsArray::from(v)))
        }
        // Iceberg `date` is a calendar day with no time-of-day. Pathway has no
        // date-only type, so writes go from `DateTimeNaive` and silently
        // truncate the time-of-day component. Matches the convention
        // `pw.io.postgres.write` uses for PostgreSQL DATE columns: the
        // documentation calls out the truncation so users producing
        // mid-day timestamps know they lose the wall-clock portion.
        ArrowDataType::Date32 => {
            let epoch =
                chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("1970-01-01 is a valid date");
            let v = array_of_simple_type::<i32>(values, |v| match v {
                Value::DateTimeNaive(dt) => {
                    let date = dt.as_chrono_datetime().date();
                    let days = date.signed_duration_since(epoch).num_days();
                    i32::try_from(days).map_err(|_| {
                        WriteError::from(IcebergError::DateOutOfRange {
                            value: format!("{date}"),
                        })
                    })
                }
                _ => Err(WriteError::TypeMismatchWithSchema(v.clone(), type_.clone())),
            })?;
            Ok(Arc::new(ArrowDate32Array::from(v)))
        }
        // Iceberg `time` (microsecond precision, no date, no timezone) maps to
        // Arrow `Time64(Microsecond)`. Pathway has no time-of-day type, so we
        // mirror the Postgres connector's TIME ↔ Duration convention: the
        // Pathway `Duration` value is interpreted as microseconds since
        // midnight. Values outside [0, 24h) are written verbatim — Iceberg
        // doesn't enforce the range either, and rejecting at write time would
        // surprise pipelines that produce, say, "duration since some epoch"
        // and store it in a `time` column.
        ArrowDataType::Time64(ArrowTimeUnit::Microsecond) => {
            let v = array_of_simple_type::<i64>(values, |v| match v {
                Value::Duration(d) => Ok(d.microseconds()),
                _ => Err(WriteError::TypeMismatchWithSchema(v.clone(), type_.clone())),
            })?;
            Ok(Arc::new(ArrowTime64MicrosecondArray::from(v)))
        }
        ArrowDataType::Timestamp(ArrowTimeUnit::Microsecond, Some(tz)) => {
            let v = array_of_simple_type::<i64>(values, |v| match v {
                #[allow(clippy::cast_possible_truncation)]
                Value::DateTimeUtc(dt) => Ok(dt.timestamp_microseconds()),
                _ => Err(WriteError::TypeMismatchWithSchema(v.clone(), type_.clone())),
            })?;
            Ok(Arc::new(
                ArrowTimestampMsArray::from(v).with_timezone(&**tz),
            ))
        }
        ArrowDataType::Timestamp(ArrowTimeUnit::Nanosecond, None) => {
            let v = array_of_simple_type::<i64>(values, |v| match v {
                #[allow(clippy::cast_possible_truncation)]
                Value::DateTimeNaive(dt) => Ok(dt.timestamp()),
                _ => Err(WriteError::TypeMismatchWithSchema(v.clone(), type_.clone())),
            })?;
            Ok(Arc::new(ArrowTimestampNsArray::from(v)))
        }
        ArrowDataType::Timestamp(ArrowTimeUnit::Nanosecond, Some(tz)) => {
            let v = array_of_simple_type::<i64>(values, |v| match v {
                #[allow(clippy::cast_possible_truncation)]
                Value::DateTimeUtc(dt) => Ok(dt.timestamp()),
                _ => Err(WriteError::TypeMismatchWithSchema(v.clone(), type_.clone())),
            })?;
            Ok(Arc::new(
                ArrowTimestampNsArray::from(v).with_timezone(&**tz),
            ))
        }
        ArrowDataType::Decimal128(precision, scale) => {
            let precision = *precision;
            let scale = *scale;
            let v = array_of_simple_type::<i128>(values, |v| match v {
                Value::String(s) => parse_decimal_string(s, precision, scale).map_err(|reason| {
                    WriteError::from(DecimalSerializationError {
                        value: s.to_string(),
                        precision,
                        scale,
                        reason,
                    })
                }),
                _ => Err(WriteError::TypeMismatchWithSchema(v.clone(), type_.clone())),
            })?;
            let array = ArrowDecimal128Array::from(v).with_precision_and_scale(precision, scale)?;
            Ok(Arc::new(array))
        }
        // Iceberg `fixed(N)` and `uuid` both materialize as Arrow FixedSizeBinary.
        // The Pathway value type chosen by the user disambiguates the encoding:
        //   * `str` at width 16 → parse canonical UUID hex, emit 16 bytes (UUID).
        //   * `bytes` → length-checked passthrough (fixed(N)).
        // Anything else is a schema mismatch.
        ArrowDataType::FixedSizeBinary(width) => {
            let width_i32 = *width;
            let width_usize = usize::try_from(width_i32).map_err(|_| {
                WriteError::from(IcebergError::FixedSizeBinaryNegativeWidth { width: width_i32 })
            })?;
            let mut buf: Vec<Option<[u8; 16]>> = Vec::new();
            let mut variable_buf: Vec<Option<Vec<u8>>> = Vec::new();
            let use_uuid_path = width_i32 == 16;
            // We collect into an Option-wrapped Vec because `try_from_sparse_iter_with_size`
            // needs to know about nulls per-row; building a separate path for Vec<u8>
            // (variable width) keeps the UUID branch on a fixed-size buffer.
            for v in values {
                match v {
                    Value::None => {
                        if use_uuid_path {
                            buf.push(None);
                        } else {
                            variable_buf.push(None);
                        }
                    }
                    Value::String(s) if use_uuid_path => {
                        let parsed = Uuid::parse_str(s).map_err(|_| {
                            WriteError::from(IcebergError::UuidParseError {
                                value: s.to_string(),
                            })
                        })?;
                        buf.push(Some(parsed.into_bytes()));
                    }
                    Value::Bytes(b) => {
                        if b.len() != width_usize {
                            return Err(IcebergError::FixedSizeBinaryLengthMismatch {
                                value_len: b.len(),
                                expected: width_usize,
                            }
                            .into());
                        }
                        if use_uuid_path {
                            // Even with width 16, accept raw bytes as a length-correct payload.
                            let mut arr = [0u8; 16];
                            arr.copy_from_slice(&b[..]);
                            buf.push(Some(arr));
                        } else {
                            variable_buf.push(Some(b.to_vec()));
                        }
                    }
                    _ => {
                        return Err(WriteError::TypeMismatchWithSchema(v.clone(), type_.clone()));
                    }
                }
            }
            let array = if use_uuid_path {
                ArrowFixedSizeBinaryArray::try_from_sparse_iter_with_size(buf.into_iter(), 16)
                    .map_err(WriteError::from)?
            } else {
                ArrowFixedSizeBinaryArray::try_from_sparse_iter_with_size(
                    variable_buf
                        .into_iter()
                        .map(|opt| opt.map(Vec::into_boxed_slice)),
                    width_i32,
                )
                .map_err(WriteError::from)?
            };
            Ok(Arc::new(array))
        }
        ArrowDataType::List(nested_type) => array_of_lists(values, nested_type, false),
        ArrowDataType::LargeList(nested_type) => array_of_lists(values, nested_type, true),
        ArrowDataType::Struct(nested_struct) => {
            array_of_structs(values, nested_struct.as_ref(), type_)
        }
        _ => panic!("provided type {type_} is unknown to the engine"),
    }
}

/// Reasons a Pathway `str` value can't be encoded into a Delta `decimal(p, s)`
/// column at write time. Wrapped by `DeltaError::DecimalSerialization` (and
/// thus surfaced as `WriteError::Delta`), naming the offending value, the
/// column's precision and scale, and the specific constraint it violated.
#[derive(Debug, thiserror::Error)]
#[error("value {value:?} cannot be encoded as decimal({precision}, {scale}): {reason}")]
pub struct DecimalSerializationError {
    pub value: String,
    pub precision: u8,
    pub scale: i8,
    pub reason: String,
}

/// Parse decimal text (e.g. `"100.50"`, `"-200.25"`, `"0.001"`) into the unscaled
/// `i128` representation Arrow's `Decimal128` array expects, validated against the
/// column's precision and scale. The returned `Err(reason)` is just the textual
/// explanation; the caller wraps it into `DecimalSerializationError` together
/// with the offending value and the target column's shape.
///
/// Accepted forms: optional leading `+` / `-`; one optional decimal point; ASCII
/// digits only on either side. Scientific notation, currency symbols, thousand
/// separators, and trailing whitespace beyond what `str::trim` removes are all
/// rejected. Fractional digits beyond the column's `scale` are rejected (we error
/// rather than round, so the round-trip preserves the user's input bit-for-bit
/// when it fits, and surfaces precision loss when it doesn't).
pub(crate) fn parse_decimal_string(s: &str, precision: u8, scale: i8) -> Result<i128, String> {
    if scale < 0 {
        return Err(format!("negative scale {scale} is not supported"));
    }
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return Err("empty string".to_string());
    }
    let (negative, body) = match trimmed.as_bytes()[0] {
        b'-' => (true, &trimmed[1..]),
        b'+' => (false, &trimmed[1..]),
        _ => (false, trimmed),
    };
    if body.is_empty() {
        return Err("missing digits after sign".to_string());
    }
    let (int_part, frac_part) = match body.find('.') {
        Some(idx) => (&body[..idx], &body[idx + 1..]),
        None => (body, ""),
    };
    if int_part.is_empty() && frac_part.is_empty() {
        return Err("missing digits".to_string());
    }
    if !int_part.bytes().all(|b| b.is_ascii_digit())
        || !frac_part.bytes().all(|b| b.is_ascii_digit())
    {
        return Err("contains non-digit characters".to_string());
    }
    // We rejected `scale < 0` above, so the cast is safe — bounded by `i8::MAX`.
    let scale_usize = usize::try_from(scale).expect("scale is non-negative");
    let frac_len = frac_part.len();
    if frac_len > scale_usize {
        return Err(format!(
            "{frac_len} fractional digits exceeds column scale {scale}"
        ));
    }
    let int_significant = int_part.trim_start_matches('0');
    let pad = scale_usize - frac_len;
    let total_significant = int_significant.len() + frac_len + pad;
    if total_significant > usize::from(precision) {
        return Err(format!(
            "{total_significant} significant digits exceeds column precision {precision}"
        ));
    }
    // Build the unscaled integer string. `int_part` may be empty (".5") and
    // `frac_part` may be shorter than `scale` (we right-pad with zeros). Keep
    // leading zeros on `int_part` here — they don't affect the value.
    let mut combined = String::with_capacity(int_part.len() + frac_part.len() + pad);
    combined.push_str(int_part);
    combined.push_str(frac_part);
    for _ in 0..pad {
        combined.push('0');
    }
    let combined = if combined.is_empty() {
        "0"
    } else {
        combined.as_str()
    };
    let unscaled: i128 = combined
        .parse()
        .map_err(|_| format!("internal parse error for unscaled string {combined:?}"))?;
    Ok(if negative { -unscaled } else { unscaled })
}

fn array_of_simple_type<ElementType>(
    values: &[Value],
    mut to_simple_type: impl FnMut(&Value) -> Result<ElementType, WriteError>,
) -> Result<Vec<Option<ElementType>>, WriteError> {
    let mut values_vec: Vec<Option<ElementType>> = Vec::new();
    for value in values {
        if matches!(value, Value::None) {
            values_vec.push(None);
            continue;
        }
        values_vec.push(Some(to_simple_type(value)?));
    }
    Ok(values_vec)
}

fn array_of_structs(
    values: &[Value],
    nested_types: &[Arc<ArrowField>],
    struct_type: &ArrowDataType,
) -> Result<Arc<dyn ArrowArray>, WriteError> {
    // Step 1. Decompose struct into separate columns
    let mut struct_columns: Vec<Vec<Value>> = vec![Vec::new(); nested_types.len()];
    let mut defined_fields_map = BooleanBufferBuilder::new(values.len());
    defined_fields_map.resize(values.len());
    for (index, value) in values.iter().enumerate() {
        defined_fields_map.set_bit(index, value != &Value::None);
        match value {
            Value::None => {
                for item in &mut struct_columns {
                    item.push(Value::None);
                }
            }
            Value::IntArray(a) => {
                struct_columns[0].push(convert_shape_to_pathway_tuple(a.shape()));
                struct_columns[1].push(convert_contents_to_pathway_tuple(a));
            }
            Value::FloatArray(a) => {
                struct_columns[0].push(convert_shape_to_pathway_tuple(a.shape()));
                struct_columns[1].push(convert_contents_to_pathway_tuple(a));
            }
            Value::Tuple(tuple_elements) => {
                for (index, field) in tuple_elements.iter().enumerate() {
                    struct_columns[index].push(field.clone());
                }
            }
            _ => {
                return Err(WriteError::TypeMismatchWithSchema(
                    value.clone(),
                    struct_type.clone(),
                ))
            }
        }
    }

    // Step 2. Create Arrow arrays for the separate columns
    let mut arrow_arrays = Vec::new();
    for (struct_column, arrow_field) in struct_columns.iter().zip(nested_types) {
        let arrow_array = array_for_type(arrow_field.data_type(), struct_column)?;
        arrow_arrays.push(arrow_array);
    }

    // Step 3. Create a struct array
    let struct_array: Arc<dyn ArrowArray> = Arc::new(ArrowStructArray::new(
        nested_types.into(),
        arrow_arrays,
        Some(NullBuffer::new(defined_fields_map.finish())),
    ));
    Ok(struct_array)
}

fn convert_shape_to_pathway_tuple(shape: &[usize]) -> Value {
    let tuple_contents: Vec<_> = shape
        .iter()
        .map(|v| Value::Int((*v).try_into().unwrap()))
        .collect();
    Value::Tuple(tuple_contents.into())
}

fn convert_contents_to_pathway_tuple<T: Into<Value> + Clone>(contents: &Handle<ArrayD<T>>) -> Value
where
    Value: std::convert::From<T>,
{
    let tuple_contents: Vec<_> = contents.iter().map(|v| Value::from((*v).clone())).collect();
    Value::Tuple(tuple_contents.into())
}

fn array_of_lists(
    values: &[Value],
    nested_type: &Arc<ArrowField>,
    use_64bit_size_type: bool,
) -> Result<Arc<dyn ArrowArray>, WriteError> {
    let mut flat_values = Vec::new();
    let mut offsets = Vec::new();

    let mut defined_fields_map = BooleanBufferBuilder::new(values.len());
    defined_fields_map.resize(values.len());
    for (index, value) in values.iter().enumerate() {
        offsets.push(flat_values.len());
        let Value::Tuple(list) = value else {
            defined_fields_map.set_bit(index, false);
            continue;
        };
        defined_fields_map.set_bit(index, true);
        for nested_value in list.as_ref() {
            flat_values.push(nested_value.clone());
        }
    }
    offsets.push(flat_values.len());

    let flat_values = array_for_type(nested_type.data_type(), &flat_values)?;
    let null_buffer = Some(NullBuffer::new(defined_fields_map.finish()));

    let list_array: Arc<dyn ArrowArray> = if use_64bit_size_type {
        let offsets: Vec<i64> = offsets.into_iter().map(|v| v.try_into().unwrap()).collect();
        let scalar_buffer = ScalarBuffer::from(offsets);
        let offset_buffer = OffsetBuffer::new(scalar_buffer);
        Arc::new(ArrowLargeListArray::new(
            nested_type.clone(),
            offset_buffer,
            flat_values,
            null_buffer,
        ))
    } else {
        let offsets: Vec<i32> = offsets.into_iter().map(|v| v.try_into().unwrap()).collect();
        let scalar_buffer = ScalarBuffer::from(offsets);
        let offset_buffer = OffsetBuffer::new(scalar_buffer);
        Arc::new(ArrowListArray::new(
            nested_type.clone(),
            offset_buffer,
            flat_values,
            null_buffer,
        ))
    };

    Ok(list_array)
}

fn arrow_data_type(
    type_: &Type,
    settings: &LakeWriterSettings,
) -> Result<ArrowDataType, WriteError> {
    Ok(match type_ {
        Type::Bool => ArrowDataType::Boolean,
        Type::Int | Type::Duration => ArrowDataType::Int64,
        Type::Float => ArrowDataType::Float64,
        Type::String | Type::Json | Type::Pointer => ArrowDataType::Utf8,
        Type::Bytes | Type::PyObjectWrapper => {
            if settings.use_64bit_size_type {
                ArrowDataType::LargeBinary
            } else {
                ArrowDataType::Binary
            }
        }
        // DeltaLake timestamps are stored in microseconds:
        // https://docs.rs/deltalake/latest/deltalake/kernel/enum.PrimitiveType.html#variant.Timestamp
        Type::DateTimeNaive => ArrowDataType::Timestamp(settings.timestamp_unit, None),
        Type::DateTimeUtc => ArrowDataType::Timestamp(
            settings.timestamp_unit,
            Some(settings.utc_timezone_name.clone().into()),
        ),
        Type::Optional(wrapped) => return arrow_data_type(wrapped, settings),
        Type::List(wrapped_type) => {
            let wrapped_type_is_optional = wrapped_type.is_optional();
            let wrapped_arrow_type = arrow_data_type(wrapped_type, settings)?;
            let list_field = ArrowField::new(
                NDARRAY_SINGLE_ELEMENT_FIELD_NAME,
                wrapped_arrow_type,
                wrapped_type_is_optional,
            );
            ArrowDataType::List(list_field.into())
        }
        Type::Array(_, wrapped_type) => {
            let wrapped_type = wrapped_type.as_ref();
            let elements_arrow_type = match wrapped_type {
                Type::Int => ArrowDataType::Int64,
                Type::Float => ArrowDataType::Float64,
                _ => panic!("Type::Array can't contain elements of the type {wrapped_type:?}"),
            };
            let struct_fields_vector = vec![
                ArrowField::new(
                    NDARRAY_SHAPE_FIELD_NAME,
                    ArrowDataType::List(
                        ArrowField::new(
                            NDARRAY_SINGLE_ELEMENT_FIELD_NAME,
                            ArrowDataType::Int64,
                            true,
                        )
                        .into(),
                    ),
                    false,
                ),
                ArrowField::new(
                    NDARRAY_ELEMENTS_FIELD_NAME,
                    ArrowDataType::List(
                        ArrowField::new(
                            NDARRAY_SINGLE_ELEMENT_FIELD_NAME,
                            elements_arrow_type,
                            true,
                        )
                        .into(),
                    ),
                    false,
                ),
            ];
            let struct_fields = ArrowFields::from(struct_fields_vector);
            ArrowDataType::Struct(struct_fields)
        }
        Type::Tuple(wrapped_types) => {
            let mut struct_fields = Vec::new();
            for (index, wrapped_type) in wrapped_types.iter().enumerate() {
                let nested_arrow_type = arrow_data_type(wrapped_type, settings)?;
                let nested_type_is_optional = wrapped_type.is_optional();
                struct_fields.push(ArrowField::new(
                    format!("[{index}]"),
                    nested_arrow_type,
                    nested_type_is_optional,
                ));
            }
            let struct_descriptor = ArrowFields::from(struct_fields);
            ArrowDataType::Struct(struct_descriptor)
        }
        Type::Any | Type::Future(_) => return Err(WriteError::UnsupportedType(type_.clone())),
    })
}

pub fn construct_schema(
    value_fields: &[ValueField],
    writer: &dyn LakeBatchWriter,
    mode: MaintenanceMode,
) -> Result<ArrowSchema, WriteError> {
    let settings = writer.settings();
    let metadata_per_column = writer.metadata_per_column();
    let arrow_type_overrides = writer.arrow_type_overrides();
    let mut schema_fields: Vec<ArrowField> = Vec::new();
    for field in value_fields {
        let metadata = metadata_per_column
            .get(&field.name)
            .unwrap_or(&HashMap::new())
            .clone();
        // The writer may override the storage type for individual columns to match
        // an existing destination schema (e.g. a Pathway `str` column written into a
        // pre-existing Delta `decimal(p, s)` column). The user-declared Pathway type
        // and the Arrow type can therefore disagree; the row-conversion path in
        // `array_for_type` decides whether the value is convertible.
        let arrow_type = match arrow_type_overrides.get(&field.name) {
            Some(t) => t.clone(),
            None => arrow_data_type(&field.type_, &settings)?,
        };
        schema_fields.push(
            ArrowField::new(field.name.clone(), arrow_type, field.type_.can_be_none())
                .with_metadata(metadata),
        );
    }
    for (field, type_) in mode.additional_output_fields() {
        let metadata = metadata_per_column
            .get(field)
            .unwrap_or(&HashMap::new())
            .clone();
        schema_fields.push(
            ArrowField::new(field, arrow_data_type(&type_, &settings)?, false)
                .with_metadata(metadata),
        );
    }
    Ok(ArrowSchema::new(schema_fields))
}

#[cfg(test)]
mod tests {
    use deltalake::arrow::array::{Array, FixedSizeBinaryArray, Float32Array, Int32Array};
    use deltalake::arrow::datatypes::DataType as ArrowDataType;

    use super::{array_for_type, parse_decimal_string};
    use crate::connectors::data_storage::data_lake::iceberg::IcebergError;
    use crate::connectors::WriteError;
    use crate::engine::Value;

    #[test]
    fn decimal_string_happy_path() {
        // Pathway str -> Arrow Decimal128 unscaled i128, with various shapes.
        assert_eq!(parse_decimal_string("100.50", 8, 2), Ok(10050));
        assert_eq!(parse_decimal_string("-200.25", 8, 2), Ok(-20025));
        assert_eq!(parse_decimal_string("100", 8, 2), Ok(10000));
        assert_eq!(parse_decimal_string("100.5", 8, 2), Ok(10050));
        assert_eq!(parse_decimal_string("0", 4, 2), Ok(0));
        assert_eq!(parse_decimal_string("0.00", 4, 2), Ok(0));
        assert_eq!(parse_decimal_string(".5", 3, 2), Ok(50));
        assert_eq!(parse_decimal_string("1.", 3, 2), Ok(100));
        assert_eq!(parse_decimal_string("+100.50", 8, 2), Ok(10050));
        assert_eq!(parse_decimal_string("  100.50  ", 8, 2), Ok(10050));
        // 38-digit decimal value at the edge of Decimal128's range.
        assert_eq!(
            parse_decimal_string("1234567890123456789012345678.9012345678", 38, 10),
            Ok(12_345_678_901_234_567_890_123_456_789_012_345_678),
        );
        // Leading zeros don't count toward precision.
        assert_eq!(parse_decimal_string("00001.5", 3, 2), Ok(150));
    }

    #[test]
    fn decimal_string_too_many_fractional_digits_errors_clearly() {
        let err = parse_decimal_string("100.555", 8, 2).unwrap_err();
        assert!(
            err.contains("fractional digits") && err.contains("scale"),
            "expected scale-violation message, got {err}"
        );
    }

    #[test]
    fn decimal_string_exceeds_precision_errors_clearly() {
        let err = parse_decimal_string("10000.00", 6, 2).unwrap_err();
        assert!(
            err.contains("precision"),
            "expected precision-violation message, got {err}"
        );
    }

    #[test]
    fn decimal_string_non_digit_errors_clearly() {
        let err = parse_decimal_string("abc", 8, 2).unwrap_err();
        assert!(
            err.contains("non-digit"),
            "expected non-digit message, got {err}"
        );
        let err = parse_decimal_string("1.5e2", 8, 2).unwrap_err();
        assert!(
            err.contains("non-digit"),
            "scientific notation is rejected, got {err}"
        );
        let err = parse_decimal_string("1,000.5", 8, 2).unwrap_err();
        assert!(
            err.contains("non-digit"),
            "thousand separators rejected, got {err}"
        );
    }

    #[test]
    fn decimal_string_empty_errors_clearly() {
        assert!(parse_decimal_string("", 8, 2)
            .unwrap_err()
            .contains("empty"));
        assert!(parse_decimal_string("   ", 8, 2)
            .unwrap_err()
            .contains("empty"));
        assert!(parse_decimal_string("-", 8, 2)
            .unwrap_err()
            .contains("missing"));
        assert!(parse_decimal_string(".", 8, 2)
            .unwrap_err()
            .contains("missing digits"));
    }

    #[test]
    fn decimal_string_negative_scale_unsupported() {
        assert!(parse_decimal_string("100", 8, -2)
            .unwrap_err()
            .contains("negative scale"));
    }

    // The Iceberg writer uses Arrow Int32 only when the existing destination
    // column is a 32-bit `int`. Pathway `int` is i64, so the value-level
    // conversion has to refuse silent truncation.
    #[test]
    fn int32_narrowing_in_range_succeeds() {
        let values = vec![
            Value::Int(0),
            Value::Int(i64::from(i32::MIN)),
            Value::Int(i64::from(i32::MAX)),
            Value::None,
        ];
        let arr = array_for_type(&ArrowDataType::Int32, &values).unwrap();
        let int_arr = arr.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(int_arr.value(0), 0);
        assert_eq!(int_arr.value(1), i32::MIN);
        assert_eq!(int_arr.value(2), i32::MAX);
        assert!(int_arr.is_null(3));
    }

    #[test]
    fn int32_narrowing_out_of_range_errors() {
        let values = vec![Value::Int(i64::from(i32::MAX) + 1)];
        let err = array_for_type(&ArrowDataType::Int32, &values).unwrap_err();
        match err {
            WriteError::Iceberg(IcebergError::IntegerNarrowingOverflow { value, target }) => {
                assert_eq!(value, i64::from(i32::MAX) + 1);
                assert_eq!(target, ArrowDataType::Int32);
            }
            other => panic!("expected IntegerNarrowingOverflow, got {other:?}"),
        }
    }

    // f64 → f32 cast is always defined and can produce ±∞ for values outside f32
    // range — the writer doesn't reject those because IEEE 754 requires exactly
    // this behavior, and reading the column back will reproduce the infinity.
    #[test]
    fn float32_narrowing_lossy_but_no_panic() {
        let values = vec![
            Value::from(0.0_f64),
            Value::from(1.5_f64),
            Value::from(f64::MAX),
            Value::None,
        ];
        let arr = array_for_type(&ArrowDataType::Float32, &values).unwrap();
        let float_arr = arr.as_any().downcast_ref::<Float32Array>().unwrap();
        // Compare bits to dodge clippy::float_cmp; the values are exactly
        // representable in f32, so there's no precision wiggle to worry about.
        assert_eq!(float_arr.value(0).to_bits(), 0.0_f32.to_bits());
        assert_eq!(float_arr.value(1).to_bits(), 1.5_f32.to_bits());
        assert!(float_arr.value(2).is_infinite());
        assert!(float_arr.is_null(3));
    }

    // FixedSizeBinary(16) is the shape Iceberg's `uuid` arrives as. The Pathway
    // value type tells the writer how to interpret it: `str` → parse canonical
    // UUID, `bytes` → length-checked passthrough.
    #[test]
    fn fixed_size_binary_uuid_from_string() {
        let values = vec![
            Value::String("550e8400-e29b-41d4-a716-446655440000".into()),
            Value::None,
        ];
        let arr = array_for_type(&ArrowDataType::FixedSizeBinary(16), &values).unwrap();
        let bin = arr.as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap();
        let expected: [u8; 16] = [
            0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44,
            0x00, 0x00,
        ];
        assert_eq!(bin.value(0), &expected);
        assert!(bin.is_null(1));
    }

    #[test]
    fn fixed_size_binary_uuid_invalid_string_errors() {
        let values = vec![Value::String("not-a-uuid".into())];
        let err = array_for_type(&ArrowDataType::FixedSizeBinary(16), &values).unwrap_err();
        match err {
            WriteError::Iceberg(IcebergError::UuidParseError { value }) => {
                assert_eq!(value, "not-a-uuid");
            }
            other => panic!("expected UuidParseError, got {other:?}"),
        }
    }

    #[test]
    fn fixed_size_binary_bytes_passthrough() {
        let values = vec![
            Value::Bytes((vec![1u8, 2, 3, 4, 5, 6, 7, 8]).into()),
            Value::None,
        ];
        let arr = array_for_type(&ArrowDataType::FixedSizeBinary(8), &values).unwrap();
        let bin = arr.as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap();
        assert_eq!(bin.value(0), &[1u8, 2, 3, 4, 5, 6, 7, 8]);
        assert!(bin.is_null(1));
    }

    #[test]
    fn fixed_size_binary_length_mismatch_errors() {
        let values = vec![Value::Bytes((vec![1u8, 2, 3]).into())];
        let err = array_for_type(&ArrowDataType::FixedSizeBinary(8), &values).unwrap_err();
        match err {
            WriteError::Iceberg(IcebergError::FixedSizeBinaryLengthMismatch {
                value_len,
                expected,
            }) => {
                assert_eq!(value_len, 3);
                assert_eq!(expected, 8);
            }
            other => panic!("expected FixedSizeBinaryLengthMismatch, got {other:?}"),
        }
    }

    // For width 16 with a Pathway `bytes` value (i.e. user is treating the
    // column as fixed(16), not uuid), accept length-correct bytes verbatim.
    #[test]
    fn fixed_size_binary_16_with_raw_bytes_passes_through() {
        let raw: Vec<u8> = (0u8..16).collect();
        let values = vec![Value::Bytes(raw.clone().into())];
        let arr = array_for_type(&ArrowDataType::FixedSizeBinary(16), &values).unwrap();
        let bin = arr.as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap();
        assert_eq!(bin.value(0), raw.as_slice());
    }

    // The writer rejects values that aren't str/bytes/None for FixedSizeBinary,
    // surfacing a clear schema-mismatch error rather than silently coercing.
    #[test]
    fn fixed_size_binary_wrong_pathway_type_errors() {
        let values = vec![Value::Int(42)];
        let err = array_for_type(&ArrowDataType::FixedSizeBinary(8), &values).unwrap_err();
        match err {
            WriteError::TypeMismatchWithSchema(value, target) => {
                assert!(matches!(value, Value::Int(42)));
                assert_eq!(target, ArrowDataType::FixedSizeBinary(8));
            }
            other => panic!("expected TypeMismatchWithSchema, got {other:?}"),
        }
    }

    // Verify the existing wide-int path still emits Int64 — guards against an
    // accidental swap with the new Int32 narrowing arm.
    #[test]
    fn int64_path_emits_int64() {
        use deltalake::arrow::array::Int64Array;
        let values = vec![Value::Int(42), Value::None];
        let arr = array_for_type(&ArrowDataType::Int64, &values).unwrap();
        let int_arr = arr.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(int_arr.value(0), 42);
        assert!(int_arr.is_null(1));
    }

    // Iceberg `time` ↔ Pathway `Duration` (microseconds since midnight). Same
    // convention as the Postgres TIME mapping.
    #[test]
    fn time64_us_from_duration() {
        use deltalake::arrow::array::Time64MicrosecondArray;
        use deltalake::arrow::datatypes::TimeUnit;

        use crate::engine::Duration as EngineDuration;
        // 12:34:56.789012 since midnight, in microseconds.
        let micros: i64 = ((12 * 3600 + 34 * 60 + 56) * 1_000_000) + 789_012;
        let values = vec![
            Value::Duration(EngineDuration::new_with_unit(micros, "us").unwrap()),
            Value::None,
        ];
        let arr = array_for_type(&ArrowDataType::Time64(TimeUnit::Microsecond), &values).unwrap();
        let time_arr = arr
            .as_any()
            .downcast_ref::<Time64MicrosecondArray>()
            .unwrap();
        assert_eq!(time_arr.value(0), micros);
        assert!(time_arr.is_null(1));
    }

    #[test]
    fn time64_us_rejects_non_duration() {
        use deltalake::arrow::datatypes::TimeUnit;
        let values = vec![Value::Int(42)];
        let err =
            array_for_type(&ArrowDataType::Time64(TimeUnit::Microsecond), &values).unwrap_err();
        match err {
            WriteError::TypeMismatchWithSchema(value, target) => {
                assert!(matches!(value, Value::Int(42)));
                assert_eq!(target, ArrowDataType::Time64(TimeUnit::Microsecond));
            }
            other => panic!("expected TypeMismatchWithSchema, got {other:?}"),
        }
    }
}
