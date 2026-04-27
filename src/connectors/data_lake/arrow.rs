use std::collections::HashMap;
use std::sync::Arc;

use deltalake::arrow::array::Array as ArrowArray;
use deltalake::arrow::array::{
    BinaryArray as ArrowBinaryArray, BooleanArray as ArrowBooleanArray, BooleanBufferBuilder,
    Decimal128Array as ArrowDecimal128Array, Float64Array as ArrowFloat64Array,
    Int64Array as ArrowInt64Array, LargeBinaryArray as ArrowLargeBinaryArray,
    LargeListArray as ArrowLargeListArray, ListArray as ArrowListArray,
    StringArray as ArrowStringArray, StructArray as ArrowStructArray,
    TimestampMicrosecondArray as ArrowTimestampMsArray,
    TimestampNanosecondArray as ArrowTimestampNsArray,
};
use deltalake::arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use deltalake::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Fields as ArrowFields, Schema as ArrowSchema,
    TimeUnit as ArrowTimeUnit,
};
use ndarray::ArrayD;

use super::{LakeWriterSettings, MaintenanceMode};
use crate::connectors::data_format::{
    NDARRAY_ELEMENTS_FIELD_NAME, NDARRAY_SHAPE_FIELD_NAME, NDARRAY_SINGLE_ELEMENT_FIELD_NAME,
};
use crate::connectors::data_lake::LakeBatchWriter;
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
        ArrowDataType::Float64 => {
            let v = array_of_simple_type::<f64>(values, |v| match v {
                Value::Float(f) => Ok((*f).into()),
                _ => Err(WriteError::TypeMismatchWithSchema(v.clone(), type_.clone())),
            })?;
            Ok(Arc::new(ArrowFloat64Array::from(v)))
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
        ArrowDataType::List(nested_type) => array_of_lists(values, nested_type, false),
        ArrowDataType::LargeList(nested_type) => array_of_lists(values, nested_type, true),
        ArrowDataType::Struct(nested_struct) => array_of_structs(values, nested_struct.as_ref()),
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
            _ => panic!("Pathway type {value} is not serializable as an arrow tuple"),
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
    use super::parse_decimal_string;

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
}
