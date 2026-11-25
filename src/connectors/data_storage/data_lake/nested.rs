// Copyright © 2026 Pathway

//! Schema-guided conversion of native data-lake structures into JSON columns.
//! Ordinary columns keep the existing vectorized decoders.

use std::collections::HashMap;
use std::sync::Arc;

use super::{
    column_into_pathway_values, parquet_value_into_pathway_value, scale_to_nanos, ArrowArray,
    ArrowDataType, AsArray, ConversionError, Int64Type, ParquetValue, Type, Value, NANOS_PER_DAY,
    NANOS_PER_MICROSECOND, NANOS_PER_MILLISECOND,
};
use crate::connectors::data_format::json_schema::{child_path, JsonSchema};
use crate::connectors::data_format::serialize_value_to_json;
use serde_json::Value as Json;

pub type NestedSchemas = HashMap<String, JsonSchema>;

pub(super) fn is_json_text_type(dtype: &ArrowDataType) -> bool {
    match dtype {
        ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 | ArrowDataType::Utf8View => true,
        ArrowDataType::Dictionary(_, value) => is_json_text_type(value),
        _ => false,
    }
}

fn unoptional(schema: Option<&JsonSchema>) -> Option<&JsonSchema> {
    match schema {
        Some(JsonSchema::Optional { wrapped }) => Some(wrapped),
        other => other,
    }
}

fn child<'a>(schema: Option<&'a JsonSchema>, name: &str, index: usize) -> Option<&'a JsonSchema> {
    match unoptional(schema) {
        Some(JsonSchema::Object { fields }) => fields
            .iter()
            .find(|field| field.name == name)
            .map(|field| &field.schema),
        Some(JsonSchema::List { items }) => Some(items),
        Some(JsonSchema::Map { values }) => Some(values),
        Some(JsonSchema::Tuple { items }) => items.get(index),
        _ => None,
    }
}

#[allow(clippy::unnecessary_box_returns)] // Match the shared ParsedValue error type.
fn invalid(path: &str, message: impl Into<String>) -> Box<ConversionError> {
    Box::new(ConversionError::new(
        "native nested value".into(),
        path.into(),
        Type::Json,
        Some(message.into()),
    ))
}

fn encode(value: &Value, path: &str) -> Result<Json, Box<ConversionError>> {
    let json = serialize_value_to_json(value).map_err(|e| invalid(path, e.to_string()))?;
    if json.is_null() && value != &Value::None {
        return Err(invalid(path, "value cannot be represented in JSON"));
    }
    Ok(json)
}

fn flatten(value: Json, shape: &[usize], elements: &mut Vec<Json>) -> Option<()> {
    match value {
        Json::Array(items) => {
            let (length, tail) = shape.split_first()?;
            if items.len() != *length {
                return None;
            }
            for item in items {
                flatten(item, tail, elements)?;
            }
            Some(())
        }
        Json::Number(_) if shape.is_empty() => {
            elements.push(value);
            Some(())
        }
        _ => None,
    }
}

fn finish_array(
    value: Json,
    schema: Option<&JsonSchema>,
    path: &str,
) -> Result<Json, Box<ConversionError>> {
    if !matches!(unoptional(schema), Some(JsonSchema::Array { .. }))
        || value.is_object()
        || value.is_null()
    {
        return Ok(value);
    }
    let mut shape = Vec::new();
    let mut first = &value;
    while let Json::Array(items) = first {
        shape.push(items.len());
        let Some(item) = items.first() else { break };
        first = item;
    }
    let mut elements = Vec::new();
    flatten(value, &shape, &mut elements)
        .ok_or_else(|| invalid(path, "expected a rectangular numeric array"))?;
    Ok(serde_json::json!({"shape": shape, "elements": elements}))
}

#[allow(clippy::too_many_lines)]
pub(super) fn parquet(
    value: &ParquetValue,
    schema: Option<&JsonSchema>,
    path: &str,
) -> Result<Json, Box<ConversionError>> {
    let schema = unoptional(schema);
    let json = match value {
        ParquetValue::Null => Json::Null,
        ParquetValue::Group(row) => {
            let mut object = serde_json::Map::new();
            let mut tuple = Vec::new();
            for (index, (name, value)) in row.get_column_iter().enumerate() {
                let json = parquet(value, child(schema, name, index), &child_path(path, name))?;
                if matches!(schema, Some(JsonSchema::Tuple { .. })) {
                    tuple.push(json);
                } else {
                    object.insert(name.clone(), json);
                }
            }
            if matches!(schema, Some(JsonSchema::Tuple { .. })) {
                Json::Array(tuple)
            } else {
                Json::Object(object)
            }
        }
        ParquetValue::ListInternal(list) => Json::Array(
            list.elements()
                .iter()
                .enumerate()
                .map(|(index, value)| {
                    parquet(
                        value,
                        child(schema, "", index),
                        &child_path(path, &index.to_string()),
                    )
                })
                .collect::<Result<_, _>>()?,
        ),
        ParquetValue::MapInternal(map) => {
            let mut object = serde_json::Map::new();
            for (key, value) in map.entries() {
                let ParquetValue::Str(name) = key else {
                    return Err(invalid(path, "JSON maps require string keys"));
                };
                let json = parquet(value, child(schema, name, 0), &child_path(path, name))?;
                if object.insert(name.clone(), json).is_some() {
                    return Err(invalid(path, format!("duplicate map key {name:?}")));
                }
            }
            Json::Object(object)
        }
        _ => {
            let mut dtype = schema.and_then(JsonSchema::leaf_type).unwrap_or(Type::Any);
            // JSON's float codec accepts integer inputs; preserve the integer
            // until the shared validator/access codec handles the conversion.
            if matches!(dtype, Type::Float | Type::Array(_, _))
                && !matches!(value, ParquetValue::Decimal(_))
            {
                dtype = Type::Any;
            }
            let scale = match (value, &dtype) {
                (ParquetValue::TimestampMicros(n), _) | (ParquetValue::Long(n), Type::Duration) => {
                    Some((*n, NANOS_PER_MICROSECOND))
                }
                (ParquetValue::TimestampMillis(n), _) => Some((*n, NANOS_PER_MILLISECOND)),
                (ParquetValue::Date(n), _) => Some((i64::from(*n), NANOS_PER_DAY)),
                _ => None,
            };
            if let Some((n, unit)) = scale {
                scale_to_nanos(n, unit, path, &dtype)?;
            }
            encode(
                &parquet_value_into_pathway_value(value, &dtype, path)?,
                path,
            )?
        }
    };
    finish_array(json, schema, path)
}

#[allow(clippy::too_many_lines)]
pub(super) fn arrow(
    column: &Arc<dyn ArrowArray>,
    index: usize,
    schema: Option<&JsonSchema>,
    path: &str,
) -> Result<Json, Box<ConversionError>> {
    if column.is_null(index) {
        return Ok(Json::Null);
    }
    let schema = unoptional(schema);
    let json = match column.data_type() {
        ArrowDataType::Struct(fields) => {
            let array = column.as_struct();
            let mut object = serde_json::Map::new();
            let mut tuple = Vec::new();
            for (position, field) in fields.iter().enumerate() {
                let name = field.name();
                let json = arrow(
                    array.column(position),
                    index,
                    child(schema, name, position),
                    &child_path(path, name),
                )?;
                if matches!(schema, Some(JsonSchema::Tuple { .. })) {
                    tuple.push(json);
                } else {
                    object.insert(name.clone(), json);
                }
            }
            if matches!(schema, Some(JsonSchema::Tuple { .. })) {
                Json::Array(tuple)
            } else {
                Json::Object(object)
            }
        }
        ArrowDataType::List(_)
        | ArrowDataType::LargeList(_)
        | ArrowDataType::FixedSizeList(_, _)
        | ArrowDataType::ListView(_)
        | ArrowDataType::LargeListView(_) => {
            let values = match column.data_type() {
                ArrowDataType::List(_) => column.as_list::<i32>().value(index),
                ArrowDataType::LargeList(_) => column.as_list::<i64>().value(index),
                ArrowDataType::ListView(_) => column.as_list_view::<i32>().value(index),
                ArrowDataType::LargeListView(_) => column.as_list_view::<i64>().value(index),
                _ => column.as_fixed_size_list().value(index),
            };
            Json::Array(
                (0..values.len())
                    .map(|i| {
                        arrow(
                            &values,
                            i,
                            child(schema, "", i),
                            &child_path(path, &i.to_string()),
                        )
                    })
                    .collect::<Result<_, _>>()?,
            )
        }
        ArrowDataType::Map(_, _) => {
            let entries = column.as_map().value(index);
            if !is_json_text_type(entries.column(0).data_type()) {
                return Err(invalid(path, "JSON maps require string keys"));
            }
            let mut object = serde_json::Map::new();
            for i in 0..entries.len() {
                let key = arrow(entries.column(0), i, None, path)?;
                let Json::String(name) = key else {
                    return Err(invalid(path, "JSON maps require string keys"));
                };
                let json = arrow(
                    entries.column(1),
                    i,
                    child(schema, &name, 0),
                    &child_path(path, &name),
                )?;
                if object.insert(name.clone(), json).is_some() {
                    return Err(invalid(path, format!("duplicate map key {name:?}")));
                }
            }
            Json::Object(object)
        }
        ArrowDataType::Dictionary(_, value_type) => {
            let decoded = deltalake::arrow::compute::cast(&column.slice(index, 1), value_type)
                .map_err(|e| invalid(path, e.to_string()))?;
            return arrow(&decoded, 0, schema, path);
        }
        _ => {
            let mut dtype = schema.and_then(JsonSchema::leaf_type).unwrap_or(Type::Any);
            if matches!(dtype, Type::Float | Type::Array(_, _))
                && !matches!(
                    column.data_type(),
                    ArrowDataType::Decimal128(_, _) | ArrowDataType::Decimal256(_, _)
                )
            {
                dtype = Type::Any;
            }
            // The flat Any decoder treats binary as serialized engine values.
            // Opaque JSON binary leaves must stay bytes instead.
            if dtype == Type::Any
                && matches!(
                    column.data_type(),
                    ArrowDataType::Binary
                        | ArrowDataType::LargeBinary
                        | ArrowDataType::BinaryView
                        | ArrowDataType::FixedSizeBinary(_)
                )
            {
                dtype = Type::Bytes;
            }
            let slice = column.slice(index, 1);
            // The flat Int64 duration representation uses microseconds.
            // Check the range before entering the legacy flat-column codec.
            if dtype == Type::Duration && *column.data_type() == ArrowDataType::Int64 {
                scale_to_nanos(
                    slice.as_primitive::<Int64Type>().value(0),
                    NANOS_PER_MICROSECOND,
                    path,
                    &dtype,
                )?;
            }
            let value = column_into_pathway_values(&slice, &dtype, path, 1).remove(0)?;
            encode(&value, path)?
        }
    };
    finish_array(json, schema, path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connectors::data_storage::data_lake::ParquetRow;
    use deltalake::arrow::array::{BinaryArray, Int64Array, StringArray, StructArray};
    use deltalake::arrow::datatypes::Field;
    use serde_json::json;

    #[test]
    fn native_nested_names_and_overflows() {
        let schema: JsonSchema = serde_json::from_value(json!({"type":"object","fields":[
            {"name":"span","schema":{"type":"duration"}},
            {"name":"value","schema":{"type":"int"}}
        ]}))
        .unwrap();
        let values: Arc<dyn ArrowArray> = Arc::new(StructArray::from(vec![
            (
                Arc::new(Field::new("value", ArrowDataType::Int64, false)),
                Arc::new(Int64Array::from(vec![7, 8])) as Arc<dyn ArrowArray>,
            ),
            (
                Arc::new(Field::new("span", ArrowDataType::Int64, false)),
                Arc::new(Int64Array::from(vec![123, i64::MAX])) as Arc<dyn ArrowArray>,
            ),
        ]));
        assert_eq!(
            arrow(&values, 0, Some(&schema), "/one").unwrap(),
            json!({"value":7,"span":123_000})
        );
        assert!(arrow(&values, 1, Some(&schema), "/one")
            .unwrap_err()
            .to_string()
            .contains("/one/span"));
        let row = ParquetValue::Group(ParquetRow::new(vec![
            ("value".into(), ParquetValue::Long(7)),
            ("span".into(), ParquetValue::Long(123)),
        ]));
        assert_eq!(
            parquet(&row, Some(&schema), "/one").unwrap(),
            json!({"value":7,"span":123_000})
        );
        for value in [
            ParquetValue::TimestampMicros(i64::MAX),
            ParquetValue::TimestampMillis(i64::MAX),
            ParquetValue::Date(i32::MAX),
        ] {
            assert!(parquet(&value, None, "/one/at")
                .unwrap_err()
                .to_string()
                .contains("/one/at"));
        }
    }

    #[test]
    fn native_nested_array_shape_and_nulls() {
        let schema: JsonSchema =
            serde_json::from_value(json!({"type":"array","n_dim":2,"items":{"type":"float"}}))
                .unwrap();
        assert_eq!(
            finish_array(json!([[1, 2], [3, 4]]), Some(&schema), "/one").unwrap(),
            json!({"shape":[2,2],"elements":[1,2,3,4]})
        );
        assert!(finish_array(json!([[1], [2, 3]]), Some(&schema), "/one").is_err());
        assert!(finish_array(json!([[null]]), Some(&schema), "/one").is_err());
        let bytes: Arc<dyn ArrowArray> = Arc::new(BinaryArray::from_iter_values([b"ok"]));
        assert_eq!(
            arrow(&bytes, 0, None, "/one/content").unwrap(),
            json!("b2s=")
        );
        let values: Arc<dyn ArrowArray> = Arc::new(StringArray::from(vec![Some("a"), None]));
        assert_eq!(arrow(&values, 0, None, "/one").unwrap(), json!("a"));
        assert_eq!(arrow(&values, 1, None, "/one").unwrap(), Json::Null);
    }
}
