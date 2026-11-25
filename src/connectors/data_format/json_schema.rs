// Copyright © 2026 Pathway

//! Validation metadata for JSON-valued columns. This is not an engine value type.

use apache_avro::types::Value as AvroValue;
use indexmap::IndexMap;
use mongodb::bson::Bson;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::engine::Type;

use super::{
    avro::{avro_value_to_engine, avro_value_to_json},
    bson::BsonParser,
    parse_value_from_json, serialize_value_to_json,
};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum JsonSchema {
    Object {
        fields: Vec<JsonField>,
    },
    List {
        items: Box<Self>,
    },
    Map {
        values: Box<Self>,
    },
    Tuple {
        items: Vec<Self>,
    },
    Optional {
        wrapped: Box<Self>,
    },
    Bool,
    Int,
    Float,
    String,
    Bytes,
    DateTimeNaive,
    DateTimeUtc,
    Duration,
    Pointer,
    PyObjectWrapper,
    Array {
        n_dim: Option<usize>,
        items: Box<Self>,
    },
    Json,
    Null,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct JsonField {
    pub(crate) name: String,
    pub(crate) schema: JsonSchema,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) default: Option<JsonDefault>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct JsonDefault {
    pub(crate) value: JsonValue,
}

#[derive(Debug, thiserror::Error)]
#[error("nested JSON schema validation failed at {path}: {message}")]
pub struct JsonSchemaError {
    path: String,
    message: String,
}

impl JsonSchemaError {
    pub(super) fn new(path: String, message: String) -> Self {
        Self { path, message }
    }

    pub(super) fn missing_field(path: String) -> Self {
        Self {
            path,
            message: "required field is missing".to_owned(),
        }
    }
}

impl JsonSchema {
    /// Normalize native Avro values before logical type and unit information
    /// is lost in structural JSON conversion. JSON-text columns bypass this
    /// walk and use the same validator as JSON input.
    #[allow(clippy::too_many_lines)]
    pub(super) fn normalize_avro(
        &self,
        value: AvroValue,
        path: &str,
    ) -> Result<JsonValue, JsonSchemaError> {
        let invalid = |message: &str| JsonSchemaError::new(path.to_owned(), message.to_owned());
        let opaque = |value| {
            avro_value_to_json(value)
                .ok_or_else(|| invalid("Avro value cannot be represented in JSON"))
        };
        if let AvroValue::Union(_, inner) = value {
            return self.normalize_avro(*inner, path);
        }
        match self {
            Self::Object { fields } => {
                let mut values: IndexMap<_, _> = match value {
                    AvroValue::Record(fields) => fields.into_iter().collect(),
                    AvroValue::Map(fields) => fields.into_iter().collect(),
                    _ => return Err(invalid("expected a record or map")),
                };
                let mut declared = serde_json::Map::new();
                for field in fields {
                    if let Some(value) = values.get_mut(&field.name) {
                        declared.insert(
                            field.name.clone(),
                            field.schema.normalize_avro(
                                std::mem::replace(value, AvroValue::Null),
                                &child_path(path, &field.name),
                            )?,
                        );
                    }
                }
                values
                    .into_iter()
                    .map(|(name, value)| {
                        let value = match declared.remove(&name) {
                            Some(value) => value,
                            None => avro_value_to_json(value).ok_or_else(|| {
                                JsonSchemaError::new(
                                    child_path(path, &name),
                                    "Avro value cannot be represented in JSON".to_owned(),
                                )
                            })?,
                        };
                        Ok((name, value))
                    })
                    .collect::<Result<_, _>>()
                    .map(JsonValue::Object)
            }
            Self::Map { values } => {
                let AvroValue::Map(map) = value else {
                    return Err(invalid("expected a map"));
                };
                map.into_iter()
                    .map(|(name, value)| {
                        Ok((
                            name.clone(),
                            values.normalize_avro(value, &child_path(path, &name))?,
                        ))
                    })
                    .collect::<Result<_, JsonSchemaError>>()
                    .map(JsonValue::Object)
            }
            Self::List { .. } | Self::Tuple { .. } => {
                let AvroValue::Array(array) = value else {
                    return Err(invalid("expected an array"));
                };
                if let Self::Tuple { items } = self {
                    if array.len() != items.len() {
                        return Err(invalid(&format!(
                            "expected an array of length {}",
                            items.len()
                        )));
                    }
                }
                array
                    .into_iter()
                    .enumerate()
                    .map(|(index, value)| {
                        let schema = match self {
                            Self::List { items } => items.as_ref(),
                            Self::Tuple { items } => &items[index],
                            _ => unreachable!(),
                        };
                        schema.normalize_avro(value, &child_path(path, &index.to_string()))
                    })
                    .collect::<Result<Vec<_>, _>>()
                    .map(JsonValue::Array)
            }
            Self::Optional { wrapped } => {
                if matches!(value, AvroValue::Null) {
                    Ok(JsonValue::Null)
                } else {
                    wrapped.normalize_avro(value, path)
                }
            }
            Self::Json | Self::Null => opaque(value),
            Self::Array { .. } => normalize_avro_array(value, path),
            Self::PyObjectWrapper => match value {
                AvroValue::Bytes(_) | AvroValue::Fixed(_, _) => opaque(value),
                _ => Err(invalid(
                    "expected bytes containing a serialized Python object wrapper",
                )),
            },
            _ => {
                let dtype = self.leaf_type().expect("scalar schema has a type");
                let value = avro_value_to_engine(value, &dtype, path, false)
                    .map_err(|error| invalid(&error.to_string()))?;
                let json =
                    serialize_value_to_json(&value).map_err(|error| invalid(&error.to_string()))?;
                if json.is_null() {
                    return Err(invalid(&format!("expected {dtype} representable in JSON")));
                }
                Ok(json)
            }
        }
    }

    /// Normalize declared BSON leaves to the JSON representation used by typed
    /// field access. Undeclared fields and opaque Json leaves keep the ordinary
    /// BSON-to-JSON conversion. Defaults and JSON representability are checked
    /// by `validate` after this conversion, using the same rules as JSON input.
    #[allow(clippy::too_many_lines)]
    pub(super) fn normalize_bson(
        &self,
        value: &Bson,
        path: &str,
    ) -> Result<JsonValue, JsonSchemaError> {
        let invalid = |message: String| JsonSchemaError {
            path: path.to_owned(),
            message,
        };
        let opaque =
            |value: &Bson| serde_json::to_value(value).map_err(|error| invalid(error.to_string()));
        match self {
            Self::Object { fields } => {
                let Bson::Document(document) = value else {
                    return Err(invalid("expected an object".to_owned()));
                };
                let mut declared = serde_json::Map::new();
                for field in fields {
                    if let Some(value) = document.get(&field.name) {
                        declared.insert(
                            field.name.clone(),
                            field
                                .schema
                                .normalize_bson(value, &child_path(path, &field.name))?,
                        );
                    }
                }
                let mut object = serde_json::Map::new();
                for (name, value) in document {
                    let value = match declared.remove(name) {
                        Some(value) => value,
                        None => opaque(value)?,
                    };
                    object.insert(name.clone(), value);
                }
                Ok(JsonValue::Object(object))
            }
            Self::Map { values } => {
                let Bson::Document(document) = value else {
                    return Err(invalid("expected an object".to_owned()));
                };
                document
                    .iter()
                    .map(|(name, value)| {
                        Ok((
                            name.clone(),
                            values.normalize_bson(value, &child_path(path, name))?,
                        ))
                    })
                    .collect::<Result<_, JsonSchemaError>>()
                    .map(JsonValue::Object)
            }
            Self::List { items } => {
                let Bson::Array(array) = value else {
                    return Err(invalid("expected an array".to_owned()));
                };
                array
                    .iter()
                    .enumerate()
                    .map(|(index, value)| {
                        items.normalize_bson(value, &child_path(path, &index.to_string()))
                    })
                    .collect::<Result<Vec<_>, _>>()
                    .map(JsonValue::Array)
            }
            Self::Tuple { items } => {
                let Bson::Array(array) = value else {
                    return Err(invalid("expected an array".to_owned()));
                };
                if array.len() != items.len() {
                    return Err(invalid(format!(
                        "expected an array of length {}",
                        items.len()
                    )));
                }
                items
                    .iter()
                    .zip(array)
                    .enumerate()
                    .map(|(index, (schema, value))| {
                        schema.normalize_bson(value, &child_path(path, &index.to_string()))
                    })
                    .collect::<Result<Vec<_>, _>>()
                    .map(JsonValue::Array)
            }
            Self::Optional { wrapped } => {
                if matches!(value, Bson::Null) {
                    Ok(JsonValue::Null)
                } else {
                    wrapped.normalize_bson(value, path)
                }
            }
            Self::Json | Self::Null => opaque(value),
            _ => {
                let dtype = self
                    .leaf_type()
                    .ok_or_else(|| invalid("expected int or float array elements".to_owned()))?;
                let value = BsonParser::bson_to_value(value, path, &dtype)
                    .map_err(|error| invalid(error.to_string()))?;
                if matches!(dtype, Type::PyObjectWrapper)
                    && !matches!(value, crate::engine::Value::PyObjectWrapper(_))
                {
                    return Err(invalid(format!("expected {dtype}")));
                }
                let json =
                    serialize_value_to_json(&value).map_err(|error| invalid(error.to_string()))?;
                // serde_json maps non-finite floats to null. Reject them here,
                // before an enclosing Optional could accept the manufactured null.
                if json.is_null() {
                    return Err(invalid(format!("expected {dtype} representable in JSON")));
                }
                Ok(json)
            }
        }
    }

    pub(crate) fn leaf_type(&self) -> Option<Type> {
        Some(match self {
            Self::Bool => Type::Bool,
            Self::Int => Type::Int,
            Self::Float => Type::Float,
            Self::String => Type::String,
            Self::Bytes => Type::Bytes,
            Self::DateTimeNaive => Type::DateTimeNaive,
            Self::DateTimeUtc => Type::DateTimeUtc,
            Self::Duration => Type::Duration,
            Self::Pointer => Type::Pointer,
            Self::PyObjectWrapper => Type::PyObjectWrapper,
            Self::Array { n_dim, items } => {
                let element_type = match items.as_ref() {
                    Self::Int => Type::Int,
                    Self::Float => Type::Float,
                    _ => return None,
                };
                Type::Array(*n_dim, element_type.into())
            }
            _ => return None,
        })
    }

    /// Validate the declared defaults even when no input row uses them.
    pub fn validate_defaults(&self, path: &str) -> Result<(), JsonSchemaError> {
        match self {
            Self::Object { fields } => {
                for field in fields {
                    let path = child_path(path, &field.name);
                    field.schema.validate_defaults(&path)?;
                    if let Some(default) = &field.default {
                        field.schema.validate(&mut default.value.clone(), &path)?;
                    }
                }
            }
            Self::Optional { wrapped } => wrapped.validate_defaults(path)?,
            Self::List { items } => items.validate_defaults(path)?,
            Self::Map { values } => values.validate_defaults(path)?,
            Self::Tuple { items } => {
                for item in items {
                    item.validate_defaults(path)?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// Keep the original JSON representation, adding only declared missing defaults.
    pub fn validate(&self, value: &mut JsonValue, path: &str) -> Result<(), JsonSchemaError> {
        let invalid = |message: String| JsonSchemaError {
            path: path.to_owned(),
            message,
        };
        match self {
            Self::Object { fields } => {
                let object = value
                    .as_object_mut()
                    .ok_or_else(|| invalid("expected an object".to_owned()))?;
                for field in fields {
                    let path = child_path(path, &field.name);
                    if !object.contains_key(&field.name) {
                        if let Some(default) = &field.default {
                            object.insert(field.name.clone(), default.value.clone());
                        } else {
                            return Err(JsonSchemaError::missing_field(path));
                        }
                    }
                    field
                        .schema
                        .validate(object.get_mut(&field.name).unwrap(), &path)?;
                }
            }
            Self::Map { values } => {
                let object = value
                    .as_object_mut()
                    .ok_or_else(|| invalid("expected an object".to_owned()))?;
                for (name, value) in object {
                    values.validate(value, &child_path(path, name))?;
                }
            }
            Self::List { items } => {
                let array = value
                    .as_array_mut()
                    .ok_or_else(|| invalid("expected an array".to_owned()))?;
                for (index, value) in array.iter_mut().enumerate() {
                    items.validate(value, &child_path(path, &index.to_string()))?;
                }
            }
            Self::Tuple { items } => {
                let array = value
                    .as_array_mut()
                    .ok_or_else(|| invalid("expected an array".to_owned()))?;
                if array.len() != items.len() {
                    return Err(invalid(format!(
                        "expected an array of length {}",
                        items.len()
                    )));
                }
                for (index, (schema, value)) in items.iter().zip(array).enumerate() {
                    schema.validate(value, &child_path(path, &index.to_string()))?;
                }
            }
            Self::Optional { wrapped } => {
                if !value.is_null() {
                    wrapped.validate(value, path)?;
                }
            }
            Self::Json => {}
            Self::Null => {
                if !value.is_null() {
                    return Err(invalid("expected null".to_owned()));
                }
            }
            Self::Bool
            | Self::Int
            | Self::Float
            | Self::String
            | Self::Bytes
            | Self::DateTimeNaive
            | Self::DateTimeUtc
            | Self::Duration
            | Self::Pointer
            | Self::PyObjectWrapper
            | Self::Array { .. } => {
                let dtype = self
                    .leaf_type()
                    .ok_or_else(|| invalid("expected int or float array elements".to_owned()))?;
                if parse_value_from_json(value, &dtype).is_none() {
                    return Err(invalid(format!("expected {dtype}")));
                }
            }
        }
        Ok(())
    }
}

fn normalize_avro_array(value: AvroValue, path: &str) -> Result<JsonValue, JsonSchemaError> {
    // The shared validator checks element type, rank and count.
    // Infer shape here while native arrays still have their nesting.
    let mut shape = Vec::new();
    let mut first = &value;
    loop {
        match first {
            AvroValue::Union(_, inner) => first = inner,
            AvroValue::Array(array) => {
                shape.push(array.len());
                let Some(value) = array.first() else { break };
                first = value;
            }
            _ => break,
        }
    }
    let mut elements = Vec::new();
    flatten_avro_array(value, &shape, &mut elements).ok_or_else(|| {
        JsonSchemaError::new(
            path.to_owned(),
            "expected a rectangular numeric array".to_owned(),
        )
    })?;
    Ok(serde_json::json!({"shape": shape, "elements": elements}))
}

/// All branches must have the inferred shape and end in finite numeric leaves.
fn flatten_avro_array(
    value: AvroValue,
    shape: &[usize],
    elements: &mut Vec<JsonValue>,
) -> Option<()> {
    match value {
        AvroValue::Union(_, inner) => flatten_avro_array(*inner, shape, elements),
        AvroValue::Array(array) => {
            let (length, tail) = shape.split_first()?;
            if array.len() != *length {
                return None;
            }
            for value in array {
                flatten_avro_array(value, tail, elements)?;
            }
            Some(())
        }
        AvroValue::Int(_) | AvroValue::Long(_) | AvroValue::Float(_) | AvroValue::Double(_)
            if shape.is_empty() =>
        {
            elements.push(avro_value_to_json(value)?);
            Some(())
        }
        _ => None,
    }
}

pub fn child_path(parent: &str, name: &str) -> String {
    format!("{parent}/{}", name.replace('~', "~0").replace('/', "~1"))
}
