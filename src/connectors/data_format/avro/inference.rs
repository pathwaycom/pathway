// Copyright © 2026 Pathway

//! Infer JSON validation/access metadata without adding an engine container type.

use super::{
    avro_default_to_engine_value, avro_schema_to_engine_type, schema_names, AvroError, AvroSchema,
    ExploredField, PayloadEncoding, DURATION_TYPE_MARKER, PATHWAY_TYPE_ATTRIBUTE,
};
use crate::connectors::data_format::json_schema::{JsonDefault, JsonField, JsonSchema};
use crate::connectors::data_format::serialize_value_to_json;
use crate::engine::{Type, Value};
use apache_avro::schema::{Name, NamesRef, RecordField};
use serde_json::Value as JsonValue;

struct Inferred {
    type_: Type,
    schema: JsonSchema,
    structured: bool,
}

fn leaf_schema(dtype: &Type) -> JsonSchema {
    match dtype {
        Type::Bool => JsonSchema::Bool,
        Type::Int => JsonSchema::Int,
        Type::Float => JsonSchema::Float,
        Type::String => JsonSchema::String,
        Type::Bytes => JsonSchema::Bytes,
        Type::DateTimeNaive => JsonSchema::DateTimeNaive,
        Type::DateTimeUtc => JsonSchema::DateTimeUtc,
        Type::Duration => JsonSchema::Duration,
        _ => JsonSchema::Json,
    }
}

fn field_info(
    field: &RecordField,
    encoding: PayloadEncoding,
    names: &NamesRef<'_>,
    visited: &mut Vec<Name>,
) -> Result<Inferred, AvroError> {
    let mut info = infer(&field.schema, encoding, names, visited)?;
    if encoding == PayloadEncoding::AvroDatum
        && field
            .custom_attributes
            .get(PATHWAY_TYPE_ATTRIBUTE)
            .and_then(JsonValue::as_str)
            == Some(DURATION_TYPE_MARKER)
    {
        if info.type_ == Type::Int {
            info.type_ = Type::Duration;
            info.schema = JsonSchema::Duration;
        } else if info.type_ == Type::Optional(Type::Int.into()) {
            info.type_ = Type::Optional(Type::Duration.into());
            info.schema = JsonSchema::Optional {
                wrapped: Box::new(JsonSchema::Duration),
            };
        }
    }
    Ok(info)
}

#[allow(clippy::too_many_lines)]
fn infer(
    schema: &AvroSchema,
    encoding: PayloadEncoding,
    names: &NamesRef<'_>,
    visited: &mut Vec<Name>,
) -> Result<Inferred, AvroError> {
    let opaque = || Inferred {
        type_: Type::Json,
        schema: JsonSchema::Json,
        structured: false,
    };
    Ok(match schema {
        AvroSchema::Ref { name } => {
            if visited.contains(name) {
                return Ok(opaque());
            }
            let target = names.get(name).ok_or_else(|| AvroError::UnresolvedRef {
                name: name.to_string(),
            })?;
            return infer(target, encoding, names, visited);
        }
        AvroSchema::Record(record) => {
            if visited.contains(&record.name) {
                return Ok(opaque());
            }
            visited.push(record.name.clone());
            let fields = record
                .fields
                .iter()
                .map(|field| {
                    let info = field_info(field, encoding, names, visited)?;
                    let default = field
                        .default
                        .as_ref()
                        .and_then(|default| {
                            normalized_default(default, &field.schema, &info.schema, names)
                        })
                        .map(|value| JsonDefault { value });
                    Ok(JsonField {
                        name: field.name.clone(),
                        schema: info.schema,
                        default,
                    })
                })
                .collect::<Result<Vec<_>, AvroError>>();
            visited.pop();
            Inferred {
                type_: Type::Json,
                schema: JsonSchema::Object { fields: fields? },
                structured: true,
            }
        }
        AvroSchema::Map(map) => {
            let value = infer(&map.types, encoding, names, visited)?;
            Inferred {
                type_: Type::Json,
                schema: JsonSchema::Map {
                    values: Box::new(value.schema),
                },
                structured: true,
            }
        }
        AvroSchema::Array(array) => {
            let item = infer(&array.items, encoding, names, visited)?;
            Inferred {
                type_: if item.structured {
                    Type::Json
                } else {
                    Type::List(item.type_.into())
                },
                schema: JsonSchema::List {
                    items: Box::new(item.schema),
                },
                structured: item.structured,
            }
        }
        AvroSchema::Union(union) => {
            let non_null: Vec<_> = union
                .variants()
                .iter()
                .filter(|item| !matches!(item, AvroSchema::Null))
                .collect();
            let mut info = match non_null.as_slice() {
                [single] => infer(single, encoding, names, visited)?,
                _ => opaque(),
            };
            if non_null.len() != union.variants().len() {
                info.type_ = Type::Optional(info.type_.into());
                info.schema = JsonSchema::Optional {
                    wrapped: Box::new(info.schema),
                };
            }
            info
        }
        AvroSchema::Null => Inferred {
            type_: Type::Json,
            schema: JsonSchema::Null,
            structured: false,
        },
        _ => {
            let type_ = avro_schema_to_engine_type(schema, encoding)?;
            Inferred {
                schema: leaf_schema(&type_),
                type_,
                structured: false,
            }
        }
    })
}

/// Defaults are Avro values, not JSON wire values: bytes/fixed strings use
/// ISO-8859-1 code points, and logical times use the schema's units.
fn normalized_default(
    value: &JsonValue,
    source: &AvroSchema,
    spec: &JsonSchema,
    names: &NamesRef<'_>,
) -> Option<JsonValue> {
    if value.is_null() {
        return Some(JsonValue::Null);
    }
    if let AvroSchema::Ref { name } = source {
        return normalized_default(value, names.get(name)?, spec, names);
    }
    if let AvroSchema::Union(union) = source {
        let source = union
            .variants()
            .iter()
            .find(|item| !matches!(item, AvroSchema::Null))?;
        return normalized_default(value, source, spec, names);
    }
    if let JsonSchema::Optional { wrapped } = spec {
        return normalized_default(value, source, wrapped, names);
    }
    match (spec, source) {
        (JsonSchema::Object { fields }, AvroSchema::Record(record)) => value
            .as_object()?
            .iter()
            .map(|(name, value)| {
                let converted = match (
                    fields.iter().find(|field| &field.name == name),
                    record.fields.iter().find(|field| &field.name == name),
                ) {
                    (Some(spec), Some(source)) => {
                        normalized_default(value, &source.schema, &spec.schema, names)?
                    }
                    _ => value.clone(),
                };
                Some((name.clone(), converted))
            })
            .collect::<Option<serde_json::Map<_, _>>>()
            .map(JsonValue::Object),
        (JsonSchema::Map { values }, AvroSchema::Map(map)) => value
            .as_object()?
            .iter()
            .map(|(name, value)| {
                Some((
                    name.clone(),
                    normalized_default(value, &map.types, values, names)?,
                ))
            })
            .collect::<Option<serde_json::Map<_, _>>>()
            .map(JsonValue::Object),
        (JsonSchema::List { items }, AvroSchema::Array(array)) => value
            .as_array()?
            .iter()
            .map(|value| normalized_default(value, &array.items, items, names))
            .collect::<Option<Vec<_>>>()
            .map(JsonValue::Array),
        (JsonSchema::Json, _) => opaque_default(value, source, names),
        _ => {
            let dtype = spec.leaf_type()?;
            serialize_value_to_json(&avro_default_to_engine_value(value, &dtype, source)?).ok()
        }
    }
}

/// Keep opaque recursive/wide-union branches in the ordinary structural Avro
/// representation, including raw logical-time numbers and base64 binary leaves.
fn opaque_default(
    value: &JsonValue,
    source: &AvroSchema,
    names: &NamesRef<'_>,
) -> Option<JsonValue> {
    if value.is_null() {
        return Some(JsonValue::Null);
    }
    match source {
        AvroSchema::Ref { name } => opaque_default(value, names.get(name)?, names),
        AvroSchema::Union(union) => opaque_default(
            value,
            union
                .variants()
                .iter()
                .find(|item| !matches!(item, AvroSchema::Null))?,
            names,
        ),
        AvroSchema::Record(record) => value
            .as_object()?
            .iter()
            .map(|(name, value)| {
                let field = record.fields.iter().find(|field| &field.name == name)?;
                Some((name.clone(), opaque_default(value, &field.schema, names)?))
            })
            .collect::<Option<serde_json::Map<_, _>>>()
            .map(JsonValue::Object),
        AvroSchema::Map(map) => value
            .as_object()?
            .iter()
            .map(|(name, value)| Some((name.clone(), opaque_default(value, &map.types, names)?)))
            .collect::<Option<serde_json::Map<_, _>>>()
            .map(JsonValue::Object),
        AvroSchema::Array(array) => value
            .as_array()?
            .iter()
            .map(|value| opaque_default(value, &array.items, names))
            .collect::<Option<Vec<_>>>()
            .map(JsonValue::Array),
        AvroSchema::Duration(_) => None,
        _ => {
            let dtype = avro_schema_to_engine_type(source, PayloadEncoding::Json).ok()?;
            serialize_value_to_json(&avro_default_to_engine_value(value, &dtype, source)?).ok()
        }
    }
}

pub(super) fn explore(
    schema: &AvroSchema,
    encoding: PayloadEncoding,
) -> Result<Vec<ExploredField>, AvroError> {
    let AvroSchema::Record(record) = schema else {
        return Err(AvroError::NonRecordSchema);
    };
    let names = schema_names(schema);
    let mut fields = Vec::with_capacity(record.fields.len());
    for field in &record.fields {
        let only_null = match &field.schema {
            AvroSchema::Null => true,
            AvroSchema::Union(union) => union
                .variants()
                .iter()
                .all(|item| matches!(item, AvroSchema::Null)),
            _ => false,
        };
        if only_null {
            return Err(AvroError::UnsupportedType {
                field: field.name.clone(),
                type_: Type::Any,
            });
        }
        let info = field_info(field, encoding, &names, &mut vec![record.name.clone()])?;
        let default = field.default.as_ref().and_then(|value| {
            let mut json = normalized_default(value, &field.schema, &info.schema, &names)?;
            if info.structured {
                info.schema.validate(&mut json, &field.name).ok()?;
                Some(if json.is_null() && info.type_.is_optional() {
                    Value::None
                } else {
                    Value::from(json)
                })
            } else {
                crate::engine::json::parse_value_from_json(&json, &info.type_)
            }
        });
        fields.push(ExploredField {
            name: field.name.clone(),
            type_: info.type_,
            default,
            doc: field.doc.clone(),
            json_schema: if info.structured {
                Some(info.schema)
            } else {
                None
            },
        });
    }
    Ok(fields)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::DateTimeUtc;
    use apache_avro::types::Value as AvroValue;
    use serde_json::json;
    use std::collections::HashMap;

    #[test]
    fn named_records_maps_arrays_and_recursive_edges() {
        let schema = AvroSchema::parse_str(
            &json!({"type":"record","name":"Row","fields":[
                {"name":"one","type":{"type":"record","name":"Node","fields":[
                    {"name":"value","type":"double"},
                    {"name":"next","type":["null","Node"],"default":null}
                ]}},
                {"name":"two","type":"Node"},
                {"name":"items","type":{"type":"array","items":"Node"}},
                {"name":"by_name","type":{"type":"map","values":"Node"}},
                {"name":"numbers","type":{"type":"array","items":"long"}}
            ]})
            .to_string(),
        )
        .unwrap();
        let fields = explore(&schema, PayloadEncoding::AvroDatum).unwrap();
        for field in &fields[..4] {
            assert_eq!(field.type_, Type::Json);
            assert!(field.json_schema.is_some());
        }
        assert!(matches!(&fields[4].type_, Type::List(inner) if **inner == Type::Int));
        assert!(fields[4].json_schema.is_none());
        let metadata = serde_json::to_value(fields[0].json_schema.as_ref().unwrap()).unwrap();
        assert_eq!(metadata["fields"][0]["schema"]["type"], "float");
        assert_eq!(
            metadata["fields"][1]["schema"],
            json!({"type":"optional","wrapped":{"type":"json"}})
        );
    }

    #[test]
    fn inferred_defaults_use_json_codecs_and_source_units() {
        let schema = AvroSchema::parse_str(&json!({"type":"record","name":"Row","fields":[
            {"name":"one","type":{"type":"record","name":"Payload","fields":[
                {"name":"content","type":"bytes","default":"ÿ"},
                {"name":"at","type":{"type":"long","logicalType":"timestamp-micros"},"default":123},
                {"name":"span","type":"long","pathwayType":"Duration","default":7}
            ]},"default":{"content":"ÿ","at":123,"span":7}},
            {"name":"binary_map","type":{"type":"map","values":"bytes"},"default":{"a":"ÿ"}}
        ]}).to_string()).unwrap();
        let fields = explore(&schema, PayloadEncoding::AvroDatum).unwrap();
        let default = serialize_value_to_json(fields[0].default.as_ref().unwrap()).unwrap();
        assert_eq!(default["content"], "/w==");
        assert_eq!(default["span"], 7_000);
        assert_eq!(
            crate::engine::json::parse_value_from_json(&default["at"], &Type::DateTimeUtc),
            Some(Value::DateTimeUtc(DateTimeUtc::new(123_000)))
        );
        assert_eq!(
            serialize_value_to_json(fields[1].default.as_ref().unwrap()).unwrap(),
            json!({"a":"/w=="})
        );
        let mut absent = json!({});
        fields[0]
            .json_schema
            .as_ref()
            .unwrap()
            .validate(&mut absent, "/one")
            .unwrap();
        assert_eq!(absent, default);
        let json_fields = explore(&schema, PayloadEncoding::Json).unwrap();
        let json_default =
            serialize_value_to_json(json_fields[0].default.as_ref().unwrap()).unwrap();
        assert_eq!(json_default["at"], 123);
        assert_eq!(json_default["span"], 7);
    }

    #[test]
    fn inferred_map_validates_dynamic_keys_with_paths() {
        let schema = AvroSchema::parse_str(
            &json!({"type":"record","name":"Row","fields":[
                {"name":"values","type":{"type":"map","values":"double"}}
            ]})
            .to_string(),
        )
        .unwrap();
        let fields = explore(&schema, PayloadEncoding::AvroDatum).unwrap();
        let metadata = fields[0].json_schema.as_ref().unwrap();
        metadata.validate(&mut json!({}), "/values").unwrap();
        assert!(metadata
            .validate(&mut json!({"a/b~c":"bad"}), "/values")
            .unwrap_err()
            .to_string()
            .contains("/values/a~1b~0c"));
        let serialized = serde_json::to_string(metadata).unwrap();
        let restored: JsonSchema = serde_json::from_str(&serialized).unwrap();
        assert_eq!(
            restored
                .normalize_avro(
                    AvroValue::Map(HashMap::from([("a".into(), AvroValue::Double(1.5))])),
                    "/values"
                )
                .unwrap(),
            json!({"a":1.5})
        );
    }
}
