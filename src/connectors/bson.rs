use std::collections::HashMap;
use std::iter::zip;

use mongodb::bson::{
    bson, spec::BinarySubtype as BsonBinarySubtype, Binary as BsonBinaryContents, Bson,
    DateTime as BsonDateTime, Document,
};
use ndarray::{ArrayViewD, Axis};
use ordered_float::OrderedFloat;

use crate::connectors::data_format::{
    ensure_all_fields_in_schema, Formatter, FormatterContext, FormatterError, InnerSchemaField,
    ParsedEventWithErrors,
};
use crate::connectors::data_storage::{ConversionError, DataEventType};
use crate::connectors::metadata::SourceMetadata;
use crate::connectors::{ParseError, ParseResult, Parser, ReaderContext, SessionType};
use crate::connectors::{SPECIAL_FIELD_DIFF, SPECIAL_FIELD_TIME};
use crate::engine::time::DateTime as DateTimeTrait;
use crate::engine::value::parse_pathway_pointer;
use crate::engine::{
    DateTimeNaive, DateTimeUtc, Duration, Key, Result as EngineResult, Timestamp, Type, Value,
};

pub struct BsonParser {
    value_field_names: Vec<String>,
    schema: HashMap<String, InnerSchemaField>,
    session_type: SessionType,
}

fn make_conversion_err(repr: String, msg: &str, field_name: &str, type_: &Type) -> ConversionError {
    ConversionError {
        value_repr: repr,
        field_name: field_name.to_string(),
        type_: type_.clone(),
        original_error_message: if msg.is_empty() {
            String::new()
        } else {
            format!(": {msg}")
        },
    }
}

impl BsonParser {
    pub fn new(
        value_field_names: Vec<String>,
        schema: HashMap<String, InnerSchemaField>,
        session_type: SessionType,
    ) -> EngineResult<BsonParser> {
        ensure_all_fields_in_schema(None, &value_field_names, &schema)?;
        Ok(BsonParser {
            value_field_names,
            schema,
            session_type,
        })
    }

    pub fn extract_value_from_document(
        document: &Document,
        field_name: &str,
        type_: &Type,
    ) -> Result<Value, Box<ConversionError>> {
        let bson_value = document.get(field_name).unwrap_or(&Bson::Null);
        Self::bson_to_value(bson_value, field_name, type_)
    }

    #[allow(clippy::cast_precision_loss)]
    #[allow(clippy::too_many_lines)]
    fn bson_to_scalar_value(
        bson: &Bson,
        field_name: &str,
        type_: &Type,
    ) -> Result<Value, Box<ConversionError>> {
        let err = |msg: &str| {
            Box::new(make_conversion_err(
                format!("{bson:?}"),
                msg,
                field_name,
                type_,
            ))
        };
        match type_ {
            Type::Bool => match bson {
                Bson::Boolean(b) => Ok(Value::Bool(*b)),
                _ => Err(err("expected boolean")),
            },

            Type::Int => match bson {
                Bson::Int32(i) => Ok(Value::Int(i64::from(*i))),
                Bson::Int64(i) => Ok(Value::Int(*i)),
                _ => Err(err("expected integer")),
            },

            Type::Float => match bson {
                Bson::Double(f) => Ok(Value::Float(OrderedFloat(*f))),
                Bson::Int32(i) => Ok(Value::Float(OrderedFloat(f64::from(*i)))),
                Bson::Int64(i) => Ok(Value::Float(OrderedFloat(*i as f64))),
                _ => Err(err("expected float")),
            },

            Type::String => match bson {
                Bson::String(s) => Ok(Value::String(s.as_str().into())),
                _ => Err(err("expected string")),
            },

            Type::Bytes => match bson {
                Bson::Binary(bin) => Ok(Value::Bytes(bin.bytes.clone().into())),
                _ => Err(err("expected binary/bytes")),
            },

            Type::Pointer => match bson {
                Bson::String(s) if s.starts_with('^') => parse_pathway_pointer(s).map_err(|e| {
                    Box::new(make_conversion_err(
                        s.clone(),
                        &e.to_string(),
                        field_name,
                        type_,
                    ))
                }),
                _ => Err(err("expected pointer string")),
            },

            Type::DateTimeNaive => match bson {
                Bson::DateTime(dt) => {
                    let ts = dt.timestamp_millis();
                    DateTimeNaive::from_timestamp(ts, "ms")
                        .map(Value::DateTimeNaive)
                        .map_err(|e| {
                            Box::new(make_conversion_err(
                                format!("{ts}"),
                                &e.to_string(),
                                field_name,
                                type_,
                            ))
                        })
                }
                _ => Err(err("expected datetime")),
            },

            Type::DateTimeUtc => match bson {
                Bson::DateTime(dt) => {
                    let ts = dt.timestamp_millis();
                    DateTimeUtc::from_timestamp(ts, "ms")
                        .map(Value::DateTimeUtc)
                        .map_err(|e| {
                            Box::new(make_conversion_err(
                                format!("{ts}"),
                                &e.to_string(),
                                field_name,
                                type_,
                            ))
                        })
                }
                _ => Err(err("expected datetime")),
            },

            Type::Duration => match bson {
                Bson::Int64(ns) => Duration::new_with_unit(*ns, "ms")
                    .map(Value::Duration)
                    .map_err(|e| {
                        Box::new(make_conversion_err(
                            format!("{ns}"),
                            &e.to_string(),
                            field_name,
                            type_,
                        ))
                    }),
                _ => Err(err("expected int64 nanoseconds for duration")),
            },

            Type::PyObjectWrapper => match bson {
                Bson::Binary(bin) => bincode::deserialize::<Value>(&bin.bytes).map_err(|e| {
                    Box::new(make_conversion_err(
                        format!("{bin:?}"),
                        &e.to_string(),
                        field_name,
                        type_,
                    ))
                }),
                _ => Err(err("expected binary/bytes")),
            },

            _ => unreachable!("bson_to_scalar_value called with non-scalar type: {type_:?}"),
        }
    }

    fn bson_to_complex_value(
        bson: &Bson,
        field_name: &str,
        type_: &Type,
    ) -> Result<Value, Box<ConversionError>> {
        let err = |msg: &str| {
            Box::new(make_conversion_err(
                format!("{bson:?}"),
                msg,
                field_name,
                type_,
            ))
        };
        match type_ {
            Type::Json => match bson {
                Bson::Document(doc) => serde_json::to_value(doc).map(Value::from).map_err(|e| {
                    Box::new(make_conversion_err(
                        format!("{doc:?}"),
                        &e.to_string(),
                        field_name,
                        type_,
                    ))
                }),
                Bson::Array(arr) => serde_json::to_value(arr).map(Value::from).map_err(|e| {
                    Box::new(make_conversion_err(
                        format!("{arr:?}"),
                        &e.to_string(),
                        field_name,
                        type_,
                    ))
                }),
                Bson::String(s) => serde_json::from_str::<serde_json::Value>(s)
                    .map(Value::from)
                    .map_err(|e| {
                        Box::new(make_conversion_err(
                            s.clone(),
                            &e.to_string(),
                            field_name,
                            type_,
                        ))
                    }),
                _ => Err(err("expected document, array or JSON string")),
            },

            Type::Tuple(inner_types) => match bson {
                Bson::Array(arr) => {
                    if arr.len() != inner_types.len() {
                        return Err(Box::new(make_conversion_err(
                            format!("{arr:?}"),
                            &format!(
                                "expected tuple of length {}, got {}",
                                inner_types.len(),
                                arr.len()
                            ),
                            field_name,
                            type_,
                        )));
                    }
                    arr.iter()
                        .zip(inner_types.iter())
                        .map(|(b, t)| Self::bson_to_value(b, field_name, t))
                        .collect::<Result<Vec<_>, _>>()
                        .map(|v| Value::Tuple(v.into()))
                }
                _ => Err(err("expected array for tuple")),
            },

            Type::List(inner) => match bson {
                Bson::Array(arr) => arr
                    .iter()
                    .map(|b| Self::bson_to_value(b, field_name, inner))
                    .collect::<Result<Vec<_>, _>>()
                    .map(|v| Value::Tuple(v.into())),
                _ => Err(err("expected array for list")),
            },

            _ => unreachable!("bson_to_complex_value called with non-complex type: {type_:?}"),
        }
    }

    fn bson_to_value(
        bson: &Bson,
        field_name: &str,
        type_: &Type,
    ) -> Result<Value, Box<ConversionError>> {
        match type_ {
            Type::Optional(inner) => {
                if matches!(bson, Bson::Null) {
                    return Ok(Value::None);
                }
                Self::bson_to_value(bson, field_name, inner)
            }

            Type::Any => Self::bson_to_any_value(bson, field_name),

            Type::Bool
            | Type::Int
            | Type::Float
            | Type::String
            | Type::Bytes
            | Type::Pointer
            | Type::DateTimeNaive
            | Type::DateTimeUtc
            | Type::Duration
            | Type::PyObjectWrapper => Self::bson_to_scalar_value(bson, field_name, type_),

            Type::Json | Type::Tuple(_) | Type::List(_) => {
                Self::bson_to_complex_value(bson, field_name, type_)
            }

            Type::Array(_, element_type) => {
                bson_to_ndarray_value(bson, field_name, type_, element_type)
            }

            Type::Future(_) => Err(Box::new(make_conversion_err(
                format!("{bson:?}"),
                "unsupported type for MongoDB conversion",
                field_name,
                type_,
            ))),
        }
    }

    fn bson_to_any_value(bson: &Bson, field_name: &str) -> Result<Value, Box<ConversionError>> {
        let err = |msg: &str| {
            Box::new(make_conversion_err(
                format!("{bson:?}"),
                msg,
                field_name,
                &Type::Any,
            ))
        };
        match bson {
            Bson::Null => Ok(Value::None),
            Bson::Boolean(b) => Ok(Value::Bool(*b)),
            Bson::Int32(i) => Ok(Value::Int(i64::from(*i))),
            Bson::Int64(i) => Ok(Value::Int(*i)),
            Bson::Double(f) => Ok(Value::Float(OrderedFloat(*f))),
            Bson::String(s) => Ok(Value::String(s.as_str().into())),
            Bson::Binary(bin) => Ok(Value::Bytes(bin.bytes.clone().into())),
            Bson::DateTime(dt) => {
                let ts = dt.timestamp_millis();
                DateTimeUtc::from_timestamp(ts, "ms")
                    .map(Value::DateTimeUtc)
                    .map_err(|e| {
                        Box::new(make_conversion_err(
                            format!("{ts}"),
                            &e.to_string(),
                            field_name,
                            &Type::Any,
                        ))
                    })
            }
            Bson::Document(doc) => serde_json::to_value(doc).map(Value::from).map_err(|e| {
                Box::new(make_conversion_err(
                    format!("{doc:?}"),
                    &e.to_string(),
                    field_name,
                    &Type::Any,
                ))
            }),
            Bson::Array(arr) => arr
                .iter()
                .map(|b| Self::bson_to_any_value(b, field_name))
                .collect::<Result<Vec<_>, _>>()
                .map(|v| Value::Tuple(v.into())),
            _ => Err(err("unsupported BSON type")),
        }
    }
}

impl Parser for BsonParser {
    fn parse(&mut self, data: &ReaderContext) -> ParseResult {
        let ReaderContext::Bson((data_event, key, document)) = data else {
            return Err(ParseError::UnsupportedReaderContext.into());
        };

        let key_fields = Some(Ok(vec![Value::String(key.as_str().into())]));

        let event = match data_event {
            DataEventType::Insert => {
                let mut parsed_value_fields = Vec::with_capacity(self.value_field_names.len());
                for field_name in &self.value_field_names {
                    let type_ = self
                        .schema
                        .get(field_name)
                        .map_or(Type::Any, |item| item.type_.clone());
                    parsed_value_fields.push(
                        Self::extract_value_from_document(document, field_name, &type_)
                            .map_err(|e| e as Box<dyn std::error::Error + Send + Sync>),
                    );
                }
                ParsedEventWithErrors::Insert((key_fields, parsed_value_fields))
            }
            DataEventType::Delete => ParsedEventWithErrors::Delete((key_fields, vec![])),
        };

        Ok(vec![event])
    }

    fn on_new_source_started(&mut self, _metadata: &SourceMetadata) {}

    fn column_count(&self) -> usize {
        self.value_field_names.len()
    }

    fn session_type(&self) -> SessionType {
        self.session_type
    }
}

fn int_ndarray_to_bson(view: &ArrayViewD<i64>) -> Bson {
    match view.ndim() {
        0 => Bson::Int64(*view.iter().next().unwrap_or(&0)),
        1 => Bson::Array(view.iter().map(|&i| Bson::Int64(i)).collect()),
        _ => Bson::Array(
            (0..view.shape()[0])
                .map(|i| int_ndarray_to_bson(&view.index_axis(Axis(0), i)))
                .collect(),
        ),
    }
}

fn float_ndarray_to_bson(view: &ArrayViewD<f64>) -> Bson {
    match view.ndim() {
        0 => Bson::Double(*view.iter().next().unwrap_or(&0.0)),
        1 => Bson::Array(view.iter().map(|&f| Bson::Double(f)).collect()),
        _ => Bson::Array(
            (0..view.shape()[0])
                .map(|i| float_ndarray_to_bson(&view.index_axis(Axis(0), i)))
                .collect(),
        ),
    }
}

fn collect_bson_ndarray<T>(
    bson: &Bson,
    field_name: &str,
    type_: &Type,
    parse_elem: &dyn Fn(&Bson) -> Option<T>,
    flat: &mut Vec<T>,
    shape: &mut Vec<usize>,
    depth: usize,
) -> Result<(), Box<ConversionError>> {
    match bson {
        Bson::Array(arr) => {
            if depth >= shape.len() {
                shape.push(arr.len());
            } else if shape[depth] != arr.len() {
                return Err(Box::new(make_conversion_err(
                    format!("{bson:?}"),
                    "jagged ndarray is not supported",
                    field_name,
                    type_,
                )));
            }
            for item in arr {
                collect_bson_ndarray(item, field_name, type_, parse_elem, flat, shape, depth + 1)?;
            }
            Ok(())
        }
        scalar => match parse_elem(scalar) {
            Some(v) => {
                flat.push(v);
                Ok(())
            }
            None => Err(Box::new(make_conversion_err(
                format!("{scalar:?}"),
                "unexpected element type in ndarray",
                field_name,
                type_,
            ))),
        },
    }
}

fn bson_to_ndarray_value(
    bson: &Bson,
    field_name: &str,
    type_: &Type,
    element_type: &Type,
) -> Result<Value, Box<ConversionError>> {
    let err = |msg: &str| {
        Box::new(make_conversion_err(
            format!("{bson:?}"),
            msg,
            field_name,
            type_,
        ))
    };
    match element_type {
        Type::Int => {
            let mut flat = Vec::new();
            let mut shape = Vec::new();
            collect_bson_ndarray(
                bson,
                field_name,
                type_,
                &|b| match b {
                    Bson::Int64(i) => Some(*i),
                    Bson::Int32(i) => Some(i64::from(*i)),
                    _ => None,
                },
                &mut flat,
                &mut shape,
                0,
            )?;
            if shape.is_empty() {
                shape.push(flat.len());
            }
            ndarray::ArrayD::from_shape_vec(ndarray::IxDyn(&shape), flat)
                .map(Value::from)
                .map_err(|e| err(&format!("cannot build int ndarray: {e}")))
        }
        Type::Float => {
            let mut flat = Vec::new();
            let mut shape = Vec::new();
            collect_bson_ndarray(
                bson,
                field_name,
                type_,
                #[allow(clippy::cast_precision_loss)]
                &|b| match b {
                    Bson::Double(f) => Some(*f),
                    Bson::Int32(i) => Some(f64::from(*i)),
                    Bson::Int64(i) => Some(*i as f64),
                    _ => None,
                },
                &mut flat,
                &mut shape,
                0,
            )?;
            if shape.is_empty() {
                shape.push(flat.len());
            }
            ndarray::ArrayD::from_shape_vec(ndarray::IxDyn(&shape), flat)
                .map(Value::from)
                .map_err(|e| err(&format!("cannot build float ndarray: {e}")))
        }
        _ => Err(err("ndarray element type must be Int or Float")),
    }
}

pub fn serialize_value_to_bson(value: &Value) -> Result<Bson, FormatterError> {
    match value {
        Value::None => Ok(Bson::Null),
        Value::Int(i) => Ok(bson!(i)),
        Value::Bool(b) => Ok(bson!(b)),
        Value::Float(f) => Ok(Bson::Double((*f).into())),
        Value::String(s) => Ok(Bson::String(s.to_string())),
        Value::Pointer(p) => Ok(bson!(p.to_string())),
        Value::Tuple(t) => {
            let mut items = Vec::with_capacity(t.len());
            for item in t.iter() {
                items.push(serialize_value_to_bson(item)?);
            }
            Ok(Bson::Array(items))
        }
        Value::IntArray(a) => Ok(int_ndarray_to_bson(&a.view())),
        Value::FloatArray(a) => Ok(float_ndarray_to_bson(&a.view())),
        Value::Bytes(b) => Ok(Bson::Binary(BsonBinaryContents {
            subtype: BsonBinarySubtype::Generic,
            bytes: b.to_vec(),
        })),

        // We use milliseconds here because BSON DateTime type has millisecond precision
        // See also: https://docs.rs/bson/2.11.0/bson/struct.DateTime.html
        Value::DateTimeNaive(dt) => Ok(Bson::DateTime(BsonDateTime::from_millis(
            dt.timestamp_milliseconds(),
        ))),
        Value::DateTimeUtc(dt) => Ok(Bson::DateTime(BsonDateTime::from_millis(
            dt.timestamp_milliseconds(),
        ))),

        // We use milliseconds in durations to be consistent with the granularity
        // of the BSON DateTime type
        Value::Duration(d) => Ok(bson!(d.milliseconds())),
        Value::Json(j) => Ok(bson!(j.to_string())),
        Value::PyObjectWrapper(_) => Ok(Bson::Binary(BsonBinaryContents {
            subtype: BsonBinarySubtype::Generic,
            bytes: bincode::serialize(value).map_err(|e| *e)?.clone(),
        })),
        Value::Error | Value::Pending => {
            Err(FormatterError::ValueNonSerializable(value.kind(), "BSON"))
        }
    }
}

pub struct BsonFormatter {
    value_field_names: Vec<String>,
    with_special_fields: bool,
}

impl BsonFormatter {
    pub fn new(value_field_names: Vec<String>, with_special_fields: bool) -> Self {
        Self {
            value_field_names,
            with_special_fields,
        }
    }
}

impl Formatter for BsonFormatter {
    fn format(
        &mut self,
        key: &Key,
        values: &[Value],
        time: Timestamp,
        diff: isize,
    ) -> Result<FormatterContext, FormatterError> {
        let mut document = Document::new();
        for (key, value) in zip(self.value_field_names.iter(), values) {
            let _ = document.insert(key, serialize_value_to_bson(value)?);
        }
        if self.with_special_fields {
            let _ = document.insert(
                SPECIAL_FIELD_DIFF,
                Bson::Int64(diff.try_into().expect("diff can only be +1 or -1")),
            );
            let _ = document.insert(
                SPECIAL_FIELD_TIME,
                Bson::Int64(
                    time.0
                        .try_into()
                        .expect("timestamp is not expected to exceed int64 type"),
                ),
            );
        }
        Ok(FormatterContext::new_single_payload(
            document,
            *key,
            Vec::new(),
            time,
            diff,
        ))
    }
}
