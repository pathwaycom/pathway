// Copyright © 2026 Pathway

use std::borrow::Cow;
use std::clone::Clone;
use std::collections::HashMap;
use std::fmt;
use std::iter::zip;

use crate::connectors::metadata::SourceMetadata;
use crate::connectors::ReaderContext::{
    Bson, CsvRecord, Diff, Empty, KeyValue, RawBytes, TokenizedEntries,
};
use crate::connectors::{DataEventType, ReaderContext, SessionType};
use crate::connectors::{SPECIAL_FIELD_DIFF, SPECIAL_FIELD_TIME};
use crate::engine::error::DynResult;
use crate::engine::{Key, Result, Timestamp, Type, Value};
use crate::python_api::ValueField;

use schema_registry_converter::blocking::json::JsonDecoder as RegistryJsonDecoder;
use schema_registry_converter::blocking::json::JsonEncoder as RegistryJsonEncoder;
use schema_registry_converter::schema_registry_common::SubjectNameStrategy as RegistrySubjectNameStrategy;
use serde::de::{IgnoredAny, MapAccess, SeqAccess, Visitor};
use serde::ser::{SerializeMap, Serializer};
use serde::Deserializer as _;
use serde_json::json;
use serde_json::Value as JsonValue;

use super::{
    ensure_all_fields_in_schema, parse_value_from_json, prepare_plaintext_str,
    serialize_value_to_json, values_by_names_from_json, Formatter, FormatterContext,
    FormatterError, InnerSchemaField, ParseError, ParseResult, ParsedEventWithErrors, Parser,
    ValueFieldsWithErrors, COMMIT_LITERAL,
};

#[derive(Debug, Copy, Clone)]
pub enum FieldSource {
    Key,
    Payload,
    Metadata,
}

#[derive(Default, Debug)]
pub struct FieldSourceLists {
    to_parse_from_key: Vec<String>,
    to_parse_from_payload: Vec<String>,
    sources_order: Vec<FieldSource>,
}

impl FieldSourceLists {
    pub fn new() -> Self {
        Self {
            to_parse_from_key: Vec::new(),
            to_parse_from_payload: Vec::new(),
            sources_order: Vec::new(),
        }
    }

    pub fn add_field(&mut self, name: String, source: FieldSource) {
        self.sources_order.push(source);
        match source {
            FieldSource::Key => self.to_parse_from_key.push(name),
            FieldSource::Payload => self.to_parse_from_payload.push(name),
            FieldSource::Metadata => {}
        }
    }
}

pub struct JsonLinesParser {
    key_field_source_lists: Option<FieldSourceLists>,
    value_field_source_lists: FieldSourceLists,
    column_paths: HashMap<String, String>,
    field_absence_is_error: bool,
    schema: HashMap<String, InnerSchemaField>,
    metadata_column_value: Value,
    // Whether any output column is the `_metadata` one. Without it the
    // metadata is never read, so `on_new_source_started` skips serializing it.
    has_metadata_column: bool,
    session_type: SessionType,
    schema_registry_decoder: Option<RegistryJsonDecoder>,

    // Fast-path state: when every field is a top-level key of the key or
    // payload JSON object (no JSON-pointer paths, no schema-registry decoder),
    // the objects are extracted with a single streaming pass that never
    // materializes the full `serde_json::Value` DOM. The specs hold, per
    // source, the field names in slot order with their types and defaults
    // resolved once, instead of once per message.
    can_use_fast_json: bool,
    fast_value_specs: FastSourceSpecs,
    fast_key_specs: Option<FastSourceSpecs>,
}

impl JsonLinesParser {
    pub fn new(
        key_field_names: Option<&[String]>,
        value_fields: Vec<ValueField>,
        column_paths: HashMap<String, String>,
        field_absence_is_error: bool,
        schema: HashMap<String, InnerSchemaField>,
        session_type: SessionType,
        schema_registry_decoder: Option<RegistryJsonDecoder>,
    ) -> Result<JsonLinesParser> {
        let key_source_lists = if let Some(key_field_names) = key_field_names {
            let mut key_sources_lists = FieldSourceLists::new();
            for key_field_name in key_field_names {
                // In order not to break the backwards compatibility, we allow that some of the
                // key fields are not present in the `value_fields` vector. We then consider that
                // they come from payload.
                let source = value_fields
                    .iter()
                    .find(|vf| vf.name == *key_field_name)
                    .map_or(FieldSource::Payload, |vf| vf.source);
                key_sources_lists.add_field(key_field_name.clone(), source);
            }
            Some(key_sources_lists)
        } else {
            None
        };

        let mut value_source_lists = FieldSourceLists::new();
        let mut value_field_names = Vec::with_capacity(value_fields.len());
        for value_field in value_fields {
            value_source_lists.add_field(value_field.name.clone(), value_field.source);
            value_field_names.push(value_field.name);
        }

        ensure_all_fields_in_schema(key_field_names, value_field_names.as_ref(), &schema)?;

        // The fast path applies when every needed field is a top-level key of
        // the key or payload JSON object. It is disabled when JSON-pointer
        // column paths are configured (nested access) or when a schema-registry
        // decoder is used (the payload is then not a plain JSON string).
        let can_use_fast_json = schema_registry_decoder.is_none() && column_paths.is_empty();
        let fast_value_specs = FastSourceSpecs::new(&value_source_lists, &schema);
        let fast_key_specs = key_source_lists
            .as_ref()
            .map(|lists| FastSourceSpecs::new(lists, &schema));
        let has_metadata_column = value_source_lists
            .sources_order
            .iter()
            .chain(
                key_source_lists
                    .iter()
                    .flat_map(|lists| lists.sources_order.iter()),
            )
            .any(|source| matches!(source, FieldSource::Metadata));

        Ok(JsonLinesParser {
            key_field_source_lists: key_source_lists,
            value_field_source_lists: value_source_lists,
            column_paths,
            field_absence_is_error,
            schema,
            metadata_column_value: Value::None,
            has_metadata_column,
            session_type,
            schema_registry_decoder,
            can_use_fast_json,
            fast_value_specs,
            fast_key_specs,
        })
    }

    /// Runs the streaming extractor over one JSON object (the key or the
    /// payload). Returns the field results in slot order, or `None` to request
    /// the DOM-based slow path, which reproduces the exact semantics of the
    /// inputs the fast path does not handle (non-object documents, empty
    /// input, field-absence errors that embed the whole payload).
    fn fast_extract(
        spec: &FastFieldSpec,
        object: Option<&str>,
        field_absence_is_error: bool,
    ) -> DynResult<Option<Vec<DynResult<Value>>>> {
        if spec.names.is_empty() {
            // Nothing is read from this source: don't even look at the bytes.
            return Ok(Some(Vec::new()));
        }
        let Some(object) = object else {
            return Ok(None);
        };
        if object.is_empty() {
            return Ok(None);
        }
        let extractor = FastFieldExtractor {
            spec,
            field_absence_is_error,
        };
        let mut deserializer = serde_json::Deserializer::from_str(object);
        let outcome = (&mut deserializer).deserialize_any(extractor)?;
        deserializer.end()?;
        match outcome {
            FastExtractOutcome::Extracted(values) => Ok(Some(values)),
            FastExtractOutcome::Fallback => Ok(None),
        }
    }

    /// Fast-path counterpart of `values_from_parsed_object`: extracts the
    /// fields of one source list from the key and payload objects and lays
    /// them out in `sources_order`.
    fn fast_values(
        &self,
        specs: &FastSourceSpecs,
        source_lists: &FieldSourceLists,
        key: Option<&str>,
        payload: Option<&str>,
    ) -> DynResult<Option<ValueFieldsWithErrors>> {
        let Some(from_key) = Self::fast_extract(&specs.from_key, key, self.field_absence_is_error)?
        else {
            return Ok(None);
        };
        let Some(from_payload) =
            Self::fast_extract(&specs.from_payload, payload, self.field_absence_is_error)?
        else {
            return Ok(None);
        };
        let mut from_key = from_key.into_iter();
        let mut from_payload = from_payload.into_iter();
        let mut result = Vec::with_capacity(source_lists.sources_order.len());
        for source in &source_lists.sources_order {
            match source {
                FieldSource::Key => result.push(from_key.next().unwrap()),
                FieldSource::Payload => result.push(from_payload.next().unwrap()),
                FieldSource::Metadata => result.push(Ok(self.metadata_column_value.clone())),
            }
        }
        Ok(Some(result))
    }

    /// Fast path of `parse`: the key and the payload objects are streamed
    /// field by field, without building the `serde_json::Value` DOM. `None`
    /// requests the slow path.
    fn try_fast_parse(
        &self,
        data_event: DataEventType,
        raw_bytes_key: &[u8],
        raw_bytes_payload: &[u8],
    ) -> DynResult<Option<Vec<ParsedEventWithErrors>>> {
        let payload = if self.has_fields_from_payload() {
            let line = prepare_plaintext_str(raw_bytes_payload)?;
            if line == COMMIT_LITERAL {
                return Ok(Some(vec![ParsedEventWithErrors::AdvanceTime]));
            }
            Some(line)
        } else {
            None
        };
        let key = if self.has_fields_from_key() {
            Some(prepare_plaintext_str(raw_bytes_key)?)
        } else {
            None
        };

        let event_key = match (&self.fast_key_specs, &self.key_field_source_lists) {
            (Some(specs), Some(source_lists)) => {
                let Some(values) = self.fast_values(specs, source_lists, key, payload)? else {
                    return Ok(None);
                };
                Some(values.into_iter().collect())
            }
            _ => None,
        };
        let Some(event_values) = self.fast_values(
            &self.fast_value_specs,
            &self.value_field_source_lists,
            key,
            payload,
        )?
        else {
            return Ok(None);
        };
        Ok(Some(vec![ParsedEventWithErrors::new(
            self.session_type,
            data_event,
            event_key,
            event_values,
        )]))
    }

    fn values_from_parsed_object(
        &self,
        key: &JsonValue,
        payload: &JsonValue,
        source_lists: &FieldSourceLists,
    ) -> ValueFieldsWithErrors {
        let mut fields_from_key_iter = values_by_names_from_json(
            key,
            source_lists.to_parse_from_key.as_slice(),
            &self.column_paths,
            self.field_absence_is_error,
            &self.schema,
        )
        .into_iter();
        let mut fields_from_payload_iter = values_by_names_from_json(
            payload,
            source_lists.to_parse_from_payload.as_slice(),
            &self.column_paths,
            self.field_absence_is_error,
            &self.schema,
        )
        .into_iter();

        let mut result = Vec::with_capacity(source_lists.sources_order.len());
        for source in &source_lists.sources_order {
            match source {
                FieldSource::Key => result.push(fields_from_key_iter.next().unwrap()),
                FieldSource::Payload => result.push(fields_from_payload_iter.next().unwrap()),
                FieldSource::Metadata => result.push(Ok(self.metadata_column_value.clone())),
            }
        }

        result
    }

    fn create_events_from_parsed_object(
        &self,
        data_event: DataEventType,
        key: &JsonValue,
        payload: &JsonValue,
    ) -> Vec<ParsedEventWithErrors> {
        let event_key = self
            .key_field_source_lists
            .as_ref()
            .map(|key_field_source_lists| {
                self.values_from_parsed_object(key, payload, key_field_source_lists)
                    .into_iter()
                    .collect()
            });
        let event_values =
            self.values_from_parsed_object(key, payload, &self.value_field_source_lists);
        let event =
            ParsedEventWithErrors::new(self.session_type, data_event, event_key, event_values);
        vec![event]
    }

    fn has_fields_from_key(&self) -> bool {
        let needed_for_key = !self
            .key_field_source_lists
            .as_ref()
            .is_none_or(|v| v.to_parse_from_key.is_empty());
        let needed_for_value = !self.value_field_source_lists.to_parse_from_key.is_empty();
        needed_for_key || needed_for_value
    }

    fn has_fields_from_payload(&self) -> bool {
        let needed_for_key = !self
            .key_field_source_lists
            .as_ref()
            .is_none_or(|v| v.to_parse_from_payload.is_empty());
        let needed_for_value = !self
            .value_field_source_lists
            .to_parse_from_payload
            .is_empty();
        needed_for_key || needed_for_value
    }

    fn prepare_json(&mut self, raw_bytes: &[u8]) -> DynResult<JsonValue> {
        let result = if let Some(decoder) = self.schema_registry_decoder.as_mut() {
            match decoder.decode(Some(raw_bytes))? {
                None => JsonValue::Null,
                Some(decode_result) => decode_result.value,
            }
        } else {
            match prepare_plaintext_str(raw_bytes)? {
                "" => JsonValue::Null,
                line => serde_json::from_str(line)?,
            }
        };
        Ok(result)
    }
}

enum FastExtractOutcome {
    Extracted(ValueFieldsWithErrors),
    Fallback,
}

/// The fields the fast path extracts from one source (key or payload): names
/// in slot order, with the schema type and default of each slot resolved once
/// at construction, and a name→slot index for wide field lists.
struct FastFieldSpec {
    names: Vec<String>,
    index: HashMap<String, usize>,
    types: Vec<Type>,
    defaults: Vec<Option<Value>>,
}

impl FastFieldSpec {
    const LINEAR_SCAN_LIMIT: usize = 8;

    fn new(names: &[String], schema: &HashMap<String, InnerSchemaField>) -> Self {
        let types = names
            .iter()
            .map(|name| {
                schema
                    .get(name)
                    .map_or(Type::Any, |field| field.type_.clone())
            })
            .collect();
        let defaults = names
            .iter()
            .map(|name| schema.get(name).and_then(|field| field.default.clone()))
            .collect();
        let index = names
            .iter()
            .enumerate()
            .map(|(index, name)| (name.clone(), index))
            .collect();
        Self {
            names: names.to_vec(),
            index,
            types,
            defaults,
        }
    }

    /// Slot of a field name. A handful of names is scanned linearly: that is
    /// cheaper than SipHash-ing the key for every message.
    fn slot(&self, key: &str) -> Option<usize> {
        if self.names.len() <= Self::LINEAR_SCAN_LIMIT {
            self.names.iter().position(|name| name == key)
        } else {
            self.index.get(key).copied()
        }
    }
}

struct FastSourceSpecs {
    from_key: FastFieldSpec,
    from_payload: FastFieldSpec,
}

impl FastSourceSpecs {
    fn new(source_lists: &FieldSourceLists, schema: &HashMap<String, InnerSchemaField>) -> Self {
        Self {
            from_key: FastFieldSpec::new(&source_lists.to_parse_from_key, schema),
            from_payload: FastFieldSpec::new(&source_lists.to_parse_from_payload, schema),
        }
    }
}

/// A JSON value deserialized without building a `serde_json::Value` for the
/// scalar cases: a string is borrowed from the input when it has no escapes,
/// numbers and booleans are kept as such. Objects and arrays are materialized
/// as the DOM, as before.
enum JsonScalar<'de> {
    Str(Cow<'de, str>),
    Int(i64),
    UInt(u64),
    Float(f64),
    Bool(bool),
    Null,
    Other(JsonValue),
}

impl<'de> serde::Deserialize<'de> for JsonScalar<'de> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct JsonScalarVisitor;

        impl<'de> Visitor<'de> for JsonScalarVisitor {
            type Value = JsonScalar<'de>;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a JSON value")
            }
            fn visit_borrowed_str<E>(self, v: &'de str) -> Result<Self::Value, E> {
                Ok(JsonScalar::Str(Cow::Borrowed(v)))
            }
            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E> {
                Ok(JsonScalar::Str(Cow::Owned(v.to_owned())))
            }
            fn visit_string<E>(self, v: String) -> Result<Self::Value, E> {
                Ok(JsonScalar::Str(Cow::Owned(v)))
            }
            fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E> {
                Ok(JsonScalar::Int(v))
            }
            fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E> {
                Ok(JsonScalar::UInt(v))
            }
            fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E> {
                Ok(JsonScalar::Float(v))
            }
            fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E> {
                Ok(JsonScalar::Bool(v))
            }
            fn visit_unit<E>(self) -> Result<Self::Value, E> {
                Ok(JsonScalar::Null)
            }
            fn visit_none<E>(self) -> Result<Self::Value, E> {
                Ok(JsonScalar::Null)
            }
            fn visit_map<A: MapAccess<'de>>(self, map: A) -> Result<Self::Value, A::Error> {
                <JsonValue as serde::Deserialize>::deserialize(
                    serde::de::value::MapAccessDeserializer::new(map),
                )
                .map(JsonScalar::Other)
            }
            fn visit_seq<A: SeqAccess<'de>>(self, seq: A) -> Result<Self::Value, A::Error> {
                <JsonValue as serde::Deserialize>::deserialize(
                    serde::de::value::SeqAccessDeserializer::new(seq),
                )
                .map(JsonScalar::Other)
            }
        }

        deserializer.deserialize_any(JsonScalarVisitor)
    }
}

impl JsonScalar<'_> {
    /// Direct conversions for the common scalar cases. Anything else is handed
    /// back as a `serde_json::Value` for `parse_value_from_json`, which keeps
    /// the exact semantics (and error payloads) of the slow path.
    // Integers wider than f64's mantissa lose precision here exactly as they
    // do in the general path, which goes through `serde_json::Number::as_f64`.
    #[allow(clippy::cast_precision_loss)]
    fn into_value(self, dtype: &Type) -> Result<Value, JsonValue> {
        match (dtype.unoptionalize(), self) {
            (Type::String | Type::Any, Self::Str(s)) => Ok(Value::from(&*s)),
            (Type::Int | Type::Any, Self::Int(i)) => Ok(Value::from(i)),
            (Type::Int | Type::Any, Self::UInt(u)) => match i64::try_from(u) {
                Ok(i) => Ok(Value::from(i)),
                Err(_) => Err(Self::UInt(u).into_json()),
            },
            (Type::Float, Self::Int(i)) => Ok(Value::from(i as f64)),
            (Type::Float, Self::UInt(u)) => Ok(Value::from(u as f64)),
            (Type::Float | Type::Any, Self::Float(f)) => Ok(Value::from(f)),
            (Type::Bool | Type::Any, Self::Bool(b)) => Ok(Value::Bool(b)),
            (_, scalar) => Err(scalar.into_json()),
        }
    }

    fn into_json(self) -> JsonValue {
        match self {
            Self::Str(s) => JsonValue::String(s.into_owned()),
            Self::Int(i) => JsonValue::from(i),
            Self::UInt(u) => JsonValue::from(u),
            Self::Float(f) => {
                serde_json::Number::from_f64(f).map_or(JsonValue::Null, JsonValue::Number)
            }
            Self::Bool(b) => JsonValue::Bool(b),
            Self::Null => JsonValue::Null,
            Self::Other(v) => v,
        }
    }
}

/// A `serde` visitor that extracts a fixed set of top-level fields from a JSON
/// object in a single streaming pass, without building the full
/// `serde_json::Value` DOM (and, crucially, without the `IndexMap`/`SipHash`
/// machinery that `serde_json::Map` pulls in workspace-wide). Non-object
/// documents and field-absence errors are deferred to the slow path by
/// returning [`FastExtractOutcome::Fallback`].
struct FastFieldExtractor<'a> {
    spec: &'a FastFieldSpec,
    field_absence_is_error: bool,
}

impl<'de> Visitor<'de> for FastFieldExtractor<'_> {
    type Value = FastExtractOutcome;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a JSON value")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let spec = self.spec;
        let mut slots: Vec<Option<DynResult<Value>>> =
            (0..spec.names.len()).map(|_| None).collect();
        while let Some(key) = map.next_key::<Cow<str>>()? {
            if let Some(index) = spec.slot(key.as_ref()) {
                // A needed field: materialize only this value. Last write wins,
                // matching `serde_json::Map` behavior on duplicate keys.
                let dtype = &spec.types[index];
                let scalar: JsonScalar = map.next_value()?;
                let parsed = match scalar.into_value(dtype) {
                    Ok(value) => Ok(value),
                    Err(value) => parse_value_from_json(&value, dtype).ok_or_else(|| {
                        ParseError::FailedToParseFromJson {
                            field_name: spec.names[index].clone(),
                            payload: value,
                            type_: dtype.clone(),
                        }
                        .into()
                    }),
                };
                slots[index] = Some(parsed);
            } else {
                map.next_value::<IgnoredAny>()?;
            }
        }

        let mut result = Vec::with_capacity(slots.len());
        for (index, slot) in slots.into_iter().enumerate() {
            if let Some(value) = slot {
                result.push(value);
            } else if let Some(default) = &spec.defaults[index] {
                result.push(Ok(default.clone()));
            } else if self.field_absence_is_error {
                // The absence error embeds the full payload, which the
                // fast path never builds. Defer to the slow path.
                return Ok(FastExtractOutcome::Fallback);
            } else {
                result.push(Ok(Value::None));
            }
        }
        Ok(FastExtractOutcome::Extracted(result))
    }

    // Non-object documents are routed to the slow path, which reproduces their
    // exact field-absence semantics. The input is fully consumed first so the
    // deserializer ends cleanly.
    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        while seq.next_element::<IgnoredAny>()?.is_some() {}
        Ok(FastExtractOutcome::Fallback)
    }

    fn visit_bool<E>(self, _value: bool) -> Result<Self::Value, E> {
        Ok(FastExtractOutcome::Fallback)
    }
    fn visit_i64<E>(self, _value: i64) -> Result<Self::Value, E> {
        Ok(FastExtractOutcome::Fallback)
    }
    fn visit_u64<E>(self, _value: u64) -> Result<Self::Value, E> {
        Ok(FastExtractOutcome::Fallback)
    }
    fn visit_f64<E>(self, _value: f64) -> Result<Self::Value, E> {
        Ok(FastExtractOutcome::Fallback)
    }
    fn visit_str<E>(self, _value: &str) -> Result<Self::Value, E> {
        Ok(FastExtractOutcome::Fallback)
    }
    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok(FastExtractOutcome::Fallback)
    }
    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(FastExtractOutcome::Fallback)
    }
}

impl Parser for JsonLinesParser {
    fn parse(&mut self, data: &ReaderContext) -> ParseResult {
        let (data_event, raw_bytes_key, raw_bytes_payload): (DataEventType, &[u8], &[u8]) =
            match data {
                RawBytes(event, raw_bytes) => (*event, &[], raw_bytes.as_slice()),
                KeyValue((key, value)) => (
                    DataEventType::Insert,
                    key.as_deref().unwrap_or_default(),
                    value.as_deref().unwrap_or_default(),
                ),
                Diff(_) | TokenizedEntries(..) | CsvRecord(..) | Bson(_) => {
                    return Err(ParseError::UnsupportedReaderContext.into());
                }
                Empty => return Ok(vec![]),
            };
        if raw_bytes_payload.is_empty() && raw_bytes_key.is_empty() {
            return Ok(vec![]);
        }

        // Fast path: stream the key/payload fields directly without building
        // the whole `serde_json::Value` DOM. Falls back to the slow path below
        // for inputs it does not handle (signaled by `Ok(None)`).
        if self.can_use_fast_json {
            if let Some(events) =
                self.try_fast_parse(data_event, raw_bytes_key, raw_bytes_payload)?
            {
                return Ok(events);
            }
        }

        let payload = if self.has_fields_from_payload() {
            if prepare_plaintext_str(raw_bytes_payload).is_ok_and(|s| s == COMMIT_LITERAL) {
                return Ok(vec![ParsedEventWithErrors::AdvanceTime]);
            }
            self.prepare_json(raw_bytes_payload)?
        } else {
            JsonValue::Null
        };

        let key = if self.has_fields_from_key() {
            self.prepare_json(raw_bytes_key)?
        } else {
            JsonValue::Null
        };

        Ok(self.create_events_from_parsed_object(data_event, &key, &payload))
    }

    fn on_new_source_started(&mut self, metadata: &SourceMetadata) {
        if self.has_metadata_column {
            let metadata_serialized: JsonValue = metadata.serialize();
            self.metadata_column_value = metadata_serialized.into();
        }
    }

    fn column_count(&self) -> usize {
        self.value_field_source_lists.sources_order.len()
    }

    fn session_type(&self) -> SessionType {
        self.session_type
    }
}

#[derive(Debug)]
pub struct RegistryEncoderWrapper {
    encoder: RegistryJsonEncoder,
    subject: String,
}

impl RegistryEncoderWrapper {
    pub fn new(encoder: RegistryJsonEncoder, subject: String) -> Self {
        Self { encoder, subject }
    }

    pub fn encode(&mut self, value: &JsonValue) -> Result<Vec<u8>, FormatterError> {
        Ok(self.encoder.encode(
            value,
            &RegistrySubjectNameStrategy::RecordNameStrategy(self.subject.clone()),
        )?)
    }
}

#[derive(Debug)]
pub struct JsonLinesFormatter {
    value_field_names: Vec<String>,
    schema_registry_encoder: Option<RegistryEncoderWrapper>,
}

impl JsonLinesFormatter {
    pub fn new(
        value_field_names: Vec<String>,
        schema_registry_encoder: Option<RegistryEncoderWrapper>,
    ) -> JsonLinesFormatter {
        JsonLinesFormatter {
            value_field_names,
            schema_registry_encoder,
        }
    }

    fn construct_json_as_raw_bytes(
        &mut self,
        values: &[Value],
        time: Timestamp,
        diff: isize,
    ) -> Result<Vec<u8>, FormatterError> {
        let mut serializer = serde_json::Serializer::new(Vec::<u8>::new());
        let mut map = serializer
            .serialize_map(Some(self.value_field_names.len() + 2))
            .unwrap();
        for (key, value) in zip(self.value_field_names.iter(), values) {
            map.serialize_entry(key, &serialize_value_to_json(value)?)
                .unwrap();
        }
        map.serialize_entry(SPECIAL_FIELD_DIFF, &diff).unwrap();
        map.serialize_entry(SPECIAL_FIELD_TIME, &time).unwrap();
        map.end().unwrap();
        Ok(serializer.into_inner())
    }

    fn construct_json_with_encoder(
        encoder: &mut RegistryEncoderWrapper,
        value_field_names: &[String],
        values: &[Value],
        time: Timestamp,
        diff: isize,
    ) -> Result<Vec<u8>, FormatterError> {
        let mut json_payload = json!({
            SPECIAL_FIELD_DIFF: diff,
            SPECIAL_FIELD_TIME: time,
        });
        let json_payload_map = json_payload.as_object_mut().unwrap();
        for (key, value) in zip(value_field_names.iter(), values) {
            json_payload_map.insert(key.clone(), serialize_value_to_json(value)?);
        }
        encoder.encode(&json_payload)
    }
}

impl Formatter for JsonLinesFormatter {
    fn format(
        &mut self,
        key: &Key,
        values: &[Value],
        time: Timestamp,
        diff: isize,
    ) -> Result<FormatterContext, FormatterError> {
        let raw_bytes = match self.schema_registry_encoder.as_mut() {
            Some(encoder) => Self::construct_json_with_encoder(
                encoder,
                &self.value_field_names,
                values,
                time,
                diff,
            ),
            None => self.construct_json_as_raw_bytes(values, time, diff),
        }?;

        Ok(FormatterContext::new_single_payload(
            raw_bytes,
            *key,
            values.to_vec(),
            time,
            diff,
        ))
    }
}
