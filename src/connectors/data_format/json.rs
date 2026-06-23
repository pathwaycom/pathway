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
    session_type: SessionType,
    schema_registry_decoder: Option<RegistryJsonDecoder>,

    // Fast-path state: when all value fields come from the payload as top-level
    // keys (no JSON-pointer paths, no key fields, no schema-registry decoder),
    // the payload is extracted with a single streaming pass that never
    // materializes the full `serde_json::Value` DOM. `payload_field_index` maps
    // each payload field name to its position within
    // `value_field_source_lists.to_parse_from_payload`.
    can_use_fast_json: bool,
    payload_field_index: HashMap<String, usize>,
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

        // The fast path applies only when every needed field is a top-level key
        // of the payload JSON. It is disabled when any field is read from the
        // key, when JSON-pointer column paths are configured, or when a
        // schema-registry decoder is used (the payload is then not a plain
        // JSON string).
        let needs_key_fields =
            key_source_lists.is_some() || !value_source_lists.to_parse_from_key.is_empty();
        let can_use_fast_json =
            schema_registry_decoder.is_none() && column_paths.is_empty() && !needs_key_fields;
        let payload_field_index = value_source_lists
            .to_parse_from_payload
            .iter()
            .enumerate()
            .map(|(index, name)| (name.clone(), index))
            .collect();

        Ok(JsonLinesParser {
            key_field_source_lists: key_source_lists,
            value_field_source_lists: value_source_lists,
            column_paths,
            field_absence_is_error,
            schema,
            metadata_column_value: Value::None,
            session_type,
            schema_registry_decoder,
            can_use_fast_json,
            payload_field_index,
        })
    }

    /// Streaming extraction of the payload fields, used when
    /// [`Self::can_use_fast_json`] holds. Returns the value-field results
    /// aligned to `value_field_source_lists.sources_order`, or `None` to
    /// request a fallback to the DOM-based slow path (for inputs whose exact
    /// behavior — e.g. error payloads or non-object JSON — is preserved there).
    fn try_fast_parse_payload(&self, line: &str) -> DynResult<Option<ValueFieldsWithErrors>> {
        if line.is_empty() {
            // Slow path maps an empty payload to `JsonValue::Null`, then to the
            // field-absence handling. Defer to it.
            return Ok(None);
        }
        let extractor = FastFieldExtractor {
            field_names: &self.value_field_source_lists.to_parse_from_payload,
            field_index: &self.payload_field_index,
            schema: &self.schema,
            field_absence_is_error: self.field_absence_is_error,
        };
        let mut deserializer = serde_json::Deserializer::from_str(line);
        let outcome = (&mut deserializer).deserialize_any(extractor)?;
        deserializer.end()?;

        let FastExtractOutcome::Extracted(payload_values) = outcome else {
            return Ok(None);
        };

        let source_lists = &self.value_field_source_lists;
        let mut payload_values = payload_values.into_iter();
        let mut result = Vec::with_capacity(source_lists.sources_order.len());
        for source in &source_lists.sources_order {
            match source {
                FieldSource::Payload => result.push(payload_values.next().unwrap()),
                FieldSource::Metadata => result.push(Ok(self.metadata_column_value.clone())),
                // Excluded by `can_use_fast_json`.
                FieldSource::Key => unreachable!("key fields disable the fast JSON path"),
            }
        }
        Ok(Some(result))
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

/// A `serde` visitor that extracts a fixed set of top-level fields from a JSON
/// object in a single streaming pass, without building the full
/// `serde_json::Value` DOM (and, crucially, without the `IndexMap`/`SipHash`
/// machinery that `serde_json::Map` pulls in workspace-wide). Non-object
/// documents and field-absence errors are deferred to the slow path by
/// returning [`FastExtractOutcome::Fallback`].
struct FastFieldExtractor<'a> {
    field_names: &'a [String],
    field_index: &'a HashMap<String, usize>,
    schema: &'a HashMap<String, InnerSchemaField>,
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
        let mut slots: Vec<Option<DynResult<Value>>> =
            (0..self.field_names.len()).map(|_| None).collect();
        while let Some(key) = map.next_key::<Cow<str>>()? {
            if let Some(&index) = self.field_index.get(key.as_ref()) {
                // A needed field: materialize only this value. Last write wins,
                // matching `serde_json::Map` behavior on duplicate keys.
                let value: JsonValue = map.next_value()?;
                let name = &self.field_names[index];
                let dtype = self
                    .schema
                    .get(name)
                    .map_or(&Type::Any, |field| &field.type_);
                let parsed = parse_value_from_json(&value, dtype).ok_or_else(|| {
                    ParseError::FailedToParseFromJson {
                        field_name: name.clone(),
                        payload: value,
                        type_: dtype.clone(),
                    }
                    .into()
                });
                slots[index] = Some(parsed);
            } else {
                map.next_value::<IgnoredAny>()?;
            }
        }

        let mut result = Vec::with_capacity(slots.len());
        for (index, slot) in slots.into_iter().enumerate() {
            if let Some(value) = slot {
                result.push(value);
            } else {
                let name = &self.field_names[index];
                let default = self
                    .schema
                    .get(name)
                    .and_then(|field| field.default.as_ref());
                if let Some(default) = default {
                    result.push(Ok(default.clone()));
                } else if self.field_absence_is_error {
                    // The absence error embeds the full payload, which the
                    // fast path never builds. Defer to the slow path.
                    return Ok(FastExtractOutcome::Fallback);
                } else {
                    result.push(Ok(Value::None));
                }
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

        // Fast path: stream the payload fields directly without building the
        // whole `serde_json::Value` DOM. Falls back to the slow path below for
        // inputs it does not handle (signaled by `Ok(None)`).
        if self.can_use_fast_json && self.has_fields_from_payload() {
            let line = prepare_plaintext_str(raw_bytes_payload)?;
            if line == COMMIT_LITERAL {
                return Ok(vec![ParsedEventWithErrors::AdvanceTime]);
            }
            if let Some(values) = self.try_fast_parse_payload(line)? {
                return Ok(vec![ParsedEventWithErrors::new(
                    self.session_type,
                    data_event,
                    None,
                    values,
                )]);
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
        let metadata_serialized: JsonValue = metadata.serialize();
        self.metadata_column_value = metadata_serialized.into();
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
