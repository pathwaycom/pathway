// Copyright © 2026 Pathway

use std::clone::Clone;
use std::collections::HashMap;
use std::iter::zip;

use crate::connectors::metadata::SourceMetadata;
use crate::connectors::ReaderContext::{Bson, Diff, Empty, KeyValue, RawBytes, TokenizedEntries};
use crate::connectors::{DataEventType, ReaderContext, SessionType};
use crate::connectors::{SPECIAL_FIELD_DIFF, SPECIAL_FIELD_TIME};
use crate::engine::error::DynResult;
use crate::engine::{Key, Result, Timestamp, Value};
use crate::python_api::ValueField;

use schema_registry_converter::blocking::json::JsonDecoder as RegistryJsonDecoder;
use schema_registry_converter::blocking::json::JsonEncoder as RegistryJsonEncoder;
use schema_registry_converter::schema_registry_common::SubjectNameStrategy as RegistrySubjectNameStrategy;
use serde::ser::{SerializeMap, Serializer};
use serde_json::json;
use serde_json::Value as JsonValue;

use super::{
    ensure_all_fields_in_schema, prepare_plaintext_string, serialize_value_to_json,
    values_by_names_from_json, Formatter, FormatterContext, FormatterError, InnerSchemaField,
    ParseError, ParseResult, ParsedEventWithErrors, Parser, ValueFieldsWithErrors, COMMIT_LITERAL,
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

        Ok(JsonLinesParser {
            key_field_source_lists: key_source_lists,
            value_field_source_lists: value_source_lists,
            column_paths,
            field_absence_is_error,
            schema,
            metadata_column_value: Value::None,
            session_type,
            schema_registry_decoder,
        })
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
            match prepare_plaintext_string(raw_bytes)?.as_str() {
                "" => JsonValue::Null,
                line => serde_json::from_str(line)?,
            }
        };
        Ok(result)
    }
}

impl Parser for JsonLinesParser {
    fn parse(&mut self, data: &ReaderContext) -> ParseResult {
        let (data_event, raw_bytes_key, raw_bytes_payload) = match data {
            RawBytes(event, raw_bytes) => (*event, vec![], raw_bytes.clone()),
            KeyValue((key, value)) => {
                let raw_bytes_key = key.clone().unwrap_or_default();
                let raw_bytes_value = value.clone().unwrap_or_default();
                (DataEventType::Insert, raw_bytes_key, raw_bytes_value)
            }
            Diff(_) | TokenizedEntries(..) | Bson(_) => {
                return Err(ParseError::UnsupportedReaderContext.into());
            }
            Empty => return Ok(vec![]),
        };
        if raw_bytes_payload.is_empty() && raw_bytes_key.is_empty() {
            return Ok(vec![]);
        }

        let payload = if self.has_fields_from_payload() {
            if prepare_plaintext_string(&raw_bytes_payload)
                .as_ref()
                .is_ok_and(|s| s.as_str() == COMMIT_LITERAL)
            {
                return Ok(vec![ParsedEventWithErrors::AdvanceTime]);
            }
            self.prepare_json(&raw_bytes_payload)?
        } else {
            JsonValue::Null
        };

        let key = if self.has_fields_from_key() {
            self.prepare_json(&raw_bytes_key)?
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
