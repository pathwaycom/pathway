// Copyright © 2026 Pathway

use std::clone::Clone;

use crate::connectors::metadata::SourceMetadata;
use crate::connectors::ReaderContext::{Bson, Diff, Empty, KeyValue, RawBytes, TokenizedEntries};
use crate::connectors::{DataEventType, ReaderContext, SessionType};
use crate::engine::error::DynResult;
use crate::engine::{Key, Result, Timestamp, Value};

use serde_json::Value as JsonValue;

use super::{
    is_commit_literal, value_from_bytes, Formatter, FormatterContext, FormatterError, ParseError,
    ParseResult, ParsedEventWithErrors, Parser, METADATA_FIELD_NAME,
};

#[derive(Clone, Copy, Debug)]
pub enum KeyGenerationPolicy {
    AlwaysAutogenerate,
    PreferMessageKey,
}

impl KeyGenerationPolicy {
    fn generate(self, key: Option<&Vec<u8>>, parse_utf8: bool) -> Option<DynResult<Vec<Value>>> {
        match &self {
            Self::AlwaysAutogenerate => None,
            Self::PreferMessageKey => key
                .as_ref()
                .map(|bytes| value_from_bytes(bytes, parse_utf8).map(|k| vec![k])),
        }
    }
}

pub struct IdentityParser {
    parse_utf8: bool,
    metadata_column_value: Value,
    session_type: SessionType,
    key_generation_policy: KeyGenerationPolicy,

    n_value_fields: usize,
    key_field_index: Option<usize>,
    metadata_field_index: Option<usize>,
    value_field_index: usize,
}

impl IdentityParser {
    pub fn new(
        value_fields: &[String],
        parse_utf8: bool,
        message_queue_key_field: Option<&String>,
        key_generation_policy: KeyGenerationPolicy,
        session_type: SessionType,
    ) -> IdentityParser {
        let mut key_field_index = None;
        let mut metadata_field_index = None;
        let mut value_field_index = None;
        for (index, value_field) in value_fields.iter().enumerate() {
            if value_field == METADATA_FIELD_NAME {
                assert!(metadata_field_index.is_none());
                metadata_field_index = Some(index);
            } else if Some(value_field) == message_queue_key_field {
                assert!(key_field_index.is_none());
                key_field_index = Some(index);
            } else {
                assert!(value_field_index.is_none());
                value_field_index = Some(index);
            }
        }

        Self {
            n_value_fields: value_fields.len(),
            parse_utf8,
            metadata_column_value: Value::None,
            key_generation_policy,
            session_type,
            key_field_index,
            metadata_field_index,
            value_field_index: value_field_index
                .expect("value field must be present in the schema"),
        }
    }
}

impl Parser for IdentityParser {
    fn parse(&mut self, data: &ReaderContext) -> ParseResult {
        let mut values = Vec::with_capacity(self.n_value_fields);
        for _ in 0..self.n_value_fields {
            // clone isn't available for the array element type, hence constructing manually
            values.push(Ok(Value::None));
        }

        let (event, key, value, metadata) = match data {
            RawBytes(event, raw_bytes) => (
                *event,
                None,
                value_from_bytes(raw_bytes, self.parse_utf8),
                Ok(None),
            ),
            KeyValue((key, value)) => {
                if let Some(key_field_index) = self.key_field_index {
                    values[key_field_index] = key
                        .as_ref()
                        .map_or_else(|| Ok(Value::None), |k| value_from_bytes(k, self.parse_utf8));
                }
                (
                    DataEventType::Insert,
                    self.key_generation_policy
                        .generate(key.as_ref(), self.parse_utf8),
                    value.as_ref().map_or_else(
                        || Ok(Value::None),
                        |bytes| value_from_bytes(bytes, self.parse_utf8),
                    ),
                    Ok(None),
                )
            }
            Diff(_) | TokenizedEntries(_, _) | Bson(_) => {
                return Err(ParseError::UnsupportedReaderContext.into())
            }
            Empty => return Ok(vec![]),
        };

        let is_commit = is_commit_literal(&value);

        let event = if is_commit {
            ParsedEventWithErrors::AdvanceTime
        } else {
            if let Some(metadata_field_index) = self.metadata_field_index {
                values[metadata_field_index] =
                    metadata.map(|metadata| metadata.unwrap_or(self.metadata_column_value.clone()));
            }
            values[self.value_field_index] = value;
            ParsedEventWithErrors::new(self.session_type(), event, key, values)
        };

        Ok(vec![event])
    }

    fn on_new_source_started(&mut self, metadata: &SourceMetadata) {
        let metadata_serialized: JsonValue = metadata.serialize();
        self.metadata_column_value = metadata_serialized.into();
    }

    fn column_count(&self) -> usize {
        self.n_value_fields
    }

    fn session_type(&self) -> SessionType {
        self.session_type
    }
}

#[derive(Default)]
pub struct IdentityFormatter {
    external_diff_column_index: Option<usize>,
}

impl IdentityFormatter {
    pub fn new(external_diff_column_index: Option<usize>) -> Self {
        Self {
            external_diff_column_index,
        }
    }
}

impl Formatter for IdentityFormatter {
    fn format(
        &mut self,
        key: &Key,
        values: &[Value],
        time: Timestamp,
        diff: isize,
    ) -> Result<FormatterContext, FormatterError> {
        let prepared_diff =
            if let Some(external_diff_column_index) = self.external_diff_column_index {
                let value = &values[external_diff_column_index];
                match value {
                    Value::Int(inner) if *inner == 1 => 1,
                    Value::Int(inner) if *inner == -1 => -1,
                    _ => return Err(FormatterError::IncorrectDiffColumnValue(value.clone())),
                }
            } else {
                diff
            };
        Ok(FormatterContext::new_single_payload(
            Vec::new(),
            *key,
            values.to_vec(),
            time,
            prepared_diff,
        ))
    }
}
