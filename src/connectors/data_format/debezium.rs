// Copyright © 2026 Pathway

use std::clone::Clone;
use std::collections::HashMap;

use crate::connectors::metadata::SourceMetadata;
use crate::connectors::ReaderContext::{Bson, Diff, Empty, KeyValue, RawBytes, TokenizedEntries};
use crate::connectors::{DataEventType, ReaderContext, SessionType};
use crate::engine::Result;

use serde_json::Value as JsonValue;

use super::{
    prepare_plaintext_string, values_by_names_from_json, DebeziumFormatError, ParseError,
    ParseResult, ParsedEventWithErrors, Parser,
};

const DEBEZIUM_EMPTY_KEY_PAYLOAD: &str = "{\"payload\": {\"before\": {}, \"after\": {}}}";

#[derive(Clone, Copy, Debug)]
pub enum DebeziumDBType {
    Postgres,
    MongoDB,
}

pub struct DebeziumMessageParser {
    key_field_names: Option<Vec<String>>,
    value_field_names: Vec<String>,
    separator: String, // how key-value pair is separated
    db_type: DebeziumDBType,
}

impl DebeziumMessageParser {
    pub fn new(
        key_field_names: Option<Vec<String>>,
        value_field_names: Vec<String>,
        separator: String,
        db_type: DebeziumDBType,
    ) -> DebeziumMessageParser {
        DebeziumMessageParser {
            key_field_names,
            value_field_names,
            separator,
            db_type,
        }
    }

    pub fn standard_separator() -> String {
        "        ".to_string()
    }

    fn parse_event(
        &mut self,
        key: &JsonValue,
        value: &JsonValue,
        event: DataEventType,
    ) -> Result<ParsedEventWithErrors, ParseError> {
        // in case of MongoDB, the message is always string
        let prepared_value: JsonValue = {
            if let JsonValue::String(serialized_json) = &value {
                let Ok(prepared_value) = serde_json::from_str::<JsonValue>(serialized_json) else {
                    return Err(ParseError::FailedToParseJson(serialized_json.clone()));
                };
                prepared_value
            } else {
                value.clone()
            }
        };

        let key = self.key_field_names.as_ref().map(|names| {
            values_by_names_from_json(key, names, &HashMap::new(), true, &HashMap::new())
                .into_iter()
                .collect()
        });

        let parsed_values = values_by_names_from_json(
            &prepared_value,
            &self.value_field_names,
            &HashMap::new(),
            true,
            &HashMap::new(),
        );

        Ok(ParsedEventWithErrors::new(
            self.session_type(),
            event,
            key,
            parsed_values,
        ))
    }

    fn parse_read_or_create(&mut self, key: &JsonValue, value: &JsonValue) -> ParseResult {
        let event = self.parse_event(key, &value["after"], DataEventType::Insert)?;
        Ok(vec![event])
    }

    fn parse_delete(&mut self, key: &JsonValue, value: &JsonValue) -> ParseResult {
        let event = match self.db_type {
            DebeziumDBType::Postgres => {
                self.parse_event(key, &value["before"], DataEventType::Delete)?
            }
            DebeziumDBType::MongoDB => {
                let key = self.key_field_names.as_ref().map(|names| {
                    values_by_names_from_json(key, names, &HashMap::new(), true, &HashMap::new())
                        .into_iter()
                        .collect()
                });
                //deletion in upsert session - no data needed
                ParsedEventWithErrors::Delete((key, vec![]))
            }
        };
        Ok(vec![event])
    }

    fn parse_update(&mut self, key: &JsonValue, value: &JsonValue) -> ParseResult {
        match self.db_type {
            DebeziumDBType::Postgres => {
                let event_before =
                    self.parse_event(key, &value["before"], DataEventType::Delete)?;
                let event_after = self.parse_event(key, &value["after"], DataEventType::Insert)?;
                Ok(vec![event_before, event_after])
            }
            DebeziumDBType::MongoDB => {
                let event_after = self.parse_event(key, &value["after"], DataEventType::Insert)?;
                Ok(vec![event_after])
            }
        }
    }
}

impl Parser for DebeziumMessageParser {
    fn parse(&mut self, data: &ReaderContext) -> ParseResult {
        let (raw_key_change, raw_value_change) = match data {
            RawBytes(event, raw_bytes) => {
                // We don't use `event` type here, because it's Debezium message parser,
                // whose messages can only arrive from Kafka.
                //
                // There is no snapshot scenario for Kafka.
                //
                // In addition, this branch of `match` condition is only used in unit-tests.
                assert_eq!(event, &DataEventType::Insert);

                let line = prepare_plaintext_string(raw_bytes)?;

                let key_and_value: Vec<&str> = line.trim().split(&self.separator).collect();
                if key_and_value.len() != 2 {
                    return Err(ParseError::KeyValueTokensIncorrect(key_and_value.len()).into());
                }
                (key_and_value[0].to_string(), key_and_value[1].to_string())
            }
            KeyValue((k, v)) => {
                let key = if let Some(bytes) = k {
                    prepare_plaintext_string(bytes)?
                } else {
                    if self.key_field_names.is_some() {
                        return Err(ParseError::EmptyKafkaPayload.into());
                    }
                    DEBEZIUM_EMPTY_KEY_PAYLOAD.to_string()
                };
                let value = match v {
                    Some(bytes) => prepare_plaintext_string(bytes)?,
                    None => return Err(ParseError::EmptyKafkaPayload.into()),
                };
                (key, value)
            }
            Diff(_) | TokenizedEntries(_, _) | Empty | Bson(_) => {
                return Err(ParseError::UnsupportedReaderContext.into());
            }
        };

        let Ok(value_change) = serde_json::from_str(&raw_value_change) else {
            return Err(ParseError::FailedToParseJson(raw_value_change).into());
        };

        let change_payload = match value_change {
            JsonValue::Object(payload_value) => payload_value,
            JsonValue::Null => return Ok(Vec::new()), // tombstone event for kafka: nothing to do for us
            _ => {
                return Err(ParseError::DebeziumFormatViolated(
                    DebeziumFormatError::IncorrectJsonRoot,
                )
                .into())
            }
        };

        let Ok(change_key) = serde_json::from_str::<JsonValue>(&raw_key_change) else {
            return Err(ParseError::FailedToParseJson(raw_key_change).into());
        };

        if !change_payload.contains_key("payload") {
            return Err(ParseError::DebeziumFormatViolated(
                DebeziumFormatError::NoPayloadAtTopLevel,
            )
            .into());
        }

        match &change_payload["payload"]["op"] {
            JsonValue::String(op) => match op.as_ref() {
                "r" | "c" => {
                    self.parse_read_or_create(&change_key["payload"], &change_payload["payload"])
                }
                "u" => self.parse_update(&change_key["payload"], &change_payload["payload"]),
                "d" => self.parse_delete(&change_key["payload"], &change_payload["payload"]),
                _ => Err(ParseError::UnsupportedDebeziumOperation(op.clone()).into()),
            },
            _ => Err(ParseError::DebeziumFormatViolated(
                DebeziumFormatError::OperationFieldMissing,
            )
            .into()),
        }
    }

    fn on_new_source_started(&mut self, _metadata: &SourceMetadata) {}

    fn column_count(&self) -> usize {
        self.value_field_names.len()
    }

    fn session_type(&self) -> SessionType {
        match self.db_type {
            DebeziumDBType::Postgres => SessionType::Native,

            // MongoDB events don't contain the previous state of the record
            // therefore we can only do the upsert with the same key and the
            // new value
            DebeziumDBType::MongoDB => SessionType::Upsert,
        }
    }
}
