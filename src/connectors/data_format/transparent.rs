// Copyright © 2026 Pathway

use std::clone::Clone;
use std::collections::HashMap;

use crate::connectors::metadata::SourceMetadata;
use crate::connectors::ReaderContext::{Diff, Empty};
use crate::connectors::{ReaderContext, SessionType};
use crate::engine::Result;

use super::{
    ensure_all_fields_in_schema, InnerSchemaField, ParseError, ParseResult, ParsedEventWithErrors,
    Parser,
};
use crate::connectors::data_storage::SpecialEvent;

/// Receives values directly from a Reader and passes them
/// further only making adjustments according to the schema.
///
/// It is useful when no raw values parsing is needed.
pub struct TransparentParser {
    key_field_names: Option<Vec<String>>,
    value_field_names: Vec<String>,
    schema: HashMap<String, InnerSchemaField>,
    session_type: SessionType,
}

impl TransparentParser {
    pub fn new(
        key_field_names: Option<Vec<String>>,
        value_field_names: Vec<String>,
        schema: HashMap<String, InnerSchemaField>,
        session_type: SessionType,
    ) -> Result<TransparentParser> {
        ensure_all_fields_in_schema(key_field_names.as_deref(), &value_field_names, &schema)?;
        Ok(TransparentParser {
            key_field_names,
            value_field_names,
            schema,
            session_type,
        })
    }
}

impl Parser for TransparentParser {
    fn parse(&mut self, data: &ReaderContext) -> ParseResult {
        let (data_event, key, values) = match data {
            Empty => return Ok(vec![]),
            Diff((data_event, key, values)) => (data_event, key, values),
            _ => return Err(ParseError::UnsupportedReaderContext.into()),
        };
        if values.get_special() == Some(SpecialEvent::Commit) {
            return Ok(vec![ParsedEventWithErrors::AdvanceTime]);
        }
        let key = key.clone().map(Ok).or_else(|| {
            self.key_field_names.as_ref().map(|key_field_names| {
                key_field_names
                    .iter()
                    .map(|name| {
                        self.schema[name] // ensure_all_fields_in_schema in new() makes sure that all keys are in the schema
                        .maybe_use_default(name, values.get(name).cloned())
                    })
                    .collect()
            })
        });

        let values: Vec<_> = self
            .value_field_names
            .iter()
            .map(|name| {
                self.schema[name] // ensure_all_fields_in_schema in new() makes sure that all keys are in the schema
                    .maybe_use_default(name, values.get(name).cloned())
            })
            .collect();

        let event = ParsedEventWithErrors::new(self.session_type, *data_event, key, values);

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
