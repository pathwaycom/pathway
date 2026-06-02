// Copyright © 2026 Pathway

use std::clone::Clone;
use std::collections::HashMap;

use crate::connectors::metadata::SourceMetadata;
use crate::connectors::ReaderContext::{Bson, Diff, Empty, KeyValue, RawBytes, TokenizedEntries};
use crate::connectors::{DataEventType, ReaderContext};
use crate::connectors::{SPECIAL_FIELD_DIFF, SPECIAL_FIELD_TIME};
use crate::engine::{Key, Result, Timestamp, Value};

use base64::Engine;
use serde_json::Value as JsonValue;

use super::{
    create_bincoded_value, ensure_all_fields_in_schema, parse_with_type, prepare_plaintext_string,
    serialize_value_to_json, Formatter, FormatterContext, FormatterError, InnerSchemaField,
    ParseError, ParseResult, ParsedEventWithErrors, Parser, ValueFieldsWithErrors, COMMIT_LITERAL,
    METADATA_FIELD_NAME,
};

pub struct DsvSettings {
    key_column_names: Option<Vec<String>>,
    value_column_names: Vec<String>,
    separator: char,
}

impl DsvSettings {
    pub fn new(
        key_column_names: Option<Vec<String>>,
        value_column_names: Vec<String>,
        separator: char,
    ) -> DsvSettings {
        DsvSettings {
            key_column_names,
            value_column_names,
            separator,
        }
    }

    pub fn formatter(self) -> Box<dyn Formatter> {
        Box::new(DsvFormatter::new(self))
    }

    pub fn parser(self, schema: HashMap<String, InnerSchemaField>) -> Result<Box<dyn Parser>> {
        Ok(Box::new(DsvParser::new(self, schema)?))
    }
}

#[derive(Clone)]
enum DsvColumnIndex {
    IndexWithSchema(usize, InnerSchemaField),
    Metadata,
}

pub struct DsvParser {
    settings: DsvSettings,
    schema: HashMap<String, InnerSchemaField>,
    header: Vec<String>,

    metadata_column_value: Value,
    key_column_indices: Option<Vec<DsvColumnIndex>>,
    value_column_indices: Vec<DsvColumnIndex>,
    dsv_header_read: bool,
}

impl DsvParser {
    pub fn new(
        settings: DsvSettings,
        schema: HashMap<String, InnerSchemaField>,
    ) -> Result<DsvParser> {
        ensure_all_fields_in_schema(
            settings.key_column_names.as_deref(),
            &settings.value_column_names,
            &schema,
        )?;
        Ok(DsvParser {
            settings,
            schema,
            metadata_column_value: Value::None,
            header: Vec::new(),
            key_column_indices: None,
            value_column_indices: Vec::new(),
            dsv_header_read: false,
        })
    }

    fn column_indices_by_names(
        tokenized_entries: &[String],
        sought_names: &[String],
        schema: &HashMap<String, InnerSchemaField>,
    ) -> Result<Vec<DsvColumnIndex>, ParseError> {
        let mut value_indices_found = 0;

        let mut column_indices = vec![DsvColumnIndex::Metadata; sought_names.len()];
        let mut requested_indices = HashMap::<String, Vec<usize>>::new();
        for (index, field) in sought_names.iter().enumerate() {
            if field == METADATA_FIELD_NAME {
                value_indices_found += 1;
                continue;
            }
            match requested_indices.get_mut(field) {
                Some(indices) => indices.push(index),
                None => {
                    requested_indices.insert(field.clone(), vec![index]);
                }
            }
        }

        for (index, value) in tokenized_entries.iter().enumerate() {
            if let Some(indices) = requested_indices.get(value) {
                let schema_item = &schema[value];
                for requested_index in indices {
                    column_indices[*requested_index] =
                        DsvColumnIndex::IndexWithSchema(index, schema_item.clone());
                    value_indices_found += 1;
                }
            }
        }

        if value_indices_found == sought_names.len() {
            Ok(column_indices)
        } else {
            Err(ParseError::FieldsNotFoundInHeader {
                parsed: tokenized_entries.to_vec(),
                requested: sought_names.to_vec(),
            })
        }
    }

    fn parse_dsv_header(&mut self, tokenized_entries: &[String]) -> Result<(), ParseError> {
        self.key_column_indices = match &self.settings.key_column_names {
            Some(names) => Some(Self::column_indices_by_names(
                tokenized_entries,
                names,
                &self.schema,
            )?),
            None => None,
        };
        self.value_column_indices = Self::column_indices_by_names(
            tokenized_entries,
            &self.settings.value_column_names,
            &self.schema,
        )?;

        self.header = tokenized_entries.to_vec();
        self.dsv_header_read = true;
        Ok(())
    }

    fn parse_bytes_simple(&mut self, event: DataEventType, raw_bytes: &[u8]) -> ParseResult {
        let line = prepare_plaintext_string(raw_bytes)?;

        if line.is_empty() {
            return Ok(Vec::new());
        }

        if line == COMMIT_LITERAL {
            return Ok(vec![ParsedEventWithErrors::AdvanceTime]);
        }

        let tokens: Vec<String> = line
            .split(self.settings.separator)
            .map(std::string::ToString::to_string)
            .collect();
        self.parse_tokenized_entries(event, &tokens)
    }

    fn values_by_indices(
        &self,
        tokens: &[String],
        indices: &[DsvColumnIndex],
        header: &[String],
    ) -> ValueFieldsWithErrors {
        let mut parsed_tokens = Vec::with_capacity(indices.len());
        for index in indices {
            let token = match index {
                DsvColumnIndex::IndexWithSchema(index, schema_item) => {
                    parse_with_type(&tokens[*index], schema_item, &header[*index])
                }
                DsvColumnIndex::Metadata => Ok(self.metadata_column_value.clone()),
            };
            parsed_tokens.push(token);
        }
        parsed_tokens
    }

    fn parse_tokenized_entries(&mut self, event: DataEventType, tokens: &[String]) -> ParseResult {
        if tokens.len() == 1 {
            let line = &tokens[0];
            if line == COMMIT_LITERAL {
                return Ok(vec![ParsedEventWithErrors::AdvanceTime]);
            }
        }

        if !self.dsv_header_read {
            self.parse_dsv_header(tokens)?;
            return Ok(Vec::new());
        }

        let mut line_has_enough_tokens = true;
        if let Some(indices) = &self.key_column_indices {
            for index in indices {
                if let DsvColumnIndex::IndexWithSchema(index, _) = index {
                    line_has_enough_tokens &= index < &tokens.len();
                }
            }
        }
        for index in &self.value_column_indices {
            if let DsvColumnIndex::IndexWithSchema(index, _) = index {
                line_has_enough_tokens &= index < &tokens.len();
            }
        }
        if line_has_enough_tokens {
            let key = match &self.key_column_indices {
                Some(indices) => Some(
                    self.values_by_indices(tokens, indices, &self.header)
                        .into_iter()
                        .collect(),
                ),
                None => None,
            };
            let parsed_tokens =
                self.values_by_indices(tokens, &self.value_column_indices, &self.header);
            let parsed_entry =
                ParsedEventWithErrors::new(self.session_type(), event, key, parsed_tokens);
            Ok(vec![parsed_entry])
        } else {
            Err(ParseError::UnexpectedNumberOfCsvTokens(tokens.len()).into())
        }
    }
}

impl Parser for DsvParser {
    fn parse(&mut self, data: &ReaderContext) -> ParseResult {
        match data {
            RawBytes(event, raw_bytes) => self.parse_bytes_simple(*event, raw_bytes),
            TokenizedEntries(event, tokenized_entries) => {
                self.parse_tokenized_entries(*event, tokenized_entries)
            }
            KeyValue((_key, value)) => match value {
                Some(bytes) => self.parse_bytes_simple(DataEventType::Insert, bytes), // In Kafka we only have additions now
                None => Err(ParseError::EmptyKafkaPayload.into()),
            },
            Diff(_) | Bson(_) => Err(ParseError::UnsupportedReaderContext.into()),
            Empty => Ok(vec![]),
        }
    }

    fn on_new_source_started(&mut self, metadata: &SourceMetadata) {
        if !metadata.commits_allowed_in_between() {
            // TODO: find a better solution
            self.dsv_header_read = false;
        }
        let metadata_serialized: JsonValue = metadata.serialize();
        self.metadata_column_value = metadata_serialized.into();
    }

    fn column_count(&self) -> usize {
        self.settings.value_column_names.len()
    }
}

pub struct DsvFormatter {
    settings: DsvSettings,

    dsv_header_written: bool,
}

impl DsvFormatter {
    pub fn new(settings: DsvSettings) -> DsvFormatter {
        DsvFormatter {
            settings,

            dsv_header_written: false,
        }
    }

    fn format_csv_row(tokens: Vec<String>, separator: u8) -> Result<Vec<u8>, FormatterError> {
        let mut writer = csv::WriterBuilder::new()
            .delimiter(separator)
            .terminator(csv::Terminator::Any(0)) // There is no option for not having a row terminator
            .quote_style(csv::QuoteStyle::Always)
            .from_writer(Vec::new());
        writer.write_record(tokens)?;
        let mut formatted = writer
            .into_inner()
            .expect("csv::Writer::into_inner can't fail for Vec<u8> as an underlying writer");
        formatted.pop(); // Remove the row terminator character
        Ok(formatted)
    }
}

impl Formatter for DsvFormatter {
    fn format(
        &mut self,
        key: &Key,
        values: &[Value],
        time: Timestamp,
        diff: isize,
    ) -> Result<FormatterContext, FormatterError> {
        if values.len() != self.settings.value_column_names.len() {
            return Err(FormatterError::ColumnsValuesCountMismatch);
        }

        let Ok(separator) = self.settings.separator.try_into() else {
            return Err(FormatterError::UnsupportedCsvSeparator(
                self.settings.separator,
            ));
        };
        let mut payloads = Vec::with_capacity(2);

        if !self.dsv_header_written {
            let header: Vec<_> = self
                .settings
                .value_column_names
                .iter()
                .map(std::string::ToString::to_string)
                .chain([
                    SPECIAL_FIELD_TIME.to_string(),
                    SPECIAL_FIELD_DIFF.to_string(),
                ])
                .collect();
            payloads.push(Self::format_csv_row(header, separator)?);
            self.dsv_header_written = true;
        }

        let mut prepared_values = Vec::with_capacity(values.len());
        for v in values {
            let prepared = match v {
                Value::String(v) => v.to_string(),
                Value::PyObjectWrapper(_) => create_bincoded_value(v)?,
                Value::Bytes(b) => base64::engine::general_purpose::STANDARD.encode(b),
                Value::Duration(d) => format!("{}", d.nanoseconds()),
                Value::IntArray(_) | Value::FloatArray(_) | Value::Tuple(_) => {
                    let json_value = serialize_value_to_json(v)?;
                    json_value.to_string()
                }
                _ => format!("{v}"),
            };
            prepared_values.push(prepared);
        }
        let line: Vec<_> = prepared_values
            .into_iter()
            .chain([format!("{time}").to_string(), format!("{diff}").to_string()])
            .collect();
        payloads.push(Self::format_csv_row(line, separator)?);

        Ok(FormatterContext::new(
            payloads,
            *key,
            values.to_vec(),
            time,
            diff,
        ))
    }
}
