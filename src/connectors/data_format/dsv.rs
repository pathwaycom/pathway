// Copyright © 2026 Pathway

use std::clone::Clone;
use std::collections::HashMap;
use std::io::Write as _;

use crate::connectors::metadata::SourceMetadata;
use crate::connectors::ReaderContext::{
    Bson, CsvRecord, Diff, Empty, KeyValue, RawBytes, TokenizedEntries,
};
use crate::connectors::{DataEventType, ReaderContext};
use crate::connectors::{SPECIAL_FIELD_DIFF, SPECIAL_FIELD_TIME};
use crate::engine::{Key, Result, Timestamp, Value};

use base64::Engine;
use serde_json::Value as JsonValue;

use super::{
    create_bincoded_value, ensure_all_fields_in_schema, parse_with_type, serialize_value_to_json,
    Formatter, FormatterContext, FormatterError, InnerSchemaField, ParseError, ParseResult,
    ParsedEventWithErrors, Parser, ValueFieldsWithErrors, COMMIT_LITERAL, METADATA_FIELD_NAME,
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
    // Smallest number of tokens a data row must contain so that every column
    // index referenced by the schema is present. Computed once when the header
    // is read, so the per-row check is a single comparison instead of a scan
    // over all requested indices.
    min_tokens_in_row: usize,
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
            min_tokens_in_row: 0,
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

        self.min_tokens_in_row = self
            .key_column_indices
            .iter()
            .flatten()
            .chain(self.value_column_indices.iter())
            .filter_map(|index| match index {
                DsvColumnIndex::IndexWithSchema(index, _) => Some(*index + 1),
                DsvColumnIndex::Metadata => None,
            })
            .max()
            .unwrap_or(0);

        self.header = tokenized_entries.to_vec();
        self.dsv_header_read = true;
        Ok(())
    }

    fn parse_bytes_simple(&mut self, event: DataEventType, raw_bytes: &[u8]) -> ParseResult {
        // Borrow the line straight out of `raw_bytes` instead of allocating an
        // owned, trimmed `String` only to split it apart again.
        let line = std::str::from_utf8(raw_bytes)?.trim();

        if line.is_empty() {
            return Ok(Vec::new());
        }

        if line == COMMIT_LITERAL {
            return Ok(vec![ParsedEventWithErrors::AdvanceTime]);
        }

        let tokens: Vec<&str> = line.split(self.settings.separator).collect();
        self.parse_tokens(event, tokens.len(), |index| tokens[index])
    }

    fn values_by_indices<'a>(
        &self,
        get_token: impl Fn(usize) -> &'a str,
        indices: &[DsvColumnIndex],
    ) -> ValueFieldsWithErrors {
        let mut parsed_tokens = Vec::with_capacity(indices.len());
        for index in indices {
            let token = match index {
                DsvColumnIndex::IndexWithSchema(index, schema_item) => {
                    parse_with_type(get_token(*index), schema_item, &self.header[*index])
                }
                DsvColumnIndex::Metadata => Ok(self.metadata_column_value.clone()),
            };
            parsed_tokens.push(token);
        }
        parsed_tokens
    }

    /// Parse a single tokenized row, reading individual tokens lazily via
    /// `get_token`. This lets callers feed tokens straight from a borrowed
    /// source (e.g. a `csv::StringRecord`) without first materializing an
    /// intermediate `Vec<String>`.
    fn parse_tokens<'a>(
        &mut self,
        event: DataEventType,
        token_count: usize,
        get_token: impl Fn(usize) -> &'a str + Copy,
    ) -> ParseResult {
        if token_count == 1 && get_token(0) == COMMIT_LITERAL {
            return Ok(vec![ParsedEventWithErrors::AdvanceTime]);
        }

        if !self.dsv_header_read {
            let header: Vec<String> = (0..token_count)
                .map(|index| get_token(index).to_string())
                .collect();
            self.parse_dsv_header(&header)?;
            return Ok(Vec::new());
        }

        if token_count < self.min_tokens_in_row {
            return Err(ParseError::UnexpectedNumberOfCsvTokens(token_count).into());
        }

        let key = self.key_column_indices.as_ref().map(|indices| {
            self.values_by_indices(get_token, indices)
                .into_iter()
                .collect()
        });
        let parsed_tokens = self.values_by_indices(get_token, &self.value_column_indices);
        let parsed_entry =
            ParsedEventWithErrors::new(self.session_type(), event, key, parsed_tokens);
        Ok(vec![parsed_entry])
    }

    fn parse_tokenized_entries(&mut self, event: DataEventType, tokens: &[String]) -> ParseResult {
        self.parse_tokens(event, tokens.len(), |index| tokens[index].as_str())
    }

    fn parse_csv_record(
        &mut self,
        event: DataEventType,
        record: &csv::StringRecord,
    ) -> ParseResult {
        self.parse_tokens(event, record.len(), |index| {
            record.get(index).unwrap_or_default()
        })
    }
}

impl Parser for DsvParser {
    fn parse(&mut self, data: &ReaderContext) -> ParseResult {
        match data {
            RawBytes(event, raw_bytes) => self.parse_bytes_simple(*event, raw_bytes),
            TokenizedEntries(event, tokenized_entries) => {
                self.parse_tokenized_entries(*event, tokenized_entries)
            }
            CsvRecord(event, record) => self.parse_csv_record(*event, record),
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
        // The column indices are resolved only once the header line has been
        // read, i.e. after this call, so the request is checked by name.
        let has_metadata_column = self
            .settings
            .value_column_names
            .iter()
            .chain(self.settings.key_column_names.iter().flatten())
            .any(|name| name == METADATA_FIELD_NAME);
        if has_metadata_column {
            let metadata_serialized: JsonValue = metadata.serialize();
            self.metadata_column_value = metadata_serialized.into();
        }
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

    /// Appends `token` with every embedded double quote doubled.
    fn write_escaped(out: &mut Vec<u8>, token: &str) {
        let bytes = token.as_bytes();
        let mut copied_until = 0;
        for (quote_pos, _) in token.match_indices('"') {
            out.extend_from_slice(&bytes[copied_until..quote_pos]);
            out.extend_from_slice(b"\"\"");
            copied_until = quote_pos + 1;
        }
        out.extend_from_slice(&bytes[copied_until..]);
    }

    /// Appends one quoted, escaped field for `v`. Scalars whose textual form
    /// cannot contain a double quote are written directly; the other types go
    /// through their string form as before.
    fn write_quoted_value(out: &mut Vec<u8>, v: &Value) -> Result<(), FormatterError> {
        out.push(b'"');
        match v {
            Value::String(s) => Self::write_escaped(out, s),
            Value::Int(_)
            | Value::Float(_)
            | Value::Bool(_)
            | Value::None
            | Value::Pointer(_)
            | Value::DateTimeNaive(_)
            | Value::DateTimeUtc(_) => {
                write!(out, "{v}").expect("writing to a vector cannot fail");
            }
            Value::Duration(d) => {
                write!(out, "{}", d.nanoseconds()).expect("writing to a vector cannot fail");
            }
            Value::Bytes(b) => {
                Self::write_escaped(out, &base64::engine::general_purpose::STANDARD.encode(b));
            }
            Value::PyObjectWrapper(_) => Self::write_escaped(out, &create_bincoded_value(v)?),
            Value::IntArray(_) | Value::FloatArray(_) | Value::Tuple(_) => {
                Self::write_escaped(out, &serialize_value_to_json(v)?.to_string());
            }
            _ => Self::write_escaped(out, &v.to_string()),
        }
        out.push(b'"');
        Ok(())
    }

    fn format_csv_row(tokens: &[String], separator: u8) -> Vec<u8> {
        // Mirrors `csv`'s `QuoteStyle::Always` with the default double-quote
        // escaping: every field is wrapped in double quotes and any embedded
        // double quote is doubled. Doing it by hand avoids constructing a
        // `csv::Writer` (with its internal buffers) for every single row.
        let escaped_len: usize = tokens
            .iter()
            .map(|token| token.len() + token.matches('"').count())
            .sum();
        let mut out = Vec::with_capacity(escaped_len + 3 * tokens.len());
        for (column, token) in tokens.iter().enumerate() {
            if column > 0 {
                out.push(separator);
            }
            out.push(b'"');
            let bytes = token.as_bytes();
            let mut copied_until = 0;
            for (quote_pos, _) in token.match_indices('"') {
                out.extend_from_slice(&bytes[copied_until..quote_pos]);
                out.extend_from_slice(b"\"\"");
                copied_until = quote_pos + 1;
            }
            out.extend_from_slice(&bytes[copied_until..]);
            out.push(b'"');
        }
        out
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
            payloads.push(Self::format_csv_row(&header, separator));
            self.dsv_header_written = true;
        }

        // The row is written straight into one buffer: every field quoted, an
        // embedded double quote doubled (the same bytes `format_csv_row`
        // produces), without materializing an intermediate `String` per field.
        let mut line = Vec::with_capacity(32 + 16 * values.len());
        for (column, v) in values.iter().enumerate() {
            if column > 0 {
                line.push(separator);
            }
            Self::write_quoted_value(&mut line, v)?;
        }
        if !values.is_empty() {
            line.push(separator);
        }
        write!(line, "\"{time}\"{}\"{diff}\"", separator as char)
            .expect("writing to a vector cannot fail");
        payloads.push(line);

        Ok(FormatterContext::new(
            payloads,
            *key,
            values.to_vec(),
            time,
            diff,
        ))
    }
}
