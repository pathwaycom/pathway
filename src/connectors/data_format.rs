use std::any::type_name;
use std::borrow::Cow;
use std::clone::Clone;
use std::collections::HashMap;
use std::fmt::Display;
use std::io::Write;
use std::iter::zip;
use std::mem::take;
use std::str::{from_utf8, Utf8Error};

use crate::connectors::ReaderContext::{Diff, KeyValue, RawBytes, TokenizedEntries};
use crate::connectors::{DataEventType, ReaderContext};
use crate::engine::error::DynError;
use crate::engine::{Key, Result, Type, Value};

use itertools::Itertools;
use serde::ser::{SerializeMap, Serializer};
use serde_json::json;
use serde_json::Value as JsonValue;

const COMMIT_LITERAL: &str = "*COMMIT*";

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ParsedEvent {
    AdvanceTime,
    Insert((Option<Vec<Value>>, Vec<Value>)),

    // If None, finding the key for the provided values becomes responsibility of the connector
    Delete((Option<Vec<Value>>, Vec<Value>)),
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ParseError {
    #[error("some fields weren't found in the header (fields present in table: {parsed:?}, fields specified in connector: {requested:?})")]
    FieldsNotFoundInHeader {
        parsed: Vec<String>,
        requested: Vec<String>,
    },

    #[error("failed to parse value {value:?} at field {field_name:?} according to the type {type_:?} in schema: {error}")]
    SchemaNotSatisfied {
        value: String,
        field_name: String,
        type_: Type,
        error: DynError,
    },

    #[error("too small number of csv tokens in the line: {0}")]
    UnexpectedNumberOfCsvTokens(usize),

    #[error("failed to parse field {field_name:?} from the following json payload: {payload}")]
    FailedToParseFromJson {
        field_name: String,
        payload: JsonValue,
    },

    #[error("key-value pair has unexpected number of tokens: {0} instead of 2")]
    KeyValueTokensIncorrect(usize),

    #[error("field {field_name} with {} is absent in {payload}", path.clone().map_or("no JsonPointer path specified".to_string(), |path| format!("path {path}")  ))]
    FailedToExtractJsonField {
        field_name: String,
        path: Option<String>,
        payload: JsonValue,
    },

    #[error("received message is not json: {0:?}")]
    FailedToParseJson(String),

    #[error("received message doesn't comply with debezium format: {0}")]
    DebeziumFormatViolated(DebeziumFormatError),

    #[error("unknown debezium operation {0:?}")]
    UnsupportedDebeziumOperation(String),

    #[error("received message doesn't have payload")]
    EmptyKafkaPayload,

    #[error("internal error, reader context is not supported in this parser")]
    UnsupportedReaderContext,

    #[error("received plaintext message is not in utf-8 format: {0}")]
    Utf8DecodeFailed(#[from] Utf8Error),

    #[error("parsing {0:?} from an external datasource is not supported")]
    UnparsableType(Type),
}

#[derive(Debug, thiserror::Error)]
pub enum DebeziumFormatError {
    #[error("incorrect type of payload.op field or it is missing")]
    OperationFieldMissing,

    #[error("there is no payload at the top level of value json")]
    NoPayloadAtTopLevel,

    #[error("the root of the JSON value is neither null nor map")]
    IncorrectJsonRoot,
}

pub type ParseResult = Result<Vec<ParsedEvent>, ParseError>;
type PrepareStringResult = Result<String, ParseError>;

#[derive(Clone, Debug)]
pub struct InnerSchemaField {
    type_: Type,
    default: Option<Value>, // None means that there is no default for the field
}

impl InnerSchemaField {
    pub fn new(type_: Type, default: Option<Value>) -> Self {
        Self { type_, default }
    }
}

impl Default for &InnerSchemaField {
    fn default() -> &'static InnerSchemaField {
        &InnerSchemaField {
            type_: Type::Any,
            default: None,
        }
    }
}

fn prepare_plaintext_string(bytes: &[u8]) -> PrepareStringResult {
    Ok(from_utf8(bytes)?.trim().to_string())
}

pub trait Parser: Send {
    fn parse(&mut self, data: &ReaderContext) -> ParseResult;
    fn on_new_source_started(&mut self);
    fn column_count(&self) -> usize;

    fn short_description(&self) -> Cow<'static, str> {
        type_name::<Self>().into()
    }
}

#[derive(Debug)]
pub struct FormatterContext {
    pub payloads: Vec<Vec<u8>>,
    pub key: Key,
    pub values: Vec<Value>,
}

impl FormatterContext {
    pub fn new(payloads: Vec<Vec<u8>>, key: Key, values: Vec<Value>) -> FormatterContext {
        FormatterContext {
            payloads,
            key,
            values,
        }
    }

    pub fn new_single_payload(payload: Vec<u8>, key: Key, values: Vec<Value>) -> FormatterContext {
        FormatterContext {
            payloads: vec![payload],
            key,
            values,
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum FormatterError {
    #[error("count of value columns and count of values mismatch")]
    ColumnsValuesCountMismatch,

    #[error("value does not fit into data type")]
    ValueDoesNotFit,
}

pub trait Formatter: Send {
    fn format(
        &mut self,
        key: &Key,
        values: &[Value],
        time: u64,
        diff: isize,
    ) -> Result<FormatterContext, FormatterError>;

    fn short_description(&self) -> Cow<'static, str> {
        type_name::<Self>().into()
    }
}

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

    pub fn parser(self, schema: HashMap<String, InnerSchemaField>) -> Box<dyn Parser> {
        Box::new(DsvParser::new(self, schema))
    }
}

pub struct DsvParser {
    settings: DsvSettings,
    schema: HashMap<String, InnerSchemaField>,
    header: Vec<String>,

    key_column_indices: Option<Vec<usize>>,
    value_column_indices: Vec<usize>,
    indexed_schema: HashMap<usize, InnerSchemaField>,
    dsv_header_read: bool,
}

// We don't use `ParseBoolError` because its message only mentions "true" and "false"
// as possible representations. It can be misleading for the user since now we support
// more ways to represent a boolean value in the string.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
enum AdvancedBoolParseError {
    #[error("provided string was not parsable as a boolean value")]
    StringNotParsable,
}

/// Use modern Postgres true/false value names list
/// to parse boolean value from string
/// Related doc: `https://www.postgresql.org/docs/16/datatype-boolean.html`
///
/// We also support "t", "f", "y", "n" as single-letter prefixes
fn parse_bool_advanced(raw_value: &str) -> Result<bool, AdvancedBoolParseError> {
    let raw_value_lowercase = raw_value.trim().to_ascii_lowercase();
    match raw_value_lowercase.as_str() {
        "true" | "yes" | "on" | "1" | "t" | "y" => Ok(true),
        "false" | "no" | "off" | "0" | "f" | "n" => Ok(false),
        _ => Err(AdvancedBoolParseError::StringNotParsable),
    }
}

fn parse_with_type(
    raw_value: &str,
    schema: &InnerSchemaField,
    field_name: &str,
) -> Result<Value, ParseError> {
    if let Some(default) = &schema.default {
        if raw_value.is_empty() && !matches!(schema.type_, Type::Any | Type::String) {
            return Ok(default.clone());
        }
    }

    match schema.type_ {
        Type::Any | Type::String => Ok(Value::from(raw_value)),
        Type::Bool => Ok(Value::Bool(parse_bool_advanced(raw_value).map_err(
            |e| ParseError::SchemaNotSatisfied {
                field_name: field_name.to_string(),
                value: raw_value.to_string(),
                type_: schema.type_,
                error: Box::new(e),
            },
        )?)),
        Type::Int => Ok(Value::Int(raw_value.parse().map_err(|e| {
            ParseError::SchemaNotSatisfied {
                field_name: field_name.to_string(),
                value: raw_value.to_string(),
                type_: schema.type_,
                error: Box::new(e),
            }
        })?)),
        Type::Float => Ok(Value::Float(raw_value.parse().map_err(|e| {
            ParseError::SchemaNotSatisfied {
                field_name: field_name.to_string(),
                value: raw_value.to_string(),
                type_: schema.type_,
                error: Box::new(e),
            }
        })?)),
        Type::Json => {
            let json: JsonValue =
                serde_json::from_str(raw_value).map_err(|e| ParseError::SchemaNotSatisfied {
                    field_name: field_name.to_string(),
                    value: raw_value.to_string(),
                    type_: schema.type_,
                    error: Box::new(e),
                })?;
            Ok(Value::from(json))
        }
        _ => Err(ParseError::UnparsableType(schema.type_)),
    }
}

impl DsvParser {
    pub fn new(settings: DsvSettings, schema: HashMap<String, InnerSchemaField>) -> DsvParser {
        DsvParser {
            settings,
            schema,

            header: Vec::new(),
            key_column_indices: None,
            value_column_indices: Vec::new(),
            indexed_schema: HashMap::new(),
            dsv_header_read: false,
        }
    }

    fn column_indices_by_names(
        tokenized_entries: &[String],
        sought_names: &[String],
    ) -> Result<Vec<usize>, ParseError> {
        let mut value_indices_found = 0;

        let mut column_indices = vec![0; sought_names.len()];
        let mut requested_indices = HashMap::<String, Vec<usize>>::new();
        for (index, field) in sought_names.iter().enumerate() {
            match requested_indices.get_mut(field) {
                Some(indices) => indices.push(index),
                None => {
                    requested_indices.insert(field.clone(), vec![index]);
                }
            };
        }

        for (index, value) in tokenized_entries.iter().enumerate() {
            if let Some(indices) = requested_indices.get(value) {
                for requested_index in indices {
                    column_indices[*requested_index] = index;
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
            Some(names) => Some(Self::column_indices_by_names(tokenized_entries, names)?),
            None => None,
        };
        self.value_column_indices =
            Self::column_indices_by_names(tokenized_entries, &self.settings.value_column_names)?;

        self.indexed_schema = {
            let mut indexed_schema = HashMap::new();
            for (index, item) in tokenized_entries.iter().enumerate() {
                if let Some(schema_item) = self.schema.get(item) {
                    indexed_schema.insert(index, (*schema_item).clone());
                }
            }
            indexed_schema
        };

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
            return Ok(vec![ParsedEvent::AdvanceTime]);
        }

        let tokens: Vec<String> = line
            .split(self.settings.separator)
            .map(std::string::ToString::to_string)
            .collect();
        self.parse_tokenized_entries(event, &tokens)
    }

    fn values_by_indices(
        tokens: &[String],
        indices: &[usize],
        indexed_schema: &HashMap<usize, InnerSchemaField>,
        header: &[String],
    ) -> Result<Vec<Value>, ParseError> {
        let mut parsed_tokens = Vec::with_capacity(indices.len());
        for index in indices {
            let schema_item = indexed_schema.get(index).unwrap_or_default();
            let token = parse_with_type(&tokens[*index], schema_item, &header[*index])?;
            parsed_tokens.push(token);
        }
        Ok(parsed_tokens)
    }

    fn parse_tokenized_entries(&mut self, event: DataEventType, tokens: &[String]) -> ParseResult {
        if tokens.len() == 1 {
            let line = &tokens[0];
            if line == COMMIT_LITERAL {
                return Ok(vec![ParsedEvent::AdvanceTime]);
            }
        }

        if !self.dsv_header_read {
            self.parse_dsv_header(tokens)?;
            return Ok(Vec::new());
        }

        let mut line_has_enough_tokens = true;
        if let Some(indices) = &self.key_column_indices {
            for index in indices {
                line_has_enough_tokens &= index < &tokens.len();
            }
        }
        for index in &self.value_column_indices {
            line_has_enough_tokens &= index < &tokens.len();
        }
        if line_has_enough_tokens {
            let key = match &self.key_column_indices {
                Some(indices) => Some(Self::values_by_indices(
                    tokens,
                    indices,
                    &self.indexed_schema,
                    &self.header,
                )?),
                None => None,
            };
            let parsed_tokens = Self::values_by_indices(
                tokens,
                &self.value_column_indices,
                &self.indexed_schema,
                &self.header,
            )?;
            let parsed_entry = match event {
                DataEventType::Insert => ParsedEvent::Insert((key, parsed_tokens)),
                DataEventType::Delete => ParsedEvent::Delete((key, parsed_tokens)),
            };
            Ok(vec![parsed_entry])
        } else {
            Err(ParseError::UnexpectedNumberOfCsvTokens(tokens.len()))
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
                None => Err(ParseError::EmptyKafkaPayload),
            },
            Diff(_) => Err(ParseError::UnsupportedReaderContext),
        }
    }

    fn on_new_source_started(&mut self) {
        self.dsv_header_read = false;
    }

    fn column_count(&self) -> usize {
        self.settings.value_column_names.len()
    }
}

pub struct IdentityParser {
    parse_utf8: bool,
}

impl IdentityParser {
    pub fn new(parse_utf8: bool) -> IdentityParser {
        Self { parse_utf8 }
    }

    fn prepare_bytes(&self, bytes: &[u8]) -> Result<Value, ParseError> {
        if self.parse_utf8 {
            Ok(Value::String(prepare_plaintext_string(bytes)?.into()))
        } else {
            Ok(Value::Bytes(bytes.into()))
        }
    }
}

impl Parser for IdentityParser {
    fn parse(&mut self, data: &ReaderContext) -> ParseResult {
        let (event, key, value) = match data {
            RawBytes(event, raw_bytes) => (*event, None, self.prepare_bytes(raw_bytes)?),
            KeyValue((_key, value)) => match value {
                Some(bytes) => (DataEventType::Insert, None, self.prepare_bytes(bytes)?),
                None => return Err(ParseError::EmptyKafkaPayload),
            },
            Diff((addition, key, values)) => (
                *addition,
                key.as_ref().map(|k| vec![k.clone()]),
                self.prepare_bytes(values)?,
            ),
            TokenizedEntries(_, _) => return Err(ParseError::UnsupportedReaderContext),
        };

        let is_commit = {
            if let Value::String(arc_str) = &value {
                arc_str.as_str() == COMMIT_LITERAL
            } else {
                false
            }
        };

        let event = if is_commit {
            ParsedEvent::AdvanceTime
        } else {
            let values = vec![value];
            match event {
                DataEventType::Insert => ParsedEvent::Insert((key, values)),
                DataEventType::Delete => ParsedEvent::Delete((key, values)),
            }
        };

        Ok(vec![event])
    }

    fn on_new_source_started(&mut self) {}
    fn column_count(&self) -> usize {
        1
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
}

impl Formatter for DsvFormatter {
    fn format(
        &mut self,
        key: &Key,
        values: &[Value],
        time: u64,
        diff: isize,
    ) -> Result<FormatterContext, FormatterError> {
        if values.len() != self.settings.value_column_names.len() {
            return Err(FormatterError::ColumnsValuesCountMismatch);
        }

        let mut sep_buf = [0; 4];
        let sep = self.settings.separator.encode_utf8(&mut sep_buf);

        let mut payloads = Vec::with_capacity(2);
        if !self.dsv_header_written {
            let mut header = Vec::new();
            write!(
                &mut header,
                "{}",
                self.settings
                    .value_column_names
                    .iter()
                    .map(String::as_str)
                    .chain(["time", "diff"])
                    .format(sep)
            )
            .expect("writing to vector should not fail");
            payloads.push(header);

            self.dsv_header_written = true;
        }

        let mut line = Vec::new();
        write!(
            &mut line,
            "{}",
            values
                .iter()
                .map(|v| v as &dyn Display)
                .chain([&time, &diff] as [&dyn Display; 2])
                .format(sep)
        )
        .unwrap();
        payloads.push(line);

        Ok(FormatterContext::new(payloads, *key, Vec::new()))
    }
}

pub struct DebeziumMessageParser {
    key_field_names: Option<Vec<String>>,
    value_field_names: Vec<String>,
    separator: String, // how key-value pair is separated
}

fn parse_value_from_json(value: &JsonValue) -> Option<Value> {
    match value {
        JsonValue::Null => Some(Value::None),
        JsonValue::String(s) => Some(Value::from(s.as_str())),
        JsonValue::Number(v) => {
            if let Some(parsed_u64) = v.as_u64() {
                Some(Value::Int(parsed_u64.try_into().unwrap()))
            } else if let Some(parsed_i64) = v.as_i64() {
                Some(Value::Int(parsed_i64))
            } else {
                v.as_f64()
                    .map(|parsed_float| Value::Float(ordered_float::OrderedFloat(parsed_float)))
            }
        }
        JsonValue::Bool(v) => Some(Value::Bool(*v)),
        JsonValue::Array(v) => {
            let mut tuple = Vec::with_capacity(v.len());
            for item in v {
                tuple.push(parse_value_from_json(item)?);
            }
            Some(Value::Tuple(tuple.into()))
        }
        JsonValue::Object(_) => None,
    }
}

fn serialize_value_to_json(value: &Value) -> Result<JsonValue, FormatterError> {
    match value {
        Value::None => Ok(JsonValue::Null),
        Value::Int(i) => Ok(json!(i)),
        Value::Float(f) => Ok(json!(f)),
        Value::Bool(b) => Ok(json!(b)),
        Value::String(s) => Ok(json!(s)),
        Value::Pointer(p) => Ok(json!(p.to_string())),
        Value::Tuple(t) => {
            let mut items = Vec::with_capacity(t.len());
            for item in t.iter() {
                items.push(serialize_value_to_json(item)?);
            }
            Ok(JsonValue::Array(items))
        }
        Value::Bytes(b) => {
            let mut items = Vec::with_capacity(b.len());
            for item in b.iter() {
                items.push(json!(item));
            }
            Ok(JsonValue::Array(items))
        }
        Value::IntArray(a) => {
            let mut items = Vec::with_capacity(a.len());
            for item in a.iter() {
                items.push(json!(item));
            }
            Ok(JsonValue::Array(items))
        }
        Value::FloatArray(a) => {
            let mut items = Vec::with_capacity(a.len());
            for item in a.iter() {
                items.push(json!(item));
            }
            Ok(JsonValue::Array(items))
        }
        Value::DateTimeNaive(dt) => Ok(json!(dt.to_string())),
        Value::DateTimeUtc(dt) => Ok(json!(dt.to_string())),
        Value::Duration(d) => Ok(json!(d.nanoseconds())),
        Value::Json(j) => Ok((**j).clone()),
    }
}

fn values_by_names_from_json(
    payload: &JsonValue,
    field_names: &[String],
    column_paths: &HashMap<String, String>,
    field_absence_is_error: bool,
    schema: &HashMap<String, InnerSchemaField>,
) -> Result<Vec<Value>, ParseError> {
    let mut parsed_values = Vec::with_capacity(field_names.len());
    for value_field in field_names {
        let (default_value, dtype) = {
            if let Some(schema_item) = schema.get(value_field) {
                if let Some(default) = &schema_item.default {
                    (Some(default), schema_item.type_)
                } else {
                    (None, schema_item.type_)
                }
            } else {
                (None, Type::Any)
            }
        };

        let value = if let Some(path) = column_paths.get(value_field) {
            if let Some(value) = payload.pointer(path) {
                match dtype {
                    Type::Json => Value::from(value.clone()),
                    _ => parse_value_from_json(value).ok_or_else(|| {
                        ParseError::FailedToParseFromJson {
                            field_name: value_field.to_string(),
                            payload: value.clone(),
                        }
                    })?,
                }
            } else if let Some(default) = default_value {
                default.clone()
            } else if field_absence_is_error {
                return Err(ParseError::FailedToExtractJsonField {
                    field_name: value_field.to_string(),
                    path: Some(path.to_string()),
                    payload: payload.clone(),
                });
            } else {
                Value::None
            }
        } else {
            let value_specified_in_json = payload.get(value_field).is_some();

            if value_specified_in_json {
                match dtype {
                    Type::Json => Value::from(payload[&value_field].clone()),
                    _ => parse_value_from_json(&payload[&value_field]).ok_or_else(|| {
                        ParseError::FailedToParseFromJson {
                            field_name: value_field.to_string(),
                            payload: payload[&value_field].clone(),
                        }
                    })?,
                }
            } else if let Some(default) = default_value {
                default.clone()
            } else if field_absence_is_error {
                return Err(ParseError::FailedToExtractJsonField {
                    field_name: value_field.to_string(),
                    path: None,
                    payload: payload.clone(),
                });
            } else {
                Value::None
            }
        };
        parsed_values.push(value);
    }
    Ok(parsed_values)
}

impl DebeziumMessageParser {
    pub fn new(
        key_field_names: Option<Vec<String>>,
        value_field_names: Vec<String>,
        separator: String,
    ) -> DebeziumMessageParser {
        DebeziumMessageParser {
            key_field_names,
            value_field_names,
            separator,
        }
    }

    pub fn standard_separator() -> String {
        "        ".to_string()
    }

    fn parse_event(
        &mut self,
        value: &JsonValue,
        event: DataEventType,
    ) -> Result<ParsedEvent, ParseError> {
        let key = match &self.key_field_names {
            None => None,
            Some(names) => Some(values_by_names_from_json(
                value,
                names,
                &HashMap::new(),
                true,
                &HashMap::new(),
            )?),
        };

        let parsed_values = values_by_names_from_json(
            value,
            &self.value_field_names,
            &HashMap::new(),
            true,
            &HashMap::new(),
        )?;

        match event {
            DataEventType::Insert => Ok(ParsedEvent::Insert((key, parsed_values))),
            DataEventType::Delete => Ok(ParsedEvent::Delete((key, parsed_values))),
        }
    }

    fn parse_read_or_create(&mut self, value: &JsonValue) -> ParseResult {
        Ok(vec![
            self.parse_event(&value["after"], DataEventType::Insert)?
        ])
    }

    fn parse_delete(&mut self, value: &JsonValue) -> ParseResult {
        Ok(vec![
            self.parse_event(&value["before"], DataEventType::Delete)?
        ])
    }

    fn parse_update(&mut self, value: &JsonValue) -> ParseResult {
        let event_before = self.parse_event(&value["before"], DataEventType::Delete)?;
        let event_after = self.parse_event(&value["after"], DataEventType::Insert)?;

        Ok(vec![event_before, event_after])
    }
}

impl Parser for DebeziumMessageParser {
    fn parse(&mut self, data: &ReaderContext) -> ParseResult {
        let raw_value_change = match data {
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
                    return Err(ParseError::KeyValueTokensIncorrect(key_and_value.len()));
                }
                key_and_value[1].to_string()
            }
            KeyValue((_k, v)) => match v {
                Some(bytes) => prepare_plaintext_string(bytes)?,
                None => return Err(ParseError::EmptyKafkaPayload),
            },
            Diff(_) | TokenizedEntries(_, _) => {
                return Err(ParseError::UnsupportedReaderContext);
            }
        };

        let Ok(value_change) = serde_json::from_str(&raw_value_change) else {
            return Err(ParseError::FailedToParseJson(raw_value_change));
        };

        let change_payload = match value_change {
            JsonValue::Object(payload_value) => payload_value,
            JsonValue::Null => return Ok(Vec::new()),
            _ => {
                return Err(ParseError::DebeziumFormatViolated(
                    DebeziumFormatError::IncorrectJsonRoot,
                ))
            }
        };

        if !change_payload.contains_key("payload") {
            return Err(ParseError::DebeziumFormatViolated(
                DebeziumFormatError::NoPayloadAtTopLevel,
            ));
        }

        match &change_payload["payload"]["op"] {
            JsonValue::String(op) => match op.as_ref() {
                "r" | "c" => self.parse_read_or_create(&change_payload["payload"]),
                "u" => self.parse_update(&change_payload["payload"]),
                "d" => self.parse_delete(&change_payload["payload"]),
                _ => Err(ParseError::UnsupportedDebeziumOperation(op.to_string())),
            },
            _ => Err(ParseError::DebeziumFormatViolated(
                DebeziumFormatError::OperationFieldMissing,
            )),
        }
    }

    fn on_new_source_started(&mut self) {}

    fn column_count(&self) -> usize {
        self.value_field_names.len()
    }
}

pub struct JsonLinesParser {
    key_field_names: Option<Vec<String>>,
    value_field_names: Vec<String>,
    column_paths: HashMap<String, String>,
    field_absence_is_error: bool,
    schema: HashMap<String, InnerSchemaField>,
}

impl JsonLinesParser {
    pub fn new(
        key_field_names: Option<Vec<String>>,
        value_field_names: Vec<String>,
        column_paths: HashMap<String, String>,
        field_absence_is_error: bool,
        schema: HashMap<String, InnerSchemaField>,
    ) -> JsonLinesParser {
        JsonLinesParser {
            key_field_names,
            value_field_names,
            column_paths,
            field_absence_is_error,
            schema,
        }
    }
}

impl Parser for JsonLinesParser {
    fn parse(&mut self, data: &ReaderContext) -> ParseResult {
        let (data_event, key, line) = match data {
            RawBytes(event, line) => {
                let line = prepare_plaintext_string(line)?;
                (*event, None, line)
            }
            KeyValue((_key, value)) => {
                if let Some(line) = value {
                    let line = prepare_plaintext_string(line)?;
                    (DataEventType::Insert, None, line)
                } else {
                    return Err(ParseError::EmptyKafkaPayload);
                }
            }
            Diff((event, key, line)) => {
                let line = prepare_plaintext_string(line)?;
                let key = key.as_ref().map(|k| vec![k.clone()]);
                (*event, key, line)
            }
            TokenizedEntries(..) => {
                return Err(ParseError::UnsupportedReaderContext);
            }
        };

        if line.is_empty() {
            return Ok(vec![]);
        }

        if line == COMMIT_LITERAL {
            return Ok(vec![ParsedEvent::AdvanceTime]);
        }

        let payload: JsonValue = match serde_json::from_str(&line) {
            Ok(json_value) => json_value,
            Err(_) => return Err(ParseError::FailedToParseJson(line)),
        };

        let key = key.or(match &self.key_field_names {
            Some(key_field_names) => Some(values_by_names_from_json(
                &payload,
                key_field_names,
                &self.column_paths,
                self.field_absence_is_error,
                &self.schema,
            )?),
            None => None, // use method from the different PR
        });

        let values = values_by_names_from_json(
            &payload,
            &self.value_field_names,
            &self.column_paths,
            self.field_absence_is_error,
            &self.schema,
        )?;

        let event = match data_event {
            DataEventType::Insert => ParsedEvent::Insert((key, values)),
            DataEventType::Delete => ParsedEvent::Delete((key, values)),
        };

        Ok(vec![event])
    }

    fn on_new_source_started(&mut self) {}

    fn column_count(&self) -> usize {
        self.value_field_names.len()
    }
}

#[derive(Debug)]
pub struct PsqlUpdatesFormatter {
    table_name: String,
    value_field_names: Vec<String>,
}

impl PsqlUpdatesFormatter {
    pub fn new(table_name: String, value_field_names: Vec<String>) -> PsqlUpdatesFormatter {
        PsqlUpdatesFormatter {
            table_name,
            value_field_names,
        }
    }
}

impl Formatter for PsqlUpdatesFormatter {
    fn format(
        &mut self,
        key: &Key,
        values: &[Value],
        time: u64,
        diff: isize,
    ) -> Result<FormatterContext, FormatterError> {
        if values.len() != self.value_field_names.len() {
            return Err(FormatterError::ColumnsValuesCountMismatch);
        }

        let mut result = Vec::new();
        writeln!(
            result,
            "INSERT INTO {} ({},time,diff) VALUES ({},{},{})",
            self.table_name,
            self.value_field_names.iter().join(","),
            (1..=values.len()).format_with(",", |x, f| f(&format_args!("${x}"))),
            time,
            diff
        )
        .unwrap();

        Ok(FormatterContext::new_single_payload(
            result,
            *key,
            values.to_vec(),
        ))
    }
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum PsqlSnapshotFormatterError {
    #[error("repeated value field {0:?}")]
    RepeatedValueField(String),

    #[error("unknown key field {0:?}")]
    UnknownKey(String),
}

#[derive(Debug)]
pub struct PsqlSnapshotFormatter {
    table_name: String,
    key_field_names: Vec<String>,
    value_field_names: Vec<String>,

    key_field_positions: Vec<usize>,
    value_field_positions: Vec<usize>,
}

impl PsqlSnapshotFormatter {
    pub fn new(
        table_name: String,
        mut key_field_names: Vec<String>,
        mut value_field_names: Vec<String>,
    ) -> Result<PsqlSnapshotFormatter, PsqlSnapshotFormatterError> {
        let mut field_positions = HashMap::<String, usize>::new();
        for (index, field_name) in value_field_names.iter_mut().enumerate() {
            if field_positions.contains_key(field_name) {
                return Err(PsqlSnapshotFormatterError::RepeatedValueField(take(
                    field_name,
                )));
            }
            field_positions.insert(field_name.clone(), index);
        }

        let mut key_field_positions = Vec::with_capacity(key_field_names.len());
        for key_field_name in &mut key_field_names {
            let position = field_positions
                .remove(key_field_name)
                .ok_or_else(|| PsqlSnapshotFormatterError::UnknownKey(take(key_field_name)))?;
            key_field_positions.push(position);
        }

        let mut value_field_positions: Vec<_> = field_positions.into_values().collect();

        key_field_positions.sort_unstable();
        value_field_positions.sort_unstable();
        Ok(PsqlSnapshotFormatter {
            table_name,
            key_field_names,
            value_field_names,

            key_field_positions,
            value_field_positions,
        })
    }
}

impl Formatter for PsqlSnapshotFormatter {
    fn format(
        &mut self,
        key: &Key,
        values: &[Value],
        time: u64,
        diff: isize,
    ) -> Result<FormatterContext, FormatterError> {
        if values.len() != self.value_field_names.len() {
            return Err(FormatterError::ColumnsValuesCountMismatch);
        }

        let mut result = Vec::new();

        let update_condition = self
            .key_field_positions
            .iter()
            .map(|position| {
                format!(
                    "{}.{}=${}",
                    self.table_name,
                    self.value_field_names[*position],
                    *position + 1
                )
            })
            .join(" AND ");

        let update_pairs = self
            .value_field_positions
            .iter()
            .map(|position| format!("{}=${}", self.value_field_names[*position], *position + 1))
            .join(",");

        writeln!(
            result,
            "INSERT INTO {} ({},time,diff) VALUES ({},{},{}) ON CONFLICT ({}) DO UPDATE SET {},time={},diff={} WHERE {} AND ({}.time<{} OR ({}.time={} AND {}.diff=-1))",
            self.table_name,  // INSERT INTO ...
            self.value_field_names.iter().format(","),  // (...
            (1..=values.len()).format_with(",", |x, f| f(&format_args!("${x}"))),  // VALUES(...
            time,  // VALUES(..., time
            diff,  // VALUES(..., time, diff
            self.key_field_names.iter().join(","),  // ON CONFLICT(...
            update_pairs,  // DO UPDATE SET ...
            time,
            diff,
            update_condition,  // WHERE ...
            self.table_name,  // AND ...time
            time,  // .time < ...
            self.table_name,  // OR (...time
            time,  // .time=...
            self.table_name,  // AND ...diff=-1))
        )
        .unwrap();

        Ok(FormatterContext::new_single_payload(
            result,
            *key,
            values.to_vec(),
        ))
    }
}

#[derive(Debug)]
pub struct JsonLinesFormatter {
    value_field_names: Vec<String>,
}

impl JsonLinesFormatter {
    pub fn new(value_field_names: Vec<String>) -> JsonLinesFormatter {
        JsonLinesFormatter { value_field_names }
    }
}

impl Formatter for JsonLinesFormatter {
    fn format(
        &mut self,
        key: &Key,
        values: &[Value],
        time: u64,
        diff: isize,
    ) -> Result<FormatterContext, FormatterError> {
        let mut serializer = serde_json::Serializer::new(Vec::<u8>::new());
        let mut map = serializer
            .serialize_map(Some(self.value_field_names.len() + 2))
            .unwrap();
        for (key, value) in zip(self.value_field_names.iter(), values) {
            map.serialize_entry(key, &serialize_value_to_json(value)?)
                .unwrap();
        }
        map.serialize_entry("diff", &diff).unwrap();
        map.serialize_entry("time", &time).unwrap();
        map.end().unwrap();

        Ok(FormatterContext::new_single_payload(
            serializer.into_inner(),
            *key,
            Vec::new(),
        ))
    }
}

pub struct NullFormatter {}

impl NullFormatter {
    pub fn new() -> NullFormatter {
        NullFormatter {}
    }
}

impl Default for NullFormatter {
    fn default() -> Self {
        Self::new()
    }
}

impl Formatter for NullFormatter {
    fn format(
        &mut self,
        key: &Key,
        _values: &[Value],
        _time: u64,
        _diff: isize,
    ) -> Result<FormatterContext, FormatterError> {
        Ok(FormatterContext::new(Vec::new(), *key, Vec::new()))
    }
}
