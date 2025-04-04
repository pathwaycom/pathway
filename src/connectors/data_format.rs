// Copyright © 2024 Pathway

use std::any::type_name;
use std::borrow::Cow;
use std::clone::Clone;
use std::collections::HashMap;
use std::io::Write;
use std::iter::zip;
use std::mem::take;
use std::str::{from_utf8, Utf8Error};

use crate::connectors::metadata::SourceMetadata;
use crate::connectors::ReaderContext::{Diff, Empty, KeyValue, RawBytes, TokenizedEntries};
use crate::connectors::{DataEventType, Offset, ReaderContext, SessionType, SnapshotEvent};
use crate::engine::error::{limit_length, DynError, DynResult, STANDARD_OBJECT_LENGTH_LIMIT};
use crate::engine::time::DateTime;
use crate::engine::{
    value::parse_pathway_pointer, DateTimeNaive, DateTimeUtc, Duration as EngineDuration, Error,
    Key, Result, Timestamp, Type, Value,
};

use async_nats::header::HeaderMap as NatsHeaders;
use base64::engine::general_purpose::STANDARD as base64encoder;
use base64::Engine;
use bincode::ErrorKind as BincodeError;
use itertools::{chain, Itertools};
use log::error;
use mongodb::bson::{
    bson, spec::BinarySubtype as BsonBinarySubtype, Binary as BsonBinaryContents,
    Bson as BsonValue, DateTime as BsonDateTime, Document as BsonDocument,
};
use ndarray::ArrayD;
use rdkafka::message::{Header as KafkaHeader, OwnedHeaders as KafkaHeaders};
use serde::ser::{SerializeMap, Serializer};
use serde_json::json;
use serde_json::{Map as JsonMap, Value as JsonValue};

use super::data_storage::{ConversionError, SpecialEvent};

pub const COMMIT_LITERAL: &str = "*COMMIT*";
const DEBEZIUM_EMPTY_KEY_PAYLOAD: &str = "{\"payload\": {\"before\": {}, \"after\": {}}}";

fn is_commit_literal(value: &DynResult<Value>) -> bool {
    match value {
        Ok(Value::String(arc_str)) => arc_str.as_str() == COMMIT_LITERAL,
        Ok(Value::Bytes(arc_bytes)) => **arc_bytes == *COMMIT_LITERAL.as_bytes(),
        _ => false,
    }
}

pub type KeyFieldsWithErrors = Option<DynResult<Vec<Value>>>;
pub type ValueFieldsWithErrors = Vec<DynResult<Value>>;
pub type ErrorRemovalLogic = Box<dyn Fn(ValueFieldsWithErrors) -> DynResult<Vec<Value>>>;

#[derive(Debug)]
pub enum ParsedEventWithErrors {
    AdvanceTime,
    Insert((KeyFieldsWithErrors, ValueFieldsWithErrors)),
    // If None, finding the key for the provided values becomes responsibility of the connector
    Delete((KeyFieldsWithErrors, ValueFieldsWithErrors)),
}

impl ParsedEventWithErrors {
    pub fn new(
        session_type: SessionType,
        data_event_type: DataEventType,
        key: KeyFieldsWithErrors,
        values: ValueFieldsWithErrors,
    ) -> Self {
        match data_event_type {
            DataEventType::Insert => ParsedEventWithErrors::Insert((key, values)),
            DataEventType::Delete => match session_type {
                SessionType::Native => ParsedEventWithErrors::Delete((key, values)),
                SessionType::Upsert => ParsedEventWithErrors::Delete((key, vec![])),
            },
        }
    }
    pub fn remove_errors(self, logic: &ErrorRemovalLogic) -> DynResult<ParsedEvent> {
        match self {
            Self::AdvanceTime => Ok(ParsedEvent::AdvanceTime),
            Self::Insert((key, values)) => key
                .transpose()
                .and_then(|key| Ok(ParsedEvent::Insert((key, logic(values)?)))),
            Self::Delete((key, values)) => key
                .transpose()
                .and_then(|key| Ok(ParsedEvent::Delete((key, logic(values)?)))),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ParsedEvent {
    AdvanceTime,
    Insert((Option<Vec<Value>>, Vec<Value>)),
    // If None, finding the key for the provided values becomes responsibility of the connector
    Delete((Option<Vec<Value>>, Vec<Value>)),
}

impl ParsedEvent {
    pub fn key(
        &self,
        mut values_to_key: impl FnMut(Option<&Vec<Value>>, Option<&Offset>) -> Key,
        offset: Option<&Offset>,
    ) -> Option<Key> {
        match self {
            ParsedEvent::Insert((raw_key, _)) | ParsedEvent::Delete((raw_key, _)) => {
                Some(values_to_key(raw_key.as_ref(), offset))
            }
            ParsedEvent::AdvanceTime => None,
        }
    }

    pub fn snapshot_event(&self, key: Key) -> Option<SnapshotEvent> {
        match self {
            ParsedEvent::Insert((_, values)) => Some(SnapshotEvent::Insert(key, values.clone())),
            ParsedEvent::Delete((_, values)) => Some(SnapshotEvent::Delete(key, values.clone())),
            ParsedEvent::AdvanceTime => None,
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ParseError {
    #[error("some fields weren't found in the header (fields present in table: {parsed:?}, fields specified in connector: {requested:?})")]
    FieldsNotFoundInHeader {
        parsed: Vec<String>,
        requested: Vec<String>,
    },

    #[error("failed to parse value {} at field {field_name:?} according to the type {type_} in schema: {error}", limit_length(format!("{value:?}"), STANDARD_OBJECT_LENGTH_LIMIT))]
    SchemaNotSatisfied {
        value: String,
        field_name: String,
        type_: Type,
        error: DynError,
    },

    #[error("too small number of csv tokens in the line: {0}")]
    UnexpectedNumberOfCsvTokens(usize),

    #[error("failed to create a field {field_name:?} with type {type_} from json payload: {}", limit_length(format!("{payload}"), STANDARD_OBJECT_LENGTH_LIMIT))]
    FailedToParseFromJson {
        field_name: String,
        payload: JsonValue,
        type_: Type,
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

    #[error("received metadata payload is not a valid json")]
    FailedToParseMetadata,

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

    #[error("parsing {0} from an external datasource is not supported")]
    UnparsableType(Type),

    #[error("error in primary key, skipping the row: {0}")]
    ErrorInKey(DynError),

    #[error("no value for {field_name:?} field and no default specified")]
    NoDefault { field_name: String },

    #[error(transparent)]
    Bincode(#[from] BincodeError),

    #[error(transparent)]
    Base64(#[from] base64::DecodeError),

    #[error("malformed complex field JSON representation")]
    MalformedComplexField,
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

impl From<DynError> for ParseError {
    fn from(value: DynError) -> Self {
        *value
            .downcast::<Self>()
            .expect("error should be ParseError")
    }
}

pub type ParseResult = DynResult<Vec<ParsedEventWithErrors>>;
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

    pub fn maybe_use_default(
        &self,
        name: &str,
        value: Option<Result<Value, Box<ConversionError>>>,
    ) -> DynResult<Value> {
        match value {
            Some(value) => Ok(value?),
            None => self.default.clone().ok_or(
                ParseError::NoDefault {
                    field_name: name.to_string(),
                }
                .into(),
            ),
        }
    }
}

fn prepare_plaintext_string(bytes: &[u8]) -> PrepareStringResult {
    Ok(from_utf8(bytes)?.trim().to_string())
}

pub trait Parser: Send {
    fn parse(&mut self, data: &ReaderContext) -> ParseResult;
    fn on_new_source_started(&mut self, metadata: &SourceMetadata);
    fn column_count(&self) -> usize;

    fn short_description(&self) -> Cow<'static, str> {
        type_name::<Self>().into()
    }

    fn session_type(&self) -> SessionType {
        SessionType::Native
    }
}

#[derive(Debug, Clone)]
pub struct PreparedMessageHeader {
    key: String,
    value: Vec<u8>,
}

impl PreparedMessageHeader {
    pub fn new(key: impl Into<String>, value: Vec<u8>) -> Self {
        Self {
            key: key.into(),
            value,
        }
    }
}

#[derive(Clone, Debug)]
pub enum FormattedDocument {
    RawBytes(Vec<u8>),
    Bson(BsonDocument),
}

impl FormattedDocument {
    pub fn into_raw_bytes(self) -> Result<Vec<u8>, FormatterError> {
        if let Self::RawBytes(b) = self {
            Ok(b)
        } else {
            Err(FormatterError::UnexpectedContextType)
        }
    }

    pub fn into_bson_document(self) -> Result<BsonDocument, FormatterError> {
        if let Self::Bson(b) = self {
            Ok(b)
        } else {
            Err(FormatterError::UnexpectedContextType)
        }
    }
}

impl From<Vec<u8>> for FormattedDocument {
    fn from(payload: Vec<u8>) -> FormattedDocument {
        FormattedDocument::RawBytes(payload)
    }
}

impl From<BsonDocument> for FormattedDocument {
    fn from(payload: BsonDocument) -> FormattedDocument {
        FormattedDocument::Bson(payload)
    }
}

#[derive(Debug)]
pub struct FormatterContext {
    pub payloads: Vec<FormattedDocument>,
    pub key: Key,
    pub values: Vec<Value>,
    pub time: Timestamp,
    pub diff: isize,
}

impl FormatterContext {
    pub fn new(
        payloads: Vec<impl Into<FormattedDocument>>,
        key: Key,
        values: Vec<Value>,
        time: Timestamp,
        diff: isize,
    ) -> FormatterContext {
        let mut prepared_payloads: Vec<FormattedDocument> = Vec::with_capacity(payloads.len());
        for payload in payloads {
            prepared_payloads.push(payload.into());
        }
        FormatterContext {
            payloads: prepared_payloads,
            key,
            values,
            time,
            diff,
        }
    }

    pub fn new_single_payload(
        payload: impl Into<FormattedDocument>,
        key: Key,
        values: Vec<Value>,
        time: Timestamp,
        diff: isize,
    ) -> FormatterContext {
        FormatterContext {
            payloads: vec![payload.into()],
            key,
            values,
            time,
            diff,
        }
    }

    fn construct_message_headers(
        &self,
        header_fields: &Vec<(String, usize)>,
        encode_bytes: bool,
    ) -> Vec<PreparedMessageHeader> {
        let mut headers = Vec::with_capacity(header_fields.len() + 2);
        headers.push(PreparedMessageHeader::new(
            "pathway_time",
            self.time.to_string().as_bytes().to_vec(),
        ));
        headers.push(PreparedMessageHeader::new(
            "pathway_diff",
            self.diff.to_string().as_bytes().to_vec(),
        ));
        for (name, position) in header_fields {
            let value: Vec<u8> = match (&self.values[*position], encode_bytes) {
                (Value::Bytes(b), false) => (*b).to_vec(),
                (Value::Bytes(b), true) => base64encoder.encode(b).into(),
                (other, _) => (*other.to_string().as_bytes()).to_vec(),
            };
            headers.push(PreparedMessageHeader::new(name, value));
        }
        headers
    }

    pub fn construct_kafka_headers(&self, header_fields: &Vec<(String, usize)>) -> KafkaHeaders {
        let raw_headers = self.construct_message_headers(header_fields, false);
        let mut kafka_headers = KafkaHeaders::new_with_capacity(raw_headers.len());
        for header in raw_headers {
            kafka_headers = kafka_headers.insert(KafkaHeader {
                key: &header.key,
                value: Some(&header.value),
            });
        }
        kafka_headers
    }

    pub fn construct_nats_headers(&self, header_fields: &Vec<(String, usize)>) -> NatsHeaders {
        let raw_headers = self.construct_message_headers(header_fields, true);
        let mut nats_headers = NatsHeaders::new();
        for header in raw_headers {
            nats_headers.insert(
                header.key,
                String::from_utf8(header.value)
                    .expect("all prepared headers must be UTF-8 serializable"),
            );
        }
        nats_headers
    }
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum FormatterError {
    #[error("count of value columns and count of values mismatch")]
    ColumnsValuesCountMismatch,

    #[error("incorrect column index")]
    IncorrectColumnIndex,

    #[error("type {type_:?} is not bson-serializable")]
    TypeNonBsonSerializable { type_: Type },

    #[error("Error value is not json-serializable")]
    ErrorValueNonJsonSerializable,

    #[error("Error value is not bson-serializable")]
    ErrorValueNonBsonSerializable,

    #[error("Pending value is not json-serializable")]
    PendingValueNonJsonSerializable,

    #[error("Pending value is not bson-serializable")]
    PendingValueNonBsonSerializable,

    #[error("this connector doesn't support this value type")]
    UnsupportedValueType,

    #[error("unexpected formatter context type")]
    UnexpectedContextType,

    #[error(transparent)]
    Bincode(#[from] BincodeError),

    #[error(transparent)]
    Csv(#[from] csv::Error),

    #[error("CSV separator must be a 8-bit character, but '{0}' is provided")]
    UnsupportedCsvSeparator(char),
}

pub trait Formatter: Send {
    fn format(
        &mut self,
        key: &Key,
        values: &[Value],
        time: Timestamp,
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

// We don't use `ParseBoolError` because its message only mentions "true" and "false"
// as possible representations. It can be misleading for the user since now we support
// more ways to represent a boolean value in the string.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum AdvancedBoolParseError {
    #[error("provided string was not parsable as a boolean value")]
    StringNotParsable,
}

/// Use modern Postgres true/false value names list
/// to parse boolean value from string
/// Related doc: `https://www.postgresql.org/docs/16/datatype-boolean.html`
///
/// We also support "t", "f", "y", "n" as single-letter prefixes
pub fn parse_bool_advanced(raw_value: &str) -> Result<bool, AdvancedBoolParseError> {
    let raw_value_lowercase = raw_value.trim().to_ascii_lowercase();
    match raw_value_lowercase.as_str() {
        "true" | "yes" | "on" | "1" | "t" | "y" => Ok(true),
        "false" | "no" | "off" | "0" | "f" | "n" => Ok(false),
        _ => Err(AdvancedBoolParseError::StringNotParsable),
    }
}

fn can_represent_null_value(raw_value: &str) -> bool {
    let raw_value_lowercase = raw_value.trim().to_ascii_lowercase();
    matches!(raw_value_lowercase.as_str(), "null" | "none" | "")
}

fn parse_str_with_type(raw_value: &str, type_: &Type) -> Result<Value, DynError> {
    if type_.is_optional() && can_represent_null_value(raw_value) {
        let type_unopt = type_.unoptionalize();
        match type_unopt {
            Type::Bool
            | Type::Int
            | Type::Float
            | Type::PyObjectWrapper
            | Type::Pointer
            | Type::DateTimeUtc
            | Type::DateTimeNaive
            | Type::Duration
            | Type::Array(_, _)
            | Type::List(_)
            | Type::Tuple(_) => return Ok(Value::None),
            // "null" is ambiguous, since it can also correspond to a serialized JSON
            // Anything else can be safely treated as a `Value::None`
            Type::Json if raw_value != "null" => return Ok(Value::None),
            _ => {}
        };
    }
    match type_.unoptionalize() {
        Type::Any | Type::String => Ok(Value::from(raw_value)),
        Type::Bool => Ok(Value::Bool(parse_bool_advanced(raw_value)?)),
        Type::Int => Ok(Value::Int(raw_value.parse()?)),
        Type::Float => Ok(Value::Float(raw_value.parse()?)),
        Type::Json => {
            let json: JsonValue = serde_json::from_str(raw_value)?;
            Ok(Value::from(json))
        }
        Type::PyObjectWrapper => {
            let value = parse_bincoded_value(raw_value)?;
            Ok(value)
        }
        Type::Pointer => {
            let value = parse_pathway_pointer(raw_value)?;
            Ok(value)
        }
        Type::DateTimeUtc => {
            let dt = DateTimeUtc::strptime(raw_value, "%Y-%m-%dT%H:%M:%S%.f%z")?;
            Ok(dt.into())
        }
        Type::DateTimeNaive => {
            let dt = DateTimeNaive::strptime(raw_value, "%Y-%m-%dT%H:%M:%S%.f")?;
            Ok(dt.into())
        }
        Type::Duration => {
            let duration_ns: i64 = raw_value.parse()?;
            let engine_duration = EngineDuration::new_with_unit(duration_ns, "ns")
                .expect("new_with_unit can't fail when 'ns' is used as a unit");
            Ok(engine_duration.into())
        }
        Type::Bytes => {
            let bytes = base64::engine::general_purpose::STANDARD.decode(raw_value)?;
            Ok(Value::Bytes(bytes.into()))
        }
        Type::Array(_, _) | Type::List(_) | Type::Tuple(_) => {
            let json: JsonValue = serde_json::from_str(raw_value)?;
            let value =
                parse_value_from_json(&json, type_).ok_or(ParseError::MalformedComplexField)?;
            Ok(value)
        }
        _ => Err(ParseError::UnparsableType(type_.clone()).into()),
    }
}

fn parse_with_type(
    raw_value: &str,
    schema: &InnerSchemaField,
    field_name: &str,
) -> DynResult<Value> {
    if let Some(default) = &schema.default {
        if raw_value.is_empty() && !matches!(schema.type_.unoptionalize(), Type::Any | Type::String)
        {
            return Ok(default.clone());
        }
    }

    let result = parse_str_with_type(raw_value, &schema.type_);
    Ok(result.map_err(|e| ParseError::SchemaNotSatisfied {
        field_name: field_name.to_string(),
        value: raw_value.to_string(),
        type_: schema.type_.clone(),
        error: e,
    })?)
}

fn ensure_all_fields_in_schema(
    key_column_names: &Option<Vec<String>>,
    value_column_names: &Vec<String>,
    schema: &HashMap<String, InnerSchemaField>,
) -> Result<()> {
    for name in chain!(key_column_names.iter().flatten(), value_column_names) {
        if !schema.contains_key(name) {
            return Err(Error::FieldNotInSchema {
                name: name.clone(),
                schema_keys: schema.keys().cloned().collect(),
            });
        }
    }
    Ok(())
}

/// "magic field" containing the metadata
const METADATA_FIELD_NAME: &str = "_metadata";

impl DsvParser {
    pub fn new(
        settings: DsvSettings,
        schema: HashMap<String, InnerSchemaField>,
    ) -> Result<DsvParser> {
        ensure_all_fields_in_schema(
            &settings.key_column_names,
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
            };
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
            Diff(_) => Err(ParseError::UnsupportedReaderContext.into()),
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

fn value_from_bytes(bytes: &[u8], parse_utf8: bool) -> DynResult<Value> {
    if parse_utf8 {
        Ok(Value::String(prepare_plaintext_string(bytes)?.into()))
    } else {
        Ok(Value::Bytes(bytes.into()))
    }
}

#[derive(Clone, Copy, Debug)]
pub enum KeyGenerationPolicy {
    AlwaysAutogenerate,
    PreferMessageKey,
}

impl KeyGenerationPolicy {
    fn generate(self, key: &Option<Vec<u8>>, parse_utf8: bool) -> Option<DynResult<Vec<Value>>> {
        match &self {
            Self::AlwaysAutogenerate => None,
            Self::PreferMessageKey => key
                .as_ref()
                .map(|bytes| value_from_bytes(bytes, parse_utf8).map(|k| vec![k])),
        }
    }
}

pub struct IdentityParser {
    value_fields: Vec<String>,
    parse_utf8: bool,
    metadata_column_value: Value,
    session_type: SessionType,
    key_generation_policy: KeyGenerationPolicy,
}

impl IdentityParser {
    pub fn new(
        value_fields: Vec<String>,
        parse_utf8: bool,
        key_generation_policy: KeyGenerationPolicy,
        session_type: SessionType,
    ) -> IdentityParser {
        Self {
            value_fields,
            parse_utf8,
            metadata_column_value: Value::None,
            key_generation_policy,
            session_type,
        }
    }
}

impl Parser for IdentityParser {
    fn parse(&mut self, data: &ReaderContext) -> ParseResult {
        let (event, key, value, metadata) = match data {
            RawBytes(event, raw_bytes) => (
                *event,
                None,
                value_from_bytes(raw_bytes, self.parse_utf8),
                Ok(None),
            ),
            KeyValue((key, value)) => match value {
                Some(bytes) => (
                    DataEventType::Insert,
                    self.key_generation_policy.generate(key, self.parse_utf8),
                    value_from_bytes(bytes, self.parse_utf8),
                    Ok(None),
                ),
                None => return Err(ParseError::EmptyKafkaPayload.into()),
            },
            Diff(_) | TokenizedEntries(_, _) => {
                return Err(ParseError::UnsupportedReaderContext.into())
            }
            Empty => return Ok(vec![]),
        };

        let is_commit = is_commit_literal(&value);

        let event = if is_commit {
            ParsedEventWithErrors::AdvanceTime
        } else {
            let mut values = Vec::new();
            let mut metadata = Some(metadata);
            let mut value = Some(value);
            for field in &self.value_fields {
                let to_insert = if field == METADATA_FIELD_NAME {
                    metadata
                        .take()
                        .expect("metadata column should be used exactly once in IdentityParser")
                        .map(|metadata| metadata.unwrap_or(self.metadata_column_value.clone()))
                } else {
                    value
                        .take()
                        .expect("value column should be used exactly once in IdentityParser")
                };
                values.push(to_insert);
            }
            ParsedEventWithErrors::new(self.session_type(), event, key, values)
        };

        Ok(vec![event])
    }

    fn on_new_source_started(&mut self, metadata: &SourceMetadata) {
        let metadata_serialized: JsonValue = metadata.serialize();
        self.metadata_column_value = metadata_serialized.into();
    }

    fn column_count(&self) -> usize {
        self.value_fields.len()
    }

    fn session_type(&self) -> SessionType {
        self.session_type
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
                .chain(["time".to_string(), "diff".to_string()])
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

pub struct SingleColumnFormatter {
    value_field_index: usize,
}

impl SingleColumnFormatter {
    pub fn new(value_field_index: usize) -> SingleColumnFormatter {
        SingleColumnFormatter { value_field_index }
    }
}

impl Formatter for SingleColumnFormatter {
    fn format(
        &mut self,
        key: &Key,
        values: &[Value],
        time: Timestamp,
        diff: isize,
    ) -> Result<FormatterContext, FormatterError> {
        let payload = match &values
            .get(self.value_field_index)
            .ok_or(FormatterError::IncorrectColumnIndex)?
        {
            Value::Bytes(bytes) => bytes.to_vec(),
            Value::String(string) => string.as_bytes().to_vec(),
            _ => return Err(FormatterError::UnsupportedValueType),
        };
        Ok(FormatterContext::new_single_payload(
            payload,
            *key,
            values.to_vec(),
            time,
            diff,
        ))
    }
}

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

fn parse_list_from_json(values: &[JsonValue], dtype: &Type) -> Option<Value> {
    let mut list = Vec::with_capacity(values.len());
    for value in values {
        list.push(parse_value_from_json(value, dtype)?);
    }
    Some(Value::from(list))
}

fn parse_tuple_from_json(values: &[JsonValue], dtypes: &[Type]) -> Option<Value> {
    if values.len() != dtypes.len() {
        return None;
    }
    let mut tuple = Vec::with_capacity(values.len());
    for (value, dtype) in values.iter().zip_eq(dtypes.iter()) {
        tuple.push(parse_value_from_json(value, dtype)?);
    }
    Some(Value::from(tuple))
}

fn parse_ndarray_from_json(value: &JsonMap<String, JsonValue>, dtype: &Type) -> Option<Value> {
    let JsonValue::Array(ref elements) = value["elements"] else {
        return None;
    };
    let JsonValue::Array(ref json_field_shape) = value["shape"] else {
        return None;
    };
    let mut shape = Vec::new();
    for shape_element in json_field_shape {
        let JsonValue::Number(shape_element) = shape_element else {
            return None;
        };
        let shape_element = shape_element.as_u64()?;
        let Ok(shape_element) = TryInto::<usize>::try_into(shape_element) else {
            return None;
        };
        shape.push(shape_element);
    }

    match dtype {
        Type::Int => {
            let mut flat_elements = Vec::new();
            for element in elements {
                let JsonValue::Number(number_element) = element else {
                    return None;
                };
                let int_element = number_element.as_i64()?;
                flat_elements.push(int_element);
            }
            let array_impl = ArrayD::<i64>::from_shape_vec(shape, flat_elements);
            if let Ok(array_impl) = array_impl {
                Some(Value::from(array_impl))
            } else {
                None
            }
        }
        Type::Float => {
            let mut flat_elements = Vec::new();
            for element in elements {
                let JsonValue::Number(number_element) = element else {
                    return None;
                };
                let float_element = number_element.as_f64()?;
                flat_elements.push(float_element);
            }
            let array_impl = ArrayD::<f64>::from_shape_vec(shape, flat_elements);
            if let Ok(array_impl) = array_impl {
                Some(Value::from(array_impl))
            } else {
                None
            }
        }
        type_ => {
            unreachable!("ndarray elements can only be either int or float, but {type_} was used")
        }
    }
}

fn create_bincoded_value(value: &Value) -> Result<String, FormatterError> {
    let raw_bytes = bincode::serialize(value).map_err(|e| *e)?;
    let encoded = base64::engine::general_purpose::STANDARD.encode(raw_bytes);
    Ok(encoded)
}

fn parse_bincoded_value(s: &str) -> Result<Value, ParseError> {
    let raw_bytes = base64::engine::general_purpose::STANDARD.decode(s)?;
    Ok(bincode::deserialize::<Value>(&raw_bytes).map_err(|e| *e)?)
}

/// Parses a value of type `dtype` from JSON variable `value`.
/// If the field is not present or has incorrect format in the
/// given payload, returns `None`. This `None` is further converted
/// into `ParseError::FailedToParseFromJson` containing verbose
/// information about parsing problem.
fn parse_value_from_json(value: &JsonValue, dtype: &Type) -> Option<Value> {
    if value.is_null() {
        if dtype.is_optional() {
            return Some(Value::None);
        }
        return None;
    }
    match (dtype.unoptionalize(), value) {
        (Type::Bool | Type::Any, JsonValue::Bool(v)) => Some(Value::Bool(*v)),
        // Numbers parsing
        (Type::Int, JsonValue::Number(v)) => {
            let i64_field = v.as_i64()?;
            Some(Value::from(i64_field))
        }
        (Type::Float, JsonValue::Number(v)) => {
            let f64_field = v.as_f64()?;
            Some(Value::from(f64_field))
        }
        (Type::Duration, JsonValue::Number(v)) => {
            let duration_ns = v.as_i64()?;
            let engine_duration = EngineDuration::new_with_unit(duration_ns, "ns")
                .expect("new_with_unit can't fail when 'ns' is used as a unit");
            Some(Value::Duration(engine_duration))
        }
        (Type::Any, JsonValue::Number(v)) => {
            if let Some(parsed_i64) = v.as_i64() {
                Some(Value::from(parsed_i64))
            } else {
                v.as_f64().map(Value::from)
            }
        }
        // Strings parsing
        (Type::String | Type::Any, JsonValue::String(s)) => Some(Value::from(s.as_str())),
        (Type::Bytes, JsonValue::String(s)) => {
            let decoded = base64::engine::general_purpose::STANDARD.decode(s);
            if let Ok(decoded) = decoded {
                Some(Value::Bytes(decoded.into()))
            } else {
                None
            }
        }
        (Type::PyObjectWrapper, JsonValue::String(s)) => parse_bincoded_value(s).ok(),
        (Type::Pointer, JsonValue::String(s)) => parse_pathway_pointer(s).ok(),
        (Type::DateTimeUtc, JsonValue::String(s)) => {
            let engine_datetime = DateTimeUtc::strptime(s, "%Y-%m-%dT%H:%M:%S%.f%z");
            if let Ok(engine_datetime) = engine_datetime {
                Some(Value::DateTimeUtc(engine_datetime))
            } else {
                None
            }
        }
        (Type::DateTimeNaive, JsonValue::String(s)) => {
            let engine_datetime = DateTimeNaive::strptime(s, "%Y-%m-%dT%H:%M:%S%.f");
            if let Ok(engine_datetime) = engine_datetime {
                Some(Value::DateTimeNaive(engine_datetime))
            } else {
                None
            }
        }
        (Type::Json, value) => Some(Value::from(value.clone())),
        (Type::Tuple(dtypes), JsonValue::Array(v)) => parse_tuple_from_json(v, dtypes),
        (Type::List(arg), JsonValue::Array(v)) => parse_list_from_json(v, arg),
        (Type::Array(_, nested_type), JsonValue::Object(v)) => {
            parse_ndarray_from_json(v, nested_type.as_ref())
        }
        _ => None,
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
            let encoded = base64::engine::general_purpose::STANDARD.encode(b);
            Ok(json!(encoded))
        }
        Value::IntArray(a) => {
            let mut flat_elements = Vec::with_capacity(a.len());
            for item in a.iter() {
                flat_elements.push(json!(item));
            }
            let serialized_values = json!({
                "shape": a.shape(),
                "elements": flat_elements,
            });
            Ok(serialized_values)
        }
        Value::FloatArray(a) => {
            let mut flat_elements = Vec::with_capacity(a.len());
            for item in a.iter() {
                flat_elements.push(json!(item));
            }
            let serialized_values = json!({
                "shape": a.shape(),
                "elements": flat_elements,
            });
            Ok(serialized_values)
        }
        Value::DateTimeNaive(dt) => Ok(json!(dt.to_string())),
        Value::DateTimeUtc(dt) => Ok(json!(dt.to_string())),
        Value::Duration(d) => Ok(json!(d.nanoseconds())),
        Value::Json(j) => Ok((**j).clone()),
        Value::PyObjectWrapper(_) => {
            let encoded = create_bincoded_value(value)?;
            Ok(json!(encoded))
        }
        Value::Error => Err(FormatterError::ErrorValueNonJsonSerializable),
        Value::Pending => Err(FormatterError::PendingValueNonJsonSerializable),
    }
}

fn values_by_names_from_json(
    payload: &JsonValue,
    field_names: &[String],
    column_paths: &HashMap<String, String>,
    field_absence_is_error: bool,
    schema: &HashMap<String, InnerSchemaField>,
    metadata_column_value: &Value,
) -> ValueFieldsWithErrors {
    let mut parsed_values = Vec::with_capacity(field_names.len());
    for value_field in field_names {
        let (default_value, dtype) = {
            if let Some(schema_item) = schema.get(value_field) {
                (schema_item.default.as_ref(), &schema_item.type_)
            } else {
                (None, &Type::Any)
            }
        };

        let value = if value_field == METADATA_FIELD_NAME {
            Ok(metadata_column_value.clone())
        } else if let Some(path) = column_paths.get(value_field) {
            if let Some(value) = payload.pointer(path) {
                parse_value_from_json(value, dtype).ok_or_else(|| {
                    ParseError::FailedToParseFromJson {
                        field_name: value_field.to_string(),
                        payload: value.clone(),
                        type_: dtype.clone(),
                    }
                    .into()
                })
            } else if let Some(default) = default_value {
                Ok(default.clone())
            } else if field_absence_is_error {
                Err(ParseError::FailedToExtractJsonField {
                    field_name: value_field.to_string(),
                    path: Some(path.to_string()),
                    payload: payload.clone(),
                }
                .into())
            } else {
                Ok(Value::None)
            }
        } else {
            let value_specified_in_json = payload.get(value_field).is_some();

            if value_specified_in_json {
                parse_value_from_json(&payload[&value_field], dtype).ok_or_else(|| {
                    ParseError::FailedToParseFromJson {
                        field_name: value_field.to_string(),
                        payload: payload[&value_field].clone(),
                        type_: dtype.clone(),
                    }
                    .into()
                })
            } else if let Some(default) = default_value {
                Ok(default.clone())
            } else if field_absence_is_error {
                Err(ParseError::FailedToExtractJsonField {
                    field_name: value_field.to_string(),
                    path: None,
                    payload: payload.clone(),
                }
                .into())
            } else {
                Ok(Value::None)
            }
        };
        parsed_values.push(value);
    }
    parsed_values
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
                    return Err(ParseError::FailedToParseJson(serialized_json.to_string()));
                };
                prepared_value
            } else {
                value.clone()
            }
        };

        let key = self.key_field_names.as_ref().map(|names| {
            values_by_names_from_json(
                key,
                names,
                &HashMap::new(),
                true,
                &HashMap::new(),
                &Value::None,
            )
            .into_iter()
            .collect()
        });

        let parsed_values = values_by_names_from_json(
            &prepared_value,
            &self.value_field_names,
            &HashMap::new(),
            true,
            &HashMap::new(),
            &Value::None,
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
                    values_by_names_from_json(
                        key,
                        names,
                        &HashMap::new(),
                        true,
                        &HashMap::new(),
                        &Value::None,
                    )
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
            Diff(_) | TokenizedEntries(_, _) | Empty => {
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
                _ => Err(ParseError::UnsupportedDebeziumOperation(op.to_string()).into()),
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

pub struct JsonLinesParser {
    key_field_names: Option<Vec<String>>,
    value_field_names: Vec<String>,
    column_paths: HashMap<String, String>,
    field_absence_is_error: bool,
    schema: HashMap<String, InnerSchemaField>,
    metadata_column_value: Value,
    session_type: SessionType,
}

impl JsonLinesParser {
    pub fn new(
        key_field_names: Option<Vec<String>>,
        value_field_names: Vec<String>,
        column_paths: HashMap<String, String>,
        field_absence_is_error: bool,
        schema: HashMap<String, InnerSchemaField>,
        session_type: SessionType,
    ) -> Result<JsonLinesParser> {
        ensure_all_fields_in_schema(&key_field_names, &value_field_names, &schema)?;
        Ok(JsonLinesParser {
            key_field_names,
            value_field_names,
            column_paths,
            field_absence_is_error,
            schema,
            metadata_column_value: Value::None,
            session_type,
        })
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
                    return Err(ParseError::EmptyKafkaPayload.into());
                }
            }
            Diff(_) | TokenizedEntries(..) => {
                return Err(ParseError::UnsupportedReaderContext.into());
            }
            Empty => return Ok(vec![]),
        };

        if line.is_empty() {
            return Ok(vec![]);
        }

        if line == COMMIT_LITERAL {
            return Ok(vec![ParsedEventWithErrors::AdvanceTime]);
        }

        let payload: JsonValue = match serde_json::from_str(&line) {
            Ok(json_value) => json_value,
            Err(_) => return Err(ParseError::FailedToParseJson(line).into()),
        };

        let key = key.or(match &self.key_field_names {
            Some(key_field_names) => Some(
                values_by_names_from_json(
                    &payload,
                    key_field_names,
                    &self.column_paths,
                    self.field_absence_is_error,
                    &self.schema,
                    &self.metadata_column_value,
                )
                .into_iter()
                .collect(),
            ),
            None => None, // use method from the different PR
        });

        let values = values_by_names_from_json(
            &payload,
            &self.value_field_names,
            &self.column_paths,
            self.field_absence_is_error,
            &self.schema,
            &self.metadata_column_value,
        );

        let event = ParsedEventWithErrors::new(self.session_type, data_event, key, values);

        Ok(vec![event])
    }

    fn on_new_source_started(&mut self, metadata: &SourceMetadata) {
        let metadata_serialized: JsonValue = metadata.serialize();
        self.metadata_column_value = metadata_serialized.into();
    }

    fn column_count(&self) -> usize {
        self.value_field_names.len()
    }

    fn session_type(&self) -> SessionType {
        self.session_type
    }
}

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
        ensure_all_fields_in_schema(&key_field_names, &value_field_names, &schema)?;
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
        time: Timestamp,
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
            time,
            diff,
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
        time: Timestamp,
        diff: isize,
    ) -> Result<FormatterContext, FormatterError> {
        if values.len() != self.value_field_names.len() {
            return Err(FormatterError::ColumnsValuesCountMismatch);
        }

        let mut result = Vec::new();

        if diff == 1 {
            let update_pairs = self
                .value_field_positions
                .iter()
                .map(|position| format!("{}=${}", self.value_field_names[*position], *position + 1))
                .join(",");

            let on_conflict = if update_pairs.is_empty() {
                "DO NOTHING".to_string()
            } else {
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
                format!(
                    "DO UPDATE SET {update_pairs},time={time},diff={diff} WHERE {update_condition}"
                )
            };

            writeln!(
                result,
                "INSERT INTO {} ({},time,diff) VALUES ({},{},{}) ON CONFLICT ({}) {}",
                self.table_name,
                self.value_field_names.iter().format(","),
                (1..=values.len()).format_with(",", |x, f| f(&format_args!("${x}"))),
                time,
                diff,
                self.key_field_names.iter().join(","),
                on_conflict
            )
            .unwrap();

            Ok(FormatterContext::new_single_payload(
                result,
                *key,
                values.to_vec(),
                time,
                diff,
            ))
        } else {
            let mut tokens = Vec::new();
            let mut key_part_values = Vec::new();
            for (name, position) in self
                .key_field_names
                .iter()
                .zip(self.key_field_positions.iter())
            {
                key_part_values.push(values[*position].clone());
                tokens.push(format!(
                    "{name}={}",
                    format_args!("${}", key_part_values.len())
                ));
            }

            writeln!(
                result,
                "DELETE FROM {} WHERE {}",
                self.table_name,
                tokens.join(" AND "),
            )
            .unwrap();

            Ok(FormatterContext::new_single_payload(
                result,
                *key,
                take(&mut key_part_values),
                time,
                diff,
            ))
        }
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
        time: Timestamp,
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
            values.to_vec(),
            time,
            diff,
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
        time: Timestamp,
        diff: isize,
    ) -> Result<FormatterContext, FormatterError> {
        Ok(FormatterContext::new(
            Vec::<FormattedDocument>::new(),
            *key,
            Vec::new(),
            time,
            diff,
        ))
    }
}

#[derive(Default)]
pub struct IdentityFormatter {}

impl IdentityFormatter {
    pub fn new() -> IdentityFormatter {
        IdentityFormatter {}
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
        Ok(FormatterContext::new_single_payload(
            Vec::new(),
            *key,
            values.to_vec(),
            time,
            diff,
        ))
    }
}

fn serialize_value_to_bson(value: &Value) -> Result<BsonValue, FormatterError> {
    match value {
        Value::None => Ok(BsonValue::Null),
        Value::Int(i) => Ok(bson!(i)),
        Value::Bool(b) => Ok(bson!(b)),
        Value::Float(f) => Ok(BsonValue::Double((*f).into())),
        Value::String(s) => Ok(BsonValue::String(s.to_string())),
        Value::Pointer(p) => Ok(bson!(p.to_string())),
        Value::Tuple(t) => {
            let mut items = Vec::with_capacity(t.len());
            for item in t.iter() {
                items.push(serialize_value_to_bson(item)?);
            }
            Ok(BsonValue::Array(items))
        }
        Value::IntArray(a) => {
            let mut items = Vec::with_capacity(a.len());
            for item in a.iter() {
                items.push(bson!(item));
            }
            Ok(BsonValue::Array(items))
        }
        Value::FloatArray(a) => {
            let mut items = Vec::with_capacity(a.len());
            for item in a.iter() {
                items.push(bson!(item));
            }
            Ok(BsonValue::Array(items))
        }
        Value::Bytes(b) => Ok(BsonValue::Binary(BsonBinaryContents {
            subtype: BsonBinarySubtype::Generic,
            bytes: b.to_vec(),
        })),

        // We use milliseconds here because BSON DateTime type has millisecond precision
        // See also: https://docs.rs/bson/2.11.0/bson/struct.DateTime.html
        Value::DateTimeNaive(dt) => Ok(BsonValue::DateTime(BsonDateTime::from_millis(
            dt.timestamp_milliseconds(),
        ))),
        Value::DateTimeUtc(dt) => Ok(BsonValue::DateTime(BsonDateTime::from_millis(
            dt.timestamp_milliseconds(),
        ))),

        // We use milliseconds in durations to be consistent with the granularity
        // of the BSON DateTime type
        Value::Duration(d) => Ok(bson!(d.milliseconds())),
        Value::Json(j) => Ok(bson!(j.to_string())),
        Value::Error => Err(FormatterError::ErrorValueNonBsonSerializable),
        Value::PyObjectWrapper(_) => Err(FormatterError::TypeNonBsonSerializable {
            type_: Type::PyObjectWrapper,
        }),
        Value::Pending => Err(FormatterError::PendingValueNonBsonSerializable),
    }
}

pub struct BsonFormatter {
    value_field_names: Vec<String>,
}

impl BsonFormatter {
    pub fn new(value_field_names: Vec<String>) -> Self {
        Self { value_field_names }
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
        let mut document = BsonDocument::new();
        for (key, value) in zip(self.value_field_names.iter(), values) {
            let _ = document.insert(key, serialize_value_to_bson(value)?);
        }
        let _ = document.insert(
            "diff",
            BsonValue::Int64(diff.try_into().expect("diff can only be +1 or -1")),
        );
        let _ = document.insert(
            "time",
            BsonValue::Int64(
                time.0
                    .try_into()
                    .expect("timestamp is not expected to exceed int64 type"),
            ),
        );
        Ok(FormatterContext::new_single_payload(
            document,
            *key,
            Vec::new(),
            time,
            diff,
        ))
    }
}
