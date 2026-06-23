// Copyright © 2026 Pathway

use std::any::type_name;
use std::borrow::Cow;
use std::clone::Clone;
use std::collections::HashMap;
use std::str::{from_utf8, Utf8Error};

use crate::connectors::metadata::SourceMetadata;
use crate::connectors::{DataEventType, Offset, ReaderContext, SessionType, SnapshotEvent};
use crate::engine::error::{limit_length, DynError, DynResult, STANDARD_OBJECT_LENGTH_LIMIT};
use crate::engine::{
    value::parse_pathway_pointer, value::Kind as ValueKind, DateTimeNaive, DateTimeUtc,
    Duration as EngineDuration, Error, Key, Result, Timestamp, Type, Value,
};

use async_nats::header::HeaderMap as NatsHeaders;
use base64::engine::general_purpose::STANDARD as base64encoder;
use base64::Engine;
use bincode::ErrorKind as BincodeError;
use itertools::Itertools;
use mongodb::bson::Document as BsonDocument;
use ndarray::ArrayD;
use rdkafka::message::{Header as KafkaHeader, OwnedHeaders as KafkaHeaders};
use schema_registry_converter::error::SRCError as SchemaRepositoryError;
use serde_json::json;
use serde_json::{Map as JsonMap, Value as JsonValue};

use super::data_storage::ConversionError;

pub mod bson;
pub mod debezium;
pub mod dsv;
pub mod identity;
pub mod json;
pub mod null;
pub mod single_column;
pub mod transparent;

pub use bson::{BsonFormatter, BsonParser};
pub use debezium::{DebeziumDBType, DebeziumMessageParser};
pub use dsv::{DsvFormatter, DsvParser, DsvSettings};
pub use identity::{IdentityFormatter, IdentityParser, KeyGenerationPolicy};
pub use json::{
    FieldSource, FieldSourceLists, JsonLinesFormatter, JsonLinesParser, RegistryEncoderWrapper,
};
pub use null::NullFormatter;
pub use single_column::SingleColumnFormatter;
pub use transparent::TransparentParser;

pub const COMMIT_LITERAL: &str = "*COMMIT*";
pub const NDARRAY_ELEMENTS_FIELD_NAME: &str = "elements";
pub const NDARRAY_SINGLE_ELEMENT_FIELD_NAME: &str = "element";
pub const NDARRAY_SHAPE_FIELD_NAME: &str = "shape";

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

    #[error(transparent)]
    SchemaRepository(#[from] SchemaRepositoryError),
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
    pub type_: Type,
    pub default: Option<Value>, // None means that there is no default for the field
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
    Ok(prepare_plaintext_str(bytes)?.to_string())
}

/// Like [`prepare_plaintext_string`], but borrows from the input instead of
/// allocating a fresh `String`. Used on hot parsing paths where the trimmed
/// text is consumed immediately.
pub(crate) fn prepare_plaintext_str(bytes: &[u8]) -> Result<&str, ParseError> {
    Ok(from_utf8(bytes)?.trim())
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
    value: Option<Vec<u8>>,
}

impl PreparedMessageHeader {
    pub fn new(key: impl Into<String>, value: Option<Vec<u8>>) -> Self {
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
        header_fields: &[(String, usize)],
        encode_bytes: bool,
    ) -> Vec<PreparedMessageHeader> {
        let mut headers = Vec::with_capacity(header_fields.len() + 2);
        headers.push(PreparedMessageHeader::new(
            "pathway_time",
            Some(self.time.to_string().as_bytes().to_vec()),
        ));
        headers.push(PreparedMessageHeader::new(
            "pathway_diff",
            Some(self.diff.to_string().as_bytes().to_vec()),
        ));
        for (name, position) in header_fields {
            let value: Option<Vec<u8>> = match (&self.values[*position], encode_bytes) {
                (Value::Bytes(b), false) => Some((*b).to_vec()),
                (Value::Bytes(b), true) => Some(base64encoder.encode(b).into()),
                (Value::String(s), _) => Some(s.as_bytes().to_vec()),
                (Value::None, _) => None,
                (other, _) => Some((*other.to_string().as_bytes()).to_vec()),
            };
            headers.push(PreparedMessageHeader::new(name, value));
        }
        headers
    }

    pub fn construct_kafka_headers(&self, header_fields: &[(String, usize)]) -> KafkaHeaders {
        let raw_headers = self.construct_message_headers(header_fields, false);
        let mut kafka_headers = KafkaHeaders::new_with_capacity(raw_headers.len());
        for header in raw_headers {
            kafka_headers = kafka_headers.insert(KafkaHeader {
                key: &header.key,
                value: header.value.as_ref(),
            });
        }
        kafka_headers
    }

    pub fn construct_nats_headers(&self, header_fields: &[(String, usize)]) -> NatsHeaders {
        let raw_headers = self.construct_message_headers(header_fields, true);
        let mut nats_headers = NatsHeaders::new();
        for header in raw_headers {
            let header_value = if let Some(header_value) = header.value {
                String::from_utf8(header_value)
                    .expect("all prepared headers must be UTF-8 serializable")
            } else {
                Value::None.to_string()
            };
            nats_headers.insert(header.key, header_value);
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

    #[error("value kind '{0:?}' is not serializable into format: '{1}'")]
    ValueNonSerializable(ValueKind, &'static str),

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

    #[error(transparent)]
    SchemaRepository(#[from] SchemaRepositoryError),

    #[error("incorrect external diff value: {0}")]
    IncorrectDiffColumnValue(Value),
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
        }
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

pub fn ensure_all_fields_in_schema(
    key_column_names: Option<&[String]>,
    value_column_names: &[String],
    schema: &HashMap<String, InnerSchemaField, impl std::hash::BuildHasher>,
) -> Result<()> {
    for name in key_column_names
        .into_iter()
        .flatten()
        .chain(value_column_names)
    {
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
pub const METADATA_FIELD_NAME: &str = "_metadata";

fn value_from_bytes(bytes: &[u8], parse_utf8: bool) -> DynResult<Value> {
    if parse_utf8 {
        Ok(Value::String(prepare_plaintext_string(bytes)?.into()))
    } else {
        Ok(Value::Bytes(bytes.into()))
    }
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
    let JsonValue::Array(ref elements) = value[NDARRAY_ELEMENTS_FIELD_NAME] else {
        return None;
    };
    let JsonValue::Array(ref json_field_shape) = value[NDARRAY_SHAPE_FIELD_NAME] else {
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

pub fn create_bincoded_value(value: &Value) -> Result<String, FormatterError> {
    let raw_bytes = bincode::serialize(value).map_err(|e| *e)?;
    let encoded = base64::engine::general_purpose::STANDARD.encode(raw_bytes);
    Ok(encoded)
}

pub fn parse_bincoded_value(s: &str) -> Result<Value, ParseError> {
    let raw_bytes = base64::engine::general_purpose::STANDARD.decode(s)?;
    Ok(bincode::deserialize::<Value>(&raw_bytes).map_err(|e| *e)?)
}

/// Parses a value of type `dtype` from JSON variable `value`.
/// If the field is not present or has incorrect format in the
/// given payload, returns `None`. This `None` is further converted
/// into `ParseError::FailedToParseFromJson` containing verbose
/// information about parsing problem.
pub fn parse_value_from_json(value: &JsonValue, dtype: &Type) -> Option<Value> {
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

pub fn serialize_value_to_json(value: &Value) -> Result<JsonValue, FormatterError> {
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
                NDARRAY_SHAPE_FIELD_NAME: a.shape(),
                NDARRAY_ELEMENTS_FIELD_NAME: flat_elements,
            });
            Ok(serialized_values)
        }
        Value::FloatArray(a) => {
            let mut flat_elements = Vec::with_capacity(a.len());
            for item in a.iter() {
                flat_elements.push(json!(item));
            }
            let serialized_values = json!({
                NDARRAY_SHAPE_FIELD_NAME: a.shape(),
                NDARRAY_ELEMENTS_FIELD_NAME: flat_elements,
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
        Value::Error | Value::Pending => {
            Err(FormatterError::ValueNonSerializable(value.kind(), "JSON"))
        }
    }
}

fn values_by_names_from_json(
    payload: &JsonValue,
    field_names: &[String],
    column_paths: &HashMap<String, String>,
    field_absence_is_error: bool,
    schema: &HashMap<String, InnerSchemaField>,
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

        let value = if let Some(path) = column_paths.get(value_field) {
            if let Some(value) = payload.pointer(path) {
                parse_value_from_json(value, dtype).ok_or_else(|| {
                    ParseError::FailedToParseFromJson {
                        field_name: value_field.clone(),
                        payload: value.clone(),
                        type_: dtype.clone(),
                    }
                    .into()
                })
            } else if let Some(default) = default_value {
                Ok(default.clone())
            } else if field_absence_is_error {
                Err(ParseError::FailedToExtractJsonField {
                    field_name: value_field.clone(),
                    path: Some(path.clone()),
                    payload: payload.clone(),
                }
                .into())
            } else {
                Ok(Value::None)
            }
        } else if let Some(found_value) = payload.get(value_field) {
            parse_value_from_json(found_value, dtype).ok_or_else(|| {
                ParseError::FailedToParseFromJson {
                    field_name: value_field.clone(),
                    payload: found_value.clone(),
                    type_: dtype.clone(),
                }
                .into()
            })
        } else if let Some(default) = default_value {
            Ok(default.clone())
        } else if field_absence_is_error {
            Err(ParseError::FailedToExtractJsonField {
                field_name: value_field.clone(),
                path: None,
                payload: payload.clone(),
            }
            .into())
        } else {
            Ok(Value::None)
        };
        parsed_values.push(value);
    }
    parsed_values
}
