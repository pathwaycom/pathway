// Copyright © 2024 Pathway

use base64::Engine;
use pyo3::exceptions::PyValueError;
use pyo3::types::PyBytes;
use s3::error::S3Error;
use std::any::type_name;
use std::borrow::Borrow;
use std::borrow::Cow;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::fmt;
use std::fmt::{Debug, Display};
use std::io;
use std::io::BufRead;
use std::io::BufWriter;
use std::io::Write;
use std::mem::take;
use std::str::{from_utf8, Utf8Error};
use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;

use arcstr::ArcStr;
use async_nats::jetstream::consumer::pull::MessagesErrorKind;
use async_nats::jetstream::consumer::StreamErrorKind;
use aws_sdk_dynamodb::error::BuildError as AwsBuildError;
use deltalake::arrow::datatypes::DataType as ArrowDataType;
use deltalake::arrow::error::ArrowError;
use deltalake::datafusion::common::DataFusionError;
use deltalake::datafusion::parquet::record::Field as ParquetValue;
use deltalake::parquet::errors::ParquetError;
use deltalake::DeltaTableError;
use iceberg::Error as IcebergError;
use itertools::Itertools;
use log::{error, info, warn};
use mysql::{
    prelude::Queryable, Error as MysqlError, Params as MysqlParams, Pool as MysqlConnectionPool,
    PooledConn as MysqlConnection, TxOpts as MysqlTxOpts, Value as MysqlValue,
};
use postgres::types::ToSql;
use questdb::ingress::{
    Buffer as QuestDBBuffer, Sender as QuestDBSender, Timestamp as QuestDBTimestamp,
    TimestampMicros as QuestDBTimestampMicros, TimestampNanos as QuestDBTimestampNanos,
};
use questdb::Error as QuestDBError;
use rumqttc::{
    mqttbytes::QoS as MqttQoS, Client as MqttClient, ClientError as MqttClientError,
    Connection as MqttConnection, ConnectionError as MqttConnectionError, Event as MqttEvent,
    Incoming as MqttIncoming, Outgoing as MqttOutgoing, Packet as MqttPacket,
};

use crate::async_runtime::create_async_tokio_runtime;
use crate::connectors::aws::dynamodb::Error as AwsDynamoDBError;
use crate::connectors::aws::kinesis::Error as AwsKinesisError;
use crate::connectors::aws::kinesis::KinesisReader;
use crate::connectors::data_format::{
    create_bincoded_value, serialize_value_to_json, FormatterContext, FormatterError,
    COMMIT_LITERAL,
};
use crate::connectors::data_lake::buffering::IncorrectSnapshotError;
use crate::connectors::metadata::{KafkaMetadata, SQLiteMetadata, SourceMetadata};
use crate::connectors::offset::EMPTY_OFFSET;
use crate::connectors::posix_like::PosixLikeReader;
use crate::connectors::scanner::s3::S3CommandName;
use crate::connectors::{Offset, OffsetKey, OffsetValue, SPECIAL_FIELD_DIFF, SPECIAL_FIELD_TIME};
use crate::engine::error::limit_length;
use crate::engine::error::DynResult;
use crate::engine::error::STANDARD_OBJECT_LENGTH_LIMIT;
use crate::engine::time::DateTime;
use crate::engine::DateTimeNaive;
use crate::engine::Type;
use crate::engine::{Duration as EngineDuration, Key, Value};
use crate::persistence::backends::Error as PersistenceBackendError;
use crate::persistence::frontier::OffsetAntichain;
use crate::persistence::tracker::WorkerPersistentStorage;
use crate::persistence::{PersistentId, UniqueName};
use crate::python_api::extract_value;
use crate::python_api::threads::PythonThreadState;
use crate::python_api::PythonSubject;
use crate::python_api::ValueField;
use crate::retry::{execute_with_retries, RetryConfig};

use async_nats::client::FlushError as NatsFlushError;
use async_nats::client::PublishError as NatsPublishError;
use async_nats::error::Error as NatsError;
use async_nats::jetstream::context::PublishErrorKind as JetStreamPublishError;
use bincode::ErrorKind as BincodeError;
use elasticsearch::{BulkParts, Elasticsearch};
use glob::PatternError as GlobPatternError;
use mongodb::bson::Document as BsonDocument;
use mongodb::error::Error as MongoError;
use mongodb::sync::Collection as MongoCollection;
use postgres::Client as PsqlClient;
use pyo3::prelude::*;
use rdkafka::consumer::{BaseConsumer, Consumer, DefaultConsumerContext};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::BorrowedMessage;
use rdkafka::producer::{BaseRecord, DefaultProducerContext, Producer, ThreadedProducer};
use rdkafka::topic_partition_list::Offset as KafkaOffset;
use rdkafka::Message;
use rdkafka::TopicPartitionList;
use rusqlite::types::ValueRef as SqliteValue;
use rusqlite::Connection as SqliteConnection;
use rusqlite::Error as SqliteError;
use serde::{Deserialize, Serialize};

pub use super::data_lake::delta::{
    DeltaTableReader, ObjectDownloader, SchemaMismatchDetails as DeltaSchemaMismatchDetails,
};
pub use super::data_lake::iceberg::IcebergReader;
pub use super::data_lake::LakeWriter;
pub use super::nats::NatsReader;
pub use super::nats::NatsWriter;

#[derive(Clone, Debug, Eq, PartialEq, Copy)]
pub enum DataEventType {
    Insert,
    Delete,
}

#[derive(Clone, Debug, Eq, PartialEq, Copy)]
pub enum PythonConnectorEventType {
    Insert,
    Delete,
    ExternalOffset,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpecialEvent {
    Finish,
    EnableAutocommits,
    DisableAutocommits,
    Commit,
}

impl TryFrom<&str> for SpecialEvent {
    type Error = ReadError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "*FINISH*" => Ok(Self::Finish),
            "*ENABLE_COMMITS*" => Ok(Self::EnableAutocommits),
            "*DISABLE_COMMITS*" => Ok(Self::DisableAutocommits),
            COMMIT_LITERAL => Ok(Self::Commit),
            value => Err(ReadError::InvalidSpecialValue(value.to_owned())),
        }
    }
}

#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub struct ValuesMap {
    map: HashMap<String, Result<Value, Box<ConversionError>>>,
    // TODO: use a vector if performance improvement is needed
    // then Reader has to be aware of the columns order
}

impl ValuesMap {
    const SPECIAL_FIELD_NAME: &'static str = "_pw_special";
    pub fn get_special(&self) -> Option<SpecialEvent> {
        if self.map.len() == 1 {
            let value = self.map.get(Self::SPECIAL_FIELD_NAME)?.as_ref();
            value.ok()?.as_string().ok()?.as_str().try_into().ok()
        } else {
            None
        }
    }

    pub fn get(&self, key: &str) -> Option<&Result<Value, Box<ConversionError>>> {
        self.map.get(key)
    }

    pub fn get_mut(&mut self, key: &str) -> Option<&mut Result<Value, Box<ConversionError>>> {
        self.map.get_mut(key)
    }

    pub fn remove(&mut self, key: &str) {
        self.map.remove(key);
    }

    pub fn into_pure_hashmap(self) -> DynResult<HashMap<String, Value>> {
        self.map
            .into_iter()
            .map(|(key, value)| Ok((key, value?)))
            .try_collect()
    }

    pub fn merge(&mut self, other: &Self) {
        for (key, value) in &other.map {
            self.map.entry(key.clone()).or_insert_with(|| value.clone());
        }
    }
}

impl From<HashMap<String, Result<Value, Box<ConversionError>>>> for ValuesMap {
    fn from(value: HashMap<String, Result<Value, Box<ConversionError>>>) -> Self {
        ValuesMap { map: value }
    }
}

#[derive(Clone, Debug)]
pub enum MessageQueueTopic {
    Fixed(String),
    Dynamic(usize), // Index of the field used as a topic
}

impl MessageQueueTopic {
    pub fn get_for_posting(&self, values: &[Value]) -> Result<String, WriteError> {
        match self {
            Self::Fixed(t) => Ok(t.clone()),
            Self::Dynamic(i) => {
                if let Value::String(t) = &values[*i] {
                    Ok(t.to_string())
                } else {
                    Err(WriteError::DynamicTopicIsNotAString(values[*i].clone()))
                }
            }
        }
    }
}

impl Display for MessageQueueTopic {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Fixed(t) => write!(f, "{t}"),
            Self::Dynamic(i) => write!(f, "${i}"),
        }
    }
}

#[derive(Debug)]
pub enum ReaderContext {
    RawBytes(DataEventType, Vec<u8>),
    TokenizedEntries(DataEventType, Vec<String>),
    KeyValue((Option<Vec<u8>>, Option<Vec<u8>>)),
    Diff((DataEventType, Option<Vec<Value>>, ValuesMap)),
    Empty,
}

impl ReaderContext {
    pub fn from_raw_bytes(event: DataEventType, raw_bytes: Vec<u8>) -> ReaderContext {
        ReaderContext::RawBytes(event, raw_bytes)
    }

    pub fn from_diff(
        event: DataEventType,
        key: Option<Vec<Value>>,
        values: ValuesMap,
    ) -> ReaderContext {
        ReaderContext::Diff((event, key, values))
    }

    pub fn from_tokenized_entries(
        event: DataEventType,
        tokenized_entries: Vec<String>,
    ) -> ReaderContext {
        ReaderContext::TokenizedEntries(event, tokenized_entries)
    }

    pub fn from_key_value(key: Option<Vec<u8>>, value: Option<Vec<u8>>) -> ReaderContext {
        ReaderContext::KeyValue((key, value))
    }
}

#[derive(Debug)]
pub enum CommitPossibility {
    Forbidden,
    Possible,
    Forced,
}

impl CommitPossibility {
    pub fn commit_allowed(&self) -> bool {
        match self {
            Self::Forbidden => false,
            Self::Possible | Self::Forced => true,
        }
    }

    pub fn commit_forced(&self) -> bool {
        match self {
            Self::Forbidden | Self::Possible => false,
            Self::Forced => true,
        }
    }
}

#[derive(Debug)]
pub enum ReadResult {
    Finished,
    NewSource(SourceMetadata),
    FinishedSource {
        commit_possibility: CommitPossibility,
    },
    Data(ReaderContext, Offset),
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ReadError {
    #[error(transparent)]
    Io(#[from] io::Error),

    #[error(transparent)]
    Kafka(#[from] KafkaError),

    #[error(transparent)]
    Csv(#[from] csv::Error),

    #[error("failed to perform S3 operation {0:?} reason: {1:?}")]
    S3(S3CommandName, S3Error),

    #[error("failed to perform Sqlite request: {0}")]
    Sqlite(#[from] SqliteError),

    #[error(transparent)]
    DeltaTable(#[from] DeltaTableError),

    #[error(transparent)]
    Parquet(#[from] ParquetError),

    #[error(transparent)]
    Py(#[from] PyErr),

    #[error(transparent)]
    GlobPattern(#[from] GlobPatternError),

    #[error(transparent)]
    Bincode(#[from] BincodeError),

    #[error(transparent)]
    Iceberg(#[from] IcebergError),

    #[error(transparent)]
    Datafusion(#[from] DataFusionError),

    #[error(transparent)]
    Mqtt(#[from] MqttConnectionError),

    #[error(transparent)]
    Persistence(#[from] PersistenceBackendError),

    #[error(transparent)]
    Kinesis(#[from] AwsKinesisError),

    #[error("malformed data")]
    MalformedData,

    #[error("no objects to read")]
    NoObjectsToRead,

    #[error("invalid special value: {0}")]
    InvalidSpecialValue(String),

    #[error("parquet value type mismatch: got {0:?} expected {1:?}")]
    WrongParquetType(ParquetValue, Type),

    #[error("deletion vectors in delta tables are not supported")]
    DeltaDeletionVectorsNotSupported,

    #[error("explicit primary key specification is required for non-append-only tables")]
    PrimaryKeyRequired,

    #[error(transparent)]
    NatsStreaming(#[from] async_nats::error::Error<StreamErrorKind>),

    #[error(transparent)]
    NatsPolling(#[from] async_nats::error::Error<MessagesErrorKind>),

    #[error("failed to acknowledge read nats message: {0}")]
    NatsMessageAck(async_nats::Error),
}

#[derive(Debug, thiserror::Error, Clone, Eq, PartialEq)]
#[error("cannot create a field {field_name:?} with type {type_} from value {value_repr}{original_error_message}")]
pub struct ConversionError {
    pub value_repr: String,
    pub field_name: String,
    pub type_: Type,
    pub original_error_message: String,
}

impl ConversionError {
    pub fn new(
        value_repr: String,
        field_name: String,
        type_: Type,
        original_error_message: Option<String>,
    ) -> Self {
        let original_error_message = if let Some(msg) = original_error_message {
            format!(". Original error: {msg}")
        } else {
            String::new()
        };
        Self {
            value_repr,
            field_name,
            type_,
            original_error_message,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug)]
pub enum StorageType {
    // Filesystem, S3Csv, S3Lines and S3Lines are left for compatibility with old versions
    FileSystem,
    S3Csv,
    S3Lines,
    CsvFilesystem,
    Kafka,
    Python,
    Sqlite,
    DeltaLake,
    Nats,
    PosixLike,
    Iceberg,
    Mqtt,
    Kinesis,
}

impl StorageType {
    pub fn merge_two_frontiers(
        self,
        lhs: &OffsetAntichain,
        rhs: &OffsetAntichain,
    ) -> OffsetAntichain {
        match self {
            StorageType::CsvFilesystem
            | StorageType::FileSystem
            | StorageType::S3Csv
            | StorageType::S3Lines
            | StorageType::PosixLike => PosixLikeReader::merge_two_frontiers(lhs, rhs),
            StorageType::Kafka => KafkaReader::merge_two_frontiers(lhs, rhs),
            StorageType::Python => PythonReader::merge_two_frontiers(lhs, rhs),
            StorageType::Sqlite => SqliteReader::merge_two_frontiers(lhs, rhs),
            StorageType::DeltaLake => DeltaTableReader::merge_two_frontiers(lhs, rhs),
            StorageType::Nats => NatsReader::merge_two_frontiers(lhs, rhs),
            StorageType::Iceberg => IcebergReader::merge_two_frontiers(lhs, rhs),
            StorageType::Mqtt => MqttReader::merge_two_frontiers(lhs, rhs),
            StorageType::Kinesis => KinesisReader::merge_two_frontiers(lhs, rhs),
        }
    }
}

pub trait Reader {
    fn read(&mut self) -> Result<ReadResult, ReadError>;

    #[allow(clippy::missing_errors_doc)]
    fn seek(&mut self, frontier: &OffsetAntichain) -> Result<(), ReadError>;

    fn short_description(&self) -> Cow<'static, str> {
        type_name::<Self>().into()
    }

    fn initialize_cached_objects_storage(
        &mut self,
        _: &mut WorkerPersistentStorage,
        _: PersistentId,
    ) -> Result<(), ReadError> {
        Ok(())
    }

    fn merge_two_frontiers(lhs: &OffsetAntichain, rhs: &OffsetAntichain) -> OffsetAntichain
    where
        Self: Sized,
    {
        let mut result = lhs.clone();
        for (offset_key, other_value) in rhs {
            match result.get_offset(offset_key) {
                Some(offset_value) => match (offset_value, other_value) {
                    (
                        OffsetValue::KafkaOffset(offset_position),
                        OffsetValue::KafkaOffset(other_position),
                    ) => {
                        if other_position > offset_position {
                            result.advance_offset(offset_key.clone(), other_value.clone());
                        }
                    }
                    (
                        OffsetValue::PythonCursor {
                            total_entries_read: offset_position,
                            ..
                        },
                        OffsetValue::PythonCursor {
                            total_entries_read: other_position,
                            ..
                        },
                    ) => {
                        if other_position > offset_position {
                            result.advance_offset(offset_key.clone(), other_value.clone());
                        }
                    }
                    (
                        OffsetValue::FilePosition {
                            total_entries_read: offset_line_idx,
                            ..
                        },
                        OffsetValue::FilePosition {
                            total_entries_read: other_line_idx,
                            ..
                        },
                    )
                    | (
                        OffsetValue::S3ObjectPosition {
                            total_entries_read: offset_line_idx,
                            ..
                        },
                        OffsetValue::S3ObjectPosition {
                            total_entries_read: other_line_idx,
                            ..
                        },
                    )
                    | (
                        OffsetValue::PosixLikeOffset {
                            total_entries_read: offset_line_idx,
                            ..
                        },
                        OffsetValue::PosixLikeOffset {
                            total_entries_read: other_line_idx,
                            ..
                        },
                    ) => {
                        if other_line_idx > offset_line_idx {
                            result.advance_offset(offset_key.clone(), other_value.clone());
                        }
                    }
                    (
                        OffsetValue::DeltaTablePosition {
                            version: offset_version,
                            rows_read_within_version: offset_position,
                            ..
                        },
                        OffsetValue::DeltaTablePosition {
                            version: other_version,
                            rows_read_within_version: other_position,
                            ..
                        },
                    ) => {
                        if (other_version, other_position) > (offset_version, offset_position) {
                            result.advance_offset(offset_key.clone(), other_value.clone());
                        }
                    }
                    (
                        OffsetValue::KinesisOffset(offset_position),
                        OffsetValue::KinesisOffset(other_position),
                    ) => {
                        // Kinesis offsets are integers without clear bound, hence we compare them as strings
                        if other_position.len() > offset_position.len()
                            || (other_position.len() == offset_position.len()
                                && other_position > offset_position)
                        {
                            result.advance_offset(offset_key.clone(), other_value.clone());
                        }
                    }
                    (_, _) => {
                        error!("Incomparable offsets in the frontier: {offset_value:?} and {other_value:?}");
                    }
                },
                None => result.advance_offset(offset_key.clone(), other_value.clone()),
            }
        }
        result
    }

    fn storage_type(&self) -> StorageType;

    fn max_allowed_consecutive_errors(&self) -> usize {
        0
    }
}

pub trait ReaderBuilder: Send + 'static {
    fn build(self: Box<Self>) -> Result<Box<dyn Reader>, ReadError>;

    fn is_internal(&self) -> bool {
        false
    }

    fn storage_type(&self) -> StorageType;
    fn short_description(&self) -> Cow<'static, str>;
    fn name(&self, unique_name: Option<&UniqueName>) -> String;
}

impl<T> ReaderBuilder for T
where
    T: Reader + Send + 'static,
{
    fn build(self: Box<Self>) -> Result<Box<dyn Reader>, ReadError> {
        Ok(self)
    }

    fn storage_type(&self) -> StorageType {
        Reader::storage_type(self)
    }

    fn short_description(&self) -> Cow<'static, str> {
        Reader::short_description(self)
    }

    fn name(&self, unique_name: Option<&UniqueName>) -> String {
        if let Some(unique_name) = unique_name {
            unique_name.clone()
        } else {
            let desc = self.short_description();
            desc.split("::").last().unwrap().replace("Builder", "")
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum WriteError {
    #[error(transparent)]
    Io(#[from] io::Error),

    #[error(transparent)]
    Kafka(#[from] KafkaError),

    #[error("failed to perform S3 operation {0:?} reason: {1:?}")]
    S3(S3CommandName, S3Error),

    #[error("failed to perform write in postgres: {0}")]
    Postgres(#[from] postgres::Error),

    #[error(transparent)]
    Utf8(#[from] Utf8Error),

    #[error(transparent)]
    Bincode(#[from] BincodeError),

    #[error(transparent)]
    DeltaTable(#[from] DeltaTableError),

    #[error(transparent)]
    Arrow(#[from] ArrowError),

    #[error(transparent)]
    NatsPublish(#[from] NatsPublishError),

    #[error(transparent)]
    MqttPublish(#[from] MqttClientError),

    #[error(transparent)]
    MqttPoll(#[from] MqttConnectionError),

    #[error(transparent)]
    NatsFlush(#[from] NatsFlushError),

    #[error(transparent)]
    JetStream(#[from] NatsError<JetStreamPublishError>),

    #[error(transparent)]
    IcebergError(#[from] IcebergError),

    #[error(transparent)]
    QuestDBError(#[from] QuestDBError),

    #[error("the 'at' QuestDB column is not of the time type: {0}")]
    QuestDBAtColumnNotTime(Value),

    #[error("type mismatch with delta table schema: got {0} expected {1}")]
    TypeMismatchWithSchema(Value, ArrowDataType),

    #[error("integer value {0} out of range")]
    IntOutOfRange(i64),

    #[error("value {0} can't be used as a key because it's neither 'bytes' nor 'string'")]
    IncorrectKeyFieldType(Value),

    #[error("unsupported type: {0:?}")]
    UnsupportedType(Type),

    #[error("query {query:?} failed: {error}")]
    PsqlQueryFailed {
        query: String,
        error: postgres::Error,
    },

    #[error("elasticsearch client error: {0:?}")]
    Elasticsearch(elasticsearch::Error),

    #[error(transparent)]
    Persistence(#[from] PersistenceBackendError),

    #[error(transparent)]
    Formatter(#[from] FormatterError),

    #[error(transparent)]
    MongoDB(#[from] MongoError),

    #[error(transparent)]
    Mysql(#[from] MysqlError),

    #[error("dynamic topic name is not a string field: {0}")]
    DynamicTopicIsNotAString(Value),

    #[error("delta table schema mismatch: {0}")]
    DeltaTableSchemaMismatch(DeltaSchemaMismatchDetails),

    #[error("table {0} doesn't exist in the destination storage")]
    TableDoesNotExist(String),

    #[error("table written in snapshot mode has a duplicate primary key: {0:?}")]
    TableAlreadyContainsKey(Key),

    #[error("table written in snapshot mode doesn't have the primary key requested to be deleted: {0:?}")]
    TableDoesntContainKey(Key),

    #[error("the snapshot of the existing data in the output delta table does not correspond to the schema: {0}")]
    IncorrectInitialSnapshot(IncorrectSnapshotError),

    #[error(transparent)]
    AwsBuildRequest(#[from] AwsBuildError),

    #[error(transparent)]
    DynamoDB(#[from] AwsDynamoDBError),

    #[error(transparent)]
    Kinesis(#[from] AwsKinesisError),

    #[error("after several retried attempts, {0} items haven't been saved")]
    SomeItemsNotDelivered(usize),

    #[error("the type {0} can't be used in the index")]
    NotIndexType(Type),

    #[error("the field '{0}' isn't present among the given value fields")]
    FieldNotFound(String),

    #[error("primary key field names must be specified for a snapshot mode")]
    EmptyKeyFieldsForSnapshot,
}

pub trait Writer: Send {
    fn write(&mut self, data: FormatterContext) -> Result<(), WriteError>;

    fn flush(&mut self, _forced: bool) -> Result<(), WriteError> {
        Ok(())
    }

    fn retriable(&self) -> bool {
        false
    }

    fn single_threaded(&self) -> bool {
        true
    }

    fn name(&self) -> String {
        let short_description: Cow<'static, str> = type_name::<Self>().into();
        short_description.split("::").last().unwrap().to_string()
    }
}

pub struct FileWriter {
    writer: BufWriter<std::fs::File>,
    output_path: String,
}

impl FileWriter {
    pub fn new(writer: BufWriter<std::fs::File>, output_path: String) -> FileWriter {
        FileWriter {
            writer,
            output_path,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReadMethod {
    ByLine,
    Full,
}

impl ReadMethod {
    pub fn read_next_bytes<R>(self, reader: &mut R, buf: &mut Vec<u8>) -> Result<usize, ReadError>
    where
        R: BufRead,
    {
        match &self {
            ReadMethod::ByLine => Ok(reader.read_until(b'\n', buf)?),
            ReadMethod::Full => Ok(reader.read_to_end(buf)?),
        }
    }
}

impl Writer for FileWriter {
    fn write(&mut self, data: FormatterContext) -> Result<(), WriteError> {
        for payload in data.payloads {
            self.writer.write_all(&payload.into_raw_bytes()?)?;
            self.writer.write_all(b"\n")?;
        }
        Ok(())
    }

    fn flush(&mut self, _forced: bool) -> Result<(), WriteError> {
        self.writer.flush()?;
        Ok(())
    }

    fn name(&self) -> String {
        format!("FileSystem({})", self.output_path)
    }
}

pub struct RdkafkaWatermark {
    pub low: i64,
    pub high: i64,
}

impl RdkafkaWatermark {
    pub fn new(low: i64, high: i64) -> Self {
        Self { low, high }
    }

    /// Checks if the offset is contained within a watermark.
    /// The offset can't be lower than the lower bound of the
    /// watermark, but it can pass beyond the upper bound, if
    /// new data were written after the pipeline start.
    pub fn contains_offset(&self, offset: i64) -> bool {
        offset < self.high
    }

    /// The given watermark has messages if the next used offset is
    /// greater than the beginning of the observed interval.
    /// The beginning of an empty interval can still be non-zero
    /// if topic compaction took place.
    pub fn has_messages(&self) -> bool {
        self.low < self.high
    }

    /// The `self.high` is the offset of the next message that will
    /// be written into partition, therefore the partition has more
    /// messages if the provided offset doesn't correspond to the last
    /// message in the watermark.
    pub fn has_messages_after_offset(&self, offset: i64) -> bool {
        offset < self.high - 1
    }
}

pub struct KafkaReader {
    consumer: BaseConsumer<DefaultConsumerContext>,
    topic: ArcStr,
    positions_for_seek: HashMap<i32, KafkaOffset>,
    watermarks: Vec<RdkafkaWatermark>,
    deferred_read_result: Option<ReadResult>,
    mode: ConnectorMode,
}

impl Reader for KafkaReader {
    fn read(&mut self) -> Result<ReadResult, ReadError> {
        if let Some(deferred_read_result) = take(&mut self.deferred_read_result) {
            return Ok(deferred_read_result);
        }

        loop {
            let kafka_message = match self.mode {
                ConnectorMode::Streaming => self
                    .consumer
                    .poll(None)
                    .expect("poll in streaming mode should never timeout")?,
                ConnectorMode::Static => {
                    if let Some(kafka_message) = self.next_message_in_static_mode()? {
                        kafka_message
                    } else {
                        return Ok(ReadResult::Finished);
                    }
                }
            };
            let message_key = kafka_message.key().map(<[u8]>::to_vec);
            let message_payload = kafka_message.payload().map(<[u8]>::to_vec);

            if let Some(lazy_seek_offset) = self.positions_for_seek.get(&kafka_message.partition())
            {
                info!(
                    "Performing Kafka topic seek for ({}, {}) to {:?}",
                    kafka_message.topic(),
                    kafka_message.partition(),
                    lazy_seek_offset
                );
                // If there is a need for seek, perform it and remove the seek requirement.
                if let Err(e) = self.consumer.seek(
                    kafka_message.topic(),
                    kafka_message.partition(),
                    *lazy_seek_offset,
                    None,
                ) {
                    error!(
                        "Failed to seek topic and partition ({}, {}) to offset {:?}: {e}",
                        kafka_message.topic(),
                        kafka_message.partition(),
                        lazy_seek_offset,
                    );
                } else {
                    self.positions_for_seek.remove(&kafka_message.partition());
                }
                continue;
            }

            let offset = {
                let offset_key = OffsetKey::Kafka(self.topic.clone(), kafka_message.partition());
                let offset_value = OffsetValue::KafkaOffset(kafka_message.offset());
                (offset_key, offset_value)
            };
            let metadata = KafkaMetadata::from_rdkafka_message(&kafka_message);
            let message = ReaderContext::from_key_value(message_key, message_payload);
            self.deferred_read_result = Some(ReadResult::Data(message, offset));

            return Ok(ReadResult::NewSource(metadata.into()));
        }
    }

    fn seek(&mut self, frontier: &OffsetAntichain) -> Result<(), ReadError> {
        // "Lazy" seek implementation
        for (offset_key, offset_value) in frontier {
            let OffsetValue::KafkaOffset(position) = offset_value else {
                warn!("Unexpected type of offset in Kafka frontier: {offset_value:?}");
                continue;
            };
            if let OffsetKey::Kafka(topic, partition) = offset_key {
                if self.topic != *topic {
                    warn!(
                        "Unexpected topic name. Expected: {}, Got: {topic}",
                        self.topic
                    );
                    continue;
                }

                /*
                    Note: we can't do seek straight away, because it works only for
                    assigned partitions.

                    We also don't do any kind of assignment here, because it needs
                    to be done on behalf of rdkafka client, taking account of other
                    members in its' consumer group.
                */
                self.positions_for_seek
                    .insert(*partition, KafkaOffset::Offset(*position + 1));
            } else {
                error!("Unexpected offset in Kafka frontier: ({offset_key:?}, {offset_value:?})");
            }
        }

        Ok(())
    }

    fn short_description(&self) -> Cow<'static, str> {
        format!("Kafka({})", self.topic).into()
    }

    fn storage_type(&self) -> StorageType {
        StorageType::Kafka
    }

    fn max_allowed_consecutive_errors(&self) -> usize {
        32
    }
}

impl KafkaReader {
    pub fn new(
        consumer: BaseConsumer<DefaultConsumerContext>,
        topic: String,
        positions_for_seek: HashMap<i32, KafkaOffset>,
        watermarks: Vec<RdkafkaWatermark>,
        mode: ConnectorMode,
    ) -> KafkaReader {
        KafkaReader {
            consumer,
            topic: topic.into(),
            positions_for_seek,
            watermarks,
            mode,
            deferred_read_result: None,
        }
    }

    fn poll_duration_for_static_mode() -> Duration {
        Duration::from_millis(500)
    }

    /// Default timeout for different broker metadata requests
    pub fn default_timeout() -> Duration {
        Duration::from_secs(30)
    }

    fn polling_attempts_count_for_static_mode() -> usize {
        60
    }

    fn message_matches_static_read_constraints(&self, message: &BorrowedMessage<'_>) -> bool {
        let partition: usize = message
            .partition()
            .try_into()
            .expect("kafka partition can't be negative");
        if partition >= self.watermarks.len() {
            // New partitions have been added after the boundaries for the
            // chunk to be read have been computed. In this case, the message
            // must be skipped.
            return false;
        }
        self.watermarks[partition].contains_offset(message.offset())
    }

    fn static_read_has_finished(&self) -> Result<bool, ReadError> {
        let total_partitions = self.watermarks.len();
        let mut tpl = TopicPartitionList::with_capacity(total_partitions);
        for partition_idx in 0..total_partitions {
            tpl.add_partition(
                self.topic.as_str(),
                partition_idx
                    .try_into()
                    .expect("kafka partition must fit 32-bit signed integer"),
            );
        }
        let committed_offsets = self
            .consumer
            .committed_offsets(tpl, Self::default_timeout())?;
        for committed_offset in committed_offsets.elements() {
            let partition: usize = committed_offset
                .partition()
                .try_into()
                .expect("kafka partition can't be negative");
            let offset = match committed_offset.offset() {
                KafkaOffset::End => {
                    // The exact offset is not reported, but it's greater than the
                    // threshold.
                    continue;
                }
                KafkaOffset::Invalid => {
                    if self.watermarks[partition].has_messages() {
                        return Ok(false);
                    }
                    // It is OK to have unassigned offsets for empty partitions.
                    continue;
                }
                KafkaOffset::Offset(offset) => offset,
                _ => {
                    // There is no way to compare this offset to the desired border.
                    return Ok(false);
                }
            };
            if self.watermarks[partition].has_messages_after_offset(offset) {
                // The committed offset is still smaller than the last offset to be read
                // from this partition.
                return Ok(false);
            }
        }

        Ok(true)
    }

    fn next_message_in_static_mode(&self) -> Result<Option<BorrowedMessage<'_>>, ReadError> {
        let mut result_message = None;
        let mut is_finished = false;
        let n_attempts = Self::polling_attempts_count_for_static_mode();
        for _ in 0..n_attempts {
            let maybe_kafka_message = self.consumer.poll(Self::poll_duration_for_static_mode());
            if let Some(maybe_matching_message) = maybe_kafka_message {
                let maybe_matching_message = maybe_matching_message?;
                if self.message_matches_static_read_constraints(&maybe_matching_message) {
                    result_message = Some(maybe_matching_message);
                    break;
                }

                // The message goes beyond the specified border within the partition or belongs
                // a partition that must not be read at all.
                // Stop reading the further messages from this partition, since they will
                // have greater offsets.
                let mut tpl = TopicPartitionList::with_capacity(1);
                tpl.add_partition(self.topic.as_str(), maybe_matching_message.partition());
                self.consumer.pause(&tpl)?;
            }

            if self.static_read_has_finished()? {
                is_finished = true;
                break;
            }
        }
        if !is_finished && result_message.is_none() {
            warn!("There was no explicit finish detected from Kafka topic '{}', but no matching events were read after {n_attempts} attempts, with {:?} duration each.", self.topic, Self::poll_duration_for_static_mode());
        }
        Ok(result_message)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConnectorMode {
    Static,
    Streaming,
}

impl ConnectorMode {
    pub fn is_polling_enabled(self) -> bool {
        match self {
            ConnectorMode::Static => false,
            ConnectorMode::Streaming => true,
        }
    }
}

pub struct PythonReaderBuilder {
    subject: Py<PythonSubject>,
    schema: HashMap<String, Type>,
}

pub struct PythonReader {
    subject: Py<PythonSubject>,
    schema: HashMap<String, Type>,
    total_entries_read: u64,
    current_external_offset: Arc<[u8]>,
    is_initialized: bool,
    is_finished: bool,

    #[allow(unused)]
    python_thread_state: PythonThreadState,
}

impl PythonReaderBuilder {
    pub fn new(subject: Py<PythonSubject>, schema: HashMap<String, Type>) -> Self {
        Self { subject, schema }
    }
}

impl ReaderBuilder for PythonReaderBuilder {
    fn build(self: Box<Self>) -> Result<Box<dyn Reader>, ReadError> {
        let python_thread_state = PythonThreadState::new();
        let Self { subject, schema } = *self;

        Ok(Box::new(PythonReader {
            subject,
            schema,
            python_thread_state,
            total_entries_read: 0,
            is_initialized: false,
            is_finished: false,
            current_external_offset: vec![].into(),
        }))
    }

    fn short_description(&self) -> Cow<'static, str> {
        type_name::<Self>().into()
    }

    fn name(&self, unique_name: Option<&UniqueName>) -> String {
        if let Some(unique_name) = unique_name {
            unique_name.clone()
        } else {
            let desc = self.short_description();
            desc.split("::").last().unwrap().replace("Builder", "")
        }
    }

    fn is_internal(&self) -> bool {
        self.subject.get().is_internal
    }

    fn storage_type(&self) -> StorageType {
        StorageType::Python
    }
}

impl PythonReader {
    fn conversion_error(
        ob: &Bound<PyAny>,
        name: String,
        type_: Type,
        err: &PyErr,
    ) -> ConversionError {
        let value_repr = limit_length(format!("{ob}"), STANDARD_OBJECT_LENGTH_LIMIT);
        ConversionError::new(value_repr, name, type_, Some(err.to_string()))
    }

    fn current_offset(&self) -> Offset {
        (
            OffsetKey::Empty,
            OffsetValue::PythonCursor {
                total_entries_read: self.total_entries_read,
                raw_external_offset: self.current_external_offset.clone(),
            },
        )
    }
}

const PW_OFFSET_FIELD_NAME: &str = "_pw_offset";

impl Reader for PythonReader {
    fn seek(&mut self, frontier: &OffsetAntichain) -> Result<(), ReadError> {
        Python::with_gil(|py| {
            self.subject
                .borrow(py)
                .on_persisted_run
                .call0(py)
                .map_err(ReadError::Py)
        })?;

        let offset_value = frontier.get_offset(&OffsetKey::Empty);
        let Some(OffsetValue::PythonCursor {
            total_entries_read,
            raw_external_offset,
        }) = offset_value
        else {
            if offset_value.is_some() {
                warn!("Incorrect type of offset value in Python frontier: {offset_value:?}");
            }
            return Ok(());
        };

        self.total_entries_read = *total_entries_read;
        if !raw_external_offset.is_empty() {
            Python::with_gil(|py| {
                let data: Vec<u8> = raw_external_offset.to_vec();
                let py_external_offset = PyBytes::new(py, &data).unbind().into_any();
                self.subject
                    .borrow(py)
                    .seek
                    .call1(py, (py_external_offset.borrow(),))
                    .map_err(ReadError::Py)
            })?;
            self.current_external_offset = raw_external_offset.clone();
        }

        Ok(())
    }

    fn read(&mut self) -> Result<ReadResult, ReadError> {
        if !self.is_initialized {
            Python::with_gil(|py| self.subject.borrow(py).start.call0(py))?;
            self.is_initialized = true;
        }
        if self.is_finished {
            return Ok(ReadResult::Finished);
        }

        Python::with_gil(|py| {
            let (py_event, key, objects): (
                PythonConnectorEventType,
                Option<Value>,
                HashMap<String, Py<PyAny>>,
            ) = self
                .subject
                .borrow(py)
                .read
                .call0(py)?
                .extract(py)
                .map_err(ReadError::Py)?;

            let event = match py_event {
                PythonConnectorEventType::Insert => DataEventType::Insert,
                PythonConnectorEventType::Delete => DataEventType::Delete,
                PythonConnectorEventType::ExternalOffset => {
                    let py_external_offset =
                        objects.get(PW_OFFSET_FIELD_NAME).unwrap_or_else(|| {
                            panic!(
                                "In ExternalOffset event '{PW_OFFSET_FIELD_NAME}' must be present"
                            )
                        });
                    self.current_external_offset = py_external_offset
                        .extract::<Vec<u8>>(py)
                        .expect("ExternalOffset must be bytes")
                        .into();
                    return Ok(ReadResult::Data(
                        ReaderContext::Empty,
                        self.current_offset(),
                    ));
                }
            };

            let key = key.map(|key| vec![key]);
            let mut values = HashMap::with_capacity(objects.len());
            for (name, ob) in objects {
                let dtype = self.schema.get(&name).unwrap_or(&Type::Any); // Any for special values
                let value = extract_value(ob.bind(py), dtype).map_err(|err| {
                    Box::new(Self::conversion_error(
                        ob.bind(py),
                        name.clone(),
                        dtype.clone(),
                        &err,
                    ))
                });
                values.insert(name, value);
            }
            let values: ValuesMap = values.into();

            if event != DataEventType::Insert && !self.subject.borrow(py).deletions_enabled {
                return Err(ReadError::Py(PyValueError::new_err(
                    "Trying to modify a row in the Python connector but deletions_enabled is set to False.",
                )));
            }

            if let Some(special_value) = values.get_special() {
                match special_value {
                    SpecialEvent::Finish => {
                        self.is_finished = true;
                        self.subject.borrow(py).end.call0(py)?;
                        return Ok(ReadResult::Finished);
                    }
                    SpecialEvent::EnableAutocommits => {
                        return Ok(ReadResult::FinishedSource {
                            commit_possibility: CommitPossibility::Possible,
                        })
                    }
                    SpecialEvent::DisableAutocommits => {
                        return Ok(ReadResult::FinishedSource {
                            commit_possibility: CommitPossibility::Forbidden,
                        })
                    }
                    SpecialEvent::Commit => {}
                }
            }
            // We use simple sequential offset because Python connector is single threaded, as
            // by default.
            //
            // If it's changed, add worker_id to the offset.
            self.total_entries_read += 1;
            Ok(ReadResult::Data(
                ReaderContext::from_diff(event, key, values),
                self.current_offset(),
            ))
        })
    }

    fn storage_type(&self) -> StorageType {
        StorageType::Python
    }
}

pub struct PsqlWriter {
    client: PsqlClient,
    max_batch_size: Option<usize>,
    buffer: Vec<FormatterContext>,
    snapshot_mode: bool,
    table_name: String,
    query: SqlQueryTemplate,
    legacy_mode: bool,
}

impl PsqlWriter {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        mut client: PsqlClient,
        max_batch_size: Option<usize>,
        snapshot_mode: bool,
        table_name: &str,
        value_fields: &[ValueField],
        key_field_names: Option<&[String]>,
        init_mode: TableWriterInitMode,
        legacy_mode: bool,
    ) -> Result<PsqlWriter, WriteError> {
        let mut transaction = client.transaction()?;
        init_mode.initialize(
            table_name,
            value_fields,
            key_field_names,
            !snapshot_mode || legacy_mode,
            |query| {
                transaction.execute(query, &[])?;
                Ok(())
            },
            Self::postgres_data_type,
        )?;
        transaction.commit()?;

        let query = SqlQueryTemplate::new(
            snapshot_mode,
            table_name,
            value_fields,
            key_field_names,
            legacy_mode,
            |index| format!("${}", index + 1),
            Self::on_insert_conflict_condition,
        )?;

        let writer = PsqlWriter {
            client,
            max_batch_size,
            query,
            buffer: Vec::new(),
            snapshot_mode,
            table_name: table_name.to_string(),
            legacy_mode,
        };

        Ok(writer)
    }

    // Generates a statement that is used in "on conflict" part of the query
    fn on_insert_conflict_condition(
        table_name: &str,
        key_field_names: &[String],
        value_fields: &[ValueField],
        legacy_mode: bool,
    ) -> String {
        let mut value_field_positions = Vec::new();
        let mut key_field_positions = vec![0; key_field_names.len()];
        for (value_index, value_field) in value_fields.iter().enumerate() {
            value_field_positions.push(value_index);
            for (key_index, key_field) in key_field_names.iter().enumerate() {
                if *key_field == value_field.name {
                    value_field_positions.pop();
                    key_field_positions[key_index] = value_index;
                    break;
                }
            }
        }

        let mut legacy_update_pairs = Vec::with_capacity(2);
        if legacy_mode {
            legacy_update_pairs.push(format!("time=${}", value_fields.len() + 1));
            legacy_update_pairs.push(format!("diff=${}", value_fields.len() + 2));
        }

        let update_pairs = value_field_positions
            .iter()
            .map(|position| format!("{}=${}", value_fields[*position].name, *position + 1))
            .chain(legacy_update_pairs)
            .join(",");

        if update_pairs.is_empty() {
            format!(
                "ON CONFLICT ({}) DO NOTHING",
                key_field_names.iter().join(",")
            )
        } else {
            let update_condition = key_field_positions
                .iter()
                .map(|position| {
                    format!(
                        "{}.{}=${}",
                        table_name,
                        value_fields[*position].name,
                        *position + 1
                    )
                })
                .join(" AND ");

            format!(
                "ON CONFLICT ({}) DO UPDATE SET {update_pairs} WHERE {update_condition}",
                key_field_names.iter().join(",")
            )
        }
    }

    fn postgres_data_type(type_: &Type, is_nested: bool) -> Result<String, WriteError> {
        let not_null_suffix = if is_nested { "" } else { " NOT NULL" };
        Ok(match type_ {
            Type::Bool => format!("BOOLEAN{not_null_suffix}"),
            Type::Int | Type::Duration => format!("BIGINT{not_null_suffix}"),
            Type::Float => format!("DOUBLE PRECISION{not_null_suffix}"),
            Type::Pointer | Type::String => format!("TEXT{not_null_suffix}"),
            Type::Bytes | Type::PyObjectWrapper => format!("BYTEA{not_null_suffix}"),
            Type::Json => format!("JSONB{not_null_suffix}"),
            Type::DateTimeNaive => format!("TIMESTAMP{not_null_suffix}"),
            Type::DateTimeUtc => format!("TIMESTAMPTZ{not_null_suffix}"),
            Type::Optional(wrapped) | Type::List(wrapped) => {
                if let Type::Any = **wrapped {
                    return Err(WriteError::UnsupportedType(type_.clone()));
                }

                let wrapped = Self::postgres_data_type(wrapped, true)?;
                if let Type::Optional(_) = type_ {
                    return Ok(wrapped);
                }
                format!("{wrapped}[]{not_null_suffix}")
            }
            Type::Tuple(fields) => {
                let mut iter = fields.iter();
                if !fields.is_empty() && iter.all(|field| field == &fields[0]) {
                    let first = Self::postgres_data_type(&fields[0], true)?;
                    return Ok(format!("{first}[]{not_null_suffix}"));
                }
                return Err(WriteError::UnsupportedType(type_.clone()));
            }
            Type::Any | Type::Array(_, _) | Type::Future(_) => {
                return Err(WriteError::UnsupportedType(type_.clone()))
            }
        })
    }
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum TableWriterInitMode {
    Default,
    CreateIfNotExists,
    Replace,
}

impl TableWriterInitMode {
    pub fn initialize(
        self,
        table_name: &str,
        value_fields: &[ValueField],
        key_field_names: Option<&[String]>,
        include_special_fields: bool,
        mut execute_query: impl FnMut(&str) -> Result<(), WriteError>,
        to_db_type: impl FnMut(&Type, bool) -> Result<String, WriteError>,
    ) -> Result<(), WriteError> {
        match self {
            TableWriterInitMode::Default => return Ok(()),
            TableWriterInitMode::Replace | TableWriterInitMode::CreateIfNotExists => {
                if self == TableWriterInitMode::Replace {
                    Self::drop_table_if_exists(table_name, &mut execute_query)?;
                }
                Self::create_table_if_not_exists(
                    table_name,
                    value_fields,
                    key_field_names,
                    include_special_fields,
                    &mut execute_query,
                    to_db_type,
                )?;
            }
        }

        Ok(())
    }

    fn create_table_if_not_exists(
        table_name: &str,
        schema: &[ValueField],
        key_field_names: Option<&[String]>,
        include_special_fields: bool,
        mut execute_query: impl FnMut(&str) -> Result<(), WriteError>,
        mut to_db_type: impl FnMut(&Type, bool) -> Result<String, WriteError>,
    ) -> Result<(), WriteError> {
        let columns: Vec<String> = schema
            .iter()
            .map(|item| {
                to_db_type(&item.type_, false).map(|dtype_str| format!("{} {dtype_str}", item.name))
            })
            .collect::<Result<Vec<_>, _>>()?;

        let primary_key = key_field_names
            .as_ref()
            .filter(|keys| !keys.is_empty())
            .map_or(String::new(), |keys| {
                format!(", PRIMARY KEY ({})", keys.join(", "))
            });

        let maybe_special_fields = if include_special_fields {
            ", time BIGINT NOT NULL, diff SMALLINT NOT NULL"
        } else {
            ""
        };

        let query = format!(
            "CREATE TABLE IF NOT EXISTS {table_name} ({} {maybe_special_fields} {primary_key})",
            columns.join(", ")
        );

        execute_query(&query)
    }

    fn drop_table_if_exists(
        table_name: &str,
        mut execute_query: impl FnMut(&str) -> Result<(), WriteError>,
    ) -> Result<(), WriteError> {
        let query = format!("DROP TABLE IF EXISTS {table_name}");
        execute_query(&query)
    }
}

mod to_sql {
    use std::error::Error;
    use std::ops::Deref;

    use bytes::BytesMut;
    use chrono::{DateTime, NaiveDateTime, Utc};
    use half::f16;
    use numpy::Ix1;
    use ordered_float::OrderedFloat;
    use pgvector::{HalfVector, Vector};
    use postgres::types::{to_sql_checked, Format, IsNull, ToSql, Type};

    use crate::engine::time::DateTime as _;
    use crate::engine::Value;

    #[derive(Debug, Clone, thiserror::Error)]
    #[error("cannot convert value of type {pathway_type:?} to Postgres type {postgres_type}")]
    struct WrongPathwayType {
        pathway_type: String,
        postgres_type: Type,
    }

    impl ToSql for Value {
        fn to_sql(
            &self,
            ty: &Type,
            out: &mut BytesMut,
        ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
            macro_rules! try_forward {
                ($type:ty, $expr:expr) => {
                    if <$type as ToSql>::accepts(ty) {
                        let value: $type = $expr.try_into()?;
                        assert!(matches!(self.encode_format(ty), Format::Binary));
                        assert!(matches!(value.encode_format(ty), Format::Binary));
                        return value.to_sql(ty, out);
                    }
                };
            }
            #[allow(clippy::match_same_arms)]
            let pathway_type = match self {
                Self::None => return Ok(IsNull::Yes),
                Self::Bool(b) => {
                    try_forward!(bool, *b);
                    "bool"
                }
                Self::Int(i) => {
                    try_forward!(i64, *i);
                    try_forward!(i32, *i);
                    try_forward!(i16, *i);
                    try_forward!(i8, *i);
                    #[allow(clippy::cast_precision_loss)]
                    {
                        try_forward!(f64, *i as f64);
                        try_forward!(f32, *i as f32);
                    }
                    "int"
                }
                Self::Float(OrderedFloat(f)) => {
                    try_forward!(f64, *f);
                    #[allow(clippy::cast_possible_truncation)]
                    {
                        try_forward!(f32, *f as f32);
                    }
                    "float"
                }
                Self::Pointer(p) => {
                    try_forward!(String, p.to_string());
                    "pointer"
                }
                Self::String(s) => {
                    try_forward!(&str, s.as_str());
                    "string"
                }
                Self::Bytes(b) => {
                    try_forward!(&[u8], &b[..]);
                    "bytes"
                }
                Self::Tuple(t) => {
                    try_forward!(&[Value], &t[..]);
                    "tuple"
                }
                Self::IntArray(_) => "int array", // TODO
                #[allow(clippy::cast_possible_truncation)]
                Self::FloatArray(a) => {
                    // TODO regular arrays
                    if a.ndim() == 1 {
                        let v = a
                            .deref()
                            .clone()
                            .into_dimensionality::<Ix1>()
                            .expect("ndim == 1")
                            .to_vec();
                        let v_32: Vec<f32> = v.iter().map(|e| *e as f32).collect();
                        try_forward!(Vector, Vector::from(v_32));
                        let v_16: Vec<f16> = v.iter().map(|e| f16::from_f64(*e)).collect();
                        try_forward!(HalfVector, HalfVector::from(v_16));
                    }
                    "float array"
                }
                Self::DateTimeNaive(dt) => {
                    try_forward!(NaiveDateTime, dt.as_chrono_datetime());
                    "naive date/time"
                }
                Self::DateTimeUtc(dt) => {
                    try_forward!(DateTime<Utc>, dt.as_chrono_datetime().and_utc());
                    "UTC date/time"
                }
                Self::Duration(dr) => {
                    try_forward!(i64, dr.microseconds());
                    "duration"
                }
                Self::Json(j) => {
                    try_forward!(&serde_json::Value, &**j);
                    "JSON"
                }
                Self::Error => "error",
                Self::PyObjectWrapper(_) => {
                    try_forward!(Vec<u8>, bincode::serialize(self).map_err(|e| *e)?);
                    "python object"
                }
                Self::Pending => "pending",
            };
            Err(Box::new(WrongPathwayType {
                pathway_type: pathway_type.to_owned(),
                postgres_type: ty.clone(),
            }))
        }

        fn accepts(_ty: &Type) -> bool {
            true // we double-check anyway
        }

        to_sql_checked!();
    }
}

impl Writer for PsqlWriter {
    fn write(&mut self, data: FormatterContext) -> Result<(), WriteError> {
        self.buffer.push(data);
        if let Some(max_batch_size) = self.max_batch_size {
            if self.buffer.len() == max_batch_size {
                self.flush(true)?;
            }
        }
        Ok(())
    }

    fn flush(&mut self, _forced: bool) -> Result<(), WriteError> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let mut transaction = self.client.transaction()?;

        for data in self.buffer.drain(..) {
            // We reuse `Value`'s serialization to pass additional `time` and `diff` columns.
            let diff = Value::Int(data.diff.try_into().unwrap());
            let time = Value::Int(data.time.0.try_into().unwrap());
            let mut params: Vec<_> = data
                .values
                .iter()
                .map(|v| v as &(dyn ToSql + Sync))
                .collect();
            let params = if self.snapshot_mode {
                if data.diff > 0 && self.legacy_mode {
                    params.push(&time);
                    params.push(&diff);
                    params
                } else if data.diff > 0 && !self.legacy_mode {
                    params
                } else {
                    self.query.primary_key_fields(params)
                }
            } else {
                params.push(&time);
                params.push(&diff);
                params
            };

            let query = self.query.build_query(data.diff);
            transaction
                .execute(&query, params.as_slice())
                .map_err(|error| WriteError::PsqlQueryFailed {
                    query: query.clone(),
                    error,
                })?;
        }

        transaction.commit()?;

        Ok(())
    }

    fn name(&self) -> String {
        format!("Postgres({})", self.table_name)
    }

    fn single_threaded(&self) -> bool {
        self.snapshot_mode
    }
}

pub struct KafkaWriter {
    producer: ThreadedProducer<DefaultProducerContext>,
    topic: MessageQueueTopic,
    header_fields: Vec<(String, usize)>,
    key_field_index: Option<usize>,
}

impl KafkaWriter {
    pub fn new(
        producer: ThreadedProducer<DefaultProducerContext>,
        topic: MessageQueueTopic,
        header_fields: Vec<(String, usize)>,
        key_field_index: Option<usize>,
    ) -> KafkaWriter {
        KafkaWriter {
            producer,
            topic,
            header_fields,
            key_field_index,
        }
    }
}

impl Drop for KafkaWriter {
    fn drop(&mut self) {
        self.producer.flush(None).expect("kafka commit should work");
    }
}

impl Writer for KafkaWriter {
    fn write(&mut self, data: FormatterContext) -> Result<(), WriteError> {
        let key_as_bytes = match self.key_field_index {
            Some(index) => match &data.values[index] {
                Value::Bytes(bytes) => bytes.to_vec(),
                Value::String(string) => string.as_bytes().to_vec(),
                _ => {
                    return Err(WriteError::IncorrectKeyFieldType(
                        data.values[index].clone(),
                    ))
                }
            },
            None => data.key.0.to_le_bytes().to_vec(),
        };

        let headers = data.construct_kafka_headers(&self.header_fields);
        for payload in data.payloads {
            let payload = payload.into_raw_bytes()?;
            let effective_topic = self.topic.get_for_posting(&data.values)?;
            let mut entry = BaseRecord::<Vec<u8>, Vec<u8>>::to(&effective_topic)
                .payload(&payload)
                .headers(headers.clone())
                .key(&key_as_bytes);
            loop {
                match self.producer.send(entry) {
                    Ok(()) => break,
                    Err((
                        KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull),
                        unsent_entry,
                    )) => {
                        self.producer.poll(Duration::from_millis(10));
                        entry = unsent_entry;
                    }
                    Err((e, _unsent_entry)) => return Err(WriteError::Kafka(e)),
                }
            }
        }
        Ok(())
    }

    fn name(&self) -> String {
        format!("Kafka({})", self.topic)
    }

    fn retriable(&self) -> bool {
        true
    }

    fn single_threaded(&self) -> bool {
        false
    }
}

pub struct ElasticSearchWriter {
    client: Elasticsearch,
    index_name: String,
    max_batch_size: Option<usize>,

    docs_buffer: Vec<Vec<u8>>,
}

impl ElasticSearchWriter {
    pub fn new(client: Elasticsearch, index_name: String, max_batch_size: Option<usize>) -> Self {
        ElasticSearchWriter {
            client,
            index_name,
            max_batch_size,
            docs_buffer: Vec::new(),
        }
    }
}

impl Writer for ElasticSearchWriter {
    fn write(&mut self, data: FormatterContext) -> Result<(), WriteError> {
        for payload in data.payloads {
            self.docs_buffer.push(b"{\"index\": {}}".to_vec());
            self.docs_buffer.push(payload.into_raw_bytes()?);
        }

        if let Some(max_batch_size) = self.max_batch_size {
            if self.docs_buffer.len() / 2 >= max_batch_size {
                self.flush(true)?;
            }
        }

        Ok(())
    }

    fn flush(&mut self, _forced: bool) -> Result<(), WriteError> {
        if self.docs_buffer.is_empty() {
            return Ok(());
        }
        create_async_tokio_runtime()?.block_on(async {
            self.client
                .bulk(BulkParts::Index(&self.index_name))
                .body(take(&mut self.docs_buffer))
                .send()
                .await
                .map_err(WriteError::Elasticsearch)?
                .error_for_status_code()
                .map_err(WriteError::Elasticsearch)?;

            Ok(())
        })
    }

    fn name(&self) -> String {
        format!("ElasticSearch({})", self.index_name)
    }

    fn single_threaded(&self) -> bool {
        false
    }
}

#[derive(Default, Debug)]
pub struct NullWriter;

impl NullWriter {
    pub fn new() -> Self {
        Self
    }
}

impl Writer for NullWriter {
    fn write(&mut self, _data: FormatterContext) -> Result<(), WriteError> {
        Ok(())
    }

    fn single_threaded(&self) -> bool {
        false
    }
}

const SQLITE_DATA_VERSION_PRAGMA: &str = "data_version";

pub struct SqliteReader {
    connection: SqliteConnection,
    table_name: String,
    schema: Vec<(String, Type)>,

    last_saved_data_version: Option<i64>,
    stored_state: HashMap<i64, ValuesMap>,
    queued_updates: VecDeque<ReadResult>,
}

impl SqliteReader {
    pub fn new(
        connection: SqliteConnection,
        table_name: String,
        schema: Vec<(String, Type)>,
    ) -> Self {
        Self {
            connection,
            table_name,
            schema,

            last_saved_data_version: None,
            queued_updates: VecDeque::new(),
            stored_state: HashMap::new(),
        }
    }

    /// Data version is required to check if there was an update in the database.
    /// There are also hooks, but they only work for changes happened in the same
    /// connection.
    /// More details why hooks don't help here: <https://sqlite.org/forum/forumpost/3174b39eeb79b6a4>
    pub fn data_version(&self) -> i64 {
        let version: ::rusqlite::Result<i64> = self.connection.pragma_query_value(
            Some(::rusqlite::DatabaseName::Main),
            SQLITE_DATA_VERSION_PRAGMA,
            |row| row.get(0),
        );
        version.expect("pragma.data_version request should not fail")
    }

    /// Convert raw `SQLite` field into one of internal value types
    /// There are only five supported types: null, integer, real, text, blob
    /// See also: <https://www.sqlite.org/datatype3.html>
    fn convert_to_value(
        orig_value: SqliteValue<'_>,
        field_name: &str,
        dtype: &Type,
    ) -> Result<Value, Box<ConversionError>> {
        let value = match (dtype, orig_value) {
            (Type::Optional(_) | Type::Any, SqliteValue::Null) => Some(Value::None),
            (Type::Optional(arg), value) => Self::convert_to_value(value, field_name, arg).ok(),
            (Type::Int | Type::Any, SqliteValue::Integer(val)) => Some(Value::Int(val)),
            (Type::Float | Type::Any, SqliteValue::Real(val)) => Some(Value::Float(val.into())),
            (Type::String | Type::Any, SqliteValue::Text(val)) => from_utf8(val)
                .ok()
                .map(|parsed_string| Value::String(parsed_string.into())),
            (Type::Json, SqliteValue::Text(val)) => from_utf8(val)
                .ok()
                .and_then(|parsed_string| {
                    serde_json::from_str::<serde_json::Value>(parsed_string).ok()
                })
                .map(Value::from),
            (Type::Bytes | Type::Any, SqliteValue::Blob(val)) => Some(Value::Bytes(val.into())),
            _ => None,
        };
        if let Some(value) = value {
            Ok(value)
        } else {
            let value_repr = limit_length(format!("{orig_value:?}"), STANDARD_OBJECT_LENGTH_LIMIT);
            Err(Box::new(ConversionError::new(
                value_repr,
                field_name.to_owned(),
                dtype.clone(),
                None,
            )))
        }
    }

    fn load_table(&mut self) -> Result<(), ReadError> {
        let column_names: Vec<&str> = self
            .schema
            .iter()
            .map(|(name, _dtype)| name.as_str())
            .collect();
        let query = format!(
            "SELECT {},_rowid_ FROM {}",
            column_names.join(","),
            self.table_name
        );

        let mut statement = self.connection.prepare(&query)?;
        let mut rows = statement.query([])?;

        let mut present_rowids = HashSet::new();
        while let Some(row) = rows.next()? {
            let rowid: i64 = row.get(self.schema.len())?;
            let mut values = HashMap::with_capacity(self.schema.len());
            for (column_idx, (column_name, column_dtype)) in self.schema.iter().enumerate() {
                let value =
                    Self::convert_to_value(row.get_ref(column_idx)?, column_name, column_dtype);
                values.insert(column_name.clone(), value);
            }
            let values: ValuesMap = values.into();
            self.stored_state
                .entry(rowid)
                .and_modify(|current_values| {
                    if current_values != &values {
                        let key = vec![Value::Int(rowid)];
                        self.queued_updates.push_back(ReadResult::Data(
                            ReaderContext::from_diff(
                                DataEventType::Delete,
                                Some(key.clone()),
                                take(current_values),
                            ),
                            EMPTY_OFFSET,
                        ));
                        self.queued_updates.push_back(ReadResult::Data(
                            ReaderContext::from_diff(
                                DataEventType::Insert,
                                Some(key),
                                values.clone(),
                            ),
                            EMPTY_OFFSET,
                        ));
                        current_values.clone_from(&values);
                    }
                })
                .or_insert_with(|| {
                    let key = vec![Value::Int(rowid)];
                    self.queued_updates.push_back(ReadResult::Data(
                        ReaderContext::from_diff(DataEventType::Insert, Some(key), values.clone()),
                        EMPTY_OFFSET,
                    ));
                    values
                });
            present_rowids.insert(rowid);
        }

        self.stored_state.retain(|rowid, values| {
            if present_rowids.contains(rowid) {
                true
            } else {
                let key = vec![Value::Int(*rowid)];
                self.queued_updates.push_back(ReadResult::Data(
                    ReaderContext::from_diff(DataEventType::Delete, Some(key), take(values)),
                    EMPTY_OFFSET,
                ));
                false
            }
        });

        if !self.queued_updates.is_empty() {
            self.queued_updates.push_back(ReadResult::FinishedSource {
                commit_possibility: CommitPossibility::Possible,
            });
        }

        Ok(())
    }

    fn wait_period() -> Duration {
        Duration::from_millis(500)
    }
}

impl Reader for SqliteReader {
    fn seek(&mut self, _frontier: &OffsetAntichain) -> Result<(), ReadError> {
        todo!("seek is not supported for Sqlite source: persistent history of changes unavailable")
    }

    fn read(&mut self) -> Result<ReadResult, ReadError> {
        loop {
            if let Some(queued_update) = self.queued_updates.pop_front() {
                return Ok(queued_update);
            }

            let current_data_version = self.data_version();
            if self.last_saved_data_version != Some(current_data_version) {
                self.load_table()?;
                self.last_saved_data_version = Some(current_data_version);
                return Ok(ReadResult::NewSource(
                    SQLiteMetadata::new(current_data_version).into(),
                ));
            }
            // Sleep to avoid non-stop pragma requests of a table
            // that did not change
            sleep(Self::wait_period());
        }
    }

    fn short_description(&self) -> Cow<'static, str> {
        format!("SQLite({})", self.table_name).into()
    }

    fn storage_type(&self) -> StorageType {
        StorageType::Sqlite
    }
}

pub struct MongoWriter {
    collection: MongoCollection<BsonDocument>,
    buffer: Vec<BsonDocument>,
    max_batch_size: Option<usize>,
}

impl MongoWriter {
    pub fn new(collection: MongoCollection<BsonDocument>, max_batch_size: Option<usize>) -> Self {
        Self {
            collection,
            max_batch_size,
            buffer: Vec::new(),
        }
    }
}

impl Writer for MongoWriter {
    fn write(&mut self, data: FormatterContext) -> Result<(), WriteError> {
        for payload in data.payloads {
            self.buffer.push(payload.into_bson_document()?);
        }
        if let Some(max_batch_size) = self.max_batch_size {
            if self.buffer.len() >= max_batch_size {
                self.flush(true)?;
            }
        }
        Ok(())
    }

    fn flush(&mut self, _forced: bool) -> Result<(), WriteError> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let command = self.collection.insert_many(take(&mut self.buffer));
        let _ = command.run()?;
        Ok(())
    }

    fn name(&self) -> String {
        format!("MongoDB({})", self.collection.name())
    }
}

pub const MQTT_MAX_MESSAGES_IN_QUEUE: usize = 1024;
pub const MQTT_CLIENT_MAX_CHANNEL_SIZE: usize = 1024 * 1024;

pub struct MqttReader {
    connection: MqttConnection,
    total_entries_read: usize,
}

impl MqttReader {
    pub fn new(connection: MqttConnection) -> Self {
        Self {
            connection,
            total_entries_read: 0,
        }
    }
}

impl Reader for MqttReader {
    fn read(&mut self) -> Result<ReadResult, ReadError> {
        loop {
            let event = match self.connection.recv() {
                Ok(event) => event?,
                Err(e) => {
                    warn!("Source channel has been closed: {e:?}");
                    break;
                }
            };
            match event {
                MqttEvent::Incoming(MqttPacket::Publish(message)) => {
                    self.total_entries_read += 1;
                    let offset = (
                        OffsetKey::Empty,
                        OffsetValue::MqttReadEntriesCount(self.total_entries_read),
                    );
                    return Ok(ReadResult::Data(
                        ReaderContext::from_raw_bytes(
                            DataEventType::Insert,
                            message.payload.to_vec(),
                        ),
                        offset,
                    ));
                }
                other => {
                    info!("Received metadata event from MQTT reader: {other:?}");
                }
            }
        }

        // The broker has closed the connection, no new messages are expected
        Ok(ReadResult::Finished)
    }

    fn seek(&mut self, frontier: &OffsetAntichain) -> Result<(), ReadError> {
        let offset_value = frontier.get_offset(&OffsetKey::Empty);
        if let Some(offset) = offset_value {
            if let OffsetValue::MqttReadEntriesCount(last_run_entries_read) = offset {
                self.total_entries_read = *last_run_entries_read;
            } else {
                error!("Unexpected offset type for MQTT reader: {offset:?}");
            }
        }

        Ok(())
    }

    fn storage_type(&self) -> StorageType {
        StorageType::Mqtt
    }
}

pub struct MqttWriter {
    client: MqttClient,
    topic: MessageQueueTopic,
    qos: MqttQoS,
    retain: bool,
    connection: MqttConnection,
    packets_in_queue: usize,
    packet_id_waits_for_confirmation: Vec<bool>,
}

impl MqttWriter {
    pub fn new(
        client: MqttClient,
        connection: MqttConnection,
        topic: MessageQueueTopic,
        qos: MqttQoS,
        retain: bool,
    ) -> Self {
        Self {
            client,
            topic,
            qos,
            retain,
            connection,
            packets_in_queue: 0,
            packet_id_waits_for_confirmation: vec![false; u16::MAX as usize + 1],
        }
    }

    fn on_packet_acked(&mut self, id: u16) {
        let id = id as usize;
        if self.packet_id_waits_for_confirmation[id] {
            self.packet_id_waits_for_confirmation[id] = false;
            self.packets_in_queue -= 1;
        } else {
            warn!("Unexpected message confirmation: id = {id}");
        }
    }

    fn ensure_max_packets_in_queue(&mut self, max_in_queue: usize) -> Result<(), WriteError> {
        while self.packets_in_queue > max_in_queue {
            let packet = match self.connection.recv() {
                Ok(Ok(event)) => event,
                Ok(Err(event_error)) => {
                    error!("Failed to communicate with MQTT broker: {event_error}");
                    return Err(WriteError::MqttPoll(event_error));
                }
                Err(e) => {
                    // Nobody can accept events or respond
                    warn!("All clients have closed the requests channel: {e:?}");
                    return Ok(());
                }
            };
            match packet {
                MqttEvent::Outgoing(MqttOutgoing::Publish(id)) => {
                    if id == 0 {
                        // ID = 0 implies that QoS is 0.
                        // The message was sent with this outgoing packet,
                        // and no acknowledgment is expected.
                        self.packets_in_queue -= 1;
                    } else {
                        self.packet_id_waits_for_confirmation[id as usize] = true;
                    }
                }
                MqttEvent::Incoming(MqttIncoming::PubAck(id)) => {
                    // A `PubAck` message implies QoS = 1.
                    // Communication works as follows:
                    // 1. An outgoing `Publish` packet is sent from Pathway to the broker.
                    // 2. When the broker receives the packet, it sends a `PubAck` message
                    //    back to Pathway with the packet's identifier.
                    //    If no `PubAck` is received within a certain time frame,
                    //    the client retries sending the `Publish` packet.
                    self.on_packet_acked(id.pkid);
                }
                MqttEvent::Incoming(MqttIncoming::PubComp(id)) => {
                    // A `PubComp` message implies QoS = 2.
                    // The communication sequence works as follows:
                    // 1. An outgoing `Publish` packet is sent from Pathway to the broker.
                    // 2. When the broker receives the packet, it sends a `PubRec` message
                    //    back to Pathway with the packet's identifier.
                    // 3. Client reads the identifier and sends a `PubRel` message to release the message.
                    // 4. The broker completes the flow by sending a `PubComp` message to Pathway.
                    // If any expected message is not received within a timeout,
                    // the MQTT client retries sending the last message with the DUP flag set.
                    self.on_packet_acked(id.pkid);
                }
                other => {
                    info!("Auxiliary information packet, unused in submission tracking: {other:?}");
                }
            }
        }
        Ok(())
    }
}

impl Writer for MqttWriter {
    fn write(&mut self, data: FormatterContext) -> Result<(), WriteError> {
        for payload in data.payloads {
            self.packets_in_queue += 1;
            let payload = payload.into_raw_bytes()?;
            let effective_topic = self.topic.get_for_posting(&data.values)?;
            self.client
                .publish(effective_topic, self.qos, self.retain, payload)
                .map_err(WriteError::MqttPublish)?;
        }

        // The message identifier is a 16-bit integer, hence we don't want
        // to keep the big amounts of messages in-fly.
        self.ensure_max_packets_in_queue(MQTT_MAX_MESSAGES_IN_QUEUE)
    }

    fn flush(&mut self, _forced: bool) -> Result<(), WriteError> {
        self.ensure_max_packets_in_queue(0)
    }

    fn name(&self) -> String {
        format!("MQTT({})", self.topic)
    }

    fn retriable(&self) -> bool {
        true
    }

    fn single_threaded(&self) -> bool {
        false
    }
}

#[allow(clippy::enum_variant_names)]
pub enum QuestDBAtColumnPolicy {
    UseNow,
    UsePathwayTime,
    UseColumn(usize),
}

pub struct QuestDBWriter {
    sender: QuestDBSender,
    table_name: String,
    field_names: Vec<String>,
    designated_timestamp_policy: QuestDBAtColumnPolicy,
    buffer: QuestDBBuffer,
    has_updates: bool,
}

impl QuestDBWriter {
    pub fn new(
        sender: QuestDBSender,
        table_name: String,
        field_names: Vec<String>,
        designated_timestamp_policy: QuestDBAtColumnPolicy,
    ) -> Result<Self, WriteError> {
        let mut buffer = QuestDBBuffer::new();
        buffer.table(table_name.as_str())?;
        Ok(Self {
            sender,
            table_name,
            field_names,
            designated_timestamp_policy,
            buffer,
            has_updates: false,
        })
    }

    fn put_value_into_buffer(
        buffer: &mut QuestDBBuffer,
        value: Value,
        column_name: &str,
    ) -> Result<(), WriteError> {
        match value {
            Value::None => buffer, // just don't specify the value
            Value::Bool(b) => buffer.column_bool(column_name, b)?,
            Value::Int(i) => buffer.column_i64(column_name, i)?,
            Value::Float(f) => buffer.column_f64(column_name, *f)?,
            Value::String(s) => buffer.column_str(column_name, s)?,
            Value::DateTimeNaive(dt) => buffer.column_ts(
                column_name,
                QuestDBTimestamp::Nanos(QuestDBTimestampNanos::new(dt.timestamp())),
            )?,
            Value::DateTimeUtc(dt) => buffer.column_ts(
                column_name,
                QuestDBTimestamp::Nanos(QuestDBTimestampNanos::new(dt.timestamp())),
            )?,
            Value::Duration(d) => buffer.column_i64(column_name, d.nanoseconds())?,
            Value::Json(j) => buffer.column_str(column_name, j.to_string())?,
            Value::PyObjectWrapper(_) => {
                buffer.column_str(column_name, create_bincoded_value(&value)?)?
            }
            Value::Pointer(p) => buffer.column_str(column_name, p.to_string())?,
            Value::Bytes(b) => {
                let encoded = base64::engine::general_purpose::STANDARD.encode(b);
                buffer.column_str(column_name, encoded)?
            }
            Value::IntArray(_) | Value::FloatArray(_) | Value::Tuple(_) => {
                let json_value = serialize_value_to_json(&value)?;
                buffer.column_str(column_name, json_value.to_string())?
            }
            Value::Pending | Value::Error => Err(FormatterError::ValueNonSerializable(
                value.kind(),
                "QuestDB",
            ))?,
        };
        Ok(())
    }
}

impl Writer for QuestDBWriter {
    fn write(&mut self, data: FormatterContext) -> Result<(), WriteError> {
        let (at_timestamp, skip_column_id) =
            if let QuestDBAtColumnPolicy::UseColumn(column_id) = self.designated_timestamp_policy {
                let at_value = &data.values[column_id];
                match at_value {
                    Value::DateTimeNaive(dt) => (
                        Some(QuestDBTimestamp::Nanos(QuestDBTimestampNanos::new(
                            dt.timestamp(),
                        ))),
                        column_id,
                    ),
                    Value::DateTimeUtc(dt) => (
                        Some(QuestDBTimestamp::Nanos(QuestDBTimestampNanos::new(
                            dt.timestamp(),
                        ))),
                        column_id,
                    ),
                    _ => return Err(WriteError::QuestDBAtColumnNotTime(at_value.clone())),
                }
            } else {
                (None, data.values.len())
            };

        for (column_id, (value, column_name)) in data
            .values
            .into_iter()
            .zip(self.field_names.iter())
            .enumerate()
        {
            if column_id == skip_column_id {
                continue;
            }
            Self::put_value_into_buffer(&mut self.buffer, value, column_name.as_str())?;
        }
        self.buffer.column_i64(
            SPECIAL_FIELD_DIFF,
            data.diff
                .try_into()
                .expect("pathway diff can only be 1 or -1"),
        )?;
        let pathway_time_casted = QuestDBTimestampMicros::new(
            data.time
                .0
                .checked_mul(1000) // Pathway minibatch time is milliseconds, hence multiplication by 1000 is needed
                .expect("pathway time must fit 64bit signed integer")
                .try_into()
                .expect("pathway time must be nonnegative"),
        );
        match self.designated_timestamp_policy {
            QuestDBAtColumnPolicy::UseNow => {
                self.buffer
                    .column_ts(SPECIAL_FIELD_TIME, pathway_time_casted)?;
                self.buffer.at_now()?;
            }
            QuestDBAtColumnPolicy::UsePathwayTime => self.buffer.at(pathway_time_casted)?,
            QuestDBAtColumnPolicy::UseColumn(_) => {
                self.buffer
                    .column_ts(SPECIAL_FIELD_TIME, pathway_time_casted)?;
                self.buffer
                    .at(at_timestamp.expect("at_timestamp must have defined upstream"))?;
            }
        }
        self.has_updates = true;

        Ok(())
    }

    fn flush(&mut self, _forced: bool) -> Result<(), WriteError> {
        if self.has_updates {
            self.sender.flush(&mut self.buffer)?;
            self.has_updates = false;
            self.buffer.table(self.table_name.as_str())?;
        }
        Ok(())
    }

    fn name(&self) -> String {
        format!("QuestDB({})", self.table_name)
    }

    fn retriable(&self) -> bool {
        true
    }

    fn single_threaded(&self) -> bool {
        false
    }
}

enum SqlQueryTemplate {
    StreamOfChanges(String),
    Snapshot {
        insertion: String,
        deletion: String,
        primary_key_indices: Vec<usize>,
    },
}

impl SqlQueryTemplate {
    fn new(
        snapshot_mode: bool,
        table_name: &str,
        value_fields: &[ValueField],
        key_field_names: Option<&[String]>,
        include_special_columns_in_snapshot_mode: bool,
        mut wildcard_by_index: impl FnMut(usize) -> String,
        mut on_insert_conflict_condition: impl FnMut(&str, &[String], &[ValueField], bool) -> String,
    ) -> Result<Self, WriteError> {
        let field_list = value_fields.iter().map(|f| &f.name).join(",");
        if snapshot_mode {
            let key_field_names = key_field_names.ok_or(WriteError::EmptyKeyFieldsForSnapshot)?;
            if key_field_names.is_empty() {
                return Err(WriteError::EmptyKeyFieldsForSnapshot);
            }
            let insertion = if include_special_columns_in_snapshot_mode {
                let placeholders = (0..value_fields.len() + 2)
                    .map(&mut wildcard_by_index)
                    .join(",");
                format!(
                    "INSERT INTO {table_name} ({field_list},time,diff) VALUES ({placeholders}) {}",
                    on_insert_conflict_condition(
                        table_name,
                        key_field_names,
                        value_fields,
                        include_special_columns_in_snapshot_mode
                    ),
                )
            } else {
                let placeholders = (0..value_fields.len())
                    .map(&mut wildcard_by_index)
                    .join(",");
                format!(
                    "INSERT INTO {table_name} ({field_list}) VALUES ({placeholders}) {}",
                    on_insert_conflict_condition(
                        table_name,
                        key_field_names,
                        value_fields,
                        include_special_columns_in_snapshot_mode
                    ),
                )
            };

            let primary_key_indices = Self::primary_key_indices(value_fields, key_field_names)?;
            let tokens: Vec<_> = key_field_names
                .iter()
                .enumerate()
                .map(|(i, name)| format!("{name}={}", wildcard_by_index(i)))
                .collect();
            let deletion = format!("DELETE FROM {table_name} WHERE {}", tokens.join(" AND "));
            Ok(SqlQueryTemplate::Snapshot {
                insertion: insertion.clone(),
                deletion: deletion.clone(),
                primary_key_indices,
            })
        } else {
            let placeholders = (0..value_fields.len() + 2)
                .map(&mut wildcard_by_index)
                .join(",");
            Ok(Self::StreamOfChanges(format!(
                "INSERT INTO {table_name} ({field_list},time,diff) VALUES ({placeholders})"
            )))
        }
    }

    fn primary_key_indices(
        value_fields: &[ValueField],
        key_field_names: &[String],
    ) -> Result<Vec<usize>, WriteError> {
        // We expect a small number of fields, so a straightforward O(N^2) scan is fine
        // and likely faster in practice than building a HashMap due to lower constant
        // factors and better cache locality.
        let mut primary_key_indices: Vec<_> = key_field_names
            .iter()
            .map(|name| {
                value_fields
                    .iter()
                    .position(|vf| vf.name == *name)
                    .ok_or_else(|| WriteError::FieldNotFound(name.clone()))
            })
            .collect::<Result<_, _>>()?;
        primary_key_indices.sort_unstable();
        Ok(primary_key_indices)
    }

    fn primary_key_fields<T>(&self, mut fields: Vec<T>) -> Vec<T> {
        let Self::Snapshot {
            primary_key_indices,
            ..
        } = self
        else {
            unreachable!()
        };
        for (index, pkey_index) in primary_key_indices.iter().enumerate() {
            fields.swap(index, *pkey_index);
        }
        fields.truncate(primary_key_indices.len());

        fields
    }

    fn build_query(&self, diff: isize) -> String {
        match self {
            Self::StreamOfChanges(query) => query.clone(),
            Self::Snapshot { insertion, .. } if diff > 0 => insertion.clone(),
            Self::Snapshot { deletion, .. } if diff < 0 => deletion.clone(),
            Self::Snapshot { .. } => unreachable!(),
        }
    }
}

const MAX_MYSQL_RETRIES: usize = 3;

pub struct MysqlWriter {
    pool: MysqlConnectionPool,
    current_connection: MysqlConnection,
    max_batch_size: Option<usize>,
    buffer: Vec<FormatterContext>,
    snapshot_mode: bool,
    table_name: String,
    query_template: SqlQueryTemplate,
}

impl MysqlWriter {
    pub fn new(
        pool: MysqlConnectionPool,
        max_batch_size: Option<usize>,
        snapshot_mode: bool,
        table_name: &str,
        value_fields: &[ValueField],
        key_field_names: Option<&[String]>,
        mode: TableWriterInitMode,
    ) -> Result<MysqlWriter, WriteError> {
        let mut connection = pool.get_conn()?;
        let mut transaction = connection.start_transaction(MysqlTxOpts::default())?;
        mode.initialize(
            table_name,
            value_fields,
            key_field_names,
            !snapshot_mode,
            |query| {
                transaction.query_drop(query)?;
                Ok(())
            },
            Self::mysql_data_type,
        )?;
        transaction.commit()?;

        let writer = MysqlWriter {
            current_connection: pool.get_conn()?,
            pool,
            max_batch_size,
            snapshot_mode,
            table_name: table_name.to_string(),
            query_template: SqlQueryTemplate::new(
                snapshot_mode,
                table_name,
                value_fields,
                key_field_names,
                false,
                |_| "?".to_string(),
                Self::on_insert_conflict_condition,
            )?,
            buffer: Vec::new(),
        };

        Ok(writer)
    }

    // Generates a statement that is used in "on conflict" part of the query
    fn on_insert_conflict_condition(
        _table_name: &str,
        _key_field_names: &[String],
        value_fields: &[ValueField],
        _legacy_mode: bool,
    ) -> String {
        let update_pairs = value_fields
            .iter()
            .map(|field| format!("{name}=new.{name}", name = field.name))
            .collect::<Vec<_>>();

        format!("AS new ON DUPLICATE KEY UPDATE {}", update_pairs.join(", "))
    }

    fn mysql_data_type(type_: &Type, is_nested: bool) -> Result<String, WriteError> {
        let not_null_suffix = if is_nested { "" } else { " NOT NULL" };
        Ok(match type_ {
            Type::Bool => format!("BOOLEAN{not_null_suffix}"),
            Type::Int => format!("BIGINT{not_null_suffix}"),
            Type::Float => format!("DOUBLE{not_null_suffix}"),
            Type::Pointer | Type::String => format!("TEXT{not_null_suffix}"),
            Type::Bytes | Type::PyObjectWrapper => format!("BLOB{not_null_suffix}"),
            Type::Json => format!("JSON{not_null_suffix}"),
            Type::Duration => format!("TIME(6){not_null_suffix}"),
            Type::DateTimeNaive | Type::DateTimeUtc => format!("DATETIME(6){not_null_suffix}"),
            Type::Optional(wrapped) => {
                if let Type::Any = **wrapped {
                    return Err(WriteError::UnsupportedType(type_.clone()));
                }
                let wrapped = Self::mysql_data_type(wrapped, true)?;
                return Ok(wrapped);
            }
            Type::Any | Type::Tuple(_) | Type::List(_) | Type::Array(_, _) | Type::Future(_) => {
                return Err(WriteError::UnsupportedType(type_.clone()))
            }
        })
    }

    fn to_mysql_date(dt: DateTimeNaive) -> MysqlValue {
        MysqlValue::Date(
            dt.year().try_into().expect("years must fit u16"),
            dt.month().try_into().expect("months must fit u8"),
            dt.day().try_into().expect("days must fit u8"),
            dt.hour().try_into().expect("hours must fit u8"),
            dt.minute().try_into().expect("minutes must fit u8"),
            dt.second().try_into().expect("seconds must fit u8"),
            dt.microsecond()
                .try_into()
                .expect("microseconds must fit u32"),
        )
    }

    fn to_mysql_value(value: &Value) -> Result<MysqlValue, WriteError> {
        match value {
            Value::None => Ok(MysqlValue::NULL),
            Value::Bool(b) => Ok(MysqlValue::from(b)),
            Value::Int(i) => Ok(MysqlValue::Int(*i)),
            Value::Float(f) => Ok(MysqlValue::Double((*f).into())),
            Value::Pointer(p) => Ok(MysqlValue::Bytes(p.to_string().into())),
            Value::String(s) => Ok(MysqlValue::Bytes(s.to_string().into())),
            Value::Bytes(b) => Ok(MysqlValue::Bytes(b.to_vec())),
            Value::DateTimeNaive(dt) => Ok(Self::to_mysql_date(*dt)),
            Value::DateTimeUtc(dt) => {
                Ok(Self::to_mysql_date(dt.to_naive_in_timezone("UTC").unwrap()))
            }
            Value::Duration(d) => {
                let is_negative = d < &EngineDuration::new(0);
                let mut total_microseconds = d.microseconds();
                if is_negative {
                    total_microseconds *= -1;
                }
                let microseconds: u32 = (total_microseconds % 1_000_000)
                    .try_into()
                    .expect("microsecond part must fit u32");

                let total_seconds = total_microseconds / 1_000_000;
                let seconds: u8 = (total_seconds % 60)
                    .try_into()
                    .expect("second part must fit u8");

                let total_minutes = total_seconds / 60;
                let minutes: u8 = (total_minutes % 60)
                    .try_into()
                    .expect("minute part must fit u8");

                let total_hours = total_minutes / 60;
                let hours: u8 = (total_hours % 24)
                    .try_into()
                    .expect("hour part must fit u8");

                let total_days = total_hours / 24;
                let days: u32 = total_days.try_into().expect("day part must fit u32");
                Ok(MysqlValue::Time(
                    is_negative,
                    days,
                    hours,
                    minutes,
                    seconds,
                    microseconds,
                ))
            }
            Value::Json(j) => Ok(MysqlValue::Bytes(j.to_string().into())),
            Value::PyObjectWrapper(_) => Ok(MysqlValue::Bytes(
                bincode::serialize(value).map_err(|e| *e)?,
            )),
            Value::IntArray(_)
            | Value::FloatArray(_)
            | Value::Tuple(_)
            | Value::Error
            | Value::Pending => Err(FormatterError::ValueNonSerializable(value.kind(), "MySQL"))?,
        }
    }

    fn flush_with_current_connection(&mut self) -> Result<(), WriteError> {
        let mut transaction = self
            .current_connection
            .start_transaction(MysqlTxOpts::default())?;
        for data in &self.buffer {
            let mut params: Vec<_> = data
                .values
                .iter()
                .map(Self::to_mysql_value)
                .collect::<Result<_, _>>()?;
            let params = if self.snapshot_mode {
                if data.diff > 0 {
                    params
                } else {
                    self.query_template.primary_key_fields(params)
                }
            } else {
                params.push(MysqlValue::Int(data.time.0.try_into().unwrap()));
                params.push(MysqlValue::Int(data.diff.try_into().unwrap()));
                params
            };
            transaction.exec_drop(
                self.query_template.build_query(data.diff),
                MysqlParams::Positional(params),
            )?;
        }
        transaction.commit()?;
        Ok(())
    }
}

impl Writer for MysqlWriter {
    fn write(&mut self, data: FormatterContext) -> Result<(), WriteError> {
        self.buffer.push(data);
        if let Some(max_batch_size) = self.max_batch_size {
            if self.buffer.len() == max_batch_size {
                self.flush(true)?;
            }
        }
        Ok(())
    }

    fn flush(&mut self, _forced: bool) -> Result<(), WriteError> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        execute_with_retries(
            || {
                let cc_save_result = self.flush_with_current_connection();
                if cc_save_result.is_err() {
                    self.current_connection = self.pool.get_conn()?;
                }
                cc_save_result
            },
            RetryConfig::default(),
            MAX_MYSQL_RETRIES,
        )?;
        self.buffer.clear();

        Ok(())
    }

    fn name(&self) -> String {
        format!("MySQL({})", self.table_name)
    }

    fn single_threaded(&self) -> bool {
        self.snapshot_mode
    }
}
