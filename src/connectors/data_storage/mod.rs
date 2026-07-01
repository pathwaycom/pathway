// Copyright © 2026 Pathway

pub mod aws;
pub mod clickhouse;
pub mod data_lake;
pub mod elasticsearch;
pub mod file;
pub mod kafka;
pub mod mongodb;
pub mod mqtt;
pub mod mssql;
pub mod mysql;
pub mod nats;
pub mod null;
pub mod pinecone;
pub mod polling;
pub mod postgres;
pub mod python;
pub mod questdb;
pub mod rabbitmq;
pub mod scanner;
pub mod sharding;
pub mod sqlite;
pub mod weaviate;

pub use file::FileWriter;
pub use kafka::{KafkaReader, KafkaReaderError, KafkaWriter, RdkafkaWatermark};
pub use mqtt::{MqttReader, MqttWriter, MQTT_CLIENT_MAX_CHANNEL_SIZE, MQTT_MAX_MESSAGES_IN_QUEUE};
pub use null::NullWriter;
pub use python::{PythonReader, PythonReaderBuilder};
pub use questdb::{QuestDBAtColumnPolicy, QuestDBWriter};

pub use self::clickhouse::{ClickHouseError, ClickHouseWriter};

use s3::error::S3Error;
use std::any::type_name;
use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt;
use std::fmt::{Debug, Display};
use std::io;
use std::io::BufRead;
use std::str::Utf8Error;

use ::mongodb::bson::Document as BsonDocument;
use ::questdb::Error as QuestDBError;
use async_nats::jetstream::consumer::pull::MessagesErrorKind;
use async_nats::jetstream::consumer::StreamErrorKind;
use aws_sdk_dynamodb::error::BuildError as AwsBuildError;
use deltalake::arrow::datatypes::DataType as ArrowDataType;
use deltalake::arrow::error::ArrowError;
use deltalake::datafusion::common::DataFusionError;
use deltalake::datafusion::parquet::record::Field as ParquetValue;
use deltalake::parquet::errors::ParquetError;
use itertools::Itertools;
use log::error;
use rumqttc::{ClientError as MqttClientError, ConnectionError as MqttConnectionError};

use crate::connectors::data_format::{FormatterContext, FormatterError, COMMIT_LITERAL};
use crate::connectors::data_storage::aws::dynamodb::Error as AwsDynamoDBError;
use crate::connectors::data_storage::aws::kinesis::Error as AwsKinesisError;
use crate::connectors::data_storage::aws::kinesis::KinesisReader;
use crate::connectors::data_storage::data_lake::buffering::IncorrectSnapshotError;
use crate::connectors::data_storage::scanner::s3::S3CommandName;
use crate::connectors::metadata::SourceMetadata;
use crate::connectors::posix_like::PosixLikeReader;
use crate::connectors::{Offset, OffsetValue};
use crate::engine::error::DynResult;
use crate::engine::{Key, Type, Value};
use crate::persistence::backends::Error as PersistenceBackendError;
use crate::persistence::frontier::OffsetAntichain;
use crate::persistence::tracker::WorkerPersistentStorage;
use crate::persistence::{PersistentId, UniqueName};
use crate::python_api::ValueField;

use async_nats::client::FlushError as NatsFlushError;
use async_nats::client::PublishError as NatsPublishError;
use async_nats::error::Error as NatsError;
use async_nats::jetstream::context::PublishErrorKind as JetStreamPublishError;
use bincode::ErrorKind as BincodeError;
use glob::PatternError as GlobPatternError;
use pyo3::prelude::*;
use rdkafka::error::KafkaError;
use serde::{Deserialize, Serialize};

pub use self::data_lake::delta::{DeltaError, DeltaTableReader, ObjectDownloader};
pub use self::data_lake::iceberg::{IcebergError, IcebergReader};
pub use self::data_lake::LakeWriter;
pub use self::elasticsearch::{ElasticSearchError, ElasticSearchReader, ElasticSearchWriter};
pub use self::mongodb::{MongoReader, MongoWriter};
pub use self::mssql::{MssqlError, MssqlReader};
pub use self::mysql::{MysqlError, MysqlReader, MysqlReaderError};
pub use self::nats::NatsReader;
pub use self::nats::NatsWriter;
pub use self::pinecone::{PineconeError, PineconeWriter};
pub use self::polling::{LiveState, PolledRow, PollingDataSource, PollingReader};
pub use self::postgres::{
    PostgresError, PsqlReader, PsqlWriter, ReplicationError as PostgresReplicationError, SslError,
};
pub use self::rabbitmq::{RabbitmqError, RabbitmqReader, RabbitmqWriter};
pub use self::sqlite::{SqliteError, SqliteReader, SqliteWriter};
pub use self::weaviate::{WeaviateError, WeaviateWriter};

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
    // A single CSV row, kept as the `csv` crate's `StringRecord`. The parser
    // reads field slices directly out of it, avoiding the intermediate
    // `Vec<String>` (and the per-field allocation) that `TokenizedEntries`
    // requires.
    CsvRecord(DataEventType, csv::StringRecord),
    KeyValue((Option<Vec<u8>>, Option<Vec<u8>>)),
    Diff((DataEventType, Option<Vec<Value>>, ValuesMap)),
    Bson((DataEventType, String, BsonDocument)),
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

    pub fn from_csv_record(event: DataEventType, record: csv::StringRecord) -> ReaderContext {
        ReaderContext::CsvRecord(event, record)
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

use crate::connectors::data_storage::mongodb::MongoDbError;

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

    #[error(transparent)]
    Sqlite(#[from] SqliteError),

    #[error(transparent)]
    Delta(#[from] DeltaError),

    #[error(transparent)]
    Postgres(#[from] PostgresError),

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
    MongoDb(#[from] MongoDbError),

    #[error(transparent)]
    Mssql(#[from] MssqlError),

    #[error(transparent)]
    Mysql(#[from] MysqlReaderError),

    #[error(transparent)]
    Persistence(#[from] PersistenceBackendError),

    #[error("persistence is not supported for storage '{0:?}'")]
    PersistenceNotSupported(StorageType),

    #[error(transparent)]
    Kinesis(#[from] Box<AwsKinesisError>),

    #[error("malformed data")]
    MalformedData,

    #[error("no objects to read")]
    NoObjectsToRead,

    #[error("invalid special value: {0}")]
    InvalidSpecialValue(String),

    #[error("parquet value type mismatch: got {0:?} expected {1:?}")]
    WrongParquetType(ParquetValue, Type),

    #[error("explicit primary key specification is required for non-append-only tables")]
    PrimaryKeyRequired,

    #[error(transparent)]
    NatsStreaming(#[from] async_nats::error::Error<StreamErrorKind>),

    #[error(transparent)]
    NatsPolling(#[from] async_nats::error::Error<MessagesErrorKind>),

    #[error("failed to acknowledge read nats message: {0}")]
    NatsMessageAck(async_nats::Error),

    #[error(transparent)]
    Rabbitmq(#[from] RabbitmqError),

    #[error(transparent)]
    ElasticSearch(#[from] ElasticSearchError),
}

// Allow `?` on unboxed `AwsKinesisError` in functions returning `Result<_, ReadError>`.
// The variant holds a `Box` to keep the enum size small.
impl From<AwsKinesisError> for ReadError {
    fn from(e: AwsKinesisError) -> Self {
        ReadError::Kinesis(Box::new(e))
    }
}

// Allow `?` on `mongodb::error::Error` in functions returning `Result<_, ReadError>`.
// Routes through `MongoDbError::Driver` so the full chain is `ReadError::MongoDb`.
impl From<::mongodb::error::Error> for ReadError {
    fn from(e: ::mongodb::error::Error) -> Self {
        ReadError::MongoDb(MongoDbError::Driver(e))
    }
}

// Allow `?` on raw `postgres::Error` and `PostgresReplicationError` in
// functions returning `Result<_, ReadError>`. Both route through the
// unified `PostgresError` so `ReadError` carries a single
// `Postgres(_)` variant.
impl From<::postgres::Error> for ReadError {
    fn from(e: ::postgres::Error) -> Self {
        ReadError::Postgres(PostgresError::Postgres(e))
    }
}

impl From<PostgresReplicationError> for ReadError {
    fn from(e: PostgresReplicationError) -> Self {
        ReadError::Postgres(PostgresError::Replication(e))
    }
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
    Mssql,
    Postgres,
    MongoDb,
    Rabbitmq,
    Mysql,
    ElasticSearch,
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
            StorageType::Mssql => MssqlReader::merge_two_frontiers(lhs, rhs),
            StorageType::Postgres => PsqlReader::merge_two_frontiers(lhs, rhs),
            StorageType::MongoDb => MongoReader::merge_two_frontiers(lhs, rhs),
            StorageType::Rabbitmq => RabbitmqReader::merge_two_frontiers(lhs, rhs),
            StorageType::Mysql => MysqlReader::merge_two_frontiers(lhs, rhs),
            StorageType::ElasticSearch => ElasticSearchReader::merge_two_frontiers(lhs, rhs),
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

    #[allow(clippy::too_many_lines, clippy::match_same_arms)]
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
                        OffsetValue::RabbitmqOffset(offset_position),
                        OffsetValue::RabbitmqOffset(other_position),
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
                    (OffsetValue::MongoDbOplogToken(a), OffsetValue::MongoDbOplogToken(b))
                        if b > a =>
                    {
                        result.advance_offset(offset_key.clone(), other_value.clone());
                    }
                    (OffsetValue::MssqlCdcLsn(a), OffsetValue::MssqlCdcLsn(b)) if b > a => {
                        result.advance_offset(offset_key.clone(), other_value.clone());
                    }
                    (
                        OffsetValue::MysqlBinlogPos {
                            filename: lhs_file,
                            position: lhs_pos,
                        },
                        OffsetValue::MysqlBinlogPos {
                            filename: rhs_file,
                            position: rhs_pos,
                        },
                    ) if self::mysql::binlog_coords_cmp(rhs_file, *rhs_pos, lhs_file, *lhs_pos)
                        .is_gt() =>
                    {
                        result.advance_offset(offset_key.clone(), other_value.clone());
                    }
                    (
                        OffsetValue::PollingWatermark {
                            entries_read: offset_entries_read,
                            ..
                        },
                        OffsetValue::PollingWatermark {
                            entries_read: other_entries_read,
                            ..
                        },
                    ) => {
                        // The watermark stalls at the live edge while rows keep
                        // arriving, so ordering by the monotonic delivered-row
                        // count is the only reliable way to pick the more
                        // advanced frontier.
                        if other_entries_read > offset_entries_read {
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

/// Stringifies an error together with its entire `source()` chain.
/// Rust-postgres' `postgres::Error` Display collapses serialization
/// failures into "error serializing parameter N" and hides the
/// wrapped conversion error (like our `ArraySerializationError`) —
/// follow the chain so the offending root cause (`jagged array`,
/// `DimensionsOverflow`, etc.) is visible to the user.
pub(crate) fn format_error_chain(err: &(dyn std::error::Error + 'static)) -> String {
    let mut out = err.to_string();
    let mut current: Option<&(dyn std::error::Error + 'static)> = err.source();
    while let Some(source) = current {
        use std::fmt::Write;
        let _ = write!(out, ": {source}");
        current = source.source();
    }
    out
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

    #[error(transparent)]
    Postgres(#[from] PostgresError),

    #[error(transparent)]
    Utf8(#[from] Utf8Error),

    #[error(transparent)]
    Bincode(#[from] BincodeError),

    #[error(transparent)]
    Delta(#[from] DeltaError),

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
    Rabbitmq(#[from] RabbitmqError),

    #[error(transparent)]
    JetStream(#[from] NatsError<JetStreamPublishError>),

    #[error(transparent)]
    Iceberg(#[from] IcebergError),

    #[error(transparent)]
    QuestDB(#[from] QuestDBError),

    #[error("the 'at' QuestDB column is not of the time type: {0}")]
    QuestDBAtColumnNotTime(Value),

    #[error(transparent)]
    ClickHouse(#[from] ClickHouseError),

    #[error("type mismatch with delta table schema: got {0} expected {1}")]
    TypeMismatchWithSchema(Value, ArrowDataType),

    #[error("integer value {0} out of range")]
    IntOutOfRange(i64),

    #[error("value {0} can't be used as a key because it's neither 'bytes' nor 'string'")]
    IncorrectKeyFieldType(Value),

    #[error("unsupported type: {0:?}")]
    UnsupportedType(Type),

    #[error(transparent)]
    ElasticSearch(#[from] ElasticSearchError),

    #[error(transparent)]
    Weaviate(#[from] WeaviateError),

    #[error(transparent)]
    Persistence(#[from] PersistenceBackendError),

    #[error(transparent)]
    Formatter(#[from] FormatterError),

    #[error(transparent)]
    MongoDB(#[from] MongoDbError),

    #[error(transparent)]
    Mssql(#[from] MssqlError),

    #[error(transparent)]
    Mysql(#[from] MysqlError),

    #[error(transparent)]
    Sqlite(#[from] SqliteError),

    #[error("dynamic topic name is not a string field: {0}")]
    DynamicTopicIsNotAString(Value),

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

    #[error(transparent)]
    Pinecone(#[from] PineconeError),
}

// Allow `?` on `mongodb::error::Error` in functions returning `Result<_, WriteError>`.
// Routes through `MongoDbError::Driver` so the full chain is `WriteError::MongoDB`.
impl From<::mongodb::error::Error> for WriteError {
    fn from(e: ::mongodb::error::Error) -> Self {
        WriteError::MongoDB(MongoDbError::Driver(e))
    }
}

// Allow `?` on `iceberg::Error` in functions returning `Result<_, WriteError>`.
// Routes through `IcebergError::Library` so the full chain is `WriteError::Iceberg`.
impl From<iceberg::Error> for WriteError {
    fn from(e: iceberg::Error) -> Self {
        WriteError::Iceberg(IcebergError::Library(e))
    }
}

// Allow `?` on `iceberg::Error` in functions returning `Result<_, ReadError>`.
// Routes through `IcebergError::Library` so the full chain is `ReadError::Iceberg`.
impl From<iceberg::Error> for ReadError {
    fn from(e: iceberg::Error) -> Self {
        ReadError::Iceberg(IcebergError::Library(e))
    }
}

// Allow `?` on raw `postgres::Error` and `SslError` in functions
// returning `Result<_, WriteError>`. Both route through the unified
// `PostgresError` so `WriteError` carries a single `Postgres(_)`
// variant.
impl From<::postgres::Error> for WriteError {
    fn from(e: ::postgres::Error) -> Self {
        WriteError::Postgres(PostgresError::Postgres(e))
    }
}

impl From<SslError> for WriteError {
    fn from(e: SslError) -> Self {
        WriteError::Postgres(PostgresError::Ssl(e))
    }
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

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum TableWriterInitMode {
    Default,
    CreateIfNotExists,
    Replace,
}

impl TableWriterInitMode {
    /// Runs the selected init mode against the destination table.
    ///
    /// `table_name` is interpolated verbatim into the generated DDL —
    /// the caller must pass the exact fragment that should appear after
    /// `CREATE TABLE IF NOT EXISTS` / `DROP TABLE IF EXISTS` (e.g.
    /// already double-quoted for `PostgreSQL`, bracket-qualified for SQL
    /// Server). `quote_ident` is applied to every column name and
    /// primary-key column, so user schemas whose columns clash with
    /// reserved words or require quoting work identically across
    /// writers.
    #[allow(clippy::too_many_arguments)]
    pub fn initialize(
        self,
        table_name: &str,
        value_fields: &[ValueField],
        key_field_names: Option<&[String]>,
        include_special_fields: bool,
        mut execute_query: impl FnMut(&str) -> Result<(), WriteError>,
        to_db_type: impl FnMut(&Type, bool) -> Result<String, WriteError>,
        quote_ident: impl Fn(&str) -> String,
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
                    quote_ident,
                )?;
            }
        }

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn create_table_if_not_exists(
        table_name: &str,
        schema: &[ValueField],
        key_field_names: Option<&[String]>,
        include_special_fields: bool,
        mut execute_query: impl FnMut(&str) -> Result<(), WriteError>,
        mut to_db_type: impl FnMut(&Type, bool) -> Result<String, WriteError>,
        quote_ident: impl Fn(&str) -> String,
    ) -> Result<(), WriteError> {
        let columns: Vec<String> = schema
            .iter()
            .map(|item| {
                to_db_type(&item.type_, false)
                    .map(|dtype_str| format!("{} {dtype_str}", quote_ident(&item.name)))
            })
            .collect::<Result<Vec<_>, _>>()?;

        let primary_key = key_field_names
            .as_ref()
            .filter(|keys| !keys.is_empty())
            .map_or(String::new(), |keys| {
                format!(
                    ", PRIMARY KEY ({})",
                    keys.iter().map(|k| quote_ident(k)).join(", ")
                )
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

/// Bundle of the four parameters that uniquely identify the table a
/// SQL connector is reading from or writing to, shared by the
/// `PostgreSQL`, `SQL Server`, and `SQLite` connectors so that
/// `Reader::new` / `Writer::new` constructors take one struct instead
/// of four positional arguments. `schema_name` is ignored by
/// connectors that don't have a schema concept (`SQLite`) — those
/// callers pass `String::new()`.
#[derive(Clone)]
pub struct TableContext {
    pub schema_name: String,
    pub table_name: String,
    pub value_fields: Vec<ValueField>,
    pub key_field_names: Option<Vec<String>>,
}

impl TableContext {
    pub fn new(
        schema_name: &str,
        table_name: &str,
        value_fields: &[ValueField],
        key_field_names: Option<&[String]>,
    ) -> Self {
        Self {
            schema_name: schema_name.to_string(),
            table_name: table_name.to_string(),
            value_fields: value_fields.to_vec(),
            key_field_names: key_field_names.map(<[String]>::to_vec),
        }
    }

    pub fn key_field_names_deref(&self) -> Option<&[String]> {
        self.key_field_names.as_deref()
    }
}

pub enum SqlQueryTemplate {
    StreamOfChanges(String),
    Snapshot {
        insertion: String,
        deletion: String,
        primary_key_indices: Vec<usize>,
    },
}

impl SqlQueryTemplate {
    /// Builds the INSERT/DELETE templates for a SQL sink.
    ///
    /// `table_name` is interpolated verbatim — the caller is responsible
    /// for producing the exact fragment that must appear between `INSERT
    /// INTO` and the column list (e.g. `"my-table"` for `PostgreSQL` or
    /// `[dbo].[MyTable]` for SQL Server), because some callers compose
    /// schema-qualified names up-front and re-quoting would corrupt
    /// them. `quote_ident` is applied to every other identifier — each
    /// `value_field.name` in the column list and the `key_field_names`
    /// in `ON CONFLICT` / `WHERE` clauses — so that user-supplied column
    /// names containing reserved words or characters that require
    /// quoting survive round-tripping.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        snapshot_mode: bool,
        table_name: &str,
        value_fields: &[ValueField],
        key_field_names: Option<&[String]>,
        include_special_columns_in_snapshot_mode: bool,
        mut wildcard_by_index: impl FnMut(usize) -> String,
        mut on_insert_conflict_condition: impl FnMut(&str, &[String], &[ValueField], bool) -> String,
        quote_ident: impl Fn(&str) -> String,
    ) -> Result<Self, WriteError> {
        // Pre-quote user identifiers so every downstream interpolation
        // sees the safe form. `time` / `diff` metadata columns are never
        // user-supplied, so they stay as plain literals.
        let quoted_value_fields: Vec<ValueField> = value_fields
            .iter()
            .map(|f| ValueField {
                name: quote_ident(&f.name),
                ..f.clone()
            })
            .collect();
        let quoted_key_field_names: Option<Vec<String>> =
            key_field_names.map(|keys| keys.iter().map(|k| quote_ident(k)).collect());
        let field_list = quoted_value_fields.iter().map(|f| &f.name).join(",");
        if snapshot_mode {
            let quoted_key_field_names =
                quoted_key_field_names.ok_or(WriteError::EmptyKeyFieldsForSnapshot)?;
            if quoted_key_field_names.is_empty() {
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
                        &quoted_key_field_names,
                        &quoted_value_fields,
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
                        &quoted_key_field_names,
                        &quoted_value_fields,
                        include_special_columns_in_snapshot_mode
                    ),
                )
            };

            let primary_key_indices = Self::primary_key_indices(
                value_fields,
                key_field_names.expect("key_field_names was Some"),
            )?;
            // DELETE tokens must follow `primary_key_indices` order
            // rather than the user-supplied `key_field_names` order,
            // because `primary_key_fields` (the function that selects
            // the bound parameters for DELETE) permutes the parameter
            // vector into the sorted-positional order that
            // `primary_key_indices` encodes. If the two disagreed, the
            // DELETE would compare each column against the wrong value
            // on retractions, silently failing to remove the row.
            //
            // Use the quoted column names so the WHERE clause survives
            // identifiers that PostgreSQL only accepts when quoted
            // (reserved words, hyphens, mixed case, ...). Without this,
            // a snapshot writer with a column named e.g. ``user`` would
            // emit ``DELETE ... WHERE user=$1``, which PG parses as
            // the reserved keyword and rejects.
            let quoted_value_field_names: Vec<String> =
                quoted_value_fields.iter().map(|f| f.name.clone()).collect();
            let tokens: Vec<_> = primary_key_indices
                .iter()
                .enumerate()
                .map(|(i, pkey_index)| {
                    format!(
                        "{}={}",
                        quoted_value_field_names[*pkey_index],
                        wildcard_by_index(i),
                    )
                })
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

    pub fn primary_key_indices(
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

    pub fn primary_key_fields<T>(&self, mut fields: Vec<T>) -> Vec<T> {
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

    pub fn build_query(&self, diff: isize) -> String {
        match self {
            Self::StreamOfChanges(query) => query.clone(),
            Self::Snapshot { insertion, .. } if diff > 0 => insertion.clone(),
            Self::Snapshot { deletion, .. } if diff < 0 => deletion.clone(),
            Self::Snapshot { .. } => unreachable!(),
        }
    }
}
