// Copyright © 2024 Pathway

use postgres::Transaction as PsqlTransaction;
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
use deltalake::arrow::datatypes::DataType as ArrowDataType;
use deltalake::arrow::error::ArrowError;
use deltalake::datafusion::common::DataFusionError;
use deltalake::datafusion::parquet::record::Field as ParquetValue;
use deltalake::parquet::errors::ParquetError;
use deltalake::DeltaTableError;
use futures::StreamExt;
use iceberg::Error as IcebergError;
use itertools::Itertools;
use log::{error, info, warn};
use postgres::types::ToSql;
use tokio::runtime::Runtime as TokioRuntime;

use crate::async_runtime::create_async_tokio_runtime;
use crate::connectors::data_format::{FormatterContext, FormatterError, COMMIT_LITERAL};
use crate::connectors::data_tokenize::{BufReaderTokenizer, CsvTokenizer};
use crate::connectors::metadata::{KafkaMetadata, SQLiteMetadata, SourceMetadata};
use crate::connectors::offset::EMPTY_OFFSET;
use crate::connectors::posix_like::PosixLikeReader;
use crate::connectors::scanner::s3::S3CommandName;
use crate::connectors::scanner::{FilesystemScanner, S3Scanner};
use crate::connectors::{Offset, OffsetKey, OffsetValue};
use crate::engine::error::limit_length;
use crate::engine::error::DynResult;
use crate::engine::error::STANDARD_OBJECT_LENGTH_LIMIT;
use crate::engine::Type;
use crate::engine::Value;
use crate::persistence::backends::Error as PersistenceBackendError;
use crate::persistence::frontier::OffsetAntichain;
use crate::persistence::tracker::WorkerPersistentStorage;
use crate::persistence::{PersistentId, UniqueName};
use crate::python_api::extract_value;
use crate::python_api::threads::PythonThreadState;
use crate::python_api::PythonSubject;

use async_nats::client::FlushError as NatsFlushError;
use async_nats::client::PublishError as NatsPublishError;
use async_nats::Client as NatsClient;
use async_nats::Subscriber as NatsSubscriber;
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
use s3::bucket::Bucket as S3Bucket;
use serde::{Deserialize, Serialize};

pub use super::data_lake::delta::{
    DeltaTableReader, ObjectDownloader, SchemaMismatchDetails as DeltaSchemaMismatchDetails,
};
pub use super::data_lake::iceberg::IcebergReader;
pub use super::data_lake::LakeWriter;

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

    pub fn remove(&mut self, key: &str) {
        self.map.remove(key);
    }

    pub fn to_pure_hashmap(self) -> DynResult<HashMap<String, Value>> {
        self.map
            .into_iter()
            .map(|(key, value)| Ok((key, value?)))
            .try_collect()
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
    fn get_for_posting(&self, values: &[Value]) -> Result<String, WriteError> {
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
pub enum ReadResult {
    Finished,
    NewSource(SourceMetadata),
    FinishedSource { commit_allowed: bool },
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
    Persistence(#[from] PersistenceBackendError),

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
}

#[derive(Debug, thiserror::Error, Clone, Eq, PartialEq)]
#[error("cannot create a field {field_name:?} with type {type_} from value {value_repr}")]
pub struct ConversionError {
    pub value_repr: String,
    pub field_name: String,
    pub type_: Type,
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
}

impl StorageType {
    pub fn merge_two_frontiers(
        &self,
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
        }
    }
}

pub fn new_filesystem_reader(
    path: &str,
    streaming_mode: ConnectorMode,
    read_method: ReadMethod,
    object_pattern: &str,
    is_persisted: bool,
) -> Result<PosixLikeReader, ReadError> {
    let scanner = FilesystemScanner::new(path, object_pattern)?;
    let tokenizer = BufReaderTokenizer::new(read_method);
    PosixLikeReader::new(
        Box::new(scanner),
        Box::new(tokenizer),
        streaming_mode,
        is_persisted,
    )
}

pub fn new_csv_filesystem_reader(
    path: &str,
    parser_builder: csv::ReaderBuilder,
    streaming_mode: ConnectorMode,
    object_pattern: &str,
    is_persisted: bool,
) -> Result<PosixLikeReader, ReadError> {
    let scanner = FilesystemScanner::new(path, object_pattern)?;
    let tokenizer = CsvTokenizer::new(parser_builder);
    PosixLikeReader::new(
        Box::new(scanner),
        Box::new(tokenizer),
        streaming_mode,
        is_persisted,
    )
}

pub fn new_s3_generic_reader(
    bucket: S3Bucket,
    objects_prefix: impl Into<String>,
    streaming_mode: ConnectorMode,
    read_method: ReadMethod,
    downloader_threads_count: usize,
    is_persisted: bool,
) -> Result<PosixLikeReader, ReadError> {
    let scanner = S3Scanner::new(bucket, objects_prefix, downloader_threads_count)?;
    let tokenizer = BufReaderTokenizer::new(read_method);
    PosixLikeReader::new(
        Box::new(scanner),
        Box::new(tokenizer),
        streaming_mode,
        is_persisted,
    )
}

pub fn new_s3_csv_reader(
    bucket: S3Bucket,
    objects_prefix: impl Into<String>,
    parser_builder: csv::ReaderBuilder,
    streaming_mode: ConnectorMode,
    downloader_threads_count: usize,
    is_persisted: bool,
) -> Result<PosixLikeReader, ReadError> {
    let scanner = S3Scanner::new(bucket, objects_prefix, downloader_threads_count)?;
    let tokenizer = CsvTokenizer::new(parser_builder);
    PosixLikeReader::new(
        Box::new(scanner),
        Box::new(tokenizer),
        streaming_mode,
        is_persisted,
    )
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
        _: &WorkerPersistentStorage,
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
            unique_name.to_string()
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
    NatsFlush(#[from] NatsFlushError),

    #[error(transparent)]
    IcebergError(#[from] IcebergError),

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

    #[error("dynamic topic name is not a string field: {0}")]
    DynamicTopicIsNotAString(Value),

    #[error("delta table schema mismatch: {0}")]
    DeltaTableSchemaMismatch(DeltaSchemaMismatchDetails),
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
    pub fn is_polling_enabled(&self) -> bool {
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
            unique_name.to_string()
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
    fn conversion_error(ob: &Bound<PyAny>, name: String, type_: Type) -> ConversionError {
        let value_repr = limit_length(format!("{ob}"), STANDARD_OBJECT_LENGTH_LIMIT);
        ConversionError {
            value_repr,
            field_name: name,
            type_,
        }
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
                let py_external_offset = PyBytes::new_bound(py, &data).unbind().into_any();
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
                let value = extract_value(ob.bind(py), dtype).map_err(|_err| {
                    Box::new(Self::conversion_error(
                        ob.bind(py),
                        name.clone(),
                        dtype.clone(),
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
                            commit_allowed: true,
                        })
                    }
                    SpecialEvent::DisableAutocommits => {
                        return Ok(ReadResult::FinishedSource {
                            commit_allowed: false,
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
}

impl PsqlWriter {
    pub fn new(
        client: PsqlClient,
        max_batch_size: Option<usize>,
        snapshot_mode: bool,
        table_name: &str,
        schema: &HashMap<String, Type>,
        key_field_names: &Option<Vec<String>>,
        mode: SqlWriterInitMode,
    ) -> Result<PsqlWriter, WriteError> {
        let mut writer = PsqlWriter {
            client,
            max_batch_size,
            buffer: Vec::new(),
            snapshot_mode,
            table_name: table_name.to_string(),
        };
        writer.initialize(mode, table_name, schema, key_field_names)?;
        Ok(writer)
    }

    pub fn initialize(
        &mut self,
        mode: SqlWriterInitMode,
        table_name: &str,
        schema: &HashMap<String, Type>,
        key_field_names: &Option<Vec<String>>,
    ) -> Result<(), WriteError> {
        match mode {
            SqlWriterInitMode::Default => return Ok(()),
            SqlWriterInitMode::Replace | SqlWriterInitMode::CreateIfNotExists => {
                let mut transaction = self.client.transaction()?;

                if mode == SqlWriterInitMode::Replace {
                    Self::drop_table_if_exists(&mut transaction, table_name)?;
                }
                Self::create_table_if_not_exists(
                    &mut transaction,
                    table_name,
                    schema,
                    key_field_names,
                )?;

                transaction.commit()?;
            }
        }

        Ok(())
    }

    fn create_table_if_not_exists(
        transaction: &mut PsqlTransaction,
        table_name: &str,
        schema: &HashMap<String, Type>,
        key_field_names: &Option<Vec<String>>,
    ) -> Result<(), WriteError> {
        let columns: Vec<String> = schema
            .iter()
            .map(|(name, dtype)| {
                Self::postgres_data_type(dtype).map(|dtype_str| format!("{name} {dtype_str}"))
            })
            .collect::<Result<Vec<_>, _>>()?;

        let primary_key = key_field_names
            .as_ref()
            .filter(|keys| !keys.is_empty())
            .map_or(String::new(), |keys| {
                format!(", PRIMARY KEY ({})", keys.join(", "))
            });

        transaction.execute(
            &format!(
                "CREATE TABLE IF NOT EXISTS {} ({}, time BIGINT, diff BIGINT{})",
                table_name,
                columns.join(", "),
                primary_key
            ),
            &[],
        )?;

        Ok(())
    }

    fn drop_table_if_exists(
        transaction: &mut PsqlTransaction,
        table_name: &str,
    ) -> Result<(), WriteError> {
        let query = format!("DROP TABLE IF EXISTS {table_name}");
        transaction.execute(&query, &[])?;
        Ok(())
    }

    fn postgres_data_type(type_: &Type) -> Result<String, WriteError> {
        Ok(match type_ {
            Type::Bool => "BOOLEAN".to_string(),
            Type::Int | Type::Duration => "BIGINT".to_string(),
            Type::Float => "DOUBLE PRECISION".to_string(),
            Type::Pointer | Type::String => "TEXT".to_string(),
            Type::Bytes | Type::PyObjectWrapper => "BYTEA".to_string(),
            Type::Json => "JSONB".to_string(),
            Type::DateTimeNaive => "TIMESTAMP".to_string(),
            Type::DateTimeUtc => "TIMESTAMPTZ".to_string(),
            Type::Optional(wrapped) | Type::List(wrapped) => {
                if let Type::Any = **wrapped {
                    return Err(WriteError::UnsupportedType(type_.clone()));
                }

                let wrapped = Self::postgres_data_type(wrapped)?;
                if let Type::Optional(_) = type_ {
                    return Ok(wrapped);
                }
                format!("{wrapped}[]")
            }
            Type::Tuple(fields) => {
                let mut iter = fields.iter();
                if !fields.is_empty() && iter.all(|field| field == &fields[0]) {
                    let first = Self::postgres_data_type(&fields[0])?;
                    return Ok(format!("{first}[]"));
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
pub enum SqlWriterInitMode {
    Default,
    CreateIfNotExists,
    Replace,
}

mod to_sql {
    use std::error::Error;

    use bytes::BytesMut;
    use chrono::{DateTime, NaiveDateTime, Utc};
    use ordered_float::OrderedFloat;
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
                Self::IntArray(_) => "int array",     // TODO
                Self::FloatArray(_) => "float array", // TODO
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
            let params: Vec<_> = data
                .values
                .iter()
                .map(|v| v as &(dyn ToSql + Sync))
                .collect();

            for payload in data.payloads {
                let payload = payload.into_raw_bytes()?;
                let query = from_utf8(&payload)?;

                transaction
                    .execute(query, params.as_slice())
                    .map_err(|error| WriteError::PsqlQueryFailed {
                        query: query.to_string(),
                        error,
                    })?;
            }
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
                        continue;
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
        value: SqliteValue<'_>,
        field_name: &str,
        dtype: &Type,
    ) -> Result<Value, Box<ConversionError>> {
        let value = match (dtype, value) {
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
            let value_repr = limit_length(format!("{value:?}"), STANDARD_OBJECT_LENGTH_LIMIT);
            Err(Box::new(ConversionError {
                value_repr,
                field_name: field_name.to_owned(),
                type_: dtype.clone(),
            }))
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
                commit_allowed: true,
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

pub struct NatsReader {
    runtime: TokioRuntime,
    subscriber: NatsSubscriber,
    worker_index: usize,
    total_entries_read: usize,
    stream_name: String,
}

impl Reader for NatsReader {
    fn read(&mut self) -> Result<ReadResult, ReadError> {
        if let Some(message) = self.runtime.block_on(async {
            self.subscriber
                .next()
                .await
                .map(|message| message.payload.to_vec())
        }) {
            let payload = ReaderContext::from_raw_bytes(DataEventType::Insert, message);
            self.total_entries_read += 1;
            let offset = (
                OffsetKey::Nats(self.worker_index),
                OffsetValue::NatsReadEntriesCount(self.total_entries_read),
            );
            Ok(ReadResult::Data(payload, offset))
        } else {
            Ok(ReadResult::Finished)
        }
    }

    fn seek(&mut self, frontier: &OffsetAntichain) -> Result<(), ReadError> {
        let offset_value = frontier.get_offset(&OffsetKey::Nats(self.worker_index));
        if let Some(offset) = offset_value {
            if let OffsetValue::NatsReadEntriesCount(last_run_entries_read) = offset {
                self.total_entries_read = *last_run_entries_read;
            } else {
                error!("Unexpected offset type for NATS reader: {offset:?}");
            }
        }
        Ok(())
    }

    fn short_description(&self) -> Cow<'static, str> {
        format!("NATS({})", self.stream_name).into()
    }

    fn storage_type(&self) -> StorageType {
        StorageType::Nats
    }

    fn max_allowed_consecutive_errors(&self) -> usize {
        32
    }
}

impl NatsReader {
    pub fn new(
        runtime: TokioRuntime,
        subscriber: NatsSubscriber,
        worker_index: usize,
        stream_name: String,
    ) -> NatsReader {
        NatsReader {
            runtime,
            subscriber,
            worker_index,
            stream_name,
            total_entries_read: 0,
        }
    }
}

pub struct NatsWriter {
    runtime: TokioRuntime,
    client: NatsClient,
    topic: MessageQueueTopic,
    header_fields: Vec<(String, usize)>,
}

impl Writer for NatsWriter {
    fn write(&mut self, data: FormatterContext) -> Result<(), WriteError> {
        self.runtime.block_on(async {
            let last_payload_index = data.payloads.len() - 1;
            let mut common_headers = data.construct_nats_headers(&self.header_fields);
            for (index, payload) in data.payloads.into_iter().enumerate() {
                // Avoid copying data on the last iteration, reuse the existing headers
                let headers = {
                    if index == last_payload_index {
                        take(&mut common_headers)
                    } else {
                        common_headers.clone()
                    }
                };
                let payload = payload.into_raw_bytes()?;
                let effective_topic = self.topic.get_for_posting(&data.values)?;
                self.client
                    .publish_with_headers(effective_topic, headers, payload.into())
                    .await
                    .map_err(WriteError::NatsPublish)?;
            }
            Ok(())
        })
    }

    fn flush(&mut self, _forced: bool) -> Result<(), WriteError> {
        self.runtime
            .block_on(async { self.client.flush().await })
            .map_err(WriteError::NatsFlush)
    }

    fn name(&self) -> String {
        format!("NATS({})", self.topic)
    }

    fn retriable(&self) -> bool {
        true
    }

    fn single_threaded(&self) -> bool {
        false
    }
}

impl Drop for NatsWriter {
    fn drop(&mut self) {
        self.flush(true).expect("failed to send the final messages");
    }
}

impl NatsWriter {
    pub fn new(
        runtime: TokioRuntime,
        client: NatsClient,
        topic: MessageQueueTopic,
        header_fields: Vec<(String, usize)>,
    ) -> Self {
        NatsWriter {
            runtime,
            client,
            topic,
            header_fields,
        }
    }
}
