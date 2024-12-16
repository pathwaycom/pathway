// Copyright © 2024 Pathway

use pyo3::exceptions::PyValueError;
use pyo3::types::PyBytes;
use rdkafka::util::Timeout;
use s3::error::S3Error;
use std::any::type_name;
use std::borrow::Borrow;
use std::borrow::Cow;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::fs::File;
use std::io;
use std::io::BufRead;
use std::io::BufWriter;
use std::io::Write;
use std::io::{Seek, SeekFrom};
use std::mem::take;
use std::path::Path;
use std::str::{from_utf8, Utf8Error};
use std::sync::Arc;
use std::thread::sleep;
use std::time::{Duration, Instant};

use arcstr::ArcStr;
use futures::StreamExt;
use itertools::Itertools;
use log::{error, info, warn};
use postgres::types::ToSql;
use tempfile::tempfile;
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
use crate::engine::time::DateTime as EngineDateTime;
use crate::engine::Type;
use crate::engine::Value;
use crate::engine::{DateTimeNaive, DateTimeUtc, Duration as EngineDuration};
use crate::persistence::backends::Error as PersistenceBackendError;
use crate::persistence::frontier::OffsetAntichain;
use crate::persistence::tracker::WorkerPersistentStorage;
use crate::persistence::{ExternalPersistentId, PersistentId};
use crate::python_api::extract_value;
use crate::python_api::threads::PythonThreadState;
use crate::python_api::PythonSubject;
use crate::python_api::ValueField;

use async_nats::client::FlushError as NatsFlushError;
use async_nats::client::PublishError as NatsPublishError;
use async_nats::Client as NatsClient;
use async_nats::Subscriber as NatsSubscriber;
use bincode::ErrorKind as BincodeError;
use deltalake::arrow::array::Array as ArrowArray;
use deltalake::arrow::array::RecordBatch as DTRecordBatch;
use deltalake::arrow::array::{
    BinaryArray as ArrowBinaryArray, BooleanArray as ArrowBooleanArray,
    Float64Array as ArrowFloat64Array, Int64Array as ArrowInt64Array,
    StringArray as ArrowStringArray, TimestampMicrosecondArray as ArrowTimestampArray,
};
use deltalake::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
    TimeUnit as ArrowTimeUnit,
};
use deltalake::arrow::error::ArrowError;
use deltalake::datafusion::parquet::file::reader::SerializedFileReader as DeltaLakeParquetReader;
use deltalake::datafusion::parquet::record::Field as ParquetValue;
use deltalake::kernel::Action as DeltaLakeAction;
use deltalake::kernel::DataType as DeltaTableKernelType;
use deltalake::kernel::PrimitiveType as DeltaTablePrimitiveType;
use deltalake::kernel::StructField as DeltaTableStructField;
use deltalake::operations::create::CreateBuilder as DeltaTableCreateBuilder;
use deltalake::parquet::errors::ParquetError;
use deltalake::parquet::file::reader::FileReader as DeltaLakeParquetFileReader;
use deltalake::parquet::record::reader::RowIter as ParquetRowIterator;
use deltalake::parquet::record::Row as ParquetRow;
use deltalake::protocol::SaveMode as DeltaTableSaveMode;
use deltalake::table::PeekCommit as DeltaLakePeekCommit;
use deltalake::writer::{DeltaWriter, RecordBatchWriter as DTRecordBatchWriter};
use deltalake::{
    open_table_with_storage_options as open_delta_table, DeltaConfigKey, DeltaTable,
    DeltaTableError,
};
use elasticsearch::{BulkParts, Elasticsearch};
use glob::PatternError as GlobPatternError;
use mongodb::bson::Document as BsonDocument;
use mongodb::error::Error as MongoError;
use mongodb::sync::Collection as MongoCollection;
use postgres::Client as PsqlClient;
use pyo3::prelude::*;
use rdkafka::consumer::{BaseConsumer, Consumer, DefaultConsumerContext};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::producer::{BaseRecord, DefaultProducerContext, Producer, ThreadedProducer};
use rdkafka::topic_partition_list::Offset as KafkaOffset;
use rdkafka::Message;
use rusqlite::types::ValueRef as SqliteValue;
use rusqlite::Connection as SqliteConnection;
use rusqlite::Error as SqliteError;
use s3::bucket::Bucket as S3Bucket;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Copy)]
pub enum DataEventType {
    Insert,
    Delete,
    Upsert,
}

#[derive(Clone, Debug, Eq, PartialEq, Copy)]
pub enum PythonConnectorEventType {
    Insert,
    Delete,
    Upsert,
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
    Persistence(#[from] PersistenceBackendError),

    #[error("malformed data")]
    MalformedData,

    #[error("no objects to read")]
    NoObjectsToRead,

    #[error("invalid special value: {0}")]
    InvalidSpecialValue(String),

    #[error("parquet value type mismatch: got {0:?} expected {1:?}")]
    WrongParquetType(ParquetValue, Type),

    #[error("only append-only delta tables are supported")]
    DeltaLakeForbiddenRemoval,
}

#[derive(Debug, thiserror::Error, Clone, Eq, PartialEq)]
#[error("cannot create a field {field_name:?} with type {type_} from value {value_repr}")]
pub struct ConversionError {
    value_repr: String,
    field_name: String,
    type_: Type,
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
        }
    }
}

pub fn new_filesystem_reader(
    path: &str,
    streaming_mode: ConnectorMode,
    persistent_id: Option<PersistentId>,
    read_method: ReadMethod,
    object_pattern: &str,
) -> Result<PosixLikeReader, ReadError> {
    let scanner = FilesystemScanner::new(path, object_pattern)?;
    let tokenizer = BufReaderTokenizer::new(read_method);
    PosixLikeReader::new(
        Box::new(scanner),
        Box::new(tokenizer),
        streaming_mode,
        persistent_id,
    )
}

pub fn new_csv_filesystem_reader(
    path: &str,
    parser_builder: csv::ReaderBuilder,
    streaming_mode: ConnectorMode,
    persistent_id: Option<PersistentId>,
    object_pattern: &str,
) -> Result<PosixLikeReader, ReadError> {
    let scanner = FilesystemScanner::new(path, object_pattern)?;
    let tokenizer = CsvTokenizer::new(parser_builder);
    PosixLikeReader::new(
        Box::new(scanner),
        Box::new(tokenizer),
        streaming_mode,
        persistent_id,
    )
}

pub fn new_s3_generic_reader(
    bucket: S3Bucket,
    objects_prefix: impl Into<String>,
    streaming_mode: ConnectorMode,
    persistent_id: Option<PersistentId>,
    read_method: ReadMethod,
    downloader_threads_count: usize,
) -> Result<PosixLikeReader, ReadError> {
    let scanner = S3Scanner::new(bucket, objects_prefix, downloader_threads_count)?;
    let tokenizer = BufReaderTokenizer::new(read_method);
    PosixLikeReader::new(
        Box::new(scanner),
        Box::new(tokenizer),
        streaming_mode,
        persistent_id,
    )
}

pub fn new_s3_csv_reader(
    bucket: S3Bucket,
    objects_prefix: impl Into<String>,
    parser_builder: csv::ReaderBuilder,
    streaming_mode: ConnectorMode,
    persistent_id: Option<PersistentId>,
    downloader_threads_count: usize,
) -> Result<PosixLikeReader, ReadError> {
    let scanner = S3Scanner::new(bucket, objects_prefix, downloader_threads_count)?;
    let tokenizer = CsvTokenizer::new(parser_builder);
    PosixLikeReader::new(
        Box::new(scanner),
        Box::new(tokenizer),
        streaming_mode,
        persistent_id,
    )
}

pub trait Reader {
    fn read(&mut self) -> Result<ReadResult, ReadError>;

    #[allow(clippy::missing_errors_doc)]
    fn seek(&mut self, frontier: &OffsetAntichain) -> Result<(), ReadError>;

    fn update_persistent_id(&mut self, persistent_id: Option<PersistentId>);
    fn persistent_id(&self) -> Option<PersistentId>;
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

    fn short_description(&self) -> Cow<'static, str> {
        type_name::<Self>().into()
    }

    fn name(&self, persistent_id: Option<&ExternalPersistentId>, id: usize) -> String {
        let desc = self.short_description();
        let name = desc.split("::").last().unwrap().replace("Builder", "");
        if let Some(id) = persistent_id {
            format!("{name}-{id}")
        } else {
            format!("{name}-{id}")
        }
    }

    fn is_internal(&self) -> bool {
        false
    }

    fn persistent_id(&self) -> Option<PersistentId>;
    fn update_persistent_id(&mut self, persistent_id: Option<PersistentId>);

    fn storage_type(&self) -> StorageType;
}

impl<T> ReaderBuilder for T
where
    T: Reader + Send + 'static,
{
    fn build(self: Box<Self>) -> Result<Box<dyn Reader>, ReadError> {
        Ok(self)
    }

    fn persistent_id(&self) -> Option<PersistentId> {
        Reader::persistent_id(self)
    }

    fn update_persistent_id(&mut self, persistent_id: Option<PersistentId>) {
        Reader::update_persistent_id(self, persistent_id);
    }

    fn storage_type(&self) -> StorageType {
        Reader::storage_type(self)
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

    fn short_description(&self) -> Cow<'static, str> {
        type_name::<Self>().into()
    }

    fn name(&self, id: usize) -> String {
        let name = self
            .short_description()
            .split("::")
            .last()
            .unwrap()
            .to_string();
        format!("{name}-{id}")
    }
}

pub struct FileWriter {
    writer: BufWriter<std::fs::File>,
}

impl FileWriter {
    pub fn new(writer: BufWriter<std::fs::File>) -> FileWriter {
        FileWriter { writer }
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
}

pub struct KafkaReader {
    consumer: BaseConsumer<DefaultConsumerContext>,
    persistent_id: Option<PersistentId>,
    topic: ArcStr,
    positions_for_seek: HashMap<i32, KafkaOffset>,
    deferred_read_result: Option<ReadResult>,
}

impl Reader for KafkaReader {
    fn read(&mut self) -> Result<ReadResult, ReadError> {
        if let Some(deferred_read_result) = take(&mut self.deferred_read_result) {
            return Ok(deferred_read_result);
        }

        loop {
            let kafka_message = self
                .consumer
                .poll(Timeout::Never)
                .expect("poll should never timeout")?;
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
            let message = ReaderContext::from_key_value(message_key, message_payload);
            self.deferred_read_result = Some(ReadResult::Data(message, offset));

            let metadata = KafkaMetadata::from_rdkafka_message(&kafka_message);
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

    fn persistent_id(&self) -> Option<PersistentId> {
        self.persistent_id
    }

    fn update_persistent_id(&mut self, persistent_id: Option<PersistentId>) {
        self.persistent_id = persistent_id;
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
        persistent_id: Option<PersistentId>,
        positions_for_seek: HashMap<i32, KafkaOffset>,
    ) -> KafkaReader {
        KafkaReader {
            consumer,
            persistent_id,
            topic: topic.into(),
            positions_for_seek,
            deferred_read_result: None,
        }
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
    persistent_id: Option<PersistentId>,
    schema: HashMap<String, Type>,
}

pub struct PythonReader {
    subject: Py<PythonSubject>,
    persistent_id: Option<PersistentId>,
    schema: HashMap<String, Type>,
    total_entries_read: u64,
    current_external_offset: Arc<[u8]>,
    is_initialized: bool,
    is_finished: bool,

    #[allow(unused)]
    python_thread_state: PythonThreadState,
}

impl PythonReaderBuilder {
    pub fn new(
        subject: Py<PythonSubject>,
        persistent_id: Option<PersistentId>,
        schema: HashMap<String, Type>,
    ) -> Self {
        Self {
            subject,
            persistent_id,
            schema,
        }
    }
}

impl ReaderBuilder for PythonReaderBuilder {
    fn build(self: Box<Self>) -> Result<Box<dyn Reader>, ReadError> {
        let python_thread_state = PythonThreadState::new();
        let Self {
            subject,
            persistent_id,
            schema,
        } = *self;

        Ok(Box::new(PythonReader {
            subject,
            persistent_id,
            schema,
            python_thread_state,
            total_entries_read: 0,
            is_initialized: false,
            is_finished: false,
            current_external_offset: vec![].into(),
        }))
    }

    fn is_internal(&self) -> bool {
        self.subject.get().is_internal
    }

    fn persistent_id(&self) -> Option<PersistentId> {
        self.persistent_id
    }

    fn update_persistent_id(&mut self, persistent_id: Option<PersistentId>) {
        self.persistent_id = persistent_id;
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
                PythonConnectorEventType::Upsert => DataEventType::Upsert,
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

    fn persistent_id(&self) -> Option<PersistentId> {
        self.persistent_id
    }

    fn update_persistent_id(&mut self, persistent_id: Option<PersistentId>) {
        self.persistent_id = persistent_id;
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
}

impl PsqlWriter {
    pub fn new(
        client: PsqlClient,
        max_batch_size: Option<usize>,
        snapshot_mode: bool,
    ) -> PsqlWriter {
        PsqlWriter {
            client,
            max_batch_size,
            buffer: Vec::new(),
            snapshot_mode,
        }
    }
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
                Self::Duration(_) => "duration", // TODO
                Self::Json(j) => {
                    try_forward!(&serde_json::Value, &**j);
                    "JSON"
                }
                Self::Error => "error",
                Self::PyObjectWrapper(_) => "PyObjectWrapper",
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

    fn single_threaded(&self) -> bool {
        self.snapshot_mode
    }
}

pub struct KafkaWriter {
    producer: ThreadedProducer<DefaultProducerContext>,
    topic: String,
    header_fields: Vec<(String, usize)>,
    key_field_index: Option<usize>,
}

impl KafkaWriter {
    pub fn new(
        producer: ThreadedProducer<DefaultProducerContext>,
        topic: String,
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
            let mut entry = BaseRecord::<Vec<u8>, Vec<u8>>::to(&self.topic)
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

    fn storage_type(&self) -> StorageType {
        StorageType::Sqlite
    }

    fn persistent_id(&self) -> Option<PersistentId> {
        None
    }

    fn update_persistent_id(&mut self, persistent_id: Option<PersistentId>) {
        if persistent_id.is_some() {
            unimplemented!("persistence is not supported for Sqlite data source")
        }
    }
}

const SPECIAL_OUTPUT_FIELDS: [(&str, Type); 2] = [("time", Type::Int), ("diff", Type::Int)];

pub struct DeltaTableWriter {
    table: DeltaTable,
    writer: DTRecordBatchWriter,
    schema: Arc<ArrowSchema>,
    buffered_columns: Vec<Vec<Value>>,
    min_commit_frequency: Option<Duration>,
    last_commit_at: Instant,
}

impl DeltaTableWriter {
    pub fn new(
        path: &str,
        value_fields: &Vec<ValueField>,
        storage_options: HashMap<String, String>,
        min_commit_frequency: Option<Duration>,
    ) -> Result<Self, WriteError> {
        let schema = Arc::new(Self::construct_schema(value_fields)?);
        let table = Self::open_table(path, value_fields, storage_options)?;
        let writer = DTRecordBatchWriter::for_table(&table)?;

        let mut empty_buffered_columns = Vec::new();
        for _ in 0..schema.flattened_fields().len() {
            empty_buffered_columns.push(Vec::new());
        }
        Ok(Self {
            table,
            writer,
            schema,
            buffered_columns: empty_buffered_columns,
            min_commit_frequency,

            // before the first commit, the time should be
            // measured from the moment of the start
            last_commit_at: Instant::now(),
        })
    }

    fn array_of_target_type<ElementType>(
        values: &Vec<Value>,
        mut to_simple_type: impl FnMut(&Value) -> Result<ElementType, WriteError>,
    ) -> Result<Vec<Option<ElementType>>, WriteError> {
        let mut values_vec: Vec<Option<ElementType>> = Vec::new();
        for value in values {
            if matches!(value, Value::None) {
                values_vec.push(None);
                continue;
            }
            values_vec.push(Some(to_simple_type(value)?));
        }
        Ok(values_vec)
    }

    fn arrow_array_for_type(
        type_: &ArrowDataType,
        values: &Vec<Value>,
    ) -> Result<Arc<dyn ArrowArray>, WriteError> {
        match type_ {
            ArrowDataType::Boolean => {
                let v = Self::array_of_target_type::<bool>(values, |v| match v {
                    Value::Bool(b) => Ok(*b),
                    _ => Err(WriteError::TypeMismatchWithSchema(v.clone(), type_.clone())),
                })?;
                Ok(Arc::new(ArrowBooleanArray::from(v)))
            }
            ArrowDataType::Int64 => {
                let v = Self::array_of_target_type::<i64>(values, |v| match v {
                    Value::Int(i) => Ok(*i),
                    Value::Duration(d) => Ok(d.microseconds()),
                    _ => Err(WriteError::TypeMismatchWithSchema(v.clone(), type_.clone())),
                })?;
                Ok(Arc::new(ArrowInt64Array::from(v)))
            }
            ArrowDataType::Float64 => {
                let v = Self::array_of_target_type::<f64>(values, |v| match v {
                    Value::Float(f) => Ok((*f).into()),
                    _ => Err(WriteError::TypeMismatchWithSchema(v.clone(), type_.clone())),
                })?;
                Ok(Arc::new(ArrowFloat64Array::from(v)))
            }
            ArrowDataType::Utf8 => {
                let v = Self::array_of_target_type::<String>(values, |v| match v {
                    Value::String(s) => Ok(s.to_string()),
                    Value::Pointer(p) => Ok(p.to_string()),
                    Value::Json(j) => Ok(j.to_string()),
                    _ => Err(WriteError::TypeMismatchWithSchema(v.clone(), type_.clone())),
                })?;
                Ok(Arc::new(ArrowStringArray::from(v)))
            }
            ArrowDataType::Binary => {
                let mut vec_owned = Self::array_of_target_type::<Vec<u8>>(values, |v| match v {
                    Value::Bytes(b) => Ok(b.to_vec()),
                    _ => Err(WriteError::TypeMismatchWithSchema(v.clone(), type_.clone())),
                })?;
                let mut vec_refs = Vec::new();
                for item in &mut vec_owned {
                    vec_refs.push(item.as_mut().map(|v| v.as_slice()));
                }
                Ok(Arc::new(ArrowBinaryArray::from(vec_refs)))
            }
            ArrowDataType::Timestamp(ArrowTimeUnit::Microsecond, None) => {
                let v = Self::array_of_target_type::<i64>(values, |v| match v {
                    #[allow(clippy::cast_possible_truncation)]
                    Value::DateTimeNaive(dt) => Ok(dt.timestamp_microseconds()),
                    _ => Err(WriteError::TypeMismatchWithSchema(v.clone(), type_.clone())),
                })?;
                Ok(Arc::new(ArrowTimestampArray::from(v)))
            }
            ArrowDataType::Timestamp(ArrowTimeUnit::Microsecond, Some(tz)) => {
                let v = Self::array_of_target_type::<i64>(values, |v| match v {
                    #[allow(clippy::cast_possible_truncation)]
                    Value::DateTimeUtc(dt) => Ok(dt.timestamp_microseconds()),
                    _ => Err(WriteError::TypeMismatchWithSchema(v.clone(), type_.clone())),
                })?;
                Ok(Arc::new(ArrowTimestampArray::from(v).with_timezone(&**tz)))
            }
            _ => panic!("provided type {type_} is unknown to the engine"),
        }
    }

    fn prepare_delta_batch(&self) -> Result<DTRecordBatch, WriteError> {
        let mut data_columns = Vec::new();
        for (index, column) in self.buffered_columns.iter().enumerate() {
            data_columns.push(Self::arrow_array_for_type(
                self.schema.field(index).data_type(),
                column,
            )?);
        }
        Ok(DTRecordBatch::try_new(self.schema.clone(), data_columns)?)
    }

    fn delta_table_primitive_type(type_: &Type) -> Result<DeltaTableKernelType, WriteError> {
        Ok(DeltaTableKernelType::Primitive(match type_ {
            Type::Bool => DeltaTablePrimitiveType::Boolean,
            Type::Float => DeltaTablePrimitiveType::Double,
            Type::String | Type::Json => DeltaTablePrimitiveType::String,
            Type::Bytes => DeltaTablePrimitiveType::Binary,
            Type::DateTimeNaive => DeltaTablePrimitiveType::TimestampNtz,
            Type::DateTimeUtc => DeltaTablePrimitiveType::Timestamp,
            Type::Int | Type::Duration => DeltaTablePrimitiveType::Long,
            Type::Optional(wrapped) => return Self::delta_table_primitive_type(wrapped),
            Type::Any
            | Type::Array(_, _)
            | Type::Tuple(_)
            | Type::List(_)
            | Type::PyObjectWrapper
            | Type::Pointer => return Err(WriteError::UnsupportedType(type_.clone())),
        }))
    }

    fn arrow_data_type(type_: &Type) -> Result<ArrowDataType, WriteError> {
        Ok(match type_ {
            Type::Bool => ArrowDataType::Boolean,
            Type::Int | Type::Duration => ArrowDataType::Int64,
            Type::Float => ArrowDataType::Float64,
            Type::Pointer | Type::String | Type::Json => ArrowDataType::Utf8,
            Type::Bytes => ArrowDataType::Binary,
            // DeltaLake timestamps are stored in microseconds:
            // https://docs.rs/deltalake/latest/deltalake/kernel/enum.PrimitiveType.html#variant.Timestamp
            Type::DateTimeNaive => ArrowDataType::Timestamp(ArrowTimeUnit::Microsecond, None),
            Type::DateTimeUtc => {
                ArrowDataType::Timestamp(ArrowTimeUnit::Microsecond, Some("UTC".into()))
            }
            Type::Optional(wrapped) => return Self::arrow_data_type(wrapped),
            Type::Any
            | Type::Array(_, _)
            | Type::Tuple(_)
            | Type::List(_)
            | Type::PyObjectWrapper => return Err(WriteError::UnsupportedType(type_.clone())),
        })
    }

    pub fn construct_schema(value_fields: &Vec<ValueField>) -> Result<ArrowSchema, WriteError> {
        let mut schema_fields: Vec<ArrowField> = Vec::new();
        for field in value_fields {
            schema_fields.push(ArrowField::new(
                field.name.clone(),
                Self::arrow_data_type(&field.type_)?,
                field.type_.can_be_none(),
            ));
        }
        for (field, type_) in SPECIAL_OUTPUT_FIELDS {
            schema_fields.push(ArrowField::new(
                field,
                Self::arrow_data_type(&type_)?,
                false,
            ));
        }
        Ok(ArrowSchema::new(schema_fields))
    }

    pub fn open_table(
        path: &str,
        schema_fields: &Vec<ValueField>,
        storage_options: HashMap<String, String>,
    ) -> Result<DeltaTable, WriteError> {
        let mut struct_fields = Vec::new();
        for field in schema_fields {
            struct_fields.push(DeltaTableStructField::new(
                field.name.clone(),
                Self::delta_table_primitive_type(&field.type_)?,
                field.type_.can_be_none(),
            ));
        }
        for (field, type_) in SPECIAL_OUTPUT_FIELDS {
            struct_fields.push(DeltaTableStructField::new(
                field,
                Self::delta_table_primitive_type(&type_)?,
                false,
            ));
        }

        let runtime = create_async_tokio_runtime()?;
        let table: DeltaTable = runtime
            .block_on(async {
                let builder = DeltaTableCreateBuilder::new()
                    .with_location(path)
                    .with_save_mode(DeltaTableSaveMode::Append)
                    .with_columns(struct_fields)
                    .with_configuration_property(DeltaConfigKey::AppendOnly, Some("true"))
                    .with_storage_options(storage_options.clone());

                builder.await
            })
            .or_else(
                |e| {
                    warn!("Unable to create DeltaTable for output: {e}. Trying to open the existing one by this path.");
                    runtime.block_on(async {
                        open_delta_table(path, storage_options).await
                    })
                }
            )?;

        Ok(table)
    }
}

impl Writer for DeltaTableWriter {
    fn write(&mut self, data: FormatterContext) -> Result<(), WriteError> {
        for (index, value) in data.values.into_iter().enumerate() {
            self.buffered_columns[index].push(value);
        }
        let time_column_idx = self.buffered_columns.len() - 2;
        let diff_column_idx = self.buffered_columns.len() - 1;
        self.buffered_columns[time_column_idx].push(Value::Int(data.time.0.try_into().unwrap()));
        self.buffered_columns[diff_column_idx].push(Value::Int(data.diff.try_into().unwrap()));
        Ok(())
    }

    fn flush(&mut self, forced: bool) -> Result<(), WriteError> {
        let commit_needed = !self.buffered_columns[0].is_empty()
            && (self
                .min_commit_frequency
                .map_or(true, |f| self.last_commit_at.elapsed() >= f)
                || forced);
        if commit_needed {
            // Deadlocks if new_current_thread is used
            create_async_tokio_runtime()?.block_on(async {
                self.writer.write(self.prepare_delta_batch()?).await?;
                self.writer.flush_and_commit(&mut self.table).await?;
                for column in &mut self.buffered_columns {
                    column.clear();
                }
                Ok::<(), WriteError>(())
            })?;
        }
        Ok(())
    }
}

pub enum ObjectDownloader {
    Local,
    S3(S3Bucket),
}

impl ObjectDownloader {
    fn download_object(&self, path: &str) -> Result<File, ReadError> {
        let obj = match self {
            Self::Local => File::open(path)?,
            Self::S3(bucket) => {
                let contents = S3Scanner::download_object_from_path_and_bucket(path, bucket)?;
                let mut tempfile = tempfile()?;
                tempfile.write_all(contents.bytes())?;
                tempfile.flush()?;
                tempfile.seek(SeekFrom::Start(0))?;
                tempfile
            }
        };
        Ok(obj)
    }
}

pub struct DeltaTableReader {
    table: DeltaTable,
    streaming_mode: ConnectorMode,
    column_types: HashMap<String, Type>,
    persistent_id: Option<PersistentId>,
    base_path: String,
    object_downloader: ObjectDownloader,

    reader: Option<ParquetRowIterator<'static>>,
    current_version: i64,
    last_fully_read_version: Option<i64>,
    rows_read_within_version: i64,
    parquet_files_queue: VecDeque<String>,
}

const DELTA_LAKE_INITIAL_POLL_DURATION: Duration = Duration::from_millis(5);
const DELTA_LAKE_MAX_POLL_DURATION: Duration = Duration::from_millis(100);
const DELTA_LAKE_POLL_BACKOFF: u32 = 2;

impl DeltaTableReader {
    pub fn new(
        path: &str,
        object_downloader: ObjectDownloader,
        storage_options: HashMap<String, String>,
        column_types: HashMap<String, Type>,
        streaming_mode: ConnectorMode,
        persistent_id: Option<PersistentId>,
    ) -> Result<Self, ReadError> {
        let runtime = create_async_tokio_runtime()?;
        let table = runtime.block_on(async { open_delta_table(path, storage_options).await })?;
        let current_version = table.version();
        let parquet_files_queue = Self::get_file_uris(&table)?;

        Ok(Self {
            table,
            column_types,
            streaming_mode,
            persistent_id,
            base_path: path.to_string(),

            current_version,
            object_downloader,
            last_fully_read_version: None,
            reader: None,
            parquet_files_queue,
            rows_read_within_version: 0,
        })
    }

    fn get_file_uris(table: &DeltaTable) -> Result<VecDeque<String>, ReadError> {
        Ok(table.get_file_uris()?.collect())
    }

    fn ensure_absolute_path(&self, path: &str) -> String {
        if path.starts_with(&self.base_path) {
            return path.to_string();
        }
        if self.base_path.ends_with('/') {
            format!("{}{path}", self.base_path)
        } else {
            format!("{}/{path}", self.base_path)
        }
    }

    fn upgrade_table_version(&mut self, is_polling_enabled: bool) -> Result<(), ReadError> {
        let runtime = create_async_tokio_runtime()?;
        runtime.block_on(async {
            self.parquet_files_queue.clear();
            let mut sleep_duration = DELTA_LAKE_INITIAL_POLL_DURATION;
            while self.parquet_files_queue.is_empty() {
                let diff = self.table.peek_next_commit(self.current_version).await?;
                let DeltaLakePeekCommit::New(next_version, txn_actions) = diff else {
                    if !is_polling_enabled {
                        break;
                    }
                    // Fully up to date, no changes yet
                    sleep(sleep_duration);
                    sleep_duration *= DELTA_LAKE_POLL_BACKOFF;
                    if sleep_duration > DELTA_LAKE_MAX_POLL_DURATION {
                        sleep_duration = DELTA_LAKE_MAX_POLL_DURATION;
                    }
                    continue;
                };

                let mut added_blocks = VecDeque::new();
                let mut data_changed = false;
                for action in txn_actions {
                    // Protocol description for Delta Lake actions:
                    // https://github.com/delta-io/delta/blob/master/PROTOCOL.md#actions
                    match action {
                        DeltaLakeAction::Remove(action) => {
                            if action.data_change {
                                return Err(ReadError::DeltaLakeForbiddenRemoval);
                            }
                        }
                        DeltaLakeAction::Add(action) => {
                            data_changed |= action.data_change;
                            added_blocks.push_back(self.ensure_absolute_path(&action.path));
                        }
                        _ => continue,
                    };
                }

                self.last_fully_read_version = Some(self.current_version);
                self.current_version = next_version;
                self.rows_read_within_version = 0;
                if data_changed {
                    self.parquet_files_queue = added_blocks;
                }
            }
            Ok(())
        })
    }

    fn read_next_row_native(&mut self, is_polling_enabled: bool) -> Result<ParquetRow, ReadError> {
        loop {
            match &mut self.reader {
                Some(ref mut reader) => {
                    match reader.next() {
                        Some(Ok(row)) => return Ok(row),
                        Some(Err(parquet_err)) => return Err(ReadError::Parquet(parquet_err)),
                        None => self.reader = None,
                    };
                }
                None => {
                    if self.parquet_files_queue.is_empty() {
                        self.upgrade_table_version(is_polling_enabled)?;
                        if self.parquet_files_queue.is_empty() {
                            return Err(ReadError::NoObjectsToRead);
                        }
                    }
                    let next_parquet_file = self.parquet_files_queue.pop_front().unwrap();
                    let local_object =
                        self.object_downloader.download_object(&next_parquet_file)?;
                    self.reader = Some(DeltaLakeParquetReader::try_from(local_object)?.into_iter());
                }
            }
        }
    }

    fn rows_in_file_count(path: &str) -> Result<i64, ReadError> {
        let reader = DeltaLakeParquetReader::try_from(Path::new(path))?;
        let metadata = reader.metadata();
        let mut n_rows = 0;
        for row_group in metadata.row_groups() {
            n_rows += row_group.num_rows();
        }
        Ok(n_rows)
    }
}

impl Reader for DeltaTableReader {
    fn read(&mut self) -> Result<ReadResult, ReadError> {
        let parquet_row = match self.read_next_row_native(self.streaming_mode.is_polling_enabled())
        {
            Ok(row) => row,
            Err(ReadError::NoObjectsToRead) => return Ok(ReadResult::Finished),
            Err(other) => return Err(other),
        };
        let mut row_map = HashMap::new();
        for (name, parquet_value) in parquet_row.get_column_iter() {
            let Some(expected_type) = self.column_types.get(name) else {
                // Column outside of the user-provided schema
                continue;
            };

            let value = match (parquet_value, expected_type) {
                (ParquetValue::Null, _) => Some(Value::None),
                (ParquetValue::Bool(b), Type::Bool | Type::Any) => Some(Value::from(*b)),
                (ParquetValue::Long(i), Type::Int | Type::Any) => Some(Value::from(*i)),
                (ParquetValue::Long(i), Type::Duration) => Some(Value::from(
                    EngineDuration::new_with_unit(*i, "us").unwrap(),
                )),
                (ParquetValue::Double(f), Type::Float | Type::Any) => {
                    Some(Value::Float((*f).into()))
                }
                (ParquetValue::Str(s), Type::String | Type::Any) => Some(Value::String(s.into())),
                (ParquetValue::Str(s), Type::Json) => serde_json::from_str::<serde_json::Value>(s)
                    .ok()
                    .map(Value::from),
                (ParquetValue::TimestampMicros(us), Type::DateTimeNaive | Type::Any) => Some(
                    Value::from(DateTimeNaive::from_timestamp(*us, "us").unwrap()),
                ),
                (ParquetValue::TimestampMicros(us), Type::DateTimeUtc) => {
                    Some(Value::from(DateTimeUtc::from_timestamp(*us, "us").unwrap()))
                }
                (ParquetValue::Bytes(b), Type::Bytes | Type::Any) => {
                    Some(Value::Bytes(b.data().into()))
                }
                _ => None,
            };
            let value = if let Some(value) = value {
                Ok(value)
            } else {
                let value_repr =
                    limit_length(format!("{parquet_value:?}"), STANDARD_OBJECT_LENGTH_LIMIT);
                Err(Box::new(ConversionError {
                    value_repr,
                    field_name: name.clone(),
                    type_: expected_type.clone(),
                }))
            };
            row_map.insert(name.clone(), value);
        }

        self.rows_read_within_version += 1;
        Ok(ReadResult::Data(
            ReaderContext::from_diff(DataEventType::Insert, None, row_map.into()),
            (
                OffsetKey::Empty,
                OffsetValue::DeltaTablePosition {
                    version: self.current_version,
                    rows_read_within_version: self.rows_read_within_version,
                    last_fully_read_version: self.last_fully_read_version,
                },
            ),
        ))
    }

    fn seek(&mut self, frontier: &OffsetAntichain) -> Result<(), ReadError> {
        // The offset denotes the last fully processed Delta Table version.
        // Then, the `seek` loads this checkpoint and ensures that no diffs
        // from the current version will be applied.
        let offset_value = frontier.get_offset(&OffsetKey::Empty);
        let Some(OffsetValue::DeltaTablePosition {
            version,
            rows_read_within_version: n_rows_to_rewind,
            last_fully_read_version,
        }) = offset_value
        else {
            if offset_value.is_some() {
                warn!("Incorrect type of offset value in DeltaLake frontier: {offset_value:?}");
            }
            return Ok(());
        };

        self.reader = None;
        let runtime = create_async_tokio_runtime()?;
        if let Some(last_fully_read_version) = last_fully_read_version {
            // The offset is based on the diff between `last_fully_read_version` and `version`
            self.current_version = *last_fully_read_version;
            runtime.block_on(async { self.table.load_version(self.current_version).await })?;
            self.upgrade_table_version(false)?;
        } else {
            // The offset is based on the full set of files present for `version`
            self.current_version = *version;
            runtime.block_on(async { self.table.load_version(self.current_version).await })?;
            self.parquet_files_queue = Self::get_file_uris(&self.table)?;
        }

        self.rows_read_within_version = 0;
        while !self.parquet_files_queue.is_empty() {
            let next_block = self.parquet_files_queue.front().unwrap();
            let block_size = Self::rows_in_file_count(next_block)?;
            if self.rows_read_within_version + block_size <= *n_rows_to_rewind {
                info!("Skipping parquet block with the size of {block_size} entries: {next_block}");
                self.rows_read_within_version += block_size;
                self.parquet_files_queue.pop_front();
            } else {
                break;
            }
        }

        let rows_left_to_rewind = *n_rows_to_rewind - self.rows_read_within_version;
        info!("Not quickly-rewindable entries count: {rows_left_to_rewind}");
        for _ in 0..rows_left_to_rewind {
            let _ = self.read_next_row_native(false)?;
        }

        Ok(())
    }

    fn update_persistent_id(&mut self, persistent_id: Option<PersistentId>) {
        self.persistent_id = persistent_id;
    }

    fn persistent_id(&self) -> Option<PersistentId> {
        self.persistent_id
    }

    fn storage_type(&self) -> StorageType {
        StorageType::DeltaLake
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
}

pub struct NatsReader {
    runtime: TokioRuntime,
    subscriber: NatsSubscriber,
    worker_index: usize,
    persistent_id: Option<PersistentId>,
    total_entries_read: usize,
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

    fn persistent_id(&self) -> Option<PersistentId> {
        self.persistent_id
    }

    fn update_persistent_id(&mut self, persistent_id: Option<PersistentId>) {
        self.persistent_id = persistent_id;
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
        persistent_id: Option<PersistentId>,
    ) -> NatsReader {
        NatsReader {
            runtime,
            subscriber,
            worker_index,
            persistent_id,
            total_entries_read: 0,
        }
    }
}

pub struct NatsWriter {
    runtime: TokioRuntime,
    client: NatsClient,
    topic: String,
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
                self.client
                    .publish_with_headers(self.topic.clone(), headers, payload.into())
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
        topic: String,
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
