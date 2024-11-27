// Copyright © 2024 Pathway

use pyo3::exceptions::PyValueError;
use pyo3::types::PyBytes;
use rand::Rng;
use rdkafka::util::Timeout;
use s3::error::S3Error;
use std::any::type_name;
use std::borrow::Borrow;
use std::borrow::Cow;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::env;
use std::fmt::Debug;
use std::fs::File;
use std::io;
use std::io::BufRead;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Cursor;
use std::io::Write;
use std::io::{Seek, SeekFrom};
use std::mem::take;
use std::ops::ControlFlow;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::str::{from_utf8, Utf8Error};
use std::sync::Arc;
use std::thread::sleep;
use std::time::{Duration, Instant, SystemTime};

use arcstr::ArcStr;
use chrono::DateTime;
use futures::StreamExt;
use itertools::Itertools;
use log::{error, info, warn};
use postgres::types::ToSql;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use rayon::{ThreadPool, ThreadPoolBuilder};
use tempfile::{tempdir, tempfile, TempDir};
use tokio::runtime::Runtime as TokioRuntime;
use xxhash_rust::xxh3::Xxh3 as Hasher;

use crate::async_runtime::create_async_tokio_runtime;
use crate::connectors::data_format::{FormatterContext, FormatterError, COMMIT_LITERAL};
use crate::connectors::metadata::SourceMetadata;
use crate::connectors::offset::EMPTY_OFFSET;
use crate::connectors::{Offset, OffsetKey, OffsetValue};
use crate::deepcopy::DeepCopy;
use crate::engine::error::limit_length;
use crate::engine::error::DynResult;
use crate::engine::error::STANDARD_OBJECT_LENGTH_LIMIT;
use crate::engine::time::DateTime as EngineDateTime;
use crate::engine::Type;
use crate::engine::Value;
use crate::engine::{DateTimeNaive, DateTimeUtc, Duration as EngineDuration};
use crate::fs_helpers::ensure_directory;
use crate::persistence::backends::Error as PersistenceBackendError;
use crate::persistence::frontier::OffsetAntichain;
use crate::persistence::{ExternalPersistentId, PersistentId};
use crate::python_api::extract_value;
use crate::python_api::threads::PythonThreadState;
use crate::python_api::PythonSubject;
use crate::python_api::ValueField;
use crate::retry::{execute_with_retries, RetryConfig};
use crate::timestamp::current_unix_timestamp_secs;

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
use glob::Pattern as GlobPattern;
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
use s3::request::request_trait::ResponseData as S3ResponseData;
use serde::{Deserialize, Serialize};

#[derive(Debug)]
pub enum S3CommandName {
    ListObjectsV2,
    GetObject,
    DeleteObject,
    InitiateMultipartUpload,
    PutMultipartChunk,
    CompleteMultipartUpload,
}

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
    NewSource(Option<SourceMetadata>),
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
    FileSystem,
    S3Csv,
    S3Lines,
    CsvFilesystem,
    Kafka,
    Python,
    Sqlite,
    DeltaLake,
    Nats,
}

impl StorageType {
    pub fn merge_two_frontiers(
        &self,
        lhs: &OffsetAntichain,
        rhs: &OffsetAntichain,
    ) -> OffsetAntichain {
        match self {
            StorageType::FileSystem => FilesystemReader::merge_two_frontiers(lhs, rhs),
            StorageType::S3Csv => S3CsvReader::merge_two_frontiers(lhs, rhs),
            StorageType::CsvFilesystem => CsvFilesystemReader::merge_two_frontiers(lhs, rhs),
            StorageType::Kafka => KafkaReader::merge_two_frontiers(lhs, rhs),
            StorageType::Python => PythonReader::merge_two_frontiers(lhs, rhs),
            StorageType::S3Lines => S3GenericReader::merge_two_frontiers(lhs, rhs),
            StorageType::Sqlite => SqliteReader::merge_two_frontiers(lhs, rhs),
            StorageType::DeltaLake => DeltaTableReader::merge_two_frontiers(lhs, rhs),
            StorageType::Nats => NatsReader::merge_two_frontiers(lhs, rhs),
        }
    }
}

pub trait Reader {
    fn read(&mut self) -> Result<ReadResult, ReadError>;

    #[allow(clippy::missing_errors_doc)]
    fn seek(&mut self, frontier: &OffsetAntichain) -> Result<(), ReadError>;

    fn update_persistent_id(&mut self, persistent_id: Option<PersistentId>);
    fn persistent_id(&self) -> Option<PersistentId>;

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
    fn read_next_bytes<R>(self, reader: &mut R, buf: &mut Vec<u8>) -> Result<usize, ReadError>
    where
        R: BufRead,
    {
        match &self {
            ReadMethod::ByLine => Ok(reader.read_until(b'\n', buf)?),
            ReadMethod::Full => Ok(reader.read_to_end(buf)?),
        }
    }
}

pub struct FilesystemReader {
    persistent_id: Option<PersistentId>,
    read_method: ReadMethod,

    reader: Option<BufReader<std::fs::File>>,
    filesystem_scanner: FilesystemScanner,
    total_entries_read: u64,
    deferred_read_result: Option<ReadResult>,
}

impl FilesystemReader {
    pub fn new(
        path: &str,
        streaming_mode: ConnectorMode,
        persistent_id: Option<PersistentId>,
        read_method: ReadMethod,
        object_pattern: &str,
    ) -> Result<FilesystemReader, ReadError> {
        let filesystem_scanner =
            FilesystemScanner::new(path, persistent_id, streaming_mode, object_pattern)?;

        Ok(Self {
            persistent_id,

            reader: None,
            filesystem_scanner,
            total_entries_read: 0,
            read_method,
            deferred_read_result: None,
        })
    }
}

impl Reader for FilesystemReader {
    fn seek(&mut self, frontier: &OffsetAntichain) -> Result<(), ReadError> {
        let offset_value = frontier.get_offset(&OffsetKey::Empty);
        let Some(OffsetValue::FilePosition {
            total_entries_read,
            path: file_path_arc,
            bytes_offset,
        }) = offset_value
        else {
            if offset_value.is_some() {
                warn!("Incorrect type of offset value in Filesystem frontier: {offset_value:?}");
            }
            return Ok(());
        };
        // Filesystem scanner part: detect already processed file
        self.filesystem_scanner
            .seek_to_file(file_path_arc.as_path())?;

        // If the last file read is missing from the offset, the program will read all files in the directory.
        // We could consider alternative strategies:
        //  1. Return an error and stop the program;
        //  2. Track all files that have been read in the offset (though this would greatly increase the offset size);
        //  3. Only use the file's last modification time.
        if !file_path_arc.exists() {
            return Ok(());
        }

        // Seek within a particular file
        self.reader = {
            let file = File::open(file_path_arc.as_path())?;
            let mut reader = BufReader::new(file);
            reader.seek(SeekFrom::Start(*bytes_offset))?;
            Some(reader)
        };
        self.total_entries_read = *total_entries_read;

        Ok(())
    }

    fn read(&mut self) -> Result<ReadResult, ReadError> {
        if let Some(deferred_read_result) = self.deferred_read_result.take() {
            return Ok(deferred_read_result);
        }

        loop {
            if let Some(reader) = &mut self.reader {
                let mut line = Vec::new();
                let len = self.read_method.read_next_bytes(reader, &mut line)?;
                if len > 0 || self.read_method == ReadMethod::Full {
                    self.total_entries_read += 1;

                    let offset = (
                        OffsetKey::Empty,
                        OffsetValue::FilePosition {
                            total_entries_read: self.total_entries_read,
                            path: self
                                .filesystem_scanner
                                .current_offset_file()
                                .clone()
                                .unwrap(),
                            bytes_offset: reader.stream_position().unwrap(),
                        },
                    );
                    let data_event_type = self
                        .filesystem_scanner
                        .data_event_type()
                        .expect("scanner action can't be empty");

                    if self.read_method == ReadMethod::Full {
                        self.deferred_read_result = Some(ReadResult::FinishedSource {
                            commit_allowed: !self.filesystem_scanner.has_planned_insertion(),
                        });
                        self.reader = None;
                    }

                    return Ok(ReadResult::Data(
                        ReaderContext::from_raw_bytes(data_event_type, line),
                        offset,
                    ));
                }

                self.reader = None;
                return Ok(ReadResult::FinishedSource {
                    commit_allowed: !self.filesystem_scanner.has_planned_insertion(),
                });
            }

            let next_read_result = self.filesystem_scanner.next_action_determined()?;
            if let Some(next_read_result) = next_read_result {
                if let Some(selected_file) = self.filesystem_scanner.current_file() {
                    let file = File::open(&*selected_file)?;
                    self.reader = Some(BufReader::new(file));
                }
                return Ok(next_read_result);
            }

            if self.filesystem_scanner.wait_for_new_files().is_break() {
                return Ok(ReadResult::Finished);
            }
        }
    }

    fn persistent_id(&self) -> Option<PersistentId> {
        self.persistent_id
    }

    fn update_persistent_id(&mut self, persistent_id: Option<PersistentId>) {
        self.persistent_id = persistent_id;
    }

    fn storage_type(&self) -> StorageType {
        StorageType::FileSystem
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
}

impl Reader for KafkaReader {
    fn read(&mut self) -> Result<ReadResult, ReadError> {
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

            return Ok(ReadResult::Data(message, offset));
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
        }
    }
}

#[derive(Debug)]
enum PosixScannerAction {
    Read(Arc<PathBuf>),
    Delete(Arc<PathBuf>),
}

#[derive(Debug)]
struct FilesystemScanner {
    path: GlobPattern,
    cache_directory_path: Option<PathBuf>,
    streaming_mode: ConnectorMode,
    object_pattern: String,

    // Mapping from the path of the loaded file to its modification timestamp
    known_files: HashMap<PathBuf, u64>,

    current_action: Option<PosixScannerAction>,
    cached_modify_times: HashMap<PathBuf, Option<SystemTime>>,
    next_file_for_insertion: Option<PathBuf>,
    cached_metadata: HashMap<PathBuf, Option<SourceMetadata>>,
    scanner_actions_queue: VecDeque<PosixScannerAction>,

    // Storage is deleted on object destruction, so we need to store it
    // for the connector's life time
    _connector_tmp_storage: Option<TempDir>,
}

impl FilesystemScanner {
    fn new(
        path: &str,
        persistent_id: Option<PersistentId>,
        streaming_mode: ConnectorMode,
        object_pattern: &str,
    ) -> Result<FilesystemScanner, ReadError> {
        let path_glob = GlobPattern::new(path)?;

        let (cache_directory_path, connector_tmp_storage) = {
            if streaming_mode.are_deletions_enabled() {
                if let Ok(root_dir_str_path) = env::var("PATHWAY_PERSISTENT_STORAGE") {
                    let root_dir_path = Path::new(&root_dir_str_path);
                    ensure_directory(root_dir_path)?;
                    let unique_id =
                        persistent_id.unwrap_or_else(|| rand::thread_rng().gen::<u128>());
                    let connector_tmp_directory = root_dir_path.join(format!("cache-{unique_id}"));
                    ensure_directory(&connector_tmp_directory)?;
                    (Some(connector_tmp_directory), None)
                } else {
                    let cache_tmp_storage = tempdir()?;
                    let connector_tmp_directory = cache_tmp_storage.path();
                    (
                        Some(connector_tmp_directory.to_path_buf()),
                        Some(cache_tmp_storage),
                    )
                }
            } else {
                (None, None)
            }
        };

        Ok(Self {
            path: path_glob,
            streaming_mode,
            cache_directory_path,

            object_pattern: object_pattern.to_string(),
            known_files: HashMap::new(),
            current_action: None,
            cached_modify_times: HashMap::new(),
            next_file_for_insertion: None,
            cached_metadata: HashMap::new(),
            scanner_actions_queue: VecDeque::new(),
            _connector_tmp_storage: connector_tmp_storage,
        })
    }

    fn has_planned_insertion(&self) -> bool {
        self.next_file_for_insertion.is_some()
    }

    fn is_polling_enabled(&self) -> bool {
        self.streaming_mode.is_polling_enabled()
    }

    fn data_event_type(&self) -> Option<DataEventType> {
        self.current_action
            .as_ref()
            .map(|current_action| match current_action {
                PosixScannerAction::Read(_) => DataEventType::Insert,
                PosixScannerAction::Delete(_) => DataEventType::Delete,
            })
    }

    /// Returns the actual file path, which needs to be read
    /// It is either a path to the file in the input directory, or a path to the file
    /// which is saved in cache
    fn current_file(&self) -> Option<Arc<PathBuf>> {
        match &self.current_action {
            Some(PosixScannerAction::Read(path)) => Some(path.clone()),
            Some(PosixScannerAction::Delete(path)) => self.cached_file_path(path).map(Arc::new),
            None => None,
        }
    }

    /// Returns the name of the currently processed file in the input directory
    fn current_offset_file(&self) -> Option<Arc<PathBuf>> {
        match &self.current_action {
            Some(PosixScannerAction::Read(path) | PosixScannerAction::Delete(path)) => {
                Some(path.clone())
            }
            None => None,
        }
    }

    fn seek_to_file(&mut self, seek_file_path: &Path) -> Result<(), ReadError> {
        if self.streaming_mode.are_deletions_enabled() {
            warn!("seek for snapshot mode may not work correctly in case deletions take place");
        }

        self.known_files.clear();
        let target_modify_time = match std::fs::metadata(seek_file_path) {
            Ok(metadata) => metadata.modified()?,
            Err(e) => {
                if !matches!(e.kind(), std::io::ErrorKind::NotFound) {
                    return Err(ReadError::Io(e));
                }
                warn!(
                    "Unable to restore state: last persisted file {seek_file_path:?} not found in directory. Processing all files in directory."
                );
                return Ok(());
            }
        };
        let matching_files: Vec<PathBuf> = self.get_matching_file_paths()?;
        for entry in matching_files {
            if !entry.is_file() {
                continue;
            }
            let Some(modify_time) = self.modify_time(&entry) else {
                continue;
            };
            if (modify_time, entry.as_path()) <= (target_modify_time, seek_file_path) {
                let modify_timestamp = modify_time
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .expect("System time should be after the Unix epoch")
                    .as_secs();
                self.known_files.insert(entry, modify_timestamp);
            }
        }
        self.current_action = Some(PosixScannerAction::Read(Arc::new(
            seek_file_path.to_path_buf(),
        )));

        Ok(())
    }

    fn modify_time(&mut self, entry: &Path) -> Option<SystemTime> {
        if self.streaming_mode.are_deletions_enabled() {
            // If deletions are enabled, we also need to handle the case when the modification
            // time of an entry changes. Hence, we can't just memorize it once.
            entry.metadata().ok()?.modified().ok()
        } else {
            *self
                .cached_modify_times
                .entry(entry.to_path_buf())
                .or_insert_with(|| entry.metadata().ok()?.modified().ok())
        }
    }

    /// Finish reading the current file and find the next one to read from.
    /// If there is a file to read from, the method returns a `ReadResult`
    /// specifying the action to be provided downstream.
    ///
    /// It can either be a `NewSource` event when the new action is found or
    /// a `FinishedSource` event when we've had a scheduled action but the
    /// corresponding file was deleted before we were able to execute this scheduled action.
    /// scheduled action.
    fn next_action_determined(&mut self) -> Result<Option<ReadResult>, ReadError> {
        // Finalize the current processing action
        if let Some(PosixScannerAction::Delete(path)) = take(&mut self.current_action) {
            let cached_path = self
                .cached_file_path(&path)
                .expect("in case of enabled deletions cache should exist");
            std::fs::remove_file(cached_path)?;
        }

        // File modification is handled as combination of its deletion and insertion
        // If a file was deleted in the last action, now we must add it, and after that
        // we may allow commit
        if let Some(next_file_for_insertion) = take(&mut self.next_file_for_insertion) {
            if next_file_for_insertion.exists() {
                return Ok(Some(
                    self.initiate_file_insertion(&next_file_for_insertion)?,
                ));
            }

            // The scheduled insertion after deletion is impossible because
            // the file has already been deleted.
            // The action was done in full now, and we can allow commits.
            return Ok(Some(ReadResult::FinishedSource {
                commit_allowed: true,
            }));
        }

        if self.scanner_actions_queue.is_empty() {
            if self.streaming_mode.are_deletions_enabled() {
                self.enqueue_deletion_entries();
            }
            self.enqueue_insertion_entries()?;
        }

        // Find the next valid action to execute
        loop {
            match self.scanner_actions_queue.pop_front() {
                Some(PosixScannerAction::Read(path)) => {
                    let next_action = self.initiate_file_insertion(&path);
                    match next_action {
                        Ok(next_action) => return Ok(Some(next_action)),
                        Err(e) => {
                            // If there was a planned action to add a file, but
                            // it no longer exists, proceed to the next planned action
                            if e.kind() == std::io::ErrorKind::NotFound {
                                continue;
                            }
                            return Err(ReadError::Io(e));
                        }
                    };
                }
                Some(PosixScannerAction::Delete(path)) => {
                    return Ok(Some(self.initiate_file_deletion(&path)))
                }
                None => return Ok(None),
            }
        }
    }

    fn enqueue_deletion_entries(&mut self) {
        let mut paths_for_deletion = Vec::new();
        for (path, modified_at) in &self.known_files {
            let metadata = std::fs::metadata(path);
            let needs_deletion = {
                match metadata {
                    Err(e) => e.kind() == std::io::ErrorKind::NotFound,
                    Ok(metadata) => {
                        if let Ok(new_modification_time) = metadata.modified() {
                            let modified_at_new = new_modification_time
                                .duration_since(SystemTime::UNIX_EPOCH)
                                .expect("System time should be after the Unix epoch")
                                .as_secs();
                            modified_at_new != *modified_at
                        } else {
                            false
                        }
                    }
                }
            };
            if needs_deletion {
                paths_for_deletion.push(path.clone());
            }
        }
        paths_for_deletion.sort_unstable();
        for path in paths_for_deletion {
            self.scanner_actions_queue
                .push_back(PosixScannerAction::Delete(Arc::new(path.clone())));
        }
    }

    fn cached_file_path(&self, path: &Path) -> Option<PathBuf> {
        self.cache_directory_path.as_ref().map(|root_path| {
            let mut hasher = Hasher::default();
            hasher.update(path.as_os_str().as_bytes());
            root_path.join(format!("{}", hasher.digest128()))
        })
    }

    fn get_matching_file_paths(&self) -> Result<Vec<PathBuf>, ReadError> {
        let mut result = Vec::new();

        let file_and_folder_paths = glob::glob(self.path.as_str())?.flatten();
        for entry in file_and_folder_paths {
            // If an entry is a file, it should just be added to result
            if entry.is_file() {
                result.push(entry);
                continue;
            }

            // Otherwise scan all files in all subdirectories and add them
            let Some(path) = entry.to_str() else {
                error!("Non-unicode paths are not supported. Ignoring: {entry:?}");
                continue;
            };

            let folder_scan_pattern = format!("{path}/**/{}", self.object_pattern);
            let folder_contents = glob::glob(&folder_scan_pattern)?.flatten();
            for nested_entry in folder_contents {
                if nested_entry.is_file() {
                    result.push(nested_entry);
                }
            }
        }

        Ok(result)
    }

    fn enqueue_insertion_entries(&mut self) -> Result<(), ReadError> {
        let matching_files: Vec<PathBuf> = self.get_matching_file_paths()?;
        let mut new_detected_files: Vec<(SystemTime, PathBuf)> = Vec::new();
        for entry in matching_files {
            if !entry.is_file() || self.known_files.contains_key(&(*entry)) {
                continue;
            }
            let Some(modify_time) = self.modify_time(&entry) else {
                continue;
            };
            new_detected_files.push((modify_time, entry));
        }

        new_detected_files.sort_unstable();
        for (_, path) in new_detected_files {
            self.scanner_actions_queue
                .push_back(PosixScannerAction::Read(Arc::new(path.clone())));
        }

        Ok(())
    }

    fn initiate_file_insertion(&mut self, new_file_name: &PathBuf) -> io::Result<ReadResult> {
        let new_file_meta =
            SourceMetadata::from_fs_meta(new_file_name, &std::fs::metadata(new_file_name)?);
        let cached_path = self.cached_file_path(new_file_name);
        if let Some(cached_path) = cached_path {
            std::fs::copy(new_file_name, cached_path)?;
        }

        // The file has been successfully saved at this point.
        // Now we can change the internal state.

        self.cached_metadata
            .insert(new_file_name.clone(), Some(new_file_meta.clone()));
        self.known_files.insert(
            new_file_name.clone(),
            new_file_meta
                .modified_at
                .unwrap_or(current_unix_timestamp_secs()),
        );

        self.current_action = Some(PosixScannerAction::Read(Arc::new(new_file_name.clone())));
        Ok(ReadResult::NewSource(Some(new_file_meta)))
    }

    fn initiate_file_deletion(&mut self, path: &PathBuf) -> ReadResult {
        // Metadata of the deleted file must be the same as when it was added
        // so that the deletion event is processed correctly by timely. To achieve
        // this, we just take the cached metadata
        let old_metadata = self
            .cached_metadata
            .remove(path)
            .expect("inconsistency between known_files and cached_metadata");

        self.known_files.remove(&path.clone().clone());
        self.current_action = Some(PosixScannerAction::Delete(Arc::new(path.clone())));
        if path.exists() {
            // If the path exists it means file modification. In this scenatio, the file
            // needs first to be deleted and then to be inserted again.
            self.next_file_for_insertion = Some(path.clone());
        }

        ReadResult::NewSource(old_metadata)
    }

    fn sleep_duration() -> Duration {
        Duration::from_millis(500)
    }

    fn wait_for_new_files(&self) -> ControlFlow<()> {
        if self.is_polling_enabled() {
            sleep(Self::sleep_duration());
            ControlFlow::Continue(())
        } else {
            ControlFlow::Break(())
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

    pub fn are_deletions_enabled(&self) -> bool {
        match self {
            ConnectorMode::Static => false,
            ConnectorMode::Streaming => true,
        }
    }
}

#[derive(Debug)]
pub struct CsvFilesystemReader {
    parser_builder: csv::ReaderBuilder,
    persistent_id: Option<PersistentId>,

    reader: Option<csv::Reader<std::fs::File>>,
    filesystem_scanner: FilesystemScanner,
    total_entries_read: u64,
    deferred_read_result: Option<ReadResult>,
}

impl CsvFilesystemReader {
    pub fn new(
        path: &str,
        parser_builder: csv::ReaderBuilder,
        streaming_mode: ConnectorMode,
        persistent_id: Option<PersistentId>,
        object_pattern: &str,
    ) -> Result<CsvFilesystemReader, ReadError> {
        let filesystem_scanner =
            FilesystemScanner::new(path, persistent_id, streaming_mode, object_pattern)?;
        Ok(CsvFilesystemReader {
            parser_builder,
            persistent_id,

            reader: None,
            filesystem_scanner,
            total_entries_read: 0,
            deferred_read_result: None,
        })
    }
}

impl Reader for CsvFilesystemReader {
    fn seek(&mut self, frontier: &OffsetAntichain) -> Result<(), ReadError> {
        let offset_value = frontier.get_offset(&OffsetKey::Empty);
        let Some(OffsetValue::FilePosition {
            total_entries_read,
            path: file_path_arc,
            bytes_offset,
        }) = offset_value
        else {
            if offset_value.is_some() {
                warn!("Incorrect type of offset value in CsvFilesystem frontier: {offset_value:?}");
            }
            return Ok(());
        };

        // Filesystem scanner part: detect already processed file
        self.filesystem_scanner
            .seek_to_file(file_path_arc.as_path())?;

        // If the last file read is missing from the offset, the program will read all files in the directory.
        // We could consider alternative strategies:
        //  1. Return an error and stop the program;
        //  2. Track all files that have been read in the offset (though this would greatly increase the offset size);
        //  3. Only use the file's last modification time.
        if !file_path_arc.exists() {
            return Ok(());
        }

        // Seek within a particular file
        self.total_entries_read = *total_entries_read;
        self.reader = {
            // Since it's a CSV reader, we will need to fit the header in the parser first
            let mut reader = self.parser_builder.from_path(file_path_arc.as_path())?;
            if *bytes_offset > 0 {
                let mut header_record = csv::StringRecord::new();
                if reader.read_record(&mut header_record)? {
                    let header_reader_context = ReaderContext::from_tokenized_entries(
                        self.filesystem_scanner
                            .data_event_type()
                            .expect("scanner action can't be empty"),
                        header_record
                            .iter()
                            .map(std::string::ToString::to_string)
                            .collect(),
                    );

                    let offset = (OffsetKey::Empty, offset_value.unwrap().clone());

                    let header_read_result = ReadResult::Data(header_reader_context, offset);
                    self.deferred_read_result = Some(header_read_result);
                }
            }

            let mut seek_position = csv::Position::new();
            seek_position.set_byte(*bytes_offset);
            reader.seek(seek_position)?;

            Some(reader)
        };

        Ok(())
    }

    fn read(&mut self) -> Result<ReadResult, ReadError> {
        if let Some(deferred_read_result) = self.deferred_read_result.take() {
            return Ok(deferred_read_result);
        }

        loop {
            match &mut self.reader {
                Some(reader) => {
                    let mut current_record = csv::StringRecord::new();
                    if reader.read_record(&mut current_record)? {
                        self.total_entries_read += 1;

                        let offset = (
                            OffsetKey::Empty,
                            OffsetValue::FilePosition {
                                total_entries_read: self.total_entries_read,
                                path: self
                                    .filesystem_scanner
                                    .current_offset_file()
                                    .clone()
                                    .unwrap(),
                                bytes_offset: reader.position().byte(),
                            },
                        );

                        return Ok(ReadResult::Data(
                            ReaderContext::from_tokenized_entries(
                                self.filesystem_scanner
                                    .data_event_type()
                                    .expect("scanner action can't be empty"),
                                current_record
                                    .iter()
                                    .map(std::string::ToString::to_string)
                                    .collect(),
                            ),
                            offset,
                        ));
                    }

                    let next_read_result = self.filesystem_scanner.next_action_determined()?;
                    if let Some(next_read_result) = next_read_result {
                        if let Some(selected_file) = self.filesystem_scanner.current_file() {
                            self.reader = Some(self.parser_builder.from_path(&*selected_file)?);
                        }
                        return Ok(next_read_result);
                    }
                    // The file came to its end, so we should drop the reader
                    self.reader = None;
                    return Ok(ReadResult::FinishedSource {
                        commit_allowed: !self.filesystem_scanner.has_planned_insertion(),
                    });
                }
                None => {
                    let next_read_result = self.filesystem_scanner.next_action_determined()?;
                    if let Some(next_read_result) = next_read_result {
                        if let Some(selected_file) = self.filesystem_scanner.current_file() {
                            self.reader = Some(
                                self.parser_builder
                                    .flexible(true)
                                    .from_path(&*selected_file)?,
                            );
                        }
                        return Ok(next_read_result);
                    }
                }
            }

            if self.filesystem_scanner.wait_for_new_files().is_break() {
                return Ok(ReadResult::Finished);
            }
        }
    }

    fn persistent_id(&self) -> Option<PersistentId> {
        self.persistent_id
    }

    fn update_persistent_id(&mut self, persistent_id: Option<PersistentId>) {
        self.persistent_id = persistent_id;
    }

    fn storage_type(&self) -> StorageType {
        StorageType::CsvFilesystem
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

const MAX_S3_RETRIES: usize = 2;
const S3_PATH_PREFIXES: [&str; 2] = ["s3://", "s3a://"];
type S3DownloadedObject = (Arc<String>, Cursor<Vec<u8>>);
type S3DownloadResult = Result<S3DownloadedObject, ReadError>;

pub struct S3Scanner {
    /*
        This class takes responsibility over S3 object selection and streaming.
        In encapsulates the selection of the next object to stream and streaming
        the object and provides reader end of the pipe to the outside user.
    */
    bucket: S3Bucket,
    objects_prefix: String,
    streaming_mode: ConnectorMode,
    current_object_path: Option<Arc<String>>,
    processed_objects: HashSet<String>,
    objects_for_processing: VecDeque<String>,
    downloader_pool: ThreadPool,
    predownloaded_objects: HashMap<String, Cursor<Vec<u8>>>,
    is_queue_initialized: bool,
}

impl S3Scanner {
    pub fn new(
        bucket: S3Bucket,
        objects_prefix: impl Into<String>,
        streaming_mode: ConnectorMode,
        downloader_threads_count: usize,
    ) -> Result<Self, ReadError> {
        let objects_prefix = objects_prefix.into();

        let object_lists = execute_with_retries(
            || bucket.list(objects_prefix.clone(), None),
            RetryConfig::default(),
            MAX_S3_RETRIES,
        )
        .map_err(|e| ReadError::S3(S3CommandName::ListObjectsV2, e))?;

        let mut has_nonempty_list = false;
        for list in object_lists {
            if !list.contents.is_empty() {
                has_nonempty_list = true;
                break;
            }
        }
        if !has_nonempty_list {
            return Err(ReadError::NoObjectsToRead);
        }

        Ok(S3Scanner {
            bucket,
            objects_prefix,
            streaming_mode,

            current_object_path: None,
            processed_objects: HashSet::new(),
            objects_for_processing: VecDeque::new(),
            downloader_pool: ThreadPoolBuilder::new()
                .num_threads(downloader_threads_count)
                .build()
                .expect("Failed to create downloader pool"), // TODO: configure number of threads
            predownloaded_objects: HashMap::new(),
            is_queue_initialized: false,
        })
    }

    pub fn deduce_bucket_and_path(s3_path: &str) -> (Option<String>, String) {
        for prefix in S3_PATH_PREFIXES {
            let Some(bucket_and_path) = s3_path.strip_prefix(prefix) else {
                continue;
            };
            let (bucket, path) = bucket_and_path
                .split_once('/')
                .unwrap_or((bucket_and_path, ""));
            return (Some(bucket.to_string()), path.to_string());
        }

        (None, s3_path.to_string())
    }

    pub fn download_object_from_path_and_bucket(
        object_path_ref: &str,
        bucket: &S3Bucket,
    ) -> Result<S3ResponseData, ReadError> {
        let (_, deduced_path) = Self::deduce_bucket_and_path(object_path_ref);
        execute_with_retries(
            || bucket.get_object(&deduced_path), // returns Err on incorrect status code because fail-on-err feature is enabled
            RetryConfig::default(),
            MAX_S3_RETRIES,
        )
        .map_err(|e| ReadError::S3(S3CommandName::GetObject, e))
    }

    pub fn stream_object_from_path_and_bucket(
        object_path_ref: &str,
        bucket: &S3Bucket,
    ) -> S3DownloadResult {
        let object_path = object_path_ref.to_string();
        let response = Self::download_object_from_path_and_bucket(&object_path, bucket)?;
        let readable_data = Cursor::new(response.bytes().to_vec());
        Ok((Arc::new(object_path_ref.to_string()), readable_data))
    }

    fn stream_object_from_path(
        &mut self,
        object_path_ref: &str,
    ) -> Result<Cursor<Vec<u8>>, ReadError> {
        let (current_object_path, reader_impl) =
            Self::stream_object_from_path_and_bucket(object_path_ref, &self.bucket.deep_copy())?;
        self.current_object_path = Some(current_object_path);
        Ok(reader_impl)
    }

    fn find_new_objects_for_processing(&mut self) -> Result<(), ReadError> {
        let object_lists = execute_with_retries(
            || self.bucket.list(self.objects_prefix.to_string(), None),
            RetryConfig::default(),
            MAX_S3_RETRIES,
        )
        .map_err(|e| ReadError::S3(S3CommandName::ListObjectsV2, e))?;

        let mut new_objects = Vec::new();
        for list in &object_lists {
            for object in &list.contents {
                if self.processed_objects.contains(&object.key) {
                    continue;
                }
                let Ok(last_modified) = DateTime::parse_from_rfc3339(&object.last_modified) else {
                    continue;
                };
                new_objects.push((last_modified, object.key.clone()));
            }
        }
        new_objects.sort_unstable();
        info!("Found {} new objects to process", new_objects.len());
        let downloading_started_at = SystemTime::now();
        let new_objects_downloaded: Vec<S3DownloadResult> = self.downloader_pool.install(|| {
            new_objects
                .par_iter()
                .map(|(_, path)| Self::stream_object_from_path_and_bucket(path, &self.bucket))
                .collect()
        });
        info!("Downloading done in {:?}", downloading_started_at.elapsed());

        for downloaded_object in new_objects_downloaded {
            match downloaded_object {
                Ok((path, prepared_reader)) => {
                    self.predownloaded_objects
                        .insert(path.to_string(), prepared_reader);
                }
                Err(e) => {
                    error!("Error while downloading an object from S3: {e}");
                }
            }
        }
        for (_, object_key) in new_objects {
            self.objects_for_processing.push_back(object_key);
        }

        info!(
            "The {} new objects have been enqueued for further processing",
            self.objects_for_processing.len()
        );
        self.is_queue_initialized = true;
        Ok(())
    }

    fn stream_next_object(&mut self) -> Result<Option<Cursor<Vec<u8>>>, ReadError> {
        let is_polling_enabled = self.streaming_mode.is_polling_enabled();
        loop {
            if let Some(selected_object_path) = self.objects_for_processing.pop_front() {
                let prepared_object = self.predownloaded_objects.remove(&selected_object_path);
                if let Some(prepared_object) = prepared_object {
                    self.processed_objects.insert(selected_object_path.clone());
                    self.current_object_path = Some(selected_object_path.into());
                    return Ok(Some(prepared_object));
                }
            } else {
                let is_queue_refresh_needed = !self.is_queue_initialized
                    || (self.objects_for_processing.is_empty() && is_polling_enabled);
                if is_queue_refresh_needed {
                    self.find_new_objects_for_processing()?;
                    if self.objects_for_processing.is_empty() && is_polling_enabled {
                        // Even after the queue refresh attempt, it's still empty
                        // Sleep before the next poll
                        sleep(Self::sleep_duration());
                    }
                } else {
                    // No elements and no further queue refreshes,
                    // the connector can stop at this point
                    return Ok(None);
                }
            }
        }
    }

    fn seek_to_object(&mut self, path: &str) -> Result<(), ReadError> {
        self.processed_objects.clear();

        /*
            S3 bucket-list calls are considered expensive, because of that we do one.
            Then, two linear passes detect the files which should be marked.
        */
        let object_lists = execute_with_retries(
            || self.bucket.list(self.objects_prefix.to_string(), None),
            RetryConfig::default(),
            MAX_S3_RETRIES,
        )
        .map_err(|e| ReadError::S3(S3CommandName::ListObjectsV2, e))?;

        let mut threshold_modification_time = None;
        for list in &object_lists {
            for object in &list.contents {
                if object.key == path {
                    let Ok(last_modified) = DateTime::parse_from_rfc3339(&object.last_modified)
                    else {
                        continue;
                    };
                    threshold_modification_time = Some(last_modified);
                }
            }
        }
        if let Some(threshold_modification_time) = threshold_modification_time {
            let path = path.to_string();
            for list in object_lists {
                for object in list.contents {
                    let Ok(last_modified) = DateTime::parse_from_rfc3339(&object.last_modified)
                    else {
                        continue;
                    };
                    if (last_modified, &object.key) < (threshold_modification_time, &path) {
                        self.processed_objects.insert(object.key);
                    }
                }
            }
            self.processed_objects.insert(path);
        } else {
            self.processed_objects.clear();
        }

        Ok(())
    }

    fn expect_current_object_path(&self) -> Arc<String> {
        self.current_object_path
            .as_ref()
            .expect("current object should be present")
            .clone()
    }

    fn sleep_duration() -> Duration {
        Duration::from_millis(10000)
    }
}

pub struct S3CsvReader {
    s3_scanner: S3Scanner,
    parser_builder: csv::ReaderBuilder,
    csv_reader: Option<csv::Reader<Cursor<Vec<u8>>>>,

    persistent_id: Option<PersistentId>,
    deferred_read_result: Option<ReadResult>,
    total_entries_read: u64,
}

impl S3CsvReader {
    pub fn new(
        bucket: S3Bucket,
        objects_prefix: impl Into<String>,
        parser_builder: csv::ReaderBuilder,
        streaming_mode: ConnectorMode,
        persistent_id: Option<PersistentId>,
        downloader_threads_count: usize,
    ) -> Result<S3CsvReader, ReadError> {
        Ok(S3CsvReader {
            s3_scanner: S3Scanner::new(
                bucket,
                objects_prefix,
                streaming_mode,
                downloader_threads_count,
            )?,
            parser_builder,
            csv_reader: None,

            persistent_id,
            deferred_read_result: None,
            total_entries_read: 0,
        })
    }

    fn stream_next_object(&mut self) -> Result<bool, ReadError> {
        if let Some(reader_impl) = self.s3_scanner.stream_next_object()? {
            self.csv_reader = Some(self.parser_builder.from_reader(reader_impl));
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

impl Reader for S3CsvReader {
    fn seek(&mut self, frontier: &OffsetAntichain) -> Result<(), ReadError> {
        let offset_value = frontier.get_offset(&OffsetKey::Empty);
        let Some(OffsetValue::S3ObjectPosition {
            total_entries_read,
            path: path_arc,
            bytes_offset,
        }) = offset_value
        else {
            if offset_value.is_some() {
                warn!("Incorrect type of offset value in S3Csv frontier: {offset_value:?}");
            }
            return Ok(());
        };

        let path = (**path_arc).clone();

        self.s3_scanner.seek_to_object(&path)?;
        let reader_impl = self.s3_scanner.stream_object_from_path(&path)?;
        let mut csv_reader = self.parser_builder.from_reader(reader_impl);

        let mut current_offset = 0;
        if *bytes_offset > 0 {
            let mut header_record = csv::StringRecord::new();
            if csv_reader.read_record(&mut header_record)? {
                let header_reader_context = ReaderContext::from_tokenized_entries(
                    DataEventType::Insert, // Currently no deletions for S3
                    header_record
                        .iter()
                        .map(std::string::ToString::to_string)
                        .collect(),
                );
                let offset = (OffsetKey::Empty, offset_value.unwrap().clone());
                let header_read_result = ReadResult::Data(header_reader_context, offset);
                self.deferred_read_result = Some(header_read_result);
                current_offset = csv_reader.position().byte();
            } else {
                error!("Empty S3 object, nothing to rewind");
                return Ok(());
            }
        }

        let mut byte_record = csv::ByteRecord::new();
        while current_offset < *bytes_offset && csv_reader.read_byte_record(&mut byte_record)? {
            current_offset = csv_reader.position().byte();
        }
        if current_offset != *bytes_offset {
            error!("Inconsistent bytes position in rewinded CSV object: expected {current_offset}, got {}", *bytes_offset);
        }

        self.total_entries_read = *total_entries_read;
        self.csv_reader = Some(csv_reader);

        Ok(())
    }

    fn read(&mut self) -> Result<ReadResult, ReadError> {
        if let Some(deferred_read_result) = self.deferred_read_result.take() {
            return Ok(deferred_read_result);
        }

        match &mut self.csv_reader {
            Some(csv_reader) => {
                let mut current_record = csv::StringRecord::new();
                if csv_reader.read_record(&mut current_record)? {
                    self.total_entries_read += 1;

                    let offset = (
                        OffsetKey::Empty,
                        OffsetValue::S3ObjectPosition {
                            total_entries_read: self.total_entries_read,
                            path: self.s3_scanner.expect_current_object_path(),
                            bytes_offset: csv_reader.position().byte(),
                        },
                    );

                    return Ok(ReadResult::Data(
                        ReaderContext::from_tokenized_entries(
                            DataEventType::Insert,
                            current_record
                                .iter()
                                .map(std::string::ToString::to_string)
                                .collect(),
                        ),
                        offset,
                    ));
                }
                if self.stream_next_object()? {
                    // No metadata is currently provided by S3 scanner
                    return Ok(ReadResult::NewSource(None));
                }
            }
            None => {
                if self.stream_next_object()? {
                    // No metadata is currently provided by S3 scanner
                    return Ok(ReadResult::NewSource(None));
                }
            }
        }

        Ok(ReadResult::Finished)
    }

    fn storage_type(&self) -> StorageType {
        StorageType::S3Csv
    }

    fn persistent_id(&self) -> Option<PersistentId> {
        self.persistent_id
    }

    fn update_persistent_id(&mut self, persistent_id: Option<PersistentId>) {
        self.persistent_id = persistent_id;
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

pub struct S3GenericReader {
    s3_scanner: S3Scanner,
    read_method: ReadMethod,

    reader: Option<BufReader<Cursor<Vec<u8>>>>,
    persistent_id: Option<PersistentId>,
    total_entries_read: u64,
    current_bytes_read: u64,
    deferred_read_result: Option<ReadResult>,
}

impl S3GenericReader {
    pub fn new(
        bucket: S3Bucket,
        objects_prefix: impl Into<String>,
        streaming_mode: ConnectorMode,
        persistent_id: Option<PersistentId>,
        read_method: ReadMethod,
        downloader_threads_count: usize,
    ) -> Result<S3GenericReader, ReadError> {
        Ok(S3GenericReader {
            s3_scanner: S3Scanner::new(
                bucket,
                objects_prefix,
                streaming_mode,
                downloader_threads_count,
            )?,
            read_method,

            reader: None,
            persistent_id,
            total_entries_read: 0,
            current_bytes_read: 0,
            deferred_read_result: None,
        })
    }

    fn stream_next_object(&mut self) -> Result<bool, ReadError> {
        if let Some(reader_impl) = self.s3_scanner.stream_next_object()? {
            self.current_bytes_read = 0;
            self.reader = Some(BufReader::new(reader_impl));
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

impl Reader for S3GenericReader {
    fn seek(&mut self, frontier: &OffsetAntichain) -> Result<(), ReadError> {
        let offset_value = frontier.get_offset(&OffsetKey::Empty);
        let Some(OffsetValue::S3ObjectPosition {
            total_entries_read,
            path: path_arc,
            bytes_offset,
        }) = offset_value
        else {
            if offset_value.is_some() {
                warn!("Incorrect type of offset value in S3Lines frontier: {offset_value:?}");
            }
            return Ok(());
        };

        let path = (**path_arc).clone();

        self.s3_scanner.seek_to_object(&path)?;
        let reader_impl = self.s3_scanner.stream_object_from_path(&path)?;

        let mut reader = BufReader::new(reader_impl);
        let mut bytes_read = 0;
        while bytes_read < *bytes_offset {
            let mut current_line = Vec::new();
            let len = self
                .read_method
                .read_next_bytes(&mut reader, &mut current_line)?;
            if len == 0 {
                break;
            }
            bytes_read += len as u64;
        }

        if bytes_read != *bytes_offset {
            if bytes_read == *bytes_offset + 1 || bytes_read == *bytes_offset + 2 {
                error!("Read {} bytes instead of expected {bytes_read}. If the file did not have newline at the end, you can ignore this message", *bytes_offset);
            } else {
                error!("Inconsistent bytes position in rewinded plaintext object: expected {bytes_read}, got {}", *bytes_offset);
            }
        }

        self.total_entries_read = *total_entries_read;
        self.current_bytes_read = bytes_read;
        self.reader = Some(reader);

        Ok(())
    }

    fn read(&mut self) -> Result<ReadResult, ReadError> {
        if let Some(deferred_read_result) = self.deferred_read_result.take() {
            return Ok(deferred_read_result);
        }

        match &mut self.reader {
            Some(reader) => {
                let mut line = Vec::new();
                let len = self.read_method.read_next_bytes(reader, &mut line)?;
                if len > 0 || self.read_method == ReadMethod::Full {
                    self.total_entries_read += 1;
                    self.current_bytes_read += len as u64;

                    let offset = (
                        OffsetKey::Empty,
                        OffsetValue::S3ObjectPosition {
                            total_entries_read: self.total_entries_read,
                            path: self.s3_scanner.expect_current_object_path(),
                            bytes_offset: self.current_bytes_read,
                        },
                    );

                    if self.read_method == ReadMethod::Full {
                        self.deferred_read_result = Some(ReadResult::FinishedSource {
                            commit_allowed: true,
                        });
                        self.reader = None;
                    }

                    return Ok(ReadResult::Data(
                        ReaderContext::from_raw_bytes(DataEventType::Insert, line), // Currently no deletions for S3
                        offset,
                    ));
                }
                self.reader = None;

                return Ok(ReadResult::FinishedSource {
                    commit_allowed: true,
                });
            }
            None => {
                if self.stream_next_object()? {
                    // No metadata is currently provided by S3 scanner
                    return Ok(ReadResult::NewSource(None));
                }
            }
        }

        Ok(ReadResult::Finished)
    }

    fn storage_type(&self) -> StorageType {
        StorageType::S3Lines
    }

    fn persistent_id(&self) -> Option<PersistentId> {
        self.persistent_id
    }

    fn update_persistent_id(&mut self, persistent_id: Option<PersistentId>) {
        self.persistent_id = persistent_id;
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
                return Ok(ReadResult::NewSource(None));
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
