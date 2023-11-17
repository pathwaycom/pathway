use rand::Rng;
use rdkafka::util::Timeout;
use s3::error::S3Error;
use std::any::type_name;
use std::borrow::Cow;
use std::collections::HashMap;
use std::collections::HashSet;
use std::env;
use std::fmt::Debug;
use std::fs::DirEntry;
use std::fs::File;
use std::io;
use std::io::BufRead;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Write;
use std::io::{Seek, SeekFrom};
use std::mem::take;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::str::{from_utf8, Utf8Error};
use std::sync::Arc;
use std::thread;
use std::thread::sleep;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chrono::{DateTime, FixedOffset};
use log::{error, warn};
use postgres::types::ToSql;
use tempfile::{tempdir, TempDir};
use xxhash_rust::xxh3::Xxh3 as Hasher;

use crate::connectors::data_format::FormatterContext;
use crate::connectors::metadata::SourceMetadata;
use crate::connectors::{Offset, OffsetKey, OffsetValue};
use crate::deepcopy::DeepCopy;
use crate::engine::Value;
use crate::fs_helpers::ensure_directory;
use crate::persistence::frontier::OffsetAntichain;
use crate::persistence::{ExternalPersistentId, PersistentId};
use crate::python_api::threads::PythonThreadState;
use crate::python_api::with_gil_and_pool;
use crate::python_api::PythonSubject;
use crate::timestamp::current_unix_timestamp_secs;

use bincode::ErrorKind as BincodeError;
use elasticsearch::{BulkParts, Elasticsearch};
use glob::Pattern as GlobPattern;
use glob::PatternError as GlobPatternError;
use pipe::PipeReader;
use postgres::Client as PsqlClient;
use pyo3::prelude::*;
use rdkafka::consumer::{BaseConsumer, Consumer, DefaultConsumerContext};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::producer::{BaseRecord, DefaultProducerContext, Producer, ThreadedProducer};
use rdkafka::topic_partition_list::Offset as KafkaOffset;
use rdkafka::Message;
use s3::bucket::Bucket as S3Bucket;
use serde::{Deserialize, Serialize};

#[cfg(target_os = "linux")]
mod inotify_support {
    use inotify::WatchMask;
    use std::path::Path;

    pub use inotify::Inotify;

    pub fn subscribe_inotify(path: impl AsRef<Path>) -> Option<Inotify> {
        let inotify = Inotify::init().ok()?;

        inotify
            .watches()
            .add(
                path,
                WatchMask::ATTRIB
                    | WatchMask::CLOSE_WRITE
                    | WatchMask::DELETE
                    | WatchMask::DELETE_SELF
                    | WatchMask::MOVE_SELF
                    | WatchMask::MOVED_FROM
                    | WatchMask::MOVED_TO,
            )
            .ok()?;

        Some(inotify)
    }

    pub fn wait(inotify: &mut Inotify) -> Option<()> {
        inotify
            .read_events_blocking(&mut [0; 1024])
            .ok()
            .map(|_events| ())
    }
}

#[cfg(not(target_os = "linux"))]
mod inotify_support {
    use std::path::Path;

    #[derive(Debug)]
    pub struct Inotify;

    pub fn subscribe_inotify(_path: impl AsRef<Path>) -> Option<Inotify> {
        None
    }

    pub fn wait(_inotify: &mut Inotify) -> Option<()> {
        None
    }
}

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

#[derive(PartialEq, Eq, Debug)]
pub enum ReaderContext {
    RawBytes(DataEventType, Vec<u8>),
    TokenizedEntries(DataEventType, Vec<String>),
    KeyValue((Option<Vec<u8>>, Option<Vec<u8>>)),
    Diff((DataEventType, Option<Value>, Vec<u8>)),
}

impl ReaderContext {
    pub fn from_raw_bytes(event: DataEventType, raw_bytes: Vec<u8>) -> ReaderContext {
        ReaderContext::RawBytes(event, raw_bytes)
    }

    pub fn from_diff(
        event: DataEventType,
        key: Option<Value>,
        raw_bytes: Vec<u8>,
    ) -> ReaderContext {
        ReaderContext::Diff((event, key, raw_bytes))
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

#[derive(Debug, Eq, PartialEq)]
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
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug)]
pub enum StorageType {
    FileSystem,
    S3Csv,
    S3Lines,
    CsvFilesystem,
    Kafka,
    Python,
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
                        OffsetValue::PythonEntrySequentialId(offset_position),
                        OffsetValue::PythonEntrySequentialId(other_position),
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

    #[error("failed to perform write in postgres")]
    Postgres(#[from] postgres::Error),

    #[error(transparent)]
    Utf8(#[from] Utf8Error),

    #[error(transparent)]
    Bincode(#[from] BincodeError),

    #[error("integer value {0} out of range")]
    IntOutOfRange(i64),

    #[error("query {query:?} failed: {error}")]
    PsqlQueryFailed {
        query: String,
        error: postgres::Error,
    },

    #[error("elasticsearch client error: {0:?}")]
    Elasticsearch(elasticsearch::Error),
}

pub trait Writer: Send {
    fn write(&mut self, data: FormatterContext) -> Result<(), WriteError>;

    fn flush(&mut self) -> Result<(), WriteError> {
        Ok(())
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
        path: impl Into<PathBuf>,
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

            if self.filesystem_scanner.is_polling_enabled() {
                self.filesystem_scanner.wait_for_new_files();
            } else {
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
        for payload in &data.payloads {
            self.writer.write_all(payload)?;
            self.writer.write_all(b"\n")?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<(), WriteError> {
        self.writer.flush()?;
        Ok(())
    }
}

pub struct KafkaReader {
    consumer: BaseConsumer<DefaultConsumerContext>,
    persistent_id: Option<PersistentId>,
    topic: Arc<String>,
    positions_for_seek: HashMap<i32, i64>,
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

            if let Some(last_read_offset) = self.positions_for_seek.get(&kafka_message.partition())
            {
                if last_read_offset >= &kafka_message.offset() {
                    if let Err(e) = self.consumer.seek(
                        kafka_message.topic(),
                        kafka_message.partition(),
                        KafkaOffset::Offset(*last_read_offset + 1),
                        None,
                    ) {
                        error!(
                            "Failed to seek topic and partition ({}, {}) to offset {}: {e}",
                            kafka_message.topic(),
                            kafka_message.partition(),
                            *last_read_offset + 1
                        );
                    }
                    continue;
                }
                self.positions_for_seek.remove(&kafka_message.partition());
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
                        *self.topic
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
                self.positions_for_seek.insert(*partition, *position);
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
}

impl KafkaReader {
    pub fn new(
        consumer: BaseConsumer<DefaultConsumerContext>,
        topic: String,
        persistent_id: Option<PersistentId>,
    ) -> KafkaReader {
        KafkaReader {
            consumer,
            persistent_id,
            topic: Arc::new(topic),
            positions_for_seek: HashMap::new(),
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
    path: PathBuf,
    cache_directory_path: Option<PathBuf>,

    is_directory: bool,
    streaming_mode: ConnectorMode,

    // Mapping from the path of the loaded file to its modification timestamp
    known_files: HashMap<PathBuf, u64>,

    current_action: Option<PosixScannerAction>,
    cached_modify_times: HashMap<PathBuf, Option<SystemTime>>,
    inotify: Option<inotify_support::Inotify>,
    object_pattern: GlobPattern,
    next_file_for_insertion: Option<PathBuf>,
    cached_metadata: HashMap<PathBuf, Option<SourceMetadata>>,

    // Storage is deleted on object destruction, so we need to store it
    // for the connector's life time
    _connector_tmp_storage: Option<TempDir>,
}

impl FilesystemScanner {
    fn new(
        path: impl Into<PathBuf>,
        persistent_id: Option<PersistentId>,
        streaming_mode: ConnectorMode,
        object_pattern: &str,
    ) -> Result<FilesystemScanner, ReadError> {
        let mut path = path.into();
        if path.exists() || matches!(streaming_mode, ConnectorMode::Static) {
            path = std::fs::canonicalize(path)?;
        }

        let is_directory = path.is_dir();
        let inotify = if streaming_mode.is_polling_enabled() {
            inotify_support::subscribe_inotify(&path)
        } else {
            None
        };

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
            path,
            is_directory,
            streaming_mode,
            cache_directory_path,

            known_files: HashMap::new(),
            current_action: None,
            cached_modify_times: HashMap::new(),
            inotify,
            object_pattern: GlobPattern::new(object_pattern)?,
            next_file_for_insertion: None,
            cached_metadata: HashMap::new(),
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

    fn seek_to_file(&mut self, seek_file_path: &Path) -> io::Result<()> {
        if self.streaming_mode.are_deletions_enabled() {
            todo!("seek for snapshot mode");
        }

        self.known_files.clear();
        if !self.is_directory {
            self.current_action = Some(PosixScannerAction::Read(Arc::new(
                seek_file_path.to_path_buf(),
            )));
            let modify_system_time = std::fs::metadata(seek_file_path)?.modified().unwrap();
            let modify_unix_timestamp = modify_system_time
                .duration_since(UNIX_EPOCH)
                .expect("File modified earlier than UNIX epoch")
                .as_secs();
            self.known_files
                .insert(seek_file_path.to_path_buf(), modify_unix_timestamp);
            return Ok(());
        }

        let target_modify_time = match std::fs::metadata(seek_file_path) {
            Ok(metadata) => metadata.modified()?,
            Err(e) => {
                if !matches!(e.kind(), std::io::ErrorKind::NotFound) {
                    return Err(e);
                }
                warn!(
                    "Unable to restore state: last persisted file {seek_file_path:?} not found in directory. Processing all files in directory."
                );
                return Ok(());
            }
        };
        let files_in_directory = std::fs::read_dir(self.path.as_path())?;
        for entry in files_in_directory.flatten() {
            if let Ok(file_type) = entry.file_type() {
                if !file_type.is_file() {
                    continue;
                }

                let Some(modify_time) = self.modify_time(&entry) else {
                    continue;
                };
                let current_path = entry.path();
                if (modify_time, current_path.as_path()) <= (target_modify_time, seek_file_path) {
                    let modify_timestamp = modify_time
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .expect("System time should be after the Unix epoch")
                        .as_secs();
                    self.known_files.insert(current_path, modify_timestamp);
                }
            }
        }
        self.current_action = Some(PosixScannerAction::Read(Arc::new(
            seek_file_path.to_path_buf(),
        )));

        Ok(())
    }

    fn modify_time(&mut self, entry: &DirEntry) -> Option<SystemTime> {
        if self.streaming_mode.are_deletions_enabled() {
            // If deletions are enabled, we also need to handle the case when the modification
            // time of an entry changes. Hence, we can't just memorize it once.
            entry.metadata().ok()?.modified().ok()
        } else {
            *self
                .cached_modify_times
                .entry(entry.path())
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
    fn next_action_determined(&mut self) -> io::Result<Option<ReadResult>> {
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

        // First check if we need to delete something
        if self.streaming_mode.are_deletions_enabled() {
            let next_for_deletion = self.next_deletion_entry();
            if next_for_deletion.is_some() {
                return Ok(next_for_deletion);
            }
        }

        // If there is nothing to delete, ingest the new entries
        self.next_insertion_entry()
    }

    fn next_deletion_entry(&mut self) -> Option<ReadResult> {
        let mut path_for_deletion: Option<PathBuf> = None;
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
                match &path_for_deletion {
                    None => path_for_deletion = Some(path.clone()),
                    Some(other_path) => {
                        if other_path > path {
                            path_for_deletion = Some(path.clone());
                        }
                    }
                }
            }
        }

        match path_for_deletion {
            Some(path) => {
                // Metadata of the deleted file must be the same as when it was added
                // so that the deletion event is processed correctly by timely. To achieve
                // this, we just take the cached metadata
                let old_metadata = self
                    .cached_metadata
                    .remove(&path)
                    .expect("inconsistency between known_files and cached_metadata");

                self.known_files.remove(&path.clone().clone());
                self.current_action = Some(PosixScannerAction::Delete(Arc::new(path.clone())));
                if path.exists() {
                    self.next_file_for_insertion = Some(path);
                }
                Some(ReadResult::NewSource(old_metadata))
            }
            None => None,
        }
    }

    fn cached_file_path(&self, path: &Path) -> Option<PathBuf> {
        self.cache_directory_path.as_ref().map(|root_path| {
            let mut hasher = Hasher::default();
            hasher.update(path.as_os_str().as_bytes());
            root_path.join(format!("{}", hasher.digest128()))
        })
    }

    fn next_insertion_entry(&mut self) -> io::Result<Option<ReadResult>> {
        let mut selected_file: Option<(PathBuf, SystemTime)> = None;
        if self.is_directory {
            let files_in_directory = std::fs::read_dir(self.path.as_path())?;

            for entry in files_in_directory.flatten() {
                if let Ok(file_type) = entry.file_type() {
                    if !file_type.is_file() {
                        continue;
                    }

                    let Some(modify_time) = self.modify_time(&entry) else {
                        continue;
                    };

                    let current_path = entry.path();

                    if let Some(file_name) = entry.file_name().to_str() {
                        if !self.object_pattern.matches(file_name) {
                            continue;
                        }
                    } else {
                        warn!(
                            "Failed to parse file name (non-unicode): {:?}. It will be ignored",
                            entry.file_name()
                        );
                        continue;
                    }

                    {
                        if self.known_files.contains_key(&(*current_path)) {
                            continue;
                        }
                        match &selected_file {
                            Some((currently_selected_name, selected_file_created_at)) => {
                                if (selected_file_created_at, currently_selected_name)
                                    > (&modify_time, &current_path)
                                {
                                    selected_file = Some((current_path, modify_time));
                                }
                            }
                            None => selected_file = Some((current_path, modify_time)),
                        }
                    }
                }
            }
        } else {
            let is_existing_file = self.path.exists() && self.path.is_file();
            if !self.known_files.is_empty() || !is_existing_file {
                return Ok(None);
            }
            selected_file = Some((self.path.clone(), SystemTime::now()));
        }

        match selected_file {
            Some((new_file_name, _)) => Ok(Some(self.initiate_file_insertion(&new_file_name)?)),
            None => Ok(None),
        }
    }

    fn initiate_file_insertion(&mut self, new_file_name: &PathBuf) -> io::Result<ReadResult> {
        let new_file_meta =
            SourceMetadata::from_fs_meta(new_file_name, &std::fs::metadata(new_file_name)?);
        self.cached_metadata
            .insert(new_file_name.clone(), Some(new_file_meta.clone()));
        self.known_files.insert(
            new_file_name.clone(),
            new_file_meta
                .modified_at
                .unwrap_or(current_unix_timestamp_secs()),
        );

        let cached_path = self.cached_file_path(new_file_name);
        if let Some(cached_path) = cached_path {
            std::fs::copy(new_file_name, cached_path)?;
        }

        self.current_action = Some(PosixScannerAction::Read(Arc::new(new_file_name.clone())));
        Ok(ReadResult::NewSource(Some(new_file_meta)))
    }

    fn sleep_duration() -> Duration {
        Duration::from_millis(500)
    }

    fn wait_for_new_files(&mut self) {
        self.inotify
            .as_mut()
            .and_then(inotify_support::wait)
            .unwrap_or_else(|| {
                sleep(Self::sleep_duration());
            });
    }
}

#[derive(Clone, Copy, Debug)]
pub enum ConnectorMode {
    Static,
    SimpleStreaming,
    StreamingWithDeletions,
}

impl ConnectorMode {
    pub fn new_from_flags(poll_new_objects: bool, allow_deletions: bool) -> Self {
        if poll_new_objects {
            if allow_deletions {
                ConnectorMode::StreamingWithDeletions
            } else {
                ConnectorMode::SimpleStreaming
            }
        } else {
            ConnectorMode::Static
        }
    }

    pub fn is_polling_enabled(&self) -> bool {
        match self {
            ConnectorMode::Static => false,
            ConnectorMode::SimpleStreaming | ConnectorMode::StreamingWithDeletions => true,
        }
    }

    pub fn are_deletions_enabled(&self) -> bool {
        match self {
            ConnectorMode::Static | ConnectorMode::SimpleStreaming => false,
            ConnectorMode::StreamingWithDeletions => true,
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
        path: impl Into<PathBuf>,
        parser_builder: csv::ReaderBuilder,
        streaming_mode: ConnectorMode,
        persistent_id: Option<PersistentId>,
        object_pattern: &str,
    ) -> Result<CsvFilesystemReader, ReadError> {
        let filesystem_scanner =
            FilesystemScanner::new(path.into(), persistent_id, streaming_mode, object_pattern)?;
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

            if self.filesystem_scanner.is_polling_enabled() {
                self.filesystem_scanner.wait_for_new_files();
            } else {
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
}

pub struct PythonReader {
    subject: Py<PythonSubject>,
    persistent_id: Option<PersistentId>,
    total_entries_read: u64,
    is_initialized: bool,

    #[allow(unused)]
    python_thread_state: PythonThreadState,
}

impl PythonReaderBuilder {
    pub fn new(subject: Py<PythonSubject>, persistent_id: Option<PersistentId>) -> Self {
        Self {
            subject,
            persistent_id,
        }
    }
}

impl ReaderBuilder for PythonReaderBuilder {
    fn build(self: Box<Self>) -> Result<Box<dyn Reader>, ReadError> {
        let python_thread_state = PythonThreadState::new();
        let Self {
            subject,
            persistent_id,
        } = *self;

        Ok(Box::new(PythonReader {
            subject,
            persistent_id,
            python_thread_state,
            total_entries_read: 0,
            is_initialized: false,
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

impl Reader for PythonReader {
    fn seek(&mut self, frontier: &OffsetAntichain) -> Result<(), ReadError> {
        let offset_value = frontier.get_offset(&OffsetKey::Empty);
        let Some(OffsetValue::PythonEntrySequentialId(offset_value)) = offset_value else {
            if offset_value.is_some() {
                warn!("Incorrect type of offset value in Python frontier: {offset_value:?}");
            }
            return Ok(());
        };

        self.total_entries_read = *offset_value;

        Ok(())
    }

    fn read(&mut self) -> Result<ReadResult, ReadError> {
        if !self.is_initialized {
            with_gil_and_pool(|py| self.subject.borrow(py).start.call0(py))?;
            self.is_initialized = true;
        }

        with_gil_and_pool(|py| {
            let (event, key, values): (DataEventType, Option<Value>, Vec<u8>) = self
                .subject
                .borrow(py)
                .read
                .call0(py)?
                .extract(py)
                .map_err(ReadError::Py)?;

            if values == "*FINISH*".as_bytes() {
                Ok(ReadResult::Finished)
            } else {
                // We use simple sequential offset because Python connector is single threaded, as
                // by default.
                //
                // If it's changed, add worker_id to the offset.
                self.total_entries_read += 1;
                let offset = (
                    OffsetKey::Empty,
                    OffsetValue::PythonEntrySequentialId(self.total_entries_read),
                );

                Ok(ReadResult::Data(
                    ReaderContext::from_diff(event, key, values),
                    offset,
                ))
            }
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
}

impl PsqlWriter {
    pub fn new(client: PsqlClient, max_batch_size: Option<usize>) -> PsqlWriter {
        PsqlWriter {
            client,
            max_batch_size,
            buffer: Vec::new(),
        }
    }
}

mod to_sql {
    use std::error::Error;

    use bytes::BytesMut;
    use chrono::{DateTime, NaiveDateTime, Utc};
    use ordered_float::OrderedFloat;
    use postgres::types::{to_sql_checked, Format, IsNull, ToSql, Type, WrongType};

    use crate::engine::time::DateTime as _;
    use crate::engine::Value;

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
            match self {
                Self::None => return Ok(IsNull::Yes),
                Self::Bool(b) => {
                    try_forward!(bool, *b);
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
                }
                Self::Float(OrderedFloat(f)) => {
                    try_forward!(f64, *f);
                    #[allow(clippy::cast_possible_truncation)]
                    {
                        try_forward!(f32, *f as f32);
                    }
                }
                Self::Pointer(p) => {
                    try_forward!(String, p.to_string());
                }
                Self::String(s) => {
                    try_forward!(&str, s.as_str());
                }
                Self::Bytes(b) => {
                    try_forward!(&[u8], &b[..]);
                }
                Self::Tuple(t) => {
                    try_forward!(&[Value], &t[..]);
                }
                Self::IntArray(_) => {}   // TODO
                Self::FloatArray(_) => {} // TODO
                Self::DateTimeNaive(dt) => {
                    try_forward!(NaiveDateTime, dt.as_chrono_datetime());
                }
                Self::DateTimeUtc(dt) => {
                    try_forward!(DateTime<Utc>, dt.as_chrono_datetime().and_utc());
                }
                Self::Duration(_) => {} // TODO
                Self::Json(j) => {
                    try_forward!(&serde_json::Value, &**j);
                }
            };
            Err(Box::new(WrongType::new::<Self>(ty.clone())))
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
                self.flush()?;
            }
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<(), WriteError> {
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

            for payload in &data.payloads {
                let query = from_utf8(payload)?;

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
        false
    }
}

pub struct CurrentlyProcessedS3Object {
    loader_thread: std::thread::JoinHandle<Result<(), ReadError>>,
    path: Arc<String>,
}

impl CurrentlyProcessedS3Object {
    pub fn finalize(self) -> Result<(), ReadError> {
        self.loader_thread.join().expect("s3 thread join failed")
    }
}

pub struct S3Scanner {
    /*
        This class takes responsibility over S3 object selection and streaming.
        In encapsulates the selection of the next object to stream and streaming
        the object and provides reader end of the pipe to the outside user.
    */
    bucket: S3Bucket,
    objects_prefix: String,
    current_object: Option<CurrentlyProcessedS3Object>,
    processed_objects: HashSet<String>,
}

impl S3Scanner {
    pub fn new(bucket: S3Bucket, objects_prefix: impl Into<String>) -> Result<Self, ReadError> {
        let objects_prefix = objects_prefix.into();

        let object_lists = bucket
            .list(objects_prefix.clone(), None)
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

            current_object: None,
            processed_objects: HashSet::new(),
        })
    }

    pub fn stream_object_from_path_and_bucket(
        object_path_ref: &str,
        bucket: S3Bucket,
    ) -> (CurrentlyProcessedS3Object, PipeReader) {
        let object_path = object_path_ref.to_string();

        let (pipe_reader, mut pipe_writer) = pipe::pipe();
        let loader_thread = thread::Builder::new()
            .name(format!("pathway:s3_get-{object_path_ref}"))
            .spawn(move || {
                let code = bucket
                    .get_object_to_writer(&object_path, &mut pipe_writer)
                    .map_err(|e| ReadError::S3(S3CommandName::GetObject, e))?;
                if code != 200 {
                    return Err(ReadError::S3(S3CommandName::GetObject, S3Error::HttpFail));
                }
                Ok(())
            })
            .expect("s3 thread creation failed");

        (
            CurrentlyProcessedS3Object {
                loader_thread,
                path: Arc::new(object_path_ref.to_string()),
            },
            pipe_reader,
        )
    }

    fn stream_object_from_path(&mut self, object_path_ref: &str) -> PipeReader {
        let (current_object, pipe_reader) =
            Self::stream_object_from_path_and_bucket(object_path_ref, self.bucket.deep_copy());
        self.current_object = Some(current_object);
        pipe_reader
    }

    fn stream_next_object(&mut self) -> Result<Option<PipeReader>, ReadError> {
        if let Some(state) = self.current_object.take() {
            state.loader_thread.join().expect("s3 thread panic")?;
        }

        let object_lists = self
            .bucket
            .list(self.objects_prefix.to_string(), None)
            .map_err(|e| ReadError::S3(S3CommandName::ListObjectsV2, e))?;

        let mut selected_object: Option<(DateTime<FixedOffset>, String)> = None;
        for list in &object_lists {
            for object in &list.contents {
                if self.processed_objects.contains(&object.key) {
                    continue;
                }

                let Ok(last_modified) = DateTime::parse_from_rfc3339(&object.last_modified) else {
                    continue;
                };

                match &selected_object {
                    Some((earliest_modify_time, selected_object_name)) => {
                        if (earliest_modify_time, selected_object_name)
                            > (&last_modified, &object.key)
                        {
                            selected_object = Some((last_modified, object.key.clone()));
                        }
                    }
                    None => selected_object = Some((last_modified, object.key.clone())),
                };
            }
        }

        match selected_object {
            Some((_earliest_modify_time, selected_object_name)) => {
                let pipe_reader = self.stream_object_from_path(&selected_object_name);
                self.processed_objects.insert(selected_object_name);
                Ok(Some(pipe_reader))
            }
            None => Ok(None),
        }
    }

    fn seek_to_object(&mut self, path: &str) -> Result<(), ReadError> {
        self.processed_objects.clear();

        /*
            S3 bucket-list calls are considered expensive, because of that we do one.
            Then, two linear passes detect the files which should be marked.
        */
        let object_lists = self
            .bucket
            .list(self.objects_prefix.to_string(), None)
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
        self.current_object
            .as_ref()
            .expect("current object should be present")
            .path
            .clone()
    }
}

pub struct S3CsvReader {
    s3_scanner: S3Scanner,
    poll_new_objects: bool,

    parser_builder: csv::ReaderBuilder,
    csv_reader: Option<csv::Reader<PipeReader>>,

    persistent_id: Option<PersistentId>,
    deferred_read_result: Option<ReadResult>,
    total_entries_read: u64,
}

impl S3CsvReader {
    pub fn new(
        bucket: S3Bucket,
        objects_prefix: impl Into<String>,
        parser_builder: csv::ReaderBuilder,
        poll_new_objects: bool,
        persistent_id: Option<PersistentId>,
    ) -> Result<S3CsvReader, ReadError> {
        Ok(S3CsvReader {
            s3_scanner: S3Scanner::new(bucket, objects_prefix)?,
            poll_new_objects,

            parser_builder,
            csv_reader: None,

            persistent_id,
            deferred_read_result: None,
            total_entries_read: 0,
        })
    }

    fn stream_next_object(&mut self) -> Result<bool, ReadError> {
        if let Some(pipe_reader) = self.s3_scanner.stream_next_object()? {
            self.csv_reader = Some(self.parser_builder.from_reader(pipe_reader));
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn sleep_duration() -> Duration {
        Duration::from_millis(10000)
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
        let pipe_reader = self.s3_scanner.stream_object_from_path(&path);
        let mut csv_reader = self.parser_builder.from_reader(pipe_reader);

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

        loop {
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

            if self.poll_new_objects {
                sleep(Self::sleep_duration());
            } else {
                return Ok(ReadResult::Finished);
            }
        }
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
}

impl KafkaWriter {
    pub fn new(producer: ThreadedProducer<DefaultProducerContext>, topic: String) -> KafkaWriter {
        KafkaWriter { producer, topic }
    }
}

impl Drop for KafkaWriter {
    fn drop(&mut self) {
        self.producer.flush(None).expect("kafka commit should work");
    }
}

impl Writer for KafkaWriter {
    fn write(&mut self, data: FormatterContext) -> Result<(), WriteError> {
        let key_as_bytes = data.key.0.to_le_bytes().to_vec();
        for payload in &data.payloads {
            let mut entry = BaseRecord::<Vec<u8>, Vec<u8>>::to(&self.topic)
                .payload(payload)
                .key(&key_as_bytes);
            loop {
                match self.producer.send(entry) {
                    Ok(_) => break,
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

impl Drop for ElasticSearchWriter {
    fn drop(&mut self) {
        if !self.docs_buffer.is_empty() {
            self.flush().unwrap();
        }
    }
}

impl Writer for ElasticSearchWriter {
    fn write(&mut self, data: FormatterContext) -> Result<(), WriteError> {
        for payload in data.payloads {
            self.docs_buffer.push(b"{\"index\": {}}".to_vec());
            self.docs_buffer.push(payload);
        }

        if let Some(max_batch_size) = self.max_batch_size {
            if self.docs_buffer.len() / 2 >= max_batch_size {
                self.flush()?;
            }
        }

        Ok(())
    }

    fn flush(&mut self) -> Result<(), WriteError> {
        if self.docs_buffer.is_empty() {
            return Ok(());
        }
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
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
    poll_new_objects: bool,
    read_method: ReadMethod,

    reader: Option<BufReader<PipeReader>>,
    persistent_id: Option<PersistentId>,
    total_entries_read: u64,
    current_bytes_read: u64,
    deferred_read_result: Option<ReadResult>,
}

impl S3GenericReader {
    pub fn new(
        bucket: S3Bucket,
        objects_prefix: impl Into<String>,
        poll_new_objects: bool,
        persistent_id: Option<PersistentId>,
        read_method: ReadMethod,
    ) -> Result<S3GenericReader, ReadError> {
        Ok(S3GenericReader {
            s3_scanner: S3Scanner::new(bucket, objects_prefix)?,
            poll_new_objects,
            read_method,

            reader: None,
            persistent_id,
            total_entries_read: 0,
            current_bytes_read: 0,
            deferred_read_result: None,
        })
    }

    fn stream_next_object(&mut self) -> Result<bool, ReadError> {
        if let Some(pipe_reader) = self.s3_scanner.stream_next_object()? {
            self.current_bytes_read = 0;
            self.reader = Some(BufReader::new(pipe_reader));
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn sleep_duration() -> Duration {
        Duration::from_millis(10000)
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
        let pipe_reader = self.s3_scanner.stream_object_from_path(&path);

        let mut reader = BufReader::new(pipe_reader);
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

        loop {
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

            if self.poll_new_objects {
                sleep(Self::sleep_duration());
            } else {
                return Ok(ReadResult::Finished);
            }
        }
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
