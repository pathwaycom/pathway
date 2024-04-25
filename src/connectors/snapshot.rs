// Copyright © 2024 Pathway

use log::{error, info, warn};
use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::{BufReader, BufWriter, ErrorKind as IoErrorKind, Seek, Write};
use std::mem::take;
use std::path::Path;
use std::path::PathBuf;
use std::sync::mpsc;
use std::sync::mpsc::Sender;
use std::thread;

use bincode::{deserialize_from, serialize, serialize_into, ErrorKind as BincodeError};
use futures::channel::oneshot;
use futures::channel::oneshot::Receiver as OneShotReceiver;
use futures::channel::oneshot::Sender as OneShotSender;
use pipe::PipeReader;
use s3::bucket::Bucket as S3Bucket;
use s3::error::S3Error;
use s3::serde_types::Part as S3Part;
use serde::{Deserialize, Serialize};

use crate::connectors::data_storage::S3CommandName;
use crate::connectors::data_storage::{
    CurrentlyProcessedS3Object, ReadError, S3Scanner, WriteError,
};
use crate::deepcopy::DeepCopy;
use crate::engine::{Key, Value};
use crate::engine::{Timestamp, TotalFrontier};
use crate::fs_helpers::ensure_directory;
use crate::persistence::frontier::OffsetAntichain;
use crate::timestamp::current_unix_timestamp_ms;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum Event {
    Insert(Key, Vec<Value>),
    Delete(Key, Vec<Value>),
    Upsert(Key, Option<Vec<Value>>),
    AdvanceTime(Timestamp, OffsetAntichain),
    Finished,
}

pub trait ReadSnapshotEvent {
    /// This method will be called every so often to read the persisted snapshot.
    /// When there are no entries left, it must return `Event::Finished`.
    fn read(&mut self) -> Result<Event, ReadError>;

    /// This method will be called after the snapshot is read until the last entry, which can be
    /// processed.
    ///
    /// It must ensure that no further data is present in the snapshot so that when it gets appended,
    /// the unused data is not written next to the non-processed tail.
    fn truncate(&mut self) -> Result<(), ReadError>;

    /// This method will be called to check, whether snapshot reading should end, when timestamp exceeds
    /// threshold from metadata or will the snapshot reader finish rewinding by itself.
    ///
    /// In the latter case, snapshot reader needs to end by sending `Event::Finished`.
    fn check_threshold_from_metadata(&mut self) -> bool {
        true
    }
}

#[allow(clippy::module_name_repetitions)]
pub type SnapshotWriterFlushFuture = OneShotReceiver<Result<(), WriteError>>;

pub trait WriteSnapshotEvent: Send {
    /// A non-blocking call, pushing an entry in the buffer.
    /// The buffer should not be flushed in the same thread.
    fn write(&mut self, event: &Event) -> Result<(), WriteError>;

    /// Flush the entries which are currently present in the buffer.
    /// The result returned must be waited and return an `Ok()` when the data is uploaded.
    ///
    /// We use `futures::channel::oneshot::channel` here instead of Future/Promise
    /// because it uses modern Rust Futures that are also used by `async`.
    fn flush(&mut self) -> OneShotReceiver<Result<(), WriteError>>;
}

pub struct LocalBinarySnapshotReader {
    root_path: PathBuf,
    reader: Option<BufReader<std::fs::File>>,
    next_file_idx: usize,
    times_advanced: Vec<Timestamp>,
}

impl LocalBinarySnapshotReader {
    pub fn new(root_path: PathBuf) -> Result<LocalBinarySnapshotReader, ReadError> {
        let mut times_advanced = Vec::new();
        let entries = fs::read_dir(&root_path).map_err(ReadError::Io)?;
        for entry in entries {
            let entry = entry.map_err(ReadError::Io)?;
            if let Ok(file_name) = entry.file_name().into_string() {
                if let Ok(parsed_time) = file_name.parse() {
                    times_advanced.push(parsed_time);
                } else {
                    error!("Unparsable timestamp: {file_name}");
                }
            } else {
                error!("Unparsable file name: {entry:#?}");
            }
        }
        times_advanced.sort_unstable();

        Ok(Self {
            root_path,
            reader: None,
            next_file_idx: 0,
            times_advanced,
        })
    }
}

impl ReadSnapshotEvent for LocalBinarySnapshotReader {
    fn read(&mut self) -> Result<Event, ReadError> {
        loop {
            match &mut self.reader {
                Some(reader) => match deserialize_from(reader) {
                    Ok(entry) => return Ok(entry),
                    Err(e) => match *e {
                        BincodeError::Io(e) => {
                            if !matches!(e.kind(), IoErrorKind::UnexpectedEof) {
                                return Err(ReadError::Io(e));
                            }
                            self.reader = None;
                            continue;
                        }
                        _ => return Err(ReadError::Bincode(*e)),
                    },
                },
                None => {
                    if self.next_file_idx >= self.times_advanced.len() {
                        break;
                    }
                    let current_file_path = Path::new(&self.root_path)
                        .join(format!("{}", self.times_advanced[self.next_file_idx]));
                    self.reader = Some(BufReader::new(
                        File::open(current_file_path).map_err(ReadError::Io)?,
                    ));
                    self.next_file_idx += 1;
                }
            }
        }
        Ok(Event::Finished)
    }

    fn truncate(&mut self) -> Result<(), ReadError> {
        if let Some(ref mut reader) = &mut self.reader {
            let stable_position = reader.stream_position()?;
            let file_path = Path::new(&self.root_path)
                .join(format!("{}", self.times_advanced[self.next_file_idx - 1]));

            info!("Truncate: Shrink {file_path:?} to {stable_position} bytes");

            let file = OpenOptions::new().write(true).open(file_path)?;
            file.set_len(stable_position)?;
        }

        for unreachable_part in &self.times_advanced[self.next_file_idx..] {
            let snapshot_file_to_remove =
                Path::new(&self.root_path).join(format!("{unreachable_part}"));
            info!("Truncate: Remove {snapshot_file_to_remove:?}");
            std::fs::remove_file(snapshot_file_to_remove)?;
        }

        Ok(())
    }
}

pub struct LocalBinarySnapshotWriter {
    root_path: PathBuf,
    lazy_writer: Option<BufWriter<std::fs::File>>,
}

impl LocalBinarySnapshotWriter {
    pub fn new(path: &Path) -> Result<LocalBinarySnapshotWriter, WriteError> {
        ensure_directory(path)?;

        Ok(Self {
            root_path: path.to_owned(),
            lazy_writer: None,
        })
    }
}

impl WriteSnapshotEvent for LocalBinarySnapshotWriter {
    fn write(&mut self, event: &Event) -> Result<(), WriteError> {
        let writer = {
            if let Some(lazy_writer) = &mut self.lazy_writer {
                lazy_writer
            } else {
                let current_timestamp = current_unix_timestamp_ms();
                let path = self.root_path.join(format!("{current_timestamp}"));

                self.lazy_writer = Some(BufWriter::new(File::create(path)?));
                self.lazy_writer.as_mut().unwrap()
            }
        };

        serialize_into(writer, &event).map_err(|e| match *e {
            BincodeError::Io(io_error) => WriteError::Io(*Box::new(io_error)),
            _ => WriteError::Bincode(*e),
        })
    }

    fn flush(&mut self) -> OneShotReceiver<Result<(), WriteError>> {
        let (sender, receiver) = oneshot::channel();

        let internal_flush_result: Result<(), WriteError> = match &mut self.lazy_writer {
            Some(ref mut writer) => writer.flush().map_err(WriteError::Io),
            None => Ok(()),
        };

        let send_result = sender.send(internal_flush_result);
        if let Err(unsent_flush_result) = send_result {
            error!("The receiver no longer waits for the result of this flush: {unsent_flush_result:?}");
        }

        receiver
    }
}

const SNAPSHOT_CONTENT_TYPE: &str = "application/octet-stream";
const MAX_CHUNK_LEN: usize = 1_000_000;

pub struct S3Writer {
    bucket: S3Bucket,
    key: String,
    upload_id: String,
    upload_parts: Vec<S3Part>,
}

impl S3Writer {
    pub fn new(bucket: S3Bucket, path: &str) -> Result<Self, (S3CommandName, S3Error)> {
        let start_upload_response = bucket
            .initiate_multipart_upload(path, SNAPSHOT_CONTENT_TYPE)
            .map_err(|e| (S3CommandName::InitiateMultipartUpload, e))?;

        Ok(Self {
            bucket,
            key: start_upload_response.key,
            upload_id: start_upload_response.upload_id,
            upload_parts: Vec::new(),
        })
    }

    pub fn put_chunk(&mut self, buffer: Vec<Event>) -> Result<(), (S3CommandName, S3Error)> {
        let chunk = {
            let mut chunk = Vec::new();
            for entry in buffer {
                let mut entry_serialized = serialize(&entry).expect("unable to serialize an entry");
                chunk.append(&mut entry_serialized);
            }
            chunk
        };

        let part_number =
            u32::try_from(self.upload_parts.len()).expect("too many upload parts") + 1;
        let part = self
            .bucket
            .put_multipart_chunk(
                chunk,
                &self.key,
                part_number,
                &self.upload_id,
                SNAPSHOT_CONTENT_TYPE,
            )
            .map_err(|e| (S3CommandName::PutMultipartChunk, e))?;
        self.upload_parts.push(part);

        Ok(())
    }

    pub fn finalize(self) -> Result<(), (S3CommandName, S3Error)> {
        let response = self
            .bucket
            .complete_multipart_upload(&self.key, &self.upload_id, self.upload_parts)
            .map_err(|e| (S3CommandName::CompleteMultipartUpload, e))?;

        if response.status_code() == 200 {
            info!("Snapshot part loaded successfully: {response}");
        } else {
            error!("Upload finalization status code incorrect: {:?}", response);
        }

        Ok(())
    }
}

pub struct S3SnapshotReader {
    root_path: String,
    reader: Option<PipeReader>,
    next_object_idx: usize,
    times_advanced: Vec<Timestamp>,

    bucket: S3Bucket,
    current_state: Option<CurrentlyProcessedS3Object>,
    current_chunk_len: usize,
}

impl S3SnapshotReader {
    pub fn new(bucket: S3Bucket, path: &str) -> Result<S3SnapshotReader, ReadError> {
        let mut times_advanced = Vec::new();

        let object_lists = bucket
            .list(path.to_string(), None)
            .map_err(|e| ReadError::S3(S3CommandName::ListObjectsV2, e))?;

        for list in &object_lists {
            for object in &list.contents {
                let path_obj = Path::new(&object.key);
                let Some(file_name) = path_obj.file_name() else {
                    warn!("Not file-like path: {}", object.key);
                    continue;
                };
                let Some(file_name_str) = file_name.to_str() else {
                    error!("Unparsable file name in path {}", object.key);
                    continue;
                };
                if let Ok(timestamp) = file_name_str.parse() {
                    times_advanced.push(timestamp);
                } else {
                    error!("Unparsable timestamp. Full path: {}", object.key);
                }
            }
        }

        times_advanced.sort_unstable();

        Ok(Self {
            bucket,
            root_path: path.to_string(),
            reader: None,
            next_object_idx: 0,
            times_advanced,
            current_state: None,
            current_chunk_len: 0,
        })
    }
}

impl ReadSnapshotEvent for S3SnapshotReader {
    fn read(&mut self) -> Result<Event, ReadError> {
        loop {
            match &mut self.reader {
                Some(reader) => match deserialize_from::<&mut PipeReader, Event>(reader) {
                    Ok(entry) => {
                        self.current_chunk_len += 1;
                        return Ok(entry);
                    }
                    Err(e) => match *e {
                        BincodeError::Io(e) => {
                            if !matches!(e.kind(), IoErrorKind::UnexpectedEof) {
                                return Err(ReadError::Io(e));
                            }
                            self.reader = None;
                            continue;
                        }
                        _ => return Err(ReadError::Bincode(*e)),
                    },
                },
                None => {
                    if self.next_object_idx >= self.times_advanced.len() {
                        break;
                    }
                    let current_file_path = format!(
                        "{}/{}",
                        self.root_path, self.times_advanced[self.next_object_idx]
                    );
                    let (new_current_state, pipe_reader) =
                        S3Scanner::stream_object_from_path_and_bucket(
                            &current_file_path,
                            self.bucket.deep_copy(),
                        );

                    if let Some(state) = self.current_state.take() {
                        state.finalize()?;
                    }

                    self.current_chunk_len = 0;
                    self.current_state = Some(new_current_state);
                    self.reader = Some(pipe_reader);
                    self.next_object_idx += 1;
                }
            }
        }
        warn!("Attempted reading after the end of snapshot, returning finish event");
        Ok(Event::Finished)
    }

    fn truncate(&mut self) -> Result<(), ReadError> {
        // Truncate the current file by saving the currently read chunk
        if self.next_object_idx > 0 && self.current_chunk_len > 0 {
            let object_for_truncation = format!(
                "{}/{}",
                self.root_path,
                self.times_advanced[self.next_object_idx - 1]
            );

            let object_after_truncation =
                format!("{}/{}", self.root_path, current_unix_timestamp_ms());

            let (_new_current_state, mut pipe_reader) =
                S3Scanner::stream_object_from_path_and_bucket(
                    &object_for_truncation,
                    self.bucket.deep_copy(),
                );
            let mut writer = S3Writer::new(self.bucket.deep_copy(), &object_after_truncation)
                .map_err(|(command, error)| ReadError::S3(command, error))?;

            let mut n_entries_processed = 0;
            let mut current_chunk = Vec::new();
            while n_entries_processed < self.current_chunk_len {
                let maybe_entry_read = deserialize_from::<&mut PipeReader, Event>(&mut pipe_reader);
                if let Ok(entry) = maybe_entry_read {
                    current_chunk.push(entry);
                } else {
                    error!("Unexpected end of dump object {object_for_truncation}");
                    break;
                }
                if current_chunk.len() == MAX_CHUNK_LEN {
                    writer
                        .put_chunk(take(&mut current_chunk))
                        .map_err(|(command, s3_error)| ReadError::S3(command, s3_error))?;
                }
                n_entries_processed += 1;
            }
            if !current_chunk.is_empty() {
                writer
                    .put_chunk(take(&mut current_chunk))
                    .map_err(|(command, s3_error)| ReadError::S3(command, s3_error))?;
            }

            writer
                .finalize()
                .map_err(|(command, error)| ReadError::S3(command, error))?;
        }

        // Delete all further non-read files
        let removal_files_start = self.next_object_idx.saturating_sub(1);
        for unreachable_part in &self.times_advanced[removal_files_start..] {
            let snapshot_file_to_remove = format!("{}/{unreachable_part}", self.root_path);
            self.bucket
                .delete_object(snapshot_file_to_remove)
                .map_err(|e| ReadError::S3(S3CommandName::DeleteObject, e))?;
        }

        Ok(())
    }
}

enum S3SnapshotWriterEvent {
    Chunk(Vec<Event>),
    Flush(OneShotSender<Result<(), WriteError>>),
    Finish,
}

pub struct S3SnapshotWriter {
    current_chunk: Vec<Event>,
    chunk_events_sender: Sender<S3SnapshotWriterEvent>,
    uploader_thread: Option<std::thread::JoinHandle<()>>,
}

impl Drop for S3SnapshotWriter {
    fn drop(&mut self) {
        self.chunk_events_sender
            .send(S3SnapshotWriterEvent::Finish)
            .expect("failed to submit the graceful shutdown event");
        if let Some(uploader_thread) = take(&mut self.uploader_thread) {
            if let Err(e) = uploader_thread.join() {
                // there is no formatter for std::any::Any
                error!("Failed to join s3 snapshot uploader thread: {e:?}");
            }
        }
    }
}

impl S3SnapshotWriter {
    pub fn new(bucket: S3Bucket, chunks_root_path: &str) -> Self {
        let (chunk_events_sender, chunk_events_receiver) = mpsc::channel();

        let inner_chunks_root_path = chunks_root_path.to_string();
        let uploader_thread = thread::Builder::new()
            .name("pathway:s3_snapshot-bg-writer".to_string())
            .spawn(move || {
                let mut s3_writer = S3Writer::new(bucket.deep_copy(), &format!("{}/{}", inner_chunks_root_path, current_unix_timestamp_ms())).expect("failed to construct s3 writer");
                loop {
                    let event = chunk_events_receiver.recv().expect("unexpected termination for s3 events sender");
                    match event {
                        S3SnapshotWriterEvent::Chunk(chunk) => {
                            s3_writer.put_chunk(chunk).expect("failed to write snapshot chunk");
                        }
                        S3SnapshotWriterEvent::Flush(sender) => {
                            let flush_result = s3_writer.finalize().map_err(|(command, s3_error)| WriteError::S3(command, s3_error));
                            s3_writer = S3Writer::new(bucket.deep_copy(), &format!("{}/{}", inner_chunks_root_path, current_unix_timestamp_ms())).expect("failed to construct s3 writer");
                            if let Err(unsent_flush_result) = sender.send(flush_result) {
                                error!("The receiver no longer waits for the result of this flush: {unsent_flush_result:?}");
                            }
                        }
                        S3SnapshotWriterEvent::Finish => break,
                    }
                }
            })
            .expect("s3 thread creation failed");

        Self {
            current_chunk: Vec::new(),
            chunk_events_sender,
            uploader_thread: Some(uploader_thread),
        }
    }
}

impl WriteSnapshotEvent for S3SnapshotWriter {
    fn write(&mut self, event: &Event) -> Result<(), WriteError> {
        self.current_chunk.push(event.clone());
        if self.current_chunk.len() == MAX_CHUNK_LEN {
            self.chunk_events_sender
                .send(S3SnapshotWriterEvent::Chunk(take(&mut self.current_chunk)))
                .expect("chunk queue submission should not fail");
        }
        Ok(())
    }

    fn flush(&mut self) -> OneShotReceiver<Result<(), WriteError>> {
        if !self.current_chunk.is_empty() {
            self.chunk_events_sender
                .send(S3SnapshotWriterEvent::Chunk(take(&mut self.current_chunk)))
                .expect("chunk queue submission should not fail");
        }

        let (sender, receiver) = oneshot::channel();
        self.chunk_events_sender
            .send(S3SnapshotWriterEvent::Flush(sender))
            .expect("flush event submission should not fail");

        receiver
    }
}

pub struct MockSnapshotReader {
    events: Box<dyn Iterator<Item = Event>>,
}

impl MockSnapshotReader {
    pub fn new(events: Vec<Event>) -> Self {
        Self {
            events: Box::new(events.into_iter()),
        }
    }
}

impl ReadSnapshotEvent for MockSnapshotReader {
    fn read(&mut self) -> Result<Event, ReadError> {
        if let Some(event) = self.events.next() {
            Ok(event)
        } else {
            Ok(Event::Finished)
        }
    }

    fn truncate(&mut self) -> Result<(), ReadError> {
        Ok(())
    }

    fn check_threshold_from_metadata(&mut self) -> bool {
        false
    }
}

#[allow(clippy::module_name_repetitions)]
pub struct SnapshotReader {
    reader_impl: Box<dyn ReadSnapshotEvent>,
    threshold_time: TotalFrontier<Timestamp>,
    entries_read: usize,
    last_frontier: OffsetAntichain,
    truncate_at_end: bool,
}

impl SnapshotReader {
    pub fn new(
        mut reader_impl: Box<dyn ReadSnapshotEvent>,
        threshold_time: TotalFrontier<Timestamp>,
        truncate_at_end: bool,
    ) -> Result<Self, ReadError> {
        if threshold_time == TotalFrontier::At(Timestamp(0)) {
            info!("No time has been advanced in the previous run, therefore no data read from the snapshot");
            if truncate_at_end {
                if let Err(e) = reader_impl.truncate() {
                    error!("Failed to truncate the snapshot, the next re-run may provide incorrect results: {e}");
                    return Err(e);
                }
            }
        }

        Ok(Self {
            reader_impl,
            threshold_time,
            last_frontier: OffsetAntichain::new(),
            entries_read: 0,
            truncate_at_end,
        })
    }

    pub fn last_frontier(&self) -> &OffsetAntichain {
        &self.last_frontier
    }

    pub fn read(&mut self) -> Result<Event, ReadError> {
        let event = self.reader_impl.read()?;
        if matches!(event, Event::Finished) {
            return Ok(event);
        }
        if let Event::AdvanceTime(new_time, ref frontier) = event {
            let read_finished = TotalFrontier::At(new_time) >= self.threshold_time;
            self.last_frontier = frontier.clone();
            if self.reader_impl.check_threshold_from_metadata() && read_finished {
                if self.truncate_at_end {
                    if let Err(e) = self.reader_impl.truncate() {
                        error!("Failed to truncate the snapshot, the next re-run may provide incorrect results: {e}");
                        return Err(e);
                    }
                }
                info!("Reached the greater logical time than preserved ({new_time}). Exiting the rewind after reading {} entries", self.entries_read);
                return Ok(Event::Finished);
            }
        }
        self.entries_read += 1;
        Ok(event)
    }
}
