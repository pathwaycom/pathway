use log::{error, info};
use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::{BufReader, BufWriter, ErrorKind as IoErrorKind, Seek, Write};
use std::path::Path;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use bincode::{deserialize_from, serialize_into, ErrorKind as BincodeError};
use futures::channel::oneshot;
use futures::channel::oneshot::Receiver as OneShotReceiver;
use serde::{Deserialize, Serialize};

use crate::connectors::data_storage::{ReadError, WriteError};
use crate::engine::{Key, Value};
use crate::fs_helpers::ensure_directory;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Event {
    Insert(Key, Vec<Value>),
    Delete(Key, Vec<Value>),
    AdvanceTime(u64),
    Finished,
}

#[allow(clippy::module_name_repetitions)]
pub trait SnapshotReaderImpl {
    /// This method will be called every so often to read the persisted snapshot.
    /// When there are no entries left, it must return `Event::Finished`.
    fn read(&mut self) -> Result<Event, ReadError>;

    /// This method will be called after the snapshot is read until the last entry, which can be
    /// processed.
    ///
    /// It must ensure that no further data is present in the snapshot so that when it gets appended,
    /// the unused data is not written next to the non-processed tail.
    fn truncate(&mut self) -> Result<(), ReadError>;
}

#[allow(clippy::module_name_repetitions)]
pub type SnapshotWriterFlushFuture = OneShotReceiver<Result<(), WriteError>>;

#[allow(clippy::module_name_repetitions)]
pub trait SnapshotWriter: Send {
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
    times_advanced: Vec<u64>,
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

impl SnapshotReaderImpl for LocalBinarySnapshotReader {
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
            let file = OpenOptions::new().append(true).open(file_path)?;
            file.set_len(stable_position)?;
        }

        for unreachable_part in &self.times_advanced[self.next_file_idx..] {
            let snapshot_file_to_remove =
                Path::new(&self.root_path).join(format!("{unreachable_part}"));
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

impl SnapshotWriter for LocalBinarySnapshotWriter {
    fn write(&mut self, event: &Event) -> Result<(), WriteError> {
        let writer = {
            if let Some(lazy_writer) = &mut self.lazy_writer {
                lazy_writer
            } else {
                let current_timestamp = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("Failed to get the current timestamp.")
                    .as_millis();
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

#[allow(clippy::module_name_repetitions)]
pub struct SnapshotReader {
    reader_impl: Box<dyn SnapshotReaderImpl>,
    threshold_time: u64,
    entries_read: usize,
}

impl SnapshotReader {
    pub fn new(
        mut reader_impl: Box<dyn SnapshotReaderImpl>,
        threshold_time: u64,
    ) -> Result<Self, ReadError> {
        if threshold_time == 0 {
            info!("No time has been advanced in the previous run, therefore no data read from the snapshot");
            if let Err(e) = reader_impl.truncate() {
                error!("Failed to truncate the snapshot, the next re-run may provide incorrect results: {e}");
                return Err(e);
            }
        }

        Ok(Self {
            reader_impl,
            threshold_time,
            entries_read: 0,
        })
    }

    pub fn read(&mut self) -> Result<Event, ReadError> {
        let event = self.reader_impl.read()?;
        if let Event::AdvanceTime(new_time) = event {
            if new_time > self.threshold_time {
                if let Err(e) = self.reader_impl.truncate() {
                    error!("Failed to truncate the snapshot, the next re-run may provide incorrect results: {e}");
                    return Err(e);
                }
                info!("Reached the greater logical time than preserved ({new_time}). Exiting the rewind after reading {} entries", self.entries_read);
                return Ok(Event::Finished);
            }
        }
        self.entries_read += 1;
        Ok(event)
    }
}
