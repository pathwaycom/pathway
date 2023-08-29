use log::error;
use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::{BufReader, BufWriter, ErrorKind as IoErrorKind, Seek, Write};
use std::path::Path;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use bincode::{deserialize_from, serialize_into, ErrorKind as BincodeError};
use serde::{Deserialize, Serialize};

use crate::connectors::data_storage::{ReadError, WriteError};
use crate::engine::{Key, Value};
use crate::fs_helpers::ensure_directory;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Event {
    Insert(Key, Vec<Value>),
    Remove(Key, Vec<Value>),
    AdvanceTime(u64),
    Finished,
}

#[allow(clippy::module_name_repetitions)]
pub trait SnapshotReader {
    fn read(&mut self) -> Result<Event, ReadError>;

    /// This method will be called after the snapshot is read until the last entry, which can be
    /// processed.
    ///
    /// It must ensure that no further data is present in the snapshot so that when it gets appended,
    /// the unused data is not written next to the non-processed tail.
    fn truncate(&mut self) -> Result<(), ReadError>;
}

#[allow(clippy::module_name_repetitions)]
pub trait SnapshotWriter {
    fn write(&mut self, event: &Event) -> Result<(), WriteError>;

    fn flush(&mut self) -> Result<(), WriteError> {
        Ok(())
    }
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

impl SnapshotReader for LocalBinarySnapshotReader {
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

    fn flush(&mut self) -> Result<(), WriteError> {
        if let Some(writer) = &mut self.lazy_writer {
            writer.flush()?;
        }
        Ok(())
    }
}
