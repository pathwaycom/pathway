use log::error;
use std::fs;
use std::fs::File;
use std::io::{BufReader, BufWriter, ErrorKind as IoErrorKind, Write};
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
    pub fn new(path: &Path) -> Result<LocalBinarySnapshotReader, ReadError> {
        let mut times_advanced = Vec::new();
        let entries = fs::read_dir(path).map_err(ReadError::Io)?;
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
            root_path: path.into(),
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
}

pub struct LocalBinarySnapshotWriter {
    writer: BufWriter<std::fs::File>,
}

impl LocalBinarySnapshotWriter {
    pub fn new(path: &Path) -> Result<LocalBinarySnapshotWriter, WriteError> {
        ensure_directory(path)?;

        let current_timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Failed to get the current timestamp.")
            .as_millis();

        Ok(Self {
            writer: BufWriter::new(File::create(path.join(format!("{current_timestamp}")))?),
        })
    }
}

impl SnapshotWriter for LocalBinarySnapshotWriter {
    fn write(&mut self, event: &Event) -> Result<(), WriteError> {
        serialize_into(&mut self.writer, &event).map_err(|e| match *e {
            BincodeError::Io(io_error) => WriteError::Io(*Box::new(io_error)),
            _ => WriteError::Bincode(*e),
        })
    }

    fn flush(&mut self) -> Result<(), WriteError> {
        self.writer.flush()?;
        Ok(())
    }
}
