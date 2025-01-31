use log::{error, info};
use std::io::{BufReader, Cursor, ErrorKind as IoErrorKind, Read, Seek, SeekFrom};
use std::mem::take;

use bincode::{deserialize_from, serialize, ErrorKind as BincodeError};
use serde::{Deserialize, Serialize};

use crate::engine::{Key, Timestamp, TotalFrontier, Value};
use crate::persistence::backends::{BackendPutFuture, PersistenceBackend};
use crate::persistence::frontier::OffsetAntichain;
use crate::persistence::Error;

const MAX_ENTRIES_PER_CHUNK: usize = 100_000;
const MAX_CHUNK_LENGTH: usize = 10_000_000;

type ChunkId = u64;

fn get_chunk_ids_with_backend(backend: &dyn PersistenceBackend) -> Result<Vec<ChunkId>, Error> {
    let mut chunk_ids = Vec::new();
    let chunk_keys = backend.list_keys()?;
    for chunk_key in chunk_keys {
        if let Ok(chunk_id) = chunk_key.parse() {
            chunk_ids.push(chunk_id);
        } else {
            error!("Unparsable chunk id: {chunk_key}");
        }
    }
    Ok(chunk_ids)
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum Event {
    Insert(Key, Vec<Value>),
    Delete(Key, Vec<Value>),
    AdvanceTime(Timestamp, OffsetAntichain),
    Finished,
}

#[derive(Debug, Clone, Copy)]
pub enum SnapshotMode {
    Full,
    OffsetsOnly,
}

impl SnapshotMode {
    pub fn is_event_included(self, event: &Event) -> bool {
        match (self, event) {
            (SnapshotMode::Full, _) | (SnapshotMode::OffsetsOnly, Event::AdvanceTime(_, _)) => true,
            (SnapshotMode::OffsetsOnly, _) => false,
        }
    }
}

#[allow(clippy::module_name_repetitions)]
pub trait ReadInputSnapshot {
    /// This method will be called every so often to read the persisted snapshot.
    /// When there are no entries left, it must return `Event::Finished`.
    fn read(&mut self) -> Result<Event, Error>;

    /// This method will be called when the read is done.
    /// It must return the most actual frontier corresponding to the data read.
    fn last_frontier(&self) -> &OffsetAntichain;
}

#[allow(clippy::module_name_repetitions)]
pub struct InputSnapshotReader {
    backend: Box<dyn PersistenceBackend>,
    threshold_time: TotalFrontier<Timestamp>,
    truncate_at_end: bool,

    reader: Option<BufReader<Cursor<Vec<u8>>>>,
    last_frontier: OffsetAntichain,
    chunk_ids: Vec<ChunkId>,
    next_chunk_idx: usize,
    entries_read: usize,
}

impl ReadInputSnapshot for InputSnapshotReader {
    fn read(&mut self) -> Result<Event, Error> {
        let event = self.next_event()?;
        if matches!(event, Event::Finished) {
            return Ok(event);
        }
        if let Event::AdvanceTime(new_time, ref frontier) = event {
            let read_finished = TotalFrontier::At(new_time) >= self.threshold_time;
            self.last_frontier = frontier.clone();
            if read_finished {
                if self.truncate_at_end {
                    if let Err(e) = self.truncate() {
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

    fn last_frontier(&self) -> &OffsetAntichain {
        &self.last_frontier
    }
}

impl InputSnapshotReader {
    pub fn new(
        backend: Box<dyn PersistenceBackend>,
        threshold_time: TotalFrontier<Timestamp>,
        truncate_at_end: bool,
    ) -> Result<Self, Error> {
        let mut chunk_ids = get_chunk_ids_with_backend(backend.as_ref())?;
        chunk_ids.sort_unstable();
        Ok(Self {
            backend,
            threshold_time,
            truncate_at_end,
            reader: None,
            last_frontier: OffsetAntichain::new(),
            chunk_ids,
            next_chunk_idx: 0,
            entries_read: 0,
        })
    }

    fn truncate(&mut self) -> Result<(), Error> {
        if let Some(ref mut reader) = &mut self.reader {
            let current_chunk_key = format!("{}", self.chunk_ids[self.next_chunk_idx - 1]);
            let stable_position = reader.stream_position()?;
            info!("Truncate: Shrink {current_chunk_key:?} to {stable_position} bytes");

            let mut stable_part = vec![0_u8; stable_position.try_into().unwrap()];
            reader.seek(SeekFrom::Start(0))?;
            reader.read_exact(stable_part.as_mut_slice())?;
            futures::executor::block_on(async {
                self.backend
                    .put_value(&current_chunk_key, stable_part)
                    .await
                    .expect("unexpected future cancelling")
            })?;
        }

        for unreachable_part in &self.chunk_ids[self.next_chunk_idx..] {
            info!("Truncate: Remove {unreachable_part:?}");
            self.backend.remove_key(&format!("{unreachable_part}"))?;
        }
        Ok(())
    }

    fn next_event(&mut self) -> Result<Event, Error> {
        loop {
            if let Some(reader) = &mut self.reader {
                match deserialize_from(reader) {
                    Ok(entry) => return Ok(entry),
                    Err(e) => match *e {
                        BincodeError::Io(e) => {
                            if !matches!(e.kind(), IoErrorKind::UnexpectedEof) {
                                return Err(Error::Io(e));
                            }
                            self.reader = None;
                            continue;
                        }
                        _ => return Err(Error::Bincode(*e)),
                    },
                }
            }
            if self.next_chunk_idx >= self.chunk_ids.len() {
                break;
            }
            let next_chunk_key = format!("{}", self.chunk_ids[self.next_chunk_idx]);
            info!(
                "Snapshot reader proceeds to the chunk {next_chunk_key} after {} snapshot entries",
                self.entries_read
            );
            let contents = self.backend.get_value(&next_chunk_key)?;
            let cursor = Cursor::new(contents);
            self.reader = Some(BufReader::new(cursor));
            self.next_chunk_idx += 1;
        }
        Ok(Event::Finished)
    }
}

pub struct MockSnapshotReader {
    events: Box<dyn Iterator<Item = Event>>,
    last_frontier: OffsetAntichain,
}

impl MockSnapshotReader {
    pub fn new(events: Vec<Event>) -> Self {
        Self {
            events: Box::new(events.into_iter()),
            last_frontier: OffsetAntichain::new(),
        }
    }
}

impl ReadInputSnapshot for MockSnapshotReader {
    fn read(&mut self) -> Result<Event, Error> {
        if let Some(event) = self.events.next() {
            if let Event::AdvanceTime(_, ref frontier) = event {
                self.last_frontier = frontier.clone();
            }
            Ok(event)
        } else {
            Ok(Event::Finished)
        }
    }

    fn last_frontier(&self) -> &OffsetAntichain {
        &self.last_frontier
    }
}

#[allow(clippy::module_name_repetitions)]
pub struct InputSnapshotWriter {
    backend: Box<dyn PersistenceBackend>,
    mode: SnapshotMode,
    current_chunk: Vec<u8>,
    current_chunk_entries: usize,
    chunk_save_futures: Vec<BackendPutFuture>,
    next_chunk_id: ChunkId,
}

impl InputSnapshotWriter {
    pub fn new(backend: Box<dyn PersistenceBackend>, mode: SnapshotMode) -> Result<Self, Error> {
        let chunk_keys = get_chunk_ids_with_backend(backend.as_ref())?;
        Ok(Self {
            backend,
            mode,
            current_chunk: Vec::new(),
            current_chunk_entries: 0,
            chunk_save_futures: Vec::new(),
            next_chunk_id: chunk_keys.iter().max().copied().unwrap_or_default() + 1,
        })
    }

    /// A non-blocking call, pushing an entry in the buffer.
    /// The buffer should not be flushed in the same thread.
    pub fn write(&mut self, event: &Event) {
        if !self.mode.is_event_included(event) {
            return;
        }

        let mut entry_serialized = serialize(&event).expect("unable to serialize an entry");
        self.current_chunk.append(&mut entry_serialized);
        self.current_chunk_entries += 1;

        let is_flush_needed = self.current_chunk_entries >= MAX_ENTRIES_PER_CHUNK
            || self.current_chunk.len() >= MAX_CHUNK_LENGTH;
        if is_flush_needed {
            let chunk_save_future = self.save_current_chunk();
            self.chunk_save_futures.push(chunk_save_future);
        }
    }

    /// Flush the entries which are currently present in the buffer.
    /// The result returned must be waited and return an `Ok()` when the data is uploaded.
    ///
    /// We use `futures::channel::oneshot::channel` here instead of Future/Promise
    /// because it uses modern Rust Futures that are also used by `async`.
    pub fn flush(&mut self) -> Vec<BackendPutFuture> {
        if !self.current_chunk.is_empty() {
            let chunk_save_future = self.save_current_chunk();
            self.chunk_save_futures.push(chunk_save_future);
        }
        take(&mut self.chunk_save_futures)
    }

    fn save_current_chunk(&mut self) -> BackendPutFuture {
        info!(
            "Persisting a chunk of {} entries ({} bytes)",
            self.current_chunk_entries,
            self.current_chunk.len()
        );
        let chunk_name = format!("{}", self.next_chunk_id);
        self.next_chunk_id += 1;
        self.current_chunk_entries = 0;
        self.backend
            .put_value(&chunk_name, take(&mut self.current_chunk))
    }
}
