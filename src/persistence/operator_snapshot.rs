// Copyright © 2024 Pathway

use std::fmt::Display;
use std::mem::{swap, take};
use std::str::FromStr;
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use bincode::{deserialize, serialize};
use differential_dataflow::ExchangeData;
use differential_dataflow::{consolidation::consolidate, difference::Semigroup};
use log::error;

use crate::engine::{Timestamp, TotalFrontier};
use crate::persistence::backends::{BackendPutFuture, Error as BackendError, PersistenceBackend};
use crate::persistence::state::FinalizedTimeQuerier;
use crate::persistence::PersistenceTime;

#[allow(clippy::module_name_repetitions)]
pub trait OperatorSnapshotReader<D, R> {
    fn load_persisted(&mut self) -> Result<Vec<(D, R)>, BackendError>;
}

#[allow(clippy::module_name_repetitions)]
pub trait OperatorSnapshotWriter<T, D, R> {
    fn persist(&mut self, time: T, data: Vec<(D, R)>);
    fn persist_single(&mut self, data: (D, R));
    fn close_time(&mut self, time: T);
    fn flush(&mut self, time: TotalFrontier<T>) -> Vec<BackendPutFuture>;
}

#[derive(Debug, Clone, Copy)]
struct ChunkName {
    level: usize,
    time: Timestamp,
    len: usize,
}

impl Display for ChunkName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}-{}", self.level, self.time, self.len)
    }
}

struct ParseChunkNameError;

impl FromStr for ChunkName {
    type Err = ParseChunkNameError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split('-').collect();
        if parts.len() == 3 {
            Ok(ChunkName {
                level: parts[0].parse().map_err(|_e| ParseChunkNameError)?,
                time: parts[1].parse().map_err(|_e| ParseChunkNameError)?,
                len: parts[2].parse().map_err(|_e| ParseChunkNameError)?,
            })
        } else {
            Err(ParseChunkNameError)
        }
    }
}

fn process_chunk_names(keys: Vec<String>) -> Vec<ChunkName> {
    let mut chunk_names = Vec::new();
    for key in keys {
        let Ok(chunk_name) = key.parse() else {
            error!("invalid persistence chunk name: {key}");
            continue;
        };
        chunk_names.push(chunk_name);
    }
    chunk_names
}

struct Chunks {
    current: Vec<ChunkName>,
    too_old: Vec<ChunkName>,
    too_new: Vec<ChunkName>,
}

#[allow(clippy::collapsible_else_if)] // it separates two cases better (0-level and other files)
fn get_chunks(keys: Vec<String>, threshold_time: TotalFrontier<Timestamp>) -> Chunks {
    let chunks = process_chunk_names(keys);
    let mut max_time_per_level = Vec::new();
    let mut current: Vec<ChunkName> = Vec::new();
    let mut too_old: Vec<ChunkName> = Vec::new();
    let mut too_new: Vec<ChunkName> = Vec::new();
    for chunk in &chunks {
        if max_time_per_level.len() <= chunk.level {
            max_time_per_level.resize(chunk.level + 1, Timestamp(0));
        }
        max_time_per_level[chunk.level] =
            std::cmp::max(max_time_per_level[chunk.level], chunk.time);
    }
    let mut max_time = Timestamp(0);
    for i in (0..max_time_per_level.len()).rev() {
        max_time = std::cmp::max(max_time, max_time_per_level[i]);
        max_time_per_level[i] = max_time;
    }
    for chunk in chunks {
        // special handling of level 0 as these are unmerged chunks
        if chunk.level == 0 {
            if TotalFrontier::At(chunk.time) >= threshold_time {
                too_new.push(chunk);
            } else if max_time_per_level
                .get(1)
                .map_or(true, |max_time| chunk.time > *max_time)
            {
                // If max_time_per_level[1] exists it means there are merged chunks.
                // Unmerged chunks are valid if their time > last merged chunk time
                current.push(chunk);
            } else {
                too_old.push(chunk);
            }
        } else {
            if chunk.time < max_time_per_level[chunk.level] {
                too_old.push(chunk);
            } else {
                assert!(chunk.time == max_time_per_level[chunk.level]);
                current.push(chunk);
            }
        }
    }

    Chunks {
        current,
        too_old,
        too_new,
    }
}

fn read_single_chunk<D, R>(
    chunk: ChunkName,
    backend: &dyn PersistenceBackend,
) -> Result<Vec<(D, R)>, BackendError>
where
    D: ExchangeData,
    R: ExchangeData,
{
    let serialized_data = backend.get_value(&chunk.to_string())?;
    deserialize(&serialized_data).map_err(|err| BackendError::Bincode(*err))
}

fn read_chunks<D, R>(
    chunks: &[ChunkName],
    backend: &dyn PersistenceBackend,
) -> Result<Vec<(D, R)>, BackendError>
where
    D: ExchangeData,
    R: ExchangeData,
{
    let mut result = Vec::new();
    for chunk in chunks {
        let mut v = read_single_chunk(*chunk, backend)?;
        if v.len() > result.len() {
            swap(&mut result, &mut v);
        }
        result.append(&mut v);
    }
    //TODO: maybe consolidate here as well - needs benchmarking
    Ok(result)
}

pub struct ConcreteSnapshotReader {
    backend: Box<dyn PersistenceBackend>,
    threshold_time: TotalFrontier<Timestamp>,
}

impl ConcreteSnapshotReader {
    pub fn new(
        backend: Box<dyn PersistenceBackend>,
        threshold_time: TotalFrontier<Timestamp>,
    ) -> Self {
        Self {
            backend,
            threshold_time,
        }
    }
}

impl<D, R> OperatorSnapshotReader<D, R> for ConcreteSnapshotReader
where
    D: ExchangeData,
    R: ExchangeData,
{
    fn load_persisted(&mut self) -> Result<Vec<(D, R)>, BackendError> {
        let keys = self.backend.list_keys()?;
        let chunks = get_chunks(keys, self.threshold_time);
        for chunk in itertools::chain(chunks.too_old.iter(), chunks.too_new.iter()) {
            self.backend.remove_key(&chunk.to_string())?;
        }
        read_chunks(&chunks.current, self.backend.as_ref())
    }
}

pub struct MultiConcreteSnapshotReader {
    snapshot_readers: Vec<ConcreteSnapshotReader>,
    sender: mpsc::Sender<()>,
}

impl MultiConcreteSnapshotReader {
    pub fn new(snapshot_readers: Vec<ConcreteSnapshotReader>, sender: mpsc::Sender<()>) -> Self {
        Self {
            snapshot_readers,
            sender,
        }
    }
}

impl<D, R> OperatorSnapshotReader<D, R> for MultiConcreteSnapshotReader
where
    D: ExchangeData,
    R: ExchangeData + Semigroup,
{
    fn load_persisted(&mut self) -> Result<Vec<(D, R)>, BackendError> {
        let mut result = Vec::new();
        for snapshot_reader in &mut self.snapshot_readers {
            let mut v = snapshot_reader.load_persisted()?;
            if v.len() > result.len() {
                swap(&mut result, &mut v);
            }
            result.append(&mut v);
        }
        consolidate(&mut result);
        self.sender.send(()).expect("merger should exist"); // inform merger that it can start its work
        Ok(result)
    }
}
pub struct ConcreteSnapshotWriter<D, R> {
    backend: Box<dyn PersistenceBackend>,
    single_time_buffer: Vec<(D, R)>,
    futures: Vec<BackendPutFuture>,
    buffer: Vec<(D, R)>,
    max_time: Option<Timestamp>,
    snapshot_interval: Duration,
}

impl<D, R> ConcreteSnapshotWriter<D, R>
where
    D: ExchangeData,
    R: ExchangeData + Semigroup,
{
    pub fn new(backend: Box<dyn PersistenceBackend>, snapshot_interval: Duration) -> Self {
        Self {
            backend,
            single_time_buffer: Vec::new(),
            futures: Vec::new(),
            buffer: Vec::new(),
            max_time: None,
            snapshot_interval,
        }
    }

    fn maybe_save(&mut self, new_time: TotalFrontier<Timestamp>) {
        if let Some(max_time) = self.max_time {
            let should_save = match new_time {
                TotalFrontier::At(new_time) => {
                    !max_time.should_be_saved_together(&new_time, self.snapshot_interval)
                }
                TotalFrontier::Done => true,
            };
            if should_save {
                self.save(max_time);
            }
        }
    }

    fn save(&mut self, time: Timestamp) {
        let mut data = take(&mut self.buffer);
        consolidate(&mut data);
        if data.is_empty() {
            return;
        }
        let chunk_name = ChunkName {
            level: 0,
            time,
            len: data.len(),
        };
        let key = chunk_name.to_string();
        let serialized_data = serialize(&data).expect("entry should be serializable");
        let future = self.backend.put_value(&key, serialized_data);
        self.futures.push(future);
    }
}

impl<D, R> OperatorSnapshotWriter<Timestamp, D, R> for ConcreteSnapshotWriter<D, R>
where
    D: ExchangeData,
    R: ExchangeData + Semigroup,
{
    fn persist(&mut self, time: Timestamp, mut data: Vec<(D, R)>) {
        self.maybe_save(TotalFrontier::At(time));
        assert!(self.max_time.map_or(true, |max_time| max_time <= time));
        self.max_time = Some(time);
        self.buffer.append(&mut data);
    }

    fn persist_single(&mut self, data: (D, R)) {
        self.single_time_buffer.push(data);
    }

    fn close_time(&mut self, time: Timestamp) {
        let buffer = take(&mut self.single_time_buffer);
        self.persist(time, buffer);
    }

    fn flush(&mut self, time: TotalFrontier<Timestamp>) -> Vec<BackendPutFuture> {
        // TODO: this could make sure only entries up to a given time are flushed
        // applies to both operator and input snapshot writers
        if let Some(max_time) = self.max_time {
            if TotalFrontier::At(max_time) < time {
                self.maybe_save(time);
                if let TotalFrontier::At(time) = time {
                    // only update max_time if TotalFrontier has some value
                    // if time is finished, there's no need to update max_time
                    self.max_time = Some(time);
                }
            }
        }
        take(&mut self.futures)
    }
}

pub trait Flushable {
    fn flush(&mut self, time: TotalFrontier<Timestamp>) -> Vec<BackendPutFuture>;
}

impl<D, R> Flushable for ConcreteSnapshotWriter<D, R>
where
    D: ExchangeData,
    R: ExchangeData + Semigroup,
{
    fn flush(&mut self, time: TotalFrontier<Timestamp>) -> Vec<BackendPutFuture> {
        OperatorSnapshotWriter::<Timestamp, D, R>::flush(self, time)
    }
}

const MINIMAL_MERGE_WAIT_TIME: core::time::Duration = core::time::Duration::from_secs(1);

pub struct ConcreteSnapshotMerger {
    finish_sender: mpsc::Sender<()>,
    thread_handle: Option<thread::JoinHandle<()>>,
}

impl ConcreteSnapshotMerger {
    pub fn new<D, R>(
        backend: Box<dyn PersistenceBackend>,
        snapshot_interval: core::time::Duration,
        time_querier: FinalizedTimeQuerier,
        receiver: mpsc::Receiver<()>,
    ) -> Self
    where
        D: ExchangeData,
        R: ExchangeData + Semigroup,
    {
        let (finish_sender, thread_handle) =
            Self::start::<D, R>(backend, snapshot_interval, time_querier, receiver);
        Self {
            finish_sender,
            thread_handle: Some(thread_handle),
        }
    }

    /// It finds chunks that are fully committed - their timestamp
    /// is already saved in persistence metadata for all workers.
    /// If among these chunks are chunks at level 0 (`unmerged_chunks`), it starts merging.
    /// It continues merging with snapshots at increasing levels (there can be at most one snapshot at each level).
    /// Merging stops when 2^level >= total length of merged snapshots.
    /// Then merged snapshots are consolidated and saved as {level}-{max_timestamp_in_data}-{length_of_data}.
    pub fn maybe_merge<D, R>(
        backend: &mut dyn PersistenceBackend,
        time_querier: &mut FinalizedTimeQuerier,
    ) -> Result<(), BackendError>
    where
        D: ExchangeData,
        R: ExchangeData + Semigroup,
    {
        let keys = backend.list_keys()?;
        let threshold_time = time_querier.last_finalized_timestamp()?;
        let chunks = get_chunks(keys, threshold_time);
        // We're still working - only too old chunks can be removed.
        // Too new chunks can't be merged yet and can't be removed.
        for chunk in chunks.too_old {
            backend.remove_key(&chunk.to_string())?;
        }
        let mut chunk_at_level = Vec::new();
        let mut unmerged_chunks = Vec::new();
        let mut max_unmerged_time = Timestamp(0);
        for chunk in chunks.current {
            if chunk.level == 0 {
                unmerged_chunks.push(chunk);
                max_unmerged_time = std::cmp::max(max_unmerged_time, chunk.time);
            } else {
                if chunk.level >= chunk_at_level.len() {
                    chunk_at_level.resize(chunk.level + 1, None);
                }
                assert!(chunk_at_level[chunk.level].is_none()); // can't have more than one chunk at a given level
                chunk_at_level[chunk.level] = Some(chunk);
            }
        }
        if unmerged_chunks.is_empty() {
            return Ok(());
        }
        let mut buffer = read_chunks::<D, R>(&unmerged_chunks, backend)?;
        let mut max_allowed_size = 2;
        let mut level = 1;
        while level < chunk_at_level.len() {
            if let Some(chunk) = chunk_at_level[level] {
                let mut v = read_single_chunk::<D, R>(chunk, backend)?;
                if v.len() > buffer.len() {
                    swap(&mut buffer, &mut v);
                }
                buffer.append(&mut v);
            }
            if buffer.len() <= max_allowed_size {
                break;
            }
            max_allowed_size *= 2;
            level += 1;
        }
        consolidate(&mut buffer);
        // loop in case we have no bigger chunks yet
        while buffer.len() > max_allowed_size {
            max_allowed_size *= 2;
            level += 1;
        }
        let chunk = ChunkName {
            level,
            time: max_unmerged_time,
            len: buffer.len(),
        };
        let serialized_data = serialize(&buffer).expect("entry should be serializable");
        let future = backend.put_value(&chunk.to_string(), serialized_data);
        // Can't start new round if future not finished.
        futures::executor::block_on(future).expect("unexpected future cancelling")
    }

    fn run<D, R>(
        mut backend: Box<dyn PersistenceBackend>,
        receiver: &mpsc::Receiver<()>,
        timeout: core::time::Duration,
        time_querier: &mut FinalizedTimeQuerier,
        reader_finished_receiver: &mpsc::Receiver<()>,
    ) where
        D: ExchangeData,
        R: ExchangeData + Semigroup,
    {
        if reader_finished_receiver.recv().is_err() {
            error!("Can't start snapshot merger as snapshot reader didn't finish gracefully");
        }
        let mut next_try_at = Instant::now();
        loop {
            let now = Instant::now();
            let duration = next_try_at
                .checked_duration_since(now)
                .unwrap_or(Duration::ZERO);
            next_try_at = now
                .checked_add(timeout)
                .expect("now with added timeout should fit into Instant");
            match receiver.recv_timeout(duration) {
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    if let Err(e) = Self::maybe_merge::<D, R>(backend.as_mut(), time_querier) {
                        error!("Error while trying to merge persisted data: {e}");
                    }
                }
                Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => break,
            }
        }
    }

    fn start<D, R>(
        backend: Box<dyn PersistenceBackend>,
        timeout: core::time::Duration,
        mut time_querier: FinalizedTimeQuerier,
        reader_finished_receiver: mpsc::Receiver<()>,
    ) -> (mpsc::Sender<()>, thread::JoinHandle<()>)
    where
        D: ExchangeData,
        R: ExchangeData + Semigroup,
    {
        let timeout = std::cmp::max(timeout, MINIMAL_MERGE_WAIT_TIME);
        let (sender, receiver) = mpsc::channel();
        let thread_handle = thread::Builder::new()
            .name("SnapshotMerger".to_string()) // TODO maybe better name
            .spawn(move || {
                Self::run::<D, R>(
                    backend,
                    &receiver,
                    timeout,
                    &mut time_querier,
                    &reader_finished_receiver,
                );
            })
            .expect("persistence read thread creation should succeed");
        (sender, thread_handle)
    }
}

impl Drop for ConcreteSnapshotMerger {
    fn drop(&mut self) {
        self.finish_sender.send(()).unwrap();
        if let Some(thread_handle) = take(&mut self.thread_handle) {
            if let Err(e) = thread_handle.join() {
                error!("Failed to join snapshot merger thread: {e:?}");
            }
        }
    }
}
