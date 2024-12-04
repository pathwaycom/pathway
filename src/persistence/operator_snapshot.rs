// Copyright © 2024 Pathway

use std::fmt::Display;
use std::mem::{swap, take};
use std::str::FromStr;
use std::time::Duration;

use bincode::{deserialize, serialize};
use differential_dataflow::ExchangeData;
use differential_dataflow::{consolidation::consolidate, difference::Semigroup};
use log::error;

use crate::engine::{Timestamp, TotalFrontier};
use crate::persistence::backends::{BackendPutFuture, Error as BackendError, PersistenceBackend};
use crate::persistence::PersistenceTime;

#[allow(clippy::module_name_repetitions)]
pub trait OperatorSnapshotReader<D, R> {
    fn load_persisted(&self) -> Result<Vec<(D, R)>, BackendError>;
}

#[allow(clippy::module_name_repetitions)]
pub trait OperatorSnapshotWriter<T, D, R> {
    fn persist(&mut self, time: T, data: Vec<(D, R)>);
    fn persist_single(&mut self, data: (D, R));
    fn close_time(&mut self, time: T);
    fn flush(&mut self, time: TotalFrontier<T>) -> Vec<BackendPutFuture>;
}

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
    current: Vec<String>,
    too_old: Vec<String>,
    too_new: Vec<String>,
}

#[allow(clippy::collapsible_else_if)] // it separates two cases better (0-level and other files)
fn get_chunks(keys: Vec<String>, threshold_time: TotalFrontier<Timestamp>) -> Chunks {
    let chunks = process_chunk_names(keys);
    let mut max_time_per_level = Vec::new();
    let mut current: Vec<String> = Vec::new();
    let mut too_old: Vec<String> = Vec::new();
    let mut too_new: Vec<String> = Vec::new();
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
                too_new.push(chunk.to_string());
            } else if max_time_per_level
                .get(1)
                .map_or(true, |max_time| chunk.time > *max_time)
            {
                // If max_time_per_level[1] exists it means there are merged chunks.
                // Unmerged chunks are valid if their time > last merged chunk time
                current.push(chunk.to_string());
            } else {
                too_old.push(chunk.to_string());
            }
        } else {
            if chunk.time < max_time_per_level[chunk.level] {
                too_old.push(chunk.to_string());
            } else {
                assert!(chunk.time == max_time_per_level[chunk.level]);
                current.push(chunk.to_string());
            }
        }
    }

    Chunks {
        current,
        too_old,
        too_new,
    }
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
    fn load_persisted(&self) -> Result<Vec<(D, R)>, BackendError> {
        let keys = self.backend.list_keys()?;
        let chunks = get_chunks(keys, self.threshold_time);
        for chunk in itertools::chain(chunks.too_old.iter(), chunks.too_new.iter()) {
            self.backend.remove_key(chunk)?;
        }
        let mut result = Vec::new();
        for chunk in &chunks.current {
            let serialized_data = self.backend.get_value(chunk)?;
            let mut v: Vec<(D, R)> =
                deserialize(&serialized_data).map_err(|err| BackendError::Bincode(*err))?;
            if v.len() > result.len() {
                swap(&mut result, &mut v);
            }
            result.append(&mut v);
        }
        //TODO: maybe consolidate here as well - needs benchmarking
        Ok(result)
    }
}

pub struct MultiConcreteSnapshotReader {
    snapshot_readers: Vec<ConcreteSnapshotReader>,
}

impl MultiConcreteSnapshotReader {
    pub fn new(snapshot_readers: Vec<ConcreteSnapshotReader>) -> Self {
        Self { snapshot_readers }
    }
}

impl<D, R> OperatorSnapshotReader<D, R> for MultiConcreteSnapshotReader
where
    D: ExchangeData,
    R: ExchangeData + Semigroup,
{
    fn load_persisted(&self) -> Result<Vec<(D, R)>, BackendError> {
        let mut result = Vec::new();
        for snapshot_reader in &self.snapshot_readers {
            let mut v = snapshot_reader.load_persisted()?;
            if v.len() > result.len() {
                swap(&mut result, &mut v);
            }
            result.append(&mut v);
        }
        consolidate(&mut result);
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
