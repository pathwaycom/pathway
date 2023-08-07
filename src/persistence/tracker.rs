use itertools::Itertools;
use log::{error, info, warn};
use std::collections::HashMap;
use std::fs;
use std::io::Error as IoError;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use crate::connectors::data_storage::{ReadError, StorageType, WriteError};
use crate::connectors::snapshot::{
    LocalBinarySnapshotReader, LocalBinarySnapshotWriter, SnapshotReader, SnapshotWriter,
};
use crate::fs_helpers::ensure_directory;
use crate::persistence::frontier::OffsetAntichain;
use crate::persistence::storage::Storage as MetadataStorage;
use crate::persistence::PersistentId;

pub trait PersistencyManager {
    fn register_input_source(
        &mut self,
        persistent_id: PersistentId,
        storage_type: &StorageType,
        source: Arc<Mutex<HashMap<u64, OffsetAntichain>>>,
    );
    fn register_sink(&mut self) -> usize;
    fn accept_finalized_timestamp(&mut self, sink_id: usize, timestamp: Option<u64>);
    fn frontier_for(&self, persistent_id: PersistentId) -> OffsetAntichain;

    fn create_snapshot_readers(
        &self,
        persistent_id: u128,
    ) -> Result<Vec<Box<dyn SnapshotReader>>, ReadError>;

    fn create_snapshot_writer(
        &mut self,
        persistent_id: u128,
    ) -> Result<Box<dyn SnapshotWriter>, WriteError>;
}

pub enum StreamStorageConfig {
    Filesystem(PathBuf),
}

pub struct PersistenceManagerConfig {
    stream_storage: StreamStorageConfig,
    worker_id: usize,
    total_workers: usize,
}

impl PersistenceManagerConfig {
    pub fn new(
        stream_storage: StreamStorageConfig,
        worker_id: usize,
        total_workers: usize,
    ) -> Self {
        Self {
            stream_storage,
            worker_id,
            total_workers,
        }
    }

    pub fn create_snapshot_readers(
        &self,
        persistent_id: u128,
    ) -> Result<Vec<Box<dyn SnapshotReader>>, ReadError> {
        match &self.stream_storage {
            StreamStorageConfig::Filesystem(root_path) => {
                let mut result: Vec<Box<dyn SnapshotReader>> = Vec::new();
                for path in self.assigned_snapshot_paths(root_path, persistent_id)? {
                    result.push(Box::new(LocalBinarySnapshotReader::new(&path)?));
                }
                Ok(result)
            }
        }
    }

    pub fn create_snapshot_writer(
        &mut self,
        persistent_id: u128,
    ) -> Result<Box<dyn SnapshotWriter>, WriteError> {
        match &self.stream_storage {
            StreamStorageConfig::Filesystem(root_path) => {
                Ok(Box::new(LocalBinarySnapshotWriter::new(
                    &self.snapshot_writer_path(root_path, persistent_id)?,
                )?))
            }
        }
    }

    fn snapshot_writer_path(
        &self,
        root_path: &Path,
        persistent_id: u128,
    ) -> Result<PathBuf, IoError> {
        ensure_directory(root_path)?;
        ensure_directory(&root_path.join("streams"))?;
        ensure_directory(&root_path.join(format!("streams/{}", self.worker_id)))?;
        Ok(root_path.join(format!("streams/{}/{persistent_id}/", self.worker_id)))
    }

    fn assigned_snapshot_paths(
        &self,
        root_path: &Path,
        persistent_id: u128,
    ) -> Result<Vec<PathBuf>, IoError> {
        ensure_directory(root_path)?;

        let streams_dir = root_path.join("streams");
        ensure_directory(&streams_dir)?;

        let mut assigned_paths = Vec::new();
        let paths = fs::read_dir(&streams_dir)?;
        for entry in paths.flatten() {
            let file_type = entry.file_type()?;
            if file_type.is_dir() {
                let dir_name = if let Some(name) = entry.file_name().to_str() {
                    name.to_string()
                } else {
                    error!("Failed to parse block folder name: {entry:?}");
                    continue;
                };

                let earlier_worker_id: usize = match dir_name.parse() {
                    Ok(worker_id) => worker_id,
                    Err(e) => {
                        error!("Could not parse worker id from snapshot directory {entry:?}. Error details: {e}");
                        continue;
                    }
                };

                if earlier_worker_id % self.total_workers != self.worker_id {
                    continue;
                }
                if earlier_worker_id != self.worker_id {
                    info!("Assigning snapshot from the former worker {earlier_worker_id} to worker {} due to reduced number of worker threads", self.worker_id);
                }

                let snapshot_path =
                    streams_dir.join(format!("{earlier_worker_id}/{persistent_id}"));
                assigned_paths.push(snapshot_path);
            } else {
                warn!("Unexpected object in snapshot directory: {entry:?}");
            }
        }

        Ok(assigned_paths)
    }
}

type FrontierByTimeForInputSources = Vec<(PersistentId, Arc<Mutex<HashMap<u64, OffsetAntichain>>>)>;
pub struct SimplePersistencyManager {
    metadata_storage: Box<dyn MetadataStorage>,
    config: PersistenceManagerConfig,
    sink_threshold_times: Vec<Option<u64>>,
    input_sources: FrontierByTimeForInputSources,
}

impl SimplePersistencyManager {
    pub fn new(
        metadata_storage: Box<dyn MetadataStorage>,
        config: PersistenceManagerConfig,
    ) -> SimplePersistencyManager {
        Self {
            metadata_storage,
            config,
            input_sources: Vec::new(),
            sink_threshold_times: Vec::new(),
        }
    }

    pub fn last_finalized_timestamp(&self) -> u64 {
        self.metadata_storage.last_advanced_timestamp()
    }
}

impl PersistencyManager for SimplePersistencyManager {
    fn register_input_source(
        &mut self,
        persistent_id: PersistentId,
        storage_type: &StorageType,
        source: Arc<Mutex<HashMap<u64, OffsetAntichain>>>,
    ) {
        assert!(
            !self
                .input_sources
                .iter()
                .map(|(x, _y)| x)
                .contains(&persistent_id),
            "Same persistent_id belongs to more than one data source: {persistent_id}"
        );
        self.input_sources.push((persistent_id, source));
        self.metadata_storage
            .register_input_source(persistent_id, storage_type);
    }

    fn frontier_for(&self, persistent_id: PersistentId) -> OffsetAntichain {
        self.metadata_storage.frontier_for(persistent_id)
    }

    fn register_sink(&mut self) -> usize {
        self.sink_threshold_times.push(Some(0));
        self.sink_threshold_times.len() - 1
    }

    fn accept_finalized_timestamp(&mut self, sink_id: usize, reported_timestamp: Option<u64>) {
        /*
            This method updates the frontiers of the data sources to the further
            ones, corresponding to the newly closed timestamp. It happens when all data
            for the particular timestamp has been outputted in full.

            When the program is restarted, it results in not outputting this data again.
        */

        self.sink_threshold_times[sink_id] = reported_timestamp;
        let finalized_timestamp = self.globally_finalized_timestamp();
        if finalized_timestamp < self.last_finalized_timestamp() {
            error!("Time isn't in the increasing order. Got advancement to {finalized_timestamp} while last advanced timestamp was {}", self.last_finalized_timestamp());
            return;
        }

        for (persistent_id, input_source) in &mut self.input_sources {
            let mut current_advancement = OffsetAntichain::new();

            /*
                Construct a new frontier, which consists of the offsets corresponding to the
                data, which won't produce any new output.

                NB: the frontier construction should be done in increasing order of offset times
                because they aren't compared and therefore the order should be enforced
                at this end.
            */
            {
                let mut frontiers_by_time = input_source.lock().unwrap();
                let mut timestamps_to_flush = Vec::new();

                for (timestamp, _) in frontiers_by_time.iter() {
                    if *timestamp <= finalized_timestamp {
                        timestamps_to_flush.push(*timestamp);
                    }
                }
                timestamps_to_flush.sort_unstable();

                for timestamp in timestamps_to_flush {
                    let frontier = frontiers_by_time.remove(&timestamp).expect(
                        "Inconsistency: offsets for {timestamp} disappeared between two passes",
                    );
                    for (antichain_item_key, antichain_item_value) in frontier {
                        current_advancement
                            .advance_offset(antichain_item_key, antichain_item_value);
                    }
                }
            }

            /*
                Now the frontier with the advancement is constructed. Below it is saved into
                a metadata storage.

                NB: the mutex for offsets storage is now released.
            */
            for (antichain_item_key, antichain_item_value) in &current_advancement {
                self.metadata_storage.save_offset(
                    *persistent_id,
                    antichain_item_key,
                    antichain_item_value,
                );
            }
        }
        self.metadata_storage
            .accept_finalized_timestamp(finalized_timestamp);
    }

    fn create_snapshot_readers(
        &self,
        persistent_id: u128,
    ) -> Result<Vec<Box<dyn SnapshotReader>>, ReadError> {
        self.config.create_snapshot_readers(persistent_id)
    }

    fn create_snapshot_writer(
        &mut self,
        persistent_id: u128,
    ) -> Result<Box<dyn SnapshotWriter>, WriteError> {
        self.config.create_snapshot_writer(persistent_id)
    }
}

impl SimplePersistencyManager {
    pub fn globally_finalized_timestamp(&self) -> u64 {
        let min_sink_timestamp = self.sink_threshold_times.iter().flatten().min();
        if let Some(min_sink_timestamp) = min_sink_timestamp {
            *min_sink_timestamp
        } else {
            let mut last_timestamp = self.last_finalized_timestamp();
            for (_, input_source) in &self.input_sources {
                let local_last_timestamp = *input_source
                    .lock()
                    .unwrap()
                    .keys()
                    .max()
                    .unwrap_or(&last_timestamp);
                if last_timestamp < local_last_timestamp {
                    last_timestamp = local_last_timestamp;
                }
            }
            last_timestamp
        }
    }
}
