use log::error;
use std::collections::HashMap;
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
    fn accept_finalized_timestamp(&mut self, timestamp: u64);
    fn on_output_finished(&mut self);
    fn frontier_for(&self, persistent_id: PersistentId) -> OffsetAntichain;

    fn create_snapshot_reader(
        &self,
        persistent_id: u128,
    ) -> Result<Box<dyn SnapshotReader>, ReadError>;

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
}

impl PersistenceManagerConfig {
    pub fn new(stream_storage: StreamStorageConfig, worker_id: usize) -> Self {
        Self {
            stream_storage,
            worker_id,
        }
    }

    pub fn create_snapshot_reader(
        &self,
        persistent_id: u128,
    ) -> Result<Box<dyn SnapshotReader>, ReadError> {
        match &self.stream_storage {
            StreamStorageConfig::Filesystem(root_path) => {
                Ok(Box::new(LocalBinarySnapshotReader::new(
                    &self.snapshot_root_path(root_path, persistent_id)?,
                )?))
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
                    &self.snapshot_root_path(root_path, persistent_id)?,
                )?))
            }
        }
    }

    fn snapshot_root_path(
        &self,
        root_path: &Path,
        persistent_id: u128,
    ) -> Result<PathBuf, IoError> {
        ensure_directory(root_path)?;
        ensure_directory(&root_path.join("streams"))?;
        ensure_directory(&root_path.join(format!("streams/{}", self.worker_id)))?;

        Ok(root_path.join(format!("streams/{}/{persistent_id}/", self.worker_id)))
    }
}

type FrontierByTimeForInputSources = Vec<(PersistentId, Arc<Mutex<HashMap<u64, OffsetAntichain>>>)>;
pub struct SimplePersistencyManager {
    metadata_storage: Box<dyn MetadataStorage>,
    config: PersistenceManagerConfig,

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
        self.input_sources.push((persistent_id, source));
        self.metadata_storage
            .register_input_source(persistent_id, storage_type);
    }

    fn frontier_for(&self, persistent_id: PersistentId) -> OffsetAntichain {
        self.metadata_storage.frontier_for(persistent_id)
    }

    fn accept_finalized_timestamp(&mut self, finalized_timestamp: u64) {
        /*
            This method updates the frontiers of the data sources to the further
            ones, corresponding to the newly closed timestamp. It happens when all data
            for the particular timestamp has been outputted in full.

            When the program is restarted, it results in not outputting this data again.
        */
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

    fn on_output_finished(&mut self) {
        let mut last_timestamp = self.last_finalized_timestamp();
        for (_, input_source) in &self.input_sources {
            for (seen_timestamp, _) in input_source.lock().unwrap().iter() {
                if last_timestamp < *seen_timestamp {
                    last_timestamp = *seen_timestamp;
                }
            }
        }
        self.accept_finalized_timestamp(last_timestamp);
    }

    fn create_snapshot_reader(
        &self,
        persistent_id: u128,
    ) -> Result<Box<dyn SnapshotReader>, ReadError> {
        self.config.create_snapshot_reader(persistent_id)
    }

    fn create_snapshot_writer(
        &mut self,
        persistent_id: u128,
    ) -> Result<Box<dyn SnapshotWriter>, WriteError> {
        self.config.create_snapshot_writer(persistent_id)
    }
}
