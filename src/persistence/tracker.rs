use itertools::Itertools;
use log::error;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::connectors::data_storage::{ReadError, StorageType, WriteError};
use crate::connectors::snapshot::{SnapshotReader, SnapshotWriter};
use crate::persistence::config::PersistenceManagerConfig;
use crate::persistence::frontier::OffsetAntichain;
use crate::persistence::metadata_backends::Error as MetadataBackendError;
use crate::persistence::state::MetadataAccessor;
use crate::persistence::PersistentId;

pub trait PersistenceManager {
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
        persistent_id: PersistentId,
    ) -> Result<Vec<Box<dyn SnapshotReader>>, ReadError>;

    fn create_snapshot_writer(
        &mut self,
        persistent_id: PersistentId,
    ) -> Result<Box<dyn SnapshotWriter>, WriteError>;
}

type FrontierByTimeForInputSources = Vec<(PersistentId, Arc<Mutex<HashMap<u64, OffsetAntichain>>>)>;
pub struct SimplePersistenceManager {
    metadata_storage: MetadataAccessor,
    config: PersistenceManagerConfig,

    sink_threshold_times: Vec<Option<u64>>,
    input_sources: FrontierByTimeForInputSources,
}

impl SimplePersistenceManager {
    pub fn new(config: PersistenceManagerConfig) -> Result<Self, MetadataBackendError> {
        Ok(Self {
            metadata_storage: config.create_metadata_storage()?,
            config,

            sink_threshold_times: Vec::new(),
            input_sources: Vec::new(),
        })
    }

    pub fn last_finalized_timestamp(&self) -> u64 {
        self.metadata_storage.last_advanced_timestamp()
    }
}

impl PersistenceManager for SimplePersistenceManager {
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

    /// Updates the frontiers of the data sources to the further ones,
    /// corresponding to the newly closed timestamp. It happens when all data
    /// for the particular timestamp has been outputted in full.
    ///
    /// When the program is restarted, it results in not outputting this data again.
    fn accept_finalized_timestamp(&mut self, sink_id: usize, reported_timestamp: Option<u64>) {
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

                for timestamp in frontiers_by_time.keys() {
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
        if let Err(e) = self.metadata_storage.save_current_state() {
            error!("Unable to save new frontiers, data on the output may duplicate in re-run. Error: {e}");
        }
    }

    fn create_snapshot_readers(
        &self,
        persistent_id: PersistentId,
    ) -> Result<Vec<Box<dyn SnapshotReader>>, ReadError> {
        self.config.create_snapshot_readers(persistent_id)
    }

    fn create_snapshot_writer(
        &mut self,
        persistent_id: PersistentId,
    ) -> Result<Box<dyn SnapshotWriter>, WriteError> {
        self.config.create_snapshot_writer(persistent_id)
    }
}

impl SimplePersistenceManager {
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
