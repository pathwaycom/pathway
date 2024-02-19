// Copyright © 2024 Pathway

use itertools::Itertools;
use log::{error, info};
use std::collections::HashMap;
use std::mem::take;
use std::sync::{Arc, Mutex};

use crate::connectors::data_storage::{ReadError, StorageType, WriteError};
use crate::connectors::snapshot::{SnapshotReader, SnapshotWriterFlushFuture};
use crate::connectors::PersistenceMode;
use crate::persistence::config::PersistenceManagerConfig;
use crate::persistence::frontier::OffsetAntichain;
use crate::persistence::metadata_backends::Error as MetadataBackendError;
use crate::persistence::state::MetadataAccessor;
use crate::persistence::{PersistentId, SharedSnapshotWriter};

type FrontierByTimeForInputSources = Vec<(PersistentId, Arc<Mutex<HashMap<u64, OffsetAntichain>>>)>;

/// The main coordinator for state persistence within one worker
/// It tracks offset frontiers and snapshots and commits them when the
/// global processing time advances
pub struct SingleWorkerPersistentStorage {
    metadata_storage: MetadataAccessor,
    config: PersistenceManagerConfig,

    snapshot_writers: HashMap<PersistentId, SharedSnapshotWriter>,
    sink_threshold_times: Vec<Option<u64>>,
    input_sources: FrontierByTimeForInputSources,
}

/// The information from the first phase of time finalization commit.
/// Returned from single worker's persistent storage, so that the caller
/// could only hold the mutex for the short period of time.
pub struct FrontierCommitData {
    // Futures, all of which should be waited before the metadata commit.
    snapshot_futures: Vec<SnapshotWriterFlushFuture>,

    // The timestamp which needs to be committed when saving is successful
    timestamp: u64,
}

impl FrontierCommitData {
    pub fn new(snapshot_futures: Vec<SnapshotWriterFlushFuture>, timestamp: u64) -> Self {
        Self {
            snapshot_futures,
            timestamp,
        }
    }

    pub fn prepare(&mut self) -> bool {
        let mut is_snapshot_saved = true;
        futures::executor::block_on(async {
            for mut future in take(&mut self.snapshot_futures) {
                if !is_snapshot_saved {
                    future.close();
                }
                let flush_result = future.await;
                if let Err(e) = flush_result {
                    error!("Failed to flush the snapshot, the data in re-run may duplicate: {e}");
                    is_snapshot_saved = false;
                }
            }
        });

        is_snapshot_saved
    }
}

impl SingleWorkerPersistentStorage {
    pub fn new(config: PersistenceManagerConfig) -> Result<Self, MetadataBackendError> {
        Ok(Self {
            metadata_storage: config.create_metadata_storage()?,
            config,

            snapshot_writers: HashMap::new(),
            sink_threshold_times: Vec::new(),
            input_sources: Vec::new(),
        })
    }

    pub fn table_persistence_enabled(&self) -> bool {
        matches!(
            self.config.persistence_mode,
            PersistenceMode::Persisting | PersistenceMode::SelectivePersisting
        )
    }

    pub fn persistent_id_generation_enabled(&self) -> bool {
        matches!(self.config.persistence_mode, PersistenceMode::Persisting)
    }

    pub fn last_finalized_timestamp(&self) -> u64 {
        self.metadata_storage.last_advanced_timestamp()
    }

    pub fn register_input_source(
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

    pub fn frontier_for(&self, persistent_id: PersistentId) -> OffsetAntichain {
        self.metadata_storage.frontier_for(persistent_id)
    }

    pub fn register_sink(&mut self) -> usize {
        self.sink_threshold_times.push(Some(0));
        self.sink_threshold_times.len() - 1
    }

    pub fn worker_id(&self) -> usize {
        self.config.worker_id
    }

    pub fn update_sink_finalized_time(
        &mut self,
        sink_id: usize,
        reported_timestamp: Option<u64>,
    ) -> Option<u64> {
        self.sink_threshold_times[sink_id] = reported_timestamp;
        self.finalized_time_within_worker()
    }

    /// This method is called when all workers have finished the processing of time `timestamp`.
    /// If `timestamp` is `None` it means that all output within all workers has finished.
    pub fn accept_globally_finalized_timestamp(
        &mut self,
        timestamp: Option<u64>,
    ) -> FrontierCommitData {
        /*
            Use the timestamp provided, or if it's None use the max timestamp across input sources
        */
        let finalized_timestamp = timestamp.unwrap_or(self.last_known_input_time() + 1);
        let prev_finalized_timestamp = self.last_finalized_timestamp();
        if finalized_timestamp < prev_finalized_timestamp {
            error!("Time isn't in the increasing order. Got advancement to {finalized_timestamp} while last advanced timestamp was {}", self.last_finalized_timestamp());

            // Empty set of snapshot commit futures and non-changed timestamp
            return FrontierCommitData::new(vec![], self.last_finalized_timestamp());
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
                    assert!(*timestamp >= prev_finalized_timestamp, "Found offsets for timestamp {} which is smaller than {prev_finalized_timestamp}", *timestamp);
                    if *timestamp < finalized_timestamp {
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
                info!("Save offset: ({antichain_item_key:?}, {antichain_item_value:?})");
                self.metadata_storage.save_offset(
                    *persistent_id,
                    antichain_item_key,
                    antichain_item_value,
                );
            }
        }

        let mut futures = Vec::new();
        for snapshot_writer in self.snapshot_writers.values() {
            let flush_future = snapshot_writer.lock().unwrap().flush();
            futures.push(flush_future);
        }

        FrontierCommitData::new(futures, finalized_timestamp)
    }

    pub fn commit_globally_finalized_timestamp(&mut self, commit_data: &FrontierCommitData) {
        self.metadata_storage
            .accept_finalized_timestamp(commit_data.timestamp);

        if let Err(e) = self.metadata_storage.save_current_state() {
            error!("Failed to save the current state, the data may duplicate in the re-run: {e}");
        }
    }

    pub fn create_snapshot_readers(
        &self,
        persistent_id: PersistentId,
    ) -> Result<Vec<SnapshotReader>, ReadError> {
        self.config.create_snapshot_readers(
            persistent_id,
            self.metadata_storage.past_runs_threshold_times(),
        )
    }

    pub fn create_snapshot_writer(
        &mut self,
        persistent_id: PersistentId,
    ) -> Result<SharedSnapshotWriter, WriteError> {
        if let Some(snapshot_writer) = self.snapshot_writers.get(&persistent_id) {
            Ok(snapshot_writer.clone())
        } else {
            let writer = self.config.create_snapshot_writer(persistent_id)?;
            self.snapshot_writers.insert(persistent_id, writer.clone());
            Ok(writer)
        }
    }

    pub fn finalized_time_within_worker(&self) -> Option<u64> {
        self.sink_threshold_times.iter().flatten().min().copied()
    }

    pub fn last_known_input_time(&self) -> u64 {
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
