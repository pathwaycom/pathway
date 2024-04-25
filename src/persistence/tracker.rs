// Copyright © 2024 Pathway

use log::{error, warn};
use std::collections::{HashMap, HashSet};
use std::mem::take;
use std::time::Instant;

use crate::connectors::data_storage::{ReadError, StorageType, WriteError};
use crate::connectors::snapshot::{SnapshotReader, SnapshotWriterFlushFuture};
use crate::connectors::PersistenceMode;
use crate::engine::{Timestamp, TotalFrontier};
use crate::persistence::config::{PersistenceManagerConfig, ReadersQueryPurpose};
use crate::persistence::metadata_backends::Error as MetadataBackendError;
use crate::persistence::state::MetadataAccessor;
use crate::persistence::{PersistentId, SharedSnapshotWriter};

/// The main coordinator for state persistence within a worker
/// It tracks logical time and snapshots and commits them when
/// the processing time advances.
pub struct WorkerPersistentStorage {
    metadata_storage: MetadataAccessor,
    config: PersistenceManagerConfig,

    snapshot_writers: HashMap<PersistentId, SharedSnapshotWriter>,
    sink_threshold_times: Vec<TotalFrontier<Timestamp>>,
    registered_persistent_ids: HashSet<PersistentId>,
    last_commit_at: Instant,
}

/// The information from the first phase of time finalization commit.
pub struct LogicalTimeCommitData {
    // Futures, all of which should be waited before the metadata commit.
    snapshot_futures: Vec<SnapshotWriterFlushFuture>,

    // The timestamp which needs to be committed when saving is successful
    timestamp: TotalFrontier<Timestamp>,
}

impl LogicalTimeCommitData {
    pub fn new(
        snapshot_futures: Vec<SnapshotWriterFlushFuture>,
        timestamp: TotalFrontier<Timestamp>,
    ) -> Self {
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

impl WorkerPersistentStorage {
    pub fn new(config: PersistenceManagerConfig) -> Result<Self, MetadataBackendError> {
        Ok(Self {
            metadata_storage: config.create_metadata_storage()?,
            config,

            snapshot_writers: HashMap::new(),
            sink_threshold_times: Vec::new(),
            registered_persistent_ids: HashSet::new(),
            last_commit_at: Instant::now(),
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

    pub fn last_finalized_timestamp(&self) -> TotalFrontier<Timestamp> {
        self.metadata_storage.last_advanced_timestamp()
    }

    pub fn register_input_source(
        &mut self,
        persistent_id: PersistentId,
        storage_type: &StorageType,
    ) {
        assert!(
            !self.registered_persistent_ids.contains(&persistent_id),
            "Same persistent_id belongs to more than one data source: {persistent_id}"
        );
        self.registered_persistent_ids.insert(persistent_id);
        self.metadata_storage
            .register_input_source(persistent_id, storage_type);
    }

    pub fn register_sink(&mut self) -> usize {
        self.sink_threshold_times
            .push(TotalFrontier::At(Timestamp(0)));
        self.sink_threshold_times.len() - 1
    }

    pub fn update_sink_finalized_time(
        &mut self,
        sink_id: usize,
        reported_timestamp: Option<Timestamp>,
    ) {
        self.sink_threshold_times[sink_id] =
            reported_timestamp.map_or(TotalFrontier::Done, TotalFrontier::At);
        let worker_finalized_timestamp = *self
            .sink_threshold_times
            .iter()
            .min()
            .expect("no known sinks");

        let timestamp_updated = worker_finalized_timestamp != self.last_finalized_timestamp();
        let commit_interval_passed = matches!(worker_finalized_timestamp, TotalFrontier::Done)
            || self.last_commit_at.elapsed() >= self.config.snapshot_interval;

        if timestamp_updated && commit_interval_passed {
            let mut commit_data = self.accept_finalized_timestamp(worker_finalized_timestamp);
            if !commit_data.prepare() {
                warn!("Failed to prepare commit data, logical time {worker_finalized_timestamp:?} won't be committed");
                return;
            }
            self.commit_finalized_timestamp(&commit_data);
            self.last_commit_at = Instant::now();
        }
    }

    /// This method is called when the worker has finished the processing of time `timestamp`.
    /// If `timestamp` is `None` it means that all output within this worker is finished.
    fn accept_finalized_timestamp(
        &mut self,
        finalized_timestamp: TotalFrontier<Timestamp>,
    ) -> LogicalTimeCommitData {
        /*
            Use the timestamp provided, or if it's None use the max timestamp across input sources
        */
        let prev_finalized_timestamp = self.last_finalized_timestamp();
        if finalized_timestamp <= prev_finalized_timestamp {
            error!("Time isn't in the increasing order. Got advancement to {finalized_timestamp:?} while last advanced timestamp was {:?}", self.last_finalized_timestamp());

            // Empty set of snapshot commit futures and non-changed timestamp
            return LogicalTimeCommitData::new(vec![], self.last_finalized_timestamp());
        }

        let mut futures = Vec::new();
        for snapshot_writer in self.snapshot_writers.values() {
            let flush_future = snapshot_writer.lock().unwrap().flush();
            futures.push(flush_future);
        }

        LogicalTimeCommitData::new(futures, finalized_timestamp)
    }

    fn commit_finalized_timestamp(&mut self, commit_data: &LogicalTimeCommitData) {
        self.metadata_storage
            .accept_finalized_timestamp(commit_data.timestamp);

        if let Err(e) = self.metadata_storage.save_current_state() {
            error!("Failed to save the current state, the data may duplicate in the re-run: {e}");
        }
    }

    pub fn create_snapshot_readers(
        &self,
        persistent_id: PersistentId,
        query_purpose: ReadersQueryPurpose,
    ) -> Result<Vec<SnapshotReader>, ReadError> {
        self.config.create_snapshot_readers(
            persistent_id,
            self.metadata_storage.past_runs_threshold_times(),
            query_purpose,
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
}
