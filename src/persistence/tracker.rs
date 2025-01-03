// Copyright © 2024 Pathway

use differential_dataflow::difference::Semigroup;
use differential_dataflow::ExchangeData;
use log::{error, warn};
use std::collections::{HashMap, HashSet};
use std::mem::take;
use std::sync::{Arc, Mutex};

use crate::connectors::PersistenceMode;
use crate::engine::{Timestamp, TotalFrontier};
use crate::persistence::backends::BackendPutFuture as PersistenceBackendFlushFuture;
use crate::persistence::cached_object_storage::CachedObjectStorage;
use crate::persistence::config::{PersistenceManagerConfig, ReadersQueryPurpose};
use crate::persistence::input_snapshot::{ReadInputSnapshot, SnapshotMode};
use crate::persistence::operator_snapshot::{
    ConcreteSnapshotMerger, Flushable, OperatorSnapshotReader,
};
use crate::persistence::state::MetadataAccessor;
use crate::persistence::Error as PersistenceBackendError;
use crate::persistence::{
    PersistenceTime, PersistentId, SharedOperatorSnapshotWriter, SharedSnapshotWriter,
};

#[derive(Debug, Clone, Copy)]
pub enum RequiredPersistenceMode {
    OperatorPersistence,
    InputOrOperatorPersistence,
}

impl RequiredPersistenceMode {
    fn matches(self, persistence_mode: PersistenceMode) -> bool {
        matches!(
            (self, persistence_mode),
            (
                Self::OperatorPersistence,
                PersistenceMode::OperatorPersisting
            ) | (
                Self::InputOrOperatorPersistence,
                PersistenceMode::Persisting | PersistenceMode::OperatorPersisting
            )
        )
    }
}

/// The main coordinator for state persistence within a worker
/// It tracks logical time and snapshots and commits them when
/// the processing time advances.
pub struct WorkerPersistentStorage {
    metadata_storage: MetadataAccessor,
    config: PersistenceManagerConfig,

    snapshot_writers: HashMap<PersistentId, SharedSnapshotWriter>,
    operator_snapshot_writers: HashMap<PersistentId, Arc<Mutex<dyn Flushable + Send>>>,
    operator_snapshot_mergers: Vec<ConcreteSnapshotMerger>,
    sink_threshold_times: Vec<TotalFrontier<Timestamp>>,
    registered_persistent_ids: HashSet<PersistentId>,
}

pub type SharedWorkerPersistentStorage = Arc<Mutex<WorkerPersistentStorage>>;

/// The information from the first phase of time finalization commit.
pub struct LogicalTimeCommitData {
    // Futures, all of which should be waited before the metadata commit.
    snapshot_futures: Vec<PersistenceBackendFlushFuture>,

    // The timestamp which needs to be committed when saving is successful
    timestamp: TotalFrontier<Timestamp>,
}

impl LogicalTimeCommitData {
    pub fn new(
        snapshot_futures: Vec<PersistenceBackendFlushFuture>,
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
    pub fn new(config: PersistenceManagerConfig) -> Result<Self, PersistenceBackendError> {
        Ok(Self {
            metadata_storage: config.create_metadata_storage()?,
            config,

            snapshot_writers: HashMap::new(),
            operator_snapshot_writers: HashMap::new(),
            operator_snapshot_mergers: Vec::new(),
            sink_threshold_times: Vec::new(),
            registered_persistent_ids: HashSet::new(),
        })
    }

    pub fn create_cached_object_storage(
        &self,
        persistent_id: PersistentId,
    ) -> Result<CachedObjectStorage, PersistenceBackendError> {
        self.config.create_cached_object_storage(persistent_id)
    }

    pub fn table_persistence_enabled(&self) -> bool {
        matches!(
            self.config.persistence_mode,
            PersistenceMode::Persisting
                | PersistenceMode::SelectivePersisting
                | PersistenceMode::OperatorPersisting
        )
    }

    pub fn input_persistence_enabled(&self) -> bool {
        // FIXME make sure the condition is correct
        !matches!(
            self.config.persistence_mode,
            PersistenceMode::OperatorPersisting | PersistenceMode::UdfCaching
        )
    }

    pub fn persistent_id_generation_enabled(
        &self,
        required_persistence_mode: RequiredPersistenceMode,
    ) -> bool {
        required_persistence_mode.matches(self.config.persistence_mode)
    }

    pub fn last_finalized_timestamp(&self) -> TotalFrontier<Timestamp> {
        self.metadata_storage.last_advanced_timestamp()
    }

    pub fn register_input_source(&mut self, persistent_id: PersistentId) {
        assert!(
            !self.registered_persistent_ids.contains(&persistent_id),
            "Same persistent_id belongs to more than one data source: {persistent_id}"
        );
        self.registered_persistent_ids.insert(persistent_id);
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

        let normalized_finalized_timestamp = match worker_finalized_timestamp {
            TotalFrontier::At(worker_finalized_timestamp) => TotalFrontier::At(
                worker_finalized_timestamp
                    .most_recent_possible_snapshot_time(self.config.snapshot_interval),
            ),
            TotalFrontier::Done => TotalFrontier::Done,
        };
        let timestamp_updated = normalized_finalized_timestamp != self.last_finalized_timestamp();
        if timestamp_updated {
            let mut commit_data = self.accept_finalized_timestamp(normalized_finalized_timestamp);
            if !commit_data.prepare() {
                warn!("Failed to prepare commit data, logical time {normalized_finalized_timestamp:?} won't be committed");
                return;
            }
            self.commit_finalized_timestamp(&commit_data);
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
            let mut flush_futures = snapshot_writer.lock().unwrap().flush();
            futures.append(&mut flush_futures);
        }
        for operator_snapshot_writer in self.operator_snapshot_writers.values() {
            let mut flush_futures = operator_snapshot_writer
                .lock()
                .unwrap()
                .flush(finalized_timestamp);
            futures.append(&mut flush_futures);
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
    ) -> Result<Vec<Box<dyn ReadInputSnapshot>>, PersistenceBackendError> {
        self.config.create_snapshot_readers(
            persistent_id,
            self.metadata_storage.past_runs_threshold_time(),
            query_purpose,
        )
    }

    pub fn create_snapshot_writer(
        &mut self,
        persistent_id: PersistentId,
        snapshot_mode: SnapshotMode,
    ) -> Result<SharedSnapshotWriter, PersistenceBackendError> {
        if let Some(snapshot_writer) = self.snapshot_writers.get(&persistent_id) {
            Ok(snapshot_writer.clone())
        } else {
            let writer = self
                .config
                .create_snapshot_writer(persistent_id, snapshot_mode)?;
            self.snapshot_writers.insert(persistent_id, writer.clone());
            Ok(writer)
        }
    }

    pub fn create_operator_snapshot_reader<D, R>(
        &mut self,
        persistent_id: PersistentId,
    ) -> Result<Box<dyn OperatorSnapshotReader<D, R> + Send>, PersistenceBackendError>
    where
        D: ExchangeData,
        R: ExchangeData + Semigroup,
    {
        let (reader, merger) = self.config.create_operator_snapshot_readers::<D, R>(
            persistent_id,
            self.metadata_storage.past_runs_threshold_time(),
        )?;
        self.operator_snapshot_mergers.push(merger);
        Ok(Box::new(reader))
    }

    pub fn create_operator_snapshot_writer<D, R>(
        &mut self,
        persistent_id: PersistentId,
    ) -> Result<SharedOperatorSnapshotWriter<D, R>, PersistenceBackendError>
    where
        D: ExchangeData,
        R: ExchangeData + Semigroup,
    {
        let writer = self.config.create_operator_snapshot_writer(persistent_id)?;
        let writer = Arc::new(Mutex::new(writer));
        let writer_flushable: Arc<Mutex<dyn Flushable + Send>> = writer.clone();
        self.operator_snapshot_writers
            .insert(persistent_id, writer_flushable);
        Ok(writer)
    }
}
