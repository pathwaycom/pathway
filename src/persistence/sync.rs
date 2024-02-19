// Copyright © 2024 Pathway

use log::{error, info, warn};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use crate::persistence::tracker::SingleWorkerPersistentStorage;

#[derive(Default)]
pub struct WorkersPersistenceCoordinator {
    refresh_frequency: Duration,
    last_flush_at: Option<SystemTime>,
    worker_persistence_managers: Vec<Option<Arc<Mutex<SingleWorkerPersistentStorage>>>>,
    last_timestamp_flushed: Option<u64>,
    expected_ready_workers: usize,
}

impl WorkersPersistenceCoordinator {
    pub fn new(
        refresh_frequency: Duration,
        num_workers: usize,
        expected_ready_workers: usize,
    ) -> Self {
        Self {
            refresh_frequency,
            expected_ready_workers,
            last_flush_at: None,
            worker_persistence_managers: vec![None; num_workers],
            last_timestamp_flushed: Some(0),
        }
    }

    /// Record shared pointer to the particular worker's persistent storage in the storage.
    /// Maintain that the pointer to the storage of the
    /// worker K occupies the K-th position in the array.
    pub fn register_worker(
        &mut self,
        persistence_manager: Arc<Mutex<SingleWorkerPersistentStorage>>,
    ) {
        let worker_id = persistence_manager.lock().unwrap().worker_id();
        self.worker_persistence_managers[worker_id] = Some(persistence_manager);
    }

    /// Handles the event of the timestamp update within a particular sink in a particular worker.
    /// In case the global advanced timestamp advances, the snapshot writers flush the data and
    /// the new frontiers are committed.
    ///
    /// The new frontiers for any particular time T are committed only when all workers finish the
    /// output for this time T. Synchronization is needed, because there is no guarantee that the
    /// worker which reads the entry will output it in case of multithreaded execution.
    pub fn accept_finalized_timestamp(
        &mut self,
        worker_id: usize,
        sink_id: usize,
        reported_timestamp: Option<u64>,
    ) {
        let worker_storage = self.worker_persistence_managers[worker_id]
            .as_ref()
            .unwrap();
        worker_storage
            .lock()
            .unwrap()
            .update_sink_finalized_time(sink_id, reported_timestamp);

        let global_finalized_timestamp = self.global_closed_timestamp();

        if global_finalized_timestamp != self.last_timestamp_flushed {
            let current_timestamp = SystemTime::now();
            let should_refresh = self.last_flush_at.map_or(true, |last_timestamp| {
                current_timestamp.duration_since(last_timestamp).unwrap() >= self.refresh_frequency
            }) || reported_timestamp.is_none();
            if should_refresh {
                info!("Updating persistent state...");
                self.last_flush_at = Some(current_timestamp);

                self.last_timestamp_flushed = global_finalized_timestamp;
                let mut worker_futures = Vec::new();

                for persistence_manager in &self.worker_persistence_managers {
                    if let Some(persistence_manager) = persistence_manager {
                        let commit_data = persistence_manager
                            .lock()
                            .unwrap()
                            .accept_globally_finalized_timestamp(global_finalized_timestamp);
                        worker_futures.push(Some(commit_data));
                    } else {
                        worker_futures.push(None);
                    }
                }

                // Ensure all snapshots are written to the required point
                for (tracker, commit_data) in self
                    .worker_persistence_managers
                    .iter()
                    .zip(worker_futures.iter_mut())
                {
                    if let Some(tracker) = tracker {
                        let commit_data = commit_data.as_mut().expect("commit data missing");
                        let is_prepared = commit_data.prepare();
                        if !is_prepared {
                            error!(
                                "Failed to prepare frontier commit for worker {}",
                                tracker.lock().unwrap().worker_id()
                            );
                            return;
                        }
                    }
                }

                // Then commit all frontiers
                for (tracker, commit_data) in self
                    .worker_persistence_managers
                    .iter()
                    .zip(worker_futures.iter_mut())
                {
                    if let Some(tracker) = tracker {
                        let commit_data = commit_data.as_mut().expect("commit data missing");
                        tracker
                            .lock()
                            .unwrap()
                            .commit_globally_finalized_timestamp(commit_data);
                    }
                }
            }
        }
    }

    pub fn global_closed_timestamp(&mut self) -> Option<u64> {
        let mut prepared_workers_count = 0;
        let mut min_closed_timestamp = None;
        for worker_pm in &self.worker_persistence_managers {
            let Some(worker_pm) = worker_pm else {
                continue;
            };
            prepared_workers_count += 1;
            let worker_closed_timestamp = worker_pm.lock().unwrap().finalized_time_within_worker();
            if let Some(worker_closed_timestamp) = worker_closed_timestamp {
                match min_closed_timestamp {
                    None => min_closed_timestamp = Some(worker_closed_timestamp),
                    Some(current_min) => {
                        if current_min > worker_closed_timestamp {
                            min_closed_timestamp = Some(worker_closed_timestamp);
                        }
                    }
                }
            }
        }
        if prepared_workers_count != self.expected_ready_workers {
            warn!(
                "Workers not ready: {prepared_workers_count} prepared out of {} expected",
                self.expected_ready_workers
            );
            return Some(0);
        }
        min_closed_timestamp
    }
}

pub type SharedWorkersPersistenceCoordinator = Arc<Mutex<WorkersPersistenceCoordinator>>;
