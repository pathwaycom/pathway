// Copyright © 2024 Pathway

use log::{error, info, warn};
use std::collections::HashMap;
use std::fmt::Display;
use std::mem::swap;

use serde::{Deserialize, Serialize};

use crate::engine::{Timestamp, TotalFrontier};
use crate::persistence::backends::PersistenceBackend;
use crate::persistence::Error;

const EXPECTED_KEY_PARTS: usize = 3;

#[derive(Debug, Serialize, Deserialize)]
pub struct StoredMetadata {
    pub last_advanced_timestamp: TotalFrontier<Timestamp>,

    // Workers count from the latest run is required to determine
    // the range of workers that contribute into the threshold timestamp
    // calculation on the start of the program.
    //
    // We use it instead of the current number of workers, because the number
    // of workers could have changed between the two different runs.
    //
    // This field is allowed to be unspecified for the backwards
    // compatibility with the older versions. Then we don't have any option
    // better than to use the current number of workers.
    #[serde(default)]
    pub total_workers: usize,
}

#[derive(Debug)]
pub struct MetadataAccessor {
    backend: Box<dyn PersistenceBackend>,
    internal_state: StoredMetadata,
    past_runs_threshold_time: TotalFrontier<Timestamp>,

    current_key_to_use: String,
    next_key_to_use: String,
}

impl StoredMetadata {
    pub fn new(total_workers: usize) -> Self {
        Self {
            last_advanced_timestamp: TotalFrontier::At(Timestamp(0)),
            total_workers,
        }
    }

    pub fn parse(bytes: &[u8], default_total_workers: usize) -> Result<Self, Error> {
        let data = std::str::from_utf8(bytes)?;
        let mut result = serde_json::from_str::<StoredMetadata>(data.trim_end())
            .map_err(|e| Error::IncorrectMetadataFormat(data.to_string(), e))?;

        // The block comes from an older version and has no number of workers specified.
        if result.total_workers == 0 {
            result.total_workers = default_total_workers;
        }
        Ok(result)
    }

    pub fn serialize(&self) -> String {
        serde_json::to_string(&self).unwrap()
    }
}

struct MetadataKey {
    version: u128,
    worker_id: usize,
    rotation_id: usize,
}

impl MetadataKey {
    fn from_str(key: &str) -> Option<Self> {
        let key_parts: Vec<&str> = key.split('-').collect();
        if key_parts.len() != EXPECTED_KEY_PARTS {
            error!("Wrong format of persistent entry key: {key}");
            return None;
        }

        let Ok(version) = key_parts[0].parse::<u128>() else {
            error!("Version is unparsable from the key {key}");
            return None;
        };

        let Ok(worker_id) = key_parts[1].parse::<usize>() else {
            error!("Worker id is unparsable from the key {key}");
            return None;
        };

        let Ok(rotation_id) = key_parts[2].parse::<usize>() else {
            error!("Rotation id is unparsable from the key {key}");
            return None;
        };

        Some(Self {
            version,
            worker_id,
            rotation_id,
        })
    }

    fn from_components(version: u128, worker_id: usize, rotation_id: usize) -> Self {
        Self {
            version,
            worker_id,
            rotation_id,
        }
    }
}

impl Display for MetadataKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}-{}-{}",
            self.version, self.worker_id, self.rotation_id
        )
    }
}

struct VersionInformation {
    worker_finalized_times: Vec<Option<TotalFrontier<Timestamp>>>,
}

impl VersionInformation {
    pub fn new(total_workers: usize) -> Self {
        Self {
            worker_finalized_times: vec![None; total_workers],
        }
    }

    pub fn update_worker_time(
        &mut self,
        worker_id: usize,
        finalized_time: TotalFrontier<Timestamp>,
    ) {
        let expected_workers = self.worker_finalized_times.len();
        if worker_id >= expected_workers {
            error!("Got worker id {worker_id} while only {expected_workers} workers were expected");
            return;
        }
        if self.worker_finalized_times[worker_id].map_or(true, |time| time < finalized_time) {
            self.worker_finalized_times[worker_id] = Some(finalized_time);
        }
    }

    pub fn threshold_time(&self) -> Option<TotalFrontier<Timestamp>> {
        if self.worker_finalized_times.contains(&None) {
            // Not all workers reported their threshold times
            None
        } else {
            self.worker_finalized_times.iter().min().copied()?
        }
    }
}

fn compute_threshold_time_and_versions(
    backend: &mut dyn PersistenceBackend,
    should_remove: bool,
    total_workers: usize,
) -> Result<(TotalFrontier<Timestamp>, u128, Option<u128>), Error> {
    // We want to start from the latest version that has metadata for all its workers
    // In the code, we call it the latest stable version
    let keys = backend.list_keys()?;
    let mut version_information = HashMap::new();
    for key in &keys {
        if key.contains('/') {
            // Only top-level keys are needed for the metadata reconstruction.
            continue;
        }
        let metadata_key = MetadataKey::from_str(key);
        let Some(metadata_key) = metadata_key else {
            continue;
        };

        let Ok(raw_block) = backend.get_value(key) else {
            // In any case, don't fail here, just use the earlier version
            warn!("Failed to retrieve the value for the metadata block {key}. Most likely it was removed as obsolete.");
            continue;
        };
        let block_result = StoredMetadata::parse(&raw_block, total_workers);
        match block_result {
            Ok(block) => {
                version_information
                    .entry(metadata_key.version)
                    .or_insert(VersionInformation::new(block.total_workers))
                    .update_worker_time(metadata_key.worker_id, block.last_advanced_timestamp);
            }
            Err(e) => {
                warn!("Broken metadata block for key {key}. Error: {e}");
                if should_remove {
                    // Avoid removing the same object from multiple workers
                    if let Err(e) = backend.remove_key(key) {
                        error!("Failed to remove the broken metadata block {key}: {e}");
                    }
                }
            }
        };
    }

    let mut past_runs_threshold_time = TotalFrontier::At(Timestamp(0));
    let mut latest_stable_version = None;
    for (version_number, version_data) in &version_information {
        let threshold_time = version_data.threshold_time();
        let Some(threshold_time) = threshold_time else {
            continue;
        };
        if latest_stable_version.map_or(true, |current_version| current_version < *version_number) {
            latest_stable_version = Some(*version_number);
            past_runs_threshold_time = threshold_time;
        }
    }

    let current_version = version_information
        .keys()
        .max()
        .copied()
        .unwrap_or_default()
        + 1;
    if let Some(latest_stable_version) = latest_stable_version {
        for key in keys {
            let metadata_key = MetadataKey::from_str(&key);
            let Some(metadata_key) = metadata_key else {
                continue;
            };
            if metadata_key.version < latest_stable_version && should_remove {
                info!("Removing obsolete metadata entry: {key}");
                // Avoid removing the same object from multiple workers
                if let Err(e) = backend.remove_key(&key) {
                    error!("Failed to remove the obsolete metadata block {key}: {e}");
                }
            }
        }
    }

    Ok((
        past_runs_threshold_time,
        current_version,
        latest_stable_version,
    ))
}

impl MetadataAccessor {
    pub fn new(
        mut backend: Box<dyn PersistenceBackend>,
        worker_id: usize,
        total_workers: usize,
    ) -> Result<Self, Error> {
        let internal_state = StoredMetadata::new(total_workers);
        let (past_runs_threshold_time, current_version, latest_stable_version) =
            compute_threshold_time_and_versions(backend.as_mut(), worker_id == 0, total_workers)?;
        info!("Worker {worker_id} is on the version {current_version}. The latest stable metadata version is {latest_stable_version:?}");
        let current_key_to_use =
            MetadataKey::from_components(current_version, worker_id, 0).to_string();
        let next_key_to_use =
            MetadataKey::from_components(current_version, worker_id, 1).to_string();

        Ok(Self {
            backend,
            internal_state,
            past_runs_threshold_time,
            current_key_to_use,
            next_key_to_use,
        })
    }

    pub fn past_runs_threshold_time(&self) -> TotalFrontier<Timestamp> {
        self.past_runs_threshold_time
    }

    pub fn accept_finalized_timestamp(&mut self, timestamp: TotalFrontier<Timestamp>) {
        self.internal_state.last_advanced_timestamp = timestamp;
    }

    pub fn last_advanced_timestamp(&self) -> TotalFrontier<Timestamp> {
        self.internal_state.last_advanced_timestamp
    }

    pub fn save_current_state(&mut self) -> Result<(), Error> {
        let serialized_state = self.internal_state.serialize();
        futures::executor::block_on(async {
            self.backend
                .put_value(&self.current_key_to_use, serialized_state.into())
                .await
                .expect("unexpected future cancelling")
        })?;
        swap(&mut self.current_key_to_use, &mut self.next_key_to_use);
        Ok(())
    }
}

pub struct FinalizedTimeQuerier {
    backend: Box<dyn PersistenceBackend>,
    total_workers: usize,
}

impl FinalizedTimeQuerier {
    pub fn new(backend: Box<dyn PersistenceBackend>, total_workers: usize) -> Self {
        Self {
            backend,
            total_workers,
        }
    }

    pub fn last_finalized_timestamp(&mut self) -> Result<TotalFrontier<Timestamp>, Error> {
        Ok(
            compute_threshold_time_and_versions(self.backend.as_mut(), false, self.total_workers)?
                .0,
        )
    }
}
