// Copyright © 2024 Pathway

use log::{error, info, warn};
use std::cmp::max;
use std::collections::HashMap;
use std::fmt::Display;
use std::mem::swap;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::connectors::data_storage::StorageType;
use crate::engine::{Timestamp, TotalFrontier};
use crate::persistence::metadata_backends::{Error, MetadataBackend};
use crate::persistence::PersistentId;

const EXPECTED_KEY_PARTS: usize = 3;

#[derive(Debug, Serialize, Deserialize)]
pub struct StoredMetadata {
    storage_types: HashMap<PersistentId, StorageType>,
    last_advanced_timestamp: TotalFrontier<Timestamp>,
}

#[derive(Debug)]
pub struct MetadataAccessor {
    backend: Box<dyn MetadataBackend>,
    internal_state: StoredMetadata,
    past_runs_threshold_times: HashMap<usize, TotalFrontier<Timestamp>>,

    current_key_to_use: String,
    next_key_to_use: String,
}

impl StoredMetadata {
    pub fn new() -> Self {
        Self {
            storage_types: HashMap::new(),
            last_advanced_timestamp: TotalFrontier::At(Timestamp(0)),
        }
    }

    pub fn parse(data: &str) -> Result<Self, Error> {
        let result = serde_json::from_str::<StoredMetadata>(data.trim_end())
            .map_err(|e| Error::IncorrectFormat(data.to_string(), e))?;
        Ok(result)
    }

    pub fn serialize(&self) -> String {
        serde_json::to_string(&self).unwrap()
    }

    pub fn merge(&mut self, other: &StoredMetadata) {
        self.storage_types.extend(other.storage_types.iter());
    }
}

impl Default for StoredMetadata {
    fn default() -> Self {
        Self::new()
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

impl MetadataAccessor {
    pub fn new(backend: Box<dyn MetadataBackend>, worker_id: usize) -> Result<Self, Error> {
        let (internal_state, past_runs_threshold_times) = {
            let mut internal_state = StoredMetadata::new();
            let mut past_runs_threshold_times = HashMap::new();
            let mut last_version_per_worker = HashMap::new();

            let keys = backend.list_keys()?;
            for key in &keys {
                let metadata_key = MetadataKey::from_str(key);
                let Some(metadata_key) = metadata_key else {
                    continue;
                };
                last_version_per_worker
                    .entry(metadata_key.worker_id)
                    .and_modify(|version: &mut u128| {
                        *version = max(*version, metadata_key.version);
                    })
                    .or_insert(metadata_key.version);
            }

            for key in keys {
                let metadata_key = MetadataKey::from_str(&key);
                let Some(metadata_key) = metadata_key else {
                    continue;
                };

                // last versions for all known worker ids are calculated above
                let target_version = last_version_per_worker
                    .get(&metadata_key.worker_id)
                    .expect("no version for worker");
                if metadata_key.version < *target_version {
                    info!("Removing obsolete metadata block: {metadata_key}. Actual version: {target_version}");
                    if let Err(e) = backend.remove_key(&key) {
                        error!("Failed to remove obsolete metadata block: {e}");
                    }
                    continue;
                }

                let other_worker_id = metadata_key.worker_id;
                let raw_block = backend.get_value(&key)?;
                let block_result = StoredMetadata::parse(&raw_block);
                match block_result {
                    Ok(block) => {
                        past_runs_threshold_times
                            .entry(other_worker_id)
                            .and_modify(|timestamp: &mut TotalFrontier<Timestamp>| {
                                *timestamp = max(*timestamp, block.last_advanced_timestamp);
                            })
                            .or_insert(block.last_advanced_timestamp);
                        info!("Merge the current state with block: {block:?}");
                        internal_state.merge(&block);
                    }
                    Err(e) => {
                        warn!("Broken offsets block with key {key}. Error: {e}");
                    }
                };
            }

            (internal_state, past_runs_threshold_times)
        };

        let current_timestamp = {
            let now = SystemTime::now();
            now.duration_since(UNIX_EPOCH)
                .expect("Failed to acquire system time")
                .as_millis()
        };
        let current_key_to_use =
            MetadataKey::from_components(current_timestamp, worker_id, 0).to_string();
        let next_key_to_use =
            MetadataKey::from_components(current_timestamp, worker_id, 1).to_string();

        Ok(Self {
            backend,
            internal_state,
            past_runs_threshold_times,
            current_key_to_use,
            next_key_to_use,
        })
    }

    pub fn past_runs_threshold_times(&self) -> &HashMap<usize, TotalFrontier<Timestamp>> {
        &self.past_runs_threshold_times
    }

    pub fn register_input_source(
        &mut self,
        persistent_id: PersistentId,
        storage_type: &StorageType,
    ) {
        self.internal_state
            .storage_types
            .insert(persistent_id, *storage_type);
    }

    pub fn accept_finalized_timestamp(&mut self, timestamp: TotalFrontier<Timestamp>) {
        self.internal_state.last_advanced_timestamp = timestamp;
    }

    pub fn last_advanced_timestamp(&self) -> TotalFrontier<Timestamp> {
        self.internal_state.last_advanced_timestamp
    }

    pub fn save_current_state(&mut self) -> Result<(), Error> {
        let serialized_state = self.internal_state.serialize();
        self.backend
            .put_value(&self.current_key_to_use, &serialized_state)?;
        swap(&mut self.current_key_to_use, &mut self.next_key_to_use);
        Ok(())
    }
}
