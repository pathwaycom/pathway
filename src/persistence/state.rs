use log::{error, warn};
use std::cmp::max;
use std::collections::HashMap;
use std::mem::swap;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::connectors::data_storage::StorageType;
use crate::connectors::{OffsetKey, OffsetValue};
use crate::persistence::frontier::OffsetAntichain;
use crate::persistence::frontier::OffsetAntichainCollection;
use crate::persistence::metadata_backends::{Error, MetadataBackend};
use crate::persistence::PersistentId;

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct StoredMetadata {
    frontiers: OffsetAntichainCollection,
    storage_types: HashMap<PersistentId, StorageType>,
    last_advanced_timestamp: u64,
}

#[derive(Debug)]
pub struct MetadataAccessor {
    backend: Box<dyn MetadataBackend>,
    internal_state: StoredMetadata,

    current_key_to_use: String,
    next_key_to_use: String,
}

impl StoredMetadata {
    pub fn new() -> Self {
        Self {
            frontiers: OffsetAntichainCollection::new(),
            storage_types: HashMap::new(),
            last_advanced_timestamp: 0,
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

    pub fn merge(&mut self, other: StoredMetadata) {
        self.storage_types.extend(other.storage_types.iter());
        self.frontiers
            .update_with_collection(other.frontiers, &other.storage_types);
        self.last_advanced_timestamp =
            max(self.last_advanced_timestamp, other.last_advanced_timestamp);
    }
}

impl MetadataAccessor {
    pub fn new(backend: Box<dyn MetadataBackend>, worker_id: usize) -> Result<Self, Error> {
        let internal_state = {
            let mut internal_state = StoredMetadata::new();

            let keys = backend.list_keys()?;
            for key in keys {
                let raw_block = backend.get_value(&key)?;
                let block_result = StoredMetadata::parse(&raw_block);
                match block_result {
                    Ok(block) => internal_state.merge(block),
                    Err(e) => {
                        warn!("Broken offsets block with key {key}. Error: {e}");
                    }
                };
            }

            internal_state
        };

        let current_timestamp = {
            let now = SystemTime::now();

            now.duration_since(UNIX_EPOCH)
                .expect("Failed to acquire system time")
                .as_millis()
        };
        let current_key_to_use = format!("{current_timestamp}-{worker_id}-0");
        let next_key_to_use = format!("{current_timestamp}-{worker_id}-1");

        Ok(Self {
            backend,
            internal_state,
            current_key_to_use,
            next_key_to_use,
        })
    }

    pub fn frontier_for(&self, persistent_id: PersistentId) -> OffsetAntichain {
        self.internal_state
            .frontiers
            .antichain_for_storage(persistent_id)
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

    pub fn save_offset(
        &mut self,
        persistent_id: PersistentId,
        offset_key: &OffsetKey,
        offset_value: &OffsetValue,
    ) {
        self.internal_state.frontiers.advance_offset(
            persistent_id,
            offset_key.clone(),
            offset_value.clone(),
        );
    }

    pub fn accept_finalized_timestamp(&mut self, timestamp: u64) {
        self.internal_state.last_advanced_timestamp = timestamp;
    }

    pub fn last_advanced_timestamp(&self) -> u64 {
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

impl Drop for MetadataAccessor {
    fn drop(&mut self) {
        if let Err(e) = self.save_current_state() {
            error!("Unsuccessful termination of metadata storage. Data may duplicate in the re-run. Error: {e}");
        }
    }
}
