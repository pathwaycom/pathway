// Copyright © 2024 Pathway

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_with::serde_as;

use crate::connectors::data_storage::StorageType;
use crate::connectors::{OffsetKey, OffsetValue};
use crate::persistence::PersistentId;

#[serde_as]
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct OffsetAntichain {
    #[serde_as(as = "Vec<(_, _)>")]
    antichain: HashMap<OffsetKey, OffsetValue>,
}

impl OffsetAntichain {
    pub fn new() -> Self {
        Self {
            antichain: HashMap::new(),
        }
    }

    pub fn as_vec(&self) -> Vec<(OffsetKey, OffsetValue)> {
        let mut result = Vec::new();
        for (key, value) in &self.antichain {
            result.push((key.clone(), value.clone()));
        }
        result
    }

    pub fn get_offset(&self, offset_key: &OffsetKey) -> Option<&OffsetValue> {
        self.antichain.get(offset_key)
    }

    pub fn advance_offset(&mut self, offset_key: OffsetKey, offset_value: OffsetValue) {
        self.antichain.insert(offset_key, offset_value);
    }

    pub fn empty(&self) -> bool {
        self.antichain.is_empty()
    }
}

impl<'a> IntoIterator for &'a OffsetAntichain {
    type Item = (&'a OffsetKey, &'a OffsetValue);
    type IntoIter = std::collections::hash_map::Iter<'a, OffsetKey, OffsetValue>;

    fn into_iter(self) -> Self::IntoIter {
        self.antichain.iter()
    }
}

impl IntoIterator for OffsetAntichain {
    type Item = (OffsetKey, OffsetValue);
    type IntoIter = std::collections::hash_map::IntoIter<OffsetKey, OffsetValue>;

    fn into_iter(self) -> Self::IntoIter {
        self.antichain.into_iter()
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct OffsetAntichainCollection {
    antichains: HashMap<PersistentId, OffsetAntichain>,
}

impl OffsetAntichainCollection {
    pub fn new() -> Self {
        Self {
            antichains: HashMap::new(),
        }
    }

    pub fn advance_offset(
        &mut self,
        persistent_id: PersistentId,
        offset_key: OffsetKey,
        offset_value: OffsetValue,
    ) {
        if let Some(data) = self.antichains.get_mut(&persistent_id) {
            data.advance_offset(offset_key, offset_value);
        } else {
            let mut antichain = OffsetAntichain::new();
            antichain.advance_offset(offset_key, offset_value);
            self.antichains.insert(persistent_id, antichain);
        }
    }

    pub fn add_antichain(
        &mut self,
        persistent_id: PersistentId,
        antichain: OffsetAntichain,
    ) -> bool {
        if self.antichains.get(&persistent_id).is_some() {
            false
        } else {
            self.antichains.insert(persistent_id, antichain);
            true
        }
    }

    pub fn update_with_collection(
        &mut self,
        other: OffsetAntichainCollection,
        storage_types: &HashMap<PersistentId, StorageType>,
    ) {
        for (persistent_id, antichain) in other.take_state() {
            let storage_type = storage_types.get(&persistent_id);
            if let Some(storage_type) = storage_type {
                self.update_antichain(persistent_id, antichain, *storage_type);
            }
        }
    }

    pub fn update_antichain(
        &mut self,
        persistent_id: PersistentId,
        antichain: OffsetAntichain,
        storage_type: StorageType,
    ) {
        if let Some(existing_antichain) = self.antichains.get_mut(&persistent_id) {
            let merged_frontier = storage_type.merge_two_frontiers(existing_antichain, &antichain);
            self.antichains.insert(persistent_id, merged_frontier);
        } else {
            self.antichains.insert(persistent_id, antichain);
        }
    }

    pub fn antichain_for_storage(&self, persistent_id: PersistentId) -> OffsetAntichain {
        match self.antichains.get(&persistent_id) {
            Some(data) => data.clone(),
            None => OffsetAntichain::new(),
        }
    }

    pub fn take_state(self) -> HashMap<PersistentId, OffsetAntichain> {
        self.antichains
    }
}
