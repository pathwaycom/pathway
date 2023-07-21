use std::collections::HashMap;

use crate::connectors::data_storage::StorageType;
use crate::connectors::{OffsetKey, OffsetValue};
use crate::persistence::{Error, PersistentId};

#[derive(Clone, Debug, Default)]
pub struct OffsetAntichain {
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

    pub fn serialize(&self) -> String {
        let mut tokens = Vec::<String>::new();
        for (offset_key, offset_value) in &self.antichain {
            let key_serialized = serde_json::to_string(offset_key)
                .expect("JSON Serialization of a simple structure should not fail");
            let value_serialized = serde_json::to_string(offset_value)
                .expect("JSON Serialization of a simple structure should not fail");
            let kv_token = format!("{key_serialized}~{value_serialized}");
            tokens.push(kv_token);
        }
        tokens.join("$")
    }

    pub fn deserialize(serialized: &str) -> Result<Self, Error> {
        let tokens = serialized.split('$');
        let mut result = OffsetAntichain::new();
        for token in tokens {
            let key_value: Vec<&str> = token.split('~').collect();
            if key_value.len() != 2 {
                return Err(Error::KeyValueIncorrect);
            }

            let deserialized_offset_key = serde_json::from_str::<OffsetKey>(key_value[0])
                .map_err(Error::IncorrectSerializedOffset)?;
            let deserialized_offset_value = serde_json::from_str::<OffsetValue>(key_value[1])
                .map_err(Error::IncorrectSerializedOffset)?;

            result.advance_offset(deserialized_offset_key, deserialized_offset_value);
        }
        Ok(result)
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

#[derive(Clone, Debug, Default)]
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

    pub fn serialize(&self) -> String {
        let mut tokens = Vec::<String>::new();
        for (persistent_id, antichain) in &self.antichains {
            tokens.push(format!("{persistent_id}|{}", antichain.serialize()));
        }
        tokens.join("^")
    }

    pub fn deserialize(serialized: &str) -> Result<Self, Error> {
        let mut result = OffsetAntichainCollection::new();

        if serialized.is_empty() {
            return Ok(Self::new());
        }

        let tokens = serialized.split('^');
        for token in tokens {
            let key_value: Vec<&str> = token.split('|').collect();
            if key_value.len() != 2 {
                return Err(Error::KeyValueIncorrect);
            }
            let persistent_id: PersistentId = key_value[0]
                .parse()
                .map_err(|_| Error::PersistentIdNotUInt)?;
            let antichain = OffsetAntichain::deserialize(key_value[1])?;
            if !result.add_antichain(persistent_id, antichain) {
                return Err(Error::DuplicatePersistentId);
            }
        }

        Ok(result)
    }
}
