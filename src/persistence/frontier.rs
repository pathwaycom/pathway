// Copyright © 2024 Pathway

use std::collections::{hash_map, HashMap};

use serde::{Deserialize, Serialize};
use serde_with::serde_as;

use crate::connectors::{OffsetKey, OffsetValue};

#[serde_as]
#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
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

    pub fn iter(&self) -> hash_map::Iter<'_, OffsetKey, OffsetValue> {
        self.antichain.iter()
    }
}

impl<'a> IntoIterator for &'a OffsetAntichain {
    type Item = (&'a OffsetKey, &'a OffsetValue);
    type IntoIter = hash_map::Iter<'a, OffsetKey, OffsetValue>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl IntoIterator for OffsetAntichain {
    type Item = (OffsetKey, OffsetValue);
    type IntoIter = hash_map::IntoIter<OffsetKey, OffsetValue>;

    fn into_iter(self) -> Self::IntoIter {
        self.antichain.into_iter()
    }
}
