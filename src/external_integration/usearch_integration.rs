// Copyright © 2024 Pathway

use std::cmp::max;
use std::sync::Arc;

use crate::engine::error::DynResult;
use crate::engine::{Error, Key};
use usearch::ffi::{IndexOptions, MetricKind, ScalarKind};
use usearch::{new_index, Index};

use super::{
    DerivedFilteredSearchIndex, ExternalIndex, ExternalIndexFactory, KeyScoreMatch,
    KeyToU64IdMapper, NonFilteringExternalIndex,
};

#[derive(Clone, Copy)]
pub struct USearchMetricKind(pub MetricKind);

pub struct USearchKNNIndex {
    index: Arc<Index>,
    key_to_id_mapper: KeyToU64IdMapper,
}

impl USearchKNNIndex {
    pub fn new(
        dimensions: usize,
        reserved_space: usize,
        metric: MetricKind,
        connectivity: usize,
        expansion_add: usize,
        expansion_search: usize,
    ) -> DynResult<USearchKNNIndex> {
        let options = IndexOptions {
            dimensions,
            metric,
            quantization: ScalarKind::F16,
            connectivity,
            expansion_add,
            expansion_search,
            multi: false,
        };

        let index = new_index(&options)?;
        index.reserve(reserved_space)?;

        Ok(USearchKNNIndex {
            index: Arc::from(index),
            key_to_id_mapper: KeyToU64IdMapper::new(),
        })
    }

    fn search_one(&self, data: &[f64], limit: usize) -> DynResult<Vec<KeyScoreMatch>> {
        let matches = self.index.search(data, limit)?;
        Ok(matches
            .keys
            .into_iter()
            .zip(matches.distances)
            .map(|(k, d)| KeyScoreMatch {
                key: self.key_to_id_mapper.get_key_for_id(k),
                score: -f64::from(d),
            })
            .collect())
    }

    fn add_one(&mut self, key: Key, data: &[f64]) -> DynResult<()> {
        let key_id = self.key_to_id_mapper.get_noncolliding_u64_id(key);
        self.index.add(key_id, data)?;
        Ok(())
    }

    fn remove_one(&mut self, key: Key) -> DynResult<()> {
        let key_id = self.key_to_id_mapper.remove_key(key)?;
        self.index.remove(key_id)?;
        Ok(())
    }
}

impl NonFilteringExternalIndex<Vec<f64>, Vec<f64>> for USearchKNNIndex {
    fn add(&mut self, add_data: Vec<(Key, Vec<f64>)>) -> Vec<(Key, DynResult<()>)> {
        if self.index.size() + add_data.len() > self.index.capacity() {
            assert!(self
                .index
                .reserve(max(
                    2 * self.index.capacity(),
                    self.index.size() + add_data.len()
                ))
                .is_ok());
        }

        add_data
            .into_iter()
            .map(|(key, data)| (key, self.add_one(key, &data)))
            .collect()
    }

    fn remove(&mut self, keys: Vec<Key>) -> Vec<(Key, DynResult<()>)> {
        keys.into_iter()
            .map(|key| (key, self.remove_one(key)))
            .collect()
    }

    fn search(
        &self,
        queries: &[(Key, Vec<f64>, usize)],
    ) -> Vec<(Key, DynResult<Vec<KeyScoreMatch>>)> {
        queries
            .iter()
            .map(|(key, data, limit)| (*key, self.search_one(data, *limit)))
            .collect()
    }
}

// index factory structure
pub struct USearchKNNIndexFactory {
    dimensions: usize,
    reserved_space: usize,
    metric: MetricKind,
    connectivity: usize,
    expansion_add: usize,
    expansion_search: usize,
}

impl USearchKNNIndexFactory {
    pub fn new(
        dimensions: usize,
        reserved_space: usize,
        metric: MetricKind,
        connectivity: usize,
        expansion_add: usize,
        expansion_search: usize,
    ) -> USearchKNNIndexFactory {
        USearchKNNIndexFactory {
            dimensions,
            reserved_space,
            metric,
            connectivity,
            expansion_add,
            expansion_search,
        }
    }
}

// implement make_instance method, which then is used to produce instance of the index for each worker / operator
impl ExternalIndexFactory for USearchKNNIndexFactory {
    fn make_instance(&self) -> Result<Box<dyn ExternalIndex>, Error> {
        let u_index = USearchKNNIndex::new(
            self.dimensions,
            self.reserved_space,
            self.metric,
            self.connectivity,
            self.expansion_add,
            self.expansion_search,
        )?;
        Ok(Box::new(DerivedFilteredSearchIndex::new(Box::new(u_index))))
    }
}
