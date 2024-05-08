// Copyright © 2024 Pathway

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
    return_distance: bool,
}

impl USearchKNNIndex {
    pub fn new(
        dimensions: usize,
        reserved_space: usize,
        metric: MetricKind,
        connectivity: usize,
        expansion_add: usize,
        expansion_search: usize,
        return_distance: bool,
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
            return_distance,
        })
    }
}

impl NonFilteringExternalIndex<Vec<f64>, Vec<f64>> for USearchKNNIndex {
    fn add(&mut self, key: Key, data: Vec<f64>) -> DynResult<()> {
        let key_id = self.key_to_id_mapper.get_noncolliding_u64_id(key);
        if self.index.size() + 1 > self.index.capacity() {
            assert!(self.index.reserve(2 * self.index.capacity()).is_ok());
        }
        Ok(self.index.add(key_id, &data)?)
    }

    fn remove(&mut self, key: Key) -> DynResult<()> {
        let key_id = self.key_to_id_mapper.remove_key(key)?;
        self.index.remove(key_id)?;
        Ok(())
    }

    fn search(&self, data: &Vec<f64>, limit: Option<usize>) -> DynResult<Vec<KeyScoreMatch>> {
        let matches = self.index.search(data, limit.unwrap())?;

        if self.return_distance {
            Ok(matches
                .keys
                .into_iter()
                .zip(matches.distances)
                .map(|(k, d)| KeyScoreMatch {
                    key: self.key_to_id_mapper.get_key_for_id(k),
                    score: Some(-f64::from(d)),
                })
                .collect())
        } else {
            Ok(matches
                .keys
                .into_iter()
                .zip(matches.distances)
                .map(|(k, _)| KeyScoreMatch {
                    key: self.key_to_id_mapper.get_key_for_id(k),
                    score: None,
                })
                .collect())
        }
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
    return_distance: bool,
}

impl USearchKNNIndexFactory {
    pub fn new(
        dimensions: usize,
        reserved_space: usize,
        metric: MetricKind,
        connectivity: usize,
        expansion_add: usize,
        expansion_search: usize,
        return_distance: bool,
    ) -> USearchKNNIndexFactory {
        USearchKNNIndexFactory {
            dimensions,
            reserved_space,
            metric,
            connectivity,
            expansion_add,
            expansion_search,
            return_distance,
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
            self.return_distance,
        )?;
        Ok(Box::new(DerivedFilteredSearchIndex::new(Box::new(u_index))))
    }
}
