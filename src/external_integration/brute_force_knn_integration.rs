// Copyright © 2024 Pathway

use itertools::Itertools;
use ndarray::{s, Array2, ArrayView2, Axis};

use crate::engine::error::DynResult;
use crate::engine::{Error, Key};
use ordered_float::{self, OrderedFloat};
use std::cmp::max;

use super::{
    DerivedFilteredSearchIndex, ExternalIndex, ExternalIndexFactory, KeyScoreMatch,
    KeyToU64IdMapper, NonFilteringExternalIndex,
};

#[derive(Clone, Copy, Debug)]
pub enum BruteForceKnnMetricKind {
    L2sq,
    Cos,
}

pub struct BruteForceKNNIndex {
    index_array: Array2<f64>,
    current_size: usize,
    current_allocated: usize,
    minimum_allocated: usize,
    auxiliary_space: usize,
    dimensions: usize,
    metric: BruteForceKnnMetricKind,
    key_to_id_mapper: KeyToU64IdMapper,
}

impl BruteForceKNNIndex {
    pub fn new(
        dimensions: usize,
        reserved_space: usize,
        auxiliary_space: usize,
        metric: BruteForceKnnMetricKind,
    ) -> DynResult<BruteForceKNNIndex> {
        let arr = Array2::default((reserved_space, dimensions));
        Ok(BruteForceKNNIndex {
            index_array: arr,
            current_size: 0,
            current_allocated: reserved_space,
            minimum_allocated: reserved_space,
            auxiliary_space,
            dimensions,
            metric,
            key_to_id_mapper: KeyToU64IdMapper::new(),
        })
    }

    fn fill_distances(
        &self,
        index_arr: &ArrayView2<f64>,
        query_arr: &ArrayView2<f64>,
        dot_p: &mut Array2<f64>,
    ) {
        match self.metric {
            BruteForceKnnMetricKind::L2sq => {
                fill_l2sq_distances(index_arr, query_arr, dot_p);
            }
            BruteForceKnnMetricKind::Cos => {
                fill_cos_distances(index_arr, query_arr, dot_p);
            }
        }
    }
}

fn fill_cos_distances(
    index_arr: &ArrayView2<f64>,
    query_arr: &ArrayView2<f64>,
    dot_p: &mut Array2<f64>,
) {
    let index_sq_norms: Vec<f64> = index_arr
        .axis_iter(Axis(0))
        .map(|row| (row.dot(&row)))
        .collect();

    let query_sq_norms: Vec<f64> = query_arr
        .axis_iter(Axis(1))
        .map(|col| (col.dot(&col)))
        .collect();

    for (i, mut row) in dot_p.axis_iter_mut(Axis(0)).enumerate() {
        for (j, entry) in row.iter_mut().enumerate() {
            *entry = 1.0 - *entry / (index_sq_norms[i] * query_sq_norms[j]).sqrt();
        }
    }
}

fn fill_l2sq_distances(
    index_arr: &ArrayView2<f64>,
    query_arr: &ArrayView2<f64>,
    dot_p: &mut Array2<f64>,
) {
    let index_sq_norms: Vec<f64> = index_arr
        .axis_iter(Axis(0))
        .map(|row| (row.dot(&row)))
        .collect();
    let query_sq_norms: Vec<f64> = query_arr
        .axis_iter(Axis(1))
        .map(|col| (col.dot(&col)))
        .collect();

    for (i, mut row) in dot_p.axis_iter_mut(Axis(0)).enumerate() {
        for (j, entry) in row.iter_mut().enumerate() {
            *entry = index_sq_norms[i] + query_sq_norms[j] - 2.0 * (*entry);
        }
    }
}

impl NonFilteringExternalIndex<Vec<f64>, Vec<f64>> for BruteForceKNNIndex {
    fn add(&mut self, add_data: Vec<(Key, Vec<f64>)>) -> Vec<(Key, DynResult<()>)> {
        if add_data.len() + self.current_size > self.current_allocated {
            let new_allocated = max(
                2 * self.current_allocated,
                add_data.len() + self.current_size,
            );

            let mut new_arr: Array2<f64> = Array2::default((new_allocated, self.dimensions));
            for (dst, src) in new_arr.iter_mut().zip(&self.index_array) {
                *dst = *src;
            }

            self.index_array = new_arr;
            self.current_allocated = new_allocated;
        }

        self.current_size += add_data.len();
        add_data
            .into_iter()
            .map(|(key, data)| {
                let idx = usize::try_from(self.key_to_id_mapper.get_next_free_u64_id(key)).unwrap();
                let mut row = self.index_array.row_mut(idx);

                for (value, input_value) in row.iter_mut().zip(data) {
                    *value = input_value;
                }

                (key, Ok(()))
            })
            .collect()
    }

    fn remove(&mut self, keys: Vec<Key>) -> Vec<(Key, DynResult<()>)> {
        let ret = keys
            .into_iter()
            .map(|key| {
                let last_row_key = self
                    .key_to_id_mapper
                    .get_key_for_id(u64::try_from(self.current_size).unwrap() - 1)
                    .unwrap();
                match self.key_to_id_mapper.remove_key(key) {
                    Ok(removed_key_id) => {
                        self.current_size -= 1;
                        self.key_to_id_mapper.decrement_next_free_id();
                        if last_row_key != key {
                            // if the last row had a different key, put its entry to the removed position
                            let last_row = &self.index_array.row(self.current_size).to_owned();
                            self.index_array
                                .row_mut(usize::try_from(removed_key_id).unwrap())
                                .assign(last_row);
                            self.key_to_id_mapper
                                .assign_key(last_row_key, removed_key_id);
                        }
                        (key, Ok(()))
                    }
                    Err(error) => (key, Err(error)),
                }
            })
            .collect();

        if 4 * self.current_size < self.current_allocated
            && self.current_allocated / 2 >= self.minimum_allocated
        {
            let new_allocated = self.current_allocated / 2;
            let mut new_arr: Array2<f64> = Array2::default((new_allocated, self.dimensions));

            for (dst, src) in new_arr.iter_mut().zip(&self.index_array) {
                *dst = *src;
            }
            self.index_array = new_arr;
            self.current_allocated = new_allocated;
        }
        ret
    }

    fn search(
        &self,
        queries: &[(Key, Vec<f64>, usize)],
    ) -> Vec<(Key, DynResult<Vec<KeyScoreMatch>>)> {
        if self.current_size == 0 {
            return queries
                .iter()
                .map(|(key, _, _)| (*key, Ok(Vec::new())))
                .collect();
        }

        let index_arr = self
            .index_array
            .slice(s![..self.current_size, ..self.dimensions]);

        let mut ret = Vec::with_capacity(queries.len());
        let max_queries = max(1, self.auxiliary_space / self.current_size);
        let query_batches = queries.chunks(max_queries);
        let mut query_arr = Array2::<f64>::default((self.dimensions, max_queries));
        for query_batch in query_batches {
            for (mut col, (_key, data, _k)) in query_arr.axis_iter_mut(Axis(1)).zip(query_batch) {
                for (entry, val) in col.iter_mut().zip(data) {
                    *entry = *val;
                }
            }
            let slice_query_array = query_arr.slice(s![..self.dimensions, ..query_batch.len()]);
            let mut dot_p = index_arr.dot(&slice_query_array);
            self.fill_distances(&index_arr, &slice_query_array, &mut dot_p);

            ret.extend(dot_p.axis_iter(Axis(1)).zip(query_batch).map(
                |(col, (key, _data, limit))| {
                    let result = col
                        .iter()
                        .enumerate()
                        .map(|(idx, x)| (OrderedFloat::from(*x), idx)) //order by distance
                        .k_smallest(*limit)
                        .map(|(distance, i)| KeyScoreMatch {
                            key: self
                                .key_to_id_mapper
                                .get_key_for_id(u64::try_from(i).unwrap())
                                .unwrap(),
                            score: -(*distance),
                        })
                        .collect();
                    (*key, Ok(result))
                },
            ));
        }
        ret
    }
}

pub struct BruteForceKNNIndexFactory {
    dimensions: usize,
    reserved_space: usize,
    auxiliary_space: usize,
    metric: BruteForceKnnMetricKind,
}

impl BruteForceKNNIndexFactory {
    pub fn new(
        dimensions: usize,
        reserved_space: usize,
        auxiliary_space: usize,
        metric: BruteForceKnnMetricKind,
    ) -> BruteForceKNNIndexFactory {
        BruteForceKNNIndexFactory {
            dimensions,
            reserved_space,
            auxiliary_space,
            metric,
        }
    }
}

impl ExternalIndexFactory for BruteForceKNNIndexFactory {
    fn make_instance(&self) -> Result<Box<dyn ExternalIndex>, Error> {
        let u_index = BruteForceKNNIndex::new(
            self.dimensions,
            self.reserved_space,
            self.auxiliary_space,
            self.metric,
        )?;
        Ok(Box::new(DerivedFilteredSearchIndex::new(Box::new(u_index))))
    }
}
