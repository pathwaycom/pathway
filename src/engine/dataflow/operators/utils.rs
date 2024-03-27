// Copyright © 2024 Pathway

use differential_dataflow::difference::{Abelian, Semigroup};
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::cursor::Cursor;
use differential_dataflow::trace::implementations::ord::{OrdValBatch, OrdValBuilder};
use differential_dataflow::trace::{BatchReader, Builder};
use std::cmp::Ord;
use std::collections::BTreeMap;
use timely::progress::{Antichain, Timestamp};
/// This struct allows to sort entries by key, val, time, before building a batch
/// it is essentially a buffer than later calls the DD batch builder
/// possible improvement: write a batch builder that allows sorting entries without buffering them
pub(crate) struct SortingBatchBuilder<K, V, T, R> {
    buff: Vec<(K, V, T, R)>,
}

impl<
        K: Ord + Clone + 'static,
        V: Ord + Clone + 'static,
        T: Lattice + timely::progress::Timestamp,
        R: Clone + Abelian,
    > SortingBatchBuilder<K, V, T, R>
{
    pub(crate) fn with_capacity(cap: usize) -> Self {
        SortingBatchBuilder {
            buff: Vec::<(K, V, T, R)>::with_capacity(cap),
        }
    }
    pub(crate) fn push(&mut self, entry: (K, V, T, R)) {
        self.buff.push(entry);
    }

    pub(crate) fn done(
        mut self,
        lower: Antichain<T>,
        upper: Antichain<T>,
        since: Antichain<T>,
    ) -> OrdValBatch<K, V, T, R, usize> {
        let mut inner_batch_builder =
            OrdValBuilder::<K, V, T, R, usize>::with_capacity(self.buff.len());
        self.buff
            .sort_by(|a, b| (&a.0, &a.1, &a.2).cmp(&(&b.0, &b.1, &b.2)));
        for entry in self.buff {
            inner_batch_builder.push(entry);
        }
        inner_batch_builder.done(lower, upper, since)
    }
}

/// This struct is a wrapper around cursor and its storage. These two are always used together,
/// and in some places we need three such pairs at the same time. Using this wrapper makes passing
/// cursor storage pairs around easier.
pub(crate) struct CursorStorageWrapper<'a, C: Cursor> {
    pub(crate) cursor: &'a mut C,
    pub(crate) storage: &'a C::Storage,
}

/// Returns the total weight associated with the current position [i.e. a fixed (key, val) pair in storage] of
/// the cursor passed in wrapper.
pub(crate) fn key_val_total_weight<C: Cursor>(wrapper: &mut CursorStorageWrapper<C>) -> Option<C::R>
where
    C::R: Semigroup,
{
    let mut ret: Option<C::R> = None;

    wrapper.cursor.map_times(wrapper.storage, |_t, diff| {
        if ret.is_none() {
            ret = Some(diff.clone());
        } else {
            let mut val = ret.clone().unwrap();
            val.plus_equals(diff);
            ret = Some(val);
        }
    });
    ret
}

/// Reorganizes a set of batches into set of vectors, each vector corresponding
/// to updates from a specific time. While doing so, it applies logic to each
/// (key, val, time, diff) tuple.
pub(crate) fn batch_by_time<B, O>(
    input: &[B],
    mut logic: impl FnMut(&B::Key, &B::Val, &B::Time, &B::R) -> O,
) -> BTreeMap<B::Time, Vec<O>>
where
    B: BatchReader,
    B::Time: Timestamp,
{
    let mut ret = BTreeMap::new();
    for batch in input {
        let mut cursor = batch.cursor();
        while let Some(key) = cursor.get_key(batch) {
            while let Some(val) = cursor.get_val(batch) {
                cursor.map_times(batch, |time, diff| {
                    if !ret.contains_key(time) {
                        ret.insert(time.clone(), Vec::new());
                    }
                    let state = ret.get_mut(time).unwrap();
                    state.push(logic(key, val, time, diff));
                });
                cursor.step_val(batch);
            }
            cursor.step_key(batch);
        }
    }
    ret
}
