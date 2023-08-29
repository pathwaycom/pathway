use super::ArrangeWithTypes;
use crate::engine::dataflow::maybe_total::MaybeTotalScope;
use crate::engine::dataflow::operators::utils::{key_val_total_weight, CursorStorageWrapper};
use crate::engine::dataflow::operators::MapWrapped;
use crate::engine::dataflow::Shard;
use differential_dataflow::difference::{Abelian, Semigroup};
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::{Arranged, TraceAgent};
use differential_dataflow::operators::iterate::Variable;
use differential_dataflow::trace::cursor::{Cursor, CursorList};
use differential_dataflow::trace::implementations::ord::OrdValBatch;
use differential_dataflow::trace::implementations::spine_fueled::Spine;
use differential_dataflow::trace::{BatchReader, TraceReader};
use differential_dataflow::{Collection, Data, Diff, ExchangeData, Hashable};
use serde::{Deserialize, Serialize};
use std::cmp::{max, Ord};
use std::rc::Rc;
use timely::communication::Push;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::OutputHandle;
use timely::dataflow::operators::CapabilityRef;
use timely::dataflow::operators::Operator;
use timely::dataflow::scopes::{Child, Scope, ScopeParent};
use timely::order::TotalOrder;
use timely::progress::timestamp::Refines;
use timely::progress::Antichain;
use timely::progress::{PathSummary, Timestamp};

type KeyValArr<G, K, V, R> =
    Arranged<G, TraceAgent<Spine<Rc<OrdValBatch<K, V, <G as ScopeParent>::Timestamp, R>>>>>;
#[derive(Debug, Default, Hash, Clone, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct TimeKey<CT, K> {
    pub time: CT,
    pub key: K,
}
impl<CT, K> Shard for TimeKey<CT, K>
where
    K: ExchangeData + Shard,
{
    fn shard(&self) -> u64 {
        self.key.shard()
    }
}

impl<'a, S, PT: Timestamp<Summary = PT> + PathSummary<PT> + Lattice> MaybeTotalScope
    for Child<'a, S, SelfCompactionTime<PT>>
where
    S: ScopeParent<Timestamp = PT>,
{
    type MaybeTotalTimestamp = Self::Timestamp;
}

#[derive(Debug, Default, Hash, Clone, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct SelfCompactionTime<T> {
    pub time: T,
    pub retraction: bool, // original < retraction in timestamp comparisons.
}

impl<T> SelfCompactionTime<T> {
    pub fn original(time: T) -> Self {
        SelfCompactionTime {
            time,
            retraction: false,
        }
    }
    pub fn retraction(time: T) -> Self {
        SelfCompactionTime {
            time,
            retraction: true,
        }
    }
}

impl<T: PartialOrder> TotalOrder for SelfCompactionTime<T> {}
// Implement timely dataflow's `PartialOrder` trait.
use timely::order::PartialOrder;
impl<T: PartialOrder> PartialOrder for SelfCompactionTime<T> {
    fn less_equal(&self, other: &Self) -> bool {
        if self.time.eq(&other.time) {
            self.retraction <= other.retraction
        } else {
            self.time.less_equal(&other.time)
        }
    }
}

// Implement timely dataflow's `PathSummary` trait.
// This is preparation for the `Timestamp` implementation below.
impl<T: Timestamp<Summary = T> + PathSummary<T> + Default> PathSummary<SelfCompactionTime<T>>
    for SelfCompactionTime<T>
{
    fn results_in(&self, timestamp: &SelfCompactionTime<T>) -> Option<SelfCompactionTime<T>> {
        self.time
            .results_in(&timestamp.clone().to_outer())
            .map(|val| SelfCompactionTime {
                time: val,
                retraction: true,
            })
    }

    fn followed_by(&self, other: &Self) -> Option<Self> {
        Some(other.clone())
    }
}

// Implement timely dataflow's `Timestamp` trait.

impl<T: Timestamp<Summary = T> + Default + PathSummary<T>> Timestamp for SelfCompactionTime<T> {
    type Summary = SelfCompactionTime<T>;
    fn minimum() -> Self {
        SelfCompactionTime::original(T::minimum())
    }
}

impl<T: Timestamp<Summary = T> + Default + PathSummary<T>> Refines<T> for SelfCompactionTime<T> {
    fn to_inner(other: T) -> Self {
        SelfCompactionTime::original(other)
    }
    fn to_outer(self: SelfCompactionTime<T>) -> T {
        self.time
    }
    fn summarize(path: SelfCompactionTime<T>) -> <T as Timestamp>::Summary {
        path.time
    }
}

impl Refines<()> for SelfCompactionTime<i32> {
    fn to_inner(other: ()) -> Self {
        SelfCompactionTime::original(<i32 as Refines<()>>::to_inner(other))
    }
    fn to_outer(self: SelfCompactionTime<i32>) {}

    fn summarize(_path: SelfCompactionTime<i32>) {}
}

// Implement differential dataflow's `Lattice` trait.
// This extends the `PartialOrder` implementation with additional structure.
impl<T: Lattice> Lattice for SelfCompactionTime<T> {
    fn join(&self, other: &Self) -> Self {
        let time = self.time.join(&other.time);
        let mut retraction = false;
        if time == self.time {
            retraction = retraction || self.retraction;
        }
        if time == other.time {
            retraction = retraction || other.retraction;
        }
        SelfCompactionTime { time, retraction }
    }
    fn meet(&self, other: &Self) -> Self {
        let time = self.time.meet(&other.time);
        let mut retraction = true;
        if time == self.time {
            retraction = retraction && self.retraction;
        }
        if time == other.time {
            retraction = retraction && other.retraction;
        }
        SelfCompactionTime { time, retraction }
    }
}

fn key_val_total_original_weight<T: Timestamp, C: Cursor<Time = SelfCompactionTime<T>>>(
    wrapper: &mut CursorStorageWrapper<C>,
) -> Option<C::R>
where
    C::R: Semigroup,
{
    let mut ret: Option<C::R> = None;

    wrapper.cursor.map_times(wrapper.storage, |t, diff| {
        if !t.retraction {
            if ret.is_none() {
                ret = Some(diff.clone());
            } else {
                let mut val = ret.clone().unwrap();
                val.plus_equals(diff);
                ret = Some(val);
            }
        }
    });
    ret
}

pub trait Epsilon {
    fn epsilon() -> Self;
}

impl Epsilon for i32 {
    fn epsilon() -> i32 {
        1
    }
}

pub trait TimeColumnSortable<
    G: Scope,
    K: ExchangeData + Abelian + Shard,
    V: ExchangeData + Abelian,
    R: ExchangeData + Abelian,
    CT: Abelian + Ord + ExchangeData,
>
{
    fn prepend_time_to_key(
        &self,
        time_column_extractor: &'static impl Fn(&V) -> CT,
    ) -> Collection<G, (TimeKey<CT, K>, V), R>;
}

impl<G, K, V, R, CT> TimeColumnSortable<G, K, V, R, CT> for Collection<G, (K, V), R>
where
    G: Scope + MaybeTotalScope,
    K: ExchangeData + Abelian + Shard,
    V: ExchangeData + Abelian,
    CT: ExchangeData + Abelian,
    R: ExchangeData + Abelian,
{
    // For each (data, Alt(time), diff) we add a (data, Neu(time), -diff).
    fn prepend_time_to_key(
        &self,
        time_column_extractor: &'static impl Fn(&V) -> CT,
    ) -> Collection<G, (TimeKey<CT, K>, V), R> {
        self.map_ex(|(k, v)| {
            (
                TimeKey {
                    time: time_column_extractor(&v),
                    key: k,
                },
                v,
            )
        })
    }
}

pub trait TimeColumnBuffer<
    G: Scope,
    K: ExchangeData + Abelian + Shard,
    V: ExchangeData + Abelian,
    R: ExchangeData + Abelian,
    CT: Abelian + Ord + ExchangeData,
>
{
    fn postpone(
        &self,
        scope: G,
        threshold_time_column_extractor: &'static impl Fn(&V) -> CT,
        current_time_column_extractor: &'static impl Fn(&V) -> CT,
    ) -> Collection<G, (K, V), R>
    where
        G::Timestamp:
            Timestamp<Summary = G::Timestamp> + Default + PathSummary<G::Timestamp> + Epsilon;
}

impl<G, K, V, R, CT> TimeColumnBuffer<G, K, V, R, CT> for Collection<G, (K, V), R>
where
    G: Scope + MaybeTotalScope,
    G::Timestamp: Lattice,
    K: ExchangeData + Abelian + Shard,
    V: ExchangeData + Abelian,
    CT: ExchangeData + Abelian,
    R: ExchangeData + Abelian,
    TimeKey<CT, K>: Hashable,
{
    fn postpone(
        &self,
        mut scope: G,
        threshold_time_column_extractor: &'static impl Fn(&V) -> CT,
        current_time_column_extractor: &'static impl Fn(&V) -> CT,
    ) -> Collection<G, (K, V), R>
    where
        G::Timestamp:
            Timestamp<Summary = G::Timestamp> + Default + PathSummary<G::Timestamp> + Epsilon,
    {
        scope.scoped::<SelfCompactionTime<G::Timestamp>, _, _>("buffer timespace", |inner| {
            let retractions = Variable::new(
                inner,
                SelfCompactionTime::retraction(<G::Timestamp as Epsilon>::epsilon()),
            );
            let ret_in_buffer_timespace_col = postpone_core(
                self.enter(inner)
                    .concat(&retractions.negate())
                    .prepend_time_to_key(threshold_time_column_extractor)
                    .arrange(),
                current_time_column_extractor,
            );
            retractions.set(&ret_in_buffer_timespace_col);
            ret_in_buffer_timespace_col.leave()
        })
    }
}

fn push_key_values_to_output<T, K, C: Cursor<Time = SelfCompactionTime<T>>, P>(
    wrapper: &mut CursorStorageWrapper<C>,
    output: &mut OutputHandle<'_, C::Time, ((K, C::Val), C::Time, C::R), P>,
    capability: &CapabilityRef<'_, C::Time>,
    k: &K,
    time: &Option<C::Time>,
    total_weight: bool,
) where
    T: Timestamp + Lattice + Clone,
    K: Ord + Clone + Data + 'static,
    C::Val: Ord + Clone + Data + 'static,
    C::Time: Lattice + timely::progress::Timestamp,
    C::R: Clone + Abelian,
    P: Push<
        timely::communication::Message<
            timely::dataflow::channels::Message<
                C::Time,
                std::vec::Vec<((K, C::Val), C::Time, C::R)>,
            >,
        >,
    >,
{
    while wrapper.cursor.val_valid(wrapper.storage) {
        let weight = if total_weight {
            key_val_total_weight(wrapper)
        } else {
            key_val_total_original_weight(wrapper)
        };
        //rust can't do if let Some(weight) = weight && !weight.is_zero()
        //while we can do if let ... { if !weight.is_zero()} that introduces if-s
        //that can be collapsed, which is bad as well
        #[allow(clippy::unnecessary_unwrap)]
        if weight.is_some() && !weight.clone().unwrap().is_zero() {
            let curr_val = wrapper.cursor.val(wrapper.storage);
            let time = time.as_ref().unwrap();
            output.session(&capability).give((
                (k.clone(), curr_val.clone()),
                time.clone(),
                weight.unwrap(),
            ));
        } else {
            eprintln!("seen non forwardable entry {k:?} with weight {weight:?}");
        }
        wrapper.cursor.step_val(wrapper.storage);
    }
}

//TODO: maybe we can separate out some blocks to remove the allow(clippy::too_many_lines)
// perhaps could be done while implementing support for complex times
#[allow(clippy::too_many_lines)]
pub fn postpone_core<
    OT: Timestamp<Summary = OT> + Default + PathSummary<OT>,
    G: ScopeParent<Timestamp = SelfCompactionTime<OT>>,
    CT: ExchangeData,
    K: ExchangeData + Shard,
    V,
    R: ExchangeData + Abelian + Diff,
>(
    mut input_arrangement: KeyValArr<G, TimeKey<CT, K>, V, R>,
    current_time_column_extractor: &'static impl Fn(&V) -> CT,
    // ) -> KeyValArr<G, K, V, R>
) -> Collection<G, (K, V), R>
where
    G: Scope,
    OT: Timestamp + Lattice + Ord,
    G::Timestamp: Lattice + Ord,
    CT: ExchangeData + Abelian,
    K: ExchangeData + Abelian,
    V: ExchangeData + Abelian,
{
    let stream = {
        input_arrangement
            .stream
            .unary(Pipeline, "buffer", move |_capability, _operator_info| {
                let mut input_buffer = Vec::new();
                let mut source_trace = input_arrangement.trace.clone();
                let mut max_column_time: Option<CT> = None;
                let mut last_arrangement_key: Option<TimeKey<CT, K>> = None;
                move |input, output| {
                    input.for_each(|capability, batches| {
                        batches.swap(&mut input_buffer);

                        let mut batch_cursor_list = Vec::new();
                        let mut batch_storage_list = Vec::new();
                        let mut upper_limit = Antichain::from_elem(G::Timestamp::minimum());

                        for batch in input_buffer.drain(..) {
                            upper_limit.clone_from(batch.upper());
                            batch_cursor_list.push(batch.cursor());
                            batch_storage_list.push(batch);
                        }

                        let (mut cursor, storage) = input_arrangement.trace.cursor();

                        let mut batch_wrapper = CursorStorageWrapper {
                            cursor: &mut CursorList::new(batch_cursor_list, &batch_storage_list),
                            storage: &batch_storage_list,
                        };

                        let mut input_wrapper = CursorStorageWrapper {
                            cursor: &mut cursor,
                            storage: &storage,
                        };

                        batch_wrapper.cursor.rewind_keys(batch_wrapper.storage);
                        let mut local_max_column_time = max_column_time.clone();
                        let mut local_last_arrangement_key = last_arrangement_key.clone();

                        let mut max_curr_time = None;
                        while batch_wrapper.cursor.key_valid(batch_wrapper.storage) {
                            while batch_wrapper.cursor.val_valid(batch_wrapper.storage) {
                                batch_wrapper.cursor.map_times(
                                    batch_wrapper.storage,
                                    |t, _diff| {
                                        if !t.retraction
                                            && (max_curr_time.is_none()
                                                || max_curr_time.as_ref().unwrap() < t)
                                        {
                                            max_curr_time = Some(t.clone());
                                        };
                                    },
                                );
                                let current_column_time = current_time_column_extractor(
                                    batch_wrapper.cursor.val(batch_wrapper.storage),
                                );

                                if let Some(time) = local_max_column_time {
                                    local_max_column_time =
                                        Some(max(current_column_time.clone(), time));
                                } else {
                                    local_max_column_time = Some(current_column_time.clone());
                                }
                                batch_wrapper.cursor.step_val(batch_wrapper.storage);
                            }
                            batch_wrapper.cursor.step_key(batch_wrapper.storage);
                        }

                        batch_wrapper.cursor.rewind_keys(batch_wrapper.storage);

                        while batch_wrapper.cursor.key_valid(batch_wrapper.storage) {
                            let tk = batch_wrapper.cursor.key(batch_wrapper.storage);
                            let (t, k) = (&tk.time, &tk.key);
                            //pass entries that are late for buffering
                            if max_column_time.is_some() && max_column_time.as_ref().unwrap() >= t {
                                push_key_values_to_output(
                                    &mut batch_wrapper,
                                    output,
                                    &capability,
                                    k,
                                    &max_curr_time,
                                    false,
                                );
                                local_last_arrangement_key =
                                    local_last_arrangement_key.map(|timekey: TimeKey<CT, K>| {
                                        if &timekey > tk {
                                            timekey
                                        } else {
                                            tk.clone()
                                        }
                                    });
                            }
                            batch_wrapper.cursor.step_key(batch_wrapper.storage);
                        }

                        // going over input arrangement has three parts
                        // 1. entries already emitted (skipped by seek_key)
                        // 2. entries to keep for later
                        // 3. entries to emit while processing this batch
                        if local_max_column_time.is_some() {
                            input_wrapper.cursor.rewind_keys(input_wrapper.storage);
                            if local_last_arrangement_key.is_some() {
                                input_wrapper.cursor.seek_key(
                                    input_wrapper.storage,
                                    local_last_arrangement_key.as_ref().unwrap(),
                                );
                                input_wrapper.cursor.step_key(input_wrapper.storage);
                            }

                            while input_wrapper.cursor.key_valid(input_wrapper.storage) {
                                let tk = input_wrapper.cursor.key(input_wrapper.storage);
                                let (t, k) = (&tk.time, &tk.key);

                                //if threshold is larger than current batch 'now', all subsequent entries
                                //will have too large threshold to be emitted
                                if t > local_max_column_time.as_ref().unwrap() {
                                    break;
                                }
                                local_last_arrangement_key = Some(tk.clone());
                                push_key_values_to_output(
                                    &mut input_wrapper,
                                    output,
                                    &capability,
                                    k,
                                    &max_curr_time,
                                    true,
                                );
                                input_wrapper.cursor.step_key(input_wrapper.storage);
                            }
                        }

                        max_column_time = match (local_max_column_time, max_column_time.clone()) {
                            (None, None) => None,
                            (None, Some(val)) | (Some(val), None) => Some(val),
                            (Some(val1), Some(val2)) => Some(max(val1, val2)),
                        };

                        last_arrangement_key = local_last_arrangement_key;

                        source_trace.advance_upper(&mut upper_limit);

                        source_trace.set_logical_compaction(upper_limit.borrow());
                        source_trace.set_physical_compaction(upper_limit.borrow());
                    });
                }
            })
    };
    Collection { inner: stream }
}
