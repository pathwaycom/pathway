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
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
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
        1 // currently there is no support for sharding by instance, we need to centralize buffer to keep column time consistent for all entries in one instance
    }
}

impl<'a, S, PT: Timestamp + Lattice> MaybeTotalScope for Child<'a, S, SelfCompactionTime<PT>>
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

impl<T: Timestamp> PathSummary<SelfCompactionTime<T>> for SelfCompactionTime<T::Summary> {
    fn results_in(&self, timestamp: &SelfCompactionTime<T>) -> Option<SelfCompactionTime<T>> {
        self.time
            .results_in(&timestamp.time)
            .map(|val| SelfCompactionTime {
                time: val,
                retraction: self.retraction || timestamp.retraction,
            })
    }

    fn followed_by(&self, other: &Self) -> Option<Self> {
        self.time
            .followed_by(&other.time)
            .map(|val| SelfCompactionTime {
                time: val,
                retraction: self.retraction || other.retraction,
            })
    }
}

impl<T: Timestamp> Timestamp for SelfCompactionTime<T> {
    type Summary = SelfCompactionTime<T::Summary>;
    fn minimum() -> Self {
        SelfCompactionTime::original(T::minimum())
    }
}

impl<T: Timestamp> Refines<T> for SelfCompactionTime<T> {
    fn to_inner(other: T) -> Self {
        SelfCompactionTime::original(other)
    }
    fn to_outer(self: SelfCompactionTime<T>) -> T {
        self.time
    }
    fn summarize(path: SelfCompactionTime<T::Summary>) -> T::Summary {
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

impl Epsilon for u64 {
    fn epsilon() -> u64 {
        1
    }
}

pub trait TimeColumnSortable<
    G: Scope,
    K: ExchangeData + Shard,
    V: ExchangeData,
    R: ExchangeData + Abelian,
    CT: Ord + ExchangeData,
    CTE,
>
{
    fn prepend_time_to_key(
        &self,
        time_column_extractor: CTE,
    ) -> Collection<G, (TimeKey<CT, K>, V), R>
    where
        CTE: Fn(&V) -> CT + 'static;
}

impl<G, K, V, R, CT, CTE> TimeColumnSortable<G, K, V, R, CT, CTE> for Collection<G, (K, V), R>
where
    G: Scope + MaybeTotalScope,
    K: ExchangeData + Shard,
    V: ExchangeData,
    CT: ExchangeData,
    R: ExchangeData + Abelian,
    CTE: Fn(&V) -> CT + 'static,
{
    fn prepend_time_to_key(
        &self,
        time_column_extractor: CTE,
    ) -> Collection<G, (TimeKey<CT, K>, V), R> {
        self.map_ex(move |(k, v)| {
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
    K: ExchangeData + Shard,
    V: ExchangeData,
    R: ExchangeData + Abelian,
    CT: Ord + ExchangeData,
    TTE,
    CTE,
>
{
    fn postpone(
        &self,
        scope: G,
        threshold_time_extractor: TTE,
        current_time_extractor: CTE,
    ) -> Collection<G, (K, V), R>
    where
        G::Timestamp:
            Timestamp<Summary = G::Timestamp> + Default + PathSummary<G::Timestamp> + Epsilon,
        TTE: Fn(&V) -> CT + 'static,
        CTE: Fn(&V) -> CT + 'static;
}

impl<G, K, V, R, CT, TTE, CTE> TimeColumnBuffer<G, K, V, R, CT, TTE, CTE>
    for Collection<G, (K, V), R>
where
    G: Scope + MaybeTotalScope,
    G::Timestamp: Lattice,
    K: ExchangeData + Shard,
    V: ExchangeData,
    CT: ExchangeData,
    R: ExchangeData + Abelian,
    TimeKey<CT, K>: Hashable,
{
    fn postpone(
        &self,
        mut scope: G,
        threshold_time_extractor: TTE,
        current_time_extractor: CTE,
    ) -> Collection<G, (K, V), R>
    where
        G::Timestamp:
            Timestamp<Summary = G::Timestamp> + Default + PathSummary<G::Timestamp> + Epsilon,
        TTE: Fn(&V) -> CT + 'static,
        CTE: Fn(&V) -> CT + 'static,
    {
        scope.scoped::<SelfCompactionTime<G::Timestamp>, _, _>("buffer timespace", |inner| {
            let retractions = Variable::new(
                inner,
                SelfCompactionTime::retraction(<G::Timestamp as Epsilon>::epsilon()),
            );
            let ret_in_buffer_timespace_col = postpone_core(
                self.enter(inner)
                    .concat(&retractions.negate())
                    .prepend_time_to_key(threshold_time_extractor)
                    .arrange(),
                current_time_extractor,
            );
            retractions.set(&ret_in_buffer_timespace_col);
            ret_in_buffer_timespace_col.leave()
        })
    }
}

fn push_key_values_to_output<K, C: Cursor, P>(
    wrapper: &mut CursorStorageWrapper<C>,
    output: &mut OutputHandle<'_, C::Time, ((K, C::Val), C::Time, C::R), P>,
    capability: &CapabilityRef<'_, C::Time>,
    k: &K,
    time: &Option<C::Time>,
    weight_fun: &'static impl Fn(&mut CursorStorageWrapper<C>) -> Option<C::R>,
) where
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
        let weight = weight_fun(wrapper);

        //rust can't do if let Some(weight) = weight && !weight.is_zero()
        //while we can do if let ... { if !weight.is_zero()} that introduces if-s
        //that can be collapsed, which is bad as well
        #[allow(clippy::unnecessary_unwrap)]
        let curr_val = wrapper.cursor.val(wrapper.storage);
        if weight.is_some() && !weight.clone().unwrap().is_zero() {
            let time = time.as_ref().unwrap();
            assert!(time >= capability.time());

            output.session(&capability).give((
                (k.clone(), curr_val.clone()),
                time.clone(),
                weight.clone().unwrap(),
            ));
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
    CTE,
>(
    mut input_arrangement: KeyValArr<G, TimeKey<CT, K>, V, R>,
    current_time_extractor: CTE,
) -> Collection<G, (K, V), R>
where
    G: Scope,
    OT: Timestamp + Lattice + Ord,
    G::Timestamp: Lattice + Ord,
    CT: ExchangeData,
    K: ExchangeData,
    V: ExchangeData,
    CTE: Fn(&V) -> CT + 'static,
{
    let stream = {
        input_arrangement
            .stream
            .unary(Pipeline, "buffer", move |_capability, _operator_info| {
                let mut input_buffer = Vec::new();
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
                                let current_column_time = current_time_extractor(
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
                                    &key_val_total_original_weight,
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
                                let found = input_wrapper.cursor.get_key(input_wrapper.storage);
                                if found.is_some()
                                    && found.as_ref().unwrap()
                                        == &local_last_arrangement_key.as_ref().unwrap()
                                {
                                    input_wrapper.cursor.step_key(input_wrapper.storage);
                                }
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
                                    &key_val_total_weight,
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

                        input_arrangement.trace.advance_upper(&mut upper_limit);

                        input_arrangement
                            .trace
                            .set_logical_compaction(upper_limit.borrow());
                        input_arrangement
                            .trace
                            .set_physical_compaction(upper_limit.borrow());
                    });
                }
            })
    };
    Collection { inner: stream }
}

pub trait TimeColumnForget<
    G: Scope,
    K: ExchangeData + Shard,
    V: ExchangeData,
    R: ExchangeData + Abelian,
    CT: Ord + ExchangeData,
    TTE,
    CTE,
>
{
    fn forget(
        &self,
        threshold_time_extractor: TTE,
        current_time_extractor: CTE,
    ) -> Collection<G, (K, V), R>
    where
        G::Timestamp: Timestamp<Summary = G::Timestamp> + PathSummary<G::Timestamp> + Epsilon,
        TTE: Fn(&V) -> CT + 'static,
        CTE: Fn(&V) -> CT + 'static;
}

impl<G, K, V, R, CT, TTE, CTE> TimeColumnForget<G, K, V, R, CT, TTE, CTE>
    for Collection<G, (K, V), R>
where
    G: Scope + MaybeTotalScope,
    G::Timestamp: Lattice,
    K: ExchangeData + Shard,
    V: ExchangeData,
    CT: ExchangeData,
    R: ExchangeData + Abelian,
    TimeKey<CT, K>: Hashable,
{
    fn forget(
        &self,
        threshold_time_extractor: TTE,
        current_time_extractor: CTE,
    ) -> Collection<G, (K, V), R>
    where
        G::Timestamp: Timestamp<Summary = G::Timestamp> + PathSummary<G::Timestamp> + Epsilon,
        TTE: Fn(&V) -> CT + 'static,
        CTE: Fn(&V) -> CT + 'static,
    {
        self.concat(&self.negate().postpone(
            self.scope(),
            threshold_time_extractor,
            current_time_extractor,
        ))
    }
}
pub trait TimeColumnFreeze<
    G: Scope,
    K: ExchangeData + Shard,
    V: ExchangeData,
    R: ExchangeData + Abelian,
    CT: Ord + ExchangeData,
    CTE,
    TTE,
>
{
    fn freeze(
        &self,
        threshold_time_extractor: TTE,
        current_column_extractor: CTE,
    ) -> (Collection<G, (K, V), R>, Collection<G, (K, V), R>)
    where
        G::Timestamp: Timestamp<Summary = G::Timestamp> + PathSummary<G::Timestamp> + Epsilon,
        TTE: Fn(&V) -> CT + 'static,
        CTE: Fn(&V) -> CT + 'static;
}

impl<G, K, V, R, CT, CTE, TTE> TimeColumnFreeze<G, K, V, R, CT, CTE, TTE>
    for Collection<G, (K, V), R>
where
    G: Scope + MaybeTotalScope,
    G::Timestamp: Lattice,
    K: ExchangeData + Shard,
    V: ExchangeData,
    CT: ExchangeData,
    R: ExchangeData + Abelian,
    TimeKey<CT, K>: Hashable,
{
    fn freeze(
        &self,
        threshold_time_extractor: TTE,
        current_time_extractor: CTE,
    ) -> (Collection<G, (K, V), R>, Collection<G, (K, V), R>)
    where
        G::Timestamp: Timestamp<Summary = G::Timestamp> + PathSummary<G::Timestamp> + Epsilon,
        CTE: Fn(&V) -> CT + 'static,
        TTE: Fn(&V) -> CT + 'static,
    {
        ignore_late(self, threshold_time_extractor, current_time_extractor)
    }
}

#[allow(clippy::too_many_lines)]
pub fn ignore_late<
    G: ScopeParent,
    CT: ExchangeData,
    K: ExchangeData + Shard,
    V,
    R: ExchangeData + Abelian + Diff,
    CTE,
    TTE,
>(
    input_collection: &Collection<G, (K, V), R>,
    threshold_time_extractor: TTE,
    current_time_extractor: CTE,
) -> (Collection<G, (K, V), R>, Collection<G, (K, V), R>)
where
    G: Scope,
    G::Timestamp: Lattice + Ord,
    CT: ExchangeData,
    K: ExchangeData,
    V: ExchangeData,
    TTE: Fn(&V) -> CT + 'static,
    CTE: Fn(&V) -> CT + 'static,
{
    let mut builder =
        OperatorBuilder::new("ignore_late".to_owned(), input_collection.inner.scope());

    let mut input = builder.new_input(&input_collection.inner, Pipeline);
    let (mut output, stream) = builder.new_output();
    let (mut late_output, late_stream) = builder.new_output();

    builder.build(move |_| {
        let mut input_buffer = Vec::new();
        let mut max_column_time: Option<CT> = None;
        move |_frontiers| {
            let mut output_handle = output.activate();
            let mut late_output_handle = late_output.activate();
            input.for_each(|capability, batch| {
                batch.swap(&mut input_buffer);
                let mut max_curr_time = None;
                for ((_key, val), time, _weight) in &input_buffer {
                    let candidate_column_time = current_time_extractor(val);
                    if max_column_time.is_none()
                        || &candidate_column_time > max_column_time.as_ref().unwrap()
                    {
                        max_column_time = Some(candidate_column_time.clone());
                    }
                    //for complex handling this will need some changes, to accommodate anitchains
                    if max_curr_time.is_none() || max_curr_time.as_ref().unwrap() < time {
                        max_curr_time = Some(time.clone());
                    };
                }

                for entry in input_buffer.drain(..) {
                    let ((_key, val), _time, _weight) = &entry;
                    let threshold = threshold_time_extractor(val);
                    if max_column_time.as_ref().unwrap() < &threshold {
                        output_handle.session(&capability).give(entry);
                    } else {
                        late_output_handle.session(&capability).give(entry);
                    }
                }
            });
        }
    });

    (
        Collection { inner: stream },
        Collection { inner: late_stream },
    )
}
