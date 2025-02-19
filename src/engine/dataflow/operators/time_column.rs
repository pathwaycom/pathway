// Copyright © 2024 Pathway

use super::utils::batch_by_time;
use super::{ArrangeWithTypes, ArrangeWithTypesSharded};
use crate::engine::dataflow::maybe_total::MaybeTotalScope;
use crate::engine::dataflow::operators::utils::{key_val_total_weight, CursorStorageWrapper};
use crate::engine::dataflow::operators::MapWrapped;
use crate::engine::dataflow::{ArrangedBySelf, Shard};
use crate::engine::error::Result;
use crate::engine::{OriginalOrRetraction, Timestamp};
use differential_dataflow::difference::{Abelian, Semigroup};
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::{Arranged, TraceAgent};
use differential_dataflow::operators::iterate::Variable;
use differential_dataflow::trace::cursor::Cursor;
use differential_dataflow::trace::implementations::ord::OrdValBatch;
use differential_dataflow::trace::implementations::spine_fueled::Spine;
use differential_dataflow::trace::{BatchReader, TraceReader};
use differential_dataflow::{AsCollection, Collection, Data, Diff, ExchangeData, Hashable};
use serde::{Deserialize, Serialize};
use std::cmp::Ord;
use std::collections::hash_map::{Entry, Keys};
use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::rc::Rc;
use timely::communication::Push;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::generic::OutputHandle;
use timely::dataflow::operators::{Capability, Inspect, Operator};
use timely::dataflow::scopes::{Scope, ScopeParent};
use timely::progress::frontier::AntichainRef;
use timely::progress::Antichain;
use timely::progress::Timestamp as TimestampTrait;
type KeyValArr<G, K, V, R> =
    Arranged<G, TraceAgent<Spine<Rc<OrdValBatch<K, V, <G as ScopeParent>::Timestamp, R>>>>>;

#[derive(Debug, Default, Hash, Clone, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct TimeKey<I, CT, K> {
    pub instance: I,
    pub time: CT,
    pub key: K,
}

impl<I, CT, K> Shard for TimeKey<I, CT, K>
where
    K: ExchangeData + Shard,
{
    fn shard(&self) -> u64 {
        1 // currently there is no support for sharding by instance, we need to centralize buffer to keep column time consistent for all entries in one instance
    }
}

pub trait Epsilon: TimestampTrait {
    fn epsilon() -> Self::Summary;
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
    I: ExchangeData,
    IE,
>
{
    fn prepend_instance_and_time_to_key(
        &self,
        time_column_extractor: CTE,
        instance_extractor: IE,
    ) -> Collection<G, (TimeKey<I, CT, K>, V), R>
    where
        CTE: Fn(&V) -> CT + 'static;
}

impl<G, K, V, R, CT, CTE, I, IE> TimeColumnSortable<G, K, V, R, CT, CTE, I, IE>
    for Collection<G, (K, V), R>
where
    G: Scope + MaybeTotalScope,
    K: ExchangeData + Shard,
    V: ExchangeData,
    CT: ExchangeData,
    R: ExchangeData + Abelian,
    CTE: Fn(&V) -> CT + 'static,
    I: ExchangeData,
    IE: Fn(&V) -> I + 'static,
{
    fn prepend_instance_and_time_to_key(
        &self,
        time_column_extractor: CTE,
        instance_extractor: IE,
    ) -> Collection<G, (TimeKey<I, CT, K>, V), R> {
        self.map_ex(move |(k, v)| {
            (
                TimeKey {
                    instance: instance_extractor(&v),
                    time: time_column_extractor(&v),
                    key: k,
                },
                v,
            )
        })
    }
}

pub trait MaxTimestamp {
    fn get_max_timestamp() -> Self;
}

impl MaxTimestamp for u64 {
    fn get_max_timestamp() -> u64 {
        u64::MAX / 2 * 2
    }
}

impl MaxTimestamp for i32 {
    fn get_max_timestamp() -> i32 {
        i32::MAX / 2 * 2
    }
}

#[derive(Debug, Clone)]
struct MaxTimePerInstance<I, T> {
    map: HashMap<I, T>,
}

impl<I, T> MaxTimePerInstance<I, T>
where
    I: Eq + Hash,
    T: PartialOrd,
{
    fn new() -> Self {
        MaxTimePerInstance {
            map: HashMap::new(),
        }
    }

    fn update(&mut self, instance: I, time: T) {
        match self.map.entry(instance) {
            Entry::Occupied(mut entry) => {
                if *entry.get() < time {
                    entry.insert(time);
                }
            }
            Entry::Vacant(entry) => {
                entry.insert(time);
            }
        }
    }

    fn merge(&mut self, other: Self) {
        for (instance, time) in other.map {
            self.update(instance, time);
        }
    }

    fn get(&self, instance: &I) -> Option<&T> {
        self.map.get(instance)
    }

    fn instances(&self) -> Keys<'_, I, T> {
        self.map.keys()
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
    I: ExchangeData + Hash,
    IE,
>
{
    #[allow(clippy::too_many_arguments)]
    fn postpone(
        &self,
        scope: G,
        threshold_time_extractor: TTE,
        current_time_extractor: CTE,
        instance_extractor: IE,
        flush_on_end: bool,
        update_time_before_emitting: bool,
        additional_logic: impl FnOnce(Collection<G, (K, V), R>) -> Result<Collection<G, (K, V), R>>,
    ) -> Result<Collection<G, (K, V), R>>
    where
        G: MaybeTotalScope,
        G::Timestamp: Epsilon + MaxTimestamp,
        TTE: Fn(&V) -> CT + 'static,
        CTE: Fn(&V) -> CT + 'static,
        IE: Fn(&V) -> I + 'static;
}

impl<G, K, V, R, CT, TTE, CTE, I, IE> TimeColumnBuffer<G, K, V, R, CT, TTE, CTE, I, IE>
    for Collection<G, (K, V), R>
where
    G: MaybeTotalScope<MaybeTotalTimestamp = Timestamp>,
    K: ExchangeData + Shard,
    V: ExchangeData,
    CT: ExchangeData,
    R: ExchangeData + Abelian,
    I: ExchangeData + Hash,
    TimeKey<I, CT, K>: Hashable,
{
    fn postpone(
        &self,
        mut scope: G,
        threshold_time_extractor: TTE,
        current_time_extractor: CTE,
        instance_extractor: IE,
        flush_on_end: bool,
        update_time_before_emitting: bool,
        additional_logic: impl FnOnce(Collection<G, (K, V), R>) -> Result<Collection<G, (K, V), R>>,
    ) -> Result<Collection<G, (K, V), R>>
    where
        G::Timestamp: Epsilon + MaxTimestamp,
        TTE: Fn(&V) -> CT + 'static,
        CTE: Fn(&V) -> CT + 'static,
        IE: Fn(&V) -> I + 'static,
    {
        let retractions = Variable::new(&mut scope, Timestamp::epsilon());

        let self_with_retractions = self.concat(&retractions.negate());

        let ret_in_buffer_timespace_col = postpone_core(
            additional_logic(self_with_retractions)?
                .prepend_instance_and_time_to_key(threshold_time_extractor, instance_extractor)
                .arrange(),
            current_time_extractor,
            flush_on_end,
            update_time_before_emitting,
        );

        retractions.set(&ret_in_buffer_timespace_col);
        Ok(ret_in_buffer_timespace_col)
    }
}

fn push_key_values_to_output<K, C: Cursor, P>(
    wrapper: &mut CursorStorageWrapper<C>,
    output: &mut OutputHandle<'_, C::Time, ((K, C::Val), C::Time, C::R), P>,
    capability: &Capability<C::Time>,
    k: &K,
    time: &Option<C::Time>,
) where
    K: Data + 'static,
    C::Val: Data + 'static,
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
        let weight = key_val_total_weight(wrapper);
        let curr_val = wrapper.cursor.val(wrapper.storage);
        if let Some(weight) = weight.filter(|w| !w.is_zero()) {
            let time = time.as_ref().unwrap();
            assert!(time >= capability.time());
            output
                .session(&capability)
                .give(((k.clone(), curr_val.clone()), time.clone(), weight));
        }
        wrapper.cursor.step_val(wrapper.storage);
    }
}

fn move_cursor_to_key<
    C: Cursor<Key = TimeKey<I, CT, K>>,
    I: ExchangeData,
    CT: ExchangeData,
    K: ExchangeData + Shard,
>(
    input_wrapper: &mut CursorStorageWrapper<C>,
    key: Option<&TimeKey<I, CT, K>>,
    step_to_next: bool,
) {
    input_wrapper.cursor.rewind_keys(input_wrapper.storage);
    if key.is_some() {
        input_wrapper
            .cursor
            .seek_key(input_wrapper.storage, key.unwrap());
        let found = input_wrapper.cursor.get_key(input_wrapper.storage);
        if step_to_next && found.is_some() && found == key {
            input_wrapper.cursor.step_key(input_wrapper.storage);
        }
    }
}

//TODO: maybe we can separate out some blocks to remove the allow(clippy::too_many_lines)
// perhaps could be done while implementing support for complex times
#[allow(clippy::too_many_lines)]
pub fn postpone_core<G, CT, K, V, R, CTE, I>(
    mut input_arrangement: KeyValArr<G, TimeKey<I, CT, K>, V, R>,
    current_time_extractor: CTE,
    flush_on_end: bool,
    update_time_before_emitting: bool,
) -> Collection<G, (K, V), R>
where
    G: Scope,
    G::Timestamp: Lattice + MaxTimestamp + OriginalOrRetraction,
    CT: ExchangeData,
    K: ExchangeData + Shard,
    V: ExchangeData,
    R: ExchangeData + Abelian + Diff,
    CTE: Fn(&V) -> CT + 'static,
    I: ExchangeData + Hash,
{
    let stream = {
        input_arrangement.stream.unary_frontier(
            Pipeline,
            "buffer",
            move |outer_cap, _operator_info| {
                let mut input_buffer: Vec<Rc<OrdValBatch<TimeKey<I, CT, K>, V, G::Timestamp, R>>> =
                    Vec::new();
                let mut already_released_column_times: MaxTimePerInstance<I, CT> =
                    MaxTimePerInstance::new();
                let mut release_threshold_column_times: MaxTimePerInstance<I, CT> =
                    MaxTimePerInstance::new();
                // remember first in instance to jump to a proper starting place for each instance in the arrangement
                let mut first_in_instance: HashMap<I, TimeKey<I, CT, K>> = HashMap::new();
                // remember last emitted arrangement key per instance to start from its upper bound after current time advancement
                let mut last_arrangement_keys: HashMap<I, TimeKey<I, CT, K>> = HashMap::new();
                // maybe_cap to emit records kept in the buffer on time end
                let mut maybe_cap = Some(outer_cap.delayed(&G::Timestamp::get_max_timestamp()));
                // remember instances updated previously to revisit them after time advancemet caused by them takes effect
                let mut previousuly_updated_instances: HashSet<I> = HashSet::new();
                move |input, output| {
                    input.for_each(|capability, batches| {
                        batches.swap(&mut input_buffer);

                        let mut upper_limit = Antichain::from_elem(G::Timestamp::minimum());
                        for batch in &input_buffer {
                            upper_limit.advance_by(AntichainRef::new(&[batch.upper().clone()]));
                        }

                        let grouped = batch_by_time(&input_buffer, |key, val, time, diff| {
                            (key.clone(), val.clone(), time.clone(), diff.clone())
                        });

                        for (time, entries) in grouped {
                            if time.is_retraction() {
                                continue;
                            }
                            let mut future_release_threshold_column_times: MaxTimePerInstance<
                                I,
                                CT,
                            > = MaxTimePerInstance::new();
                            for (key, val, _time, _diff) in &entries {
                                future_release_threshold_column_times
                                    .update(key.instance.clone(), current_time_extractor(val));
                            }

                            if update_time_before_emitting {
                                release_threshold_column_times
                                    .merge(future_release_threshold_column_times.clone());
                            }

                            let mut updated_instances: HashSet<I> = HashSet::new();
                            for (key, val, _time, diff) in &entries {
                                //pass entries that are late for buffering
                                let instance = &key.instance;
                                let already_released_time =
                                    already_released_column_times.get(instance);
                                let last_arrangement_key = last_arrangement_keys.get(instance);
                                if already_released_time.is_some()
                                    && already_released_time.unwrap() >= &key.time
                                    && Some(key) < last_arrangement_key
                                // only emit those entries that won't be emitted while iterating over the arrangement below
                                {
                                    output.session(&capability.delayed(&time)).give((
                                        (key.key.clone(), val.clone()),
                                        time.clone(),
                                        diff.clone(),
                                    ));
                                }
                                match first_in_instance.entry(instance.clone()) {
                                    Entry::Occupied(mut occupied_entry) => {
                                        if occupied_entry.get() > key {
                                            occupied_entry.insert(key.clone());
                                        }
                                    }
                                    Entry::Vacant(vacant_entry) => {
                                        vacant_entry.insert(key.clone());
                                    }
                                }
                                updated_instances.insert(instance.clone());
                            }

                            let (mut cursor, storage) = input_arrangement.trace.cursor();
                            let mut input_wrapper = CursorStorageWrapper {
                                cursor: &mut cursor,
                                storage: &storage,
                            };
                            // going over input arrangement has three parts
                            // 1. entries already emitted (skipped by seek_key)
                            // 2. entries to keep for later
                            // 3. entries to emit while processing this batch

                            for instance in updated_instances
                                .iter()
                                .chain(previousuly_updated_instances.iter())
                            {
                                let release_threshold_column_time =
                                    release_threshold_column_times.get(instance);
                                if release_threshold_column_time.is_some() {
                                    let mut last_arrangement_key = None;
                                    move_cursor_to_key(
                                        &mut input_wrapper,
                                        last_arrangement_keys
                                            .get(instance)
                                            .or_else(|| first_in_instance.get(instance)),
                                        last_arrangement_keys.contains_key(instance),
                                    );
                                    while input_wrapper.cursor.key_valid(input_wrapper.storage) {
                                        let tk = input_wrapper.cursor.key(input_wrapper.storage);

                                        //if threshold is larger than current 'now', all subsequent entries
                                        //will have too large threshold to be emitted
                                        if tk.instance != *instance
                                            || &tk.time
                                                > release_threshold_column_time.as_ref().unwrap()
                                        {
                                            break;
                                        }
                                        last_arrangement_key = Some(tk.clone());
                                        push_key_values_to_output(
                                            &mut input_wrapper,
                                            output,
                                            &capability.delayed(&time),
                                            &tk.key,
                                            &Some(time.clone()),
                                        );
                                        input_wrapper.cursor.step_key(input_wrapper.storage);
                                    }
                                    if let Some(last_arrangement_key_entry) = last_arrangement_key {
                                        last_arrangement_keys
                                            .insert(instance.clone(), last_arrangement_key_entry);
                                    }
                                }
                            }

                            already_released_column_times
                                .merge(release_threshold_column_times.clone());

                            if !update_time_before_emitting {
                                release_threshold_column_times
                                    .merge(future_release_threshold_column_times);
                            }
                            previousuly_updated_instances = updated_instances;

                            if !upper_limit.is_empty() {
                                input_arrangement
                                    .trace
                                    .set_logical_compaction(upper_limit.borrow());
                                input_arrangement
                                    .trace
                                    .set_physical_compaction(upper_limit.borrow());
                            }
                        }
                    });

                    let frontier = input.frontier.frontier();
                    if !frontier.less_equal(&G::Timestamp::get_max_timestamp()) {
                        if flush_on_end && maybe_cap.is_some() {
                            let (mut cursor, storage) = input_arrangement.trace.cursor();
                            let mut input_wrapper = CursorStorageWrapper {
                                cursor: &mut cursor,
                                storage: &storage,
                            };

                            for instance in release_threshold_column_times.instances() {
                                move_cursor_to_key(
                                    &mut input_wrapper,
                                    last_arrangement_keys
                                        .get(instance)
                                        .or_else(|| first_in_instance.get(instance)),
                                    last_arrangement_keys.contains_key(instance),
                                );

                                while input_wrapper.cursor.key_valid(input_wrapper.storage) {
                                    let tk = input_wrapper.cursor.key(input_wrapper.storage);
                                    if tk.instance != *instance {
                                        break;
                                    }
                                    push_key_values_to_output(
                                        &mut input_wrapper,
                                        output,
                                        maybe_cap.as_ref().unwrap(),
                                        &tk.key,
                                        &Some(maybe_cap.as_ref().unwrap().time().clone()),
                                    );

                                    input_wrapper.cursor.step_key(input_wrapper.storage);
                                }
                            }
                        }

                        input_arrangement
                            .trace
                            .set_logical_compaction(AntichainRef::new(&[]));
                        input_arrangement
                            .trace
                            .set_physical_compaction(AntichainRef::new(&[]));

                        maybe_cap = None;
                    }
                }
            },
        )
    };
    stream.as_collection()
}

pub trait TimeColumnForget<
    G: Scope,
    K: ExchangeData + Shard,
    V: ExchangeData,
    R: ExchangeData + Abelian,
    CT: Ord + ExchangeData,
    TTE,
    CTE,
    I: ExchangeData + Hash,
    IE,
>
{
    fn forget(
        &self,
        threshold_time_extractor: TTE,
        current_time_extractor: CTE,
        instance_extractor: IE,
        mark_forgetting_records: bool,
        additional_logic: impl FnOnce(Collection<G, (K, V), R>) -> Result<Collection<G, (K, V), R>>,
    ) -> Result<Collection<G, (K, V), R>>
    where
        G: MaybeTotalScope,
        G::Timestamp: MaxTimestamp,
        TTE: Fn(&V) -> CT + 'static,
        CTE: Fn(&V) -> CT + 'static,
        IE: Fn(&V) -> I + 'static;
}

impl<G, K, V, R, CT, TTE, CTE, I, IE> TimeColumnForget<G, K, V, R, CT, TTE, CTE, I, IE>
    for Collection<G, (K, V), R>
where
    G: MaybeTotalScope<MaybeTotalTimestamp = Timestamp>,
    K: ExchangeData + Shard,
    V: ExchangeData,
    CT: ExchangeData,
    R: ExchangeData + Abelian,
    TimeKey<I, CT, K>: Hashable,
    I: ExchangeData + Hash,
{
    fn forget(
        &self,
        threshold_time_extractor: TTE,
        current_time_extractor: CTE,
        instance_extractor: IE,
        mark_forgetting_records: bool,
        additional_logic: impl FnOnce(Collection<G, (K, V), R>) -> Result<Collection<G, (K, V), R>>,
    ) -> Result<Collection<G, (K, V), R>>
    where
        G::Timestamp: Epsilon + MaxTimestamp,
        TTE: Fn(&V) -> CT + 'static,
        CTE: Fn(&V) -> CT + 'static,
        IE: Fn(&V) -> I + 'static,
    {
        let forgetting_stream = self.negate().postpone(
            self.scope(),
            threshold_time_extractor,
            current_time_extractor,
            instance_extractor,
            false,
            false,
            additional_logic,
        )?;
        let forgetting_stream = if mark_forgetting_records {
            forgetting_stream
                .inspect(|(_data, time, _diff)| {
                    assert!(
                        time.is_original(),
                        "Neu time encountered at forget() buffer output."
                    );
                })
                .delay(|time| {
                    // can be called on times other than appearing in records
                    // that's why assert is in inspect above
                    // if two times are ordered, they should have the same order once func is applied to them
                    Timestamp(time.0 + 1) // produce neu times
                })
        } else {
            forgetting_stream
        };
        Ok(self
            .inner
            .inspect(|(_data, time, _diff)| {
                assert!(
                    time.is_original(),
                    "Neu time encountered at forget() input."
                );
            })
            .as_collection()
            .concat(&forgetting_stream))
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
    I,
    IE,
>
{
    fn freeze(
        &self,
        threshold_time_extractor: TTE,
        current_column_extractor: CTE,
        instance_extractor: IE,
    ) -> (Collection<G, (K, V), R>, Collection<G, (K, V), R>)
    where
        G::Timestamp: Epsilon,
        TTE: Fn(&V) -> CT + 'static,
        CTE: Fn(&V) -> CT + 'static,
        I: Eq + Hash + 'static,
        IE: Fn(&V) -> I + 'static;
}

impl<G, K, V, R, CT, CTE, TTE, I, IE> TimeColumnFreeze<G, K, V, R, CT, CTE, TTE, I, IE>
    for Collection<G, (K, V), R>
where
    G: Scope + MaybeTotalScope,
    G::Timestamp: Lattice,
    K: ExchangeData + Shard,
    V: ExchangeData,
    CT: ExchangeData,
    R: ExchangeData + Abelian,
{
    fn freeze(
        &self,
        threshold_time_extractor: TTE,
        current_time_extractor: CTE,
        instance_extractor: IE,
    ) -> (Collection<G, (K, V), R>, Collection<G, (K, V), R>)
    where
        G::Timestamp: Epsilon,
        CTE: Fn(&V) -> CT + 'static,
        TTE: Fn(&V) -> CT + 'static,
        I: Eq + Hash + 'static,
        IE: Fn(&V) -> I + 'static,
    {
        ignore_late(
            self,
            threshold_time_extractor,
            current_time_extractor,
            instance_extractor,
        )
    }
}

pub fn ignore_late<G, CT, K, V, R, CTE, TTE, I, IE>(
    input_collection: &Collection<G, (K, V), R>,
    threshold_time_extractor: TTE,
    current_time_extractor: CTE,
    instance_extractor: IE,
) -> (Collection<G, (K, V), R>, Collection<G, (K, V), R>)
where
    G: MaybeTotalScope,
    G::Timestamp: Lattice + Ord,
    CT: ExchangeData,
    K: ExchangeData + Shard,
    V: ExchangeData,
    R: ExchangeData + Abelian + Diff,
    TTE: Fn(&V) -> CT + 'static,
    CTE: Fn(&V) -> CT + 'static,
    I: Eq + Hash + 'static,
    IE: Fn(&V) -> I + 'static,
{
    let input_arrangement: ArrangedBySelf<G, (K, V), R> = input_collection.arrange_sharded(|_| 0);
    let mut builder =
        OperatorBuilder::new("ignore_late".to_owned(), input_collection.inner.scope());

    let mut input = builder.new_input(&input_arrangement.stream, Pipeline);
    let (mut output, stream) = builder.new_output();
    let (mut late_output, late_stream) = builder.new_output();

    builder.build(move |_| {
        let mut input_buffer = Vec::new();
        let mut max_column_times: MaxTimePerInstance<I, CT> = MaxTimePerInstance::new();

        move |_frontiers| {
            let mut output_handle = output.activate();
            let mut late_output_handle = late_output.activate();
            input.for_each(|capability, batch| {
                batch.swap(&mut input_buffer);
                let grouped = batch_by_time(&input_buffer, |key_val, (), time, diff| {
                    (key_val.clone(), time.clone(), diff.clone())
                });
                for data in grouped.into_values() {
                    let mut future_max_column_times: MaxTimePerInstance<I, CT> =
                        MaxTimePerInstance::new();
                    for ((key, val), time, weight) in &data {
                        let instance = instance_extractor(val);
                        let max_time = max_column_times.get(&instance);
                        let threshold = threshold_time_extractor(val);
                        if max_time.is_none() || max_time.unwrap() < &threshold {
                            output_handle.session(&capability).give((
                                (key.clone(), val.clone()),
                                time.clone(),
                                weight.clone(),
                            ));
                        } else {
                            late_output_handle.session(&capability).give((
                                (key.clone(), val.clone()),
                                time.clone(),
                                weight.clone(),
                            ));
                        }
                        future_max_column_times.update(instance, current_time_extractor(val));
                    }
                    max_column_times.merge(future_max_column_times);
                }
            });
        }
    });

    (stream.as_collection(), late_stream.as_collection())
}
