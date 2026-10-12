// Copyright © 2026 Pathway

pub mod external_index;
pub mod gradual_broadcast;
pub mod output;
pub mod prev_next;
pub mod stateful_reduce;
pub mod time_column;
mod utils;

use std::any::type_name;
use std::collections::BTreeMap;
use std::panic::Location;

use differential_dataflow::consolidation::consolidate_updates;
use differential_dataflow::difference::{Monoid, Semigroup};
use differential_dataflow::operators::arrange::{Arranged, TraceAgent};
use differential_dataflow::trace::{Batch, Trace, TraceReader};
use differential_dataflow::{AsCollection, Collection, Data, ExchangeData};
use itertools::Itertools;
use timely::dataflow::channels::pact::{Exchange, Pipeline};
use timely::dataflow::operators::Exchange as _;
use timely::dataflow::operators::{Capability, Operator};

use crate::engine::dataflow::operators::output::OutputBatch;
use crate::engine::BatchWrapper;

use self::output::ConsolidateForOutput;

use super::maybe_total::{MaybeTotalScope, MaybeTotalSwitch};
use super::monitoring::OperatorProbe;
use super::shard::Shard;
use super::ArrangedBySelf;

pub trait ArrangeWithTypes<S, K, V, R>
where
    S: MaybeTotalScope,
    K: ExchangeData,
    V: ExchangeData,
    R: Semigroup + ExchangeData,
{
    #[track_caller]
    fn arrange<Tr>(&self) -> Arranged<S, TraceAgent<Tr>>
    where
        Tr: Trace + TraceReader<Key = K, Val = V, Time = S::Timestamp, R = R> + 'static,
        Tr::Batch: Batch,
    {
        self.arrange_named("Arrange")
    }

    fn arrange_named<Tr>(&self, name: &str) -> Arranged<S, TraceAgent<Tr>>
    where
        Tr: Trace + TraceReader<Key = K, Val = V, Time = S::Timestamp, R = R> + 'static,
        Tr::Batch: Batch;
}

pub trait ArrangeWithTypesSharded<S, K, V, R>
where
    S: MaybeTotalScope,
    K: ExchangeData,
    V: ExchangeData,
    R: Semigroup + ExchangeData,
{
    #[track_caller]
    fn arrange_sharded<Tr>(
        &self,
        sharding: impl FnMut(&K) -> u64 + 'static,
    ) -> Arranged<S, TraceAgent<Tr>>
    where
        Tr: Trace + TraceReader<Key = K, Val = V, Time = S::Timestamp, R = R> + 'static,
        Tr::Batch: Batch,
    {
        self.arrange_sharded_named("Arrange", sharding)
    }

    fn arrange_sharded_named<Tr>(
        &self,
        name: &str,
        sharding: impl FnMut(&K) -> u64 + 'static,
    ) -> Arranged<S, TraceAgent<Tr>>
    where
        Tr: Trace + TraceReader<Key = K, Val = V, Time = S::Timestamp, R = R> + 'static,
        Tr::Batch: Batch;
}

impl<T, S, K, V, R> ArrangeWithTypes<S, K, V, R> for T
where
    T: differential_dataflow::operators::arrange::arrangement::Arrange<S, K, V, R>,
    S: MaybeTotalScope,
    K: ExchangeData + Shard,
    V: ExchangeData,
    R: Semigroup + ExchangeData,
{
    #[track_caller]
    fn arrange_named<Tr>(&self, name: &str) -> Arranged<S, TraceAgent<Tr>>
    where
        Tr: Trace + TraceReader<Key = K, Val = V, Time = S::Timestamp, R = R> + 'static,
        Tr::Batch: Batch,
    {
        self.arrange_sharded_named(name, Shard::shard)
    }
}

impl<T, S, K, V, R> ArrangeWithTypesSharded<S, K, V, R> for T
where
    T: differential_dataflow::operators::arrange::arrangement::Arrange<S, K, V, R>,
    S: MaybeTotalScope,
    K: ExchangeData,
    V: ExchangeData,
    R: Semigroup + ExchangeData,
{
    #[track_caller]
    fn arrange_sharded_named<Tr>(
        &self,
        name: &str,
        mut sharding: impl FnMut(&K) -> u64 + 'static,
    ) -> Arranged<S, TraceAgent<Tr>>
    where
        Tr: Trace + TraceReader<Key = K, Val = V, Time = S::Timestamp, R = R> + 'static,
        Tr::Batch: Batch,
    {
        let caller = Location::caller();
        let name = format!(
            "{name} [{key}, {value}] at {caller}",
            key = type_name::<K>(),
            value = type_name::<V>()
        );
        let exchange =
            Exchange::new(move |((key, _value), _time, _diff): &((K, V), _, _)| sharding(key));
        #[allow(clippy::disallowed_methods)]
        differential_dataflow::operators::arrange::arrangement::Arrange::arrange_core(
            self, exchange, &name,
        )
    }
}

// Keep incomplete times compact without retaining a historical trace.
struct PendingTimeUpdates<K, V, T: timely::progress::Timestamp> {
    capability: Capability<T>,
    updates: Vec<((K, V), T, isize)>,
    next_compaction: usize,
    has_insertions: bool,
    has_retractions: bool,
}

impl<K: Ord, V: Ord, T: timely::progress::Timestamp> PendingTimeUpdates<K, V, T> {
    const MIN_COMPACTION_UPDATES: usize = 4096;

    fn new(capability: Capability<T>) -> Self {
        Self {
            capability,
            updates: Vec::new(),
            next_compaction: Self::MIN_COMPACTION_UPDATES,
            has_insertions: false,
            has_retractions: false,
        }
    }

    fn append(&mut self, updates: &mut Vec<((K, V), T, isize)>) {
        self.has_insertions = self.has_insertions || updates.iter().any(|(_, _, diff)| *diff > 0);
        self.has_retractions = self.has_retractions || updates.iter().any(|(_, _, diff)| *diff < 0);
        self.updates.append(updates);
        self.compact_if_needed();
    }

    fn push(&mut self, update: ((K, V), T, isize)) {
        self.has_insertions |= update.2 > 0;
        self.has_retractions |= update.2 < 0;
        self.updates.push(update);
        self.compact_if_needed();
    }

    fn compact_if_needed(&mut self) {
        if self.has_insertions && self.has_retractions && self.updates.len() >= self.next_compaction
        {
            consolidate_updates(&mut self.updates);
            self.has_insertions = self.updates.iter().any(|(_, _, diff)| *diff > 0);
            self.has_retractions = self.updates.iter().any(|(_, _, diff)| *diff < 0);
            // Doubling avoids sorting a growing set for every input message.
            // One-sign input needs no intermediate sorting.
            self.next_compaction = self
                .updates
                .len()
                .saturating_mul(2)
                .max(Self::MIN_COMPACTION_UPDATES);
            if self.updates.capacity() > self.next_compaction.saturating_mul(2) {
                self.updates.shrink_to(self.next_compaction);
            }
        }
    }
}

/// Consolidation of a keyed collection without an arrangement.
///
/// The updates of a time are gathered until the frontier passes it and are
/// then consolidated in place and sent on: the same result as reading the
/// collection back from an arrangement of it, without building the
/// arrangement's batches, keeping its trace alive, or cloning every record
/// out of it. Updates of one sign have nothing to cancel, so a time that
/// holds only insertions (a batch load, an append-only stream) or only
/// retractions is passed on as it is, with no sort either.
pub trait ConsolidatePerTime<S, K, V>
where
    S: MaybeTotalScope,
{
    fn consolidate_per_time(&self) -> Collection<S, (K, V), isize>;
}

impl<S, K, V> ConsolidatePerTime<S, K, V> for Collection<S, (K, V), isize>
where
    S: MaybeTotalScope,
    K: ExchangeData + Shard + Ord,
    V: ExchangeData + Ord,
{
    #[track_caller]
    fn consolidate_per_time(&self) -> Collection<S, (K, V), isize> {
        type Update<K, V, T> = ((K, V), T, isize);
        let caller = Location::caller();
        let name = format!("ConsolidatePerTime at {caller}");
        // The updates of a key meet on its worker, as they do in an arrangement.
        let exchange =
            Exchange::new(|((key, _value), _time, _diff): &Update<K, V, S::Timestamp>| key.shard());
        self.inner
            .unary_frontier(exchange, &name, |_capability, _info| {
                let mut stash: BTreeMap<S::Timestamp, PendingTimeUpdates<K, V, S::Timestamp>> =
                    BTreeMap::new();
                let mut buffer = Vec::new();
                move |input, output| {
                    input.for_each(|capability, data| {
                        data.swap(&mut buffer);
                        if buffer
                            .iter()
                            .all(|(_data, time, _diff)| time == capability.time())
                        {
                            stash
                                .entry(capability.time().clone())
                                .or_insert_with(|| PendingTimeUpdates::new(capability.retain()))
                                .append(&mut buffer);
                        } else {
                            for update in buffer.drain(..) {
                                let time = update.1.clone();
                                stash
                                    .entry(time.clone())
                                    .or_insert_with(|| {
                                        PendingTimeUpdates::new(capability.delayed(&time))
                                    })
                                    .push(update);
                            }
                        }
                    });
                    // A time is complete once the frontier has passed it.
                    let frontier = input.frontier();
                    let complete: Vec<S::Timestamp> = stash
                        .keys()
                        .filter(|time| !frontier.less_equal(time))
                        .cloned()
                        .collect();
                    let mut complete = complete.into_iter().peekable();
                    while let Some(time) = complete.next() {
                        let PendingTimeUpdates {
                            capability,
                            mut updates,
                            ..
                        } = stash.remove(&time).expect("the time is stashed");
                        let mut session = output.session(&capability);
                        loop {
                            let has_insertions =
                                updates.iter().any(|(_data, _time, diff)| *diff > 0);
                            let has_retractions =
                                updates.iter().any(|(_data, _time, diff)| *diff < 0);
                            if has_insertions && has_retractions {
                                consolidate_updates(&mut updates);
                            }
                            // Batch small completed times, while transferring large
                            // vectors directly to avoid allocating their output twice.
                            if updates.len()
                                >= timely::container::buffer::default_capacity::<
                                    Update<K, V, S::Timestamp>,
                                >()
                            {
                                session.give_vec(&mut updates);
                            } else {
                                session.give_iterator(updates.drain(..));
                            }
                            let Some(next_time) = complete.peek() else {
                                break;
                            };
                            // Lexicographic ordering need not imply PartialOrder
                            // in iterative scopes; incomparable times need separate
                            // capabilities. Retain no earlier capability across calls.
                            if !timely::PartialOrder::less_equal(capability.time(), next_time) {
                                break;
                            }
                            let next_time = complete.next().expect("a completed time exists");
                            updates = stash
                                .remove(&next_time)
                                .expect("the time is stashed")
                                .updates;
                        }
                    }
                }
            })
            .as_collection()
    }
}

pub trait MaybeTotal<S, K, R>
where
    S: MaybeTotalScope,
    K: Data,
    R: Semigroup,
{
    fn count(&self) -> Collection<S, (K, R), isize>;

    fn distinct(&self) -> Collection<S, K, isize>;
}

impl<S, T> MaybeTotal<S, T::Key, T::R> for Arranged<S, T>
where
    S: MaybeTotalScope,
    T: TraceReader<Val = (), Time = S::Timestamp> + Clone + 'static,
    T::Key: Data,
    T::R: Semigroup,
{
    fn count(&self) -> Collection<S, (T::Key, T::R), isize> {
        S::IsTotal::count(self)
    }

    fn distinct(&self) -> Collection<S, T::Key, isize> {
        S::IsTotal::distinct(self)
    }
}

impl<S, K, R> MaybeTotal<S, K, R> for Collection<S, K, R>
where
    S: MaybeTotalScope,
    K: ExchangeData + Shard,
    R: Semigroup + ExchangeData,
{
    fn count(&self) -> Collection<S, (K, R), isize> {
        let arranged: ArrangedBySelf<S, K, R> = self.arrange_named("Arrange: CountMaybeTotal");
        arranged.count()
    }

    fn distinct(&self) -> Collection<S, K, isize> {
        let arranged: ArrangedBySelf<S, K, R> = self.arrange_named("Arrange: DistinctMaybeTotal");
        arranged.distinct()
    }
}

pub trait MapWrapped<S, D, R>
where
    S: MaybeTotalScope,
    R: Semigroup,
{
    #[track_caller]
    fn map_wrapped<D2: Data>(
        &self,
        wrapper: BatchWrapper,
        logic: impl FnMut(D) -> D2 + 'static,
    ) -> Collection<S, D2, R> {
        let name = format!("MapWrapped({wrapper:?})");
        self.map_wrapped_named(&name, wrapper, logic)
    }

    #[track_caller]
    fn map_ex<D2: Data>(&self, logic: impl FnMut(D) -> D2 + 'static) -> Collection<S, D2, R> {
        self.map_wrapped_named("MapEx", BatchWrapper::None, logic)
    }

    #[track_caller]
    fn map_named<D2: Data>(
        &self,
        name: &str,
        logic: impl FnMut(D) -> D2 + 'static,
    ) -> Collection<S, D2, R> {
        self.map_wrapped_named(name, BatchWrapper::None, logic)
    }

    fn map_wrapped_named<D2: Data>(
        &self,
        name: &str,
        wrapper: BatchWrapper,
        logic: impl FnMut(D) -> D2 + 'static,
    ) -> Collection<S, D2, R>;

    fn map_wrapped_batched_named<D2: Data>(
        &self,
        name: &str,
        logic: impl FnMut(Vec<D>) -> Vec<D2> + 'static,
    ) -> Collection<S, D2, R>;
}

impl<S, D, R> MapWrapped<S, D, R> for Collection<S, D, R>
where
    S: MaybeTotalScope,
    D: Data,
    R: Semigroup,
{
    #[track_caller]
    fn map_wrapped_named<D2: Data>(
        &self,
        name: &str,
        wrapper: BatchWrapper,
        mut logic: impl FnMut(D) -> D2 + 'static,
    ) -> Collection<S, D2, R> {
        let caller = Location::caller();
        let name = format!("{name} at {caller}");
        let mut vector = Vec::new();
        self.inner
            .unary(Pipeline, &name, move |_, _| {
                move |input, output| {
                    wrapper.run(|| {
                        while let Some((time, data)) = input.next() {
                            data.swap(&mut vector);
                            output.session(&time).give_iterator(
                                vector
                                    .drain(..)
                                    .map(|(data, time, diff)| (logic(data), time, diff)),
                            );
                        }
                    });
                }
            })
            .as_collection()
    }

    #[track_caller]
    fn map_wrapped_batched_named<D2: Data>(
        &self,
        name: &str,
        mut logic: impl FnMut(Vec<D>) -> Vec<D2> + 'static,
    ) -> Collection<S, D2, R> {
        let caller = Location::caller();
        let name = format!("{name} at {caller}");
        let mut vector = Vec::new();
        self.inner
            .unary(Pipeline, &name, move |_, _| {
                move |input, output| {
                    while let Some((time, data)) = input.next() {
                        data.swap(&mut vector);
                        let times_diffs: Vec<_> = vector
                            .iter()
                            .map(|(_data, time, diff)| (time.clone(), diff.clone()))
                            .collect();
                        let results =
                            logic(vector.drain(..).map(|(data, _time, _diff)| data).collect());
                        output.session(&time).give_iterator(
                            results
                                .into_iter()
                                .zip_eq(times_diffs)
                                .map(|(result, (time, diff))| (result, time, diff)),
                        );
                    }
                }
            })
            .as_collection()
    }
}

pub trait FlatMapBatchedWithDeletionsFirst<S, D, R>
where
    S: MaybeTotalScope,
    R: Monoid + ExchangeData,
{
    fn flat_map_batched_named_with_deletions_first<D2: Data>(
        &self,
        name: &str,
        logic: impl FnMut(Vec<(D, R)>) -> Vec<(D2, R)> + 'static,
    ) -> Collection<S, D2, R>;
}

impl<S, D, R> FlatMapBatchedWithDeletionsFirst<S, D, R> for Collection<S, D, R>
where
    S: MaybeTotalScope,
    D: ExchangeData + Shard,
    R: Monoid + ExchangeData + Copy,
{
    #[track_caller]
    fn flat_map_batched_named_with_deletions_first<D2: Data>(
        &self,
        name: &str,
        mut logic: impl FnMut(Vec<(D, R)>) -> Vec<(D2, R)> + 'static,
    ) -> Collection<S, D2, R> {
        let caller = Location::caller();
        let name = format!("{name} at {caller}");
        self.consolidate_for_output_named(&format!("ConsolidateForOutput: {name}"), false)
            .unary(Pipeline, &name, move |_, _| {
                let mut vector = Vec::new();
                move |input, output| {
                    while let Some((cap, data)) = input.next() {
                        data.swap(&mut vector);
                        for batch in vector.drain(..) {
                            let OutputBatch { time, data } = batch;
                            output.session(&cap.delayed(&time)).give_iterator(
                                logic(data)
                                    .into_iter()
                                    .map(|(result, diff)| (result, time.clone(), diff)),
                            );
                        }
                    }
                }
            })
            .as_collection()
    }
}

pub trait Reshard<S, D, R>
where
    S: MaybeTotalScope,
    D: Data,
    R: Semigroup,
{
    fn reshard(&self) -> Collection<S, D, R>;
    fn reshard_to_first_worker(&self) -> Collection<S, D, R>;
}

impl<S, D, R> Reshard<S, D, R> for Collection<S, D, R>
where
    S: MaybeTotalScope,
    D: ExchangeData + Shard,
    R: ExchangeData + Semigroup,
{
    fn reshard(&self) -> Collection<S, D, R> {
        self.inner
            .exchange(|(data, _time, _diff)| data.shard())
            .as_collection()
    }

    fn reshard_to_first_worker(&self) -> Collection<S, D, R> {
        self.inner.exchange(|_| 0).as_collection()
    }
}

pub trait ExtendedProbeWith<S, D>
where
    S: MaybeTotalScope,
    D: ExchangeData,
{
    fn extended_probe_with(
        &self,
        probe: &mut OperatorProbe<S::MaybeTotalTimestamp>,
    ) -> Collection<S, D, isize>;
}

impl<S, D> ExtendedProbeWith<S, D> for Collection<S, D, isize>
where
    S: MaybeTotalScope,
    D: ExchangeData,
{
    fn extended_probe_with(
        &self,
        probe: &mut OperatorProbe<S::MaybeTotalTimestamp>,
    ) -> Collection<S, D, isize> {
        let counter = probe.counter.clone();
        self.probe_with(&mut probe.frontier)
            .inspect_batch(move |_time, data| {
                let mut counter = counter.borrow_mut();
                for (_, _, diff) in data {
                    counter.update(*diff);
                }
            })
    }
}
