//! Arranges a collection into a re-usable trace structure.
//!
//! The `arrange` operator applies to a differential dataflow `Collection` and returns an `Arranged`
//! structure, provides access to both an indexed form of accepted updates as well as a stream of
//! batches of newly arranged updates.
//!
//! Several operators (`join`, `group`, and `cogroup`, among others) are implemented against `Arranged`,
//! and can be applied directly to arranged data instead of the collection. Internally, the operators
//! will borrow the shared state, and listen on the timely stream for shared batches of data. The
//! resources to index the collection---communication, computation, and memory---are spent only once,
//! and only one copy of the index needs to be maintained as the collection changes.
//!
//! The arranged collection is stored in a trace, whose append-only operation means that it is safe to
//! share between the single `arrange` writer and multiple readers. Each reader is expected to interrogate
//! the trace only at times for which it knows the trace is complete, as indicated by the frontiers on its
//! incoming channels. Failing to do this is "safe" in the Rust sense of memory safety, but the reader may
//! see ill-defined data at times for which the trace is not complete. (All current implementations
//! commit only completed data to the trace).

use timely::dataflow::operators::{Enter, Map};
use timely::order::{PartialOrder, TotalOrder};
use timely::dataflow::{Scope, Stream};
use timely::dataflow::operators::generic::Operator;
use timely::dataflow::channels::pact::{ParallelizationContract, Pipeline, Exchange};
use timely::progress::Timestamp;
use timely::progress::{Antichain, frontier::AntichainRef};
use timely::dataflow::operators::Capability;

use ::{Data, ExchangeData, Collection, AsCollection, Hashable};
use ::difference::Semigroup;
use lattice::Lattice;
use trace::{Trace, TraceReader, Batch, BatchReader, Batcher, Cursor};
use trace::implementations::ord::OrdValSpine as DefaultValTrace;
use trace::implementations::ord::OrdKeySpine as DefaultKeyTrace;

use trace::wrappers::enter::{TraceEnter, BatchEnter};
use trace::wrappers::enter_at::TraceEnter as TraceEnterAt;
use trace::wrappers::enter_at::BatchEnter as BatchEnterAt;
use trace::wrappers::filter::{TraceFilter, BatchFilter};

use super::TraceAgent;

/// An arranged collection of `(K,V)` values.
///
/// An `Arranged` allows multiple differential operators to share the resources (communication,
/// computation, memory) required to produce and maintain an indexed representation of a collection.
pub struct Arranged<G: Scope, Tr>
where
    G::Timestamp: Lattice+Ord,
    Tr: TraceReader+Clone,
{
    /// A stream containing arranged updates.
    ///
    /// This stream contains the same batches of updates the trace itself accepts, so there should
    /// be no additional overhead to receiving these records. The batches can be navigated just as
    /// the batches in the trace, by key and by value.
    pub stream: Stream<G, Tr::Batch>,
    /// A shared trace, updated by the `Arrange` operator and readable by others.
    pub trace: Tr,
    // TODO : We might have an `Option<Collection<G, (K, V)>>` here, which `as_collection` sets and
    // returns when invoked, so as to not duplicate work with multiple calls to `as_collection`.
}

impl<G: Scope, Tr> Clone for Arranged<G, Tr>
where
    G::Timestamp: Lattice+Ord,
    Tr: TraceReader<Time=G::Timestamp> + Clone,
{
    fn clone(&self) -> Self {
        Arranged {
            stream: self.stream.clone(),
            trace: self.trace.clone(),
        }
    }
}

use ::timely::dataflow::scopes::Child;
use ::timely::progress::timestamp::Refines;

impl<G: Scope, Tr> Arranged<G, Tr>
where
    G::Timestamp: Lattice+Ord,
    Tr: TraceReader<Time=G::Timestamp> + Clone,
{
    /// Brings an arranged collection into a nested scope.
    ///
    /// This method produces a proxy trace handle that uses the same backing data, but acts as if the timestamps
    /// have all been extended with an additional coordinate with the default value. The resulting collection does
    /// not vary with the new timestamp coordinate.
    pub fn enter<'a, TInner>(&self, child: &Child<'a, G, TInner>)
        -> Arranged<Child<'a, G, TInner>, TraceEnter<Tr, TInner>>
        where
            Tr::Key: 'static,
            Tr::Val: 'static,
            Tr::R: 'static,
            G::Timestamp: Clone+'static,
            TInner: Refines<G::Timestamp>+Lattice+Timestamp+Clone+'static,
    {
        Arranged {
            stream: self.stream.enter(child).map(|bw| BatchEnter::make_from(bw)),
            trace: TraceEnter::make_from(self.trace.clone()),
        }
    }

    /// Brings an arranged collection into a nested region.
    ///
    /// This method only applies to *regions*, which are subscopes with the same timestamp
    /// as their containing scope. In this case, the trace type does not need to change.
    pub fn enter_region<'a>(&self, child: &Child<'a, G, G::Timestamp>)
        -> Arranged<Child<'a, G, G::Timestamp>, Tr>
        where
            Tr::Key: 'static,
            Tr::Val: 'static,
            Tr::R: 'static,
            G::Timestamp: Clone+'static,
    {
        Arranged {
            stream: self.stream.enter(child),
            trace: self.trace.clone(),
        }
    }

    /// Brings an arranged collection into a nested scope.
    ///
    /// This method produces a proxy trace handle that uses the same backing data, but acts as if the timestamps
    /// have all been extended with an additional coordinate with the default value. The resulting collection does
    /// not vary with the new timestamp coordinate.
    pub fn enter_at<'a, TInner, F, P>(&self, child: &Child<'a, G, TInner>, logic: F, prior: P)
        -> Arranged<Child<'a, G, TInner>, TraceEnterAt<Tr, TInner, F, P>>
        where
            Tr::Key: 'static,
            Tr::Val: 'static,
            Tr::R: 'static,
            G::Timestamp: Clone+'static,
            TInner: Refines<G::Timestamp>+Lattice+Timestamp+Clone+'static,
            F: FnMut(&Tr::Key, &Tr::Val, &G::Timestamp)->TInner+Clone+'static,
            P: FnMut(&TInner)->Tr::Time+Clone+'static,
        {
        let logic1 = logic.clone();
        let logic2 = logic.clone();
        Arranged {
            trace: TraceEnterAt::make_from(self.trace.clone(), logic1, prior),
            stream: self.stream.enter(child).map(move |bw| BatchEnterAt::make_from(bw, logic2.clone())),
        }
    }

    /// Filters an arranged collection.
    ///
    /// This method produces a new arrangement backed by the same shared
    /// arrangement as `self`, paired with user-specified logic that can
    /// filter by key and value. The resulting collection is restricted
    /// to the keys and values that return true under the user predicate.
    ///
    /// # Examples
    ///
    /// ```
    /// extern crate timely;
    /// extern crate differential_dataflow;
    ///
    /// use differential_dataflow::input::Input;
    /// use differential_dataflow::operators::arrange::ArrangeByKey;
    ///
    /// fn main() {
    ///     ::timely::example(|scope| {
    ///
    ///         let arranged =
    ///         scope.new_collection_from(0 .. 10).1
    ///              .map(|x| (x, x+1))
    ///              .arrange_by_key();
    ///
    ///         arranged
    ///             .filter(|k,v| k == v)
    ///             .as_collection(|k,v| (*k,*v))
    ///             .assert_empty();
    ///     });
    /// }
    /// ```
    pub fn filter<F>(&self, logic: F)
        -> Arranged<G, TraceFilter<Tr, F>>
        where
            Tr::Key: 'static,
            Tr::Val: 'static,
            Tr::R: 'static,
            G::Timestamp: Clone+'static,
            F: FnMut(&Tr::Key, &Tr::Val)->bool+Clone+'static,
    {
        let logic1 = logic.clone();
        let logic2 = logic.clone();
        Arranged {
            trace: TraceFilter::make_from(self.trace.clone(), logic1),
            stream: self.stream.map(move |bw| BatchFilter::make_from(bw, logic2.clone())),
        }
    }
    /// Flattens the stream into a `Collection`.
    ///
    /// The underlying `Stream<G, BatchWrapper<T::Batch>>` is a much more efficient way to access the data,
    /// and this method should only be used when the data need to be transformed or exchanged, rather than
    /// supplied as arguments to an operator using the same key-value structure.
    pub fn as_collection<D: Data, L>(&self, mut logic: L) -> Collection<G, D, Tr::R>
        where
            Tr::R: Semigroup,
            L: FnMut(&Tr::Key, &Tr::Val) -> D+'static,
    {
        self.flat_map_ref(move |key, val| Some(logic(key,val)))
    }

    /// Extracts elements from an arrangement as a collection.
    ///
    /// The supplied logic may produce an iterator over output values, allowing either
    /// filtering or flat mapping as part of the extraction.
    pub fn flat_map_ref<I, L>(&self, logic: L) -> Collection<G, I::Item, Tr::R>
        where
            Tr::R: Semigroup,
            I: IntoIterator,
            I::Item: Data,
            L: FnMut(&Tr::Key, &Tr::Val) -> I+'static,
    {
        Self::flat_map_batches(&self.stream, logic)
    }

    /// Extracts elements from a stream of batches as a collection.
    ///
    /// The supplied logic may produce an iterator over output values, allowing either
    /// filtering or flat mapping as part of the extraction.
    ///
    /// This method exists for streams of batches without the corresponding arrangement.
    /// If you have the arrangement, its `flat_map_ref` method is equivalent to this.
    pub fn flat_map_batches<I, L>(stream: &Stream<G, Tr::Batch>, mut logic: L) -> Collection<G, I::Item, Tr::R>
    where
        Tr::R: Semigroup,
        I: IntoIterator,
        I::Item: Data,
        L: FnMut(&Tr::Key, &Tr::Val) -> I+'static,
    {
        stream.unary(Pipeline, "AsCollection", move |_,_| move |input, output| {
            input.for_each(|time, data| {
                let mut session = output.session(&time);
                for wrapper in data.iter() {
                    let batch = &wrapper;
                    let mut cursor = batch.cursor();
                    while let Some(key) = cursor.get_key(batch) {
                        while let Some(val) = cursor.get_val(batch) {
                            for datum in logic(key, val) {
                                cursor.map_times(batch, |time, diff| {
                                    session.give((datum.clone(), time.clone(), diff.clone()));
                                });
                            }
                            cursor.step_val(batch);
                        }
                        cursor.step_key(batch);
                    }
                }
            });
        })
        .as_collection()
    }

    /// Report values associated with keys at certain times.
    ///
    /// This method consumes a stream of (key, time) queries and reports the corresponding stream of
    /// (key, value, time, diff) accumulations in the `self` trace.
    pub fn lookup(&self, queries: &Stream<G, (Tr::Key, G::Timestamp)>) -> Stream<G, (Tr::Key, Tr::Val, G::Timestamp, Tr::R)>
    where
        G::Timestamp: Data+Lattice+Ord+TotalOrder,
        Tr::Key: ExchangeData+Hashable,
        Tr::Val: ExchangeData,
        Tr::R: ExchangeData+Semigroup,
        Tr: 'static,
    {
        // while the arrangement is already correctly distributed, the query stream may not be.
        let exchange = Exchange::new(move |update: &(Tr::Key,G::Timestamp)| update.0.hashed().into());
        queries.binary_frontier(&self.stream, exchange, Pipeline, "TraceQuery", move |_capability, _info| {

            let mut trace = Some(self.trace.clone());
            // release `set_physical_compaction` capability.
            trace.as_mut().unwrap().set_physical_compaction(Antichain::new().borrow());

            let mut stash = Vec::new();
            let mut capability: Option<Capability<G::Timestamp>> = None;

            let mut active = Vec::new();
            let mut retain = Vec::new();

            let mut working: Vec<(G::Timestamp, Tr::Val, Tr::R)> = Vec::new();
            let mut working2: Vec<(Tr::Val, Tr::R)> = Vec::new();

            move |input1, input2, output| {

                input1.for_each(|time, data| {
                    // if the minimum capability "improves" retain it.
                    if capability.is_none() || time.time().less_than(capability.as_ref().unwrap().time()) {
                        capability = Some(time.retain());
                    }
                    stash.extend(data.iter().cloned());
                });

                // drain input2; we will consult `trace` directly.
                input2.for_each(|_time, _data| { });

                assert_eq!(capability.is_none(), stash.is_empty());

                let mut drained = false;
                if let Some(capability) = capability.as_mut() {
                    if !input2.frontier().less_equal(capability.time()) {
                        for datum in stash.drain(..) {
                            if !input2.frontier().less_equal(&datum.1) {
                                active.push(datum);
                            }
                            else {
                                retain.push(datum);
                            }
                        }
                        drained = !active.is_empty();

                        ::std::mem::swap(&mut stash, &mut retain);    // retain now the stashed queries.

                        // sort temp1 by key and then by time.
                        active.sort_unstable_by(|x,y| x.0.cmp(&y.0));

                        let (mut cursor, storage) = trace.as_mut().unwrap().cursor();
                        let mut session = output.session(&capability);

                        // // V0: Potentially quadratic under load.
                        // for (key, time) in active.drain(..) {
                        //     cursor.seek_key(&storage, &key);
                        //     if cursor.get_key(&storage) == Some(&key) {
                        //         while let Some(val) = cursor.get_val(&storage) {
                        //             let mut count = R::zero();
                        //             cursor.map_times(&storage, |t, d| if t.less_equal(&time) {
                        //                 count = count + d;
                        //             });
                        //             if !count.is_zero() {
                        //                 session.give((key.clone(), val.clone(), time.clone(), count));
                        //             }
                        //             cursor.step_val(&storage);
                        //         }
                        //     }
                        // }

                        // V1: Stable under load
                        let mut active_finger = 0;
                        while active_finger < active.len() {

                            let key = &active[active_finger].0;
                            let mut same_key = active_finger;
                            while active.get(same_key).map(|x| &x.0) == Some(key) {
                                same_key += 1;
                            }

                            cursor.seek_key(&storage, key);
                            if cursor.get_key(&storage) == Some(key) {

                                let mut active = &active[active_finger .. same_key];

                                while let Some(val) = cursor.get_val(&storage) {
                                    cursor.map_times(&storage, |t,d| working.push((t.clone(), val.clone(), d.clone())));
                                    cursor.step_val(&storage);
                                }

                                working.sort_by(|x,y| x.0.cmp(&y.0));
                                for (time, val, diff) in working.drain(..) {
                                    if !active.is_empty() && active[0].1.less_than(&time) {
                                        crate::consolidation::consolidate(&mut working2);
                                        while !active.is_empty() && active[0].1.less_than(&time) {
                                            for &(ref val, ref count) in working2.iter() {
                                                session.give((key.clone(), val.clone(), active[0].1.clone(), count.clone()));
                                            }
                                            active = &active[1..];
                                        }
                                    }
                                    working2.push((val, diff));
                                }
                                if !active.is_empty() {
                                    crate::consolidation::consolidate(&mut working2);
                                    while !active.is_empty() {
                                        for &(ref val, ref count) in working2.iter() {
                                            session.give((key.clone(), val.clone(), active[0].1.clone(), count.clone()));
                                        }
                                        active = &active[1..];
                                    }
                                }
                            }
                            active_finger = same_key;
                        }
                        active.clear();
                    }
                }

                if drained {
                    if stash.is_empty() { capability = None; }
                    if let Some(capability) = capability.as_mut() {
                        let mut min_time = stash[0].1.clone();
                        for datum in stash[1..].iter() {
                            if datum.1.less_than(&min_time) {
                                min_time = datum.1.clone();
                            }
                        }
                        capability.downgrade(&min_time);
                    }
                }

                // Determine new frontier on queries that may be issued.
                // TODO: This code looks very suspect; explain better or fix.
                let frontier = IntoIterator::into_iter([
                    capability.as_ref().map(|c| c.time().clone()),
                    input1.frontier().frontier().get(0).cloned(),
                ]).filter_map(|t| t).min();

                if let Some(frontier) = frontier {
                    trace.as_mut().map(|t| t.set_logical_compaction(AntichainRef::new(&[frontier])));
                }
                else {
                    trace = None;
                }
            }
        })
    }
}

impl<'a, G: Scope, Tr> Arranged<Child<'a, G, G::Timestamp>, Tr>
where
    G::Timestamp: Lattice+Ord,
    Tr: TraceReader<Time=G::Timestamp> + Clone,
{
    /// Brings an arranged collection out of a nested region.
    ///
    /// This method only applies to *regions*, which are subscopes with the same timestamp
    /// as their containing scope. In this case, the trace type does not need to change.
    pub fn leave_region(&self) -> Arranged<G, Tr> {
        use timely::dataflow::operators::Leave;
        Arranged {
            stream: self.stream.leave(),
            trace: self.trace.clone(),
        }
    }
}

/// A type that can be arranged into a trace of type `T`.
///
/// This trait is implemented for appropriately typed collections and all traces that might accommodate them,
/// as well as by arranged data for their corresponding trace type.
pub trait Arrange<G: Scope, K, V, R: Semigroup>
where
    G::Timestamp: Lattice,
    K: Data,
    V: Data,
{
    /// Arranges a stream of `(Key, Val)` updates by `Key`. Accepts an empty instance of the trace type.
    ///
    /// This operator arranges a stream of values into a shared trace, whose contents it maintains.
    /// This trace is current for all times marked completed in the output stream, and probing this stream
    /// is the correct way to determine that times in the shared trace are committed.
    fn arrange<Tr>(&self) -> Arranged<G, TraceAgent<Tr>>
    where
        K: ExchangeData+Hashable,
        V: ExchangeData,
        R: ExchangeData,
        Tr: Trace+TraceReader<Key=K,Val=V,Time=G::Timestamp,R=R>+'static,
        Tr::Batch: Batch,
    {
        self.arrange_named("Arrange")
    }

    /// Arranges a stream of `(Key, Val)` updates by `Key`. Accepts an empty instance of the trace type.
    ///
    /// This operator arranges a stream of values into a shared trace, whose contents it maintains.
    /// This trace is current for all times marked completed in the output stream, and probing this stream
    /// is the correct way to determine that times in the shared trace are committed.
    fn arrange_named<Tr>(&self, name: &str) -> Arranged<G, TraceAgent<Tr>>
    where
        K: ExchangeData+Hashable,
        V: ExchangeData,
        R: ExchangeData,
        Tr: Trace+TraceReader<Key=K,Val=V,Time=G::Timestamp,R=R>+'static,
        Tr::Batch: Batch,
    {
        let exchange = Exchange::new(move |update: &((K,V),G::Timestamp,R)| (update.0).0.hashed().into());
        self.arrange_core(exchange, name)
    }

    /// Arranges a stream of `(Key, Val)` updates by `Key`. Accepts an empty instance of the trace type.
    ///
    /// This operator arranges a stream of values into a shared trace, whose contents it maintains.
    /// This trace is current for all times marked completed in the output stream, and probing this stream
    /// is the correct way to determine that times in the shared trace are committed.
    fn arrange_core<P, Tr>(&self, pact: P, name: &str) -> Arranged<G, TraceAgent<Tr>>
    where
        P: ParallelizationContract<G::Timestamp, ((K,V),G::Timestamp,R)>,
        Tr: Trace+TraceReader<Key=K,Val=V,Time=G::Timestamp,R=R>+'static,
        Tr::Batch: Batch,
    ;
}

impl<G, K, V, R> Arrange<G, K, V, R> for Collection<G, (K, V), R>
where
    G: Scope,
    G::Timestamp: Lattice+Ord,
    K: Data,
    V: Data,
    R: Semigroup,
{
    fn arrange<Tr>(&self) -> Arranged<G, TraceAgent<Tr>>
    where
        K: ExchangeData + Hashable,
        V: ExchangeData,
        R: ExchangeData,
        Tr: Trace + TraceReader<Key=K, Val=V, Time=G::Timestamp, R=R> + 'static, Tr::Batch: Batch
    {
        self.arrange_named("Arrange")
    }

    fn arrange_named<Tr>(&self, name: &str) -> Arranged<G, TraceAgent<Tr>>
    where
        K: ExchangeData + Hashable,
        V: ExchangeData,
        R: ExchangeData,
        Tr: Trace + TraceReader<Key=K, Val=V, Time=G::Timestamp, R=R> + 'static, Tr::Batch: Batch
    {
        let exchange = Exchange::new(move |update: &((K,V),G::Timestamp,R)| (update.0).0.hashed().into());
        self.arrange_core(exchange, name)
    }

    fn arrange_core<P, Tr>(&self, pact: P, name: &str) -> Arranged<G, TraceAgent<Tr>>
    where
        P: ParallelizationContract<G::Timestamp, ((K,V),G::Timestamp,R)>,
        Tr: Trace+TraceReader<Key=K,Val=V,Time=G::Timestamp,R=R>+'static,
        Tr::Batch: Batch,
    {
        // The `Arrange` operator is tasked with reacting to an advancing input
        // frontier by producing the sequence of batches whose lower and upper
        // bounds are those frontiers, containing updates at times greater or
        // equal to lower and not greater or equal to upper.
        //
        // The operator uses its batch type's `Batcher`, which accepts update
        // triples and responds to requests to "seal" batches (presented as new
        // upper frontiers).
        //
        // Each sealed batch is presented to the trace, and if at all possible
        // transmitted along the outgoing channel. Empty batches may not have
        // a corresponding capability, as they are only retained for actual data
        // held by the batcher, which may prevents the operator from sending an
        // empty batch.

        let mut reader: Option<TraceAgent<Tr>> = None;

        // fabricate a data-parallel operator using the `unary_notify` pattern.
        let stream = {

            let reader = &mut reader;

            self.inner.unary_frontier(pact, name, move |_capability, info| {

                // Acquire a logger for arrange events.
                let logger = {
                    let scope = self.scope();
                    let register = scope.log_register();
                    register.get::<::logging::DifferentialEvent>("differential/arrange")
                };

                // Where we will deposit received updates, and from which we extract batches.
                let mut batcher = <Tr::Batch as Batch>::Batcher::new();

                // Capabilities for the lower envelope of updates in `batcher`.
                let mut capabilities = Antichain::<Capability<G::Timestamp>>::new();

                let (activator, effort) =
                if let Some(effort) = self.inner.scope().config().get::<isize>("differential/idle_merge_effort").cloned() {
                    (Some(self.scope().activator_for(&info.address[..])), Some(effort))
                }
                else {
                    (None, None)
                };

                let empty_trace = Tr::new(info.clone(), logger.clone(), activator);
                let (reader_local, mut writer) = TraceAgent::new(empty_trace, info, logger);

                *reader = Some(reader_local);

                // Initialize to the minimal input frontier.
                let mut prev_frontier = Antichain::from_elem(<G::Timestamp as Timestamp>::minimum());

                move |input, output| {

                    // As we receive data, we need to (i) stash the data and (ii) keep *enough* capabilities.
                    // We don't have to keep all capabilities, but we need to be able to form output messages
                    // when we realize that time intervals are complete.

                    input.for_each(|cap, data| {
                        capabilities.insert(cap.retain());
                        batcher.push_batch(data);
                    });

                    // The frontier may have advanced by multiple elements, which is an issue because
                    // timely dataflow currently only allows one capability per message. This means we
                    // must pretend to process the frontier advances one element at a time, batching
                    // and sending smaller bites than we might have otherwise done.

                    // Assert that the frontier never regresses.
                    assert!(PartialOrder::less_equal(&prev_frontier.borrow(), &input.frontier().frontier()));

                    // Test to see if strict progress has occurred, which happens whenever the new
                    // frontier isn't equal to the previous. It is only in this case that we have any
                    // data processing to do.
                    if prev_frontier.borrow() != input.frontier().frontier() {
                        // There are two cases to handle with some care:
                        //
                        // 1. If any held capabilities are not in advance of the new input frontier,
                        //    we must carve out updates now in advance of the new input frontier and
                        //    transmit them as batches, which requires appropriate *single* capabilites;
                        //    Until timely dataflow supports multiple capabilities on messages, at least.
                        //
                        // 2. If there are no held capabilities in advance of the new input frontier,
                        //    then there are no updates not in advance of the new input frontier and
                        //    we can simply create an empty input batch with the new upper frontier
                        //    and feed this to the trace agent (but not along the timely output).

                        // If there is at least one capability not in advance of the input frontier ...
                        if capabilities.elements().iter().any(|c| !input.frontier().less_equal(c.time())) {

                            let mut upper = Antichain::new();   // re-used allocation for sealing batches.

                            // For each capability not in advance of the input frontier ...
                            for (index, capability) in capabilities.elements().iter().enumerate() {

                                if !input.frontier().less_equal(capability.time()) {

                                    // Assemble the upper bound on times we can commit with this capabilities.
                                    // We must respect the input frontier, and *subsequent* capabilities, as
                                    // we are pretending to retire the capability changes one by one.
                                    upper.clear();
                                    for time in input.frontier().frontier().iter() {
                                        upper.insert(time.clone());
                                    }
                                    for other_capability in &capabilities.elements()[(index + 1) .. ] {
                                        upper.insert(other_capability.time().clone());
                                    }

                                    // Extract updates not in advance of `upper`.
                                    let batch = batcher.seal(upper.clone());

                                    writer.insert(batch.clone(), Some(capability.time().clone()));

                                    // send the batch to downstream consumers, empty or not.
                                    output.session(&capabilities.elements()[index]).give(batch);
                                }
                            }

                            // Having extracted and sent batches between each capability and the input frontier,
                            // we should downgrade all capabilities to match the batcher's lower update frontier.
                            // This may involve discarding capabilities, which is fine as any new updates arrive
                            // in messages with new capabilities.

                            let mut new_capabilities = Antichain::new();
                            for time in batcher.frontier().iter() {
                                if let Some(capability) = capabilities.elements().iter().find(|c| c.time().less_equal(time)) {
                                    new_capabilities.insert(capability.delayed(time));
                                }
                                else {
                                    panic!("failed to find capability");
                                }
                            }

                            capabilities = new_capabilities;
                        }
                        else {
                            // Announce progress updates, even without data.
                            let _batch = batcher.seal(input.frontier().frontier().to_owned());
                            writer.seal(input.frontier().frontier().to_owned());
                        }

                        prev_frontier.clear();
                        prev_frontier.extend(input.frontier().frontier().iter().cloned());
                    }

                    if let Some(mut fuel) = effort.clone() {
                        writer.exert(&mut fuel);
                    }
                }
            })
        };

        Arranged { stream: stream, trace: reader.unwrap() }
    }
}

impl<G: Scope, K: Data, R: Semigroup> Arrange<G, K, (), R> for Collection<G, K, R>
where
    G::Timestamp: Lattice+Ord,
{
    fn arrange_core<P, Tr>(&self, pact: P, name: &str) -> Arranged<G, TraceAgent<Tr>>
    where
        P: ParallelizationContract<G::Timestamp, ((K,()),G::Timestamp,R)>,
        Tr: Trace+TraceReader<Key=K, Val=(), Time=G::Timestamp, R=R>+'static,
        Tr::Batch: Batch,
    {
        self.map(|k| (k, ()))
            .arrange_core(pact, name)
    }
}

/// Arranges something as `(Key,Val)` pairs according to a type `T` of trace.
///
/// This arrangement requires `Key: Hashable`, and uses the `hashed()` method to place keys in a hashed
/// map. This can result in many hash calls, and in some cases it may help to first transform `K` to the
/// pair `(u64, K)` of hash value and key.
pub trait ArrangeByKey<G: Scope, K: Data+Hashable, V: Data, R: Semigroup>
where G::Timestamp: Lattice+Ord {
    /// Arranges a collection of `(Key, Val)` records by `Key`.
    ///
    /// This operator arranges a stream of values into a shared trace, whose contents it maintains.
    /// This trace is current for all times completed by the output stream, which can be used to
    /// safely identify the stable times and values in the trace.
    fn arrange_by_key(&self) -> Arranged<G, TraceAgent<DefaultValTrace<K, V, G::Timestamp, R>>>;

    /// As `arrange_by_key` but with the ability to name the arrangement.
    fn arrange_by_key_named(&self, name: &str) -> Arranged<G, TraceAgent<DefaultValTrace<K, V, G::Timestamp, R>>>;
}

impl<G: Scope, K: ExchangeData+Hashable, V: ExchangeData, R: ExchangeData+Semigroup> ArrangeByKey<G, K, V, R> for Collection<G, (K,V), R>
where
    G::Timestamp: Lattice+Ord
{
    fn arrange_by_key(&self) -> Arranged<G, TraceAgent<DefaultValTrace<K, V, G::Timestamp, R>>> {
        self.arrange_by_key_named("ArrangeByKey")
    }

    fn arrange_by_key_named(&self, name: &str) -> Arranged<G, TraceAgent<DefaultValTrace<K, V, G::Timestamp, R>>> {
        self.arrange_named(name)
    }
}

/// Arranges something as `(Key, ())` pairs according to a type `T` of trace.
///
/// This arrangement requires `Key: Hashable`, and uses the `hashed()` method to place keys in a hashed
/// map. This can result in many hash calls, and in some cases it may help to first transform `K` to the
/// pair `(u64, K)` of hash value and key.
pub trait ArrangeBySelf<G: Scope, K: Data+Hashable, R: Semigroup>
where
    G::Timestamp: Lattice+Ord
{
    /// Arranges a collection of `Key` records by `Key`.
    ///
    /// This operator arranges a collection of records into a shared trace, whose contents it maintains.
    /// This trace is current for all times complete in the output stream, which can be used to safely
    /// identify the stable times and values in the trace.
    fn arrange_by_self(&self) -> Arranged<G, TraceAgent<DefaultKeyTrace<K, G::Timestamp, R>>>;

    /// As `arrange_by_self` but with the ability to name the arrangement.
    fn arrange_by_self_named(&self, name: &str) -> Arranged<G, TraceAgent<DefaultKeyTrace<K, G::Timestamp, R>>>;
}


impl<G: Scope, K: ExchangeData+Hashable, R: ExchangeData+Semigroup> ArrangeBySelf<G, K, R> for Collection<G, K, R>
where
    G::Timestamp: Lattice+Ord
{
    fn arrange_by_self(&self) -> Arranged<G, TraceAgent<DefaultKeyTrace<K, G::Timestamp, R>>> {
        self.arrange_by_self_named("ArrangeBySelf")
    }

    fn arrange_by_self_named(&self, name: &str) -> Arranged<G, TraceAgent<DefaultKeyTrace<K, G::Timestamp, R>>> {
        self.map(|k| (k, ()))
            .arrange_named(name)
    }
}
