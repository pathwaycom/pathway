//! Match pairs of records based on a key.
//!
//! The various `join` implementations require that the units of each collection can be multiplied, and that
//! the multiplication distributes over addition. That is, we will repeatedly evaluate (a + b) * c as (a * c)
//! + (b * c), and if this is not equal to the former term, little is known about the actual output.
use std::fmt::Debug;
use std::cmp::Ordering;

use timely::order::PartialOrder;
use timely::progress::Timestamp;
use timely::dataflow::Scope;
use timely::dataflow::operators::generic::{Operator, OutputHandle};
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Capability;
use timely::dataflow::channels::pushers::tee::Tee;

use hashable::Hashable;
use ::{Data, ExchangeData, Collection, AsCollection};
use ::difference::{Semigroup, Abelian, Multiply};
use lattice::Lattice;
use operators::arrange::{Arranged, ArrangeByKey, ArrangeBySelf};
use trace::{BatchReader, Cursor};
use operators::ValueHistory;

use trace::TraceReader;

/// Join implementations for `(key,val)` data.
pub trait Join<G: Scope, K: Data, V: Data, R: Semigroup> {

    /// Matches pairs `(key,val1)` and `(key,val2)` based on `key` and yields pairs `(key, (val1, val2))`.
    ///
    /// The [`join_map`](Join::join_map) method may be more convenient for non-trivial processing pipelines.
    ///
    /// # Examples
    ///
    /// ```
    /// extern crate timely;
    /// extern crate differential_dataflow;
    ///
    /// use differential_dataflow::input::Input;
    /// use differential_dataflow::operators::Join;
    ///
    /// fn main() {
    ///     ::timely::example(|scope| {
    ///
    ///         let x = scope.new_collection_from(vec![(0, 1), (1, 3)]).1;
    ///         let y = scope.new_collection_from(vec![(0, 'a'), (1, 'b')]).1;
    ///         let z = scope.new_collection_from(vec![(0, (1, 'a')), (1, (3, 'b'))]).1;
    ///
    ///         x.join(&y)
    ///          .assert_eq(&z);
    ///     });
    /// }
    /// ```
    fn join<V2, R2>(&self, other: &Collection<G, (K,V2), R2>) -> Collection<G, (K,(V,V2)), <R as Multiply<R2>>::Output>
    where
        K: ExchangeData,
        V2: ExchangeData,
        R2: ExchangeData+Semigroup,
        R: Multiply<R2>,
        <R as Multiply<R2>>::Output: Semigroup
    {
        self.join_map(other, |k,v,v2| (k.clone(),(v.clone(),v2.clone())))
    }

    /// Matches pairs `(key,val1)` and `(key,val2)` based on `key` and then applies a function.
    ///
    /// # Examples
    ///
    /// ```
    /// extern crate timely;
    /// extern crate differential_dataflow;
    ///
    /// use differential_dataflow::input::Input;
    /// use differential_dataflow::operators::Join;
    ///
    /// fn main() {
    ///     ::timely::example(|scope| {
    ///
    ///         let x = scope.new_collection_from(vec![(0, 1), (1, 3)]).1;
    ///         let y = scope.new_collection_from(vec![(0, 'a'), (1, 'b')]).1;
    ///         let z = scope.new_collection_from(vec![(1, 'a'), (3, 'b')]).1;
    ///
    ///         x.join_map(&y, |_key, &a, &b| (a,b))
    ///          .assert_eq(&z);
    ///     });
    /// }
    /// ```
    fn join_map<V2, R2, D, L>(&self, other: &Collection<G, (K,V2), R2>, logic: L) -> Collection<G, D, <R as Multiply<R2>>::Output>
    where K: ExchangeData, V2: ExchangeData, R2: ExchangeData+Semigroup, R: Multiply<R2>, <R as Multiply<R2>>::Output: Semigroup, D: Data, L: FnMut(&K, &V, &V2)->D+'static;

    /// Matches pairs `(key, val)` and `key` based on `key`, producing the former with frequencies multiplied.
    ///
    /// When the second collection contains frequencies that are either zero or one this is the more traditional
    /// relational semijoin. When the second collection may contain multiplicities, this operation may scale up
    /// the counts of the records in the first input.
    ///
    /// # Examples
    ///
    /// ```
    /// extern crate timely;
    /// extern crate differential_dataflow;
    ///
    /// use differential_dataflow::input::Input;
    /// use differential_dataflow::operators::Join;
    ///
    /// fn main() {
    ///     ::timely::example(|scope| {
    ///
    ///         let x = scope.new_collection_from(vec![(0, 1), (1, 3)]).1;
    ///         let y = scope.new_collection_from(vec![0, 2]).1;
    ///         let z = scope.new_collection_from(vec![(0, 1)]).1;
    ///
    ///         x.semijoin(&y)
    ///          .assert_eq(&z);
    ///     });
    /// }
    /// ```
    fn semijoin<R2>(&self, other: &Collection<G, K, R2>) -> Collection<G, (K, V), <R as Multiply<R2>>::Output>
    where K: ExchangeData, R2: ExchangeData+Semigroup, R: Multiply<R2>, <R as Multiply<R2>>::Output: Semigroup;

    /// Subtracts the semijoin with `other` from `self`.
    ///
    /// In the case that `other` has multiplicities zero or one this results
    /// in a relational antijoin, in which we discard input records whose key
    /// is present in `other`. If the multiplicities could be other than zero
    /// or one, the semantic interpretation of this operator is less clear.
    ///
    /// In almost all cases, you should ensure that `other` has multiplicities
    /// that are zero or one, perhaps by using the `distinct` operator.
    ///
    /// # Examples
    ///
    /// ```
    /// extern crate timely;
    /// extern crate differential_dataflow;
    ///
    /// use differential_dataflow::input::Input;
    /// use differential_dataflow::operators::Join;
    ///
    /// fn main() {
    ///     ::timely::example(|scope| {
    ///
    ///         let x = scope.new_collection_from(vec![(0, 1), (1, 3)]).1;
    ///         let y = scope.new_collection_from(vec![0, 2]).1;
    ///         let z = scope.new_collection_from(vec![(1, 3)]).1;
    ///
    ///         x.antijoin(&y)
    ///          .assert_eq(&z);
    ///     });
    /// }
    /// ```
    fn antijoin<R2>(&self, other: &Collection<G, K, R2>) -> Collection<G, (K, V), R>
    where K: ExchangeData, R2: ExchangeData+Semigroup, R: Multiply<R2, Output = R>, R: Abelian;
}

impl<G, K, V, R> Join<G, K, V, R> for Collection<G, (K, V), R>
where
    G: Scope,
    K: ExchangeData+Hashable,
    V: ExchangeData,
    R: ExchangeData+Semigroup,
    G::Timestamp: Lattice+Ord,
{
    fn join_map<V2: ExchangeData, R2: ExchangeData+Semigroup, D: Data, L>(&self, other: &Collection<G, (K, V2), R2>, mut logic: L) -> Collection<G, D, <R as Multiply<R2>>::Output>
    where R: Multiply<R2>, <R as Multiply<R2>>::Output: Semigroup, L: FnMut(&K, &V, &V2)->D+'static {
        let arranged1 = self.arrange_by_key();
        let arranged2 = other.arrange_by_key();
        arranged1.join_core(&arranged2, move |k,v1,v2| Some(logic(k,v1,v2)))
    }

    fn semijoin<R2: ExchangeData+Semigroup>(&self, other: &Collection<G, K, R2>) -> Collection<G, (K, V), <R as Multiply<R2>>::Output>
    where R: Multiply<R2>, <R as Multiply<R2>>::Output: Semigroup {
        let arranged1 = self.arrange_by_key();
        let arranged2 = other.arrange_by_self();
        arranged1.join_core(&arranged2, |k,v,_| Some((k.clone(), v.clone())))
    }

    fn antijoin<R2: ExchangeData+Semigroup>(&self, other: &Collection<G, K, R2>) -> Collection<G, (K, V), R>
    where R: Multiply<R2, Output=R>, R: Abelian {
        self.concat(&self.semijoin(other).negate())
    }
}

impl<G, Tr> Join<G, Tr::Key, Tr::Val, Tr::R> for Arranged<G, Tr>
where
    G: Scope,
    G::Timestamp: Lattice+Ord,
    Tr: TraceReader<Time=G::Timestamp>+Clone+'static,
    Tr::Key: Data+Hashable,
    Tr::Val: Data,
    Tr::R: Semigroup,
{
    fn join_map<V2: ExchangeData, R2: ExchangeData+Semigroup, D: Data, L>(&self, other: &Collection<G, (Tr::Key, V2), R2>, mut logic: L) -> Collection<G, D, <Tr::R as Multiply<R2>>::Output>
    where Tr::Key: ExchangeData, Tr::R: Multiply<R2>, <Tr::R as Multiply<R2>>::Output: Semigroup, L: FnMut(&Tr::Key, &Tr::Val, &V2)->D+'static {
        let arranged2 = other.arrange_by_key();
        self.join_core(&arranged2, move |k,v1,v2| Some(logic(k,v1,v2)))
    }

    fn semijoin<R2: ExchangeData+Semigroup>(&self, other: &Collection<G, Tr::Key, R2>) -> Collection<G, (Tr::Key, Tr::Val), <Tr::R as Multiply<R2>>::Output>
    where Tr::Key: ExchangeData, Tr::R: Multiply<R2>, <Tr::R as Multiply<R2>>::Output: Semigroup {
        let arranged2 = other.arrange_by_self();
        self.join_core(&arranged2, |k,v,_| Some((k.clone(), v.clone())))
    }

    fn antijoin<R2: ExchangeData+Semigroup>(&self, other: &Collection<G, Tr::Key, R2>) -> Collection<G, (Tr::Key, Tr::Val), Tr::R>
    where Tr::Key: ExchangeData, Tr::R: Multiply<R2, Output=Tr::R>, Tr::R: Abelian {
        self.as_collection(|k,v| (k.clone(), v.clone()))
            .concat(&self.semijoin(other).negate())
    }
}

/// Matches the elements of two arranged traces.
///
/// This method is used by the various `join` implementations, but it can also be used
/// directly in the event that one has a handle to an `Arranged<G,T>`, perhaps because
/// the arrangement is available for re-use, or from the output of a `reduce` operator.
pub trait JoinCore<G: Scope, K: 'static, V: 'static, R: Semigroup> where G::Timestamp: Lattice+Ord {

    /// Joins two arranged collections with the same key type.
    ///
    /// Each matching pair of records `(key, val1)` and `(key, val2)` are subjected to the `result` function,
    /// which produces something implementing `IntoIterator`, where the output collection will have an entry for
    /// every value returned by the iterator.
    ///
    /// This trait is implemented for arrangements (`Arranged<G, T>`) rather than collections. The `Join` trait
    /// contains the implementations for collections.
    ///
    /// # Examples
    ///
    /// ```
    /// extern crate timely;
    /// extern crate differential_dataflow;
    ///
    /// use differential_dataflow::input::Input;
    /// use differential_dataflow::operators::arrange::ArrangeByKey;
    /// use differential_dataflow::operators::join::JoinCore;
    /// use differential_dataflow::trace::Trace;
    /// use differential_dataflow::trace::implementations::ord::OrdValSpine;
    ///
    /// fn main() {
    ///     ::timely::example(|scope| {
    ///
    ///         let x = scope.new_collection_from(vec![(0u32, 1), (1, 3)]).1
    ///                      .arrange_by_key();
    ///         let y = scope.new_collection_from(vec![(0, 'a'), (1, 'b')]).1
    ///                      .arrange_by_key();
    ///
    ///         let z = scope.new_collection_from(vec![(1, 'a'), (3, 'b')]).1;
    ///
    ///         x.join_core(&y, |_key, &a, &b| Some((a, b)))
    ///          .assert_eq(&z);
    ///     });
    /// }
    /// ```
    fn join_core<Tr2,I,L> (&self, stream2: &Arranged<G,Tr2>, result: L) -> Collection<G,I::Item,<R as Multiply<Tr2::R>>::Output>
    where
        Tr2: TraceReader<Key=K, Time=G::Timestamp>+Clone+'static,
        Tr2::Val: Ord+Clone+Debug+'static,
        Tr2::R: Semigroup,
        R: Multiply<Tr2::R>,
        <R as Multiply<Tr2::R>>::Output: Semigroup,
        I: IntoIterator,
        I::Item: Data,
        L: FnMut(&K,&V,&Tr2::Val)->I+'static,
        ;

    /// An unsafe variant of `join_core` where the `result` closure takes additional arguments for `time` and
    /// `diff` as input and returns an iterator over `(data, time, diff)` triplets. This allows for more
    /// flexibility, but is more error-prone.
    ///
    /// Each matching pair of records `(key, val1)` and `(key, val2)` are subjected to the `result` function,
    /// which produces something implementing `IntoIterator`, where the output collection will have an entry
    /// for every value returned by the iterator.
    ///
    /// This trait is implemented for arrangements (`Arranged<G, T>`) rather than collections. The `Join` trait
    /// contains the implementations for collections.
    ///
    /// # Examples
    ///
    /// ```
    /// extern crate timely;
    /// extern crate differential_dataflow;
    ///
    /// use differential_dataflow::input::Input;
    /// use differential_dataflow::operators::arrange::ArrangeByKey;
    /// use differential_dataflow::operators::join::JoinCore;
    /// use differential_dataflow::trace::Trace;
    /// use differential_dataflow::trace::implementations::ord::OrdValSpine;
    ///
    /// fn main() {
    ///     ::timely::example(|scope| {
    ///
    ///         let x = scope.new_collection_from(vec![(0u32, 1), (1, 3)]).1
    ///                      .arrange_by_key();
    ///         let y = scope.new_collection_from(vec![(0, 'a'), (1, 'b')]).1
    ///                      .arrange_by_key();
    ///
    ///         let z = scope.new_collection_from(vec![(1, 'a'), (3, 'b'), (3, 'b'), (3, 'b')]).1;
    ///
    ///         // Returned values have weight `a`
    ///         x.join_core_internal_unsafe(&y, |_key, &a, &b, &t, &r1, &r2| Some(((a, b), t.clone(), a)))
    ///          .assert_eq(&z);
    ///     });
    /// }
    /// ```
    fn join_core_internal_unsafe<Tr2,I,L,D,ROut> (&self, stream2: &Arranged<G,Tr2>, result: L) -> Collection<G,D,ROut>
    where
        Tr2: TraceReader<Key=K, Time=G::Timestamp>+Clone+'static,
        Tr2::Val: Ord+Clone+Debug+'static,
        Tr2::R: Semigroup,
        D: Data,
        ROut: Semigroup,
        I: IntoIterator<Item=(D, G::Timestamp, ROut)>,
        L: FnMut(&K,&V,&Tr2::Val,&G::Timestamp,&R,&Tr2::R)->I+'static,
        ;
}


impl<G, K, V, R> JoinCore<G, K, V, R> for Collection<G, (K, V), R>
where
    G: Scope,
    K: ExchangeData+Hashable,
    V: ExchangeData,
    R: ExchangeData+Semigroup,
    G::Timestamp: Lattice+Ord,
{
    fn join_core<Tr2,I,L> (&self, stream2: &Arranged<G,Tr2>, result: L) -> Collection<G,I::Item,<R as Multiply<Tr2::R>>::Output>
    where
        Tr2: TraceReader<Key=K, Time=G::Timestamp>+Clone+'static,
        Tr2::Val: Ord+Clone+Debug+'static,
        Tr2::R: Semigroup,
        R: Multiply<Tr2::R>,
        <R as Multiply<Tr2::R>>::Output: Semigroup,
        I: IntoIterator,
        I::Item: Data,
        L: FnMut(&K,&V,&Tr2::Val)->I+'static,
    {
        self.arrange_by_key()
            .join_core(stream2, result)
    }

    fn join_core_internal_unsafe<Tr2,I,L,D,ROut> (&self, stream2: &Arranged<G,Tr2>, result: L) -> Collection<G,D,ROut>
    where
        Tr2: TraceReader<Key=K, Time=G::Timestamp>+Clone+'static,
        Tr2::Val: Ord+Clone+Debug+'static,
        Tr2::R: Semigroup,
        R: Semigroup,
        D: Data,
        ROut: Semigroup,
        I: IntoIterator<Item=(D, G::Timestamp, ROut)>,
        L: FnMut(&K,&V,&Tr2::Val,&G::Timestamp,&R,&Tr2::R)->I+'static,
    {
        self.arrange_by_key().join_core_internal_unsafe(stream2, result)
    }

}

impl<G, T1> JoinCore<G, T1::Key, T1::Val, T1::R> for Arranged<G,T1>
    where
        G: Scope,
        G::Timestamp: Lattice+Ord+Debug,
        T1: TraceReader<Time=G::Timestamp>+Clone+'static,
        T1::Key: Ord+Debug+'static,
        T1::Val: Ord+Clone+Debug+'static,
        T1::R: Semigroup,
{
    fn join_core<Tr2,I,L>(&self, other: &Arranged<G,Tr2>, mut result: L) -> Collection<G,I::Item,<T1::R as Multiply<Tr2::R>>::Output>
    where
        Tr2::Val: Ord+Clone+Debug+'static,
        Tr2: TraceReader<Key=T1::Key,Time=G::Timestamp>+Clone+'static,
        Tr2::R: Semigroup,
        T1::R: Multiply<Tr2::R>,
        <T1::R as Multiply<Tr2::R>>::Output: Semigroup,
        I: IntoIterator,
        I::Item: Data,
        L: FnMut(&T1::Key,&T1::Val,&Tr2::Val)->I+'static
    {
        let result = move |k: &T1::Key, v1: &T1::Val, v2: &Tr2::Val, t: &G::Timestamp, r1: &T1::R, r2: &Tr2::R| {
            let t = t.clone();
            let r = (r1.clone()).multiply(r2);
            result(k, v1, v2).into_iter().map(move |d| (d, t.clone(), r.clone()))
        };
        self.join_core_internal_unsafe(other, result)
    }

    fn join_core_internal_unsafe<Tr2,I,L,D,ROut> (&self, other: &Arranged<G,Tr2>, mut result: L) -> Collection<G,D,ROut>
    where
        Tr2: TraceReader<Key=T1::Key, Time=G::Timestamp>+Clone+'static,
        Tr2::Val: Ord+Clone+Debug+'static,
        Tr2::R: Semigroup,
        D: Data,
        ROut: Semigroup,
        I: IntoIterator<Item=(D, G::Timestamp, ROut)>,
        L: FnMut(&T1::Key,&T1::Val,&Tr2::Val,&G::Timestamp,&T1::R,&Tr2::R)->I+'static,
    {
        // Rename traces for symmetry from here on out.
        let mut trace1 = self.trace.clone();
        let mut trace2 = other.trace.clone();

        self.stream.binary_frontier(&other.stream, Pipeline, Pipeline, "Join", move |capability, info| {

            // Acquire an activator to reschedule the operator when it has unfinished work.
            use timely::scheduling::Activator;
            let activations = self.stream.scope().activations().clone();
            let activator = Activator::new(&info.address[..], activations);

            // Our initial invariants are that for each trace, physical compaction is less or equal the trace's upper bound.
            // These invariants ensure that we can reference observed batch frontiers from `_start_upper` onward, as long as
            // we maintain our physical compaction capabilities appropriately. These assertions are tested as we load up the
            // initial work for the two traces, and before the operator is constructed.

            // Acknowledged frontier for each input.
            // These two are used exclusively to track batch boundaries on which we may want/need to call `cursor_through`.
            // They will drive our physical compaction of each trace, and we want to maintain at all times that each is beyond
            // the physical compaction frontier of their corresponding trace.
            // Should we ever *drop* a trace, these are 1. much harder to maintain correctly, but 2. no longer used.
            use timely::progress::frontier::Antichain;
            let mut acknowledged1 = Antichain::from_elem(<G::Timestamp>::minimum());
            let mut acknowledged2 = Antichain::from_elem(<G::Timestamp>::minimum());

            // deferred work of batches from each input.
            let mut todo1 = std::collections::VecDeque::new();
            let mut todo2 = std::collections::VecDeque::new();

            // We'll unload the initial batches here, to put ourselves in a less non-deterministic state to start.
            trace1.map_batches(|batch1| {
                acknowledged1.clone_from(batch1.upper());
                // No `todo1` work here, because we haven't accepted anything into `batches2` yet.
                // It is effectively "empty", because we choose to drain `trace1` before `trace2`.
                // Once we start streaming batches in, we will need to respond to new batches from
                // `input1` with logic that would have otherwise been here. Check out the next loop
                // for the structure.
            });
            // At this point, `ack1` should exactly equal `trace1.read_upper()`, as they are both determined by
            // iterating through batches and capturing the upper bound. This is a great moment to assert that
            // `trace1`'s physical compaction frontier is before the frontier of completed times in `trace1`.
            // TODO: in the case that this does not hold, instead "upgrade" the physical compaction frontier.
            assert!(PartialOrder::less_equal(&trace1.get_physical_compaction(), &acknowledged1.borrow()));

            // We capture batch2 cursors first and establish work second to avoid taking a `RefCell` lock
            // on both traces at the same time, as they could be the same trace and this would panic.
            let mut batch2_cursors = Vec::new();
            trace2.map_batches(|batch2| {
                acknowledged2.clone_from(batch2.upper());
                batch2_cursors.push((batch2.cursor(), batch2.clone()));
            });
            // At this point, `ack2` should exactly equal `trace2.read_upper()`, as they are both determined by
            // iterating through batches and capturing the upper bound. This is a great moment to assert that
            // `trace2`'s physical compaction frontier is before the frontier of completed times in `trace2`.
            // TODO: in the case that this does not hold, instead "upgrade" the physical compaction frontier.
            assert!(PartialOrder::less_equal(&trace2.get_physical_compaction(), &acknowledged2.borrow()));

            // Load up deferred work using trace2 cursors and batches captured just above.
            for (batch2_cursor, batch2) in batch2_cursors.into_iter() {
                // It is safe to ask for `ack1` because we have confirmed it to be in advance of `distinguish_since`.
                let (trace1_cursor, trace1_storage) = trace1.cursor_through(acknowledged1.borrow()).unwrap();
                // We could downgrade the capability here, but doing so is a bit complicated mathematically.
                // TODO: downgrade the capability by searching out the one time in `batch2.lower()` and not
                // in `batch2.upper()`. Only necessary for non-empty batches, as empty batches may not have
                // that property.
                todo2.push_back(Deferred::new(trace1_cursor, trace1_storage, batch2_cursor, batch2.clone(), capability.clone()));
            }

            // Droppable handles to shared trace data structures.
            let mut trace1_option = Some(trace1);
            let mut trace2_option = Some(trace2);

            // Swappable buffers for input extraction.
            let mut input1_buffer = Vec::new();
            let mut input2_buffer = Vec::new();

            move |input1, input2, output| {

                // 1. Consuming input.
                //
                // The join computation repeatedly accepts batches of updates from each of its inputs.
                //
                // For each accepted batch, it prepares a work-item to join the batch against previously "accepted"
                // updates from its other input. It is important to track which updates have been accepted, because
                // we use a shared trace and there may be updates present that are in advance of this accepted bound.
                //
                // Batches are accepted: 1. in bulk at start-up (above), 2. as we observe them in the input stream,
                // and 3. if the trace can confirm a region of empty space directly following our accepted bound.
                // This last case is a consequence of our inability to transmit empty batches, as they may be formed
                // in the absence of timely dataflow capabilities.

                // Drain input 1, prepare work.
                input1.for_each(|capability, data| {
                    // This test *should* always pass, as we only drop a trace in response to the other input emptying.
                    if let Some(ref mut trace2) = trace2_option {
                        let capability = capability.retain();
                        data.swap(&mut input1_buffer);
                        for batch1 in input1_buffer.drain(..) {
                            // Ignore any pre-loaded data.
                            if PartialOrder::less_equal(&acknowledged1, &batch1.lower()) {
                                if !batch1.is_empty() {
                                    // It is safe to ask for `ack2` as we validated that it was at least `get_physical_compaction()`
                                    // at start-up, and have held back physical compaction ever since.
                                    let (trace2_cursor, trace2_storage) = trace2.cursor_through(acknowledged2.borrow()).unwrap();
                                    let batch1_cursor = batch1.cursor();
                                    todo1.push_back(Deferred::new(trace2_cursor, trace2_storage, batch1_cursor, batch1.clone(), capability.clone()));
                                }

                                // To update `acknowledged1` we might presume that `batch1.lower` should equal it, but we
                                // may have skipped over empty batches. Still, the batches are in-order, and we should be
                                // able to just assume the most recent `batch1.upper`
                                debug_assert!(PartialOrder::less_equal(&acknowledged1, batch1.upper()));
                                acknowledged1.clone_from(batch1.upper());
                            }
                        }
                    }
                    else { panic!("`trace2_option` dropped before `input1` emptied!"); }
                });

                // Drain input 2, prepare work.
                input2.for_each(|capability, data| {
                    // This test *should* always pass, as we only drop a trace in response to the other input emptying.
                    if let Some(ref mut trace1) = trace1_option {
                        let capability = capability.retain();
                        data.swap(&mut input2_buffer);
                        for batch2 in input2_buffer.drain(..) {
                            // Ignore any pre-loaded data.
                            if PartialOrder::less_equal(&acknowledged2, &batch2.lower()) {
                                if !batch2.is_empty() {
                                    // It is safe to ask for `ack1` as we validated that it was at least `get_physical_compaction()`
                                    // at start-up, and have held back physical compaction ever since.
                                    let (trace1_cursor, trace1_storage) = trace1.cursor_through(acknowledged1.borrow()).unwrap();
                                    let batch2_cursor = batch2.cursor();
                                    todo2.push_back(Deferred::new(trace1_cursor, trace1_storage, batch2_cursor, batch2.clone(), capability.clone()));
                                }

                                // To update `acknowledged2` we might presume that `batch2.lower` should equal it, but we
                                // may have skipped over empty batches. Still, the batches are in-order, and we should be
                                // able to just assume the most recent `batch2.upper`
                                debug_assert!(PartialOrder::less_equal(&acknowledged2, batch2.upper()));
                                acknowledged2.clone_from(batch2.upper());
                            }
                        }
                    }
                    else { panic!("`trace1_option` dropped before `input2` emptied!"); }
                });

                // Advance acknowledged frontiers through any empty regions that we may not receive as batches.
                if let Some(trace1) = trace1_option.as_mut() {
                    trace1.advance_upper(&mut acknowledged1);
                }
                if let Some(trace2) = trace2_option.as_mut() {
                    trace2.advance_upper(&mut acknowledged2);
                }

                // 2. Join computation.
                //
                // For each of the inputs, we do some amount of work (measured in terms of number
                // of output records produced). This is meant to yield control to allow downstream
                // operators to consume and reduce the output, but it it also means to provide some
                // degree of responsiveness. There is a potential risk here that if we fall behind
                // then the increasing queues hold back physical compaction of the underlying traces
                // which results in unintentionally quadratic processing time (each batch of either
                // input must scan all batches from the other input).

                // Perform some amount of outstanding work.
                let mut fuel = 1_000_000;
                while !todo1.is_empty() && fuel > 0 {
                    todo1.front_mut().unwrap().work(
                        output,
                        |k,v2,v1,t,r2,r1| result(k,v1,v2,t,r1,r2),
                        &mut fuel
                    );
                    if !todo1.front().unwrap().work_remains() { todo1.pop_front(); }
                }

                // Perform some amount of outstanding work.
                let mut fuel = 1_000_000;
                while !todo2.is_empty() && fuel > 0 {
                    todo2.front_mut().unwrap().work(
                        output,
                        |k,v1,v2,t,r1,r2| result(k,v1,v2,t,r1,r2),
                        &mut fuel
                    );
                    if !todo2.front().unwrap().work_remains() { todo2.pop_front(); }
                }

                // Re-activate operator if work remains.
                if !todo1.is_empty() || !todo2.is_empty() {
                    activator.activate();
                }

                // 3. Trace maintenance.
                //
                // Importantly, we use `input.frontier()` here rather than `acknowledged` to track
                // the progress of an input, because should we ever drop one of the traces we will
                // lose the ability to extract information from anything other than the input.
                // For example, if we dropped `trace2` we would not be able to use `advance_upper`
                // to keep `acknowledged2` up to date wrt empty batches, and would hold back logical
                // compaction of `trace1`.

                // Maintain `trace1`. Drop if `input2` is empty, or advance based on future needs.
                if let Some(trace1) = trace1_option.as_mut() {
                    if input2.frontier().is_empty() { trace1_option = None; }
                    else {
                        // Allow `trace1` to compact logically up to the frontier we may yet receive,
                        // in the opposing input (`input2`). All `input2` times will be beyond this
                        // frontier, and joined times only need to be accurate when advanced to it.
                        trace1.set_logical_compaction(input2.frontier().frontier());
                        // Allow `trace1` to compact physically up to the upper bound of batches we
                        // have received in its input (`input1`). We will not require a cursor that
                        // is not beyond this bound.
                        trace1.set_physical_compaction(acknowledged1.borrow());
                    }
                }

                // Maintain `trace2`. Drop if `input1` is empty, or advance based on future needs.
                if let Some(trace2) = trace2_option.as_mut() {
                    if input1.frontier().is_empty() { trace2_option = None;}
                    else {
                        // Allow `trace2` to compact logically up to the frontier we may yet receive,
                        // in the opposing input (`input1`). All `input1` times will be beyond this
                        // frontier, and joined times only need to be accurate when advanced to it.
                        trace2.set_logical_compaction(input1.frontier().frontier());
                        // Allow `trace2` to compact physically up to the upper bound of batches we
                        // have received in its input (`input2`). We will not require a cursor that
                        // is not beyond this bound.
                        trace2.set_physical_compaction(acknowledged2.borrow());
                    }
                }
            }
        })
        .as_collection()
    }
}

/// Deferred join computation.
///
/// The structure wraps cursors which allow us to play out join computation at whatever rate we like.
/// This allows us to avoid producing and buffering massive amounts of data, without giving the timely
/// dataflow system a chance to run operators that can consume and aggregate the data.
struct Deferred<K, T, R, C1, C2, D>
where
    T: Timestamp+Lattice+Ord+Debug,
    R: Semigroup,
    C1: Cursor<Key=K, Time=T>,
    C2: Cursor<Key=K, Time=T>,
    C1::Val: Ord+Clone,
    C2::Val: Ord+Clone,
    C1::R: Semigroup,
    C2::R: Semigroup,
    D: Ord+Clone+Data,
{
    phant: ::std::marker::PhantomData<K>,
    trace: C1,
    trace_storage: C1::Storage,
    batch: C2,
    batch_storage: C2::Storage,
    capability: Capability<T>,
    done: bool,
    temp: Vec<((D, T), R)>,
}

impl<K, T, R, C1, C2, D> Deferred<K, T, R, C1, C2, D>
where
    K: Ord+Debug+Eq,
    C1: Cursor<Key=K, Time=T>,
    C2: Cursor<Key=K, Time=T>,
    C1::Val: Ord+Clone+Debug,
    C2::Val: Ord+Clone+Debug,
    C1::R: Semigroup,
    C2::R: Semigroup,
    T: Timestamp+Lattice+Ord+Debug,
    R: Semigroup,
    D: Clone+Data,
{
    fn new(trace: C1, trace_storage: C1::Storage, batch: C2, batch_storage: C2::Storage, capability: Capability<T>) -> Self {
        Deferred {
            phant: ::std::marker::PhantomData,
            trace,
            trace_storage,
            batch,
            batch_storage,
            capability,
            done: false,
            temp: Vec::new(),
        }
    }

    fn work_remains(&self) -> bool {
        !self.done
    }

    /// Process keys until at least `fuel` output tuples produced, or the work is exhausted.
    #[inline(never)]
    fn work<L, I>(&mut self, output: &mut OutputHandle<T, (D, T, R), Tee<T, (D, T, R)>>, mut logic: L, fuel: &mut usize)
    where I: IntoIterator<Item=(D, T, R)>, L: FnMut(&K, &C1::Val, &C2::Val, &T, &C1::R, &C2::R)->I {

        let meet = self.capability.time();

        let mut effort = 0;
        let mut session = output.session(&self.capability);

        let trace_storage = &self.trace_storage;
        let batch_storage = &self.batch_storage;

        let trace = &mut self.trace;
        let batch = &mut self.batch;

        let temp = &mut self.temp;
        let mut thinker = JoinThinker::new();

        while batch.key_valid(batch_storage) && trace.key_valid(trace_storage) && effort < *fuel {

            match trace.key(trace_storage).cmp(batch.key(batch_storage)) {
                Ordering::Less => trace.seek_key(trace_storage, batch.key(batch_storage)),
                Ordering::Greater => batch.seek_key(batch_storage, trace.key(trace_storage)),
                Ordering::Equal => {

                    thinker.history1.edits.load(trace, trace_storage, |time| time.join(&meet));
                    thinker.history2.edits.load(batch, batch_storage, |time| time.clone());

                    assert_eq!(temp.len(), 0);

                    // populate `temp` with the results in the best way we know how.
                    thinker.think(|v1,v2,t,r1,r2| {
                        let key = batch.key(batch_storage);
                        for (d, t, r) in logic(key, v1, v2, &t, r1, r2) {
                            temp.push(((d, t), r));
                        }
                    });

                    // TODO: This consolidation is optional, and it may not be very
                    //       helpful. We might try harder to understand whether we
                    //       should do this work here, or downstream at consumers.
                    // TODO: Perhaps `thinker` should have the buffer, do smarter
                    //       consolidation, and then deposit results in `session`.
                    crate::consolidation::consolidate(temp);

                    effort += temp.len();
                    for ((d, t), r) in temp.drain(..) {
                        session.give((d, t, r));
                    }

                    batch.step_key(batch_storage);
                    trace.step_key(trace_storage);

                    thinker.history1.clear();
                    thinker.history2.clear();
                }
            }
        }

        self.done = !batch.key_valid(batch_storage) || !trace.key_valid(trace_storage);

        if effort > *fuel { *fuel = 0; }
        else              { *fuel -= effort; }
    }
}

struct JoinThinker<'a, V1: Ord+Clone+'a, V2: Ord+Clone+'a, T: Lattice+Ord+Clone, R1: Semigroup, R2: Semigroup> {
    pub history1: ValueHistory<'a, V1, T, R1>,
    pub history2: ValueHistory<'a, V2, T, R2>,
}

impl<'a, V1: Ord+Clone, V2: Ord+Clone, T: Lattice+Ord+Clone, R1: Semigroup, R2: Semigroup> JoinThinker<'a, V1, V2, T, R1, R2>
where V1: Debug, V2: Debug, T: Debug
{
    fn new() -> Self {
        JoinThinker {
            history1: ValueHistory::new(),
            history2: ValueHistory::new(),
        }
    }

    fn think<F: FnMut(&V1,&V2,T,&R1,&R2)>(&mut self, mut results: F) {

        // for reasonably sized edits, do the dead-simple thing.
        if self.history1.edits.len() < 10 || self.history2.edits.len() < 10 {
            self.history1.edits.map(|v1, t1, d1| {
                self.history2.edits.map(|v2, t2, d2| {
                    results(v1, v2, t1.join(t2), &d1, &d2);
                })
            })
        }
        else {

            let mut replay1 = self.history1.replay();
            let mut replay2 = self.history2.replay();

            // TODO: It seems like there is probably a good deal of redundant `advance_buffer_by`
            //       in here. If a time is ever repeated, for example, the call will be identical
            //       and accomplish nothing. If only a single record has been added, it may not
            //       be worth the time to collapse (advance, re-sort) the data when a linear scan
            //       is sufficient.

            while !replay1.is_done() && !replay2.is_done() {

                if replay1.time().unwrap().cmp(&replay2.time().unwrap()) == ::std::cmp::Ordering::Less {
                    replay2.advance_buffer_by(replay1.meet().unwrap());
                    for &((ref val2, ref time2), ref diff2) in replay2.buffer().iter() {
                        let (val1, time1, ref diff1) = replay1.edit().unwrap();
                        results(val1, val2, time1.join(time2), diff1, diff2);
                    }
                    replay1.step();
                }
                else {
                    replay1.advance_buffer_by(replay2.meet().unwrap());
                    for &((ref val1, ref time1), ref diff1) in replay1.buffer().iter() {
                        let (val2, time2, ref diff2) = replay2.edit().unwrap();
                        results(val1, val2, time1.join(time2), diff1, diff2);
                    }
                    replay2.step();
                }
            }

            while !replay1.is_done() {
                replay2.advance_buffer_by(replay1.meet().unwrap());
                for &((ref val2, ref time2), ref diff2) in replay2.buffer().iter() {
                    let (val1, time1, ref diff1) = replay1.edit().unwrap();
                    results(val1, val2, time1.join(time2), diff1, diff2);
                }
                replay1.step();
            }
            while !replay2.is_done() {
                replay1.advance_buffer_by(replay2.meet().unwrap());
                for &((ref val1, ref time1), ref diff1) in replay1.buffer().iter() {
                    let (val2, time2, ref diff2) = replay2.edit().unwrap();
                    results(val1, val2, time1.join(time2), diff1, diff2);
                }
                replay2.step();
            }
        }
    }
}
