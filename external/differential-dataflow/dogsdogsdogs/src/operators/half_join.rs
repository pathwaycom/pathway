//! Dataflow operator for delta joins over partially ordered timestamps.
//!
//! Given multiple streams of updates `(data, time, diff)` that are each
//! defined over the same partially ordered `time`, we want to form the
//! full cross-join of all relations (we will *later* apply some filters
//! and instead equijoin on keys).
//!
//! The "correct" output is the outer join of these triples, where
//!   1. The `data` entries are just tuple'd up together,
//!   2. The `time` entries are subjected to the lattice `join` operator,
//!   3. The `diff` entries are multiplied.
//!
//! One way to produce the correct output is to form independent dataflow
//! fragments for each input stream, such that each intended output is then
//! produced by exactly one of these input streams.
//!
//! There are several incorrect ways one might do this, but here is one way
//! that I hope is not incorrect:
//!
//! Each input stream of updates is joined with each other input collection,
//! where each input update is matched against each other input update that
//! has a `time` that is less-than the input update's `time`, *UNDER A TOTAL
//! ORDER ON `time`*. The output are the `(data, time, diff)` entries that
//! follow the rules above, except that we additionally preserve the input's
//! initial `time` as well, for use in subsequent joins with the other input
//! collections.
//!
//! There are some caveats about ties, and we should treat each `time` for
//! each input as occuring at distinct times, one after the other (so that
//! ties are resolved by the index of the input). There is also the matter
//! of logical compaction, which should not be done in a way that prevents
//! the correct determination of the total order comparison.

use std::collections::HashMap;

use timely::dataflow::Scope;
use timely::dataflow::channels::pact::{Pipeline, Exchange};
use timely::dataflow::operators::Operator;
use timely::progress::Antichain;

use differential_dataflow::{ExchangeData, Collection, AsCollection, Hashable};
use differential_dataflow::difference::{Monoid, Semigroup};
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::trace::{Cursor, TraceReader};
use differential_dataflow::consolidation::{consolidate, consolidate_updates};

/// A binary equijoin that responds to updates on only its first input.
///
/// This operator responds to inputs of the form
///
/// ```ignore
/// ((key, val1, time1), initial_time, diff1)
/// ```
///
/// where `initial_time` is less or equal to `time1`, and produces as output
///
/// ```ignore
/// ((output_func(key, val1, val2), lub(time1, time2)), initial_time, diff1 * diff2)
/// ```
///
/// for each `((key, val2), time2, diff2)` present in `arrangement`, where
/// `time2` is less than `initial_time` *UNDER THE TOTAL ORDER ON TIMES*.
/// This last constraint is important to ensure that we correctly produce
/// all pairs of output updates across multiple `half_join` operators.
///
/// Notice that the time is hoisted up into data. The expectation is that
/// once out of the "delta flow region", the updates will be `delay`d to the
/// times specified in the payloads.
pub fn half_join<G, V, Tr, FF, CF, DOut, S>(
    stream: &Collection<G, (Tr::Key, V, G::Timestamp), Tr::R>,
    arrangement: Arranged<G, Tr>,
    frontier_func: FF,
    comparison: CF,
    mut output_func: S,
) -> Collection<G, (DOut, G::Timestamp), Tr::R>
where
    G: Scope,
    G::Timestamp: Lattice,
    V: ExchangeData,
    Tr: TraceReader<Time=G::Timestamp>+Clone+'static,
    Tr::Key: Ord+Hashable+ExchangeData,
    Tr::Val: Clone,
    Tr::R: Monoid+ExchangeData,
    FF: Fn(&G::Timestamp) -> G::Timestamp + 'static,
    CF: Fn(&G::Timestamp, &G::Timestamp) -> bool + 'static,
    DOut: Clone+'static,
    Tr::R: std::ops::Mul<Tr::R, Output=Tr::R>,
    S: FnMut(&Tr::Key, &V, &Tr::Val)->DOut+'static,
{
    let output_func = move |k: &Tr::Key, v1: &V, v2: &Tr::Val, initial: &G::Timestamp, time: &G::Timestamp, diff1: &Tr::R, diff2: &Tr::R| {
        let diff = diff1.clone() * diff2.clone();
        let dout = (output_func(k, v1, v2), time.clone());
        Some((dout, initial.clone(), diff))
    };
    half_join_internal_unsafe(stream, arrangement, frontier_func, comparison, |_timer, _count| false, output_func)
}

/// An unsafe variant of `half_join` where the `output_func` closure takes
/// additional arguments for `time` and `diff` as input and returns an iterator
/// over `(data, time, diff)` triplets. This allows for more flexibility, but
/// is more error-prone.
///
/// This operator responds to inputs of the form
///
/// ```ignore
/// ((key, val1, time1), initial_time, diff1)
/// ```
///
/// where `initial_time` is less or equal to `time1`, and produces as output
///
/// ```ignore
/// output_func(key, val1, val2, initial_time, lub(time1, time2), diff1, diff2)
/// ```
///
/// for each `((key, val2), time2, diff2)` present in `arrangement`, where
/// `time2` is less than `initial_time` *UNDER THE TOTAL ORDER ON TIMES*.
///
/// The `yield_function` allows the caller to indicate when the operator should
/// yield control, as a function of the elapsed time and the number of matched
/// records. Note this is not the number of *output* records, owing mainly to
/// the number of matched records being easiest to record with low overhead.
pub fn half_join_internal_unsafe<G, V, Tr, FF, CF, DOut, ROut, Y, I, S>(
    stream: &Collection<G, (Tr::Key, V, G::Timestamp), Tr::R>,
    mut arrangement: Arranged<G, Tr>,
    frontier_func: FF,
    comparison: CF,
    yield_function: Y,
    mut output_func: S,
) -> Collection<G, DOut, ROut>
where
    G: Scope,
    G::Timestamp: Lattice,
    V: ExchangeData,
    Tr: TraceReader<Time=G::Timestamp>+Clone+'static,
    Tr::Key: Ord+Hashable+ExchangeData,
    Tr::Val: Clone,
    Tr::R: Monoid+ExchangeData,
    FF: Fn(&G::Timestamp) -> G::Timestamp + 'static,
    CF: Fn(&G::Timestamp, &G::Timestamp) -> bool + 'static,
    DOut: Clone+'static,
    ROut: Monoid,
    Y: Fn(std::time::Instant, usize) -> bool + 'static,
    I: IntoIterator<Item=(DOut, G::Timestamp, ROut)>,
    S: FnMut(&Tr::Key, &V, &Tr::Val, &G::Timestamp, &G::Timestamp, &Tr::R, &Tr::R)-> I + 'static,
{
    // No need to block physical merging for this operator.
    arrangement.trace.set_physical_compaction(Antichain::new().borrow());
    let mut arrangement_trace = Some(arrangement.trace);
    let arrangement_stream = arrangement.stream;

    let mut stash = HashMap::new();
    let mut buffer = Vec::new();

    let exchange = Exchange::new(move |update: &((Tr::Key, V, G::Timestamp),G::Timestamp,Tr::R)| (update.0).0.hashed().into());

    // Stash for (time, diff) accumulation.
    let mut output_buffer = Vec::new();

    stream.inner.binary_frontier(&arrangement_stream, exchange, Pipeline, "HalfJoin", move |_,info| {

        // Acquire an activator to reschedule the operator when it has unfinished work.
        use timely::scheduling::Activator;
        let activations = stream.scope().activations();
        let activator = Activator::new(&info.address[..], activations);

        move |input1, input2, output| {

            // drain the first input, stashing requests.
            input1.for_each(|capability, data| {
                data.swap(&mut buffer);
                stash.entry(capability.retain())
                    .or_insert(Vec::new())
                    .extend(buffer.drain(..))
            });

            // Drain input batches; although we do not observe them, we want access to the input
            // to observe the frontier and to drive scheduling.
            input2.for_each(|_, _| { });

            // Local variables to track if and when we should exit early.
            // The rough logic is that we fully process inputs and set their differences to zero,
            // stopping at any point. We clean up all of the zeros in buffers that did any work,
            // and reactivate at the end if the yield function still says so.
            let mut yielded = false;
            let timer = std::time::Instant::now();
            let mut work = 0;

            if let Some(ref mut trace) = arrangement_trace {

                for (capability, proposals) in stash.iter_mut() {

                    // Avoid computation if we should already yield.
                    // TODO: Verify this is correct for TOTAL ORDER.
                    yielded = yielded || yield_function(timer, work);
                    if !yielded && !input2.frontier.less_equal(capability.time()) {

                        let mut session = output.session(capability);

                        // Sort requests by key for in-order cursor traversal.
                        consolidate_updates(proposals);

                        let (mut cursor, storage) = trace.cursor();

                        // Process proposals one at a time, stopping if we should yield.
                        for &mut ((ref key, ref val1, ref time), ref initial, ref mut diff1) in proposals.iter_mut() {
                            // Use TOTAL ORDER to allow the release of `time`.
                            yielded = yielded || yield_function(timer, work);
                            if !yielded && !input2.frontier.frontier().iter().any(|t| comparison(t, initial)) {
                                cursor.seek_key(&storage, &key);
                                if cursor.get_key(&storage) == Some(&key) {
                                    while let Some(val2) = cursor.get_val(&storage) {
                                        cursor.map_times(&storage, |t, d| {
                                            if comparison(t, initial) {
                                                output_buffer.push((t.join(time), d.clone()))
                                            }
                                        });
                                        consolidate(&mut output_buffer);
                                        work += output_buffer.len();
                                        for (time, diff2) in output_buffer.drain(..) {
                                            for dout in output_func(key, val1, val2, initial, &time, &diff1, &diff2) {
                                                session.give(dout);
                                            }
                                        }
                                        cursor.step_val(&storage);
                                    }
                                    cursor.rewind_vals(&storage);
                                }
                                *diff1 = Tr::R::zero();
                            }
                        }

                        proposals.retain(|ptd| !ptd.2.is_zero());
                    }
                }
            }

            // If we yielded, re-activate the operator.
            if yielded {
                activator.activate();
            }

            // drop fully processed capabilities.
            stash.retain(|_,proposals| !proposals.is_empty());

            // The logical merging frontier depends on both input1 and stash.
            let mut frontier = timely::progress::frontier::Antichain::new();
            for time in input1.frontier().frontier().iter() {
                frontier.insert(frontier_func(time));
            }
            for key in stash.keys() {
                frontier.insert(frontier_func(key.time()));
            }
            arrangement_trace.as_mut().map(|trace| trace.set_logical_compaction(frontier.borrow()));

            if input1.frontier().is_empty() && stash.is_empty() {
                arrangement_trace = None;
            }
        }
    }).as_collection()
}
