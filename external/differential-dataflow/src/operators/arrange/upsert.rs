//! Support for forming collections from streams of upsert.
//!
//! Upserts are sequences of keyed optional values, and they define a collection of
//! the pairs of keys and each's most recent value, if it is present. Element in the
//! sequence effectively overwrites the previous value at the key, if present, and if
//! the value is not present it uninstalls the key.
//!
//! Upserts are non-trivial because they do not themselves describe the deletions that
//! the `Collection` update stream must present. However, if one creates an `Arrangement`
//! then this state provides sufficient information. The arrangement will continue to
//! exist even if dropped until the input or dataflow shuts down, as the upsert operator
//! itself needs access to its accumulated state.
//!
//! # Notes
//!
//! Upserts currently only work with totally ordered timestamps.
//!
//! In the case of ties in timestamps (concurrent updates to the same key) they choose
//! the *greatest* value according to `Option<Val>` ordering, which will prefer a value
//! to `None` and choose the greatest value (informally, as if applied in order of value).
//!
//! If the same value is repeated, no change will occur in the output. That may make this
//! operator effective at determining the difference between collections of keyed values,
//! but note that it will not notice the absence of keys in a collection.
//!
//! To effect "filtering" in a way that reduces the arrangement footprint, apply a map to
//! the input stream, mapping values that fail the predicate to `None` values, like so:
//!
//! ```ignore
//! // Dropped values should be retained as "uninstall" upserts.
//! upserts.map(|(key,opt_val)| (key, opt_val.filter(predicate)))
//! ```
//!
//! # Example
//!
//! ```rust
//! extern crate timely;
//! extern crate differential_dataflow;
//!
//! fn main() {
//!
//!     // define a new timely dataflow computation.
//!     timely::execute_from_args(std::env::args().skip(1), move |worker| {
//!
//!         type Key = String;
//!         type Val = String;
//!
//!         let mut input = timely::dataflow::InputHandle::new();
//!         let mut probe = timely::dataflow::ProbeHandle::new();
//!
//!         // Create a dataflow demonstrating upserts.
//!         //
//!         // Upserts are a sequence of records (key, option<val>) where the intended
//!         // value associated with a key is the most recent value, and if that is a
//!         // `none` then the key is removed (until a new value shows up).
//!         //
//!         // The challenge with upserts is that the value to *retract* isn't supplied
//!         // as part of the input stream. We have to determine what it should be!
//!
//!         worker.dataflow(|scope| {
//!
//!             use timely::dataflow::operators::Input;
//!             use differential_dataflow::trace::implementations::ord::OrdValSpine;
//!             use differential_dataflow::operators::arrange::upsert;
//!
//!             let stream = scope.input_from(&mut input);
//!             let arranged = upsert::arrange_from_upsert::<_, OrdValSpine<Key, Val, _, _>>(&stream, &"test");
//!
//!             arranged
//!                 .as_collection(|k,v| (k.clone(), v.clone()))
//!                 .inspect(|x| println!("Observed: {:?}", x))
//!                 .probe_with(&mut probe);
//!         });
//!
//!         // Introduce the key, with a specific value.
//!         input.send(("frank".to_string(), Some("mcsherry".to_string()), 3));
//!         input.advance_to(4);
//!         while probe.less_than(input.time()) { worker.step(); }
//!
//!         // Change the value to a different value.
//!         input.send(("frank".to_string(), Some("zappa".to_string()), 4));
//!         input.advance_to(5);
//!         while probe.less_than(input.time()) { worker.step(); }
//!
//!         // Remove the key and its value.
//!         input.send(("frank".to_string(), None, 5));
//!         input.advance_to(9);
//!         while probe.less_than(input.time()) { worker.step(); }
//!
//!         // Introduce a new totally different value
//!         input.send(("frank".to_string(), Some("oz".to_string()), 9));
//!         input.advance_to(10);
//!         while probe.less_than(input.time()) { worker.step(); }
//!
//!         // Repeat the value, which should produce no output.
//!         input.send(("frank".to_string(), Some("oz".to_string()), 11));
//!         input.advance_to(12);
//!         while probe.less_than(input.time()) { worker.step(); }

//!         // Remove the key and value.
//!         input.send(("frank".to_string(), None, 15));
//!         input.close();
//!
//!     }).unwrap();
//! }
//! ```

use std::collections::{BinaryHeap, HashMap};

use timely::order::{PartialOrder, TotalOrder};
use timely::dataflow::{Scope, Stream};
use timely::dataflow::operators::generic::Operator;
use timely::dataflow::channels::pact::Exchange;
use timely::progress::Timestamp;
use timely::progress::Antichain;
use timely::dataflow::operators::Capability;

use ::{ExchangeData, Hashable};
use lattice::Lattice;
use trace::{Trace, TraceReader, Batch, Cursor};

use trace::Builder;

use operators::arrange::arrangement::Arranged;

use super::TraceAgent;

/// Arrange data from a stream of keyed upserts.
///
/// The input should be a stream of timestamped pairs of Key and Option<Val>.
/// The contents of the collection are defined key-by-key, where each optional
/// value in sequence either replaces or removes the existing value, should it
/// exist.
///
/// This method is only implemented for totally ordered times, as we do not yet
/// understand what a "sequence" of upserts would mean for partially ordered
/// timestamps.
pub fn arrange_from_upsert<G, Tr>(
    stream: &Stream<G, (Tr::Key, Option<Tr::Val>, G::Timestamp)>,
    name: &str,
) -> Arranged<G, TraceAgent<Tr>>
where
    G: Scope,
    G::Timestamp: Lattice+Ord+TotalOrder+ExchangeData,
    Tr::Key: ExchangeData+Hashable+std::hash::Hash,
    Tr::Val: ExchangeData,
    Tr: Trace+TraceReader<Time=G::Timestamp,R=isize>+'static,
    Tr::Batch: Batch,
{
    let mut reader: Option<TraceAgent<Tr>> = None;

    // fabricate a data-parallel operator using the `unary_notify` pattern.
    let stream = {

        let reader = &mut reader;

        let exchange = Exchange::new(move |update: &(Tr::Key,Option<Tr::Val>,G::Timestamp)| (update.0).hashed().into());

        stream.unary_frontier(exchange, name, move |_capability, info| {

            // Acquire a logger for arrange events.
            let logger = {
                let scope = stream.scope();
                let register = scope.log_register();
                register.get::<::logging::DifferentialEvent>("differential/arrange")
            };

            // Establish compaction effort to apply even without updates.
            let (activator, effort) =
            if let Some(effort) = stream.scope().config().get::<isize>("differential/idle_merge_effort").cloned() {
                (Some(stream.scope().activator_for(&info.address[..])), Some(effort))
            }
            else {
                (None, None)
            };

            // Tracks the lower envelope of times in `priority_queue`.
            let mut capabilities = Antichain::<Capability<G::Timestamp>>::new();
            let mut buffer = Vec::new();
            // Form the trace we will both use internally and publish.
            let empty_trace = Tr::new(info.clone(), logger.clone(), activator);
            let (mut reader_local, mut writer) = TraceAgent::new(empty_trace, info, logger);
            // Capture the reader outside the builder scope.
            *reader = Some(reader_local.clone());

            // Tracks the input frontier, used to populate the lower bound of new batches.
            let mut prev_frontier = Antichain::from_elem(<G::Timestamp as Timestamp>::minimum());

            // For stashing input upserts, ordered increasing by time (`BinaryHeap` is a max-heap).
            let mut priority_queue = BinaryHeap::<std::cmp::Reverse<(G::Timestamp, Tr::Key, Option<Tr::Val>)>>::new();
            let mut updates = Vec::new();

            move |input, output| {

                // Stash capabilities and associated data (ordered by time).
                input.for_each(|cap, data| {
                    capabilities.insert(cap.retain());
                    data.swap(&mut buffer);
                    for (key, val, time) in buffer.drain(..) {
                        priority_queue.push(std::cmp::Reverse((time, key, val)))
                    }
                });

                // Assert that the frontier never regresses.
                assert!(PartialOrder::less_equal(&prev_frontier.borrow(), &input.frontier().frontier()));

                // Test to see if strict progress has occurred, which happens whenever the new
                // frontier isn't equal to the previous. It is only in this case that we have any
                // data processing to do.
                if prev_frontier.borrow() != input.frontier().frontier() {

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

                                // Extract upserts available to process as of this `upper`.
                                let mut to_process = HashMap::new();
                                while priority_queue.peek().map(|std::cmp::Reverse((t,_k,_v))| !upper.less_equal(t)).unwrap_or(false) {
                                    let std::cmp::Reverse((time, key, val)) = priority_queue.pop().expect("Priority queue just ensured non-empty");
                                    to_process.entry(key).or_insert(Vec::new()).push((time, std::cmp::Reverse(val)));
                                }
                                // Reduce the allocation behind the priority queue if it is presently excessive.
                                // A factor of four is used to avoid repeated doubling and shrinking.
                                // TODO: if the queue were a sequence of geometrically sized allocations, we could
                                // shed the additional capacity without copying any data.
                                if priority_queue.capacity() > 4 * priority_queue.len() {
                                    priority_queue.shrink_to_fit();
                                }

                                // Put (key, list) into key order, to match cursor enumeration.
                                let mut to_process = to_process.into_iter().collect::<Vec<_>>();
                                to_process.sort();

                                // Prepare a cursor to the existing arrangement, and a batch builder for
                                // new stuff that we add.
                                let (mut trace_cursor, trace_storage) = reader_local.cursor();
                                let mut builder = <Tr::Batch as Batch>::Builder::new();
                                for (key, mut list) in to_process.drain(..) {

                                    // The prior value associated with the key.
                                    let mut prev_value: Option<Tr::Val> = None;

                                    // Attempt to find the key in the trace.
                                    trace_cursor.seek_key(&trace_storage, &key);
                                    if trace_cursor.get_key(&trace_storage) == Some(&key) {
                                        // Determine the prior value associated with the key.
                                        while let Some(val) = trace_cursor.get_val(&trace_storage) {
                                            let mut count = 0;
                                            trace_cursor.map_times(&trace_storage, |_time, diff| count += *diff);
                                            assert!(count == 0 || count == 1);
                                            if count == 1 {
                                                assert!(prev_value.is_none());
                                                prev_value = Some(val.clone());
                                            }
                                            trace_cursor.step_val(&trace_storage);
                                        }
                                        trace_cursor.step_key(&trace_storage);
                                    }

                                    // Sort the list of upserts to `key` by their time, suppress multiple updates.
                                    list.sort();
                                    list.dedup_by(|(t1,_), (t2,_)| t1 == t2);
                                    for (time, std::cmp::Reverse(next)) in list {
                                        if prev_value != next {
                                            if let Some(prev) = prev_value {
                                                updates.push((key.clone(), prev, time.clone(), -1));
                                            }
                                            if let Some(next) = next.as_ref() {
                                                updates.push((key.clone(), next.clone(), time.clone(), 1));
                                            }
                                            prev_value = next;
                                        }
                                    }
                                    // Must insert updates in (key, val, time) order.
                                    updates.sort();
                                    for update in updates.drain(..) {
                                        builder.push(update);
                                    }
                                }
                                let batch = builder.done(prev_frontier.clone(), upper.clone(), Antichain::from_elem(G::Timestamp::minimum()));
                                prev_frontier.clone_from(&upper);

                                // Communicate `batch` to the arrangement and the stream.
                                writer.insert(batch.clone(), Some(capability.time().clone()));
                                output.session(&capabilities.elements()[index]).give(batch);
                            }
                        }

                        // Having extracted and sent batches between each capability and the input frontier,
                        // we should downgrade all capabilities to match the batcher's lower update frontier.
                        // This may involve discarding capabilities, which is fine as any new updates arrive
                        // in messages with new capabilities.

                        let mut new_capabilities = Antichain::new();
                        if let Some(std::cmp::Reverse((time, _, _))) = priority_queue.peek() {
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
                        writer.seal(input.frontier().frontier().to_owned());
                    }

                    // Update our view of the input frontier.
                    prev_frontier.clear();
                    prev_frontier.extend(input.frontier().frontier().iter().cloned());

                    // Downgrade capabilities for `reader_local`.
                    reader_local.set_logical_compaction(prev_frontier.borrow());
                    reader_local.set_physical_compaction(prev_frontier.borrow());
                }

                if let Some(mut fuel) = effort.clone() {
                    writer.exert(&mut fuel);
                }
            }
        })
    };

    Arranged { stream: stream, trace: reader.unwrap() }

}
