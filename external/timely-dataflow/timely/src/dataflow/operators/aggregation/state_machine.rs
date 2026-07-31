//! General purpose state transition operator.
use std::collections::HashMap;
use std::hash::Hash;

use crate::dataflow::channels::pact::Exchange;
use crate::dataflow::operators::generic::operator::Operator;
use crate::dataflow::{Scope, Stream};
use crate::{Data, ExchangeData};

/// Generic state-transition machinery: each key has a state, and receives a sequence of events.
/// Events are applied in time-order, but no other promises are made. Each state transition can
/// produce output, which is sent.
///
/// `state_machine` will buffer inputs if earlier inputs may still arrive. it will directly apply
/// updates for the current time reflected in the notificator, though. In the case of partially
/// ordered times, the only guarantee is that updates are not applied out of order, not that there
/// is some total order on times respecting the total order (updates may be interleaved).

/// Provides the `state_machine` method.
pub trait StateMachine<S: Scope, K: ExchangeData + Hash + Eq, V: ExchangeData> {
    /// Tracks a state for each presented key, using user-supplied state transition logic.
    ///
    /// The transition logic `fold` may mutate the state, and produce both output records and
    /// a `bool` indicating that it is appropriate to deregister the state, cleaning up once
    /// the state is no longer helpful.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Map, Inspect};
    /// use timely::dataflow::operators::aggregation::StateMachine;
    ///
    /// timely::example(|scope| {
    ///
    ///     // these results happen to be right, but aren't guaranteed.
    ///     // the system is at liberty to re-order within a timestamp.
    ///     let result = vec![(0,0), (0,2), (0,6), (0,12), (0,20),
    ///                       (1,1), (1,4), (1,9), (1,16), (1,25)];
    ///
    ///         (0..10).to_stream(scope)
    ///                .map(|x| (x % 2, x))
    ///                .state_machine(
    ///                    |_key, val, agg| { *agg += val; (false, Some((*_key, *agg))) },
    ///                    |key| *key as u64
    ///                )
    ///                .inspect(move |x| assert!(result.contains(x)));
    /// });
    /// ```
    fn state_machine<
        R: Data,                                     // output type
        D: Default + 'static,                        // per-key state (data)
        I: IntoIterator<Item = R>,                   // type of output iterator
        F: Fn(&K, V, &mut D) -> (bool, I) + 'static, // state update logic
        H: Fn(&K) -> u64 + 'static,                  // "hash" function for keys
    >(
        &self,
        fold: F,
        hash: H,
    ) -> Stream<S, R>
    where
        S::Timestamp: Hash + Eq;
}

impl<S: Scope, K: ExchangeData + Hash + Eq, V: ExchangeData> StateMachine<S, K, V>
    for Stream<S, (K, V)>
{
    fn state_machine<
        R: Data,                                     // output type
        D: Default + 'static,                        // per-key state (data)
        I: IntoIterator<Item = R>,                   // type of output iterator
        F: Fn(&K, V, &mut D) -> (bool, I) + 'static, // state update logic
        H: Fn(&K) -> u64 + 'static,                  // "hash" function for keys
    >(
        &self,
        fold: F,
        hash: H,
    ) -> Stream<S, R>
    where
        S::Timestamp: Hash + Eq,
    {
        let mut pending: HashMap<_, Vec<(K, V)>> = HashMap::new(); // times -> (keys -> state)
        let mut states = HashMap::new(); // keys -> state

        let mut vector = Vec::new();

        self.unary_notify(
            Exchange::new(move |&(ref k, _)| hash(k)),
            "StateMachine",
            vec![],
            move |input, output, notificator| {
                // go through each time with data, process each (key, val) pair.
                notificator.for_each(|time, _, _| {
                    if let Some(pend) = pending.remove(time.time()) {
                        let mut session = output.session(&time);
                        for (key, val) in pend {
                            let (remove, output) = {
                                let state =
                                    states.entry(key.clone()).or_insert_with(Default::default);
                                fold(&key, val, state)
                            };
                            if remove {
                                states.remove(&key);
                            }
                            session.give_iterator(output.into_iter());
                        }
                    }
                });

                // stash each input and request a notification when ready
                input.for_each(|time, data| {
                    data.swap(&mut vector);

                    // stash if not time yet
                    if notificator.frontier(0).less_than(time.time()) {
                        pending
                            .entry(time.time().clone())
                            .or_insert_with(Vec::new)
                            .extend(vector.drain(..));
                        notificator.notify_at(time.retain());
                    } else {
                        // else we can process immediately
                        let mut session = output.session(&time);
                        for (key, val) in vector.drain(..) {
                            let (remove, output) = {
                                let state =
                                    states.entry(key.clone()).or_insert_with(Default::default);
                                fold(&key, val, state)
                            };
                            if remove {
                                states.remove(&key);
                            }
                            session.give_iterator(output.into_iter());
                        }
                    }
                });
            },
        )
    }
}
