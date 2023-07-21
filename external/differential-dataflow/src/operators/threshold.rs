//! Reduce the collection to one occurrence of each distinct element.
//!
//! The `distinct_total` and `distinct_total_u` operators are optimizations of the more general
//! `distinct` and `distinct_u` operators for the case in which time is totally ordered.

use timely::order::TotalOrder;
use timely::dataflow::*;
use timely::dataflow::operators::Operator;
use timely::dataflow::channels::pact::Pipeline;

use lattice::Lattice;
use ::{ExchangeData, Collection, Data};
use ::difference::{Semigroup, Abelian};
use hashable::Hashable;
use collection::AsCollection;
use operators::arrange::{Arranged, ArrangeBySelf};
use trace::{BatchReader, Cursor, TraceReader};

/// Extension trait for the `distinct` differential dataflow method.
pub trait ThresholdTotal<G: Scope, K: Data, R: Semigroup> where G::Timestamp: TotalOrder+Lattice+Ord {
    /// Reduces the collection to one occurrence of each distinct element.
    fn threshold_semigroup<R2, F>(&self, thresh: F) -> Collection<G, K, R2>
    where
        R2: Semigroup,
        F: FnMut(&K,&R,Option<&R>)->Option<R2>+'static,
        ;
    /// Reduces the collection to one occurrence of each distinct element.
    ///
    /// # Examples
    ///
    /// ```
    /// extern crate timely;
    /// extern crate differential_dataflow;
    ///
    /// use differential_dataflow::input::Input;
    /// use differential_dataflow::operators::ThresholdTotal;
    ///
    /// fn main() {
    ///     ::timely::example(|scope| {
    ///         // report the number of occurrences of each key
    ///         scope.new_collection_from(1 .. 10).1
    ///              .map(|x| x / 3)
    ///              .threshold_total(|_,c| c % 2);
    ///     });
    /// }
    /// ```
    fn threshold_total<R2: Abelian, F: FnMut(&K,&R)->R2+'static>(&self, mut thresh: F) -> Collection<G, K, R2> {
        self.threshold_semigroup(move |key, new, old| {
            let mut new = if !new.is_zero() { thresh(key, new) } else { R2::zero() };
            if let Some(old) = old { if !old.is_zero() { new.plus_equals(&thresh(key, old).negate()); } }
            if !new.is_zero() { Some(new) } else { None }
        })
    }
    /// Reduces the collection to one occurrence of each distinct element.
    ///
    /// This reduction only tests whether the weight associated with a record is non-zero, and otherwise
    /// ignores its specific value. To take more general actions based on the accumulated weight, consider
    /// the `threshold` method.
    ///
    /// # Examples
    ///
    /// ```
    /// extern crate timely;
    /// extern crate differential_dataflow;
    ///
    /// use differential_dataflow::input::Input;
    /// use differential_dataflow::operators::ThresholdTotal;
    ///
    /// fn main() {
    ///     ::timely::example(|scope| {
    ///         // report the number of occurrences of each key
    ///         scope.new_collection_from(1 .. 10).1
    ///              .map(|x| x / 3)
    ///              .distinct_total();
    ///     });
    /// }
    /// ```
    fn distinct_total(&self) -> Collection<G, K, isize> {
        self.distinct_total_core()
    }

    /// Distinct for general integer differences.
    ///
    /// This method allows `distinct` to produce collections whose difference
    /// type is something other than an `isize` integer, for example perhaps an
    /// `i32`.
    fn distinct_total_core<R2: Abelian+From<i8>>(&self) -> Collection<G, K, R2> {
        self.threshold_total(|_,_| R2::from(1i8))
    }

}

impl<G: Scope, K: ExchangeData+Hashable, R: ExchangeData+Semigroup> ThresholdTotal<G, K, R> for Collection<G, K, R>
where G::Timestamp: TotalOrder+Lattice+Ord {
    fn threshold_semigroup<R2, F>(&self, thresh: F) -> Collection<G, K, R2>
    where
        R2: Semigroup,
        F: FnMut(&K,&R,Option<&R>)->Option<R2>+'static,
    {
        self.arrange_by_self_named("Arrange: ThresholdTotal")
            .threshold_semigroup(thresh)
    }
}

impl<G: Scope, T1> ThresholdTotal<G, T1::Key, T1::R> for Arranged<G, T1>
where
    G::Timestamp: TotalOrder+Lattice+Ord,
    T1: TraceReader<Val=(), Time=G::Timestamp>+Clone+'static,
    T1::Key: Data,
    T1::R: Semigroup,
{
    fn threshold_semigroup<R2, F>(&self, mut thresh: F) -> Collection<G, T1::Key, R2>
    where
        R2: Semigroup,
        F: FnMut(&T1::Key,&T1::R,Option<&T1::R>)->Option<R2>+'static,
    {

        let mut trace = self.trace.clone();
        let mut buffer = Vec::new();

        self.stream.unary_frontier(Pipeline, "ThresholdTotal", move |_,_| {

            // tracks the upper limit of known-complete timestamps.
            let mut upper_limit = timely::progress::frontier::Antichain::from_elem(<G::Timestamp as timely::progress::Timestamp>::minimum());

            move |input, output| {

                input.for_each(|capability, batches| {
                    batches.swap(&mut buffer);
                    let mut session = output.session(&capability);
                    for batch in buffer.drain(..) {

                        let mut batch_cursor = batch.cursor();
                        let (mut trace_cursor, trace_storage) = trace.cursor_through(batch.lower().borrow()).unwrap();

                        upper_limit.clone_from(batch.upper());

                        while batch_cursor.key_valid(&batch) {
                            let key = batch_cursor.key(&batch);
                            let mut count: Option<T1::R> = None;

                            // Compute the multiplicity of this key before the current batch.
                            trace_cursor.seek_key(&trace_storage, key);
                            if trace_cursor.get_key(&trace_storage) == Some(key) {
                                trace_cursor.map_times(&trace_storage, |_, diff| {
                                    count.as_mut().map(|c| c.plus_equals(diff));
                                    if count.is_none() { count = Some(diff.clone()); }
                                });
                            }

                            // Apply `thresh` both before and after `diff` is applied to `count`.
                            // If the result is non-zero, send it along.
                            batch_cursor.map_times(&batch, |time, diff| {

                                let difference =
                                match &count {
                                    Some(old) => {
                                        let mut temp = old.clone();
                                        temp.plus_equals(diff);
                                        thresh(key, &temp, Some(old))
                                    },
                                    None => { thresh(key, diff, None) },
                                };

                                // Either add or assign `diff` to `count`.
                                if let Some(count) = &mut count {
                                    count.plus_equals(diff);
                                }
                                else {
                                    count = Some(diff.clone());
                                }

                                if let Some(difference) = difference {
                                    if !difference.is_zero() {
                                        session.give((key.clone(), time.clone(), difference));
                                    }
                                }
                            });

                            batch_cursor.step_key(&batch);
                        }
                    }
                });

                // tidy up the shared input trace.
                trace.advance_upper(&mut upper_limit);
                trace.set_logical_compaction(upper_limit.borrow());
                trace.set_physical_compaction(upper_limit.borrow());
            }
        })
        .as_collection()
    }
}
