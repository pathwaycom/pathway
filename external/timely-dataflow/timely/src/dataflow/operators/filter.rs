//! Filters a stream by a predicate.

use crate::dataflow::channels::pact::Pipeline;
use crate::dataflow::operators::generic::operator::Operator;
use crate::dataflow::{Scope, Stream};
use crate::Data;

/// Extension trait for filtering.
pub trait Filter<D: Data> {
    /// Returns a new instance of `self` containing only records satisfying `predicate`.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Filter, Inspect};
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .filter(|x| *x % 2 == 0)
    ///            .inspect(|x| println!("seen: {:?}", x));
    /// });
    /// ```
    fn filter<P: FnMut(&D) -> bool + 'static>(&self, predicate: P) -> Self;
}

impl<G: Scope, D: Data> Filter<D> for Stream<G, D> {
    fn filter<P: FnMut(&D) -> bool + 'static>(&self, mut predicate: P) -> Stream<G, D> {
        let mut vector = Vec::new();
        self.unary(Pipeline, "Filter", move |_, _| {
            move |input, output| {
                input.for_each(|time, data| {
                    data.swap(&mut vector);
                    vector.retain(|x| predicate(x));
                    if !vector.is_empty() {
                        output.session(&time).give_vec(&mut vector);
                    }
                });
            }
        })
    }
}
