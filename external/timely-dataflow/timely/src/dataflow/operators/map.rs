//! Extension methods for `Stream` based on record-by-record transformation.

use crate::dataflow::channels::pact::Pipeline;
use crate::dataflow::operators::generic::operator::Operator;
use crate::dataflow::{Scope, Stream};
use crate::Data;

/// Extension trait for `Stream`.
pub trait Map<S: Scope, D: Data> {
    /// Consumes each element of the stream and yields a new element.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Map, Inspect};
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .map(|x| x + 1)
    ///            .inspect(|x| println!("seen: {:?}", x));
    /// });
    /// ```
    fn map<D2: Data, L: FnMut(D) -> D2 + 'static>(&self, logic: L) -> Stream<S, D2>;
    /// Updates each element of the stream and yields the element, re-using memory where possible.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Map, Inspect};
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .map_in_place(|x| *x += 1)
    ///            .inspect(|x| println!("seen: {:?}", x));
    /// });
    /// ```
    fn map_in_place<L: FnMut(&mut D) + 'static>(&self, logic: L) -> Stream<S, D>;
    /// Consumes each element of the stream and yields some number of new elements.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Map, Inspect};
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .flat_map(|x| (0..x))
    ///            .inspect(|x| println!("seen: {:?}", x));
    /// });
    /// ```
    fn flat_map<I: IntoIterator, L: FnMut(D) -> I + 'static>(&self, logic: L) -> Stream<S, I::Item>
    where
        I::Item: Data;
}

impl<S: Scope, D: Data> Map<S, D> for Stream<S, D> {
    fn map<D2: Data, L: FnMut(D) -> D2 + 'static>(&self, mut logic: L) -> Stream<S, D2> {
        let mut vector = Vec::new();
        self.unary(Pipeline, "Map", move |_, _| {
            move |input, output| {
                input.for_each(|time, data| {
                    data.swap(&mut vector);
                    output
                        .session(&time)
                        .give_iterator(vector.drain(..).map(|x| logic(x)));
                });
            }
        })
    }
    fn map_in_place<L: FnMut(&mut D) + 'static>(&self, mut logic: L) -> Stream<S, D> {
        let mut vector = Vec::new();
        self.unary(Pipeline, "MapInPlace", move |_, _| {
            move |input, output| {
                input.for_each(|time, data| {
                    data.swap(&mut vector);
                    for datum in vector.iter_mut() {
                        logic(datum);
                    }
                    output.session(&time).give_vec(&mut vector);
                })
            }
        })
    }
    // TODO : This would be more robust if it captured an iterator and then pulled an appropriate
    // TODO : number of elements from the iterator. This would allow iterators that produce many
    // TODO : records without taking arbitrarily long and arbitrarily much memory.
    fn flat_map<I: IntoIterator, L: FnMut(D) -> I + 'static>(
        &self,
        mut logic: L,
    ) -> Stream<S, I::Item>
    where
        I::Item: Data,
    {
        let mut vector = Vec::new();
        self.unary(Pipeline, "FlatMap", move |_, _| {
            move |input, output| {
                input.for_each(|time, data| {
                    data.swap(&mut vector);
                    output
                        .session(&time)
                        .give_iterator(vector.drain(..).flat_map(|x| logic(x).into_iter()));
                });
            }
        })
    }
}
