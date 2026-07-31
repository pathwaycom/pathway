//! Counts the number of records at each time.
use std::collections::HashMap;

use crate::communication::message::RefOrMut;

use crate::dataflow::channels::pact::Pipeline;
use crate::dataflow::operators::generic::operator::Operator;
use crate::dataflow::{Scope, Stream};
use crate::Data;

/// Accumulates records within a timestamp.
pub trait Accumulate<G: Scope, D: Data> {
    /// Accumulates records within a timestamp.
    ///
    /// # Examples
    ///
    /// ```
    /// use timely::dataflow::operators::{ToStream, Accumulate, Capture};
    /// use timely::dataflow::operators::capture::Extract;
    ///
    /// let captured = timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .accumulate(0, |sum, data| { for &x in data.iter() { *sum += x; } })
    ///            .capture()
    /// });
    ///
    /// let extracted = captured.extract();
    /// assert_eq!(extracted, vec![(0, vec![45])]);
    /// ```
    fn accumulate<A: Data>(
        &self,
        default: A,
        logic: impl Fn(&mut A, RefOrMut<Vec<D>>) + 'static,
    ) -> Stream<G, A>;
    /// Counts the number of records observed at each time.
    ///
    /// # Examples
    ///
    /// ```
    /// use timely::dataflow::operators::{ToStream, Accumulate, Capture};
    /// use timely::dataflow::operators::capture::Extract;
    ///
    /// let captured = timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .count()
    ///            .capture()
    /// });
    ///
    /// let extracted = captured.extract();
    /// assert_eq!(extracted, vec![(0, vec![10])]);
    /// ```
    fn count(&self) -> Stream<G, usize> {
        self.accumulate(0, |sum, data| *sum += data.len())
    }
}

impl<G: Scope, D: Data> Accumulate<G, D> for Stream<G, D> {
    fn accumulate<A: Data>(
        &self,
        default: A,
        logic: impl Fn(&mut A, RefOrMut<Vec<D>>) + 'static,
    ) -> Stream<G, A> {
        let mut accums = HashMap::new();
        self.unary_notify(
            Pipeline,
            "Accumulate",
            vec![],
            move |input, output, notificator| {
                input.for_each(|time, data| {
                    logic(
                        &mut accums
                            .entry(time.time().clone())
                            .or_insert_with(|| default.clone()),
                        data,
                    );
                    notificator.notify_at(time.retain());
                });

                notificator.for_each(|time, _, _| {
                    if let Some(accum) = accums.remove(&time) {
                        output.session(&time).give(accum);
                    }
                });
            },
        )
    }
}
