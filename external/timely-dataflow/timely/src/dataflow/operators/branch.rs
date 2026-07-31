//! Operators that separate one stream into two streams based on some condition

use crate::dataflow::channels::pact::Pipeline;
use crate::dataflow::operators::generic::builder_rc::OperatorBuilder;
use crate::dataflow::{Scope, Stream, StreamCore};
use crate::{Container, Data};

/// Extension trait for `Stream`.
pub trait Branch<S: Scope, D: Data> {
    /// Takes one input stream and splits it into two output streams.
    /// For each record, the supplied closure is called with a reference to
    /// the data and its time. If it returns true, the record will be sent
    /// to the second returned stream, otherwise it will be sent to the first.
    ///
    /// If the result of the closure only depends on the time, not the data,
    /// `branch_when` should be used instead.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Branch, Inspect};
    ///
    /// timely::example(|scope| {
    ///     let (odd, even) = (0..10)
    ///         .to_stream(scope)
    ///         .branch(|_time, x| *x % 2 == 0);
    ///
    ///     even.inspect(|x| println!("even numbers: {:?}", x));
    ///     odd.inspect(|x| println!("odd numbers: {:?}", x));
    /// });
    /// ```
    fn branch(
        &self,
        condition: impl Fn(&S::Timestamp, &D) -> bool + 'static,
    ) -> (Stream<S, D>, Stream<S, D>);
}

impl<S: Scope, D: Data> Branch<S, D> for Stream<S, D> {
    fn branch(
        &self,
        condition: impl Fn(&S::Timestamp, &D) -> bool + 'static,
    ) -> (Stream<S, D>, Stream<S, D>) {
        let mut builder = OperatorBuilder::new("Branch".to_owned(), self.scope());

        let mut input = builder.new_input(self, Pipeline);
        let (mut output1, stream1) = builder.new_output();
        let (mut output2, stream2) = builder.new_output();

        builder.build(move |_| {
            let mut vector = Vec::new();
            move |_frontiers| {
                let mut output1_handle = output1.activate();
                let mut output2_handle = output2.activate();

                input.for_each(|time, data| {
                    data.swap(&mut vector);
                    let mut out1 = output1_handle.session(&time);
                    let mut out2 = output2_handle.session(&time);
                    for datum in vector.drain(..) {
                        if condition(&time.time(), &datum) {
                            out2.give(datum);
                        } else {
                            out1.give(datum);
                        }
                    }
                });
            }
        });

        (stream1, stream2)
    }
}

/// Extension trait for `Stream`.
pub trait BranchWhen<T>: Sized {
    /// Takes one input stream and splits it into two output streams.
    /// For each time, the supplied closure is called. If it returns true,
    /// the records for that will be sent to the second returned stream, otherwise
    /// they will be sent to the first.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, BranchWhen, Inspect, Delay};
    ///
    /// timely::example(|scope| {
    ///     let (before_five, after_five) = (0..10)
    ///         .to_stream(scope)
    ///         .delay(|x,t| *x) // data 0..10 at time 0..10
    ///         .branch_when(|time| time >= &5);
    ///
    ///     before_five.inspect(|x| println!("Times 0-4: {:?}", x));
    ///     after_five.inspect(|x| println!("Times 5 and later: {:?}", x));
    /// });
    /// ```
    fn branch_when(&self, condition: impl Fn(&T) -> bool + 'static) -> (Self, Self);
}

impl<S: Scope, C: Container> BranchWhen<S::Timestamp> for StreamCore<S, C> {
    fn branch_when(&self, condition: impl Fn(&S::Timestamp) -> bool + 'static) -> (Self, Self) {
        let mut builder = OperatorBuilder::new("Branch".to_owned(), self.scope());

        let mut input = builder.new_input(self, Pipeline);
        let (mut output1, stream1) = builder.new_output();
        let (mut output2, stream2) = builder.new_output();

        builder.build(move |_| {
            let mut container = Default::default();
            move |_frontiers| {
                let mut output1_handle = output1.activate();
                let mut output2_handle = output2.activate();

                input.for_each(|time, data| {
                    data.swap(&mut container);
                    let mut out = if condition(&time.time()) {
                        output2_handle.session(&time)
                    } else {
                        output1_handle.session(&time)
                    };
                    out.give_container(&mut container);
                });
            }
        });

        (stream1, stream2)
    }
}
