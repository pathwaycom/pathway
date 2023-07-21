//! Operators that separate one stream into two streams based on some condition

use crate::dataflow::channels::pact::Pipeline;
use crate::dataflow::operators::generic::builder_rc::OperatorBuilder;
use crate::dataflow::{Scope, Stream};
use crate::Data;

/// Extension trait for `Stream`.
pub trait OkErr<S: Scope, D: Data> {
    /// Takes one input stream and splits it into two output streams.
    /// For each record, the supplied closure is called with the data.
    /// If it returns `Ok(x)`, then `x` will be sent
    /// to the first returned stream; otherwise, if it returns `Err(e)`,
    /// then `e` will be sent to the second.
    ///
    /// If the result of the closure only depends on the time, not the data,
    /// `branch_when` should be used instead.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, OkErr, Inspect};
    ///
    /// timely::example(|scope| {
    ///     let (odd, even) = (0..10)
    ///         .to_stream(scope)
    ///         .ok_err(|x| if x % 2 == 0 { Ok(x) } else { Err(x) });
    ///
    ///     even.inspect(|x| println!("even numbers: {:?}", x));
    ///     odd.inspect(|x| println!("odd numbers: {:?}", x));
    /// });
    /// ```
    fn ok_err<D1, D2, L>(
        &self,
        logic: L,
    ) -> (Stream<S, D1>, Stream<S, D2>)

    where
        D1: Data,
        D2: Data,
        L: FnMut(D) -> Result<D1,D2>+'static
    ;
}

impl<S: Scope, D: Data> OkErr<S, D> for Stream<S, D> {
    fn ok_err<D1, D2, L>(
        &self,
        mut logic: L,
    ) -> (Stream<S, D1>, Stream<S, D2>)

    where
        D1: Data,
        D2: Data,
        L: FnMut(D) -> Result<D1,D2>+'static
    {
        let mut builder = OperatorBuilder::new("OkErr".to_owned(), self.scope());

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
                        match logic(datum) {
                            Ok(datum) => out1.give(datum),
                            Err(datum) => out2.give(datum),
                        }
                    }
                });
            }
        });

        (stream1, stream2)
    }
}
