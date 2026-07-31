//! Partition a stream of records into multiple streams.

use crate::dataflow::channels::pact::Pipeline;
use crate::dataflow::operators::generic::builder_rc::OperatorBuilder;
use crate::dataflow::{Scope, Stream};
use crate::Data;

/// Partition a stream of records into multiple streams.
pub trait Partition<G: Scope, D: Data, D2: Data, F: Fn(D) -> (u64, D2)> {
    /// Produces `parts` output streams, containing records produced and assigned by `route`.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Partition, Inspect};
    ///
    /// timely::example(|scope| {
    ///     let streams = (0..10).to_stream(scope)
    ///                          .partition(3, |x| (x % 3, x));
    ///
    ///     streams[0].inspect(|x| println!("seen 0: {:?}", x));
    ///     streams[1].inspect(|x| println!("seen 1: {:?}", x));
    ///     streams[2].inspect(|x| println!("seen 2: {:?}", x));
    /// });
    /// ```
    fn partition(&self, parts: u64, route: F) -> Vec<Stream<G, D2>>;
}

impl<G: Scope, D: Data, D2: Data, F: Fn(D) -> (u64, D2) + 'static> Partition<G, D, D2, F>
    for Stream<G, D>
{
    fn partition(&self, parts: u64, route: F) -> Vec<Stream<G, D2>> {
        let mut builder = OperatorBuilder::new("Partition".to_owned(), self.scope());

        let mut input = builder.new_input(self, Pipeline);
        let mut outputs = Vec::with_capacity(parts as usize);
        let mut streams = Vec::with_capacity(parts as usize);

        for _ in 0..parts {
            let (output, stream) = builder.new_output();
            outputs.push(output);
            streams.push(stream);
        }

        builder.build(move |_| {
            let mut vector = Vec::new();
            move |_frontiers| {
                let mut handles = outputs.iter_mut().map(|o| o.activate()).collect::<Vec<_>>();
                input.for_each(|time, data| {
                    data.swap(&mut vector);
                    let mut sessions = handles
                        .iter_mut()
                        .map(|h| h.session(&time))
                        .collect::<Vec<_>>();

                    for datum in vector.drain(..) {
                        let (part, datum2) = route(datum);
                        sessions[part as usize].give(datum2);
                    }
                });
            }
        });

        streams
    }
}
