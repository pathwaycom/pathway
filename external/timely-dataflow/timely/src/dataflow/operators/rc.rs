//! Shared containers

use crate::dataflow::channels::pact::Pipeline;
use crate::dataflow::operators::Operator;
use crate::dataflow::{Scope, StreamCore};
use crate::Container;
use std::rc::Rc;

/// Convert a stream into a stream of shared containers
pub trait SharedStream<S: Scope, C: Container> {
    /// Convert a stream into a stream of shared data
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Inspect};
    /// use timely::dataflow::operators::rc::SharedStream;
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .shared()
    ///            .inspect(|x| println!("seen: {:?}", x));
    /// });
    /// ```
    fn shared(&self) -> StreamCore<S, Rc<C>>;
}

impl<S: Scope, C: Container> SharedStream<S, C> for StreamCore<S, C> {
    fn shared(&self) -> StreamCore<S, Rc<C>> {
        let mut container = Default::default();
        self.unary(Pipeline, "Shared", move |_, _| {
            move |input, output| {
                input.for_each(|time, data| {
                    data.swap(&mut container);
                    output
                        .session(&time)
                        .give_container(&mut Rc::new(std::mem::take(&mut container)));
                });
            }
        })
    }
}

#[cfg(test)]
mod test {
    use crate::dataflow::channels::pact::Pipeline;
    use crate::dataflow::operators::capture::Extract;
    use crate::dataflow::operators::rc::SharedStream;
    use crate::dataflow::operators::{Capture, Concatenate, Operator, ToStream};

    #[test]
    fn test_shared() {
        let output = crate::example(|scope| {
            let shared = vec![Ok(0), Err(())].to_stream(scope).shared();
            scope
                .concatenate([
                    shared.unary(Pipeline, "read shared 1", |_, _| {
                        let mut container = Default::default();
                        move |input, output| {
                            input.for_each(|time, data| {
                                data.swap(&mut container);
                                output.session(&time).give(container.as_ptr() as usize);
                            });
                        }
                    }),
                    shared.unary(Pipeline, "read shared 2", |_, _| {
                        let mut container = Default::default();
                        move |input, output| {
                            input.for_each(|time, data| {
                                data.swap(&mut container);
                                output.session(&time).give(container.as_ptr() as usize);
                            });
                        }
                    }),
                ])
                .capture()
        });
        let output = &mut output.extract()[0].1;
        output.sort();
        output.dedup();
        assert_eq!(output.len(), 1);
    }
}
