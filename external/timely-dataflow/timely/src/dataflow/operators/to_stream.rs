//! Conversion to the `Stream` type from iterators.

use crate::Container;
use crate::progress::Timestamp;
use crate::Data;
use crate::dataflow::operators::generic::operator::source;
use crate::dataflow::{StreamCore, Stream, Scope};

/// Converts to a timely `Stream`.
pub trait ToStream<T: Timestamp, D: Data> {
    /// Converts to a timely `Stream`.
    ///
    /// # Examples
    ///
    /// ```
    /// use timely::dataflow::operators::{ToStream, Capture};
    /// use timely::dataflow::operators::capture::Extract;
    ///
    /// let (data1, data2) = timely::example(|scope| {
    ///     let data1 = (0..3).to_stream(scope).capture();
    ///     let data2 = vec![0,1,2].to_stream(scope).capture();
    ///     (data1, data2)
    /// });
    ///
    /// assert_eq!(data1.extract(), data2.extract());
    /// ```
    fn to_stream<S: Scope<Timestamp=T>>(self, scope: &mut S) -> Stream<S, D>;
}

impl<T: Timestamp, I: IntoIterator+'static> ToStream<T, I::Item> for I where I::Item: Data {
    fn to_stream<S: Scope<Timestamp=T>>(self, scope: &mut S) -> Stream<S, I::Item> {

        source(scope, "ToStream", |capability, info| {

            // Acquire an activator, so that the operator can rescheduled itself.
            let activator = scope.activator_for(&info.address[..]);

            let mut iterator = self.into_iter().fuse();
            let mut capability = Some(capability);

            move |output| {

                if let Some(element) = iterator.next() {
                    let mut session = output.session(capability.as_ref().unwrap());
                    session.give(element);
                    let n = 256 * crate::container::buffer::default_capacity::<I::Item>();
                    for element in iterator.by_ref().take(n - 1) {
                        session.give(element);
                    }
                    activator.activate();
                }
                else {
                    capability = None;
                }
            }
        })
    }
}

/// Converts to a timely [StreamCore].
pub trait ToStreamCore<T: Timestamp, C: Container> {
    /// Converts to a timely [StreamCore].
    ///
    /// # Examples
    ///
    /// ```
    /// use timely::dataflow::operators::{ToStreamCore, Capture};
    /// use timely::dataflow::operators::capture::Extract;
    ///
    /// let (data1, data2) = timely::example(|scope| {
    ///     let data1 = Some((0..3).collect::<Vec<_>>()).to_stream_core(scope).capture();
    ///     let data2 = Some(vec![0,1,2]).to_stream_core(scope).capture();
    ///     (data1, data2)
    /// });
    ///
    /// assert_eq!(data1.extract(), data2.extract());
    /// ```
    fn to_stream_core<S: Scope<Timestamp=T>>(self, scope: &mut S) -> StreamCore<S, C>;
}

impl<T: Timestamp, I: IntoIterator+'static> ToStreamCore<T, I::Item> for I where I::Item: Container {
    fn to_stream_core<S: Scope<Timestamp=T>>(self, scope: &mut S) -> StreamCore<S, I::Item> {

        source(scope, "ToStreamCore", |capability, info| {

            // Acquire an activator, so that the operator can rescheduled itself.
            let activator = scope.activator_for(&info.address[..]);

            let mut iterator = self.into_iter().fuse();
            let mut capability = Some(capability);

            move |output| {

                if let Some(mut element) = iterator.next() {
                    let mut session = output.session(capability.as_ref().unwrap());
                    session.give_container(&mut element);
                    let n = 256;
                    for mut element in iterator.by_ref().take(n - 1) {
                        session.give_container(&mut element);
                    }
                    activator.activate();
                }
                else {
                    capability = None;
                }
            }
        })
    }
}
