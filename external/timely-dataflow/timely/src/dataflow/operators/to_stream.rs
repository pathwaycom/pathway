//! Conversion to the `Stream` type from iterators.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use crate::Container;

use crate::dataflow::operators::generic::operator::source;
use crate::dataflow::operators::CapabilitySet;
use crate::dataflow::{StreamCore, Scope, Stream};
use crate::progress::Timestamp;
use crate::Data;

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

/// Data and progress events of the native stream.
pub enum Event<F: IntoIterator, D> {
    /// Indicates that timestamps have advanced to frontier F
    Progress(F),
    /// Indicates that event D happened at time T
    Message(F::Item, D),
}

/// Converts to a timely `Stream`.
pub trait ToStreamAsync<T: Timestamp, D: Data> {
    /// Converts a [native `Stream`](futures_util::stream::Stream) of [`Event`s](Event) into a [timely
    /// `Stream`](crate::dataflow::Stream).
    ///
    /// # Examples
    ///
    /// ```
    /// use futures_util::stream;
    ///
    /// use timely::dataflow::operators::{Capture, Event, ToStream, ToStreamAsync};
    /// use timely::dataflow::operators::capture::Extract;
    ///
    /// let native_stream = stream::iter(vec![
    ///     Event::Message(0, 0),
    ///     Event::Message(0, 1),
    ///     Event::Message(0, 2),
    ///     Event::Progress(Some(0)),
    /// ]);
    ///
    /// let (data1, data2) = timely::example(|scope| {
    ///     let data1 = native_stream.to_stream(scope).capture();
    ///     let data2 = vec![0,1,2].to_stream(scope).capture();
    ///
    ///     (data1, data2)
    /// });
    ///
    /// assert_eq!(data1.extract(), data2.extract());
    /// ```
    fn to_stream<S: Scope<Timestamp = T>>(self, scope: &S) -> Stream<S, D>;
}

impl<T, D, F, I> ToStreamAsync<T, D> for I
where
    D: Data,
    T: Timestamp,
    F: IntoIterator<Item = T>,
    I: futures_util::stream::Stream<Item = Event<F, D>> + Unpin + 'static,
{
    fn to_stream<S: Scope<Timestamp = T>>(mut self, scope: &S) -> Stream<S, D> {
        source(scope, "ToStreamAsync", move |capability, info| {
            let activator = Arc::new(scope.sync_activator_for(&info.address[..]));

            let mut cap_set = CapabilitySet::from_elem(capability);

            move |output| {
                let waker = futures_util::task::waker_ref(&activator);
                let mut context = Context::from_waker(&waker);

                // Consume all the ready items of the source_stream and issue them to the operator
                while let Poll::Ready(item) = Pin::new(&mut self).poll_next(&mut context) {
                    match item {
                        Some(Event::Progress(time)) => {
                            cap_set.downgrade(time);
                        }
                        Some(Event::Message(time, data)) => {
                            output.session(&cap_set.delayed(&time)).give(data);
                        }
                        None => {
                            cap_set.downgrade(&[]);
                            break;
                        }
                    }
                }
            }
        })
    }
}
