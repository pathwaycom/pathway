//! Traits and types for replaying captured timely dataflow streams.
//!
//! A type can be replayed into any timely dataflow scope if it presents as an
//! iterator whose `Item` type implements `EventIterator` with the same timestamp.
//! Other types can implement the `ReplayInto` trait, but this should be done with
//! care, as there is a protocol the replayer follows that must be respected if the
//! computation is to make sense.
//!
//! # Protocol
//!
//! The stream of events produced by each `EventIterator` implementation must satisfy,
//! starting from a default timestamp of `Timestamp::minimum()` with count 1,
//!
//! 1. The progress messages may only increment the count for a timestamp if
//!    the cumulative count for some prior or equal timestamp is positive.
//! 2. The data messages map only use a timestamp if the cumulative count for
//!    some prior or equal timestamp is positive.
//!
//! Alternately, the sequence of events should, starting from an initial count of 1
//! for the timestamp `Default::default()`, describe decrements to held capabilities
//! or the production of capabilities in their future, or messages sent at times in
//! the future of held capabilities.
//!
//! The order is very important here. One can move `Event::Message` events arbitrarily
//! earlier in the sequence, and `Event::Progress` events arbitrarily later, but one
//! cannot move a `Event::Progress` message that discards a last capability before any
//! `Event::Message` that would use that capability.
//!
//! For an example, the `Operate<T>` implementation for `capture::CaptureOperator<T, D, P>`
//! records exactly what data is presented at the operator, both in terms of progress
//! messages and data received.
//!
//! # Notes
//!
//! Provided no stream of events reports the consumption of capabilities it does not hold,
//! any interleaving of the streams of events will still maintain the invariants above.
//! This means that each timely dataflow replay operator can replay any number of streams,
//! allowing the replay to occur in a timely dataflow computation with more or fewer workers
//! than that in which the stream was captured.

use crate::dataflow::channels::pushers::buffer::BufferCore as PushBuffer;
use crate::dataflow::channels::pushers::CounterCore as PushCounter;
use crate::dataflow::operators::generic::builder_raw::OperatorBuilder;
use crate::dataflow::{Scope, StreamCore};
use crate::progress::Timestamp;

use super::event::EventIteratorCore;
use super::EventCore;
use crate::Container;

/// Replay a capture stream into a scope with the same timestamp.
pub trait Replay<T: Timestamp, C>: Sized {
    /// Replays `self` into the provided scope, as a `Stream<S, D>`.
    fn replay_into<S: Scope<Timestamp = T>>(self, scope: &mut S) -> StreamCore<S, C> {
        self.replay_core(scope, Some(std::time::Duration::new(0, 0)))
    }
    /// Replays `self` into the provided scope, as a `Stream<S, D>'.
    ///
    /// The `period` argument allows the specification of a re-activation period, where the operator
    /// will re-activate itself every so often. The `None` argument instructs the operator not to
    /// re-activate itself.us
    fn replay_core<S: Scope<Timestamp = T>>(
        self,
        scope: &mut S,
        period: Option<std::time::Duration>,
    ) -> StreamCore<S, C>;
}

impl<T: Timestamp, C: Container, I> Replay<T, C> for I
where
    I: IntoIterator,
    <I as IntoIterator>::Item: EventIteratorCore<T, C> + 'static,
{
    fn replay_core<S: Scope<Timestamp = T>>(
        self,
        scope: &mut S,
        period: Option<std::time::Duration>,
    ) -> StreamCore<S, C> {
        let mut builder = OperatorBuilder::new("Replay".to_owned(), scope.clone());

        let address = builder.operator_info().address;
        let activator = scope.activator_for(&address[..]);

        let (targets, stream) = builder.new_output();

        let mut output = PushBuffer::new(PushCounter::new(targets));
        let mut event_streams = self.into_iter().collect::<Vec<_>>();
        let mut started = false;
        let mut allocation: C = Default::default();

        builder.build(move |progress| {
            if !started {
                // The first thing we do is modify our capabilities to match the number of streams we manage.
                // This should be a simple change of `self.event_streams.len() - 1`. We only do this once, as
                // our very first action.
                progress.internals[0]
                    .update(S::Timestamp::minimum(), (event_streams.len() as i64) - 1);
                started = true;
            }

            for event_stream in event_streams.iter_mut() {
                while let Some(event) = event_stream.next() {
                    match event {
                        EventCore::Progress(vec) => {
                            progress.internals[0].extend(vec.iter().cloned());
                        }
                        EventCore::Messages(ref time, data) => {
                            allocation.clone_from(data);
                            output.session(time).give_container(&mut allocation);
                        }
                    }
                }
            }

            // A `None` period indicates that we do not re-activate here.
            if let Some(delay) = period {
                activator.activate_after(delay);
            }

            output.cease();
            output
                .inner()
                .produced()
                .borrow_mut()
                .drain_into(&mut progress.produceds[0]);

            false
        });

        stream
    }
}
