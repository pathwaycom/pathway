//! Traits and types for capturing timely dataflow streams.
//!
//! All timely dataflow streams can be captured, but there are many ways to capture
//! these streams. A stream may be `capture_into`'d any type implementing `EventPusher`,
//! and there are several default implementations, including a linked-list, Rust's MPSC
//! queue, and a binary serializer wrapping any `W: Write`.

use crate::dataflow::channels::pact::Pipeline;
use crate::dataflow::channels::pullers::Counter as PullCounter;
use crate::dataflow::operators::generic::builder_raw::OperatorBuilder;
use crate::dataflow::{Scope, StreamCore};

use crate::progress::ChangeBatch;
use crate::progress::Timestamp;
use crate::Container;

use super::{EventCore, EventPusherCore};

/// Capture a stream of timestamped data for later replay.
pub trait Capture<T: Timestamp, D: Container> {
    /// Captures a stream of timestamped data for later replay.
    ///
    /// # Examples
    ///
    /// The type `Rc<EventLink<T,D>>` implements a typed linked list,
    /// and can be captured into and replayed from.
    ///
    /// ```rust
    /// use std::rc::Rc;
    /// use std::sync::{Arc, Mutex};
    /// use timely::dataflow::Scope;
    /// use timely::dataflow::operators::{Capture, ToStream, Inspect};
    /// use timely::dataflow::operators::capture::{EventLinkCore, Replay, Extract};
    ///
    /// // get send and recv endpoints, wrap send to share
    /// let (send, recv) = ::std::sync::mpsc::channel();
    /// let send = Arc::new(Mutex::new(send));
    ///
    /// timely::execute(timely::Config::thread(), move |worker| {
    ///
    ///     // this is only to validate the output.
    ///     let send = send.lock().unwrap().clone();
    ///
    ///     // these are to capture/replay the stream.
    ///     let handle1 = Rc::new(EventLinkCore::new());
    ///     let handle2 = Some(handle1.clone());
    ///
    ///     worker.dataflow::<u64,_,_>(|scope1|
    ///         (0..10).to_stream(scope1)
    ///                .capture_into(handle1)
    ///     );
    ///
    ///     worker.dataflow(|scope2| {
    ///         handle2.replay_into(scope2)
    ///                .capture_into(send)
    ///     });
    /// }).unwrap();
    ///
    /// assert_eq!(recv.extract()[0].1, (0..10).collect::<Vec<_>>());
    /// ```
    ///
    /// The types `EventWriter<T, D, W>` and `EventReader<T, D, R>` can be
    /// captured into and replayed from, respectively. They use binary writers
    /// and readers respectively, and can be backed by files, network sockets,
    /// etc.
    ///
    /// ```
    /// use std::rc::Rc;
    /// use std::net::{TcpListener, TcpStream};
    /// use std::sync::{Arc, Mutex};
    /// use timely::dataflow::Scope;
    /// use timely::dataflow::operators::{Capture, ToStream, Inspect};
    /// use timely::dataflow::operators::capture::{EventReader, EventWriter, Replay, Extract};
    ///
    /// // get send and recv endpoints, wrap send to share
    /// let (send0, recv0) = ::std::sync::mpsc::channel();
    /// let send0 = Arc::new(Mutex::new(send0));
    ///
    /// timely::execute(timely::Config::thread(), move |worker| {
    ///
    ///     // this is only to validate the output.
    ///     let send0 = send0.lock().unwrap().clone();
    ///
    ///     // these allow us to capture / replay a timely stream.
    ///     let list = TcpListener::bind("127.0.0.1:8000").unwrap();
    ///     let send = TcpStream::connect("127.0.0.1:8000").unwrap();
    ///     let recv = list.incoming().next().unwrap().unwrap();
    ///
    ///     recv.set_nonblocking(true).unwrap();
    ///
    ///     worker.dataflow::<u64,_,_>(|scope1|
    ///         (0..10u64)
    ///             .to_stream(scope1)
    ///             .capture_into(EventWriter::new(send))
    ///     );
    ///
    ///     worker.dataflow::<u64,_,_>(|scope2| {
    ///         Some(EventReader::<_,u64,_>::new(recv))
    ///             .replay_into(scope2)
    ///             .capture_into(send0)
    ///     });
    /// }).unwrap();
    ///
    /// assert_eq!(recv0.extract()[0].1, (0..10).collect::<Vec<_>>());
    /// ```
    fn capture_into<P: EventPusherCore<T, D> + 'static>(&self, pusher: P);

    /// Captures a stream using Rust's MPSC channels.
    fn capture(&self) -> ::std::sync::mpsc::Receiver<EventCore<T, D>> {
        let (send, recv) = ::std::sync::mpsc::channel();
        self.capture_into(send);
        recv
    }
}

impl<S: Scope, D: Container> Capture<S::Timestamp, D> for StreamCore<S, D> {
    fn capture_into<P: EventPusherCore<S::Timestamp, D> + 'static>(&self, mut event_pusher: P) {
        let mut builder = OperatorBuilder::new("Capture".to_owned(), self.scope());
        let mut input = PullCounter::new(builder.new_input(self, Pipeline));
        let mut started = false;

        builder.build(move |progress| {
            if !started {
                // discard initial capability.
                progress.frontiers[0].update(S::Timestamp::minimum(), -1);
                started = true;
            }
            if !progress.frontiers[0].is_empty() {
                // transmit any frontier progress.
                let to_send = ::std::mem::replace(&mut progress.frontiers[0], ChangeBatch::new());
                event_pusher.push(EventCore::Progress(to_send.into_inner()));
            }

            use crate::communication::message::RefOrMut;

            // turn each received message into an event.
            while let Some(message) = input.next() {
                let (time, data) = match message.as_ref_or_mut() {
                    RefOrMut::Ref(reference) => (&reference.time, RefOrMut::Ref(&reference.data)),
                    RefOrMut::Mut(reference) => {
                        (&reference.time, RefOrMut::Mut(&mut reference.data))
                    }
                };
                let vector = data.replace(Default::default());
                event_pusher.push(EventCore::Messages(time.clone(), vector));
            }
            input
                .consumed()
                .borrow_mut()
                .drain_into(&mut progress.consumeds[0]);
            false
        });
    }
}
