//! Traits and types describing timely dataflow events.
//!
//! The `Event` type describes the information an operator can observe about a timely dataflow
//! stream. There are two types of events, (i) the receipt of data and (ii) reports of progress
//! of timestamps.

/// Data and progress events of the captured stream.
#[derive(
    Debug, Clone, Abomonation, Hash, Ord, PartialOrd, Eq, PartialEq, Deserialize, Serialize,
)]
pub enum EventCore<T, D> {
    /// Progress received via `push_external_progress`.
    Progress(Vec<(T, i64)>),
    /// Messages received via the data stream.
    Messages(T, D),
}

/// Data and progress events of the captured stream, specialized to vector-based containers.
pub type Event<T, D> = EventCore<T, Vec<D>>;

/// Iterates over contained `EventCore<T, D>`.
///
/// The `EventIterator` trait describes types that can iterate over references to events,
/// and which can be used to replay a stream into a new timely dataflow computation.
///
/// This method is not simply an iterator because of the lifetime in the result.
pub trait EventIteratorCore<T, D> {
    /// Iterates over references to `EventCore<T, D>` elements.
    fn next(&mut self) -> Option<&EventCore<T, D>>;
}

/// A [EventIteratorCore] specialized to vector-based containers.
// TODO: use trait aliases once stable.
pub trait EventIterator<T, D>: EventIteratorCore<T, Vec<D>> {
    /// Iterates over references to `Event<T, D>` elements.
    fn next(&mut self) -> Option<&Event<T, D>>;
}
impl<T, D, E: EventIteratorCore<T, Vec<D>>> EventIterator<T, D> for E {
    fn next(&mut self) -> Option<&Event<T, D>> {
        <Self as EventIteratorCore<_, _>>::next(self)
    }
}

/// Receives `EventCore<T, D>` events.
pub trait EventPusherCore<T, D> {
    /// Provides a new `Event<T, D>` to the pusher.
    fn push(&mut self, event: EventCore<T, D>);
}

/// A [EventPusherCore] specialized to vector-based containers.
// TODO: use trait aliases once stable.
pub trait EventPusher<T, D>: EventPusherCore<T, Vec<D>> {}
impl<T, D, E: EventPusherCore<T, Vec<D>>> EventPusher<T, D> for E {}

// implementation for the linked list behind a `Handle`.
impl<T, D> EventPusherCore<T, D> for ::std::sync::mpsc::Sender<EventCore<T, D>> {
    fn push(&mut self, event: EventCore<T, D>) {
        // NOTE: An Err(x) result just means "data not accepted" most likely
        //       because the receiver is gone. No need to panic.
        let _ = self.send(event);
    }
}

/// A linked-list event pusher and iterator.
pub mod link {

    use std::cell::RefCell;
    use std::rc::Rc;

    use super::{EventCore, EventIteratorCore, EventPusherCore};

    /// A linked list of EventCore<T, D>.
    pub struct EventLinkCore<T, D> {
        /// An event, if one exists.
        ///
        /// An event might not exist, if either we want to insert a `None` and have the output iterator pause,
        /// or in the case of the very first linked list element, which has no event when constructed.
        pub event: Option<EventCore<T, D>>,
        /// The next event, if it exists.
        pub next: RefCell<Option<Rc<EventLinkCore<T, D>>>>,
    }

    /// A [EventLinkCore] specialized to vector-based containers.
    pub type EventLink<T, D> = EventLinkCore<T, Vec<D>>;

    impl<T, D> EventLinkCore<T, D> {
        /// Allocates a new `EventLink`.
        pub fn new() -> EventLinkCore<T, D> {
            EventLinkCore {
                event: None,
                next: RefCell::new(None),
            }
        }
    }

    // implementation for the linked list behind a `Handle`.
    impl<T, D> EventPusherCore<T, D> for Rc<EventLinkCore<T, D>> {
        fn push(&mut self, event: EventCore<T, D>) {
            *self.next.borrow_mut() = Some(Rc::new(EventLinkCore {
                event: Some(event),
                next: RefCell::new(None),
            }));
            let next = self.next.borrow().as_ref().unwrap().clone();
            *self = next;
        }
    }

    impl<T, D> EventIteratorCore<T, D> for Rc<EventLinkCore<T, D>> {
        fn next(&mut self) -> Option<&EventCore<T, D>> {
            let is_some = self.next.borrow().is_some();
            if is_some {
                let next = self.next.borrow().as_ref().unwrap().clone();
                *self = next;
                self.event.as_ref()
            } else {
                None
            }
        }
    }

    // Drop implementation to prevent stack overflow through naive drop impl.
    impl<T, D> Drop for EventLinkCore<T, D> {
        fn drop(&mut self) {
            while let Some(link) = self.next.replace(None) {
                if let Ok(head) = Rc::try_unwrap(link) {
                    *self = head;
                }
            }
        }
    }

    impl<T, D> Default for EventLinkCore<T, D> {
        fn default() -> Self {
            Self::new()
        }
    }

    #[test]
    fn avoid_stack_overflow_in_drop() {
        let mut event1 = Rc::new(EventLinkCore::<(), ()>::new());
        let _event2 = event1.clone();
        for _ in 0..1_000_000 {
            event1.push(EventCore::Progress(vec![]));
        }
    }
}

/// A binary event pusher and iterator.
pub mod binary {

    use super::{EventCore, EventIteratorCore, EventPusherCore};
    use abomonation::Abomonation;
    use std::io::Write;

    /// A wrapper for `W: Write` implementing `EventPusherCore<T, D>`.
    pub struct EventWriterCore<T, D, W: ::std::io::Write> {
        stream: W,
        phant: ::std::marker::PhantomData<(T, D)>,
    }

    /// [EventWriterCore] specialized to vector-based containers.
    pub type EventWriter<T, D, W> = EventWriterCore<T, Vec<D>, W>;

    impl<T, D, W: ::std::io::Write> EventWriterCore<T, D, W> {
        /// Allocates a new `EventWriter` wrapping a supplied writer.
        pub fn new(w: W) -> Self {
            Self {
                stream: w,
                phant: ::std::marker::PhantomData,
            }
        }
    }

    impl<T: Abomonation, D: Abomonation, W: ::std::io::Write> EventPusherCore<T, D>
        for EventWriterCore<T, D, W>
    {
        fn push(&mut self, event: EventCore<T, D>) {
            // TODO: `push` has no mechanism to report errors, so we `unwrap`.
            unsafe {
                ::abomonation::encode(&event, &mut self.stream)
                    .expect("Event abomonation/write failed");
            }
        }
    }

    /// A Wrapper for `R: Read` implementing `EventIterator<T, D>`.
    pub struct EventReaderCore<T, D, R: ::std::io::Read> {
        reader: R,
        bytes: Vec<u8>,
        buff1: Vec<u8>,
        buff2: Vec<u8>,
        consumed: usize,
        valid: usize,
        phant: ::std::marker::PhantomData<(T, D)>,
    }

    /// [EventReaderCore] specialized to vector-based containers.
    pub type EventReader<T, D, R> = EventReaderCore<T, Vec<D>, R>;

    impl<T, D, R: ::std::io::Read> EventReaderCore<T, D, R> {
        /// Allocates a new `EventReader` wrapping a supplied reader.
        pub fn new(r: R) -> Self {
            Self {
                reader: r,
                bytes: vec![0u8; 1 << 20],
                buff1: vec![],
                buff2: vec![],
                consumed: 0,
                valid: 0,
                phant: ::std::marker::PhantomData,
            }
        }
    }

    impl<T: Abomonation, D: Abomonation, R: ::std::io::Read> EventIteratorCore<T, D>
        for EventReaderCore<T, D, R>
    {
        fn next(&mut self) -> Option<&EventCore<T, D>> {
            // if we can decode something, we should just return it! :D
            if unsafe { ::abomonation::decode::<EventCore<T, D>>(&mut self.buff1[self.consumed..]) }
                .is_some()
            {
                let (item, rest) = unsafe {
                    ::abomonation::decode::<EventCore<T, D>>(&mut self.buff1[self.consumed..])
                }
                .unwrap();
                self.consumed = self.valid - rest.len();
                return Some(item);
            }
            // if we exhaust data we should shift back (if any shifting to do)
            if self.consumed > 0 {
                self.buff2.clear();
                self.buff2.write_all(&self.buff1[self.consumed..]).unwrap();
                ::std::mem::swap(&mut self.buff1, &mut self.buff2);
                self.valid = self.buff1.len();
                self.consumed = 0;
            }

            if let Ok(len) = self.reader.read(&mut self.bytes[..]) {
                self.buff1.write_all(&self.bytes[..len]).unwrap();
                self.valid = self.buff1.len();
            }

            None
        }
    }
}
