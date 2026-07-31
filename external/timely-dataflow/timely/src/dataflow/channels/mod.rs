//! Structured communication between timely dataflow operators.

use crate::communication::Push;
use crate::Container;

/// Parallelization contracts, describing how data must be exchanged between operators.
pub mod pact;
/// A collection of types that may be pulled from.
pub mod pullers;
/// A collection of types that may be pushed at.
pub mod pushers;

/// The input to and output from timely dataflow communication channels.
pub type BundleCore<T, D> = crate::communication::Message<Message<T, D>>;

/// The input to and output from timely dataflow communication channels specialized to vectors.
pub type Bundle<T, D> = BundleCore<T, Vec<D>>;

/// A serializable representation of timestamped data.
#[derive(Clone, Abomonation, Serialize, Deserialize)]
pub struct Message<T, D> {
    /// The timestamp associated with the message.
    pub time: T,
    /// The data in the message.
    pub data: D,
    /// The source worker.
    pub from: usize,
    /// A sequence number for this worker-to-worker stream.
    pub seq: usize,
}

impl<T, D> Message<T, D> {
    /// Default buffer size.
    #[deprecated = "Use timely::buffer::default_capacity instead"]
    pub fn default_length() -> usize {
        crate::container::buffer::default_capacity::<D>()
    }
}

impl<T, D: Container> Message<T, D> {
    /// Creates a new message instance from arguments.
    pub fn new(time: T, data: D, from: usize, seq: usize) -> Self {
        Message {
            time,
            data,
            from,
            seq,
        }
    }

    /// Forms a message, and pushes contents at `pusher`. Replaces `buffer` with what the pusher
    /// leaves in place, or the container's default element.
    #[inline]
    pub fn push_at<P: Push<BundleCore<T, D>>>(buffer: &mut D, time: T, pusher: &mut P) {
        let data = ::std::mem::take(buffer);
        let message = Message::new(time, data, 0, 0);
        let mut bundle = Some(BundleCore::from_typed(message));

        pusher.push(&mut bundle);

        if let Some(message) = bundle {
            if let Some(message) = message.if_typed() {
                *buffer = message.data;
                buffer.clear();
            }
        }
    }
}
