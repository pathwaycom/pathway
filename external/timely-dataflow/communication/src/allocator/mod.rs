//! Types and traits for the allocation of channels.

use std::rc::Rc;
use std::cell::RefCell;
use std::time::Duration;

pub use self::thread::Thread;
pub use self::process::Process;
pub use self::generic::{Generic, GenericBuilder};

pub mod thread;
pub mod process;
pub mod generic;

pub mod canary;
pub mod counters;

pub mod zero_copy;

use crate::{Data, Push, Pull, Message};

/// A proto-allocator, which implements `Send` and can be completed with `build`.
///
/// This trait exists because some allocators contain elements that do not implement
/// the `Send` trait, for example `Rc` wrappers for shared state. As such, what we
/// actually need to create to initialize a computation are builders, which we can
/// then move into new threads each of which then construct their actual allocator.
pub trait AllocateBuilder : Send {
    /// The type of allocator to be built.
    type Allocator: Allocate;
    /// Builds allocator, consumes self.
    fn build(self) -> Self::Allocator;
}

/// A type capable of allocating channels.
///
/// There is some feature creep, in that this contains several convenience methods about the nature
/// of the allocated channels, and maintenance methods to ensure that they move records around.
pub trait Allocate {
    /// The index of the worker out of `(0..self.peers())`.
    fn index(&self) -> usize;
    /// The number of workers in the communication group.
    fn peers(&self) -> usize;
    /// Constructs several send endpoints and one receive endpoint.
    fn allocate<T: Data>(&mut self, identifier: usize) -> (Vec<Box<dyn Push<Message<T>>>>, Box<dyn Pull<Message<T>>>);
    /// A shared queue of communication events with channel identifier.
    ///
    /// It is expected that users of the channel allocator will regularly
    /// drain these events in order to drive their computation. If they
    /// fail to do so the event queue may become quite large, and turn
    /// into a performance problem.
    fn events(&self) -> &Rc<RefCell<Vec<usize>>>;

    /// Awaits communication events.
    ///
    /// This method may park the current thread, for at most `duration`,
    /// until new events arrive.
    /// The method is not guaranteed to wait for any amount of time, but
    /// good implementations should use this as a hint to park the thread.
    fn await_events(&self, _duration: Option<Duration>) { }

    /// Ensure that received messages are surfaced in each channel.
    ///
    /// This method should be called to ensure that received messages are
    /// surfaced in each channel, but failing to call the method does not
    /// ensure that they are not surfaced.
    ///
    /// Generally, this method is the indication that the allocator should
    /// present messages contained in otherwise scarce resources (for example
    /// network buffers), under the premise that someone is about to consume
    /// the messages and release the resources.
    fn receive(&mut self) { }

    /// Signal the completion of a batch of reads from channels.
    ///
    /// Conventionally, this method signals to the communication fabric
    /// that the worker is taking a break from reading from channels, and
    /// the fabric should consider re-acquiring scarce resources. This can
    /// lead to the fabric performing defensive copies out of un-consumed
    /// buffers, and can be a performance problem if invoked casually.
    fn release(&mut self) { }

    /// Constructs a pipeline channel from the worker to itself.
    ///
    /// By default, this method uses the thread-local channel constructor
    /// based on a shared `VecDeque` which updates the event queue.
    fn pipeline<T: 'static>(&mut self, identifier: usize) ->
        (thread::ThreadPusher<Message<T>>,
         thread::ThreadPuller<Message<T>>)
    {
        thread::Thread::new_from(identifier, self.events().clone())
    }
}
