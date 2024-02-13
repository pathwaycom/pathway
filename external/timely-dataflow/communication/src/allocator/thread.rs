//! Intra-thread communication.

use std::rc::Rc;
use std::cell::RefCell;
use std::time::Duration;
use std::collections::VecDeque;

use crate::allocator::{Allocate, AllocateBuilder};
use crate::allocator::counters::Pusher as CountPusher;
use crate::allocator::counters::Puller as CountPuller;
use crate::{Push, Pull, Message};

/// Builder for single-threaded allocator.
pub struct ThreadBuilder;

impl AllocateBuilder for ThreadBuilder {
    type Allocator = Thread;
    fn build(self) -> Self::Allocator { Thread::new() }
}


/// An allocator for intra-thread communication.
pub struct Thread {
    /// Shared counts of messages in channels.
    events: Rc<RefCell<Vec<usize>>>,
}

impl Allocate for Thread {
    fn index(&self) -> usize { 0 }
    fn peers(&self) -> usize { 1 }
    fn allocate<T: 'static>(&mut self, identifier: usize) -> (Vec<Box<dyn Push<Message<T>>>>, Box<dyn Pull<Message<T>>>) {
        let (pusher, puller) = Thread::new_from(identifier, self.events.clone());
        (vec![Box::new(pusher)], Box::new(puller))
    }
    fn events(&self) -> &Rc<RefCell<Vec<usize>>> {
        &self.events
    }
    fn await_events(&self, duration: Option<Duration>) {
        if self.events.borrow().is_empty() {
            if let Some(duration) = duration {
                std::thread::park_timeout(duration);
            }
            else {
                std::thread::park();
            }
        }
    }
}

/// Thread-local counting channel push endpoint.
pub type ThreadPusher<T> = CountPusher<T, Pusher<T>>;
/// Thread-local counting channel pull endpoint.
pub type ThreadPuller<T> = CountPuller<T, Puller<T>>;

impl Thread {
    /// Allocates a new thread-local channel allocator.
    pub fn new() -> Self {
        Thread {
            events: Rc::new(RefCell::new(Default::default())),
        }
    }

    /// Creates a new thread-local channel from an identifier and shared counts.
    pub fn new_from<T: 'static>(identifier: usize, events: Rc<RefCell<Vec<usize>>>)
        -> (ThreadPusher<Message<T>>, ThreadPuller<Message<T>>)
    {
        let shared = Rc::new(RefCell::new((VecDeque::<Message<T>>::new(), VecDeque::<Message<T>>::new())));
        let pusher = Pusher { target: shared.clone() };
        let pusher = CountPusher::new(pusher, identifier, events.clone());
        let puller = Puller { source: shared, current: None };
        let puller = CountPuller::new(puller, identifier, events);
        (pusher, puller)
    }
}


/// The push half of an intra-thread channel.
pub struct Pusher<T> {
    target: Rc<RefCell<(VecDeque<T>, VecDeque<T>)>>,
}

impl<T> Push<T> for Pusher<T> {
    #[inline]
    fn push(&mut self, element: &mut Option<T>) {
        let mut borrow = self.target.borrow_mut();
        if let Some(element) = element.take() {
            borrow.0.push_back(element);
        }
        *element = borrow.1.pop_front();
    }
}

/// The pull half of an intra-thread channel.
pub struct Puller<T> {
    current: Option<T>,
    source: Rc<RefCell<(VecDeque<T>, VecDeque<T>)>>,
}

impl<T> Pull<T> for Puller<T> {
    #[inline]
    fn pull(&mut self) -> &mut Option<T> {
        let mut borrow = self.source.borrow_mut();
        // if let Some(element) = self.current.take() {
        //     // TODO : Arbitrary constant.
        //     if borrow.1.len() < 16 {
        //         borrow.1.push_back(element);
        //     }
        // }
        self.current = borrow.0.pop_front();
        &mut self.current
    }
}
