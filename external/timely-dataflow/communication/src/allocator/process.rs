//! Typed inter-thread, intra-process channels.

use std::rc::Rc;
use std::cell::RefCell;
use std::sync::{Arc, Mutex};
use std::any::Any;
use std::time::Duration;
use std::collections::{HashMap};
use crossbeam_channel::{Sender, Receiver};

use crate::allocator::thread::{ThreadBuilder};
use crate::allocator::{Allocate, AllocateBuilder, Thread};
use crate::{Push, Pull, Message};
use crate::buzzer::Buzzer;

/// An allocator for inter-thread, intra-process communication
pub struct ProcessBuilder {
    inner: ThreadBuilder,
    index: usize,
    peers: usize,
    // below: `Box<Any+Send>` is a `Box<Vec<Option<(Vec<Sender<T>>, Receiver<T>)>>>`
    channels: Arc<Mutex<HashMap<usize, Box<dyn Any+Send>>>>,

    // Buzzers for waking other local workers.
    buzzers_send: Vec<Sender<Buzzer>>,
    buzzers_recv: Vec<Receiver<Buzzer>>,

    counters_send: Vec<Sender<usize>>,
    counters_recv: Receiver<usize>,
}

impl AllocateBuilder for ProcessBuilder {
    type Allocator = Process;
    fn build(self) -> Self::Allocator {

        // Initialize buzzers; send first, then recv.
        for worker in self.buzzers_send.iter() {
            let buzzer = Buzzer::new();
            worker.send(buzzer).expect("Failed to send buzzer");
        }
        let mut buzzers = Vec::with_capacity(self.buzzers_recv.len());
        for worker in self.buzzers_recv.iter() {
            buzzers.push(worker.recv().expect("Failed to recv buzzer"));
        }

        Process {
            inner: self.inner.build(),
            index: self.index,
            peers: self.peers,
            channels: self.channels,
            buzzers,
            counters_send: self.counters_send,
            counters_recv: self.counters_recv,
        }
    }
}

/// An allocator for inter-thread, intra-process communication
pub struct Process {
    inner: Thread,
    index: usize,
    peers: usize,
    // below: `Box<Any+Send>` is a `Box<Vec<Option<(Vec<Sender<T>>, Receiver<T>)>>>`
    channels: Arc<Mutex<HashMap</* channel id */ usize, Box<dyn Any+Send>>>>,
    buzzers: Vec<Buzzer>,
    counters_send: Vec<Sender<usize>>,
    counters_recv: Receiver<usize>,
}

impl Process {
    /// Access the wrapped inner allocator.
    pub fn inner(&mut self) -> &mut Thread { &mut self.inner }
    /// Allocate a list of connected intra-process allocators.
    pub fn new_vector(peers: usize) -> Vec<ProcessBuilder> {

        let mut counters_send = Vec::with_capacity(peers);
        let mut counters_recv = Vec::with_capacity(peers);
        for _ in 0 .. peers {
            let (send, recv) = crossbeam_channel::unbounded();
            counters_send.push(send);
            counters_recv.push(recv);
        }

        let channels = Arc::new(Mutex::new(HashMap::with_capacity(peers)));

        // Allocate matrix of buzzer send and recv endpoints.
        let (buzzers_send, buzzers_recv) = crate::promise_futures(peers, peers);

        counters_recv
            .into_iter()
            .zip(buzzers_send.into_iter())
            .zip(buzzers_recv.into_iter())
            .enumerate()
            .map(|(index, ((recv, bsend), brecv))| {
                ProcessBuilder {
                    inner: ThreadBuilder,
                    index,
                    peers,
                    buzzers_send: bsend,
                    buzzers_recv: brecv,
                    channels: channels.clone(),
                    counters_send: counters_send.clone(),
                    counters_recv: recv,
                }
            })
            .collect()
    }
}

impl Allocate for Process {
    fn index(&self) -> usize { self.index }
    fn peers(&self) -> usize { self.peers }
    fn allocate<T: Any+Send+Sync+'static>(&mut self, identifier: usize) -> (Vec<Box<dyn Push<Message<T>>>>, Box<dyn Pull<Message<T>>>) {

        // this is race-y global initialisation of all channels for all workers, performed by the
        // first worker that enters this critical section

        // ensure exclusive access to shared list of channels
        let mut channels = self.channels.lock().expect("mutex error?");

        let (sends, recv, empty) = {

            // we may need to alloc a new channel ...
            let entry = channels.entry(identifier).or_insert_with(|| {

                let mut pushers = Vec::with_capacity(self.peers);
                let mut pullers = Vec::with_capacity(self.peers);
                for buzzer in self.buzzers.iter() {
                    let (s, r): (Sender<Message<T>>, Receiver<Message<T>>) = crossbeam_channel::unbounded();
                    // TODO: the buzzer in the pusher may be redundant, because we need to buzz post-counter.
                    pushers.push((Pusher { target: s }, buzzer.clone()));
                    pullers.push(Puller { source: r, current: None });
                }

                let mut to_box = Vec::with_capacity(pullers.len());
                for recv in pullers.into_iter() {
                    to_box.push(Some((pushers.clone(), recv)));
                }

                Box::new(to_box)
            });

            let vector =
            entry
                .downcast_mut::<Vec<Option<(Vec<(Pusher<Message<T>>, Buzzer)>, Puller<Message<T>>)>>>()
                .expect("failed to correctly cast channel");

            let (sends, recv) =
            vector[self.index]
                .take()
                .expect("channel already consumed");

            let empty = vector.iter().all(|x| x.is_none());

            (sends, recv, empty)
        };

        // send is a vec of all senders, recv is this worker's receiver

        if empty { channels.remove(&identifier); }

        use crate::allocator::counters::ArcPusher as CountPusher;
        use crate::allocator::counters::Puller as CountPuller;

        let sends =
        sends.into_iter()
             .zip(self.counters_send.iter())
             .map(|((s,b), sender)| CountPusher::new(s, identifier, sender.clone(), b))
             .map(|s| Box::new(s) as Box<dyn Push<super::Message<T>>>)
             .collect::<Vec<_>>();

        let recv = Box::new(CountPuller::new(recv, identifier, self.inner.events().clone())) as Box<dyn Pull<super::Message<T>>>;

        (sends, recv)
    }

    fn events(&self) -> &Rc<RefCell<Vec<usize>>> {
        self.inner.events()
    }

    fn await_events(&self, duration: Option<Duration>) {
        self.inner.await_events(duration);
    }

    fn receive(&mut self) {
        let mut events = self.inner.events().borrow_mut();
        while let Ok(index) = self.counters_recv.try_recv() {
            events.push(index);
        }
    }
}

/// The push half of an intra-process channel.
struct Pusher<T> {
    target: Sender<T>,
}

impl<T> Clone for Pusher<T> {
    fn clone(&self) -> Self {
        Self {
            target: self.target.clone(),
        }
    }
}

impl<T> Push<T> for Pusher<T> {
    #[inline] fn push(&mut self, element: &mut Option<T>) {
        if let Some(element) = element.take() {
            // The remote endpoint could be shut down, and so
            // it is not fundamentally an error to fail to send.
            let _ = self.target.send(element);
        }
    }
}

/// The pull half of an intra-process channel.
struct Puller<T> {
    current: Option<T>,
    source: Receiver<T>,
}

impl<T> Pull<T> for Puller<T> {
    #[inline]
    fn pull(&mut self) -> &mut Option<T> {
        self.current = self.source.try_recv().ok();
        &mut self.current
    }
}
