//! Zero-copy allocator for intra-process serialized communication.

use std::rc::Rc;
use std::cell::RefCell;
use std::collections::{VecDeque, HashMap, hash_map::Entry};
use crossbeam_channel::{Sender, Receiver};

use bytes::arc::Bytes;

use crate::networking::MessageHeader;

use crate::{Allocate, Message, Data, Push, Pull};
use crate::allocator::{AllocateBuilder};
use crate::allocator::canary::Canary;

use super::bytes_exchange::{BytesPull, SendEndpoint, MergeQueue};

use super::push_pull::{Pusher, Puller};

/// Builds an instance of a ProcessAllocator.
///
/// Builders are required because some of the state in a `ProcessAllocator` cannot be sent between
/// threads (specifically, the `Rc<RefCell<_>>` local channels). So, we must package up the state
/// shared between threads here, and then provide a method that will instantiate the non-movable
/// members once in the destination thread.
pub struct ProcessBuilder {
    index:  usize,                      // number out of peers
    peers:  usize,                      // number of peer allocators.
    pushers: Vec<Receiver<MergeQueue>>, // for pushing bytes at other workers.
    pullers: Vec<Sender<MergeQueue>>,   // for pulling bytes from other workers.
}

impl ProcessBuilder {
    /// Creates a vector of builders, sharing appropriate state.
    ///
    /// This method requires access to a byte exchanger, from which it mints channels.
    pub fn new_vector(count: usize) -> Vec<ProcessBuilder> {

        // Channels for the exchange of `MergeQueue` endpoints.
        let (pullers_vec, pushers_vec) = crate::promise_futures(count, count);

        pushers_vec
            .into_iter()
            .zip(pullers_vec)
            .enumerate()
            .map(|(index, (pushers, pullers))|
                ProcessBuilder {
                    index,
                    peers: count,
                    pushers,
                    pullers,
                }
            )
            .collect()
    }

    /// Builds a `ProcessAllocator`, instantiating `Rc<RefCell<_>>` elements.
    pub fn build(self) -> ProcessAllocator {

        // Fulfill puller obligations.
        let mut recvs = Vec::with_capacity(self.peers);
        for puller in self.pullers.into_iter() {
            let buzzer = crate::buzzer::Buzzer::new();
            let queue = MergeQueue::new(buzzer);
            puller.send(queue.clone()).expect("Failed to send MergeQueue");
            recvs.push(queue.clone());
        }

        // Extract pusher commitments.
        let mut sends = Vec::with_capacity(self.peers);
        for pusher in self.pushers.into_iter() {
            let queue = pusher.recv().expect("Failed to receive MergeQueue");
            let sendpoint = SendEndpoint::new(queue);
            sends.push(Rc::new(RefCell::new(sendpoint)));
        }

        ProcessAllocator {
            index: self.index,
            peers: self.peers,
            events: Rc::new(RefCell::new(Default::default())),
            canaries: Rc::new(RefCell::new(Vec::new())),
            channel_id_bound: None,
            staged: Vec::new(),
            sends,
            recvs,
            to_local: HashMap::new(),
        }
    }
}

impl AllocateBuilder for ProcessBuilder {
    type Allocator = ProcessAllocator;
    /// Builds allocator, consumes self.
    fn build(self) -> Self::Allocator {
        self.build()
    }

}

/// A serializing allocator for inter-thread intra-process communication.
pub struct ProcessAllocator {

    index:      usize,                              // number out of peers
    peers:      usize,                              // number of peer allocators (for typed channel allocation).

    events: Rc<RefCell<Vec<usize>>>,

    canaries: Rc<RefCell<Vec<usize>>>,

    channel_id_bound: Option<usize>,

    // sending, receiving, and responding to binary buffers.
    staged:     Vec<Bytes>,
    sends:      Vec<Rc<RefCell<SendEndpoint<MergeQueue>>>>, // sends[x] -> goes to thread x.
    recvs:      Vec<MergeQueue>,                            // recvs[x] <- from thread x.
    to_local:   HashMap<usize, Rc<RefCell<VecDeque<Bytes>>>>,          // to worker-local typed pullers.
}

impl Allocate for ProcessAllocator {
    fn index(&self) -> usize { self.index }
    fn peers(&self) -> usize { self.peers }
    fn allocate<T: Data>(&mut self, identifier: usize) -> (Vec<Box<dyn Push<Message<T>>>>, Box<dyn Pull<Message<T>>>) {

        // Assume and enforce in-order identifier allocation.
        if let Some(bound) = self.channel_id_bound {
            assert!(bound < identifier);
        }
        self.channel_id_bound = Some(identifier);

        let mut pushes = Vec::<Box<dyn Push<Message<T>>>>::with_capacity(self.peers());

        for target_index in 0 .. self.peers() {

            // message header template.
            let header = MessageHeader {
                channel:    identifier,
                source:     self.index,
                target:     target_index,
                length:     0,
                seqno:      0,
            };

            // create, box, and stash new process_binary pusher.
            pushes.push(Box::new(Pusher::new(header, self.sends[target_index].clone())));
        }

        let channel =
        self.to_local
            .entry(identifier)
            .or_insert_with(|| Rc::new(RefCell::new(VecDeque::new())))
            .clone();

        use crate::allocator::counters::Puller as CountPuller;
        let canary = Canary::new(identifier, self.canaries.clone());
        let puller = Box::new(CountPuller::new(Puller::new(channel, canary), identifier, self.events().clone()));

        (pushes, puller)
    }

    // Perform preparatory work, most likely reading binary buffers from self.recv.
    #[inline(never)]
    fn receive(&mut self) {

        // Check for channels whose `Puller` has been dropped.
        let mut canaries = self.canaries.borrow_mut();
        for dropped_channel in canaries.drain(..) {
            let _dropped =
            self.to_local
                .remove(&dropped_channel)
                .expect("non-existent channel dropped");
            // Borrowed channels may be non-empty, if the dataflow was forcibly
            // dropped. The contract is that if a dataflow is dropped, all other
            // workers will drop the dataflow too, without blocking indefinitely
            // on events from it.
            // assert!(dropped.borrow().is_empty());
        }
        std::mem::drop(canaries);

        let mut events = self.events.borrow_mut();

        for recv in self.recvs.iter_mut() {
            recv.drain_into(&mut self.staged);
        }

        for mut bytes in self.staged.drain(..) {

            // We expect that `bytes` contains an integral number of messages.
            // No splitting occurs across allocations.
            while bytes.len() > 0 {

                if let Some(header) = MessageHeader::try_read(&mut bytes[..]) {

                    // Get the header and payload, ditch the header.
                    let mut peel = bytes.extract_to(header.required_bytes());
                    let _ = peel.extract_to(40);

                    // Increment message count for channel.
                    // Safe to do this even if the channel has been dropped.
                    events.push(header.channel);

                    // Ensure that a queue exists.
                    match self.to_local.entry(header.channel) {
                        Entry::Vacant(entry) => {
                            // We may receive data before allocating, and shouldn't block.
                            if self.channel_id_bound.map(|b| b < header.channel).unwrap_or(true) {
                                entry.insert(Rc::new(RefCell::new(VecDeque::new())))
                                    .borrow_mut()
                                    .push_back(peel);
                            }
                        }
                        Entry::Occupied(mut entry) => {
                            entry.get_mut().borrow_mut().push_back(peel);
                        }
                    }
                }
                else {
                    println!("failed to read full header!");
                }
            }
        }
    }

    // Perform postparatory work, most likely sending un-full binary buffers.
    fn release(&mut self) {
        // Publish outgoing byte ledgers.
        for send in self.sends.iter_mut() {
            send.borrow_mut().publish();
        }

        // OPTIONAL: Tattle on channels sitting on borrowed data.
        // OPTIONAL: Perhaps copy borrowed data into owned allocation.
        // for (index, list) in self.to_local.iter() {
        //     let len = list.borrow_mut().len();
        //     if len > 0 {
        //         eprintln!("Warning: worker {}, undrained channel[{}].len() = {}", self.index, index, len);
        //     }
        // }
    }

    fn events(&self) -> &Rc<RefCell<Vec<usize>>> {
        &self.events
    }
    fn await_events(&self, duration: Option<std::time::Duration>) {
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