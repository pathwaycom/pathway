//! A shared ordered log.

use std::rc::Rc;
use std::cell::RefCell;
use std::time::{Instant, Duration};
use std::collections::VecDeque;

use crate::{communication::Allocate, ExchangeData, PartialOrder};
use crate::scheduling::Scheduler;
use crate::worker::Worker;
use crate::dataflow::channels::pact::Exchange;
use crate::dataflow::operators::generic::operator::source;
use crate::dataflow::operators::generic::operator::Operator;
use crate::scheduling::activate::Activator;

// A Sequencer needs all operators firing with high frequency, because
// it uses the timer to gauge progress. If other workers cease
// advancing their own capabilities, although they might receive a
// record they may not actually tick forward their own source clocks,
// and no one will actually form the sequence.
//
// A CatchupActivator is an activator with an optional timestamp
// attached. This allows us to represent a special state, where after
// receiving an action from another worker, each of the other workers
// will keep scheduling its source operator, until its capability
// timestamp exceeds the greatest timestamp that the sink has
// received.
//
// This allows operators to go quiet again until a new requests shows
// up. The operators lose the ability to confirm that nothing is
// scheduled for a particular time (they could request this with a
// no-op event bearing a timestamp), but everyone still sees the same
// sequence.
struct CatchupActivator {
    pub catchup_until: Option<Duration>,
    activator: Activator,
}

impl CatchupActivator {
    pub fn activate(&self) {
        self.activator.activate();
    }
}

/// Orders elements inserted across all workers.
///
/// A Sequencer allows each worker to insert into a consistent ordered
/// sequence that is seen by all workers in the same order.
pub struct Sequencer<T> {
    activator: Rc<RefCell<Option<CatchupActivator>>>,
    send: Rc<RefCell<VecDeque<T>>>, // proposed items.
    recv: Rc<RefCell<VecDeque<T>>>, // sequenced items.
}

impl<T: ExchangeData> Sequencer<T> {

    /// Creates a new Sequencer.
    ///
    /// The `timer` instant is used to synchronize the workers, who use this
    /// elapsed time as their timestamp. Elements are ordered by this time,
    /// and cannot be made visible until all workers have reached the time.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::time::{Instant, Duration};
    ///
    /// use timely::Config;
    /// use timely::synchronization::Sequencer;
    ///
    /// timely::execute(Config::process(4), |worker| {
    ///     let timer = Instant::now();
    ///     let mut sequencer = Sequencer::new(worker, timer);
    ///
    ///     for round in 0 .. 10 {
    ///
    ///         // Sleep, and then send an announcement on wake-up.
    ///         std::thread::sleep(Duration::from_millis(1 + worker.index() as u64));
    ///         sequencer.push(format!("worker {:?}, round {:?}", worker.index(), round));
    ///
    ///         // Ensures the pushed string is sent.
    ///         worker.step();
    ///
    ///         // Read out received announcements.
    ///         while let Some(element) = sequencer.next() {
    ///             println!("{:?}:\tWorker {:?}:\t recv'd: {:?}", timer.elapsed(), worker.index(), element);
    ///         }
    ///     }
    /// }).expect("Timely computation did not complete correctly.");
    /// ```
    pub fn new<A: Allocate>(worker: &mut Worker<A>, timer: Instant) -> Self {
        Sequencer::preloaded(worker, timer, VecDeque::new())
    }

    /// Creates a new Sequencer preloaded with a queue of
    /// elements.
    pub fn preloaded<A: Allocate>(worker: &mut Worker<A>, timer: Instant, preload: VecDeque<T>) -> Self {

        let send: Rc<RefCell<VecDeque<T>>> = Rc::new(RefCell::new(VecDeque::new()));
        let recv = Rc::new(RefCell::new(preload));
        let send_weak = Rc::downgrade(&send);
        let recv_weak = Rc::downgrade(&recv);

        // The SequenceInput activator will be held by the sequencer,
        // by the operator itself, and by the sink operator. We can
        // only initialize the activator once we obtain the operator
        // address.
        let activator = Rc::new(RefCell::new(None));
        let activator_source = activator.clone();
        let activator_sink = activator.clone();

        // build a dataflow used to serialize and circulate commands
        worker.dataflow::<Duration,_,_>(move |dataflow| {

            let scope = dataflow.clone();
            let peers = dataflow.peers();

            let mut recvd = Vec::new();
            let mut vector = Vec::new();

            // monotonic counter to maintain per-worker total order.
            let mut counter = 0;

            // a source that attempts to pull from `recv` and produce commands for everyone
            source(dataflow, "SequenceInput", move |capability, info| {

                // intialize activator, now that we have the address
                activator_source
                    .borrow_mut()
                    .replace(CatchupActivator {
                        activator: scope.activator_for(&info.address[..]),
                        catchup_until: None,
                    });

                // so we can drop, if input queue vanishes.
                let mut capability = Some(capability);

                // closure broadcasts any commands it grabs.
                move |output| {

                    if let Some(send_queue) = send_weak.upgrade() {

                        // capability *should* still be non-None.
                        let capability = capability.as_mut().expect("Capability unavailable");

                        // downgrade capability to current time.
                        capability.downgrade(&timer.elapsed());

                        // drain and broadcast `send`.
                        let mut session = output.session(&capability);
                        let mut borrow = send_queue.borrow_mut();
                        for element in borrow.drain(..) {
                            for worker_index in 0 .. peers {
                                session.give((worker_index, counter, element.clone()));
                            }
                            counter += 1;
                        }

                        let mut activator_borrow = activator_source.borrow_mut();
                        let mut activator = activator_borrow.as_mut().unwrap();

                        if let Some(t) = activator.catchup_until {
                            if capability.time().less_than(&t) {
                                activator.activate();
                            } else {
                                activator.catchup_until = None;
                            }
                        }
                    } else {
                        capability = None;
                    }
                }
            })
            .sink(
                Exchange::new(|x: &(usize, usize, T)| x.0 as u64),
                "SequenceOutput",
                move |input| {

                    // grab each command and queue it up
                    input.for_each(|time, data| {
                        data.swap(&mut vector);

                        recvd.reserve(vector.len());
                        for (worker, counter, element) in vector.drain(..) {
                            recvd.push(((time.time().clone(), worker, counter), element));
                        }
                    });

                    recvd.sort_by(|x,y| x.0.cmp(&y.0));

                    if let Some(last) = recvd.last() {
                        let mut activator_borrow = activator_sink.borrow_mut();
                        let mut activator = activator_borrow.as_mut().unwrap();

                        activator.catchup_until = Some((last.0).0);
                        activator.activate();
                    }

                    // determine how many (which) elements to read from `recvd`.
                    let count = recvd.iter().filter(|&((ref time, _, _), _)| !input.frontier().less_equal(time)).count();
                    let iter = recvd.drain(..count);

                    if let Some(recv_queue) = recv_weak.upgrade() {
                        recv_queue.borrow_mut().extend(iter.map(|(_,elem)| elem));
                    }
                }
            );
        });

        Sequencer { activator, send, recv, }
    }

    /// Adds an element to the shared log.
    pub fn push(&mut self, element: T) {
        self.send.borrow_mut().push_back(element);
        self.activator.borrow_mut().as_mut().unwrap().activate();
    }
}

impl<T> Iterator for Sequencer<T> {
    type Item = T;
    fn next(&mut self) -> Option<T> {
        self.recv.borrow_mut().pop_front()
    }
}

// We should activate on drop, as this will cause the source to drop its capability.
impl<T> Drop for Sequencer<T> {
    fn drop(&mut self) {
        self.activator
            .borrow()
            .as_ref()
            .expect("Sequencer.activator unavailable")
            .activate()
    }
}