//! Parallelization contracts, describing requirements for data movement along dataflow edges.
//!
//! Pacts describe how data should be exchanged between workers, and implement a method which
//! creates a pair of `Push` and `Pull` implementors from an `A: AsWorker`. These two endpoints
//! respectively distribute and collect data among workers according to the pact.
//!
//! The only requirement of a pact is that it not alter the number of `D` records at each time `T`.
//! The progress tracking logic assumes that this number is independent of the pact used.

use std::{
    fmt::{self, Debug},
    marker::PhantomData,
};
use timely_container::PushPartitioned;

use crate::communication::allocator::thread::{ThreadPuller, ThreadPusher};
use crate::communication::{Data, Pull, Push};
use crate::Container;

use super::{BundleCore, Message};
use crate::dataflow::channels::pushers::Exchange as ExchangePusher;
use crate::worker::AsWorker;

use crate::logging::{MessagesEvent, TimelyLogger as Logger};
use crate::progress::Timestamp;

/// A `ParallelizationContractCore` allocates paired `Push` and `Pull` implementors.
pub trait ParallelizationContractCore<T, D> {
    /// Type implementing `Push` produced by this pact.
    type Pusher: Push<BundleCore<T, D>> + 'static;
    /// Type implementing `Pull` produced by this pact.
    type Puller: Pull<BundleCore<T, D>> + 'static;
    /// Allocates a matched pair of push and pull endpoints implementing the pact.
    fn connect<A: AsWorker>(
        self,
        allocator: &mut A,
        identifier: usize,
        address: &[usize],
        logging: Option<Logger>,
    ) -> (Self::Pusher, Self::Puller);
}

/// A `ParallelizationContractCore` specialized for `Vec` containers
/// TODO: Use trait aliases once stable.
pub trait ParallelizationContract<T, D: Clone>: ParallelizationContractCore<T, Vec<D>> {}
impl<T, D: Clone, P: ParallelizationContractCore<T, Vec<D>>> ParallelizationContract<T, D> for P {}

/// A direct connection
#[derive(Debug)]
pub struct Pipeline;

impl<T: 'static, D: Container> ParallelizationContractCore<T, D> for Pipeline {
    type Pusher = LogPusher<T, D, ThreadPusher<BundleCore<T, D>>>;
    type Puller = LogPuller<T, D, ThreadPuller<BundleCore<T, D>>>;
    fn connect<A: AsWorker>(
        self,
        allocator: &mut A,
        identifier: usize,
        address: &[usize],
        logging: Option<Logger>,
    ) -> (Self::Pusher, Self::Puller) {
        let (pusher, puller) = allocator.pipeline::<Message<T, D>>(identifier, address);
        // // ignore `&mut A` and use thread allocator
        // let (pusher, puller) = Thread::new::<Bundle<T, D>>();
        (
            LogPusher::new(
                pusher,
                allocator.index(),
                allocator.index(),
                identifier,
                logging.clone(),
            ),
            LogPuller::new(puller, allocator.index(), identifier, logging),
        )
    }
}

/// An exchange between multiple observers by data
pub struct ExchangeCore<C, D, F> {
    hash_func: F,
    phantom: PhantomData<(C, D)>,
}

/// [ExchangeCore] specialized to vector-based containers.
pub type Exchange<D, F> = ExchangeCore<Vec<D>, D, F>;

impl<C, D, F: FnMut(&D) -> u64 + 'static> ExchangeCore<C, D, F> {
    /// Allocates a new `Exchange` pact from a distribution function.
    pub fn new(func: F) -> ExchangeCore<C, D, F> {
        ExchangeCore {
            hash_func: func,
            phantom: PhantomData,
        }
    }
}

// Exchange uses a `Box<Pushable>` because it cannot know what type of pushable will return from the allocator.
impl<T: Timestamp, C, D: Data + Clone, F: FnMut(&D) -> u64 + 'static>
    ParallelizationContractCore<T, C> for ExchangeCore<C, D, F>
where
    C: Data + Container + PushPartitioned<Item = D>,
{
    type Pusher = ExchangePusher<T, C, D, LogPusher<T, C, Box<dyn Push<BundleCore<T, C>>>>, F>;
    type Puller = LogPuller<T, C, Box<dyn Pull<BundleCore<T, C>>>>;

    fn connect<A: AsWorker>(
        self,
        allocator: &mut A,
        identifier: usize,
        address: &[usize],
        logging: Option<Logger>,
    ) -> (Self::Pusher, Self::Puller) {
        let (senders, receiver) = allocator.allocate::<Message<T, C>>(identifier, address);
        let senders = senders
            .into_iter()
            .enumerate()
            .map(|(i, x)| LogPusher::new(x, allocator.index(), i, identifier, logging.clone()))
            .collect::<Vec<_>>();
        (
            ExchangePusher::new(senders, self.hash_func),
            LogPuller::new(receiver, allocator.index(), identifier, logging.clone()),
        )
    }
}

impl<C, D, F> Debug for ExchangeCore<C, D, F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Exchange").finish()
    }
}

/// Wraps a `Message<T,D>` pusher to provide a `Push<(T, Content<D>)>`.
#[derive(Debug)]
pub struct LogPusher<T, D, P: Push<BundleCore<T, D>>> {
    pusher: P,
    channel: usize,
    counter: usize,
    source: usize,
    target: usize,
    phantom: PhantomData<(T, D)>,
    logging: Option<Logger>,
}

impl<T, D, P: Push<BundleCore<T, D>>> LogPusher<T, D, P> {
    /// Allocates a new pusher.
    pub fn new(
        pusher: P,
        source: usize,
        target: usize,
        channel: usize,
        logging: Option<Logger>,
    ) -> Self {
        LogPusher {
            pusher,
            channel,
            counter: 0,
            source,
            target,
            phantom: PhantomData,
            logging,
        }
    }
}

impl<T, D: Container, P: Push<BundleCore<T, D>>> Push<BundleCore<T, D>> for LogPusher<T, D, P> {
    #[inline]
    fn push(&mut self, pair: &mut Option<BundleCore<T, D>>) {
        if let Some(bundle) = pair {
            self.counter += 1;

            // Stamp the sequence number and source.
            // FIXME: Awkward moment/logic.
            if let Some(message) = bundle.if_mut() {
                message.seq = self.counter - 1;
                message.from = self.source;
            }

            if let Some(logger) = self.logging.as_ref() {
                logger.log(MessagesEvent {
                    is_send: true,
                    channel: self.channel,
                    source: self.source,
                    target: self.target,
                    seq_no: self.counter - 1,
                    length: bundle.data.len(),
                })
            }
        }

        self.pusher.push(pair);
    }
}

/// Wraps a `Message<T,D>` puller to provide a `Pull<(T, Content<D>)>`.
#[derive(Debug)]
pub struct LogPuller<T, D, P: Pull<BundleCore<T, D>>> {
    puller: P,
    channel: usize,
    index: usize,
    phantom: PhantomData<(T, D)>,
    logging: Option<Logger>,
}

impl<T, D, P: Pull<BundleCore<T, D>>> LogPuller<T, D, P> {
    /// Allocates a new `Puller`.
    pub fn new(puller: P, index: usize, channel: usize, logging: Option<Logger>) -> Self {
        LogPuller {
            puller,
            channel,
            index,
            phantom: PhantomData,
            logging,
        }
    }
}

impl<T, D: Container, P: Pull<BundleCore<T, D>>> Pull<BundleCore<T, D>> for LogPuller<T, D, P> {
    #[inline]
    fn pull(&mut self) -> &mut Option<BundleCore<T, D>> {
        let result = self.puller.pull();
        if let Some(bundle) = result {
            let channel = self.channel;
            let target = self.index;

            if let Some(logger) = self.logging.as_ref() {
                logger.log(MessagesEvent {
                    is_send: false,
                    channel,
                    source: bundle.from,
                    target,
                    seq_no: bundle.seq,
                    length: bundle.data.len(),
                });
            }
        }

        result
    }
}
