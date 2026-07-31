//! The exchange pattern distributes pushed data between many target pushees.

use crate::communication::Push;
use crate::dataflow::channels::{BundleCore, Message};
use crate::{Container, Data};
use timely_container::PushPartitioned;

// TODO : Software write combining
/// Distributes records among target pushees according to a distribution function.
pub struct Exchange<T, C: Container, D, P: Push<BundleCore<T, C>>, H: FnMut(&D) -> u64> {
    pushers: Vec<P>,
    buffers: Vec<C>,
    current: Option<T>,
    hash_func: H,
    phantom: std::marker::PhantomData<D>,
}

impl<T: Clone, C: Container, D: Data, P: Push<BundleCore<T, C>>, H: FnMut(&D) -> u64>
    Exchange<T, C, D, P, H>
{
    /// Allocates a new `Exchange` from a supplied set of pushers and a distribution function.
    pub fn new(pushers: Vec<P>, key: H) -> Exchange<T, C, D, P, H> {
        let mut buffers = vec![];
        for _ in 0..pushers.len() {
            buffers.push(Default::default());
        }
        Exchange {
            pushers,
            hash_func: key,
            buffers,
            current: None,
            phantom: std::marker::PhantomData,
        }
    }
    #[inline]
    fn flush(&mut self, index: usize) {
        if !self.buffers[index].is_empty() {
            if let Some(ref time) = self.current {
                Message::push_at(
                    &mut self.buffers[index],
                    time.clone(),
                    &mut self.pushers[index],
                );
            }
        }
    }
}

impl<T: Eq + Data, C: Container, D: Data, P: Push<BundleCore<T, C>>, H: FnMut(&D) -> u64>
    Push<BundleCore<T, C>> for Exchange<T, C, D, P, H>
where
    C: PushPartitioned<Item = D>,
{
    #[inline(never)]
    fn push(&mut self, message: &mut Option<BundleCore<T, C>>) {
        // if only one pusher, no exchange
        if self.pushers.len() == 1 {
            self.pushers[0].push(message);
        } else if let Some(message) = message {
            let message = message.as_mut();
            let time = &message.time;
            let data = &mut message.data;

            // if the time isn't right, flush everything.
            if self.current.as_ref().map_or(false, |x| x != time) {
                for index in 0..self.pushers.len() {
                    self.flush(index);
                }
            }
            self.current = Some(time.clone());

            let hash_func = &mut self.hash_func;

            // if the number of pushers is a power of two, use a mask
            if (self.pushers.len() & (self.pushers.len() - 1)) == 0 {
                let mask = (self.pushers.len() - 1) as u64;
                let pushers = &mut self.pushers;
                data.push_partitioned(
                    &mut self.buffers,
                    move |datum| ((hash_func)(datum) & mask) as usize,
                    |index, buffer| {
                        Message::push_at(buffer, time.clone(), &mut pushers[index]);
                    },
                );
            }
            // as a last resort, use mod (%)
            else {
                let num_pushers = self.pushers.len() as u64;
                let pushers = &mut self.pushers;
                data.push_partitioned(
                    &mut self.buffers,
                    move |datum| ((hash_func)(datum) % num_pushers) as usize,
                    |index, buffer| {
                        Message::push_at(buffer, time.clone(), &mut pushers[index]);
                    },
                );
            }
        } else {
            // flush
            for index in 0..self.pushers.len() {
                self.flush(index);
                self.pushers[index].push(&mut None);
            }
        }
    }
}
