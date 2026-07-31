//! Broadcast records to all workers.

use crate::dataflow::operators::{Exchange, Map};
use crate::dataflow::{Scope, Stream};
use crate::ExchangeData;

/// Broadcast records to all workers.
pub trait Broadcast<D: ExchangeData> {
    /// Broadcast records to all workers.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Broadcast, Inspect};
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .broadcast()
    ///            .inspect(|x| println!("seen: {:?}", x));
    /// });
    /// ```
    fn broadcast(&self) -> Self;
}

impl<G: Scope, D: ExchangeData> Broadcast<D> for Stream<G, D> {
    fn broadcast(&self) -> Stream<G, D> {
        // NOTE: Simplified implementation due to underlying motion
        // in timely dataflow internals. Optimize once they have
        // settled down.
        let peers = self.scope().peers() as u64;
        self.flat_map(move |x| (0..peers).map(move |i| (i, x.clone())))
            .exchange(|ix| ix.0)
            .map(|(_i, x)| x)
    }
}
