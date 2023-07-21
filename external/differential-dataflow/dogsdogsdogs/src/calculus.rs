//! Traits and implementations for differentiating and integrating collections.
//!
//! The `Differentiate` and `Integrate` traits allow us to move between standard differential
//! dataflow collections, and collections that describe their instantaneous change. The first
//! trait converts a collection to one that contains each change at the moment it occurs, but
//! then immediately retracting it. The second trait takes such a representation are recreates
//! the collection from its instantaneous changes.
//!
//! These two traits together allow us to build dataflows that maintain computates over inputs
//! that are the instantaneous changes, and then to reconstruct collections from them. The most
//! clear use case for this are "delta query" implementations of relational joins, where linearity
//! allows us to write dataflows based on instantaneous changes, whose "accumluated state" is
//! almost everywhere empty (and so has a low memory footprint, if the system works as planned).

use timely::dataflow::Scope;
use timely::dataflow::scopes::Child;
use timely::dataflow::operators::{Filter, Map};
use differential_dataflow::{AsCollection, Collection, Data};
use differential_dataflow::difference::Abelian;

use crate::altneu::AltNeu;

/// Produce a collection containing the changes at the moments they happen.
pub trait Differentiate<G: Scope, D: Data, R: Abelian> {
    fn differentiate<'a>(&self, child: &Child<'a, G, AltNeu<G::Timestamp>>) -> Collection<Child<'a, G, AltNeu<G::Timestamp>>, D, R>;
}

/// Collect instantaneous changes back in to a collection.
pub trait Integrate<G: Scope, D: Data, R: Abelian> {
    fn integrate(&self) -> Collection<G, D, R>;
}

impl<G, D, R> Differentiate<G, D, R> for Collection<G, D, R>
where
    G: Scope,
    D: Data,
    R: Abelian,
{
    // For each (data, Alt(time), diff) we add a (data, Neu(time), -diff).
    fn differentiate<'a>(&self, child: &Child<'a, G, AltNeu<G::Timestamp>>) -> Collection<Child<'a, G, AltNeu<G::Timestamp>>, D, R> {
        self.enter(child)
            .inner
            .flat_map(|(data, time, diff)| {
                let neu = (data.clone(), AltNeu::neu(time.time.clone()), diff.clone().negate());
                let alt = (data, time, diff);
                Some(alt).into_iter().chain(Some(neu))
            })
            .as_collection()
    }
}

impl<'a, G, D, R> Integrate<G, D, R> for Collection<Child<'a, G, AltNeu<G::Timestamp>>, D, R>
where
    G: Scope,
    D: Data,
    R: Abelian,
{
    // We discard each `neu` variant and strip off the `alt` wrapper.
    fn integrate(&self) -> Collection<G, D, R> {
        self.inner
            .filter(|(_d,t,_r)| !t.neu)
            .as_collection()
            .leave()
    }
}
