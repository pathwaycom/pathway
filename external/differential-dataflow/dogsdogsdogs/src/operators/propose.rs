use timely::dataflow::Scope;

use differential_dataflow::{ExchangeData, Collection, Hashable};
use differential_dataflow::difference::{Monoid, Multiply};
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::trace::TraceReader;

/// Proposes extensions to a prefix stream.
///
/// This method takes a collection `prefixes` and an arrangement `arrangement` and for each
/// update in the collection joins with the accumulated arranged records at a time less or
/// equal to that of the update. Note that this is not a join by itself, but can be used to
/// create a join if the `prefixes` collection is also arranged and responds to changes that
/// `arrangement` undergoes. More complicated patterns are also appropriate, as in the case
/// of delta queries.
pub fn propose<G, Tr, F, P>(
    prefixes: &Collection<G, P, Tr::R>,
    arrangement: Arranged<G, Tr>,
    key_selector: F,
) -> Collection<G, (P, Tr::Val), Tr::R>
where
    G: Scope,
    G::Timestamp: Lattice,
    Tr: TraceReader<Time=G::Timestamp>+Clone+'static,
    Tr::Key: Ord+Hashable+Default,
    Tr::Val: Clone,
    Tr::R: Monoid+Multiply<Output = Tr::R>+ExchangeData,
    F: Fn(&P)->Tr::Key+Clone+'static,
    P: ExchangeData,
{
    crate::operators::lookup_map(
        prefixes,
        arrangement,
        move |p: &P, k: &mut Tr::Key| { *k = key_selector(p); },
        |prefix, diff, value, sum| ((prefix.clone(), value.clone()), diff.clone().multiply(sum)),
        Default::default(),
        Default::default(),
        Default::default(),
    )
}

/// Proposes distinct extensions to a prefix stream.
///
/// Unlike `propose`, this method does not scale the multiplicity of matched
/// prefixes by the number of matches in `arrangement`. This can be useful to
/// avoid the need to prepare an arrangement of distinct extensions.
pub fn propose_distinct<G, Tr, F, P>(
    prefixes: &Collection<G, P, Tr::R>,
    arrangement: Arranged<G, Tr>,
    key_selector: F,
) -> Collection<G, (P, Tr::Val), Tr::R>
where
    G: Scope,
    G::Timestamp: Lattice,
    Tr: TraceReader<Time=G::Timestamp>+Clone+'static,
    Tr::Key: Ord+Hashable+Default,
    Tr::Val: Clone,
    Tr::R: Monoid+Multiply<Output = Tr::R>+ExchangeData,
    F: Fn(&P)->Tr::Key+Clone+'static,
    P: ExchangeData,
{
    crate::operators::lookup_map(
        prefixes,
        arrangement,
        move |p: &P, k: &mut Tr::Key| { *k = key_selector(p); },
        |prefix, diff, value, _sum| ((prefix.clone(), value.clone()), diff.clone()),
        Default::default(),
        Default::default(),
        Default::default(),
    )
}
