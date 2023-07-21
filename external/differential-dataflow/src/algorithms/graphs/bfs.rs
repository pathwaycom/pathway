//! Breadth-first distance labeling.

use std::hash::Hash;

use timely::dataflow::*;

use ::{Collection, ExchangeData};
use ::operators::*;
use ::lattice::Lattice;

/// Returns pairs (node, dist) indicating distance of each node from a root.
pub fn bfs<G, N>(edges: &Collection<G, (N,N)>, roots: &Collection<G, N>) -> Collection<G, (N,u32)>
where
    G: Scope,
    G::Timestamp: Lattice+Ord,
    N: ExchangeData+Hash,
{
    use operators::arrange::arrangement::ArrangeByKey;
    let edges = edges.arrange_by_key();
    bfs_arranged(&edges, roots)
}

use crate::trace::TraceReader;
use crate::operators::arrange::Arranged;

/// Returns pairs (node, dist) indicating distance of each node from a root.
pub fn bfs_arranged<G, N, Tr>(edges: &Arranged<G, Tr>, roots: &Collection<G, N>) -> Collection<G, (N, u32)>
where
    G: Scope,
    G::Timestamp: Lattice+Ord,
    N: ExchangeData+Hash,
    Tr: TraceReader<Key=N, Val=N, Time=G::Timestamp, R=isize>+Clone+'static,
{
    // initialize roots as reaching themselves at distance 0
    let nodes = roots.map(|x| (x, 0));

    // repeatedly update minimal distances each node can be reached from each root
    nodes.iterate(|inner| {

        let edges = edges.enter(&inner.scope());
        let nodes = nodes.enter(&inner.scope());

        inner.join_core(&edges, |_k,l,d| Some((d.clone(), l+1)))
             .concat(&nodes)
             .reduce(|_, s, t| t.push((s[0].0.clone(), 1)))
     })
}