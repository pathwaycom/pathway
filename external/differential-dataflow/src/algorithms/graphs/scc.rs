//! Strongly connected component structure.

use std::mem;
use std::hash::Hash;

use timely::dataflow::*;

use ::{Collection, ExchangeData};
use ::operators::*;
use ::lattice::Lattice;
use ::difference::{Abelian, Multiply};

use super::propagate::propagate;

/// Iteratively removes nodes with no in-edges.
pub fn trim<G, N, R>(graph: &Collection<G, (N,N), R>) -> Collection<G, (N,N), R>
where
    G: Scope,
    G::Timestamp: Lattice+Ord,
    N: ExchangeData+Hash,
    R: ExchangeData + Abelian,
    R: Multiply<R, Output=R>,
    R: From<i8>,
{
    graph.iterate(|edges| {
        // keep edges from active edge destinations.
        let active =
        edges.map(|(_src,dst)| dst)
             .threshold(|_,c| if c.is_zero() { R::from(0 as i8) } else { R::from(1 as i8) });

        graph.enter(&edges.scope())
             .semijoin(&active)
    })
}

/// Returns the subset of edges in the same strongly connected component.
pub fn strongly_connected<G, N, R>(graph: &Collection<G, (N,N), R>) -> Collection<G, (N,N), R>
where
    G: Scope,
    G::Timestamp: Lattice+Ord,
    N: ExchangeData+Hash,
    R: ExchangeData + Abelian,
    R: Multiply<R, Output=R>,
    R: From<i8>
{
    graph.iterate(|inner| {
        let edges = graph.enter(&inner.scope());
        let trans = edges.map_in_place(|x| mem::swap(&mut x.0, &mut x.1));
        trim_edges(&trim_edges(inner, &edges), &trans)
    })
}

fn trim_edges<G, N, R>(cycle: &Collection<G, (N,N), R>, edges: &Collection<G, (N,N), R>)
    -> Collection<G, (N,N), R>
where
    G: Scope,
    G::Timestamp: Lattice+Ord,
    N: ExchangeData+Hash,
    R: ExchangeData + Abelian,
    R: Multiply<R, Output=R>,
    R: From<i8>
{
    let nodes = edges.map_in_place(|x| x.0 = x.1.clone())
                     .consolidate();

    // NOTE: With a node -> int function, can be improved by:
    // let labels = propagate_at(&cycle, &nodes, |x| *x as u64);
    let labels = propagate(&cycle, &nodes);

    edges.join_map(&labels, |e1,e2,l1| (e2.clone(),(e1.clone(),l1.clone())))
         .join_map(&labels, |e2,(e1,l1),l2| ((e1.clone(),e2.clone()),(l1.clone(),l2.clone())))
         .filter(|(_,(l1,l2))| l1 == l2)
         .map(|((x1,x2),_)| (x2,x1))
}
