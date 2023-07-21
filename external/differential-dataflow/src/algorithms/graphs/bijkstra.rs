//! Bi-directional Dijkstra distance labeling.

use std::hash::Hash;

use timely::order::Product;
use timely::dataflow::*;

use ::{Collection, ExchangeData};
use ::operators::*;
use ::lattice::Lattice;
use ::operators::iterate::Variable;

/// Returns the subset of `goals` that can reach each other in `edges`, with distance.
///
/// This method performs bidirectional search, from both ends of each goal in forward
/// and reverse direction, for the sources and targets respectively. Each search can
/// examine a fraction of the graph before meeting, and multiple searches can be managed
/// concurrently.
///
/// Goals that cannot reach from the source to the target are relatively expensive, as
/// the entire graph must be explored to confirm this. A graph connectivity pre-filter
/// could be good insurance here.
pub fn bidijkstra<G, N>(edges: &Collection<G, (N,N)>, goals: &Collection<G, (N,N)>) -> Collection<G, ((N,N), u32)>
where
    G: Scope,
    G::Timestamp: Lattice+Ord,
    N: ExchangeData+Hash,
{
    use operators::arrange::arrangement::ArrangeByKey;
    let forward = edges.arrange_by_key();
    let reverse = edges.map(|(x,y)| (y,x)).arrange_by_key();
    bidijkstra_arranged(&forward, &reverse, goals)
}

use crate::trace::TraceReader;
use crate::operators::arrange::Arranged;

/// Bi-directional Dijkstra search using arranged forward and reverse edge collections.
pub fn bidijkstra_arranged<G, N, Tr>(
    forward: &Arranged<G, Tr>,
    reverse: &Arranged<G, Tr>,
    goals: &Collection<G, (N,N)>
) -> Collection<G, ((N,N), u32)>
where
    G: Scope,
    G::Timestamp: Lattice+Ord,
    N: ExchangeData+Hash,
    Tr: TraceReader<Key=N, Val=N, Time=G::Timestamp, R=isize>+Clone+'static,
{
    forward
        .stream
        .scope().iterative::<u64,_,_>(|inner| {

            let forward_edges = forward.enter(inner);
            let reverse_edges = reverse.enter(inner);

        // Our plan is to start evolving distances from both sources and destinations.
        // The evolution from a source or destination should continue as long as there
        // is a corresponding destination or source that has not yet been reached.

        // forward and reverse (node, (root, dist))
        let forward = Variable::new_from(goals.map(|(x,_)| (x.clone(),(x.clone(),0))).enter(inner), Product::new(Default::default(), 1));
        let reverse = Variable::new_from(goals.map(|(_,y)| (y.clone(),(y.clone(),0))).enter(inner), Product::new(Default::default(), 1));

        forward.map(|_| ()).consolidate().inspect(|x| println!("forward: {:?}", x));
        reverse.map(|_| ()).consolidate().inspect(|x| println!("reverse: {:?}", x));

        let goals = goals.enter(inner);
        // let edges = edges.enter(inner);

        // Let's determine which (src, dst) pairs are ready to return.
        //
        //   done(src, dst) := forward(src, med), reverse(dst, med), goal(src, dst).
        //
        // This is a cyclic join, which should scare us a bunch.
        let reached =
        forward
            .join_map(&reverse, |_, (src,d1), (dst,d2)| ((src.clone(), dst.clone()), *d1 + *d2))
            .reduce(|_key, s, t| t.push((s[0].0.clone(), 1)))
            .semijoin(&goals);

        let active =
        reached
            .negate()
            .map(|(srcdst,_)| srcdst)
            .concat(&goals)
            .consolidate();

        // Let's expand out forward queries that are active.
        let forward_active = active.map(|(x,_y)| x).distinct();
        let forward_next =
        forward
            .map(|(med, (src, dist))| (src, (med, dist)))
            .semijoin(&forward_active)
            .map(|(src, (med, dist))| (med, (src, dist)))
            .join_core(&forward_edges, |_med, (src, dist), next| Some((next.clone(), (src.clone(), *dist+1))))
            .concat(&forward)
            .map(|(next, (src, dist))| ((next, src), dist))
            .reduce(|_key, s, t| t.push((s[0].0.clone(), 1)))
            .map(|((next, src), dist)| (next, (src, dist)));

        forward_next.map(|_| ()).consolidate().inspect(|x| println!("forward_next: {:?}", x));

        forward.set(&forward_next);

        // Let's expand out reverse queries that are active.
        let reverse_active = active.map(|(_x,y)| y).distinct();
        let reverse_next =
        reverse
            .map(|(med, (rev, dist))| (rev, (med, dist)))
            .semijoin(&reverse_active)
            .map(|(rev, (med, dist))| (med, (rev, dist)))
            .join_core(&reverse_edges, |_med, (rev, dist), next| Some((next.clone(), (rev.clone(), *dist+1))))
            .concat(&reverse)
            .map(|(next, (rev, dist))| ((next, rev), dist))
            .reduce(|_key, s, t| t.push((s[0].0.clone(), 1)))
            .map(|((next,rev), dist)| (next, (rev, dist)));

        reverse_next.map(|_| ()).consolidate().inspect(|x| println!("reverse_next: {:?}", x));

        reverse.set(&reverse_next);

        reached.leave()
    })
}
