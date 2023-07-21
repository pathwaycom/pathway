extern crate rand;
extern crate timely;
extern crate differential_dataflow;

use std::rc::Rc;

use rand::{Rng, SeedableRng, StdRng};

use timely::dataflow::*;

use differential_dataflow::input::Input;
use differential_dataflow::Collection;
use differential_dataflow::operators::*;
use differential_dataflow::trace::Trace;
use differential_dataflow::operators::arrange::ArrangeByKey;
use differential_dataflow::operators::arrange::ArrangeBySelf;

use differential_dataflow::trace::implementations::spine_fueled::Spine;

type Node = usize;

use differential_dataflow::trace::implementations::ord::OrdValBatch;
// use differential_dataflow::trace::implementations::ord::OrdValSpine;

// type GraphTrace<N> = Spine<usize, N, (), isize, Rc<GraphBatch<N>>>;
type GraphTrace = Spine<Node, Node, (), isize, Rc<OrdValBatch<Node, Node, (), isize>>>;

fn main() {

    let nodes: usize = std::env::args().nth(1).unwrap().parse().unwrap();
    let edges: usize = std::env::args().nth(2).unwrap().parse().unwrap();

    // Our setting involves four read query types, and two updatable base relations.
    //
    //  Q1: Point lookup: reads "state" associated with a node.
    //  Q2: One-hop lookup: reads "state" associated with neighbors of a node.
    //  Q3: Two-hop lookup: reads "state" associated with n-of-n's of a node.
    //  Q4: Shortest path: reports hop count between two query nodes.
    //
    //  R1: "State": a pair of (node, T) for some type T that I don't currently know.
    //  R2: "Graph": pairs (node, node) indicating linkage between the two nodes.

    timely::execute_from_args(std::env::args().skip(3), move |worker| {

        let index = worker.index();
        let peers = worker.peers();
        let timer = ::std::time::Instant::now();

        let (mut graph, mut trace) = worker.dataflow(|scope| {
            let (graph_input, graph) = scope.new_collection();
            let graph_indexed = graph.arrange_by_key();
            // let graph_indexed = graph.arrange_by_key();
            (graph_input, graph_indexed.trace)
        });

        let seed: &[_] = &[1, 2, 3, index];
        let mut rng1: StdRng = SeedableRng::from_seed(seed);    // rng for edge additions
        // let mut rng2: StdRng = SeedableRng::from_seed(seed);    // rng for edge deletions

        if index == 0 { println!("performing workload on random graph with {} nodes, {} edges:", nodes, edges); }

        let worker_edges = edges/peers + if index < (edges % peers) { 1 } else { 0 };
        for _ in 0 .. worker_edges {
            graph.insert((rng1.gen_range(0, nodes) as Node, rng1.gen_range(0, nodes) as Node));
        }
        graph.close();
        while worker.step() { }
        if index == 0 { println!("{:?}\tgraph loaded", timer.elapsed()); }

        // Phase 2: Reachability.
        let mut roots = worker.dataflow(|scope| {
            let (roots_input, roots) = scope.new_collection();
            reach(&mut trace, roots);
            roots_input
        });

        if index == 0 { roots.insert(0); }
        roots.close();
        while worker.step() { }
        if index == 0 { println!("{:?}\treach complete", timer.elapsed()); }

        // Phase 3: Breadth-first distance labeling.
        let mut roots = worker.dataflow(|scope| {
            let (roots_input, roots) = scope.new_collection();
            bfs(&mut trace, roots);
            roots_input
        });

        if index == 0 { roots.insert(0); }
        roots.close();
        while worker.step() { }
        if index == 0 { println!("{:?}\tbfs complete", timer.elapsed()); }


    }).unwrap();
}

// use differential_dataflow::trace::implementations::ord::OrdValSpine;
use differential_dataflow::operators::arrange::TraceAgent;

type TraceHandle = TraceAgent<GraphTrace>;

fn reach<G: Scope<Timestamp = ()>> (
    graph: &mut TraceHandle,
    roots: Collection<G, Node>
) -> Collection<G, Node> {

    let graph = graph.import(&roots.scope());

    roots.iterate(|inner| {

        let graph = graph.enter(&inner.scope());
        let roots = roots.enter(&inner.scope());

        // let reach = inner.concat(&roots).distinct_total().arrange_by_self();
        // graph.join_core(&reach, |_src,&dst,&()| Some(dst))

        graph.join_core(&inner.arrange_by_self(), |_src,&dst,&()| Some(dst))
             .concat(&roots)
             .distinct_total()
    })
}


fn bfs<G: Scope<Timestamp = ()>> (
    graph: &mut TraceHandle,
    roots: Collection<G, Node>
) -> Collection<G, (Node, u32)> {

    let graph = graph.import(&roots.scope());
    let roots = roots.map(|r| (r,0));

    roots.iterate(|inner| {

        let graph = graph.enter(&inner.scope());
        let roots = roots.enter(&inner.scope());

        graph.join_map(&inner, |_src,&dest,&dist| (dest, dist+1))
             .concat(&roots)
             .reduce(|_key, input, output| output.push((*input[0].0,1)))
    })
}

// fn connected_components<G: Scope<Timestamp = ()>>(
//     graph: &mut TraceHandle<Node>
// ) -> Collection<G, (Node, Node)> {

//     // each edge (x,y) means that we need at least a label for the min of x and y.
//     let nodes =
//     graph
//         .as_collection(|&k,&v| {
//             let min = std::cmp::min(k,v);
//             (min, min)
//         })
//         .consolidate();

//     // each edge should exist in both directions.
//     let edges = edges.map_in_place(|x| mem::swap(&mut x.0, &mut x.1))
//                      .concat(&edges);

//     // don't actually use these labels, just grab the type
//     nodes.filter(|_| false)
//          .iterate(|inner| {
//              let edges = edges.enter(&inner.scope());
//              let nodes = nodes.enter_at(&inner.scope(), |r| 256 * (64 - r.1.leading_zeros() as u64));

//             inner.join_map(&edges, |_k,l,d| (*d,*l))
//                  .concat(&nodes)
//                  .group(|_, s, t| { t.push((*s[0].0, 1)); } )
//          })
// }
