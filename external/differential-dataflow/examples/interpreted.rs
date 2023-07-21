extern crate timely;
extern crate graph_map;
extern crate differential_dataflow;

use std::hash::Hash;
use timely::dataflow::*;
use timely::dataflow::operators::*;

use differential_dataflow::Collection;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::*;

use differential_dataflow::operators::arrange::ArrangeBySelf;
use differential_dataflow::operators::arrange::ArrangeByKey;

use graph_map::GraphMMap;

type Node = u32;
type Edge = (Node, Node);

fn main() {

    // snag a filename to use for the input graph.
    let filename = std::env::args().nth(1).unwrap();

    timely::execute_from_args(std::env::args().skip(2), move |worker| {

        let peers = worker.peers();
        let index = worker.index();

        // // What you might do if you used GraphMMap:
        let graph = GraphMMap::new(&filename);
        let nodes = graph.nodes();
        let edges = (0..nodes).filter(move |node| node % peers == index)
                              .flat_map(|node| graph.edges(node).iter().cloned().map(move |dst| ((node as u32, dst))))
                              .map(|(src, dst)| ((src, dst), Default::default(), 1))
                              .collect::<Vec<_>>();

        println!("loaded {} nodes, {} edges", nodes, edges.len());

        worker.dataflow::<(),_,_>(|scope| {
            interpret(&Collection::new(edges.to_stream(scope)), &[(0,2), (1,2)]);
        });

    }).unwrap();
}

fn interpret<G: Scope>(edges: &Collection<G, Edge>, relations: &[(usize, usize)]) -> Collection<G, Vec<Node>>
where G::Timestamp: Lattice+Hash+Ord {

    // arrange the edge relation three ways.
    let as_self = edges.arrange_by_self();
    let forward = edges.arrange_by_key();
    let reverse = edges.map_in_place(|x| ::std::mem::swap(&mut x.0, &mut x.1))
                       .arrange_by_key();

    let mut field_present = ::std::collections::HashSet::new();

    let mut results = edges.map(|(x,y)| vec![x, y]);

    field_present.insert(0);
    field_present.insert(1);

    for &(src, dst) in relations.iter() {

        let src_present = field_present.contains(&src);
        let dst_present = field_present.contains(&dst);

        results = match (src_present, dst_present) {
            (true, true) => {
                // Both variables are bound, so this is a semijoin.
                results
                    .map(move |vec| ((vec[src], vec[dst]), vec))
                    .join_core(&as_self, |_key, vec, &()| Some(vec.clone()))
            }
            (true, false) => {
                // Only `src` is bound, so we must use `forward` to propose `dst`.
                field_present.insert(dst);
                results
                    .map(move |vec| (vec[src], vec))
                    .join_core(&forward, move |_src_val, vec, &dst_val| {
                        let mut temp = vec.clone();
                        while temp.len() <= dst { temp.push(0); }
                        temp[dst] = dst_val;
                        Some(temp)
                    })
            }
            (false, true) => {
                // Only `dst` is bound, so we must use `reverse` to propose `src`.
                field_present.insert(src);
                results
                    .map(move |vec| (vec[dst], vec))
                    .join_core(&reverse, move |_dst_val, vec, &src_val| {
                        let mut temp = vec.clone();
                        while temp.len() <= src { temp.push(0); }
                        temp[src] = src_val;
                        Some(temp)
                    })
            }
            (false, false) => {
                // Neither variable is bound, which we treat as user error.
                panic!("error: joining with unbound variables");
            }
        };
    }

    results
}