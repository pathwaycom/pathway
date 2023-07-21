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

    timely::execute_from_args(std::env::args().skip(1), move |worker| {

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
            triangles(&Collection::new(edges.to_stream(scope))).inner.count().inspect(|x| println!("{:?}", x));
        });

    }).unwrap();
}

fn triangles<G: Scope>(edges: &Collection<G, Edge>) -> Collection<G, (Node, Node, Node)>
where G::Timestamp: Lattice+Hash+Ord {

    // only use forward-pointing edges.
    let edges = edges.filter(|&(src, dst)| src < dst);

    // arrange the edge relation three ways.
    let as_self = edges.arrange_by_self();
    let forward = edges.arrange_by_key();
    let reverse = edges.map_in_place(|x| ::std::mem::swap(&mut x.0, &mut x.1))
                       .arrange_by_key();

    // arrange the count of extensions from each source.
    let counts = edges.map(|(src, _dst)| src)
                      .arrange_by_self();

    // extract ((src, dst), idx) tuples with weights equal to the number of extensions.
    let cand_count1 = forward.join_core(&counts, |&src, &dst, &()| Some(((src, dst), 1)));
    let cand_count2 = reverse.join_core(&counts, |&dst, &src, &()| Some(((src, dst), 2)));

    // determine for each (src, dst) tuple which index would propose the fewest extensions.
    let winners = cand_count1.concat(&cand_count2)
                             .reduce(|_srcdst, counts, output| {
                                 if counts.len() == 2 {
                                     let mut min_cnt = isize::max_value();
                                     let mut min_idx = usize::max_value();
                                     for &(&idx, cnt) in counts.iter() {
                                         if min_cnt > cnt {
                                             min_idx = idx;
                                             min_cnt = cnt;
                                         }
                                     }
                                     output.push((min_idx, 1));
                                 }
                             });

    // select tuples with the first relation minimizing the proposals, join, then intersect.
    let winners1 = winners.flat_map(|((src, dst), index)| if index == 1 { Some((src, dst)) } else { None })
                          .join_core(&forward, |&src, &dst, &ext| Some(((dst, ext), src)))
                          .join_core(&as_self, |&(dst, ext), &src, &()| Some(((dst, ext), src)))
                          .map(|((dst, ext), src)| (src, dst, ext));

    // select tuples with the second relation minimizing the proposals, join, then intersect.
    let winners2 = winners.flat_map(|((src, dst), index)| if index == 2 { Some((dst, src)) } else { None })
                          .join_core(&forward, |&dst, &src, &ext| Some(((src, ext), dst)))
                          .join_core(&as_self, |&(src, ext), &dst, &()| Some(((src, ext), dst)))
                          .map(|((src, ext), dst)| (src, dst, ext));

    // collect and return results.
    winners1.concat(&winners2)
}
