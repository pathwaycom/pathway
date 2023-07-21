extern crate timely;
extern crate graph_map;
extern crate differential_dataflow;

extern crate dogsdogsdogs;

use timely::dataflow::operators::{ToStream, Partition, Accumulate, Inspect, Probe};
use timely::dataflow::operators::probe::Handle;
use differential_dataflow::{Collection, AsCollection};
use differential_dataflow::input::Input;
use graph_map::GraphMMap;

use dogsdogsdogs::{CollectionIndex, PrefixExtender};

fn main() {

    // snag a filename to use for the input graph.
    let filename = std::env::args().nth(1).unwrap();
    let batching = std::env::args().nth(2).unwrap().parse::<usize>().unwrap();

    timely::execute_from_args(std::env::args().skip(2), move |worker| {

        // let timer = std::time::Instant::now();

        let peers = worker.peers();
        let index = worker.index();

        // // What you might do if you used GraphMMap:
        let graph = GraphMMap::new(&filename);
        let nodes = graph.nodes();
        let edges = (0..nodes).filter(move |node| node % peers == index)
                              .flat_map(|node| graph.edges(node).iter().cloned().map(move |dst| ((node as u32, dst))))
                              .map(|(src, dst)| ((src, dst), Default::default(), 1))
                              .collect::<Vec<_>>();

        let edges2 = edges.clone();

        println!("loaded {} nodes, {} edges", nodes, edges.len());

        let index = worker.dataflow::<usize,_,_>(|scope| {
            CollectionIndex::index(&Collection::new(edges.to_stream(scope)))
        });

        let mut index_xz = index.extend_using(|&(ref x, ref _y)| *x);
        let mut index_yz = index.extend_using(|&(ref _x, ref y)| *y);

        let mut probe = Handle::new();

        let mut edges = worker.dataflow::<usize,_,_>(|scope| {

            let (edges_input, edges) = scope.new_collection();

            // determine stream of (prefix, count, index) indicating relation with fewest extensions.
            let counts  = edges.map(|p| (p, usize::max_value(), usize::max_value()));
            let counts0 = index_xz.count(&counts,  0);
            let counts1 = index_yz.count(&counts0, 1);

            // partition by index.
            let parts = counts1.inner.partition(2, |((p, _c, i),t,d)| (i as u64,(p,t,d)));

            // propose extensions using relation based on index.
            let propose0 = index_xz.propose(&parts[0].as_collection());
            let propose1 = index_yz.propose(&parts[1].as_collection());

            // validate proposals with the other index.
            let validate0 = index_yz.validate(&propose0);
            let validate1 = index_xz.validate(&propose1);

            validate0
                .concat(&validate1)
                .inner
                .count()
                .inspect(move |x| println!("{:?}", x))
                // .inspect(move |x| println!("{:?}:\t{:?}", timer.elapsed(), x))
                .probe_with(&mut probe);

            edges_input
        });

        let mut index = 0;
        while index < edges2.len() {
            let limit = std::cmp::min(batching, edges2.len() - index);
            for offset in 0 .. limit {
                edges.insert(edges2[index + offset].0);
                edges.advance_to(index + offset + 1);
            }
            index += limit;
            edges.flush();
            while probe.less_than(edges.time()) {
                worker.step();
            }
        }

    }).unwrap();
}