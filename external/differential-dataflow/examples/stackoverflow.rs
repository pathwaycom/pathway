extern crate timely;
extern crate differential_dataflow;

use std::io::{BufRead, BufReader};
use std::fs::File;

use timely::dataflow::*;
use timely::dataflow::operators::probe::Handle;

use differential_dataflow::input::InputSession;
use differential_dataflow::Collection;
use differential_dataflow::operators::*;
use differential_dataflow::lattice::Lattice;

type Time = usize;
type Node = u32;
type Edge = (Node, Node);

fn main() {

    let mut args = std::env::args().skip(1);

    let filename = args.next().expect("must specify a filename");
    let root: Node = args.next().expect("must specify root node").parse().expect("root node must be an integer");
    let batch: usize = args.next().expect("must specify batching").parse().expect("batch must be an integer");
    let compression: Time = args.next().expect("must specify compression").parse().expect("compression must be an integer");
    let inspect: bool = args.next().expect("must specify inspect bit").parse().expect("inspect must be boolean");

    // define a new computational scope, in which to run BFS
    timely::execute_from_args(args, move |worker| {

        let timer = ::std::time::Instant::now();
        let index = worker.index();
        let peers = worker.peers();

        // define BFS dataflow; return handles to roots and edges inputs
        let mut roots = InputSession::new();
        let mut graph = InputSession::new();
        let mut probe = Handle::new();

        worker.dataflow(|scope| {

            let roots = roots.to_collection(scope);
            let graph = graph.to_collection(scope);

            bfs(&graph, &roots)
                .filter(move |_| inspect)
                .map(|(_,l)| l)
                .consolidate()
                .inspect(|x| println!("\t{:?}", x))
                .probe_with(&mut probe);
        });

        // Load the (fraction of the) data!
        let mut edges = Vec::new();
        let file = BufReader::new(File::open(filename.clone()).unwrap());
        for (count, readline) in file.lines().enumerate() {
            if count % peers == index {
                let line = readline.ok().expect("read error");
                if !line.starts_with('#') {
                    let mut elts = line[..].split_whitespace();
                    let src: Node = elts.next().expect("line missing src field").parse().expect("malformed src");
                    let dst: Node = elts.next().expect("line missing dst field").parse().expect("malformed dst");
                    let sec: Time = elts.next().expect("line missing sec field").parse().expect("malformed sec");
                    edges.push((compression * ((sec / compression) + 1), src, dst));
                }
            }
        }

        // Could leave this open, continually update/advance.
        roots.insert(root);
        roots.close();

        println!("{:?}\tData loaded.", timer.elapsed());

        // Sort edges by second.
        edges.sort();
        println!("{:?}\tData sorted.", timer.elapsed());

        // Repeatedly introduce `batch` many records, then await their completion.
        let mut slice = &edges[..];
        while !slice.is_empty() {

            // Catch up to this point.
            graph.advance_to(slice[0].0);
            graph.flush();
            while probe.less_than(graph.time()) {
                worker.step();
            }
            println!("{:?}\tTime {:?} reached", timer.elapsed(), graph.time());

            let limit = std::cmp::min(slice.len(), batch);
            for &(time, src, dst) in slice[..limit].iter() {
                graph.advance_to(time);
                graph.insert((src, dst));
            }


            slice = &slice[limit..];
        }

        graph.close();
        while worker.step() { }

        println!("{:?}\tComputation complete.", timer.elapsed());

    }).unwrap();
}

// returns pairs (n, s) indicating node n can be reached from a root in s steps.
fn bfs<G: Scope>(edges: &Collection<G, Edge>, roots: &Collection<G, Node>) -> Collection<G, (Node, u32)>
where G::Timestamp: Lattice+Ord {

    // initialize roots as reaching themselves at distance 0
    let nodes = roots.map(|x| (x, 0));

    // repeatedly update minimal distances each node can be reached from each root
    nodes.iterate(|inner| {

        let edges = edges.enter(&inner.scope());
        let nodes = nodes.enter(&inner.scope());

        inner.join_map(&edges, |_k,l,d| (*d, l+1))
             .concat(&nodes)
             .reduce(|_, s, t| t.push((*s[0].0, 1)))
     })
}