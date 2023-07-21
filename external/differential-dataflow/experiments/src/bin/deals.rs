extern crate rand;
extern crate timely;
extern crate differential_dataflow;
extern crate core_affinity;

use std::time::Instant;

use timely::dataflow::*;

use differential_dataflow::input::Input;
use differential_dataflow::Collection;
use differential_dataflow::operators::*;

use differential_dataflow::trace::implementations::ord::{OrdValSpine, OrdKeySpine};
use differential_dataflow::operators::arrange::TraceAgent;
use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::operators::arrange::Arrange;
use differential_dataflow::operators::iterate::SemigroupVariable;
use differential_dataflow::difference::Present;

type EdgeArranged<G, K, V, R> = Arranged<G, TraceAgent<OrdValSpine<K, V, <G as ScopeParent>::Timestamp, R, Offs>>>;

type Node = u32;
type Edge = (Node, Node);
type Iter = u32;
type Offs = u32;

fn main() {

    // snag a filename to use for the input graph.
    let filename = std::env::args().nth(1).unwrap();
    let program = std::env::args().nth(2).unwrap();

    timely::execute_from_args(std::env::args().skip(1), move |worker| {

        let peers = worker.peers();
        let index = worker.index();
        let timer = Instant::now();

        let core_ids = core_affinity::get_core_ids().unwrap();
        core_affinity::set_for_current(core_ids[index % core_ids.len()]);

        let inspect = true;

        let mut input = worker.dataflow::<(),_,_>(|scope| {

            let (input, graph) = scope.new_collection();

            // each edge should exist in both directions.
            let graph = graph.arrange::<OrdValSpine<_,_,_,_,Offs>>();

            match program.as_str() {
                "tc"    => tc(&graph).filter(move |_| inspect).map(|_| ()).consolidate().inspect(|x| println!("tc count: {:?}", x)).probe(),
                "sg"    => sg(&graph).filter(move |_| inspect).map(|_| ()).consolidate().inspect(|x| println!("sg count: {:?}", x)).probe(),
                _       => panic!("must specify one of 'tc', 'sg'.")
            };

            input
        });

        let mut nodes = 0;

        use std::io::{BufReader, BufRead};
        use std::fs::File;

        let file = BufReader::new(File::open(filename.clone()).unwrap());
        for (count, readline) in file.lines().enumerate() {
            let line = readline.ok().expect("read error");
            if count % peers == index && !line.starts_with('#') {
                let mut elts = line[..].split_whitespace();
                let src: u32 = elts.next().unwrap().parse().ok().expect("malformed src");
                let dst: u32 = elts.next().unwrap().parse().ok().expect("malformed dst");
                if nodes < src { nodes = src; }
                if nodes < dst { nodes = dst; }
                input.update((src, dst), Present);
            }
        }

        if index == 0 { println!("{:?}\tData ingested", timer.elapsed()); }

        input.close();
        while worker.step() { }

        if index == 0 { println!("{:?}\tComputation complete", timer.elapsed()); }

    }).unwrap();
}

use timely::order::Product;

// returns pairs (n, s) indicating node n can be reached from a root in s steps.
fn tc<G: Scope<Timestamp=()>>(edges: &EdgeArranged<G, Node, Node, Present>) -> Collection<G, Edge, Present> {

    // repeatedly update minimal distances each node can be reached from each root
    edges.stream.scope().iterative::<Iter,_,_>(|scope| {

            let inner = SemigroupVariable::new(scope, Product::new(Default::default(), 1));
            let edges = edges.enter(&inner.scope());

            let result =
            inner
                .map(|(x,y)| (y,x))
                .arrange::<OrdValSpine<_,_,_,_,Offs>>()
                .join_core(&edges, |_y,&x,&z| Some((x, z)))
                .concat(&edges.as_collection(|&k,&v| (k,v)))
                .arrange::<OrdKeySpine<_,_,_,Offs>>()
                .threshold_semigroup(|_,_,x| if x.is_none() { Some(Present) } else { None })
                ;

            inner.set(&result);
            result.leave()
        }
    )
}

// returns pairs (n, s) indicating node n can be reached from a root in s steps.
fn sg<G: Scope<Timestamp=()>>(edges: &EdgeArranged<G, Node, Node, Present>) -> Collection<G, Edge, Present> {

    let peers = edges.join_core(&edges, |_,&x,&y| Some((x,y))).filter(|&(x,y)| x != y);

    // repeatedly update minimal distances each node can be reached from each root
    peers.scope().iterative::<Iter,_,_>(|scope| {

            let inner = SemigroupVariable::new(scope, Product::new(Default::default(), 1));
            let edges = edges.enter(&inner.scope());
            let peers = peers.enter(&inner.scope());

            let result =
            inner
                .arrange::<OrdValSpine<_,_,_,_,Offs>>()
                .join_core(&edges, |_,&x,&z| Some((x, z)))
                .arrange::<OrdValSpine<_,_,_,_,Offs>>()
                .join_core(&edges, |_,&x,&z| Some((x, z)))
                .concat(&peers)
                .arrange::<OrdKeySpine<_,_,_,Offs>>()
                .threshold_semigroup(|_,_,x| if x.is_none() { Some(Present) } else { None })
                ;

            inner.set(&result);
            result.leave()
        }
    )
}
