extern crate timely;
extern crate graph_map;
extern crate differential_dataflow;

use timely::order::Product;
use timely::dataflow::{*, operators::Filter};

use differential_dataflow::Collection;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::{*, iterate::Variable};
use differential_dataflow::input::InputSession;
use differential_dataflow::AsCollection;

use graph_map::GraphMMap;

type Node = u32;
type Edge = (Node, Node);
type Time = u32;
type Iter = u32;
type Diff = isize;

fn main() {

    // snag a filename to use for the input graph.
    let filename = std::env::args().nth(1).unwrap();
    let iterations: Iter = std::env::args().nth(2).unwrap().parse().unwrap();
    let inspect = std::env::args().nth(3) == Some("inspect".to_string());

    timely::execute_from_args(std::env::args().skip(2), move |worker| {

        let peers = worker.peers();
        let index = worker.index();
        let timer = worker.timer();

        let mut input = InputSession::new();
        let mut probe = ProbeHandle::new();

        worker.dataflow::<Time,_,_>(|scope| {
            let edges = input.to_collection(scope);
            pagerank(iterations, &edges)
                .filter(move |_| inspect)
                .consolidate()
                .inspect(|x| println!("{:?}", x))
                .probe_with(&mut probe);
        });

        // // What you might do if you used GraphMMap:
        let graph = GraphMMap::new(&filename);
        let mut node = index;
        while node < graph.nodes() {
            for &edge in graph.edges(node) {
                input.update((node as Node, edge as Node), 1);
            }
            node += peers;
        }

        input.advance_to(1);
        input.flush();
        while probe.less_than(input.time()) {
            worker.step();
        }

        println!("{:?}\tinitial compute complete", timer.elapsed());

        for node in 1 .. graph.nodes() {
            if node % peers == index {
                if !graph.edges(node).is_empty() {
                    input.update((node as Node, graph.edges(node)[0] as Node), -1);
                }
            }
            input.advance_to((node + 1) as Time);
            input.flush();
            while probe.less_than(input.time()) {
                worker.step();
            }
            println!("{:?}\tround {} complete", timer.elapsed(), node);
        }

    }).unwrap();
}

// Returns a weighted collection in which the weight of each node is proportional
// to its PageRank in the input graph `edges`.
fn pagerank<G>(iters: Iter, edges: &Collection<G, Edge, Diff>) -> Collection<G, Node, Diff>
where
    G: Scope,
    G::Timestamp: Lattice,
{
    // initialize many surfers at each node.
    let nodes =
    edges.flat_map(|(x,y)| Some(x).into_iter().chain(Some(y)))
         .distinct();

    // snag out-degrees for each node.
    let degrs = edges.map(|(src,_dst)| src)
                     .count();

    edges.scope().iterative::<Iter,_,_>(|inner| {

        // Bring various collections into the scope.
        let edges = edges.enter(inner);
        let nodes = nodes.enter(inner);
        let degrs = degrs.enter(inner);

        // Initial and reset numbers of surfers at each node.
        let inits = nodes.explode(|node| Some((node, 6_000_000)));
        let reset = nodes.explode(|node| Some((node, 1_000_000)));

        // Define a recursive variable to track surfers.
        // We start from `inits` and cycle only `iters`.
        let ranks = Variable::new_from(inits, Product::new(Default::default(), 1));

        // Match each surfer with the degree, scale numbers down.
        let to_push =
        degrs.semijoin(&ranks)
             .threshold(|(_node, degr), rank| (5 * rank) / (6 * degr))
             .map(|(node, _degr)| node);

        // Propagate surfers along links, blend in reset surfers.
        let mut pushed =
        edges.semijoin(&to_push)
             .map(|(_node, dest)| dest)
             .concat(&reset)
             .consolidate();

        if iters > 0 {
            pushed =
            pushed
             .inner
             .filter(move |(_d,t,_r)| t.inner < iters)
             .as_collection();
        }

        // Bind the recursive variable, return its limit.
        ranks.set(&pushed);
        pushed.leave()
    })
}
