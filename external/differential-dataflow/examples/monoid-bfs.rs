extern crate rand;
extern crate timely;
extern crate differential_dataflow;

#[macro_use]
extern crate abomonation_derive;
extern crate abomonation;
#[macro_use]
extern crate serde_derive;
extern crate serde;


use rand::{Rng, SeedableRng, StdRng};

use timely::dataflow::*;
use timely::dataflow::operators::probe::Handle;

use differential_dataflow::input::Input;
use differential_dataflow::Collection;
use differential_dataflow::operators::*;
use differential_dataflow::lattice::Lattice;

type Node = u32;
type Edge = (Node, Node);

#[derive(Abomonation, Copy, Ord, PartialOrd, Eq, PartialEq, Debug, Clone, Serialize, Deserialize, Hash)]
pub struct MinSum {
    value: u32,
}

use differential_dataflow::difference::{Semigroup, Multiply};

impl Semigroup for MinSum {
    fn plus_equals(&mut self, rhs: &Self) {
        self.value = std::cmp::min(self.value, rhs.value);
    }
    fn is_zero(&self) -> bool { false }
}

impl Multiply<Self> for MinSum {
    type Output = Self;
    fn multiply(self, rhs: &Self) -> Self {
        MinSum { value: self.value + rhs.value }
    }
}

fn main() {

    let nodes: u32 = std::env::args().nth(1).unwrap().parse().unwrap();
    let edges: u32 = std::env::args().nth(2).unwrap().parse().unwrap();
    let weight: u32 = std::env::args().nth(3).unwrap().parse().unwrap();
    let batch: u32 = std::env::args().nth(4).unwrap().parse().unwrap();
    let rounds: u32 = std::env::args().nth(5).unwrap().parse().unwrap();
    let inspect: bool = std::env::args().nth(6).unwrap() == "inspect";

    // define a new computational scope, in which to run BFS
    timely::execute_from_args(std::env::args(), move |worker| {

        let timer = ::std::time::Instant::now();

        // define BFS dataflow; return handles to roots and edges inputs
        let mut probe = Handle::new();
        let (mut roots, mut graph) = worker.dataflow(|scope| {

            let (root_input, roots) = scope.new_collection();
            let (edge_input, graph) = scope.new_collection();

            let mut result = bfs(&graph, &roots);

            if !inspect {
                result = result.filter(|_| false);
            }

            result.count()
                  .map(|(_,l)| l)
                  .consolidate()
                  .inspect(|x| println!("\t{:?}", x))
                  .probe_with(&mut probe);

            (root_input, edge_input)
        });

        let seed: &[_] = &[1, 2, 3, 4];
        let mut rng1: StdRng = SeedableRng::from_seed(seed);    // rng for edge additions

        roots.update_at(0, Default::default(), MinSum { value: 0 });
        roots.close();

        println!("performing BFS on {} nodes, {} edges:", nodes, edges);

        if worker.index() == 0 {
            for _ in 0 .. edges {
                graph.update_at(
                    (rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)),
                    Default::default(),
                    MinSum { value: rng1.gen_range(0, weight) },
                );
            }
        }

        println!("{:?}\tloaded", timer.elapsed());

        graph.advance_to(1);
        graph.flush();
        worker.step_while(|| probe.less_than(graph.time()));

        println!("{:?}\tstable", timer.elapsed());

        for round in 0 .. rounds {
            for element in 0 .. batch {
                if worker.index() == 0 {
                    graph.update_at(
                        (rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)),
                        1 + round * batch + element,
                        MinSum { value: rng1.gen_range(0, weight) },
                    );
                }
                graph.advance_to(2 + round * batch + element);
            }
            graph.flush();

            let timer2 = ::std::time::Instant::now();
            worker.step_while(|| probe.less_than(&graph.time()));

            if worker.index() == 0 {
                let elapsed = timer2.elapsed();
                println!("{:?}\t{:?}:\t{}", timer.elapsed(), round, elapsed.as_secs() * 1000000000 + (elapsed.subsec_nanos() as u64));
            }
        }
        println!("finished; elapsed: {:?}", timer.elapsed());
    }).unwrap();
}

// returns pairs (n, s) indicating node n can be reached from a root in s steps.
fn bfs<G: Scope>(edges: &Collection<G, Edge, MinSum>, roots: &Collection<G, Node, MinSum>) -> Collection<G, Node, MinSum>
where G::Timestamp: Lattice+Ord {

    // repeatedly update minimal distances each node can be reached from each root
    roots.scope().iterative::<u32,_,_>(|scope| {

        use differential_dataflow::operators::iterate::SemigroupVariable;
        use differential_dataflow::operators::reduce::ReduceCore;
        use differential_dataflow::trace::implementations::ord::OrdKeySpine as DefaultKeyTrace;


        use timely::order::Product;
        let variable = SemigroupVariable::new(scope, Product::new(Default::default(), 1));

        let edges = edges.enter(scope);
        let roots = roots.enter(scope);

        let result =
        variable
            .map(|n| (n,()))
            .join_map(&edges, |_k,&(),d| *d)
            .concat(&roots)
            .map(|x| (x,()))
            .reduce_core::<_,DefaultKeyTrace<_,_,_>>("Reduce", |_key, input, output, updates| {
                if output.is_empty() || input[0].1 < output[0].1 {
                    updates.push(((), input[0].1));
                }
            })
            .as_collection(|k,()| *k);

        variable.set(&result);
        result.leave()
     })
}
