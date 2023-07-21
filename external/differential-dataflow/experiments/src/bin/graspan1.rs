extern crate timely;
extern crate graph_map;
extern crate differential_dataflow;

use std::io::{BufRead, BufReader};
use std::fs::File;

use timely::dataflow::Scope;
use timely::order::Product;

use differential_dataflow::difference::Present;
use differential_dataflow::input::Input;
use differential_dataflow::trace::implementations::ord::OrdValSpine;
use differential_dataflow::operators::*;
use differential_dataflow::operators::arrange::Arrange;
use differential_dataflow::operators::iterate::SemigroupVariable;

type Node = u32;
type Time = ();
type Iter = u32;
type Offs = u32;

fn main() {

    timely::execute_from_args(std::env::args(), move |worker| {

        let timer = ::std::time::Instant::now();

        let peers = worker.peers();
        let index = worker.index();

        let (mut nodes, mut edges) = worker.dataflow::<Time,_,_>(|scope| {

            // let timer = timer.clone();

            let (n_handle, nodes) = scope.new_collection();
            let (e_handle, edges) = scope.new_collection();

            let edges = edges.arrange::<OrdValSpine<_,_,_,_,Offs>>();

            // a N c  <-  a N b && b E c
            // N(a,c) <-  N(a,b), E(b, c)
            let reached =
            nodes.scope().iterative::<Iter,_,_>(|inner| {

                let nodes = nodes.enter(inner).map(|(a,b)| (b,a));
                let edges = edges.enter(inner);

                let labels = SemigroupVariable::new(inner, Product::new(Default::default(), 1));

                let next =
                labels.join_core(&edges, |_b, a, c| Some((*c, *a)))
                      .concat(&nodes)
                      .arrange::<OrdValSpine<_,_,_,_,Offs>>()
                    //   .distinct_total_core::<Diff>();
                      .threshold_semigroup(|_,_,x| if x.is_none() { Some(Present) } else { None });

                labels.set(&next);
                next.leave()
            });

            reached
                .map(|_| ())
                .consolidate()
                .inspect(|x| println!("{:?}", x))
                ;

            (n_handle, e_handle)
        });

        if index == 0 { println!("{:?}:\tDataflow assembled", timer.elapsed()); }

        // snag a filename to use for the input graph.
        let filename = std::env::args().nth(1).unwrap();
        let file = BufReader::new(File::open(filename).unwrap());
        for readline in file.lines() {
            let line = readline.ok().expect("read error");
            if !line.starts_with('#') && line.len() > 0 {
                let mut elts = line[..].split_whitespace();
                let src: Node = elts.next().unwrap().parse().ok().expect("malformed src");
                if (src as usize) % peers == index {
                    let dst: Node = elts.next().unwrap().parse().ok().expect("malformed dst");
                    let typ: &str = elts.next().unwrap();
                    match typ {
                        "n" => { nodes.update((src, dst), Present); },
                        "e" => { edges.update((src, dst), Present); },
                        unk => { panic!("unknown type: {}", unk)},
                    }
                }
            }
        }

        if index == 0 { println!("{:?}:\tData loaded", timer.elapsed()); }

        nodes.close();
        edges.close();
        while worker.step() { }

        if index == 0 { println!("{:?}:\tComputation complete", timer.elapsed()); }

    }).unwrap();
}
