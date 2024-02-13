extern crate rand;
extern crate timely;
extern crate differential_dataflow;

use rand::{Rng, SeedableRng, StdRng};

use timely::dataflow::operators::*;
use timely::order::Product;
use timely::scheduling::Scheduler;

use differential_dataflow::input::Input;
use differential_dataflow::AsCollection;
use differential_dataflow::operators::arrange::ArrangeByKey;
use differential_dataflow::operators::reduce::Reduce;
use differential_dataflow::operators::join::JoinCore;
use differential_dataflow::operators::Iterate;

fn main() {

    let nodes: u32 = std::env::args().nth(1).unwrap().parse().unwrap();
    let edges: usize = std::env::args().nth(2).unwrap().parse().unwrap();
    let batch: usize = std::env::args().nth(3).unwrap().parse().unwrap();
    let pre: usize = std::env::args().nth(4).unwrap().parse().unwrap();
    let inspect: bool = std::env::args().nth(5).unwrap() == "inspect";


    // define a new timely dataflow computation.
    timely::execute_from_args(std::env::args().skip(6), move |worker| {

        let timer = ::std::time::Instant::now();

        let index = worker.index();
        let peers = worker.peers();

        let mut probe = timely::dataflow::operators::probe::Handle::new();

        // create a dataflow managing an ever-changing edge collection.
        let mut graph = worker.dataflow::<Product<(),usize>,_,_>(|scope| {

            // create a source operator which will produce random edges and delete them.
            timely::dataflow::operators::generic::source(scope, "RandomGraph", |mut capability, info| {

                let activator = scope.activator_for(&info.address[..]);

                let seed: &[_] = &[1, 2, 3, index];
                let mut rng1: StdRng = SeedableRng::from_seed(seed);    // rng for edge additions
                let mut rng2: StdRng = SeedableRng::from_seed(seed);    // rng for edge deletions

                let mut additions = 0;
                let mut deletions = 0;

                let handle = probe.clone();

                move |output| {

                    // do nothing if the probe is not caught up to us
                    if !handle.less_than(capability.time()) {

                        let mut time = capability.time().clone();
                        // println!("{:?}\tintroducing edges for batch starting {:?}", timer.elapsed(), time);

                        {   // scope to allow session to drop, un-borrow.
                            let mut session = output.session(&capability);

                            // we want to send at times.inner + (0 .. batch).
                            for _ in 0 .. batch {

                                while additions < time.inner + edges {
                                    if additions % peers == index {
                                        let src = rng1.gen_range(0, nodes);
                                        let dst = rng1.gen_range(0, nodes);
                                        session.give(((src, dst), time, 1));
                                    }
                                    additions += 1;
                                }
                                while deletions < time.inner {
                                    if deletions % peers == index {
                                        let src = rng2.gen_range(0, nodes);
                                        let dst = rng2.gen_range(0, nodes);
                                        session.give(((src, dst), time, -1));
                                    }
                                    deletions += 1;
                                }

                                time.inner += 1;
                            }
                        }

                        // println!("downgrading {:?} to {:?}", capability, time);
                        capability.downgrade(&time);
                    }
                    activator.activate();
                }
            })
            .probe_with(&mut probe)
            .as_collection()
            .arrange_by_key()
            // .arrange::<OrdValSpineAbom>()
            .trace
        });

        println!("{:?}:\tloading edges", timer.elapsed());

        for _ in 0 .. pre {
            worker.step();
        }

        println!("{:?}\tedges loaded; building query dataflows", timer.elapsed());

        let mut roots = worker.dataflow(|scope| {

            let edges = graph.import(scope);
            let (input, roots) = scope.new_collection();
            let roots = roots.map(|x| (x, 0));

            // repeatedly update minimal distances each node can be reached from each root
            roots.iterate(|dists| {

                let edges = edges.enter(&dists.scope());
                let roots = roots.enter(&dists.scope());

                dists.arrange_by_key()
                     .join_core(&edges, |_k,l,d| Some((*d, l+1)))
                     .concat(&roots)
                     .reduce(|_, s, t| t.push((*s[0].0, 1)))
            })
            .map(|(_node, dist)| dist)
            .consolidate()
            .inspect(|x| println!("distance update: {:?}", x))
            .probe_with(&mut probe);

            input
        });

        let mut query = worker.dataflow(|scope| {

            let edges = graph.import(scope);
            let (input, query) = scope.new_collection();

            query.map(|x| (x, x))
                 .join_core(&edges, |_n, &q, &d| Some((d, q)))
                 .join_core(&edges, |_n, &q, &d| Some((d, q)))
                 .join_core(&edges, |_n, &q, &d| Some((d, q)))
                 .filter(move |_| inspect)
                 .map(|x| x.1)
                 .consolidate()
                 .inspect(|x| println!("{:?}", x))
                 .probe_with(&mut probe);

            input
        });

        println!("{:?}\tquery dataflows built; querying", timer.elapsed());


        // the trace will not compact unless we release capabilities.
        // we drop rather than continually downgrade them as we run.
        drop(graph);

        if batch > 0 {
            for round in 0 .. {

                let mut time = query.time().clone();

                // roots.insert(round % nodes);
                if index == 0 {
                    query.insert(round % nodes);
                }

                time.inner += batch;

                roots.advance_to(time); roots.flush();
                query.advance_to(time); query.flush();

                if index == 0 {
                    query.remove(round % nodes);
                }

                worker.step_while(|| probe.less_than(&time));

                println!("{:?}\tquery round {:?} complete", timer.elapsed(), round);
            }
        }
    }).unwrap();
}