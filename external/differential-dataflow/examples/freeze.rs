extern crate timely;
extern crate differential_dataflow;

use timely::dataflow::operators::probe::Handle;
use timely::dataflow::operators::Map;

use differential_dataflow::input::Input;
use differential_dataflow::AsCollection;
use differential_dataflow::operators::*;
use differential_dataflow::operators::arrange::{ArrangeByKey, ArrangeBySelf};
use differential_dataflow::trace::wrappers::freeze::freeze;

fn main() {

    // define a new computational scope, in which to run BFS
    timely::execute_from_args(std::env::args(), move |worker| {
        
        // define BFS dataflow; return handles to roots and edges inputs
        let mut probe = Handle::new();
        let (mut rules, mut graph) = worker.dataflow(|scope| {

            let (rule_input, rules) = scope.new_collection();
            let (edge_input, graph) = scope.new_collection();

            let result = graph.iterate(|inner| {

                let rules = rules.enter(&inner.scope());
                let arranged = inner.arrange_by_key();

                // rule 0: remove self-loops:
                let freeze0 = freeze(&arranged, |t| {
                    if t.inner <= 0 {
                        let mut t = t.clone();
                        t.inner = 0;
                        Some(t)
                    }
                    else { None }
                });
                let rule0 = freeze0.as_collection(|&k,&v| (k,v))
                                   .filter(|x| x.0 == x.1)
                                   .negate()
                                   .inspect(|x| println!("rule0:\t{:?}", x));

                // subtract self loops once, not each round.
                let rule0 = &rule0.inner
                                  .map_in_place(|dtr| { dtr.1.inner += 1; })
                                  .as_collection()
                                  .negate()
                                  .concat(&rule0);

                // rule 1: overwrite keys present in `rules`
                let freeze1 = freeze(&arranged, |t| {
                    if t.inner <= 1 {
                        let mut t = t.clone();
                        t.inner = 1;
                        Some(t)
                    }
                    else { None }
                });
                let rule1 = freeze1.join_core(&rules.map(|(x,_y)| x).distinct().arrange_by_self(), |&k, &x, &()| Some((k,x)))
                                   .negate()
                                   .concat(&rules.inner.map_in_place(|dtr| dtr.1.inner = 1).as_collection())
                                   .inspect(|x| println!("rule1:\t{:?}", x));

                let rule1 = &rule1.inner
                                  .map_in_place(|dtr| { dtr.1.inner += 1; })
                                  .as_collection()
                                  .negate()
                                  .concat(&rule1);

                inner
                    .concat(&rule0)
                    .concat(&rule1)
                    .consolidate()
                    .inspect(|x| println!("inner:\t{:?}", x))
            });

            result.consolidate()
                  .inspect(|x| println!("output\t{:?}", x))
                  .probe_with(&mut probe);

            (rule_input, edge_input)
        });

        println!("starting up");

        graph.insert((0, 1));
        graph.insert((1, 1));
        graph.insert((2, 1));
        graph.insert((2, 3));
        graph.advance_to(1); graph.flush();
        rules.advance_to(1); rules.flush();

        while probe.less_than(graph.time()) { worker.step(); }
        println!("round 0 complete");

        graph.insert((3, 3));
        graph.advance_to(2); graph.flush();
        rules.advance_to(2); rules.flush();

        while probe.less_than(graph.time()) { worker.step(); }
        println!("round 1 complete");

        rules.insert((2, 2));
        graph.advance_to(3); graph.flush();
        rules.advance_to(3); rules.flush();

        while probe.less_than(graph.time()) { worker.step(); }
        println!("round 2 complete");

    }).unwrap();
}