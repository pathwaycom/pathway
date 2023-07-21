extern crate rand;
extern crate timely;

use std::collections::HashMap;

use rand::{Rng, SeedableRng, rngs::SmallRng};

use timely::dataflow::*;
use timely::dataflow::operators::{Input, Probe};
use timely::dataflow::operators::generic::Operator;
use timely::dataflow::channels::pact::Exchange;

fn main() {

    // command-line args: numbers of nodes and edges in the random graph.
    let keys: u64 = std::env::args().nth(1).unwrap().parse().unwrap();
    let vals: usize = std::env::args().nth(2).unwrap().parse().unwrap();
    let batch: usize = std::env::args().nth(3).unwrap().parse().unwrap();

    timely::execute_from_args(std::env::args().skip(4), move |worker| {

        let index = worker.index();
        let peers = worker.peers();

        let mut input1 = InputHandle::new();
        let mut input2 = InputHandle::new();
        let mut probe = ProbeHandle::new();

        worker.dataflow(|scope| {

            let stream1 = scope.input_from(&mut input1);
            let stream2 = scope.input_from(&mut input2);

            let exchange1 = Exchange::new(|x: &(u64, u64)| x.0);
            let exchange2 = Exchange::new(|x: &(u64, u64)| x.0);

            stream1
                .binary(&stream2, exchange1, exchange2, "HashJoin", |_capability, _info| {

                    let mut map1 = HashMap::<u64, Vec<u64>>::new();
                    let mut map2 = HashMap::<u64, Vec<u64>>::new();

                    let mut vector1 = Vec::new();
                    let mut vector2 = Vec::new();

                    move |input1, input2, output| {

                        // Drain first input, check second map, update first map.
                        input1.for_each(|time, data| {
                            data.swap(&mut vector1);
                            let mut session = output.session(&time);
                            for (key, val1) in vector1.drain(..) {
                                if let Some(values) = map2.get(&key) {
                                    for val2 in values.iter() {
                                        session.give((val1.clone(), val2.clone()));
                                    }
                                }

                                map1.entry(key).or_insert(Vec::new()).push(val1);
                            }
                        });

                        // Drain second input, check first map, update second map.
                        input2.for_each(|time, data| {
                            data.swap(&mut vector2);
                            let mut session = output.session(&time);
                            for (key, val2) in vector2.drain(..) {
                                if let Some(values) = map1.get(&key) {
                                    for val1 in values.iter() {
                                        session.give((val1.clone(), val2.clone()));
                                    }
                                }

                                map2.entry(key).or_insert(Vec::new()).push(val2);
                            }
                        });
                    }
                })
                .probe_with(&mut probe);
        });

        let mut rng: SmallRng = SeedableRng::seed_from_u64(index as u64);

        let timer = std::time::Instant::now();

        let mut sent = 0;
        while sent < (vals / peers) {

            // Send some amount of data, no more than `batch`.
            let to_send = std::cmp::min(batch, vals/peers - sent);
            for _ in 0 .. to_send {
                input1.send((rng.gen_range(0..keys), rng.gen_range(0..keys)));
                input2.send((rng.gen_range(0..keys), rng.gen_range(0..keys)));
            }
            sent += to_send;

            // Advance input, iterate until data cleared.
            let next = input1.epoch() + 1;
            input1.advance_to(next);
            input2.advance_to(next);
            while probe.less_than(input1.time()) {
                worker.step();
            }

            println!("{:?}\tworker {} batch complete", timer.elapsed(), index)
        }

    }).unwrap(); // asserts error-free execution;
}
