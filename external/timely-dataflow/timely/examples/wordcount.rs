extern crate timely;

use std::collections::HashMap;

use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::{Inspect, Map, Operator, Probe};
use timely::dataflow::{InputHandle, ProbeHandle};

fn main() {
    // initializes and runs a timely dataflow.
    timely::execute_from_args(std::env::args(), |worker| {
        let mut input = InputHandle::new();
        let mut probe = ProbeHandle::new();

        // define a distribution function for strings.
        let exchange = Exchange::new(|x: &(String, i64)| (x.0).len() as u64);

        // create a new input, exchange data, and inspect its output
        worker.dataflow::<usize, _, _>(|scope| {
            input
                .to_stream(scope)
                .flat_map(|(text, diff): (String, i64)| {
                    text.split_whitespace()
                        .map(move |word| (word.to_owned(), diff))
                        .collect::<Vec<_>>()
                })
                .unary_frontier(exchange, "WordCount", |_capability, _info| {
                    let mut queues = HashMap::new();
                    let mut counts = HashMap::new();

                    move |input, output| {
                        while let Some((time, data)) = input.next() {
                            queues
                                .entry(time.retain())
                                .or_insert(Vec::new())
                                .push(data.replace(Vec::new()));
                        }

                        for (key, val) in queues.iter_mut() {
                            if !input.frontier().less_equal(key.time()) {
                                let mut session = output.session(key);
                                for mut batch in val.drain(..) {
                                    for (word, diff) in batch.drain(..) {
                                        let entry = counts.entry(word.clone()).or_insert(0i64);
                                        *entry += diff;
                                        session.give((word, *entry));
                                    }
                                }
                            }
                        }

                        queues.retain(|_key, val| !val.is_empty());
                    }
                })
                .inspect(|x| println!("seen: {:?}", x))
                .probe_with(&mut probe);
        });

        // introduce data and watch!
        for round in 0..10 {
            input.send(("round".to_owned(), 1));
            input.advance_to(round + 1);
            while probe.less_than(input.time()) {
                worker.step();
            }
        }
    })
    .unwrap();
}
