extern crate timely;

use timely::dataflow::operators::{Exchange, Input, Inspect, Probe};
use timely::dataflow::{InputHandle, ProbeHandle};

fn main() {
    // initializes and runs a timely dataflow.
    timely::execute_from_args(std::env::args(), |worker| {
        let index = worker.index();
        let mut input = InputHandle::new();
        let mut probe = ProbeHandle::new();

        // create a new input, exchange data, and inspect its output
        worker.dataflow(|scope| {
            scope
                .input_from(&mut input)
                .exchange(|x| *x)
                .inspect(move |x| println!("worker {}:\thello {}", index, x))
                .probe_with(&mut probe);
        });

        // introduce data and watch!
        for round in 0..10 {
            if index == 0 {
                input.send(round);
            }
            input.advance_to(round + 1);
            while probe.less_than(input.time()) {
                worker.step();
            }
        }
    })
    .unwrap();
}
