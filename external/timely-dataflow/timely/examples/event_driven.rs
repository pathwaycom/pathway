extern crate timely;

// use timely::dataflow::{InputHandle, ProbeHandle};
use timely::dataflow::operators::{Input, Map, Probe};

fn main() {
    // initializes and runs a timely dataflow.
    timely::execute_from_args(std::env::args(), |worker| {

        let timer = std::time::Instant::now();

        let mut args = std::env::args();
        args.next();

        let dataflows = args.next().unwrap().parse::<usize>().unwrap();
        let length = args.next().unwrap().parse::<usize>().unwrap();
        let record = args.next() == Some("record".to_string());

        let mut inputs = Vec::new();
        let mut probes = Vec::new();

        // create a new input, exchange data, and inspect its output
        for _dataflow in 0 .. dataflows {
            worker.dataflow(|scope| {
                let (input, mut stream) = scope.new_input();
                for _step in 0 .. length {
                    stream = stream.map(|x: ()| x);
                }
                let probe = stream.probe();
                inputs.push(input);
                probes.push(probe);
            });
        }

        println!("{:?}\tdataflows built ({} x {})", timer.elapsed(), dataflows, length);

        for round in 0 .. {
            let dataflow = round % dataflows;
            if record {
                inputs[dataflow].send(());
            }
            inputs[dataflow].advance_to(round);
            let mut steps = 0;
            while probes[dataflow].less_than(&round) {
                worker.step();
                steps += 1;
            }
            println!("{:?}\tround {} complete in {} steps", timer.elapsed(), round, steps);
        }

    }).unwrap();
}
