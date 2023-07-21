extern crate timely;

use timely::dataflow::{InputHandle, ProbeHandle};
use timely::dataflow::operators::{Input, Exchange, Probe};

// use timely::dataflow::operators::capture::EventWriter;
// use timely::dataflow::ScopeParent;
use timely::logging::{TimelyEvent, TimelyProgressEvent};

fn main() {
    // initializes and runs a timely dataflow.
    timely::execute_from_args(std::env::args(), |worker| {

        let batch = std::env::args().nth(1).unwrap().parse::<usize>().unwrap();
        let rounds = std::env::args().nth(2).unwrap().parse::<usize>().unwrap();
        let mut input = InputHandle::new();
        let mut probe = ProbeHandle::new();

        // Register timely worker logging.
        worker.log_register().insert::<TimelyEvent,_>("timely", |_time, data|
            data.iter().for_each(|x| println!("LOG1: {:?}", x))
        );

        // Register timely progress logging.
        // Less generally useful: intended for debugging advanced custom operators or timely
        // internals.
        worker.log_register().insert::<TimelyProgressEvent,_>("timely/progress", |_time, data|
            data.iter().for_each(|x| {
                println!("PROGRESS: {:?}", x);
                let (_, _, ev) = x;
                print!("PROGRESS: TYPED MESSAGES: ");
                for (n, p, t, d) in ev.messages.iter() {
                    print!("{:?}, ", (n, p, t.as_any().downcast_ref::<usize>(), d));
                }
                println!();
                print!("PROGRESS: TYPED INTERNAL: ");
                for (n, p, t, d) in ev.internal.iter() {
                    print!("{:?}, ", (n, p, t.as_any().downcast_ref::<usize>(), d));
                }
                println!();
            })
        );

        // create a new input, exchange data, and inspect its output
        worker.dataflow(|scope| {
            scope
                .input_from(&mut input)
                .exchange(|&x| x as u64)
                .probe_with(&mut probe);
        });

        // Register timely worker logging.
        worker.log_register().insert::<TimelyEvent,_>("timely", |_time, data|
            data.iter().for_each(|x| println!("LOG2: {:?}", x))
        );

        // create a new input, exchange data, and inspect its output
        worker.dataflow(|scope| {
            scope
                .input_from(&mut input)
                .exchange(|&x| x as u64)
                .probe_with(&mut probe);
        });

        // Register user-level logging.
        worker.log_register().insert::<(),_>("input", |_time, data|
            for element in data.iter() {
                println!("Round tick at: {:?}", element.0);
            }
        );

        let input_logger = worker.log_register().get::<()>("input").expect("Input logger absent");

        let timer = std::time::Instant::now();

        for round in 0 .. rounds {

            for i in 0 .. batch {
                input.send(i);
            }
            input.advance_to(round);
            input_logger.log(());

            while probe.less_than(input.time()) {
                worker.step();
            }

        }

        let volume = (rounds * batch) as f64;
        let elapsed = timer.elapsed();
        let seconds = elapsed.as_secs() as f64 + (f64::from(elapsed.subsec_nanos())/1000000000.0);

        println!("{:?}\tworker {} complete; rate: {:?}", timer.elapsed(), worker.index(), volume / seconds);

    }).unwrap();
}
