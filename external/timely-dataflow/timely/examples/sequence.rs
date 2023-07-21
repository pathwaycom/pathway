extern crate timely;

use std::time::{Instant, Duration};

use timely::Config;
use timely::synchronization::Sequencer;

fn main() {
    timely::execute(Config::process(4), |worker| {

        let timer = Instant::now();
        let mut sequencer = Sequencer::new(worker, Instant::now());

        for round in 0 .. {
            // if worker.index() < 3 {
                std::thread::sleep(Duration::from_secs(1 + worker.index() as u64));
                sequencer.push(format!("worker {:?}, round {:?}", worker.index(), round));
            // }
            while let Some(element) = sequencer.next() {
                println!("{:?}:\tWorker {:?}:\t recv'd: {:?}", timer.elapsed(), worker.index(), element);
            }
            worker.step();
        }

    }).unwrap(); // asserts error-free execution;
}
