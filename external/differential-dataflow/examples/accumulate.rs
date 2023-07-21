extern crate rand;
extern crate timely;
extern crate differential_dataflow;

use rand::{Rng, SeedableRng, StdRng};

use differential_dataflow::input::Input;
use differential_dataflow::operators::Consolidate;

fn main() {

    let keys: usize = std::env::args().nth(1).unwrap().parse().unwrap();
    let batch: usize = 10_000;

    // This computation demonstrates in-place accumulation of arbitrarily large 
    // volumes of input data, consuming space bounded by the number of distinct keys.
    timely::execute_from_args(std::env::args().skip(2), move |worker| {

        let index = worker.index();
        let peers = worker.peers();

        let mut input = worker.dataflow::<(), _, _>(|scope| {
            let (input, data) = scope.new_collection::<_, isize>();
            data.consolidate();
            input
        });

        let seed: &[_] = &[1, 2, 3, 4];
        let mut rng: StdRng = SeedableRng::from_seed(seed);
        let timer = ::std::time::Instant::now();

        let mut counter = 0;
        let mut last_sec = 0;

        loop {

            for _ in 0 .. batch {
                input.insert(rng.gen_range(0, keys as u32));
            }
            counter += batch;

            worker.step(); 
            let elapsed = timer.elapsed();

            if elapsed.as_secs() as usize > last_sec {
                let secs = elapsed.as_secs() as f64 + (elapsed.subsec_nanos() as f64)/1000000000.0;
                if last_sec % peers == index {
                    println!("tuples: {:?},\telts/sec: {:?}", peers * counter, (peers * counter) as f64 / secs);
                }
                last_sec = elapsed.as_secs() as usize;
            }
        }

    }).unwrap();
}