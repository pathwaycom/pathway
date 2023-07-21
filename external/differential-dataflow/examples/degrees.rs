extern crate rand;
extern crate timely;
extern crate differential_dataflow;

use rand::{Rng, SeedableRng, StdRng};

use differential_dataflow::input::Input;
use differential_dataflow::operators::count::CountTotal;

fn main() {

    let nodes: u32 = std::env::args().nth(1).unwrap().parse().unwrap();
    let edges: usize = std::env::args().nth(2).unwrap().parse().unwrap();
    let batch: usize = std::env::args().nth(3).unwrap().parse().unwrap();
    let inspect: bool = std::env::args().nth(4).unwrap() == "inspect";
    let open_loop: bool = std::env::args().nth(5).unwrap() == "open-loop";

    // define a new computational scope, in which to run BFS
    timely::execute_from_args(std::env::args().skip(6), move |worker| {

        let timer = ::std::time::Instant::now();

        let index = worker.index();
        let peers = worker.peers();

        // create a degree counting differential dataflow
        let (mut input, probe) = worker.dataflow(|scope| {

            // create edge input, count a few ways.
            let (input, edges) = scope.new_collection();

            let degrs = edges.map(|(src, _dst)| src)
                             .count_total()
                             ;

            // // pull of count, and count.
            // let distr = degrs.map(|(_src, cnt)| cnt as usize)
            //                  .count_total();

            // show us something about the collection, notice when done.
            let probe = if inspect {
                degrs.inspect(|x| println!("observed: {:?}", x))
                     .probe()
            }
            else { degrs.probe() };

            (input, probe)
        });

        let seed: &[_] = &[1, 2, 3, index];
        let mut rng1: StdRng = SeedableRng::from_seed(seed);    // rng for edge additions
        let mut rng2: StdRng = SeedableRng::from_seed(seed);    // rng for edge additions

        // load up graph dataz
        for _ in 0 .. (edges / peers) + if index < (edges % peers) { 1 } else { 0 } {
            input.insert((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)))
        }

        input.advance_to(1u64);
        input.flush();
        worker.step_while(|| probe.less_than(input.time()));

        println!("round 0 finished after {:?} (loading)", timer.elapsed());

        if batch > 0 {

            if !open_loop {

                let mut counts = vec![0u64; 64];
                // let mut changed = 0;

                // closed-loop latency-throughput test, parameterized by batch size.
                let timer = ::std::time::Instant::now();
                let mut wave = 1;
                while timer.elapsed().as_secs() < 10 {
                    for round in 0 .. batch {
                        input.advance_to((((wave * batch) + round) * peers + index) as u64);
                        input.insert((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)));
                        input.remove((rng2.gen_range(0, nodes), rng2.gen_range(0, nodes)));
                    }
                    // changed += 2 * batch;

                    wave += 1;
                    input.advance_to((wave * batch * peers) as u64);
                    input.flush();
                    let elapsed1 = timer.elapsed();
                    let elapsed1_ns = elapsed1.as_secs() * 1_000_000_000 + (elapsed1.subsec_nanos() as u64);
                    worker.step_while(|| probe.less_than(input.time()));
                    let elapsed2 = timer.elapsed();
                    let elapsed2_ns = elapsed2.as_secs() * 1_000_000_000 + (elapsed2.subsec_nanos() as u64);
                    let count_index = (elapsed2_ns - elapsed1_ns).next_power_of_two().trailing_zeros() as usize;
                    counts[count_index] += 1;
                }

                let elapsed = timer.elapsed();
                let seconds = elapsed.as_secs() as f64 + (elapsed.subsec_nanos() as f64) / 1000000000.0;
                println!("{:?}, {:?}", seconds / (wave - 1) as f64, ((wave - 1) * batch * peers) as f64 / seconds);
                println!("latencies:");
                for index in 0 .. counts.len() {
                    if counts[index] > 0 {
                        println!("\tcount[{}]:\t{}", index, counts[index]);
                    }
                }
            }
            else {

                let requests_per_sec = batch;
                let ns_per_request = 1_000_000_000 / requests_per_sec;
                let mut request_counter = peers + index;    // skip first request for each.
                let mut ack_counter = peers + index;
                let mut counts = vec![0u64; 64];
                let timer = ::std::time::Instant::now();

                let mut inserted_ns = 1;

                loop {

                    // Open-loop latency-throughput test, parameterized by offered rate `ns_per_request`.
                    let elapsed = timer.elapsed();
                    let elapsed_ns = elapsed.as_secs() * 1_000_000_000 + (elapsed.subsec_nanos() as u64);

                    // Determine completed ns.
                    let acknowledged_ns: u64 = probe.with_frontier(|frontier| frontier[0]);

                    // any un-recorded measurements that are complete should be recorded.
                    while ((ack_counter * ns_per_request) as u64) < acknowledged_ns {
                        let requested_at = (ack_counter * ns_per_request) as u64;
                        let count_index = (elapsed_ns - requested_at).next_power_of_two().trailing_zeros() as usize;
                        counts[count_index] += 1;
                        ack_counter += peers;

                        if (ack_counter & ((1 << 20) - 1)) == 0 {
                            println!("latencies:");
                            for index in 0 .. counts.len() {
                                if counts[index] > 0 {
                                    println!("\tcount[{}]:\t{}", index, counts[index]);
                                }
                            }
                            counts = vec![0u64; 64];
                        }
                    }

                    // Now, should we introduce more records before stepping the worker?
                    //
                    // Thinking: inserted_ns - acknowledged_ns is some amount of time that
                    // is currently outstanding in the system, and we needn't advance our
                    // inputs unless by this order of magnitude.
                    //
                    // The more sophisticated plan is: we compute the next power of two
                    // greater than inserted_ns - acknowledged_ns and look for the last
                    // multiple of this number in the interval [inserted_ns, elapsed_ns].
                    // If such a multiple exists, we introduce records to that point and
                    // advance the input.

                    let scale = (inserted_ns - acknowledged_ns).next_power_of_two();

                    // last multiple of `scale` less than `elapsed_ns`.
                    let target_ns = elapsed_ns & !(scale - 1);

                    if inserted_ns < target_ns {

                        // println!("acknowledged_ns:\t{:?}\t{:X}", acknowledged_ns, acknowledged_ns);
                        // println!("inserted_ns:\t\t{:?}\t{:X}", inserted_ns, inserted_ns);
                        // println!("target_ns:\t\t{:?}\t{:X}", target_ns, target_ns);
                        // println!("elapsed_ns:\t\t{:?}\t{:X}", elapsed_ns, elapsed_ns);
                        // println!();

                        while ((request_counter * ns_per_request) as u64) < target_ns {
                            input.advance_to((request_counter * ns_per_request) as u64);
                            input.insert((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)));
                            input.remove((rng2.gen_range(0, nodes), rng2.gen_range(0, nodes)));
                            request_counter += peers;
                        }
                        input.advance_to(target_ns);
                        input.flush();
                        inserted_ns = target_ns;
                    }

                    worker.step();
                }
            }
        }
    }).unwrap();
}