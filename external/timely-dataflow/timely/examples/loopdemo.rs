extern crate timely;

use timely::dataflow::{InputHandle, ProbeHandle};
use timely::dataflow::operators::{Input, Feedback, Concat, Map, Filter, ConnectLoop, Probe};

fn main() {

    let mut args = std::env::args();
    args.next();
    let rate: usize = args.next().expect("Must specify rate").parse().expect("Rate must be an usize");
    let duration_s: usize = args.next().expect("Must specify duration_s").parse().expect("duration_s must be an usize");

    timely::execute_from_args(args, move |worker| {

        let index = worker.index();
        let peers = worker.peers();

        let timer = std::time::Instant::now();

        let mut input = InputHandle::new();
        let mut probe = ProbeHandle::new();

        // Create a dataflow that discards input data (just syncronizes).
        worker.dataflow(|scope| {

            let stream = scope.input_from(&mut input);

            let (loop_handle, loop_stream) = scope.feedback(1);

            let step =
            stream
                .concat(&loop_stream)
                .map(|x| if x % 2 == 0 { x / 2 } else { 3 * x + 1 })
                .filter(|x| x > &1);

            step.connect_loop(loop_handle);
            step.probe_with(&mut probe);
        });

        let ns_per_request = 1_000_000_000 / rate;
        let mut insert_counter = index;           // counts up as we insert records.
        let mut retire_counter = index;           // counts up as we retire records.

        let mut inserted_ns = 0;

        // We repeatedly consult the elapsed time, and introduce any data now considered available.
        // At the same time, we observe the output and record which inputs are considered retired.

        let mut counts = vec![[0u64; 16]; 64];

        let counter_limit = rate * duration_s;
        while retire_counter < counter_limit {

            // Open-loop latency-throughput test, parameterized by offered rate `ns_per_request`.
            let elapsed = timer.elapsed();
            let elapsed_ns = elapsed.as_secs() * 1_000_000_000 + (elapsed.subsec_nanos() as u64);

            // Determine completed ns.
            let acknowledged_ns: u64 = probe.with_frontier(|frontier| frontier[0]);

            // Notice any newly-retired records.
            while ((retire_counter * ns_per_request) as u64) < acknowledged_ns && retire_counter < counter_limit {
                let requested_at = (retire_counter * ns_per_request) as u64;
                let latency_ns = elapsed_ns - requested_at;

                let count_index = latency_ns.next_power_of_two().trailing_zeros() as usize;
                let low_bits = ((elapsed_ns - requested_at) >> (count_index - 5)) & 0xF;
                counts[count_index][low_bits as usize] += 1;

                retire_counter += peers;
            }

            // Now, should we introduce more records before stepping the worker?
            // Three choices here:
            //
            //   1. Wait until previous batch acknowledged.
            //   2. Tick at most once every millisecond-ish.
            //   3. Geometrically increasing outstanding batches.

            // Technique 1:
            // let target_ns = if acknowledged_ns >= inserted_ns { elapsed_ns } else { inserted_ns };

            // Technique 2:
            // let target_ns = elapsed_ns & !((1 << 20) - 1);

            // Technique 3:
            let scale = (inserted_ns - acknowledged_ns).next_power_of_two();
            let target_ns = elapsed_ns & !(scale - 1);

            if inserted_ns < target_ns {

                while ((insert_counter * ns_per_request) as u64) < target_ns {
                    input.send(insert_counter);
                    insert_counter += peers;
                }
                input.advance_to(target_ns);
                inserted_ns = target_ns;
            }

            worker.step();
        }

        // Report observed latency measurements.
        if index == 0 {

            let mut results = Vec::new();
            let total = counts.iter().map(|x| x.iter().sum::<u64>()).sum();
            let mut sum = 0;
            for index in (10 .. counts.len()).rev() {
                for sub in (0 .. 16).rev() {
                    if sum > 0 && sum < total {
                        let latency = (1 << (index-1)) + (sub << (index-5));
                        let fraction = (sum as f64) / (total as f64);
                        results.push((latency, fraction));
                    }
                    sum += counts[index][sub];
                }
            }
            for (latency, fraction) in results.drain(..).rev() {
                println!("{}\t{}", latency, fraction);
            }
        }

    }).unwrap();
}