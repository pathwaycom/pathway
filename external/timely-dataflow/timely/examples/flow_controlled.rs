extern crate timely;

use timely::dataflow::operators::flow_controlled::{iterator_source, IteratorSourceInput};
use timely::dataflow::operators::{probe, Inspect, Probe};

fn main() {
    timely::execute_from_args(std::env::args(), |worker| {
        let mut input = (0u64..100000).peekable();
        worker.dataflow(|scope| {
            let mut probe_handle = probe::Handle::new();
            let probe_handle_2 = probe_handle.clone();

            iterator_source(
                scope,
                "Source",
                move |prev_t| {
                    if let Some(first_x) = input.peek().cloned() {
                        let next_t = first_x / 100 * 100;
                        Some(IteratorSourceInput {
                            lower_bound: Default::default(),
                            data: vec![(
                                next_t,
                                input
                                    .by_ref()
                                    .take(10)
                                    .map(|x| (/* "timestamp" */ x, x))
                                    .collect::<Vec<_>>(),
                            )],
                            target: *prev_t,
                        })
                    } else {
                        None
                    }
                },
                probe_handle_2,
            )
            .inspect_time(|t, d| eprintln!("@ {:?}: {:?}", t, d))
            .probe_with(&mut probe_handle);
        });
    })
    .unwrap();
}
