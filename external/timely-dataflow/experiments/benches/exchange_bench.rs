extern crate timely;

use std::fmt::{Display, Formatter};
use std::time::{Duration, Instant};

use criterion::black_box;
use criterion::*;

use timely::dataflow::operators::{Exchange, Input, Probe};
use timely::dataflow::InputHandle;
use timely::{CommunicationConfig, Config, WorkerConfig};

#[derive(Clone)]
struct ExperimentConfig {
    threads: usize,
    batch: u64,
}

impl Display for ExperimentConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "threads={:2},batch={:5}", self.threads, self.batch)
    }
}

fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("exchange");
    for threads in [1, 2, 4, 8, 16] {
        for shift in [0, 4, 8, 14] {
            let params = ExperimentConfig {
                threads,
                batch: 1u64 << shift,
            };
            group.bench_with_input(
                BenchmarkId::new("Default", params.clone()),
                &params,
                move |b, params| {
                    b.iter_custom(|iters| {
                        let config = Config::process(params.threads);
                        black_box(experiment_exchange(
                            config,
                            params.batch,
                            iters,
                        ))
                    })
                },
            );
            group.bench_with_input(
                BenchmarkId::new("DefaultZero", params.clone()),
                &params,
                move |b, params| {
                    b.iter_custom(|iters| {
                        let config = Config {
                            communication: CommunicationConfig::ProcessBinary(params.threads),
                            worker: WorkerConfig::default(),
                        };
                        black_box(experiment_exchange(
                            config,
                            params.batch,
                            iters,
                        ))
                    })
                },
            );
        }
    }
}

fn experiment_exchange(
    config: Config,
    batch: u64,
    rounds: u64,
) -> Duration {
    timely::execute(config, move |worker| {
        let mut input = InputHandle::new();
        let probe = worker.dataflow(|scope| scope.input_from(&mut input).exchange(|x| *x).probe());

        let mut time = 0;
        let timer = Instant::now();

        let buffer = (0..batch).collect();
        let mut copy = Vec::new();

        for _round in 0..rounds {
            copy.clone_from(&buffer);
            input.send_batch(&mut copy);
            copy.clear();
            time += 1;
            input.advance_to(time);
            while probe.less_than(input.time()) {
                worker.step();
            }
        }
        timer.elapsed()
    })
    .unwrap()
    .join()
    .into_iter()
    .next()
    .unwrap()
    .unwrap()
}

criterion_group!(benches, bench);
criterion_main!(benches);
