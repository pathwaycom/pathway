extern crate timely;

use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::operators::{ConnectLoop, Feedback};
use timely::{CommunicationConfig, Config, WorkerConfig};

#[test]
fn barrier_sync_1w() {
    barrier_sync_helper(CommunicationConfig::Thread);
}
#[test]
fn barrier_sync_2w() {
    barrier_sync_helper(CommunicationConfig::Process(2));
}
#[test]
fn barrier_sync_3w() {
    barrier_sync_helper(CommunicationConfig::Process(3));
}

// This method asserts that each round of execution is notified of at most one time.
fn barrier_sync_helper(comm_config: ::timely::CommunicationConfig) {
    let config = Config {
        communication: comm_config,
        worker: WorkerConfig::default(),
    };
    timely::execute(config, move |worker| {
        worker.dataflow(move |scope| {
            let (handle, stream) = scope.feedback::<u64>(1);
            stream
                .unary_notify(Pipeline, "Barrier", vec![0, 1], move |_, _, notificator| {
                    let mut count = 0;
                    while let Some((cap, _count)) = notificator.next() {
                        count += 1;
                        let time = *cap.time() + 1;
                        if time < 100 {
                            notificator.notify_at(cap.delayed(&time));
                        }
                    }
                    assert!(count <= 1);
                })
                .connect_loop(handle);
        });
    })
    .unwrap(); // asserts error-free execution;
}
