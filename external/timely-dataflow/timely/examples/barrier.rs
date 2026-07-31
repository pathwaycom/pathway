extern crate timely;

use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::operators::{ConnectLoop, Feedback};

fn main() {
    let iterations = std::env::args()
        .nth(1)
        .unwrap()
        .parse::<usize>()
        .unwrap_or(1_000_000);

    timely::execute_from_args(std::env::args().skip(2), move |worker| {
        worker.dataflow(move |scope| {
            let (handle, stream) = scope.feedback::<usize>(1);
            stream
                .unary_notify(Pipeline, "Barrier", vec![0], move |_, _, notificator| {
                    while let Some((cap, _count)) = notificator.next() {
                        let time = *cap.time() + 1;
                        if time < iterations {
                            notificator.notify_at(cap.delayed(&time));
                        }
                    }
                })
                .connect_loop(handle);
        });
    })
    .unwrap(); // asserts error-free execution;
}
