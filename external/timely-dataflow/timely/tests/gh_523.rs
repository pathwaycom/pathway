use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::{Exchange, Input, Operator, Probe};
use timely::dataflow::InputHandle;
use timely::Config;

#[test]
fn gh_523() {
    timely::execute(Config::thread(), |worker| {
        let mut input = InputHandle::new();
        let mut buf = Vec::new();
        let probe = worker.dataflow::<u64, _, _>(|scope| {
            scope
                .input_from(&mut input)
                .unary(Pipeline, "Test", move |_, _| {
                    move |input, output| {
                        input.for_each(|cap, data| {
                            data.swap(&mut buf);
                            let mut session = output.session(&cap);
                            session.give_container(&mut Vec::new());
                            session.give_container(&mut buf);
                        });
                    }
                })
                .exchange(|x| *x)
                .probe()
        });

        for round in 0..2 {
            input.send(round);
            input.advance_to(round + 1);
        }
        input.close();

        while !probe.done() {
            worker.step();
        }

        println!("worker {} complete", worker.index());
    })
    .unwrap();
}
