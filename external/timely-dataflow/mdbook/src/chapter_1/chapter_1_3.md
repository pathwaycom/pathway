# Tracking Progress

Both dataflow and timestamps are valuable in their own right, but when we bring them together we get something even better. We get the ability to reason about the flow of timestamps through our computation, and we recover the ability to inform each dataflow component about how much of its input data it has seen.

Let's recall that bit of code we commented out from `examples/hello.rs`, which had to do with consulting something named `probe`.

```rust
extern crate timely;

use timely::dataflow::InputHandle;
use timely::dataflow::operators::{Input, Exchange, Inspect, Probe};

fn main() {
    // initializes and runs a timely dataflow.
    timely::execute_from_args(std::env::args(), |worker| {

        let index = worker.index();
        let mut input = InputHandle::new();

        // create a new input, exchange data, and inspect its output
        let probe = worker.dataflow(|scope|
            scope.input_from(&mut input)
                 .exchange(|x| *x)
                 .inspect(move |x| println!("worker {}:\thello {}", index, x))
                 .probe()
        );

        // introduce data and watch!
        for round in 0..10 {
            if worker.index() == 0 {
                input.send(round);
            }
            input.advance_to(round + 1);
            worker.step_while(|| probe.less_than(input.time()));
        }
    }).unwrap();
}
```

We'll put the whole program up here, but there are really just two lines that deal with progress tracking:

```rust,ignore
input.advance_to(round + 1);
worker.step_while(|| probe.less_than(input.time()));
```

Let's talk about each of them.

## Input capabilities

The `input` structure is how we provide data to a timely dataflow computation, and it has a timestamp associated with it. Initially this timestamp is the default value, usually something like `0` for integers. Whatever timestamp `input` has, it can introduce data with that timestamp or greater. We can advance this timestamp, via the `advance_to` method, which restricts the timestamps we can use to those greater or equal to whatever timestamp is supplied as the argument.

The `advance_to` method is a big deal. This is the moment in the computation where our program reveals to the system, and through the system to all other dataflow workers, that we might soon be able to announce a timestamp as complete. There may still be records in flight bearing that timestamp, but as they are retired the system can finally report that progress has been made.

## Output possibilities

The `probe` structure is how we learn about the possibility of timestamped data at some point in the dataflow graph. We can, at any point, consult a probe with the `less_than` method and ask whether it is still possible that we might see a time less than the argument at that point in the dataflow graph. There is also a `less_equal` method, if you prefer that.

Putting a probe after the `inspect` operator, which passes through all data it receives as input only after invoking its method, tells us whether we should expect to see the method associated with `inspect` fire again for a given timestamp. If we are told we won't see any more messages with timestamp `t` after the `inspect`, then the `inspect` won't see any either.

The `less_than` and `less_equal` methods are the only place where we learn about the state of the rest of the system. These methods are non-blocking; they always return immediately with either a "yes, you might see such a timestamp" or a "no, you will not see such a timestamp".

## Responding to progress information

Progress information is relatively passive. We get to observe what happens in the rest of the system, and perhaps change our behavior based on the amount of progress. We do not get to tell the system what to do next, we just get to see what has happened since last we checked.

This passive approach to coordination allows the system to operate with minimal overhead. Workers exchange both data and progress information. If workers want to wait for further progress before introducing more data they see they are welcome to do so, but they can also go and work on a different part of the dataflow graph as well.

Progress information provides a relatively unopinionated view of coordination. Workers are welcome to impose a more synchronous discipline using progress information, perhaps proceeding in sequence through operators by consulting probes installed after each of them, but they are not required to do so. Synchronization is possible, but it becomes a choice made by the workers themselves, rather than imposed on them by the system.
