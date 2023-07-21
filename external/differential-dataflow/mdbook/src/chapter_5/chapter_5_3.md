# Sharing across dataflows

Arrangements have the additional appealing property that they can be shared not only within a dataflow, but *across* dataflows.

Imagine we want to build and maintain a relatively large and continually changing collection. But we want to do this in a way that allows an arbitrary number of subsequent queries to access the collection at almost no additional cost.

The following example demonstrates going from an interactive input session (`input`) to an arrangement (`trace`) returned from the dataflow and available for use by others.

```rust
extern crate timely;
extern crate differential_dataflow;

use differential_dataflow::operators::JoinCore;
use differential_dataflow::operators::arrange::ArrangeByKey;

fn main() {

    // define a new timely dataflow computation.
    timely::execute_from_args(::std::env::args(), move |worker| {

        let mut knows = differential_dataflow::input::InputSession::new();

        // Input and arrange a dynamic collection.
        let mut trace = worker.dataflow(|scope| {

            let knows = knows.to_collection(scope);
            let knows = knows.arrange_by_key();

            // Return the `.trace` field of the arrangement`.
            knows.trace

        });

#       // to help with type inference ...
#       knows.update_at((0,0), 0usize, 1isize);
#       query.update_at((0,0), 0usize, 1isize);
    });
}
```

This example arranges the `knows` collection as before, but rather than use the arrangement it returns the `.trace` field of the arrangement. We can use this field to re-introduce the arrangement into other dataflows.

For example, let's imagine we want to construct many new dataflows each of which create a query set, which they then use to read out part of the `knows` collection.

```rust,ignore
    for round in 1 .. 1_000 {

        worker.dataflow(|scope| {

            // Round-specific query set.
            let query =
            (round .. round + 3)
                .to_stream(scope)
                .map(move |x| (x, round, 1))
                .as_collection();

            // Import arrangement, extract keys from `query`.
            trace
                .import(scope)
                .semijoin(&query)
                .consolidate()
                .inspect(move |x| println!("{:?}\t{:?}", timer.elapsed(), x))
                .probe_with(&mut probe);

        });

        // Change the collection a bit.
        input.remove((round, round));
        input.advance_to(round + 1);
        input.flush();

        // Run until all computations are current.
        while probe.less_than(input.time()) {
            worker.step();
        }

    }
```

The crucial line above is the line

```rust,ignore
            trace
                .import(scope)
```

which takes the `trace` and brings it in to the dataflow as an arrangement. It is now ready to be used in operations like `semijoin` that can exploit pre-arranged data.

Where in other stream processing systems such a computation might maintain one thousand independent indices each containing independent (but identical) copies of `knows`, here we are able to support all of these uses with a single arrangement.

## Great responsibility

When we extract a trace from an arrangement, we acquire the ability to replay the arrangement in any new scope. With that great power comes great responsibility. As long as we simply hold the trace, we prevent the system from compacting and efficiently managing its representation.

A `TraceHandle` (the type of `trace`) has two important methods. Their names are not great, and subject to change in the future. Their idioms may also change as more information flows in about users and use cases.

1. `set_logical_compaction(frontier)`. This method informs `trace` that it will no longer be called upon to handle queries for times not in advance of `frontier`, a set of timestamps. This gives the arrangement permission to coalesce otherwise indistinguishable timestamps, which it will start to do once all handles have advanced.

2. `set_physical_compaction(frontier)`. This method unblocks the merging of physical batches. It is very rare that a user wants to do anything with this other than call `trace.set_physical_compaction(&[])`, which unblocks all merging. Certain operators, namely `join`, do need to carefully manipulate this method.
