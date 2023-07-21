# Timely Skeleton

Differential dataflow computations are really just [timely dataflow](https://github.com/frankmcsherry/timely-dataflow) computations where we supply a sweet set of operators and idioms for you. As such, when you build a new differential dataflow computation it will need to have a timely dataflow skeleton built first. For example:

```rust,ignore
extern crate timely;
extern crate differential_dataflow;

fn main() {

    // prepare a timely dataflow execution environment.
    timely::execute_from_args(std::env::args(), |worker| {

        // create a differential dataflow.
        let mut input = worker.dataflow::<usize,_,_>(|scope| {

            // create inputs, build dataflow, return input handle.

        });

        // drive the input around here.

    }).unwrap();
}
```

This is a pretty standard skeleton, where our program immediately starts up a timely dataflow instance by defining what each independent worker should do. A standard pattern, seen above, is to have each worker construct a dataflow and then drive the inputs around somehow.

We'll get more specific in just a moment.