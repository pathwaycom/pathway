# Building Timely Dataflows

Let's talk about how to create timely dataflows.

This section will be a bit of a tour through the dataflow construction process, ignoring for the moment details about the interesting ways in which you can get data into and out of your dataflow; those will show up in the "Running Timely Dataflows" section. For now we are going to work with examples with fixed input data and no interactivity to speak of, focusing on what we can cause to happen to that data.

Here is a relatively simple example, taken from `timely/examples/simple.rs`, that turns the numbers zero through nine into a stream, and then feeds them through an `inspect` operator printing them to the screen.

```rust
extern crate timely;

use timely::dataflow::operators::{ToStream, Inspect};

fn main() {
    timely::example(|scope| {
        (0..10).to_stream(scope)
               .inspect(|x| println!("seen: {:?}", x));
    });
}
```

We are going to develop out this example, showing off both the built-in operators as well as timely's generic operator construction features.

---

**NOTE**: Timely very much assumes that you are going to build the same dataflow on each worker. You don't literally have to, in that you could build a dataflow from user input, or with a random number generator, things like that. Please don't! It will not be a good use of your time.
