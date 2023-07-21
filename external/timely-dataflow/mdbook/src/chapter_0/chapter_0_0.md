## A simplest example

Let's start with what may be the simplest non-trivial timely dataflow program.

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

This program gives us a bit of a flavor for what a timely dataflow program might look like, including a bit of what Rust looks like, without getting too bogged down in weird stream processing details. Not to worry; we will do that in just a moment!

If we run the program up above, we see it print out the numbers zero through nine.

```ignore
    Echidnatron% cargo run --example simple
        Finished dev [unoptimized + debuginfo] target(s) in 0.05s
         Running `target/debug/examples/simple`
    seen: 0
    seen: 1
    seen: 2
    seen: 3
    seen: 4
    seen: 5
    seen: 6
    seen: 7
    seen: 8
    seen: 9
    Echidnatron%
```

This isn't very different from a Rust program that would do this much more simply, namely the program

```rust
fn main() {
    (0..10).for_each(|x| println!("seen: {:?}", x));
}
```

Why would we want to make our life so complicated? The main reason is that we can make our program *reactive*, so that we can run it without knowing ahead of time the data we will use, and it will respond as we produce new data.