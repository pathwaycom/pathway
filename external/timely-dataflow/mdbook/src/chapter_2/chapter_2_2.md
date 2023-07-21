# Observing Outputs

Having constructed a minimal streaming computation, we might like to take a peek at the output. There are a few ways to do this, but the simplest by far is the `inspect` operator.

The `inspect` operator is called with a closure, and it ensures that the closure is run on each record that passes through the operator. This closure can do just about anything, from printing to the screen or writing to a file.

```rust
extern crate timely;

use timely::dataflow::operators::{ToStream, Inspect};

fn main() {
    timely::execute_from_args(std::env::args(), |worker| {
        worker.dataflow::<(),_,_>(|scope| {
            (0 .. 9)
                .to_stream(scope)
                .inspect(|x| println!("hello: {}", x));
        });
    }).unwrap();
}
```

This simple example turns the sequence zero through nine into a stream and then prints the results to the screen.

## Inspecting Batches

The `inspect` operator has a big sibling, `inspect_batch`, whose closure gets access to whole batches of records at a time, just like the underlying operator. More precisely, `inspect_batch` takes a closure of two parameters: first, the timestamp of a batch, and second a reference to the batch itself. The `inspect_batch` operator can be especially helpful if you want to process the outputs more efficiently.

```rust
extern crate timely;

use timely::dataflow::operators::{ToStream, Inspect};

fn main() {
    timely::execute_from_args(std::env::args(), |worker| {
        worker.dataflow::<(),_,_>(|scope| {
            (0 .. 10)
                .to_stream(scope)
                .inspect_batch(|t, xs| println!("hello: {:?} @ {:?}", xs, t));
        });
    }).unwrap();
}
```

## Capturing Streams

The `Capture` trait provides a mechanism for exfiltrating a stream from a dataflow, into information that can be replayed in other dataflows. The trait is pretty general, and can even capture a stream to a binary writer that can be read back from to reconstruct the stream (see `examples/capture_send.rs` and `examples/capture_recv.rs`).

The simplest form of capture is the `capture()` method, which turns the stream into a shared queue of "events", which are the sequence of events the operator is exposed to: data arriving and notification of progress through the input stream. The `capture` method is used in many of timely's documentation tests, to extract a stream and verify that it is correct.

Consider the documentation test for the `ToStream` trait:

```rust
extern crate timely;

use timely::dataflow::operators::{ToStream, Capture};
use timely::dataflow::operators::capture::Extract;

fn main() {
    let (data1, data2) = timely::example(|scope| {
        let data1 = (0..3).to_stream(scope).capture();
        let data2 = vec![0,1,2].to_stream(scope).capture();
        (data1, data2)
    });

    assert_eq!(data1.extract(), data2.extract());
}
```

Here the two `capture` methods each return the receive side of one of Rust's threadsafe channels. The data moving along the channel have a type `capture::Event<T,D>` which you would need to read about, but which your main thread can drain out of the channel and process as it sees fit.
