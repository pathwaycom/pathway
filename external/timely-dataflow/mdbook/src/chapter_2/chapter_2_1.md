# Creating Inputs

Let's start with the first thing we'll want for a dataflow computation: a source of data.

Almost all operators in timely can only be defined from a source of data, with a few exceptions. One of these exceptions is the `to_stream` operator, which is defined for various types and which takes a `scope` as an argument and produces a stream in that scope. Our `InputHandle` type from previous examples has a `to_stream` method, as well as any type that can be turned into an iterator (which we used in the preceding example).

For example, we can create a new dataflow with one interactive input and one static input:

```rust
extern crate timely;

use timely::dataflow::InputHandle;
use timely::dataflow::operators::ToStream;

fn main() {
    // initializes and runs a timely dataflow.
    timely::execute_from_args(std::env::args(), |worker| {

        let mut input = InputHandle::<(), String>::new();

        // define a new dataflow
        worker.dataflow(|scope| {

            let stream1 = input.to_stream(scope);
            let stream2 = (0 .. 9).to_stream(scope);

        });

    }).unwrap();
}
```

There will be more to do to get data into `input`, and we aren't going to worry about that at the moment. But, now you know two of the places you can get data from!

## Other sources

There are other sources of input that are a bit more advanced. Once we learn how to create custom operators, the `source` method will allow us to create a custom operator with zero input streams and one output stream, which looks like a source of data (hence the name). There are also the `Capture` and `Replay` traits that allow us to exfiltrate the contents of a stream from one dataflow (using `capture_into`) and re-load it in another dataflow (using `replay_into`).
