## An example

Timely dataflow means to capture a large number of idioms, so it is a bit tricky to wrap together one example that shows off all of its features, but let's look at something that shows off some core functionality to give a taste.

The following complete program initializes a timely dataflow computation, in which participants can supply a stream of numbers which are exchanged between the workers based on their value. Workers print to the screen when they see numbers. You can also find this as [`examples/hello.rs`](https://github.com/TimelyDataflow/timely-dataflow/blob/master/examples/hello.rs) in the [timely dataflow repository](https://github.com/TimelyDataflow/timely-dataflow/tree/master/examples).

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
            if index == 0 {
                input.send(round);
            }
            input.advance_to(round + 1);
            while probe.less_than(input.time()) {
                worker.step();
            }
        }
    }).unwrap();
}
```

We can run this program in a variety of configurations: with just a single worker thread, with one process and multiple worker threads, and with multiple processes each with multiple worker threads.

To try this out yourself, first clone the timely dataflow repository using `git`

```ignore
    Echidnatron% git clone https://github.com/TimelyDataflow/timely-dataflow
    Cloning into 'timely-dataflow'...
    remote: Counting objects: 14254, done.
    remote: Compressing objects: 100% (2267/2267), done.
    remote: Total 14254 (delta 2625), reused 3824 (delta 2123), pack-reused 9856
    Receiving objects: 100% (14254/14254), 9.01 MiB | 1.04 MiB/s, done.
    Resolving deltas: 100% (10686/10686), done.
```

Now `cd` into the directory and build timely dataflow by typing

```ignore
    Echidnatron% cd timely-dataflow
    Echidnatron% cargo build
        Updating registry `https://github.com/rust-lang/crates.io-index`
    Compiling timely_sort v0.1.6
    Compiling byteorder v0.4.2
    Compiling libc v0.2.29
    Compiling abomonation v0.4.5
    Compiling getopts v0.2.14
    Compiling time v0.1.38
    Compiling timely_communication v0.1.7
    Compiling timely v0.2.0 (file:///Users/mcsherry/Projects/temporary/timely-dataflow)
        Finished dev [unoptimized + debuginfo] target(s) in 6.37 secs
```

Now we build the `hello` example

```ignore
    Echidnatron% cargo build --example hello
    Compiling rand v0.3.16
    Compiling timely v0.2.0 (file:///Users/mcsherry/Projects/temporary/timely-dataflow)
        Finished dev [unoptimized + debuginfo] target(s) in 6.35 secs
```

And finally we run the `hello` example

```ignore
    Echidnatron% cargo run --example hello
        Finished dev [unoptimized + debuginfo] target(s) in 0.0 secs
        Running `target/debug/examples/hello`
    worker 0:	hello 0
    worker 0:	hello 1
    worker 0:	hello 2
    worker 0:	hello 3
    worker 0:	hello 4
    worker 0:	hello 5
    worker 0:	hello 6
    worker 0:	hello 7
    worker 0:	hello 8
    worker 0:	hello 9
    Echidnatron%
```

Rust is relatively clever, and we could have skipped the `cargo build` and `cargo build --example hello` commands; just invoking `cargo run --example hello` will build (or rebuild) anything necessary.

Of course, we can run this with multiple workers using the `-w` or `--workers` flag, followed by the number of workers we want in the process. Notice that you'll need an `--` before the arguments to our program; any arguments before that are treated as arguments to the `cargo` command.

```ignore
    Echidnatron% cargo run --example hello -- -w2
        Finished dev [unoptimized + debuginfo] target(s) in 0.0 secs
        Running `target/debug/examples/hello -w2`
    worker 0:	hello 0
    worker 1:	hello 1
    worker 0:	hello 2
    worker 1:	hello 3
    worker 0:	hello 4
    worker 1:	hello 5
    worker 0:	hello 6
    worker 1:	hello 7
    worker 0:	hello 8
    worker 1:	hello 9
    Echidnatron%
```

Although you can't easily see this happening, timely dataflow has spun up *two* worker threads and together they have exchanged some data and printed the results as before. However, notice that the worker index is now varied; this is our only clue that different workers exist, and processed different pieces of data. Worker zero introduces all of the data (notice the guard in the code; without this *each* worker would introduce `0 .. 10`), and then it is shuffled between the workers. The only *guarantee* is that records that evaluate to the same integer in the exchange closure go to the same worker. In practice, we (currently) route records based on the remainder of the number when divided by the number of workers.

Finally, let's run with multiple processes. To do this, you use the `-n` and `-p` arguments, which tell each process how many total processes to expect (the `-n` parameter) and which index this process should identify as (the `-p` parameter). You can also use `-h` to specify a host file with names and ports of each of the processes involved, but if you leave it off timely defaults to using the local host.

In one shell, I'm going to start a computation that expects multiple processes. It will hang out waiting for the other processes to start up.

```ignore
    Echidnatron% cargo run --example hello -- -n2 -p0
        Finished dev [unoptimized + debuginfo] target(s) in 0.0 secs
        Running `target/debug/examples/hello -n2 -p0`
```

Now if we head over to another shell, we can type the same thing but with a different `-p` identifier.

```ignore
    Echidnatron% cargo run --example hello -- -n2 -p1
        Finished dev [unoptimized + debuginfo] target(s) in 0.0 secs
        Running `target/debug/examples/hello -n2 -p1`
    worker 1:	hello 1
    worker 1:	hello 3
    worker 1:	hello 5
    worker 1:	hello 7
    worker 1:	hello 9
    Echidnatron%
```

Wow, fast! And, we get to see some output too. Only the output for this worker, though. If we head back to the other shell we see the process got moving and produced the other half of the output.

```ignore
    Echidnatron% cargo run --example hello -- -n2 -p0
        Finished dev [unoptimized + debuginfo] target(s) in 0.0 secs
        Running `target/debug/examples/hello -n2 -p0`
    worker 0:	hello 0
    worker 0:	hello 2
    worker 0:	hello 4
    worker 0:	hello 6
    worker 0:	hello 8
    Echidnatron%
```

This may seem only slightly interesting so far, but we will progressively build up more interesting tools and more interesting computations, and see how timely dataflow can efficiently execute them for us.
