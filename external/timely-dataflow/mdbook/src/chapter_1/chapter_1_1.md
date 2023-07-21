# Dataflow Programming

Dataflow programming is fundamentally about describing your program as independent components, each of which operate in response to the availability of input data, as well as describing the connections between these components. This has several advantages, mostly in how it allows a computer to execute your program, but it can take a bit of thinking to re-imagine your imperative computation as a dataflow computation.

## An example

Let's write an overly simple dataflow program. Remember our `examples/hello.rs` program? We are going to revisit that, but with some **timestamp** aspects removed. The goal is to get a sense for dataflow with all of its warts, and to get you excited for the next section where we bring back the timestamps. :)

Here is a reduced version of `examples/hello.rs` that just feeds data into our dataflow, without paying any attention to progress made. In particular, we have removed the `probe()` operation, the resulting `probe` variable, and the use of `probe` to determine how long we should step the worker before introducing more data.

```rust
#![allow(unused_variables)]
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
            // worker.step_while(|| probe.less_than(input.time()));
        }
    }).unwrap();
}
```

This program is a *dataflow program*. There are two dataflow operators here, `exchange` and `inspect`, each of which is asked to do a thing in response to input data. The `exchange` operator takes each datum and hands it to a downstream worker based on the value it sees; with two workers, one will get all the even numbers and the other all the odd numbers. The `inspect` operator takes an action for each datum, in this case printing something to the screen.

Importantly, we haven't imposed any constraints on how these operators need to run. We removed the code that caused the input to be delayed until a certain amount of progress had been made, and it shows in the results when we run with more than one worker:

```ignore
    Echidnatron% cargo run --example hello -- -w2
        Finished dev [unoptimized + debuginfo] target(s) in 0.0 secs
        Running `target/debug/examples/hello -w2`
    worker 1:	hello 1
    worker 1:	hello 3
    worker 0:	hello 0
    worker 1:	hello 5
    worker 0:	hello 2
    worker 1:	hello 7
    worker 1:	hello 9
    worker 0:	hello 4
    worker 0:	hello 6
    worker 0:	hello 8
    Echidnatron%
```

What a mess. Nothing in our dataflow program requires that workers zero and one alternate printing to the screen, and you can even see that worker one is *done* before worker zero even gets to printing `hello 4`.

---

However, this is only a mess if we are concerned about the order, and in many cases we are not. Imagine instead of just printing the number to the screen, we want to find out which numbers are prime and print *them* to the screen.

```rust,ignore
.inspect(|x| {
    // we only need to test factors up to sqrt(x)
    let limit = (*x as f64).sqrt() as u64;
    if *x > 1 && (2 .. limit + 1).all(|i| x % i > 0) {
        println!("{} is prime", x);
    }
})
```

 We don't really care that much about the order (we just want the results), and we have written such a simple primality test that we are going to be thrilled if we can distribute the work across multiple cores.

 Let's check out the time to print out the prime numbers up to 10,000 using one worker:

```ignore
    Echidnatron% time cargo run --example hello -- -w1 > output1.txt
        Finished dev [unoptimized + debuginfo] target(s) in 0.0 secs
        Running `target/debug/examples/hello -w1`
    cargo run --example hello -- -w1 > output1.txt  59.84s user 0.10s system 99% cpu 1:00.01 total
    Echidnatron%
```

And now again with two workers:

```ignore
    Echidnatron% time cargo run --example hello -- -w2 > output2.txt
        Finished dev [unoptimized + debuginfo] target(s) in 0.0 secs
        Running `target/debug/examples/hello -w2`
    cargo run --example hello -- -w2 > output2.txt  60.74s user 0.12s system 196% cpu 30.943 total
    Echidnatron%
```

The time is basically halved, from one minute to thirty seconds, which is a great result for those of us who like factoring small numbers. Furthermore, although the 1,262 lines of results of `output1.txt` and `output2.txt` are not in the same order, it takes a fraction of a second to make them so, and verify that they are identical:

```ignore
    Echidnatron% sort output1.txt > sorted1.txt
    Echidnatron% sort output2.txt > sorted2.txt
    Echidnatron% diff sorted1.txt sorted2.txt
    Echidnatron%
```

---

This is probably as good a time as any to tell you about Rust's `--release` flag. I haven't been using it up above to keep things simple, but adding the `--release` flag to cargo's arguments makes the compilation take a little longer, but the resulting program run a *lot* faster. Let's do that now, to get a sense for how much of a difference it makes:

```ignore
    Echidnatron% time cargo run --release --example hello -- -w1 > output1.txt
        Finished release [optimized] target(s) in 0.0 secs
        Running `target/release/examples/hello -w1`
    cargo run --release --example hello -- -w1 > output1.txt  0.78s user 0.06s system 96% cpu 0.881 total
    Echidnatron% time cargo run --release --example hello -- -w2 > output2.txt
        Finished release [optimized] target(s) in 0.0 secs
        Running `target/release/examples/hello -w2`
    cargo run --release --example hello -- -w2 > output2.txt  0.73s user 0.05s system 165% cpu 0.474 total
```

That is about a 60x speed-up. The good news is that we are still getting approximately a 2x speed-up going from one worker to two, but you can see that dataflow programming does not magically extract all performance from your computer.

This is also a fine time to point out that dataflow programming is not religion. There is an important part of our program up above that is imperative:

```ignore, rust
    let limit = (*x as f64).sqrt() as u64;
    if *x > 1 && (2 .. limit + 1).all(|i| x % i > 0) {
        println!("{} is prime", x);
    }
```

This is an imperative fragment telling the `inspect` operator what to do. We *could* write this as a dataflow fragment if we wanted, but it is frustrating to do so, and less efficient. The control flow fragment lets us do something important, something that dataflow is bad at: the `all` method above *stops* as soon as it sees a factor of `x`.

There is a time and a place for dataflow programming and for control flow programming. We are going to try and get the best of both.
