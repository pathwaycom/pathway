## Step 3: Make things more exciting.

We are going to make our example program a bit more exciting, in a few different ways.

### Increase the scale.

Ten people is a pretty small organization. Let's do ten million instead.

We are going to have to turn off the output printing here, so comment out the `inspect()` line (but keep the semicolon). Also, we'll need to add the `--release` flag to our command line, so that we optimize our binary and don't try running debug code for millions of steps.

We'll break down the results of our modified computation two ways, just loading up the initial computation, and then doing that plus all of the changes to the reporting structure. We haven't learned how to interactively load all of the input and await results yet (in just a moment), so we will only see elapsed times measuring the throughput, not the latency.

First, if we just produce the collection of skip-level management (with the step two code from before):

    Echidnatron% time cargo run --release -- 10000000
    cargo run --release --example hello 10000000 -w1  2.74s user 1.00s system 98% cpu 3.786 total
    Echidnatron%

Four seconds. We have no clue if this is a good or bad time.

Second, if we produce the skip-level management and then modify it 10 million times (including the step two code from before):

    Echidnatron% time cargo run --release -- 10000000
    cargo run --release --example hello 10000000  10.64s user 2.22s system 99% cpu 12.939 total
    Echidnatron%

About thirteen seconds now. Just over a microsecond per modification, though these are throughput rather than latency numbers.

### Increase the parallelism.

Differential dataflow works great using multiple threads. Produces the same output as with one thread and everything.

For this to work out, we'll want to ask each worker to load up a fraction of the input. Each timely dataflow worker has methods `index()` and `peers()`, which indicate the workers number and out of how many total workers. We can load our input up like so:

```rust,no_run
    let mut person = worker.index();
    while person < people {
        input.insert((person/2, person));
        person += worker.peers();
    }

```

We can also make the same changes to the code that supplies the change, where each worker is responsible for those people whose number equals `worker.index()` modulo `worker.peers()`.

I'm on a laptop with two cores. Let's load the data again, without modifying it, but let's use two worker threads (with the `-w2` argument)

    Echidnatron% time cargo run --release -- 10000000 -w2
    cargo run --release --example hello 10000000 -w2  3.34s user 1.27s system 191% cpu 2.402 total
    Echidnatron%

Now let's try loading and doing ten million modifications, but with two worker threads.

    Echidnatron% time cargo run --release -- 10000000 -w2
    cargo run --release --example hello 10000000 -w2  13.06s user 3.14s system 196% cpu 8.261 total
    Echidnatron%

Each of these improve on the single-threaded execution (they do more total work, because). Perhaps amazingly, they even improve the case where we need to do ten million *sequential* modifications. We get exactly the same answer, too.

### Increase the interaction.

Instead of loading all of our changes and only waiting for the result, we can load each change and await its results before supplying the next change. This requires a bit of timely dataflow magic, where we add a probe to the end of our dataflow:

```rust,no_run
    // create a manager
    let probe = worker.dataflow(|scope| {

        // create a new collection from an input session.
        let manages = input.to_collection(scope);

        // if (m2, m1) and (m1, p), then output (m1, (m2, p))
        manages
            .map(|(m2, m1)| (m1, m2))
            .join(&manages)
            // .inspect(|x| println!("{:?}", x))
            .probe()
    });
```

We can then use this probe to limit the introduction of new data, by waiting for it to catch up with our input before we insert new data:

```rust,no_run
    // wait for data loading.
    input.advance_to(1); input.flush();
    while probe.less_than(&input.time()) { worker.step(); }
    println!("{:?}\tdata loaded", timer.elapsed());

    // make changes, but await completion.
    let mut person = 1 + index;
    while person < people {
        input.remove((person/2, person));
        input.insert((person/3, person));
        input.advance_to(person); input.flush();
        person += peers;
        while probe.less_than(&input.time()) { worker.step(); }
        println!("{:?}\tstep {} complete", timer.elapsed(), person);
    }
```

This starts to print out a mess of data, indicating not only how long it takes to start up the computation, but also how long each individual round of updates takes.

    Echidnatron% cargo run --release --example hello 10000000
        Finished release [optimized + debuginfo] target(s) in 0.06s
         Running `target/release/examples/hello 10000000`
    4.092895186s    data loaded
    4.092975626s    step 2 complete
    4.093021676s    step 3 complete
    4.093041130s    step 4 complete
    4.093110803s    step 5 complete
    4.093144075s    step 6 complete
    4.093187645s    step 7 complete
    4.093208245s    step 8 complete
    4.093236460s    step 9 complete
    4.093281793s    step 10 complete

which continues for quite a while.

    21.689493445s   step 397525 complete
    21.689522815s   step 397526 complete
    21.689553410s   step 397527 complete
    21.689593500s   step 397528 complete
    21.689643055s   step 397529 complete

You can see that this is pretty prompt; the latencies are in the tens of microseconds, but also that the whole computation is clearly going to take a bit longer. This is because we've forced some work to finish before we start the next work, which we haven't done before.