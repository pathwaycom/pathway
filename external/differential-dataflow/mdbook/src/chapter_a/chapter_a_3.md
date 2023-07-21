## Increase the interaction.

Instead of loading all of our changes and only waiting for the result, we can load each change and await its results before supplying the next change. This requires a bit of timely dataflow magic, where we add a probe to the end of our dataflow:

```rust,ignore
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

We can then use this probe to limit the introduction of new data, by waiting for it to catch up with our input before we insert new data. For example, after we insert our initial data, we should wait until everyone has caught up.

```rust,ignore
    let mut person = worker.index();
    while person < size {
        input.insert((person/2, person));
        person += worker.peers();
    }

    // wait for data loading.
    input.advance_to(1);
    input.flush();
    while probe.less_than(&input.time()) { worker.step(); }
    println!("{:?}\tdata loaded", worker.timer().elapsed());
```

These four new lines are each important, especially the one that prints things out. The other three do a bit of magic that get timely dataflow to work for us until we are certain that inputs have been completely processed.

We can make the same changes for the interactive loading, but we'll synchronize the workers for each person they load.

```rust,ignore
    // make changes, but await completion.
    let mut person = 1 + worker.index();
    while person < size {
        input.remove((person/2, person));
        input.insert((person/3, person));
        input.advance_to(person);
        input.flush();
        while probe.less_than(&input.time()) { worker.step(); }
        println!("{:?}\tstep {} complete", worker.timer().elapsed(), person);
        person += worker.peers();
    }
```

This starts to print out a mess of data, indicating not only how long it takes to start up the computation, but also how long each individual round of updates takes.

```ignore
        Echidnatron% cargo run --release -- 10000000
            Finished release [optimized] target(s) in 0.24s
             Running `target/release/my_project 10000000`
        4.092895186s    data loaded
        4.092975626s    step 1 complete
        4.093021676s    step 2 complete
        4.093041130s    step 3 complete
        4.093110803s    step 4 complete
        4.093144075s    step 5 complete
        4.093187645s    step 6 complete
        4.093208245s    step 7 complete
        4.093236460s    step 8 complete
        4.093281793s    step 9 complete
```

which continues for quite a while.

```ignore
        21.689493445s   step 397525 complete
        21.689522815s   step 397526 complete
        21.689553410s   step 397527 complete
        21.689593500s   step 397528 complete
        21.689643055s   step 397529 complete
```

You can see that this is pretty prompt; the latencies are in the tens of microseconds.

You can also see that the whole computation is clearly going to take a bit longer. This is because we've forced some work to finish before we start the next work, which we haven't done before. We will explore later on how to trade off latency and throughput when we come to "open-loop" interaction.