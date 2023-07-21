## Increase the scale.

Ten people was a pretty small organization. Let's do ten million instead.

We are going to have to turn off the output printing here (comment out the `inspect`, but save the semicolon). We'll also need to add the `--release` flag to our command line, to avoid waiting forever.

We'll break down our computation two ways, first just loading up the initial computation, and second doing that plus all of the changes to the reporting structure. We haven't learned how to interactively load all of the input and await results yet (in just a moment), so we will only see elapsed times measuring the throughput, not the latency.

First, we produce the skip-level management (with the "change its input" code commented out).

```ignore
        Echidnatron% time cargo run --release -- 10000000
            Finished release [optimized] target(s) in 0.24s
             Running `target/release/my_project 10000000`
        cargo run --release my_project 10000000 -w1  2.74s user 1.00s system 98% cpu 3.786 total
        Echidnatron%
```

Four seconds. We have no clue if this is a good or bad time.

Second, we produce the skip-level management and then modify it 10 million times (as in "change its input").

```ignore
        Echidnatron% time cargo run --release -- 10000000
            Finished release [optimized] target(s) in 0.24s
             Running `target/release/my_project 10000000`
        cargo run --release my_project 10000000  10.64s user 2.22s system 99% cpu 12.939 total
        Echidnatron%
```

About thirteen seconds now.

That's less than a microsecond per modification (subtracting the loading time). Importantly, these are throughput measurements rather than latency numbers; we aren't actually doing the 10 million updates one after the other. But, if you compare this to a sequence of 10 million updates to a database, we would be pretty pleased with a microsecond per operation.