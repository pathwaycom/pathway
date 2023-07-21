## Increase the parallelism.

Differential dataflow works great using multiple threads and computers. It even produces the same output and everything.

For this to work out, we'll want to ask each worker to load up a fraction of the input. If we just run the same code with multiple workers, then each of the workers will run

```rust,ignore
    for person in 0 .. size {
        input.insert((person/2, person));
    }
```

and each will insert the entire input collection. We don't want that!

Instead, each timely dataflow worker has methods `index()` and `peers()`, which indicate the workers number and out of how many total workers. We can change the code so that each worker only loads their fraction of the input, like so:

```rust,ignore
    let mut person = worker.index();
    while person < size {
        input.insert((person/2, person));
        person += worker.peers();
    }
```

We can also make the same changes to the code that supplies the change, where each worker is responsible for those people whose number equals `worker.index()` modulo `worker.peers()`.

```rust,ignore
    let mut person = worker.index();
    while person < size {
        input.remove((person/2, person));
        input.insert((person/3, person));
        input.advance_to(person);
        person += worker.peers();
    }
```

I'm on a laptop with two cores. Let's load the data again, without modifying it, but let's use two worker threads (with the `-w2` argument)

```ignore
        Echidnatron% time cargo run --release -- 10000000 -w2
            Finished release [optimized] target(s) in 0.24s
             Running `target/release/my_project 10000000 -w2`
        cargo run --release -- 10000000 -w2  3.34s user 1.27s system 191% cpu 2.402 total
        Echidnatron%
```

Now let's try loading and doing ten million modifications, but with two worker threads.

```ignore
        Echidnatron% time cargo run --release -- 10000000 -w2
            Finished release [optimized] target(s) in 0.24s
             Running `target/release/my_project 10000000 -w2`
        cargo run --release -- 10000000 -w2  13.06s user 3.14s system 196% cpu 8.261 total
        Echidnatron%
```

Each of these improve on the single-threaded execution (they do more total work, because). Perhaps amazingly, they even improve the case where we need to do ten million *sequential* modifications. We get exactly the same answer, too.
