## Differential Interactions

Once a computation is written, we have only to interact with it. At its heart, this reduces to changing the inputs to the computation and observing changes in the outputs. This can be very easy to do, but there is also intentionally great latitude to allow you to allow the system to perform more efficiently.

Our goal is to go in-order through each of the elements of the code from our interactive example.

```rust,no_run
    // make changes, but await completion.
    let mut person = index;
    while person < people {
        input.remove((person/2, person));
        input.insert((person/3, person));
        input.advance_to(person);
        input.flush();
        while probe.less_than(&input.time()) {
            worker.step();
        }
        person += peers;
    }
```

Each of these parts, more or less, do something interesting and important. There is also some flexibility in how they are used, which we will also try to highlight.