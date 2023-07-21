## Observing Probes

Probes are an important concept in timely dataflow, and they play the same role in differential dataflow.

Dataflow computations differ from imperative computations in that you do not *force* computation to happen, you must wait until it has happened. The `probe()` operator on collections returns a probe that can tell you at which times a collection may still experience changes.

For example, recall our example of interacting with our management computation, where we wrote

```rust,no_run
    // create a manager
    let probe = worker.dataflow(|scope| {

        // create a new collection from an input session.
        let manages = input.to_collection(scope);

        // if (m2, m1) and (m1, p), then output (m1, (m2, p))
        manages
            .map(|(m2, m1)| (m1, m2))
            .join(&manages)
            .probe()
    });
```

The returned probe allows us to ask whether the computation has stabilized to the point that there will be no more changes at certain query timestamps. We used the probe later on, when we wrote

```rust,no_run
    while probe.less_than(&input.time()) { worker.step(); }
```

This causes the dataflow worker to continue to run until such a point as there can be no more changes strictly less than the current input time (what we are about to introduce). At this point all changes introduced at strictly prior times must be fully resolved, as the probe tells us that no further changes at their time can appear in the output.
