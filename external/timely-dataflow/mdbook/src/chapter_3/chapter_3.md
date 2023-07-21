# Running Timely Dataflows

In this section we will look at driving a timely dataflow computation.

With a dataflow graph defined, how do we interactively supply data to the computation, and how do we understand what the computation has actually done given that we are not ourselves doing it? These are good questions, and the dataflow execution model is indeed a bit of a departure from how most folks first experience programming.

The first thing to understand about timely dataflow is that *we are programming the worker threads*. Part of this program is asking the worker to build up a dataflow graph; we did that when we created an `InputHandle` and when we called `dataflow` followed by some `filter` and `map` and `probe` commands. But another part is where we actually start feeding the dataflow graph, advancing the inputs, and letting the worker give each of the operators a chance to run.

```rust,ignore
for round in 0..10 {
    input.send(round);
    input.advance_to(round + 1);
    while probe.less_than(input.time()) {
        worker.step();
    }
}
```

This is the loop that we've seen in several examples. It looks fairly simple, but this is what actually causes work to happen. We do send data and advance the input, but we also call `worker.step()`, and this is where the actual timely dataflow computation happens. Until you call this, all the data are just building up in queues.

In this section, we'll look at these moving parts in more detail.
