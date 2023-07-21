# Monitoring Probes

On the flip side of inputs we have probes. Probes aren't *outputs* per se, but rather ways for you to monitor progress. They report on the possible timestamps remaining at certain places in the dataflow graph (wherever you attach them).

The easiest way to create a `ProbeHandle` is by calling `.probe()` on a stream. This attaches a probe to a point in the dataflow, and when you inspect the probe (in just a moment) you'll learn about what might happen at that point.

You can also create a `ProbeHandle` directly with `ProbeHandle::new()`. Such a probe handle is not very interesting yet, but you can attach a probe handle by calling `.probe_with(&mut handle)` on a stream. This has the cute benefit that you can attach one probe to multiple parts of the dataflow graph, and it will report on the union of their times. If you would like to watch multiple outputs, you could call `.probe()` multiple times, or attach one common handle to each with multiple calls to `.probe_with()`. Both are reasonable, depending on whether you need to distinguish between the multiple locations.

A probe handle monitors information that timely provides about the availability of timestamps. You can think of it as holding on to a `Vec<Time>`, where any possible future time must be greater or equal to one of the elements in the list.

There are a few convenience methods provided, which allow helpful access to the state a probe handle wraps:

1. The `less_than(&Time)` method returns true if a time strictly less than the argument is possible.
2. The `less_equal(&Time)` method returns true if a time less or equal to the argument is possible.
3. The `done()` method returns true if no times are possible.

Probe handles also have a `with_frontier` method that allows you to provide a closure that can observe the frontier and return arbitrary results. This is a bit of a song and dance, because the frontier is shared mutable state and cannot be trivially handed back up to your code without peril (you would gain a `RefMut` that may cause the system to panic if you do not drop before calling `worker.step()`).

The most common thing to do with a probe handle is to check whether we are "caught up" to the input times. The following is a very safe idiom for doing this:

```rust,ignore
probe.less_than(input.time())
```

This checks if there are any times strictly less than what the input is positioned to provide next. If so, it means we could keep doing work and making progress, because we know that the system *could* catch up to `input.time()` as we can't produce anything less than this from `input`.

However, you are free to use whatever logic you like. Perhaps you just want to check this test a few times, rather than iterating for as long as it is true (which we commonly do). This would give the dataflow a chance to catch up, but it would start work on the next batch of data anyhow, to keep things moving along. There is a trade-off between overloading the system (if you provide data faster than you can retire it) and underloading it by constantly waiting rather than working.
