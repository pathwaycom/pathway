## Real-time streaming input

Our examples so far have involved careful manipulation of the input, making changes and advancing time. What happens when we want to connect all of this to an external source that can change at arbitrary rates, and which might not wait for us to complete some work before issuing new changes?

Imagine an external data source that we can poll for changes, and when polled responds with all outstanding changes and the logical times at which each occurred. There is a fairly natural pattern we can write that exposes these changes to differential dataflow and asks it to resolve all changes concurrently, while retaining the logical times of each of the input changes.

```rust,no_run
    while !source.done() {
        // fetch a bounded amount of input changes.
        for (data, time, diff) in source.fetch() {
            input.update_at(data, time, diff);
        }
        // advance time to the source guarantee.
        let time = source.low_watermark();
        input.advance_to(time);
        input.flush();
        worker.step();
    }
```

This pattern repeatedly extracts changes from some source of change, perhaps a streaming source like Kafka, or perhaps a socket connection that you have set up to play back a timely stream. It introduces all of the changes, and then advances the input's time to match whatever the source guarantees it will no longer change.

Importantly, we don't know ahead of time what these data will be, nor even which times will we `advance_to`. These are results of the volume of streamed input changes and the amount of time spent in `worker.step()`.

Under heavy input load differential dataflow may take more time to do its work, and we would expect larger batches of input to result. All components of differential dataflow have been designed so that the throughput *improves* as batch size increases, which means that the system should adapt its batch size to match the offered load. As load increases both batch size and latency will increase; as the load reduces the batch size and latency will decrease again.
