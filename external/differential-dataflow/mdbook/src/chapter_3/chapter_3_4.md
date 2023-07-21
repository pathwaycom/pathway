## Advancing Time

Differential dataflow will perform relatively little work until it believes it has enough information to produce the correct answer. This involves you supplying input changes, but just as importantly it involves you promising to *stop* changing the collection.

The `InputSession` type provides a method `advance_to(time)`, which moves the internal time of the session forward to `time`, and prevents you from supplying input changes at times that are not greater or equal to `time`. This is a very strong statement made to the differential dataflow infrastructure that you have stopped changing this input at all times not greater or equal to this input time, and the system can now start to make progress determining the corresponding output changes.

Crucially, the calls to `advance_to` will be buffered until you call `input.flush()`, which exposes this information to the underlying timely dataflow system. This allows you to call `advance_to` as frequently as once per record, without overwhelming the underlying system.

**IMPORTANT**: Before expecting differential and timely dataflow to actually make forward progress, make sure you have advanced and flushed your inputs.

This is a classic source of error, still made by yours truly, that cause a computation to appear to have hung. In fact, the computation is almost always correct, and simply cannot make progress if you have held back the information that an input has stopped changing at some input times.

### Temporal Concurrency

You do not need to call `flush()` each time you call `advance_to(time)`.

Calls to `advance_to` change the logical time at which a change occurs, but you are welcome to buffer up many of these changes and the call flush only once, allowing differential dataflow to process the sequence of changes concurrently. This can greatly improve the throughput, often at a nominal affect on latency.

We will see this more clearly when we investigate the example application of real-time streaming input.