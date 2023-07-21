# Operator Execution

Perhaps the most important statement in a timely dataflow program:

```rust,ignore
worker.step()
```

This is the method that tells the worker that it is now a good time to schedule each of the operators. If you recall, when designing our dataflow we wrote these operators, each of which were programmed by what they would do when shown their input and output handles. This is where we run that code.

The `worker.step()` call is the heart of data processing in timely dataflow. The system will do a swing through each dataflow operator and call in to its closure once. Each operator has the opportunity to drain its input and produce some output, and depending on how they are coded they may do just that.

Importantly, this is also where we start moving data around. Until we call `worker.step()` all data are just sitting in queues. The parts of our computation that do clever things like filtering down the data, or projecting out just a few small fields, or pre-aggregating the data before we act on it, these all happen here and don't happen until we call this.

Make sure to call `worker.step()` now and again, like you would your parents.
