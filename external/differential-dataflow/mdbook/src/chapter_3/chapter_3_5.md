## Performing Work

All of the differential dataflow computation happens in what seems like a fairly small an unobtrusive operation:

```rust,no_run
    worker.step();
```

This call schedules each differential dataflow operator, and performs some amount of work they have outstanding. Repeated calls to this method will advance all of the work in the system, and should eventually bring output probes in-line with the times of the inputs on which they depend.

At the end of your differential dataflow computation, when we exit the closure supplied to timely dataflow, the system will call `worker.step()` until the computation completes. For this reason, it is safe to let workers who otherwise have nothing further to contribute to simply exit, as they will continue to participate until all other workers have completed as well.

For example, our first example computations didn't call `worker.step()` explicitly, but just exited once it supplied the input changes. Exiting causes all of the work to happen (and complete, as the inputs are automatically closed as they are dropped).

Explicit calls to `worker.step()` are important when we are maintaining interactive access to probes, and do not want to simply complete the computation.