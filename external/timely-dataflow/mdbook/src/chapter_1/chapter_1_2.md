# Logical Timestamps

When dataflow programs move data around arbitrarily, it becomes hard to correlate the produced outputs with the supplied inputs. If we supply a stream of bank transactions as input, and the output is a stream of bank balances, how can we know which input transactions are reflected in which output balances?

The standard approach to this problem is to install *timestamps* on the data. Each record gets a logical timestamp associated with it that indicates *when* it should be thought to happen. This is not necessarily "when" in terms of the date, time, or specific nanosecond the record was emitted; a timestamp could simply be a sequence number identifying a batch of input records. Or, and we will get into the terrifying details later, it could be much more complicated than this.

Timestamps are what allow us to correlate inputs and outputs. When we introduce records with some logical timestamp, unless our dataflow computation changes the timestamps, we expect to see corresponding outputs with that same timestamp.

## An example

Remember from the dataflow section how when we remove the coordination from our `examples/hello.rs` program, the output was produced in some horrible order? In fact, each of those records had a timestamp associated with it that would reveal the correct order; we just weren't printing the timestamp because `inspect` doesn't have access to it.

Let's change the program to print out the timestamp with each record. This shouldn't be a very thrilling output, because the timestamp is exactly the same as the number itself, but that didn't have to be the case. We are just going to replace the line

```rust,ignore
.inspect(move |x| println!("worker {}:\thello {}", index, x))
```

with a slightly more complicated operator, `inspect_batch`.

```rust,ignore
.inspect_batch(move |t,xs| {
    for x in xs.iter() {
        println!("worker {}:\thello {} @ {:?}", index, x, t)
    }
})
```

The `inspect_batch` operator gets lower-level access to data in timely dataflow, in particular access to batches of records with the same timestamp. It is intended for diagnosing system-level details, but we can also use it to see what timestamps accompany the data.

The output we get with two workers is now:

```ignore
    Echidnatron% cargo run --example hello -- -w2
        Finished dev [unoptimized + debuginfo] target(s) in 0.0 secs
        Running `target/debug/examples/hello -w2`
    worker 1:	hello 1 @ (Root, 1)
    worker 1:	hello 3 @ (Root, 3)
    worker 1:	hello 5 @ (Root, 5)
    worker 0:	hello 0 @ (Root, 0)
    worker 0:	hello 2 @ (Root, 2)
    worker 0:	hello 4 @ (Root, 4)
    worker 0:	hello 6 @ (Root, 6)
    worker 0:	hello 8 @ (Root, 8)
    worker 1:	hello 7 @ (Root, 7)
    worker 1:	hello 9 @ (Root, 9)
    Echidnatron%
```

The timestamps are the `(Root, i)` things for various values of `i`. These happen to correspond to the data themselves, but had we provided random input data rather than `i` itself we would still be able to make sense of the output and put it back "in order".

## Timestamps for dataflow operators

Timestamps are not only helpful for dataflow users, but for the operators themselves. With time we will start to write more interesting dataflow operators, and it may be important for them to understand which records should be thought to come before others.

Imagine, for example, a dataflow operator whose job is to report the "sum so far", where "so far" should be with respect to the timestamp (as opposed to whatever arbitrary order the operator receives the records). Such an operator can't simply take its input records, add them to a total, and produce the result. The input records may no longer be ordered by timestamp, and the produced summations may not reflect any partial sum of the input. Instead, the operator needs to look at the timestamps on the records, and incorporate the numbers in order of their timestamps.

Of course, such an operator works great as long as it expects exactly one record for each timestamp. Things get harder for it if it might receive multiple records at each timestamp, or perhaps none. To address this, the underlying system will have to help the operator reason about the progress of its input, up next.
