# Introducing Operators

In between introducing streams of data and inspecting or capturing the output, we'll probably want to do some computation on those data. There are a lot of things that you can do, and timely comes with a set of generally useful operators built in. We will survey a few of these, but this list will be necessarily incomplete: the operators are pretty easy to write, and keep showing up.

## Mapping

One of the simplest things one can do with a stream of data is to transform each record into a new record. In database terminology this would be called "projection", where you extract some fields from a larger record, but as we are in a more rich programming language we can perform arbitrary transformations.

The `map` operator takes as an argument a closure from the input data type to an output data type that you get to define. The result is the stream of records corresponding to the application of your closure to each record in the input stream.

The following program should print out the numbers one through ten.

```rust
extern crate timely;

use timely::dataflow::operators::{ToStream, Inspect, Map};

fn main() {
    timely::execute_from_args(std::env::args(), |worker| {
        worker.dataflow::<(),_,_>(|scope| {
            (0 .. 9)
                .to_stream(scope)
                .map(|x| x + 1)
                .inspect(|x| println!("hello: {}", x));
        });
    }).unwrap();
}
```

The closure `map` takes *owned* data as input, which means you are able to mutate it as you like without cloning or copying the data. For example, if you have a stream of `String` data, then you could upper-case the string contents without having to make a second copy; your closure owns the data that comes in, with all the benefits that entails.

```rust
extern crate timely;

use timely::dataflow::operators::{ToStream, Inspect, Map};

fn main() {
    timely::execute_from_args(std::env::args(), |worker| {
        worker.dataflow::<(),_,_>(|scope| {
            (0 .. 9)
                .to_stream(scope)
                .map(|x| x.to_string())
                .map(|mut x| { x.truncate(5); x } )
                .inspect(|x| println!("hello: {}", x));
        });
    }).unwrap();
}
```

### Map variants

There are a few variants of `map` with different functionality.

For example, the `map_in_place` method takes a closure which receives a mutable reference and produces no output; instead, this method allows you to change the data *in-place*, which can be a valuable way to avoid duplication of resources.

```rust
extern crate timely;

use timely::dataflow::operators::{ToStream, Inspect, Map};

fn main() {
    timely::execute_from_args(std::env::args(), |worker| {
        worker.dataflow::<(),_,_>(|scope| {
            (0 .. 9)
                .to_stream(scope)
                .map(|x| x.to_string())
                .map_in_place(|x| x.truncate(5))
                .inspect(|x| println!("hello: {}", x));
        });
    }).unwrap();
}
```

Alternately, the `flat_map` method takes input data and allows your closure to transform each element to an iterator, which it then enumerates into the output stream. The following fragment takes each number from zero through eight and has each produce all numbers less than it. The result should be 8 zeros, 7 ones, and so on up to 1 seven.

```rust
extern crate timely;

use timely::dataflow::operators::{ToStream, Inspect, Map};

fn main() {
    timely::execute_from_args(std::env::args(), |worker| {
        worker.dataflow::<(),_,_>(|scope| {
            (0 .. 9)
                .to_stream(scope)
                .flat_map(|x| 0 .. x)
                .inspect(|x| println!("hello: {}", x));
        });
    }).unwrap();
}
```

## Filtering

Another fundamental operation is *filtering*, in which a predicate dictates a subset of the stream to retain.

```rust
extern crate timely;

use timely::dataflow::operators::{ToStream, Inspect, Filter};

fn main() {
    timely::execute_from_args(std::env::args(), |worker| {
        worker.dataflow::<(),_,_>(|scope| {
            (0 .. 9)
                .to_stream(scope)
                .filter(|x| *x % 2 == 0)
                .inspect(|x| println!("hello: {}", x));
        });
    }).unwrap();
}
```

Unlike `map`, the predicate passed to the `filter` operator does not receive *owned* data, but rather a reference to the data. This allows `filter` to observe the data to determine whether to keep it, but not to change it.

## Logical Partitioning

There are two operators for splitting and combining streams, `partition` and `concat` respectively.

The `partition` operator takes two arguments, a number of resulting streams to produce, and a closure which takes each record to a pair of the target partition identifier and the output record. The output of `partition` is a list of streams, where each stream contains those elements mapped to the stream under the closure.

```rust
extern crate timely;

use timely::dataflow::operators::{ToStream, Partition, Inspect};

fn main() {
    timely::example(|scope| {
        let streams = (0..10).to_stream(scope)
                             .partition(3, |x| (x % 3, x));

        streams[0].inspect(|x| println!("seen 0: {:?}", x));
        streams[1].inspect(|x| println!("seen 1: {:?}", x));
        streams[2].inspect(|x| println!("seen 2: {:?}", x));
    });
}
```

This example breaks the input stream apart into three logical streams, which are then subjected to different `inspect` operators. Importantly, `partition` only *logically* partitions the data, it does not move the data between workers. In the example above, each worker partitions its stream into three parts and no data are exchanged at all (as `inspect` does not require that of its inputs).

In the other direction, `concat` takes two streams and produces one output stream containing elements sent along either. The following example merges the partitioned streams back together.

```rust
extern crate timely;

use timely::dataflow::operators::{ToStream, Partition, Concat, Inspect};

fn main() {
    timely::example(|scope| {
        let streams = (0..10).to_stream(scope)
                             .partition(3, |x| (x % 3, x));
        streams[0]
            .concat(&streams[1])
            .concat(&streams[2])
            .inspect(|x| println!("seen: {:?}", x));
    });
}
```

There is also a `concatenate` method defined for scopes which collects all streams from a supplied vector, effectively undoing the work of `partition` in one operator.

```rust
extern crate timely;

use timely::dataflow::operators::{ToStream, Partition, Concatenate, Inspect};

fn main() {
    timely::example(|scope| {
        let streams = (0..10).to_stream(scope)
                             .partition(3, |x| (x % 3, x));

        scope.concatenate(streams)
             .inspect(|x| println!("seen: {:?}", x));
    });
}
```

Both `concat` and `concatenate` are efficient operations that exchange no data between workers, operate only on batches of stream elements, and do not make further copies of the data.

## Physical Partitioning

To complement the logical partitioning of `partition`, timely also provides the physical partitioning operator `exchange` which routes records to a worker based on a supplied closure. The `exchange` operator does not change the contents of the stream, but rather the distribution of elements to the workers. This operation can be important if you would like to collect records before printing statistics to the screen, or otherwise do some work that requires a specific data distribution.

Operators that require a specific data distribution will ensure that this occurs as part of their definition. As the programmer, you should not need to invoke `exchange`.

There are times where `exchange` can be useful. For example, if a stream is used by two operators requiring the same distribution, simply using the stream twice will cause duplicate data exchange as each operator satisfies its requirements. Instead, it may make sense to invoke `exchange` to move the data once, at which point the two operators will no longer require serialization and communication to shuffle their inputs appropriately.

## Other operators

There are any number of other operators, most of which you should be able to find in the `timely::dataflow::operators` module. Scanning through the documentation for this module may lead you to operators that you need, and alternately their implementations may demonstrate how to *construct* similar operators, if the one you require is not present. Operator construction is the subject of the next section!
