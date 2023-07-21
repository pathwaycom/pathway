# Scopes

The first bit of complexity we will introduce is the timely dataflow *scope*. A scope is a region of dataflow, much like how curly braces provide scopes in imperative languages:

```rust
{
    // this is an outer scope

    {
        // this is an inner scope
    }
}
```

You can create a new scope in any other scope by invoking the `scoped` method:

```rust
extern crate timely;

use timely::dataflow::Scope;

fn main() {
    timely::example(|scope| {

        // Create a new scope with the same (u64) timestamp.
        scope.scoped::<u64,_,_>("SubScope", |subscope| {
            // probably want something here
        })

    });
}
```

The main thing that a scope does for you is allow you to enrich the timestamp of all streams that are brought into it. For example, perhaps the timestamp type in your base dataflow scope is `usize`, your nested scope could augment this to `(usize, u32)`, or whatever additional timestamp type you might want. In the example above, we choose to keep the same timestamp, `u64`.

Why are additional timestamps useful? They allow operators in the nested scope to change their behavior based on the enriched timestamp, without worrying the streams and operators outside the scope about this detail. We will soon see the example of *iterative* dataflow, where a scope allows cycles within it, but does not bother operators outside the scope with that detail.

## Entering and exiting scopes

In addition to *creating* scopes, we will also need to get streams of data into and out of scopes.

There are two simple methods, `enter` and `leave`, that allow streams of data into and out of scopes. It is important that you use them! If you try to use a stream in a nested scope, Rust will be confused because it can't get the timestamps of your streams to typecheck.

```rust
extern crate timely;

use timely::dataflow::Scope;
use timely::dataflow::operators::*;

fn main() {
    timely::example(|scope| {

        let stream = (0 .. 10).to_stream(scope);

        // Create a new scope with the same (u64) timestamp.
        let result = scope.scoped::<u64,_,_>("SubScope", |subscope| {
            stream.enter(subscope)
                  .inspect_batch(|t, xs| println!("{:?}, {:?}", t, xs))
                  .leave()
        });

    });
}
```

Notice how we can both `enter` a stream and `leave` in a single sequence of operations.

The `enter` operator introduces each batch of records as a batch with an enriched timestamp, which usually means "the same" or "with a new zero coordinate". The `leave` just de-enriches the timestamp, correspondingly "the same" or "without that new coordinate". The `leave` operator results in a stream fit for consumption in the containing scope.

## Regions

There is a handy shortcut for scopes that do not change the timestamp type, a `region`.

```rust
extern crate timely;

use timely::dataflow::Scope;
use timely::dataflow::operators::*;

fn main() {
    timely::example(|scope| {

        let stream = (0 .. 10).to_stream(scope);

        // Create a new scope with the same (u64) timestamp.
        let result = scope.region(|subscope| {
            stream.enter(subscope)
                  .inspect_batch(|t, xs| println!("{:?}, {:?}", t, xs))
                  .leave()
        });

    });
}
```

Regions are mostly here to help you organize your dataflow. A region presents itself to its surrounding dataflow as a single operator, and that can make it easier for you or others to reason about the logical structure of your dataflow.

**IMPORTANT** Although you can probably write a program that uses `region()` but then skips the calls to `enter` and `leave`, because the timestamp types are the same, this is very naughty and please do not do this.

## Iteration

Iteration is a particularly neat form of nested scope in which all timestamps are enriched with an iteration counter. This counter starts at zero for streams that `enter` the scope, may increment in the context of the loop (next section gives examples of this), and is removed when the `leave` method is called.

The enriched timestamp type is `timely::order::Product<TOuter, TInner>`, for an outer timestamp type `TOuter` and an iteration counter type `TInner`. However, there is a convenience method that can allow you to skip thinking too hard about this.

```rust
extern crate timely;

use timely::dataflow::Scope;
use timely::dataflow::operators::*;

fn main() {
    timely::example(|scope| {

        let stream = (0 .. 10).to_stream(scope);

        // Create a new scope with a (u64, u32) timestamp.
        let result = scope.iterative::<u32,_,_>(|subscope| {
            stream.enter(subscope)
                  .inspect_batch(|t, xs| println!("{:?}, {:?}", t, xs))
                  .leave()
        });

    });
}
```