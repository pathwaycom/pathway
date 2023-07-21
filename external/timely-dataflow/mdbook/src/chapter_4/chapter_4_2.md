# Iteration

One of the neatest things you can do in timely dataflow is iteration. It is a consequence of how timely dataflow tracks progress that we can build loops, circulate data within the loop, and still allow downstream operators to properly judge the progress of their input streams.

There is nothing special about iterative scopes. They are just timely dataflow scopes. In fact, we could even introduce cycles into the root scope that all `dataflow` calls provide.

What timely dataflow provides is a special stream called a `LoopVariable`. This is a stream whose contents are not yet defined. You can start using the stream as if they were, but they won't be until later on in the dataflow when we define them.

That may be a bit abstract, so let's look at a simple example.

We are going to check the [Collatz conjecture](https://en.wikipedia.org/wiki/Collatz_conjecture), which says that if you repeatedly divide even numbers by two, and multiply odd numbers by three and add one, you eventually reach the number one. We could do this in lots of ways, but this is the timely dataflow way to do it.

```rust
extern crate timely;

use timely::dataflow::operators::*;

fn main() {
    timely::example(|scope| {

        // create a loop that cycles unboundedly.
        let (handle, stream) = scope.feedback(1);

        // circulate numbers, Collatz stepping each time.
        (1 .. 10)
            .to_stream(scope)
            .concat(&stream)
            .map(|x| if x % 2 == 0 { x / 2 } else { 3 * x + 1 } )
            .inspect(|x| println!("{:?}", x))
            .filter(|x| *x != 1)
            .branch_when(|t| t < &100).1
            .connect_loop(handle);
    });
}
```

This program first creates a loop variable, using the `feedback` method on scopes. This method comes from the `Feedback` extension trait in `dataflow::operators`, in case you can't find it. When we create a new loop variable, we have to tell timely dataflow by how much we should increment the timestamp each time around the loop. To be more specific, we have to give a path summary which often is just a number that tells us by how much to increment the timestamp. When we later connect the output of an operation back to this loop variable we can specify an upper bound on the number of iterations by using the `branch_when` method.

We start with a stream of the numbers from one through nine, because we have to start somewhere. Our plan is to repeatedly apply the Collatz step, and then discard any numbers equal to one, but we want to apply this not only to our input but also to whatever comes back around our loop variable. So, the very first step is to `concat` our input stream with the feedback stream. Then we can apply the Collatz step, filter out the ones, and then connect the resulting stream as the definition of the feedback stream.

We've built an upper limit of `100` in so that we don't spin out of control, in case the conjecture is false. It turns out that `9` will take 19 steps to converge, so this should be good enough. You could try it out for larger numbers!

## Mutual Recursion

You can have as many loop variables as you want!

Perhaps you are a very clever person, and you've realized that we don't need to test the results of odd numbers to see if they are one, which would be easy if we broke our stream apart into streams of even and odd numbers. We can do that, by having two loop variables, one for even (`0`) and one for odd (`1`).

```rust
extern crate timely;

use timely::dataflow::operators::*;

fn main() {
    timely::example(|scope| {

        // create a loop that cycles unboundedly.
        let (handle0, stream0) = scope.feedback(1);
        let (handle1, stream1) = scope.feedback(1);

        // do the right steps for even and odd numbers, respectively.
        let results0 = stream0.map(|x| x / 2).filter(|x| *x != 1);
        let results1 = stream1.map(|x| 3 * x + 1);

        // partition the input and feedback streams by even-ness.
        let parts =
            (1 .. 10)
                .to_stream(scope)
                .concat(&results0)
                .concat(&results1)
                .inspect(|x| println!("{:?}", x))
                .partition(2, |x| (x % 2, x));

        // connect each part appropriately.
        parts[0].connect_loop(handle0);
        parts[1].connect_loop(handle1);
    });
}
```

This is a different way to do the same computation (similar; because we've moved input from the head of the dataflow, the limit of `100` is effectively reduced by one). I won't say it is a better way to do the same computation, but it is different.

## Scopes

Of course, you can do all of this in a nested scope, if that is appropriate. In the example above, we just used the ambient `u64` timestamp from `timely::example`, but perhaps you were hoping that it would correspond to streamed input, or something else. We just need to introduce a new nested scope, in that case.

```rust
extern crate timely;

use timely::dataflow::operators::*;
use timely::dataflow::Scope;

fn main() {
    timely::example(|scope| {

        let input = (1 .. 10).to_stream(scope);

        // Create a nested iterative scope.
        // Rust needs help understanding the iteration counter type.
        scope.iterative::<u64,_,_>(|subscope| {

            let (handle, stream) = subscope.loop_variable(1);

            input
                .enter(subscope)
                .concat(&stream)
                .map(|x| if x % 2 == 0 { x / 2 } else { 3 * x + 1 } )
                .inspect(|x| println!("{:?}", x))
                .filter(|x| *x != 1)
                .connect_loop(handle);
        })

    });
}
```

We could also pop out the results with `leave` if we had any.

**Exercise**: Rewrite the above example so that while iterating each tuple tracks (i) its progenitor number, and (ii) the number of times it has gone around the loop, and rather than discarding tuples whose value is now one they `leave` the loop and report for each input number how many steps it takes to reach one.
