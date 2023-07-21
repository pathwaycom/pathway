# A Worked Example

You may have heard of `word_count` as the archetypical "big data" problem: you have a large collection of text, and what you want most in life is to know how many of each word are present in the text. The data are too large to load into memory, but let's assume that the set of distinct words, each with an associated count, is small enough to fit in memory.

Let's take the `word_count` example in the streaming direction. For whatever reason, your collection of text *changes*. As time moves along, some new texts are added and some old texts are retracted. We don't know why this happens, we just get told about the changes. Our new job is to *maintain* the `word_count` computation, in the face of arbitrary changes to the collection of texts, as promptly as possible.

Let's model a changing corpus of text as a list of pairs of *times* which will be `u64` integers with a list of *changes* which are each pairs `(String, i64)` indicating the text and whether it has been added (+1) or removed (-1).

We are going to write a program that is the moral equivalent of the following sequential Rust program:

```rust
/// From a sequence of changes to the occurrences of text,
/// produce the changing counts of words in that text.
fn word_count(mut history: Vec<(u64, Vec<(String, i64)>)>) {
    let mut counts = ::std::collections::HashMap::new();
    for (time, mut changes) in history.drain(..) {
        for (text, diff) in changes.drain(..) {
            for word in text.split_whitespace() {
                let mut entry = counts.entry(word.to_owned())
                                      .or_insert(0i64);
                *entry += diff;
                println!("seen: {:?}", (word, *entry));
            }
        }
    }
}
```

This program is fairly straightforward; hopefully you understand its intent, even if you aren't familiar with every method and type. However, the program is also very specific about what must happen: we process the history in order, and for each time we process the text changes in order. The program does not allow for any flexibility here.

Our program will be a bit larger, but it will be more flexible. By specifying more about what we want to happen to the data, and less about which order this needs to happen, we will gain the ability to scale out to multiple workers across multiple machines.

## Starting out with text streams

Let's first build a timely computation into which we can send text and which will show us the text back. Our next steps will be to put more clever logic in place, but let's start here to get some boiler-plate out of the way.

```rust
extern crate timely;

use timely::dataflow::{InputHandle, ProbeHandle};
use timely::dataflow::operators::{Inspect, Probe};

fn main() {
    // initializes and runs a timely dataflow.
    timely::execute_from_args(std::env::args(), |worker| {

        // create input and output handles.
        let mut input = InputHandle::new();
        let mut probe = ProbeHandle::new();

        // build a new dataflow.
        worker.dataflow(|scope| {
            input.to_stream(scope)
                 .inspect(|x| println!("seen: {:?}", x))
                 .probe_with(&mut probe);
        });

        // feed the dataflow with data.
        for round in 0..10 {
            input.send(("round".to_owned(), 1));
            input.advance_to(round + 1);
            while probe.less_than(input.time()) {
                worker.step();
            }
        }
    }).unwrap();
}
```

This example code is pretty close to a minimal non-trivial timely dataflow computation. It explains how participating timely workers (there may be many, remember) should construct and run a timely dataflow computation.

After some boiler-plate including the `timely` crate and some of its traits and types, we get to work:

```rust,ignore
        // create input and output handles.
        let mut input = InputHandle::new();
        let mut probe = ProbeHandle::new();
```

The input handle is how we supply data to the computation, and the probe handle is how we check whether the computation is complete up through certain inputs. Since a streaming computation may never "finish", `probe` is the only way to understand how much progress we've made.

The next step is to build a timely dataflow. Here we use `input` as a source of data, and attach `probe` to the end so that we can watch for completion of work.

```rust,ignore
        // build a new dataflow.
        worker.dataflow(|scope| {
            input.to_stream(scope)
                 .inspect(|x| println!("seen: {:?}", x))
                 .probe_with(&mut probe);
        });
```

This computation is pretty simple: it just prints out the inputs we send at it.

Having constructed the dataflow, we feed it some data.

```rust,ignore
        // feed the dataflow with data.
        for round in 0..10 {
            input.send(("round".to_owned(), 1));
            input.advance_to(round + 1);
            while probe.less_than(input.time()) {
                worker.step();
            }
        }
```

There are several things going on here. First, we `send` some data into the input, which allows the data to circulate through the workers along the dataflow. This data will be of type `(String, i64)`, because our example wants to send some text and annotate each with the change in the count (we add or remove text with `+1` or `-1`, respectively). Second, we `advance_to` to tell timely dataflow that we have ceased sending data for `round` and anything before it. At this point timely can start to reason about `round` becoming complete, once all the associated data make their way through the dataflow. Finally, we repeatedly `step` the worker until `probe` reports that it has caught up to `round + 1`, meaning that data for `round` are fully flushed from the system (and printed to the screen, one hopes).

## Breaking text into words

Let's add a simple operator that takes our text strings we supply as input and breaks them into words.

More specifically, we will take `(String, i64)` pairs and break them into many `(String, i64)` pairs with the same `i64` value, because if we are adding some text we'll add the words, and if subtracting text we'll subtract the words.

Rather than repeat all the code up above, I'm just going to show you the fragment you insert between `to_stream` and `inspect`:

```rust
# extern crate timely;
#
# use timely::dataflow::{InputHandle, ProbeHandle};
# use timely::dataflow::operators::{Inspect, Probe, Map};
#
# fn main() {
#     // initializes and runs a timely dataflow.
#     timely::execute_from_args(std::env::args(), |worker| {
#
#         // create input and output handles.
#         let mut input = InputHandle::new();
#         let mut probe = ProbeHandle::new();
#
#         // build a new dataflow.
#         worker.dataflow(|scope| {
#             input.to_stream(scope)
                 .flat_map(|(text, diff): (String, i64)|
                     text.split_whitespace()
                         .map(move |word| (word.to_owned(), diff))
                         .collect::<Vec<_>>()
                 )
#                  .inspect(|x| println!("seen: {:?}", x))
#                  .probe_with(&mut probe);
#         });
#
#         // feed the dataflow with data.
#         for round in 0..10 {
#             input.send(("round".to_owned(), 1));
#             input.advance_to(round + 1);
#             while probe.less_than(input.time()) {
#                 worker.step();
#             }
#         }
#     }).unwrap();
# }
```

The `flat_map` method expects to be told how to take each record and turn it into an iterator. Here, we are saying that each received `text` should be split (at whitespace boundaries), and each resulting `word` should be paired up with `diff`. We do a weird `collect` thing at the end because `split_whitespace` tries to hand back pointers into `text` and it makes life complicated. Sorry, blame Rust (and then blame me for using Rust).

This code should now show us the stream of `(word, diff)` pairs that fly by, but we still haven't done anything complicated with them yet.

## Maintaining word counts

This gets a bit more interesting. We don't have an operator to maintain word counts, so we are going to write one.

We start with a stream of words and differences coming at us. This stream has no particular structure, and in particular if the stream is distributed across multiple workers we have no assurance that all instances of the same word are at the same worker. This means that if each worker just adds up the counts for each word, we will get a bunch of partial results, local to each worker.

We will need to introduce *data exchange*, where the workers communicate with each other to shuffle the data so that the resulting distribution provides correct results. Specifically, we are going to distribute the data so that each individual word goes to the same worker, but the words themselves may be distributed across workers.

Having exchanged the data, each worker will need a moment of care when it processes its inputs. Because the data are coming in from multiple workers, they may no longer be in "time order"; some workers may have moved through their inputs faster than others, and may be producing data for the next time while others lag behind. This operator means to produce the word count changes *as if processed sequentially*, and it will need to delay processing changes that come early.

As before, I'm just going to show you the new code, which now lives just after `flat_map` and just before `inspect`:

```rust
# extern crate timely;
#
# use timely::dataflow::{InputHandle, ProbeHandle};
# use timely::dataflow::operators::{Inspect, Probe, Map};
#
# use std::collections::HashMap;
# use timely::dataflow::channels::pact::Exchange;
# use timely::dataflow::operators::Operator;
#
#
# fn main() {
#     // initializes and runs a timely dataflow.
#     timely::execute_from_args(std::env::args(), |worker| {
#
#         // create input and output handles.
#         let mut input = InputHandle::new();
#         let mut probe = ProbeHandle::new();
#
#         // build a new dataflow.
#         worker.dataflow::<usize,_,_>(|scope| {
#             input.to_stream(scope)
#                 .flat_map(|(text, diff): (String, i64)|
#                     text.split_whitespace()
#                         .map(move |word| (word.to_owned(), diff))
#                         .collect::<Vec<_>>()
#                 )
                .unary_frontier(
                    Exchange::new(|x: &(String, i64)| (x.0).len() as u64),
                    "WordCount",
                    |_capability, operator_info| {

                    // allocate operator-local storage.
                    let mut queues = HashMap::new();
                    let mut counts = HashMap::new();
                    let mut buffer = Vec::new();

                    move |input, output| {

                        // for each input batch, stash it at `time`.
                        while let Some((time, data)) = input.next() {
                            queues.entry(time.retain())
                                  .or_insert(Vec::new())
                                  .extend(data.replace(Vec::new()));
                        }

                        // enable each stashed time if ready.
                        for (time, vals) in queues.iter_mut() {
                            if !input.frontier().less_equal(time.time()) {
                                let vals = std::mem::replace(vals, Vec::new());
                                buffer.push((time.clone(), vals));
                            }
                        }

                        // drop complete time and allocations.
                        queues.retain(|time, vals| vals.len() > 0);

                        // sort ready updates by time.
                        buffer.sort_by(|x,y| (x.0).time().cmp(&(y.0).time()));

                        // retire updates in time order.
                        for (time, mut vals) in buffer.drain(..) {
                            let mut session = output.session(&time);
                            for (word, diff) in vals.drain(..) {
                                let entry = counts.entry(word.clone()).or_insert(0i64);
                                *entry += diff;
                                session.give((word, *entry));
                            }
                        }
                    }
                })
#                  .inspect(|x| println!("seen: {:?}", x))
#                  .probe_with(&mut probe);
#         });
#
#         // feed the dataflow with data.
#         for round in 0..10 {
#             input.send(("round".to_owned(), 1));
#             input.advance_to(round + 1);
#             while probe.less_than(input.time()) {
#                 worker.step();
#             }
#         }
#     }).unwrap();
# }
```

That was probably a lot to see all at once. So let's break down each of the things we did.

```rust,ignore
.unary_frontier(
    Exchange::new(|x: &(String, i64)| (x.0).len() as u64),
    "WordCount",
    |_capability, operator_info| {
        // coming soon!
    }
)
```

The very first thing we did was state that we are going to build a new unary dataflow operator. Timely lets you build your own operators just by specifying the logic for them as a closure. So easy! But, we have to explain a few things to the operator.

First, we tell it how it should distribute the data (pairs of strings and differences) between workers. Here we are saying "by the length of the text" which is a deranged way to do it, but we'd need about five more lines to properly write hashing code for the string.

Second, we give a descriptive name so that the operator is recognizable in logging and diagnostic code; you probably don't care at the moment, but you might later on if you wonder what is going on.

Third and finally, we specify a closure. The closure has an argument, which we ignore in the code (it has to do with writing operators that can send output data before they receive any input data) and we will ignore it now. This closure is actually a "closure builder": it is a closure that just returns another closure:

```rust,ignore
    // allocate operator-local storage.
    let mut queues = HashMap::new();
    let mut counts = HashMap::new();
    let mut buffer = Vec::new();

    move |input, output| {
        // coming soon!
    }
```

The closure that we end up returning is the `|input, output|` closure. It describes what the operator would do when presented with a handle to the input and a handle to the output. We've also named two hash maps and a vector we will need, and provided the `move` keyword to Rust so that it knows that the resulting closure *owns* these hash maps, rather than *borrows* them.

Inside the closure, we do two things: (i) read inputs and (ii) update counts and send outputs. Let's do the input reading first:

```rust,ignore
        // for each input batch, stash it at `time`.
        while let Some((time, data)) = input.next() {
            queues.entry(time.retain())
                  .or_insert(Vec::new())
                  .extend(data.replace(Vec::new()));
        }
```

The `input` handle has a `next` method, and it optionally returns a pair of `time` and `data`, representing a timely dataflow timestamp and a hunk of data bearing that timestamp, respectively. Our plan is to iterate through all available input (the `next()` method doesn't block, it just returns `None` when it runs out of data), accepting it from the timely dataflow system and moving it into our `queue` hash map.

Why do we do this? Because this is a streaming system, we could be getting data out of order. Our goal is to update the counts in time order, and to do this we'll need to enqueue what we get until we also get word that the associated `time` is complete. That happens in the next few hunks of code

First, we extract those times and their data that are ready to go:

```rust,ignore
        // enable each stashed time if ready.
        for (time, vals) in queues.iter_mut() {
            if !input.frontier().less_equal(time.time()) {
                let vals = std::mem::replace(vals, Vec::new());
                buffer.push((time.clone(), vals));
            }
        }
```

Here we look through each `(time, vals)` pair that we've queued up. We then check `input.frontier`, which is what tells us whether we might expect more times or not. The `input.frontier()` describes times we may yet see on the input; if it is `less_equal` to the time, then it is possible there might be more data.

If the time is complete, we extract the data and get ready to act on it. We don't actually act *yet*, because many times may become available at once, and we want to process them in order too. Before we do that, some housekeeping:

```rust,ignore
        // drop complete time and allocations.
        queues.retain(|time, vals| vals.len() > 0);

        // sort ready updates by time.
        buffer.sort_by(|x,y| (x.0).time().cmp(&(y.0).time()));
```

These calls clean up the `queues` hash map removing keys we are processing, and then sort `buffer` by time to make sure we process them in order. This first step is surprisingly important: the keys of this hash map are timestamps that can be used to send data, and we need to drop them for timely dataflow to understand that we give up the ability to send data at these times.

Finally, we drain `buffer` and process updates in time order

```rust,ignore
        // retire updates in time order.
        for (time, mut vals) in buffer.drain(..) {
            let mut session = output.session(&time);
            for (word, diff) in vals.drain(..) {
                let entry = counts.entry(word.clone()).or_insert(0i64);
                *entry += diff;
                session.give((word, *entry));
            }
        }
```

Here we process each time in order (we sorted them!). For each time, we create a new output session from `output` using `time` More importantly, this actually needs to be the same type as `time` from before; the system is smart and knows that if you drop all references to a time you cannot create new output sessions. It's a feature, not a bug.

We then proceed through each of the batches we enqueue, and through each of the `(word, diff)` pairs in each of the batches. I've decided that what we are going to do is update the count and announce the new count, but you could probably imagine doing lots of different things here.

## The finished product

You can check out the result in [`examples/wordcount.rs`](https://github.com/TimelyDataflow/timely-dataflow/blob/master/timely/examples/wordcount.rs). If you run it as written, you'll see output that looks like:

```ignore
    Echidnatron% cargo run --example wordcount
        Finished dev [unoptimized + debuginfo] target(s) in 0.0 secs
        Running `target/debug/examples/wordcount`
    seen: ("round", 1)
    seen: ("round", 2)
    seen: ("round", 3)
    seen: ("round", 4)
    seen: ("round", 5)
    seen: ("round", 6)
    seen: ("round", 7)
    seen: ("round", 8)
    seen: ("round", 9)
    seen: ("round", 10)
    Echidnatron%
```

We kept sending the same word over and over, so its count went up. Neat. If you'd like to run it with two workers, you just need to put `-- -w2` at the end of the command, like so:

```ignore
    Echidnatron% cargo run --example wordcount -- -w2
        Finished dev [unoptimized + debuginfo] target(s) in 0.0 secs
        Running `target/debug/examples/wordcount -w2`
    seen: ("round", 1)
    seen: ("round", 2)
    ...
    seen: ("round", 19)
    seen: ("round", 20)
    Echidnatron%
```

Because there are two workers, each inputting `"round"` repeatedly, we count up to twenty. By the end of this text you should be able to produce more interesting examples, for example reading the contents of directories and divvying up responsibility for the files between the workers.
