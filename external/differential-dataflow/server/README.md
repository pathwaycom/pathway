# Graph Server

A differential dataflow server for continually changing graphs

## Overview

The graph server is a system that hosts continually changing graph datasets, and computation defined over them. Users are able to build and load shared libraries that can both define computation over existing graph datasets or define and share new graphs. The user code is able to share the same underlying representations for the graphs, reducing the overhead and streamlining the execution.

## A Sketch

The graph server is very much in progress, and its goals are mostly to exercise timely and differential dataflow as systems, and to see where this leads. At the moment, the server provides a small set of graph definitions and graph analyses one can load against them. This is mostly so that we can see what it is like to start up and shut down graph computations with shared state, and understand if we are doing this well. 

Several computations are defined in `./dataflows/` which give examples of defining your own computation. These projects have no special status, and could each be re-implemented by users such as yourself. The intent is that once your project is compiled, the server can load the associated shared libraries and bring your computation into the same shared address space as the other graph computations.

## Examples

The main binary is `bin/server.rs`, which can be invoked by running

    cargo run --bin server

This .. doesn't appear to do anything:

    Echidnatron% cargo run --bin server
        Finished dev [unoptimized + debuginfo] target(s) in 0.0 secs
         Running `target/debug/server`

What has happened is that the server is up and running and waiting for some input! Pretty exciting. 

Before getting ahead of ourselves, we'll need to build some code to run. Let's quit out of the server for now and do the following:

    Echidnatron% cd dataflows/random_graph
    Echidnatron% cargo build
       Compiling random_graph v0.1.0 (file:///Users/mcsherry/Projects/differential-dataflow/grapht/dataflows/random_graph)
        Finished dev [unoptimized + debuginfo] target(s) in 5.90 secs
    Echidnatron%

and then 

    Echidnatron% cd ../degr_dist
    Echidnatron% cargo build
       Compiling degr_dist v0.1.0 (file:///Users/mcsherry/Projects/differential-dataflow/grapht/dataflows/degr_dist)
        Finished dev [unoptimized + debuginfo] target(s) in 8.14 secs
    Echidnatron%

These commands will build two shared libraries, `librandom_graph.dylib` and `libdegr_dist.dylib`, which we will use in our server!

Ok, back to the server now. Load that puppy up again and type something after it:

    Echidnatron% cargo run --bin server
        Finished dev [unoptimized + debuginfo] target(s) in 0.0 secs
         Running `target/debug/server`
    load ./dataflows/random_graph/target/debug/librandom_graph.dylib build <graph_name> 1000 2000 10

Ok. We have now bound to the string `<graph_name>` a random graph on 1,000 nodes comprising a sliding window over 2,000 edges, which changes ten times every second. If you would like to, you can change any of the arguments passed, though if you increase the rate of change too much you may overload the system. Let's leave it how it is for now.

Up next, let's attach the `degr_dist` computation to `<graph_name>` and see what we get:

    load ./dataflows/degr_dist/target/debug/libdegr_dist.dylib build <graph_name>

This will attach our pre-defined degree distribution computation, which goes and computes the stream of changes to the counts of nodes with each out-degree. 

Rather than blast all that information at us (it would only be tens of changes per second, but still), it reports on the distribution of latencies of changes: how long did it take a change from the time it should have entered the system, through to being observed in the output?

The first line you'll see may look like so (it will depend on the performance of your system):

    delays: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8, 6, 16, 18, 64, 66, 0, 9, 0, 0, 0, 0, 0, 0, 0]

These counts report the number of observed latencies for each power-of-two number of microseconds. It seems that the lowest latency here is `(1 << 17)` microseconds, or roughly 131 milliseconds. That is a large number, but what is going on here is that the first line is the `degr_dist` computation catching up on historical data. Subsequent lines should look better:

    delays: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    delays: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    delays: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    delays: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    delays: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    delays: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]

We are now at entries 11 and 12, corresponding to between one and four millisecond latencies. This number is still pretty large, and it is due to us running without release optimizations. If we turn those on, things look a bit better:

    delays: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8, 0, 16, 6, 26, 46, 14, 0, 9, 0, 0, 0, 0, 0, 0, 0]
    delays: [0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    delays: [0, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    delays: [0, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    delays: [0, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    delays: [0, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    delays: [0, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]

We see a similar first batch of latencies, reflecting the gap between historical times and when I typed the second `load` line, followed by latencies that are all in the range of 65 to 128 microseconds range (the index incremented comes from `delay_us.next_power_of_two().trailing_zeros()` methods).

This is an especially lightly loaded computation, and you can play around with the data generation parameters to get a feel for how the latencies respond as you increase the load.

## An example computation

Let's take a closer look at the `degr_dist` computation. What does it look like?

You can find [the source](https://github.com/frankmcsherry/differential-dataflow/tree/master/grapht/dataflows/degr_dist) in the repository, but it's intentionally concise so let's just take a peek at [`dataflows/degr_dist/src/lib.rs`](https://github.com/frankmcsherry/differential-dataflow/blob/master/grapht/dataflows/degr_dist/src/lib.rs) where all of the logic lives:

First, there is the standard Rust boilerplate, calling out which crates and traits we want to use. These are complicated only because `degr_dist` uses these types, and could be much simpler with some wrapping.

```rust
extern crate timely;
extern crate differential_dataflow;
extern crate dd_server;

use std::rc::Rc;
use std::cell::RefCell;

use timely::dataflow::operators::inspect::Inspect;
use timely::dataflow::operators::Probe;
use differential_dataflow::operators::CountTotal;
use dd_server::{Environment, TraceHandle};
```

Once we get past the boilerplate, we get to define a method that takes some context about the larger world, and is free to build up some dataflow!

```rust
#[no_mangle]
pub fn build((dataflow, handles, probe, _timer, args): Environment) -> Result<(), String> {

    if args.len() != 1 { return Err(format!("expected one argument, instead: {:?}", args)); }

    handles
        .get_mut::<Rc<RefCell<Option<TraceHandle>>>>(&args[0])?
        .borrow_mut().as_mut().unwrap()
        .import(dataflow)
        .as_collection(|k,v| (k.clone(), v.clone()))
        .map(|(src, _dst)| src as usize).count_total()
        .map(|(_src, cnt)| cnt as usize).count_total()
        .probe_with(probe);

    Ok(())
}
```

I've deleted the code that does all the printing to the screen, because ideally real computations don't actually write that sort of stuff, but you can check it out in the repository link up above.

You can drill down on some of the types, but `handles` is a `HashMap<String, Box<Any>>` in which we stash various things, including access to the random graph we created and are continually updating. We can look up the graph by name, import it in to our dataflow, and then write code using standard differential dataflow operators.

### Stashing outputs

Watching `println!` statements fly past is only so interesting. Which is to say: "not very". Step one is obviously to comment out the `.inspect()` line, but where do we go from there? Probably, we would want to publish the output, which we can do with our mutable access to `handles`.

Instead, let's look at what the `random_graph` library does. Now, we aren't going to look at all the code, because there is a lot of random graph stuff, but from the point where we have a differential dataflow collection of edges, which we `probe`, onwards it looks like:

```rust
    let trace = 
        // .. lots of stuff ..
        .probe_with(probe)
        .as_collection()
        .arrange_by_key_u()
        .trace;

    *trace_handle.borrow_mut() = Some(trace);
    handles.set::<Rc<RefCell<Option<TraceHandle>>>>(name.to_owned(), trace_handle);
```

This looks like a bit of a mess, which is fair, but we are roughly wrapping up a trace handle so that it can be shared with others, and then registering it under `name.to_owned()` in the `handles` map.

In fact, we stash a few other things in the map, which allows any program that knows what to look for to get access to shared state. For example, we stash the capability that the random graph uses to produce its changes, so that anyone could drop the capability and cause the graph generation to cease.