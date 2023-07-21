# Progress Tracking

Progress tracking is a fundamental component of timely dataflow, and it is important to understand how it works to have a complete understanding of what timely dataflow does for you.

Let's start with a statement about what progress tracking means to accomplish.

The setting is that there are multiple workers, each of whom move data through a common dataflow graph. The data may move between workers, and as the data are processed by operators we have relatively few guarantees about their consequences: a worker may receive a record and do nothing, or it could send one thousand output records some of which are now destined for us. Nonetheless, we need to be able to make meaningful statements about the possibility of receiving more data.

Timely dataflow's approach is that data bear a logical *timestamp*, indicating some moment in the computation at which they should be thought to exist. This is not necessarily a physical timestamp, like the worker's clock when the record was created, but could be any type satisfying a few constraints. A common example is sequence numbers, counting up from zero.

Timely dataflow imposes a few constraints, we think they are natural, on the structure of the dataflow graph, from which it is able to make restrictive statements at each location in the dataflow graph of the form "you will only ever see timestamps greater or equal to these times". This provides each dataflow operator with an understanding of *progress* in the computation. Eventually, we may even learn that the set of future timestamps is empty, indicating completion of the stream of data.

Timely dataflow computations are structured so that to send a timestamped message, an operator must hold a capability for that timestamp. Timely dataflow's progress tracking can be viewed as (i) workers collectively maintaining a view of outstanding timestamp capabilities at each location in the dataflow graph, and (ii) each worker independently determines and communicates the implications of changes in its view of capabilities to other locations in its instance of the dataflow graph.

Before we get into these two aspects, we will first need to be able to name parts of our dataflow graph.

## Dataflow Structure

A dataflow graph hosts some number of operators. For progress tracking, these operators are simply identified by their index. Each operator has some number of *input ports*, and some number of *output ports*. The dataflow operators are connected by connecting each input port to a single output port (typically of another operator). Each output port may be connected to multiple distinct input ports (a message produced at an output port is to be delivered to all attached input ports).

In timely dataflow progress tracking, we identify output ports by the type `Source` and input ports by the type `Target`, as from the progress coordinator's point of view, an operator's output port is a *source* of timestamped data, and an operator's input port is a *target* of timestamped data. Each source and target can be described by their operator index and then an operator-local index of the corresponding port. The use of distinct types helps us avoid mistaking input and output ports.

```rust,ignore
pub struct Source {
    /// Index of the source operator.
    pub index: usize,
    /// Number of the output port from the operator.
    pub port: usize,
}

pub struct Target {
    /// Index of the target operator.
    pub index: usize,
    /// Number of the input port to the operator.
    pub port: usize,
}
```

The structure of the dataflow graph can be described by a list of all of the connections in the graph, a `Vec<(Source, Target)>`. From this, we could infer the number of operators and their numbers of input and output ports, as well as enumerate all of the connections themselves.

At this point we have the structure of a dataflow graph. We can draw a circle for each operator, a stub for each input and output port, and edges connecting the output ports to their destination input ports. Importantly, we have names for every location in the dataflow graph, which will either be a `Source` or a `Target`.

## Maintaining Capabilities

Our first goal is for the workers to collectively track the number of outstanding timestamp capabilities in the system, for each timestamp and at each location, as dataflow operators run and messages are sent and received. Capabilities can exist in two places in timely dataflow: an operator can explicitly hold capabilities to send timestamped messages on each of its outputs, and each timestamped message bears a capability for its timestamp.

When tracking capabilities, we will track their *multiplicity*: how *many* capabilities for time `t` are there at location `l`? For most locations and times this number will be zero. Unless the computation has completed, for some locations and times this number must be positive. Numbers can also be transiently negative, as reports of changes may arrive out of order.

When a timely dataflow computation starts, there are no messages in flight. Rather, each operator starts with the capabilities to send any timestamped message on any of its outputs. As this is common knowledge among the workers, each initializes its counts with `#workers` for capabilities at each operator output. Each worker understands that it will need to hear `#workers` reports of such capabilities being dropped before they are actually out of the system.

As a computation proceeds, operators may perform three classes of action:

1. They may consume input messages, acquiring the associated capability.
2. They may clone, downgrade, or drop any capability they hold.
3. They may send output messages at any timestamp for which they hold the capability.

The results of these actions are a stream of changes to the occurrences of capabilities at each location in the dataflow graph. As input messages are consumed, capabilities located at the corresponding `Target` input port are decremented, and capabilities at the operators `Source` output ports are incremented. Cloning, downgrading, and dropping capabilities changes the counts at each of the operators corresponding `Source` output ports. Sending messages results in increments to the counts at the `Target` ports of each `Target` connected to the `Source` from which the message is sent.

Concretely, a batch of changes has the form `(Vec<(Source, Time, i64)>, Vec<(Target, Time, i64)>)`, indicating the increments and decrements for each time, at each dataflow location. Importantly we must keep these batches intact; the safety of the progress tracking protocol relies on not communicating half-formed progress messages (for example, consuming an input message but forgetting to indicate the acquisition of its capability).

Each worker broadcasts the stream of progress change batches to all workers in the system (including itself) along point-to-point FIFO channels. At any point in time, each worker has seen an arbitrary prefix of the sequence of progress change batches produced by each other worker, and it is expected that as time proceeds each worker eventually sees every prefix of each sequence.

At any point in time, each worker's view of the capabilities in the system is defined by the accumulation of all received progress change batches, plus initial capabilities at each output (with starting multiplicity equal to the number of workers). This view may be surprising and messy (there may be negative counts), but at all times it satisfies an important safety condition, related to the communicated implications of these capabilities, which we now develop.

## Communicating Implications

Each worker maintains an accumulation of progress update batches, which explains where capabilities may exist in the dataflow graph. This information is useful, but it is not yet sufficient to make strong statements about the possibility of timestamped messages arriving at *other* locations in the dataflow graph. Even though a capability for timestamp `t` may exist, this does not mean that the time may arrive at any location in the dataflow graph. To be precise, we must discuss the paths through the dataflow graph which the timestamp capability could follow.

### Path Summaries

Progress tracking occurs in the context of a dataflow graph of operators all with a common timestamp type `T: Timestamp`. The `Timestamp` trait requires the `PartialOrder` trait, meaning two timestamps may be ordered but need not be. Each type implementing `Timestamp` must also specify an associated type `Summary` implementing `PathSummary<Self>`.

```rust,ignore
pub trait Timestamp: PartialOrder {
    type Summary : PathSummary<Self>;
}
```

A path summary is informally meant to summarize what *must* happen to a timestamp as it travels along a path in a timely dataflow. Most paths that you might draw have trivial summaries ("no change guaranteed"), but some paths do force changes on timestamps. For example, a path that goes from the end of a loop back to its head *must* increment the loop counter coordinate of any timestamp capability passed along it.

```rust,ignore
pub trait PathSummary<T> : PartialOrder {
    fn results_in(&self, src: &T) -> Option<T>;
    fn followed_by(&self, other: &Self) -> Option<Self>;
}
```

The types implementing `PathSummary` must be partially ordered, and implement two methods:

1. The `results_in` method explains what must happen to a timestamp moving along a path. Note the possibility of `None`; a timestamp could *not* move along a path. For example, a path summary `path` could increment a timestamp by one, for which
```rust,ignore
path.results_in(&4) == Some(5);
path.results_in(&u64::max_value()) == None;
```
It is important that `results_in` only advance timestamps: for all path summaries `p` we require that
```rust,ignore
if let Some(time) = p.results_in(&x) {
    assert!(x.less_equal(&time));
}
```


2. The `followed_by` method explains how two path summaries combine. When we build the summaries we will start with paths corresponding to single edges, and build out more complex paths by combining the effects of multiple paths (and their summaries). As with `results_in`, it may be that two paths combined result in something that will never pass a timestamp, and the summary may be `None` (for example, two paths that each increment a loop counter by half of its maximum value).

Two path summaries are ordered if for all timestamps the two results of the path summaries applied to the timestamp are also ordered.

Path summaries are only partially ordered, and when summarizing what must happen to a timestamp when going from one location to another, along one of many paths, we will quickly find ourselves speaking about *collections* of path summaries. There may be several summaries corresponding to different paths we might take. We can discard summaries from this collection that are strictly greater than other elements of the collection, but we may still have multiple incomparable path summaries.

The second part of progress tracking, communicating the implications of current capabilities, is fundamentally about determining and locking in the minimal collections of path summaries between any two locations (`Source` or `Target`) in the dataflow graph. This is the "compiled" representation of the dataflow graph, from which we can derive statements about the possibility of messages at one point in the graph leading to messages at another point.

### Operator summaries

Where do path summaries come from?

Each operator in timely dataflow must implement the `Operate` trait. Among other things, that we will get to, the `Operate` trait requires that the operator summarize *itself*, providing a collection of path summaries for each of its internal paths, from each of its inputs to each of its outputs. The operator must describe what timestamps could possibly result at each of its output as a function of a timestamped message arriving at each of its inputs.

For most operators, this summary is simply: "timestamped data at each of my inputs could result in equivalently timestamped data at each of my outputs". This is a fairly simple summary, and while it isn't very helpful in making progress, it is the only guarantee the operator can provide.

The `Feedback` operator is where most of our interesting path summaries start: it is the operator found in feedback loops that ensures that a specific coordinate of the timestamp is incremented. All cycles in a timely dataflow graph must pass through such an operator, and in cyclic dataflow we see non-trivial summaries from the outputs of downstream operators to the inputs of upstream operators.

Another useful summary is the absence of a summary. Sometimes there is no path between two points in a dataflow graph. Perhaps you do some computation on your dataflow inputs, and then introduce them into an iterative computation; there is no path that returns that data to the computation you do on the inputs. Records in-flight in the iterative subcomputation will not block the computation you do on your inputs, because we know that there is no path back from the iteration to the inputs.

For example, an operator could plausibly have two inputs, a data input and a diagnostic input, and corresponding data and diagnostic outputs. The operator's internal summary could reveal that diagnostic input cannot result in data output, which allows us to issue diagnostic queries (as data for that input) without blocking downstream consumers of the data output. Timely dataflow can see that even though there are messages in flight, they cannot reach the data output and need not be on the critical path of data computation.

### A Compiled Representation

From the operator summaries we build path summaries, and from the path summaries we determine, for every pair of either `Source` or `Target` a collection of path summaries between the two. How could a timestamped message at one location lead to timestamped messages at the other?

The only constraint we require is that there should be no cycles that do not strictly advance a timestamp.

## A Safety Property

Each operator maintains a collection of counts of timestamp capabilities at each location (`Source` or `Target`) in the dataflow graph. At the same time, there is a statically defined set of path summaries from each location to any other location in the dataflow graph.

The safety property is: for any collection of counts resulting from the accumulation of arbitrary prefixes of progress update batches from participating workers, if for any location `l1` in the dataflow graph and timestamp `t1` there is not another location `l2` and timestamp `t2` with strictly positive accumulated count such that there is a path summary `p` from `l2` to `l1` with `p(t2) <= t1`, then no message will ever arrive at location `l1` bearing timestamp `t1`.

This property has [a formalization in TLA](https://www.microsoft.com/en-us/research/publication/the-naiad-clock-protocol-specification-model-checking-and-correctness-proof/) courtesy of Tom Rodeheffer, but let's try to develop the intuition for its correctness.

Actually, let's first develop some *counter*-intuition for its correctness. This property holds, but we've said almost nothing about the communication of messages between workers. The only property we use about message delivery in proving the safety property is that it is "at most once"; messages should not be multiplied in flight. Does it matter in which order messages are delivered? No. Does it matter that messages are *ever* delivered? No (this is only a safety property). Can operators consume, digest, and send messages that their local progress protocol doesn't even know exist yet? Yes.

There is almost no coordination between the data plane, on which messages get sent, and the control plane, along which progress update batches get sent. The only requirement is that you do not send a progress update batch for some action that you have not performed.

So where do we find intuition for the correctness of the protocol?

Although we may have only seen prefixes of the progress update batches from other workers, we can nonetheless reason about what future progress update batches from each worker will need to look like. In the main, we will use the property that if updates correspond to things that actually happen:

1. Any message consumed must have a corresponding message produced, even if we haven't heard about it yet.
2. Any message produced must involve a capability held, even if we haven't heard about it yet.
3. Any capability held must involve either another capability held, or a message consumed.

We'll argue that for any sequence of progress updates some prefix of which accumulates as assumed in the statement of the safety property, then no extension of these prefixes could possibly include an update acknowledging a message received at `l1` with timestamp `t1`.

First, we define an order on the pairs `(li, ti)` of locations and timestamps, in the Naiad paper called *pointstamps*. Pointstamps are partially ordered by the *could-result-in* relation: `(li, ti)` *could-result-in* `(lj, tj)` if there is a path summary `p` from `li` to `lj` where `p(ti) <= tj`. This is an order because it is (i) reflexive by definition, (ii) antisymmetric because two distinct pointstamps that *could-result-in* each other would imply a cycle that does not strictly advance the timestamp, and (iii) transitive by the assumed correctness of path summary construction.

Next, each progress update batch has a property, one that is easier to state if we imagine operators cannot clone capabilities, and must consume a capability to send a message: each atomic progress update batch decrements a pointstamp and optionally increments pointstamps strictly greater than it. Alternately, each progress update batch that increments a pointstamp must decrement a pointstamp strictly less than it. Inductively, any collection of progress update batches whose net effect increments a pointstamp must have a net effect decrementing some pointstamp strictly less than it.

Now, just before a worker might hypothetically receive a message at `(l1, t1)`, we freeze the system. We stop performing new actions, and instead resolve all progress updates for actions actually performed. Let these progress update batches circulate, so that the worker in question now has a complete and current view of the frozen state of the system. All pointstamp counts should now be non-negative.

The additional progress updated batches received as part of stabilizing the system can only have a net positive effect on `(l1, t1)` if they have a net negative effect on some poinstamp strictly less than it. However, as the final accumulation is non-negative, this can only happen if the strictly prior pointstamp had a positive accumulation before stabilization. If no such pointstamp has a positive accumulation before stabilization, it is not possible for `(l1, t1)` to have a positive accumulation at stabilization, and consequently there cannot be a message waiting to be received.
