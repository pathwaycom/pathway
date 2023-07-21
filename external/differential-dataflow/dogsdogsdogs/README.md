## Worst-case optimal joins and delta queries in differential dataflow

This project collects differential dataflow operators sufficient to implement memory-efficient delta queries, and worst-case optimal delta queries in the framework of [differential dataflow](https://github.com/frankmcsherry/differential-dataflow). Many of the ideas are a transportation of work done in the [`dataflow-join`](https://github.com/frankmcsherry/dataflow-join) project.

---

Imagine we have a collection of `(src, dst)` pairs describing a graph, and we want to determine the set of triangles:

    triangles(a,b,c) := edge(a,b), edge(b,c), edge(a,c)

Differential dataflow provides a binary `join` operator that matches records in its two inputs with the same key, and produces the pairs of values. While this operator serves its role well, its naive implementation needs to maintain each of its inputs in indexed form to respond quickly to changes on either input.

In the example above, we might write

```rust
edges.join(&edges)
     .map(|(a,(b,c))| ((b,c),a))
     .semijoin(&edges)
     .map(|((b,c),a)| (a,b,c));
```

This produces all triples `(a,b,c)` satisfying the triangle definition above, and it will update incrementally for arbitrary changes to the `edges` collection. Unfortunately, the first input to `semijoin` has size proportional to the sum of the squares of the degrees in the graph, which can be quite large.

The database community has a few remedies for this problem, and we will be borrowing two of them.

### Delta queries

Starting from the triangles query above, let's treat each of the three uses of `edges` as distinct relations.

    triangles(a,b,c) := edge1(a,b), edge2(b,c), edge3(a,c)

We can write relational "delta queries" for how the `triangles` collection should change as a function of changes to any of the input relations.

    d_tris1(a,b,c) := d_edge1(a,b), edge2(b,c), edge3(a,c)
    d_tris2(a,b,c) := d_edge2(b,c), edge1(a,b), edge3(a,c)
    d_tris3(a,b,c) := d_edge3(a,c), edge1(a,b), edge2(b,c)

All I've done here is written three rules by taking each relation in turn, replacing it by a version with a `d_` prefix, and then listing the remaining relations. Each of these queries tell us how to respond to a change to the associated relation.

Delta queries are appealing because they can be implemented in a stateless manner, maintaining only the indexed representations of `edges` (probably indexed both by source and by destination). They do *not* need to maintain intermediate collections, as differential dataflow does by default.

There is an important subtle detail, which the above example demonstrates: if there are simultaneous updates to many relations, we can't simply apply all delta queries at the same time. One change to `edges` could cause the three rules above to derive the same change multiple times. Instead, we need to execute the delta queries "in order", perhaps the order written above. Of course, we don't plan on actually doing that, but we can use timely dataflow's timestamps to impose a logical if not physical order on the execution.

In [examples/delta_query.rs](https://github.com/frankmcsherry/differential-dataflow/blob/master/dogsdogsdogs/examples/delta_query.rs#L52-L79) we can write these three rules as so:

```rust
// d_tris1(a,b,c) := d_edge1(a,b), edge2(b,c), edge3(a,c)
let d_tris1 = forward
    .propose_using(&mut neu_forward.extend_using(|(_a,b)| *b))
    .validate_using(&mut neu_forward.extend_using(|(a,_b)| *a))
    .map(|((a,b),c)| (a,b,c));

// d_tris2(a,b,c) := d_edge2(b,c), edge1(a,b), edge3(a,c)
let d_tris2 = forward
    .propose_using(&mut alt_reverse.extend_using(|(b,_c)| *b))
    .validate_using(&mut neu_reverse.extend_using(|(_b,c)| *c))
    .map(|((b,c),a)| (a,b,c));

// d_tris3(a,b,c) := d_edge3(a,c), edge1(a,b), edge2(b,c)
let d_tris3 = forward
    .propose_using(&mut alt_forward.extend_using(|(a,_c)| *a))
    .validate_using(&mut alt_reverse.extend_using(|(_a,c)| *c))
    .map(|((a,c),b)| (a,b,c));
```

where the `alt` and `neu` prefixes refer to whether the data are old or new (old data can't "see" new data until the next timestamp), and the `forward` and `reverse` suffixes are the direction the data are indexed.

There is clearly a lot of code jargon going on here, but this all works and maintains a fixed memory footprint, proportional to the number of edges rather than the number of intermediate triangle candidates.

### Worst-case optimal joins

If you don't know what these are you are in for a treat.

Worst-case optimal joins are a new way of join processing that operate attribute-at-a-time, rather than relation-at-a-time, and which take time proportional to the worst-case possible result size set. For something like triangle computation, this is at most the number of edges raised to the power 1.5, which is better than the power 2, which is what a traditional relational engine would guarantee.

There are several worst-case optimal join algorithms, but my favorite comes from [the dataflow-join repository](https://github.com/frankmcsherry/dataflow-join). The gist here is that whenever you have a tuple like `(a,b)` to which you are thinking about adding a new attribute `c`, best to take stock of the possible ways that the new attribute `c` might be added. For example, in

    d_tris1(a,b,c) := d_edge1(a,b), edge2(b,c), edge3(a,c)

the new `c` needs to be present in both `edge2` and in `edge3`. However, for a specific `(a,b)` pair, either of these two relations might propose more or fewer candidate `c` values. What if you were smart and first asked each relation to report the number of distinct values they would propose? Then you could have that relation propose the values, and the other relations validate these choices in time proportional to the number of proposals.

You can perform this in native differential dataflow over in the [examples/ngo.rs](https://github.com/frankmcsherry/differential-dataflow/blob/master/examples/ngo.rs) example. While cool, this version has the defect that the differential implementation again maintains intermediate state. This state can be much smaller than the naive joins, but it can also be much larger than the input relations.

Of course, we can combine the delta queries above with the worst-case optimal join processing, using all of the sweet loot in this project here. In [examples/delta_query_wcoj.rs](https://github.com/frankmcsherry/differential-dataflow/blob/master/dogsdogsdogs/examples/delta_query_wcoj.rs#L61-L87) we write the same delta queries, but implemented using worst-case optimal joins.

```rust
// d_tris1(a,b,c) := d_edge1(a,b), edge2(b,c), edge3(a,c)
let d_tris1 = forward
    .extend(&mut [
        &mut neu_forward.extend_using(|(_a,b)| *b),
        &mut neu_forward.extend_using(|(a,_b)| *a),
    ])
    .map(|((a,b),c)| (a,b,c));

// d_tris2(a,b,c) := d_edge2(b,c), edge1(a,b), edge3(a,c)
let d_tris2 = forward
    .extend(&mut [
        &mut alt_reverse.extend_using(|(b,_c)| *b),
        &mut neu_reverse.extend_using(|(_b,c)| *c),
    ])
    .map(|((b,c),a)| (a,b,c));

// d_tris3(a,b,c) := d_edge3(a,c), edge1(a,b), edge2(b,c)
let d_tris3 = forward
    .extend(&mut [
        &mut alt_forward.extend_using(|(a,_c)| *a),
        &mut alt_reverse.extend_using(|(_a,c)| *c),
    ])
    .map(|((a,c),b)| (a,b,c));
```

### Some numbers

Let's take these two implementations for a performance spin. None of these numbers are going to be nearly as exciting as for hand-rolled triangle counting implementations, sorry! However, they are fully differential, which means you could use them inside of an iterative Datalog computation, for example. Or with high refresh frequency, or distributed on multiple workers, etc. It's up to you to decide if these things are worth doing.

We have a few examples, including a naive differential WCOJ (examples/ngo.rs), and the delta queries and worst-case optimal delta queries. For the moment, let's just look at the second two.

If we spin up `examples/delta_query.rs` with the livejournal graph, asking it to introduce nodes (and all their edges) one at a time, we see

```
Echidnatron% cargo run --release --example delta_query -- ~/Projects/Datasets/livejournal 1
    Finished release [optimized] target(s) in 0.07s
     Running `target/release/examples/delta_query /Users/mcsherry/Projects/Datasets/livejournal 1`
3.429001ms  Round 1 complete
32.624059ms Round 2 complete
38.686335ms Round 3 complete
41.348212ms Round 4 complete
44.279022ms Round 5 complete
...
```

which looks like a great start. However, it goes really fast and quickly gets "stuck". If we wait a moment, we see

```
425.859742ms    Round 85 complete
426.276961ms    Round 86 complete
426.550825ms    Round 87 complete
26.317746169s   Round 88 complete
26.318698709s   Round 89 complete
26.319160175s   Round 90 complete
...
```

which is pretty bad news.

What happened here is a great example of why you want worst-case optimal joins. It turns out that node 87 has 13,127 out-going edges. That means that the third update rule in the delta query

```rust
// d_tris3(a,b,c) := d_edge3(a,c), edge1(a,b), edge2(b,c)
let d_tris3 = forward
    .propose_using(&mut alt_forward.extend_using(|(a,_c)| *a))
    .validate_using(&mut alt_reverse.extend_using(|(_a,c)| *c))
    .map(|((a,c),b)| (a,b,c));
```

takes in 13,127 changes and produces 172,318,129 proposals, being that number of changes *squared*. In this case, it was a horrible idea to ask `edge1` to propose changes to extend `d_edge3`. It would have been much better to ask `edge2` to do the proposals, especially because we are loading the graph in node order and there aren't so many reverse edges yet. Of course we don't want to bake that in as a rule, but the worst-case optimal implementation can figure this out automatically.

Let's see how `examples/delta_query_wcoj.rs` does:

```
Echidnatron% cargo run --release --example delta_query_wcoj -- ~/Projects/Datasets/livejournal 1
    Finished release [optimized] target(s) in 0.14s
     Running `target/release/examples/delta_query_wcoj /Users/mcsherry/Projects/Datasets/livejournal 1`
3.649981ms  Round 1 complete
5.279384ms  Round 2 complete
6.196575ms  Round 3 complete
6.990896ms  Round 4 complete
7.662358ms  Round 5 complete
...
```

Lots faster, you might have noticed. Even without the horrible behavior seen above, most of these nodes have degree in the hundreds, and the implementation is smart enough to use the reverse directions when appropriate.

```
84.9538ms   Round 85 complete
87.320066ms Round 86 complete
88.923623ms Round 87 complete
144.530066ms    Round 88 complete
145.450224ms    Round 89 complete
146.103021ms    Round 90 complete
...
```

Node 87 still causes some mischief, but this has more to do with our computation ingesting those 13,127 edges and checking each of them to see what needs to be done. We still perform linear work, just not quadratic in this case.

You can take the examples out for spins with bigger batching if you want higher throughput. I wouldn't recommend this for the vanilla delta query (it has problems enough) but you can do e.g.

```
Echidnatron% cargo run --release --example delta_query_wcoj -- ~/Projects/Datasets/livejournal 1000
    Finished release [optimized] target(s) in 0.07s
     Running `target/release/examples/delta_query_wcoj /Users/mcsherry/Projects/Datasets/livejournal 1000`
675.443522ms    Round 1000 complete
1.534094533s    Round 2000 complete
2.341095966s    Round 3000 complete
3.192567920s    Round 4000 complete
3.952057645s    Round 5000 complete
4.619403029s    Round 6000 complete
5.617067697s    Round 7000 complete
6.260885764s    Round 8000 complete
7.649967738s    Round 9000 complete
8.279152976s    Round 10000 complete
...
```

This is showing us that we can perform over one thousand distinct updates each second (note: not a batch of one thousand updates), though this number will probably drop off as we let it run (the graph fills out). There is still plenty to improve and work on, and at the moment this is more about unique functionality rather than raw performance.