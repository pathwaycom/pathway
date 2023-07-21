## Interactive Queries

We have just described an algorithm for determining a collection containing `(node, label)` pairs describing the connected component structure of an undirected graph. But, this collection is a bit unwieldy. Do we print it to the screen? Flip through the pages of results to find a specific pair of query nodes?

Instead, let's describe an extended computation that lets us query the results, interactively!

Imagine `labels` contains the results of the iterative computation from before. Let's create a new input, `queries`, which will simply contain node identifiers.

```rust,no_run
    labels.semijoin(queries)
          .inspect(|x| println!("{:?}", x));
```

This is a fairly brief looking computation; what does it do?

As we update the `queries` relation, we change which elements of `labels` are passed through to the inspect. As we add new elements, the corresponding label from `labels` is fished out and presented for inspection. In effect, our computation is now an interactive service that returns the connected component id of nodes we ask for, in our sub-millisecond interactive timescales.

Even more importantly, our computation *maintains* the results of these queries as `labels` changes, perhaps due to changes in its input `edges` collection. When we add an element to `queries` we are installing a standing query that will monitor the label of the query, and report all changes to it, until such a point as we uninstall the query by removing it from `queries`.

Contrast this with an approach where the connectivity results are stashed in a key-value store, one that you probe for node labels. While you may read back two labels that are the same, were they actually the same at the same moment? If the labels are not the same, does that mean they were not the same or are they perhaps changing in tandem as the graph changes?

Differential dataflow prevents these ambiguities through its commitment to producing the correct answer, and clearly explaining a consistent history of changes through logical timestamps.
