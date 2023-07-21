## Graph Computation

Graph computation covers a lot of ground, and we will pick just one example here: graph connectivity.

Imagine you have a collection containing pairs `(source, target)` of graph edges, and you would like to determine which nodes can reach which other nodes along graph edges (using either direction).

One algorithm for this graph connectivity is "label propagation", in which each graph node maintains a label (initially its own name) and all nodes repeatedly exchange labels and maintain the smallest label they have yet seen. This process converges to a limit where each node has the smallest label in its connected component.

Let's write this computation starting from a collection `edges`, using differential dataflow.

```rust,no_run
    // create initial labels from sources.
    let labels = edges.map(|(src,dst)| (src,src))
                      .distinct();

    labels
        .iterate(|inner| {
            let labels = labels.enter(inner.scope());
            let edges = edges.enter(inner.scope());
            inner.join(&edges)
                 .map(|(_src,(lbl,dst))| (dst,lbl))
                 .concat(&labels)
                 .reduce(|_dst, lbls, out| {
                     let min_lbl =
                     lbls.iter()
                         .map(|x| *x.0)
                         .min()
                         .unwrap();
                     out.push((min_lbl, 1));
                 })
        });
```

This computation first determines some initial labels, taking as the set of nodes those identifiers from which edges emerge. We are assuming that the input graph is symmetric, so every node should be the source of some edge.

The computation then iteratively develops the label collection, by joining whatever labels we have at any point with the set of edges, effectively "proposing" each node's label to each of the node's neighbors. All nodes fold in these proposals with their initial labels, and each retains one copy of the smallest label they are provided as input.

The resulting collection contains pairs `(node, label)` from which we can determine if two nodes are in the same connected component: do they have the same label? Let's see how to use this interactively next!