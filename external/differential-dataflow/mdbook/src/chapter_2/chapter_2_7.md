## The Iterate Operator

The `iterate` operator takes a starting input collection and a closure to repeatedly apply to this input collection. The output of the iterate operator is the collection that results from an unbounded number of applications of this closure to the input. Ideally this process converges, as otherwise the computation will run forever!

As an example, we can take our `manages` relation and determine for all employees all managers above them in the organizational chat. To do this, we start from the `manages` relation and write a closure that extends any transitive management pairs by "one hop" along the management relation, using a join operation.

```rust,no_run
    manages   // transitive contains (manager, person) for many hops.
        .iterate(|transitive| {
            transitive
                .map(|(mk, m1)| (m1, mk))
                .join(&transitive)
                .map(|(m1, (mk, p))| (mk, p))
                .concat(&transitive)
                .distinct()
        });
```

Although the first three lines of the closure may look like our skip-level management example, we have three more steps that are very important.

1. We apply a `map` to remove `m1` from the tuple. This was the middle manager, but to have the same type as the input collection we need to produce only pairs.

2. We concatenate in `transitive`, which ensures that we don't "lose" any shorter management relations. Otherwise the loop body would insist that we take two steps along `transitive`.

3. We apply `distinct()` at the end. This is important to ensure convergence. Otherwise, the multiplicities of facts would increase indefinitely. The distinct operator makes sure that we wind down as we stop discovering new transitive management relations.

### Enter

The `enter` operator is a helpful method that brings collections outside a loop into the loop, unchanging as the iterations proceed.

In the example above, we could rewrite

```rust,no_run
    manages   // transitive contains (manager, person) for many hops.
        .iterate(|transitive| {

            let manages = manages.enter(transitive.scope());

            transitive
                .map(|(mk, m1)| (m1, mk))
                .join(&manages)
                .map(|(m1, (mk, p))| (mk, p))
                .concat(&manages)
                .distinct()
        });
```

This modified version extends `transitive` by one step along `manages`, rather than by a step along `transitive`. It also concatenates in `manages` rather than `transitive`. This modified version can perform better, as while it takes shorter steps, they are also more measured.

### Leave

The `leave` operator allows you to extract a collection from an iterative context. It isn't exactly clear how you do this yet, but it will be in just a moment. When you call leave on a collection, it returns a collection in the enclosing scope (outside the iteration) equal to the final value of the collection.

### Variables

You can manually construct iterative contexts, if you like, using differential dataflow's `Variable` type. This is a collection that can be used before its contents are defined, establishing a recursive definition. Its contents will then develop iteratively, much as they do for the iterate operator.

Manual construction can be important when you have mutual recursion, perhaps among multiple collections (rather than the one collection iterate supports), or if you want to return something other than the result of the closure (perhaps intermediate collections).

As an example, the implementation of the `iterate` operator looks something like this:

```rust,no_run
    collection.scope().scoped(|subgraph| {
        let variable = Variable::from(collection.enter(subgraph));
        let result = logic(&variable);
        variable.set(&result);
        result.leave()
    })
```