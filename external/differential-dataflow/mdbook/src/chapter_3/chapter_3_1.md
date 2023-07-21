## Creating Inputs

We've seen already one example of creating a differential dataflow input in our management example.

```rust,no_run
    // create an input collection of data.
    let mut input = InputSession::new();

    // define a new computation.
    worker.dataflow(|scope| {

        // create a new collection from our input.
        let manages = input.to_collection(scope);

        // ...
```

Most commonly, a differential dataflow input is managed by an `InputSession` instance. We've created one above and then turned it in to a differential dataflow collection. Changes that we make to the input session will make their way into the differential dataflow computation; the input session acts as a bridge from our imperative code to the differential dataflow.

### New Collections

You can also create input sessions from the `new_collection` and `new_collection_from` methods defined on timely dataflow scopes, by way of differential dataflow's `Input` trait. These methods allow you to define a collection in-line, and optionally supply initial data for the collection.

For example, above we could have written the above as:

```rust,no_run
    // define a new computation.
    let mut input = worker.dataflow(|scope| {

        // create a new collection from our input.
        let (input, manages) = scope.new_collection();

        // ...

        input
    });
```

Notice that we need to return the input from the closure, and bind it as the result of our call to `dataflow()`.

### As Collections

Any timely dataflow stream of the correct record type, specifically `(data, time, diff)`, can be re-interpreted as a differential dataflow collection using the `AsCollection` trait, which provides a method `as_collection()`.

This operator is helpful in the implementation of differential dataflow operators, when you need to dive in to timely dataflow specializations, and when you need to interoperate with timely dataflow computations. Perhaps you bring your data in from Kafka using timely dataflow; you must change it from a timely dataflow stream to a differential dataflow collection.