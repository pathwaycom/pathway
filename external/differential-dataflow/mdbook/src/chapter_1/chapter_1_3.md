# Iteration

One of the most interesting parts of differential dataflow is its support for iterative computation.

Iteration is accomplished in differential dataflow with the `iterate` operator, a method that takes one collection as an argument, and logic describing how you would transform that collection as one step of iteration. The operator then applies this logic to the input collection indefinitely, producing as output the "fixed point" of your logic starting from your input collection.

That might sound a bit complicated, so let's look at an example.

Perhaps you have a collections of `(Manager, Employee)` pairs indicating who manages whom. Perhaps you are also really excited to produce (and maintain) the number of employees in total under each manager.

Let's describe an iterative computation that starts from manager-employee relation, and repeatedly expands it to include transitive management relationships.

```rust,ignore
    manager_employee
        .iterate(|manages| {
            // if x manages y, and y manages z, then x manages z (transitively).
            manages
                .map(|(x, y)| (y, x))
                .join(&manages)
                .map(|(y, x, z)| (x, z))
        });
```

This is a simplistic use of `iterate`, and your needs may become more complicated.

## General Iteration

More generally, you may want to describe iterative computations with i. multiple inputs, ii. multiple loop variables, and iii. multiple outputs. Differential dataflow provides a way to manually construct iterative contexts, where you can do all of these things.

Timely dataflow provides "scopes", which are nested dataflows in which you can augment timestamps with further information, for example a "round of iteration" for a loop. Our first step to construct a general iterative computation is to create a new scope within the current scope.

```rust,ignore
    // if you don't otherwise have the scope ..
    let scope = manager_employee.scope();

    scope.scoped(|subscope| {

        // More stuff will go here

    });
```

Each timely dataflow stream, and differential dataflow collection, are associated with a scope. To use a collection that is outside our subscope, we will need to bring it in to the subscope. This is done with the `enter` operator.

```rust,ignore
    // if you don't otherwise have the scope ..
    let scope = manager_employee.scope();

    scope.scoped(|subscope| {

        // we can now use m_e in this scope.
        let m_e = manager_employee.enter(subscope);

    });
```

To create an iterative computation, we now need to define some variables that can be updated in each round of iteration. Differential dataflow provides a [Variable](https://github.com/frankmcsherry/differential-dataflow/blob/master/src/operators/iterate.rs#L132-L137) struct that does exactly this. We create a variable by specifying its initial value (a collection), and then `set` the definition of the collection which will instruct it how to update.

```rust,ignore
    // if you don't otherwise have the scope ..
    let scope = manager_employee.scope();

    scope.scoped(|subscope| {

        // we can now use m_e in this scope.
        let m_e = manager_employee.enter(subscope);

        let variable = Variable::from(m_e);

        let step =
        variable
            .map(|(x, y)| (y, x))
            .join(&variable)
            .map(|(y, x, z)| (x, z));

        variable.set(step);

    });
```

Finally, we probably want to return the final value of the variable, what it converges to (assuming it does so). There is a `leave` operator that matches the `enter` operator we used to bring data into the scope; it produces the final value of the collection it is called on:

```rust,ignore
    // if you don't otherwise have the scope ..
    let scope = manager_employee.scope();

    let result =
    scope.scoped(|subscope| {

        // we can now use m_e in this scope.
        let m_e = manager_employee.enter(subscope);

        let variable = Variable::from(m_e);

        let step =
        variable
            .map(|(x, y)| (y, x))
            .join(&variable)
            .map(|(y, x, z)| (x, z));

        variable
            .set(step)
            .leave()
    });
```

Although a bit more verbose, this is (or should be) the same computation we described up above with the `iterate` method. However, if you found you needed to use more inputs, outputs, or variables, it will be here for you.