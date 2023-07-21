## The Map Operator

The `map` operator applies a supplied function to each element of a collection, the results of which are accumulated into a new collection. The map operator preserves the counts of elements, and any elements that are made equal by the map operator will have their counts accumulate.

As an example, our example program used `map` to reverse the pairs of identifiers in the `manages` collection, to place the second element first.

```rust,no_run
    manages
        .map(|(m2, m1)| (m1, m2))
        .join(&manages)
        .inspect(|x| println!("{:?}", x));
```

If instead we had just written

```rust,no_run
    manages
        .map(|(m2, m1)| m2);
```

we would have a collection containing each manager with a multiplicity equal to the number of individuals they manage.