## The Join Operator

The `join` operator takes two input collections, each of which must have records with a `(key, value)` structure, and must have the same type of `key`. For each pair of elements with matching key, one from each input, the join operator produces the output `(key, (value1, value2))`.

Our example from earlier uses a join to match up pairs `(m2, m1)` and `(m1, p)` when the `m1` is in common. To do this, we first have to switch the records in the first collection around, so that they are keyed by `m1` instead of `m2`.

```rust,no_run
    manages
        .map(|(m2, m1)| (m1, m2))
        .join(&manages)
        .inspect(|x| println!("{:?}", x));
```

The join operator multiplies frequencies, so if a record `(key, val1)` has multiplicity five, and a matching record `(key, val2)` has multiplicity three, the output result will be `(key, (val1, val2))` with multiplicity fifteen.
