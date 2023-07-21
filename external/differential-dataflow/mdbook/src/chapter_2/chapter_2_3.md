## The Concat Operator

The `concat` operator takes two collections whose element have the same type, and produces the collection in which the counts of each element are added together.

For example, we might form the symmetric "management relation" by concatenating the `manages` collection with the same collection with its fields flipped:

```rust,no_run
    manages
        .map(|(m2, m1)| (m1, m2))
        .concat(&manages);
```

This collection likely has at most one copy of each record, unless perhaps any manager manages itself. In fact, zero manages itself, and the element `(0, 0)` would have count two.

Importantly, `concat` doesn't do the hard work of ensuring that there is only one physical of each element. If we inspect the output of the `concat` above, we might see

        ((0, 0), 0, 1)
        ((0, 0), 0, 1)

Although these are two updates to the same element at the same time, `concat` is a bit lazy (read: efficient) and doesn't do the hard work until we ask it. For that, we'll need the `consolidate` operator.