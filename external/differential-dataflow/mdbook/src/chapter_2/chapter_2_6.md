## The Reduce Operator

The `reduce` operator takes an input collection whose records have a `(key, value)` structure, and it applies a user-supplied reduction closure to each group of values with the same key.

For example, to produce for each manager their managee with the lowest identifier, we might write

```rust,no_run
    manages
        .reduce(|_key, input, output| {
            let mut min_index = 0;

            // Each element of input is a `(&Value, Count)`
            for index in 1 .. input.len() {
                if input[min_index].0 > input[index].0 {
                    min_index = index;
                }
            }

            // Must produce outputs as `(Value, Count)`.
            output.push((*input[min_index].0, 1));
        });
```

The reduce operator has some tricky Rust details about how it is expressed. The type of closure you must provide is technically

```rust,no_run
        Fn(&Key, &[(&Val, Cnt)], &mut Vec<(Val2, Cnt2)>)
```

which means a function of three arguments:

    1. A reference to the common key (`_key` above).
    2. A slice (list) of pairs of value references and counts.
    3. A mutable vector into which one can put pairs of values and counts.

The method is structured this way so that you can efficiently observe and manipulate records with large multiplicities without actually walking through that number of records. For example, we can write a `count` operator much more efficiently with the count looking at us than if we had to traverse as many copies of each record as we were counting up.

Speaking of which ...

### Count

The convenience method `count` wraps the `reduce` operator, and performs the common operation much more easily. The count operator takes arbitrary input collections, and produces a collection as output whose records are pairs of input records and their accumulated count.

### Distinct

The `distinct` operator is another convenience operator, and it takes any input collection to one in which each input record occurs at most once. The distinct operator is a great way to recover set semantics despite differential dataflow's native multiset semantics.

### Threshold

More general than `distinct`, the `threshold` operator takes any function from one count to another count and yields the collection with counts correspondingly updated. This is used to implement the distinct operator, but also operators like "records with count at least three".