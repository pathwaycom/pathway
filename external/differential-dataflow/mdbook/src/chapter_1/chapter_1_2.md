# Operators

Differential dataflow operators are how we build more interesting programs. There are relatively few operators, and the game is to figure out how to use them to do what you need. Let's now go through a few of the operators!

## Map

The `map` operator is like the map operator in Rust's `Iterator` trait, and the map method in timely dataflow: it takes a closure that can be applied to each record to transform it to some new output. For example, we might write:

```rust,ignore
    collection
        .map(|word| word.len())
        .count()
```

which would transform each of our words into their length, and then count the number of times each length appears. This would give us a shorter synopses of our data, if we don't actually care about the word itself.

The map operator is also used for "projection" in the database sense, where we have an input records containing many fields and we just want to select out some of them for further computation. This is handy to do as soon as possible, so that the next bit of computation only needs to manage the data relevant to its interests.

## Filter

The `filter` operator is also like that of Rust's `Iterator` trait and the method in timely dataflow: it takes a closure that reports "true" or "false" for each input record, and passes only those records that result in true. If we only wanted to keep relatively short strings, we might write

```rust,ignore
    collection
        .filter(|word| word.len() < 8)
        .count()
```

Perhaps we want to count all words but only report those words whose count is at least 100.

```rust,ignore
    collection
        .count()
        .filter(|(word, count)| count >= 100)
        .map(|(word, _count)| word);
```

This computation has the potential to *change* much less frequently than its input changes. Even though counts may go up and down, it is only when a word's frequency crosses the 100 boundary that we report a change.

## Concat

The `concat` operator merges two collections together, essentially adding the occurrence frequencies of each record. For example, we might have words coming in from two sources, that perhaps change independently:

```rust,ignore
    collection1
        .concat(&collection2)
        .count()
```

## Join

The `join` operator is one of the first exotic operations that isn't just a record-by-record operation. The join method only acts on two collections, each of which must have data of the form `(key, val1)` and `(key, val2)`. That is, their data must be pairs, and the type of the first element must match.

The join operator results in a collection whose data have type `(key, val1, val2)` where there is an output triple for each pair of records in the two inputs with matching key. More specifically, the number of times the record `(key, val1, val2)` appears in the output is the product of the number of times `(key, val1)` and `(key, val2)` appear in their respective inputs.

Join has a lot of uses, but one common example is to "look up" data. If we have a collection containing pairs `(person, address)`, we can use joins against this relation to recover the address of a person (perhaps we are trying to deliver a package to them):

```rust,ignore
    let deliver_to =
    ordered_by
        .join(&person_address)
        .map(|(person, package, address)| (package, address));
```

Alternately, we can use the same relation to find people living at a given address (perhaps because we want to allow any of them to sign for the package we want to deliver).

```rust,ignore
    let can_sign_for =
    deliver_to
        .map(|(package, address)| (address, package))
        .join(&person_address.map(|(p,a)| (a,p)))
        .map(|(address, package, person)| (package, person));
```

As the underlying `ordered_by` and `person_address` collections change, the derived `deliver_to` and `can_sign_for` collections will change as well, maintaining correct and consistent results corresponding to the inputs.

## Reduce

The `reduce` operator applies to one input collection whose records have the form `(key, val)`, and it allows you produce output as an arbitrary function of the key and the list of values. The following example starts from the list of all orders, and produces any duplicate packages in the ordering system (those with count two or greater).

```rust,ignore
    ordered_by
        .map(|(package, person)| (person, package))
        .reduce(|person, packages, duplicates| {
            for (package, count) in packages.iter() {
                if count > 1 {
                    duplicates.push((package.clone(), count));
                }
            }
        });
```

There are some subtle details here, ones that will likely trip you up (as they trip up me):

The second and third arguments (the input and output, here `packages` and `duplicates`) contain pairs `(val, count)`. This is great when we want to count things that occur many times (in that `("word", 1000000)` is more succinct than one million copies of `"word"), but in casual use we need to remember that even when we expect the numbers to be mostly one, we need to use them.

In actual fact the input (`packages`) contains pairs of type `(&Val, Count)`, which in Rust-isms mean that you only get to view the associated value, you do not get to take ownership of it. This means that if we want to reproduce it in the output we need to do something like `.clone()` to get a new copy. If it were a string, or had other allocated data behind it, our read-only access to that data means we need to spend the time to create new copies for the output.