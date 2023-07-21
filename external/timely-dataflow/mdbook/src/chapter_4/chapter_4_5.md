# Custom Datatypes

**WORK IN PROGRESS**

Timely dataflow allows you to use a variety of Rust types, but you may also find that you need (or would prefer) your own `struct` and `enum` types.

Timely dataflow provides two traits, `Data` and `ExchangeData` for types that timely dataflow can transport within a worker thread and across threads.

## The `Data` trait

The `Data` trait is essentially a synonym for `Clone+'static`, meaning the type must be cloneable and cannot contain any references with other than a static lifetime. Most types implement these traits automatically, but if yours do not you should decorate your struct definition with a derivation of the `Clone` trait:

```rust,ignore
#[derive(Clone)]
struct YourStruct { .. }
```

## The `ExchangeData` trait

The `ExchangeData` trait is more complicated, and is established in the `communication/` module. There are two options for this trait, which are determined by whether you use the `--bincode` feature at compilation, or not.

*   If you use `--bincode` then the trait is a synonym for

    ```rust,ignore
    Send+Sync+Any+serde::Serialize+for<'a>serde::Deserialize<'a>+'static
    ```

    where `serde` is Rust's most popular serialization and deserialization crate. A great many types implement these traits. If your types does not, you should add these decorators to their definition:

    ```rust,ignore
    #[derive(Serialize, Deserialize)]
    ```

    You must include the `serde` crate, and if not on Rust 2018 the `serde_derive` crate.

    The downside to the `--bincode` flag is that deserialization will always involve a clone of the data, which has the potential to adversely impact performance. For example, if you have structures that contain lots of strings, timely dataflow will create allocations for each string even if you do not plan to use all of them.

*  If you do not use the `--bincode` feature, then the `Serialize` and `Deserialize` requirements are replaced by `Abomonation`, from the `abomonation` crate. This trait allows in-place deserialization, but is implemented for fewer types, and has the potential to be a bit scarier (due to in-place pointer correction).

    Your types likely do not implement `Abomonation` by default, but you can similarly use

    ```rust,ignore
    #[derive(Abomonation)]
    ```

    You must include the `abomonation` and `abomonation_derive` crate for this to work correctly.

## An example

Let's imagine you would like to play around with a tree data structure as something you might send around in timely dataflow. I've written the following candidate example:

```rust,ignore
extern crate timely;

use timely::dataflow::operators::*;

fn main() {
    timely::example(|scope| {
        (0..10).to_stream(scope)
               .map(|x| TreeNode::new(x))
               .inspect(|x| println!("seen: {:?}", x));
    });
}

// A tree structure.
struct TreeNode<D> {
    data: D,
    children: Vec<TreeNode<D>>,
}

impl<D> TreeNode<D> {
    fn new(data: D) -> Self {
        Self { data, children: Vec::new() }
    }
}
```

This doesn't work. You'll probably get two errors, that `TreeNode` doesn't implement `Clone`, nor does it implement `Debug`. Timely data types need to implement `Clone`, and our attempt to print out the trees requires an implementation of `Debug`. We can create these implementations by decorating the `struct` declaration like so:

```rust
#[derive(Clone, Debug)]
struct TreeNode<D> {
    data: D,
    children: Vec<TreeNode<D>>,
}
```

This works for me! We see

```ignore
    Echidnatron% cargo run --example types
       Compiling timely v0.8.0 (/Users/mcsherry/Projects/timely-dataflow)
        Finished dev [unoptimized + debuginfo] target(s) in 5.33s
         Running `target/debug/examples/types`
    seen: TreeNode { data: 0, children: [] }
    seen: TreeNode { data: 1, children: [] }
    seen: TreeNode { data: 2, children: [] }
    seen: TreeNode { data: 3, children: [] }
    seen: TreeNode { data: 4, children: [] }
    seen: TreeNode { data: 5, children: [] }
    seen: TreeNode { data: 6, children: [] }
    seen: TreeNode { data: 7, children: [] }
    seen: TreeNode { data: 8, children: [] }
    seen: TreeNode { data: 9, children: [] }
    Echidnatron%
```

### Exchanging data

Let's up the level a bit and try and shuffle our tree data between workers.

If we replace our `main` method with this new one:

```rust,ignore
extern crate timely;

use timely::dataflow::operators::*;

fn main() {
    timely::example(|scope| {
        (0..10).to_stream(scope)
               .map(|x| TreeNode::new(x))
               .exchange(|x| x.data)
               .inspect(|x| println!("seen: {:?}", x));
    });
}

#[derive(Clone, Debug)]
struct TreeNode<D> {
    data: D,
    children: Vec<TreeNode<D>>,
}

impl<D> TreeNode<D> {
    fn new(data: D) -> Self {
        Self { data, children: Vec::new() }
    }
}
```

We get a new error. A not especially helpful error. It says that it cannot find an `exchange` method, or more specifically that one exists but it doesn't apply to our type at hand. This is because the data need to satisfy the `ExchangeData` trait but do not. It would be better if this were clearer in the error messages, I agree.

We can fix the problem two ways. First, if you would like to use `bincode`, then we update the source like so:

```rust,ignore
#[macro_use]
extern crate serde_derive;
extern crate serde;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct TreeNode<D> {
    data: D,
    children: Vec<TreeNode<D>>,
}
```

and make sure to include the `serde_derive` and `serde` crates. Now when we run things (notice the `--features` flag) we see:

```ignore
    Echidnatron% cargo run --example types --features bincode
        Finished dev [unoptimized + debuginfo] target(s) in 0.07s
         Running `target/debug/examples/types`
    seen: TreeNode { data: 0, children: [] }
    seen: TreeNode { data: 1, children: [] }
    seen: TreeNode { data: 2, children: [] }
    seen: TreeNode { data: 3, children: [] }
    seen: TreeNode { data: 4, children: [] }
    seen: TreeNode { data: 5, children: [] }
    seen: TreeNode { data: 6, children: [] }
    seen: TreeNode { data: 7, children: [] }
    seen: TreeNode { data: 8, children: [] }
    seen: TreeNode { data: 9, children: [] }
    Echidnatron%
```

Great news!
