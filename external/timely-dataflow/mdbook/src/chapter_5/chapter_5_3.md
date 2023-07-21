# Containers

Timely's core isn't tied to a specific representation of data that flows along dataflow edges.
While some convenience operators make assumptions about the type of batches, the core infrastructure is generic in what containers are exchanged.
This section explains what containers are and what their contract with the Timely APIs is.

Many parts of Timely assume that data is organized into `Vec<T>`, i.e., batches store data as consecutive elements in memory.
This abstractions works well for many cases but precludes some advanced techniques, such as transferring translated or columnar data between operators.
With the container abstraction, Timely specifies a minimal interface it requires tracking progress and provide data to operators.

## Core operators

In Timely, we provide a set of `Core` operators that are generic on the container type they can handle.
In most cases, the `Core` operators are a immediate generalization of their non-core variant, providing the semantically equivalent functionality.

## Limitations

A challenge when genericizing Timely operators is that all interfaces need to be declared independent of a concrete type, for example as part of a trait.
For this reason, Timely doesn't currently support operators that require knowledge of the elements of a container or how to partition a container, with the only exception being the `Vec` type.

## A custom container

Let's walk through an example container that resembles a `Result` type, but moves the storage to within the result.

```rust
extern crate timely_container;

use timely_container::Container;

#[derive(Clone)]
enum ResultContainer<T, E> {
    Ok(Vec<T>),
    Err(E),
}

impl<T, E> Default for ResultContainer<T, E> {
    fn default() -> Self {
        Self::Ok(Default::default())
    }
}

impl<T: Clone + 'static, E: Clone + 'static> Container for ResultContainer<T, E> {
    type Item = Result<T, E>;

    fn len(&self) -> usize {
        match self {
            ResultContainer::Ok(data) => data.len(),
            ResultContainer::Err(_) => 1,
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            ResultContainer::Ok(data) => data.is_empty(),
            ResultContainer::Err(_) => false,
        }
    }

    fn capacity(&self) -> usize {
        match self {
            ResultContainer::Ok(data) => data.capacity(),
            ResultContainer::Err(_) => 1,
        }
    }

    fn clear(&mut self) {
        match self {
            ResultContainer::Ok(data) => data.clear(),
            ResultContainer::Err(_) => {},
        }
    }
}
```

The type can either store a vector of data, or a single error.
Its length is the length of the vector, or 1 if it represents an error.
