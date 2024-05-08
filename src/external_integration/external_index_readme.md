# Adding Integration with External Indices
If you are in need of adding integration with another index implemented by some external library, and have no idea how to do that, this is the right place to get you started.

# What is supported?
Currently, the mechanism supports using indices in the as-of-now mode (that is, further modifications to the index won't trigger recomputation of the answers to old queries). As such, query stream should be append only.

# Why use this instruction?
## Benefits
What do you get from it:
- you don't touch the column context / table context / path storage / autotupling part of the stack 
- no need to define several boilerplate methods that only pass the parameters (happens between the rust-python border and the actual implementation of the operator); on a similar note, you avoid operator boilerplate

# Things that you need to implement:
To plug in the index into graph of computation, you need to provide struct that implements
`ExternalIndex`. To that end, you can either provide a struct that implements `NonFilteringExternalIndex` and use `DerivedFilteredSearchIndex` to add the optional filtering or provide a struct that implements `ExternalIndex` directly (which should be the way to go, if the index provides a built in filter in its search).

# `NonFilteringExternalIndex` (mod.rs)
It has 3 methods:`add(...)`,`remove(...)`, and `search(...)`, and they should interact with the index that is being integrated.

# Builder functions
Additionally, you also need to provide a way to create instances of the index, given some
configuration. To that end:
- you need a struct that implements `ExternalIndexFactory` (its `make_instance` method is used to build an instance of the index for each worker)
- provide a method in `PyIndexFactory`

## Trait ExternalIndexFactory (mod.rs)
This trait describes a structure with one method: `make_instance` and one job - to make an instance of `ExternalIndex`. 
The general idea is that an index specific instance of `ExternalIndexFactory` is initiated with all parameters needed to build an instance of index specific `ExternalIndex`. In order to see how to instantiate this index specific factory, see the next point.

## Method in PyExternalIndexFactory (../python_api/external_index_wrappers.rs)
This struct is the bridge between the Python and Rust layers. You need to provide a method that given set of relevant parameters, returns an instance of struct implementing `ExternalIndexFactory`. This struct is then used by the workers to obtain an instance of `ExternalIndex`. It should have annotations indicating it's mapped to something on the python side (`#[pymethods]` before the `impl` block and `#[staticmethod]` before the method).

## Method binding in engine.pyi
Method in rust `PyIndexFactory` needs to have a counterpart in python `PyIndexFactory` (in `engine.pyi`).

## User Facing API
You need to define some user friendly API, that later down the line:
- calls PyIndexFactory method with according parameters, to get an instance of index factory
- uses this factory and whatever columns were passed as index data / query data / filter-related data to call `use_external_index_as_of_now`
In other words, please don't make `_use_external_index_as_of_now` public, as it'e easier to do a wrapper than to explain the whole _you need to make a factory_ thing.

As of now, all indices are accessible via DataIndex / subclasses of InnerIndex. However, later down the line they may be exposed via some other API, if the indexes are needed in
some other context (e.g standalone data structure).
