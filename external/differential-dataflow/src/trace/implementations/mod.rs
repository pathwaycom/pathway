//! Implementations of `Trace` and associated traits.
//!
//! The `Trace` trait provides access to an ordered collection of `(key, val, time, diff)` tuples, but
//! there is substantial flexibility in implementations of this trait. Depending on characteristics of
//! the data, we may wish to represent the data in different ways. This module contains several of these
//! implementations, and combiners for merging the results of different traces.
//!
//! As examples of implementations,
//!
//! *  The `trie` module is meant to represent general update tuples, with no particular assumptions made
//!    about their contents. It organizes the data first by key, then by val, and then leaves the rest
//!    in an unordered pile.
//!
//! *  The `keys` module is meant for collections whose value type is `()`, which is to say there is no
//!    (key, val) structure on the records; all of them are just viewed as "keys".
//!
//! *  The `time` module is meant for collections with a single time value. This can remove repetition
//!    from the representation, at the cost of requiring more instances and run-time merging.
//!
//! *  The `base` module is meant for collections with a single time value equivalent to the least time.
//!    These collections must always accumulate to non-negative collections, and as such we can indicate
//!    the frequency of an element by its multiplicity. This removes both the time and weight from the
//!    representation, but is only appropriate for a subset (often substantial) of the data.
//!
//! Each of these representations is best suited for different data, but they can be combined to get the
//! benefits of each, as appropriate. There are several `Cursor` combiners, `CursorList` and `CursorPair`,
//! for homogenous and inhomogenous cursors, respectively.
//!
//! #Musings
//!
//! What is less clear is how to transfer updates between the representations at merge time in a tasteful
//! way. Perhaps we could put an ordering on the representations, each pair with a dominant representation,
//! and part of merging the latter filters updates into the former. Although back and forth might be
//! appealing, more thinking is required to negotiate all of these policies.
//!
//! One option would be to require the layer builder to handle these smarts. Merging is currently done by
//! the layer as part of custom code, but we could make it simply be "iterate through cursor, push results
//! into 'ordered builder'". Then the builder would be bright enough to emit a "batch" for the composite
//! trace, rather than just a batch of the type merged.

pub mod spine_fueled;

mod merge_batcher;

pub use self::merge_batcher::MergeBatcher as Batcher;

pub mod ord;
