//! Traits and types related to the distribution of data.
//!
//! These traits and types are in support of a flexible approach to data distribution and organization,
//! in which we might like to more explicitly manage how certain types are handled. Although the term
//! "hashing" is used throughout, it is a misnomer; these traits relate to extracting reasonably distributed
//! integers from the types, and hashing happens to be evocative of this.
//!
//! Differential dataflow operators need to co-locate data that are equivalent so that they may have
//! the differences consolidated, and eventually cancelled. The chose approach is to extract an integer
//! from the keys of the data, ensuring that elements with the same key arrive at the same worker, where
//! the consolidation can occur.
//!
//! The intent is that types should be able to indicate how this integer is determined, so that general
//! data types can use a generic hash function, where as more specialized types such as uniformly
//! distributed integers can perhaps do something simpler (like report their own value).

use std::hash::Hasher;

/// Types with a `hashed` method, producing an unsigned output of some type.
///
/// The output type may vary from a `u8` up to a `u64`, allowing types with simple keys
/// to communicate this through their size. Certain algorithms, for example radix sorting,
/// can take advantage of the smaller size.
pub trait Hashable {
    /// The type of the output value.
    type Output: Into<u64>+Copy;
    /// A well-distributed integer derived from the data.
    fn hashed(&self) -> Self::Output;
}

impl<T: ::std::hash::Hash> Hashable for T {
    type Output = u64;
    fn hashed(&self) -> u64 {
        let mut h: ::fnv::FnvHasher = Default::default();
        self.hash(&mut h);
        h.finish()
    }
}
