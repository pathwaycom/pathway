//! Traits and types for building trie-based indices.
//!
//! The trie structure has each each element of each layer indicate a range of elements
//! in the next layer. Similarly, ranges of elements in the layer itself may correspond
//! to single elements in the layer above.

#[cfg(feature = "columnation")]
use timely::container::columnation::TimelyStack;
#[cfg(feature = "columnation")]
use timely::container::columnation::Columnation;

pub mod ordered;
pub mod ordered_leaf;
// pub mod hashed;
// pub mod weighted;
// pub mod unordered;

/// A collection of tuples, and types for building and enumerating them.
///
/// There are some implicit assumptions about the elements in trie-structured data, mostly that
/// the items have some `(key, val)` structure. Perhaps we will nail these down better in the
/// future and get a better name for the trait.
pub trait Trie  : ::std::marker::Sized {
    /// The type of item from which the type is constructed.
    type Item;
    /// The type of cursor used to navigate the type.
    type Cursor: Cursor<Self>;
    /// The type used to merge instances of the type together.
    type MergeBuilder: MergeBuilder<Trie=Self>;
    /// The type used to assemble instances of the type from its `Item`s.
    type TupleBuilder: TupleBuilder<Trie=Self, Item=Self::Item>;

    /// The number of distinct keys, as distinct from the total number of tuples.
    fn keys(&self) -> usize;
    /// The total number of tuples in the collection.
    fn tuples(&self) -> usize;
    /// Returns a cursor capable of navigating the collection.
    fn cursor(&self) -> Self::Cursor { self.cursor_from(0, self.keys()) }
    /// Returns a cursor over a range of data, commonly used by others to restrict navigation to
    /// sub-collections.
    fn cursor_from(&self, lower: usize, upper: usize) -> Self::Cursor;

    /// Merges two collections into a third.
    ///
    /// Collections are allowed their own semantics for merging. For example, unordered collections
    /// simply collect values, whereas weighted collections accumulate weights and discard elements
    /// whose weights are zero.
    fn merge(&self, other: &Self) -> Self {
        let mut merger = Self::MergeBuilder::with_capacity(self, other);
        // println!("{:?} and {:?}", self.keys(), other.keys());
        merger.push_merge((self, 0, self.keys()), (other, 0, other.keys()));
        merger.done()
    }
}

/// A type used to assemble collections.
pub trait Builder {
    /// The type of collection produced.
    type Trie: Trie;
    /// Requests a commitment to the offset of the current-most sub-collection.
    ///
    /// This is most often used by parent collections to indicate that some set of values are now
    /// logically distinct from the next set of values, and that the builder should acknowledge this
    /// and report the limit (to store as an offset in the parent collection).
    fn boundary(&mut self) -> usize;
    /// Finalizes the building process and returns the collection.
    fn done(self) -> Self::Trie;
}

/// A type used to assemble collections by merging other instances.
pub trait MergeBuilder : Builder {
    /// Allocates an instance of the builder with sufficient capacity to contain the merged data.
    fn with_capacity(other1: &Self::Trie, other2: &Self::Trie) -> Self;
    /// Copies sub-collections of `other` into this collection.
    fn copy_range(&mut self, other: &Self::Trie, lower: usize, upper: usize);
    /// Merges two sub-collections into one sub-collection.
    fn push_merge(&mut self, other1: (&Self::Trie, usize, usize), other2: (&Self::Trie, usize, usize)) -> usize;
}

/// A type used to assemble collections from ordered sequences of tuples.
pub trait TupleBuilder : Builder {
    /// The type of item accepted for construction.
    type Item;
    /// Allocates a new builder.
    fn new() -> Self;
    /// Allocates a new builder with capacity for at least `cap` tuples.
    fn with_capacity(cap: usize) -> Self;    // <-- unclear how to set child capacities...
    /// Inserts a new into the collection.
    fn push_tuple(&mut self, tuple: Self::Item);
}

/// A type supporting navigation.
///
/// The precise meaning of this navigation is not defined by the trait. It is likely that having
/// navigated around, the cursor will be different in some other way, but the `Cursor` trait does
/// not explain how this is so.
pub trait Cursor<Storage> {
    /// The type revealed by the cursor.
    type Key;
    /// Reveals the current key.
    fn key<'a>(&self, storage: &'a Storage) -> &'a Self::Key;
    /// Advances the cursor by one element.
    fn step(&mut self, storage: &Storage);
    /// Advances the cursor until the location where `key` would be expected.
    /// [Pathway comment]: more precisely: it finds a position of first non-smaller element
    /// [Pathway comment]: this additional specification is relevant when:
    /// [Pathway comment]: - key is no present 
    /// [Pathway comment]: - key is present multiple times
    fn seek(&mut self, storage: &Storage, key: &Self::Key);
    /// Returns `true` if the cursor points at valid data. Returns `false` if the cursor is exhausted.
    fn valid(&self, storage: &Storage) -> bool;
    /// Rewinds the cursor to its initial state.
    fn rewind(&mut self, storage: &Storage);
    /// Repositions the cursor to a different range of values.
    fn reposition(&mut self, storage: &Storage, lower: usize, upper: usize);
    /// [Pathway extension]: Recedes the cursor by one element.
    fn step_back(&mut self, storage: &Storage);
    /// [Pathway extension]: Advances the cursor to the last position that holds
    /// [Pathway extension]: element smaller or equal to the specified key.
    fn seek_smaller_eq(&mut self, storage: &Storage, key: &Self::Key);
}

/// A general-purpose container resembling `Vec<T>`.
pub trait BatchContainer: Default {
    /// The type of contained item.
    type Item;
    /// Inserts an owned item.
    fn push(&mut self, item: Self::Item);
    /// Inserts a borrowed item.
    fn copy(&mut self, item: &Self::Item);
    /// Extends from a slice of items.
    fn copy_slice(&mut self, slice: &[Self::Item]);
    /// Creates a new container with sufficient capacity.
    fn with_capacity(size: usize) -> Self;
    /// Reserves additional capacity.
    fn reserve(&mut self, additional: usize);
    /// Creates a new container with sufficient capacity.
    fn merge_capacity(cont1: &Self, cont2: &Self) -> Self;
}

impl<T: Clone> BatchContainer for Vec<T> {
    type Item = T;
    fn push(&mut self, item: T) {
        self.push(item);
    }
    fn copy(&mut self, item: &T) {
        self.push(item.clone());
    }
    fn copy_slice(&mut self, slice: &[T]) {
        self.extend_from_slice(slice);
    }
    fn with_capacity(size: usize) -> Self {
        Vec::with_capacity(size)
    }
    fn reserve(&mut self, additional: usize) {
        self.reserve(additional);
    }
    fn merge_capacity(cont1: &Self, cont2: &Self) -> Self {
        Vec::with_capacity(cont1.len() + cont2.len())
    }
}

#[cfg(feature = "columnation")]
impl<T: Columnation> BatchContainer for TimelyStack<T> {
    type Item = T;
    fn push(&mut self, item: T) {
        self.copy(&item);
    }
    fn copy(&mut self, item: &T) {
        self.copy(item);
    }
    fn copy_slice(&mut self, slice: &[T]) {
        self.reserve_items(slice.iter());
        for item in slice.iter() {
            self.copy(item);
        }
    }
    fn with_capacity(size: usize) -> Self {
        Self::with_capacity(size)
    }
    fn reserve(&mut self, _additional: usize) {
    }
    fn merge_capacity(cont1: &Self, cont2: &Self) -> Self {
        let mut new = Self::default();
        new.reserve_regions(std::iter::once(cont1).chain(std::iter::once(cont2)));
        new
    }
}


/// Reports the number of elements satisfing the predicate.
///
/// This methods *relies strongly* on the assumption that the predicate
/// stays false once it becomes false, a joint property of the predicate
/// and the slice. This allows `advance` to use exponential search to
/// count the number of elements in time logarithmic in the result.
pub fn advance<T, F: Fn(&T)->bool>(slice: &[T], function: F) -> usize {

    let small_limit = 8;

    // Exponential seach if the answer isn't within `small_limit`.
    if slice.len() > small_limit && function(&slice[small_limit]) {

        // start with no advance
        let mut index = small_limit + 1;
        if index < slice.len() && function(&slice[index]) {

            // advance in exponentially growing steps.
            let mut step = 1;
            while index + step < slice.len() && function(&slice[index + step]) {
                index += step;
                step = step << 1;
            }

            // advance in exponentially shrinking steps.
            step = step >> 1;
            while step > 0 {
                if index + step < slice.len() && function(&slice[index + step]) {
                    index += step;
                }
                step = step >> 1;
            }

            index += 1;
        }

        index
    }
    else {
        let limit = std::cmp::min(slice.len(), small_limit);
        slice[..limit].iter().filter(|x| function(*x)).count()
    }
}
