//! Tracks minimal sets of mutually incomparable elements of a partial order.

use crate::progress::ChangeBatch;
use crate::order::{PartialOrder, TotalOrder};

/// A set of mutually incomparable elements.
///
/// An antichain is a set of partially ordered elements, each of which is incomparable to the others.
/// This antichain implementation allows you to repeatedly introduce elements to the antichain, and
/// which will evict larger elements to maintain the *minimal* antichain, those incomparable elements
/// no greater than any other element.
///
/// Two antichains are equal if the contain the same set of elements, even if in different orders.
/// This can make equality testing quadratic, though linear in the common case that the sequences
/// are identical.
#[derive(Debug, Abomonation, Serialize, Deserialize)]
pub struct Antichain<T> {
    elements: Vec<T>
}

impl<T: PartialOrder> Antichain<T> {
    /// Updates the `Antichain` if the element is not greater than or equal to some present element.
    ///
    /// Returns true if element is added to the set
    ///
    /// # Examples
    ///
    ///```
    /// use timely::progress::frontier::Antichain;
    ///
    /// let mut frontier = Antichain::new();
    /// assert!(frontier.insert(2));
    /// assert!(!frontier.insert(3));
    ///```
    pub fn insert(&mut self, element: T) -> bool {
        if !self.elements.iter().any(|x| x.less_equal(&element)) {
            self.elements.retain(|x| !element.less_equal(x));
            self.elements.push(element);
            true
        }
        else {
            false
        }
    }

    /// Updates the `Antichain` if the element is not greater than or equal to some present element.
    ///
    /// Returns true if element is added to the set
    ///
    /// Accepts a reference to an element, which is cloned when inserting.
    ///
    /// # Examples
    ///
    ///```
    /// use timely::progress::frontier::Antichain;
    ///
    /// let mut frontier = Antichain::new();
    /// assert!(frontier.insert_ref(&2));
    /// assert!(!frontier.insert(3));
    ///```
    pub fn insert_ref(&mut self, element: &T) -> bool where T: Clone {
        if !self.elements.iter().any(|x| x.less_equal(element)) {
            self.elements.retain(|x| !element.less_equal(x));
            self.elements.push(element.clone());
            true
        }
        else {
            false
        }
    }

    /// Reserves capacity for at least additional more elements to be inserted in the given `Antichain`
    pub fn reserve(&mut self, additional: usize) {
        self.elements.reserve(additional);
    }

    /// Performs a sequence of insertion and return true iff any insertion does.
    ///
    /// # Examples
    ///
    ///```
    /// use timely::progress::frontier::Antichain;
    ///
    /// let mut frontier = Antichain::new();
    /// assert!(frontier.extend(Some(3)));
    /// assert!(frontier.extend(vec![2, 5]));
    /// assert!(!frontier.extend(vec![3, 4]));
    ///```
    pub fn extend<I: IntoIterator<Item=T>>(&mut self, iterator: I) -> bool {
        let mut added = false;
        for element in iterator {
            added = self.insert(element) || added;
        }
        added
    }

    /// Returns true if any item in the antichain is strictly less than the argument.
    ///
    /// # Examples
    ///
    ///```
    /// use timely::progress::frontier::Antichain;
    ///
    /// let mut frontier = Antichain::from_elem(2);
    /// assert!(frontier.less_than(&3));
    /// assert!(!frontier.less_than(&2));
    /// assert!(!frontier.less_than(&1));
    ///
    /// frontier.clear();
    /// assert!(!frontier.less_than(&3));
    ///```
    #[inline]
    pub fn less_than(&self, time: &T) -> bool {
        self.elements.iter().any(|x| x.less_than(time))
    }

    /// Returns true if any item in the antichain is less than or equal to the argument.
    ///
    /// # Examples
    ///
    ///```
    /// use timely::progress::frontier::Antichain;
    ///
    /// let mut frontier = Antichain::from_elem(2);
    /// assert!(frontier.less_equal(&3));
    /// assert!(frontier.less_equal(&2));
    /// assert!(!frontier.less_equal(&1));
    ///
    /// frontier.clear();
    /// assert!(!frontier.less_equal(&3));
    ///```
    #[inline]
    pub fn less_equal(&self, time: &T) -> bool {
        self.elements.iter().any(|x| x.less_equal(time))
    }

    /// Returns true if every element of `other` is greater or equal to some element of `self`.
    #[deprecated(since="0.12.0", note="please use `PartialOrder::less_equal` instead")]
    #[inline]
    pub fn dominates(&self, other: &Antichain<T>) -> bool {
        <Self as PartialOrder>::less_equal(self, other)
    }
}

impl<T: PartialOrder> std::iter::FromIterator<T> for Antichain<T> {
    fn from_iter<I>(iterator: I) -> Self
    where
        I: IntoIterator<Item=T>
    {
        let mut result = Self::new();
        result.extend(iterator);
        result
    }
}

impl<T> Antichain<T> {

    /// Creates a new empty `Antichain`.
    ///
    /// # Examples
    ///
    ///```
    /// use timely::progress::frontier::Antichain;
    ///
    /// let mut frontier = Antichain::<u32>::new();
    ///```
    pub fn new() -> Antichain<T> { Antichain { elements: Vec::new() } }

    /// Creates a new empty `Antichain` with space for `capacity` elements.
    ///
    /// # Examples
    ///
    ///```
    /// use timely::progress::frontier::Antichain;
    ///
    /// let mut frontier = Antichain::<u32>::with_capacity(10);
    ///```
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            elements: Vec::with_capacity(capacity),
        }
    }

    /// Creates a new singleton `Antichain`.
    ///
    /// # Examples
    ///
    ///```
    /// use timely::progress::frontier::Antichain;
    ///
    /// let mut frontier = Antichain::from_elem(2);
    ///```
    pub fn from_elem(element: T) -> Antichain<T> { Antichain { elements: vec![element] } }

    /// Clears the contents of the antichain.
    ///
    /// # Examples
    ///
    ///```
    /// use timely::progress::frontier::Antichain;
    ///
    /// let mut frontier = Antichain::from_elem(2);
    /// frontier.clear();
    /// assert!(frontier.elements().is_empty());
    ///```
    pub fn clear(&mut self) { self.elements.clear() }

    /// Sorts the elements so that comparisons between antichains can be made.
    pub fn sort(&mut self) where T: Ord { self.elements.sort() }

    /// Reveals the elements in the antichain.
    ///
    /// This method is redundant with `<Antichain<T> as Deref>`, but the method
    /// is in such broad use that we probably don't want to deprecate it without
    /// some time to fix all things.
    ///
    /// # Examples
    ///
    ///```
    /// use timely::progress::frontier::Antichain;
    ///
    /// let mut frontier = Antichain::from_elem(2);
    /// assert_eq!(frontier.elements(), &[2]);
    ///```
    #[inline] pub fn elements(&self) -> &[T] { &self[..] }

    /// Reveals the elements in the antichain.
    ///
    /// # Examples
    ///
    ///```
    /// use timely::progress::frontier::Antichain;
    ///
    /// let mut frontier = Antichain::from_elem(2);
    /// assert_eq!(&*frontier.borrow(), &[2]);
    ///```
    #[inline] pub fn borrow(&self) -> AntichainRef<T> { AntichainRef::new(&self.elements) }}

impl<T: PartialEq> PartialEq for Antichain<T> {
    fn eq(&self, other: &Self) -> bool {
        // Lengths should be the same, with the option for fast acceptance if identical.
        self.elements().len() == other.elements().len() &&
        (
            self.elements().iter().zip(other.elements().iter()).all(|(t1,t2)| t1 == t2) ||
            self.elements().iter().all(|t1| other.elements().iter().any(|t2| t1.eq(t2)))
        )
    }
}

impl<T: Eq> Eq for Antichain<T> { }

impl<T: PartialOrder> PartialOrder for Antichain<T> {
    fn less_equal(&self, other: &Self) -> bool {
        other.elements().iter().all(|t2| self.elements().iter().any(|t1| t1.less_equal(t2)))
    }
}

impl<T: Clone> Clone for Antichain<T> {
    fn clone(&self) -> Self {
        Antichain { elements: self.elements.clone() }
    }
    fn clone_from(&mut self, source: &Self) {
        self.elements.clone_from(&source.elements)
    }
}

impl<T> Default for Antichain<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: TotalOrder> TotalOrder for Antichain<T> { }

impl<T: TotalOrder> Antichain<T> {
    /// Convert to the at most one element the antichain contains.
    pub fn into_option(mut self) -> Option<T> {
        debug_assert!(self.len() <= 1);
        self.elements.pop()
    }
    /// Return a reference to the at most one element the antichain contains.
    pub fn as_option(&self) -> Option<&T> {
        debug_assert!(self.len() <= 1);
        self.elements.last()
    }
}

impl<T: Ord+std::hash::Hash> std::hash::Hash for Antichain<T> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        let mut temp = self.elements.iter().collect::<Vec<_>>();
        temp.sort();
        for element in temp {
            element.hash(state);
        }
    }
}

impl<T: PartialOrder> From<Vec<T>> for Antichain<T> {
    fn from(vec: Vec<T>) -> Self {
        // TODO: We could reuse `vec` with some care.
        let mut temp = Antichain::new();
        for elem in vec.into_iter() { temp.insert(elem); }
        temp
    }
}

impl<T> Into<Vec<T>> for Antichain<T> {
    fn into(self) -> Vec<T> {
        self.elements
    }
}

impl<T> ::std::ops::Deref for Antichain<T> {
    type Target = [T];
    fn deref(&self) -> &Self::Target {
        &self.elements
    }
}

impl<T> ::std::iter::IntoIterator for Antichain<T> {
    type Item = T;
    type IntoIter = ::std::vec::IntoIter<T>;
    fn into_iter(self) -> Self::IntoIter {
        self.elements.into_iter()
    }
}

/// An antichain based on a multiset whose elements frequencies can be updated.
///
/// The `MutableAntichain` maintains frequencies for many elements of type `T`, and exposes the set
/// of elements with positive count not greater than any other elements with positive count. The
/// antichain may both advance and retreat; the changes do not all need to be to elements greater or
/// equal to some elements of the frontier.
///
/// The type `T` must implement `PartialOrder` as well as `Ord`. The implementation of the `Ord` trait
/// is used to efficiently organize the updates for cancellation, and to efficiently determine the lower
/// bounds, and only needs to not contradict the `PartialOrder` implementation (that is, if `PartialOrder`
/// orders two elements, then so does the `Ord` implementation).
///
/// The `MutableAntichain` implementation is done with the intent that updates to it are done in batches,
/// and it is acceptable to rebuild the frontier from scratch when a batch of updates change it. This means
/// that it can be expensive to maintain a large number of counts and change few elements near the frontier.
#[derive(Clone, Debug, Abomonation, Serialize, Deserialize)]
pub struct MutableAntichain<T> {
    updates: ChangeBatch<T>,
    frontier: Vec<T>,
    changes: ChangeBatch<T>,
}

impl<T> MutableAntichain<T> {
    /// Creates a new empty `MutableAntichain`.
    ///
    /// # Examples
    ///
    ///```
    /// use timely::progress::frontier::MutableAntichain;
    ///
    /// let frontier = MutableAntichain::<usize>::new();
    /// assert!(frontier.is_empty());
    ///```
    #[inline]
    pub fn new() -> MutableAntichain<T> {
        MutableAntichain {
            updates: ChangeBatch::new(),
            frontier:  Vec::new(),
            changes: ChangeBatch::new(),
        }
    }

    /// Removes all elements.
    ///
    /// # Examples
    ///
    ///```
    /// use timely::progress::frontier::MutableAntichain;
    ///
    /// let mut frontier = MutableAntichain::<usize>::new();
    /// frontier.clear();
    /// assert!(frontier.is_empty());
    ///```
    #[inline]
    pub fn clear(&mut self) {
        self.updates.clear();
        self.frontier.clear();
        self.changes.clear();
    }

    /// Reveals the minimal elements with positive count.
    ///
    /// # Examples
    ///
    ///```
    /// use timely::progress::frontier::MutableAntichain;
    ///
    /// let mut frontier = MutableAntichain::<usize>::new();
    /// assert!(frontier.frontier().len() == 0);
    ///```
    #[inline]
    pub fn frontier(&self) -> AntichainRef<'_, T> {
        AntichainRef::new(&self.frontier)
    }

    /// Creates a new singleton `MutableAntichain`.
    ///
    /// # Examples
    ///
    ///```
    /// use timely::progress::frontier::{AntichainRef, MutableAntichain};
    ///
    /// let mut frontier = MutableAntichain::new_bottom(0u64);
    /// assert!(frontier.frontier() == AntichainRef::new(&[0u64]));
    ///```
    #[inline]
    pub fn new_bottom(bottom: T) -> MutableAntichain<T> 
    where
        T: Ord+Clone,
    {
        MutableAntichain {
            updates: ChangeBatch::new_from(bottom.clone(), 1),
            frontier: vec![bottom],
            changes: ChangeBatch::new(),
        }
    }

    /// Returns true if there are no elements in the `MutableAntichain`.
    ///
    /// # Examples
    ///
    ///```
    /// use timely::progress::frontier::MutableAntichain;
    ///
    /// let mut frontier = MutableAntichain::<usize>::new();
    /// assert!(frontier.is_empty());
    ///```
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.frontier.is_empty()
    }

    /// Returns true if any item in the `MutableAntichain` is strictly less than the argument.
    ///
    /// # Examples
    ///
    ///```
    /// use timely::progress::frontier::MutableAntichain;
    ///
    /// let mut frontier = MutableAntichain::new_bottom(1u64);
    /// assert!(!frontier.less_than(&0));
    /// assert!(!frontier.less_than(&1));
    /// assert!(frontier.less_than(&2));
    ///```
    #[inline]
    pub fn less_than(&self, time: &T) -> bool
    where
        T: PartialOrder,
    {
        self.frontier().less_than(time)
    }

    /// Returns true if any item in the `MutableAntichain` is less than or equal to the argument.
    ///
    /// # Examples
    ///
    ///```
    /// use timely::progress::frontier::MutableAntichain;
    ///
    /// let mut frontier = MutableAntichain::new_bottom(1u64);
    /// assert!(!frontier.less_equal(&0));
    /// assert!(frontier.less_equal(&1));
    /// assert!(frontier.less_equal(&2));
    ///```
    #[inline]
    pub fn less_equal(&self, time: &T) -> bool
    where
        T: PartialOrder,
    {
        self.frontier().less_equal(time)
    }

    /// Applies updates to the antichain and enumerates any changes.
    ///
    /// # Examples
    ///
    ///```
    /// use timely::progress::frontier::{AntichainRef, MutableAntichain};
    ///
    /// let mut frontier = MutableAntichain::new_bottom(1u64);
    /// let changes =
    /// frontier
    ///     .update_iter(vec![(1, -1), (2, 7)])
    ///     .collect::<Vec<_>>();
    ///
    /// assert!(frontier.frontier() == AntichainRef::new(&[2]));
    /// assert!(changes == vec![(1, -1), (2, 1)]);
    ///```
    #[inline]
    pub fn update_iter<I>(&mut self, updates: I) -> ::std::vec::Drain<'_, (T, i64)>
    where
        T: Clone + PartialOrder + Ord,
        I: IntoIterator<Item = (T, i64)>,
    {
        let updates = updates.into_iter();

        // track whether a rebuild is needed.
        let mut rebuild_required = false;
        for (time, delta) in updates {

            // If we do not yet require a rebuild, test whether we might require one
            // and set the flag in that case.
            if !rebuild_required {
                let beyond_frontier = self.frontier.iter().any(|f| f.less_than(&time));
                let before_frontier = !self.frontier.iter().any(|f| f.less_equal(&time));
                rebuild_required = !(beyond_frontier || (delta < 0 && before_frontier));
            }

            self.updates.update(time, delta);
        }

        if rebuild_required {
            self.rebuild()
        }
        self.changes.drain()
    }

    /// Rebuilds `self.frontier` from `self.updates`.
    ///
    /// This method is meant to be used for bulk updates to the frontier, and does more work than one might do
    /// for single updates, but is meant to be an efficient way to process multiple updates together. This is
    /// especially true when we want to apply very large numbers of updates.
    fn rebuild(&mut self)
    where
        T: Clone + PartialOrder + Ord,
    {
        for time in self.frontier.drain(..) {
            self.changes.update(time, -1);
        }

        // build new frontier using strictly positive times.
        // as the times are sorted, we don't need to worry that we might displace frontier elements.
        for time in self.updates.iter().filter(|x| x.1 > 0) {
            if !self.frontier.iter().any(|f| f.less_equal(&time.0)) {
                self.frontier.push(time.0.clone());
            }
        }

        for time in self.frontier.iter() {
            self.changes.update(time.clone(), 1);
        }
    }

    /// Reports the count for a queried time.
    pub fn count_for(&self, query_time: &T) -> i64
    where
        T: Ord,
    {
        self.updates
            .unstable_internal_updates()
            .iter()
            .filter(|td| td.0.eq(query_time))
            .map(|td| td.1)
            .sum()
    }

    /// Reports the updates that form the frontier. Returns an iterator of timestamps and their frequency.
    ///
    /// Rebuilds the internal representation before revealing times and frequencies.
    pub fn updates(&mut self) -> impl Iterator<Item=&(T, i64)>
    where
        T: Clone + PartialOrder + Ord,
    {
        self.rebuild();
        self.updates.iter()
    }
}

impl<T> Default for MutableAntichain<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// Extension trait for filtering time changes through antichains.
pub trait MutableAntichainFilter<T: PartialOrder+Ord+Clone> {
    /// Filters time changes through an antichain.
    ///
    /// # Examples
    ///
    /// ```
    /// use timely::progress::frontier::{MutableAntichain, MutableAntichainFilter};
    ///
    /// let mut frontier = MutableAntichain::new_bottom(1u64);
    /// let changes =
    /// vec![(1, -1), (2, 7)]
    ///     .filter_through(&mut frontier)
    ///     .collect::<Vec<_>>();
    ///
    /// assert!(changes == vec![(1, -1), (2, 1)]);
    /// ```
    fn filter_through(self, antichain: &mut MutableAntichain<T>) -> ::std::vec::Drain<(T,i64)>;
}

impl<T: PartialOrder+Ord+Clone, I: IntoIterator<Item=(T,i64)>> MutableAntichainFilter<T> for I {
    fn filter_through(self, antichain: &mut MutableAntichain<T>) -> ::std::vec::Drain<(T,i64)> {
        antichain.update_iter(self.into_iter())
    }
}

impl<T: PartialOrder+Ord+Clone> From<Antichain<T>> for MutableAntichain<T> {
    fn from(antichain: Antichain<T>) -> Self {
        let mut result = MutableAntichain::new();
        result.update_iter(antichain.into_iter().map(|time| (time, 1)));
        result
    }
}
impl<'a, T: PartialOrder+Ord+Clone> From<AntichainRef<'a, T>> for MutableAntichain<T> {
    fn from(antichain: AntichainRef<'a, T>) -> Self {
        let mut result = MutableAntichain::new();
        result.update_iter(antichain.into_iter().map(|time| (time.clone(), 1)));
        result
    }
}

impl<T> std::iter::FromIterator<(T, i64)> for MutableAntichain<T>
where
    T: Clone + PartialOrder + Ord,
{
    fn from_iter<I>(iterator: I) -> Self
    where
        I: IntoIterator<Item=(T, i64)>,
    {
        let mut result = Self::new();
        result.update_iter(iterator);
        result
    }
}

/// A wrapper for elements of an antichain.
#[derive(Debug)]
pub struct AntichainRef<'a, T: 'a> {
    /// Elements contained in the antichain.
    frontier: &'a [T],
}

impl<'a, T: 'a> Clone for AntichainRef<'a, T> {
    fn clone(&self) -> Self {
        Self {
            frontier: self.frontier,
        }
    }
}

impl<'a, T: 'a> Copy for AntichainRef<'a, T> { }

impl<'a, T: 'a> AntichainRef<'a, T> {
    /// Create a new `AntichainRef` from a reference to a slice of elements forming the frontier.
    ///
    /// This method does not check that this antichain has any particular properties, for example
    /// that there are no elements strictly less than other elements.
    pub fn new(frontier: &'a [T]) -> Self {
        Self {
            frontier,
        }
    }

    /// Constructs an owned antichain from the antichain reference.
    ///
    /// # Examples
    ///
    ///```
    /// use timely::progress::{Antichain, frontier::AntichainRef};
    ///
    /// let frontier = AntichainRef::new(&[1u64]);
    /// assert_eq!(frontier.to_owned(), Antichain::from_elem(1u64));
    ///```
    pub fn to_owned(&self) -> Antichain<T> where T: Clone {
        Antichain {
            elements: self.frontier.to_vec()
        }
    }
}

impl<'a, T: 'a+PartialOrder> AntichainRef<'a, T> {

    /// Returns true if any item in the `AntichainRef` is strictly less than the argument.
    ///
    /// # Examples
    ///
    ///```
    /// use timely::progress::frontier::AntichainRef;
    ///
    /// let frontier = AntichainRef::new(&[1u64]);
    /// assert!(!frontier.less_than(&0));
    /// assert!(!frontier.less_than(&1));
    /// assert!(frontier.less_than(&2));
    ///```
    #[inline]
    pub fn less_than(&self, time: &T) -> bool {
        self.iter().any(|x| x.less_than(time))
    }

    /// Returns true if any item in the `AntichainRef` is less than or equal to the argument.
    #[inline]
    ///
    /// # Examples
    ///
    ///```
    /// use timely::progress::frontier::AntichainRef;
    ///
    /// let frontier = AntichainRef::new(&[1u64]);
    /// assert!(!frontier.less_equal(&0));
    /// assert!(frontier.less_equal(&1));
    /// assert!(frontier.less_equal(&2));
    ///```
    pub fn less_equal(&self, time: &T) -> bool {
        self.iter().any(|x| x.less_equal(time))
    }
}

impl<'a, T: PartialEq> PartialEq for AntichainRef<'a, T> {
    fn eq(&self, other: &Self) -> bool {
        // Lengths should be the same, with the option for fast acceptance if identical.
        self.len() == other.len() &&
        (
            self.iter().zip(other.iter()).all(|(t1,t2)| t1 == t2) ||
            self.iter().all(|t1| other.iter().any(|t2| t1.eq(t2)))
        )
    }
}

impl<'a, T: Eq> Eq for AntichainRef<'a, T> { }

impl<'a, T: PartialOrder> PartialOrder for AntichainRef<'a, T> {
    fn less_equal(&self, other: &Self) -> bool {
        other.iter().all(|t2| self.iter().any(|t1| t1.less_equal(t2)))
    }
}

impl<'a, T: TotalOrder> TotalOrder for AntichainRef<'a, T> { }

impl<'a, T: TotalOrder> AntichainRef<'a, T> {
    /// Return a reference to the at most one element the antichain contains.
    pub fn as_option(&self) -> Option<&T> {
        debug_assert!(self.len() <= 1);
        self.frontier.last()
    }
}

impl<'a, T> ::std::ops::Deref for AntichainRef<'a, T> {
    type Target = [T];
    fn deref(&self) -> &Self::Target {
        self.frontier
    }
}

impl<'a, T: 'a> ::std::iter::IntoIterator for &'a AntichainRef<'a, T> {
    type Item = &'a T;
    type IntoIter = ::std::slice::Iter<'a, T>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    #[derive(PartialEq, Eq, PartialOrd, Ord, Hash)]
    struct Elem(char, usize);

    impl PartialOrder for Elem {
        fn less_equal(&self, other: &Self) -> bool {
            self.0 <= other.0 && self.1 <= other.1
        }
    }

    #[test]
    fn antichain_hash() {
        let mut hashed = HashSet::new();
        hashed.insert(Antichain::from(vec![Elem('a', 2), Elem('b', 1)]));

        assert!(hashed.contains(&Antichain::from(vec![Elem('a', 2), Elem('b', 1)])));
        assert!(hashed.contains(&Antichain::from(vec![Elem('b', 1), Elem('a', 2)])));

        assert!(!hashed.contains(&Antichain::from(vec![Elem('a', 2)])));
        assert!(!hashed.contains(&Antichain::from(vec![Elem('a', 1)])));
        assert!(!hashed.contains(&Antichain::from(vec![Elem('b', 2)])));
        assert!(!hashed.contains(&Antichain::from(vec![Elem('a', 1), Elem('b', 2)])));
        assert!(!hashed.contains(&Antichain::from(vec![Elem('c', 3)])));
        assert!(!hashed.contains(&Antichain::from(vec![])));
    }

    #[test]
    fn mutable_compaction() {
        let mut mutable = MutableAntichain::new();
        mutable.update_iter(Some((7, 1)));
        mutable.update_iter(Some((7, 1)));
        mutable.update_iter(Some((7, 1)));
        mutable.update_iter(Some((7, 1)));
        mutable.update_iter(Some((7, 1)));
        mutable.update_iter(Some((7, 1)));
        mutable.update_iter(Some((8, 1)));
        mutable.update_iter(Some((8, 1)));
        mutable.update_iter(Some((8, 1)));
        mutable.update_iter(Some((8, 1)));
        mutable.update_iter(Some((8, 1)));
        for _ in 0 .. 1000 {
            mutable.update_iter(Some((9, 1)));
            mutable.update_iter(Some((9, -1)));
        }
        assert!(mutable.updates.unstable_internal_updates().len() <= 32);
    }
}
