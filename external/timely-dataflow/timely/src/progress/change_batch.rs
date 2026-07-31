//! A collection of updates of the form `(T, i64)`.

/// A collection of updates of the form `(T, i64)`.
///
/// A `ChangeBatch` accumulates updates of the form `(T, i64)`, where it is capable of consolidating
/// the representation and removing elements whose `i64` field accumulates to zero.
///
/// The implementation is designed to be as lazy as possible, simply appending to a list of updates
/// until they are required. This means that several seemingly simple operations may be expensive, in
/// that they may provoke a compaction. I've tried to prevent exposing methods that allow surprisingly
/// expensive operations; all operations should take an amortized constant or logarithmic time.
#[derive(Clone, Debug, Eq, PartialEq, Abomonation, Serialize, Deserialize)]
pub struct ChangeBatch<T> {
    // A list of updates to which we append.
    updates: Vec<(T, i64)>,
    // The length of the prefix of `self.updates` known to be compact.
    clean: usize,
}

impl<T> ChangeBatch<T> {
    /// Allocates a new empty `ChangeBatch`.
    ///
    /// # Examples
    ///
    ///```
    /// use timely::progress::ChangeBatch;
    ///
    /// let mut batch = ChangeBatch::<usize>::new();
    /// assert!(batch.is_empty());
    ///```
    pub fn new() -> ChangeBatch<T> {
        ChangeBatch {
            updates: Vec::new(),
            clean: 0,
        }
    }

    /// Allocates a new empty `ChangeBatch` with space for `capacity` updates.
    ///
    /// # Examples
    ///
    ///```
    /// use timely::progress::ChangeBatch;
    ///
    /// let mut batch = ChangeBatch::<usize>::with_capacity(10);
    /// assert!(batch.is_empty());
    ///```
    pub fn with_capacity(capacity: usize) -> ChangeBatch<T> {
        ChangeBatch {
            updates: Vec::with_capacity(capacity),
            clean: 0,
        }
    }

    /// Returns true if the change batch is not guaranteed compact.
    pub fn is_dirty(&self) -> bool {
        self.updates.len() > self.clean
    }

    /// Expose the internal vector of updates.
    pub fn unstable_internal_updates(&self) -> &Vec<(T, i64)> {
        &self.updates
    }

    /// Expose the internal value of `clean`.
    pub fn unstable_internal_clean(&self) -> usize {
        self.clean
    }

    /// Clears the map.
    ///
    /// # Examples
    ///
    ///```
    /// use timely::progress::ChangeBatch;
    ///
    /// let mut batch = ChangeBatch::<usize>::new_from(17, 1);
    /// batch.clear();
    /// assert!(batch.is_empty());
    ///```
    #[inline]
    pub fn clear(&mut self) {
        self.updates.clear();
        self.clean = 0;
    }
}

impl<T> ChangeBatch<T>
where
    T: Ord,
{
    /// Allocates a new `ChangeBatch` with a single entry.
    ///
    /// # Examples
    ///
    ///```
    /// use timely::progress::ChangeBatch;
    ///
    /// let mut batch = ChangeBatch::<usize>::new_from(17, 1);
    /// assert!(!batch.is_empty());
    ///```
    pub fn new_from(key: T, val: i64) -> ChangeBatch<T> {
        let mut result = ChangeBatch::new();
        result.update(key, val);
        result
    }

    /// Adds a new update, for `item` with `value`.
    ///
    /// This could be optimized to perform compaction when the number of "dirty" elements exceeds
    /// half the length of the list, which would keep the total footprint within reasonable bounds
    /// even under an arbitrary number of updates. This has a cost, and it isn't clear whether it
    /// is worth paying without some experimentation.
    ///
    /// # Examples
    ///
    ///```
    /// use timely::progress::ChangeBatch;
    ///
    /// let mut batch = ChangeBatch::<usize>::new();
    /// batch.update(17, 1);
    /// assert!(!batch.is_empty());
    ///```
    #[inline]
    pub fn update(&mut self, item: T, value: i64) {
        self.updates.push((item, value));
        self.maintain_bounds();
    }

    /// Performs a sequence of updates described by `iterator`.
    ///
    /// # Examples
    ///
    ///```
    /// use timely::progress::ChangeBatch;
    ///
    /// let mut batch = ChangeBatch::<usize>::new_from(17, 1);
    /// batch.extend(vec![(17, -1)].into_iter());
    /// assert!(batch.is_empty());
    ///```
    #[inline]
    pub fn extend<I: Iterator<Item = (T, i64)>>(&mut self, iterator: I) {
        self.updates.extend(iterator);
        self.maintain_bounds();
    }

    /// Extracts the `Vec<(T, i64)>` from the map, consuming it.
    ///
    /// # Examples
    ///
    ///```
    /// use timely::progress::ChangeBatch;
    ///
    /// let batch = ChangeBatch::<usize>::new_from(17, 1);
    /// assert_eq!(batch.into_inner(), vec![(17, 1)]);
    ///```
    pub fn into_inner(mut self) -> Vec<(T, i64)> {
        self.compact();
        self.updates
    }

    /// Iterates over the contents of the map.
    ///
    /// # Examples
    ///
    ///```
    /// use timely::progress::ChangeBatch;
    ///
    /// let mut batch = ChangeBatch::<usize>::new_from(17, 1);
    /// {   // scope allows borrow of `batch` to drop.
    ///     let mut iter = batch.iter();
    ///     assert_eq!(iter.next(), Some(&(17, 1)));
    ///     assert_eq!(iter.next(), None);
    /// }
    /// assert!(!batch.is_empty());
    ///```
    #[inline]
    pub fn iter(&mut self) -> ::std::slice::Iter<(T, i64)> {
        self.compact();
        self.updates.iter()
    }

    /// Drains the set of updates.
    ///
    /// This operation first compacts the set of updates so that the drained results
    /// have at most one occurence of each item.
    ///
    /// # Examples
    ///
    ///```
    /// use timely::progress::ChangeBatch;
    ///
    /// let mut batch = ChangeBatch::<usize>::new_from(17, 1);
    /// {   // scope allows borrow of `batch` to drop.
    ///     let mut iter = batch.drain();
    ///     assert_eq!(iter.next(), Some((17, 1)));
    ///     assert_eq!(iter.next(), None);
    /// }
    /// assert!(batch.is_empty());
    ///```
    #[inline]
    pub fn drain(&mut self) -> ::std::vec::Drain<(T, i64)> {
        self.compact();
        self.clean = 0;
        self.updates.drain(..)
    }

    /// True iff all keys have value zero.
    ///
    /// This method requires mutable access to `self` because it may need to compact the representation
    /// to determine if the batch of updates is indeed empty. We could also implement a weaker form of
    /// `is_empty` which just checks the length of `self.updates`, and which could confirm the absence of
    /// any updates, but could report false negatives if there are updates which would cancel.
    ///
    /// # Examples
    ///
    ///```
    /// use timely::progress::ChangeBatch;
    ///
    /// let mut batch = ChangeBatch::<usize>::new_from(17, 1);
    /// batch.update(17, -1);
    /// assert!(batch.is_empty());
    ///```
    #[inline]
    pub fn is_empty(&mut self) -> bool {
        if self.clean > self.updates.len() / 2 {
            false
        } else {
            self.compact();
            self.updates.is_empty()
        }
    }

    /// Number of compacted updates.
    ///
    /// This method requires mutable access to `self` because it may need to compact the
    /// representation to determine the number of actual updates.
    ///
    /// # Examples
    ///
    ///```
    /// use timely::progress::ChangeBatch;
    ///
    /// let mut batch = ChangeBatch::<usize>::new_from(17, 1);
    /// batch.update(17, -1);
    /// batch.update(14, -1);
    /// assert_eq!(batch.len(), 1);
    ///```
    #[inline]
    pub fn len(&mut self) -> usize {
        self.compact();
        self.updates.len()
    }

    /// Drains `self` into `other`.
    ///
    /// This method has similar a effect to calling `other.extend(self.drain())`, but has the
    /// opportunity to optimize this to a `::std::mem::swap(self, other)` when `other` is empty.
    /// As many uses of this method are to propagate updates, this optimization can be quite
    /// handy.
    ///
    /// # Examples
    ///
    ///```
    /// use timely::progress::ChangeBatch;
    ///
    /// let mut batch1 = ChangeBatch::<usize>::new_from(17, 1);
    /// let mut batch2 = ChangeBatch::new();
    /// batch1.drain_into(&mut batch2);
    /// assert!(batch1.is_empty());
    /// assert!(!batch2.is_empty());
    ///```
    #[inline]
    pub fn drain_into(&mut self, other: &mut ChangeBatch<T>)
    where
        T: Clone,
    {
        if other.updates.is_empty() {
            ::std::mem::swap(self, other);
        } else {
            other.extend(self.updates.drain(..));
            self.clean = 0;
        }
    }

    /// Compact the internal representation.
    ///
    /// This method sort `self.updates` and consolidates elements with equal item, discarding
    /// any whose accumulation is zero. It is optimized to only do this if the number of dirty
    /// elements is non-zero.
    #[inline]
    pub fn compact(&mut self) {
        if self.clean < self.updates.len() && self.updates.len() > 1 {
            self.updates.sort_by(|x, y| x.0.cmp(&y.0));
            for i in 0..self.updates.len() - 1 {
                if self.updates[i].0 == self.updates[i + 1].0 {
                    self.updates[i + 1].1 += self.updates[i].1;
                    self.updates[i].1 = 0;
                }
            }

            self.updates.retain(|x| x.1 != 0);
        }
        self.clean = self.updates.len();
    }

    /// Maintain the bounds of pending (non-compacted) updates versus clean (compacted) data.
    /// This function tries to minimize work by only compacting if enough work has accumulated.
    fn maintain_bounds(&mut self) {
        // if we have more than 32 elements and at least half of them are not clean, compact
        if self.updates.len() > 32 && self.updates.len() >> 1 >= self.clean {
            self.compact()
        }
    }
}

impl<T> Default for ChangeBatch<T> {
    fn default() -> Self {
        Self::new()
    }
}
