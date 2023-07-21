//! Implementation using ordered keys and exponential search.

use super::{Trie, Cursor, Builder, MergeBuilder, TupleBuilder, BatchContainer, advance};
use std::convert::{TryFrom, TryInto};
use std::fmt::Debug;
use std::ops::{Sub,Add,Deref};

/// Trait for types used as offsets into an ordered layer.
/// This is usually `usize`, but `u32` can also be used in applications
/// where huge batches do not occur to reduce metadata size.
pub trait OrdOffset: Copy + PartialEq + Add<Output=Self> + Sub<Output=Self> + TryFrom<usize> + TryInto<usize>
{}

impl<O> OrdOffset for O
where
    O: Copy + PartialEq + Add<Output=Self> + Sub<Output=Self> + TryFrom<usize> + TryInto<usize>,
{}

/// A level of the trie, with keys and offsets into a lower layer.
///
/// In this representation, the values for `keys[i]` are found at `vals[offs[i] .. offs[i+1]]`.
#[derive(Debug, Eq, PartialEq, Clone, Abomonation)]
pub struct OrderedLayer<K, L, O=usize, C=Vec<K>>
where
    K: Ord,
    C: BatchContainer<Item=K>+Deref<Target=[K]>,
    O: OrdOffset, <O as TryFrom<usize>>::Error: Debug, <O as TryInto<usize>>::Error: Debug
{
    /// The keys of the layer.
    pub keys: C,
    /// The offsets associate with each key.
    ///
    /// The bounds for `keys[i]` are `(offs[i], offs[i+1]`). The offset array is guaranteed to be one
    /// element longer than the keys array, ensuring that these accesses do not panic.
    pub offs: Vec<O>,
    /// The ranges of values associated with the keys.
    pub vals: L,
}

impl<K, L, O, C> Trie for OrderedLayer<K, L, O, C>
where
    K: Ord+Clone,
    C: BatchContainer<Item=K>+Deref<Target=[K]>,
    L: Trie,
    O: OrdOffset, <O as TryFrom<usize>>::Error: Debug, <O as TryInto<usize>>::Error: Debug
{
    type Item = (K, L::Item);
    type Cursor = OrderedCursor<L>;
    type MergeBuilder = OrderedBuilder<K, L::MergeBuilder, O, C>;
    type TupleBuilder = OrderedBuilder<K, L::TupleBuilder, O, C>;

    fn keys(&self) -> usize { self.keys.len() }
    fn tuples(&self) -> usize { self.vals.tuples() }
    fn cursor_from(&self, lower: usize, upper: usize) -> Self::Cursor {

        if lower < upper {

            let child_lower = self.offs[lower];
            let child_upper = self.offs[lower + 1];
            OrderedCursor {
                bounds: (lower, upper),
                child: self.vals.cursor_from(child_lower.try_into().unwrap(), child_upper.try_into().unwrap()),
                pos: lower,
                pos_nonnegative: true,
            }
        }
        else {
            OrderedCursor {
                bounds: (0, 0),
                child: self.vals.cursor_from(0, 0),
                pos: 0,
                pos_nonnegative: true,
            }
        }
    }
}

/// Assembles a layer of this
pub struct OrderedBuilder<K, L, O=usize, C=Vec<K>>
where
    K: Ord+Clone,
    C: BatchContainer<Item=K>+Deref<Target=[K]>,
    O: OrdOffset, <O as TryFrom<usize>>::Error: Debug, <O as TryInto<usize>>::Error: Debug
{
    /// Keys
    pub keys: C,
    /// Offsets
    pub offs: Vec<O>,
    /// The next layer down
    pub vals: L,
}

impl<K, L, O, C> Builder for OrderedBuilder<K, L, O, C>
where
    K: Ord+Clone,
    C: BatchContainer<Item=K>+Deref<Target=[K]>,
    L: Builder,
    O: OrdOffset, <O as TryFrom<usize>>::Error: Debug, <O as TryInto<usize>>::Error: Debug
{
    type Trie = OrderedLayer<K, L::Trie, O, C>;
    fn boundary(&mut self) -> usize {
        self.offs[self.keys.len()] = O::try_from(self.vals.boundary()).unwrap();
        self.keys.len()
    }
    fn done(mut self) -> Self::Trie {
        if self.keys.len() > 0 && self.offs[self.keys.len()].try_into().unwrap() == 0 {
            self.offs[self.keys.len()] = O::try_from(self.vals.boundary()).unwrap();
        }
        OrderedLayer {
            keys: self.keys,
            offs: self.offs,
            vals: self.vals.done(),
        }
    }
}

impl<K, L, O, C> MergeBuilder for OrderedBuilder<K, L, O, C>
where
    K: Ord+Clone,
    C: BatchContainer<Item=K>+Deref<Target=[K]>,
    L: MergeBuilder,
    O: OrdOffset, <O as TryFrom<usize>>::Error: Debug, <O as TryInto<usize>>::Error: Debug
{
    fn with_capacity(other1: &Self::Trie, other2: &Self::Trie) -> Self {
        let mut offs = Vec::with_capacity(other1.keys() + other2.keys() + 1);
        offs.push(O::try_from(0 as usize).unwrap());
        OrderedBuilder {
            keys: C::merge_capacity(&other1.keys, &other2.keys),
            offs: offs,
            vals: L::with_capacity(&other1.vals, &other2.vals),
        }
    }
    #[inline]
    fn copy_range(&mut self, other: &Self::Trie, lower: usize, upper: usize) {
        debug_assert!(lower < upper);
        let other_basis = other.offs[lower];
        let self_basis = self.offs.last().map(|&x| x).unwrap_or(O::try_from(0).unwrap());

        self.keys.copy_slice(&other.keys[lower .. upper]);
        for index in lower .. upper {
            self.offs.push((other.offs[index + 1] + self_basis) - other_basis);
        }
        self.vals.copy_range(&other.vals, other_basis.try_into().unwrap(), other.offs[upper].try_into().unwrap());
    }

    fn push_merge(&mut self, other1: (&Self::Trie, usize, usize), other2: (&Self::Trie, usize, usize)) -> usize {
        let (trie1, mut lower1, upper1) = other1;
        let (trie2, mut lower2, upper2) = other2;

        self.keys.reserve((upper1 - lower1) + (upper2 - lower2));

        // while both mergees are still active
        while lower1 < upper1 && lower2 < upper2 {
            self.merge_step((trie1, &mut lower1, upper1), (trie2, &mut lower2, upper2));
        }

        if lower1 < upper1 { self.copy_range(trie1, lower1, upper1); }
        if lower2 < upper2 { self.copy_range(trie2, lower2, upper2); }

        self.keys.len()
    }
}

impl<K, L, O, C> OrderedBuilder<K, L, O, C>
where
    K: Ord+Clone,
    C: BatchContainer<Item=K>+Deref<Target=[K]>,
    L: MergeBuilder,
    O: OrdOffset, <O as TryFrom<usize>>::Error: Debug, <O as TryInto<usize>>::Error: Debug
{
    /// Performs one step of merging.
    #[inline]
    pub fn merge_step(&mut self, other1: (&<Self as Builder>::Trie, &mut usize, usize), other2: (&<Self as Builder>::Trie, &mut usize, usize)) {

        let (trie1, lower1, upper1) = other1;
        let (trie2, lower2, upper2) = other2;

        match trie1.keys[*lower1].cmp(&trie2.keys[*lower2]) {
            ::std::cmp::Ordering::Less => {
                // determine how far we can advance lower1 until we reach/pass lower2
                let step = 1 + advance(&trie1.keys[(1 + *lower1)..upper1], |x| x < &trie2.keys[*lower2]);
                let step = std::cmp::min(step, 1_000);
                self.copy_range(trie1, *lower1, *lower1 + step);
                *lower1 += step;
            },
            ::std::cmp::Ordering::Equal => {
                let lower = self.vals.boundary();
                // record vals_length so we can tell if anything was pushed.
                let upper = self.vals.push_merge(
                    (&trie1.vals, trie1.offs[*lower1].try_into().unwrap(), trie1.offs[*lower1 + 1].try_into().unwrap()),
                    (&trie2.vals, trie2.offs[*lower2].try_into().unwrap(), trie2.offs[*lower2 + 1].try_into().unwrap())
                );
                if upper > lower {
                    self.keys.copy(&trie1.keys[*lower1]);
                    self.offs.push(O::try_from(upper).unwrap());
                }

                *lower1 += 1;
                *lower2 += 1;
            },
            ::std::cmp::Ordering::Greater => {
                // determine how far we can advance lower2 until we reach/pass lower1
                let step = 1 + advance(&trie2.keys[(1 + *lower2)..upper2], |x| x < &trie1.keys[*lower1]);
                let step = std::cmp::min(step, 1_000);
                self.copy_range(trie2, *lower2, *lower2 + step);
                *lower2 += step;
            },
        }
    }
}

impl<K, L, O, C> TupleBuilder for OrderedBuilder<K, L, O, C>
where
    K: Ord+Clone,
    C: BatchContainer<Item=K>+Deref<Target=[K]>,
    L: TupleBuilder,
    O: OrdOffset, <O as TryFrom<usize>>::Error: Debug, <O as TryInto<usize>>::Error: Debug
{
    type Item = (K, L::Item);
    fn new() -> Self { OrderedBuilder { keys: C::default(), offs: vec![O::try_from(0).unwrap()], vals: L::new() } }
    fn with_capacity(cap: usize) -> Self {
        let mut offs = Vec::with_capacity(cap + 1);
        offs.push(O::try_from(0).unwrap());
        OrderedBuilder{
            keys: C::with_capacity(cap),
            offs: offs,
            vals: L::with_capacity(cap),
        }
    }
    #[inline]
    fn push_tuple(&mut self, (key, val): (K, L::Item)) {

        // if first element, prior element finish, or different element, need to push and maybe punctuate.
        if self.keys.len() == 0 || self.offs[self.keys.len()].try_into().unwrap() != 0 || self.keys[self.keys.len()-1] != key {
            if self.keys.len() > 0 && self.offs[self.keys.len()].try_into().unwrap() == 0 {
                self.offs[self.keys.len()] = O::try_from(self.vals.boundary()).unwrap();
            }
            self.keys.push(key);
            self.offs.push(O::try_from(0).unwrap());        // <-- indicates "unfinished".
        }
        self.vals.push_tuple(val);
    }
}

/// A cursor with a child cursor that is updated as we move.
#[derive(Debug)]
pub struct OrderedCursor<L: Trie> {
    pos: usize,
    bounds: (usize, usize),
    pos_nonnegative: bool, //[Pathway extension]: needed for a cursor that can do a step back 
    /// The cursor for the trie layer below this one.
    pub child: L::Cursor,
}

impl<K, L, O, C> Cursor<OrderedLayer<K, L, O, C>> for OrderedCursor<L>
where
    K: Ord+Clone,
    C: BatchContainer<Item=K>+Deref<Target=[K]>,
    L: Trie,
    O: OrdOffset, <O as TryFrom<usize>>::Error: Debug, <O as TryInto<usize>>::Error: Debug
{
    type Key = K;
    fn key<'a>(&self, storage: &'a OrderedLayer<K, L, O, C>) -> &'a Self::Key { &storage.keys[self.pos] }
    fn step(&mut self, storage: &OrderedLayer<K, L, O, C>) {
        //[Pathway extension]: pos_nonnegative indicates that index was moved to 
        //[Pathway extension]: negative value, here we undo it
        if self.pos_nonnegative { 
            self.pos += 1; 
        } else {
            self.pos_nonnegative = true;
        }
        if self.valid(storage) {
            self.child.reposition(&storage.vals, storage.offs[self.pos].try_into().unwrap(), storage.offs[self.pos + 1].try_into().unwrap());
        }
        if self.pos > self.bounds.1 {
            self.pos = self.bounds.1;
        }
    }
    fn seek(&mut self, storage: &OrderedLayer<K, L, O, C>, key: &Self::Key) {
        self.pos += advance(&storage.keys[self.pos .. self.bounds.1], |k| k.lt(key));
        if self.valid(storage) {
            self.child.reposition(&storage.vals, storage.offs[self.pos].try_into().unwrap(), storage.offs[self.pos + 1].try_into().unwrap());
        }
    }
    // fn size(&self) -> usize { self.bounds.1 - self.bounds.0 }
    //[Pathway extension]: valid considers also violation caused by stepping back
    fn valid(&self, _storage: &OrderedLayer<K, L, O, C>) -> bool { 
        self.pos >= self.bounds.0 && self.pos < self.bounds.1 && self.pos_nonnegative
    }
    fn rewind(&mut self, storage: &OrderedLayer<K, L, O, C>) {
        self.pos = self.bounds.0;
        if self.valid(storage) {
            self.child.reposition(&storage.vals, storage.offs[self.pos].try_into().unwrap(), storage.offs[self.pos + 1].try_into().unwrap());
        }
        self.pos_nonnegative = true; //[Pathway extension]: rewind makes cursor position nonnegative
    }
    fn reposition(&mut self, storage: &OrderedLayer<K, L, O, C>, lower: usize, upper: usize) {
        self.pos = lower;
        self.bounds = (lower, upper);
        if self.valid(storage) {
            self.child.reposition(&storage.vals, storage.offs[self.pos].try_into().unwrap(), storage.offs[self.pos + 1].try_into().unwrap());
        }
        self.pos_nonnegative = true; //[Pathway extension]: reposition makes cursor position nonnegative
    }

    //[Pathway extension]: needed for Bidirectional Cursor implementation 
    fn step_back(&mut self, storage: &OrderedLayer<K, L, O, C>) {
        if self.pos == 0 {
            self.pos_nonnegative = false;
        } else {
            self.pos -= 1;
            if self.valid(storage) {
                self.child.reposition(&storage.vals, storage.offs[self.pos].try_into().unwrap(), storage.offs[self.pos+1].try_into().unwrap());
            } else {
                if self.pos > self.bounds.1 { self.pos = self.bounds.1; }
                if self.pos < self.bounds.0 { self.pos = self.bounds.0; }
            }
        }
    }

    //[Pathway extension]: needed for Bidirectional Cursor implementation
    //finds last element that is smaller-equal than the key
    fn seek_smaller_eq(&mut self, storage: &OrderedLayer<K, L, O, C>, key: &Self::Key) {          
        // adv is the offset s.t. self.pos+adv points at first larger value
        // as such, self.pos + adv-1 points either fon last occurence of key
        // or largest value smaller than key
        let adv = advance(&storage.keys[self.pos .. self.bounds.1], |k| k.le(key));
        if self.pos + adv > 0 {
            self.pos += adv-1;
        } else {
            self.pos = 0;
            self.pos_nonnegative = false;
        }
        
        if self.valid(storage) {
            self.child.reposition(&storage.vals, storage.offs[self.pos].try_into().unwrap(), storage.offs[self.pos + 1].try_into().unwrap());
        }
    }
}
