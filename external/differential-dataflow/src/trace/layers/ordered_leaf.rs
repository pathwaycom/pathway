//! Implementation using ordered keys and exponential search.

use ::difference::Semigroup;

use super::{Trie, Cursor, Builder, MergeBuilder, TupleBuilder, BatchContainer, advance};
use std::ops::Deref;

/// A layer of unordered values.
#[derive(Debug, Eq, PartialEq, Clone, Abomonation)]
pub struct OrderedLeaf<K, R, C=Vec<(K,R)>>
where
    C: BatchContainer<Item=(K, R)>+Deref<Target=[(K, R)]>,
{
    /// Unordered values.
    pub vals: C,
}

impl<K: Ord+Clone, R: Semigroup+Clone, C> Trie for OrderedLeaf<K, R, C>
where
    C: BatchContainer<Item=(K, R)>+Deref<Target=[(K, R)]>,
{
    type Item = (K, R);
    type Cursor = OrderedLeafCursor;
    type MergeBuilder = OrderedLeafBuilder<K, R, C>;
    type TupleBuilder = OrderedLeafBuilder<K, R, C>;
    fn keys(&self) -> usize { self.vals.len() }
    fn tuples(&self) -> usize { <OrderedLeaf<K, R, C> as Trie>::keys(&self) }
    fn cursor_from(&self, lower: usize, upper: usize) -> Self::Cursor {
        OrderedLeafCursor {
            bounds: (lower, upper),
            pos: lower,
            pos_nonnegative: true,
        }
    }
}

/// A builder for unordered values.
pub struct OrderedLeafBuilder<K, R, C=Vec<(K,R)>>
where
    C: BatchContainer<Item=(K, R)>+Deref<Target=[(K, R)]>,
{
    /// Unordered values.
    pub vals: C,
}

impl<K: Ord+Clone, R: Semigroup+Clone, C> Builder for OrderedLeafBuilder<K, R, C>
where
    C: BatchContainer<Item=(K, R)>+Deref<Target=[(K, R)]>,
{
    type Trie = OrderedLeaf<K, R, C>;
    fn boundary(&mut self) -> usize { self.vals.len() }
    fn done(self) -> Self::Trie { OrderedLeaf { vals: self.vals } }
}

impl<K: Ord+Clone, R: Semigroup+Clone, C> MergeBuilder for OrderedLeafBuilder<K, R, C>
where
    C: BatchContainer<Item=(K, R)>+Deref<Target=[(K, R)]>,
{
    fn with_capacity(other1: &Self::Trie, other2: &Self::Trie) -> Self {
        OrderedLeafBuilder {
            vals: C::merge_capacity(&other1.vals, &other2.vals),
        }
    }
    #[inline]
    fn copy_range(&mut self, other: &Self::Trie, lower: usize, upper: usize) {
        self.vals.copy_slice(&other.vals[lower .. upper]);
    }
    fn push_merge(&mut self, other1: (&Self::Trie, usize, usize), other2: (&Self::Trie, usize, usize)) -> usize {

        let (trie1, mut lower1, upper1) = other1;
        let (trie2, mut lower2, upper2) = other2;

        self.vals.reserve((upper1 - lower1) + (upper2 - lower2));

        // while both mergees are still active
        while lower1 < upper1 && lower2 < upper2 {

            match trie1.vals[lower1].0.cmp(&trie2.vals[lower2].0) {
                ::std::cmp::Ordering::Less => {
                    // determine how far we can advance lower1 until we reach/pass lower2
                    let step = 1 + advance(&trie1.vals[(1+lower1)..upper1], |x| x.0 < trie2.vals[lower2].0);
                    let step = std::cmp::min(step, 1000);
                    <OrderedLeafBuilder<K, R, C> as MergeBuilder>::copy_range(self, trie1, lower1, lower1 + step);
                    lower1 += step;
                }
                ::std::cmp::Ordering::Equal => {

                    let mut sum = trie1.vals[lower1].1.clone();
                    sum.plus_equals(&trie2.vals[lower2].1);
                    if !sum.is_zero() {
                        self.vals.push((trie1.vals[lower1].0.clone(), sum));
                    }

                    lower1 += 1;
                    lower2 += 1;
                }
                ::std::cmp::Ordering::Greater => {
                    // determine how far we can advance lower2 until we reach/pass lower1
                    let step = 1 + advance(&trie2.vals[(1+lower2)..upper2], |x| x.0 < trie1.vals[lower1].0);
                    let step = std::cmp::min(step, 1000);
                    <OrderedLeafBuilder<K, R, C> as MergeBuilder>::copy_range(self, trie2, lower2, lower2 + step);
                    lower2 += step;
                }
            }
        }

        if lower1 < upper1 { <OrderedLeafBuilder<K, R, C> as MergeBuilder>::copy_range(self, trie1, lower1, upper1); }
        if lower2 < upper2 { <OrderedLeafBuilder<K, R, C> as MergeBuilder>::copy_range(self, trie2, lower2, upper2); }

        self.vals.len()
    }
}

impl<K: Ord+Clone, R: Semigroup+Clone, C> TupleBuilder for OrderedLeafBuilder<K, R, C>
where
    C: BatchContainer<Item=(K, R)>+Deref<Target=[(K, R)]>,
{
    type Item = (K, R);
    fn new() -> Self { OrderedLeafBuilder { vals: C::default() } }
    fn with_capacity(cap: usize) -> Self { OrderedLeafBuilder { vals: C::with_capacity(cap) } }
    #[inline] fn push_tuple(&mut self, tuple: (K, R)) { self.vals.push(tuple) }
}

/// A cursor for walking through an unordered sequence of values.
///
/// This cursor does not support `seek`, though I'm not certain how to expose this.
#[derive(Debug)]
pub struct OrderedLeafCursor {
    pos: usize,
    bounds: (usize, usize),
    pos_nonnegative: bool, //[Pathway extension]: needed for a cursor that can do a step back 
}

impl<K: Clone, R: Clone, C> Cursor<OrderedLeaf<K, R, C>> for OrderedLeafCursor
where
    C: BatchContainer<Item=(K, R)>+Deref<Target=[(K, R)]>,
{
    type Key = (K, R);
    fn key<'a>(&self, storage: &'a OrderedLeaf<K, R, C>) -> &'a Self::Key { &storage.vals[self.pos] }
    fn step(&mut self, _storage: &OrderedLeaf<K, R, C>) {
        //[Pathway extension]: valid indicates that index was moved to 
        //[Pathway extension]: negative value, here we undo it
        if self.pos_nonnegative { 
            self.pos += 1; 
        } else {
            self.pos_nonnegative = true;
        }        
        
        if self.pos > self.bounds.1 {
            self.pos = self.bounds.1;
        }
    }
    fn seek(&mut self, _storage: &OrderedLeaf<K, R, C>, _key: &Self::Key) {
        panic!("seeking in an OrderedLeafCursor; should be fine, panic is wrong.");
    }
    //[Pathway extension]: valid considers also violation caused by stepping back
    fn valid(&self, _storage: &OrderedLeaf<K, R, C>) -> bool { self.pos >= self.bounds.0 && self.pos < self.bounds.1 && self.pos_nonnegative}
    fn rewind(&mut self, _storage: &OrderedLeaf<K, R, C>) {
        self.pos = self.bounds.0;
        self.pos_nonnegative = true; //[Pathway extension]: rewind makes cursor position valid
    }
    fn reposition(&mut self, _storage: &OrderedLeaf<K, R, C>, lower: usize, upper: usize) {
        self.pos = lower;
        self.bounds = (lower, upper);
        self.pos_nonnegative = true; //[Pathway extension]: reposition makes cursor position valid
    }
    
    //[Pathway extension]: needed for Bidirectional Cursor implementation 
    fn step_back(&mut self, _storage: &OrderedLeaf<K, R, C>) {
        if self.pos == 0 {
            self.pos_nonnegative = false;
        } else {
            self.pos -= 1;
            if self.pos > self.bounds.1 { self.pos = self.bounds.1; }
            if self.pos < self.bounds.0 { self.pos = self.bounds.0; }
            
        }
    }
    //[Pathway extension]: copy-pasting from implementation of seek, we panic whenever seek 
    // happens on ordered leaf. TODO - investigate, why the original author of seek decided to
    // leave it as it is.
    fn seek_smaller_eq(&mut self, _storage: &OrderedLeaf<K, R, C>, _key: &Self::Key){
        panic!("seeking in an OrderedLeafCursor; should be fine, panic is wrong.");
    }
}
