//! Bidirectional Batch Reader trait and its implementations

use std::convert::{TryFrom, TryInto};
use std::fmt::Debug;
use std::ops::Deref;

use crate::{
    difference::Semigroup,
    lattice::Lattice,
    trace::{
        implementations::ord::{OrdKeyBatch, OrdKeyCursor, OrdValBatch, OrdValCursor, RetainFrom},
        layers::{ordered::OrdOffset, BatchContainer},
        BatchReader,
    },
};

use pathway::cursor::BidirectionalCursor;

/// Extends BatchReader by a bidirectional cursor
pub trait BidirectionalBatchReader: BatchReader {
    /// The type used to navigate the batch's contents.
    type BidirectionalCursor: BidirectionalCursor<
        Key = Self::Key,
        Val = Self::Val,
        Time = Self::Time,
        R = Self::R,
        Storage = Self,
    >;

    /// Acquires a bidirectional cursor to the batch's contents.
    fn bidirectional_cursor(&self) -> Self::BidirectionalCursor;
}

/// Blanket implementations for Rc wrapper
pub mod rc_blanket_impls {

    use pathway::cursor::BidirectionalCursor;
    use std::rc::Rc;

    use crate::trace::Cursor;

    use super::BidirectionalBatchReader;

    impl<B: BidirectionalBatchReader> BidirectionalBatchReader for Rc<B> {
        type BidirectionalCursor = RcBatchBidirectionalCursor<B>;

        fn bidirectional_cursor(&self) -> Self::BidirectionalCursor {
            RcBatchBidirectionalCursor::new((&**self).bidirectional_cursor())
        }
    }

    /// Wrapper to provide bidirectional cursor to nested scope.
    pub struct RcBatchBidirectionalCursor<B: BidirectionalBatchReader> {
        bidirectional_cursor: B::BidirectionalCursor,
    }

    impl<B: BidirectionalBatchReader> RcBatchBidirectionalCursor<B> {
        fn new(bidirectional_cursor: B::BidirectionalCursor) -> Self {
            RcBatchBidirectionalCursor {
                bidirectional_cursor,
            }
        }
    }

    impl<B: BidirectionalBatchReader> Cursor for RcBatchBidirectionalCursor<B> {
        type Key = B::Key;

        type Val = B::Val;

        type Time = B::Time;

        type R = B::R;

        type Storage = Rc<B>;

        fn key_valid(&self, storage: &Self::Storage) -> bool {
            self.bidirectional_cursor.key_valid(storage)
        }

        fn val_valid(&self, storage: &Self::Storage) -> bool {
            self.bidirectional_cursor.val_valid(storage)
        }

        fn key<'a>(&self, storage: &'a Self::Storage) -> &'a Self::Key {
            self.bidirectional_cursor.key(storage)
        }

        fn val<'a>(&self, storage: &'a Self::Storage) -> &'a Self::Val {
            self.bidirectional_cursor.val(storage)
        }

        fn map_times<L: FnMut(&Self::Time, &Self::R)>(
            &mut self,
            storage: &Self::Storage,
            logic: L,
        ) {
            self.bidirectional_cursor.map_times(storage, logic)
        }

        fn step_key(&mut self, storage: &Self::Storage) {
            self.bidirectional_cursor.step_key(storage)
        }

        fn seek_key(&mut self, storage: &Self::Storage, key: &Self::Key) {
            self.bidirectional_cursor.seek_key(storage, key)
        }

        fn step_val(&mut self, storage: &Self::Storage) {
            self.bidirectional_cursor.step_val(storage)
        }

        fn seek_val(&mut self, storage: &Self::Storage, val: &Self::Val) {
            self.bidirectional_cursor.seek_val(storage, val)
        }

        fn rewind_keys(&mut self, storage: &Self::Storage) {
            self.bidirectional_cursor.rewind_keys(storage)
        }

        fn rewind_vals(&mut self, storage: &Self::Storage) {
            self.bidirectional_cursor.rewind_vals(storage)
        }
    }

    impl<B: BidirectionalBatchReader> BidirectionalCursor for RcBatchBidirectionalCursor<B> {
        #[inline]

        fn step_back_key(&mut self, storage: &Self::Storage) {
            self.bidirectional_cursor.step_back_key(storage)
        }

        fn step_back_val(&mut self, storage: &Self::Storage) {
            self.bidirectional_cursor.step_back_val(storage)
        }

        fn seek_smaller_eq_key(&mut self, storage: &Self::Storage, key: &Self::Key) {
            self.bidirectional_cursor.seek_smaller_eq_key(storage, key)
        }

        fn seek_smaller_eq_val(&mut self, storage: &Self::Storage, val: &Self::Val) {
            self.bidirectional_cursor.seek_smaller_eq_val(storage, val)
        }
    }
}

impl<K, V, T, R, O, CK, CV> BidirectionalBatchReader for OrdValBatch<K, V, T, R, O, CK, CV>
where
    K: Ord + Clone + 'static,
    V: Ord + Clone + 'static,
    T: Lattice + Ord + Clone + 'static,
    R: Semigroup,
    O: OrdOffset,
    <O as TryFrom<usize>>::Error: Debug,
    <O as TryInto<usize>>::Error: Debug,
    CK: BatchContainer<Item = K> + Deref<Target = [K]> + RetainFrom<K>,
    CV: BatchContainer<Item = V> + Deref<Target = [V]> + RetainFrom<V>,
{
    type BidirectionalCursor = OrdValCursor<K, V, T, R, O, CK, CV>;

    fn bidirectional_cursor(&self) -> Self::BidirectionalCursor {
        self.cursor()
    }
}

impl<K, T, R, O, CK> BidirectionalBatchReader for OrdKeyBatch<K, T, R, O, CK>
where
    K: Ord + Clone + 'static,
    T: Lattice + Ord + Clone + 'static,
    R: Clone + Semigroup,
    O: OrdOffset,
    <O as TryFrom<usize>>::Error: Debug,
    <O as TryInto<usize>>::Error: Debug,
    CK: BatchContainer<Item = K> + Deref<Target = [K]> + RetainFrom<K>,
{
    type BidirectionalCursor = OrdKeyCursor<K, T, R, O, CK>;

    fn bidirectional_cursor(&self) -> Self::BidirectionalCursor {
        self.cursor()
    }
}
