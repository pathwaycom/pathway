//! Bidirectional Cursor implemented for several data containers, needed for prev-next pointers
use std::convert::TryFrom;
use std::convert::TryInto;
use std::fmt::Debug;
use std::ops::Deref;

use trace::cursor::Cursor;
use trace::cursor::CursorList;
use trace::layers::Cursor as LayerCursor;

use crate::difference::Semigroup;
use crate::lattice::Lattice;
use crate::trace::implementations::ord::OrdKeyCursor;
use crate::trace::implementations::ord::OrdValCursor;
use crate::trace::implementations::ord::RetainFrom;
use crate::trace::layers::ordered::OrdOffset;
use crate::trace::layers::BatchContainer;

/*TODO cover with unit tests val-related navigation functions
Overall, the current implementation should be OK, but we don't really use
this functionality (step_val, step_back_val, ,seek_val, seek_smaller_eq_val),
and writing unit tests got postponed to the point we actually need this function.
*/
/// a Bidirectional Cursor
pub trait BidirectionalCursor: Cursor {
    /// Recedes the cursor to the previous key.
    fn step_back_key(&mut self, storage: &Self::Storage);
    /// Recedes the cursor to the previous value.
    fn step_back_val(&mut self, storage: &Self::Storage);

    /// Moves cursor to the last position holding a key smaller or equal
    /// to the specified key.
    fn seek_smaller_eq_key(&mut self, storage: &Self::Storage, key: &Self::Key);

    /// Moves cursor to the last position holding a val smaller or equal
    /// to the specified val.
    fn seek_smaller_eq_val(&mut self, storage: &Self::Storage, val: &Self::Val);
}

/// A bidirectional cursor made out of a list of bidirectional cursors
#[derive(Debug)]
pub struct BidirectionalCursorList<C: BidirectionalCursor> {
    cursors: Vec<C>,
    min_key: Vec<usize>,
    min_val: Vec<usize>,
    max_key: Vec<usize>,
    max_val: Vec<usize>,
    keys_to_move_back: Vec<usize>,
    keys_to_move_forward: Vec<usize>,
    in_lower_bound_positions: bool,
}

/*
    Tracking keys that need to be moved when moving forward / backward is a little bit convoluted;
    original implementation only moves up, so after seek+minimize keys, we are in right positions

    Here, we track indices of cursors that:
    - after step_key may point to proper next-key
    - after step_back_key may point to proper prev-key

    As such, whenever we end up in positions that upper-bound the position of the current key (in case
    os seek the current key is the one we look for), we mark all cursors with value larger-equal than
    the current key as to be moved back. We mark cursors for lower-bound positions in a symmetric way.
*/

/// Provides a bidirectional cursor interface over a list of bidirectional cursors.
///
/// The `BidirectionalCursorList` tracks the indices of bidirectional cursors with both minimum and maximum keys, and the the indices of cursors with
/// the minimum / maximum key and minimum / maximum value. It performs no clever management of these sets otherwise.
impl<C: BidirectionalCursor> BidirectionalCursorList<C>
where
    C::Key: Ord,
    C::Val: Ord,
{
    /// Creates a new cursor list from pre-existing cursors.
    pub fn new(cursors: Vec<C>, storage: &[C::Storage]) -> Self {
        let mut result = BidirectionalCursorList {
            cursors: cursors,
            min_key: Vec::new(),
            min_val: Vec::new(),
            max_key: Vec::new(),
            max_val: Vec::new(),
            keys_to_move_back: Vec::new(),
            keys_to_move_forward: Vec::new(),
            in_lower_bound_positions: false,
        };
        result.handle_upper_positions(storage);
        result
    }

    // Initialize min_key with the indices of cursors with the minimum key.
    //
    // This method scans the current keys of each cursor, and tracks the indices
    // of cursors whose key equals the minimum valid key seen so far. As it goes,
    // if it observes an improved key it clears the current list, updates the
    // minimum key, and continues.
    //
    // Once finished, it invokes `minimize_vals()` to ensure the value cursor is
    // in a consistent state as well.

    // Furthermore, for a cursor list in the upper-bound position, this function:
    // - finds a set of indices holding cursors with smallest key
    // - computes set of indices to be moved on step_key
    // - computes set of indices to be moved in step_key_back
    fn handle_upper_positions(&mut self, storage: &[C::Storage]) {
        self.min_key.clear();
        self.keys_to_move_forward.clear();
        self.keys_to_move_back.clear();

        let mut all_keys = Vec::new();
        let mut min_key_opt = None;
        for (index, cursor) in self.cursors.iter().enumerate() {
            let key = cursor.get_key(&storage[index]);
            all_keys.push(key);

            if key.is_some() {
                if min_key_opt.is_none() || key.lt(&min_key_opt) {
                    min_key_opt = key;
                    self.min_key.clear();
                }
                if key.eq(&min_key_opt) {
                    self.min_key.push(index);
                }
            }
        }

        //if no valid key, all positions need to be added for both lists (hence the first part of if-conditions)
        for (index, k) in all_keys.iter().enumerate() {
            //any upper-bound position that minimizes key may have next as successor
            //any upper bound position that is invalid because it is too small may have next as successor
            //other invalid keys are too large, can be safely added as they have no valid successors to break the invariants
            if min_key_opt.is_none() || k.is_none() || k.eq(&min_key_opt) {
                self.keys_to_move_forward.push(index);
            }

            //any upper-bound position that is equal or larger than the min may have prev as predecessor,
            //any upper-bound position that is invalid because it is too large may have prev as predecessor
            //other invalid keys are too small,  can be safely added as they have no valid predecessors to break the invariants
            // any position meets at least one of the above conditions
            self.keys_to_move_back.push(index);
        }
        self.minimize_vals(storage);
    }

    fn minimize_vals(&mut self, storage: &[C::Storage]) {
        self.min_val.clear();

        // Determine the index of the cursor with maximum value.
        let mut min_val_opt = None;
        for &index in self.min_key.iter() {
            let val = self.cursors[index].get_val(&storage[index]);
            if val.is_some() {
                if min_val_opt.is_none() || val.lt(&min_val_opt) {
                    min_val_opt = val;
                    self.min_val.clear();
                }
                if val.eq(&min_val_opt) {
                    self.min_val.push(index);
                }
            }
        }
    }

    // Initialize max_key with the indices of cursors with the maximum key.
    //
    // This method scans the current keys of each cursor, and tracks the indices
    // of cursors whose key equals the maximum valid key seen so far. As it goes,
    // if it observes an improved key it clears the current list, updates the
    // maximum key, and continues.
    //
    // Once finished, it invokes `maximize_vals()` to ensure the value cursor is
    // in a consistent state as well.

    // Furthermore, for a cursor list in the lower-bound position, this function:
    // - finds a set of indices holding cursors with largest key
    // - computes set of indices to be moved on step_key
    // - computes set of indices to be moved in step_key_back
    fn handle_lower_positions(&mut self, storage: &[C::Storage]) {
        self.max_key.clear();
        self.keys_to_move_forward.clear();
        self.keys_to_move_back.clear();

        let mut all_keys = Vec::new();
        let mut max_key_opt = None;
        for (index, cursor) in self.cursors.iter().enumerate() {
            let key = cursor.get_key(&storage[index]);
            all_keys.push(key);

            if key.is_some() {
                if max_key_opt.is_none() || key.gt(&max_key_opt) {
                    max_key_opt = key;
                    self.max_key.clear();
                }
                if key.eq(&max_key_opt) {
                    self.max_key.push(index);
                }
            }
        }

        //if no valid key, all positions need to be added for both lists (hence the first part of if-conditions)
        for (index, k) in all_keys.iter().enumerate() {
            //any lower-bound position that maximizes key have prev as predecessor,
            //any lower-bound position that is invalid because it is too large may have prev as predecessor
            //other invalid keys are too small, can be safely added as they have no valid predecessors to break the invariants
            if max_key_opt.is_none() || k.is_none() || k.eq(&max_key_opt) {
                self.keys_to_move_back.push(index);
            }

            //any lower-bound position that is smaller or equal to max key may have next as successor
            //any lower-bound position that is invalid because it is too small may have next as successor
            //other invalid keys are too large, can be safely added as they have no valid successors to break the invariants
            // any position meets at least one of the above conditions
            self.keys_to_move_forward.push(index);
        }
        self.maximize_vals(storage);
    }

    // Initialize max_val with the indices of maximum key cursors with the maximum value.
    //
    // This method scans the current values of cursor with maximum keys, and tracks the
    // indices of cursors whose value equals the maximum valid value seen so far. As it
    // goes, if it observes an improved value it clears the current list, updates the maximum
    // value, and continues.
    fn maximize_vals(&mut self, storage: &[C::Storage]) {
        self.max_val.clear();

        // Determine the index of the cursor with maximum value.
        let mut max_val_opt = None;
        for &index in self.max_key.iter() {
            let val = self.cursors[index].get_val(&storage[index]);
            if val.is_some() {
                if max_val_opt.is_none() || val.gt(&max_val_opt) {
                    max_val_opt = val;
                    self.max_val.clear();
                }
                if val.eq(&max_val_opt) {
                    self.max_val.push(index);
                }
            }
        }
    }
}

impl<C: BidirectionalCursor> Cursor for BidirectionalCursorList<C>
where
    C::Key: Ord,
    C::Val: Ord,
{
    type Key = <CursorList<C> as Cursor>::Key;

    type Val = <CursorList<C> as Cursor>::Val;

    type Time = <CursorList<C> as Cursor>::Time;

    type R = <CursorList<C> as Cursor>::R;

    type Storage = <CursorList<C> as Cursor>::Storage;

    #[inline]
    fn key_valid(&self, _storage: &Self::Storage) -> bool {
        if !self.in_lower_bound_positions {
            !self.min_key.is_empty()
        } else {
            !self.max_key.is_empty()
        }
    }
    #[inline]
    fn val_valid(&self, _storage: &Self::Storage) -> bool {
        if !self.in_lower_bound_positions {
            !self.min_val.is_empty()
        } else {
            !self.max_val.is_empty()
        }
    }
    #[inline]
    fn key<'a>(&self, storage: &'a Self::Storage) -> &'a Self::Key {
        debug_assert!(self.key_valid(storage));
        let index = if self.in_lower_bound_positions {
            *self.max_key.last().unwrap()
        } else {
            self.min_key[0]
        };
        debug_assert!(self.cursors[index].key_valid(&storage[index]));
        self.cursors[index].key(&storage[index])
    }
    #[inline]
    fn val<'a>(&self, storage: &'a Self::Storage) -> &'a Self::Val {
        debug_assert!(self.key_valid(storage));
        debug_assert!(self.val_valid(storage));
        let index = if self.in_lower_bound_positions {
            *self.max_val.last().unwrap()
        } else {
            self.min_val[0]
        };
        debug_assert!(self.cursors[index].val_valid(&storage[index]));
        self.cursors[index].val(&storage[index])
    }
    #[inline]
    fn map_times<L: FnMut(&Self::Time, &Self::R)>(
        &mut self,
        storage: &Self::Storage,
        mut logic: L,
    ) {
        let indices = if self.in_lower_bound_positions {
            &self.max_val
        } else {
            &self.min_val
        };
        for &index in indices.iter() {
            self.cursors[index].map_times(&storage[index], |t, d| logic(t, d));
        }
    }
    #[inline]
    fn step_key(&mut self, storage: &Self::Storage) {
        self.in_lower_bound_positions = false;
        for &index in self.keys_to_move_forward.iter() {
            self.cursors[index].step_key(&storage[index]);
        }
        self.handle_upper_positions(storage);
    }
    #[inline]
    fn seek_key(&mut self, storage: &Self::Storage, key: &Self::Key) {
        self.in_lower_bound_positions = false;
        for index in 0..self.cursors.len() {
            self.cursors[index].seek_key(&storage[index], key);
        }
        self.handle_upper_positions(storage);
    }
    #[inline]
    #[allow(unreachable_code, unused_variables)]
    fn step_val(&mut self, storage: &Self::Storage) {
        todo!("please cover this with unit tests, before using");
        for &index in self.min_val.iter() {
            self.cursors[index].step_val(&storage[index]);
        }
        self.minimize_vals(storage);
        self.maximize_vals(storage)
    }
    #[inline]
    #[allow(unreachable_code, unused_variables)]
    fn seek_val(&mut self, storage: &Self::Storage, val: &Self::Val) {
        todo!("please cover this with unit tests, before using");
        for &index in self.min_key.iter() {
            self.cursors[index].seek_val(&storage[index], val);
        }
        self.minimize_vals(storage);
        self.maximize_vals(storage)
    }
    #[inline]
    fn rewind_keys(&mut self, storage: &Self::Storage) {
        for index in 0..self.cursors.len() {
            self.cursors[index].rewind_keys(&storage[index]);
        }
        self.handle_upper_positions(storage);
    }
    #[inline]
    #[allow(unreachable_code, unused_variables)]
    fn rewind_vals(&mut self, storage: &Self::Storage) {
        todo!("please cover this with unit tests, before using");
        let indices = if self.in_lower_bound_positions {
            &self.max_key
        } else {
            &self.min_key
        };
        for &index in indices.iter() {
            self.cursors[index].rewind_vals(&storage[index]);
        }

        self.minimize_vals(storage);
        self.maximize_vals(storage)
    }
}

impl<C: BidirectionalCursor> BidirectionalCursor for BidirectionalCursorList<C>
where
    C::Key: Ord,
    C::Val: Ord,
{
    #[inline]
    fn step_back_key(&mut self, storage: &Self::Storage) {
        self.in_lower_bound_positions = true;
        for &index in self.keys_to_move_back.iter() {
            self.cursors[index].step_back_key(&storage[index]);
        }
        self.handle_lower_positions(storage);
    }

    #[inline]
    #[allow(unreachable_code, unused_variables)]
    fn step_back_val(&mut self, storage: &Self::Storage) {
        todo!("please cover this with unit tests, before using");
        for &index in self.max_val.iter() {
            self.cursors[index].step_back_val(&storage[index]);
        }
        self.minimize_vals(storage);
        self.maximize_vals(storage)
    }

    fn seek_smaller_eq_key(&mut self, storage: &Self::Storage, key: &Self::Key) {
        self.in_lower_bound_positions = true;
        for index in 0..self.cursors.len() {
            self.cursors[index].seek_smaller_eq_key(&storage[index], key);
        }
        self.handle_lower_positions(storage);
    }

    #[allow(unreachable_code, unused_variables)]
    fn seek_smaller_eq_val(&mut self, storage: &Self::Storage, val: &Self::Val) {
        todo!("please cover this with unit tests, before using");
        for &index in self.max_key.iter() {
            self.cursors[index].seek_smaller_eq_val(&storage[index], val);
        }
        self.handle_lower_positions(storage)
    }
}

impl<K, V, T, R, O, CK, CV> BidirectionalCursor for OrdValCursor<K, V, T, R, O, CK, CV>
where
    K: Ord + Clone,
    V: Ord + Clone,
    T: Lattice + Ord + Clone,
    R: Semigroup,
    O: OrdOffset,
    <O as TryFrom<usize>>::Error: Debug,
    <O as TryInto<usize>>::Error: Debug,
    CK: BatchContainer<Item = K> + Deref<Target = [K]> + RetainFrom<K>,
    CV: BatchContainer<Item = V> + Deref<Target = [V]> + RetainFrom<V>,
{
    fn step_back_key(&mut self, storage: &Self::Storage) {
        self.cursor.step_back(&storage.layer)
    }

    #[allow(unreachable_code, unused_variables)]
    fn step_back_val(&mut self, storage: &Self::Storage) {
        todo!("please cover this with unit tests, before using");
        self.cursor.child.step_back(&storage.layer.vals);
    }

    fn seek_smaller_eq_key(&mut self, storage: &Self::Storage, key: &Self::Key) {
        self.cursor.seek_smaller_eq(&storage.layer, key);
    }

    #[allow(unreachable_code, unused_variables)]
    fn seek_smaller_eq_val(&mut self, storage: &Self::Storage, val: &Self::Val) {
        todo!("please cover this with unit tests, before using");
        self.cursor.child.seek_smaller_eq(&storage.layer.vals, val);
    }
}

/// Exposes the capability of the cursor to move back
impl<K, T, R, O, CK> BidirectionalCursor for OrdKeyCursor<K, T, R, O, CK>
where
    K: Ord + Clone,
    T: Lattice + Ord + Clone,
    R: Semigroup,
    O: OrdOffset,
    <O as TryFrom<usize>>::Error: Debug,
    <O as TryInto<usize>>::Error: Debug,
    CK: BatchContainer<Item = K> + Deref<Target = [K]> + RetainFrom<K>,
{
    fn step_back_key(&mut self, storage: &Self::Storage) {
        self.cursor.step_back(&storage.layer);
    }

    #[allow(unreachable_code, unused_variables)]
    fn step_back_val(&mut self, _storage: &Self::Storage) {
        todo!("please cover this with unit tests, before using");
        self.valid = false;
    }

    fn seek_smaller_eq_key(&mut self, storage: &Self::Storage, key: &Self::Key) {
        self.cursor.seek_smaller_eq(&storage.layer, key)
    }

    fn seek_smaller_eq_val(&mut self, _storage: &Self::Storage, _val: &()) {}
}
