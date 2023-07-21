//! A generic cursor implementation merging pairs of different cursors.

use std::cmp::Ordering;

use super::Cursor;

/// A cursor over the combined updates of two different cursors.
///
/// A `CursorPair` wraps two cursors over the same types of updates, and provides navigation
/// through their merged updates.
pub struct CursorPair<C1, C2> {
    cursor1: C1,
    cursor2: C2,
    key_order: Ordering,    // Invalid keys are `Greater` than all other keys. `Equal` implies both valid.
    val_order: Ordering,    // Invalid vals are `Greater` than all other vals. `Equal` implies both valid.
}

impl<K, V, T, R, C1, C2> Cursor for CursorPair<C1, C2>
where
    K: Ord,
    V: Ord,
    C1: Cursor<Key = K, Val = V, Time = T, R = R>,
    C2: Cursor<Key = K, Val = V, Time = T, R = R>,
{
    type Key = K;
    type Val = V;
    type Time = T;
    type R = R;

    type Storage = (C1::Storage, C2::Storage);

    // validation methods
    fn key_valid(&self, storage: &Self::Storage) -> bool {
        match self.key_order {
            Ordering::Less => self.cursor1.key_valid(&storage.0),
            Ordering::Equal => true,
            Ordering::Greater => self.cursor2.key_valid(&storage.1),
        }
    }
    fn val_valid(&self, storage: &Self::Storage) -> bool {
        match (self.key_order, self.val_order) {
            (Ordering::Less, _) => self.cursor1.val_valid(&storage.0),
            (Ordering::Greater, _) => self.cursor2.val_valid(&storage.1),
            (Ordering::Equal, Ordering::Less) => self.cursor1.val_valid(&storage.0),
            (Ordering::Equal, Ordering::Greater) => self.cursor2.val_valid(&storage.1),
            (Ordering::Equal, Ordering::Equal) => true,
        }
    }

    // accessors
    fn key<'a>(&self, storage: &'a Self::Storage) -> &'a K {
        match self.key_order {
            Ordering::Less => self.cursor1.key(&storage.0),
            _ => self.cursor2.key(&storage.1),
        }
    }
    fn val<'a>(&self, storage: &'a Self::Storage) -> &'a V {
        if self.key_order == Ordering::Less || (self.key_order == Ordering::Equal && self.val_order != Ordering::Greater) {
            self.cursor1.val(&storage.0)
        }
        else {
            self.cursor2.val(&storage.1)
        }
    }
    fn map_times<L: FnMut(&T, &R)>(&mut self, storage: &Self::Storage, mut logic: L) {
        if self.key_order == Ordering::Less || (self.key_order == Ordering::Equal && self.val_order != Ordering::Greater) {
            self.cursor1.map_times(&storage.0, |t,d| logic(t,d));
        }
        if self.key_order == Ordering::Greater || (self.key_order == Ordering::Equal && self.val_order != Ordering::Less) {
            self.cursor2.map_times(&storage.1, |t,d| logic(t,d));
        }
    }

    // key methods
    fn step_key(&mut self, storage: &Self::Storage) {

        if self.key_order != Ordering::Greater { self.cursor1.step_key(&storage.0); }
        if self.key_order != Ordering::Less { self.cursor2.step_key(&storage.1); }

        self.key_order = match (self.cursor1.key_valid(&storage.0), self.cursor2.key_valid(&storage.1)) {
            (false, _) => Ordering::Greater,
            (_, false) => Ordering::Less,
            (true, true) => self.cursor1.key(&storage.0).cmp(self.cursor2.key(&storage.1)),
        };
    }
    fn seek_key(&mut self, storage: &Self::Storage, key: &K) {

        self.cursor1.seek_key(&storage.0, key);
        self.cursor2.seek_key(&storage.1, key);

        self.key_order = match (self.cursor1.key_valid(&storage.0), self.cursor2.key_valid(&storage.1)) {
            (false, _) => Ordering::Greater,
            (_, false) => Ordering::Less,
            (true, true) => self.cursor1.key(&storage.0).cmp(self.cursor2.key(&storage.1)),
        };
    }

    // value methods
    fn step_val(&mut self, storage: &Self::Storage) {
        match self.key_order {
            Ordering::Less => self.cursor1.step_val(&storage.0),
            Ordering::Equal => {
                if self.val_order != Ordering::Greater { self.cursor1.step_val(&storage.0); }
                if self.val_order != Ordering::Less { self.cursor2.step_val(&storage.1); }
                self.val_order = match (self.cursor1.val_valid(&storage.0), self.cursor2.val_valid(&storage.1)) {
                    (false, _) => Ordering::Greater,
                    (_, false) => Ordering::Less,
                    (true, true) => self.cursor1.val(&storage.0).cmp(self.cursor2.val(&storage.1)),
                };
            },
            Ordering::Greater => self.cursor2.step_val(&storage.1),
        }
    }
    fn seek_val(&mut self, storage: &Self::Storage, val: &V) {
        match self.key_order {
            Ordering::Less => self.cursor1.seek_val(&storage.0, val),
            Ordering::Equal => {
                self.cursor1.seek_val(&storage.0, val);
                self.cursor2.seek_val(&storage.1, val);
                self.val_order = match (self.cursor1.val_valid(&storage.0), self.cursor2.val_valid(&storage.1)) {
                    (false, _) => Ordering::Greater,
                    (_, false) => Ordering::Less,
                    (true, true) => self.cursor1.val(&storage.0).cmp(self.cursor2.val(&storage.1)),
                };
            },
            Ordering::Greater => self.cursor2.seek_val(&storage.1, val),
        }
    }

    // rewinding methods
    fn rewind_keys(&mut self, storage: &Self::Storage) {
        self.cursor1.rewind_keys(&storage.0);
        self.cursor2.rewind_keys(&storage.1);
    }
    fn rewind_vals(&mut self, storage: &Self::Storage) {
        if self.key_order != Ordering::Greater { self.cursor1.rewind_vals(&storage.0); }
        if self.key_order != Ordering::Less { self.cursor2.rewind_vals(&storage.1); }
    }
}