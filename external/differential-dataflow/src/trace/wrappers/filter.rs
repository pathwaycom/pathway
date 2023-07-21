//! Wrapper for filtered trace.

use timely::progress::Timestamp;
use timely::progress::frontier::AntichainRef;

use trace::{TraceReader, BatchReader, Description};
use trace::cursor::Cursor;

/// Wrapper to provide trace to nested scope.
pub struct TraceFilter<Tr, F> {
    trace: Tr,
    logic: F,
}

impl<Tr,F> Clone for TraceFilter<Tr, F>
where
    Tr: TraceReader+Clone,
    F: Clone,
{
    fn clone(&self) -> Self {
        TraceFilter {
            trace: self.trace.clone(),
            logic: self.logic.clone(),
        }
    }
}

impl<Tr, F> TraceReader for TraceFilter<Tr, F>
where
    Tr: TraceReader,
    Tr::Batch: Clone,
    Tr::Key: 'static,
    Tr::Val: 'static,
    Tr::Time: Timestamp,
    Tr::R: 'static,
    F: FnMut(&Tr::Key, &Tr::Val)->bool+Clone+'static,
{
    type Key = Tr::Key;
    type Val = Tr::Val;
    type Time = Tr::Time;
    type R = Tr::R;

    type Batch = BatchFilter<Tr::Batch, F>;
    type Cursor = CursorFilter<Tr::Cursor, F>;

    fn map_batches<F2: FnMut(&Self::Batch)>(&self, mut f: F2) {
        let logic = self.logic.clone();
        self.trace
            .map_batches(|batch| f(&Self::Batch::make_from(batch.clone(), logic.clone())))
    }

    fn set_logical_compaction(&mut self, frontier: AntichainRef<Tr::Time>) { self.trace.set_logical_compaction(frontier) }
    fn get_logical_compaction(&mut self) -> AntichainRef<Tr::Time> { self.trace.get_logical_compaction() }

    fn set_physical_compaction(&mut self, frontier: AntichainRef<Tr::Time>) { self.trace.set_physical_compaction(frontier) }
    fn get_physical_compaction(&mut self) -> AntichainRef<Tr::Time> { self.trace.get_physical_compaction() }

    fn cursor_through(&mut self, upper: AntichainRef<Tr::Time>) -> Option<(Self::Cursor, <Self::Cursor as Cursor>::Storage)> {
        self.trace.cursor_through(upper).map(|(x,y)| (CursorFilter::new(x, self.logic.clone()), y))
    }
}

impl<Tr, F> TraceFilter<Tr, F>
where
    Tr: TraceReader,
    Tr::Time: Timestamp,
{
    /// Makes a new trace wrapper
    pub fn make_from(trace: Tr, logic: F) -> Self {
        TraceFilter {
            trace,
            logic,
        }
    }
}


/// Wrapper to provide batch to nested scope.
#[derive(Clone)]
pub struct BatchFilter<B, F> {
    batch: B,
    logic: F,
}

impl<B, F> BatchReader for BatchFilter<B, F>
where
    B: BatchReader,
    B::Time: Timestamp,
    F: FnMut(&B::Key, &B::Val)->bool+Clone+'static
{
    type Key = B::Key;
    type Val = B::Val;
    type Time = B::Time;
    type R = B::R;

    type Cursor = BatchCursorFilter<B, F>;

    fn cursor(&self) -> Self::Cursor {
        BatchCursorFilter::new(self.batch.cursor(), self.logic.clone())
    }
    fn len(&self) -> usize { self.batch.len() }
    fn description(&self) -> &Description<B::Time> { &self.batch.description() }
}

impl<B, F> BatchFilter<B, F>
where
    B: BatchReader,
    B::Time: Timestamp,
{
    /// Makes a new batch wrapper
    pub fn make_from(batch: B, logic: F) -> Self {
        BatchFilter {
            batch,
            logic,
        }
    }
}

/// Wrapper to provide cursor to nested scope.
pub struct CursorFilter<C: Cursor, F> {
    cursor: C,
    logic: F,
}

impl<C: Cursor, F> CursorFilter<C, F> {
    fn new(cursor: C, logic: F) -> Self {
        CursorFilter {
            cursor,
            logic,
        }
    }
}

impl<C, F> Cursor for CursorFilter<C, F>
where
    C: Cursor,
    C::Time: Timestamp,
    F: FnMut(&C::Key, &C::Val)->bool+'static
{
    type Key = C::Key;
    type Val = C::Val;
    type Time = C::Time;
    type R = C::R;

    type Storage = C::Storage;

    #[inline] fn key_valid(&self, storage: &Self::Storage) -> bool { self.cursor.key_valid(storage) }
    #[inline] fn val_valid(&self, storage: &Self::Storage) -> bool { self.cursor.val_valid(storage) }

    #[inline] fn key<'a>(&self, storage: &'a Self::Storage) -> &'a Self::Key { self.cursor.key(storage) }
    #[inline] fn val<'a>(&self, storage: &'a Self::Storage) -> &'a Self::Val { self.cursor.val(storage) }

    #[inline]
    fn map_times<L: FnMut(&Self::Time,&Self::R)>(&mut self, storage: &Self::Storage, logic: L) {
        let key = self.key(storage);
        let val = self.val(storage);
        if (self.logic)(key, val) {
            self.cursor.map_times(storage, logic)
        }
    }

    #[inline] fn step_key(&mut self, storage: &Self::Storage) { self.cursor.step_key(storage) }
    #[inline] fn seek_key(&mut self, storage: &Self::Storage, key: &Self::Key) { self.cursor.seek_key(storage, key) }

    #[inline] fn step_val(&mut self, storage: &Self::Storage) { self.cursor.step_val(storage) }
    #[inline] fn seek_val(&mut self, storage: &Self::Storage, val: &Self::Val) { self.cursor.seek_val(storage, val) }

    #[inline] fn rewind_keys(&mut self, storage: &Self::Storage) { self.cursor.rewind_keys(storage) }
    #[inline] fn rewind_vals(&mut self, storage: &Self::Storage) { self.cursor.rewind_vals(storage) }
}



/// Wrapper to provide cursor to nested scope.
pub struct BatchCursorFilter<B: BatchReader, F> {
    cursor: B::Cursor,
    logic: F,
}

impl<B: BatchReader, F> BatchCursorFilter<B, F> {
    fn new(cursor: B::Cursor, logic: F) -> Self {
        BatchCursorFilter {
            cursor,
            logic,
        }
    }
}

impl<B: BatchReader, F> Cursor for BatchCursorFilter<B, F>
where
    B::Time: Timestamp,
    F: FnMut(&B::Key, &B::Val)->bool+'static,
{
    type Key = B::Key;
    type Val = B::Val;
    type Time = B::Time;
    type R = B::R;

    type Storage = BatchFilter<B, F>;

    #[inline] fn key_valid(&self, storage: &Self::Storage) -> bool { self.cursor.key_valid(&storage.batch) }
    #[inline] fn val_valid(&self, storage: &Self::Storage) -> bool { self.cursor.val_valid(&storage.batch) }

    #[inline] fn key<'a>(&self, storage: &'a Self::Storage) -> &'a Self::Key { self.cursor.key(&storage.batch) }
    #[inline] fn val<'a>(&self, storage: &'a Self::Storage) -> &'a Self::Val { self.cursor.val(&storage.batch) }

    #[inline]
    fn map_times<L: FnMut(&Self::Time,&Self::R)>(&mut self, storage: &Self::Storage, logic: L) {
        let key = self.key(storage);
        let val = self.val(storage);
        if (self.logic)(key, val) {
            self.cursor.map_times(&storage.batch, logic)
        }
    }

    #[inline] fn step_key(&mut self, storage: &Self::Storage) { self.cursor.step_key(&storage.batch) }
    #[inline] fn seek_key(&mut self, storage: &Self::Storage, key: &Self::Key) { self.cursor.seek_key(&storage.batch, key) }

    #[inline] fn step_val(&mut self, storage: &Self::Storage) { self.cursor.step_val(&storage.batch) }
    #[inline] fn seek_val(&mut self, storage: &Self::Storage, val: &Self::Val) { self.cursor.seek_val(&storage.batch, val) }

    #[inline] fn rewind_keys(&mut self, storage: &Self::Storage) { self.cursor.rewind_keys(&storage.batch) }
    #[inline] fn rewind_vals(&mut self, storage: &Self::Storage) { self.cursor.rewind_vals(&storage.batch) }
}
