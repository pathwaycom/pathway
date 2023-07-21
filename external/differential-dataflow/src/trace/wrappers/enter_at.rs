//! Wrappers to provide trace access to nested scopes.

use timely::progress::timestamp::Refines;
use timely::progress::Timestamp;
use timely::progress::{Antichain, frontier::AntichainRef};

use lattice::Lattice;
use trace::{TraceReader, BatchReader, Description};
use trace::cursor::Cursor;

/// Wrapper to provide trace to nested scope.
///
/// Each wrapped update is presented with a timestamp determined by `logic`.
///
/// At the same time, we require a method `prior` that can "invert" timestamps,
/// and which will be applied to compaction frontiers as they are communicated
/// back to the wrapped traces. A better explanation is pending, and until that
/// happens use this construct at your own peril!
pub struct TraceEnter<Tr, TInner, F, G>
where
    Tr: TraceReader,
{
    trace: Tr,
    stash1: Antichain<Tr::Time>,
    stash2: Antichain<TInner>,
    logic: F,
    prior: G,
}

impl<Tr,TInner,F,G> Clone for TraceEnter<Tr, TInner, F, G>
where
    Tr: TraceReader+Clone,
    F: Clone,
    G: Clone,
{
    fn clone(&self) -> Self {
        TraceEnter {
            trace: self.trace.clone(),
            stash1: Antichain::new(),
            stash2: Antichain::new(),
            logic: self.logic.clone(),
            prior: self.prior.clone(),
        }
    }
}

impl<Tr, TInner, F, G> TraceReader for TraceEnter<Tr, TInner, F, G>
where
    Tr: TraceReader,
    Tr::Batch: Clone,
    Tr::Key: 'static,
    Tr::Val: 'static,
    Tr::Time: Timestamp,
    TInner: Refines<Tr::Time>+Lattice,
    Tr::R: 'static,
    F: 'static,
    F: FnMut(&Tr::Key, &Tr::Val, &Tr::Time)->TInner+Clone,
    G: FnMut(&TInner)->Tr::Time+Clone+'static,
{
    type Key = Tr::Key;
    type Val = Tr::Val;
    type Time = TInner;
    type R = Tr::R;

    type Batch = BatchEnter<Tr::Batch, TInner,F>;
    type Cursor = CursorEnter<Tr::Cursor, TInner,F>;

    fn map_batches<F2: FnMut(&Self::Batch)>(&self, mut f: F2) {
        let logic = self.logic.clone();
        self.trace.map_batches(|batch| {
            f(&Self::Batch::make_from(batch.clone(), logic.clone()));
        })
    }

    fn set_logical_compaction(&mut self, frontier: AntichainRef<TInner>) {
        self.stash1.clear();
        for time in frontier.iter() {
            self.stash1.insert((self.prior)(time));
        }
        self.trace.set_logical_compaction(self.stash1.borrow());
    }
    fn get_logical_compaction(&mut self) -> AntichainRef<TInner> {
        self.stash2.clear();
        for time in self.trace.get_logical_compaction().iter() {
            self.stash2.insert(TInner::to_inner(time.clone()));
        }
        self.stash2.borrow()
    }

    fn set_physical_compaction(&mut self, frontier: AntichainRef<TInner>) {
        self.stash1.clear();
        for time in frontier.iter() {
            self.stash1.insert((self.prior)(time));
        }
        self.trace.set_physical_compaction(self.stash1.borrow());
    }
    fn get_physical_compaction(&mut self) -> AntichainRef<TInner> {
        self.stash2.clear();
        for time in self.trace.get_physical_compaction().iter() {
            self.stash2.insert(TInner::to_inner(time.clone()));
        }
        self.stash2.borrow()
    }

    fn cursor_through(&mut self, upper: AntichainRef<TInner>) -> Option<(Self::Cursor, <Self::Cursor as Cursor>::Storage)> {
        self.stash1.clear();
        for time in upper.iter() {
            self.stash1.insert(time.clone().to_outer());
        }
        self.trace.cursor_through(self.stash1.borrow()).map(|(x,y)| (CursorEnter::new(x, self.logic.clone()), y))
    }
}

impl<Tr, TInner, F, G> TraceEnter<Tr, TInner, F, G>
where
    Tr: TraceReader,
    Tr::Time: Timestamp,
    TInner: Refines<Tr::Time>+Lattice,
{
    /// Makes a new trace wrapper
    pub fn make_from(trace: Tr, logic: F, prior: G) -> Self {
        TraceEnter {
            trace: trace,
            stash1: Antichain::new(),
            stash2: Antichain::new(),
            logic,
            prior,
        }
    }
}


/// Wrapper to provide batch to nested scope.
#[derive(Clone)]
pub struct BatchEnter<B, TInner, F> {
    batch: B,
    description: Description<TInner>,
    logic: F,
}

impl<B, TInner, F> BatchReader for BatchEnter<B, TInner, F>
where
    B: BatchReader,
    B::Time: Timestamp,
    TInner: Refines<B::Time>+Lattice,
    F: FnMut(&B::Key, &B::Val, &B::Time)->TInner+Clone,
{
    type Key = B::Key;
    type Val = B::Val;
    type Time = TInner;
    type R = B::R;

    type Cursor = BatchCursorEnter<B, TInner, F>;

    fn cursor(&self) -> Self::Cursor {
        BatchCursorEnter::new(self.batch.cursor(), self.logic.clone())
    }
    fn len(&self) -> usize { self.batch.len() }
    fn description(&self) -> &Description<TInner> { &self.description }
}

impl<B, TInner, F> BatchEnter<B, TInner, F>
where
    B: BatchReader,
    B::Time: Timestamp,
    TInner: Refines<B::Time>+Lattice,
{
    /// Makes a new batch wrapper
    pub fn make_from(batch: B, logic: F) -> Self {
        let lower: Vec<_> = batch.description().lower().elements().iter().map(|x| TInner::to_inner(x.clone())).collect();
        let upper: Vec<_> = batch.description().upper().elements().iter().map(|x| TInner::to_inner(x.clone())).collect();
        let since: Vec<_> = batch.description().since().elements().iter().map(|x| TInner::to_inner(x.clone())).collect();

        BatchEnter {
            batch,
            description: Description::new(Antichain::from(lower), Antichain::from(upper), Antichain::from(since)),
            logic,
        }
    }
}

/// Wrapper to provide cursor to nested scope.
pub struct CursorEnter<C: Cursor, TInner, F> {
    phantom: ::std::marker::PhantomData<TInner>,
    cursor: C,
    logic: F,
}

impl<C: Cursor, TInner, F> CursorEnter<C, TInner, F> {
    fn new(cursor: C, logic: F) -> Self {
        CursorEnter {
            phantom: ::std::marker::PhantomData,
            cursor,
            logic,
        }
    }
}

impl<C, TInner, F> Cursor for CursorEnter<C, TInner, F>
where
    C: Cursor,
    C::Time: Timestamp,
    TInner: Refines<C::Time>+Lattice,
    F: FnMut(&C::Key, &C::Val, &C::Time)->TInner,
{
    type Key = C::Key;
    type Val = C::Val;
    type Time = TInner;
    type R = C::R;

    type Storage = C::Storage;

    #[inline] fn key_valid(&self, storage: &Self::Storage) -> bool { self.cursor.key_valid(storage) }
    #[inline] fn val_valid(&self, storage: &Self::Storage) -> bool { self.cursor.val_valid(storage) }

    #[inline] fn key<'a>(&self, storage: &'a Self::Storage) -> &'a Self::Key { self.cursor.key(storage) }
    #[inline] fn val<'a>(&self, storage: &'a Self::Storage) -> &'a Self::Val { self.cursor.val(storage) }

    #[inline]
    fn map_times<L: FnMut(&TInner, &Self::R)>(&mut self, storage: &Self::Storage, mut logic: L) {
        let key = self.key(storage);
        let val = self.val(storage);
        let logic2 = &mut self.logic;
        self.cursor.map_times(storage, |time, diff| {
            logic(&logic2(key, val, time), diff)
        })
    }

    #[inline] fn step_key(&mut self, storage: &Self::Storage) { self.cursor.step_key(storage) }
    #[inline] fn seek_key(&mut self, storage: &Self::Storage, key: &Self::Key) { self.cursor.seek_key(storage, key) }

    #[inline] fn step_val(&mut self, storage: &Self::Storage) { self.cursor.step_val(storage) }
    #[inline] fn seek_val(&mut self, storage: &Self::Storage, val: &Self::Val) { self.cursor.seek_val(storage, val) }

    #[inline] fn rewind_keys(&mut self, storage: &Self::Storage) { self.cursor.rewind_keys(storage) }
    #[inline] fn rewind_vals(&mut self, storage: &Self::Storage) { self.cursor.rewind_vals(storage) }
}



/// Wrapper to provide cursor to nested scope.
pub struct BatchCursorEnter<B: BatchReader, TInner, F> {
    phantom: ::std::marker::PhantomData<TInner>,
    cursor: B::Cursor,
    logic: F,
}

impl<B: BatchReader, TInner, F> BatchCursorEnter<B, TInner, F> {
    fn new(cursor: B::Cursor, logic: F) -> Self {
        BatchCursorEnter {
            phantom: ::std::marker::PhantomData,
            cursor,
            logic,
        }
    }
}

impl<TInner, B: BatchReader, F> Cursor for BatchCursorEnter<B, TInner, F>
where
    B::Time: Timestamp,
    TInner: Refines<B::Time>+Lattice,
    F: FnMut(&B::Key, &B::Val, &B::Time)->TInner,
{
    type Key = B::Key;
    type Val = B::Val;
    type Time = TInner;
    type R = B::R;

    type Storage = BatchEnter<B, TInner, F>;

    #[inline] fn key_valid(&self, storage: &Self::Storage) -> bool { self.cursor.key_valid(&storage.batch) }
    #[inline] fn val_valid(&self, storage: &Self::Storage) -> bool { self.cursor.val_valid(&storage.batch) }

    #[inline] fn key<'a>(&self, storage: &'a Self::Storage) -> &'a Self::Key { self.cursor.key(&storage.batch) }
    #[inline] fn val<'a>(&self, storage: &'a Self::Storage) -> &'a Self::Val { self.cursor.val(&storage.batch) }

    #[inline]
    fn map_times<L: FnMut(&TInner, &Self::R)>(&mut self, storage: &Self::Storage, mut logic: L) {
        let key = self.key(storage);
        let val = self.val(storage);
        let logic2 = &mut self.logic;
        self.cursor.map_times(&storage.batch, |time, diff| {
            logic(&logic2(key, val, time), diff)
        })
    }

    #[inline] fn step_key(&mut self, storage: &Self::Storage) { self.cursor.step_key(&storage.batch) }
    #[inline] fn seek_key(&mut self, storage: &Self::Storage, key: &Self::Key) { self.cursor.seek_key(&storage.batch, key) }

    #[inline] fn step_val(&mut self, storage: &Self::Storage) { self.cursor.step_val(&storage.batch) }
    #[inline] fn seek_val(&mut self, storage: &Self::Storage, val: &Self::Val) { self.cursor.seek_val(&storage.batch, val) }

    #[inline] fn rewind_keys(&mut self, storage: &Self::Storage) { self.cursor.rewind_keys(&storage.batch) }
    #[inline] fn rewind_vals(&mut self, storage: &Self::Storage) { self.cursor.rewind_vals(&storage.batch) }
}
