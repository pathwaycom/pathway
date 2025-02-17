// Copyright © 2024 Pathway
use std::collections::HashMap;

use differential_dataflow::collection::AsCollection;
use differential_dataflow::input::InputSession;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::Collection;
use timely::dataflow::operators::input::Handle;
use timely::dataflow::operators::Input as TimelyInput;
use timely::order::TotalOrder;
use timely::progress::Timestamp as TimelyTimestamp;

use crate::engine::dataflow::maybe_total::MaybeTotalScope;
use crate::engine::{Key, Value};

pub type GenericValues<S> = Collection<S, (Key, Value)>;
pub type ValuesSessionAdaptor<Timestamp> = Box<dyn InputAdaptor<Timestamp>>;

#[derive(Clone, Copy, Debug)]
pub enum SessionType {
    Native,
    Upsert,
}

pub trait InputAdaptor<Timestamp> {
    fn new() -> Self
    where
        Self: Sized;

    fn insert(&mut self, key: Key, value: Value);
    fn remove(&mut self, key: Key, value: Value);

    fn advance_to(&mut self, time: Timestamp);
    fn time(&self) -> &Timestamp;

    fn flush(&mut self);
}

#[derive(Default)]
pub struct UpsertSession<Timestamp: TimelyTimestamp + Lattice + TotalOrder> {
    time: Timestamp,
    buffer: Vec<((Key, Value), Timestamp, isize)>,
    handle: Handle<Timestamp, ((Key, Value), Timestamp, isize)>,
}

impl<Timestamp: TimelyTimestamp + Lattice + TotalOrder> UpsertSession<Timestamp> {
    pub fn to_collection<S: MaybeTotalScope<MaybeTotalTimestamp = Timestamp>>(
        &mut self,
        scope: &mut S,
    ) -> Collection<S, (Key, Value)> {
        scope.input_from(&mut self.handle).as_collection()
    }

    fn consolidate_buffer(&mut self) {
        let mut keep: HashMap<Key, (Value, Timestamp, isize)> = HashMap::new();
        for ((key, value), time, diff) in self.buffer.drain(..) {
            keep.insert(key, (value, time, diff));
        }
        self.buffer.extend(
            keep.into_iter()
                .map(|(key, (value, time, diff))| ((key, value), time, diff)),
        );
    }
}

impl<Timestamp: TimelyTimestamp + Lattice + TotalOrder> InputAdaptor<Timestamp>
    for UpsertSession<Timestamp>
{
    /// The implementation below mostly reuses differetial dataflow's `InputSession` internals.
    ///
    /// The main difference is the consolidation of the buffer before flushing.
    /// Without consolidation, if we have multiple entries for a single key,
    /// we may end up with any entry for this key, not necessarily the final one.

    fn new() -> Self {
        let handle: Handle<Timestamp, _> = Handle::new();
        UpsertSession {
            time: handle.time().clone(),
            buffer: Vec::new(),
            handle,
        }
    }

    fn flush(&mut self) {
        self.consolidate_buffer();
        self.handle.send_batch(&mut self.buffer);
        if self.handle.epoch().less_than(&self.time) {
            self.handle.advance_to(self.time.clone());
        }
    }

    fn advance_to(&mut self, time: Timestamp) {
        assert!(self.handle.epoch().less_equal(&time));
        assert!(self.time.less_equal(&time));
        self.time = time;
    }

    fn insert(&mut self, key: Key, value: Value) {
        self.buffer.push(((key, value), self.time.clone(), 1));
    }

    fn remove(&mut self, key: Key, value: Value) {
        assert_eq!(value, Value::Tuple([].into()));
        self.buffer.push(((key, value), self.time.clone(), -1));
    }

    fn time(&self) -> &Timestamp {
        &self.time
    }
}

impl<Timestamp: TimelyTimestamp + Lattice + TotalOrder> Drop for UpsertSession<Timestamp> {
    fn drop(&mut self) {
        self.flush();
    }
}

impl<Timestamp: TimelyTimestamp + Lattice + TotalOrder> InputAdaptor<Timestamp>
    for InputSession<Timestamp, (Key, Value), isize>
{
    fn new() -> Self {
        Self::new()
    }

    fn insert(&mut self, key: Key, value: Value) {
        self.insert((key, value));
    }

    fn remove(&mut self, key: Key, value: Value) {
        self.remove((key, value));
    }

    fn flush(&mut self) {
        self.flush();
    }

    fn advance_to(&mut self, time: Timestamp) {
        self.advance_to(time);
    }

    fn time(&self) -> &Timestamp {
        self.time()
    }
}
