use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::upsert::arrange_from_upsert;
use differential_dataflow::trace::implementations::ord::OrdValBatch;
use differential_dataflow::trace::implementations::spine_fueled::Spine;
use differential_dataflow::Collection;
use timely::dataflow::operators::input::Handle;
use timely::dataflow::operators::Input as TimelyInput;
use timely::order::TotalOrder;
use timely::progress::Timestamp as TimelyTimestamp;

use crate::engine::dataflow::maybe_total::MaybeTotalScope;
use crate::engine::{Key, Value};

use std::rc::Rc;

pub struct UpsertSession<Timestamp: TimelyTimestamp + Lattice + TotalOrder> {
    time: Timestamp,
    buffer: Vec<(Key, Option<Value>, Timestamp)>,
    handle: Handle<Timestamp, (Key, Option<Value>, Timestamp)>,
}

impl<Timestamp: TimelyTimestamp + Lattice + TotalOrder> UpsertSession<Timestamp> {
    /*
        The implementation below mostly reuses differetial dataflow's InputSession internals.

        The main difference is the interface of the `to_collection` method and more task-based
        insert and remove methods.
    */

    pub fn new() -> Self {
        let handle: Handle<Timestamp, _> = Handle::new();
        UpsertSession {
            time: handle.time().clone(),
            buffer: Vec::new(),
            handle,
        }
    }

    pub fn to_collection<S: MaybeTotalScope<MaybeTotalTimestamp = Timestamp>>(
        &mut self,
        scope: &mut S,
    ) -> Collection<S, (Key, Value)> {
        arrange_from_upsert::<S, Spine<Rc<OrdValBatch<Key, Value, Timestamp, isize>>>>(
            &scope.input_from(&mut self.handle),
            "UpsertSession",
        )
        .as_collection(|k, v| (*k, v.clone()))
    }

    pub fn flush(&mut self) {
        self.handle.send_batch(&mut self.buffer);
        if self.handle.epoch().less_than(&self.time) {
            self.handle.advance_to(self.time.clone());
        }
    }

    pub fn advance_to(&mut self, time: Timestamp) {
        assert!(self.handle.epoch().less_equal(&time));
        assert!(self.time.less_equal(&time));
        self.time = time;
    }

    pub fn update(&mut self, key: Key, value: Option<Value>) {
        if self.buffer.len() == self.buffer.capacity() {
            if !self.buffer.is_empty() {
                self.handle.send_batch(&mut self.buffer);
            }
            self.buffer.reserve(1024);
        }
        self.buffer.push((key, value, self.time.clone()));
    }

    pub fn insert(&mut self, key: Key, value: Value) {
        self.update(key, Some(value));
    }

    pub fn remove(&mut self, key: Key) {
        self.update(key, None);
    }
}

impl<Timestamp: TimelyTimestamp + Lattice + TotalOrder> Drop for UpsertSession<Timestamp> {
    fn drop(&mut self) {
        self.flush();
    }
}

impl<Timestamp: TimelyTimestamp + Lattice + TotalOrder> Default for UpsertSession<Timestamp> {
    fn default() -> Self {
        Self::new()
    }
}
