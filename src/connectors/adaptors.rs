// Copyright © 2024 Pathway

use differential_dataflow::input::InputSession;
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

pub type GenericValues<S> = Collection<S, (Key, Value)>;
pub type ValuesSessionAdaptor<Timestamp> = Box<dyn InputAdaptor<Timestamp>>;

#[derive(Clone, Copy, Debug)]
pub enum SessionType {
    Native,
    Upsert,
}

impl SessionType {
    pub fn new_collection<
        Timestamp: TimelyTimestamp + Lattice + TotalOrder,
        S: MaybeTotalScope<MaybeTotalTimestamp = Timestamp>,
    >(
        &self,
        scope: &mut S,
    ) -> (ValuesSessionAdaptor<Timestamp>, GenericValues<S>) {
        match &self {
            SessionType::Native => {
                let mut input_session = InputSession::new();
                let collection = input_session.to_collection(scope);
                (Box::new(input_session), collection)
            }
            SessionType::Upsert => {
                let mut upsert_session = UpsertSession::new();
                let collection = upsert_session.to_collection(scope);
                (Box::new(upsert_session), collection)
            }
        }
    }
}

pub trait InputAdaptor<Timestamp> {
    fn new() -> Self
    where
        Self: Sized;

    fn insert(&mut self, key: Key, value: Value);
    fn remove(&mut self, key: Key, value: Value);
    fn upsert(&mut self, key: Key, value: Option<Value>);

    fn advance_to(&mut self, time: Timestamp);
    fn time(&self) -> &Timestamp;

    fn flush(&mut self);
}

#[derive(Default)]
pub struct UpsertSession<Timestamp: TimelyTimestamp + Lattice + TotalOrder> {
    time: Timestamp,
    buffer: Vec<(Key, Option<Value>, Timestamp)>,
    handle: Handle<Timestamp, (Key, Option<Value>, Timestamp)>,
}

impl<Timestamp: TimelyTimestamp + Lattice + TotalOrder> UpsertSession<Timestamp> {
    pub fn to_collection<S: MaybeTotalScope<MaybeTotalTimestamp = Timestamp>>(
        &mut self,
        scope: &mut S,
    ) -> Collection<S, (Key, Value)> {
        // We require that any given key is provided only to a single worker.
        arrange_from_upsert::<S, Spine<Rc<OrdValBatch<Key, Value, Timestamp, isize>>>>(
            &scope.input_from(&mut self.handle),
            "UpsertSession",
        )
        .as_collection(|k, v| (*k, v.clone()))
    }
}

impl<Timestamp: TimelyTimestamp + Lattice + TotalOrder> InputAdaptor<Timestamp>
    for UpsertSession<Timestamp>
{
    /// The implementation below mostly reuses differetial dataflow's `InputSession` internals.
    ///
    /// The main difference is the interface of the `to_collection` method and more task-based
    /// insert and remove methods.

    fn new() -> Self {
        let handle: Handle<Timestamp, _> = Handle::new();
        UpsertSession {
            time: handle.time().clone(),
            buffer: Vec::new(),
            handle,
        }
    }

    fn flush(&mut self) {
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

    fn insert(&mut self, _key: Key, _value: Value) {
        unimplemented!("this type of InputAdaptor doesn't support inserts")
    }

    fn remove(&mut self, _key: Key, _value: Value) {
        unimplemented!("this type of InputAdaptor doesn't support removals")
    }

    fn upsert(&mut self, key: Key, value: Option<Value>) {
        if self.buffer.len() == self.buffer.capacity() {
            if !self.buffer.is_empty() {
                self.handle.send_batch(&mut self.buffer);
            }
            self.buffer.reserve(1024);
        }
        self.buffer.push((key, value, self.time.clone()));
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

    fn upsert(&mut self, _key: Key, _value: Option<Value>) {
        unimplemented!("this type of InputAdaptor doesn't support upserts")
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
