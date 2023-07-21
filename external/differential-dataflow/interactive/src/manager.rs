//! Management of inputs and traces.

use std::collections::HashMap;
use std::hash::Hash;
// use std::time::Duration;

use timely::dataflow::ProbeHandle;
use timely::communication::Allocate;
use timely::worker::Worker;
use timely::logging::TimelyEvent;

// use timely::dataflow::operators::capture::event::EventIterator;

use differential_dataflow::ExchangeData;
use differential_dataflow::trace::implementations::ord::{OrdKeySpine, OrdValSpine};
use differential_dataflow::operators::arrange::TraceAgent;
use differential_dataflow::input::InputSession;

use differential_dataflow::logging::DifferentialEvent;

use crate::{Time, Diff, Plan, Datum};

/// A trace handle for key-only data.
pub type TraceKeyHandle<K, T, R> = TraceAgent<OrdKeySpine<K, T, R>>;
/// A trace handle for key-value data.
pub type TraceValHandle<K, V, T, R> = TraceAgent<OrdValSpine<K, V, T, R>>;
/// A key-only trace handle binding `Time` and `Diff` using `Vec<V>` as data.
pub type KeysOnlyHandle<V> = TraceKeyHandle<Vec<V>, Time, Diff>;
/// A key-value trace handle binding `Time` and `Diff` using `Vec<V>` as data.
pub type KeysValsHandle<V> = TraceValHandle<Vec<V>, Vec<V>, Time, Diff>;

/// Manages inputs and traces.
pub struct Manager<V: ExchangeData+Datum> {
    /// Manages input sessions.
    pub inputs: InputManager<V>,
    /// Manages maintained traces.
    pub traces: TraceManager<V>,
    /// Probes all computations.
    pub probe: ProbeHandle<Time>,
}

impl<V: ExchangeData+Datum> Manager<V>
// where
//     V: ExchangeData+Hash+LoggingValue,
{

    /// Creates a new empty manager.
    pub fn new() -> Self {
        Manager {
            inputs: InputManager::new(),
            traces: TraceManager::new(),
            probe: ProbeHandle::new(),
        }
    }

    // /// Enables logging of timely and differential events.
    // pub fn enable_logging<A: Allocate>(&mut self, worker: &mut Worker<A>) {

    //     use std::rc::Rc;
    //     use timely::dataflow::operators::capture::event::link::EventLink;
    //     use timely::logging::BatchLogger;

    //     let timely_events = Rc::new(EventLink::new());
    //     let differential_events = Rc::new(EventLink::new());

    //     self.publish_timely_logging(worker, Some(timely_events.clone()));
    //     self.publish_differential_logging(worker, Some(differential_events.clone()));

    //     let mut timely_logger = BatchLogger::new(timely_events.clone());
    //     worker
    //         .log_register()
    //         .insert::<TimelyEvent,_>("timely", move |time, data| timely_logger.publish_batch(time, data));

    //     let mut differential_logger = BatchLogger::new(differential_events.clone());
    //     worker
    //         .log_register()
    //         .insert::<DifferentialEvent,_>("differential/arrange", move |time, data| differential_logger.publish_batch(time, data));

    // }

    /// Clear the managed inputs and traces.
    pub fn shutdown<A: Allocate>(&mut self, worker: &mut Worker<A>) {
        self.inputs.sessions.clear();
        self.traces.inputs.clear();
        self.traces.arrangements.clear();

        // Deregister loggers, so that the logging dataflows can shut down.
        worker
            .log_register()
            .insert::<TimelyEvent,_>("timely", move |_time, _data| { });

        worker
            .log_register()
            .insert::<DifferentialEvent,_>("differential/arrange", move |_time, _data| { });
    }

    /// Inserts a new input session by name.
    pub fn insert_input(
        &mut self,
        name: String,
        input: InputSession<Time, Vec<V>, Diff>,
        trace: KeysOnlyHandle<V>)
    {
        self.inputs.sessions.insert(name.clone(), input);
        self.traces.set_unkeyed(&Plan::Source(name), &trace);
    }

    /// Advances inputs and traces to `time`.
    pub fn advance_time(&mut self, time: &Time) {
        self.inputs.advance_time(time);
        self.traces.advance_time(time);
    }

    // /// Timely logging capture and arrangement.
    // pub fn publish_timely_logging<A, I>(&mut self, worker: &mut Worker<A>, events: I)
    // where
    //     A: Allocate,
    //     I : IntoIterator,
    //     <I as IntoIterator>::Item: EventIterator<Duration, (Duration, usize, TimelyEvent)>+'static
    // {
    //     crate::logging::publish_timely_logging(self, worker, 1, "interactive", events)
    // }

    // /// Timely logging capture and arrangement.
    // pub fn publish_differential_logging<A, I>(&mut self, worker: &mut Worker<A>, events: I)
    // where
    //     A: Allocate,
    //     I : IntoIterator,
    //     <I as IntoIterator>::Item: EventIterator<Duration, (Duration, usize, DifferentialEvent)>+'static
    // {
    //     crate::logging::publish_differential_logging(self, worker, 1, "interactive", events)
    // }
}

/// Manages input sessions.
pub struct InputManager<V: ExchangeData> {
    /// Input sessions by name.
    pub sessions: HashMap<String, InputSession<Time, Vec<V>, Diff>>,
}

impl<V: ExchangeData> InputManager<V> {

    /// Creates a new empty input manager.
    pub fn new() -> Self { Self { sessions: HashMap::new() } }

    /// Advances the times of all managed inputs.
    pub fn advance_time(&mut self, time: &Time) {
        for session in self.sessions.values_mut() {
            session.advance_to(time.clone());
            session.flush();
        }
    }

}

/// Root handles to maintained collections.
///
/// Manages a map from plan (describing a collection)
/// to various arranged forms of that collection.
pub struct TraceManager<V: ExchangeData+Datum> {

    /// Arrangements where the record itself is they key.
    ///
    /// This contains both input collections, which are here cached so that
    /// they can be re-used, intermediate collections that are cached, and
    /// any collections that are explicitly published.
    inputs: HashMap<Plan<V>, KeysOnlyHandle<V>>,

    /// Arrangements of collections by key.
    arrangements: HashMap<Plan<V>, HashMap<Vec<usize>, KeysValsHandle<V>>>,

}

impl<V: ExchangeData+Hash+Datum> TraceManager<V> {

    /// Creates a new empty trace manager.
    pub fn new() -> Self {
        Self {
            inputs: HashMap::new(),
            arrangements: HashMap::new()
        }
    }

    /// Advances the frontier of each maintained trace.
    pub fn advance_time(&mut self, time: &Time) {
        use differential_dataflow::trace::TraceReader;
        use timely::progress::frontier::Antichain;
        let frontier = Antichain::from_elem(time.clone());
        for trace in self.inputs.values_mut() {
            trace.set_logical_compaction(frontier.borrow());
            trace.set_physical_compaction(frontier.borrow());
        }
        for map in self.arrangements.values_mut() {
            for trace in map.values_mut() {
                trace.set_logical_compaction(frontier.borrow());
                trace.set_physical_compaction(frontier.borrow());
            }
        }
    }

    /// Recover an arrangement by plan and keys, if it is cached.
    pub fn get_unkeyed(&self, plan: &Plan<V>) -> Option<KeysOnlyHandle<V>> {
        self.inputs
            .get(plan)
            .map(|x| x.clone())
    }

    /// Installs an unkeyed arrangement for a specified plan.
    pub fn set_unkeyed(&mut self, plan: &Plan<V>, handle: &KeysOnlyHandle<V>) {
        self.inputs
            .insert(plan.clone(), handle.clone());
    }

    /// Recover an arrangement by plan and keys, if it is cached.
    pub fn get_keyed(&self, plan: &Plan<V>, keys: &[usize]) -> Option<KeysValsHandle<V>> {
        self.arrangements
            .get(plan)
            .and_then(|map| map.get(keys).map(|x| x.clone()))
    }

    /// Installs a keyed arrangement for a specified plan and sequence of keys.
    pub fn set_keyed(&mut self, plan: &Plan<V>, keys: &[usize], handle: &KeysValsHandle<V>) {
        self.arrangements
            .entry(plan.clone())
            .or_insert(HashMap::new())
            .insert(keys.to_vec(), handle.clone());
    }

}