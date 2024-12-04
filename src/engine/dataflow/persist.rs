// Copyright © 2024 Pathway
use std::cmp::Reverse;
use std::collections::{hash_map::Entry, BinaryHeap, HashMap};
use std::ops::ControlFlow;
use std::sync::mpsc::{self, TryRecvError};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, SystemTime};

use differential_dataflow::difference::Semigroup;
use differential_dataflow::input::InputSession;
use differential_dataflow::{AsCollection, Collection, ExchangeData};
use log::error;
use ordered_float::OrderedFloat;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::{Capability, Operator};
use timely::dataflow::Scope;
use timely::{order::TotalOrder, progress::Timestamp as TimelyTimestampTrait};

use crate::engine::reduce::IntSumState;
use crate::engine::{Key, Result, Timestamp, Value};
use crate::persistence::config::PersistenceManagerConfig;
use crate::persistence::operator_snapshot::{OperatorSnapshotReader, OperatorSnapshotWriter};
use crate::persistence::tracker::{SharedWorkerPersistentStorage, WorkerPersistentStorage};
use crate::persistence::{PersistenceTime, PersistentId};

use super::maybe_total::MaybeTotalScope;
use super::{shard::Shard, Poller};

pub trait PersistenceWrapper<S>
where
    S: MaybeTotalScope,
{
    fn get_persistence_config(&self) -> Option<&PersistenceManagerConfig>;
    fn get_worker_persistent_storage(&self) -> Option<&SharedWorkerPersistentStorage>;
    fn maybe_persist_named(
        &mut self,
        collection: PersistableCollection<S>,
        name: &str,
        persistent_id: PersistentId,
    ) -> Result<(
        PersistableCollection<S>,
        Option<Poller>,
        Option<std::thread::JoinHandle<()>>,
    )>;
}

pub struct EmptyPersistenceWrapper;

impl<S> PersistenceWrapper<S> for EmptyPersistenceWrapper
where
    S: MaybeTotalScope,
{
    fn get_persistence_config(&self) -> Option<&PersistenceManagerConfig> {
        None
    }

    fn get_worker_persistent_storage(&self) -> Option<&SharedWorkerPersistentStorage> {
        None
    }

    fn maybe_persist_named(
        &mut self,
        collection: PersistableCollection<S>,
        _name: &str,
        _persistent_id: PersistentId,
    ) -> Result<(
        PersistableCollection<S>,
        Option<Poller>,
        Option<std::thread::JoinHandle<()>>,
    )> {
        Ok((collection, None, None))
    }
}

/// Why is `PersistableCollection` needed? We could have generic `maybe_persist_named` instead?
/// Because we have `Box<dyn PersistenceWrapper<S>>` in `DataflowGraphInner`
/// and when `PersistenceWrapper` trait has a generic method, we can't create `Box<dyn PersistenceWrapper<S>>`
/// ("the trait cannot be made into an object" error).

/// And why do we need `PersistenceWrapper` at all?
/// We need it to create operator snapshot writer that uses `Timestamp` type. The operator snapshot writer
/// cannot be generic in `MaybeTotalTimestamp` as the whole persistence uses concrete `Timestamp`, not generic `MaybeTotalTimestamp`. It makes
/// no sense to modify persistence to support generic `MaybeTotalTimestamp`, as it is going to operate on a simple `Timestamp` anyway.
/// The operator snapshot writer cannot be created inside `DataflowGraphInner` as then it would have to be generic
/// because is used in methods that don't have the constraint S: `MaybeTotalScope`<`MaybeTotalTimestamp` = `Timestamp`>.
/// To handle this, operator snapshot writer is created in a separate object (instance of `TimestampBasedPersistenceWrapper`)
/// that is aware that `MaybeTotalTimestamp` = `Timestamp`.

pub enum PersistableCollection<S: MaybeTotalScope> {
    KeyValueIsize(Collection<S, (Key, Value), isize>),
    KeyIntSumState(Collection<S, Key, IntSumState>),
    KeyIsize(Collection<S, Key, isize>),
    KeyOptionOrderderFloatIsize(Collection<S, (Key, Option<OrderedFloat<f64>>), isize>),
    KeyOptionValueIsize(Collection<S, (Key, Option<Value>), isize>),
    KeyOptionValueKeyIsize(Collection<S, (Key, Option<(Value, Key)>), isize>),
    KeyOptionVecValueIsize(Collection<S, (Key, Option<Vec<Value>>), isize>),
    KeyOptionVecOptionValueKeyValue(
        Collection<S, (Key, Option<Vec<(Option<Value>, Key, Value)>>), isize>,
    ),
    KeyOptionKeyValue(Collection<S, (Key, Option<(Key, Value)>), isize>),
}

macro_rules! impl_conversion {
    ($variant:path, $data_type:ty, $diff_type:ty) => {
        impl<S: MaybeTotalScope> From<Collection<S, $data_type, $diff_type>>
            for PersistableCollection<S>
        {
            fn from(c: Collection<S, $data_type, $diff_type>) -> Self {
                $variant(c)
            }
        }

        impl<S: MaybeTotalScope> From<PersistableCollection<S>>
            for Collection<S, $data_type, $diff_type>
        {
            fn from(p: PersistableCollection<S>) -> Self {
                if let $variant(collection) = p {
                    collection
                } else {
                    panic!("Can't convert PersistableCollection back");
                }
            }
        }
    };
}

impl_conversion!(PersistableCollection::KeyValueIsize, (Key, Value), isize);
impl_conversion!(PersistableCollection::KeyIntSumState, Key, IntSumState);
impl_conversion!(PersistableCollection::KeyIsize, Key, isize);
impl_conversion!(
    PersistableCollection::KeyOptionOrderderFloatIsize,
    (Key, Option<OrderedFloat<f64>>),
    isize
);
impl_conversion!(
    PersistableCollection::KeyOptionValueIsize,
    (Key, Option<Value>),
    isize
);
impl_conversion!(
    PersistableCollection::KeyOptionValueKeyIsize,
    (Key, Option<(Value, Key)>),
    isize
);
impl_conversion!(
    PersistableCollection::KeyOptionVecValueIsize,
    (Key, Option<Vec<Value>>),
    isize
);
impl_conversion!(
    PersistableCollection::KeyOptionVecOptionValueKeyValue,
    (Key, Option<Vec<(Option<Value>, Key, Value)>>),
    isize
);
impl_conversion!(
    PersistableCollection::KeyOptionKeyValue,
    (Key, Option<(Key, Value)>),
    isize
);

pub struct TimestampBasedPersistenceWrapper {
    persistence_config: PersistenceManagerConfig,
    worker_persistent_storage: SharedWorkerPersistentStorage,
}

impl TimestampBasedPersistenceWrapper {
    pub fn new(persistence_config: PersistenceManagerConfig) -> Result<Self> {
        let worker_persistent_storage = Arc::new(Mutex::new(WorkerPersistentStorage::new(
            persistence_config.clone(),
        )?));
        Ok(Self {
            persistence_config,
            worker_persistent_storage,
        })
    }

    fn generic_maybe_persist<S, D, R>(
        &mut self,
        collection: &Collection<S, D, R>,
        name: &str,
        persistent_id: PersistentId,
    ) -> Result<(
        PersistableCollection<S>,
        Option<Poller>,
        Option<std::thread::JoinHandle<()>>,
    )>
    where
        S: MaybeTotalScope<MaybeTotalTimestamp = Timestamp>,
        D: ExchangeData + Shard,
        R: ExchangeData + Semigroup,
        Collection<S, D, R>: Into<PersistableCollection<S>>,
    {
        let mut worker_persistent_storage = self.worker_persistent_storage.lock().unwrap();
        let reader = worker_persistent_storage.create_operator_snapshot_reader(persistent_id)?;
        let writer = worker_persistent_storage.create_operator_snapshot_writer(persistent_id)?;
        let (result, poller, thread_handle) = collection.persist_named(name, reader, writer);
        Ok((result.into(), Some(poller), Some(thread_handle)))
    }
}

impl<S: MaybeTotalScope<MaybeTotalTimestamp = Timestamp>> PersistenceWrapper<S>
    for TimestampBasedPersistenceWrapper
{
    fn get_persistence_config(&self) -> Option<&PersistenceManagerConfig> {
        Some(&self.persistence_config)
    }

    fn get_worker_persistent_storage(&self) -> Option<&SharedWorkerPersistentStorage> {
        Some(&self.worker_persistent_storage)
    }

    fn maybe_persist_named(
        &mut self,
        collection: PersistableCollection<S>,
        name: &str,
        persistent_id: PersistentId,
    ) -> Result<(
        PersistableCollection<S>,
        Option<Poller>,
        Option<std::thread::JoinHandle<()>>,
    )> {
        match collection {
            PersistableCollection::KeyValueIsize(collection) => {
                self.generic_maybe_persist(&collection, name, persistent_id)
            }
            PersistableCollection::KeyIntSumState(collection) => {
                self.generic_maybe_persist(&collection, name, persistent_id)
            }
            PersistableCollection::KeyIsize(collection) => {
                self.generic_maybe_persist(&collection, name, persistent_id)
            }
            PersistableCollection::KeyOptionOrderderFloatIsize(collection) => {
                self.generic_maybe_persist(&collection, name, persistent_id)
            }
            PersistableCollection::KeyOptionValueIsize(collection) => {
                self.generic_maybe_persist(&collection, name, persistent_id)
            }
            PersistableCollection::KeyOptionValueKeyIsize(collection) => {
                self.generic_maybe_persist(&collection, name, persistent_id)
            }
            PersistableCollection::KeyOptionVecValueIsize(collection) => {
                self.generic_maybe_persist(&collection, name, persistent_id)
            }
            PersistableCollection::KeyOptionVecOptionValueKeyValue(collection) => {
                self.generic_maybe_persist(&collection, name, persistent_id)
            }
            PersistableCollection::KeyOptionKeyValue(collection) => {
                self.generic_maybe_persist(&collection, name, persistent_id)
            }
        }
    }
}

struct CapabilityOrdWrapper<T: TimelyTimestampTrait + TotalOrder>(Capability<T>);

fn compare_capabilities<T: TimelyTimestampTrait + TotalOrder>(
    a: &CapabilityOrdWrapper<T>,
    b: &CapabilityOrdWrapper<T>,
) -> std::cmp::Ordering {
    a.0.time().cmp(b.0.time())
}

impl<T> PartialEq for CapabilityOrdWrapper<T>
where
    T: TimelyTimestampTrait + TotalOrder,
{
    fn eq(&self, other: &Self) -> bool {
        compare_capabilities(self, other) == std::cmp::Ordering::Equal
    }
}

impl<T> Eq for CapabilityOrdWrapper<T> where T: TimelyTimestampTrait + TotalOrder {}

impl<T> PartialOrd for CapabilityOrdWrapper<T>
where
    T: TimelyTimestampTrait + TotalOrder,
{
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(compare_capabilities(self, other))
    }
}

impl<T> Ord for CapabilityOrdWrapper<T>
where
    T: TimelyTimestampTrait + TotalOrder,
{
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        compare_capabilities(self, other)
    }
}

pub trait Persist<S, D, R>
where
    S: Scope,
    S::Timestamp: TotalOrder,
    R: Semigroup,
{
    fn persist(
        &self,
        reader: Box<dyn OperatorSnapshotReader<D, R> + Send>,
        writer: Arc<Mutex<dyn OperatorSnapshotWriter<S::Timestamp, D, R>>>,
    ) -> (Collection<S, D, R>, Poller, thread::JoinHandle<()>) {
        self.persist_named("persist", reader, writer)
    }

    fn persist_named(
        &self,
        name: &str,
        reader: Box<dyn OperatorSnapshotReader<D, R> + Send>,
        writer: Arc<Mutex<dyn OperatorSnapshotWriter<S::Timestamp, D, R>>>,
    ) -> (Collection<S, D, R>, Poller, thread::JoinHandle<()>);
}

fn read_persisted_state<S, D, R>(
    name: &str,
    mut scope: S,
    reader: Box<dyn OperatorSnapshotReader<D, R> + Send>,
) -> (Collection<S, D, R>, Poller, thread::JoinHandle<()>)
where
    S: Scope,
    S::Timestamp: TotalOrder + PersistenceTime,
    D: ExchangeData,
    R: ExchangeData + Semigroup,
{
    let mut input_session = InputSession::new();
    let state = input_session.to_collection(&mut scope);
    let (sender, receiver) = mpsc::channel();
    let thread_handle = thread::Builder::new()
        .name(name.to_string())
        .spawn(move || {
            let data = reader.load_persisted();
            if let Err(e) = sender.send(data) {
                error!("Failed to send data from persistence: {e}"); // FIXME possibly exit
            }
        })
        .expect("persistence read thread creation should succeed");

    let poller = Box::new(move || {
        input_session.advance_to(S::Timestamp::persistence_time());
        let next_try_at = SystemTime::now() + Duration::from_secs(1);
        match receiver.try_recv() {
            Ok(Ok(data)) => {
                for (val, diff) in data {
                    input_session.update(val, diff);
                }
                ControlFlow::Continue(Some(next_try_at))
            }
            Ok(Err(backend_error)) => {
                error!("Error while reading persisted data: {backend_error}");
                ControlFlow::Continue(Some(next_try_at))
            }
            Err(TryRecvError::Empty) => ControlFlow::Continue(Some(next_try_at)),
            Err(TryRecvError::Disconnected) => {
                input_session.flush();
                ControlFlow::Break(())
            }
        }
    });
    (state, poller, thread_handle)
}

impl<S, D, R> Persist<S, D, R> for Collection<S, D, R>
where
    S: Scope,
    S::Timestamp: TotalOrder + PersistenceTime,
    D: ExchangeData + Shard,
    R: ExchangeData + Semigroup,
{
    fn persist_named(
        &self,
        name: &str,
        reader: Box<dyn OperatorSnapshotReader<D, R> + Send>,
        writer: Arc<Mutex<dyn OperatorSnapshotWriter<S::Timestamp, D, R>>>,
    ) -> (Collection<S, D, R>, Poller, thread::JoinHandle<()>) {
        let (state, poller, thread_handle) = read_persisted_state(name, self.scope(), reader);
        let exchange =
            Exchange::new(move |(data, _time, _diff): &(D, S::Timestamp, R)| data.shard());
        let collection_after_saving = self
            .concat(&state) // concat state before saving to prevent persisting new data before reading from persistence finishes
            .inner
            .unary_frontier(exchange, name, |_capability, _info| {
                // The opreator streams input to output.
                // It holds capabilities, only drop them if the input frontier advances.
                // Without holding the capabities, frontier can advance in output connectors
                // earlier than we persist the data for this frontier.
                let mut data_by_time: HashMap<S::Timestamp, Vec<(D, R)>> = HashMap::new();
                let mut buffer = Vec::new();
                let mut capabilities = BinaryHeap::new();
                let mut times_in_data = BinaryHeap::new();
                move |input, output| {
                    input.for_each(|capability, data| {
                        data.swap(&mut buffer);
                        // preserve input data to persist it later
                        let mut new_times = Vec::new();
                        for (val, time, diff) in &buffer {
                            if *time == S::Timestamp::persistence_time() {
                                continue; // we don't want to save already saved entries
                            }
                            match data_by_time.entry(time.clone()) {
                                Entry::Occupied(mut occupied_entry) => {
                                    occupied_entry.get_mut().push((val.clone(), diff.clone()));
                                }
                                Entry::Vacant(vacant_entry) => {
                                    let vec = vacant_entry.insert(Vec::new());
                                    vec.push((val.clone(), diff.clone()));
                                    times_in_data.push(Reverse(time.clone()));
                                    new_times.push(time.clone());
                                }
                            }
                        }
                        // push input to output
                        output.session(&capability).give_vec(&mut buffer);
                        // preserve capabilities
                        for time in &new_times {
                            capabilities
                                .push(Reverse(CapabilityOrdWrapper(capability.delayed(time))));
                        }
                    });

                    let mut writer_local = writer.lock().unwrap();
                    while !times_in_data.is_empty() {
                        let Reverse(time) = times_in_data.peek().unwrap();
                        // If the input frontier is less or equal to a given time,
                        // we might still get entries with this time in the future.
                        // We want to persist all entries for a given time at once
                        // and we can't persist it now. We break and wait for a next closure call.
                        if input.frontier().less_equal(time) {
                            break;
                        }
                        let data = data_by_time.remove(time).unwrap();
                        let Reverse(time) = times_in_data.pop().unwrap();
                        writer_local.persist(time, data);
                    }

                    while !capabilities.is_empty() {
                        // Holding capabilities gives us a guarantee that we write data
                        // up to time t to persistence earlier than time t is finished
                        // in all output operators. If it was finished in output operators
                        // earlier than we would persist the data, we would advance
                        // the persistence metadata to time t without having all data saved.
                        let Reverse(CapabilityOrdWrapper(c)) = capabilities.peek().unwrap();
                        if input.frontier().less_equal(c.time()) {
                            break;
                        }
                        // We drop the capability. If it was associated with time `t`,
                        // dropping it guarantees that the operator won't produce entries with
                        // time lower or equal to `t` and output frontier can advance.
                        // Note that the capabilities are dropped in order of increasing time.
                        capabilities.pop();
                    }
                }
            })
            .as_collection();

        (collection_after_saving, poller, thread_handle)
    }
}

// arrange in reduce uses DD's Hashable, not our hasher in exchange
