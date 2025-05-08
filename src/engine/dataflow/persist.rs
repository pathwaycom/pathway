// Copyright © 2024 Pathway
use std::cmp::Reverse;
use std::collections::{hash_map::Entry, BinaryHeap, HashMap};
use std::hash::Hash;
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
use serde::{Deserialize, Serialize};
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::{Capability, Filter, Operator};
use timely::dataflow::Scope;
use timely::{order::TotalOrder, progress::Timestamp as TimelyTimestampTrait};

use crate::engine::dataflow::maybe_total::MaybeTotalScope;
use crate::engine::dataflow::operators::stateful_reduce::StatefulReduce;
use crate::engine::dataflow::operators::MapWrapped;
use crate::engine::dataflow::shard::Shard;
use crate::engine::dataflow::{MaybeUpdate, Poller, SortingCell, Tuple};
use crate::engine::reduce::IntSumState;
use crate::engine::{Key, Result, Timestamp, Value};
use crate::persistence::config::PersistenceManagerConfig;
use crate::persistence::operator_snapshot::{OperatorSnapshotReader, OperatorSnapshotWriter};
use crate::persistence::tracker::{
    RequiredPersistenceMode, SharedWorkerPersistentStorage, WorkerPersistentStorage,
};
use crate::persistence::{PersistenceTime, PersistentId, UniqueName};

pub(super) fn effective_persistent_id<S>(
    persistence_wrapper: &mut Box<dyn PersistenceWrapper<S>>,
    reader_is_internal: bool,
    unique_name: Option<&UniqueName>,
    required_persistence_mode: RequiredPersistenceMode,
    logic: impl FnOnce(u64) -> String,
) -> Option<UniqueName>
where
    S: MaybeTotalScope,
{
    let has_persistent_storage = persistence_wrapper
        .get_worker_persistent_storage()
        .is_some();
    if let Some(unique_name) = unique_name {
        if has_persistent_storage {
            Some(unique_name.clone())
        } else {
            None
        }
    } else if has_persistent_storage && !reader_is_internal {
        let next_state_id = persistence_wrapper.next_state_id();
        let worker_persistent_storage = persistence_wrapper
            .get_worker_persistent_storage()
            .unwrap()
            .lock()
            .unwrap();
        if worker_persistent_storage.persistent_id_generation_enabled(required_persistence_mode)
            && worker_persistent_storage.table_persistence_enabled()
        {
            Some(logic(next_state_id))
        } else {
            None
        }
    } else {
        None
    }
}

pub(super) trait PersistenceWrapper<S>
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
    fn filter_out_persisted(
        &self,
        collection: PersistableCollection<S>,
    ) -> PersistableCollection<S>;
    fn maybe_persist_with_logic(
        &self,
        collection: Collection<S, (Key, Value), isize>,
        name: &str,
        persistent_id: Option<PersistentId>,
        logic: Box<
            dyn FnOnce(Collection<S, (Key, OldOrNew<Value, Value>)>) -> Collection<S, (Key, Value)>,
        >,
        purge: Box<dyn Fn(Value) -> Value>,
    ) -> Result<(
        Collection<S, (Key, Value), isize>,
        Option<Poller>,
        Option<std::thread::JoinHandle<()>>,
    )>;
    fn next_state_id(&mut self) -> u64;
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

    fn filter_out_persisted(
        &self,
        collection: PersistableCollection<S>,
    ) -> PersistableCollection<S> {
        collection
    }

    fn next_state_id(&mut self) -> u64 {
        0
    }

    fn maybe_persist_with_logic(
        &self,
        collection: Collection<S, (Key, Value), isize>,
        _name: &str,
        _persistent_id: Option<PersistentId>,
        logic: Box<
            dyn FnOnce(Collection<S, (Key, OldOrNew<Value, Value>)>) -> Collection<S, (Key, Value)>,
        >,
        _purge: Box<dyn Fn(Value) -> Value>,
    ) -> Result<(
        Collection<S, (Key, Value), isize>,
        Option<Poller>,
        Option<std::thread::JoinHandle<()>>,
    )> {
        Ok((
            logic(
                collection.map_named("NoPersist:New", |(key, value)| (key, OldOrNew::New(value))),
            ),
            None,
            None,
        ))
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

pub(super) enum PersistableCollection<S: MaybeTotalScope> {
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
    SortingCellIsize(Collection<S, SortingCell, isize>),
    KeyMaybeUpdateIsize(Collection<S, (Key, MaybeUpdate<Value>), isize>),
    KeyKeyIsize(Collection<S, (Key, Key), isize>),
    KeyKeyValueIsize(Collection<S, (Key, (Key, Value)), isize>),
    KeyIntSumStateIsize(Collection<S, (Key, IntSumState), isize>),
    KeyIsizeIsize(Collection<S, (Key, isize), isize>),
    KeyKeyValueKeyValueIsize(Collection<S, (Key, (Key, Value), (Key, Value)), isize>),
    KeyVecValueIsize(Collection<S, (Key, Vec<Value>), isize>),
    KeyTupleIsize(Collection<S, (Key, Tuple), isize>),
    KeyOptionValueValueIsize(Collection<S, (Key, Option<(Value, Value)>), isize>),
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
impl_conversion!(PersistableCollection::SortingCellIsize, SortingCell, isize);
impl_conversion!(
    PersistableCollection::KeyMaybeUpdateIsize,
    (Key, MaybeUpdate<Value>),
    isize
);
impl_conversion!(PersistableCollection::KeyKeyIsize, (Key, Key), isize);
impl_conversion!(
    PersistableCollection::KeyKeyValueIsize,
    (Key, (Key, Value)),
    isize
);
impl_conversion!(
    PersistableCollection::KeyIntSumStateIsize,
    (Key, IntSumState),
    isize
);
impl_conversion!(PersistableCollection::KeyIsizeIsize, (Key, isize), isize);
impl_conversion!(
    PersistableCollection::KeyKeyValueKeyValueIsize,
    (Key, (Key, Value), (Key, Value)),
    isize
);
impl_conversion!(
    PersistableCollection::KeyVecValueIsize,
    (Key, Vec<Value>),
    isize
);
impl_conversion!(PersistableCollection::KeyTupleIsize, (Key, Tuple), isize);
impl_conversion!(
    PersistableCollection::KeyOptionValueValueIsize,
    (Key, Option<(Value, Value)>),
    isize
);

pub struct TimestampBasedPersistenceWrapper {
    persistence_config: PersistenceManagerConfig,
    worker_persistent_storage: SharedWorkerPersistentStorage,
    persisted_states_count: u64,
}

impl TimestampBasedPersistenceWrapper {
    pub fn new(persistence_config: PersistenceManagerConfig) -> Result<Self> {
        let worker_persistent_storage = Arc::new(Mutex::new(WorkerPersistentStorage::new(
            persistence_config.clone(),
        )?));
        Ok(Self {
            persistence_config,
            worker_persistent_storage,
            persisted_states_count: 0,
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

fn generic_filter_out_persisted<S, D, R>(
    collection: &Collection<S, D, R>,
) -> PersistableCollection<S>
where
    S: MaybeTotalScope<MaybeTotalTimestamp = Timestamp>,
    D: ExchangeData + Shard,
    R: ExchangeData + Semigroup,
    Collection<S, D, R>: Into<PersistableCollection<S>>,
{
    collection
        .inner
        .filter(|(_data, time, _diff)| !time.is_from_persistence())
        .as_collection()
        .into()
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
            PersistableCollection::SortingCellIsize(collection) => {
                self.generic_maybe_persist(&collection, name, persistent_id)
            }
            PersistableCollection::KeyMaybeUpdateIsize(collection) => {
                self.generic_maybe_persist(&collection, name, persistent_id)
            }
            PersistableCollection::KeyKeyIsize(collection) => {
                self.generic_maybe_persist(&collection, name, persistent_id)
            }
            PersistableCollection::KeyKeyValueIsize(collection) => {
                self.generic_maybe_persist(&collection, name, persistent_id)
            }
            PersistableCollection::KeyIntSumStateIsize(collection) => {
                self.generic_maybe_persist(&collection, name, persistent_id)
            }
            PersistableCollection::KeyIsizeIsize(collection) => {
                self.generic_maybe_persist(&collection, name, persistent_id)
            }
            PersistableCollection::KeyKeyValueKeyValueIsize(collection) => {
                self.generic_maybe_persist(&collection, name, persistent_id)
            }
            PersistableCollection::KeyVecValueIsize(collection) => {
                self.generic_maybe_persist(&collection, name, persistent_id)
            }
            PersistableCollection::KeyTupleIsize(collection) => {
                self.generic_maybe_persist(&collection, name, persistent_id)
            }
            PersistableCollection::KeyOptionValueValueIsize(collection) => {
                self.generic_maybe_persist(&collection, name, persistent_id)
            }
        }
    }

    fn filter_out_persisted(
        &self,
        collection: PersistableCollection<S>,
    ) -> PersistableCollection<S> {
        match collection {
            PersistableCollection::KeyValueIsize(collection) => {
                generic_filter_out_persisted(&collection)
            }
            PersistableCollection::KeyIntSumState(collection) => {
                generic_filter_out_persisted(&collection)
            }
            PersistableCollection::KeyIsize(collection) => {
                generic_filter_out_persisted(&collection)
            }
            PersistableCollection::KeyOptionOrderderFloatIsize(collection) => {
                generic_filter_out_persisted(&collection)
            }
            PersistableCollection::KeyOptionValueIsize(collection) => {
                generic_filter_out_persisted(&collection)
            }
            PersistableCollection::KeyOptionValueKeyIsize(collection) => {
                generic_filter_out_persisted(&collection)
            }
            PersistableCollection::KeyOptionVecValueIsize(collection) => {
                generic_filter_out_persisted(&collection)
            }
            PersistableCollection::KeyOptionVecOptionValueKeyValue(collection) => {
                generic_filter_out_persisted(&collection)
            }
            PersistableCollection::KeyOptionKeyValue(collection) => {
                generic_filter_out_persisted(&collection)
            }
            PersistableCollection::SortingCellIsize(collection) => {
                generic_filter_out_persisted(&collection)
            }
            PersistableCollection::KeyMaybeUpdateIsize(collection) => {
                generic_filter_out_persisted(&collection)
            }
            PersistableCollection::KeyKeyIsize(collection) => {
                generic_filter_out_persisted(&collection)
            }
            PersistableCollection::KeyKeyValueIsize(collection) => {
                generic_filter_out_persisted(&collection)
            }
            PersistableCollection::KeyIntSumStateIsize(collection) => {
                generic_filter_out_persisted(&collection)
            }
            PersistableCollection::KeyIsizeIsize(collection) => {
                generic_filter_out_persisted(&collection)
            }
            PersistableCollection::KeyKeyValueKeyValueIsize(collection) => {
                generic_filter_out_persisted(&collection)
            }
            PersistableCollection::KeyVecValueIsize(collection) => {
                generic_filter_out_persisted(&collection)
            }
            PersistableCollection::KeyTupleIsize(collection) => {
                generic_filter_out_persisted(&collection)
            }
            PersistableCollection::KeyOptionValueValueIsize(collection) => {
                generic_filter_out_persisted(&collection)
            }
        }
    }

    fn maybe_persist_with_logic(
        &self,
        collection: Collection<S, (Key, Value), isize>,
        name: &str,
        persistent_id: Option<PersistentId>,
        logic: Box<
            dyn FnOnce(Collection<S, (Key, OldOrNew<Value, Value>)>) -> Collection<S, (Key, Value)>,
        >,
        purge: Box<dyn Fn(Value) -> Value>,
    ) -> Result<(
        Collection<S, (Key, Value), isize>,
        Option<Poller>,
        Option<std::thread::JoinHandle<()>>,
    )> {
        let Some(persistent_id) = persistent_id else {
            return EmptyPersistenceWrapper.maybe_persist_with_logic(
                collection,
                name,
                persistent_id,
                logic,
                purge,
            );
        };
        let mut worker_persistent_storage = self.worker_persistent_storage.lock().unwrap();
        let reader = worker_persistent_storage.create_operator_snapshot_reader(persistent_id)?;
        let writer = worker_persistent_storage.create_operator_snapshot_writer(persistent_id)?;
        let (state, poller, thread_handle) = read_persisted_state(name, collection.scope(), reader);
        let new_data =
            collection.map_named("Persist: New", |(key, value)| (key, OldOrNew::New(value)));
        let state = state.map_named("Persist: Old", |(key, value)| (key, OldOrNew::Old(value)));
        let processed = logic(new_data.concat(&state));
        let collection_after_saving = persist_state(
            &processed,
            &format!("Persist: {name}"),
            writer,
            move |(key, value)| (key, purge(value)),
        );
        Ok((collection_after_saving, Some(poller), Some(thread_handle)))
    }

    fn next_state_id(&mut self) -> u64 {
        self.persisted_states_count += 1;
        self.persisted_states_count
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
    mut reader: Box<dyn OperatorSnapshotReader<D, R> + Send>,
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
                error!("Failed to send data from persistence: {e}");
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
                panic!("Error while reading persisted data: {backend_error}"); // TODO make pollers return Result
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

fn persist_state<S, D, R>(
    collection: &Collection<S, D, R>,
    name: &str,
    writer: Arc<Mutex<dyn OperatorSnapshotWriter<S::Timestamp, D, R>>>,
    purge: impl Fn(D) -> D + 'static,
) -> Collection<S, D, R>
where
    S: Scope,
    S::Timestamp: TotalOrder + PersistenceTime,
    D: ExchangeData + Shard,
    R: ExchangeData + Semigroup,
{
    let exchange = Exchange::new(move |(data, _time, _diff): &(D, S::Timestamp, R)| data.shard());
    collection
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
                        let purged_val = purge(val.clone());
                        match data_by_time.entry(time.clone()) {
                            Entry::Occupied(mut occupied_entry) => {
                                occupied_entry.get_mut().push((purged_val, diff.clone()));
                            }
                            Entry::Vacant(vacant_entry) => {
                                let vec = vacant_entry.insert(Vec::new());
                                vec.push((purged_val, diff.clone()));
                                times_in_data.push(Reverse(time.clone()));
                                new_times.push(time.clone());
                            }
                        }
                    }
                    // push input to output
                    output.session(&capability).give_vec(&mut buffer);
                    // preserve capabilities
                    for time in &new_times {
                        capabilities.push(Reverse(CapabilityOrdWrapper(capability.delayed(time))));
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
        .as_collection()
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
        let collection_after_saving =
            persist_state(&self.concat(&state), name, writer, |data| data);
        (collection_after_saving, poller, thread_handle)
    }
}

// arrange in reduce uses DD's Hashable, not our hasher in exchange

pub trait PersistedStatefulReduce<S, K, V, R>
where
    S: Scope,
    S::Timestamp: TotalOrder,
    R: Semigroup,
{
    fn persisted_stateful_reduce_named<V2>(
        &self,
        name: &str,
        logic: impl FnMut(Option<&V2>, Vec<(V, R)>) -> Option<V2> + 'static,
        reader: Box<dyn OperatorSnapshotReader<(K, V2), R> + Send>,
        writer: Arc<Mutex<dyn OperatorSnapshotWriter<S::Timestamp, (K, V2), R>>>,
    ) -> (Collection<S, (K, V2), R>, Poller, thread::JoinHandle<()>)
    where
        (K, V2): Shard,
        V2: ExchangeData;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum OldOrNew<A, B> {
    Old(A),
    New(B),
}

impl<S, K, V, R> PersistedStatefulReduce<S, K, V, R> for Collection<S, (K, V), R>
where
    S: MaybeTotalScope,
    S::Timestamp: TotalOrder + PersistenceTime,
    K: ExchangeData + Hash + Shard,
    V: ExchangeData,
    R: ExchangeData + Semigroup + From<i8>,
{
    fn persisted_stateful_reduce_named<V2>(
        &self,
        name: &str,
        mut logic: impl FnMut(Option<&V2>, Vec<(V, R)>) -> Option<V2> + 'static,
        reader: Box<dyn OperatorSnapshotReader<(K, V2), R> + Send>,
        writer: Arc<Mutex<dyn OperatorSnapshotWriter<S::Timestamp, (K, V2), R>>>,
    ) -> (Collection<S, (K, V2), R>, Poller, thread::JoinHandle<()>)
    where
        (K, V2): Shard,
        V2: ExchangeData,
    {
        let (state, poller, thread_handle) = read_persisted_state(name, self.scope(), reader);
        let new_data = self.map_named("Persist:New", |(key, value)| (key, OldOrNew::New(value)));
        let state = state.map_named("Persist:Old", |(key, value)| (key, OldOrNew::Old(value)));
        let reduced = new_data
            .concat(&state)
            .stateful_reduce_named(name, move |state, data| {
                let mut old = None;
                let mut new = Vec::with_capacity(data.len());
                for entry in data {
                    match entry {
                        (OldOrNew::Old(entry), diff) => {
                            assert_eq!(diff, R::from(1));
                            assert!(old.is_none());
                            old = Some(entry);
                        }
                        (OldOrNew::New(entry), diff) => new.push((entry, diff)),
                    }
                }
                // If `state` is not None, state from persistence (if any) should be already read in previous iterations.
                // If `state` is None, we check if we can read state from persistence.
                let state = if let Some(state) = state {
                    assert!(old.is_none());
                    Some(state)
                } else {
                    old.as_ref()
                };

                if new.is_empty() {
                    state.cloned()
                } else {
                    logic(state, new)
                }
            });
        let collection_after_saving =
            persist_state(&reduced, &format!("Persist: {name}"), writer, |key_state| {
                key_state
            });
        (collection_after_saving, poller, thread_handle)
    }
}
