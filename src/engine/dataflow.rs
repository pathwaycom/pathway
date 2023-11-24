#![allow(clippy::module_name_repetitions)]

mod complex_columns;
pub mod maybe_total;
pub mod operators;
pub mod shard;

use crate::connectors::adaptors::{GenericValues, ValuesSessionAdaptor};
use crate::connectors::data_format::{Formatter, Parser};
use crate::connectors::data_storage::{ReaderBuilder, Writer};
use crate::connectors::monitoring::{ConnectorMonitor, ConnectorStats, OutputConnectorStats};
use crate::connectors::ARTIFICIAL_TIME_ON_REWIND_START;
use crate::connectors::{Connector, ReplayMode, SnapshotAccess};
use crate::engine::dataflow::operators::gradual_broadcast::GradualBroadcast;
use crate::engine::dataflow::operators::time_column::{
    Epsilon, TimeColumnForget, TimeColumnFreeze,
};

use crate::engine::value::HashInto;
use crate::persistence::config::{PersistenceManagerConfig, PersistenceManagerOuterConfig};
use crate::persistence::sync::SharedWorkersPersistenceCoordinator;
use crate::persistence::tracker::SingleWorkerPersistentStorage;
use crate::persistence::{ExternalPersistentId, IntoPersistentId};

use std::any::type_name;
use std::borrow::{Borrow, Cow};
use std::cell::RefCell;
use std::cmp::min;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt::Display;
use std::iter::once;
use std::marker::PhantomData;
use std::ops::{ControlFlow, Deref};
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::rc::Rc;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use std::sync::{mpsc, Arc};
use std::thread::{Builder, JoinHandle};
use std::time::{Duration, SystemTime};
use std::{env, slice};

use arcstr::ArcStr;
use crossbeam_channel::{bounded, never, select, Receiver, RecvError, Sender};
use derivative::Derivative;
use differential_dataflow::collection::concatenate;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::{Arranged, TraceAgent};
use differential_dataflow::operators::iterate::Variable;
use differential_dataflow::operators::reduce::{Reduce, ReduceCore};
use differential_dataflow::operators::{Consolidate, JoinCore};
use differential_dataflow::trace::implementations::ord::{OrdKeySpine, OrdValSpine};
use differential_dataflow::Collection;
use differential_dataflow::{AsCollection as _, Data};
use futures::future::BoxFuture;
use id_arena::Arena;
use itertools::{process_results, Itertools};
use log::{info, warn};
use ndarray::ArrayD;
use once_cell::unsync::{Lazy, OnceCell};
use pyo3::PyObject;
use serde::{Deserialize, Serialize};
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::probe::Handle as ProbeHandle;
use timely::dataflow::operators::ToStream as _;
use timely::dataflow::operators::{Exchange, Filter, Inspect, Operator, Probe};
use timely::dataflow::scopes::Child;
use timely::order::{Product, TotalOrder};
use timely::progress::timestamp::Refines;
use timely::progress::{PathSummary, Timestamp};
use timely::{execute, CommunicationConfig, Config, WorkerConfig};
use xxhash_rust::xxh3::Xxh3 as Hasher;

use self::complex_columns::complex_columns;
use self::maybe_total::{MaybeTotalScope, MaybeTotalTimestamp, NotTotal, Total};
use self::operators::prev_next::add_prev_next_pointers;
use self::operators::stateful_reduce::StatefulReduce;
use self::operators::time_column::TimeColumnBuffer;
use self::operators::{ArrangeWithTypes, MapWrapped};
use self::operators::{ConsolidateForOutput, Reshard};
use self::operators::{ConsolidateForOutputMap, MaybeTotal};
use self::shard::Shard;
use super::error::{DynError, DynResult, Trace};
use super::expression::AnyExpression;
use super::graph::DataRow;
use super::http_server::maybe_run_http_server_thread;
use super::progress_reporter::{maybe_run_reporter, MonitoringLevel};
use super::reduce::{
    AnyReducer, ArgMaxReducer, ArgMinReducer, ArraySumReducer, CountReducer, FloatSumReducer,
    IntSumReducer, MaxReducer, MinReducer, ReducerImpl, SemigroupReducerImpl, SortedTupleReducer,
    StatefulReducer, TupleReducer, UniqueReducer,
};
use super::report_error::{ReportError, ReportErrorExt, SpawnWithReporter, UnwrapWithReporter};
use super::{
    BatchWrapper, ColumnHandle, ColumnPath, ColumnProperties, ComplexColumn, Error, Expression,
    ExpressionData, Graph, IterationLogic, IxKeyPolicy, JoinType, Key, LegacyTable, OperatorStats,
    ProberStats, Reducer, ReducerData, Result, TableHandle, TableProperties, UniverseHandle, Value,
};

pub type WakeupReceiver = Receiver<Box<dyn FnOnce() -> DynResult<()> + Send + Sync + 'static>>;

const MAX_WORKERS: usize = if cfg!(feature = "unlimited-workers") {
    usize::MAX
} else {
    8
};

const YOLO: &[&str] = &[
    #[cfg(feature = "yolo-id32")]
    "id32",
    #[cfg(feature = "yolo-id64")]
    "id64",
];

#[derive(Clone, Debug)]
struct ErrorReporter {
    sender: Sender<Error>,
}

impl ErrorReporter {
    fn create() -> (Self, Receiver<Error>) {
        let (sender, receiver) = bounded(1);
        let reporter = Self { sender };
        (reporter, receiver)
    }
}

impl ReportError for ErrorReporter {
    fn report(&self, error: Error) {
        self.sender.try_send(error).unwrap_or(());
    }
}

type ArrangedBySelf<S, K, R = isize> =
    Arranged<S, TraceAgent<OrdKeySpine<K, <S as MaybeTotalScope>::MaybeTotalTimestamp, R>>>;
type ArrangedByKey<S, K, V, R = isize> =
    Arranged<S, TraceAgent<OrdValSpine<K, V, <S as MaybeTotalScope>::MaybeTotalTimestamp, R>>>;

type Var<S, D, R = isize> = Variable<S, D, R>;

type Keys<S> = Collection<S, Key>;
type KeysArranged<S> = ArrangedBySelf<S, Key>;
type KeysVar<S> = Var<S, Key>;
type ValuesArranged<S> = ArrangedByKey<S, Key, Value>;
type ValuesVar<S> = Var<S, (Key, Value)>;

#[derive(Clone)]
enum Values<S: MaybeTotalScope> {
    Int {
        int_collection: Collection<S, (Key, i64)>,
        generic_collection: OnceCell<Collection<S, (Key, Value)>>,
    },
    Pointer {
        pointer_collection: Collection<S, (Key, Key)>,
        generic_collection: OnceCell<Collection<S, (Key, Value)>>,
    },
    Generic {
        generic_collection: Collection<S, (Key, Value)>,
    },
}

impl<S: MaybeTotalScope> Deref for Values<S> {
    type Target = Collection<S, (Key, Value)>;

    fn deref(&self) -> &Self::Target {
        self.as_generic()
    }
}

impl<S: MaybeTotalScope> Values<S> {
    fn as_generic(&self) -> &Collection<S, (Key, Value)> {
        match self {
            Self::Int {
                int_collection,
                generic_collection,
                ..
            } => generic_collection.get_or_init(|| {
                int_collection.map_named("Values::Int -> generic", |(key, value)| {
                    (key, Value::from(value))
                })
            }),
            Self::Pointer {
                pointer_collection,
                generic_collection,
                ..
            } => generic_collection.get_or_init(|| {
                pointer_collection.map_named("Values::Pointer -> generic", |(key, value)| {
                    (key, Value::from(value))
                })
            }),
            Self::Generic {
                generic_collection, ..
            } => generic_collection,
        }
    }
}

impl<S: MaybeTotalScope> From<Collection<S, (Key, i64)>> for Values<S> {
    fn from(int_collection: Collection<S, (Key, i64)>) -> Self {
        Values::Int {
            int_collection,
            generic_collection: OnceCell::new(),
        }
    }
}

impl<S: MaybeTotalScope> From<Collection<S, (Key, Key)>> for Values<S> {
    fn from(pointer_collection: Collection<S, (Key, Key)>) -> Self {
        Values::Pointer {
            pointer_collection,
            generic_collection: OnceCell::new(),
        }
    }
}

impl<S: MaybeTotalScope> From<Collection<S, (Key, Value)>> for Values<S> {
    fn from(generic_collection: Collection<S, (Key, Value)>) -> Self {
        Values::Generic { generic_collection }
    }
}

type WorkerPersistentStorage = Option<Arc<Mutex<SingleWorkerPersistentStorage>>>;
type GlobalPersistentStorage = Option<SharedWorkersPersistenceCoordinator>;

enum UniverseData<S: MaybeTotalScope> {
    FromCollection {
        collection: Keys<S>,
        arranged: OnceCell<KeysArranged<S>>,
        consolidated: OnceCell<Keys<S>>,
    },
    FromArranged {
        arranged: KeysArranged<S>,
        collection: OnceCell<Keys<S>>,
    },
}

impl<S: MaybeTotalScope> UniverseData<S> {
    fn from_collection(collection: Keys<S>) -> Self {
        let arranged = OnceCell::new();
        let consolidated = OnceCell::new();
        Self::FromCollection {
            collection,
            arranged,
            consolidated,
        }
    }

    fn from_arranged(arranged: KeysArranged<S>) -> Self {
        let collection = OnceCell::new();
        Self::FromArranged {
            collection,
            arranged,
        }
    }

    fn collection(&self) -> &Keys<S> {
        match self {
            Self::FromCollection { collection, .. } => collection,
            Self::FromArranged {
                arranged,
                collection,
            } => collection.get_or_init(|| arranged.as_collection(|k, ()| *k)),
        }
    }

    fn arranged(&self) -> &KeysArranged<S> {
        match self {
            Self::FromArranged { arranged, .. } => arranged,
            Self::FromCollection {
                collection,
                arranged,
                ..
            } => arranged.get_or_init(|| collection.arrange()),
        }
    }

    fn consolidated(&self) -> &Keys<S> {
        match self {
            Self::FromCollection { consolidated, .. } => {
                consolidated.get_or_init(|| self.arranged().as_collection(|k, ()| *k))
            }
            Self::FromArranged { .. } => self.collection(),
        }
    }
}

struct Universe<S: MaybeTotalScope> {
    data: Rc<UniverseData<S>>,
}

impl<S: MaybeTotalScope> Universe<S> {
    fn new(data: Rc<UniverseData<S>>) -> Self {
        Self { data }
    }

    fn from_collection(keys: Keys<S>) -> Self {
        let data = Rc::new(UniverseData::from_collection(keys));
        Self::new(data)
    }

    fn from_arranged(keys: KeysArranged<S>) -> Self {
        let data = Rc::new(UniverseData::from_arranged(keys));
        Self::new(data)
    }

    fn keys(&self) -> &Keys<S> {
        self.data.collection()
    }

    fn keys_arranged(&self) -> &KeysArranged<S> {
        self.data.arranged()
    }

    fn keys_consolidated(&self) -> &Keys<S> {
        self.data.consolidated()
    }
}

enum ColumnData<S: MaybeTotalScope> {
    Collection {
        collection: Values<S>,
        arranged: OnceCell<ValuesArranged<S>>,
        consolidated: OnceCell<Values<S>>,
        keys: OnceCell<Keys<S>>,
    },
    Arranged {
        arranged: ValuesArranged<S>,
        collection: OnceCell<Values<S>>,
        keys: OnceCell<Keys<S>>,
    },
}

impl<S: MaybeTotalScope> ColumnData<S> {
    fn from_collection(collection: Values<S>) -> Self {
        Self::Collection {
            collection,
            arranged: OnceCell::new(),
            consolidated: OnceCell::new(),
            keys: OnceCell::new(),
        }
    }

    fn from_arranged(arranged: ValuesArranged<S>) -> Self {
        Self::Arranged {
            collection: OnceCell::new(),
            arranged,
            keys: OnceCell::new(),
        }
    }

    fn collection(&self) -> &Values<S> {
        match self {
            Self::Collection { collection, .. } => collection,
            Self::Arranged {
                arranged,
                collection,
                ..
            } => collection.get_or_init(|| arranged.as_collection(|k, v| (*k, v.clone())).into()),
        }
    }

    fn arranged(&self) -> &ValuesArranged<S> {
        match self {
            Self::Arranged { arranged, .. } => arranged,
            Self::Collection { arranged, .. } => {
                arranged.get_or_init(|| self.collection().arrange())
            }
        }
    }

    fn keys(&self) -> &Keys<S> {
        match self {
            Self::Collection { keys, .. } | Self::Arranged { keys, .. } => keys.get_or_init(|| {
                self.collection()
                    .map_named("ColumnData -> Keys", |(key, _value)| key)
            }),
        }
    }

    fn keys_arranged(&self) -> KeysArranged<S> {
        self.keys().arrange()
        // FIXME: maybe sth better if it is possible to extract arranged keys from an arranged collection
    }

    fn consolidated(&self) -> &Values<S> {
        match self {
            Self::Collection { consolidated, .. } => consolidated
                .get_or_init(|| self.arranged().as_collection(|k, v| (*k, v.clone())).into()),
            Self::Arranged { .. } => self.collection(),
        }
    }
}

#[derive(Clone)]
struct Column<S: MaybeTotalScope> {
    universe: UniverseHandle,
    data: Rc<ColumnData<S>>,
    properties: Arc<TableProperties>,
}

impl<S: MaybeTotalScope> Column<S> {
    fn from_collection(universe: UniverseHandle, values: impl Into<Values<S>>) -> Self {
        let data = Rc::new(ColumnData::from_collection(values.into()));
        Self {
            universe,
            data,
            properties: Arc::new(TableProperties::Empty),
        }
    }

    fn from_arranged(universe: UniverseHandle, values: ValuesArranged<S>) -> Self {
        let data = Rc::new(ColumnData::from_arranged(values));
        Self {
            universe,
            data,
            properties: Arc::new(TableProperties::Empty),
        }
    }

    fn with_properties(mut self, properties: Arc<TableProperties>) -> Self {
        self.properties = properties;
        self
    }

    fn with_column_properties(mut self, properties: Arc<ColumnProperties>) -> Self {
        self.properties = Arc::new(TableProperties::Column(properties));
        self
    }

    fn values(&self) -> &Values<S> {
        self.data.collection()
    }

    fn values_arranged(&self) -> &ValuesArranged<S> {
        self.data.arranged()
    }

    fn values_consolidated(&self) -> &Values<S> {
        self.data.consolidated()
    }
}

type TableData<S> = ColumnData<S>;

#[derive(Clone)]
struct Table<S: MaybeTotalScope> {
    data: Rc<TableData<S>>,
    properties: Arc<TableProperties>,
}

impl<S: MaybeTotalScope> Table<S> {
    fn from_collection(values: impl Into<Values<S>>) -> Self {
        let data = Rc::new(ColumnData::from_collection(values.into()));
        Self::from_data(data)
    }

    fn with_properties(mut self, table_properties: Arc<TableProperties>) -> Self {
        self.properties = table_properties;
        self
    }

    fn from_data(data: Rc<ColumnData<S>>) -> Self {
        Self {
            data,
            properties: Arc::new(TableProperties::Empty),
        }
    }

    fn from_arranged(values: ValuesArranged<S>) -> Self {
        let data = Rc::new(ColumnData::from_arranged(values));
        Self::from_data(data)
    }

    fn values(&self) -> &Values<S> {
        self.data.collection()
    }

    fn values_arranged(&self) -> &ValuesArranged<S> {
        self.data.arranged()
    }

    fn keys(&self) -> &Keys<S> {
        self.data.keys()
    }

    fn keys_arranged(&self) -> KeysArranged<S> {
        self.data.keys_arranged()
    }
}

struct Prober {
    input_time: Option<u64>,
    input_time_changed: Option<SystemTime>,
    output_time: Option<u64>,
    output_time_changed: Option<SystemTime>,
    intermediate_probes_required: bool,
    run_callback_every_time: bool,
    stats: HashMap<usize, OperatorStats>,
    callback: Box<dyn FnMut(ProberStats)>,
}

impl Prober {
    fn new(
        callback: Box<dyn FnMut(ProberStats)>,
        intermediate_probes_required: bool,
        run_callback_every_time: bool,
    ) -> Self {
        Self {
            input_time: None,
            input_time_changed: None,
            output_time: None,
            output_time_changed: None,
            intermediate_probes_required,
            run_callback_every_time,
            stats: HashMap::new(),
            callback,
        }
    }

    #[allow(clippy::cast_possible_truncation)]
    fn create_stats(probe: &ProbeHandle<u64>, input_time: Option<u64>) -> OperatorStats {
        let frontier = probe.with_frontier(|frontier| frontier.as_option().copied());
        if let Some(timestamp) = frontier {
            OperatorStats {
                time: if timestamp > 0 { Some(timestamp) } else { None },
                lag: input_time.map(|input_time_unwrapped| {
                    if input_time_unwrapped >= timestamp {
                        input_time_unwrapped - timestamp
                    } else {
                        0
                    }
                }),
                done: false,
            }
        } else {
            OperatorStats {
                time: None,
                lag: None,
                done: true,
            }
        }
    }

    #[allow(clippy::cast_possible_truncation)]
    fn update(
        &mut self,
        input_probe: &ProbeHandle<u64>,
        output_probe: &ProbeHandle<u64>,
        intermediate_probes: &HashMap<usize, ProbeHandle<u64>>,
        connector_monitors: &[Rc<RefCell<ConnectorMonitor>>],
    ) {
        let now = Lazy::new(SystemTime::now);

        let mut changed = false;

        let new_input_time = input_probe.with_frontier(|frontier| frontier.as_option().copied());
        if new_input_time != self.input_time {
            self.input_time = new_input_time;
            self.input_time_changed = Some(*now);
            changed = true;
        }

        let new_output_time = output_probe.with_frontier(|frontier| frontier.as_option().copied());
        if new_output_time != self.output_time {
            self.output_time = new_output_time;
            self.output_time_changed = Some(*now);
            changed = true;
        }

        if self.intermediate_probes_required {
            for (id, probe) in intermediate_probes {
                let new_time = probe.with_frontier(|frontier| frontier.as_option().copied());
                let stat = self.stats.get(id);
                if let Some(stat) = stat {
                    if new_time != stat.time {
                        changed = true;
                    }
                } else {
                    changed = true;
                }
            }
        }

        let connector_stats: Vec<(String, ConnectorStats)> = connector_monitors
            .iter()
            .map(|connector_monitor| {
                let monitor = (**connector_monitor).borrow();
                (monitor.get_name(), monitor.get_stats())
            })
            .collect();

        if changed || self.run_callback_every_time {
            if self.intermediate_probes_required {
                for (id, probe) in intermediate_probes {
                    self.stats
                        .insert(*id, Self::create_stats(probe, self.input_time));
                }
            }

            let prober_stats = ProberStats {
                input_stats: Self::create_stats(input_probe, self.input_time),
                output_stats: Self::create_stats(output_probe, self.input_time),
                operators_stats: self.stats.clone(),
                connector_stats,
            };

            (self.callback)(prober_stats);
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash, Serialize, Deserialize)]
struct SortingCell {
    instance: Value,
    key: Value,
    id: Key,
}

impl SortingCell {
    fn new(instance: Value, key: Value, id: Key) -> Self {
        Self { instance, key, id }
    }
}

impl Shard for SortingCell {
    fn shard(&self) -> u64 {
        Key::for_value(&self.instance).shard()
    }
}

struct DataflowGraphInner<S: MaybeTotalScope> {
    scope: S,
    universes: Arena<Universe<S>, UniverseHandle>,
    columns: Arena<Column<S>, ColumnHandle>,
    tables: Arena<Table<S>, TableHandle>,
    pollers: Vec<Box<dyn FnMut() -> ControlFlow<(), Option<SystemTime>>>>,
    connector_threads: Vec<JoinHandle<()>>,
    connector_monitors: Vec<Rc<RefCell<ConnectorMonitor>>>,
    error_reporter: ErrorReporter,
    input_probe: ProbeHandle<S::Timestamp>,
    output_probe: ProbeHandle<S::Timestamp>,
    probers: Vec<Prober>,
    probes: HashMap<usize, ProbeHandle<S::Timestamp>>,
    ignore_asserts: bool,
    persistence_config: Option<PersistenceManagerConfig>,
    worker_persistent_storage: WorkerPersistentStorage,
    global_persistent_storage: GlobalPersistentStorage,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
enum Tuple {
    Zero,
    One(Value),
    Two([Value; 2]),
    More(Arc<[Value]>),
}

impl Deref for Tuple {
    type Target = [Value];

    fn deref(&self) -> &[Value] {
        self.as_value_slice()
    }
}

trait AsValueSlice {
    fn as_value_slice(&self) -> &[Value];

    fn key(&self) -> Key {
        Key::for_values(self.as_value_slice())
    }
}

impl AsValueSlice for () {
    fn as_value_slice(&self) -> &[Value] {
        &[]
    }
}

impl AsValueSlice for Value {
    fn as_value_slice(&self) -> &[Value] {
        slice::from_ref(self)
    }
}

impl<const N: usize> AsValueSlice for [Value; N] {
    fn as_value_slice(&self) -> &[Value] {
        self.as_slice()
    }
}

impl AsValueSlice for Arc<[Value]> {
    fn as_value_slice(&self) -> &[Value] {
        self
    }
}

impl AsValueSlice for Tuple {
    fn as_value_slice(&self) -> &[Value] {
        match self {
            Tuple::Zero => &[],
            Tuple::One(v) => slice::from_ref(v),
            Tuple::Two(vs) => vs,
            Tuple::More(vs) => vs,
        }
    }
}

enum TupleCollection<S: MaybeTotalScope> {
    Zero(Collection<S, Key>),
    One(Collection<S, (Key, Value)>),
    Two(Collection<S, (Key, [Value; 2])>),
    More(Collection<S, (Key, Arc<[Value]>)>),
}

impl<S: MaybeTotalScope> TupleCollection<S> {
    #[track_caller]
    fn map_wrapped_named<D: Data>(
        &self,
        name: &str,
        wrapper: BatchWrapper,
        mut logic: impl FnMut(Key, &[Value]) -> D + 'static,
    ) -> Collection<S, D> {
        match self {
            Self::Zero(c) => {
                c.map_wrapped_named(name, wrapper, move |key| logic(key, ().as_value_slice()))
            }
            Self::One(c) => c.map_wrapped_named(name, wrapper, move |(key, value)| {
                logic(key, value.as_value_slice())
            }),
            Self::Two(c) => c.map_wrapped_named(name, wrapper, move |(key, values)| {
                logic(key, values.as_value_slice())
            }),
            Self::More(c) => c.map_wrapped_named(name, wrapper, move |(key, values)| {
                logic(key, values.as_value_slice())
            }),
        }
    }

    #[track_caller]
    fn as_collection(&self) -> Collection<S, (Key, Tuple)> {
        match self {
            Self::Zero(c) => c.map_named("TupleCollection::as_collection", move |key| {
                (key, Tuple::Zero)
            }),
            Self::One(c) => c.map_named("TupleCollection::as_collection", move |(key, value)| {
                (key, Tuple::One(value))
            }),
            Self::Two(c) => c.map_named("TupleCollection::as_collection", move |(key, values)| {
                (key, Tuple::Two(values))
            }),
            Self::More(c) => c.map_named("TupleCollection::as_collection", move |(key, values)| {
                (key, Tuple::More(values))
            }),
        }
    }
}

#[derive(Derivative, Debug, Clone, Serialize, Deserialize)]
#[derivative(PartialEq, Eq, PartialOrd, Ord, Hash)]
struct KeyWith<T>(
    Key,
    #[derivative(
        PartialEq = "ignore",
        PartialOrd = "ignore",
        Ord = "ignore",
        Hash = "ignore"
    )]
    T,
);

impl<T> Shard for KeyWith<T> {
    fn shard(&self) -> u64 {
        self.0.shard()
    }
}

#[allow(clippy::unnecessary_wraps)] // we want to always return Result for symmetry
impl<S: MaybeTotalScope> DataflowGraphInner<S> {
    fn new(
        scope: S,
        error_reporter: ErrorReporter,
        ignore_asserts: bool,
        persistence_config: Option<PersistenceManagerConfig>,
        global_persistent_storage: Option<SharedWorkersPersistenceCoordinator>,
    ) -> Result<Self> {
        let worker_persistent_storage = {
            if let Some(persistence_config) = &persistence_config {
                let worker_storage = Arc::new(Mutex::new(SingleWorkerPersistentStorage::new(
                    persistence_config.clone(),
                )?));
                global_persistent_storage
                    .as_ref()
                    .expect("inconsistent persistence params")
                    .lock()
                    .unwrap()
                    .register_worker(worker_storage.clone());

                Some(worker_storage)
            } else {
                None
            }
        };

        Ok(Self {
            scope,
            universes: Arena::new(),
            columns: Arena::new(),
            tables: Arena::new(),
            pollers: Vec::new(),
            connector_threads: Vec::new(),
            connector_monitors: Vec::new(),
            error_reporter,
            input_probe: ProbeHandle::new(),
            output_probe: ProbeHandle::new(),
            probers: Vec::new(),
            probes: HashMap::new(),
            ignore_asserts,
            persistence_config,
            worker_persistent_storage,
            global_persistent_storage,
        })
    }

    fn worker_index(&self) -> usize {
        self.scope.index()
    }

    fn worker_count(&self) -> usize {
        self.scope.peers()
    }

    fn empty_universe(&mut self) -> Result<UniverseHandle> {
        self.static_universe(Vec::new())
    }

    fn empty_column(
        &mut self,
        universe_handle: UniverseHandle,
        column_properties: Arc<ColumnProperties>,
    ) -> Result<ColumnHandle> {
        self.static_column(universe_handle, Vec::new(), column_properties)
    }

    #[track_caller]
    fn assert_collections_same_size<V1: Data, V2: Data>(
        &self,
        left: &Collection<S, V1>,
        right: &Collection<S, V2>,
    ) {
        let error_reporter = self.error_reporter.clone();
        left.map_named("assert_collections_same_size::left", |_| ())
            .negate()
            .concat(&right.map_named("assert_collections_same_size::right", |_| ()))
            .consolidate()
            .inspect(move |(_data, _time, diff)| {
                assert_ne!(diff, &0);
                error_reporter.report_and_panic(Error::ValueMissing);
            });
    }

    #[track_caller]
    fn assert_keys_match_values(
        &self,
        keys: &Keys<S>,
        values: impl Deref<Target = Collection<S, (Key, Value)>>,
    ) {
        let error_reporter = self.error_reporter.clone();
        keys.concat(
            &values
                .map_named("assert_keys_match_values", |(k, _)| k)
                .negate(),
        )
        .consolidate()
        .inspect(move |(key, _time, diff)| {
            assert_ne!(diff, &0);
            if diff > &0 {
                error_reporter.report_and_panic(Error::KeyMissingInColumn(*key));
            } else {
                error_reporter.report_and_panic(Error::KeyMissingInUniverse(*key));
            }
        });
    }

    #[track_caller]
    fn assert_keys_match(&self, keys: &Keys<S>, other_keys: &Keys<S>) {
        let error_reporter = self.error_reporter.clone();
        keys.concat(&other_keys.negate())
            .consolidate()
            .inspect(move |(key, _time, diff)| {
                assert_ne!(diff, &0);
                error_reporter.report_and_panic(Error::KeyMissingInUniverse(*key));
            });
    }

    #[track_caller]
    fn assert_keys_are_distinct(&self, keys: &Keys<S>) {
        let error_reporter = self.error_reporter.clone();
        keys.concat(&keys.distinct().negate())
            .consolidate()
            .inspect(move |(key, _time, diff)| {
                assert_ne!(diff, &0);
                error_reporter.report_and_panic(Error::DuplicateKey(*key));
            });
    }

    fn static_universe(&mut self, keys: Vec<Key>) -> Result<UniverseHandle> {
        let worker_count = self.scope.peers();
        let worker_index = self.scope.index();
        let keys = keys
            .into_iter()
            .filter(move |k| k.shard_as_usize() % worker_count == worker_index)
            .map(|k| (k, S::Timestamp::minimum(), 1))
            .to_stream(&mut self.scope)
            .as_collection()
            .probe_with(&mut self.input_probe);
        let universe_handle = self.universes.alloc(Universe::from_collection(keys));
        Ok(universe_handle)
    }

    fn static_column(
        &mut self,
        universe_handle: UniverseHandle,
        values: Vec<(Key, Value)>,
        column_properties: Arc<ColumnProperties>,
    ) -> Result<ColumnHandle> {
        let worker_count = self.scope.peers();
        let worker_index = self.scope.index();
        let universe = self
            .universes
            .get(universe_handle)
            .ok_or(Error::InvalidUniverseHandle)?;
        let values = values
            .into_iter()
            .filter(move |(k, _v)| k.shard_as_usize() % worker_count == worker_index)
            .map(|d| (d, S::Timestamp::minimum(), 1))
            .to_stream(&mut self.scope)
            .as_collection()
            .probe_with(&mut self.input_probe);

        if !self.ignore_asserts {
            // verify the universe
            self.assert_keys_match_values(universe.keys(), &values);
        };

        let column_handle = self.columns.alloc(
            Column::from_collection(universe_handle, values)
                .with_column_properties(column_properties),
        );
        Ok(column_handle)
    }

    fn tuples(
        &mut self,
        universe_handle: UniverseHandle,
        column_handles: &[ColumnHandle],
    ) -> Result<TupleCollection<S>> {
        let universe = self
            .universes
            .get(universe_handle)
            .ok_or(Error::InvalidUniverseHandle)?;
        process_results(
            column_handles
                .iter()
                .map(|c| self.columns.get(*c).ok_or(Error::InvalidColumnHandle)),
            |mut columns| {
                let Some(first_column) = columns.next() else {
                    return Ok(TupleCollection::Zero(universe.keys().clone()));
                };
                let Some(second_column) = columns.next() else {
                    return Ok(TupleCollection::One(
                        first_column.values().as_generic().clone(),
                    ));
                };
                let two = first_column
                    .values_arranged()
                    .join_core(second_column.values_arranged(), |key, first, second| {
                        once((*key, [first.clone(), second.clone()]))
                    });
                let Some(third_column) = columns.next() else {
                    return Ok(TupleCollection::Two(two));
                };
                let two_arranged: ArrangedByKey<S, _, _> = two.arrange();
                let mut more = two_arranged.join_core(
                    third_column.values_arranged(),
                    |key, [first, second], third| {
                        let values: Arc<[Value]> =
                            [first, second, third].into_iter().cloned().collect();
                        once((*key, values))
                    },
                );
                for column in columns {
                    let more_arranged: ArrangedByKey<S, _, _> = more.arrange();
                    more =
                        more_arranged.join_core(column.values_arranged(), |key, values, value| {
                            let new_values: Arc<[Value]> =
                                values.iter().chain([value]).cloned().collect();
                            once((*key, new_values))
                        });
                }
                Ok(TupleCollection::More(more))
            },
        )?
    }

    fn extract_columns(
        &mut self,
        table_handle: TableHandle,
        column_paths: Vec<ColumnPath>,
    ) -> Result<TupleCollection<S>> {
        let table = self
            .tables
            .get(table_handle)
            .ok_or(Error::InvalidTableHandle)?;

        let error_reporter = self.error_reporter.clone();

        let result = table
            .values()
            .map_named("extract_columns::extract", move |(key, values)| {
                let extracted_values: Arc<[Value]> = column_paths
                    .iter()
                    .map(|path| path.extract(&key, &values))
                    .try_collect()
                    .unwrap_with_reporter(&error_reporter);
                (key, extracted_values)
            });
        Ok(TupleCollection::More(result))
    }

    fn expression_column(
        &mut self,
        wrapper: BatchWrapper,
        expression: Arc<Expression>,
        universe_handle: UniverseHandle,
        column_handles: &[ColumnHandle],
        column_properties: Arc<ColumnProperties>,
        trace: Trace,
    ) -> Result<ColumnHandle> where {
        if column_handles.is_empty() {
            let universe = self
                .universes
                .get(universe_handle)
                .ok_or(Error::InvalidUniverseHandle)?;
            let value = wrapper.run(|| expression.eval(&[]))?;
            let values = universe
                .keys()
                .map_named("expression_column::keys_values", move |k| {
                    (k, value.clone())
                });
            let column_handle = self.columns.alloc(
                Column::from_collection(universe_handle, values)
                    .with_column_properties(column_properties),
            );
            return Ok(column_handle);
        }
        if let Expression::Any(AnyExpression::Argument(index)) = &*expression {
            let column_handle = *column_handles.get(*index).ok_or(Error::IndexOutOfBounds)?;
            let column = self
                .columns
                .get(column_handle)
                .ok_or(Error::InvalidColumnHandle)?;
            if column.universe != universe_handle {
                return Err(Error::UniverseMismatch);
            }
            return Ok(column_handle);
        }
        let error_reporter = self.error_reporter.clone();
        let name = format!("Expression {wrapper:?} {expression:?}");
        let new_values = self
            .tuples(universe_handle, column_handles)?
            .map_wrapped_named(&name, wrapper, move |key, values| {
                let result = expression
                    .eval(values)
                    .unwrap_with_reporter_and_trace(&error_reporter, &trace);
                (key, result)
            });

        let new_column_handle = self.columns.alloc(
            Column::from_collection(universe_handle, new_values)
                .with_column_properties(column_properties),
        );
        Ok(new_column_handle)
    }

    fn expression_table(
        &mut self,
        table_handle: TableHandle,
        column_paths: Vec<ColumnPath>,
        expressions: Vec<ExpressionData>,
        wrapper: BatchWrapper,
    ) -> Result<TableHandle> {
        let table = self
            .tables
            .get(table_handle)
            .ok_or(Error::InvalidTableHandle)?;

        let properties: Vec<_> = expressions
            .iter()
            .map(|expression_data| expression_data.properties.as_ref().clone())
            .collect();
        let properties = TableProperties::Table(properties.as_slice().into());

        let error_reporter = self.error_reporter.clone();

        let new_values = table.values().consolidate_for_output().map_wrapped_named(
            "expression_table::evaluate_expression",
            wrapper,
            move |(key, values)| {
                let args: Vec<Value> = column_paths
                    .iter()
                    .map(|path| path.extract(&key, &values))
                    .collect::<Result<_>>()
                    .unwrap_with_reporter(&error_reporter);
                let new_values = expressions.iter().map(|expression_data| {
                    let result = expression_data
                        .expression
                        .eval(&args)
                        .unwrap_with_reporter_and_trace(
                            &error_reporter,
                            expression_data.properties.trace(),
                        );
                    result
                });
                (key, Value::Tuple(new_values.collect()))
            },
        );

        Ok(self
            .tables
            .alloc(Table::from_collection(new_values).with_properties(Arc::new(properties))))
    }

    fn columns_to_table_properties(
        &mut self,
        columns: Vec<(ColumnHandle, ColumnPath)>,
    ) -> Result<TableProperties> {
        let properties: Result<Vec<_>> = columns
            .into_iter()
            .map(|(column_handle, path)| {
                let properties = self
                    .columns
                    .get(column_handle)
                    .ok_or(Error::InvalidColumnHandle)?
                    .properties
                    .clone();
                Ok((path, properties.as_ref().clone()))
            })
            .collect();

        TableProperties::from_paths(properties?)
    }

    fn columns_to_table(
        &mut self,
        universe_handle: UniverseHandle,
        columns: Vec<(ColumnHandle, ColumnPath)>,
    ) -> Result<TableHandle> {
        fn produce_nested_tuple(
            paths: &[(usize, Vec<usize>)],
            depth: usize,
            data: &Arc<[Value]>,
        ) -> Value {
            if !paths.is_empty() && paths.first().unwrap().1.len() == depth {
                let id = paths.first().unwrap().0;
                for (next_id, _path) in &paths[1..] {
                    assert_eq!(data[id], data[*next_id]);
                }
                return data[id].clone();
            }
            let mut path_prefix = 0;
            let mut i = 0;
            let mut j = 0;
            let mut result = Vec::new();
            while i < paths.len() {
                while i < paths.len() && path_prefix == paths[i].1[depth] {
                    i += 1;
                }
                path_prefix += 1;
                if i == j {
                    // XXX: remove after iterate is properly implemented
                    // emulate unused cols
                    result.push(Value::Tuple(Arc::from([])));
                    continue;
                }
                assert!(j < i); //there is at least one entry
                result.push(produce_nested_tuple(&paths[j..i], depth + 1, data));
                j = i;
            }
            Value::from(result.as_slice())
        }
        let column_handles: Vec<ColumnHandle> = columns.iter().map(|(handle, _)| *handle).collect();
        let tuples_collection = self.tuples(universe_handle, &column_handles)?;
        let tuples: Collection<S, (Key, Arc<[Value]>)> = match tuples_collection {
            TupleCollection::Zero(c) => {
                c.map_named("columns_to_table:zero", |key| (key, [].as_slice().into()))
            }
            TupleCollection::One(c) => c.map_named("columns_to_table:one", |(key, value)| {
                (key, [value].as_slice().into())
            }),
            TupleCollection::Two(c) => c.map_named("columns_to_table:two", |(key, values)| {
                (key, values.as_slice().into())
            }),
            TupleCollection::More(c) => c,
        };
        let properties = self.columns_to_table_properties(columns.clone())?;

        let mut paths: Vec<(usize, Vec<usize>)> = columns
            .into_iter()
            .enumerate()
            .map(|(i, (_, path))| match path {
                ColumnPath::ValuePath(path) => Ok((i, path)),
                ColumnPath::Key => Err(Error::ValueError(
                    "It is not allowed to use ids in column to table".into(),
                )),
            })
            .collect::<Result<_>>()?;

        paths.sort_by(|(_, path_i), (_, path_j)| path_i.partial_cmp(path_j).unwrap());

        let table_values = tuples.map_named("columns_to_table:pack", move |(key, values)| {
            (key, produce_nested_tuple(&paths, 0, &values))
        });

        Ok(self
            .tables
            .alloc(Table::from_collection(table_values).with_properties(Arc::new(properties))))
    }

    fn table_column(
        &mut self,
        universe_handle: UniverseHandle,
        table_handle: TableHandle,
        column_path: ColumnPath,
    ) -> Result<ColumnHandle> {
        let table = self
            .tables
            .get(table_handle)
            .ok_or(Error::InvalidTableHandle)?;
        let error_reporter = self.error_reporter.clone();
        let properties = column_path.extract_properties(&table.properties)?;
        let values = table
            .values()
            .map_named("table_column::extract", move |(key, tuple)| {
                (
                    key,
                    column_path
                        .extract(&key, &tuple)
                        .unwrap_with_reporter(&error_reporter),
                )
            });

        let column =
            Column::from_collection(universe_handle, values).with_properties(Arc::new(properties));
        let handle = self.columns.alloc(column);
        Ok(handle)
    }

    fn table_universe(&mut self, table_handle: TableHandle) -> Result<UniverseHandle> {
        let table = self
            .tables
            .get(table_handle)
            .ok_or(Error::InvalidTableHandle)?;

        let universe_handle = self
            .universes
            .alloc(Universe::from_collection(table.keys().clone()));

        Ok(universe_handle)
    }

    fn table_properties(&mut self, table_handle: TableHandle) -> Result<Arc<TableProperties>> {
        let table = self
            .tables
            .get(table_handle)
            .ok_or(Error::InvalidTableHandle)?;
        Ok(table.properties.clone())
    }

    fn flatten_table_storage(
        &mut self,
        table_handle: TableHandle,
        column_paths: Vec<ColumnPath>,
    ) -> Result<TableHandle> {
        let table = self
            .tables
            .get(table_handle)
            .ok_or(Error::InvalidTableHandle)?;
        let properties: Result<Vec<_>> = column_paths
            .iter()
            .map(|path| path.extract_properties(&table.properties))
            .collect();
        let table_values =
            table
                .values()
                .map_named("flatten_table_storage:flatten", move |(key, values)| {
                    let new_values: Arc<[Value]> = column_paths
                        .iter()
                        .map(|path| path.extract(&key, &values).unwrap_or(Value::None))
                        .collect();
                    // FIXME: unwrap_or needed now to support ExternalMaterializedColumns in iterate
                    (key, Value::Tuple(new_values))
                });
        let properties = Arc::new(TableProperties::Table(properties?.as_slice().into()));
        let table_handle = self
            .tables
            .alloc(Table::from_collection(table_values).with_properties(properties));
        Ok(table_handle)
    }

    fn async_apply_table(
        &mut self,
        function: Arc<dyn Fn(Key, &[Value]) -> BoxFuture<'static, DynResult<Value>> + Send + Sync>,
        table_handle: TableHandle,
        column_paths: Vec<ColumnPath>,
        table_properties: Arc<TableProperties>,
        trace: Trace,
    ) -> Result<TableHandle> {
        let table = self
            .tables
            .get(table_handle)
            .ok_or(Error::InvalidTableHandle)?;
        let error_reporter = self.error_reporter.clone();
        let trace = Arc::new(trace);
        let new_values = table.values().map_named_async(
            "expression_column::apply_async",
            move |(key, values)| {
                let args: Vec<Value> = column_paths
                    .iter()
                    .map(|path| path.extract(&key, &values))
                    .collect::<Result<_>>()
                    .unwrap_with_reporter_and_trace(&error_reporter, &trace);
                let error_reporter = error_reporter.clone();
                let function = function.clone();
                let trace = trace.clone();
                let future = async move {
                    let value = async {
                        function(key, &args)
                            .await
                            .unwrap_with_reporter_and_trace(&error_reporter, &trace)
                    }
                    .await;
                    (key, Value::from([value].as_slice()))
                };
                Box::pin(future)
            },
        );
        Ok(self
            .tables
            .alloc(Table::from_collection(new_values).with_properties(table_properties)))
    }

    fn filter_table(
        &mut self,
        table_handle: TableHandle,
        filtering_column_path: ColumnPath,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        let table = self
            .tables
            .get(table_handle)
            .ok_or(Error::InvalidTableHandle)?;

        let error_reporter = self.error_reporter.clone();

        let new_table = table.values().flat_map(move |(key, values)| {
            if filtering_column_path
                .extract(&key, &values)
                .unwrap_with_reporter(&error_reporter)
                .as_bool()
                .unwrap_with_reporter(&error_reporter)
            {
                Some((key, values))
            } else {
                None
            }
        });
        Ok(self
            .tables
            .alloc(Table::from_collection(new_table).with_properties(table_properties)))
    }

    fn freeze(
        &mut self,
        table_handle: TableHandle,
        threshold_time_column_path: ColumnPath,
        current_time_column_path: ColumnPath,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle>
    where
        <S as MaybeTotalScope>::MaybeTotalTimestamp: Timestamp<Summary = <S as MaybeTotalScope>::MaybeTotalTimestamp>
            + PathSummary<<S as MaybeTotalScope>::MaybeTotalTimestamp>
            + Epsilon,
    {
        let table = self
            .tables
            .get(table_handle)
            .ok_or(Error::InvalidTableHandle)?;

        //TODO: report errors
        let _error_reporter = self.error_reporter.clone();
        let gathered = table.values().inner.exchange(|_| 0).as_collection();
        #[allow(clippy::disallowed_methods)]
        let consolidated =
            <Collection<S, (Key, Value)> as differential_dataflow::operators::arrange::Arrange<
                S,
                Key,
                Value,
                isize,
            >>::arrange_core::<
                Pipeline,
                OrdValSpine<Key, Value, <S as MaybeTotalScope>::MaybeTotalTimestamp, isize>,
            >(&gathered, Pipeline, "consolidate_without_shard")
            .consolidate_for_output_map(|k, v| (*k, v.clone()));

        let (on_time, _late) = consolidated.freeze(
            move |val| threshold_time_column_path.extract_from_value(val).unwrap(),
            move |val| current_time_column_path.extract_from_value(val).unwrap(),
        );

        Ok(self
            .tables
            .alloc(Table::from_collection(on_time).with_properties(table_properties)))
    }

    fn buffer(
        &mut self,
        table_handle: TableHandle,
        threshold_time_column_path: ColumnPath,
        current_time_column_path: ColumnPath,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle>
    where
        <S as MaybeTotalScope>::MaybeTotalTimestamp: Timestamp<Summary = <S as MaybeTotalScope>::MaybeTotalTimestamp>
            + PathSummary<<S as MaybeTotalScope>::MaybeTotalTimestamp>
            + Epsilon,
    {
        let table = self
            .tables
            .get(table_handle)
            .ok_or(Error::InvalidTableHandle)?;

        //TODO: report errors
        let _error_reporter = self.error_reporter.clone();

        let new_table = table.values().consolidate_for_output().postpone(
            table.values().scope(),
            move |val| threshold_time_column_path.extract_from_value(val).unwrap(),
            move |val| current_time_column_path.extract_from_value(val).unwrap(),
        );

        Ok(self
            .tables
            .alloc(Table::from_collection(new_table).with_properties(table_properties)))
    }

    fn restrict_column(
        &mut self,
        universe_handle: UniverseHandle,
        column_handle: ColumnHandle,
    ) -> Result<ColumnHandle> {
        let universe = self
            .universes
            .get(universe_handle)
            .ok_or(Error::InvalidUniverseHandle)?;
        let column = self
            .columns
            .get(column_handle)
            .ok_or(Error::InvalidColumnHandle)?;
        if column.universe == universe_handle {
            return Ok(column_handle);
        }
        let new_values = universe
            .keys_arranged()
            .join_core(column.values_arranged(), |k, (), v| once((*k, v.clone())));
        if !self.ignore_asserts {
            self.assert_keys_match_values(universe.keys(), &new_values);
        };
        let new_column_handle = self
            .columns
            .alloc(Column::from_collection(universe_handle, new_values));
        Ok(new_column_handle)
    }

    fn restrict_or_override_table_universe(
        &mut self,
        original_table_handle: TableHandle,
        new_table_handle: TableHandle,
        same_universes: bool,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        let original_table = self
            .tables
            .get(original_table_handle)
            .ok_or(Error::InvalidTableHandle)?;

        let new_table = self
            .tables
            .get(new_table_handle)
            .ok_or(Error::InvalidTableHandle)?;

        let new_values = new_table.values_arranged().join_core(
            original_table.values_arranged(),
            |key, new_values, orig_values| {
                once((
                    *key,
                    Value::from([new_values.clone(), orig_values.clone()].as_slice()),
                ))
            },
        );

        if !self.ignore_asserts {
            self.assert_keys_match_values(new_table.keys(), &new_values);
            if same_universes {
                self.assert_keys_match_values(original_table.keys(), &new_values);
            }
        };

        Ok(self
            .tables
            .alloc(Table::from_collection(new_values).with_properties(table_properties)))
    }

    fn intersect_tables(
        &mut self,
        table_handle: TableHandle,
        other_table_handles: Vec<TableHandle>,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        let table = self
            .tables
            .get(table_handle)
            .ok_or(Error::InvalidTableHandle)?;
        let mut new_values = table.data.clone();
        for other_table_handle in other_table_handles {
            let other_table = self
                .tables
                .get(other_table_handle)
                .ok_or(Error::InvalidTableHandle)?;
            new_values = Rc::new(ColumnData::from_collection(
                new_values
                    .arranged()
                    .join_core(&other_table.keys_arranged(), |k, values, ()| {
                        once((*k, values.clone()))
                    })
                    .into(),
            ));
        }

        Ok(self
            .tables
            .alloc(Table::from_data(new_values).with_properties(table_properties)))
    }

    fn reindex_table(
        &mut self,
        table_handle: TableHandle,
        reindexing_column_path: ColumnPath,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        let table = self
            .tables
            .get(table_handle)
            .ok_or(Error::InvalidTableHandle)?;

        let error_reporter = self.error_reporter.clone();

        let new_values =
            table
                .values()
                .map_named("reindex_table::new_values", move |(key, values)| {
                    let value = reindexing_column_path
                        .extract(&key, &values)
                        .unwrap_with_reporter(&error_reporter);
                    (
                        value.as_pointer().unwrap_with_reporter(&error_reporter),
                        values,
                    )
                });

        if !self.ignore_asserts {
            let new_keys = new_values.map_named("reindex_table:new_keys", |(key, _)| key);
            self.assert_keys_are_distinct(&new_keys);
        };

        Ok(self
            .tables
            .alloc(Table::from_collection(new_values).with_properties(table_properties)))
    }

    fn subtract_table(
        &mut self,
        left_table_handle: TableHandle,
        right_table_handle: TableHandle,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        let left_table = self
            .tables
            .get(left_table_handle)
            .ok_or(Error::InvalidTableHandle)?;
        let right_table = self
            .tables
            .get(right_table_handle)
            .ok_or(Error::InvalidTableHandle)?;

        let intersection = left_table
            .values_arranged()
            .join_core(&right_table.keys_arranged(), |k, values, ()| {
                once((*k, values.clone()))
            });

        let new_values = left_table
            .values()
            .as_generic()
            .concat(&intersection.negate());

        Ok(self
            .tables
            .alloc(Table::from_collection(new_values).with_properties(table_properties)))
    }

    fn concat_tables(
        &mut self,
        table_handles: &[TableHandle],
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        let table_collections: Vec<_> = table_handles
            .iter()
            .map(|handle| {
                let table = self.tables.get(*handle).ok_or(Error::InvalidTableHandle)?;
                Ok(table.values().as_generic().clone())
            })
            .collect::<Result<_>>()?;
        let table = Table::from_collection(concatenate(&mut self.scope, table_collections))
            .with_properties(table_properties);
        if !self.ignore_asserts {
            self.assert_keys_are_distinct(table.keys());
        };
        let table_handle = self.tables.alloc(table);
        Ok(table_handle)
    }

    fn flatten_table(
        &mut self,
        table_handle: TableHandle,
        flatten_column_path: ColumnPath,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        fn flatten_ndarray<T>(array: &ArrayD<T>) -> Vec<Value>
        where
            T: Clone,
            Value: From<T>,
            Value: From<ArrayD<T>>,
        {
            if array.shape().len() == 1 {
                array.iter().map(|x| Value::from(x.clone())).collect()
            } else {
                array
                    .outer_iter()
                    .map(|x| Value::from(x.to_owned()))
                    .collect()
            }
        }

        let table = self
            .tables
            .get(table_handle)
            .ok_or(Error::InvalidTableHandle)?;

        let error_reporter = self.error_reporter.clone();

        let new_table = table.values().flat_map(move |(key, values)| {
            let value = flatten_column_path
                .extract(&key, &values)
                .unwrap_with_reporter(&error_reporter);
            let wrapped = match value {
                Value::IntArray(array) => Ok(flatten_ndarray(&array)),
                Value::FloatArray(array) => Ok(flatten_ndarray(&array)),
                Value::Tuple(array) => Ok((*array).to_vec()),
                Value::String(s) => Ok((*s)
                    .chars()
                    .map(|c| Value::from(ArcStr::from(c.to_string())))
                    .collect()),
                value => Err(Error::ValueError(format!(
                    "Pathway can't flatten this value {value:?}"
                ))),
            }
            .unwrap_with_reporter(&error_reporter);
            wrapped.into_iter().enumerate().map(move |(i, entry)| {
                (
                    Key::for_values(&[Value::from(key), Value::from(i64::try_from(i).unwrap())]),
                    Value::Tuple([values.clone(), entry].into_iter().collect()),
                )
            })
        });
        Ok(self
            .tables
            .alloc(Table::from_collection(new_table).with_properties(table_properties)))
    }

    fn sort_table(
        &mut self,
        table_handle: TableHandle,
        key_column_path: ColumnPath,
        instance_column_path: ColumnPath,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        let table = self
            .tables
            .get(table_handle)
            .ok_or(Error::InvalidTableHandle)?;

        let error_reporter = self.error_reporter.clone();

        let instance_key_id_arranged = table
            .values()
            .map_named(
                "sort_table::instance_key_id_arranged",
                move |(id, values)| {
                    let instance = instance_column_path
                        .extract(&id, &values)
                        .unwrap_with_reporter(&error_reporter);
                    let key = key_column_path
                        .extract(&id, &values)
                        .unwrap_with_reporter(&error_reporter);
                    SortingCell::new(instance, key, id)
                },
            )
            .arrange();

        let prev_next: ArrangedByKey<S, Key, [Value; 2]> =
            add_prev_next_pointers(instance_key_id_arranged, &|a, b| a.instance == b.instance)
                .as_collection(|current, prev_next| {
                    let prev = prev_next
                        .0
                        .clone()
                        .map_or(Value::None, |prev| Value::Pointer(prev.id));
                    let next = prev_next
                        .1
                        .clone()
                        .map_or(Value::None, |next| Value::Pointer(next.id));
                    (current.id, [prev, next])
                })
                .arrange();

        let new_values = table
            .values_arranged()
            .join_core(&prev_next, |key, values, prev_next| {
                once((
                    *key,
                    Value::Tuple(
                        [values.clone()]
                            .into_iter()
                            .chain(prev_next.clone())
                            .collect(),
                    ),
                ))
            });

        Ok(self
            .tables
            .alloc(Table::from_collection(new_values).with_properties(table_properties)))
    }

    fn update_rows_arrange(
        &mut self,
        table_handle: TableHandle,
        update_handle: TableHandle,
    ) -> Result<ArrangedByKey<S, Key, (bool, Value)>> {
        let table = self
            .tables
            .get(table_handle)
            .ok_or(Error::InvalidTableHandle)?;
        let update = self
            .tables
            .get(update_handle)
            .ok_or(Error::InvalidTableHandle)?;

        Ok(table
            .values()
            .map_named("update_rows_arrange::table", |(k, v)| (k, (false, v)))
            .concat(
                &update
                    .values()
                    .map_named("update_rows_arrange::update", |(k, v)| (k, (true, v))),
            )
            .arrange_named("update_rows_arrange::both"))
    }

    fn update_rows_table(
        &mut self,
        table_handle: TableHandle,
        update_handle: TableHandle,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        let both_arranged = self.update_rows_arrange(table_handle, update_handle)?;

        let updated_values = both_arranged.reduce_abelian(
            "update_rows_table::updated",
            move |_key, input, output| {
                let ((_is_right, value), _count) = input.last().unwrap();
                output.push((value.clone(), 1));
            },
        );

        Ok(self
            .tables
            .alloc(Table::from_arranged(updated_values).with_properties(table_properties)))
    }

    fn update_cells_table(
        &mut self,
        table_handle: TableHandle,
        update_handle: TableHandle,
        column_paths: Vec<ColumnPath>,
        update_paths: Vec<ColumnPath>,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        let both_arranged = self.update_rows_arrange(table_handle, update_handle)?;

        let error_reporter = self.error_reporter.clone();

        let updated_values = both_arranged.reduce_abelian(
            "update_cells_table::updated",
            move |key, input, output| {
                let ((_is_right, values), _count) = input[0];
                let maybe_update: Vec<_> = match input.get(1) {
                    Some(((_is_right, updates), _count)) => update_paths
                        .iter()
                        .map(|path| path.extract(key, updates))
                        .collect::<Result<_>>(),
                    None => column_paths
                        .iter()
                        .map(|path| path.extract(key, values))
                        .collect::<Result<_>>(),
                }
                .unwrap_with_reporter(&error_reporter);

                let result =
                    Value::Tuple([values.clone()].into_iter().chain(maybe_update).collect());
                output.push((result, 1));
            },
        );

        Ok(self
            .tables
            .alloc(Table::from_arranged(updated_values).with_properties(table_properties)))
    }

    fn gradual_broadcast(
        &mut self,
        input_table_handle: TableHandle,
        threshold_table_handle: TableHandle,
        lower_path: ColumnPath,
        value_path: ColumnPath,
        upper_path: ColumnPath,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        let table = self
            .tables
            .get(input_table_handle)
            .ok_or(Error::InvalidTableHandle)?;
        let threshold_table = self
            .tables
            .get(threshold_table_handle)
            .ok_or(Error::InvalidTableHandle)?;
        let error_reporter = self.error_reporter.clone();
        let threshold_collection_to_process = threshold_table.values().map_named(
            "trim to lower, value, upper",
            move |(id, values)| {
                let lower = lower_path
                    .extract(&id, &values)
                    .unwrap_with_reporter(&error_reporter)
                    .as_ordered_float()
                    .unwrap_with_reporter(&error_reporter);

                let value = value_path
                    .extract(&id, &values)
                    .unwrap_with_reporter(&error_reporter)
                    .as_ordered_float()
                    .unwrap_with_reporter(&error_reporter);

                let upper = upper_path
                    .extract(&id, &values)
                    .unwrap_with_reporter(&error_reporter)
                    .as_ordered_float()
                    .unwrap_with_reporter(&error_reporter);

                (id, (lower, value, upper))
            },
        );

        let new_values = table
            .values()
            .as_generic()
            .gradual_broadcast(&threshold_collection_to_process)
            .map_named(
                "wrap broadcast result into value",
                move |(id, (old_values, new_cell))| {
                    (
                        id,
                        Value::Tuple(Arc::from([old_values, Value::from(new_cell)])),
                    )
                },
            );
        Ok(self
            .tables
            .alloc(Table::from_collection(new_values).with_properties(table_properties)))
    }

    fn ix_table(
        &mut self,
        to_ix_handle: TableHandle,
        key_handle: TableHandle,
        key_column_path: ColumnPath,
        ix_key_policy: IxKeyPolicy,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        let to_ix_table = self
            .tables
            .get(to_ix_handle)
            .ok_or(Error::InvalidTableHandle)?;
        let key_table = self
            .tables
            .get(key_handle)
            .ok_or(Error::InvalidTableHandle)?;

        let error_reporter = self.error_reporter.clone();

        let key_table_extracted =
            key_table
                .values()
                .map_named("ix_table extracting key values", move |(key, values)| {
                    let value = key_column_path
                        .extract(&key, &values)
                        .unwrap_with_reporter(&error_reporter);
                    (key, (values, value))
                });

        let error_reporter = self.error_reporter.clone();
        let values_to_keys_arranged: ArrangedByKey<S, Key, (Key, Value)> = match ix_key_policy {
            IxKeyPolicy::FailMissing => key_table_extracted.map_named(
                "ix_table unwrapping pointers",
                move |(key, (values, value))| {
                    let pointer = value.as_pointer().unwrap_with_reporter(&error_reporter);
                    (pointer, (key, values))
                },
            ),
            _ => key_table_extracted.flat_map(move |(key, (values, value))| {
                if value == Value::None {
                    None
                } else {
                    let pointer = value.as_pointer().unwrap_with_reporter(&error_reporter);
                    Some((pointer, (key, values)))
                }
            }),
        }
        .arrange();
        let new_table = match ix_key_policy {
            IxKeyPolicy::SkipMissing => values_to_keys_arranged.join_core(
                to_ix_table.values_arranged(),
                |_source_key, (result_key, _result_row), to_ix_row| {
                    once((*result_key, to_ix_row.clone()))
                },
            ),
            _ => values_to_keys_arranged.join_core(
                to_ix_table.values_arranged(),
                |_source_key, (result_key, result_row), to_ix_row| {
                    once((
                        *result_key,
                        Value::from([result_row.clone(), to_ix_row.clone()].as_slice()),
                    ))
                },
            ),
        };
        let new_table = match ix_key_policy {
            IxKeyPolicy::ForwardNone => {
                let none_keys =
                    key_table_extracted.flat_map(move |(key, (values, value))| match value {
                        Value::None => Some((key, Value::from([values, Value::None].as_slice()))),
                        _ => None,
                    });
                new_table.concat(&none_keys)
            }
            _ => new_table,
        };
        if !self.ignore_asserts && ix_key_policy != IxKeyPolicy::SkipMissing {
            self.assert_collections_same_size(key_table.values(), &new_table);
        };

        Ok(self
            .tables
            .alloc(Table::from_collection(new_table).with_properties(table_properties)))
    }

    #[allow(clippy::too_many_lines)]
    fn join_tables(
        &mut self,
        left_table_handle: TableHandle,
        right_table_handle: TableHandle,
        left_column_paths: Vec<ColumnPath>,
        right_column_paths: Vec<ColumnPath>,
        join_type: JoinType,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        let left_table = self
            .tables
            .get(left_table_handle)
            .ok_or(Error::InvalidTableHandle)?;
        let right_table = self
            .tables
            .get(right_table_handle)
            .ok_or(Error::InvalidTableHandle)?;

        let error_reporter_left = self.error_reporter.clone();
        let error_reporter_right = self.error_reporter.clone();

        let join_left =
            left_table
                .values()
                .map_named("join_tables::join_left", move |(key, values)| {
                    let join_key_parts: Vec<_> = left_column_paths
                        .iter()
                        .map(|path| path.extract(&key, &values))
                        .collect::<Result<_>>()
                        .unwrap_with_reporter(&error_reporter_left);
                    let join_key = Key::for_values(&join_key_parts);
                    (join_key, (key, values))
                });
        let join_left_arranged: ArrangedByKey<S, Key, (Key, Value)> = join_left.arrange();
        let join_right =
            right_table
                .values()
                .map_named("join_tables::join_right", move |(key, values)| {
                    let join_key_parts: Vec<_> = right_column_paths
                        .iter()
                        .map(|path| path.extract(&key, &values))
                        .collect::<Result<_>>()
                        .unwrap_with_reporter(&error_reporter_right);
                    let join_key = Key::for_values(&join_key_parts);
                    (join_key, (key, values))
                });
        let join_right_arranged: ArrangedByKey<S, Key, (Key, Value)> = join_right.arrange();

        let join_left_right = join_left_arranged
            .join_core(&join_right_arranged, |join_key, left_key, right_key| {
                once((*join_key, left_key.clone(), right_key.clone()))
            });

        let left_right_to_result_fn = match join_type {
            JoinType::LeftKeysFull | JoinType::LeftKeysSubset => |(left_key, _right_key)| left_key,
            _ => |(left_key, right_key)| {
                Key::for_values(&[Value::from(left_key), Value::from(right_key)])
            },
        };
        let result_left_right = join_left_right.map_named(
            "join::result_left_right",
            move |(_join_key, (left_key, left_values), (right_key, right_values))| {
                (
                    left_right_to_result_fn((left_key, right_key)),
                    Value::from(
                        [
                            Value::Pointer(left_key),
                            left_values,
                            Value::Pointer(right_key),
                            right_values,
                        ]
                        .as_slice(),
                    ),
                )
            },
        );

        let left_outer = || {
            join_left
                .map_named(
                    "join_tables::left_outer_left",
                    |(_join_key, left_key_values)| left_key_values,
                )
                .concat(
                    &join_left_right
                        .map_named(
                            "join::left_outer_res",
                            |(_join_key, left_key_values, _right_key_values)| left_key_values,
                        )
                        .distinct()
                        .negate(),
                )
        };
        let result_left_outer = match join_type {
            JoinType::LeftOuter | JoinType::FullOuter => Some(left_outer().map_named(
                "join::result_left_outer",
                |(left_key, left_values)| {
                    let result_key = Key::for_values(&[Value::from(left_key), Value::None]);
                    (left_key, left_values, result_key)
                },
            )),
            JoinType::LeftKeysFull => Some(
                left_outer().map_named("join::result_left_outer", |(left_key, left_values)| {
                    (left_key, left_values, left_key)
                }),
            ),
            _ => None,
        }
        .map(|result_left_outer| {
            result_left_outer.map_named(
                "join::result_left_outer_reorder",
                |(left_key, left_values, result_key)| {
                    (
                        result_key,
                        Value::from(
                            [
                                Value::Pointer(left_key),
                                left_values,
                                Value::None,
                                Value::None,
                            ]
                            .as_slice(),
                        ),
                    )
                },
            )
        });
        let result_left_right = if let Some(result_left_outer) = result_left_outer {
            result_left_right.concat(&result_left_outer)
        } else {
            result_left_right
        };

        let right_outer = || {
            join_right
                .map_named(
                    "join::right_outer_right",
                    |(_join_key, right_key_values)| right_key_values,
                )
                .concat(
                    &join_left_right
                        .map_named(
                            "join::right_outer_res",
                            |(_join_key, _left_key, right_key_values)| right_key_values,
                        )
                        .distinct()
                        .negate(),
                )
        };
        let result_right_outer = match join_type {
            JoinType::RightOuter | JoinType::FullOuter => Some(right_outer().map_named(
                "join::right_result_outer",
                |(right_key, right_values)| {
                    let result_key = Key::for_values(&[Value::None, Value::from(right_key)]);
                    (
                        result_key,
                        Value::from(
                            [
                                Value::None,
                                Value::None,
                                Value::Pointer(right_key),
                                right_values,
                            ]
                            .as_slice(),
                        ),
                    )
                },
            )),
            _ => None,
        };
        let result_left_right = if let Some(result_right_outer) = result_right_outer {
            result_left_right.concat(&result_right_outer)
        } else {
            result_left_right
        };

        let result_table =
            Table::from_collection(result_left_right).with_properties(table_properties);

        match join_type {
            JoinType::LeftKeysFull => {
                self.assert_keys_match(left_table.keys(), result_table.keys());
            }
            JoinType::LeftKeysSubset => self.assert_keys_are_distinct(result_table.keys()),
            _ => {}
        }

        Ok(self.tables.alloc(result_table))
    }

    fn complex_columns(&mut self, inputs: Vec<ComplexColumn>) -> Result<Vec<ColumnHandle>> {
        complex_columns(self, inputs)
    }

    fn debug_universe(&self, tag: String, table_handle: TableHandle) -> Result<()> {
        let worker = self.scope.index();
        let table = self
            .tables
            .get(table_handle)
            .ok_or(Error::InvalidTableHandle)?;
        println!("[{worker}][{tag}] {table_handle:?}");
        table.keys().inspect(move |(key, time, diff)| {
            println!("[{worker}][{tag}] @{time:?} {diff:+} {key}");
        });
        Ok(())
    }

    fn debug_column(
        &self,
        tag: String,
        table_handle: TableHandle,
        column_path: ColumnPath,
    ) -> Result<()> {
        let worker = self.scope.index();
        let table = self
            .tables
            .get(table_handle)
            .ok_or(Error::InvalidTableHandle)?;
        let error_reporter = self.error_reporter.clone();
        println!("[{worker}][{tag}] {table_handle:?} {column_path:?}");
        table
            .values()
            .map_named("debug_column", move |(key, values)| {
                (
                    key,
                    column_path
                        .extract(&key, &values)
                        .unwrap_with_reporter(&error_reporter),
                )
            })
            .inspect(move |((key, value), time, diff)| {
                println!("[{worker}][{tag}] @{time:?} {diff:+} {key} {value}");
            });
        Ok(())
    }

    fn probe_table(&mut self, table_handle: TableHandle, operator_id: usize) -> Result<()> {
        let table = self
            .tables
            .get(table_handle)
            .ok_or(Error::InvalidTableHandle)?;
        table
            .values()
            .probe_with(self.probes.entry(operator_id).or_insert(ProbeHandle::new()));
        Ok(())
    }
}

trait DataflowReducer<S: MaybeTotalScope> {
    fn reduce(self: Rc<Self>, values: &Collection<S, (Key, Key, Vec<Value>)>) -> Values<S>;
}

impl<S: MaybeTotalScope, R: ReducerImpl> DataflowReducer<S> for R {
    fn reduce(self: Rc<Self>, values: &Collection<S, (Key, Key, Vec<Value>)>) -> Values<S> {
        let initialized = values.map_named("DataFlowReducer::reduce::init", {
            let self_ = self.clone();
            move |(source_key, result_key, values)| {
                let state = self_.init(&source_key, &values).unwrap_or_else(|| {
                    panic!(
                        "{reducer_type}::init() failed for {values:?} of key {source_key:?}",
                        reducer_type = type_name::<R>()
                    )
                }); // XXX
                (result_key, state)
            }
        });

        initialized
            .reduce({
                let self_ = self.clone();
                move |_key, input, output| {
                    let result = self_.combine(input.iter().map(|&(state, cnt)| {
                        (state, usize::try_from(cnt).unwrap().try_into().unwrap())
                    }));
                    output.push((result, 1));
                }
            })
            .map_named("DataFlowReducer::reduce", move |(key, state)| {
                (key, self.finish(state))
            })
            .into()
    }
}

impl<S: MaybeTotalScope> DataflowReducer<S> for IntSumReducer {
    fn reduce(self: Rc<Self>, values: &Collection<S, (Key, Key, Vec<Value>)>) -> Values<S> {
        values
            .map_named("IntSumReducer::reduce::init", {
                let self_ = self.clone();
                move |(source_key, result_key, values)| {
                    let state = self_.init(&source_key, &values[0]).unwrap_or_else(|| {
                        panic!(
                            "{reducer_type}::init() failed for {values:?} of key {source_key:?}",
                            reducer_type = "IntSumReducer"
                        )
                    }); // XXX
                    (result_key, state)
                }
            })
            .explode(|(key, state)| once((key, state)))
            .count()
            .map_named("IntSumReducer::reduce", move |(key, state)| {
                (key, self.finish(state))
            })
            .into()
    }
}

impl<S: MaybeTotalScope> DataflowReducer<S> for CountReducer {
    fn reduce(self: Rc<Self>, values: &Collection<S, (Key, Key, Vec<Value>)>) -> Values<S> {
        values
            .map_named(
                "CountReducer::reduce::init",
                |(_source_key, result_key, _values)| (result_key),
            )
            .count()
            .map_named("CountReducer::reduce", |(key, count)| {
                (key, Value::from(count as i64))
            })
            .into()
    }
}

impl<S: MaybeTotalScope> DataflowReducer<S> for StatefulReducer
where
    S::MaybeTotalTimestamp: TotalOrder,
{
    fn reduce(self: Rc<Self>, values: &Collection<S, (Key, Key, Vec<Value>)>) -> Values<S> {
        values
            .map_named(
                "StatefulReducer::reduce::init",
                |(_source_key, result_key, values)| (result_key, values),
            )
            .stateful_reduce_named("StatefulReducer::reduce::reduce", move |state, values| {
                self.combine(state, values)
            })
            .into()
    }
}

trait CreateDataflowReducer<S: MaybeTotalScope> {
    fn create_dataflow_reducer(reducer: &Reducer) -> Result<Rc<dyn DataflowReducer<S>>>;
}

impl<S> CreateDataflowReducer<S> for NotTotal
where
    S: MaybeTotalScope,
{
    fn create_dataflow_reducer(reducer: &Reducer) -> Result<Rc<dyn DataflowReducer<S>>> {
        let res: Rc<dyn DataflowReducer<S>> = match reducer {
            Reducer::Count => Rc::new(CountReducer),
            Reducer::FloatSum => Rc::new(FloatSumReducer),
            Reducer::IntSum => Rc::new(IntSumReducer),
            Reducer::ArraySum => Rc::new(ArraySumReducer),
            Reducer::Unique => Rc::new(UniqueReducer),
            Reducer::Min => Rc::new(MinReducer),
            Reducer::ArgMin => Rc::new(ArgMinReducer),
            Reducer::Max => Rc::new(MaxReducer),
            Reducer::ArgMax => Rc::new(ArgMaxReducer),
            Reducer::SortedTuple { skip_nones } => Rc::new(SortedTupleReducer::new(*skip_nones)),
            Reducer::Tuple { skip_nones } => Rc::new(TupleReducer::new(*skip_nones)),

            Reducer::Any => Rc::new(AnyReducer),
            Reducer::Stateful { .. } => return Err(Error::NotSupportedInIteration),
        };

        Ok(res)
    }
}

impl<S> CreateDataflowReducer<S> for Total
where
    S: MaybeTotalScope,
    S::Timestamp: TotalOrder,
{
    fn create_dataflow_reducer(reducer: &Reducer) -> Result<Rc<dyn DataflowReducer<S>>> {
        let res: Rc<dyn DataflowReducer<S>> = match reducer {
            Reducer::Stateful { combine_fn } => Rc::new(StatefulReducer::new(combine_fn.clone())),
            other => NotTotal::create_dataflow_reducer(other)?,
        };

        Ok(res)
    }
}

impl<S: MaybeTotalScope> DataflowGraphInner<S>
where
    <S::MaybeTotalTimestamp as MaybeTotalTimestamp>::IsTotal: CreateDataflowReducer<S>,
{
    fn group_by_table(
        &mut self,
        table_handle: TableHandle,
        grouping_columns_paths: Vec<ColumnPath>,
        reducers: Vec<ReducerData>,
        set_id: bool,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        if set_id {
            assert!(grouping_columns_paths.len() == 1);
        }
        let table = self
            .tables
            .get(table_handle)
            .ok_or(Error::InvalidTableHandle)?;

        let error_reporter_1 = self.error_reporter.clone();
        let reducer_impls: Vec<_> = reducers
            .iter()
            .map(|reducer_data| {
                <S::MaybeTotalTimestamp as MaybeTotalTimestamp>::IsTotal::create_dataflow_reducer(
                    &reducer_data.reducer,
                )
            })
            .try_collect()?;

        let with_new_key =
            table
                .values()
                .map_named("group_by_table::new_key", move |(key, values)| {
                    let new_key_parts: Vec<_> = grouping_columns_paths
                        .iter()
                        .map(|path| path.extract(&key, &values))
                        .collect::<Result<_>>()
                        .unwrap_with_reporter(&error_reporter_1);
                    let new_key = if set_id {
                        new_key_parts
                            .first()
                            .unwrap()
                            .as_pointer()
                            .unwrap_with_reporter(&error_reporter_1)
                    } else {
                        Key::for_values(&new_key_parts)
                    };
                    (key, new_key, values)
                });

        let reduced_columns: Vec<_> = reducer_impls
            .iter()
            .zip(reducers)
            .map(|(reducer_impl, data)| {
                let error_reporter_2 = self.error_reporter.clone();
                let with_extracted_value = with_new_key.map_named(
                    "group_by_table::reducers",
                    move |(key, new_key, values)| {
                        (
                            key,
                            new_key,
                            data.column_paths
                                .iter()
                                .map(|path| path.extract(&key, &values))
                                .try_collect()
                                .unwrap_with_reporter(&error_reporter_2),
                        )
                    },
                );
                reducer_impl.clone().reduce(&with_extracted_value)
            })
            .collect();
        let new_values = if let Some(first) = reduced_columns.first() {
            let mut joined: Collection<S, (Key, Arc<[Value]>)> = first
                .map_named("group_by_table::join", |(key, value)| {
                    (key, Arc::from([value].as_slice()))
                });
            for column in reduced_columns.iter().skip(1) {
                let joined_arranged: ArrangedByKey<S, Key, Arc<[Value]>> = joined.arrange();
                let column_arranged: ArrangedByKey<S, Key, Value> = column.arrange();
                joined = joined_arranged.join_core(&column_arranged, |key, values, value| {
                    let new_values: Arc<[Value]> = values.iter().chain([value]).cloned().collect();
                    once((*key, new_values))
                });
            }
            joined.map_named("group_by_table::wrap", |(key, values)| {
                (key, Value::Tuple(values))
            })
        } else {
            with_new_key
                .map_named("group_by_table::empty", |(_key, new_key, _values)| {
                    (new_key, Value::Tuple(Arc::from([])))
                })
                .distinct()
        };
        Ok(self
            .tables
            .alloc(Table::from_collection(new_values).with_properties(table_properties)))
    }
}

#[derive(Debug, Clone)]
enum OutputEvent {
    Commit(Option<u64>),
    Batch(Vec<((Key, Tuple), u64, isize)>),
}

#[allow(clippy::unnecessary_wraps)] // we want to always return Result for symmetry
impl<S: MaybeTotalScope<MaybeTotalTimestamp = u64>> DataflowGraphInner<S> {
    fn empty_table(&mut self, table_properties: Arc<TableProperties>) -> Result<TableHandle> {
        self.static_table(Vec::new(), table_properties)
    }

    fn static_table(
        &mut self,
        values: Vec<DataRow>,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        let worker_count = self.scope.peers();
        let worker_index = self.scope.index();
        let values = values
            .into_iter()
            .filter(move |row| {
                row.shard.unwrap_or_else(|| row.key.shard_as_usize()) % worker_count == worker_index
            })
            .map(|row| {
                assert!(row.diff == 1 || row.diff == -1);
                (
                    (row.key, Value::from(row.values.as_slice())),
                    row.time,
                    row.diff,
                )
            })
            .to_stream(&mut self.scope)
            .as_collection()
            .probe_with(&mut self.input_probe);

        Ok(self
            .tables
            .alloc(Table::from_collection(values).with_properties(table_properties)))
    }

    fn connector_table(
        &mut self,
        mut reader: Box<dyn ReaderBuilder>,
        parser: Box<dyn Parser>,
        commit_duration: Option<Duration>,
        parallel_readers: usize,
        table_properties: Arc<TableProperties>,
        external_persistent_id: Option<&ExternalPersistentId>,
    ) -> Result<TableHandle> {
        let has_persistent_storage = self.worker_persistent_storage.is_some();
        if let Some(external_persistent_id) = external_persistent_id {
            if !has_persistent_storage {
                return Err(Error::NoPersistentStorage(external_persistent_id.clone()));
            }
        }

        let effective_persistent_id = {
            if external_persistent_id.is_some() {
                external_persistent_id.cloned()
            } else if has_persistent_storage && !reader.is_internal() {
                let generated_external_id = reader.name(None, self.connector_monitors.len());
                reader
                    .update_persistent_id(Some(generated_external_id.clone().into_persistent_id()));
                info!(
                    "Persistent ID autogenerated for a {:?} reader: {generated_external_id}",
                    reader.storage_type()
                );
                Some(generated_external_id)
            } else {
                None
            }
        };

        let (input_session, table_values): (ValuesSessionAdaptor<S::Timestamp>, GenericValues<S>) =
            parser.session_type().new_collection(&mut self.scope);

        let table_values = table_values.reshard();
        table_values.probe_with(&mut self.input_probe);

        let realtime_reader_needed = self.scope.index() < parallel_readers
            && self
                .persistence_config
                .as_ref()
                .map_or(true, |config| config.continue_after_replay);
        let persisted_table =
            reader.persistent_id().is_some() && self.worker_persistent_storage.is_some();

        if realtime_reader_needed || persisted_table {
            let persistent_id = reader.persistent_id();
            let reader_storage_type = reader.storage_type();
            let replay_mode = self
                .persistence_config
                .as_ref()
                .map_or(ReplayMode::Batch, |config| config.replay_mode);
            let snapshot_access = self
                .persistence_config
                .as_ref()
                .map_or(SnapshotAccess::Full, |config| config.snapshot_access);

            let connector = Connector::<S::Timestamp>::new(commit_duration, parser.column_count());
            let state = connector.run(
                reader,
                parser,
                input_session,
                move |values, offset| match values {
                    None => {
                        let (offset_key, offset_value) =
                            offset.expect("offset is required for key generation");
                        let mut hasher = Hasher::default();
                        offset_key.hash_into(&mut hasher);
                        offset_value.hash_into(&mut hasher);
                        Key::from_hasher(&hasher)
                    }
                    Some(values) => {
                        // do not hash again if this is pointer already
                        if values.len() == 1 {
                            if let Value::Pointer(key) = values[0] {
                                return key;
                            }
                        }
                        Key::for_values(values)
                    }
                },
                self.output_probe.clone(),
                self.worker_persistent_storage.clone(),
                self.connector_monitors.len(),
                realtime_reader_needed,
                effective_persistent_id.as_ref(),
                replay_mode,
                snapshot_access,
                self.error_reporter.clone(),
            )?;

            self.pollers.push(state.poller);
            self.connector_threads.push(state.input_thread_handle);
            if let Some(persistent_id) = persistent_id {
                // If there is a persistent id, there's also a persistent storage
                // It is checked in the beginning of the method
                self.worker_persistent_storage
                    .as_ref()
                    .unwrap()
                    .lock()
                    .unwrap()
                    .register_input_source(
                        persistent_id,
                        &reader_storage_type,
                        state.offsets_by_time,
                    );
            }
            self.connector_monitors.push(state.connector_monitor);
        }

        Ok(self
            .tables
            .alloc(Table::from_collection(table_values).with_properties(table_properties)))
    }

    fn forget(
        &mut self,
        table_handle: TableHandle,
        threshold_time_column_path: ColumnPath,
        current_time_column_path: ColumnPath,
        mark_forgetting_records: bool,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        let table = self
            .tables
            .get(table_handle)
            .ok_or(Error::InvalidTableHandle)?;

        let error_reporter_1 = self.error_reporter.clone();
        let error_reporter_2 = self.error_reporter.clone();

        let new_table = table.values().consolidate_for_output().forget(
            move |val| {
                threshold_time_column_path
                    .extract_from_value(val)
                    .unwrap_with_reporter(&error_reporter_1)
            },
            move |val| {
                current_time_column_path
                    .extract_from_value(val)
                    .unwrap_with_reporter(&error_reporter_2)
            },
            mark_forgetting_records,
        );

        Ok(self
            .tables
            .alloc(Table::from_collection(new_table).with_properties(table_properties)))
    }

    fn forget_immediately(
        &mut self,
        table_handle: TableHandle,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        let table = self
            .tables
            .get(table_handle)
            .ok_or(Error::InvalidTableHandle)?;

        let forgetting_stream = table.values().negate().delay(|time| {
            // can be called on times other than appearing in records
            time + 1 // produce neu times
        });
        let new_table = table
            .values()
            .inner
            .inspect(|(_data, time, _diff)| {
                assert!(time % 2 == 0, "Neu time encountered at forget() input.");
            })
            .as_collection()
            .concat(&forgetting_stream);

        Ok(self
            .tables
            .alloc(Table::from_collection(new_table).with_properties(table_properties)))
    }

    fn filter_out_results_of_forgetting(
        &mut self,
        table_handle: TableHandle,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        let table = self
            .tables
            .get(table_handle)
            .ok_or(Error::InvalidTableHandle)?;
        let new_table = table
            .values()
            .inner
            .filter(|(_data, time, _diff)| time % 2 == 0)
            .as_collection();
        Ok(self
            .tables
            .alloc(Table::from_collection(new_table).with_properties(table_properties)))
    }

    fn output_batch(
        stats: &mut OutputConnectorStats,
        batch: Vec<((Key, Tuple), u64, isize)>,
        data_sink: &mut Box<dyn Writer>,
        data_formatter: &mut Box<dyn Formatter>,
        global_persistent_storage: &GlobalPersistentStorage,
    ) -> Result<(), DynError> {
        stats.on_batch_started();
        for ((key, values), time, diff) in batch {
            if time == ARTIFICIAL_TIME_ON_REWIND_START && global_persistent_storage.is_some() {
                // Ignore entries, which had been written before
                continue;
            }
            let formatted = data_formatter
                .format(&key, &values, time, diff)
                .map_err(DynError::from)?;
            data_sink.write(formatted).map_err(DynError::from)?;
            stats.on_batch_entry_written();
        }
        stats.on_batch_finished();
        data_sink.flush().map_err(DynError::from)?;

        Ok(())
    }

    fn commit_output_time(
        stats: &mut OutputConnectorStats,
        t: Option<u64>,
        worker_index: usize,
        sink_id: Option<usize>,
        global_persistent_storage: &GlobalPersistentStorage,
    ) {
        if let Some(global_persistent_storage) = &global_persistent_storage {
            global_persistent_storage
                .lock()
                .unwrap()
                .accept_finalized_timestamp(
                    worker_index,
                    sink_id.expect("undefined sink_id while using persistent storage"),
                    t,
                );
        }
        stats.on_time_committed(t);
    }

    fn output_table(
        &mut self,
        mut data_sink: Box<dyn Writer>,
        mut data_formatter: Box<dyn Formatter>,
        table_handle: TableHandle,
        column_paths: Vec<ColumnPath>,
    ) -> Result<()> {
        let output_columns = self
            .extract_columns(table_handle, column_paths)?
            .as_collection();
        let single_threaded = data_sink.single_threaded();

        let output = output_columns.consolidate_for_output().inner;
        let inspect_output = {
            if single_threaded {
                output.exchange(|_| 0)
            } else {
                output
            }
        };

        let worker_index = self.scope.index();
        let sender = if !single_threaded || worker_index == 0 {
            let (sender, receiver) = mpsc::channel();

            let thread_name = format!(
                "pathway:output_table-{}-{}",
                data_sink.short_description(),
                data_formatter.short_description()
            );

            let global_persistent_storage = self.global_persistent_storage.clone();
            let sink_id = self
                .worker_persistent_storage
                .as_ref()
                .map(|storage| storage.lock().unwrap().register_sink());

            // connector_threads vector contains both, input and output connector threads
            // connector_monitors vector contains monitors only for input connectors
            let output_connector_id = self.connector_threads.len() - self.connector_monitors.len();
            let mut stats = OutputConnectorStats::new(data_sink.name(output_connector_id));

            let output_joiner_handle = Builder::new()
                .name(thread_name)
                .spawn_with_reporter(
                    self.error_reporter.clone().with_extra(receiver),
                    move |error_reporter_with_receiver| loop {
                        let receiver = error_reporter_with_receiver.get();
                        match receiver.recv() {
                            Ok(OutputEvent::Batch(batch)) => {
                                Self::output_batch(
                                    &mut stats,
                                    batch,
                                    &mut data_sink,
                                    &mut data_formatter,
                                    &global_persistent_storage,
                                )?;
                            }
                            Ok(OutputEvent::Commit(t)) => {
                                Self::commit_output_time(
                                    &mut stats,
                                    t,
                                    worker_index,
                                    sink_id,
                                    &global_persistent_storage,
                                );
                                if t.is_none() {
                                    break Ok(());
                                }
                            }
                            Err(mpsc::RecvError) => break Ok(()),
                        }
                    },
                )
                .expect("output thread creation failed");
            self.connector_threads.push(output_joiner_handle);
            Some(sender)
        } else {
            None
        };

        inspect_output
            .inspect_core(move |event| match sender {
                None => {
                    // There is no connector thread for this worker.
                    // We shouldn't be getting any data, but we can get frontier updates.
                    match event {
                        Ok((_time, [])) => {}
                        Err(_frontier) => {}
                        _ => panic!("got data in a worker that is not doing output"),
                    }
                }
                Some(ref sender) => {
                    let event = match event {
                        Ok((_time, data)) => OutputEvent::Batch(data.to_vec()),
                        Err(frontier) => {
                            assert!(frontier.len() <= 1);
                            OutputEvent::Commit(frontier.first().copied())
                        }
                    };
                    sender
                        .send(event)
                        .expect("sending output event should not fail");
                }
            })
            .probe_with(&mut self.output_probe);

        Ok(())
    }

    fn subscribe_table(
        &mut self,
        wrapper: BatchWrapper,
        mut callback: Box<dyn FnMut(Key, &[Value], u64, isize) -> DynResult<()>>,
        mut on_end: Box<dyn FnMut() -> DynResult<()>>,
        table_handle: TableHandle,
        column_paths: Vec<ColumnPath>,
        skip_persisted_batch: bool,
    ) -> Result<()> {
        let mut vector = Vec::new();

        let error_reporter = self.error_reporter.clone();
        let worker_index = self.scope.index();

        let sink_id = self
            .worker_persistent_storage
            .as_ref()
            .map(|m| m.lock().unwrap().register_sink());
        let global_persistent_storage = self.global_persistent_storage.clone();
        let skip_initial_time = skip_persisted_batch && global_persistent_storage.is_some();
        self.extract_columns(table_handle, column_paths)?
            .as_collection()
            .consolidate_for_output()
            .inner
            .probe_with(&mut self.output_probe)
            .sink(Pipeline, "SubscribeColumn", move |input| {
                wrapper.run(|| {
                    while let Some((_time, data)) = input.next() {
                        data.swap(&mut vector);
                        for ((key, values), time, diff) in vector.drain(..) {
                            if time == ARTIFICIAL_TIME_ON_REWIND_START && skip_initial_time {
                                continue;
                            }
                            callback(key, &values, time, diff)
                                .unwrap_with_reporter(&error_reporter);
                        }
                    }

                    let time_processed = input.frontier().frontier().first().copied();
                    if let Some(global_persistent_storage) = &global_persistent_storage {
                        global_persistent_storage
                            .lock()
                            .unwrap()
                            .accept_finalized_timestamp(
                                worker_index,
                                sink_id.expect("undefined sink_id while using persistent storage"),
                                time_processed,
                            );
                    }

                    if time_processed.is_none() {
                        on_end().unwrap_with_reporter(&error_reporter);
                    }
                });
            });

        Ok(())
    }

    fn iterate<'a>(
        &'a mut self,
        iterated: Vec<LegacyTable>,
        iterated_with_universe: Vec<LegacyTable>,
        extra: Vec<LegacyTable>,
        limit: Option<u32>,
        logic: IterationLogic<'a>,
    ) -> Result<(Vec<LegacyTable>, Vec<LegacyTable>)> {
        let mut scope = self.scope.clone();
        if let Some(v) = limit {
            if v <= 1 {
                return Err(Error::IterationLimitTooSmall);
            }
        }
        scope.iterative::<u32, _, _>(|subscope| {
            let step = Product::new(Default::default(), 1);
            let subgraph = InnerDataflowGraph::new(
                subscope.clone(),
                self.error_reporter.clone(),
                self.ignore_asserts,
                self.persistence_config.clone(),
                self.global_persistent_storage.clone(),
            )?;
            let mut subgraph_ref = subgraph.0.borrow_mut();
            let mut state = BeforeIterate::new(self, &mut subgraph_ref, step);
            let inner_iterated: Vec<IteratedLegacyTable<_, _>> = state.create_tables(iterated)?;
            let inner_iterated_with_universe: Vec<IteratedWithUniverseLegacyTable<_, _>> =
                state.create_tables(iterated_with_universe)?;
            let inner_extra: Vec<ExtraLegacyTable<_, _>> = state.create_tables(extra)?;
            drop(subgraph_ref);
            let iterated_handles = extract_handles(inner_iterated.iter());
            let iterated_with_universe_handles =
                extract_handles(inner_iterated_with_universe.iter());
            let extra_handles = extract_handles(inner_extra);
            let (result, result_with_universe) = logic(
                &subgraph,
                iterated_handles,
                iterated_with_universe_handles,
                extra_handles,
            )?;
            let subgraph_ref = subgraph.0.borrow();
            let mut state = AfterIterate::new(self, &subgraph_ref, limit);
            let result = result
                .into_iter()
                .zip_longest(inner_iterated)
                .map(|element| {
                    let ((universe_handle, column_handles), inner_table) =
                        element.both().ok_or(Error::LengthMismatch)?;
                    inner_table.finish(&mut state, universe_handle, column_handles)
                })
                .collect::<Result<_>>()?;
            let result_with_universe = result_with_universe
                .into_iter()
                .zip_longest(inner_iterated_with_universe)
                .map(|element| {
                    let ((universe_handle, column_handles), inner_table) =
                        element.both().ok_or(Error::LengthMismatch)?;
                    inner_table.finish(&mut state, universe_handle, column_handles)
                })
                .collect::<Result<_>>()?;
            Ok((result, result_with_universe))
        })
    }

    fn attach_prober(
        &mut self,
        logic: Box<dyn FnMut(ProberStats)>,
        intermediate_probes_required: bool,
        run_callback_every_time: bool,
    ) -> Result<()> {
        self.probers.push(Prober::new(
            logic,
            intermediate_probes_required,
            run_callback_every_time,
        ));
        Ok(())
    }
}

trait InnerUniverse {
    type Outer: MaybeTotalScope;
    type Inner: MaybeTotalScope;

    fn create(
        state: &mut BeforeIterate<Self::Outer, Self::Inner>,
        outer_handle: UniverseHandle,
    ) -> Result<Self>
    where
        Self: Sized;

    fn finish(
        self,
        state: &mut AfterIterate<Self::Outer, Self::Inner>,
        inner_handle: UniverseHandle,
    ) -> Result<UniverseHandle>;

    fn inner_handle(&self) -> UniverseHandle;
    fn outer_handle(&self) -> UniverseHandle;
}

struct ImportedUniverse<O, I> {
    outer: PhantomData<*mut O>,
    inner: PhantomData<*mut I>,
    outer_handle: UniverseHandle,
    inner_handle: UniverseHandle,
}

impl<'c, S: MaybeTotalScope, T> InnerUniverse for ImportedUniverse<S, Child<'c, S, T>>
where
    T: Refines<S::MaybeTotalTimestamp> + Lattice,
    Child<'c, S, T>: MaybeTotalScope,
{
    type Outer = S;
    type Inner = Child<'c, S, T>;

    fn create(
        state: &mut BeforeIterate<Self::Outer, Self::Inner>,
        outer_handle: UniverseHandle,
    ) -> Result<Self> {
        let inner_handle = match state.universe_cache.entry(outer_handle) {
            Entry::Occupied(o) => *o.get(),
            Entry::Vacant(v) => {
                let universe = state
                    .outer
                    .universes
                    .get(*v.key())
                    .ok_or(Error::InvalidUniverseHandle)?;
                let new_keys = universe.keys().enter(&state.inner.scope);
                // TODO: import the arrangement
                let new_universe_handle = state
                    .inner
                    .universes
                    .alloc(Universe::from_collection(new_keys));
                *v.insert(new_universe_handle)
            }
        };

        Ok(Self {
            outer: PhantomData,
            inner: PhantomData,
            outer_handle,
            inner_handle,
        })
    }

    fn finish(
        self,
        _state: &mut AfterIterate<Self::Outer, Self::Inner>,
        inner_handle: UniverseHandle,
    ) -> Result<UniverseHandle> {
        if self.inner_handle != inner_handle {
            return Err(Error::UniverseMismatch);
        }
        Ok(self.outer_handle)
    }

    fn inner_handle(&self) -> UniverseHandle {
        self.inner_handle
    }

    fn outer_handle(&self) -> UniverseHandle {
        self.outer_handle
    }
}

struct IteratedUniverse<O, I: MaybeTotalScope> {
    outer: PhantomData<*mut O>,
    outer_handle: UniverseHandle,
    inner_handle: UniverseHandle,
    keys_var: KeysVar<I>,
}

impl<'c, S: MaybeTotalScope> InnerUniverse
    for IteratedUniverse<S, Child<'c, S, Product<S::Timestamp, u32>>>
{
    type Outer = S;
    type Inner = Child<'c, S, Product<S::Timestamp, u32>>;

    fn create(
        state: &mut BeforeIterate<Self::Outer, Self::Inner>,
        outer_handle: UniverseHandle,
    ) -> Result<Self> {
        let universe = state
            .outer
            .universes
            .get(outer_handle)
            .ok_or(Error::InvalidUniverseHandle)?;
        let keys_var = Variable::new_from(
            universe.keys().enter(&state.inner.scope),
            state.step.clone(),
        );
        let inner_handle = state
            .inner
            .universes
            .alloc(Universe::from_collection(keys_var.clone()));

        Ok(Self {
            outer: PhantomData,
            outer_handle,
            inner_handle,
            keys_var,
        })
    }

    fn inner_handle(&self) -> UniverseHandle {
        self.inner_handle
    }

    fn outer_handle(&self) -> UniverseHandle {
        self.outer_handle
    }

    fn finish(
        self,
        state: &mut AfterIterate<Self::Outer, Self::Inner>,
        inner_handle: UniverseHandle,
    ) -> Result<UniverseHandle> {
        let universe = state
            .inner
            .universes
            .get(inner_handle)
            .ok_or(Error::InvalidUniverseHandle)?;
        let keys = universe.keys_consolidated();
        self.keys_var.set(&state.apply_limit(keys));
        // arrange consolidates the output
        let outer_handle = state
            .outer
            .universes
            .alloc(Universe::from_arranged(keys.leave().arrange()));
        Ok(outer_handle)
    }
}

trait InnerColumn {
    type Outer: MaybeTotalScope;
    type Inner: MaybeTotalScope;

    fn create(
        state: &mut BeforeIterate<Self::Outer, Self::Inner>,
        universe: &impl InnerUniverse<Outer = Self::Outer, Inner = Self::Inner>,
        outer_handle: ColumnHandle,
    ) -> Result<Self>
    where
        Self: Sized;

    fn inner_handle(&self) -> ColumnHandle;
    fn outer_handle(&self) -> ColumnHandle;
}

struct ImportedColumn<O, I> {
    outer: PhantomData<*mut O>,
    inner: PhantomData<*mut I>,
    outer_handle: ColumnHandle,
    inner_handle: ColumnHandle,
}

impl<'c, S: MaybeTotalScope, T> InnerColumn for ImportedColumn<S, Child<'c, S, T>>
where
    T: Refines<S::MaybeTotalTimestamp> + Lattice,
    Child<'c, S, T>: MaybeTotalScope,
{
    type Outer = S;
    type Inner = Child<'c, S, T>;

    fn create(
        state: &mut BeforeIterate<Self::Outer, Self::Inner>,
        universe: &impl InnerUniverse<Outer = Self::Outer, Inner = Self::Inner>,
        outer_handle: ColumnHandle,
    ) -> Result<Self> {
        let inner_handle = match state.column_cache.entry(outer_handle) {
            Entry::Occupied(o) => *o.get(),
            Entry::Vacant(v) => {
                let column = state
                    .outer
                    .columns
                    .get(*v.key())
                    .ok_or(Error::InvalidColumnHandle)?;
                let new_values = column.values().enter(&state.inner.scope);
                // TODO: import the arrangement
                let new_column_handle = state
                    .inner
                    .columns
                    .alloc(Column::from_collection(universe.inner_handle(), new_values));
                *v.insert(new_column_handle)
            }
        };

        Ok(Self {
            outer: PhantomData,
            inner: PhantomData,
            outer_handle,
            inner_handle,
        })
    }

    fn inner_handle(&self) -> ColumnHandle {
        self.inner_handle
    }

    fn outer_handle(&self) -> ColumnHandle {
        self.outer_handle
    }
}

struct IteratedColumn<O, I: MaybeTotalScope> {
    outer: PhantomData<*mut O>,
    outer_handle: ColumnHandle,
    inner_handle: ColumnHandle,
    values_var: ValuesVar<I>,
}

impl<'c, S: MaybeTotalScope, T> InnerColumn for IteratedColumn<S, Child<'c, S, T>>
where
    T: Refines<S::MaybeTotalTimestamp> + Lattice,
    Child<'c, S, T>: MaybeTotalScope,
{
    type Outer = S;
    type Inner = Child<'c, S, T>;

    fn create(
        state: &mut BeforeIterate<Self::Outer, Self::Inner>,
        universe: &impl InnerUniverse<Outer = Self::Outer, Inner = Self::Inner>,
        outer_handle: ColumnHandle,
    ) -> Result<Self> {
        let column = state
            .outer
            .columns
            .get(outer_handle)
            .ok_or(Error::InvalidColumnHandle)?;
        let values_var = Variable::new_from(
            column.values().enter(&state.inner.scope),
            state.step.clone(),
        );
        let inner_handle = state.inner.columns.alloc(Column::from_collection(
            universe.inner_handle(),
            values_var.clone(),
        ));

        Ok(Self {
            outer: PhantomData,
            outer_handle,
            inner_handle,
            values_var,
        })
    }

    fn inner_handle(&self) -> ColumnHandle {
        self.inner_handle
    }

    fn outer_handle(&self) -> ColumnHandle {
        self.outer_handle
    }
}

impl<'c, S: MaybeTotalScope> IteratedColumn<S, Child<'c, S, Product<S::Timestamp, u32>>> {
    fn finish(
        self,
        state: &mut AfterIterate<S, Child<'c, S, Product<S::Timestamp, u32>>>,
        outer_universe_handle: UniverseHandle,
        inner_handle: ColumnHandle,
    ) -> Result<ColumnHandle> {
        let column = state
            .inner
            .columns
            .get(inner_handle)
            .ok_or(Error::InvalidColumnHandle)?;
        let values = column.values_consolidated();
        self.values_var.set(&state.apply_limit(values));
        // arrange consolidates the output
        let outer_handle = state.outer.columns.alloc(Column::from_arranged(
            outer_universe_handle,
            values.leave().arrange(),
        ));
        Ok(outer_handle)
    }
}

struct InnerLegacyTable<U: InnerUniverse, C: InnerColumn> {
    universe: U,
    columns: Vec<C>,
}

type IteratedLegacyTable<O, I> = InnerLegacyTable<ImportedUniverse<O, I>, IteratedColumn<O, I>>;
type IteratedWithUniverseLegacyTable<O, I> =
    InnerLegacyTable<IteratedUniverse<O, I>, IteratedColumn<O, I>>;
type ExtraLegacyTable<O, I> = InnerLegacyTable<ImportedUniverse<O, I>, ImportedColumn<O, I>>;

impl<U: InnerUniverse, C: InnerColumn<Outer = U::Outer, Inner = U::Inner>> InnerLegacyTable<U, C> {
    fn create(
        state: &mut BeforeIterate<U::Outer, U::Inner>,
        universe_handle: UniverseHandle,
        column_handles: impl IntoIterator<Item = ColumnHandle>,
    ) -> Result<Self> {
        let universe = U::create(state, universe_handle)?;
        let columns = column_handles
            .into_iter()
            .map(|column_handle| C::create(state, &universe, column_handle))
            .collect::<Result<_>>()?;
        Ok(Self { universe, columns })
    }
}

impl<'c, S: MaybeTotalScope, U>
    InnerLegacyTable<U, IteratedColumn<S, Child<'c, S, Product<S::Timestamp, u32>>>>
where
    U: InnerUniverse<Outer = S, Inner = Child<'c, S, Product<S::Timestamp, u32>>>,
{
    fn finish(
        self,
        state: &mut AfterIterate<S, Child<'c, S, Product<S::Timestamp, u32>>>,
        inner_universe_handle: UniverseHandle,
        inner_column_handles: impl IntoIterator<Item = ColumnHandle>,
    ) -> Result<(UniverseHandle, Vec<ColumnHandle>)> {
        let outer_universe_handle = self.universe.finish(state, inner_universe_handle)?;
        let outer_column_handles = inner_column_handles
            .into_iter()
            .zip_longest(self.columns)
            .map(|element| {
                let (inner_column_handle, inner_column) =
                    element.both().ok_or(Error::LengthMismatch)?;
                inner_column.finish(state, outer_universe_handle, inner_column_handle)
            })
            .collect::<Result<_>>()?;
        Ok((outer_universe_handle, outer_column_handles))
    }
}

struct BeforeIterate<'g, O: MaybeTotalScope, I: MaybeTotalScope> {
    outer: &'g DataflowGraphInner<O>,
    inner: &'g mut DataflowGraphInner<I>,
    step: <I::Timestamp as Timestamp>::Summary,
    universe_cache: HashMap<UniverseHandle, UniverseHandle>,
    column_cache: HashMap<ColumnHandle, ColumnHandle>,
}

impl<'g, 'c, S: MaybeTotalScope, T> BeforeIterate<'g, S, Child<'c, S, T>>
where
    T: Refines<S::MaybeTotalTimestamp> + Lattice,
    Child<'c, S, T>: MaybeTotalScope<MaybeTotalTimestamp = T>,
{
    fn new(
        outer: &'g DataflowGraphInner<S>,
        inner: &'g mut DataflowGraphInner<Child<'c, S, T>>,
        step: T::Summary,
    ) -> Self {
        Self {
            outer,
            inner,
            step,
            universe_cache: HashMap::new(),
            column_cache: HashMap::new(),
        }
    }

    fn create_tables<U, C>(
        &mut self,
        tables: impl IntoIterator<Item = (UniverseHandle, impl IntoIterator<Item = ColumnHandle>)>,
    ) -> Result<Vec<InnerLegacyTable<U, C>>>
    where
        U: InnerUniverse<Outer = S, Inner = Child<'c, S, T>>,
        C: InnerColumn<Outer = S, Inner = Child<'c, S, T>>,
    {
        tables
            .into_iter()
            .map(|(universe_handle, column_handles)| {
                InnerLegacyTable::create(self, universe_handle, column_handles)
            })
            .collect::<Result<_>>()
    }
}

struct AfterIterate<'g, O: MaybeTotalScope, I: MaybeTotalScope> {
    outer: &'g mut DataflowGraphInner<O>,
    inner: &'g DataflowGraphInner<I>,
    limit: Option<u32>,
}

impl<'g, 'c, S: MaybeTotalScope> AfterIterate<'g, S, Child<'c, S, Product<S::Timestamp, u32>>> {
    fn new(
        outer: &'g mut DataflowGraphInner<S>,
        inner: &'g DataflowGraphInner<Child<'c, S, Product<S::MaybeTotalTimestamp, u32>>>,
        limit: Option<u32>,
    ) -> Self {
        Self {
            outer,
            inner,
            limit,
        }
    }

    fn apply_limit<'a, D>(
        &self,
        collection: &'a Collection<Child<'c, S, Product<S::Timestamp, u32>>, D>,
    ) -> Cow<'a, Collection<Child<'c, S, Product<S::Timestamp, u32>>, D>>
    where
        D: Data,
    {
        if let Some(limit) = self.limit {
            Cow::Owned(
                collection
                    .inner
                    .filter(move |(_data, time, _diff)| time.inner < limit - 1)
                    .as_collection(),
            )
        } else {
            Cow::Borrowed(collection)
        }
    }
}

fn extract_handles<U, C>(
    tables: impl IntoIterator<Item = impl Borrow<InnerLegacyTable<U, C>>>,
) -> Vec<LegacyTable>
where
    U: InnerUniverse,
    C: InnerColumn<Outer = U::Outer, Inner = U::Inner>,
{
    tables
        .into_iter()
        .map(|table| {
            let universe_handle = table.borrow().universe.inner_handle();
            let column_handles = table
                .borrow()
                .columns
                .iter()
                .map(InnerColumn::inner_handle)
                .collect();
            (universe_handle, column_handles)
        })
        .collect()
}

struct InnerDataflowGraph<S: MaybeTotalScope>(RefCell<DataflowGraphInner<S>>);

impl<S: MaybeTotalScope> InnerDataflowGraph<S> {
    pub fn new(
        scope: S,
        error_reporter: ErrorReporter,
        ignore_asserts: bool,
        persistence_config: Option<PersistenceManagerConfig>,
        global_persistent_storage: Option<SharedWorkersPersistenceCoordinator>,
    ) -> Result<Self> {
        Ok(Self(RefCell::new(DataflowGraphInner::new(
            scope,
            error_reporter,
            ignore_asserts,
            persistence_config,
            global_persistent_storage,
        )?)))
    }
}

impl<S: MaybeTotalScope> Graph for InnerDataflowGraph<S>
where
    <S::MaybeTotalTimestamp as MaybeTotalTimestamp>::IsTotal: CreateDataflowReducer<S>,
{
    fn worker_index(&self) -> usize {
        self.0.borrow().worker_index()
    }

    fn worker_count(&self) -> usize {
        self.0.borrow().worker_count()
    }

    fn empty_universe(&self) -> Result<UniverseHandle> {
        self.0.borrow_mut().empty_universe()
    }

    fn empty_column(
        &self,
        universe_handle: UniverseHandle,
        column_properties: Arc<ColumnProperties>,
    ) -> Result<ColumnHandle> {
        self.0
            .borrow_mut()
            .empty_column(universe_handle, column_properties)
    }

    fn empty_table(&self, _table_properties: Arc<TableProperties>) -> Result<TableHandle> {
        Err(Error::IoNotPossible)
    }

    fn static_universe(&self, keys: Vec<Key>) -> Result<UniverseHandle> {
        self.0.borrow_mut().static_universe(keys)
    }

    fn static_column(
        &self,
        universe_handle: UniverseHandle,
        values: Vec<(Key, Value)>,
        column_properties: Arc<ColumnProperties>,
    ) -> Result<ColumnHandle> {
        self.0
            .borrow_mut()
            .static_column(universe_handle, values, column_properties)
    }

    fn static_table(
        &self,
        _data: Vec<DataRow>,
        _table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        Err(Error::IoNotPossible)
    }

    fn expression_column(
        &self,
        wrapper: BatchWrapper,
        expression: Arc<Expression>,
        universe_handle: UniverseHandle,
        column_handles: Vec<ColumnHandle>,
        column_properties: Arc<ColumnProperties>,
        trace: Trace,
    ) -> Result<ColumnHandle> {
        self.0.borrow_mut().expression_column(
            wrapper,
            expression,
            universe_handle,
            &column_handles,
            column_properties,
            trace,
        )
    }

    fn expression_table(
        &self,
        table_handle: TableHandle,
        column_paths: Vec<ColumnPath>,
        expressions: Vec<ExpressionData>,
        wrapper: BatchWrapper,
    ) -> Result<TableHandle> {
        self.0
            .borrow_mut()
            .expression_table(table_handle, column_paths, expressions, wrapper)
    }

    fn columns_to_table(
        &self,
        universe_handle: UniverseHandle,
        columns: Vec<(ColumnHandle, ColumnPath)>,
    ) -> Result<TableHandle> {
        self.0
            .borrow_mut()
            .columns_to_table(universe_handle, columns)
    }

    fn table_column(
        &self,
        universe_handle: UniverseHandle,
        table_handle: TableHandle,
        column_path: ColumnPath,
    ) -> Result<ColumnHandle> {
        self.0
            .borrow_mut()
            .table_column(universe_handle, table_handle, column_path)
    }

    fn table_universe(&self, table_handle: TableHandle) -> Result<UniverseHandle> {
        self.0.borrow_mut().table_universe(table_handle)
    }

    fn table_properties(&self, table_handle: TableHandle) -> Result<Arc<TableProperties>> {
        self.0.borrow_mut().table_properties(table_handle)
    }

    fn flatten_table_storage(
        &self,
        table_handle: TableHandle,
        column_paths: Vec<ColumnPath>,
    ) -> Result<TableHandle> {
        self.0
            .borrow_mut()
            .flatten_table_storage(table_handle, column_paths)
    }

    fn async_apply_table(
        &self,
        function: Arc<dyn Fn(Key, &[Value]) -> BoxFuture<'static, DynResult<Value>> + Send + Sync>,
        table_handle: TableHandle,
        column_paths: Vec<ColumnPath>,
        table_properties: Arc<TableProperties>,
        trace: Trace,
    ) -> Result<TableHandle> {
        self.0.borrow_mut().async_apply_table(
            function,
            table_handle,
            column_paths,
            table_properties,
            trace,
        )
    }

    fn filter_table(
        &self,
        table_handle: TableHandle,
        filtering_column_path: ColumnPath,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        self.0
            .borrow_mut()
            .filter_table(table_handle, filtering_column_path, table_properties)
    }

    fn forget(
        &self,
        _table_handle: TableHandle,
        _threshold_time_column_path: ColumnPath,
        _current_time_column_path: ColumnPath,
        _mark_forgetting_records: bool,
        _table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        Err(Error::NotSupportedInIteration)
    }

    fn forget_immediately(
        &self,
        _table_handle: TableHandle,
        _table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        Err(Error::NotSupportedInIteration)
    }

    fn filter_out_results_of_forgetting(
        &self,
        _table_handle: TableHandle,
        _table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        Err(Error::NotSupportedInIteration)
    }

    fn freeze(
        &self,
        _table_handle: TableHandle,
        _threshold_time_column_path: ColumnPath,
        _current_time_column_path: ColumnPath,
        _table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        Err(Error::NotSupportedInIteration)
    }

    fn buffer(
        &self,
        _table_handle: TableHandle,
        _threshold_time_column_path: ColumnPath,
        _current_time_column_path: ColumnPath,
        _table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        Err(Error::NotSupportedInIteration)
    }

    fn restrict_column(
        &self,
        universe_handle: UniverseHandle,
        column_handle: ColumnHandle,
    ) -> Result<ColumnHandle> {
        self.0
            .borrow_mut()
            .restrict_column(universe_handle, column_handle)
    }

    fn restrict_or_override_table_universe(
        &self,
        original_table_handle: TableHandle,
        new_table_handle: TableHandle,
        same_universes: bool,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        self.0.borrow_mut().restrict_or_override_table_universe(
            original_table_handle,
            new_table_handle,
            same_universes,
            table_properties,
        )
    }

    fn intersect_tables(
        &self,
        table_handle: TableHandle,
        other_table_handles: Vec<TableHandle>,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        self.0
            .borrow_mut()
            .intersect_tables(table_handle, other_table_handles, table_properties)
    }

    fn subtract_table(
        &self,
        left_table_handle: TableHandle,
        right_table_handle: TableHandle,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        self.0
            .borrow_mut()
            .subtract_table(left_table_handle, right_table_handle, table_properties)
    }

    fn concat_tables(
        &self,
        table_handles: Vec<TableHandle>,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        self.0
            .borrow_mut()
            .concat_tables(&table_handles, table_properties)
    }

    fn flatten_table(
        &self,
        table_handle: TableHandle,
        flatten_column_path: ColumnPath,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        self.0
            .borrow_mut()
            .flatten_table(table_handle, flatten_column_path, table_properties)
    }

    fn sort_table(
        &self,
        table_handle: TableHandle,
        key_column_path: ColumnPath,
        instance_column_path: ColumnPath,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        self.0.borrow_mut().sort_table(
            table_handle,
            key_column_path,
            instance_column_path,
            table_properties,
        )
    }

    fn reindex_table(
        &self,
        table_handle: TableHandle,
        reindexing_column_path: ColumnPath,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        self.0
            .borrow_mut()
            .reindex_table(table_handle, reindexing_column_path, table_properties)
    }

    fn update_rows_table(
        &self,
        table_handle: TableHandle,
        update_handle: TableHandle,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        self.0
            .borrow_mut()
            .update_rows_table(table_handle, update_handle, table_properties)
    }

    fn update_cells_table(
        &self,
        table_handle: TableHandle,
        update_handle: TableHandle,
        column_paths: Vec<ColumnPath>,
        update_paths: Vec<ColumnPath>,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        self.0.borrow_mut().update_cells_table(
            table_handle,
            update_handle,
            column_paths,
            update_paths,
            table_properties,
        )
    }

    fn group_by_table(
        &self,
        table_handle: TableHandle,
        grouping_columns_paths: Vec<ColumnPath>,
        reducers: Vec<ReducerData>,
        set_id: bool,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        self.0.borrow_mut().group_by_table(
            table_handle,
            grouping_columns_paths,
            reducers,
            set_id,
            table_properties,
        )
    }

    fn gradual_broadcast(
        &self,
        input_table_handle: TableHandle,
        threshold_table_handle: TableHandle,
        lower_path: ColumnPath,
        value_path: ColumnPath,
        upper_path: ColumnPath,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        self.0.borrow_mut().gradual_broadcast(
            input_table_handle,
            threshold_table_handle,
            lower_path,
            value_path,
            upper_path,
            table_properties,
        )
    }

    fn ix_table(
        &self,
        to_ix_handle: TableHandle,
        key_handle: TableHandle,
        key_column_path: ColumnPath,
        ix_key_policy: IxKeyPolicy,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        self.0.borrow_mut().ix_table(
            to_ix_handle,
            key_handle,
            key_column_path,
            ix_key_policy,
            table_properties,
        )
    }

    fn join_tables(
        &self,
        left_table_handle: TableHandle,
        right_table_handle: TableHandle,
        left_column_paths: Vec<ColumnPath>,
        right_column_paths: Vec<ColumnPath>,
        join_type: JoinType,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        self.0.borrow_mut().join_tables(
            left_table_handle,
            right_table_handle,
            left_column_paths,
            right_column_paths,
            join_type,
            table_properties,
        )
    }

    fn iterate<'a>(
        &'a self,
        _iterated: Vec<LegacyTable>,
        _iterated_with_universe: Vec<LegacyTable>,
        _extra: Vec<LegacyTable>,
        _limit: Option<u32>,
        _logic: IterationLogic<'a>,
    ) -> Result<(Vec<LegacyTable>, Vec<LegacyTable>)> {
        Err(Error::IterationNotPossible)
    }

    fn complex_columns(&self, inputs: Vec<ComplexColumn>) -> Result<Vec<ColumnHandle>> {
        self.0.borrow_mut().complex_columns(inputs)
    }

    fn debug_universe(&self, tag: String, table_handle: TableHandle) -> Result<()> {
        self.0.borrow().debug_universe(tag, table_handle)
    }

    fn debug_column(
        &self,
        tag: String,
        table_handle: TableHandle,
        column_path: ColumnPath,
    ) -> Result<()> {
        self.0.borrow().debug_column(tag, table_handle, column_path)
    }

    fn connector_table(
        &self,
        _reader: Box<dyn ReaderBuilder>,
        _parser: Box<dyn Parser>,
        _commit_duration: Option<Duration>,
        _parallel_readers: usize,
        _table_properties: Arc<TableProperties>,
        _external_persistent_id: Option<&ExternalPersistentId>,
    ) -> Result<TableHandle> {
        Err(Error::IoNotPossible)
    }

    fn subscribe_table(
        &self,
        _wrapper: BatchWrapper,
        _callback: Box<dyn FnMut(Key, &[Value], u64, isize) -> DynResult<()>>,
        _on_end: Box<dyn FnMut() -> DynResult<()>>,
        _table_handle: TableHandle,
        _column_paths: Vec<ColumnPath>,
        _skip_persisted_batch: bool,
    ) -> Result<()> {
        Err(Error::IoNotPossible)
    }

    fn output_table(
        &self,
        mut _data_sink: Box<dyn Writer>,
        mut _data_formatter: Box<dyn Formatter>,
        _table_handle: TableHandle,
        _column_paths: Vec<ColumnPath>,
    ) -> Result<()> {
        Err(Error::IoNotPossible)
    }

    fn attach_prober(
        &self,
        _logic: Box<dyn FnMut(ProberStats)>,
        _intermediate_probes_required: bool,
        _run_callback_every_time: bool,
    ) -> Result<()> {
        Err(Error::IoNotPossible)
    }

    fn probe_table(&self, table_handle: TableHandle, operator_id: usize) -> Result<()> {
        self.0.borrow_mut().probe_table(table_handle, operator_id)
    }
}

struct OuterDataflowGraph<S: MaybeTotalScope<MaybeTotalTimestamp = u64>>(
    RefCell<DataflowGraphInner<S>>,
);

impl<S: MaybeTotalScope<MaybeTotalTimestamp = u64>> OuterDataflowGraph<S> {
    pub fn new(
        scope: S,
        error_reporter: ErrorReporter,
        ignore_asserts: bool,
        persistence_config: Option<PersistenceManagerOuterConfig>,
        global_persistent_storage: Option<SharedWorkersPersistenceCoordinator>,
    ) -> Result<Self> {
        let worker_idx = scope.index();
        let total_workers = scope.peers();
        Ok(Self(RefCell::new(DataflowGraphInner::new(
            scope,
            error_reporter,
            ignore_asserts,
            persistence_config.map(|cfg| cfg.into_inner(worker_idx, total_workers)),
            global_persistent_storage,
        )?)))
    }
}

impl<S: MaybeTotalScope<MaybeTotalTimestamp = u64>> Graph for OuterDataflowGraph<S> {
    fn worker_index(&self) -> usize {
        self.0.borrow().worker_index()
    }

    fn worker_count(&self) -> usize {
        self.0.borrow().worker_count()
    }

    fn empty_universe(&self) -> Result<UniverseHandle> {
        self.0.borrow_mut().empty_universe()
    }

    fn empty_column(
        &self,
        universe_handle: UniverseHandle,
        column_properties: Arc<ColumnProperties>,
    ) -> Result<ColumnHandle> {
        self.0
            .borrow_mut()
            .empty_column(universe_handle, column_properties)
    }

    fn empty_table(&self, table_properties: Arc<TableProperties>) -> Result<TableHandle> {
        self.0.borrow_mut().empty_table(table_properties)
    }

    fn static_universe(&self, keys: Vec<Key>) -> Result<UniverseHandle> {
        self.0.borrow_mut().static_universe(keys)
    }

    fn static_column(
        &self,
        universe_handle: UniverseHandle,
        values: Vec<(Key, Value)>,
        column_properties: Arc<ColumnProperties>,
    ) -> Result<ColumnHandle> {
        self.0
            .borrow_mut()
            .static_column(universe_handle, values, column_properties)
    }

    fn static_table(
        &self,
        data: Vec<DataRow>,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        self.0.borrow_mut().static_table(data, table_properties)
    }

    fn expression_column(
        &self,
        wrapper: BatchWrapper,
        expression: Arc<Expression>,
        universe_handle: UniverseHandle,
        column_handles: Vec<ColumnHandle>,
        column_properties: Arc<ColumnProperties>,
        trace: Trace,
    ) -> Result<ColumnHandle> {
        self.0.borrow_mut().expression_column(
            wrapper,
            expression,
            universe_handle,
            &column_handles,
            column_properties,
            trace,
        )
    }

    fn expression_table(
        &self,
        table_handle: TableHandle,
        column_paths: Vec<ColumnPath>,
        expressions: Vec<ExpressionData>,
        wrapper: BatchWrapper,
    ) -> Result<TableHandle> {
        self.0
            .borrow_mut()
            .expression_table(table_handle, column_paths, expressions, wrapper)
    }

    fn columns_to_table(
        &self,
        universe_handle: UniverseHandle,
        columns: Vec<(ColumnHandle, ColumnPath)>,
    ) -> Result<TableHandle> {
        self.0
            .borrow_mut()
            .columns_to_table(universe_handle, columns)
    }

    fn table_column(
        &self,
        universe_handle: UniverseHandle,
        table_handle: TableHandle,
        column_path: ColumnPath,
    ) -> Result<ColumnHandle> {
        self.0
            .borrow_mut()
            .table_column(universe_handle, table_handle, column_path)
    }

    fn table_universe(&self, table_handle: TableHandle) -> Result<UniverseHandle> {
        self.0.borrow_mut().table_universe(table_handle)
    }

    fn table_properties(&self, table_handle: TableHandle) -> Result<Arc<TableProperties>> {
        self.0.borrow_mut().table_properties(table_handle)
    }

    fn flatten_table_storage(
        &self,
        table_handle: TableHandle,
        column_paths: Vec<ColumnPath>,
    ) -> Result<TableHandle> {
        self.0
            .borrow_mut()
            .flatten_table_storage(table_handle, column_paths)
    }

    fn async_apply_table(
        &self,
        function: Arc<dyn Fn(Key, &[Value]) -> BoxFuture<'static, DynResult<Value>> + Send + Sync>,
        table_handle: TableHandle,
        column_paths: Vec<ColumnPath>,
        table_properties: Arc<TableProperties>,
        trace: Trace,
    ) -> Result<TableHandle> {
        self.0.borrow_mut().async_apply_table(
            function,
            table_handle,
            column_paths,
            table_properties,
            trace,
        )
    }

    fn subscribe_table(
        &self,
        wrapper: BatchWrapper,
        callback: Box<dyn FnMut(Key, &[Value], u64, isize) -> DynResult<()>>,
        on_end: Box<dyn FnMut() -> DynResult<()>>,
        table_handle: TableHandle,
        column_paths: Vec<ColumnPath>,
        skip_persisted_batch: bool,
    ) -> Result<()> {
        self.0.borrow_mut().subscribe_table(
            wrapper,
            callback,
            on_end,
            table_handle,
            column_paths,
            skip_persisted_batch,
        )
    }

    fn filter_table(
        &self,
        table_handle: TableHandle,
        filtering_column_path: ColumnPath,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        self.0
            .borrow_mut()
            .filter_table(table_handle, filtering_column_path, table_properties)
    }

    fn forget(
        &self,
        table_handle: TableHandle,
        threshold_time_column_path: ColumnPath,
        current_time_column_path: ColumnPath,
        mark_forgetting_records: bool,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        self.0.borrow_mut().forget(
            table_handle,
            threshold_time_column_path,
            current_time_column_path,
            mark_forgetting_records,
            table_properties,
        )
    }

    fn forget_immediately(
        &self,
        table_handle: TableHandle,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        self.0
            .borrow_mut()
            .forget_immediately(table_handle, table_properties)
    }

    fn filter_out_results_of_forgetting(
        &self,
        table_handle: TableHandle,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        self.0
            .borrow_mut()
            .filter_out_results_of_forgetting(table_handle, table_properties)
    }

    fn freeze(
        &self,
        table_handle: TableHandle,
        threshold_time_column_path: ColumnPath,
        current_time_column_path: ColumnPath,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        self.0.borrow_mut().freeze(
            table_handle,
            threshold_time_column_path,
            current_time_column_path,
            table_properties,
        )
    }

    fn buffer(
        &self,
        table_handle: TableHandle,
        threshold_time_column_path: ColumnPath,
        current_time_column_path: ColumnPath,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        self.0.borrow_mut().buffer(
            table_handle,
            threshold_time_column_path,
            current_time_column_path,
            table_properties,
        )
    }

    fn restrict_column(
        &self,
        universe_handle: UniverseHandle,
        column_handle: ColumnHandle,
    ) -> Result<ColumnHandle> {
        self.0
            .borrow_mut()
            .restrict_column(universe_handle, column_handle)
    }

    fn restrict_or_override_table_universe(
        &self,
        original_table_handle: TableHandle,
        new_table_handle: TableHandle,
        same_universes: bool,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        self.0.borrow_mut().restrict_or_override_table_universe(
            original_table_handle,
            new_table_handle,
            same_universes,
            table_properties,
        )
    }

    fn intersect_tables(
        &self,
        table_handle: TableHandle,
        other_table_handles: Vec<TableHandle>,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        self.0
            .borrow_mut()
            .intersect_tables(table_handle, other_table_handles, table_properties)
    }

    fn subtract_table(
        &self,
        left_table_handle: TableHandle,
        right_table_handle: TableHandle,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        self.0
            .borrow_mut()
            .subtract_table(left_table_handle, right_table_handle, table_properties)
    }

    fn concat_tables(
        &self,
        table_handles: Vec<TableHandle>,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        self.0
            .borrow_mut()
            .concat_tables(&table_handles, table_properties)
    }

    fn flatten_table(
        &self,
        table_handle: TableHandle,
        flatten_column_path: ColumnPath,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        self.0
            .borrow_mut()
            .flatten_table(table_handle, flatten_column_path, table_properties)
    }

    fn sort_table(
        &self,
        table_handle: TableHandle,
        key_column_path: ColumnPath,
        instance_column_path: ColumnPath,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        self.0.borrow_mut().sort_table(
            table_handle,
            key_column_path,
            instance_column_path,
            table_properties,
        )
    }

    fn reindex_table(
        &self,
        table_handle: TableHandle,
        reindexing_column_path: ColumnPath,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        self.0
            .borrow_mut()
            .reindex_table(table_handle, reindexing_column_path, table_properties)
    }

    fn update_rows_table(
        &self,
        table_handle: TableHandle,
        update_handle: TableHandle,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        self.0
            .borrow_mut()
            .update_rows_table(table_handle, update_handle, table_properties)
    }

    fn update_cells_table(
        &self,
        table_handle: TableHandle,
        update_handle: TableHandle,
        column_paths: Vec<ColumnPath>,
        update_paths: Vec<ColumnPath>,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        self.0.borrow_mut().update_cells_table(
            table_handle,
            update_handle,
            column_paths,
            update_paths,
            table_properties,
        )
    }

    fn group_by_table(
        &self,
        table_handle: TableHandle,
        grouping_columns_paths: Vec<ColumnPath>,
        reducers: Vec<ReducerData>,
        set_id: bool,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        self.0.borrow_mut().group_by_table(
            table_handle,
            grouping_columns_paths,
            reducers,
            set_id,
            table_properties,
        )
    }

    fn gradual_broadcast(
        &self,
        input_table_handle: TableHandle,
        threshold_table_handle: TableHandle,
        lower_path: ColumnPath,
        value_path: ColumnPath,
        upper_path: ColumnPath,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        self.0.borrow_mut().gradual_broadcast(
            input_table_handle,
            threshold_table_handle,
            lower_path,
            value_path,
            upper_path,
            table_properties,
        )
    }

    fn ix_table(
        &self,
        to_ix_handle: TableHandle,
        key_handle: TableHandle,
        key_column_path: ColumnPath,
        ix_key_policy: IxKeyPolicy,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        self.0.borrow_mut().ix_table(
            to_ix_handle,
            key_handle,
            key_column_path,
            ix_key_policy,
            table_properties,
        )
    }

    fn join_tables(
        &self,
        left_table_handle: TableHandle,
        right_table_handle: TableHandle,
        left_column_paths: Vec<ColumnPath>,
        right_column_paths: Vec<ColumnPath>,
        join_type: JoinType,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        self.0.borrow_mut().join_tables(
            left_table_handle,
            right_table_handle,
            left_column_paths,
            right_column_paths,
            join_type,
            table_properties,
        )
    }

    fn iterate<'a>(
        &'a self,
        iterated: Vec<LegacyTable>,
        iterated_with_universe: Vec<LegacyTable>,
        extra: Vec<LegacyTable>,
        limit: Option<u32>,
        logic: IterationLogic<'a>,
    ) -> Result<(Vec<LegacyTable>, Vec<LegacyTable>)> {
        self.0
            .borrow_mut()
            .iterate(iterated, iterated_with_universe, extra, limit, logic)
    }

    fn complex_columns(&self, inputs: Vec<ComplexColumn>) -> Result<Vec<ColumnHandle>> {
        self.0.borrow_mut().complex_columns(inputs)
    }

    fn debug_universe(&self, tag: String, table_handle: TableHandle) -> Result<()> {
        self.0.borrow().debug_universe(tag, table_handle)
    }

    fn debug_column(
        &self,
        tag: String,
        table_handle: TableHandle,
        column_path: ColumnPath,
    ) -> Result<()> {
        self.0.borrow().debug_column(tag, table_handle, column_path)
    }

    fn connector_table(
        &self,
        reader: Box<dyn ReaderBuilder>,
        parser: Box<dyn Parser>,
        commit_duration: Option<Duration>,
        parallel_readers: usize,
        table_properties: Arc<TableProperties>,
        external_persistent_id: Option<&ExternalPersistentId>,
    ) -> Result<TableHandle> {
        self.0.borrow_mut().connector_table(
            reader,
            parser,
            commit_duration,
            parallel_readers,
            table_properties,
            external_persistent_id,
        )
    }

    fn output_table(
        &self,
        data_sink: Box<dyn Writer>,
        data_formatter: Box<dyn Formatter>,
        table_handle: TableHandle,
        column_paths: Vec<ColumnPath>,
    ) -> Result<()> {
        self.0
            .borrow_mut()
            .output_table(data_sink, data_formatter, table_handle, column_paths)
    }

    fn attach_prober(
        &self,
        logic: Box<dyn FnMut(ProberStats)>,
        intermediate_probes_required: bool,
        run_callback_every_time: bool,
    ) -> Result<()> {
        self.0.borrow_mut().attach_prober(
            logic,
            intermediate_probes_required,
            run_callback_every_time,
        )
    }

    fn probe_table(&self, table_handle: TableHandle, operator_id: usize) -> Result<()> {
        self.0.borrow_mut().probe_table(table_handle, operator_id)
    }
}

fn parse_env_var<T: FromStr>(name: &str) -> Result<Option<T>, String>
where
    T::Err: Display,
{
    if let Some(value) = env::var_os(name) {
        Ok(Some(
            value
                .into_string()
                .map_err(|_| format!("Couldn't parse the value of {name} as UTF-8 string"))?
                .parse()
                .map_err(|err| format!("Couldn't parse the value of {name}: {err}"))?,
        ))
    } else {
        Ok(None)
    }
}

fn parse_env_var_required<T: FromStr>(name: &str) -> Result<T, String>
where
    T::Err: Display,
{
    parse_env_var(name)?.ok_or_else(|| format!("{name} is not set"))
}

pub fn config_from_env() -> Result<(Config, usize), String> {
    let mut threads: usize = parse_env_var("PATHWAY_THREADS")?.unwrap_or(1);
    let mut processes: usize = parse_env_var("PATHWAY_PROCESSES")?.unwrap_or(1);
    if threads == 0 {
        return Err("Can't run with 0 threads".to_string());
    }
    if processes == 0 {
        return Err("Can't run with 0 processes".to_string());
    }
    let workers = threads * processes;
    if workers > MAX_WORKERS {
        warn!("{workers} is greater than the the maximum allowed number of workers ({MAX_WORKERS}), reducing");
        threads = MAX_WORKERS / processes;
        if threads == 0 {
            threads = 1;
            processes = MAX_WORKERS;
        }
    }
    let workers = threads * processes;
    assert!(workers <= MAX_WORKERS);
    let config = if processes > 1 {
        let process_id: usize = parse_env_var_required("PATHWAY_PROCESS_ID")?;
        if process_id >= processes {
            return Err(format!("Process ID {process_id} is too big"));
        }
        let first_port: usize = parse_env_var_required("PATHWAY_FIRST_PORT")?;
        let addresses = (0..processes)
            .map(|id| format!("127.0.0.1:{}", first_port + id))
            .collect();
        Config {
            communication: CommunicationConfig::Cluster {
                threads,
                process: process_id,
                addresses,
                report: false,
                log_fn: Box::new(|_| None),
            },
            worker: WorkerConfig::default(),
        }
    } else if threads > 1 {
        Config::process(threads)
    } else {
        Config::thread()
    };
    Ok((config, workers))
}

#[allow(clippy::too_many_lines)] // XXX
#[allow(clippy::too_many_arguments)] // XXX
pub fn run_with_new_dataflow_graph<R, R2>(
    logic: impl Fn(&dyn Graph) -> DynResult<R> + Send + Sync + 'static,
    finish: impl Fn(R) -> R2 + Send + Sync + 'static,
    config: Config,
    mut wakeup_receiver: Option<WakeupReceiver>,
    stats_monitor: Option<PyObject>,
    ignore_asserts: bool,
    monitoring_level: MonitoringLevel,
    with_http_server: bool,
    persistence_config: Option<PersistenceManagerOuterConfig>,
    num_workers: usize,
) -> Result<Vec<R2>>
where
    R: 'static,
    R2: Send + 'static,
{
    if !env::var("PATHWAY_SKIP_START_LOG").is_ok_and(|v| v == "1") {
        info!("Preparing Pathway computation");
    }
    if !YOLO.is_empty() {
        info!("Running in YOLO mode: {}", YOLO.iter().format(", "));
    }
    let (error_reporter, error_receiver) = ErrorReporter::create();
    let failed = Arc::new(AtomicBool::new(false));
    let failed_2 = failed.clone();
    let process_id = match config.communication {
        CommunicationConfig::Cluster { process, .. } => process,
        _ => 0,
    };
    let global_persistent_storage = persistence_config.as_ref().map(|cfg| {
        Arc::new(Mutex::new(
            cfg.create_workers_persistence_coordinator(num_workers),
        ))
    });

    let guards = execute(config, move |worker| {
        catch_unwind(AssertUnwindSafe(|| {
            if let Ok(addr) = env::var("DIFFERENTIAL_LOG_ADDR") {
                if let Ok(stream) = std::net::TcpStream::connect(&addr) {
                    differential_dataflow::logging::enable(worker, stream);
                    info!("enabled differential logging to {addr}");
                } else {
                    panic!("Could not connect to differential log address: {addr:?}");
                }
            }

            let (
                res,
                mut pollers,
                connector_threads,
                connector_monitors,
                input_probe,
                output_probe,
                intermediate_probes,
                mut probers,
                progress_reporter_runner,
                http_server_runner,
            ) = worker.dataflow::<u64, _, _>(|scope| {
                let graph = OuterDataflowGraph::new(
                    scope.clone(),
                    error_reporter.clone(),
                    ignore_asserts,
                    persistence_config.clone(),
                    global_persistent_storage.clone(),
                )
                .unwrap_with_reporter(&error_reporter);
                let res = logic(&graph).unwrap_with_reporter(&error_reporter);
                let progress_reporter_runner =
                    maybe_run_reporter(&monitoring_level, &graph, stats_monitor.clone());
                let http_server_runner =
                    maybe_run_http_server_thread(with_http_server, &graph, process_id);
                let graph = graph.0.into_inner();
                (
                    res,
                    graph.pollers,
                    graph.connector_threads,
                    graph.connector_monitors,
                    graph.input_probe,
                    graph.output_probe,
                    graph.probes,
                    graph.probers,
                    progress_reporter_runner,
                    http_server_runner,
                )
            });

            loop {
                if failed.load(Ordering::SeqCst) {
                    resume_unwind(Box::new("other worker panicked"));
                }

                for prober in &mut probers {
                    prober.update(
                        &input_probe,
                        &output_probe,
                        &intermediate_probes,
                        &connector_monitors,
                    );
                }

                let mut next_step_duration = None;

                let iteration_start = SystemTime::now();

                pollers.retain_mut(|poller| match poller() {
                    ControlFlow::Continue(None) => true,
                    ControlFlow::Continue(Some(next_commit_at)) => {
                        let time_to_commit = next_commit_at
                            .duration_since(iteration_start)
                            .unwrap_or(Duration::ZERO);

                        next_step_duration = Some(
                            next_step_duration.map_or(time_to_commit, |x| min(x, time_to_commit)),
                        );
                        true
                    }
                    ControlFlow::Break(()) => false,
                });

                if !worker.step_or_park(next_step_duration) {
                    break;
                }
            }

            for connector_thread in connector_threads {
                connector_thread
                    .join()
                    .expect("connector thread should not panic");
            }

            for prober in &mut probers {
                prober.update(
                    &input_probe,
                    &output_probe,
                    &intermediate_probes,
                    &connector_monitors,
                );
            }

            drop(http_server_runner);
            drop(progress_reporter_runner);

            finish(res)
        }))
        .unwrap_or_else(|panic_payload| {
            let error = Error::from_panic_payload(panic_payload);
            let message = error.to_string();
            error_reporter.report(error);
            resume_unwind(Box::new(message));
        })
    })
    .map_err(Error::Dataflow)?;

    let res = loop {
        select! {
            recv(error_receiver) -> res => {
                match res {
                    Ok(error) => break Err(error),
                    Err(RecvError) => break Ok(()),
                }
            }
            recv(wakeup_receiver.as_ref().unwrap_or(&never())) -> res => {
                match res {
                    Ok(callback) => match callback() {
                        Ok(()) => {}
                        Err(error) => break Err(Error::from(error)),
                    }
                    Err(RecvError) => wakeup_receiver = None,
                }
            }
        }
    };
    match res {
        Ok(()) => {}
        Err(error) => {
            failed_2.store(true, Ordering::SeqCst);
            for handle in guards.guards() {
                handle.thread().unpark();
            }
            catch_unwind(AssertUnwindSafe(|| drop(guards.join()))).unwrap_or(());
            return Err(error);
        }
    };

    let res = guards
        .join()
        .into_iter()
        .map(|res| res.map_err(Error::WorkerPanic))
        .collect::<Result<Vec<_>>>()?;
    Ok(res)
}
