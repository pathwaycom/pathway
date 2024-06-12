// Copyright © 2024 Pathway

#![allow(clippy::module_name_repetitions)]
#![allow(clippy::non_canonical_partial_ord_impl)] // False positive with Derivative

mod complex_columns;
pub mod config;
mod export;
pub mod maybe_total;
pub mod operators;
pub mod shard;

use crate::connectors::adaptors::{GenericValues, ValuesSessionAdaptor};
use crate::connectors::data_format::{Formatter, Parser};
use crate::connectors::data_storage::{ReaderBuilder, Writer};
use crate::connectors::monitoring::{ConnectorMonitor, ConnectorStats, OutputConnectorStats};
use crate::connectors::snapshot::Event as SnapshotEvent;
use crate::connectors::{read_persisted_state, ARTIFICIAL_TIME_ON_REWIND_START};
use crate::connectors::{Connector, PersistenceMode, SnapshotAccess};
use crate::engine::dataflow::operators::external_index::UseExternalIndexAsOfNow;
use crate::engine::dataflow::operators::gradual_broadcast::GradualBroadcast;
use crate::engine::dataflow::operators::time_column::{
    Epsilon, TimeColumnForget, TimeColumnFreeze,
};
use crate::engine::telemetry::Config as TelemetryConfig;
use crate::engine::value::HashInto;
use crate::persistence::config::{PersistenceManagerConfig, PersistenceManagerOuterConfig};
use crate::persistence::frontier::OffsetAntichain;
use crate::persistence::tracker::WorkerPersistentStorage;
use crate::persistence::{ExternalPersistentId, IntoPersistentId};
use crate::timestamp::current_unix_timestamp_ms;

use std::borrow::{Borrow, Cow};
use std::cell::RefCell;
use std::cmp::min;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::iter::once;
use std::marker::PhantomData;
use std::ops::{ControlFlow, Deref};
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use std::sync::{mpsc, Arc};
use std::thread::{Builder, JoinHandle};
use std::time::{Duration, SystemTime};
use std::{env, slice};

use arcstr;
use arcstr::ArcStr;
use crossbeam_channel::{bounded, never, select, Receiver, RecvError, Sender};
use derivative::Derivative;
use differential_dataflow::collection::concatenate;
use differential_dataflow::input::InputSession;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::{Arranged, TraceAgent};
use differential_dataflow::operators::iterate::Variable;
use differential_dataflow::operators::reduce::{Reduce, ReduceCore};
use differential_dataflow::operators::JoinCore;
use differential_dataflow::trace::implementations::ord::{OrdKeySpine, OrdValSpine};
use differential_dataflow::Collection;
use differential_dataflow::{AsCollection as _, Data};
use futures::future::BoxFuture;
use id_arena::Arena;
use itertools::{chain, process_results, Itertools};
use log::{error, info};
use ndarray::ArrayD;
use once_cell::unsync::{Lazy, OnceCell};
use pyo3::PyObject;
use rand::{thread_rng, Rng};
use serde::{Deserialize, Serialize};
use timely::dataflow::operators::probe::Handle as ProbeHandle;
use timely::dataflow::operators::{Filter, Inspect, Probe};
use timely::dataflow::operators::{Map, ToStream as _};
use timely::dataflow::scopes::Child;
use timely::execute;
use timely::order::{Product, TotalOrder};
use timely::progress::timestamp::Refines;
use timely::progress::Timestamp as TimestampTrait;
use xxhash_rust::xxh3::Xxh3 as Hasher;

use self::complex_columns::complex_columns;
use self::export::{export_table, import_table};
use self::maybe_total::{MaybeTotalScope, MaybeTotalTimestamp, NotTotal, Total};
use self::operators::output::{ConsolidateForOutput, OutputBatch};
use self::operators::prev_next::add_prev_next_pointers;
use self::operators::stateful_reduce::StatefulReduce;
use self::operators::time_column::{MaxTimestamp, TimeColumnBuffer};
use self::operators::{ArrangeWithTypes, MapWithConsistentDeletions, MapWrapped};
use self::operators::{MaybeTotal, Reshard};
use self::shard::Shard;
use super::error::{DataError, DataResult, DynError, DynResult, Trace};
use super::expression::AnyExpression;
use super::external_index_wrappers::{ExternalIndexData, ExternalIndexQuery};
use super::graph::{DataRow, ExportedTable, OperatorProperties, SubscribeCallbacks};
use super::http_server::maybe_run_http_server_thread;
use super::license::License;
use super::progress_reporter::{maybe_run_reporter, MonitoringLevel};
use super::reduce::{
    AnyReducer, ArgMaxReducer, ArgMinReducer, ArraySumReducer, CountReducer, EarliestReducer,
    FloatSumReducer, IntSumReducer, LatestReducer, MaxReducer, MinReducer, ReducerImpl,
    SemigroupReducerImpl, SortedTupleReducer, StatefulCombineFn, StatefulReducer, TupleReducer,
    UniqueReducer,
};
use super::report_error::{
    LogError, ReportError, ReportErrorExt, SpawnWithReporter, UnwrapWithErrorLogger,
    UnwrapWithReporter,
};
use super::telemetry::maybe_run_telemetry_thread;
use super::{
    BatchWrapper, ColumnHandle, ColumnPath, ColumnProperties, ComplexColumn, Error, ErrorLogHandle,
    Expression, ExpressionData, Graph, IterationLogic, IxKeyPolicy, JoinData, JoinType, Key,
    LegacyTable, OperatorStats, ProberStats, Reducer, ReducerData, Result, ShardPolicy,
    TableHandle, TableProperties, Timestamp, UniverseHandle, Value,
};
use crate::external_integration::{
    make_accessor, make_option_accessor, ExternalIndex, IndexDerivedImpl,
};

pub use self::config::Config;

pub type WakeupReceiver = Receiver<Box<dyn FnOnce() -> DynResult<()> + Send + Sync + 'static>>;

const YOLO: &[&str] = &[
    #[cfg(feature = "yolo-id32")]
    "id32",
    #[cfg(feature = "yolo-id64")]
    "id64",
];

const OUTPUT_RETRIES: usize = 5;
const RETRY_SLEEP_INITIAL: Duration = Duration::from_secs(1);
const RETRY_SLEEP_BACKOFF_FACTOR: f64 = 1.2;
const RETRY_JITTER: Duration = Duration::from_millis(800);
const ERROR_LOG_FLUSH_PERIOD: Duration = Duration::from_secs(1);

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

type SharedWorkerPersistentStorage = Option<Arc<Mutex<WorkerPersistentStorage>>>;

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

    fn values_consolidated(&self) -> &Values<S> {
        self.data.consolidated()
    }

    fn keys(&self) -> &Keys<S> {
        self.data.keys()
    }

    fn keys_arranged(&self) -> KeysArranged<S> {
        self.data.keys_arranged()
    }
}

struct ErrorLogInner {
    input_session: InputSession<Timestamp, (Key, Value), isize>,
    last_flush: Option<SystemTime>,
}

impl ErrorLogInner {
    fn new(input_session: InputSession<Timestamp, (Key, Value), isize>) -> Self {
        let mut log = ErrorLogInner {
            input_session,
            last_flush: None,
        };
        log.maybe_flush();
        log
    }
    fn insert(&mut self, value: Value) {
        self.input_session.insert((Key::random(), value));
        self.maybe_flush();
    }
    fn maybe_flush(&mut self) -> SystemTime {
        // returns time of the next flush
        let now = SystemTime::now();
        let flush = self.last_flush.map_or(true, |last_flush| {
            last_flush + ERROR_LOG_FLUSH_PERIOD <= now
        });
        if flush {
            self.last_flush = Some(now);
            let new_timestamp = u64::try_from(current_unix_timestamp_ms())
                .expect("number of milliseconds should fit in 64 bits");
            let new_timestamp = (new_timestamp / 2) * 2; //use only even times (required by alt-neu)
            self.input_session.advance_to(Timestamp(new_timestamp));
            self.input_session.flush();
        }
        self.last_flush.expect("last_flush should be set") + ERROR_LOG_FLUSH_PERIOD
    }
}

#[derive(Clone)]
struct ErrorLog {
    inner: Rc<RefCell<ErrorLogInner>>,
}

impl ErrorLog {
    fn new(input_session: InputSession<Timestamp, (Key, Value), isize>) -> ErrorLog {
        let inner = ErrorLogInner::new(input_session);
        ErrorLog {
            inner: Rc::new(RefCell::new(inner)),
        }
    }

    fn insert(&self, value: Value) {
        self.inner.borrow_mut().insert(value);
    }

    fn maybe_flush(&self) -> SystemTime {
        self.inner.borrow_mut().maybe_flush()
    }
}

struct ErrorLogger {
    operator_id: i64,
    error_log: Option<ErrorLog>,
}

impl ErrorLogger {
    fn inner_log(&self, error: &DataError, trace: Option<String>) {
        if matches!(error, DataError::ErrorInValue) {
            return;
        }
        let trace = trace.unwrap_or_default();
        let error = error.to_string();
        error!("{error} in operator {}. {trace}", self.operator_id);
        if let Some(error_log) = self.error_log.as_ref() {
            error_log.insert(Value::from(
                [
                    Value::from(self.operator_id),
                    Value::from(ArcStr::from(error)),
                    Value::from(ArcStr::from(trace)),
                ]
                .as_slice(),
            ));
        }
    }
}

impl LogError for ErrorLogger {
    fn log_error(&self, error: DataError) {
        self.inner_log(&error, None);
    }

    fn log_error_with_trace(&self, error: DynError, trace: &Trace) {
        self.inner_log(&error.into(), Some(format!("{trace}")));
    }
}

struct Prober {
    input_time: Option<Timestamp>,
    input_time_changed: Option<SystemTime>,
    output_time: Option<Timestamp>,
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
    fn create_stats(
        probe: &ProbeHandle<Timestamp>,
        input_time: Option<Timestamp>,
    ) -> OperatorStats {
        let frontier = probe.with_frontier(|frontier| frontier.as_option().copied());
        if let Some(timestamp) = frontier {
            OperatorStats {
                time: if timestamp.0 > 0 {
                    Some(timestamp)
                } else {
                    None
                },
                lag: input_time.map(|input_time_unwrapped| {
                    if input_time_unwrapped >= timestamp {
                        input_time_unwrapped.0 - timestamp.0
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
        input_probe: &ProbeHandle<Timestamp>,
        output_probe: &ProbeHandle<Timestamp>,
        intermediate_probes: &HashMap<usize, ProbeHandle<Timestamp>>,
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
    error_logs: Arena<ErrorLog, ErrorLogHandle>,
    flushers: Vec<Box<dyn FnMut() -> SystemTime>>,
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
    worker_persistent_storage: SharedWorkerPersistentStorage,
    persisted_states_count: u64,
    config: Arc<Config>,
    terminate_on_error: bool,
    default_error_log: Option<ErrorLog>,
    current_error_log: Option<ErrorLog>,
    current_operator_properties: Option<OperatorProperties>,
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

trait ReplaceDuplicatesWithError {
    fn replace_duplicates_with_error(
        &self,
        error_logic: impl FnMut(&Value) -> Value + 'static,
        error_logger: Box<dyn LogError>,
    ) -> Self;
}

impl<S: MaybeTotalScope> ReplaceDuplicatesWithError for Collection<S, (Key, Value)> {
    fn replace_duplicates_with_error(
        &self,
        mut error_logic: impl FnMut(&Value) -> Value + 'static,
        error_logger: Box<dyn LogError>,
    ) -> Self {
        self.reduce(move |key, input, output| {
            let res = match input {
                [(value, 1)] => (*value).clone(),
                [] => unreachable!(),
                [(value, _), ..] => {
                    error_logger.log_error(DataError::DuplicateKey(*key));
                    error_logic(value)
                }
            };
            output.push((res, 1));
        })
    }
}

trait FilterOutErrors {
    fn filter_out_errors(&self, error_logger: Option<Box<dyn LogError>>) -> Self;
}

impl<S: MaybeTotalScope> FilterOutErrors for Collection<S, (Key, Tuple)> {
    fn filter_out_errors(&self, error_logger: Option<Box<dyn LogError>>) -> Self {
        self.filter(move |(_key, values)| {
            let contains_errors = values.as_value_slice().contains(&Value::Error);
            if contains_errors {
                if let Some(error_logger) = error_logger.as_ref() {
                    error_logger.log_error(DataError::ErrorInOutput);
                }
            }
            !contains_errors
        })
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

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
enum MaybeUpdate<T> {
    Original(T),
    Update(T),
}

#[allow(clippy::unnecessary_wraps)] // we want to always return Result for symmetry
impl<S: MaybeTotalScope> DataflowGraphInner<S> {
    #[allow(clippy::too_many_arguments)]
    fn new(
        scope: S,
        error_reporter: ErrorReporter,
        ignore_asserts: bool,
        persistence_config: Option<PersistenceManagerConfig>,
        config: Arc<Config>,
        terminate_on_error: bool,
        default_error_log: Option<ErrorLog>,
    ) -> Result<Self> {
        let worker_persistent_storage = {
            if let Some(persistence_config) = &persistence_config {
                let worker_storage = Arc::new(Mutex::new(WorkerPersistentStorage::new(
                    persistence_config.clone(),
                )?));
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
            error_logs: Arena::new(),
            flushers: Vec::new(),
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
            persisted_states_count: 0,
            config,
            terminate_on_error,
            default_error_log,
            current_error_log: None,
            current_operator_properties: None,
        })
    }

    fn worker_index(&self) -> usize {
        self.scope.index()
    }

    fn worker_count(&self) -> usize {
        self.scope.peers()
    }

    fn thread_count(&self) -> usize {
        self.config.threads()
    }

    fn process_count(&self) -> usize {
        self.config.processes()
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
    ) -> Result<()> {
        let error_logger = self.create_error_logger()?;
        left.map_named("assert_collections_same_size::left", |_| ())
            .negate()
            .concat(&right.map_named("assert_collections_same_size::right", |_| ()))
            .consolidate()
            .inspect(move |(_data, _time, diff)| {
                assert_ne!(diff, &0);
                error_logger.log_error(DataError::ValueMissing);
            });
        Ok(())
    }

    #[track_caller]
    fn assert_input_keys_match_output_keys(
        &self,
        input_keys: &Keys<S>,
        output_collection: impl Deref<Target = Collection<S, (Key, Value)>>,
    ) -> Result<()> {
        let error_logger = self.create_error_logger()?;
        input_keys
            .concat(
                &output_collection
                    .map_named("assert_input_keys_match_output_keys", |(k, _)| k)
                    .negate(),
            )
            .consolidate()
            .inspect(move |(key, _time, diff)| {
                assert_ne!(diff, &0);
                if diff > &0 {
                    error_logger.log_error(DataError::KeyMissingInOutputTable(*key));
                } else {
                    error_logger.log_error(DataError::KeyMissingInInputTable(*key));
                }
            });
        Ok(())
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
            self.assert_input_keys_match_output_keys(universe.keys(), &values)?;
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
        deterministic: bool,
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
        let error_logger = self.create_error_logger()?;

        let closure = move |(key, values)| {
            let args: Vec<Value> = column_paths
                .iter()
                .map(|path| path.extract(&key, &values))
                .collect::<Result<_>>()
                .unwrap_with_reporter(&error_reporter);
            let new_values = expressions.iter().map(|expression_data| {
                let result = expression_data
                    .expression
                    .eval(&args)
                    .unwrap_or_log_with_trace(
                        error_logger.as_ref(),
                        expression_data.properties.trace(),
                        Value::Error,
                    );
                result
            });
            (key, Value::Tuple(new_values.collect()))
        };

        let new_values = if deterministic {
            table.values_consolidated().map_wrapped_named(
                "expression_table::evaluate_expression",
                wrapper,
                closure,
            )
        } else {
            table.values().map_named_with_consistent_deletions(
                "expression_table::evaluate_expression",
                wrapper,
                closure,
            )
        };
        // TODO: move determinization to apply (create a list of caches for each expression_table)

        Ok(self
            .tables
            .alloc(Table::from_collection(new_values).with_properties(Arc::new(properties))))
    }

    fn columns_to_table_properties(
        &mut self,
        columns: Vec<ColumnHandle>,
    ) -> Result<TableProperties> {
        let properties: Result<Vec<_>> = columns
            .into_iter()
            .map(|column_handle| {
                let properties = self
                    .columns
                    .get(column_handle)
                    .ok_or(Error::InvalidColumnHandle)?
                    .properties
                    .clone();
                Ok(properties.as_ref().clone())
            })
            .collect();

        Ok(TableProperties::Table(properties?.as_slice().into()))
    }

    fn columns_to_table(
        &mut self,
        universe_handle: UniverseHandle,
        column_handles: Vec<ColumnHandle>,
    ) -> Result<TableHandle> {
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
        let properties = self.columns_to_table_properties(column_handles)?;

        let table_values = tuples.map_named("columns_to_table:pack", move |(key, values)| {
            (key, Value::from(values.as_ref()))
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

    fn table_properties(
        &mut self,
        table_handle: TableHandle,
        path: &ColumnPath,
    ) -> Result<Arc<TableProperties>> {
        let table = self
            .tables
            .get(table_handle)
            .ok_or(Error::InvalidTableHandle)?;
        Ok(Arc::from(path.extract_properties(&table.properties)?))
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
        deterministic: bool,
    ) -> Result<TableHandle> {
        let table = self
            .tables
            .get(table_handle)
            .ok_or(Error::InvalidTableHandle)?;
        let error_reporter = self.error_reporter.clone();
        let error_logger: Rc<dyn LogError> = self.create_error_logger()?.into();
        let trace = Arc::new(trace);
        let closure = move |(key, values)| {
            let args: Vec<Value> = column_paths
                .iter()
                .map(|path| path.extract(&key, &values))
                .collect::<Result<_>>()
                .unwrap_with_reporter_and_trace(&error_reporter, &trace);
            let error_logger = error_logger.clone();
            let function = function.clone();
            let trace = trace.clone();
            let future = async move {
                let value = async {
                    function(key, &args).await.unwrap_or_log_with_trace(
                        error_logger.as_ref(),
                        &trace,
                        Value::Error,
                    )
                }
                .await;
                (key, Value::from([value].as_slice()))
            };
            Box::pin(future)
        };
        let new_values = if deterministic {
            table
                .values()
                .map_named_async("expression_column::apply_async", closure)
        } else {
            table.values().map_named_async_with_consistent_deletions(
                "expression_column::apply_async",
                closure,
            )
        };
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
        let error_logger = self.create_error_logger()?;

        let new_table = table.values().flat_map(move |(key, values)| {
            if filtering_column_path
                .extract(&key, &values)
                .unwrap_with_reporter(&error_reporter)
                .into_result()
                .map_err(|_err| DataError::ErrorInFilter)
                .unwrap_or_log(error_logger.as_ref(), Value::Bool(false))
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

    fn remove_retractions_from_table(
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
            .flat_map(|(data, time, diff)| {
                if diff > 0 {
                    Some((data, time, diff))
                } else {
                    None
                }
            })
            .as_collection();
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
        S::MaybeTotalTimestamp: Epsilon,
    {
        let table = self
            .tables
            .get(table_handle)
            .ok_or(Error::InvalidTableHandle)?;

        //TODO: report errors
        let _error_reporter = self.error_reporter.clone();

        let (on_time, _late) = table.values().freeze(
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
        S::MaybeTotalTimestamp: Epsilon + MaxTimestamp,
    {
        let table = self
            .tables
            .get(table_handle)
            .ok_or(Error::InvalidTableHandle)?;

        //TODO: report errors
        let _error_reporter = self.error_reporter.clone();

        let new_table = table.values().postpone(
            table.values().scope(),
            move |val| threshold_time_column_path.extract_from_value(val).unwrap(),
            move |val| current_time_column_path.extract_from_value(val).unwrap(),
            true,
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
            self.assert_input_keys_match_output_keys(universe.keys(), &new_values)?;
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

        let result = new_table.values_arranged().join_core(
            original_table.values_arranged(),
            |key, new_values, orig_values| {
                once((
                    *key,
                    Value::from([new_values.clone(), orig_values.clone()].as_slice()),
                ))
            },
        );
        let leftover_values = new_table.values().concat(
            &result
                .map_named(
                    "restrict_or_override_table_universe::compare",
                    |(key, values)| {
                        (
                            key,
                            values.as_tuple().expect("values should be a tuple")[0].clone(),
                        )
                    },
                )
                .distinct()
                .negate(),
        );
        let error_logger = self.create_error_logger()?;

        let result = result.concat(&leftover_values.consolidate().map_named(
            "restrict_or_override_table_universe::fill",
            move |(key, new_values)| {
                error_logger.log_error(DataError::KeyMissingInOutputTable(key));
                (key, Value::from([new_values, Value::Error].as_slice()))
            },
        ));

        if !self.ignore_asserts && same_universes {
            self.assert_input_keys_match_output_keys(original_table.keys(), &result)?;
        };

        Ok(self
            .tables
            .alloc(Table::from_collection(result).with_properties(table_properties)))
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
        let error_logger = self.create_error_logger()?;

        let new_values = table.values().flat_map(move |(key, values)| {
            let value = reindexing_column_path
                .extract(&key, &values)
                .unwrap_with_reporter(&error_reporter);
            match value {
                Value::Error => {
                    error_logger.log_error(DataError::ErrorInReindex);
                    None
                }
                value => Some((
                    value.as_pointer().unwrap_with_reporter(&error_reporter),
                    values,
                )),
            }
        });

        let new_values = if self.ignore_asserts {
            new_values
        } else {
            let error_logger = self.create_error_logger()?;
            new_values.replace_duplicates_with_error(|_| Value::Error, error_logger)
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
        let result = concatenate(&mut self.scope, table_collections);
        let result = if self.ignore_asserts {
            result
        } else {
            let error_logger = self.create_error_logger()?;
            result.replace_duplicates_with_error(|_| Value::Error, error_logger)
        };
        let table = Table::from_collection(result).with_properties(table_properties);
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
        let error_logger = self.create_error_logger()?;

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
                Value::Json(json) => {
                    if let serde_json::Value::Array(array) = (*json).clone() {
                        Ok(array.into_iter().map(Value::from).collect())
                    } else {
                        let repr = json.to_string();
                        Err(DataError::ValueError(format!(
                            "Pathway can't flatten this Json: {repr}"
                        )))
                    }
                }
                value => Err(DataError::ValueError(format!(
                    "Pathway can't flatten this value {value:?}"
                ))),
            }
            .unwrap_or_log(error_logger.as_ref(), vec![]);
            wrapped.into_iter().enumerate().map(move |(i, entry)| {
                let new_key_parts = [Value::from(key), Value::from(i64::try_from(i).unwrap())];
                (
                    Key::for_values(&new_key_parts).with_shard_of(key),
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
    ) -> Result<ArrangedByKey<S, Key, MaybeUpdate<Value>>> {
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
            .map_named("update_rows_arrange::table", |(k, v)| {
                (k, MaybeUpdate::Original(v))
            })
            .concat(
                &update
                    .values()
                    .map_named("update_rows_arrange::update", |(k, v)| {
                        (k, MaybeUpdate::Update(v))
                    }),
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
                let values = match input {
                    [(MaybeUpdate::Original(original_values), 1)] => original_values,
                    [(MaybeUpdate::Update(new_values), 1)] => new_values,
                    [(MaybeUpdate::Original(_), 1), (MaybeUpdate::Update(new_values), 1)] => {
                        new_values
                    }
                    _ => {
                        panic!("unexpected counts in input");
                    }
                };
                output.push((values.clone(), 1));
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
                let (original_values, selected_values, selected_paths) = match input {
                    [(MaybeUpdate::Original(original_values), 1)] => {
                        (original_values, original_values, &column_paths)
                    }
                    [
                        (MaybeUpdate::Original(original_values), 1),
                        (MaybeUpdate::Update(new_values), 1),
                    ] => {
                        (original_values, new_values, &update_paths)
                    }
                    [(MaybeUpdate::Update(_), 1)] => {
                        panic!("updating a row that does not exist");
                    }
                    _ => {
                        panic!("unexpected counts in input");
                    }
                };
                let updates: Vec<_> = selected_paths
                    .iter()
                    .map(|path| path.extract(key, selected_values))
                    .try_collect()
                    .unwrap_with_reporter(&error_reporter);

                let result = Value::Tuple(chain!([original_values.clone()], updates).collect());
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
            self.assert_collections_same_size(key_table.values(), &new_table)?;
        };

        Ok(self
            .tables
            .alloc(Table::from_collection(new_table).with_properties(table_properties)))
    }

    fn use_external_index_as_of_now(
        &mut self,
        index_stream: ExternalIndexData,
        query_stream: ExternalIndexQuery,
        table_properties: Arc<TableProperties>,
        external_index: Box<dyn ExternalIndex>,
    ) -> Result<TableHandle> {
        let index = self
            .tables
            .get(index_stream.table)
            .ok_or(Error::InvalidTableHandle)?;

        let queries = self
            .tables
            .get(query_stream.table)
            .ok_or(Error::InvalidTableHandle)?;

        let data_acc = make_accessor(index_stream.data_column, self.error_reporter.clone());
        let filter_data_acc =
            make_option_accessor(index_stream.filter_data_column, self.error_reporter.clone());
        let query_acc = make_accessor(query_stream.query_column, self.error_reporter.clone());
        let limit_acc =
            make_option_accessor(query_stream.limit_column, self.error_reporter.clone());
        let filter_acc =
            make_option_accessor(query_stream.filter_column, self.error_reporter.clone());

        let extended_external_index = Box::new(IndexDerivedImpl::new(
            external_index,
            self.create_error_logger()?,
            data_acc,
            filter_data_acc,
            query_acc,
            limit_acc,
            filter_acc,
        ));

        let new_values = index
            .values()
            .use_external_index_as_of_now(queries.values(), extended_external_index);

        Ok(self
            .tables
            .alloc(Table::from_collection(new_values).with_properties(table_properties)))
    }

    #[allow(clippy::too_many_lines)]
    fn join_tables(
        &mut self,
        left_data: JoinData,
        right_data: JoinData,
        shard_policy: ShardPolicy,
        join_type: JoinType,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        fn extract_join_key(
            key: &Key,
            values: &Value,
            column_paths: &[ColumnPath],
            shard_policy: ShardPolicy,
            error_reporter: &ErrorReporter,
            error_logger: &mut dyn LogError,
        ) -> Option<Key> {
            let join_key_parts: DataResult<Vec<_>> = column_paths
                .iter()
                .map(|path| path.extract(key, values))
                .collect::<Result<Vec<_>>>()
                .unwrap_with_reporter(error_reporter)
                .into_iter()
                .map(|v| v.into_result().map_err(|_err| DataError::ErrorInJoin))
                .try_collect();
            match join_key_parts {
                Ok(join_key_parts) => {
                    let join_key = shard_policy.generate_key(&join_key_parts);
                    Some(join_key)
                }
                Err(error) => {
                    error_logger.log_error(error);
                    None
                }
            }
        }

        if left_data.column_paths.len() != right_data.column_paths.len() {
            return Err(Error::DifferentJoinConditionLengths);
        }

        let left_table = self
            .tables
            .get(left_data.table_handle)
            .ok_or(Error::InvalidTableHandle)?;
        let right_table = self
            .tables
            .get(right_data.table_handle)
            .ok_or(Error::InvalidTableHandle)?;

        let error_reporter_left = self.error_reporter.clone();
        let error_reporter_right = self.error_reporter.clone();

        let mut error_logger_left = self.create_error_logger()?;
        let mut error_logger_right = self.create_error_logger()?;

        let left_with_join_key =
            left_table
                .values()
                .map_named("join::extract_keys", move |(key, values)| {
                    let join_key = extract_join_key(
                        &key,
                        &values,
                        &left_data.column_paths,
                        shard_policy,
                        &error_reporter_left,
                        error_logger_left.as_mut(),
                    );
                    (join_key, (key, values))
                });
        let join_left = left_with_join_key
            .flat_map(|(join_key, left_key_values)| Some((join_key?, left_key_values)));
        let join_left_arranged: ArrangedByKey<S, Key, (Key, Value)> = join_left.arrange();
        let right_with_join_key =
            right_table
                .values()
                .map_named("join::extract_keys", move |(key, values)| {
                    let join_key = extract_join_key(
                        &key,
                        &values,
                        &right_data.column_paths,
                        shard_policy,
                        &error_reporter_right,
                        error_logger_right.as_mut(),
                    );
                    (join_key, (key, values))
                });
        let join_right = right_with_join_key
            .flat_map(|(join_key, right_key_values)| Some((join_key?, right_key_values)));
        let join_right_arranged: ArrangedByKey<S, Key, (Key, Value)> = join_right.arrange();

        let join_left_right = join_left_arranged
            .join_core(&join_right_arranged, |join_key, left_key, right_key| {
                once((*join_key, left_key.clone(), right_key.clone()))
            });

        let join_left_right_to_result_fn = match join_type {
            JoinType::LeftKeysFull | JoinType::LeftKeysSubset => {
                |_join_key, left_key, _right_key| left_key
            }
            _ => |join_key, left_key, right_key| {
                Key::for_values(&[Value::from(left_key), Value::from(right_key)])
                    .with_shard_of(join_key)
            },
        };
        let result_left_right = join_left_right.map_named(
            "join::result_left_right",
            move |(join_key, (left_key, left_values), (right_key, right_values))| {
                (
                    join_left_right_to_result_fn(join_key, left_key, right_key),
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
            left_with_join_key.concat(
                &join_left_right
                    .map_named(
                        "join::left_outer_res",
                        |(join_key, left_key_values, _right_key_values)| {
                            (join_key, left_key_values)
                        },
                    )
                    .distinct()
                    .negate()
                    .map_named("join::left_outer_wrap", |(key, values)| (Some(key), values)),
            )
        };
        let result_left_outer = match join_type {
            JoinType::LeftOuter | JoinType::FullOuter => Some(left_outer().map_named(
                "join::result_left_outer",
                |(join_key, (left_key, left_values))| {
                    let result_key = Key::for_values(&[Value::from(left_key), Value::None])
                        .with_shard_of(join_key.unwrap_or(left_key));
                    // unwrap_or needed for rows with Value::Error in join condition
                    (left_key, left_values, result_key)
                },
            )),
            JoinType::LeftKeysFull => Some(left_outer().map_named(
                "join::result_left_outer",
                |(_join_key, (left_key, left_values))| (left_key, left_values, left_key),
            )),
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
            right_with_join_key.concat(
                &join_left_right
                    .map_named(
                        "join::right_outer_res",
                        |(join_key, _left_key, right_key_values)| (join_key, right_key_values),
                    )
                    .distinct()
                    .negate()
                    .map_named("join::right_outer_wrap", |(key, values)| {
                        (Some(key), values)
                    }),
            )
        };
        let result_right_outer = match join_type {
            JoinType::RightOuter | JoinType::FullOuter => Some(right_outer().map_named(
                "join::right_result_outer",
                |(join_key, (right_key, right_values))| {
                    let result_key = Key::for_values(&[Value::None, Value::from(right_key)])
                        .with_shard_of(join_key.unwrap_or(right_key));
                    // unwrap_or needed for rows with Value::Error in join condition
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

        let result = if matches!(join_type, JoinType::LeftKeysFull | JoinType::LeftKeysSubset) {
            let error_logger = self.create_error_logger()?;
            let error_reporter = self.error_reporter.clone();
            result_left_right.replace_duplicates_with_error(
                move |value| {
                    let tuple = value.as_tuple().unwrap_with_reporter(&error_reporter);
                    Value::from(
                        [
                            tuple[0].clone(), // left key
                            tuple[1].clone(), // left value
                            Value::Error,
                            Value::Error,
                        ]
                        .as_slice(),
                    )
                },
                error_logger,
            )
        } else {
            result_left_right
        };

        let result_table = Table::from_collection(result).with_properties(table_properties);

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
            .probe_with(self.probes.entry(operator_id).or_default());
        Ok(())
    }

    fn create_error_logger(&self) -> Result<Box<dyn LogError>> {
        if self.terminate_on_error {
            Ok(Box::new(self.error_reporter.clone()))
        } else {
            let operator_properties = self
                .current_operator_properties
                .as_ref()
                .ok_or_else(|| Error::OperatorIdNotSet)?;
            let error_log = if operator_properties.depends_on_error_log {
                None
                // if the current operator depends on error log table, we can't insert errors from it
                // to the log as it'll prevent dropping InputSession and timely will never finish
            } else {
                self.current_error_log
                    .clone()
                    .or(self.default_error_log.clone())
            };
            Ok(Box::new(ErrorLogger {
                operator_id: operator_properties.id.try_into().map_err(DynError::from)?,
                error_log,
            }))
        }
    }

    fn set_operator_properties(&mut self, operator_properties: OperatorProperties) -> Result<()> {
        self.current_operator_properties = Some(operator_properties);
        Ok(())
    }

    fn set_error_log(&mut self, error_log_handle: Option<ErrorLogHandle>) -> Result<()> {
        self.current_error_log = error_log_handle
            .map(|handle| -> Result<ErrorLog> {
                Ok(self
                    .error_logs
                    .get(handle)
                    .ok_or(Error::InvalidErrorLogHandle)?
                    .clone())
            })
            .transpose()?;
        Ok(())
    }

    fn remove_errors_from_table(
        &mut self,
        table_handle: TableHandle,
        column_paths: Vec<ColumnPath>,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        let new_values = self
            .extract_columns(table_handle, column_paths)?
            .as_collection()
            .filter_out_errors(None)
            .map_named("remove_errors_from_table", |(key, tuple)| {
                (key, Value::from(tuple.as_value_slice()))
            });

        Ok(self
            .tables
            .alloc(Table::from_collection(new_values).with_properties(table_properties)))
    }
}

trait DataflowReducer<S: MaybeTotalScope> {
    fn reduce(
        self: Rc<Self>,
        values: &Collection<S, (Key, Key, Vec<Value>)>,
        error_logger: Rc<dyn LogError>,
    ) -> Values<S>;
}

impl<S: MaybeTotalScope, R: ReducerImpl> DataflowReducer<S> for R {
    fn reduce(
        self: Rc<Self>,
        values: &Collection<S, (Key, Key, Vec<Value>)>,
        error_logger: Rc<dyn LogError>,
    ) -> Values<S> {
        let initialized = values.map_named("DataFlowReducer::reduce::init", {
            let self_ = self.clone();
            let error_logger = error_logger.clone();
            move |(source_key, result_key, values)| {
                let state = if values.contains(&Value::Error) {
                    None
                } else {
                    self_
                        .init(&source_key, &values)
                        .ok_with_logger(error_logger.as_ref())
                };
                (result_key, state)
            }
        });

        initialized
            .reduce({
                let self_ = self.clone();
                move |_key, input, output| {
                    let result = if input.iter().any(|&(state, _)| state.is_none()) {
                        None // None means that the state for a given key contains Value::Error
                    } else {
                        self_
                            .combine(input.iter().map(|&(state, cnt)| {
                                (
                                    state.as_ref().unwrap(),
                                    usize::try_from(cnt).unwrap().try_into().unwrap(),
                                )
                            }))
                            .ok_with_logger(error_logger.as_ref())
                    };
                    output.push((result, 1));
                }
            })
            .map_named("DataFlowReducer::reduce", move |(key, state)| {
                let result = if let Some(state) = state {
                    self.finish(state)
                } else {
                    Value::Error
                };
                (key, result)
            })
            .into()
    }
}

impl<S: MaybeTotalScope> DataflowReducer<S> for IntSumReducer {
    fn reduce(
        self: Rc<Self>,
        values: &Collection<S, (Key, Key, Vec<Value>)>,
        error_logger: Rc<dyn LogError>,
    ) -> Values<S> {
        values
            .map_named("IntSumReducer::reduce::init", {
                let self_ = self.clone();
                move |(source_key, result_key, values)| {
                    let state = if values.contains(&Value::Error) {
                        self_.init_error()
                    } else {
                        self_
                            .init(&source_key, &values[0])
                            .unwrap_or_else_log(error_logger.as_ref(), || self_.init_error())
                    };
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
    fn reduce(
        self: Rc<Self>,
        values: &Collection<S, (Key, Key, Vec<Value>)>,
        _error_logger: Rc<dyn LogError>,
    ) -> Values<S> {
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
    fn reduce(
        self: Rc<Self>,
        values: &Collection<S, (Key, Key, Vec<Value>)>,
        error_logger: Rc<dyn LogError>,
    ) -> Values<S> {
        values
            .map_named(
                "StatefulReducer::reduce::init",
                |(_source_key, result_key, values)| (result_key, values),
            )
            .stateful_reduce_named("StatefulReducer::reduce::reduce", move |state, values| {
                let contains_errors = state == Some(&Value::Error)
                    || values.iter().any(|(row, _cnt)| row.contains(&Value::Error));
                if contains_errors {
                    Some(Value::Error)
                } else {
                    self.combine(state, values)
                        .unwrap_or_log(error_logger.as_ref(), Some(Value::Error))
                }
            })
            .into()
    }
}

impl<S> DataflowReducer<S> for LatestReducer
where
    S: MaybeTotalScope,
    S::MaybeTotalTimestamp: TotalOrder,
{
    fn reduce(
        self: Rc<Self>,
        values: &Collection<S, (Key, Key, Vec<Value>)>,
        _error_logger: Rc<dyn LogError>,
    ) -> Values<S> {
        values
            .map_named(
                "LatestReducer::reduce::init",
                |(source_key, result_key, values)| (result_key, (source_key, values)),
            )
            .stateful_reduce_named("LatestReducer::reduce::reduce", move |_state, values| {
                let (_result_key, result_value) = values
                    .into_iter()
                    .map(|((key, values), diff)| {
                        assert!(diff > 0, "deletion encountered in latest reducer");
                        (key, values.into_iter().exactly_one().unwrap())
                    })
                    .max_by_key(|(key, _value)| *key)
                    .expect("input values shouldn't be empty");
                Some(result_value)
            })
            .into()
    }
}

impl<S> DataflowReducer<S> for EarliestReducer
where
    S: MaybeTotalScope,
    S::MaybeTotalTimestamp: TotalOrder,
{
    fn reduce(
        self: Rc<Self>,
        values: &Collection<S, (Key, Key, Vec<Value>)>,
        _error_logger: Rc<dyn LogError>,
    ) -> Values<S> {
        values
            .map_named(
                "EarliestReducer::reduce::init",
                |(source_key, result_key, values)| (result_key, (source_key, values)),
            )
            .stateful_reduce_named("EarliestReducer::reduce::reduce", move |state, values| {
                if state.is_some() {
                    return state.cloned();
                }
                let (_result_key, result_value) = values
                    .into_iter()
                    .map(|((key, values), diff)| {
                        assert!(diff > 0, "deletion encountered in earliest reducer");
                        (key, values.into_iter().exactly_one().unwrap())
                    })
                    .min_by_key(|(key, _value)| *key)
                    .expect("input values shouldn't be empty");
                Some(result_value)
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
            Reducer::Stateful { .. } | Reducer::Earliest | Reducer::Latest => {
                return Err(Error::NotSupportedInIteration)
            }
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
            Reducer::Earliest => Rc::new(EarliestReducer),
            Reducer::Latest => Rc::new(LatestReducer),
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
        shard_policy: ShardPolicy,
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

        let error_logger = self.create_error_logger()?;
        let with_new_key = table.values().flat_map(move |(key, values)| {
            let new_key_parts: Vec<Value> = grouping_columns_paths
                .iter()
                .map(|path| path.extract(&key, &values))
                .collect::<Result<_>>()
                .unwrap_with_reporter(&error_reporter_1);
            let new_key = if new_key_parts.contains(&Value::Error) {
                error_logger.log_error(DataError::ErrorInGroupby);
                None
            } else if set_id {
                Some(
                    new_key_parts
                        .first()
                        .unwrap()
                        .as_pointer()
                        .unwrap_with_reporter(&error_reporter_1),
                )
            } else {
                Some(shard_policy.generate_key(&new_key_parts))
            };
            Some((key, new_key?, values))
        });
        let reduced_columns: Vec<_> = reducer_impls
            .iter()
            .zip(reducers)
            .map(|(reducer_impl, data)| {
                let error_reporter_2 = self.error_reporter.clone();
                let with_extracted_value = with_new_key.flat_map(move |(key, new_key, values)| {
                    let new_values: Vec<_> = data
                        .column_paths
                        .iter()
                        .map(|path| path.extract(&key, &values))
                        .try_collect()
                        .unwrap_with_reporter(&error_reporter_2);
                    if new_values.contains(&Value::Error) && data.skip_errors {
                        None
                    } else {
                        Some((key, new_key, new_values))
                    }
                });
                Ok(reducer_impl
                    .clone()
                    .reduce(&with_extracted_value, self.create_error_logger()?.into()))
            })
            .collect::<Result<_>>()?;
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

impl<S: MaybeTotalScope<MaybeTotalTimestamp = Timestamp>> DataflowGraphInner<S>
where
    S::MaybeTotalTimestamp: TotalOrder,
{
    #[allow(clippy::too_many_lines)]
    fn deduplicate(
        &mut self,
        table_handle: TableHandle,
        grouping_columns_paths: Vec<ColumnPath>,
        reduced_column_paths: Vec<ColumnPath>,
        combine_fn: StatefulCombineFn,
        external_persistent_id: Option<&ExternalPersistentId>,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        let table = self
            .tables
            .get(table_handle)
            .ok_or(Error::InvalidTableHandle)?;

        let error_reporter = self.error_reporter.clone();
        self.persisted_states_count += 1;
        let effective_persistent_id =
            self.effective_persistent_id(false, external_persistent_id, || {
                let generated_external_id = format!("deduplicate-{}", self.persisted_states_count);
                info!("Persistent ID autogenerated for deduplicate: {generated_external_id}");
                generated_external_id
            })?;
        let persistent_id = effective_persistent_id
            .clone()
            .map(IntoPersistentId::into_persistent_id);

        let error_logger = self.create_error_logger()?;
        let with_new_keys = table
            .values()
            .flat_map(move |(key, values)| {
                let new_key_parts: Vec<_> = grouping_columns_paths
                    .iter()
                    .map(|path| path.extract(&key, &values))
                    .collect::<Result<_>>()
                    .unwrap_with_reporter(&error_reporter);

                if new_key_parts.contains(&Value::Error) {
                    error_logger.log_error(DataError::ErrorInDeduplicate);
                    None
                } else {
                    let new_values: Vec<_> = reduced_column_paths
                        .iter()
                        .map(|path| path.extract(&key, &values))
                        .collect::<Result<_>>()
                        .unwrap_with_reporter(&error_reporter);

                    let new_key = Key::for_values(&new_key_parts);
                    Some((new_key, new_values))
                }
            })
            .inner // skip artificial time created by a normal persistence
            .filter(|((_key, _values), time, _diff)| *time != ARTIFICIAL_TIME_ON_REWIND_START)
            .as_collection();

        let with_persisted_state = if let Some(persistent_id) = persistent_id {
            let mut input_session = InputSession::new();
            let state = input_session.to_collection(&mut self.scope);
            let snapshot_reader_state = read_persisted_state(
                input_session,
                self.worker_persistent_storage.as_ref().unwrap().clone(),
                &effective_persistent_id
                    .expect("persistent_id exists so effective_persistent_id also exists"),
                persistent_id,
            );
            self.pollers.push(snapshot_reader_state.poller);
            self.connector_threads
                .push(snapshot_reader_state.input_thread_handle);
            with_new_keys.concat(&state).consolidate()
        } else {
            with_new_keys
        };
        let error_logger = self.create_error_logger()?;
        let new_values = with_persisted_state.stateful_reduce_named(
            "deduplicate::reduce",
            move |state, values| match (combine_fn)(state, values) {
                Ok(new_state) => new_state,
                Err(error) => {
                    error_logger.log_error(error.into());
                    state.cloned()
                }
            },
        );

        let new_values_persisted = if let Some(persistent_id) = persistent_id {
            let error_reporter = self.error_reporter.clone();
            let snapshot_writer = self
                .worker_persistent_storage
                .as_ref()
                .unwrap()
                .lock()
                .unwrap()
                .create_snapshot_writer(persistent_id)
                .expect("Failed to create a snapshot writer in deduplicate");
            new_values
                .consolidate()
                .inner
                .inspect_core(move |event| match event {
                    Ok((_time, data)) => {
                        for ((key, values), time, diff) in data {
                            if *time == ARTIFICIAL_TIME_ON_REWIND_START {
                                continue;
                            }
                            assert!(*diff == 1 || *diff == -1);
                            let values_vec: Vec<Value> =
                                (**values.as_tuple().unwrap_with_reporter(&error_reporter)).into();
                            let event = if *diff == 1 {
                                SnapshotEvent::Insert(*key, values_vec)
                            } else {
                                SnapshotEvent::Delete(*key, values_vec)
                            };
                            snapshot_writer
                                .lock()
                                .unwrap()
                                .write(&event)
                                .expect("Failed to save row in persistent buffer.");
                        }
                    }
                    Err(frontier) => {
                        assert!(frontier.len() <= 1);
                        if let Some(time) = frontier.first() {
                            snapshot_writer
                                .lock()
                                .unwrap()
                                .write(&SnapshotEvent::AdvanceTime(*time, OffsetAntichain::new()))
                                .expect("Failed to save time advancement in persistent buffer.");
                        }
                    }
                })
                .as_collection()
        } else {
            new_values
        };

        Ok(self
            .tables
            .alloc(Table::from_collection(new_values_persisted).with_properties(table_properties)))
    }
}

#[derive(Debug, Clone)]
enum OutputEvent {
    Commit(Option<Timestamp>),
    Batch(OutputBatch<Timestamp, (Key, Tuple), isize>),
}

#[allow(clippy::unnecessary_wraps)] // we want to always return Result for symmetry
impl<S: MaybeTotalScope<MaybeTotalTimestamp = Timestamp>> DataflowGraphInner<S> {
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

    fn effective_persistent_id(
        &self,
        reader_is_internal: bool,
        external_persistent_id: Option<&ExternalPersistentId>,
        logic: impl FnOnce() -> String,
    ) -> Result<Option<ExternalPersistentId>> {
        let has_persistent_storage = self.worker_persistent_storage.is_some();
        if let Some(external_persistent_id) = external_persistent_id {
            if !has_persistent_storage {
                return Err(Error::NoPersistentStorage(external_persistent_id.clone()));
            }
        }
        if external_persistent_id.is_some() {
            Ok(external_persistent_id.cloned())
        } else if has_persistent_storage
            && !reader_is_internal
            && self
                .worker_persistent_storage
                .as_ref()
                .unwrap()
                .lock()
                .unwrap()
                .persistent_id_generation_enabled()
        {
            let table_persistence_enabled = self
                .worker_persistent_storage
                .as_ref()
                .unwrap()
                .lock()
                .unwrap()
                .table_persistence_enabled();
            if table_persistence_enabled {
                Ok(Some(logic()))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
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
        let effective_persistent_id =
            self.effective_persistent_id(reader.is_internal(), external_persistent_id, || {
                let generated_external_id = reader.name(None, self.connector_monitors.len());
                reader
                    .update_persistent_id(Some(generated_external_id.clone().into_persistent_id()));
                info!(
                    "Persistent ID autogenerated for a {:?} reader: {generated_external_id}",
                    reader.storage_type()
                );
                generated_external_id
            })?;

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
            let persistence_mode = self
                .persistence_config
                .as_ref()
                .map_or(PersistenceMode::Batch, |config| config.persistence_mode);
            let snapshot_access = self
                .persistence_config
                .as_ref()
                .map_or(SnapshotAccess::Full, |config| config.snapshot_access);

            let connector = Connector::new(
                commit_duration,
                parser.column_count(),
                self.terminate_on_error,
                self.create_error_logger()?.into(),
            );
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
                persistence_mode,
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
                    .register_input_source(persistent_id, &reader_storage_type);
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

        let new_table = table.values().forget(
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
            Timestamp(time.0 + 1) // produce neu times
        });
        let new_table = table
            .values()
            .inner
            .inspect(|(_data, time, _diff)| {
                assert!(time.0 % 2 == 0, "Neu time encountered at forget() input.");
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
            .filter(|(_data, time, _diff)| time.0 % 2 == 0)
            .as_collection();
        Ok(self
            .tables
            .alloc(Table::from_collection(new_table).with_properties(table_properties)))
    }

    fn output_batch(
        stats: &mut OutputConnectorStats,
        batch: OutputBatch<Timestamp, (Key, Tuple), isize>,
        data_sink: &mut Box<dyn Writer>,
        data_formatter: &mut Box<dyn Formatter>,
        worker_persistent_storage: &SharedWorkerPersistentStorage,
    ) -> Result<(), DynError> {
        stats.on_batch_started();
        let time = batch.time;
        for ((key, values), diff) in batch.data {
            if time == ARTIFICIAL_TIME_ON_REWIND_START && worker_persistent_storage.is_some() {
                // Ignore entries, which had been written before
                continue;
            }

            // TODO: provide a way to configure it individually per connector maybe?
            let mut sleep_duration = RETRY_SLEEP_INITIAL;
            let retries = if data_sink.retriable() {
                OUTPUT_RETRIES
            } else {
                1
            };
            for attempt in 0..retries {
                let formatted = data_formatter
                    .format(&key, &values, time, diff)
                    .map_err(DynError::from)?;
                let write_result = data_sink.write(formatted);
                if write_result.is_err() {
                    if attempt == retries - 1 {
                        // If this is the last attempt and the output has failed, fail
                        // the whole computation
                        write_result.map_err(DynError::from)?;
                    }
                    std::thread::sleep(sleep_duration);
                    sleep_duration = sleep_duration.mul_f64(RETRY_SLEEP_BACKOFF_FACTOR)
                        + thread_rng().gen_range(Duration::ZERO..RETRY_JITTER);
                } else {
                    break;
                }
            }
            stats.on_batch_entry_written();
        }
        stats.on_batch_finished();
        data_sink.flush(false).map_err(DynError::from)?;

        Ok(())
    }

    fn commit_output_time(
        stats: &mut OutputConnectorStats,
        t: Option<Timestamp>,
        sink_id: Option<usize>,
        worker_persistent_storage: &SharedWorkerPersistentStorage,
    ) {
        if let Some(worker_persistent_storage) = &worker_persistent_storage {
            worker_persistent_storage
                .lock()
                .unwrap()
                .update_sink_finalized_time(
                    sink_id.expect("undefined sink_id while using persistent storage"),
                    t,
                );
        }
        stats.on_time_committed(t.map(|t| t.0));
    }

    fn output_table(
        &mut self,
        mut data_sink: Box<dyn Writer>,
        mut data_formatter: Box<dyn Formatter>,
        table_handle: TableHandle,
        column_paths: Vec<ColumnPath>,
    ) -> Result<()> {
        let worker_index = self.scope.index();
        let error_logger = self.create_error_logger()?;
        let output_columns = self
            .extract_columns(table_handle, column_paths)?
            .as_collection()
            .filter_out_errors(Some(error_logger));
        let single_threaded = data_sink.single_threaded();
        let connector_does_output = !single_threaded || worker_index == 0;

        let output = output_columns.consolidate_for_output(single_threaded);

        let sink_id = self
            .worker_persistent_storage
            .as_ref()
            .map(|storage| storage.lock().unwrap().register_sink());

        let sender = {
            let (sender, receiver) = mpsc::channel();

            let thread_name = format!(
                "pathway:output_table-{}-{}",
                data_sink.short_description(),
                data_formatter.short_description()
            );

            let worker_persistent_storage = self.worker_persistent_storage.clone();

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
                                    &worker_persistent_storage,
                                )?;
                            }
                            Ok(OutputEvent::Commit(t)) => {
                                Self::commit_output_time(
                                    &mut stats,
                                    t,
                                    sink_id,
                                    &worker_persistent_storage,
                                );
                                if t.is_none() {
                                    data_sink.flush(true).map_err(DynError::from)?;
                                    break Ok(());
                                }
                            }
                            Err(mpsc::RecvError) => break Ok(()),
                        }
                    },
                )
                .expect("output thread creation failed");
            self.connector_threads.push(output_joiner_handle);
            sender
        };

        output
            .inspect_core(move |event| {
                match event {
                    Ok((_time, batches)) => {
                        assert!(connector_does_output || batches.is_empty());
                        for batch in batches {
                            sender
                                .send(OutputEvent::Batch(batch.clone()))
                                .expect("sending output batch should not fail");
                        }
                    }
                    Err(frontier) => {
                        assert!(frontier.len() <= 1);
                        sender
                            .send(OutputEvent::Commit(frontier.first().copied()))
                            .expect("sending output commit should not fail");
                    }
                };
            })
            .probe_with(&self.output_probe);

        Ok(())
    }

    fn subscribe_table(
        &mut self,
        table_handle: TableHandle,
        column_paths: Vec<ColumnPath>,
        callbacks: SubscribeCallbacks,
        skip_persisted_batch: bool,
        skip_errors: bool,
    ) -> Result<()> {
        let worker_index = self.scope.index();

        let sink_id = self
            .worker_persistent_storage
            .as_ref()
            .map(|m| m.lock().unwrap().register_sink());
        let worker_persistent_storage = self.worker_persistent_storage.clone();
        let skip_initial_time = skip_persisted_batch && worker_persistent_storage.is_some();

        let error_reporter = self.error_reporter.clone();
        let error_reporter_2 = self.error_reporter.clone();
        let error_logger = self.create_error_logger()?;

        let SubscribeCallbacks {
            wrapper,
            mut on_data,
            mut on_time_end,
            mut on_end,
        } = callbacks;
        let wrapper_2 = wrapper.clone();

        let output_columns = self
            .extract_columns(table_handle, column_paths)?
            .as_collection();
        let output_columns = if skip_errors {
            output_columns.filter_out_errors(Some(error_logger))
        } else {
            output_columns
        };
        output_columns
            .consolidate_for_output(true)
            .inspect(move |batch| {
                if batch.time == ARTIFICIAL_TIME_ON_REWIND_START && skip_initial_time {
                    return;
                }
                wrapper
                    .run(|| -> DynResult<()> {
                        if let Some(on_data) = on_data.as_mut() {
                            for ((key, values), diff) in &batch.data {
                                on_data(*key, values, batch.time, *diff)?;
                            }
                        }
                        if let Some(on_time_end) = on_time_end.as_mut() {
                            on_time_end(batch.time)?;
                        }
                        Ok(())
                    })
                    .unwrap_with_reporter(&error_reporter);
            })
            .inspect_core(move |event| {
                // Another inspect, so we are looking at the first inspect's output frontier,
                // i.e., we are called after every worker has finished processing callbacks from
                // the first inspect for this frontier.
                if let Err(frontier) = event {
                    if worker_index == 0 && frontier.is_empty() {
                        if let Some(on_end) = on_end.as_mut() {
                            wrapper_2
                                .run(on_end)
                                .unwrap_with_reporter(&error_reporter_2);
                        }
                    }

                    assert!(frontier.len() <= 1);
                    let time_processed = frontier.first().copied();
                    if let Some(worker_persistent_storage) = &worker_persistent_storage {
                        worker_persistent_storage
                            .lock()
                            .unwrap()
                            .update_sink_finalized_time(
                                sink_id.expect("undefined sink_id while using persistent storage"),
                                time_processed,
                            );
                    }
                }
            })
            .probe_with(&self.output_probe);

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
            #[allow(clippy::default_trait_access)] // not really more readable
            let step = Product::new(Default::default(), 1);
            let subgraph = InnerDataflowGraph::new(
                subscope.clone(),
                self.error_reporter.clone(),
                self.ignore_asserts,
                self.config.clone(),
                self.terminate_on_error,
                self.current_error_log.clone(),
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

    fn error_log(
        &mut self,
        table_properties: Arc<TableProperties>,
    ) -> Result<(TableHandle, ErrorLogHandle)> {
        let mut input_session = InputSession::new();
        let collection = input_session.to_collection(&mut self.scope);
        let table_handle = self
            .tables
            .alloc(Table::from_collection(collection).with_properties(table_properties));
        let error_log = ErrorLog::new(input_session);
        let error_log_2 = error_log.clone();
        self.flushers
            .push(Box::new(move || error_log_2.maybe_flush()));
        let error_log_handle = self.error_logs.alloc(error_log);
        Ok((table_handle, error_log_handle))
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

    fn export_table(
        &mut self,
        table_handle: TableHandle,
        column_paths: Vec<ColumnPath>,
    ) -> Result<Arc<dyn ExportedTable>> {
        export_table(self, table_handle, column_paths)
    }

    fn import_table(&mut self, table: Arc<dyn ExportedTable>) -> Result<TableHandle> {
        import_table(self, table)
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
}

struct IteratedUniverse<O, I: MaybeTotalScope> {
    outer: PhantomData<*mut O>,
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
            inner_handle,
            keys_var,
        })
    }

    fn inner_handle(&self) -> UniverseHandle {
        self.inner_handle
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
}

struct ImportedColumn<O, I> {
    outer: PhantomData<*mut O>,
    inner: PhantomData<*mut I>,
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
            inner_handle,
        })
    }

    fn inner_handle(&self) -> ColumnHandle {
        self.inner_handle
    }
}

struct IteratedColumn<O, I: MaybeTotalScope> {
    outer: PhantomData<*mut O>,
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
            inner_handle,
            values_var,
        })
    }

    fn inner_handle(&self) -> ColumnHandle {
        self.inner_handle
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
    step: <I::Timestamp as TimestampTrait>::Summary,
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
        config: Arc<Config>,
        terminate_on_error: bool,
        default_error_log: Option<ErrorLog>,
    ) -> Result<Self> {
        Ok(Self(RefCell::new(DataflowGraphInner::new(
            scope,
            error_reporter,
            ignore_asserts,
            None,
            config,
            terminate_on_error,
            default_error_log,
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

    fn thread_count(&self) -> usize {
        self.0.borrow().thread_count()
    }

    fn process_count(&self) -> usize {
        self.0.borrow().process_count()
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
        deterministic: bool,
    ) -> Result<TableHandle> {
        if deterministic {
            self.0.borrow_mut().expression_table(
                table_handle,
                column_paths,
                expressions,
                wrapper,
                deterministic,
            )
        } else {
            Err(Error::NotSupportedInIteration)
        }
    }

    fn columns_to_table(
        &self,
        universe_handle: UniverseHandle,
        columns: Vec<ColumnHandle>,
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

    fn table_properties(
        &self,
        table_handle: TableHandle,
        path: &ColumnPath,
    ) -> Result<Arc<TableProperties>> {
        self.0.borrow_mut().table_properties(table_handle, path)
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
        deterministic: bool,
    ) -> Result<TableHandle> {
        self.0.borrow_mut().async_apply_table(
            function,
            table_handle,
            column_paths,
            table_properties,
            trace,
            deterministic,
        )
    }

    fn subscribe_table(
        &self,
        _table_handle: TableHandle,
        _column_paths: Vec<ColumnPath>,
        _callbacks: SubscribeCallbacks,
        _skip_persisted_batch: bool,
        _skip_errors: bool,
    ) -> Result<()> {
        Err(Error::IoNotPossible)
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

    fn remove_retractions_from_table(
        &self,
        table_handle: TableHandle,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        self.0
            .borrow_mut()
            .remove_retractions_from_table(table_handle, table_properties)
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
        shard_policy: ShardPolicy,
        reducers: Vec<ReducerData>,
        set_id: bool,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        self.0.borrow_mut().group_by_table(
            table_handle,
            grouping_columns_paths,
            shard_policy,
            reducers,
            set_id,
            table_properties,
        )
    }

    fn deduplicate(
        &self,
        _table_handle: TableHandle,
        _grouping_columns_paths: Vec<ColumnPath>,
        _reduced_column_paths: Vec<ColumnPath>,
        _combine_fn: StatefulCombineFn,
        _external_persistent_id: Option<&ExternalPersistentId>,
        _table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        Err(Error::NotSupportedInIteration)
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

    fn use_external_index_as_of_now(
        &self,
        index_stream: ExternalIndexData,
        query_stream: ExternalIndexQuery,
        table_properties: Arc<TableProperties>,
        external_index: Box<dyn ExternalIndex>,
    ) -> Result<TableHandle> {
        self.0.borrow_mut().use_external_index_as_of_now(
            index_stream,
            query_stream,
            table_properties,
            external_index,
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
        left_data: JoinData,
        right_data: JoinData,
        shard_policy: ShardPolicy,
        join_type: JoinType,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        self.0.borrow_mut().join_tables(
            left_data,
            right_data,
            shard_policy,
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

    fn output_table(
        &self,
        mut _data_sink: Box<dyn Writer>,
        mut _data_formatter: Box<dyn Formatter>,
        _table_handle: TableHandle,
        _column_paths: Vec<ColumnPath>,
    ) -> Result<()> {
        Err(Error::IoNotPossible)
    }

    fn set_operator_properties(&self, operator_properties: OperatorProperties) -> Result<()> {
        self.0
            .borrow_mut()
            .set_operator_properties(operator_properties)
    }

    fn set_error_log(&self, _error_log_handle: Option<ErrorLogHandle>) -> Result<()> {
        Err(Error::NotSupportedInIteration)
    }

    fn error_log(
        &self,
        _table_properties: Arc<TableProperties>,
    ) -> Result<(TableHandle, ErrorLogHandle)> {
        Err(Error::NotSupportedInIteration)
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

    fn export_table(
        &self,
        _table_handle: TableHandle,
        _column_paths: Vec<ColumnPath>,
    ) -> Result<Arc<dyn ExportedTable>> {
        Err(Error::IoNotPossible)
    }

    fn import_table(&self, _table: Arc<dyn ExportedTable>) -> Result<TableHandle> {
        Err(Error::IoNotPossible)
    }

    fn remove_errors_from_table(
        &self,
        table_handle: TableHandle,
        column_paths: Vec<ColumnPath>,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        self.0
            .borrow_mut()
            .remove_errors_from_table(table_handle, column_paths, table_properties)
    }
}

struct OuterDataflowGraph<S: MaybeTotalScope<MaybeTotalTimestamp = Timestamp>>(
    RefCell<DataflowGraphInner<S>>,
);

impl<S: MaybeTotalScope<MaybeTotalTimestamp = Timestamp>> OuterDataflowGraph<S> {
    pub fn new(
        scope: S,
        error_reporter: ErrorReporter,
        ignore_asserts: bool,
        persistence_config: Option<PersistenceManagerOuterConfig>,
        config: Arc<Config>,
        terminate_on_error: bool,
    ) -> Result<Self> {
        let worker_idx = scope.index();
        let total_workers = scope.peers();
        Ok(Self(RefCell::new(DataflowGraphInner::new(
            scope,
            error_reporter,
            ignore_asserts,
            persistence_config.map(|cfg| cfg.into_inner(worker_idx, total_workers)),
            config,
            terminate_on_error,
            None,
        )?)))
    }
}

impl<S: MaybeTotalScope<MaybeTotalTimestamp = Timestamp>> Graph for OuterDataflowGraph<S> {
    fn worker_index(&self) -> usize {
        self.0.borrow().worker_index()
    }

    fn worker_count(&self) -> usize {
        self.0.borrow().worker_count()
    }

    fn thread_count(&self) -> usize {
        self.0.borrow().thread_count()
    }

    fn process_count(&self) -> usize {
        self.0.borrow().process_count()
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
        deterministic: bool,
    ) -> Result<TableHandle> {
        self.0.borrow_mut().expression_table(
            table_handle,
            column_paths,
            expressions,
            wrapper,
            deterministic,
        )
    }

    fn columns_to_table(
        &self,
        universe_handle: UniverseHandle,
        columns: Vec<ColumnHandle>,
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

    fn table_properties(
        &self,
        table_handle: TableHandle,
        path: &ColumnPath,
    ) -> Result<Arc<TableProperties>> {
        self.0.borrow_mut().table_properties(table_handle, path)
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
        deterministic: bool,
    ) -> Result<TableHandle> {
        self.0.borrow_mut().async_apply_table(
            function,
            table_handle,
            column_paths,
            table_properties,
            trace,
            deterministic,
        )
    }

    fn subscribe_table(
        &self,
        table_handle: TableHandle,
        column_paths: Vec<ColumnPath>,
        callbacks: SubscribeCallbacks,
        skip_persisted_batch: bool,
        skip_errors: bool,
    ) -> Result<()> {
        self.0.borrow_mut().subscribe_table(
            table_handle,
            column_paths,
            callbacks,
            skip_persisted_batch,
            skip_errors,
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

    fn remove_retractions_from_table(
        &self,
        table_handle: TableHandle,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        self.0
            .borrow_mut()
            .remove_retractions_from_table(table_handle, table_properties)
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
        shard_policy: ShardPolicy,
        reducers: Vec<ReducerData>,
        set_id: bool,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        self.0.borrow_mut().group_by_table(
            table_handle,
            grouping_columns_paths,
            shard_policy,
            reducers,
            set_id,
            table_properties,
        )
    }

    fn deduplicate(
        &self,
        table_handle: TableHandle,
        grouping_columns_paths: Vec<ColumnPath>,
        reduced_column_paths: Vec<ColumnPath>,
        combine_fn: StatefulCombineFn,
        external_persistent_id: Option<&ExternalPersistentId>,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        self.0.borrow_mut().deduplicate(
            table_handle,
            grouping_columns_paths,
            reduced_column_paths,
            combine_fn,
            external_persistent_id,
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

    fn use_external_index_as_of_now(
        &self,
        index_stream: ExternalIndexData,
        query_stream: ExternalIndexQuery,
        table_properties: Arc<TableProperties>,
        external_index: Box<dyn ExternalIndex>,
    ) -> Result<TableHandle> {
        self.0.borrow_mut().use_external_index_as_of_now(
            index_stream,
            query_stream,
            table_properties,
            external_index,
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
        left_data: JoinData,
        right_data: JoinData,
        shard_policy: ShardPolicy,
        join_type: JoinType,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        self.0.borrow_mut().join_tables(
            left_data,
            right_data,
            shard_policy,
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

    fn set_operator_properties(&self, operator_properties: OperatorProperties) -> Result<()> {
        self.0
            .borrow_mut()
            .set_operator_properties(operator_properties)
    }

    fn set_error_log(&self, error_log_handle: Option<ErrorLogHandle>) -> Result<()> {
        self.0.borrow_mut().set_error_log(error_log_handle)
    }

    fn error_log(
        &self,
        table_properties: Arc<TableProperties>,
    ) -> Result<(TableHandle, ErrorLogHandle)> {
        self.0.borrow_mut().error_log(table_properties)
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

    fn export_table(
        &self,
        table_handle: TableHandle,
        column_paths: Vec<ColumnPath>,
    ) -> Result<Arc<dyn ExportedTable>> {
        self.0.borrow_mut().export_table(table_handle, column_paths)
    }

    fn import_table(&self, table: Arc<dyn ExportedTable>) -> Result<TableHandle> {
        self.0.borrow_mut().import_table(table)
    }

    fn remove_errors_from_table(
        &self,
        table_handle: TableHandle,
        column_paths: Vec<ColumnPath>,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        self.0
            .borrow_mut()
            .remove_errors_from_table(table_handle, column_paths, table_properties)
    }
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
    #[allow(unused)] license: &License,
    telemetry_config: TelemetryConfig,
    terminate_on_error: bool,
) -> Result<Vec<R2>>
where
    R: 'static,
    R2: Send + 'static,
{
    if !env::var("PATHWAY_SKIP_START_LOG").is_ok_and(|v| v == "1") {
        info!("Preparing Pathway computation");
    }

    #[allow(unknown_lints, clippy::const_is_empty)]
    if !YOLO.is_empty() {
        info!("Running in YOLO mode: {}", YOLO.iter().format(", "));
    }

    let config = Arc::new(config);
    let (error_reporter, error_receiver) = ErrorReporter::create();
    let failed = Arc::new(AtomicBool::new(false));
    let failed_2 = failed.clone();

    let guards = execute(config.to_timely_config(), move |worker| {
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
                mut flushers,
                mut pollers,
                connector_threads,
                connector_monitors,
                input_probe,
                output_probe,
                intermediate_probes,
                mut probers,
                progress_reporter_runner,
                http_server_runner,
                telemetry_runner,
            ) = worker.dataflow::<Timestamp, _, _>(|scope| {
                let graph = OuterDataflowGraph::new(
                    scope.clone(),
                    error_reporter.clone(),
                    ignore_asserts,
                    persistence_config.clone(),
                    config.clone(),
                    terminate_on_error,
                )
                .unwrap_with_reporter(&error_reporter);
                let telemetry_runner = maybe_run_telemetry_thread(&graph, telemetry_config.clone());
                let res = logic(&graph).unwrap_with_reporter(&error_reporter);
                let progress_reporter_runner =
                    maybe_run_reporter(&monitoring_level, &graph, stats_monitor.clone());
                let http_server_runner =
                    maybe_run_http_server_thread(with_http_server, &graph, config.process_id());
                let graph = graph.0.into_inner();
                (
                    res,
                    graph.flushers,
                    graph.pollers,
                    graph.connector_threads,
                    graph.connector_monitors,
                    graph.input_probe,
                    graph.output_probe,
                    graph.probes,
                    graph.probers,
                    progress_reporter_runner,
                    http_server_runner,
                    telemetry_runner,
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

                let next_step_duration_computer =
                    |next_commit_at: SystemTime, next_step_duration: Option<Duration>| {
                        let time_to_commit = next_commit_at
                            .duration_since(iteration_start)
                            .unwrap_or(Duration::ZERO);

                        Some(next_step_duration.map_or(time_to_commit, |x| min(x, time_to_commit)))
                    };

                for flusher in &mut flushers {
                    let next_flush_at = flusher();
                    next_step_duration =
                        next_step_duration_computer(next_flush_at, next_step_duration);
                }

                pollers.retain_mut(|poller| match poller() {
                    ControlFlow::Continue(None) => true,
                    ControlFlow::Continue(Some(next_commit_at)) => {
                        next_step_duration =
                            next_step_duration_computer(next_commit_at, next_step_duration);
                        true
                    }
                    ControlFlow::Break(()) => false,
                });

                if pollers.is_empty() {
                    //flushers don't know if they're no longer needed
                    //if there are no pollers left, computation is close to finishing
                    //so stop flushing and have the final flush at input session drop
                    flushers.clear();
                }

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
            drop(telemetry_runner);

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
