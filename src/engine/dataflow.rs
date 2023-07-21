#![allow(clippy::module_name_repetitions)]

mod complex_columns;
pub mod maybe_total;
pub mod operators;
pub mod shard;

use crate::connectors::data_format::{Formatter, Parser};
use crate::connectors::data_storage::{ReaderBuilder, Writer};
use crate::connectors::monitoring::{ConnectorMonitor, ConnectorStats};
use crate::connectors::ARTIFICIAL_TIME_ON_REWIND_START;
use crate::persistence::storage::{
    filesystem::SaveStatePolicy, FileSystem as FileSystemMetadataStorage,
};
use crate::persistence::tracker::{
    PersistenceManagerConfig, PersistencyManager, SimplePersistencyManager, StreamStorageConfig,
};

use std::any::type_name;
use std::borrow::{Borrow, Cow};
use std::cell::RefCell;
use std::cmp::min;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt::Display;
use std::iter::{once, zip};
use std::marker::PhantomData;
use std::ops::{ControlFlow, Deref};
use std::panic::{catch_unwind, panic_any, resume_unwind, AssertUnwindSafe};
use std::path::Path;
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
use differential_dataflow::input::{Input, InputSession};
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
use timely::order::Product;
use timely::progress::timestamp::Refines;
use timely::progress::Timestamp;
use timely::{execute, CommunicationConfig, Config, WorkerConfig};

use self::complex_columns::complex_columns;
use self::maybe_total::MaybeTotalScope;
use self::operators::prev_next::add_prev_next_pointers;
use self::operators::MaybeTotal;
use self::operators::{ArrangeWithTypes, MapWrapped};
use self::operators::{ConsolidateNondecreasing, Reshard};
use self::shard::Shard;
use super::error::{DynError, DynResult, Trace};
use super::expression::AnyExpression;
use super::graph::FlattenHandle;
use super::http_server::maybe_run_http_server_thread;
use super::progress_reporter::{maybe_run_reporter, MonitoringLevel};
use super::reduce::{
    AnyReducer, ArgMaxReducer, ArgMinReducer, IntSumReducer, MaxReducer, MinReducer, ReducerImpl,
    SemigroupReducerImpl, SortedTupleReducer, SumReducer, UniqueReducer,
};
use super::{
    BatchWrapper, ColumnHandle, ComplexColumn, ConcatHandle, Error, Expression, Graph,
    GrouperHandle, IterationLogic, IxKeyPolicy, IxerHandle, JoinType, JoinerHandle, Key,
    OperatorStats, ProberStats, Reducer, Result, Table, UniverseHandle, Value, VennUniverseHandle,
};
use crate::connectors::Connector;

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

    fn report(&self, error: Error) {
        self.sender.try_send(error).unwrap_or(());
    }

    #[track_caller]
    fn report_and_panic(&self, error: Error) -> ! {
        let message = error.to_string();
        self.report(error);
        panic_any(message);
    }

    #[track_caller]
    fn report_and_panic_with_trace(&self, error: DynError, trace: &Trace) -> ! {
        let message = error.to_string();
        self.report(Error::with_trace(error, trace.clone()));
        panic_any(message);
    }
}

trait UnwrapWithReporter<T> {
    fn unwrap_with_reporter(self, error_reporter: &ErrorReporter) -> T;
    fn unwrap_with_reporter_and_trace(self, error_reporter: &ErrorReporter, trace: &Trace) -> T;
}

impl<T> UnwrapWithReporter<T> for DynResult<T> {
    #[track_caller]
    fn unwrap_with_reporter(self, error_reporter: &ErrorReporter) -> T {
        self.unwrap_or_else(|err| error_reporter.report_and_panic(Error::from(err)))
    }
    #[track_caller]
    fn unwrap_with_reporter_and_trace(self, error_reporter: &ErrorReporter, trace: &Trace) -> T {
        self.unwrap_or_else(|err| error_reporter.report_and_panic_with_trace(err, trace))
    }
}

impl<T> UnwrapWithReporter<T> for Result<T> {
    #[track_caller]
    fn unwrap_with_reporter(self, error_reporter: &ErrorReporter) -> T {
        self.unwrap_or_else(|err| error_reporter.report_and_panic(err))
    }
    #[track_caller]
    fn unwrap_with_reporter_and_trace(self, error_reporter: &ErrorReporter, trace: &Trace) -> T {
        self.unwrap_or_else(|err| {
            error_reporter.report_and_panic_with_trace(DynError::from(err), trace)
        })
    }
}

type ArrangedBySelf<S, K, R = isize> =
    Arranged<S, TraceAgent<OrdKeySpine<K, <S as MaybeTotalScope>::MaybeTotalTimestamp, R>>>;
type ArrangedByKey<S, K, V, R = isize> =
    Arranged<S, TraceAgent<OrdValSpine<K, V, <S as MaybeTotalScope>::MaybeTotalTimestamp, R>>>;

type Session<S, D, R = isize> = InputSession<<S as MaybeTotalScope>::MaybeTotalTimestamp, D, R>;

type Var<S, D, R = isize> = Variable<S, D, R>;

type Keys<S> = Collection<S, Key>;
type KeysArranged<S> = ArrangedBySelf<S, Key>;
type KeysSession<S> = Session<S, Key>;
type KeysVar<S> = Var<S, Key>;
type GenericValues<S> = Collection<S, (Key, Value)>;
type ValuesArranged<S> = ArrangedByKey<S, Key, Value>;
type ValuesSession<S> = Session<S, (Key, Value)>;
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
        unpacked_pointer_collection: OnceCell<Collection<S, (Key, Key)>>,
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

    fn as_unpacked_pointers(
        &self,
        graph: &DataflowGraphInner<S>,
    ) -> Result<&Collection<S, (Key, Key)>> {
        match self {
            Values::Int { .. } => Err(Error::ColumnTypeMismatch {
                expected: "pointers",
                actual: "ints",
            }),
            Values::Pointer {
                pointer_collection, ..
            } => Ok(pointer_collection),
            Values::Generic {
                generic_collection,
                unpacked_pointer_collection,
                ..
            } => Ok(unpacked_pointer_collection.get_or_init(|| {
                let error_reporter = graph.error_reporter.clone();
                generic_collection.map_named(
                    "Values::as_unpacked_pointers(Generic)",
                    move |(key, value)| {
                        let value = value.as_pointer().unwrap_with_reporter(&error_reporter);
                        (key, value)
                    },
                )
            })),
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
        Values::Generic {
            generic_collection,
            unpacked_pointer_collection: OnceCell::new(),
        }
    }
}

type PersistentStorage = Option<Arc<Mutex<SimplePersistencyManager>>>;

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
    id_column: OnceCell<ColumnHandle>,
}

impl<S: MaybeTotalScope> Universe<S> {
    fn new(data: Rc<UniverseData<S>>) -> Self {
        Self {
            data,
            id_column: OnceCell::new(),
        }
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
    },
    Arranged {
        arranged: ValuesArranged<S>,
        collection: OnceCell<Values<S>>,
    },
    Universe {
        universe_data: Rc<UniverseData<S>>,
        collection: OnceCell<Values<S>>,
        arranged: OnceCell<ValuesArranged<S>>,
        consolidated: OnceCell<Values<S>>,
    },
}

impl<S: MaybeTotalScope> ColumnData<S> {
    fn from_collection(collection: Values<S>) -> Self {
        Self::Collection {
            collection,
            arranged: OnceCell::new(),
            consolidated: OnceCell::new(),
        }
    }

    fn from_arranged(arranged: ValuesArranged<S>) -> Self {
        Self::Arranged {
            collection: OnceCell::new(),
            arranged,
        }
    }

    fn from_universe(universe_data: Rc<UniverseData<S>>) -> Self {
        Self::Universe {
            collection: OnceCell::new(),
            arranged: OnceCell::new(),
            consolidated: OnceCell::new(),
            universe_data,
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
            Self::Universe {
                collection,
                universe_data,
                ..
            } => collection.get_or_init(|| {
                universe_data
                    .collection()
                    .map_named("UniverseData -> ColumnData", |key| (key, key))
                    .into()
            }),
        }
    }

    fn arranged(&self) -> &ValuesArranged<S> {
        match self {
            Self::Arranged { arranged, .. } => arranged,
            Self::Collection { arranged, .. } | Self::Universe { arranged, .. } => {
                arranged.get_or_init(|| self.collection().arrange())
            }
        }
    }

    fn consolidated(&self) -> &Values<S> {
        match self {
            Self::Collection { consolidated, .. } | Self::Universe { consolidated, .. } => {
                consolidated
                    .get_or_init(|| self.arranged().as_collection(|k, v| (*k, v.clone())).into())
            }
            Self::Arranged { .. } => self.collection(),
        }
    }
}

#[derive(Clone)]
struct Column<S: MaybeTotalScope> {
    universe: UniverseHandle,
    data: Rc<ColumnData<S>>,
}

impl<S: MaybeTotalScope> Column<S> {
    fn new(universe: UniverseHandle, data: Rc<ColumnData<S>>) -> Self {
        Self { universe, data }
    }

    fn from_collection(universe: UniverseHandle, values: impl Into<Values<S>>) -> Self {
        let data = Rc::new(ColumnData::from_collection(values.into()));
        Self { universe, data }
    }

    fn from_universe(universe_handle: UniverseHandle, universe: &Universe<S>) -> Self {
        let data = Rc::new(ColumnData::from_universe(universe.data.clone()));
        Self {
            universe: universe_handle,
            data,
        }
    }

    fn from_arranged(universe: UniverseHandle, values: ValuesArranged<S>) -> Self {
        let data = Rc::new(ColumnData::from_arranged(values));
        Self { universe, data }
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

struct Grouper<S: MaybeTotalScope> {
    source_universe: UniverseHandle,
    requested_columns: Vec<ColumnHandle>,
    result_keys_values: OnceCell<KeyWithTupleCollection<S>>,
    result_keys: OnceCell<Collection<S, Key>>,
    result_keys_values_arranged: OnceCell<KeyWithTupleArranged<S>>,
    result_universe: OnceCell<UniverseHandle>,
    source_key_to_result_key_values: KeyAndKeyWithTupleCollection<S>,
    source_key_to_result_key: OnceCell<ArrangedByKey<S, Key, Key>>,
}

impl<S: MaybeTotalScope> Grouper<S> {
    fn new(
        source_universe: UniverseHandle,
        requested_columns: Vec<ColumnHandle>,
        source_key_to_result_key_values: KeyAndKeyWithTupleCollection<S>,
    ) -> Self {
        Self {
            source_universe,
            requested_columns,
            result_keys_values: OnceCell::new(),
            result_keys: OnceCell::new(),
            result_keys_values_arranged: OnceCell::new(),
            result_universe: OnceCell::new(),
            source_key_to_result_key_values,
            source_key_to_result_key: OnceCell::new(),
        }
    }

    fn source_key_to_result_key(&self) -> &ArrangedByKey<S, Key, Key> {
        self.source_key_to_result_key.get_or_init(|| {
            self.source_key_to_result_key_values
                .as_key_collection()
                .arrange_named("Grouper::source_key_to_result_key")
        })
    }

    fn result_keys_values_arranged(&self) -> &KeyWithTupleArranged<S> {
        self.result_keys_values_arranged.get_or_init(|| {
            self.source_key_to_result_key_values
                .as_key_with_values_collection()
                .arrange_named("group_by::result_keys_values_arranged")
        })
    }

    fn result_keys_values(&self) -> &KeyWithTupleCollection<S> {
        self.result_keys_values
            .get_or_init(|| self.result_keys_values_arranged().distinct())
    }

    fn result_keys(&self) -> &Collection<S, Key> {
        self.result_keys
            .get_or_init(|| self.result_keys_values().as_key_collection())
    }
}

struct Ixer<S: MaybeTotalScope> {
    input_universe: UniverseHandle,
    output_universe: OnceCell<UniverseHandle>,
    keys_values: Collection<S, (Key, Key)>,
    none_keys: Option<Collection<S, Key>>,
    ix_key_policy: IxKeyPolicy,
    values_to_keys_arranged: OnceCell<ArrangedByKey<S, Key, Key>>,
    keys_to_values_arranged: OnceCell<ArrangedByKey<S, Key, Key>>,
}

impl<S: MaybeTotalScope> Ixer<S> {
    fn new(
        input_universe: UniverseHandle,
        output_universe: OnceCell<UniverseHandle>,
        keys_values: Collection<S, (Key, Key)>,
        none_keys: Option<Collection<S, Key>>,
        ix_key_policy: IxKeyPolicy,
    ) -> Self {
        Self {
            input_universe,
            output_universe,
            keys_values,
            none_keys,
            ix_key_policy,
            values_to_keys_arranged: OnceCell::new(),
            keys_to_values_arranged: OnceCell::new(),
        }
    }

    fn values_to_keys_arranged(&self) -> &ArrangedByKey<S, Key, Key> {
        self.values_to_keys_arranged.get_or_init(|| {
            self.keys_values
                .map_named("Ixer::values_to_keys_arranged", |(key, value)| (value, key))
                .arrange()
        })
    }

    fn keys_to_values_arranged(&self) -> &ArrangedByKey<S, Key, Key> {
        self.keys_to_values_arranged
            .get_or_init(|| self.keys_values.arrange())
    }
}
struct Joiner<S: MaybeTotalScope> {
    left_universe: UniverseHandle,
    right_universe: UniverseHandle,
    result_universe: UniverseHandle,
    left_result_total: Collection<S, (Key, Key)>,
    right_result_total: Collection<S, (Key, Key)>,
    left_key_to_result_key: OnceCell<ArrangedByKey<S, Key, Key>>,
    right_key_to_result_key: OnceCell<ArrangedByKey<S, Key, Key>>,
    left_filler: Option<Values<S>>,
    right_filler: Option<Values<S>>,
}

impl<S: MaybeTotalScope> Joiner<S> {
    fn new(
        left_universe: UniverseHandle,
        right_universe: UniverseHandle,
        result_universe: UniverseHandle,
        left_result_total: Collection<S, (Key, Key)>,
        right_result_total: Collection<S, (Key, Key)>,
        left_filler: Option<Values<S>>,
        right_filler: Option<Values<S>>,
    ) -> Self {
        Self {
            left_universe,
            right_universe,
            result_universe,
            left_result_total,
            right_result_total,
            left_key_to_result_key: OnceCell::new(),
            right_key_to_result_key: OnceCell::new(),
            left_filler,
            right_filler,
        }
    }

    fn left_key_to_result_key(&self) -> &ArrangedByKey<S, Key, Key> {
        self.left_key_to_result_key
            .get_or_init(|| self.left_result_total.arrange())
    }

    fn right_key_to_result_key(&self) -> &ArrangedByKey<S, Key, Key> {
        self.right_key_to_result_key
            .get_or_init(|| self.right_result_total.arrange())
    }
}

struct Concat {
    source_universes: Vec<UniverseHandle>,
    result_universe: UniverseHandle,
}

impl Concat {
    fn new(source_universes: Vec<UniverseHandle>, result_universe: UniverseHandle) -> Self {
        Self {
            source_universes,
            result_universe,
        }
    }
}

struct Flatten<S: MaybeTotalScope> {
    universe_handle: UniverseHandle,
    source_key_to_result_key_values: KeyAndKeyWithTupleCollection<S>,
    source_key_to_result_key: OnceCell<ArrangedByKey<S, Key, Key>>,
}

impl<S: MaybeTotalScope> Flatten<S> {
    fn new(
        universe_handle: UniverseHandle,
        source_key_to_result_key_values: KeyAndKeyWithTupleCollection<S>,
    ) -> Self {
        Self {
            universe_handle,
            source_key_to_result_key_values,
            source_key_to_result_key: OnceCell::new(),
        }
    }

    fn source_key_to_result_key(&self) -> &ArrangedByKey<S, Key, Key> {
        self.source_key_to_result_key.get_or_init(|| {
            self.source_key_to_result_key_values
                .as_key_collection()
                .arrange_named("Flatten::source_key_to_result_key")
        })
    }
}

pub struct VennUniverse<S: MaybeTotalScope> {
    left_keys: Collection<S, Key>,
    right_keys: Collection<S, Key>,
    both_keys: Collection<S, Key>,
    only_left_universe: OnceCell<UniverseHandle>,
    only_right_universe: OnceCell<UniverseHandle>,
}

impl<S: MaybeTotalScope> VennUniverse<S> {
    fn new(
        left_keys: Collection<S, Key>,
        right_keys: Collection<S, Key>,
        both_keys: Collection<S, Key>,
    ) -> Self {
        Self {
            left_keys,
            right_keys,
            both_keys,
            only_left_universe: OnceCell::new(),
            only_right_universe: OnceCell::new(),
        }
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
    groupers: Arena<Grouper<S>, GrouperHandle>,
    joiners: Arena<Joiner<S>, JoinerHandle>,
    ixers: Arena<Ixer<S>, IxerHandle>,
    concats: Arena<Concat, ConcatHandle>,
    flattens: Arena<Flatten<S>, FlattenHandle>,
    venn_universes: Arena<VennUniverse<S>, VennUniverseHandle>,
    pollers: Vec<Box<dyn FnMut() -> ControlFlow<(), Option<SystemTime>>>>,
    connector_threads: Vec<JoinHandle<DynResult<()>>>,
    connector_monitors: Vec<Rc<RefCell<ConnectorMonitor>>>,
    error_reporter: ErrorReporter,
    input_probe: ProbeHandle<S::Timestamp>,
    output_probe: ProbeHandle<S::Timestamp>,
    probers: Vec<Prober>,
    probes: HashMap<usize, ProbeHandle<S::Timestamp>>,
    ixers_cache: HashMap<(ColumnHandle, UniverseHandle, IxKeyPolicy), IxerHandle>,
    groupers_cache: HashMap<(Vec<ColumnHandle>, Vec<ColumnHandle>, UniverseHandle), GrouperHandle>,
    groupers_id_cache: HashMap<(ColumnHandle, Vec<ColumnHandle>, UniverseHandle), GrouperHandle>,
    groupers_ixers_cache: HashMap<(GrouperHandle, IxerHandle), ArrangedByKey<S, Key, Key>>,
    concat_cache: HashMap<Vec<UniverseHandle>, ConcatHandle>,
    joiners_cache: HashMap<
        (
            Vec<ColumnHandle>,
            UniverseHandle,
            Vec<ColumnHandle>,
            UniverseHandle,
            JoinType,
        ),
        JoinerHandle,
    >,
    ignore_asserts: bool,
    persistent_storage: PersistentStorage,
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
    fn map<D: Data>(
        &self,
        mut logic: impl FnMut(Key, &[Value]) -> D + 'static,
    ) -> Collection<S, D> {
        match self {
            Self::Zero(c) => c.map_named("TupleCollection::map", move |key| {
                logic(key, ().as_value_slice())
            }),
            Self::One(c) => c.map_named("TupleCollection::map", move |(key, value)| {
                logic(key, value.as_value_slice())
            }),
            Self::Two(c) => c.map_named("TupleCollection::map", move |(key, values)| {
                logic(key, values.as_value_slice())
            }),
            Self::More(c) => c.map_named("TupleCollection::map", move |(key, values)| {
                logic(key, values.as_value_slice())
            }),
        }
    }

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

    #[track_caller]
    fn as_key_and_key_with_value_subset_collection(
        &self,
        subset: Vec<usize>,
    ) -> KeyAndKeyWithTupleCollection<S> {
        #[allow(clippy::match_same_arms)] // more readable this way
        match (self, subset.as_slice()) {
            (Self::Zero(c), &[]) => KeyAndKeyWithTupleCollection::Zero(c.map_named(
                "TupleCollection::as_key_and_key_with_value_subset_collection(Zero -> Zero)",
                |k| (k, ().key()),
            )),
            (Self::Zero(_), _) => unreachable!(),
            (Self::One(c), &[]) => KeyAndKeyWithTupleCollection::Zero(c.map_named(
                "TupleCollection::as_key_and_key_with_value_subset_collection(One -> Zero)",
                |(k, v)| (k, v.key()),
            )),
            (Self::One(c), &[0]) => KeyAndKeyWithTupleCollection::One(c.map_named(
                "TupleCollection::as_key_and_key_with_value_subset_collection(One -> One)",
                |(k, v)| (k, KeyWith(v.key(), v)),
            )),
            (Self::One(_), _) => unreachable!(),
            (Self::Two(c), &[]) => KeyAndKeyWithTupleCollection::Zero(c.map_named(
                "TupleCollection::as_key_and_key_with_value_subset_collection(Two -> Zero)",
                |(k, vs)| (k, vs.key()),
            )),
            (Self::Two(c), &[0]) => {
                KeyAndKeyWithTupleCollection::One(c.map_named(
                    "TupleCollection::as_key_and_key_with_value_subset_collection(Two -> One)",
                    |(k, vs)| (k, KeyWith(vs.key(), vs[0].clone())),
                )) // XXX
            }
            (Self::Two(c), &[1]) => {
                KeyAndKeyWithTupleCollection::One(c.map_named(
                    "TupleCollection::as_key_and_key_with_value_subset_collection(Two -> One)",
                    |(k, vs)| (k, KeyWith(vs.key(), vs[1].clone())),
                )) // XXX
            }
            (Self::Two(c), &[0, 1]) => KeyAndKeyWithTupleCollection::Two(c.map_named(
                "TupleCollection::as_key_and_key_with_value_subset_collection(Two -> Two)",
                |(k, vs)| (k, KeyWith(vs.key(), vs)),
            )),
            (Self::Two(_), _) => unreachable!(),
            (Self::More(c), &[]) => KeyAndKeyWithTupleCollection::Zero(c.map_named(
                "TupleCollection::as_key_and_key_with_value_subset_collection(More -> Zero)",
                |(k, vs)| (k, vs.key()),
            )),
            (Self::More(c), &[i]) => KeyAndKeyWithTupleCollection::One(c.map_named(
                "TupleCollection::as_key_and_key_with_value_subset_collection(More -> One)",
                move |(k, vs)| (k, KeyWith(vs.key(), vs[i].clone())),
            )),
            (Self::More(c), &[i, j]) => KeyAndKeyWithTupleCollection::Two(c.map_named(
                "TupleCollection::as_key_and_key_with_value_subset_collection(More -> Two)",
                move |(k, vs)| (k, KeyWith(vs.key(), [vs[i].clone(), vs[j].clone()])),
            )),
            (Self::More(c), _) => {
                // XXX:optimize for identity
                KeyAndKeyWithTupleCollection::More(c.map_named(
                    "TupleCollection::as_key_and_key_with_value_subset_collection(More -> More)",
                    move |(k, vs)| {
                        let values = subset.iter().map(|i| vs[*i].clone()).collect();
                        (k, KeyWith(vs.key(), values))
                    },
                ))
            }
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

enum KeyAndKeyWithTupleCollection<S: MaybeTotalScope> {
    Zero(Collection<S, (Key, Key)>),
    One(Collection<S, (Key, KeyWith<Value>)>),
    Two(Collection<S, (Key, KeyWith<[Value; 2]>)>),
    More(Collection<S, (Key, KeyWith<Arc<[Value]>>)>),
}

impl<S: MaybeTotalScope> KeyAndKeyWithTupleCollection<S> {
    #[track_caller]
    fn as_key_collection(&self) -> Collection<S, (Key, Key)> {
        match self {
            Self::Zero(c) => c.clone(),
            Self::One(c) => c.map_named(
                "KeyAndKeyWithTupleCollection::as_key_collection(One)",
                |(k1, KeyWith(k2, _))| (k1, k2),
            ),
            Self::Two(c) => c.map_named(
                "KeyAndKeyWithTupleCollection::as_key_collection(Two)",
                |(k1, KeyWith(k2, _))| (k1, k2),
            ),
            Self::More(c) => c.map_named(
                "KeyAndKeyWithTupleCollection::as_key_collection(More)",
                |(k1, KeyWith(k2, _))| (k1, k2),
            ),
        }
    }

    #[track_caller]
    fn as_key_with_values_collection(&self) -> KeyWithTupleCollection<S> {
        match self {
            Self::Zero(c) => KeyWithTupleCollection::Zero(c.map_named(
                "KeyAndKeyWithTupleCollection::as_key_with_values_collection(Zero)",
                |(_k1, k2)| k2,
            )),
            Self::One(c) => KeyWithTupleCollection::One(c.map_named(
                "KeyAndKeyWithTupleCollection::as_key_with_values_collection(One)",
                |(_k, kv)| kv,
            )),
            Self::Two(c) => KeyWithTupleCollection::Two(c.map_named(
                "KeyAndKeyWithTupleCollection::as_key_with_values_collection(Two)",
                |(_k, kv)| kv,
            )),
            Self::More(c) => KeyWithTupleCollection::More(c.map_named(
                "KeyAndKeyWithTupleCollection::as_key_with_values_collection(More)",
                |(_k, kv)| kv,
            )),
        }
    }
}

enum KeyWithTupleCollection<S: MaybeTotalScope> {
    Zero(Collection<S, Key>),
    One(Collection<S, KeyWith<Value>>),
    Two(Collection<S, KeyWith<[Value; 2]>>),
    More(Collection<S, KeyWith<Arc<[Value]>>>),
}

impl<S: MaybeTotalScope> KeyWithTupleCollection<S> {
    fn arrange_named(&self, name: &str) -> KeyWithTupleArranged<S> {
        match self {
            Self::Zero(c) => KeyWithTupleArranged::Zero(c.arrange_named(name)),
            Self::One(c) => KeyWithTupleArranged::One(c.arrange_named(name)),
            Self::Two(c) => KeyWithTupleArranged::Two(c.arrange_named(name)),
            Self::More(c) => KeyWithTupleArranged::More(c.arrange_named(name)),
        }
    }

    fn as_key_collection(&self) -> Collection<S, Key> {
        match self {
            Self::Zero(c) => c.clone(),
            Self::One(c) => c.map_named(
                "KeyWithTupleCollection::as_key_collection(One)",
                |KeyWith(k, _v)| k,
            ),
            Self::Two(c) => c.map_named(
                "KeyWithTupleCollection::as_key_collection(Two)",
                |KeyWith(k, _v)| k,
            ),
            Self::More(c) => c.map_named(
                "KeyWithTupleCollection::as_key_collection(More)",
                |KeyWith(k, _v)| k,
            ),
        }
    }

    fn as_key_with_value_collection(&self, index: usize) -> Values<S> {
        match self {
            Self::Zero(c) => {
                // XXX
                assert_eq!(index, 0);
                c.map_named(
                    "KeyWithTupleCollection::as_key_with_value_collection(Zero)",
                    |k| (k, Value::from(k)),
                )
            }
            Self::One(c) => {
                assert_eq!(index, 0);
                c.map_named(
                    "KeyWithTupleCollection::as_key_with_value_collection(One)",
                    |KeyWith(k, v)| (k, v),
                )
            }
            Self::Two(c) => match index {
                0 => c.map_named(
                    "KeyWithTupleCollection::as_key_with_value_collection(Two)",
                    move |KeyWith(k, [v0, _v1])| (k, v0),
                ),
                1 => c.map_named(
                    "KeyWithTupleCollection::as_key_with_value_collection(Two)",
                    move |KeyWith(k, [_v0, v1])| (k, v1),
                ),
                _ => panic!("index out of range: {index}"),
            },
            Self::More(c) => c.map_named(
                "KeyWithTupleCollection::as_key_with_value_collection(More)",
                move |KeyWith(k, v)| (k, v[index].clone()),
            ),
        }
        .into()
    }
}

enum KeyWithTupleArranged<S: MaybeTotalScope> {
    Zero(ArrangedBySelf<S, Key>),
    One(ArrangedBySelf<S, KeyWith<Value>>),
    Two(ArrangedBySelf<S, KeyWith<[Value; 2]>>),
    More(ArrangedBySelf<S, KeyWith<Arc<[Value]>>>),
}

impl<S: MaybeTotalScope> KeyWithTupleArranged<S> {
    fn distinct(&self) -> KeyWithTupleCollection<S> {
        match self {
            Self::Zero(a) => KeyWithTupleCollection::Zero(a.distinct()),
            Self::One(a) => KeyWithTupleCollection::One(a.distinct()),
            Self::Two(a) => KeyWithTupleCollection::Two(a.distinct()),
            Self::More(a) => KeyWithTupleCollection::More(a.distinct()),
        }
    }

    fn count_keys(&self) -> Collection<S, (Key, isize)> {
        match self {
            Self::Zero(a) => a.count(),
            Self::One(a) => a.count().map_named(
                "KeyWithTupleArranged::count_keys(One)",
                |(KeyWith(k, _), c)| (k, c),
            ),
            Self::Two(a) => a.count().map_named(
                "KeyWithTupleArranged::count_keys(Two)",
                |(KeyWith(k, _), c)| (k, c),
            ),
            Self::More(a) => a.count().map_named(
                "KeyWithTupleArranged::count_keys(More)",
                |(KeyWith(k, _), c)| (k, c),
            ),
        }
    }
}

#[allow(clippy::unnecessary_wraps)] // we want to always return Result for symmetry
impl<S: MaybeTotalScope> DataflowGraphInner<S> {
    fn new(scope: S, error_reporter: ErrorReporter, ignore_asserts: bool) -> Self {
        let persistent_storage = Self::construct_persistent_storage(scope.index());
        Self {
            scope,
            universes: Arena::new(),
            columns: Arena::new(),
            groupers: Arena::new(),
            joiners: Arena::new(),
            ixers: Arena::new(),
            concats: Arena::new(),
            flattens: Arena::new(),
            venn_universes: Arena::new(),
            pollers: Vec::new(),
            connector_threads: Vec::new(),
            connector_monitors: Vec::new(),
            error_reporter,
            input_probe: ProbeHandle::new(),
            output_probe: ProbeHandle::new(),
            probers: Vec::new(),
            probes: HashMap::new(),
            ixers_cache: HashMap::new(),
            groupers_cache: HashMap::new(),
            groupers_id_cache: HashMap::new(),
            groupers_ixers_cache: HashMap::new(),
            concat_cache: HashMap::new(),
            joiners_cache: HashMap::new(),
            ignore_asserts,
            persistent_storage,
        }
    }

    fn worker_index(&self) -> usize {
        self.scope.index()
    }

    fn worker_count(&self) -> usize {
        self.scope.peers()
    }

    fn construct_persistent_storage(worker_id: usize) -> PersistentStorage {
        let persistent_storage_path = std::env::var("PATHWAY_PERSISTENT_STORAGE").ok()?;

        let storage = FileSystemMetadataStorage::new(
            Path::new(&persistent_storage_path),
            worker_id,
            SaveStatePolicy::Background(Duration::from_secs(1)),
        )
        .expect("Persistent storage failed to initialize. Worker id: {worker_id}");

        let persistency_manager = Arc::new(Mutex::new(SimplePersistencyManager::new(
            Box::new(storage),
            PersistenceManagerConfig::new(
                StreamStorageConfig::Filesystem(persistent_storage_path.into()),
                worker_id,
            ),
        )));
        Some(persistency_manager)
    }

    fn empty_universe(&mut self) -> Result<UniverseHandle> {
        self.static_universe(Vec::new())
    }

    fn empty_column(&mut self, universe_handle: UniverseHandle) -> Result<ColumnHandle> {
        self.static_column(universe_handle, Vec::new())
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

        let column_handle = self
            .columns
            .alloc(Column::from_collection(universe_handle, values));
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
                    return Ok(TupleCollection::Zero(universe.keys().clone()))
                };
                let Some(second_column) = columns.next() else {
                    return Ok(TupleCollection::One(first_column.values().as_generic().clone()))
                };
                let two = first_column
                    .values_arranged()
                    .join_core(second_column.values_arranged(), |key, first, second| {
                        once((*key, [first.clone(), second.clone()]))
                    });
                let Some(third_column) = columns.next() else {
                    return Ok(TupleCollection::Two(two))
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

    fn expression_column(
        &mut self,
        wrapper: BatchWrapper,
        expression: Arc<Expression>,
        universe_handle: UniverseHandle,
        column_handles: &[ColumnHandle],
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
            let column_handle = self
                .columns
                .alloc(Column::from_collection(universe_handle, values));
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

        let new_column_handle = self
            .columns
            .alloc(Column::from_collection(universe_handle, new_values));
        Ok(new_column_handle)
    }

    fn async_apply_column(
        &mut self,
        function: Arc<dyn Fn(Key, &[Value]) -> BoxFuture<'static, DynResult<Value>> + Send + Sync>,
        universe_handle: UniverseHandle,
        column_handles: &[ColumnHandle],
        trace: Trace,
    ) -> Result<ColumnHandle> where {
        let error_reporter = self.error_reporter.clone();
        let trace = Arc::new(trace);
        let values = self
            .tuples(universe_handle, column_handles)?
            .as_collection()
            .map_named_async("expression_column::apply_async", move |(key, values)| {
                let error_reporter = error_reporter.clone();
                let function = function.clone();
                let trace = trace.clone();
                let future = async move {
                    let value = async {
                        function(key, &values)
                            .await
                            .unwrap_with_reporter_and_trace(&error_reporter, &trace)
                    }
                    .await;
                    (key, value)
                };
                Box::pin(future)
            });

        let new_column_handle = self
            .columns
            .alloc(Column::from_collection(universe_handle, values));
        Ok(new_column_handle)
    }

    fn id_column(&mut self, universe_handle: UniverseHandle) -> Result<ColumnHandle> {
        let universe = self
            .universes
            .get(universe_handle)
            .ok_or(Error::InvalidUniverseHandle)?;
        let column_handle = *universe.id_column.get_or_init(|| {
            self.columns
                .alloc(Column::from_universe(universe_handle, universe))
        });
        Ok(column_handle)
    }

    fn filter_universe(
        &mut self,
        universe_handle: UniverseHandle,
        column_handle: ColumnHandle,
    ) -> Result<UniverseHandle> {
        let column = self
            .columns
            .get(column_handle)
            .ok_or(Error::InvalidColumnHandle)?;
        if column.universe != universe_handle {
            return Err(Error::InvalidUniverseHandle);
        }
        let new_keys = column.values().flat_map(|(key, value)| match value {
            Value::Bool(false) => None,
            Value::Bool(true) => Some(key),
            _ => panic!("Expected boolean, got {value}"),
        });
        let new_universe_handle = self.universes.alloc(Universe::from_collection(new_keys));
        Ok(new_universe_handle)
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

    fn intersect_universe(
        &mut self,
        universe_handles: Vec<UniverseHandle>,
    ) -> Result<UniverseHandle> {
        let mut universe_handles = universe_handles.into_iter();
        let new_keys = match universe_handles.next() {
            None => {
                return Err(Error::EmptyIntersection);
            }
            Some(universe_handle) => {
                let universe = self
                    .universes
                    .get(universe_handle)
                    .ok_or(Error::InvalidUniverseHandle)?;
                let mut new_keys = universe.data.clone();
                for universe_handle in universe_handles {
                    let universe = self
                        .universes
                        .get(universe_handle)
                        .ok_or(Error::InvalidUniverseHandle)?;
                    new_keys = Rc::new(UniverseData::from_collection(
                        new_keys
                            .arranged()
                            .join_core(universe.keys_arranged(), |k, (), ()| once(*k)),
                    ));
                }
                new_keys
            }
        };

        let new_universe_handle = self.universes.alloc(Universe::new(new_keys));
        Ok(new_universe_handle)
    }

    fn union_universe(&mut self, universe_handles: &[UniverseHandle]) -> Result<UniverseHandle> {
        if universe_handles.is_empty() {
            return self.empty_universe();
        }

        let universe_collections: Vec<Keys<S>> = universe_handles
            .iter()
            .map(|universe_handle| {
                let universe = self
                    .universes
                    .get(*universe_handle)
                    .ok_or(Error::InvalidUniverseHandle)?;
                Ok(universe.keys().clone())
            })
            .collect::<Result<_>>()?;

        if universe_collections.len() == 1 {
            return Ok(universe_handles[0]);
        }

        let new_universe = Universe::from_collection(
            concatenate(&mut self.scope, universe_collections).distinct(),
        );
        let new_universe_handle = self.universes.alloc(new_universe);

        Ok(new_universe_handle)
    }

    fn reindex_universe(&mut self, column_handle: ColumnHandle) -> Result<UniverseHandle> {
        let column = self
            .columns
            .get(column_handle)
            .ok_or(Error::InvalidColumnHandle)?;

        let error_reporter = self.error_reporter.clone();
        let new_universe_keys = column.values().map_named(
            "reindex_universe::new_universe_keys",
            move |(_key, value)| value.as_pointer().unwrap_with_reporter(&error_reporter),
        );

        if !self.ignore_asserts {
            self.assert_keys_are_distinct(&new_universe_keys);
        };
        let new_universe_handle = self
            .universes
            .alloc(Universe::from_collection(new_universe_keys));

        Ok(new_universe_handle)
    }

    fn venn_universes(
        &mut self,
        left_universe_handle: UniverseHandle,
        right_universe_handle: UniverseHandle,
    ) -> Result<VennUniverseHandle> {
        //TODO add caching of the whole structure
        let left_universe = self
            .universes
            .get(left_universe_handle)
            .ok_or(Error::InvalidUniverseHandle)?;

        let right_universe = self
            .universes
            .get(right_universe_handle)
            .ok_or(Error::InvalidUniverseHandle)?;

        let both_keys = left_universe
            .keys_arranged()
            .join_core(right_universe.keys_arranged(), |k, (), ()| once(*k));

        let venn_universes = VennUniverse::new(
            left_universe.keys().clone(),
            right_universe.keys().clone(),
            both_keys,
        );
        let venn_universes_handle = self.venn_universes.alloc(venn_universes);
        Ok(venn_universes_handle)
    }

    fn venn_universes_only_left(
        &mut self,
        venn_universes_handle: VennUniverseHandle,
    ) -> Result<UniverseHandle> {
        let venn_universe = self
            .venn_universes
            .get(venn_universes_handle)
            .ok_or(Error::InvalidVennUniversesHandle)?;
        Ok(*venn_universe.only_left_universe.get_or_init(|| {
            self.universes.alloc(Universe::from_collection(
                venn_universe
                    .left_keys
                    .concat(&venn_universe.both_keys.negate()),
            ))
        }))
    }

    fn venn_universes_only_right(
        &mut self,
        venn_universes_handle: VennUniverseHandle,
    ) -> Result<UniverseHandle> {
        let venn_universe = self
            .venn_universes
            .get(venn_universes_handle)
            .ok_or(Error::InvalidVennUniversesHandle)?;
        Ok(*venn_universe.only_right_universe.get_or_init(|| {
            self.universes.alloc(Universe::from_collection(
                venn_universe
                    .right_keys
                    .concat(&venn_universe.both_keys.negate()),
            ))
        }))
    }

    fn venn_universes_both(
        &mut self,
        venn_universes_handle: VennUniverseHandle,
    ) -> Result<UniverseHandle> {
        let venn_universe = self
            .venn_universes
            .get(venn_universes_handle)
            .ok_or(Error::InvalidVennUniversesHandle)?;
        Ok(self
            .universes
            .alloc(Universe::from_collection(venn_universe.both_keys.clone())))
    }

    fn concat(&mut self, universe_handles: Vec<UniverseHandle>) -> Result<ConcatHandle> {
        if let Some(val) = self.concat_cache.get(&universe_handles) {
            return Ok(*val);
        }
        let universe_collections: Vec<Keys<S>> = universe_handles
            .iter()
            .map(|universe_handle| {
                let universe = self
                    .universes
                    .get(*universe_handle)
                    .ok_or(Error::InvalidUniverseHandle)?;
                Ok(universe.keys().clone())
            })
            .collect::<Result<_>>()?;

        let result_keys = concatenate(&mut self.scope, universe_collections);
        if !self.ignore_asserts {
            self.assert_keys_are_distinct(&result_keys);
        };
        let result_universe = Universe::from_collection(result_keys);
        let result_universe_handle = self.universes.alloc(result_universe);

        let concat_handle = self.concats.alloc(Concat::new(
            universe_handles.clone(),
            result_universe_handle,
        ));
        self.concat_cache.insert(universe_handles, concat_handle);
        Ok(concat_handle)
    }

    fn concat_universe(&mut self, concat_handle: ConcatHandle) -> Result<UniverseHandle> {
        let concat = self
            .concats
            .get(concat_handle)
            .ok_or(Error::InvalidConcatHandle)?;
        Ok(concat.result_universe)
    }

    fn concat_column(
        &mut self,
        concat_handle: ConcatHandle,
        column_handles: &[ColumnHandle],
    ) -> Result<ColumnHandle> {
        let concat = self
            .concats
            .get(concat_handle)
            .ok_or(Error::InvalidConcatHandle)?;
        if concat.source_universes.len() != column_handles.len() {
            return Err(Error::LengthMismatch);
        }
        let column_collections: Vec<_> = zip(column_handles, &concat.source_universes)
            .map(|(column_handle, universe_handle)| {
                let column = self
                    .columns
                    .get(*column_handle)
                    .ok_or(Error::InvalidColumnHandle)?;
                if &column.universe != universe_handle {
                    return Err(Error::UniverseMismatch);
                }
                Ok(column.values().as_generic().clone())
            })
            .collect::<Result<_>>()?;

        let column = Column::from_collection(
            concat.result_universe,
            concatenate(&mut self.scope, column_collections),
        );
        let column_handle = self.columns.alloc(column);
        Ok(column_handle)
    }

    fn flatten(
        &mut self,
        column_handle: ColumnHandle,
    ) -> Result<(UniverseHandle, ColumnHandle, FlattenHandle)> {
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

        let column = self
            .columns
            .get(column_handle)
            .ok_or(Error::InvalidColumnHandle)?;

        let source_key_to_result_key_value = column.values().flat_map(|(key, value)| {
            let wrapped = match value {
                Value::IntArray(array) => flatten_ndarray(&array),
                Value::FloatArray(array) => flatten_ndarray(&array),
                Value::Tuple(array) => (*array).to_vec(),
                Value::String(s) => (*s)
                    .chars()
                    .map(|c| Value::from(ArcStr::from(c.to_string())))
                    .collect(),
                _ => panic!("Pathway can't flatten this value {value:?}"),
            };
            wrapped.into_iter().enumerate().map(move |(i, value)| {
                (
                    key,
                    KeyWith(
                        Tuple::Two([Value::from(key), Value::from(i as i64)]).key(),
                        value,
                    ),
                )
            })
        });
        let source_key_to_result_key_value_wrapped =
            KeyAndKeyWithTupleCollection::One(source_key_to_result_key_value);

        let new_keys_values =
            source_key_to_result_key_value_wrapped.as_key_with_values_collection();
        let new_keys = new_keys_values.as_key_collection();
        let new_universe_handle = self.universes.alloc(Universe::from_collection(new_keys));

        let new_column = Column::from_collection(
            new_universe_handle,
            new_keys_values.as_key_with_value_collection(0),
        );
        let new_column_handle = self.columns.alloc(new_column);

        let flatten_handle = self.flattens.alloc(Flatten::new(
            new_universe_handle,
            source_key_to_result_key_value_wrapped,
        ));

        Ok((new_universe_handle, new_column_handle, flatten_handle))
    }

    fn explode(
        &mut self,
        flatten_handle: FlattenHandle,
        column_handle: ColumnHandle,
    ) -> Result<ColumnHandle> {
        let flatten = self
            .flattens
            .get(flatten_handle)
            .ok_or(Error::InvalidConcatHandle)?;

        let column = self
            .columns
            .get(column_handle)
            .ok_or(Error::InvalidColumnHandle)?;

        let new_values = flatten.source_key_to_result_key().join_core(
            column.values_arranged(),
            |_source_key, result_key, value| once((*result_key, value.clone())),
        );

        let new_column_handle = self
            .columns
            .alloc(Column::from_collection(flatten.universe_handle, new_values));
        Ok(new_column_handle)
    }

    fn sort(
        &mut self,
        key_column_handle: ColumnHandle,
        instance_column_handle: ColumnHandle,
    ) -> Result<(ColumnHandle, ColumnHandle)> {
        let universe_handle = self
            .columns
            .get(key_column_handle)
            .ok_or(Error::InvalidColumnHandle)?
            .universe;
        let instance_key_id_arranged = self
            .tuples(
                universe_handle,
                &[instance_column_handle, key_column_handle],
            )?
            .map(|id, tuple| SortingCell::new(tuple[0].clone(), tuple[1].clone(), id))
            .arrange();

        let prev_next =
            add_prev_next_pointers(instance_key_id_arranged, &|a, b| a.instance == b.instance);
        let prev_values = prev_next.as_collection(|current, prev_next| {
            (
                current.id,
                prev_next
                    .0
                    .clone()
                    .map_or(Value::None, |prev| Value::Pointer(prev.id)),
            )
        });
        let next_values = prev_next.as_collection(|current, prev_next| {
            (
                current.id,
                prev_next
                    .1
                    .clone()
                    .map_or(Value::None, |next| Value::Pointer(next.id)),
            )
        });
        let prev_column_handle = self
            .columns
            .alloc(Column::from_collection(universe_handle, prev_values));
        let next_column_handle = self
            .columns
            .alloc(Column::from_collection(universe_handle, next_values));

        Ok((prev_column_handle, next_column_handle))
    }

    fn reindex_column(
        &mut self,
        column_to_reindex_handle: ColumnHandle,
        reindexing_column_handle: ColumnHandle,
        reindexing_universe_handle: UniverseHandle,
    ) -> Result<ColumnHandle> {
        let column_to_reindex = self
            .columns
            .get(column_to_reindex_handle)
            .ok_or(Error::InvalidColumnHandle)?;

        let reindexing_column = self
            .columns
            .get(reindexing_column_handle)
            .ok_or(Error::InvalidColumnHandle)?;

        let reindexing_universe = self
            .universes
            .get(reindexing_universe_handle)
            .ok_or(Error::InvalidUniverseHandle)?;

        let error_reporter = self.error_reporter.clone();
        let result_collection = reindexing_column.values_arranged().join_core(
            column_to_reindex.values_arranged(),
            move |_key, value_reindexing_column, value_column_to_reindex| {
                once((
                    value_reindexing_column
                        .as_pointer()
                        .unwrap_with_reporter(&error_reporter),
                    value_column_to_reindex.clone(),
                ))
            },
        );

        if !self.ignore_asserts {
            self.assert_keys_match_values(reindexing_universe.keys(), &result_collection);
        };

        let new_column_handle = self.columns.alloc(Column::from_collection(
            reindexing_universe_handle,
            result_collection,
        ));

        Ok(new_column_handle)
    }

    fn update_rows(
        &mut self,
        universe_handle: UniverseHandle,
        column_handle: ColumnHandle,
        updates_handle: ColumnHandle,
    ) -> Result<ColumnHandle> {
        let universe = self
            .universes
            .get(universe_handle)
            .ok_or(Error::InvalidUniverseHandle)?;

        let column = self
            .columns
            .get(column_handle)
            .ok_or(Error::InvalidColumnHandle)?;
        let updates = self
            .columns
            .get(updates_handle)
            .ok_or(Error::InvalidColumnHandle)?;

        let both_arranged: ArrangedByKey<S, Key, (bool, Value)> = column
            .values()
            .map_named("update_rows::updated", |(k, v)| (k, (true, v)))
            .concat(
                &updates
                    .data
                    .clone()
                    .collection()
                    .map_named("update_rows::updates", |(k, v)| (k, (false, v))),
            )
            .arrange_named("update_rows::both");
        let updated_values =
            both_arranged.reduce_abelian("update_rows::updated", move |_key, input, output| {
                let ((_is_left, value), _count) = input[0];
                output.push((value.clone(), 1));
            });

        if !self.ignore_asserts {
            self.assert_keys_match_values(
                universe.keys(),
                &updated_values.as_collection(|k, v: &Value| (*k, v.clone())),
            );
        };

        let new_column_handle = self
            .columns
            .alloc(Column::from_arranged(universe_handle, updated_values));

        Ok(new_column_handle)
    }

    fn override_column_universe(
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
        if !self.ignore_asserts {
            self.assert_keys_match_values(universe.keys(), column.values().clone());
        };
        let new_column_handle = self
            .columns
            .alloc(Column::new(universe_handle, column.data.clone()));
        Ok(new_column_handle)
    }

    fn create_dataflow_reducer(reducer: Reducer) -> Rc<dyn DataflowReducer<S>> {
        match reducer {
            Reducer::Sum => Rc::new(SumReducer),
            Reducer::IntSum => Rc::new(IntSumReducer),
            Reducer::Unique => Rc::new(UniqueReducer),
            Reducer::Min => Rc::new(MinReducer),
            Reducer::ArgMin => Rc::new(ArgMinReducer),
            Reducer::Max => Rc::new(MaxReducer),
            Reducer::ArgMax => Rc::new(ArgMaxReducer),
            Reducer::SortedTuple => Rc::new(SortedTupleReducer),
            Reducer::Any => Rc::new(AnyReducer),
        }
    }

    fn group_by(
        &mut self,
        universe_handle: UniverseHandle,
        column_handles: &[ColumnHandle],
        requested_column_handles: Vec<ColumnHandle>,
    ) -> Result<GrouperHandle> {
        let requested_indices: Vec<usize> = requested_column_handles
            .into_iter()
            .map(|c| column_handles.iter().position(|c2| c2 == &c))
            .sorted()
            .dedup()
            .map(|res| res.ok_or(Error::InvalidRequestedColumns))
            .try_collect()?;
        let requested_column_handles = requested_indices
            .iter()
            .map(|i| column_handles[*i])
            .collect_vec();
        let cache_key = (
            column_handles.to_owned(),
            requested_column_handles.clone(),
            universe_handle,
        );
        if let Some(val) = self.groupers_cache.get(&cache_key) {
            return Ok(*val);
        }
        let source_key_to_result_key_value = self
            .tuples(universe_handle, column_handles)?
            .as_key_and_key_with_value_subset_collection(requested_indices);
        let grouper_handle = self.groupers.alloc(Grouper::new(
            universe_handle,
            requested_column_handles,
            source_key_to_result_key_value,
        ));
        self.groupers_cache.insert(cache_key, grouper_handle);
        Ok(grouper_handle)
    }

    fn group_by_id(
        &mut self,
        universe_handle: UniverseHandle,
        column_handle: ColumnHandle,
        requested_column_handles: Vec<ColumnHandle>,
    ) -> Result<GrouperHandle> {
        let cache_key = (
            column_handle,
            requested_column_handles.clone(),
            universe_handle,
        );
        if let Some(val) = self.groupers_id_cache.get(&cache_key) {
            return Ok(*val);
        }
        if requested_column_handles.iter().any(|c| c != &column_handle) {
            return Err(Error::InvalidRequestedColumns);
        }
        let _universe = self
            .universes
            .get(universe_handle)
            .ok_or(Error::InvalidUniverseHandle)?;
        let column = self
            .columns
            .get(column_handle)
            .ok_or(Error::InvalidColumnHandle)?;
        let source_key_to_result_key_values =
            KeyAndKeyWithTupleCollection::Zero(column.values().as_unpacked_pointers(self)?.clone());
        let grouper_handle = self.groupers.alloc(Grouper::new(
            universe_handle,
            requested_column_handles,
            source_key_to_result_key_values,
        ));
        self.groupers_id_cache.insert(cache_key, grouper_handle);
        Ok(grouper_handle)
    }

    fn grouper_universe(&mut self, grouper_handle: GrouperHandle) -> Result<UniverseHandle> {
        let grouper = self
            .groupers
            .get(grouper_handle)
            .ok_or(Error::InvalidGrouperHandle)?;
        Ok(*(grouper.result_universe.get_or_init(|| {
            self.universes
                .alloc(Universe::from_collection(grouper.result_keys().clone()))
        })))
    }

    fn grouper_input_column(
        &mut self,
        grouper_handle: GrouperHandle,
        column_handle: ColumnHandle,
    ) -> Result<ColumnHandle> {
        let grouper = self
            .groupers
            .get(grouper_handle)
            .ok_or(Error::InvalidGrouperHandle)?;
        if !grouper.requested_columns.contains(&column_handle) {
            return Err(Error::InvalidColumnHandle)?;
        }
        let index = grouper
            .requested_columns
            .iter()
            .position(|handle| handle == &column_handle)
            .ok_or(Error::InvalidColumnHandle)?;
        let new_values = grouper
            .result_keys_values()
            .as_key_with_value_collection(index);
        grouper.result_keys.get_or_init(|| {
            new_values.map_named("grouper_input_column::result_keys", |(key, _value)| key)
        });
        let result_universe = self.grouper_universe(grouper_handle)?;
        let new_column_handle = self
            .columns
            .alloc(Column::from_collection(result_universe, new_values));
        Ok(new_column_handle)
    }

    fn grouper_count_column(&mut self, grouper_handle: GrouperHandle) -> Result<ColumnHandle> {
        let grouper = self
            .groupers
            .get(grouper_handle)
            .ok_or(Error::InvalidGrouperHandle)?;
        let new_values = grouper
            .result_keys_values_arranged()
            .count_keys()
            .map_named("grouper_count_column::new_values", |(key, count)| {
                (key, Value::from_isize(count))
            });
        grouper.result_keys.get_or_init(|| {
            new_values.map_named("grouper_count_column::result_keys", |(key, _value)| key)
        });
        let result_universe = self.grouper_universe(grouper_handle)?;
        let new_column_handle = self
            .columns
            .alloc(Column::from_collection(result_universe, new_values));
        Ok(new_column_handle)
    }

    fn grouper_reducer_column(
        &mut self,
        grouper_handle: GrouperHandle,
        reducer: Reducer,
        column_handle: ColumnHandle,
    ) -> Result<ColumnHandle> {
        let grouper = self
            .groupers
            .get(grouper_handle)
            .ok_or(Error::InvalidGrouperHandle)?;
        let dataflow_reducer = Self::create_dataflow_reducer(reducer);
        let column = self
            .columns
            .get(column_handle)
            .ok_or(Error::InvalidColumnHandle)?;
        if column.universe != grouper.source_universe {
            return Err(Error::UniverseMismatch);
        }
        let new_values = dataflow_reducer.reduce(
            self,
            grouper.source_key_to_result_key(),
            column.values_arranged(),
        );
        grouper.result_keys.get_or_init(|| {
            new_values.map_named("grouper_reducer_column::result_keys", |(key, _value)| key)
        });
        let result_universe = self.grouper_universe(grouper_handle)?;
        let new_column_handle = self
            .columns
            .alloc(Column::from_collection(result_universe, new_values));
        Ok(new_column_handle)
    }

    fn grouper_reducer_column_ix(
        &mut self,
        grouper_handle: GrouperHandle,
        reducer: Reducer,
        ixer_handle: IxerHandle,
        column_handle: ColumnHandle,
    ) -> Result<ColumnHandle> {
        let dataflow_reducer = Self::create_dataflow_reducer(reducer);

        let ixer = self
            .ixers
            .get(ixer_handle)
            .ok_or(Error::InvalidIxerHandle)?;
        assert!(ixer.ix_key_policy != IxKeyPolicy::SkipMissing);

        let column = self
            .columns
            .get(column_handle)
            .ok_or(Error::InvalidColumnHandle)?;
        if column.universe != ixer.input_universe {
            return Err(Error::UniverseMismatch);
        }

        let grouper = self
            .groupers
            .get(grouper_handle)
            .ok_or(Error::InvalidGrouperHandle)?;

        let new_source_key_to_result_key = if let Some(val) = self
            .groupers_ixers_cache
            .get(&(grouper_handle, ixer_handle))
        {
            val.clone()
        } else {
            let new_source_key_to_result_key = ixer
                .keys_to_values_arranged()
                .join_core(
                    grouper.source_key_to_result_key(),
                    |_source_key, col_key, result_key| once((*col_key, *result_key)),
                )
                .arrange_named("grouper_reducer_column_ix::new_source_key_to_result_key");
            self.groupers_ixers_cache.insert(
                (grouper_handle, ixer_handle),
                new_source_key_to_result_key.clone(),
            );
            new_source_key_to_result_key
        };

        if !self.ignore_asserts {
            self.assert_collections_same_size(
                &ixer.keys_values,
                &new_source_key_to_result_key.as_collection(|_k, _v| ()),
            );
        };

        let new_values = dataflow_reducer.reduce(
            self,
            &new_source_key_to_result_key,
            column.values_arranged(),
        );

        grouper.result_keys.get_or_init(|| {
            new_values.map_named("grouper_reducer_column_ix::result_keys", |(key, _value)| {
                key
            })
        });
        let result_universe = self.grouper_universe(grouper_handle)?;
        let new_column_handle = self
            .columns
            .alloc(Column::from_collection(result_universe, new_values));
        Ok(new_column_handle)
    }

    fn ix(
        &mut self,
        key_column_handle: ColumnHandle,
        input_universe_handle: UniverseHandle,
        ix_key_policy: IxKeyPolicy,
    ) -> Result<IxerHandle> {
        let cache_key = (key_column_handle, input_universe_handle, ix_key_policy);
        if let Some(val) = self.ixers_cache.get(&cache_key) {
            return Ok(*val);
        }
        let error_reporter = self.error_reporter.clone();
        let key_column = &self.columns[key_column_handle];
        let keys_values = match ix_key_policy {
            IxKeyPolicy::FailMissing => key_column.values().as_unpacked_pointers(self)?.clone(),
            _ => key_column
                .values()
                .flat_map(move |(key, value)| match value {
                    Value::None => None,
                    _ => Some((
                        key,
                        value.as_pointer().unwrap_with_reporter(&error_reporter),
                    )),
                }),
        };

        let none_keys = match ix_key_policy {
            IxKeyPolicy::ForwardNone => {
                let none_keys = key_column
                    .values()
                    .flat_map(move |(key, value)| match value {
                        Value::None => Some(key),
                        _ => None,
                    });
                Some(none_keys)
            }
            _ => None,
        };

        let output_universe: OnceCell<UniverseHandle> = match ix_key_policy {
            IxKeyPolicy::SkipMissing => OnceCell::new(),
            _ => OnceCell::with_value(key_column.universe),
        };

        let ixer_handle = self.ixers.alloc(Ixer::new(
            input_universe_handle,
            output_universe,
            keys_values,
            none_keys,
            ix_key_policy,
        ));
        self.ixers_cache.insert(cache_key, ixer_handle);
        Ok(ixer_handle)
    }

    fn ix_column(
        &mut self,
        ixer_handle: IxerHandle,
        column_handle: ColumnHandle,
    ) -> Result<ColumnHandle> {
        let ixer = self
            .ixers
            .get(ixer_handle)
            .ok_or(Error::InvalidIxerHandle)?;
        let column = self
            .columns
            .get(column_handle)
            .ok_or(Error::InvalidColumnHandle)?;
        if column.universe != ixer.input_universe {
            return Err(Error::UniverseMismatch);
        }
        let new_values = ixer.values_to_keys_arranged().join_core(
            column.values_arranged(),
            |_source_key, result_key, value| once((*result_key, value.clone())),
        );

        let new_values = if let Some(none_keys) = &ixer.none_keys {
            new_values.concat(
                &none_keys.map_named("ix_column empty keys handling", |key| (key, Value::None)),
            )
        } else {
            new_values
        };

        let output_universe = if ixer.ix_key_policy == IxKeyPolicy::SkipMissing {
            *ixer.output_universe.get_or_init(|| {
                let universe_keys =
                    new_values.map_named("ix_column::universe_keys", |(key, _value)| key);
                self.universes
                    .alloc(Universe::from_collection(universe_keys))
            })
        } else {
            let output_universe = self.ixer_universe(ixer_handle)?;
            if !self.ignore_asserts {
                self.assert_collections_same_size(
                    &new_values,
                    self.universes[output_universe].keys(),
                );
            };
            output_universe
        };

        let new_column = Column::from_collection(output_universe, new_values);
        let new_column_handle = self.columns.alloc(new_column);
        Ok(new_column_handle)
    }

    fn ixer_universe(&mut self, ixer_handle: IxerHandle) -> Result<UniverseHandle> {
        let ixer = self
            .ixers
            .get(ixer_handle)
            .ok_or(Error::InvalidIxerHandle)?;
        Ok(*ixer.output_universe.get_or_init(|| {
            let universe_keys = ixer.values_to_keys_arranged().join_core(
                self.universes[ixer.input_universe].keys_arranged(),
                |_key, val, ()| once(*val),
            );
            self.universes
                .alloc(Universe::from_collection(universe_keys))
        }))
    }

    #[allow(clippy::too_many_lines)]
    fn join(
        &mut self,
        left_universe_handle: UniverseHandle,
        left_column_handles: &[ColumnHandle],
        right_universe_handle: UniverseHandle,
        right_column_handles: &[ColumnHandle],
        join_type: JoinType,
    ) -> Result<JoinerHandle> {
        if left_column_handles.len() != right_column_handles.len() {
            return Err(Error::DifferentJoinConditionLengths);
        }
        let cache_key = (
            left_column_handles.to_owned(),
            left_universe_handle,
            right_column_handles.to_owned(),
            right_universe_handle,
            join_type,
        );
        if let Some(val) = self.joiners_cache.get(&cache_key) {
            return Ok(*val);
        }
        let join_left = self
            .tuples(left_universe_handle, left_column_handles)?
            .map(|key, tuple| (Key::for_values(tuple), key));
        let join_key_to_left_key_arranged: ArrangedByKey<S, Key, Key> = join_left.arrange();
        let join_right = self
            .tuples(right_universe_handle, right_column_handles)?
            .map(|key, tuple| (Key::for_values(tuple), key));
        let join_key_to_right_key_arranged: ArrangedByKey<S, Key, Key> = join_right.arrange();
        let join_left_right = join_key_to_left_key_arranged.join_core(
            &join_key_to_right_key_arranged,
            |join_key, left_key, right_key| once((*join_key, *left_key, *right_key)),
        );

        let left_right_to_result_fn = match join_type {
            JoinType::LeftKeysFull | JoinType::LeftKeysSubset => |(left_key, _right_key)| left_key,
            _ => |(left_key, right_key)| {
                Key::for_values(&[Value::from(left_key), Value::from(right_key)])
            },
        };
        let result_left_right = join_left_right.map_named(
            "join::result_left_right",
            move |(_join_key, left_key, right_key)| {
                (
                    left_right_to_result_fn((left_key, right_key)),
                    left_key,
                    right_key,
                )
            },
        );
        let result_keys = result_left_right.map_named(
            "join::result_keys",
            |(result_key, _left_key, _right_key)| result_key,
        );

        let left_outer = || {
            join_left
                .map_named("join::left_outer left", |(_join_key, left_key)| left_key)
                .concat(
                    &join_left_right
                        .map_named(
                            "join::left_outer right",
                            |(_join_key, left_key, _right_key)| left_key,
                        )
                        .distinct()
                        .negate(),
                )
        };
        let left_result_outer = match join_type {
            JoinType::LeftOuter | JoinType::FullOuter => Some(left_outer().map_named(
                "join::left_result_outer",
                |left_key| {
                    let result_key = Key::for_values(&[Value::from(left_key), Value::None]);
                    (left_key, result_key)
                },
            )),
            JoinType::LeftKeysFull => Some(
                left_outer().map_named("join::left_result_outer", |left_key| (left_key, left_key)),
            ),
            _ => None,
        };

        let result_keys = if let Some(left_result_outer) = &left_result_outer {
            result_keys.concat(
                &left_result_outer
                    .map_named("join::result_keys", |(_left_key, result_key)| result_key),
            )
        } else {
            result_keys
        };

        let right_outer = || {
            join_right
                .map_named("join::right_outer right", |(_join_key, right_key)| {
                    right_key
                })
                .concat(
                    &join_left_right
                        .map_named(
                            "join::right_outer left",
                            |(_join_key, _left_key, right_key)| right_key,
                        )
                        .distinct()
                        .negate(),
                )
        };
        let right_result_outer = match join_type {
            JoinType::RightOuter | JoinType::FullOuter => Some(right_outer().map_named(
                "join::right_result_outer",
                |right_key| {
                    (
                        right_key,
                        Key::for_values(&[Value::from(None::<Key>), Value::from(right_key)]),
                    )
                },
            )),
            _ => None,
        };

        let result_keys = if let Some(right_result_outer) = &right_result_outer {
            result_keys.concat(
                &right_result_outer
                    .map_named("join::result_keys", |(_right_key, result_key)| result_key),
            )
        } else {
            result_keys
        };

        let result_universe_handle = match join_type {
            JoinType::LeftKeysFull => {
                let left_universe = &self.universes[left_universe_handle];
                if !self.ignore_asserts {
                    self.assert_keys_match(left_universe.keys(), &result_keys);
                };
                left_universe_handle
            }
            JoinType::LeftKeysSubset => {
                if !self.ignore_asserts {
                    self.assert_keys_are_distinct(&result_keys);
                };
                self.universes.alloc(Universe::from_collection(result_keys))
            }
            _ => self.universes.alloc(Universe::from_collection(result_keys)),
        };

        let left_result_inner = result_left_right.map_named(
            "join::left_result_inner",
            |(result_key, left_key, _right_key)| (left_key, result_key),
        );

        let left_result_total = if let Some(left_result_outer) = &left_result_outer {
            left_result_inner.concat(left_result_outer)
        } else {
            left_result_inner
        };

        let left_filler = left_result_outer.map(|left_result_outer| {
            left_result_outer.map_named("join::left_filler", |(_left_key, result_key)| {
                (result_key, Value::None)
            })
        });

        let right_result_inner = result_left_right.map_named(
            "join::right_result_inner",
            |(result_key, _left_key, right_key)| (right_key, result_key),
        );

        let right_result_total = if let Some(right_result_outer) = &right_result_outer {
            right_result_inner.concat(right_result_outer)
        } else {
            right_result_inner
        };

        let right_filler = right_result_outer.map(|right_result_outer| {
            right_result_outer.map_named("join::right_filler", |(_right_key, result_key)| {
                (result_key, Value::None)
            })
        });

        let joiner_handle = self.joiners.alloc(Joiner::new(
            left_universe_handle,
            right_universe_handle,
            result_universe_handle,
            left_result_total,
            right_result_total,
            left_filler.map(Into::into),
            right_filler.map(Into::into),
        ));
        self.joiners_cache.insert(cache_key, joiner_handle);
        Ok(joiner_handle)
    }

    fn joiner_universe(&mut self, joiner_handle: JoinerHandle) -> Result<UniverseHandle> {
        let joiner = self
            .joiners
            .get(joiner_handle)
            .ok_or(Error::InvalidJoinerHandle)?;
        Ok(joiner.result_universe)
    }

    fn joiner_column(
        &self,
        universe_handle: UniverseHandle,
        source_key_to_result_key: &ArrangedByKey<S, Key, Key>,
        value_filler: Option<&Values<S>>,
        result_universe_handle: UniverseHandle,
        column_handle: ColumnHandle,
    ) -> Result<Column<S>> {
        let column = self
            .columns
            .get(column_handle)
            .ok_or(Error::InvalidColumnHandle)?;
        if column.universe != universe_handle {
            return Err(Error::UniverseMismatch);
        }
        let new_values = source_key_to_result_key.join_core(
            column.values_arranged(),
            |_source_key, result_key, value| once((*result_key, value.clone())),
        );
        let new_values = if let Some(value_filler) = value_filler {
            new_values.concat(value_filler)
        } else {
            new_values
        };
        let new_column = Column::from_collection(result_universe_handle, new_values);
        Ok(new_column)
    }

    fn joiner_left_column(
        &mut self,
        joiner_handle: JoinerHandle,
        column_handle: ColumnHandle,
    ) -> Result<ColumnHandle> {
        let joiner = self
            .joiners
            .get(joiner_handle)
            .ok_or(Error::InvalidJoinerHandle)?;
        let new_column_handle = self.columns.alloc(self.joiner_column(
            joiner.left_universe,
            joiner.left_key_to_result_key(),
            joiner.right_filler.as_ref(),
            joiner.result_universe,
            column_handle,
        )?);
        Ok(new_column_handle)
    }

    fn joiner_right_column(
        &mut self,
        joiner_handle: JoinerHandle,
        column_handle: ColumnHandle,
    ) -> Result<ColumnHandle> {
        let joiner = self
            .joiners
            .get(joiner_handle)
            .ok_or(Error::InvalidJoinerHandle)?;
        let new_column_handle = self.columns.alloc(self.joiner_column(
            joiner.right_universe,
            joiner.right_key_to_result_key(),
            joiner.left_filler.as_ref(),
            joiner.result_universe,
            column_handle,
        )?);
        Ok(new_column_handle)
    }

    fn complex_columns(&mut self, inputs: Vec<ComplexColumn>) -> Result<Vec<ColumnHandle>> {
        complex_columns(self, inputs)
    }

    fn debug_universe(&self, tag: String, universe_handle: UniverseHandle) -> Result<()> {
        let worker = self.scope.index();
        let universe = self
            .universes
            .get(universe_handle)
            .ok_or(Error::InvalidUniverseHandle)?;
        println!("[{worker}][{tag}] {universe_handle:?}");
        universe.keys().inspect(move |(key, time, diff)| {
            println!("[{worker}][{tag}] @{time:?} {diff:+} {key:#}");
        });
        Ok(())
    }

    fn debug_column(&self, tag: String, column_handle: ColumnHandle) -> Result<()> {
        let worker = self.scope.index();
        let column = self
            .columns
            .get(column_handle)
            .ok_or(Error::InvalidColumnHandle)?;
        println!("[{worker}][{tag}] {column_handle:?}");
        column.values().inspect(move |((key, value), time, diff)| {
            println!("[{worker}][{tag}] @{time:?} {diff:+} {key:#} {value}");
        });
        Ok(())
    }

    fn on_universe_data(
        &mut self,
        universe_handle: UniverseHandle,
        mut function: Box<dyn FnMut(&Key, isize) -> DynResult<()>>,
    ) -> Result<()> {
        let universe = self
            .universes
            .get(universe_handle)
            .ok_or(Error::InvalidUniverseHandle)?;
        universe
            .keys_consolidated()
            .inner
            .exchange(|_| 0)
            .inspect(move |(key, _time, diff)| {
                function(key, *diff).unwrap();
            })
            .probe_with(&mut self.output_probe);

        Ok(())
    }

    fn on_column_data(
        &mut self,
        column_handle: ColumnHandle,
        mut function: Box<dyn FnMut(&Key, &Value, isize) -> DynResult<()>>,
    ) -> Result<()> {
        let column = self
            .columns
            .get(column_handle)
            .ok_or(Error::InvalidColumnHandle)?;
        column
            .values_consolidated()
            .inner
            .exchange(|_| 0)
            .inspect(move |((key, value), _time, diff)| {
                function(key, value, *diff).unwrap();
            })
            .probe_with(&mut self.output_probe);

        Ok(())
    }

    fn probe_universe(
        &mut self,
        universe_handle: UniverseHandle,
        operator_id: usize,
    ) -> Result<()> {
        let universe = self
            .universes
            .get(universe_handle)
            .ok_or(Error::InvalidUniverseHandle)?;
        universe
            .keys()
            .probe_with(self.probes.entry(operator_id).or_insert(ProbeHandle::new()));

        Ok(())
    }

    fn probe_column(&mut self, column_handle: ColumnHandle, operator_id: usize) -> Result<()> {
        let column = self
            .columns
            .get(column_handle)
            .ok_or(Error::InvalidColumnHandle)?;
        column
            .values()
            .probe_with(self.probes.entry(operator_id).or_insert(ProbeHandle::new()));
        Ok(())
    }
}

trait DataflowReducer<S: MaybeTotalScope> {
    fn reduce(
        self: Rc<Self>,
        graph: &DataflowGraphInner<S>,
        source_key_to_result_key: &ArrangedByKey<S, Key, Key>,
        values: &ValuesArranged<S>,
    ) -> Values<S>;
}

impl<S: MaybeTotalScope, R: ReducerImpl> DataflowReducer<S> for R {
    fn reduce(
        self: Rc<Self>,
        graph: &DataflowGraphInner<S>,
        source_key_to_result_key: &ArrangedByKey<S, Key, Key>,
        values: &ValuesArranged<S>,
    ) -> Values<S> {
        let joined = source_key_to_result_key.join_core(values, {
            let self_ = self.clone();
            move |source_key, result_key, value| {
                let state = self_.init(source_key, value).unwrap_or_else(|| {
                    panic!(
                        "{reducer_type}::init() failed for {value:?} of key {source_key:?}",
                        reducer_type = type_name::<R>()
                    )
                }); // XXX
                once((*result_key, state))
            }
        });

        if !graph.ignore_asserts {
            graph.assert_collections_same_size(&joined, &values.as_collection(|_k, _v| ()));
        };
        joined
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
    fn reduce(
        self: Rc<Self>,
        _graph: &DataflowGraphInner<S>,
        source_key_to_result_key: &ArrangedByKey<S, Key, Key>,
        values: &ValuesArranged<S>,
    ) -> Values<S> {
        source_key_to_result_key
            .join_core(values, {
                let self_ = self.clone();
                move |source_key, result_key, value| {
                    let state = self_.init(source_key, value).unwrap_or_else(|| {
                        panic!(
                            "{reducer_type}::init() failed for {value:?} of key {source_key:?}",
                            reducer_type = "IntSumReducer"
                        )
                    }); // XXX
                    once((*result_key, state))
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

#[allow(clippy::unnecessary_wraps)] // we want to always return Result for symmetry
impl<S: MaybeTotalScope<MaybeTotalTimestamp = u64>> DataflowGraphInner<S> {
    fn connector_table(
        &mut self,
        reader: Box<dyn ReaderBuilder>,
        parser: Box<dyn Parser>,
        commit_duration: Option<Duration>,
        parallel_readers: usize,
    ) -> Result<(UniverseHandle, Vec<ColumnHandle>)> {
        let (universe_session, keys): (KeysSession<S>, Keys<S>) = self.scope.new_collection();
        let keys = keys.reshard();
        keys.probe_with(&mut self.input_probe);

        let mut column_sessions = Vec::new();
        let mut column_values = Vec::new();
        for _ in 0..parser.column_count() {
            let (column_session, column_collection): (ValuesSession<S>, GenericValues<S>) =
                self.scope.new_collection();
            let column_collection = column_collection.reshard();
            column_collection.probe_with(&mut self.input_probe);
            column_sessions.push(column_session);
            column_values.push(column_collection);
        }

        if self.scope.index() < parallel_readers {
            let persistent_id = reader.persistent_id();
            let reader_storage_type = reader.storage_type();

            let connector = Connector::<S::Timestamp>::new(commit_duration);
            let (poller, input_thread_handle, offsets_by_time, connector_monitor) = connector.run(
                reader,
                parser,
                column_sessions,
                universe_session,
                |values| match values {
                    None => Key::random(),
                    Some(values) => {
                        // do not hash again if this is pointer already
                        if values.len() == 1 {
                            if let Value::Pointer(key) = values[0] {
                                return key;
                            }
                        }
                        Key::for_values(&values)
                    }
                },
                self.persistent_storage.clone(),
                self.connector_monitors.len(),
            );

            if self.pollers.is_empty() {
                if let Some(persistent_storage) = &mut self.persistent_storage {
                    let prober_persistent_storage = persistent_storage.clone();
                    self.attach_prober(
                        Box::new(move |prober_stats| {
                            if let Some(output_time) = prober_stats.output_stats.time {
                                prober_persistent_storage
                                    .lock()
                                    .unwrap()
                                    .accept_finalized_timestamp(output_time);
                            } else {
                                info!("Closing persistent metadata storage");
                                prober_persistent_storage
                                    .lock()
                                    .unwrap()
                                    .on_output_finished();
                            }
                        }),
                        false,
                        false,
                    )
                    .expect("Failed to start closed timestamps tracker");
                }
            }
            self.pollers.push(poller);

            self.connector_threads.push(input_thread_handle);
            if let Some(persistent_storage) = &mut self.persistent_storage {
                if let Some(persistent_id) = persistent_id {
                    persistent_storage.lock().unwrap().register_input_source(
                        persistent_id,
                        &reader_storage_type,
                        offsets_by_time,
                    );
                }
            }
            self.connector_monitors.push(connector_monitor);
        }

        let universe = Universe::from_collection(keys);
        let universe_handle = self.universes.alloc(universe);

        let mut column_handles = Vec::new();
        for column_values in column_values {
            let column_handle = self
                .columns
                .alloc(Column::from_collection(universe_handle, column_values));
            column_handles.push(column_handle);
        }

        Ok((universe_handle, column_handles))
    }

    fn output_table(
        &mut self,
        mut data_sink: Box<dyn Writer>,
        mut data_formatter: Box<dyn Formatter>,
        universe_handle: UniverseHandle,
        column_handles: &[ColumnHandle],
    ) -> Result<()> {
        let output_columns = self.tuples(universe_handle, column_handles)?;
        let single_threaded = data_sink.single_threaded();
        let (sender, receiver) = mpsc::channel();

        let output = output_columns
            .as_collection()
            .consolidate_nondecreasing()
            .inner;
        let inspect_output = {
            if single_threaded {
                output.exchange(|_| 0)
            } else {
                output
            }
        };
        inspect_output
            .inspect_batch(move |_time, batch| {
                sender
                    .send(batch.to_vec())
                    .expect("output connection failed");
            })
            .probe_with(&mut self.output_probe);

        if !single_threaded || self.scope.index() == 0 {
            let thread_name = format!(
                "pathway:output_table-{}-{}",
                data_sink.short_description(),
                data_formatter.short_description()
            );

            let has_persistent_storage = self.persistent_storage.is_some();

            let output_joiner_handle = Builder::new()
                .name(thread_name)
                .spawn(move || loop {
                    match receiver.recv() {
                        Ok(batch) => {
                            for ((key, values), time, diff) in batch {
                                if time == ARTIFICIAL_TIME_ON_REWIND_START && has_persistent_storage
                                {
                                    // Ignore entries, which had been written before
                                    continue;
                                }
                                let formatted = data_formatter
                                    .format(&key, &values, time, diff)
                                    .map_err(DynError::from)?;
                                data_sink.write(formatted).map_err(DynError::from)?;
                            }
                            data_sink.flush().map_err(DynError::from)?;
                        }
                        Err(mpsc::RecvError) => break Ok(()),
                    }
                })
                .expect("output thread creation failed");
            self.connector_threads.push(output_joiner_handle);
        }

        Ok(())
    }

    fn subscribe_column(
        &mut self,
        wrapper: BatchWrapper,
        mut callback: Box<dyn FnMut(Key, &[Value], u64, isize) -> DynResult<()>>,
        mut on_end: Box<dyn FnMut() -> DynResult<()>>,
        universe_handle: UniverseHandle,
        column_handles: &[ColumnHandle],
    ) -> Result<()> {
        let mut vector = Vec::new();

        let error_reporter = self.error_reporter.clone();

        self.tuples(universe_handle, column_handles)?
            .as_collection()
            .inner
            .probe_with(&mut self.output_probe)
            .sink(Pipeline, "SubscribeColumn", move |input| {
                wrapper.run(|| {
                    while let Some((_time, data)) = input.next() {
                        data.swap(&mut vector);
                        for ((key, values), time, diff) in vector.drain(..) {
                            callback(key, &values, time, diff)
                                .unwrap_with_reporter(&error_reporter);
                        }
                    }
                    if input.frontier().is_empty() {
                        on_end().unwrap_with_reporter(&error_reporter);
                    }
                });
            });

        Ok(())
    }

    fn iterate<'a>(
        &'a mut self,
        iterated: Vec<Table>,
        iterated_with_universe: Vec<Table>,
        extra: Vec<Table>,
        limit: Option<u32>,
        logic: IterationLogic<'a>,
    ) -> Result<(Vec<Table>, Vec<Table>)> {
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
            );
            let mut subgraph_ref = subgraph.0.borrow_mut();
            let mut state = BeforeIterate::new(self, &mut subgraph_ref, step);
            let inner_iterated: Vec<IteratedTable<_, _>> = state.create_tables(iterated)?;
            let inner_iterated_with_universe: Vec<IteratedWithUniverseTable<_, _>> =
                state.create_tables(iterated_with_universe)?;
            let inner_extra: Vec<ExtraTable<_, _>> = state.create_tables(extra)?;
            drop(subgraph_ref);
            let iterated_handles = extract_handles(inner_iterated.iter());
            let iterated_with_universe_handles =
                extract_handles(inner_iterated_with_universe.iter());
            let extra_handles = extract_handles(inner_extra.into_iter());
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

struct InnerTable<U: InnerUniverse, C: InnerColumn> {
    universe: U,
    columns: Vec<C>,
}

type IteratedTable<O, I> = InnerTable<ImportedUniverse<O, I>, IteratedColumn<O, I>>;
type IteratedWithUniverseTable<O, I> = InnerTable<IteratedUniverse<O, I>, IteratedColumn<O, I>>;
type ExtraTable<O, I> = InnerTable<ImportedUniverse<O, I>, ImportedColumn<O, I>>;

impl<U: InnerUniverse, C: InnerColumn<Outer = U::Outer, Inner = U::Inner>> InnerTable<U, C> {
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
    InnerTable<U, IteratedColumn<S, Child<'c, S, Product<S::Timestamp, u32>>>>
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
    ) -> Result<Vec<InnerTable<U, C>>>
    where
        U: InnerUniverse<Outer = S, Inner = Child<'c, S, T>>,
        C: InnerColumn<Outer = S, Inner = Child<'c, S, T>>,
    {
        tables
            .into_iter()
            .map(|(universe_handle, column_handles)| {
                InnerTable::create(self, universe_handle, column_handles)
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
    tables: impl IntoIterator<Item = impl Borrow<InnerTable<U, C>>>,
) -> Vec<Table>
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
    pub fn new(scope: S, error_reporter: ErrorReporter, ignore_asserts: bool) -> Self {
        Self(RefCell::new(DataflowGraphInner::new(
            scope,
            error_reporter,
            ignore_asserts,
        )))
    }
}

impl<S: MaybeTotalScope> Graph for InnerDataflowGraph<S> {
    fn worker_index(&self) -> usize {
        self.0.borrow().worker_index()
    }

    fn worker_count(&self) -> usize {
        self.0.borrow().worker_count()
    }

    fn empty_universe(&self) -> Result<UniverseHandle> {
        self.0.borrow_mut().empty_universe()
    }

    fn empty_column(&self, universe_handle: UniverseHandle) -> Result<ColumnHandle> {
        self.0.borrow_mut().empty_column(universe_handle)
    }

    fn static_universe(&self, keys: Vec<Key>) -> Result<UniverseHandle> {
        self.0.borrow_mut().static_universe(keys)
    }

    fn static_column(
        &self,
        universe_handle: UniverseHandle,
        values: Vec<(Key, Value)>,
    ) -> Result<ColumnHandle> {
        self.0.borrow_mut().static_column(universe_handle, values)
    }

    fn expression_column(
        &self,
        wrapper: BatchWrapper,
        expression: Arc<Expression>,
        universe_handle: UniverseHandle,
        column_handles: Vec<ColumnHandle>,
        trace: Trace,
    ) -> Result<ColumnHandle> {
        self.0.borrow_mut().expression_column(
            wrapper,
            expression,
            universe_handle,
            &column_handles,
            trace,
        )
    }

    fn async_apply_column(
        &self,
        function: Arc<dyn Fn(Key, &[Value]) -> BoxFuture<'static, DynResult<Value>> + Send + Sync>,
        universe_handle: UniverseHandle,
        column_handles: Vec<ColumnHandle>,
        trace: Trace,
    ) -> Result<ColumnHandle> {
        self.0
            .borrow_mut()
            .async_apply_column(function, universe_handle, &column_handles, trace)
    }

    fn filter_universe(
        &self,
        universe_handle: UniverseHandle,
        column_handle: ColumnHandle,
    ) -> Result<UniverseHandle> {
        self.0
            .borrow_mut()
            .filter_universe(universe_handle, column_handle)
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

    fn override_column_universe(
        &self,
        universe_handle: UniverseHandle,
        column_handle: ColumnHandle,
    ) -> Result<ColumnHandle> {
        self.0
            .borrow_mut()
            .override_column_universe(universe_handle, column_handle)
    }

    fn id_column(&self, universe_handle: UniverseHandle) -> Result<ColumnHandle> {
        self.0.borrow_mut().id_column(universe_handle)
    }

    fn intersect_universe(&self, universe_handles: Vec<UniverseHandle>) -> Result<UniverseHandle> {
        self.0.borrow_mut().intersect_universe(universe_handles)
    }

    fn union_universe(&self, universe_handles: Vec<UniverseHandle>) -> Result<UniverseHandle> {
        self.0.borrow_mut().union_universe(&universe_handles)
    }

    fn venn_universes(
        &self,
        left_universe_handle: UniverseHandle,
        right_universe_handle: UniverseHandle,
    ) -> Result<VennUniverseHandle> {
        self.0
            .borrow_mut()
            .venn_universes(left_universe_handle, right_universe_handle)
    }

    fn venn_universes_only_left(
        &self,
        venn_universes_handle: VennUniverseHandle,
    ) -> Result<UniverseHandle> {
        self.0
            .borrow_mut()
            .venn_universes_only_left(venn_universes_handle)
    }

    fn venn_universes_only_right(
        &self,
        venn_universes_handle: VennUniverseHandle,
    ) -> Result<UniverseHandle> {
        self.0
            .borrow_mut()
            .venn_universes_only_right(venn_universes_handle)
    }

    fn venn_universes_both(
        &self,
        venn_universes_handle: VennUniverseHandle,
    ) -> Result<UniverseHandle> {
        self.0
            .borrow_mut()
            .venn_universes_both(venn_universes_handle)
    }

    fn concat(&self, universe_handles: Vec<UniverseHandle>) -> Result<ConcatHandle> {
        self.0.borrow_mut().concat(universe_handles)
    }

    fn concat_universe(&self, concat_handle: ConcatHandle) -> Result<UniverseHandle> {
        self.0.borrow_mut().concat_universe(concat_handle)
    }

    fn concat_column(
        &self,
        concat_handle: ConcatHandle,
        column_handles: Vec<ColumnHandle>,
    ) -> Result<ColumnHandle> {
        self.0
            .borrow_mut()
            .concat_column(concat_handle, &column_handles)
    }

    fn flatten(
        &self,
        column_handle: ColumnHandle,
    ) -> Result<(UniverseHandle, ColumnHandle, FlattenHandle)> {
        self.0.borrow_mut().flatten(column_handle)
    }

    fn explode(
        &self,
        flatten_handle: FlattenHandle,
        column_handle: ColumnHandle,
    ) -> Result<ColumnHandle> {
        self.0.borrow_mut().explode(flatten_handle, column_handle)
    }

    fn sort(
        &self,
        key_column_handle: ColumnHandle,
        instance_column_handle: ColumnHandle,
    ) -> Result<(ColumnHandle, ColumnHandle)> {
        self.0
            .borrow_mut()
            .sort(key_column_handle, instance_column_handle)
    }

    fn reindex_universe(&self, column_handle: ColumnHandle) -> Result<UniverseHandle> {
        self.0.borrow_mut().reindex_universe(column_handle)
    }

    fn reindex_column(
        &self,
        column_to_reindex: ColumnHandle,
        reindexing_column: ColumnHandle,
        reindexing_universe: UniverseHandle,
    ) -> Result<ColumnHandle> {
        self.0.borrow_mut().reindex_column(
            column_to_reindex,
            reindexing_column,
            reindexing_universe,
        )
    }

    fn update_rows(
        &self,
        universe_handle: UniverseHandle,
        column_handle: ColumnHandle,
        updates_handle: ColumnHandle,
    ) -> Result<ColumnHandle> {
        self.0
            .borrow_mut()
            .update_rows(universe_handle, column_handle, updates_handle)
    }

    fn group_by(
        &self,
        universe_handle: UniverseHandle,
        column_handles: Vec<ColumnHandle>,
        requested_column_handles: Vec<ColumnHandle>,
    ) -> Result<GrouperHandle> {
        self.0
            .borrow_mut()
            .group_by(universe_handle, &column_handles, requested_column_handles)
    }

    fn group_by_id(
        &self,
        universe_handle: UniverseHandle,
        column_handle: ColumnHandle,
        requested_column_handles: Vec<ColumnHandle>,
    ) -> Result<GrouperHandle> {
        self.0
            .borrow_mut()
            .group_by_id(universe_handle, column_handle, requested_column_handles)
    }

    fn grouper_universe(&self, grouper_handle: GrouperHandle) -> Result<UniverseHandle> {
        self.0.borrow_mut().grouper_universe(grouper_handle)
    }

    fn grouper_input_column(
        &self,
        grouper_handle: GrouperHandle,
        column_handle: ColumnHandle,
    ) -> Result<ColumnHandle> {
        self.0
            .borrow_mut()
            .grouper_input_column(grouper_handle, column_handle)
    }

    fn grouper_count_column(&self, grouper_handle: GrouperHandle) -> Result<ColumnHandle> {
        self.0.borrow_mut().grouper_count_column(grouper_handle)
    }

    fn grouper_reducer_column(
        &self,
        grouper_handle: GrouperHandle,
        reducer: Reducer,
        column_handle: ColumnHandle,
    ) -> Result<ColumnHandle> {
        self.0
            .borrow_mut()
            .grouper_reducer_column(grouper_handle, reducer, column_handle)
    }

    fn grouper_reducer_column_ix(
        &self,
        grouper_handle: GrouperHandle,
        reducer: Reducer,
        ixer_handle: IxerHandle,
        column_handle: ColumnHandle,
    ) -> Result<ColumnHandle> {
        self.0.borrow_mut().grouper_reducer_column_ix(
            grouper_handle,
            reducer,
            ixer_handle,
            column_handle,
        )
    }

    fn ix(
        &self,
        key_column_handle: ColumnHandle,
        input_universe_handle: UniverseHandle,
        ix_key_policy: IxKeyPolicy,
    ) -> Result<IxerHandle> {
        self.0
            .borrow_mut()
            .ix(key_column_handle, input_universe_handle, ix_key_policy)
    }

    fn ix_column(
        &self,
        ixer_handle: IxerHandle,
        column_handle: ColumnHandle,
    ) -> Result<ColumnHandle> {
        self.0.borrow_mut().ix_column(ixer_handle, column_handle)
    }

    fn ixer_universe(&self, ixer_handle: IxerHandle) -> Result<UniverseHandle> {
        self.0.borrow_mut().ixer_universe(ixer_handle)
    }

    fn join(
        &self,
        left_universe_handle: UniverseHandle,
        left_column_handles: Vec<ColumnHandle>,
        right_universe_handle: UniverseHandle,
        right_column_handles: Vec<ColumnHandle>,
        join_type: JoinType,
    ) -> Result<JoinerHandle> {
        self.0.borrow_mut().join(
            left_universe_handle,
            &left_column_handles,
            right_universe_handle,
            &right_column_handles,
            join_type,
        )
    }

    fn joiner_universe(&self, joiner_handle: JoinerHandle) -> Result<UniverseHandle> {
        self.0.borrow_mut().joiner_universe(joiner_handle)
    }

    fn joiner_left_column(
        &self,
        joiner_handle: JoinerHandle,
        column_handle: ColumnHandle,
    ) -> Result<ColumnHandle> {
        self.0
            .borrow_mut()
            .joiner_left_column(joiner_handle, column_handle)
    }

    fn joiner_right_column(
        &self,
        joiner_handle: JoinerHandle,
        column_handle: ColumnHandle,
    ) -> Result<ColumnHandle> {
        self.0
            .borrow_mut()
            .joiner_right_column(joiner_handle, column_handle)
    }

    fn iterate<'a>(
        &'a self,
        _iterated: Vec<Table>,
        _iterated_with_universe: Vec<Table>,
        _extra: Vec<Table>,
        _limit: Option<u32>,
        _logic: IterationLogic<'a>,
    ) -> Result<(Vec<Table>, Vec<Table>)> {
        Err(Error::IterationNotPossible)
    }

    fn complex_columns(&self, inputs: Vec<ComplexColumn>) -> Result<Vec<ColumnHandle>> {
        self.0.borrow_mut().complex_columns(inputs)
    }

    fn debug_universe(&self, tag: String, universe_handle: UniverseHandle) -> Result<()> {
        self.0.borrow().debug_universe(tag, universe_handle)
    }

    fn debug_column(&self, tag: String, column_handle: ColumnHandle) -> Result<()> {
        self.0.borrow().debug_column(tag, column_handle)
    }

    fn connector_table(
        &self,
        _reader: Box<dyn ReaderBuilder>,
        _parser: Box<dyn Parser>,
        _commit_duration: Option<Duration>,
        _parallel_readers: usize,
    ) -> Result<(UniverseHandle, Vec<ColumnHandle>)> {
        Err(Error::IoNotPossible)
    }

    fn subscribe_column(
        &self,
        _wrapper: BatchWrapper,
        _callback: Box<dyn FnMut(Key, &[Value], u64, isize) -> DynResult<()>>,
        _on_end: Box<dyn FnMut() -> DynResult<()>>,
        _universe_handle: UniverseHandle,
        _column_handles: Vec<ColumnHandle>,
    ) -> Result<()> {
        Err(Error::IoNotPossible)
    }

    fn output_table(
        &self,
        mut _data_sink: Box<dyn Writer>,
        mut _data_formatter: Box<dyn Formatter>,
        _universe_handle: UniverseHandle,
        _column_handles: Vec<ColumnHandle>,
    ) -> Result<()> {
        Err(Error::IoNotPossible)
    }

    fn on_universe_data(
        &self,
        universe_handle: UniverseHandle,
        function: Box<dyn FnMut(&Key, isize) -> DynResult<()>>,
    ) -> Result<()> {
        self.0
            .borrow_mut()
            .on_universe_data(universe_handle, function)
    }

    fn on_column_data(
        &self,
        column_handle: ColumnHandle,
        function: Box<dyn FnMut(&Key, &Value, isize) -> DynResult<()>>,
    ) -> Result<()> {
        self.0.borrow_mut().on_column_data(column_handle, function)
    }

    fn attach_prober(
        &self,
        _logic: Box<dyn FnMut(ProberStats)>,
        _intermediate_probes_required: bool,
        _run_callback_every_time: bool,
    ) -> Result<()> {
        Err(Error::IoNotPossible)
    }

    fn probe_universe(&self, universe_handle: UniverseHandle, operator_id: usize) -> Result<()> {
        self.0
            .borrow_mut()
            .probe_universe(universe_handle, operator_id)
    }

    fn probe_column(&self, column_handle: ColumnHandle, operator_id: usize) -> Result<()> {
        self.0.borrow_mut().probe_column(column_handle, operator_id)
    }
}

struct OuterDataflowGraph<S: MaybeTotalScope<MaybeTotalTimestamp = u64>>(
    RefCell<DataflowGraphInner<S>>,
);

impl<S: MaybeTotalScope<MaybeTotalTimestamp = u64>> OuterDataflowGraph<S> {
    pub fn new(scope: S, error_reporter: ErrorReporter, ignore_asserts: bool) -> Self {
        Self(RefCell::new(DataflowGraphInner::new(
            scope,
            error_reporter,
            ignore_asserts,
        )))
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

    fn empty_column(&self, universe_handle: UniverseHandle) -> Result<ColumnHandle> {
        self.0.borrow_mut().empty_column(universe_handle)
    }

    fn static_universe(&self, keys: Vec<Key>) -> Result<UniverseHandle> {
        self.0.borrow_mut().static_universe(keys)
    }

    fn static_column(
        &self,
        universe_handle: UniverseHandle,
        values: Vec<(Key, Value)>,
    ) -> Result<ColumnHandle> {
        self.0.borrow_mut().static_column(universe_handle, values)
    }

    fn expression_column(
        &self,
        wrapper: BatchWrapper,
        expression: Arc<Expression>,
        universe_handle: UniverseHandle,
        column_handles: Vec<ColumnHandle>,
        trace: Trace,
    ) -> Result<ColumnHandle> {
        self.0.borrow_mut().expression_column(
            wrapper,
            expression,
            universe_handle,
            &column_handles,
            trace,
        )
    }

    fn async_apply_column(
        &self,
        function: Arc<dyn Fn(Key, &[Value]) -> BoxFuture<'static, DynResult<Value>> + Send + Sync>,
        universe_handle: UniverseHandle,
        column_handles: Vec<ColumnHandle>,
        trace: Trace,
    ) -> Result<ColumnHandle> {
        self.0
            .borrow_mut()
            .async_apply_column(function, universe_handle, &column_handles, trace)
    }

    fn subscribe_column(
        &self,
        wrapper: BatchWrapper,
        callback: Box<dyn FnMut(Key, &[Value], u64, isize) -> DynResult<()>>,
        on_end: Box<dyn FnMut() -> DynResult<()>>,
        universe_handle: UniverseHandle,
        column_handles: Vec<ColumnHandle>,
    ) -> Result<()> {
        self.0.borrow_mut().subscribe_column(
            wrapper,
            callback,
            on_end,
            universe_handle,
            &column_handles,
        )
    }

    fn filter_universe(
        &self,
        universe_handle: UniverseHandle,
        column_handle: ColumnHandle,
    ) -> Result<UniverseHandle> {
        self.0
            .borrow_mut()
            .filter_universe(universe_handle, column_handle)
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

    fn override_column_universe(
        &self,
        universe_handle: UniverseHandle,
        column_handle: ColumnHandle,
    ) -> Result<ColumnHandle> {
        self.0
            .borrow_mut()
            .override_column_universe(universe_handle, column_handle)
    }

    fn id_column(&self, universe_handle: UniverseHandle) -> Result<ColumnHandle> {
        self.0.borrow_mut().id_column(universe_handle)
    }

    fn intersect_universe(&self, universe_handles: Vec<UniverseHandle>) -> Result<UniverseHandle> {
        self.0.borrow_mut().intersect_universe(universe_handles)
    }

    fn union_universe(&self, universe_handles: Vec<UniverseHandle>) -> Result<UniverseHandle> {
        self.0.borrow_mut().union_universe(&universe_handles)
    }

    fn venn_universes(
        &self,
        left_universe_handle: UniverseHandle,
        right_universe_handle: UniverseHandle,
    ) -> Result<VennUniverseHandle> {
        self.0
            .borrow_mut()
            .venn_universes(left_universe_handle, right_universe_handle)
    }

    fn venn_universes_only_left(
        &self,
        venn_universes_handle: VennUniverseHandle,
    ) -> Result<UniverseHandle> {
        self.0
            .borrow_mut()
            .venn_universes_only_left(venn_universes_handle)
    }

    fn venn_universes_only_right(
        &self,
        venn_universes_handle: VennUniverseHandle,
    ) -> Result<UniverseHandle> {
        self.0
            .borrow_mut()
            .venn_universes_only_right(venn_universes_handle)
    }

    fn venn_universes_both(
        &self,
        venn_universes_handle: VennUniverseHandle,
    ) -> Result<UniverseHandle> {
        self.0
            .borrow_mut()
            .venn_universes_both(venn_universes_handle)
    }

    fn concat(&self, universe_handles: Vec<UniverseHandle>) -> Result<ConcatHandle> {
        self.0.borrow_mut().concat(universe_handles)
    }

    fn concat_universe(&self, concat_handle: ConcatHandle) -> Result<UniverseHandle> {
        self.0.borrow_mut().concat_universe(concat_handle)
    }

    fn concat_column(
        &self,
        concat_handle: ConcatHandle,
        column_handles: Vec<ColumnHandle>,
    ) -> Result<ColumnHandle> {
        self.0
            .borrow_mut()
            .concat_column(concat_handle, &column_handles)
    }

    fn flatten(
        &self,
        column_handle: ColumnHandle,
    ) -> Result<(UniverseHandle, ColumnHandle, FlattenHandle)> {
        self.0.borrow_mut().flatten(column_handle)
    }

    fn explode(
        &self,
        flatten_handle: FlattenHandle,
        column_handle: ColumnHandle,
    ) -> Result<ColumnHandle> {
        self.0.borrow_mut().explode(flatten_handle, column_handle)
    }

    fn sort(
        &self,
        key_column_handle: ColumnHandle,
        instance_column_handle: ColumnHandle,
    ) -> Result<(ColumnHandle, ColumnHandle)> {
        self.0
            .borrow_mut()
            .sort(key_column_handle, instance_column_handle)
    }

    fn reindex_universe(&self, column_handle: ColumnHandle) -> Result<UniverseHandle> {
        self.0.borrow_mut().reindex_universe(column_handle)
    }

    fn reindex_column(
        &self,
        column_to_reindex: ColumnHandle,
        reindexing_column: ColumnHandle,
        reindexing_universe: UniverseHandle,
    ) -> Result<ColumnHandle> {
        self.0.borrow_mut().reindex_column(
            column_to_reindex,
            reindexing_column,
            reindexing_universe,
        )
    }

    fn update_rows(
        &self,
        universe_handle: UniverseHandle,
        column_handle: ColumnHandle,
        updates_handle: ColumnHandle,
    ) -> Result<ColumnHandle> {
        self.0
            .borrow_mut()
            .update_rows(universe_handle, column_handle, updates_handle)
    }

    fn group_by(
        &self,
        universe_handle: UniverseHandle,
        column_handles: Vec<ColumnHandle>,
        requested_column_handles: Vec<ColumnHandle>,
    ) -> Result<GrouperHandle> {
        self.0
            .borrow_mut()
            .group_by(universe_handle, &column_handles, requested_column_handles)
    }

    fn group_by_id(
        &self,
        universe_handle: UniverseHandle,
        column_handle: ColumnHandle,
        requested_column_handles: Vec<ColumnHandle>,
    ) -> Result<GrouperHandle> {
        self.0
            .borrow_mut()
            .group_by_id(universe_handle, column_handle, requested_column_handles)
    }

    fn grouper_universe(&self, grouper_handle: GrouperHandle) -> Result<UniverseHandle> {
        self.0.borrow_mut().grouper_universe(grouper_handle)
    }

    fn grouper_input_column(
        &self,
        grouper_handle: GrouperHandle,
        column_handle: ColumnHandle,
    ) -> Result<ColumnHandle> {
        self.0
            .borrow_mut()
            .grouper_input_column(grouper_handle, column_handle)
    }

    fn grouper_count_column(&self, grouper_handle: GrouperHandle) -> Result<ColumnHandle> {
        self.0.borrow_mut().grouper_count_column(grouper_handle)
    }

    fn grouper_reducer_column(
        &self,
        grouper_handle: GrouperHandle,
        reducer: Reducer,
        column_handle: ColumnHandle,
    ) -> Result<ColumnHandle> {
        self.0
            .borrow_mut()
            .grouper_reducer_column(grouper_handle, reducer, column_handle)
    }

    fn grouper_reducer_column_ix(
        &self,
        grouper_handle: GrouperHandle,
        reducer: Reducer,
        ixer_handle: IxerHandle,
        column_handle: ColumnHandle,
    ) -> Result<ColumnHandle> {
        self.0.borrow_mut().grouper_reducer_column_ix(
            grouper_handle,
            reducer,
            ixer_handle,
            column_handle,
        )
    }

    fn ix(
        &self,
        key_column_handle: ColumnHandle,
        input_universe_handle: UniverseHandle,
        ix_key_policy: IxKeyPolicy,
    ) -> Result<IxerHandle> {
        self.0
            .borrow_mut()
            .ix(key_column_handle, input_universe_handle, ix_key_policy)
    }

    fn ix_column(
        &self,
        ixer_handle: IxerHandle,
        column_handle: ColumnHandle,
    ) -> Result<ColumnHandle> {
        self.0.borrow_mut().ix_column(ixer_handle, column_handle)
    }

    fn ixer_universe(&self, ixer_handle: IxerHandle) -> Result<UniverseHandle> {
        self.0.borrow_mut().ixer_universe(ixer_handle)
    }

    fn join(
        &self,
        left_universe_handle: UniverseHandle,
        left_column_handles: Vec<ColumnHandle>,
        right_universe_handle: UniverseHandle,
        right_column_handles: Vec<ColumnHandle>,
        join_type: JoinType,
    ) -> Result<JoinerHandle> {
        self.0.borrow_mut().join(
            left_universe_handle,
            &left_column_handles,
            right_universe_handle,
            &right_column_handles,
            join_type,
        )
    }

    fn joiner_universe(&self, joiner_handle: JoinerHandle) -> Result<UniverseHandle> {
        self.0.borrow_mut().joiner_universe(joiner_handle)
    }

    fn joiner_left_column(
        &self,
        joiner_handle: JoinerHandle,
        column_handle: ColumnHandle,
    ) -> Result<ColumnHandle> {
        self.0
            .borrow_mut()
            .joiner_left_column(joiner_handle, column_handle)
    }

    fn joiner_right_column(
        &self,
        joiner_handle: JoinerHandle,
        column_handle: ColumnHandle,
    ) -> Result<ColumnHandle> {
        self.0
            .borrow_mut()
            .joiner_right_column(joiner_handle, column_handle)
    }

    fn iterate<'a>(
        &'a self,
        iterated: Vec<Table>,
        iterated_with_universe: Vec<Table>,
        extra: Vec<Table>,
        limit: Option<u32>,
        logic: IterationLogic<'a>,
    ) -> Result<(Vec<Table>, Vec<Table>)> {
        self.0
            .borrow_mut()
            .iterate(iterated, iterated_with_universe, extra, limit, logic)
    }

    fn complex_columns(&self, inputs: Vec<ComplexColumn>) -> Result<Vec<ColumnHandle>> {
        self.0.borrow_mut().complex_columns(inputs)
    }

    fn debug_universe(&self, tag: String, universe_handle: UniverseHandle) -> Result<()> {
        self.0.borrow().debug_universe(tag, universe_handle)
    }

    fn debug_column(&self, tag: String, column_handle: ColumnHandle) -> Result<()> {
        self.0.borrow().debug_column(tag, column_handle)
    }

    fn connector_table(
        &self,
        reader: Box<dyn ReaderBuilder>,
        parser: Box<dyn Parser>,
        commit_duration: Option<Duration>,
        parallel_readers: usize,
    ) -> Result<(UniverseHandle, Vec<ColumnHandle>)> {
        self.0
            .borrow_mut()
            .connector_table(reader, parser, commit_duration, parallel_readers)
    }

    fn output_table(
        &self,
        data_sink: Box<dyn Writer>,
        data_formatter: Box<dyn Formatter>,
        universe_handle: UniverseHandle,
        column_handles: Vec<ColumnHandle>,
    ) -> Result<()> {
        self.0.borrow_mut().output_table(
            data_sink,
            data_formatter,
            universe_handle,
            &column_handles,
        )
    }

    fn on_universe_data(
        &self,
        universe_handle: UniverseHandle,
        function: Box<dyn FnMut(&Key, isize) -> DynResult<()>>,
    ) -> Result<()> {
        self.0
            .borrow_mut()
            .on_universe_data(universe_handle, function)
    }

    fn on_column_data(
        &self,
        column_handle: ColumnHandle,
        function: Box<dyn FnMut(&Key, &Value, isize) -> DynResult<()>>,
    ) -> Result<()> {
        self.0.borrow_mut().on_column_data(column_handle, function)
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

    fn probe_universe(&self, universe_handle: UniverseHandle, operator_id: usize) -> Result<()> {
        self.0
            .borrow_mut()
            .probe_universe(universe_handle, operator_id)
    }

    fn probe_column(&self, column_handle: ColumnHandle, operator_id: usize) -> Result<()> {
        self.0.borrow_mut().probe_column(column_handle, operator_id)
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

pub fn config_from_env() -> Result<Config, String> {
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
    Ok(config)
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
                let graph =
                    OuterDataflowGraph::new(scope.clone(), error_reporter.clone(), ignore_asserts);
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
                let join_result = connector_thread
                    .join()
                    .expect("join connector thread failed");
                join_result.unwrap_with_reporter(&error_reporter);
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
