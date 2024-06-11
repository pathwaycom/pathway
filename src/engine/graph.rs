// Copyright © 2024 Pathway

use std::any::Any;
use std::cell::Cell;
use std::collections::HashMap;
use std::ops::ControlFlow;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use futures::future::BoxFuture;
use id_arena::ArenaBehavior;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::pymethods;
use pyo3::pyclass::CompareOp;
use pyo3::{pyclass, PyResult, Python};
use scopeguard::defer;

use crate::connectors::data_format::{Formatter, Parser};
use crate::connectors::data_storage::{ReaderBuilder, Writer};
use crate::connectors::monitoring::ConnectorStats;
use crate::external_integration::ExternalIndex;
use crate::persistence::ExternalPersistentId;

use super::error::{DynResult, Trace};
use super::external_index_wrappers::{ExternalIndexData, ExternalIndexQuery};
use super::reduce::StatefulCombineFn;
use super::{
    Error, Expression, Key, Reducer, Result, ShardPolicy, Timestamp, TotalFrontier, Type, Value,
};

macro_rules! define_handle {
    ($handle:ident) => {
        #[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
        pub struct $handle {
            arena_id: u32,
            index: u32,
        }

        impl ArenaBehavior for $handle {
            type Id = Self;

            fn new_id(arena_id: u32, index: usize) -> Self {
                let index = index.try_into().unwrap();
                Self { arena_id, index }
            }

            fn arena_id(handle: Self) -> u32 {
                handle.arena_id
            }

            fn index(handle: Self) -> usize {
                handle.index.try_into().unwrap()
            }
        }
    };
}

define_handle!(UniverseHandle);

define_handle!(ColumnHandle);

define_handle!(TableHandle);

define_handle!(IxerHandle);

define_handle!(ConcatHandle);

define_handle!(ErrorLogHandle);

pub type LegacyTable = (UniverseHandle, Vec<ColumnHandle>);

pub trait Context: Send {
    fn this_row(&self) -> Key;

    fn data(&self) -> Value;

    fn get(&self, column_index: usize, row: Key, args: Vec<Value>) -> Option<Value>;
}

pub struct ScopedContext(Cell<Option<*const dyn Context>>);

impl ScopedContext {
    pub fn new() -> Self {
        Self(Cell::new(None))
    }

    pub fn scoped<'a, R>(&'a self, value: &'a (dyn Context + 'a), fun: impl FnOnce() -> R) -> R {
        // SAFETY: we will only allow to dereference the pointer after casting into a lifetime
        // outlived by 'a
        #[allow(clippy::transmute_ptr_to_ptr)] // the cast doesn't seem to be enough
        let new =
            unsafe { std::mem::transmute::<*const (dyn Context + 'a), *const dyn Context>(value) };
        let old = self.0.replace(Some(new));
        defer! {
            self.0.set(old);
        }
        fun()
    }

    pub fn with<R>(&self, fun: impl for<'a> FnOnce(&'a (dyn Context + 'a)) -> R) -> Option<R> {
        self.0.get().map(|ptr| {
            // SAFETY: pointer is only stored inside while it is valid
            fun(unsafe { &*ptr })
        })
    }

    pub fn try_with<R>(
        &self,
        fun: impl for<'a> FnOnce(&'a (dyn Context + 'a)) -> Result<R>,
    ) -> Result<R> {
        self.with(fun).ok_or(Error::ContextNotInScope)?
    }
}

impl Default for ScopedContext {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum ColumnPath {
    Key,
    ValuePath(Vec<usize>),
}

impl ColumnPath {
    pub fn extract(&self, key: &Key, value: &Value) -> Result<Value> {
        match self {
            Self::Key => Ok(Value::from(*key)),
            Self::ValuePath(path) => {
                let mut value = value;
                for i in path {
                    if *value == Value::None || *value == Value::Error {
                        break;
                        // needed in outer joins and replacing rows with duplicated ids with error
                    }
                    value = value
                        .as_tuple()?
                        .get(*i)
                        .ok_or_else(|| Error::InvalidColumnPath(self.clone()))?;
                }
                Ok(value.clone())
            }
        }
    }

    pub fn extract_from_value(&self, value: &Value) -> Result<Value> {
        match self {
            Self::Key => Err(Error::ExtractFromValueNotSupportedForKey),
            Self::ValuePath(path) => {
                let mut value = value;
                for i in path {
                    value = value
                        .as_tuple()?
                        .get(*i)
                        .ok_or_else(|| Error::InvalidColumnPath(self.clone()))?;
                }
                Ok(value.clone())
            }
        }
    }

    pub fn extract_properties(
        &self,
        table_properties: &TableProperties,
    ) -> Result<TableProperties> {
        match self {
            ColumnPath::Key => Ok(TableProperties::Empty),
            ColumnPath::ValuePath(path) => {
                let mut table_properties = table_properties;
                for i in path {
                    match table_properties {
                        TableProperties::Table(inner) => {
                            table_properties = inner
                                .get(*i)
                                .ok_or_else(|| Error::InvalidColumnPath(self.clone()))?;
                        }
                        _ => return Err(Error::InvalidColumnPath(self.clone())),
                    }
                }
                Ok(table_properties.clone())
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[pyclass(module = "pathway.engine", frozen, get_all)]
pub struct DataRow {
    pub key: Key,
    pub values: Vec<Value>,
    pub time: Timestamp,
    pub diff: isize,
    pub shard: Option<usize>,
}

#[pymethods]
impl DataRow {
    #[new]
    #[pyo3(signature = (
        key,
        values,
        time = Timestamp(0),
        diff = 1,
        shard = None,
    ))]
    pub fn new(
        key: Key,
        values: Vec<Value>,
        time: Timestamp,
        diff: isize,
        shard: Option<usize>,
    ) -> Self {
        Self {
            key,
            values,
            time,
            diff,
            shard,
        }
    }

    fn __repr__(&self) -> String {
        format!("{self:?}")
    }

    fn __richcmp__(&self, other: &Self, op: CompareOp) -> PyResult<bool> {
        // TODO: replace with __eq__ when pyo3 is updated to 0.20
        match op {
            CompareOp::Eq => Ok(self == other),
            CompareOp::Ne => Ok(self != other),
            _ => Err(PyTypeError::new_err(format!(
                "{op:?} not supported between instances of DataRow and DataRow"
            ))),
        }
    }
}

impl DataRow {
    pub fn from_engine(key: Key, values: Vec<Value>, time: Timestamp, diff: isize) -> Self {
        Self {
            key,
            values,
            time,
            diff,
            shard: None,
        }
    }
}

pub struct ExpressionData {
    pub expression: Arc<Expression>,
    pub properties: Arc<TableProperties>,
}

impl ExpressionData {
    pub fn new(expression: Arc<Expression>, properties: Arc<TableProperties>) -> Self {
        ExpressionData {
            expression,
            properties,
        }
    }
}

#[derive(Clone)]
pub struct ReducerData {
    pub reducer: Reducer,
    pub skip_errors: bool,
    pub column_paths: Vec<ColumnPath>,
}

impl ReducerData {
    pub fn new(reducer: Reducer, skip_errors: bool, column_paths: Vec<ColumnPath>) -> Self {
        ReducerData {
            reducer,
            skip_errors,
            column_paths,
        }
    }
}

pub struct JoinData {
    pub table_handle: TableHandle,
    pub column_paths: Vec<ColumnPath>,
}

impl JoinData {
    pub fn new(table_handle: TableHandle, column_paths: Vec<ColumnPath>) -> Self {
        JoinData {
            table_handle,
            column_paths,
        }
    }
}

pub enum Computer {
    Attribute {
        logic: Box<dyn FnMut(&dyn Context) -> DynResult<Option<Value>>>,
        universe_handle: UniverseHandle,
    },

    Method {
        logic: Box<dyn FnMut(&dyn Context, &[Value]) -> DynResult<Option<Value>>>,
        universe_handle: UniverseHandle,
        data: Value,
        data_column_handle: Option<ColumnHandle>,
    },
}

impl Computer {
    pub fn compute<C: Context>(&mut self, context: &C, args: &[Value]) -> DynResult<Option<Value>> {
        match self {
            Computer::Attribute { logic, .. } => {
                assert!(args.is_empty());
                logic(context)
            }
            Computer::Method { logic, .. } => logic(context, args),
        }
    }
}

#[non_exhaustive]
pub enum ComplexColumn {
    Column(ColumnHandle),
    InternalComputer(Computer),
    ExternalComputer(Computer),
}

impl ComplexColumn {
    pub fn takes_args(&self) -> bool {
        matches!(
            self,
            Self::InternalComputer(Computer::Method { .. })
                | Self::ExternalComputer(Computer::Method { .. })
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnProperties {
    pub dtype: Type,
    pub append_only: bool,
    pub trace: Trace,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TableProperties {
    Table(Arc<[TableProperties]>),
    Column(Arc<ColumnProperties>),
    Empty,
}

impl TableProperties {
    pub fn flat(column_properties: Vec<Arc<ColumnProperties>>) -> Self {
        let column_properties: Vec<_> = column_properties
            .into_iter()
            .map(TableProperties::Column)
            .collect();

        TableProperties::Table(column_properties.into())
    }

    pub fn from_paths(properties: Vec<(ColumnPath, TableProperties)>) -> Result<TableProperties> {
        fn produce_nested_tuple(
            props: &[(Vec<usize>, TableProperties)],
            depth: usize,
        ) -> Result<TableProperties> {
            if !props.is_empty() && props.first().unwrap().0.len() == depth {
                let first = &props.first().unwrap().1;
                for (_path, other) in &props[1..] {
                    if first != other {
                        return Err(Error::InconsistentColumnProperties);
                    }
                }
                return Ok(first.clone());
            }
            let mut prefix = 0;
            let mut begin = 0;
            let mut end = 0;
            let mut result = Vec::new();

            while end < props.len() {
                while end < props.len() && prefix == props[end].0[depth] {
                    end += 1;
                }
                prefix += 1;
                if begin == end {
                    // XXX: remove after iterate is properly implemented
                    // emulate unused cols
                    result.push(TableProperties::Empty);
                    continue;
                }
                assert!(begin < end);
                result.push(produce_nested_tuple(&props[begin..end], depth + 1)?);
                begin = end;
            }

            Ok(TableProperties::Table(result.as_slice().into()))
        }

        let mut properties: Vec<(Vec<usize>, TableProperties)> = properties
            .into_iter()
            .map(|(path, props)| match path {
                ColumnPath::ValuePath(path) => Ok((path, props)),
                ColumnPath::Key => Err(Error::IdInTableProperties),
            })
            .collect::<Result<_>>()?;

        properties.sort_unstable_by(|(left_path, _), (right_path, _)| left_path.cmp(right_path));

        produce_nested_tuple(properties.as_slice(), 0)
    }

    pub fn trace(&self) -> &Trace {
        match self {
            Self::Column(properties) => &properties.trace,
            _ => &Trace::Empty,
        }
    }
}

pub struct OperatorProperties {
    pub id: usize,
    pub depends_on_error_log: bool,
}

pub type IterationLogic<'a> = Box<
    dyn FnOnce(
            &dyn Graph,
            Vec<LegacyTable>,
            Vec<LegacyTable>,
            Vec<LegacyTable>,
        ) -> DynResult<(Vec<LegacyTable>, Vec<LegacyTable>)>
        + 'a,
>;

#[derive(Clone, Debug)]
pub enum BatchWrapper {
    None,
    WithGil,
}

impl BatchWrapper {
    pub fn run<R>(&self, logic: impl FnOnce() -> R) -> R {
        match self {
            BatchWrapper::None => logic(),
            BatchWrapper::WithGil => Python::with_gil(|_| logic()),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum JoinType {
    Inner,
    LeftOuter,
    RightOuter,
    FullOuter,
    LeftKeysSubset,
    LeftKeysFull,
}

impl JoinType {
    pub fn from_assign_left_right(assign_id: bool, left: bool, right: bool) -> Result<Self> {
        match (assign_id, left, right) {
            (false, false, false) => Ok(Self::Inner),
            (true, false, false) => Ok(Self::LeftKeysSubset),
            (false, false, true) => Ok(Self::RightOuter),
            (false, true, false) => Ok(Self::LeftOuter),
            (true, true, false) => Ok(Self::LeftKeysFull),
            (false, true, true) => Ok(Self::FullOuter),
            _ => Err(Error::BadJoinType),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum IxKeyPolicy {
    FailMissing,
    SkipMissing,
    ForwardNone,
}

impl IxKeyPolicy {
    pub fn from_strict_optional(strict: bool, optional: bool) -> Result<Self> {
        match (strict, optional) {
            (false, false) => Ok(Self::SkipMissing),
            (false, true) => Err(Error::BadIxKeyPolicy),
            (true, false) => Ok(Self::FailMissing),
            (true, true) => Ok(Self::ForwardNone),
        }
    }
}

#[derive(Debug, Clone, Copy)]
#[pyclass]
pub struct OperatorStats {
    #[pyo3(get, set)]
    pub time: Option<Timestamp>,
    #[pyo3(get, set)]
    pub lag: Option<u64>,
    #[pyo3(get, set)]
    pub done: bool,
}

impl OperatorStats {
    pub fn latency(&self, now: SystemTime) -> Option<u64> {
        if let Some(time) = self.time {
            let duration = u64::try_from(
                now.duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_millis(),
            )
            .unwrap();
            if duration < time.0 {
                Some(0)
            } else {
                Some(duration - time.0)
            }
        } else {
            None
        }
    }
}

#[derive(Debug, Clone)]
#[pyclass]
pub struct ProberStats {
    #[pyo3(get, set)]
    pub input_stats: OperatorStats,
    #[pyo3(get, set)]
    pub output_stats: OperatorStats,
    #[pyo3(get, set)]
    pub operators_stats: HashMap<usize, OperatorStats>,
    #[pyo3(get, set)]
    pub connector_stats: Vec<(String, ConnectorStats)>,
}

pub type OnDataFn = Box<dyn FnMut(Key, &[Value], Timestamp, isize) -> DynResult<()>>;
pub type OnTimeEndFn = Box<dyn FnMut(Timestamp) -> DynResult<()>>;
pub type OnEndFn = Box<dyn FnMut() -> DynResult<()>>;

pub struct SubscribeCallbacks {
    pub wrapper: BatchWrapper,
    pub on_data: Option<OnDataFn>,
    pub on_time_end: Option<OnTimeEndFn>,
    pub on_end: Option<OnEndFn>,
}

pub struct SubscribeCallbacksBuilder {
    inner: SubscribeCallbacks,
}

impl SubscribeCallbacksBuilder {
    pub fn new() -> Self {
        Self {
            inner: SubscribeCallbacks {
                wrapper: BatchWrapper::None,
                on_data: None,
                on_time_end: None,
                on_end: None,
            },
        }
    }

    #[must_use]
    pub fn build(self) -> SubscribeCallbacks {
        self.inner
    }

    #[must_use]
    pub fn wrapper(mut self, wrapper: BatchWrapper) -> Self {
        self.inner.wrapper = wrapper;
        self
    }

    #[must_use]
    pub fn on_data(mut self, on_data: OnDataFn) -> Self {
        self.inner.on_data = Some(on_data);
        self
    }

    #[must_use]
    pub fn on_time_end(mut self, on_time_end: OnTimeEndFn) -> Self {
        self.inner.on_time_end = Some(on_time_end);
        self
    }

    #[must_use]
    pub fn on_end(mut self, on_end: OnEndFn) -> Self {
        self.inner.on_end = Some(on_end);
        self
    }
}

impl Default for SubscribeCallbacksBuilder {
    fn default() -> Self {
        Self::new()
    }
}

pub type ExportedTableCallback = Box<dyn FnMut() -> ControlFlow<()> + Send>;

pub trait ExportedTable: Send + Sync + Any {
    fn failed(&self) -> bool;

    fn properties(&self) -> Arc<TableProperties>;

    fn frontier(&self) -> TotalFrontier<Timestamp>;

    fn data_from_offset(&self, offset: usize) -> (Vec<DataRow>, usize);

    fn subscribe(&self, callback: ExportedTableCallback);

    fn snapshot_at(&self, frontier: TotalFrontier<Timestamp>) -> Vec<(Key, Vec<Value>)> {
        let (mut data, _offset) = self.data_from_offset(0);
        data.retain(|row| frontier.is_time_done(&row.time));
        data.sort_unstable_by(|a, b| a.key.cmp(&b.key).then_with(|| a.values.cmp(&b.values)));
        data.dedup_by(|new, old| {
            if new.key == old.key && new.values == old.values {
                old.diff += new.diff;
                true
            } else {
                false
            }
        });
        data.into_iter()
            .filter_map(|row| {
                (row.diff != 0).then(|| {
                    assert_eq!(row.diff, 1, "row had a final count different from 1");
                    (row.key, row.values)
                })
            })
            .collect()
    }
}

pub trait Graph {
    fn worker_index(&self) -> usize;

    fn worker_count(&self) -> usize;

    fn thread_count(&self) -> usize;

    fn process_count(&self) -> usize;

    fn empty_universe(&self) -> Result<UniverseHandle>;

    fn empty_column(
        &self,
        universe_handle: UniverseHandle,
        column_properties: Arc<ColumnProperties>,
    ) -> Result<ColumnHandle>;

    fn empty_table(&self, table_properties: Arc<TableProperties>) -> Result<TableHandle>;

    fn static_universe(&self, keys: Vec<Key>) -> Result<UniverseHandle>;

    fn static_column(
        &self,
        universe_handle: UniverseHandle,
        values: Vec<(Key, Value)>,
        column_properties: Arc<ColumnProperties>,
    ) -> Result<ColumnHandle>;

    fn static_table(
        &self,
        data: Vec<DataRow>,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle>;

    fn expression_column(
        &self,
        wrapper: BatchWrapper,
        expression: Arc<Expression>,
        universe_handle: UniverseHandle,
        column_handles: Vec<ColumnHandle>,
        column_properties: Arc<ColumnProperties>,
        trace: Trace,
    ) -> Result<ColumnHandle>;

    fn expression_table(
        &self,
        table_handle: TableHandle,
        column_paths: Vec<ColumnPath>,
        expressions: Vec<ExpressionData>,
        wrapper: BatchWrapper,
        deterministic: bool,
    ) -> Result<TableHandle>;

    fn columns_to_table(
        &self,
        universe_handle: UniverseHandle,
        columns: Vec<ColumnHandle>,
    ) -> Result<TableHandle>;

    fn table_column(
        &self,
        universe_handle: UniverseHandle,
        table_handle: TableHandle,
        column_paths: ColumnPath,
    ) -> Result<ColumnHandle>;

    fn table_universe(&self, table_handle: TableHandle) -> Result<UniverseHandle>;

    fn table_properties(
        &self,
        table_handle: TableHandle,
        path: &ColumnPath,
    ) -> Result<Arc<TableProperties>>;

    fn flatten_table_storage(
        &self,
        table_handle: TableHandle,
        column_paths: Vec<ColumnPath>,
    ) -> Result<TableHandle>;

    fn async_apply_table(
        &self,
        function: Arc<dyn Fn(Key, &[Value]) -> BoxFuture<'static, DynResult<Value>> + Send + Sync>,
        table_handle: TableHandle,
        column_paths: Vec<ColumnPath>,
        table_properties: Arc<TableProperties>,
        trace: Trace,
        deterministic: bool,
    ) -> Result<TableHandle>;

    fn subscribe_table(
        &self,
        table_handle: TableHandle,
        column_paths: Vec<ColumnPath>,
        callbacks: SubscribeCallbacks,
        skip_persisted_batch: bool,
        skip_errors: bool,
    ) -> Result<()>;

    fn filter_table(
        &self,
        table_handle: TableHandle,
        filtering_column_path: ColumnPath,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle>;

    fn remove_retractions_from_table(
        &self,
        table_handle: TableHandle,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle>;

    fn forget(
        &self,
        table_handle: TableHandle,
        threshold_time_column_path: ColumnPath,
        current_time_column_path: ColumnPath,
        mark_forgetting_records: bool,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle>;

    fn forget_immediately(
        &self,
        table_handle: TableHandle,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle>;

    fn filter_out_results_of_forgetting(
        &self,
        table_handle: TableHandle,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle>;

    fn freeze(
        &self,
        table_handle: TableHandle,
        threshold_time_column_path: ColumnPath,
        current_time_column_path: ColumnPath,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle>;

    fn buffer(
        &self,
        table_handle: TableHandle,
        threshold_time_column_path: ColumnPath,
        current_time_column_path: ColumnPath,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle>;

    fn restrict_column(
        &self,
        universe_handle: UniverseHandle,
        column_handle: ColumnHandle,
    ) -> Result<ColumnHandle>;

    fn restrict_or_override_table_universe(
        &self,
        original_table_handle: TableHandle,
        new_table_handle: TableHandle,
        same_universes: bool,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle>;

    fn intersect_tables(
        &self,
        table_handle: TableHandle,
        other_table_handles: Vec<TableHandle>,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle>;

    fn subtract_table(
        &self,
        left_table_handle: TableHandle,
        right_table_handle: TableHandle,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle>;

    fn concat_tables(
        &self,
        table_handles: Vec<TableHandle>,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle>;

    fn flatten_table(
        &self,
        table_handle: TableHandle,
        flatten_column_path: ColumnPath,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle>;

    fn sort_table(
        &self,
        table_handle: TableHandle,
        key_column_path: ColumnPath,
        instance_column_path: ColumnPath,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle>;

    fn reindex_table(
        &self,
        table_handle: TableHandle,
        reindexing_column_path: ColumnPath,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle>;

    fn update_rows_table(
        &self,
        table_handle: TableHandle,
        update_handle: TableHandle,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle>;

    fn update_cells_table(
        &self,
        table_handle: TableHandle,
        update_handle: TableHandle,
        column_paths: Vec<ColumnPath>,
        update_paths: Vec<ColumnPath>,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle>;

    fn group_by_table(
        &self,
        table_handle: TableHandle,
        grouping_columns_paths: Vec<ColumnPath>,
        shard_policy: ShardPolicy,
        reducers: Vec<ReducerData>,
        set_id: bool,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle>;

    fn deduplicate(
        &self,
        table_handle: TableHandle,
        grouping_columns_paths: Vec<ColumnPath>,
        reduced_column_paths: Vec<ColumnPath>,
        combine_fn: StatefulCombineFn,
        external_persistent_id: Option<&ExternalPersistentId>,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle>;

    fn gradual_broadcast(
        &self,
        input_table_handle: TableHandle,
        threshold_table_handle: TableHandle,
        lower_path: ColumnPath,
        value_path: ColumnPath,
        upper_path: ColumnPath,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle>;

    fn use_external_index_as_of_now(
        &self,
        index_stream: ExternalIndexData,
        query_stream: ExternalIndexQuery,
        table_properties: Arc<TableProperties>,
        external_index: Box<dyn ExternalIndex>,
    ) -> Result<TableHandle>;

    fn ix_table(
        &self,
        to_ix_handle: TableHandle,
        key_handle: TableHandle,
        key_column_path: ColumnPath,
        ix_key_policy: IxKeyPolicy,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle>;

    fn join_tables(
        &self,
        left_data: JoinData,
        right_data: JoinData,
        shard_policy: ShardPolicy,
        join_type: JoinType,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle>;

    fn iterate<'a>(
        &'a self,
        iterated: Vec<LegacyTable>,
        iterated_with_universe: Vec<LegacyTable>,
        extra: Vec<LegacyTable>,
        limit: Option<u32>,
        logic: IterationLogic<'a>,
    ) -> Result<(Vec<LegacyTable>, Vec<LegacyTable>)>;

    fn complex_columns(&self, inputs: Vec<ComplexColumn>) -> Result<Vec<ColumnHandle>>;

    fn debug_universe(&self, tag: String, table_handle: TableHandle) -> Result<()>;

    fn debug_column(
        &self,
        tag: String,
        table_handle: TableHandle,
        column_path: ColumnPath,
    ) -> Result<()>;

    fn connector_table(
        &self,
        reader: Box<dyn ReaderBuilder>,
        parser: Box<dyn Parser>,
        commit_duration: Option<Duration>,
        parallel_readers: usize,
        table_properties: Arc<TableProperties>,
        external_persistent_id: Option<&ExternalPersistentId>,
    ) -> Result<TableHandle>;

    fn output_table(
        &self,
        data_sink: Box<dyn Writer>,
        data_formatter: Box<dyn Formatter>,
        table_handle: TableHandle,
        column_paths: Vec<ColumnPath>,
    ) -> Result<()>;

    fn set_operator_properties(&self, operator_properties: OperatorProperties) -> Result<()>;

    fn set_error_log(&self, error_log_handle: Option<ErrorLogHandle>) -> Result<()>;

    fn error_log(
        &self,
        table_properties: Arc<TableProperties>,
    ) -> Result<(TableHandle, ErrorLogHandle)>;

    fn attach_prober(
        &self,
        logic: Box<dyn FnMut(ProberStats)>,
        intermediate_probes_required: bool,
        run_callback_every_time: bool,
    ) -> Result<()>;

    fn probe_table(&self, table_handle: TableHandle, operator_id: usize) -> Result<()>;

    fn export_table(
        &self,
        table_handle: TableHandle,
        column_paths: Vec<ColumnPath>,
    ) -> Result<Arc<dyn ExportedTable>>;

    fn import_table(&self, table: Arc<dyn ExportedTable>) -> Result<TableHandle>;

    fn remove_errors_from_table(
        &self,
        table_handle: TableHandle,
        column_paths: Vec<ColumnPath>,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle>;
}

#[allow(clippy::module_name_repetitions)]
pub struct ScopedGraph(Cell<Option<*const dyn Graph>>);

impl ScopedGraph {
    pub fn new() -> Self {
        Self(Cell::new(None))
    }

    pub fn scoped<'a, R>(&'a self, value: &'a (dyn Graph + 'a), fun: impl FnOnce() -> R) -> R {
        // SAFETY: we will only allow to dereference the pointer after casting into a lifetime
        // outlived by 'a
        #[allow(clippy::transmute_ptr_to_ptr)] // the cast doesn't seem to be enough
        let new =
            unsafe { std::mem::transmute::<*const (dyn Graph + 'a), *const dyn Graph>(value) };
        let old = self.0.replace(Some(new));
        defer! {
            self.0.set(old);
        }
        fun()
    }

    pub fn with<R>(&self, fun: impl for<'a> FnOnce(&'a (dyn Graph + 'a)) -> R) -> Option<R> {
        self.0.get().map(|ptr| {
            // SAFETY: pointer is only stored inside while it is valid
            fun(unsafe { &*ptr })
        })
    }

    pub fn try_with<R>(
        &self,
        fun: impl for<'a> FnOnce(&'a (dyn Graph + 'a)) -> Result<R>,
    ) -> Result<R> {
        self.with(fun).ok_or(Error::GraphNotInScope)?
    }
}

impl Default for ScopedGraph {
    fn default() -> Self {
        Self::new()
    }
}

impl Graph for ScopedGraph {
    fn worker_index(&self) -> usize {
        self.try_with(|g| Ok(g.worker_index())).unwrap()
    }

    fn worker_count(&self) -> usize {
        self.try_with(|g| Ok(g.worker_count())).unwrap()
    }
    fn thread_count(&self) -> usize {
        self.try_with(|g| Ok(g.thread_count())).unwrap()
    }

    fn process_count(&self) -> usize {
        self.try_with(|g| Ok(g.process_count())).unwrap()
    }

    fn empty_universe(&self) -> Result<UniverseHandle> {
        #[allow(clippy::redundant_closure_for_method_calls)]
        self.try_with(|g| g.empty_universe())
    }

    fn empty_column(
        &self,
        universe_handle: UniverseHandle,
        column_properties: Arc<ColumnProperties>,
    ) -> Result<ColumnHandle> {
        self.try_with(|g| g.empty_column(universe_handle, column_properties))
    }

    fn empty_table(&self, table_properties: Arc<TableProperties>) -> Result<TableHandle> {
        #[allow(clippy::redundant_closure_for_method_calls)]
        self.try_with(|g| g.empty_table(table_properties))
    }

    fn static_universe(&self, keys: Vec<Key>) -> Result<UniverseHandle> {
        self.try_with(|g| g.static_universe(keys))
    }

    fn static_column(
        &self,
        universe_handle: UniverseHandle,
        values: Vec<(Key, Value)>,
        column_properties: Arc<ColumnProperties>,
    ) -> Result<ColumnHandle> {
        self.try_with(|g| g.static_column(universe_handle, values, column_properties))
    }

    fn static_table(
        &self,
        data: Vec<DataRow>,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        self.try_with(|g| g.static_table(data, table_properties))
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
        self.try_with(|g| {
            g.expression_column(
                wrapper,
                expression,
                universe_handle,
                column_handles,
                column_properties,
                trace,
            )
        })
    }

    fn expression_table(
        &self,
        table_handle: TableHandle,
        column_paths: Vec<ColumnPath>,
        expressions: Vec<ExpressionData>,
        wrapper: BatchWrapper,
        deterministic: bool,
    ) -> Result<TableHandle> {
        self.try_with(|g| {
            g.expression_table(
                table_handle,
                column_paths,
                expressions,
                wrapper,
                deterministic,
            )
        })
    }

    fn columns_to_table(
        &self,
        universe_handle: UniverseHandle,
        columns: Vec<ColumnHandle>,
    ) -> Result<TableHandle> {
        self.try_with(|g| g.columns_to_table(universe_handle, columns))
    }

    fn table_column(
        &self,
        universe_handle: UniverseHandle,
        table_handle: TableHandle,
        column_path: ColumnPath,
    ) -> Result<ColumnHandle> {
        self.try_with(|g| g.table_column(universe_handle, table_handle, column_path))
    }

    fn table_universe(&self, table_handle: TableHandle) -> Result<UniverseHandle> {
        self.try_with(|g| g.table_universe(table_handle))
    }

    fn table_properties(
        &self,
        table_handle: TableHandle,
        path: &ColumnPath,
    ) -> Result<Arc<TableProperties>> {
        self.try_with(|g| g.table_properties(table_handle, path))
    }

    fn flatten_table_storage(
        &self,
        table_handle: TableHandle,
        column_paths: Vec<ColumnPath>,
    ) -> Result<TableHandle> {
        self.try_with(|g| g.flatten_table_storage(table_handle, column_paths))
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
        self.try_with(|g| {
            g.async_apply_table(
                function,
                table_handle,
                column_paths,
                table_properties,
                trace,
                deterministic,
            )
        })
    }

    fn subscribe_table(
        &self,
        table_handle: TableHandle,
        column_paths: Vec<ColumnPath>,
        callbacks: SubscribeCallbacks,
        skip_persisted_batch: bool,
        skip_errors: bool,
    ) -> Result<()> {
        self.try_with(|g| {
            g.subscribe_table(
                table_handle,
                column_paths,
                callbacks,
                skip_persisted_batch,
                skip_errors,
            )
        })
    }

    fn filter_table(
        &self,
        table_handle: TableHandle,
        filtering_column_path: ColumnPath,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        self.try_with(|g| g.filter_table(table_handle, filtering_column_path, table_properties))
    }

    fn remove_retractions_from_table(
        &self,
        table_handle: TableHandle,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        self.try_with(|g| g.remove_retractions_from_table(table_handle, table_properties))
    }

    fn forget(
        &self,
        table_handle: TableHandle,
        threshold_time_column_path: ColumnPath,
        current_time_column_path: ColumnPath,
        mark_forgetting_records: bool,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        self.try_with(|g| {
            g.forget(
                table_handle,
                threshold_time_column_path,
                current_time_column_path,
                mark_forgetting_records,
                table_properties,
            )
        })
    }

    fn use_external_index_as_of_now(
        &self,
        index_stream: ExternalIndexData,
        query_stream: ExternalIndexQuery,
        table_properties: Arc<TableProperties>,
        external_index: Box<dyn ExternalIndex>,
    ) -> Result<TableHandle> {
        self.try_with(|g| {
            g.use_external_index_as_of_now(
                index_stream,
                query_stream,
                table_properties,
                external_index,
            )
        })
    }
    fn forget_immediately(
        &self,
        table_handle: TableHandle,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        self.try_with(|g| g.forget_immediately(table_handle, table_properties))
    }

    fn filter_out_results_of_forgetting(
        &self,
        table_handle: TableHandle,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        self.try_with(|g| g.filter_out_results_of_forgetting(table_handle, table_properties))
    }

    fn freeze(
        &self,
        table_handle: TableHandle,
        threshold_time_column_path: ColumnPath,
        current_time_column_path: ColumnPath,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        self.try_with(|g| {
            g.freeze(
                table_handle,
                threshold_time_column_path,
                current_time_column_path,
                table_properties,
            )
        })
    }

    fn buffer(
        &self,
        table_handle: TableHandle,
        threshold_time_column_path: ColumnPath,
        current_time_column_path: ColumnPath,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        self.try_with(|g| {
            g.buffer(
                table_handle,
                threshold_time_column_path,
                current_time_column_path,
                table_properties,
            )
        })
    }

    fn restrict_column(
        &self,
        universe_handle: UniverseHandle,
        column_handle: ColumnHandle,
    ) -> Result<ColumnHandle> {
        self.try_with(|g| g.restrict_column(universe_handle, column_handle))
    }

    fn restrict_or_override_table_universe(
        &self,
        original_table_handle: TableHandle,
        new_table_handle: TableHandle,
        same_universes: bool,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        self.try_with(|g| {
            g.restrict_or_override_table_universe(
                original_table_handle,
                new_table_handle,
                same_universes,
                table_properties,
            )
        })
    }

    fn intersect_tables(
        &self,
        table_handle: TableHandle,
        other_table_handles: Vec<TableHandle>,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        self.try_with(|g| g.intersect_tables(table_handle, other_table_handles, table_properties))
    }

    fn subtract_table(
        &self,
        left_table_handle: TableHandle,
        right_table_handle: TableHandle,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        self.try_with(|g| g.subtract_table(left_table_handle, right_table_handle, table_properties))
    }

    fn concat_tables(
        &self,
        table_handles: Vec<TableHandle>,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        self.try_with(|g| g.concat_tables(table_handles, table_properties))
    }

    fn flatten_table(
        &self,
        table_handle: TableHandle,
        flatten_column_path: ColumnPath,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        self.try_with(|g| g.flatten_table(table_handle, flatten_column_path, table_properties))
    }

    fn sort_table(
        &self,
        table_handle: TableHandle,
        key_column_path: ColumnPath,
        instance_column_path: ColumnPath,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        self.try_with(|g| {
            g.sort_table(
                table_handle,
                key_column_path,
                instance_column_path,
                table_properties,
            )
        })
    }

    fn reindex_table(
        &self,
        table_handle: TableHandle,
        reindexing_column_path: ColumnPath,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        self.try_with(|g| g.reindex_table(table_handle, reindexing_column_path, table_properties))
    }

    fn update_rows_table(
        &self,
        table_handle: TableHandle,
        update_handle: TableHandle,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        self.try_with(|g| g.update_rows_table(table_handle, update_handle, table_properties))
    }

    fn update_cells_table(
        &self,
        table_handle: TableHandle,
        update_handle: TableHandle,
        column_paths: Vec<ColumnPath>,
        update_paths: Vec<ColumnPath>,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        self.try_with(|g| {
            g.update_cells_table(
                table_handle,
                update_handle,
                column_paths,
                update_paths,
                table_properties,
            )
        })
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
        self.try_with(|g| {
            g.group_by_table(
                table_handle,
                grouping_columns_paths,
                shard_policy,
                reducers,
                set_id,
                table_properties,
            )
        })
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
        self.try_with(|g| {
            g.deduplicate(
                table_handle,
                grouping_columns_paths,
                reduced_column_paths,
                combine_fn,
                external_persistent_id,
                table_properties,
            )
        })
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
        self.try_with(|g| {
            g.gradual_broadcast(
                input_table_handle,
                threshold_table_handle,
                lower_path,
                value_path,
                upper_path,
                table_properties,
            )
        })
    }

    fn ix_table(
        &self,
        to_ix_handle: TableHandle,
        key_handle: TableHandle,
        key_column_path: ColumnPath,
        ix_key_policy: IxKeyPolicy,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        self.try_with(|g| {
            g.ix_table(
                to_ix_handle,
                key_handle,
                key_column_path,
                ix_key_policy,
                table_properties,
            )
        })
    }

    fn join_tables(
        &self,
        left_data: JoinData,
        right_data: JoinData,
        shard_policy: ShardPolicy,
        join_type: JoinType,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        self.try_with(|g| {
            g.join_tables(
                left_data,
                right_data,
                shard_policy,
                join_type,
                table_properties,
            )
        })
    }

    fn iterate<'a>(
        &'a self,
        iterated: Vec<LegacyTable>,
        iterated_with_universe: Vec<LegacyTable>,
        extra: Vec<LegacyTable>,
        limit: Option<u32>,
        logic: IterationLogic<'a>,
    ) -> Result<(Vec<LegacyTable>, Vec<LegacyTable>)> {
        self.try_with(|g| g.iterate(iterated, iterated_with_universe, extra, limit, logic))
    }

    fn complex_columns(&self, inputs: Vec<ComplexColumn>) -> Result<Vec<ColumnHandle>> {
        self.try_with(|g| g.complex_columns(inputs))
    }

    fn debug_universe(&self, tag: String, table_handle: TableHandle) -> Result<()> {
        self.try_with(|g| g.debug_universe(tag, table_handle))
    }

    fn debug_column(
        &self,
        tag: String,
        table_handle: TableHandle,
        column_path: ColumnPath,
    ) -> Result<()> {
        self.try_with(|g| g.debug_column(tag, table_handle, column_path))
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
        self.try_with(|g| {
            g.connector_table(
                reader,
                parser,
                commit_duration,
                parallel_readers,
                table_properties,
                external_persistent_id,
            )
        })
    }

    fn output_table(
        &self,
        data_sink: Box<dyn Writer>,
        data_formatter: Box<dyn Formatter>,
        table_handle: TableHandle,
        column_paths: Vec<ColumnPath>,
    ) -> Result<()> {
        self.try_with(|g| g.output_table(data_sink, data_formatter, table_handle, column_paths))
    }

    fn set_operator_properties(&self, operator_properties: OperatorProperties) -> Result<()> {
        self.try_with(|g| g.set_operator_properties(operator_properties))
    }

    fn set_error_log(&self, error_log_handle: Option<ErrorLogHandle>) -> Result<()> {
        self.try_with(|g| g.set_error_log(error_log_handle))
    }

    fn error_log(
        &self,
        table_properties: Arc<TableProperties>,
    ) -> Result<(TableHandle, ErrorLogHandle)> {
        self.try_with(|g| g.error_log(table_properties))
    }

    fn attach_prober(
        &self,
        logic: Box<dyn FnMut(ProberStats)>,
        intermediate_probes_required: bool,
        run_callback_every_time: bool,
    ) -> Result<()> {
        self.try_with(|g| {
            g.attach_prober(logic, intermediate_probes_required, run_callback_every_time)
        })
    }

    fn probe_table(&self, table_handle: TableHandle, operator_id: usize) -> Result<()> {
        self.try_with(|g| g.probe_table(table_handle, operator_id))
    }

    fn export_table(
        &self,
        table_handle: TableHandle,
        column_paths: Vec<ColumnPath>,
    ) -> Result<Arc<dyn ExportedTable>> {
        self.try_with(|g| g.export_table(table_handle, column_paths))
    }

    fn import_table(&self, table: Arc<dyn ExportedTable>) -> Result<TableHandle> {
        self.try_with(|g| g.import_table(table))
    }

    fn remove_errors_from_table(
        &self,
        table_handle: TableHandle,
        column_paths: Vec<ColumnPath>,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        self.try_with(|g| g.remove_errors_from_table(table_handle, column_paths, table_properties))
    }
}
