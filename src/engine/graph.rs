use std::cell::Cell;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use futures::future::BoxFuture;
use id_arena::ArenaBehavior;
use pyo3::{pyclass, Python};
use scopeguard::defer;

use crate::connectors::data_format::{Formatter, Parser};
use crate::connectors::data_storage::{ReaderBuilder, Writer};
use crate::connectors::monitoring::ConnectorStats;
use crate::persistence::ExternalPersistentId;

use super::error::{DynResult, Trace};
use super::{Error, Expression, Key, Reducer, Result, Value};

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

define_handle!(GrouperHandle);

define_handle!(IxerHandle);

define_handle!(ConcatHandle);

define_handle!(VennUniverseHandle);

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
        let new = unsafe { std::mem::transmute(value as *const dyn Context) };
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

#[derive(PartialEq, Eq, PartialOrd, Ord, Debug)]
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
                    if *value == Value::None {
                        break; // FIXME needed in outer joins. Maybe have it as a separate function?
                    }
                    value = value.as_tuple()?.get(*i).ok_or(Error::IndexOutOfBounds)?;
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
                    value = value.as_tuple()?.get(*i).ok_or(Error::IndexOutOfBounds)?;
                }
                Ok(value.clone())
            }
        }
    }
}

pub struct ExpressionData {
    pub expression: Arc<Expression>,
    pub trace: Trace,
}

impl ExpressionData {
    pub fn new(expression: Arc<Expression>, trace: Trace) -> Self {
        ExpressionData { expression, trace }
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
    pub time: Option<u64>,
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
            if duration < time {
                Some(0)
            } else {
                Some(duration - time)
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

pub trait Graph {
    fn worker_index(&self) -> usize;

    fn worker_count(&self) -> usize;

    fn empty_universe(&self) -> Result<UniverseHandle>;

    fn empty_column(&self, universe_handle: UniverseHandle) -> Result<ColumnHandle>;

    fn empty_table(&self) -> Result<TableHandle>;

    fn static_universe(&self, keys: Vec<Key>) -> Result<UniverseHandle>;

    fn static_column(
        &self,
        universe_handle: UniverseHandle,
        values: Vec<(Key, Value)>,
    ) -> Result<ColumnHandle>;

    fn static_table(&self, values: Vec<(Key, Vec<Value>)>) -> Result<TableHandle>;

    fn expression_column(
        &self,
        wrapper: BatchWrapper,
        expression: Arc<Expression>,
        universe_handle: UniverseHandle,
        column_handles: Vec<ColumnHandle>,
        trace: Trace,
    ) -> Result<ColumnHandle>;

    fn expression_table(
        &self,
        table_handle: TableHandle,
        column_paths: Vec<ColumnPath>,
        expressions: Vec<ExpressionData>,
        wrapper: BatchWrapper,
    ) -> Result<TableHandle>;

    fn columns_to_table(
        &self,
        universe_handle: UniverseHandle,
        columns: Vec<(ColumnHandle, ColumnPath)>,
    ) -> Result<TableHandle>;

    fn table_column(
        &self,
        universe_handle: UniverseHandle,
        table_handle: TableHandle,
        column_paths: ColumnPath,
    ) -> Result<ColumnHandle>;

    fn table_universe(&self, table_handle: TableHandle) -> Result<UniverseHandle>;

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
        trace: Trace,
    ) -> Result<TableHandle>;

    fn subscribe_table(
        &self,
        wrapper: BatchWrapper,
        callback: Box<dyn FnMut(Key, &[Value], u64, isize) -> DynResult<()>>,
        on_end: Box<dyn FnMut() -> DynResult<()>>,
        table_handle: TableHandle,
        column_paths: Vec<ColumnPath>,
    ) -> Result<()>;

    fn filter_universe(
        &self,
        universe_handle: UniverseHandle,
        column_handle: ColumnHandle,
    ) -> Result<UniverseHandle>;

    fn filter_table(
        &self,
        table_handle: TableHandle,
        filtering_column_path: ColumnPath,
    ) -> Result<TableHandle>;

    fn forget(
        &self,
        storage_handle: TableHandle,
        threshold_time_column_path: ColumnPath,
        current_time_column_path: ColumnPath,
    ) -> Result<TableHandle>;

    fn freeze(
        &self,
        table_handle: TableHandle,
        threshold_time_column_path: ColumnPath,
        current_time_column_path: ColumnPath,
    ) -> Result<TableHandle>;

    fn buffer(
        &self,
        table_handle: TableHandle,
        threshold_time_column_path: ColumnPath,
        current_time_column_path: ColumnPath,
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
    ) -> Result<TableHandle>;

    fn override_column_universe(
        &self,
        universe_handle: UniverseHandle,
        column_handle: ColumnHandle,
    ) -> Result<ColumnHandle>;

    fn id_column(&self, universe_handle: UniverseHandle) -> Result<ColumnHandle>;

    fn intersect_universe(&self, universe_handles: Vec<UniverseHandle>) -> Result<UniverseHandle>;

    fn intersect_tables(
        &self,
        table_handle: TableHandle,
        other_table_handles: Vec<TableHandle>,
    ) -> Result<TableHandle>;

    fn subtract_table(
        &self,
        left_table_handle: TableHandle,
        right_table_handle: TableHandle,
    ) -> Result<TableHandle>;

    fn union_universe(&self, universe_handles: Vec<UniverseHandle>) -> Result<UniverseHandle>;

    fn venn_universes(
        &self,
        left_universe_handle: UniverseHandle,
        right_universe_handle: UniverseHandle,
    ) -> Result<VennUniverseHandle>;

    fn venn_universes_only_left(
        &self,
        venn_universes_handle: VennUniverseHandle,
    ) -> Result<UniverseHandle>;

    fn venn_universes_only_right(
        &self,
        venn_universes_handle: VennUniverseHandle,
    ) -> Result<UniverseHandle>;

    fn venn_universes_both(
        &self,
        venn_universes_handle: VennUniverseHandle,
    ) -> Result<UniverseHandle>;

    fn concat(&self, universe_handles: Vec<UniverseHandle>) -> Result<ConcatHandle>;

    fn concat_universe(&self, concat_handle: ConcatHandle) -> Result<UniverseHandle>;

    fn concat_column(
        &self,
        concat_handle: ConcatHandle,
        column_handles: Vec<ColumnHandle>,
    ) -> Result<ColumnHandle>;

    fn concat_tables(&self, table_handles: Vec<TableHandle>) -> Result<TableHandle>;

    fn flatten_table(
        &self,
        table_handle: TableHandle,
        flatten_column_path: ColumnPath,
    ) -> Result<TableHandle>;

    fn sort(
        &self,
        key_column_handle: ColumnHandle,
        instance_column_handle: ColumnHandle,
    ) -> Result<(ColumnHandle, ColumnHandle)>;

    fn sort_table(
        &self,
        table_handle: TableHandle,
        key_column_path: ColumnPath,
        instance_column_path: ColumnPath,
    ) -> Result<TableHandle>;

    fn reindex_universe(&self, column_handle: ColumnHandle) -> Result<UniverseHandle>;

    fn reindex_column(
        &self,
        column_to_reindex: ColumnHandle,
        reindexing_column: ColumnHandle,
        reindexing_universe: UniverseHandle,
    ) -> Result<ColumnHandle>;

    fn reindex_table(
        &self,
        table_handle: TableHandle,
        reindexing_column_path: ColumnPath,
    ) -> Result<TableHandle>;

    fn update_rows_table(
        &self,
        table_handle: TableHandle,
        update_handle: TableHandle,
    ) -> Result<TableHandle>;

    fn update_cells_table(
        &self,
        table_handle: TableHandle,
        update_handle: TableHandle,
        column_paths: Vec<ColumnPath>,
        update_paths: Vec<ColumnPath>,
    ) -> Result<TableHandle>;

    fn group_by(
        &self,
        universe_handle: UniverseHandle,
        column_handles: Vec<ColumnHandle>,
        requested_column_handles: Vec<ColumnHandle>,
    ) -> Result<GrouperHandle>;
    fn group_by_id(
        &self,
        universe_handle: UniverseHandle,
        column_handle: ColumnHandle,
        requested_column_handles: Vec<ColumnHandle>,
    ) -> Result<GrouperHandle>;

    fn grouper_universe(&self, grouper_handle: GrouperHandle) -> Result<UniverseHandle>;
    fn grouper_input_column(
        &self,
        grouper_handle: GrouperHandle,
        column_handle: ColumnHandle,
    ) -> Result<ColumnHandle>;
    fn grouper_count_column(&self, grouper_handle: GrouperHandle) -> Result<ColumnHandle>;
    fn grouper_reducer_column(
        &self,
        grouper_handle: GrouperHandle,
        reducer: Reducer,
        column_handles: Vec<ColumnHandle>,
    ) -> Result<ColumnHandle>;

    fn ix(
        &self,
        key_column_handle: ColumnHandle,
        input_universe_handle: UniverseHandle,
        ix_key_policy: IxKeyPolicy,
    ) -> Result<IxerHandle>;

    fn ix_column(
        &self,
        ixer_handle: IxerHandle,
        column_handle: ColumnHandle,
    ) -> Result<ColumnHandle>;

    fn ixer_universe(&self, ixer_handle: IxerHandle) -> Result<UniverseHandle>;

    fn join_tables(
        &self,
        left_table_handle: TableHandle,
        right_table_handle: TableHandle,
        left_column_paths: Vec<ColumnPath>,
        right_column_paths: Vec<ColumnPath>,
        join_type: JoinType,
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
        external_persistent_id: &Option<ExternalPersistentId>,
    ) -> Result<TableHandle>;

    fn output_table(
        &self,
        data_sink: Box<dyn Writer>,
        data_formatter: Box<dyn Formatter>,
        table_handle: TableHandle,
        column_paths: Vec<ColumnPath>,
    ) -> Result<()>;

    fn on_universe_data(
        &self,
        universe_handle: UniverseHandle,
        function: Box<dyn FnMut(&Key, isize) -> DynResult<()>>,
    ) -> Result<()>;

    fn on_column_data(
        &self,
        column_handle: ColumnHandle,
        function: Box<dyn FnMut(&Key, &Value, isize) -> DynResult<()>>,
    ) -> Result<()>;

    fn on_table_data(
        &self,
        table_handle: TableHandle,
        column_paths: Vec<ColumnPath>,
        function: Box<dyn FnMut(&Key, &[Value], isize) -> DynResult<()>>,
    ) -> Result<()>;

    fn attach_prober(
        &self,
        logic: Box<dyn FnMut(ProberStats)>,
        intermediate_probes_required: bool,
        run_callback_every_time: bool,
    ) -> Result<()>;

    fn probe_universe(&self, universe_handle: UniverseHandle, operator_id: usize) -> Result<()>;

    fn probe_column(&self, column_handle: ColumnHandle, operator_id: usize) -> Result<()>;
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
        let new = unsafe { std::mem::transmute(value as *const dyn Graph) };
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
    fn empty_universe(&self) -> Result<UniverseHandle> {
        #[allow(clippy::redundant_closure_for_method_calls)]
        self.try_with(|g| g.empty_universe())
    }

    fn empty_column(&self, universe_handle: UniverseHandle) -> Result<ColumnHandle> {
        self.try_with(|g| g.empty_column(universe_handle))
    }

    fn empty_table(&self) -> Result<TableHandle> {
        #[allow(clippy::redundant_closure_for_method_calls)]
        self.try_with(|g| g.empty_table())
    }

    fn static_universe(&self, keys: Vec<Key>) -> Result<UniverseHandle> {
        self.try_with(|g| g.static_universe(keys))
    }

    fn static_column(
        &self,
        universe_handle: UniverseHandle,
        values: Vec<(Key, Value)>,
    ) -> Result<ColumnHandle> {
        self.try_with(|g| g.static_column(universe_handle, values))
    }

    fn static_table(&self, values: Vec<(Key, Vec<Value>)>) -> Result<TableHandle> {
        self.try_with(|g| g.static_table(values))
    }

    fn expression_column(
        &self,
        wrapper: BatchWrapper,
        expression: Arc<Expression>,
        universe_handle: UniverseHandle,
        column_handles: Vec<ColumnHandle>,
        trace: Trace,
    ) -> Result<ColumnHandle> {
        self.try_with(|g| {
            g.expression_column(wrapper, expression, universe_handle, column_handles, trace)
        })
    }

    fn expression_table(
        &self,
        table_handle: TableHandle,
        column_paths: Vec<ColumnPath>,
        expressions: Vec<ExpressionData>,
        wrapper: BatchWrapper,
    ) -> Result<TableHandle> {
        self.try_with(|g| g.expression_table(table_handle, column_paths, expressions, wrapper))
    }

    fn columns_to_table(
        &self,
        universe_handle: UniverseHandle,
        columns: Vec<(ColumnHandle, ColumnPath)>,
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
        trace: Trace,
    ) -> Result<TableHandle> {
        self.try_with(|g| g.async_apply_table(function, table_handle, column_paths, trace))
    }

    fn subscribe_table(
        &self,
        wrapper: BatchWrapper,
        callback: Box<dyn FnMut(Key, &[Value], u64, isize) -> DynResult<()>>,
        on_end: Box<dyn FnMut() -> DynResult<()>>,
        table_handle: TableHandle,
        column_paths: Vec<ColumnPath>,
    ) -> Result<()> {
        self.try_with(|g| g.subscribe_table(wrapper, callback, on_end, table_handle, column_paths))
    }

    fn filter_universe(
        &self,
        universe_handle: UniverseHandle,
        column_handle: ColumnHandle,
    ) -> Result<UniverseHandle> {
        self.try_with(|g| g.filter_universe(universe_handle, column_handle))
    }

    fn filter_table(
        &self,
        table_handle: TableHandle,
        filtering_column_path: ColumnPath,
    ) -> Result<TableHandle> {
        self.try_with(|g| g.filter_table(table_handle, filtering_column_path))
    }

    fn forget(
        &self,
        table_handle: TableHandle,
        threshold_time_column_path: ColumnPath,
        current_time_column_path: ColumnPath,
    ) -> Result<TableHandle> {
        self.try_with(|g| {
            g.forget(
                table_handle,
                threshold_time_column_path,
                current_time_column_path,
            )
        })
    }

    fn freeze(
        &self,
        table_handle: TableHandle,
        threshold_time_column_path: ColumnPath,
        current_time_column_path: ColumnPath,
    ) -> Result<TableHandle> {
        self.try_with(|g| {
            g.freeze(
                table_handle,
                threshold_time_column_path,
                current_time_column_path,
            )
        })
    }

    fn buffer(
        &self,
        table_handle: TableHandle,
        threshold_time_column_path: ColumnPath,
        current_time_column_path: ColumnPath,
    ) -> Result<TableHandle> {
        self.try_with(|g| {
            g.buffer(
                table_handle,
                threshold_time_column_path,
                current_time_column_path,
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
    ) -> Result<TableHandle> {
        self.try_with(|g| {
            g.restrict_or_override_table_universe(
                original_table_handle,
                new_table_handle,
                same_universes,
            )
        })
    }

    fn override_column_universe(
        &self,
        universe_handle: UniverseHandle,
        column_handle: ColumnHandle,
    ) -> Result<ColumnHandle> {
        self.try_with(|g| g.override_column_universe(universe_handle, column_handle))
    }

    fn id_column(&self, universe_handle: UniverseHandle) -> Result<ColumnHandle> {
        self.try_with(|g| g.id_column(universe_handle))
    }

    fn intersect_universe(&self, universe_handles: Vec<UniverseHandle>) -> Result<UniverseHandle> {
        self.try_with(|g| g.intersect_universe(universe_handles))
    }

    fn intersect_tables(
        &self,
        table_handle: TableHandle,
        other_table_handles: Vec<TableHandle>,
    ) -> Result<TableHandle> {
        self.try_with(|g| g.intersect_tables(table_handle, other_table_handles))
    }

    fn subtract_table(
        &self,
        left_table_handle: TableHandle,
        right_table_handle: TableHandle,
    ) -> Result<TableHandle> {
        self.try_with(|g| g.subtract_table(left_table_handle, right_table_handle))
    }

    fn union_universe(&self, universe_handles: Vec<UniverseHandle>) -> Result<UniverseHandle> {
        self.try_with(|g| g.union_universe(universe_handles))
    }

    fn venn_universes(
        &self,
        left_universe_handle: UniverseHandle,
        right_universe_handle: UniverseHandle,
    ) -> Result<VennUniverseHandle> {
        self.try_with(|g| g.venn_universes(left_universe_handle, right_universe_handle))
    }

    fn venn_universes_only_left(
        &self,
        venn_universes_handle: VennUniverseHandle,
    ) -> Result<UniverseHandle> {
        self.try_with(|g| g.venn_universes_only_left(venn_universes_handle))
    }

    fn venn_universes_only_right(
        &self,
        venn_universes_handle: VennUniverseHandle,
    ) -> Result<UniverseHandle> {
        self.try_with(|g| g.venn_universes_only_right(venn_universes_handle))
    }

    fn venn_universes_both(
        &self,
        venn_universes_handle: VennUniverseHandle,
    ) -> Result<UniverseHandle> {
        self.try_with(|g| g.venn_universes_both(venn_universes_handle))
    }

    fn concat(&self, universe_handles: Vec<UniverseHandle>) -> Result<ConcatHandle> {
        self.try_with(|g| g.concat(universe_handles))
    }

    fn concat_universe(&self, concat_handle: ConcatHandle) -> Result<UniverseHandle> {
        self.try_with(|g| g.concat_universe(concat_handle))
    }

    fn concat_column(
        &self,
        concat_handle: ConcatHandle,
        column_handles: Vec<ColumnHandle>,
    ) -> Result<ColumnHandle> {
        self.try_with(|g| g.concat_column(concat_handle, column_handles))
    }

    fn concat_tables(&self, table_handles: Vec<TableHandle>) -> Result<TableHandle> {
        self.try_with(|g| g.concat_tables(table_handles))
    }

    fn flatten_table(
        &self,
        table_handle: TableHandle,
        flatten_column_path: ColumnPath,
    ) -> Result<TableHandle> {
        self.try_with(|g| g.flatten_table(table_handle, flatten_column_path))
    }

    fn sort(
        &self,
        key_column_handle: ColumnHandle,
        instance_column_handle: ColumnHandle,
    ) -> Result<(ColumnHandle, ColumnHandle)> {
        self.try_with(|g| g.sort(key_column_handle, instance_column_handle))
    }

    fn sort_table(
        &self,
        table_handle: TableHandle,
        key_column_path: ColumnPath,
        instance_column_path: ColumnPath,
    ) -> Result<TableHandle> {
        self.try_with(|g| g.sort_table(table_handle, key_column_path, instance_column_path))
    }

    fn reindex_universe(&self, column_handle: ColumnHandle) -> Result<UniverseHandle> {
        self.try_with(|g| g.reindex_universe(column_handle))
    }

    fn reindex_column(
        &self,
        column_to_reindex: ColumnHandle,
        reindexing_column: ColumnHandle,
        reindexing_universe: UniverseHandle,
    ) -> Result<ColumnHandle> {
        self.try_with(|g| {
            g.reindex_column(column_to_reindex, reindexing_column, reindexing_universe)
        })
    }

    fn reindex_table(
        &self,
        table_handle: TableHandle,
        reindexing_column_path: ColumnPath,
    ) -> Result<TableHandle> {
        self.try_with(|g| g.reindex_table(table_handle, reindexing_column_path))
    }

    fn update_rows_table(
        &self,
        table_handle: TableHandle,
        update_handle: TableHandle,
    ) -> Result<TableHandle> {
        self.try_with(|g| g.update_rows_table(table_handle, update_handle))
    }

    fn update_cells_table(
        &self,
        table_handle: TableHandle,
        update_handle: TableHandle,
        column_paths: Vec<ColumnPath>,
        update_paths: Vec<ColumnPath>,
    ) -> Result<TableHandle> {
        self.try_with(|g| {
            g.update_cells_table(table_handle, update_handle, column_paths, update_paths)
        })
    }

    fn group_by(
        &self,
        universe_handle: UniverseHandle,
        column_handles: Vec<ColumnHandle>,
        requested_column_handles: Vec<ColumnHandle>,
    ) -> Result<GrouperHandle> {
        self.try_with(|g| g.group_by(universe_handle, column_handles, requested_column_handles))
    }

    fn group_by_id(
        &self,
        universe_handle: UniverseHandle,
        column_handle: ColumnHandle,
        requested_column_handles: Vec<ColumnHandle>,
    ) -> Result<GrouperHandle> {
        self.try_with(|g| g.group_by_id(universe_handle, column_handle, requested_column_handles))
    }

    fn grouper_universe(&self, grouper_handle: GrouperHandle) -> Result<UniverseHandle> {
        self.try_with(|g| g.grouper_universe(grouper_handle))
    }

    fn grouper_input_column(
        &self,
        grouper_handle: GrouperHandle,
        column_handle: ColumnHandle,
    ) -> Result<ColumnHandle> {
        self.try_with(|g| g.grouper_input_column(grouper_handle, column_handle))
    }

    fn grouper_count_column(&self, grouper_handle: GrouperHandle) -> Result<ColumnHandle> {
        self.try_with(|g| g.grouper_count_column(grouper_handle))
    }

    fn grouper_reducer_column(
        &self,
        grouper_handle: GrouperHandle,
        reducer: Reducer,
        column_handles: Vec<ColumnHandle>,
    ) -> Result<ColumnHandle> {
        self.try_with(|g| g.grouper_reducer_column(grouper_handle, reducer, column_handles))
    }

    fn ix(
        &self,
        key_column_handle: ColumnHandle,
        input_universe_handle: UniverseHandle,
        ix_key_policy: IxKeyPolicy,
    ) -> Result<IxerHandle> {
        self.try_with(|g| g.ix(key_column_handle, input_universe_handle, ix_key_policy))
    }

    fn ix_column(
        &self,
        ixer_handle: IxerHandle,
        column_handle: ColumnHandle,
    ) -> Result<ColumnHandle> {
        self.try_with(|g| g.ix_column(ixer_handle, column_handle))
    }

    fn ixer_universe(&self, ixer_handle: IxerHandle) -> Result<UniverseHandle> {
        self.try_with(|g| g.ixer_universe(ixer_handle))
    }

    fn join_tables(
        &self,
        left_table_handle: TableHandle,
        right_table_handle: TableHandle,
        left_column_paths: Vec<ColumnPath>,
        right_column_paths: Vec<ColumnPath>,
        join_type: JoinType,
    ) -> Result<TableHandle> {
        self.try_with(|g| {
            g.join_tables(
                left_table_handle,
                right_table_handle,
                left_column_paths,
                right_column_paths,
                join_type,
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
        external_persistent_id: &Option<ExternalPersistentId>,
    ) -> Result<TableHandle> {
        self.try_with(|g| {
            g.connector_table(
                reader,
                parser,
                commit_duration,
                parallel_readers,
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

    fn on_universe_data(
        &self,
        universe_handle: UniverseHandle,
        function: Box<dyn FnMut(&Key, isize) -> DynResult<()>>,
    ) -> Result<()> {
        self.try_with(|g| g.on_universe_data(universe_handle, function))
    }

    fn on_column_data(
        &self,
        column_handle: ColumnHandle,
        function: Box<dyn FnMut(&Key, &Value, isize) -> DynResult<()>>,
    ) -> Result<()> {
        self.try_with(|g| g.on_column_data(column_handle, function))
    }

    fn on_table_data(
        &self,
        table_handle: TableHandle,
        column_paths: Vec<ColumnPath>,
        function: Box<dyn FnMut(&Key, &[Value], isize) -> DynResult<()>>,
    ) -> Result<()> {
        self.try_with(|g| g.on_table_data(table_handle, column_paths, function))
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

    fn probe_universe(&self, universe_handle: UniverseHandle, operator_id: usize) -> Result<()> {
        self.try_with(|g| g.probe_universe(universe_handle, operator_id))
    }

    fn probe_column(&self, column_handle: ColumnHandle, operator_id: usize) -> Result<()> {
        self.try_with(|g| g.probe_column(column_handle, operator_id))
    }
}
