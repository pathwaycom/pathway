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
use super::{Error, Expression, Key, Reducer, Result, Type, Value};

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
    pub column_properties: Arc<ColumnProperties>,
}

impl ExpressionData {
    pub fn new(expression: Arc<Expression>, column_properties: Arc<ColumnProperties>) -> Self {
        ExpressionData {
            expression,
            column_properties,
        }
    }
}

pub struct ReducerData {
    pub reducer: Reducer,
    pub column_paths: Vec<ColumnPath>,
}

impl ReducerData {
    pub fn new(reducer: Reducer, column_paths: Vec<ColumnPath>) -> Self {
        ReducerData {
            reducer,
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

#[derive(Clone, Debug)]
pub struct ColumnProperties {
    pub dtype: Type,
    pub append_only: bool,
    pub trace: Trace,
}

impl ColumnProperties {
    pub fn new() -> Self {
        Self {
            dtype: Type::Any,
            append_only: false,
            trace: Trace::Empty,
        }
    }
}

impl Default for ColumnProperties {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Debug)]
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

    pub fn from_paths(
        column_properties: Vec<(ColumnPath, Arc<ColumnProperties>)>,
    ) -> Result<TableProperties> {
        fn produce_nested_tuple(
            props: &[(Vec<usize>, Arc<ColumnProperties>)],
            depth: usize,
        ) -> TableProperties {
            if props.len() == 1 && props.first().unwrap().0.len() == depth {
                return TableProperties::Column(props.first().unwrap().1.clone());
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
                result.push(produce_nested_tuple(&props[begin..end], depth + 1));
                begin = end;
            }

            TableProperties::Table(result.as_slice().into())
        }

        let mut column_properties: Vec<(Vec<usize>, Arc<ColumnProperties>)> = column_properties
            .into_iter()
            .map(|(path, props)| match path {
                ColumnPath::ValuePath(path) => Ok((path, props)),
                ColumnPath::Key => Err(Error::ValueError(
                    "It is not allowed to use ids when creating table properties".into(),
                )),
            })
            .collect::<Result<_>>()?;

        column_properties
            .sort_unstable_by(|(left_path, _), (right_path, _)| left_path.cmp(right_path));

        Ok(produce_nested_tuple(column_properties.as_slice(), 0))
    }

    pub fn expression_table_properties(
        input_table_properties: &Arc<TableProperties>,
        column_properties: Vec<Arc<ColumnProperties>>,
    ) -> Self {
        if column_properties.is_empty() {
            return TableProperties::Empty;
        }
        let mut properties: Vec<_> = column_properties
            .into_iter()
            .map(TableProperties::Column)
            .collect();
        properties[0] = (*(*input_table_properties)).clone();
        TableProperties::Table(properties.into())
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
        values: Vec<(Key, Vec<Value>)>,
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
        table_properties: Arc<TableProperties>,
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

    fn filter_table(
        &self,
        table_handle: TableHandle,
        filtering_column_path: ColumnPath,
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

    fn id_column(&self, universe_handle: UniverseHandle) -> Result<ColumnHandle>;

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
        reducers: Vec<ReducerData>,
        set_id: bool,
        table_properties: Arc<TableProperties>,
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
        left_table_handle: TableHandle,
        right_table_handle: TableHandle,
        left_column_paths: Vec<ColumnPath>,
        right_column_paths: Vec<ColumnPath>,
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

    fn probe_table(&self, table_handle: TableHandle, operator_id: usize) -> Result<()>;
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
        values: Vec<(Key, Vec<Value>)>,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        self.try_with(|g| g.static_table(values, table_properties))
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
        table_properties: Arc<TableProperties>,
        trace: Trace,
    ) -> Result<TableHandle> {
        self.try_with(|g| {
            g.async_apply_table(
                function,
                table_handle,
                column_paths,
                table_properties,
                trace,
            )
        })
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

    fn filter_table(
        &self,
        table_handle: TableHandle,
        filtering_column_path: ColumnPath,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        self.try_with(|g| g.filter_table(table_handle, filtering_column_path, table_properties))
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

    fn id_column(&self, universe_handle: UniverseHandle) -> Result<ColumnHandle> {
        self.try_with(|g| g.id_column(universe_handle))
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
        reducers: Vec<ReducerData>,
        set_id: bool,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        self.try_with(|g| {
            g.group_by_table(
                table_handle,
                grouping_columns_paths,
                reducers,
                set_id,
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
        left_table_handle: TableHandle,
        right_table_handle: TableHandle,
        left_column_paths: Vec<ColumnPath>,
        right_column_paths: Vec<ColumnPath>,
        join_type: JoinType,
        table_properties: Arc<TableProperties>,
    ) -> Result<TableHandle> {
        self.try_with(|g| {
            g.join_tables(
                left_table_handle,
                right_table_handle,
                left_column_paths,
                right_column_paths,
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
        external_persistent_id: &Option<ExternalPersistentId>,
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

    fn probe_table(&self, table_handle: TableHandle, operator_id: usize) -> Result<()> {
        self.try_with(|g| g.probe_table(table_handle, operator_id))
    }
}
