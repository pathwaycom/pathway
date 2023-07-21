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

define_handle!(GrouperHandle);

define_handle!(JoinerHandle);

define_handle!(IxerHandle);

define_handle!(ConcatHandle);

define_handle!(FlattenHandle);

define_handle!(VennUniverseHandle);

pub type Table = (UniverseHandle, Vec<ColumnHandle>);

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
            Vec<Table>,
            Vec<Table>,
            Vec<Table>,
        ) -> DynResult<(Vec<Table>, Vec<Table>)>
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

    fn static_universe(&self, keys: Vec<Key>) -> Result<UniverseHandle>;

    fn static_column(
        &self,
        universe_handle: UniverseHandle,
        values: Vec<(Key, Value)>,
    ) -> Result<ColumnHandle>;

    fn expression_column(
        &self,
        wrapper: BatchWrapper,
        expression: Arc<Expression>,
        universe_handle: UniverseHandle,
        column_handles: Vec<ColumnHandle>,
        trace: Trace,
    ) -> Result<ColumnHandle>;

    fn async_apply_column(
        &self,
        function: Arc<dyn Fn(Key, &[Value]) -> BoxFuture<'static, DynResult<Value>> + Send + Sync>,
        universe_handle: UniverseHandle,
        column_handles: Vec<ColumnHandle>,
        trace: Trace,
    ) -> Result<ColumnHandle>;

    fn subscribe_column(
        &self,
        wrapper: BatchWrapper,
        callback: Box<dyn FnMut(Key, &[Value], u64, isize) -> DynResult<()>>,
        on_end: Box<dyn FnMut() -> DynResult<()>>,
        universe_handle: UniverseHandle,
        column_handles: Vec<ColumnHandle>,
    ) -> Result<()>;

    fn filter_universe(
        &self,
        universe_handle: UniverseHandle,
        column_handle: ColumnHandle,
    ) -> Result<UniverseHandle>;

    fn restrict_column(
        &self,
        universe_handle: UniverseHandle,
        column_handle: ColumnHandle,
    ) -> Result<ColumnHandle>;

    fn override_column_universe(
        &self,
        universe_handle: UniverseHandle,
        column_handle: ColumnHandle,
    ) -> Result<ColumnHandle>;

    fn id_column(&self, universe_handle: UniverseHandle) -> Result<ColumnHandle>;

    fn intersect_universe(&self, universe_handles: Vec<UniverseHandle>) -> Result<UniverseHandle>;

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

    fn flatten(
        &self,
        column_handle: ColumnHandle,
    ) -> Result<(UniverseHandle, ColumnHandle, FlattenHandle)>;

    fn explode(
        &self,
        flatten_handle: FlattenHandle,
        column_handle: ColumnHandle,
    ) -> Result<ColumnHandle>;

    fn sort(
        &self,
        key_column_handle: ColumnHandle,
        instance_column_handle: ColumnHandle,
    ) -> Result<(ColumnHandle, ColumnHandle)>;

    fn reindex_universe(&self, column_handle: ColumnHandle) -> Result<UniverseHandle>;

    fn reindex_column(
        &self,
        column_to_reindex: ColumnHandle,
        reindexing_column: ColumnHandle,
        reindexing_universe: UniverseHandle,
    ) -> Result<ColumnHandle>;

    fn update_rows(
        &self,
        universe_handle: UniverseHandle,
        column_handle: ColumnHandle,
        updates_handle: ColumnHandle,
    ) -> Result<ColumnHandle>;

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
        column_handle: ColumnHandle,
    ) -> Result<ColumnHandle>;

    fn grouper_reducer_column_ix(
        &self,
        grouper_handle: GrouperHandle,
        reducer: Reducer,
        ixer_handle: IxerHandle,
        column_handle: ColumnHandle,
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

    fn join(
        &self,
        left_universe_handle: UniverseHandle,
        left_column_handles: Vec<ColumnHandle>,
        right_universe_handle: UniverseHandle,
        right_column_handles: Vec<ColumnHandle>,
        join_type: JoinType,
    ) -> Result<JoinerHandle>;

    fn joiner_universe(&self, joiner_handle: JoinerHandle) -> Result<UniverseHandle>;

    fn joiner_left_column(
        &self,
        joiner_handle: JoinerHandle,
        column_handle: ColumnHandle,
    ) -> Result<ColumnHandle>;

    fn joiner_right_column(
        &self,
        joiner_handle: JoinerHandle,
        column_handle: ColumnHandle,
    ) -> Result<ColumnHandle>;

    fn iterate<'a>(
        &'a self,
        iterated: Vec<Table>,
        iterated_with_universe: Vec<Table>,
        extra: Vec<Table>,
        limit: Option<u32>,
        logic: IterationLogic<'a>,
    ) -> Result<(Vec<Table>, Vec<Table>)>;

    fn complex_columns(&self, inputs: Vec<ComplexColumn>) -> Result<Vec<ColumnHandle>>;

    fn debug_universe(&self, tag: String, universe_handle: UniverseHandle) -> Result<()>;

    fn debug_column(&self, tag: String, column_handle: ColumnHandle) -> Result<()>;

    fn connector_table(
        &self,
        reader: Box<dyn ReaderBuilder>,
        parser: Box<dyn Parser>,
        commit_duration: Option<Duration>,
        parallel_readers: usize,
    ) -> Result<(UniverseHandle, Vec<ColumnHandle>)>;

    fn output_table(
        &self,
        data_sink: Box<dyn Writer>,
        data_formatter: Box<dyn Formatter>,
        universe_handle: UniverseHandle,
        column_handles: Vec<ColumnHandle>,
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

    fn async_apply_column(
        &self,
        function: Arc<dyn Fn(Key, &[Value]) -> BoxFuture<'static, DynResult<Value>> + Send + Sync>,
        universe_handle: UniverseHandle,
        column_handles: Vec<ColumnHandle>,
        trace: Trace,
    ) -> Result<ColumnHandle> {
        self.try_with(|g| g.async_apply_column(function, universe_handle, column_handles, trace))
    }

    fn subscribe_column(
        &self,
        wrapper: BatchWrapper,
        callback: Box<dyn FnMut(Key, &[Value], u64, isize) -> DynResult<()>>,
        on_end: Box<dyn FnMut() -> DynResult<()>>,
        universe_handle: UniverseHandle,
        column_handles: Vec<ColumnHandle>,
    ) -> Result<()> {
        self.try_with(|g| {
            g.subscribe_column(wrapper, callback, on_end, universe_handle, column_handles)
        })
    }

    fn filter_universe(
        &self,
        universe_handle: UniverseHandle,
        column_handle: ColumnHandle,
    ) -> Result<UniverseHandle> {
        self.try_with(|g| g.filter_universe(universe_handle, column_handle))
    }

    fn restrict_column(
        &self,
        universe_handle: UniverseHandle,
        column_handle: ColumnHandle,
    ) -> Result<ColumnHandle> {
        self.try_with(|g| g.restrict_column(universe_handle, column_handle))
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

    fn flatten(
        &self,
        column_handle: ColumnHandle,
    ) -> Result<(UniverseHandle, ColumnHandle, FlattenHandle)> {
        self.try_with(|g| g.flatten(column_handle))
    }

    fn explode(
        &self,
        flatten_handle: FlattenHandle,
        column_handle: ColumnHandle,
    ) -> Result<ColumnHandle> {
        self.try_with(|g| g.explode(flatten_handle, column_handle))
    }

    fn sort(
        &self,
        key_column_handle: ColumnHandle,
        instance_column_handle: ColumnHandle,
    ) -> Result<(ColumnHandle, ColumnHandle)> {
        self.try_with(|g| g.sort(key_column_handle, instance_column_handle))
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

    fn update_rows(
        &self,
        universe_handle: UniverseHandle,
        column_handle: ColumnHandle,
        updates_handle: ColumnHandle,
    ) -> Result<ColumnHandle> {
        self.try_with(|g| g.update_rows(universe_handle, column_handle, updates_handle))
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
        column_handle: ColumnHandle,
    ) -> Result<ColumnHandle> {
        self.try_with(|g| g.grouper_reducer_column(grouper_handle, reducer, column_handle))
    }

    fn grouper_reducer_column_ix(
        &self,
        grouper_handle: GrouperHandle,
        reducer: Reducer,
        ixer_handle: IxerHandle,
        column_handle: ColumnHandle,
    ) -> Result<ColumnHandle> {
        self.try_with(|g| {
            g.grouper_reducer_column_ix(grouper_handle, reducer, ixer_handle, column_handle)
        })
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

    fn join(
        &self,
        left_universe_handle: UniverseHandle,
        left_column_handles: Vec<ColumnHandle>,
        right_universe_handle: UniverseHandle,
        right_column_handles: Vec<ColumnHandle>,
        join_type: JoinType,
    ) -> Result<JoinerHandle> {
        self.try_with(|g| {
            g.join(
                left_universe_handle,
                left_column_handles,
                right_universe_handle,
                right_column_handles,
                join_type,
            )
        })
    }

    fn joiner_universe(&self, joiner_handle: JoinerHandle) -> Result<UniverseHandle> {
        self.try_with(|g| g.joiner_universe(joiner_handle))
    }

    fn joiner_left_column(
        &self,
        joiner_handle: JoinerHandle,
        column_handle: ColumnHandle,
    ) -> Result<ColumnHandle> {
        self.try_with(|g| g.joiner_left_column(joiner_handle, column_handle))
    }

    fn joiner_right_column(
        &self,
        joiner_handle: JoinerHandle,
        column_handle: ColumnHandle,
    ) -> Result<ColumnHandle> {
        self.try_with(|g| g.joiner_right_column(joiner_handle, column_handle))
    }

    fn iterate<'a>(
        &'a self,
        iterated: Vec<Table>,
        iterated_with_universe: Vec<Table>,
        extra: Vec<Table>,
        limit: Option<u32>,
        logic: IterationLogic<'a>,
    ) -> Result<(Vec<Table>, Vec<Table>)> {
        self.try_with(|g| g.iterate(iterated, iterated_with_universe, extra, limit, logic))
    }

    fn complex_columns(&self, inputs: Vec<ComplexColumn>) -> Result<Vec<ColumnHandle>> {
        self.try_with(|g| g.complex_columns(inputs))
    }

    fn debug_universe(&self, tag: String, universe_handle: UniverseHandle) -> Result<()> {
        self.try_with(|g| g.debug_universe(tag, universe_handle))
    }

    fn debug_column(&self, tag: String, column_handle: ColumnHandle) -> Result<()> {
        self.try_with(|g| g.debug_column(tag, column_handle))
    }

    fn connector_table(
        &self,
        reader: Box<dyn ReaderBuilder>,
        parser: Box<dyn Parser>,
        commit_duration: Option<Duration>,
        parallel_readers: usize,
    ) -> Result<(UniverseHandle, Vec<ColumnHandle>)> {
        self.try_with(|g| g.connector_table(reader, parser, commit_duration, parallel_readers))
    }

    fn output_table(
        &self,
        data_sink: Box<dyn Writer>,
        data_formatter: Box<dyn Formatter>,
        universe_handle: UniverseHandle,
        column_handles: Vec<ColumnHandle>,
    ) -> Result<()> {
        self.try_with(|g| {
            g.output_table(data_sink, data_formatter, universe_handle, column_handles)
        })
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
