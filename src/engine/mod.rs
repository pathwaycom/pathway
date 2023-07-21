// too sensitive for `Box<dyn FnMut(...)>`
#![allow(clippy::type_complexity)]

pub mod error;
pub use self::error::{Error, Result};

pub mod value;
pub use self::value::{Key, KeyImpl, Type, Value};

pub mod reduce;
pub use reduce::Reducer;

pub mod graph;
pub use graph::{
    BatchWrapper, ColumnHandle, ComplexColumn, Computer, ConcatHandle, Context, FlattenHandle,
    Graph, GrouperHandle, IterationLogic, IxKeyPolicy, IxerHandle, JoinType, JoinerHandle,
    OperatorStats, ProberStats, ScopedGraph, Table, UniverseHandle, VennUniverseHandle,
};

pub mod http_server;
pub use http_server::maybe_run_http_server_thread;

pub mod dataflow;
pub use dataflow::{run_with_new_dataflow_graph, VennUniverse, WakeupReceiver};

pub mod expression;
pub use expression::{
    AnyExpression, BoolExpression, DateTimeNaiveExpression, DateTimeUtcExpression,
    DurationExpression, Expression, Expressions, FloatExpression, IntExpression, PointerExpression,
    StringExpression,
};

pub mod progress_reporter;
pub mod time;
pub use time::{DateTimeNaive, DateTimeUtc, Duration};
