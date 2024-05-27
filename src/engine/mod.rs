// Copyright © 2024 Pathway

// too sensitive for `Box<dyn FnMut(...)>`
#![allow(clippy::type_complexity)]

pub mod error;
pub mod license;
pub use self::error::{DataError, Error, Result};

pub mod report_error;

pub mod value;
pub use self::value::{CompoundType, Key, KeyImpl, ShardPolicy, Type, Value};

pub mod reduce;
pub use reduce::Reducer;

pub mod graph;
pub use graph::{
    BatchWrapper, ColumnHandle, ColumnPath, ColumnProperties, ComplexColumn, Computer,
    ConcatHandle, Context, DataRow, ErrorLogHandle, ExportedTable, ExportedTableCallback,
    ExpressionData, Graph, IterationLogic, IxKeyPolicy, IxerHandle, JoinData, JoinType,
    LegacyTable, OperatorStats, ProberStats, ReducerData, ScopedGraph, TableHandle,
    TableProperties, UniverseHandle,
};

pub mod http_server;
pub use http_server::maybe_run_http_server_thread;

pub mod dataflow;
pub use dataflow::{run_with_new_dataflow_graph, WakeupReceiver};

pub mod expression;
pub use expression::{
    AnyExpression, BoolExpression, DateTimeNaiveExpression, DateTimeUtcExpression,
    DurationExpression, Expression, Expressions, FloatExpression, IntExpression, PointerExpression,
    StringExpression,
};

pub mod progress_reporter;
pub mod time;
pub use time::{DateTimeNaive, DateTimeUtc, Duration};

pub mod frontier;
pub use frontier::TotalFrontier;

pub mod telemetry;
pub use telemetry::Config;

pub mod external_index_wrappers;

pub mod timestamp;
pub use timestamp::Timestamp;

pub mod py_object_wrapper;
pub use py_object_wrapper::PyObjectWrapper;
