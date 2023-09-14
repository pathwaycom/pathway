// pyo3 macros seem to trigger this
#![allow(clippy::used_underscore_binding)]
// `PyRef`s need to be passed by value
#![allow(clippy::needless_pass_by_value)]

use pyo3::marker::Ungil;
use pyo3::prelude::*;
use pyo3::sync::GILOnceCell;
use std::os::unix::prelude::*;

use csv::ReaderBuilder as CsvReaderBuilder;
use differential_dataflow::consolidation::{consolidate, consolidate_updates};
use elasticsearch::{
    auth::Credentials as ESCredentials,
    http::{
        transport::{SingleNodeConnectionPool, TransportBuilder},
        Url,
    },
    Elasticsearch,
};
use itertools::Itertools;
use log::warn;
use numpy::{PyArray, PyReadonlyArrayDyn};
use once_cell::sync::{Lazy, OnceCell};
use postgres::{Client, NoTls};
use pyo3::exceptions::{
    PyBaseException, PyException, PyIOError, PyIndexError, PyKeyError, PyRuntimeError, PyTypeError,
    PyValueError, PyZeroDivisionError,
};
use pyo3::pyclass::CompareOp;
use pyo3::types::{PyBool, PyDict, PyFloat, PyInt, PyString, PyTuple, PyType};
use pyo3::{AsPyPointer, PyTypeInfo};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::producer::{DefaultProducerContext, ThreadedProducer};
use rdkafka::ClientConfig;
use s3::bucket::Bucket as S3Bucket;
use scopeguard::defer;
use send_wrapper::SendWrapper;
use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufWriter, Read};
use std::iter::zip;
use std::mem::take;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time;
use xxhash_rust::xxh3::Xxh3 as Hasher;

use self::threads::PythonThreadState;
use crate::connectors::data_format::{
    DebeziumMessageParser, DsvSettings, Formatter, IdentityParser, InnerSchemaField,
    JsonLinesFormatter, JsonLinesParser, NullFormatter, Parser, PsqlSnapshotFormatter,
    PsqlUpdatesFormatter,
};
use crate::connectors::data_storage::{
    ConnectorMode, CsvFilesystemReader, ElasticSearchWriter, FileWriter, FilesystemReader,
    KafkaReader, KafkaWriter, NullWriter, PsqlWriter, PythonReaderBuilder, ReaderBuilder,
    S3CsvReader, S3LinesReader, Writer,
};
use crate::engine::dataflow::config_from_env;
use crate::engine::error::{DynError, DynResult, Trace as EngineTrace};
use crate::engine::graph::ScopedContext;
use crate::engine::progress_reporter::MonitoringLevel;
use crate::engine::time::DateTime;
use crate::engine::{
    run_with_new_dataflow_graph, BatchWrapper, ColumnHandle, ConcatHandle, DateTimeNaive,
    DateTimeUtc, Duration, FlattenHandle, GrouperHandle, IxKeyPolicy, IxerHandle, JoinType,
    JoinerHandle, Key, KeyImpl, PointerExpression, Reducer, ScopedGraph, Type, UniverseHandle,
    Value, VennUniverseHandle,
};
use crate::engine::{AnyExpression, Context as EngineContext};
use crate::engine::{BoolExpression, Error as EngineError};
use crate::engine::{ComplexColumn as EngineComplexColumn, WakeupReceiver};
use crate::engine::{Computer as EngineComputer, Expressions};
use crate::engine::{DateTimeNaiveExpression, DateTimeUtcExpression, DurationExpression};
use crate::engine::{Expression, IntExpression};
use crate::engine::{FloatExpression, Graph};
use crate::engine::{StringExpression, Table as EngineTable};
use crate::persistence::config::{
    MetadataStorageConfig, PersistenceManagerOuterConfig, StreamStorageConfig,
};
use crate::persistence::{ExternalPersistentId, PersistentId};
use crate::pipe::{pipe, ReaderType, WriterType};

mod logging;
mod numba;
pub mod threads;

pub fn with_gil_and_pool<R>(f: impl FnOnce(Python) -> R + Ungil) -> R {
    Python::with_gil(|py| py.with_pool(f))
}

static CONVERT: GILOnceCell<PyObject> = GILOnceCell::new();

fn get_convert_python_module(py: Python<'_>) -> &PyAny {
    CONVERT
        .get_or_init(py, || {
            PyModule::import(py, "pathway.internals.utils.convert")
                .unwrap()
                .to_object(py)
        })
        .as_ref(py)
}

#[allow(unused)] // XXX
macro_rules! pytodo {
    () => {
        return Err(PyNotImplementedError::new_err(()));
    };
    ($($arg:tt)+) => {
        return Err(PyNotImplementedError::new_err(format!($($arg)+)));
    };
}

impl<'source> FromPyObject<'source> for Key {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        Ok(ob.extract::<PyRef<BasePointer>>()?.0)
    }
}

impl ToPyObject for Key {
    fn to_object(&self, py: Python<'_>) -> PyObject {
        BasePointer(*self).into_py(py)
    }
}

impl IntoPy<PyObject> for Key {
    fn into_py(self, py: Python<'_>) -> PyObject {
        BasePointer(self).into_py(py)
    }
}

fn value_from_python_datetime(ob: &PyAny) -> PyResult<Value> {
    let py = ob.py();
    let (timestamp_ns, is_tz_aware) = get_convert_python_module(py)
        .call_method1("_datetime_to_rust", (ob,))?
        .extract::<(i64, bool)>()?;
    if is_tz_aware {
        Ok(Value::DateTimeUtc(DateTimeUtc::new(timestamp_ns)))
    } else {
        Ok(Value::DateTimeNaive(DateTimeNaive::new(timestamp_ns)))
    }
}

fn value_from_python_timedelta(ob: &PyAny) -> PyResult<Value> {
    let py = ob.py();
    let duration_ns = get_convert_python_module(py)
        .call_method1("_timedelta_to_rust", (ob,))?
        .extract::<i64>()?;
    Ok(Value::Duration(Duration::new(duration_ns)))
}

fn value_from_pandas_timestamp(ob: &PyAny) -> PyResult<Value> {
    let py = ob.py();
    let (timestamp, is_tz_aware) = get_convert_python_module(py)
        .call_method1("_pd_timestamp_to_rust", (ob,))?
        .extract::<(i64, bool)>()?;
    if is_tz_aware {
        Ok(Value::DateTimeUtc(DateTimeUtc::new(timestamp)))
    } else {
        Ok(Value::DateTimeNaive(DateTimeNaive::new(timestamp)))
    }
}

fn value_from_pandas_timedelta(ob: &PyAny) -> PyResult<Value> {
    let py = ob.py();
    let duration = get_convert_python_module(py)
        .call_method1("_pd_timedelta_to_rust", (ob,))?
        .extract::<i64>()?;
    Ok(Value::Duration(Duration::new(duration)))
}

impl ToPyObject for DateTimeNaive {
    fn to_object(&self, py: Python<'_>) -> PyObject {
        get_convert_python_module(py)
            .call_method1("_pd_timestamp_from_naive_ns", (self.timestamp(),))
            .unwrap()
            .into_py(py)
    }
}

impl IntoPy<PyObject> for DateTimeNaive {
    fn into_py(self, py: Python<'_>) -> PyObject {
        self.to_object(py)
    }
}

impl ToPyObject for DateTimeUtc {
    fn to_object(&self, py: Python<'_>) -> PyObject {
        get_convert_python_module(py)
            .call_method1("_pd_timestamp_from_utc_ns", (self.timestamp(),))
            .unwrap()
            .into_py(py)
    }
}

impl IntoPy<PyObject> for DateTimeUtc {
    fn into_py(self, py: Python<'_>) -> PyObject {
        self.to_object(py)
    }
}

impl ToPyObject for Duration {
    fn to_object(&self, py: Python<'_>) -> PyObject {
        get_convert_python_module(py)
            .call_method1("_pd_timedelta_from_ns", (self.nanoseconds(),))
            .unwrap()
            .into_py(py)
    }
}

impl IntoPy<PyObject> for Duration {
    fn into_py(self, py: Python<'_>) -> PyObject {
        self.to_object(py)
    }
}

impl<'source> FromPyObject<'source> for Value {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        if ob.is_none() {
            Ok(Value::None)
        } else if PyString::is_exact_type_of(ob) {
            Ok(Value::from(
                ob.downcast::<PyString>()
                    .expect("type conversion should work for str")
                    .to_str()?,
            ))
        } else if PyInt::is_exact_type_of(ob) {
            Ok(Value::Int(
                ob.extract::<i64>()
                    .expect("type conversion should work for int"),
            ))
        } else if PyFloat::is_exact_type_of(ob) {
            Ok(Value::Float(
                ob.extract::<f64>()
                    .expect("type conversion should work for float")
                    .into(),
            ))
        } else if PyBool::is_exact_type_of(ob) {
            Ok(Value::Bool(
                ob.extract::<&PyBool>()
                    .expect("type conversion should work for bool")
                    .is_true(),
            ))
        } else if BasePointer::is_exact_type_of(ob) {
            Ok(Value::Pointer(
                ob.extract::<Key>()
                    .expect("type conversion should work for Key"),
            ))
        } else if let Ok(b) = ob.extract::<&PyBool>() {
            // Fallback checks from now on
            Ok(Value::Bool(b.is_true()))
        } else if let Ok(array) = ob.extract::<PyReadonlyArrayDyn<i64>>() {
            // single-element arrays convert to scalars, so we need to check for arrays first
            Ok(Value::from(array.as_array().to_owned()))
        } else if let Ok(array) = ob.extract::<PyReadonlyArrayDyn<i32>>() {
            Ok(Value::from(array.as_array().mapv(i64::from)))
        } else if let Ok(array) = ob.extract::<PyReadonlyArrayDyn<u32>>() {
            Ok(Value::from(array.as_array().mapv(i64::from)))
        } else if let Ok(array) = ob.extract::<PyReadonlyArrayDyn<f64>>() {
            Ok(Value::from(array.as_array().to_owned()))
        } else if let Ok(array) = ob.extract::<PyReadonlyArrayDyn<f32>>() {
            Ok(Value::from(array.as_array().mapv(f64::from)))
        } else if let Ok(i) = ob.extract::<i64>() {
            Ok(Value::Int(i))
        } else if let Ok(f) = ob.extract::<f64>() {
            // XXX: bigints go here
            Ok(Value::Float(f.into()))
        } else if let Ok(k) = ob.extract::<Key>() {
            Ok(Value::Pointer(k))
        } else if let Ok(s) = ob.downcast::<PyString>() {
            Ok(s.to_str()?.into())
        } else if let Ok(t) = ob.extract::<Vec<Self>>() {
            Ok(Value::from(t.as_slice()))
        } else {
            // XXX: check types, not names
            let type_name = ob.get_type().name()?;
            if type_name == "datetime" {
                return value_from_python_datetime(ob);
            } else if type_name == "timedelta" {
                return value_from_python_timedelta(ob);
            } else if type_name == "Timestamp" {
                return value_from_pandas_timestamp(ob);
            } else if type_name == "Timedelta" {
                return value_from_pandas_timedelta(ob);
            }

            if let Ok(vec) = ob.extract::<Vec<&PyAny>>() {
                // generate a nicer error message if the type of an element is the problem
                for v in vec {
                    v.extract::<Self>()?;
                }
            }

            Err(PyTypeError::new_err(format!(
                "unsupported value type: {}",
                ob.get_type().name()?
            )))
        }
    }
}

impl ToPyObject for Value {
    fn to_object(&self, py: Python<'_>) -> PyObject {
        match self {
            Self::None => py.None(),
            Self::Bool(b) => b.into_py(py),
            Self::Int(i) => i.into_py(py),
            Self::Float(f) => f.into_py(py),
            Self::Pointer(k) => k.into_py(py),
            Self::String(s) => s.into_py(py),
            Self::Tuple(t) => PyTuple::new(py, t.iter()).into(),
            Self::IntArray(a) => PyArray::from_array(py, a).into(),
            Self::FloatArray(a) => PyArray::from_array(py, a).into(),
            Self::DateTimeNaive(dt) => dt.into_py(py),
            Self::DateTimeUtc(dt) => dt.into_py(py),
            Self::Duration(d) => d.into_py(py),
        }
    }
}

impl IntoPy<PyObject> for Value {
    fn into_py(self, py: Python<'_>) -> PyObject {
        self.to_object(py)
    }
}

impl<'source> FromPyObject<'source> for Reducer {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        Ok(ob.extract::<PyRef<PyReducer>>()?.0)
    }
}

impl IntoPy<PyObject> for Reducer {
    fn into_py(self, py: Python<'_>) -> PyObject {
        PyReducer(self).into_py(py)
    }
}

impl<'source> FromPyObject<'source> for Type {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        Ok(ob.extract::<PyRef<PathwayType>>()?.0)
    }
}

impl IntoPy<PyObject> for Type {
    fn into_py(self, py: Python<'_>) -> PyObject {
        PathwayType(self).into_py(py)
    }
}

impl<'source> FromPyObject<'source> for ConnectorMode {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        Ok(ob.extract::<PyRef<PyConnectorMode>>()?.0)
    }
}

impl IntoPy<PyObject> for ConnectorMode {
    fn into_py(self, py: Python<'_>) -> PyObject {
        PyConnectorMode(self).into_py(py)
    }
}

impl<'source> FromPyObject<'source> for MonitoringLevel {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        Ok(ob.extract::<PyRef<PyMonitoringLevel>>()?.0)
    }
}

impl IntoPy<PyObject> for MonitoringLevel {
    fn into_py(self, py: Python<'_>) -> PyObject {
        PyMonitoringLevel(self).into_py(py)
    }
}

impl From<EngineError> for PyErr {
    fn from(mut error: EngineError) -> Self {
        match error.downcast::<PyErr>() {
            Ok(error) => return error,
            Err(other) => error = other,
        };
        Python::with_gil(|py| {
            if let EngineError::WithTrace { inner, trace } = error {
                let inner = PyErr::from(EngineError::from(inner));
                let args = (inner, trace);
                return PyErr::from_type(ENGINE_ERROR_WITH_TRACE_TYPE.as_ref(py), args);
            }
            let exception_type = match error {
                EngineError::TypeMismatch { .. } => PyTypeError::type_object(py),
                EngineError::DuplicateKey(_)
                | EngineError::ValueMissing
                | EngineError::KeyMissingInColumn(_)
                | EngineError::KeyMissingInUniverse(_) => PyKeyError::type_object(py),
                EngineError::DivisionByZero => PyZeroDivisionError::type_object(py),
                EngineError::IterationLimitTooSmall
                | EngineError::ValueError(_)
                | EngineError::PersistentIdNotAssigned(_)
                | EngineError::NoPersistentStorage(_)
                | EngineError::ParseError(_) => PyValueError::type_object(py),
                EngineError::IndexOutOfBounds => PyIndexError::type_object(py),
                _ => ENGINE_ERROR_TYPE.as_ref(py),
            };
            let message = error.to_string();
            PyErr::from_type(exception_type, message)
        })
    }
}

fn check_identity(a: &impl AsPyPointer, b: &impl AsPyPointer, msg: &'static str) -> PyResult<()> {
    if a.as_ptr() == b.as_ptr() {
        Ok(())
    } else {
        Err(PyValueError::new_err(msg))
    }
}

fn from_py_iterable<'py, T>(iterable: &'py PyAny) -> PyResult<Vec<T>>
where
    T: FromPyObject<'py>,
{
    iterable.iter()?.map(|obj| obj?.extract()).collect()
}

fn engine_tables_from_py_iterable(iterable: &PyAny) -> PyResult<Vec<EngineTable>> {
    let py = iterable.py();
    iterable
        .iter()?
        .map(|table| {
            let table: PyRef<Table> = table?.extract()?;
            Ok(table.to_engine(py))
        })
        .collect()
}

#[pyclass(module = "pathway.engine", frozen, subclass)]
pub struct BasePointer(Key);

#[pymethods]
impl BasePointer {
    pub fn __str__(&self) -> String {
        format!("{:#}", self.0)
    }

    pub fn __repr__(&self) -> String {
        format!("Pointer(\"{}\")", self.0.to_string().escape_default())
    }

    fn __hash__(&self) -> usize {
        self.0 .0 as usize
    }

    fn __int__(&self) -> KeyImpl {
        self.0 .0
    }

    fn __richcmp__(&self, other: &PyAny, op: CompareOp) -> Py<PyAny> {
        let py = other.py();
        if let Ok(other) = other.extract::<PyRef<Self>>() {
            return op.matches(self.0.cmp(&other.0)).into_py(py);
        }
        if let Ok(other) = other.extract::<f64>() {
            // XXX: comparisons to ±∞
            if other == f64::NEG_INFINITY {
                return op.matches(Ordering::Greater).into_py(py);
            }
            if other == f64::INFINITY {
                return op.matches(Ordering::Less).into_py(py);
            }
        }
        py.NotImplemented()
    }
}

#[pyclass(module = "pathway.engine", frozen, name = "Reducer")]
pub struct PyReducer(Reducer);

#[pymethods]
impl PyReducer {
    #[classattr]
    pub const ARG_MIN: Reducer = Reducer::ArgMin;

    #[classattr]
    pub const MIN: Reducer = Reducer::Min;

    #[classattr]
    pub const ARG_MAX: Reducer = Reducer::ArgMax;

    #[classattr]
    pub const MAX: Reducer = Reducer::Max;

    #[classattr]
    pub const FLOAT_SUM: Reducer = Reducer::FloatSum;

    #[classattr]
    pub const INT_SUM: Reducer = Reducer::IntSum;

    #[classattr]
    pub const ARRAY_SUM: Reducer = Reducer::ArraySum;

    #[classattr]
    pub const SORTED_TUPLE: Reducer = Reducer::SortedTuple;

    #[classattr]
    pub const TUPLE: Reducer = Reducer::Tuple;

    #[classattr]
    pub const UNIQUE: Reducer = Reducer::Unique;

    #[classattr]
    pub const ANY: Reducer = Reducer::Any;
}

#[derive(Clone, Copy, Debug)]
pub enum UnaryOperator {
    Inv,
    Neg,
}

#[derive(Clone, Copy, Debug)]
pub enum BinaryOperator {
    And,
    Or,
    Xor,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    Add,
    Sub,
    Mul,
    FloorDiv,
    TrueDiv,
    Mod,
    Pow,
    Lshift,
    Rshift,
    MatMul,
}

#[pyclass(module = "pathway.engine", frozen, name = "UnaryOperator")]
pub struct PyUnaryOperator(UnaryOperator);

#[pymethods]
impl PyUnaryOperator {
    #[classattr]
    pub const INV: UnaryOperator = UnaryOperator::Inv;
    #[classattr]
    pub const NEG: UnaryOperator = UnaryOperator::Neg;
}

impl<'source> FromPyObject<'source> for UnaryOperator {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        Ok(ob.extract::<PyRef<PyUnaryOperator>>()?.0)
    }
}

impl IntoPy<PyObject> for UnaryOperator {
    fn into_py(self, py: Python<'_>) -> PyObject {
        PyUnaryOperator(self).into_py(py)
    }
}

#[pyclass(module = "pathway.engine", frozen, name = "BinaryOperator")]
pub struct PyBinaryOperator(BinaryOperator);

#[pymethods]
impl PyBinaryOperator {
    #[classattr]
    pub const AND: BinaryOperator = BinaryOperator::And;
    #[classattr]
    pub const OR: BinaryOperator = BinaryOperator::Or;
    #[classattr]
    pub const XOR: BinaryOperator = BinaryOperator::Xor;
    #[classattr]
    pub const EQ: BinaryOperator = BinaryOperator::Eq;
    #[classattr]
    pub const NE: BinaryOperator = BinaryOperator::Ne;
    #[classattr]
    pub const LT: BinaryOperator = BinaryOperator::Lt;
    #[classattr]
    pub const LE: BinaryOperator = BinaryOperator::Le;
    #[classattr]
    pub const GT: BinaryOperator = BinaryOperator::Gt;
    #[classattr]
    pub const GE: BinaryOperator = BinaryOperator::Ge;
    #[classattr]
    pub const ADD: BinaryOperator = BinaryOperator::Add;
    #[classattr]
    pub const SUB: BinaryOperator = BinaryOperator::Sub;
    #[classattr]
    pub const MUL: BinaryOperator = BinaryOperator::Mul;
    #[classattr]
    pub const FLOOR_DIV: BinaryOperator = BinaryOperator::FloorDiv;
    #[classattr]
    pub const TRUE_DIV: BinaryOperator = BinaryOperator::TrueDiv;
    #[classattr]
    pub const MOD: BinaryOperator = BinaryOperator::Mod;
    #[classattr]
    pub const POW: BinaryOperator = BinaryOperator::Pow;
    #[classattr]
    pub const LSHIFT: BinaryOperator = BinaryOperator::Lshift;
    #[classattr]
    pub const RSHIFT: BinaryOperator = BinaryOperator::Rshift;
    #[classattr]
    pub const MATMUL: BinaryOperator = BinaryOperator::MatMul;
}

impl<'source> FromPyObject<'source> for BinaryOperator {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        Ok(ob.extract::<PyRef<PyBinaryOperator>>()?.0)
    }
}

impl IntoPy<PyObject> for BinaryOperator {
    fn into_py(self, py: Python<'_>) -> PyObject {
        PyBinaryOperator(self).into_py(py)
    }
}

#[pyclass(module = "pathway.engine", frozen, name = "Expression")]
pub struct PyExpression {
    inner: Arc<Expression>,
    gil: bool,
}

impl PyExpression {
    fn new(inner: Arc<Expression>, gil: bool) -> Self {
        Self { inner, gil }
    }
}

macro_rules! unary_op {
    ($expression:path, $e:expr $(, $arg:expr)*) => {
        Self::new(
            Arc::new(Expression::from($expression($e.inner.clone() $(, $arg)*))),
            $e.gil,
        )
    };
}

macro_rules! binary_op {
    ($expression:path, $lhs:expr, $rhs:expr $(, $arg:expr)*) => {
        Self::new(
            Arc::new(Expression::from($expression(
                $lhs.inner.clone(),
                $rhs.inner.clone(),
                $($arg,)*
            ))),
            $lhs.gil || $rhs.gil,
        )
    };
}

macro_rules! unary_expr {
    ($name:ident, $expression:path $(, $arg:ident : $type:ty)*) => {
        #[pymethods]
        impl PyExpression {
            #[staticmethod]
            fn $name(expr: &Self $(, $arg : $type)*) -> Self {
                unary_op!($expression, expr $(, $arg)*)
            }
        }
    };
}

macro_rules! binary_expr {
    ($name:ident, $expression:path $(, $arg:ident : $type:ty)*) => {
        #[pymethods]
        impl PyExpression {
            #[staticmethod]
            fn $name(lhs: &Self, rhs: &Self $(, $arg : $type)*) -> Self {
                binary_op!($expression, lhs, rhs $(, $arg)*)
            }
        }
    };
}

#[pymethods]
impl PyExpression {
    #[staticmethod]
    fn r#const(value: Value) -> Self {
        Self::new(Arc::new(Expression::new_const(value)), false)
    }

    #[staticmethod]
    fn argument(index: usize) -> Self {
        Self::new(
            Arc::new(Expression::Any(AnyExpression::Argument(index))),
            false,
        )
    }

    #[staticmethod]
    #[pyo3(signature = (function, *args))]
    fn apply(function: Py<PyAny>, args: Vec<PyRef<PyExpression>>) -> Self {
        let args = args
            .into_iter()
            .map(|expr| expr.inner.clone())
            .collect_vec();
        Self::new(
            Arc::new(Expression::Any(AnyExpression::Apply(
                Box::new(move |input| {
                    with_gil_and_pool(|py| -> DynResult<Value> {
                        let args = PyTuple::new(py, input);
                        Ok(function.call1(py, args)?.extract::<Value>(py)?)
                    })
                }),
                args.into(),
            ))),
            true,
        )
    }

    #[staticmethod]
    fn unary_expression(
        expr: &PyExpression,
        operator: UnaryOperator,
        expr_dtype: Type,
    ) -> Option<Self> {
        match (operator, expr_dtype) {
            (UnaryOperator::Inv, Type::Bool) => Some(unary_op!(BoolExpression::Not, expr)),
            (UnaryOperator::Neg, Type::Int) => Some(unary_op!(IntExpression::Neg, expr)),
            (UnaryOperator::Neg, Type::Float) => Some(unary_op!(FloatExpression::Neg, expr)),
            (UnaryOperator::Neg, Type::Duration) => Some(unary_op!(DurationExpression::Neg, expr)),
            (_, _) => None,
        }
    }

    #[allow(clippy::too_many_lines)]
    #[staticmethod]
    fn binary_expression(
        lhs: &PyExpression,
        rhs: &PyExpression,
        operator: BinaryOperator,
        left_dtype: Type,
        right_dtype: Type,
    ) -> Option<Self> {
        type Tp = Type;
        type Op = BinaryOperator;
        type AnyE = AnyExpression;
        type BoolE = BoolExpression;
        type IntE = IntExpression;
        type FloatE = FloatExpression;
        type StringE = StringExpression;
        type DurationE = DurationExpression;
        match (operator, left_dtype, right_dtype) {
            (Op::And, Tp::Bool, Tp::Bool) => Some(binary_op!(BoolE::And, lhs, rhs)),
            (Op::Or, Tp::Bool, Tp::Bool) => Some(binary_op!(BoolE::Or, lhs, rhs)),
            (Op::Xor, Tp::Bool, Tp::Bool) => Some(binary_op!(BoolE::Xor, lhs, rhs)),
            (Op::Eq, Tp::Int, Tp::Int) => Some(binary_op!(BoolE::IntEq, lhs, rhs)),
            (Op::Ne, Tp::Int, Tp::Int) => Some(binary_op!(BoolE::IntNe, lhs, rhs)),
            (Op::Lt, Tp::Int, Tp::Int) => Some(binary_op!(BoolE::IntLt, lhs, rhs)),
            (Op::Le, Tp::Int, Tp::Int) => Some(binary_op!(BoolE::IntLe, lhs, rhs)),
            (Op::Gt, Tp::Int, Tp::Int) => Some(binary_op!(BoolE::IntGt, lhs, rhs)),
            (Op::Ge, Tp::Int, Tp::Int) => Some(binary_op!(BoolE::IntGe, lhs, rhs)),
            (Op::Eq, Tp::Bool, Tp::Bool) => Some(binary_op!(BoolE::BoolEq, lhs, rhs)),
            (Op::Ne, Tp::Bool, Tp::Bool) => Some(binary_op!(BoolE::BoolNe, lhs, rhs)),
            (Op::Lt, Tp::Bool, Tp::Bool) => Some(binary_op!(BoolE::BoolLt, lhs, rhs)),
            (Op::Le, Tp::Bool, Tp::Bool) => Some(binary_op!(BoolE::BoolLe, lhs, rhs)),
            (Op::Gt, Tp::Bool, Tp::Bool) => Some(binary_op!(BoolE::BoolGt, lhs, rhs)),
            (Op::Ge, Tp::Bool, Tp::Bool) => Some(binary_op!(BoolE::BoolGe, lhs, rhs)),
            (Op::Add, Tp::Int, Tp::Int) => Some(binary_op!(IntE::Add, lhs, rhs)),
            (Op::Sub, Tp::Int, Tp::Int) => Some(binary_op!(IntE::Sub, lhs, rhs)),
            (Op::Mul, Tp::Int, Tp::Int) => Some(binary_op!(IntE::Mul, lhs, rhs)),
            (Op::FloorDiv, Tp::Int, Tp::Int) => Some(binary_op!(IntE::FloorDiv, lhs, rhs)),
            (Op::TrueDiv, Tp::Int, Tp::Int) => Some(binary_op!(FloatE::IntTrueDiv, lhs, rhs)),
            (Op::Mod, Tp::Int, Tp::Int) => Some(binary_op!(IntE::Mod, lhs, rhs)),
            (Op::Pow, Tp::Int, Tp::Int) => Some(binary_op!(IntE::Pow, lhs, rhs)),
            (Op::Lshift, Tp::Int, Tp::Int) => Some(binary_op!(IntE::Lshift, lhs, rhs)),
            (Op::Rshift, Tp::Int, Tp::Int) => Some(binary_op!(IntE::Rshift, lhs, rhs)),
            (Op::And, Tp::Int, Tp::Int) => Some(binary_op!(IntE::And, lhs, rhs)),
            (Op::Or, Tp::Int, Tp::Int) => Some(binary_op!(IntE::Or, lhs, rhs)),
            (Op::Xor, Tp::Int, Tp::Int) => Some(binary_op!(IntE::Xor, lhs, rhs)),
            (Op::Eq, Tp::Float, Tp::Float) => Some(binary_op!(BoolE::FloatEq, lhs, rhs)),
            (Op::Ne, Tp::Float, Tp::Float) => Some(binary_op!(BoolE::FloatNe, lhs, rhs)),
            (Op::Lt, Tp::Float, Tp::Float) => Some(binary_op!(BoolE::FloatLt, lhs, rhs)),
            (Op::Le, Tp::Float, Tp::Float) => Some(binary_op!(BoolE::FloatLe, lhs, rhs)),
            (Op::Gt, Tp::Float, Tp::Float) => Some(binary_op!(BoolE::FloatGt, lhs, rhs)),
            (Op::Ge, Tp::Float, Tp::Float) => Some(binary_op!(BoolE::FloatGe, lhs, rhs)),
            (Op::Add, Tp::Float, Tp::Float) => Some(binary_op!(FloatE::Add, lhs, rhs)),
            (Op::Sub, Tp::Float, Tp::Float) => Some(binary_op!(FloatE::Sub, lhs, rhs)),
            (Op::Mul, Tp::Float, Tp::Float) => Some(binary_op!(FloatE::Mul, lhs, rhs)),
            (Op::FloorDiv, Tp::Float, Tp::Float) => Some(binary_op!(FloatE::FloorDiv, lhs, rhs)),
            (Op::TrueDiv, Tp::Float, Tp::Float) => Some(binary_op!(FloatE::TrueDiv, lhs, rhs)),
            (Op::Mod, Tp::Float, Tp::Float) => Some(binary_op!(FloatE::Mod, lhs, rhs)),
            (Op::Pow, Tp::Float, Tp::Float) => Some(binary_op!(FloatE::Pow, lhs, rhs)),
            (Op::Eq, Tp::String, Tp::String) => Some(binary_op!(BoolE::StringEq, lhs, rhs)),
            (Op::Ne, Tp::String, Tp::String) => Some(binary_op!(BoolE::StringNe, lhs, rhs)),
            (Op::Lt, Tp::String, Tp::String) => Some(binary_op!(BoolE::StringLt, lhs, rhs)),
            (Op::Le, Tp::String, Tp::String) => Some(binary_op!(BoolE::StringLe, lhs, rhs)),
            (Op::Gt, Tp::String, Tp::String) => Some(binary_op!(BoolE::StringGt, lhs, rhs)),
            (Op::Ge, Tp::String, Tp::String) => Some(binary_op!(BoolE::StringGe, lhs, rhs)),
            (Op::Add, Tp::String, Tp::String) => Some(binary_op!(StringE::Add, lhs, rhs)),
            (Op::Mul, Tp::String, Tp::Int) => Some(binary_op!(StringE::Mul, lhs, rhs)),
            (Op::Mul, Tp::Int, Tp::String) => Some(binary_op!(StringE::Mul, rhs, lhs)),
            (Op::Eq, Tp::Pointer, Tp::Pointer) => Some(binary_op!(BoolE::PtrEq, lhs, rhs)),
            (Op::Ne, Tp::Pointer, Tp::Pointer) => Some(binary_op!(BoolE::PtrNe, lhs, rhs)),
            (Op::Lt, Tp::Pointer, Tp::Pointer) => Some(binary_op!(BoolE::PtrLt, lhs, rhs)),
            (Op::Le, Tp::Pointer, Tp::Pointer) => Some(binary_op!(BoolE::PtrLe, lhs, rhs)),
            (Op::Gt, Tp::Pointer, Tp::Pointer) => Some(binary_op!(BoolE::PtrGt, lhs, rhs)),
            (Op::Ge, Tp::Pointer, Tp::Pointer) => Some(binary_op!(BoolE::PtrGe, lhs, rhs)),
            (Op::Eq, Tp::DateTimeNaive, Tp::DateTimeNaive) => {
                Some(binary_op!(BoolE::DateTimeNaiveEq, lhs, rhs))
            }
            (Op::Ne, Tp::DateTimeNaive, Tp::DateTimeNaive) => {
                Some(binary_op!(BoolE::DateTimeNaiveNe, lhs, rhs))
            }
            (Op::Lt, Tp::DateTimeNaive, Tp::DateTimeNaive) => {
                Some(binary_op!(BoolE::DateTimeNaiveLt, lhs, rhs))
            }
            (Op::Le, Tp::DateTimeNaive, Tp::DateTimeNaive) => {
                Some(binary_op!(BoolE::DateTimeNaiveLe, lhs, rhs))
            }
            (Op::Gt, Tp::DateTimeNaive, Tp::DateTimeNaive) => {
                Some(binary_op!(BoolE::DateTimeNaiveGt, lhs, rhs))
            }
            (Op::Ge, Tp::DateTimeNaive, Tp::DateTimeNaive) => {
                Some(binary_op!(BoolE::DateTimeNaiveGe, lhs, rhs))
            }
            (Op::Sub, Tp::DateTimeNaive, Tp::DateTimeNaive) => {
                Some(binary_op!(DurationExpression::DateTimeNaiveSub, lhs, rhs))
            }
            (Op::Add, Tp::DateTimeNaive, Tp::Duration) => {
                Some(binary_op!(DateTimeNaiveExpression::AddDuration, lhs, rhs))
            }
            (Op::Sub, Tp::DateTimeNaive, Tp::Duration) => {
                Some(binary_op!(DateTimeNaiveExpression::SubDuration, lhs, rhs))
            }
            (Op::Eq, Tp::DateTimeUtc, Tp::DateTimeUtc) => {
                Some(binary_op!(BoolE::DateTimeUtcEq, lhs, rhs))
            }
            (Op::Ne, Tp::DateTimeUtc, Tp::DateTimeUtc) => {
                Some(binary_op!(BoolE::DateTimeUtcNe, lhs, rhs))
            }
            (Op::Lt, Tp::DateTimeUtc, Tp::DateTimeUtc) => {
                Some(binary_op!(BoolE::DateTimeUtcLt, lhs, rhs))
            }
            (Op::Le, Tp::DateTimeUtc, Tp::DateTimeUtc) => {
                Some(binary_op!(BoolE::DateTimeUtcLe, lhs, rhs))
            }
            (Op::Gt, Tp::DateTimeUtc, Tp::DateTimeUtc) => {
                Some(binary_op!(BoolE::DateTimeUtcGt, lhs, rhs))
            }
            (Op::Ge, Tp::DateTimeUtc, Tp::DateTimeUtc) => {
                Some(binary_op!(BoolE::DateTimeUtcGe, lhs, rhs))
            }
            (Op::Sub, Tp::DateTimeUtc, Tp::DateTimeUtc) => {
                Some(binary_op!(DurationExpression::DateTimeUtcSub, lhs, rhs))
            }
            (Op::Add, Tp::DateTimeUtc, Tp::Duration) => {
                Some(binary_op!(DateTimeUtcExpression::AddDuration, lhs, rhs))
            }
            (Op::Sub, Tp::DateTimeUtc, Tp::Duration) => {
                Some(binary_op!(DateTimeUtcExpression::SubDuration, lhs, rhs))
            }
            (Op::Eq, Tp::Duration, Tp::Duration) => Some(binary_op!(BoolE::DurationEq, lhs, rhs)),
            (Op::Ne, Tp::Duration, Tp::Duration) => Some(binary_op!(BoolE::DurationNe, lhs, rhs)),
            (Op::Lt, Tp::Duration, Tp::Duration) => Some(binary_op!(BoolE::DurationLt, lhs, rhs)),
            (Op::Le, Tp::Duration, Tp::Duration) => Some(binary_op!(BoolE::DurationLe, lhs, rhs)),
            (Op::Gt, Tp::Duration, Tp::Duration) => Some(binary_op!(BoolE::DurationGt, lhs, rhs)),
            (Op::Ge, Tp::Duration, Tp::Duration) => Some(binary_op!(BoolE::DurationGe, lhs, rhs)),
            (Op::Add, Tp::Duration, Tp::Duration) => Some(binary_op!(DurationE::Add, lhs, rhs)),
            (Op::Sub, Tp::Duration, Tp::Duration) => Some(binary_op!(DurationE::Sub, lhs, rhs)),
            (Op::Add, Tp::Duration, Tp::DateTimeNaive) => {
                Some(binary_op!(DateTimeNaiveExpression::AddDuration, rhs, lhs))
            }
            (Op::Add, Tp::Duration, Tp::DateTimeUtc) => {
                Some(binary_op!(DateTimeUtcExpression::AddDuration, rhs, lhs))
            }
            (Op::Mul, Tp::Duration, Tp::Int) => Some(binary_op!(DurationE::MulByInt, lhs, rhs)),
            (Op::Mul, Tp::Int, Tp::Duration) => Some(binary_op!(DurationE::MulByInt, rhs, lhs)),
            (Op::FloorDiv, Tp::Duration, Tp::Int) => {
                Some(binary_op!(DurationE::DivByInt, lhs, rhs))
            }
            (Op::FloorDiv, Tp::Duration, Tp::Duration) => {
                Some(binary_op!(IntExpression::DurationFloorDiv, lhs, rhs))
            }
            (Op::TrueDiv, Tp::Duration, Tp::Duration) => {
                Some(binary_op!(FloatExpression::DurationTrueDiv, lhs, rhs))
            }
            (Op::Mod, Tp::Duration, Tp::Duration) => Some(binary_op!(DurationE::Mod, lhs, rhs)),
            (Op::MatMul, Tp::Array, Tp::Array) => Some(binary_op!(AnyE::MatMul, lhs, rhs)),
            (_, _, _) => None,
        }
    }

    #[staticmethod]
    fn cast(expr: &PyExpression, source_type: Type, target_type: Type) -> Option<Self> {
        type Tp = Type;
        match (source_type, target_type) {
            (Tp::Int, Tp::Float) => Some(unary_op!(FloatExpression::CastFromInt, expr)),
            (Tp::Int, Tp::Bool) => Some(unary_op!(BoolExpression::CastFromInt, expr)),
            (Tp::Int, Tp::String) => Some(unary_op!(StringExpression::CastFromInt, expr)),
            (Tp::Float, Tp::Int) => Some(unary_op!(IntExpression::CastFromFloat, expr)),
            (Tp::Float, Tp::Bool) => Some(unary_op!(BoolExpression::CastFromFloat, expr)),
            (Tp::Float, Tp::String) => Some(unary_op!(StringExpression::CastFromFloat, expr)),
            (Tp::Bool, Tp::Int) => Some(unary_op!(IntExpression::CastFromBool, expr)),
            (Tp::Bool, Tp::Float) => Some(unary_op!(FloatExpression::CastFromBool, expr)),
            (Tp::Bool, Tp::String) => Some(unary_op!(StringExpression::CastFromBool, expr)),
            (Tp::String, Tp::Int) => Some(unary_op!(IntExpression::CastFromString, expr)),
            (Tp::String, Tp::Float) => Some(unary_op!(FloatExpression::CastFromString, expr)),
            (Tp::String, Tp::Bool) => Some(unary_op!(BoolExpression::CastFromString, expr)),
            (_, _) => None,
        }
    }

    #[staticmethod]
    fn cast_optional(expr: &PyExpression, source_type: Type, target_type: Type) -> Option<Self> {
        type Tp = Type;
        match (target_type, source_type) {
            (Tp::Int, Tp::Float) => Some(unary_op!(
                AnyExpression::CastToOptionalIntFromOptionalFloat,
                expr
            )),
            (Tp::Float, Tp::Int) => Some(unary_op!(
                AnyExpression::CastToOptionalFloatFromOptionalInt,
                expr
            )),
            (_, _) => None,
        }
    }

    #[staticmethod]
    fn if_else(if_: &PyExpression, then: &PyExpression, else_: &PyExpression) -> Self {
        Self::new(
            Arc::new(Expression::Any(AnyExpression::IfElse(
                if_.inner.clone(),
                then.inner.clone(),
                else_.inner.clone(),
            ))),
            if_.gil || then.gil || else_.gil,
        )
    }

    #[staticmethod]
    #[pyo3(signature = (*args, optional = false))]
    fn pointer_from(args: Vec<PyRef<PyExpression>>, optional: bool) -> Self {
        let gil = args.iter().any(|a| a.gil);
        let args = args
            .into_iter()
            .map(|expr| expr.inner.clone())
            .collect_vec();
        let expr = if optional {
            Arc::new(Expression::Any(AnyExpression::OptionalPointerFrom(
                args.into(),
            )))
        } else {
            Arc::new(Expression::Pointer(PointerExpression::PointerFrom(
                args.into(),
            )))
        };
        Self::new(expr, gil)
    }

    #[staticmethod]
    #[pyo3(signature = (*args))]
    fn make_tuple(args: Vec<PyRef<PyExpression>>) -> Self {
        let gil = args.iter().any(|a| a.gil);
        let args = args
            .into_iter()
            .map(|expr| expr.inner.clone())
            .collect_vec();
        Self::new(
            Arc::new(Expression::Any(AnyExpression::MakeTuple(args.into()))),
            gil,
        )
    }

    #[staticmethod]
    fn sequence_get_item_checked(
        expr: &PyExpression,
        index: &PyExpression,
        default: &PyExpression,
    ) -> Self {
        Self::new(
            Arc::new(Expression::Any(AnyExpression::TupleGetItemChecked(
                expr.inner.clone(),
                index.inner.clone(),
                default.inner.clone(),
            ))),
            expr.gil || index.gil || default.gil,
        )
    }
}

unary_expr!(is_none, BoolExpression::IsNone);
binary_expr!(eq, BoolExpression::Eq);
binary_expr!(ne, BoolExpression::Ne);
binary_expr!(
    sequence_get_item_unchecked,
    AnyExpression::TupleGetItemUnchecked
);
unary_expr!(
    date_time_naive_nanosecond,
    IntExpression::DateTimeNaiveNanosecond
);
unary_expr!(
    date_time_naive_microsecond,
    IntExpression::DateTimeNaiveMicrosecond
);
unary_expr!(
    date_time_naive_millisecond,
    IntExpression::DateTimeNaiveMillisecond
);
unary_expr!(date_time_naive_second, IntExpression::DateTimeNaiveSecond);
unary_expr!(date_time_naive_minute, IntExpression::DateTimeNaiveMinute);
unary_expr!(date_time_naive_hour, IntExpression::DateTimeNaiveHour);
unary_expr!(date_time_naive_day, IntExpression::DateTimeNaiveDay);
unary_expr!(date_time_naive_month, IntExpression::DateTimeNaiveMonth);
unary_expr!(date_time_naive_year, IntExpression::DateTimeNaiveYear);
unary_expr!(
    date_time_naive_timestamp,
    IntExpression::DateTimeNaiveTimestamp
);
binary_expr!(date_time_naive_strptime, DateTimeNaiveExpression::Strptime);
binary_expr!(
    date_time_naive_strftime,
    StringExpression::DateTimeNaiveStrftime
);
binary_expr!(
    date_time_naive_from_timestamp,
    DateTimeNaiveExpression::FromTimestamp
);
binary_expr!(date_time_naive_to_utc, DateTimeUtcExpression::FromNaive);
binary_expr!(date_time_naive_round, DateTimeNaiveExpression::Round);
binary_expr!(date_time_naive_floor, DateTimeNaiveExpression::Floor);
unary_expr!(
    date_time_utc_nanosecond,
    IntExpression::DateTimeUtcNanosecond
);
unary_expr!(
    date_time_utc_microsecond,
    IntExpression::DateTimeUtcMicrosecond
);
unary_expr!(
    date_time_utc_millisecond,
    IntExpression::DateTimeUtcMillisecond
);
unary_expr!(date_time_utc_second, IntExpression::DateTimeUtcSecond);
unary_expr!(date_time_utc_minute, IntExpression::DateTimeUtcMinute);
unary_expr!(date_time_utc_hour, IntExpression::DateTimeUtcHour);
unary_expr!(date_time_utc_day, IntExpression::DateTimeUtcDay);
unary_expr!(date_time_utc_month, IntExpression::DateTimeUtcMonth);
unary_expr!(date_time_utc_year, IntExpression::DateTimeUtcYear);
unary_expr!(date_time_utc_timestamp, IntExpression::DateTimeUtcTimestamp);
binary_expr!(date_time_utc_strptime, DateTimeUtcExpression::Strptime);
binary_expr!(
    date_time_utc_strftime,
    StringExpression::DateTimeUtcStrftime
);
binary_expr!(date_time_utc_to_naive, DateTimeNaiveExpression::FromUtc);
binary_expr!(date_time_utc_round, DateTimeUtcExpression::Round);
binary_expr!(date_time_utc_floor, DateTimeUtcExpression::Floor);
unary_expr!(duration_nanoseconds, IntExpression::DurationNanoseconds);
unary_expr!(duration_microseconds, IntExpression::DurationMicroseconds);
unary_expr!(duration_milliseconds, IntExpression::DurationMilliseconds);
unary_expr!(duration_seconds, IntExpression::DurationSeconds);
unary_expr!(duration_minutes, IntExpression::DurationMinutes);
unary_expr!(duration_hours, IntExpression::DurationHours);
unary_expr!(duration_days, IntExpression::DurationDays);
unary_expr!(duration_weeks, IntExpression::DurationWeeks);
unary_expr!(unwrap, AnyExpression::Unwrap);
unary_expr!(to_string, StringExpression::ToString);
unary_expr!(parse_int, AnyExpression::ParseStringToInt, optional: bool);
unary_expr!(parse_float, AnyExpression::ParseStringToFloat, optional: bool);
unary_expr!(
    parse_bool,
    AnyExpression::ParseStringToBool,
    true_list: Vec<String>,
    false_list: Vec<String>,
    optional: bool
);

#[pyclass(module = "pathway.engine", frozen, name = "PathwayType")]
pub struct PathwayType(Type);

#[pymethods]
impl PathwayType {
    #[classattr]
    pub const ANY: Type = Type::Any;
    #[classattr]
    pub const BOOL: Type = Type::Bool;
    #[classattr]
    pub const INT: Type = Type::Int;
    #[classattr]
    pub const FLOAT: Type = Type::Float;
    #[classattr]
    pub const POINTER: Type = Type::Pointer;
    #[classattr]
    pub const STRING: Type = Type::String;
    #[classattr]
    pub const DATE_TIME_NAIVE: Type = Type::DateTimeNaive;
    #[classattr]
    pub const DATE_TIME_UTC: Type = Type::DateTimeUtc;
    #[classattr]
    pub const DURATION: Type = Type::Duration;
    #[classattr]
    pub const ARRAY: Type = Type::Array;
}

#[pyclass(module = "pathway.engine", frozen, name = "ConnectorMode")]
pub struct PyConnectorMode(ConnectorMode);

#[pymethods]
impl PyConnectorMode {
    #[classattr]
    pub const STATIC: ConnectorMode = ConnectorMode::Static;
    #[classattr]
    pub const SIMPLE_STREAMING: ConnectorMode = ConnectorMode::SimpleStreaming;
    #[classattr]
    pub const STREAMING_WITH_DELETIONS: ConnectorMode = ConnectorMode::StreamingWithDeletions;
}

#[pyclass(module = "pathway.engine", frozen, name = "MonitoringLevel")]
pub struct PyMonitoringLevel(MonitoringLevel);

#[pymethods]
impl PyMonitoringLevel {
    #[classattr]
    pub const NONE: MonitoringLevel = MonitoringLevel::None;

    #[classattr]
    pub const IN_OUT: MonitoringLevel = MonitoringLevel::InOut;

    #[classattr]
    pub const ALL: MonitoringLevel = MonitoringLevel::All;
}

#[pyclass(module = "pathway.engine", frozen)]
pub struct Universe {
    scope: Py<Scope>,
    handle: UniverseHandle,
    id_column: OnceCell<Py<Column>>,
}

impl Universe {
    fn new(scope: &PyCell<Scope>, handle: UniverseHandle) -> PyResult<Py<Self>> {
        let py = scope.py();
        if let Some(universe) = scope.borrow().universes.borrow().get(&handle) {
            return Ok(universe.clone());
        }
        let res = Py::new(
            py,
            Self {
                scope: scope.into(),
                handle,
                id_column: OnceCell::new(),
            },
        )?;
        scope
            .borrow()
            .universes
            .borrow_mut()
            .insert(handle, res.clone());
        Ok(res)
    }
}

#[pymethods]
impl Universe {
    #[getter]
    pub fn id_column(self_: &PyCell<Self>) -> PyResult<Py<Column>> {
        let this = self_.borrow();
        let id_column = this
            .id_column
            .get_or_try_init(|| {
                let py = self_.py();
                let handle = this.scope.borrow(py).graph.id_column(this.handle)?;
                Column::new(self_, handle)
            })?
            .clone();
        Ok(id_column)
    }

    pub fn __repr__(&self) -> String {
        format!("<Universe {:?}>", self.handle)
    }
}

#[pyclass(module = "pathway.engine", frozen, subclass)]
pub struct ComplexColumn;

impl ComplexColumn {
    fn output_universe(self_: &PyCell<Self>) -> Option<Py<Universe>> {
        if let Ok(_column) = self_.downcast::<PyCell<Column>>() {
            None
        } else if let Ok(computer) = self_.downcast::<PyCell<Computer>>() {
            let computer = computer.borrow();
            if computer.is_output {
                Some(computer.universe.clone())
            } else {
                None
            }
        } else {
            unreachable!("Unknown ComplexColumn subclass");
        }
    }

    fn to_engine(self_: &PyCell<Self>) -> EngineComplexColumn {
        if let Ok(column) = self_.downcast::<PyCell<Column>>() {
            EngineComplexColumn::Column(column.borrow().handle)
        } else if let Ok(computer) = self_.downcast::<PyCell<Computer>>() {
            Computer::to_engine(computer)
        } else {
            unreachable!("Unknown ComplexColumn subclass");
        }
    }
}

#[pyclass(module = "pathway.engine", frozen, extends = ComplexColumn)]
pub struct Column {
    #[pyo3(get)] // ?
    universe: Py<Universe>,
    handle: ColumnHandle,
}

impl Column {
    fn new(universe: &PyCell<Universe>, handle: ColumnHandle) -> PyResult<Py<Self>> {
        let py = universe.py();
        let universe_ref = universe.borrow();
        let scope = &universe_ref.scope.borrow(py);
        if let Some(column) = scope.columns.borrow().get(&handle) {
            assert!(column.borrow(py).universe.is(universe));
            return Ok(column.clone());
        }
        let res = Py::new(
            py,
            (
                Self {
                    universe: universe.into(),
                    handle,
                },
                ComplexColumn,
            ),
        )?;
        scope.columns.borrow_mut().insert(handle, res.clone());
        Ok(res)
    }
}

#[pymethods]
impl Column {
    pub fn __repr__(&self, py: Python) -> String {
        format!(
            "<Column universe={:?} {:?}>",
            self.universe.borrow(py).handle,
            self.handle
        )
    }
}

#[pyclass(module = "pathway.engine", frozen)]
pub struct Table {
    #[pyo3(get)] // ?
    universe: Py<Universe>,

    #[pyo3(get)] // ?
    columns: Vec<Py<Column>>,
}

#[pymethods]
impl Table {
    #[new]
    pub fn new(
        universe: &PyCell<Universe>,
        #[pyo3(from_py_with = "from_py_iterable")] columns: Vec<Py<Column>>,
    ) -> PyResult<Self> {
        let py = universe.py();
        for column in &columns {
            check_identity(&column.borrow(py).universe, universe, "universe mismatch")?;
        }
        Ok(Self {
            universe: universe.into(),
            columns,
        })
    }

    pub fn __repr__(&self, py: Python) -> String {
        format!(
            "<Table universe={:?} columns=[{}]>",
            self.universe.borrow(py).handle,
            self.columns.iter().format_with(", ", |column, f| {
                f(&format_args!("{:?}", column.borrow(py).handle))
            })
        )
    }
}

impl Table {
    fn to_engine(&self, py: Python) -> (UniverseHandle, Vec<ColumnHandle>) {
        let universe = self.universe.borrow(py);
        let column_handles = self.columns.iter().map(|c| c.borrow(py).handle).collect();
        (universe.handle, column_handles)
    }

    fn from_handles(
        scope: &PyCell<Scope>,
        universe_handle: UniverseHandle,
        column_handles: impl IntoIterator<Item = ColumnHandle>,
    ) -> PyResult<Self> {
        let py = scope.py();
        let universe = Universe::new(scope, universe_handle)?;
        let universe = universe.as_ref(py);
        let columns = column_handles
            .into_iter()
            .map(|column_handle| Column::new(universe, column_handle))
            .collect::<PyResult<_>>()?;
        Self::new(universe, columns)
    }

    fn from_engine(scope: &PyCell<Scope>, table: EngineTable) -> Self {
        let (universe_handle, column_handles) = table;
        Self::from_handles(scope, universe_handle, column_handles).unwrap()
    }
}

static MISSING_VALUE_ERROR_TYPE: Lazy<Py<PyType>> = Lazy::new(|| {
    Python::with_gil(|py| {
        PyErr::new_type(
            py,
            "pathway.engine.MissingValueError",
            None,
            Some(PyBaseException::type_object(py)),
            None,
        )
        .expect("creating MissingValueError type should not fail")
    })
});

static ENGINE_ERROR_TYPE: Lazy<Py<PyType>> = Lazy::new(|| {
    Python::with_gil(|py| {
        PyErr::new_type(
            py,
            "pathway.engine.EngineError",
            None,
            Some(PyException::type_object(py)),
            None,
        )
        .expect("creating EngineError type should not fail")
    })
});

static ENGINE_ERROR_WITH_TRACE_TYPE: Lazy<Py<PyType>> = Lazy::new(|| {
    Python::with_gil(|py| {
        PyErr::new_type(
            py,
            "pathway.engine.EngineErrorWithTrace",
            None,
            Some(PyException::type_object(py)),
            None,
        )
        .expect("creating EngineErrorWithTrace type should not fail")
    })
});

#[pyclass(module = "pathway.engine", frozen)]
pub struct Context(SendWrapper<ScopedContext>);

#[allow(clippy::redundant_closure_for_method_calls)] // false positives
#[pymethods]
impl Context {
    #[getter]
    fn this_row(&self) -> PyResult<Key> {
        self.0
            .with(|context| context.this_row())
            .ok_or_else(|| PyValueError::new_err("context out of scope"))
    }

    #[getter]
    fn data(&self) -> PyResult<Value> {
        self.0
            .with(|context| context.data())
            .ok_or_else(|| PyValueError::new_err("context out of scope"))
    }

    #[pyo3(signature=(column, row, *args))]
    fn raising_get(
        &self,
        py: Python,
        column: usize,
        row: Key,
        args: Vec<Value>,
    ) -> PyResult<Value> {
        self.0
            .with(|context| {
                context
                    .get(column, row, args)
                    .ok_or_else(|| PyErr::from_type(MISSING_VALUE_ERROR_TYPE.as_ref(py), ()))
            })
            .unwrap_or_else(|| Err(PyValueError::new_err("context out of scope")))
    }
}

#[pyclass(module = "pathway.engine", frozen, extends = ComplexColumn)]
pub struct Computer {
    fun: Py<PyAny>,
    #[allow(unused)] // XXX
    dtype: Py<PyAny>,
    is_output: bool,
    is_method: bool,
    universe: Py<Universe>,
    data: Value,
    data_column: Option<Py<Column>>,
}

#[pymethods]
impl Computer {
    #[allow(clippy::too_many_arguments)]
    #[staticmethod]
    #[pyo3(signature = (
        fun,
        dtype,
        is_output,
        is_method,
        universe,
        data = Value::None,
        data_column = None,
    ))]
    pub fn from_raising_fun(
        py: Python,
        fun: Py<PyAny>,
        #[allow(unused)] dtype: Py<PyAny>,
        is_output: bool,
        is_method: bool,
        universe: Py<Universe>,
        data: Value,
        data_column: Option<Py<Column>>,
    ) -> PyResult<Py<Self>> {
        Py::new(
            py,
            (
                Self {
                    fun,
                    dtype,
                    is_output,
                    is_method,
                    universe,
                    data,
                    data_column,
                },
                ComplexColumn,
            ),
        )
    }
}

impl Computer {
    fn compute(
        &self,
        py: Python,
        engine_context: &dyn EngineContext,
        args: &[Value],
    ) -> PyResult<Option<Value>> {
        let context = PyCell::new(py, Context(SendWrapper::new(ScopedContext::default())))?;
        let mut all_args = Vec::with_capacity(args.len() + 1);
        all_args.push(context.to_object(py));
        all_args.extend(args.iter().map(|value| value.to_object(py)));
        let res = context.borrow().0.scoped(
            engine_context,
            || self.fun.as_ref(py).call1(PyTuple::new(py, all_args)), // FIXME
        );
        // let res = context.0. self.fun.as_ref(py).call1((context,));
        match res {
            Ok(value) => Ok(Some(value.extract()?)),
            Err(error) => {
                if error.is_instance(py, MISSING_VALUE_ERROR_TYPE.as_ref(py)) {
                    Ok(None)
                } else {
                    Err(error)
                }
            }
        }
    }

    fn to_engine(self_: &PyCell<Self>) -> EngineComplexColumn {
        let py = self_.py();
        let self_ref = self_.borrow();
        let computer: Py<Self> = self_.into();
        let engine_computer = if self_ref.is_method {
            let data_column_handle = self_ref
                .data_column
                .as_ref()
                .map(|data_column| data_column.borrow(py).handle);
            EngineComputer::Method {
                logic: Box::new(move |engine_context, args| {
                    let engine_context = SendWrapper::new(engine_context);
                    Ok(with_gil_and_pool(|py| {
                        let engine_context = engine_context.take();
                        computer.borrow(py).compute(py, engine_context, args)
                    })?)
                }),
                universe_handle: self_ref.universe.borrow(py).handle,
                data: self_ref.data.clone(),
                data_column_handle,
            }
        } else {
            // XXX: check these asserts in constructor
            assert_eq!(self_ref.data, Value::None);
            assert!(self_ref.data_column.is_none());
            EngineComputer::Attribute {
                logic: Box::new(move |engine_context| {
                    let engine_context = SendWrapper::new(engine_context);
                    Ok(with_gil_and_pool(|py| {
                        let engine_context = engine_context.take();
                        computer.borrow(py).compute(py, engine_context, &[])
                    })?)
                }),
                universe_handle: self_ref.universe.borrow(py).handle,
            }
        };
        if self_ref.is_output {
            EngineComplexColumn::ExternalComputer(engine_computer)
        } else {
            EngineComplexColumn::InternalComputer(engine_computer)
        }
    }
}

#[pyclass(module = "pathway.engine", frozen)]
pub struct VennUniverses {
    scope: Py<Scope>,
    venn_universes_handle: VennUniverseHandle,
}

#[pymethods]
impl VennUniverses {
    fn only_left(self_: PyRef<Self>, py: Python) -> PyResult<Py<Universe>> {
        Universe::new(
            self_.scope.as_ref(py),
            self_
                .scope
                .borrow(py)
                .graph
                .venn_universes_only_left(self_.venn_universes_handle)?,
        )
    }

    fn only_right(self_: PyRef<Self>, py: Python) -> PyResult<Py<Universe>> {
        Universe::new(
            self_.scope.as_ref(py),
            self_
                .scope
                .borrow(py)
                .graph
                .venn_universes_only_right(self_.venn_universes_handle)?,
        )
    }

    fn both(self_: PyRef<Self>, py: Python) -> PyResult<Py<Universe>> {
        Universe::new(
            self_.scope.as_ref(py),
            self_
                .scope
                .borrow(py)
                .graph
                .venn_universes_both(self_.venn_universes_handle)?,
        )
    }
}

#[pyclass(module = "pathway.engine", frozen)]
pub struct Scope {
    #[pyo3(get)]
    parent: Option<Py<Self>>,

    graph: SendWrapper<ScopedGraph>,

    // empty_universe: Lazy<Py<Universe>>,
    universes: RefCell<HashMap<UniverseHandle, Py<Universe>>>,
    columns: RefCell<HashMap<ColumnHandle, Py<Column>>>,
    persistent_ids: RefCell<HashSet<ExternalPersistentId>>,
    event_loop: PyObject,
}

impl Scope {
    fn new(parent: Option<Py<Self>>, event_loop: PyObject) -> Self {
        Scope {
            parent,

            graph: SendWrapper::new(ScopedGraph::new()),
            universes: RefCell::new(HashMap::new()),
            columns: RefCell::new(HashMap::new()),
            persistent_ids: RefCell::new(HashSet::new()),
            event_loop,
        }
    }

    fn clear_caches(&self) {
        self.universes.borrow_mut().clear();
        self.columns.borrow_mut().clear();
    }
}

#[pymethods]
impl Scope {
    pub fn empty_table(
        self_: &PyCell<Self>,
        #[pyo3(from_py_with = "from_py_iterable")] dtypes: Vec<&PyAny>,
    ) -> PyResult<Table> {
        let py = self_.py();
        let universe_handle = self_.borrow().graph.empty_universe()?;
        let universe = Universe::new(self_, universe_handle)?;
        let columns = dtypes
            .into_iter()
            .map(|_dt| {
                let handle = self_.borrow().graph.empty_column(universe_handle)?;
                Column::new(universe.as_ref(py), handle)
            })
            .collect::<PyResult<_>>()?;
        Table::new(universe.as_ref(py), columns)
    }

    pub fn static_universe(
        self_: &PyCell<Self>,
        #[pyo3(from_py_with = "from_py_iterable")] keys: Vec<Key>,
    ) -> PyResult<Py<Universe>> {
        let handle = self_.borrow().graph.static_universe(keys)?;
        Universe::new(self_, handle)
    }

    pub fn static_column(
        self_: &PyCell<Self>,
        universe: &PyCell<Universe>,
        #[pyo3(from_py_with = "from_py_iterable")] values: Vec<(Key, Value)>,
        #[allow(unused)] dtype: &PyAny,
    ) -> PyResult<Py<Column>> {
        check_identity(self_, &universe.borrow().scope, "scope mismatch")?;
        let handle = self_
            .borrow()
            .graph
            .static_column(universe.borrow().handle, values)?;
        Column::new(universe, handle)
    }

    pub fn connector_table(
        self_: &PyCell<Self>,
        data_source: &PyCell<DataStorage>,
        data_format: &PyCell<DataFormat>,
        properties: ConnectorProperties,
    ) -> PyResult<Table> {
        let py = self_.py();

        let persistent_id = data_source.borrow().persistent_id.clone();
        if let Some(persistent_id) = &persistent_id {
            let is_unique_id = self_
                .borrow()
                .persistent_ids
                .borrow_mut()
                .insert(persistent_id.to_string());
            if !is_unique_id {
                return Err(PyValueError::new_err(format!(
                    "Persistent ID '{persistent_id}' used more than once"
                )));
            }
        }

        let (reader_impl, parallel_readers) = data_source.borrow().construct_reader(py)?;

        let parser_impl = data_format.borrow().construct_parser(py)?;
        let (universe_handle, column_handles) = self_.borrow().graph.connector_table(
            reader_impl,
            parser_impl,
            properties
                .commit_duration_ms
                .map(time::Duration::from_millis),
            parallel_readers,
            &persistent_id,
        )?;

        let universe = Universe::new(self_, universe_handle)?;

        let mut columns = Vec::new();
        for column_handle in column_handles {
            let column = Column::new(universe.as_ref(py), column_handle)?;
            columns.push(column);
        }

        Ok(Table { universe, columns })
    }

    #[allow(clippy::type_complexity)]
    #[pyo3(signature = (iterated, iterated_with_universe, extra, logic, *, limit = None))]
    pub fn iterate(
        self_: &PyCell<Self>,
        #[pyo3(from_py_with = "engine_tables_from_py_iterable")] iterated: Vec<EngineTable>,
        #[pyo3(from_py_with = "engine_tables_from_py_iterable")] iterated_with_universe: Vec<
            EngineTable,
        >,
        #[pyo3(from_py_with = "engine_tables_from_py_iterable")] extra: Vec<EngineTable>,
        logic: &PyAny,
        limit: Option<u32>,
    ) -> PyResult<(Vec<Py<Table>>, Vec<Py<Table>>)> {
        let py = self_.py();
        let (result, result_with_universe) = self_.borrow().graph.iterate(
            iterated,
            iterated_with_universe,
            extra,
            limit,
            Box::new(|graph, iterated, iterated_with_universe, extra| {
                let scope = PyCell::new(
                    py,
                    Scope::new(Some(self_.into()), self_.borrow().event_loop.clone()),
                )?;
                scope.borrow().graph.scoped(graph, || {
                    let iterated = iterated
                        .into_iter()
                        .map(|table| Table::from_engine(scope, table))
                        .collect::<Vec<_>>();
                    let iterated_with_universe = iterated_with_universe
                        .into_iter()
                        .map(|table| Table::from_engine(scope, table))
                        .collect::<Vec<_>>();
                    let extra = extra
                        .into_iter()
                        .map(|table| Table::from_engine(scope, table))
                        .collect::<Vec<_>>();
                    let (result, result_with_universe): (&PyAny, &PyAny) = logic
                        .call1((scope, iterated, iterated_with_universe, extra))?
                        .extract()?;
                    let result = result
                        .iter()?
                        .map(|table| {
                            let table: PyRef<Table> = table?.extract()?;
                            Ok(table.to_engine(py))
                        })
                        .collect::<PyResult<_>>()?;
                    let result_with_universe = result_with_universe
                        .iter()?
                        .map(|table| {
                            let table: PyRef<Table> = table?.extract()?;
                            Ok(table.to_engine(py))
                        })
                        .collect::<PyResult<_>>()?;
                    Ok((result, result_with_universe))
                })
            }),
        )?;
        let result = result
            .into_iter()
            .map(|table| Py::new(py, Table::from_engine(self_, table)))
            .collect::<PyResult<_>>()?;
        let result_with_universe = result_with_universe
            .into_iter()
            .map(|table| Py::new(py, Table::from_engine(self_, table)))
            .collect::<PyResult<_>>()?;
        Ok((result, result_with_universe))
    }

    pub fn map_column(
        self_: &PyCell<Self>,
        table: &Table,
        function: Py<PyAny>,
        properties: EvalProperties,
    ) -> PyResult<Py<Column>> {
        let py = self_.py();
        let universe = table.universe.as_ref(py);
        let universe_ref = universe.borrow();
        check_identity(self_, &universe_ref.scope, "scope mismatch")?;
        let column_handles = table.columns.iter().map(|c| c.borrow(py).handle).collect();
        let handle = self_.borrow().graph.expression_column(
            BatchWrapper::WithGil,
            Arc::new(Expression::Any(AnyExpression::Apply(
                Box::new(move |input| {
                    with_gil_and_pool(|py| -> DynResult<_> {
                        let inputs = PyTuple::new(py, input);
                        let args = PyTuple::new(py, [inputs]);
                        Ok(function.call1(py, args)?.extract::<Value>(py)?)
                    })
                }),
                Expressions::AllArguments,
            ))),
            universe_ref.handle,
            column_handles,
            properties.trace(py)?,
        )?;
        Column::new(universe, handle)
    }

    pub fn async_map_column(
        self_: &PyCell<Self>,
        table: &Table,
        function: Py<PyAny>,
        properties: EvalProperties,
    ) -> PyResult<Py<Column>> {
        let py = self_.py();
        let universe = table.universe.as_ref(py);
        let universe_ref = universe.borrow();
        check_identity(self_, &universe_ref.scope, "scope mismatch")?;
        let column_handles = table.columns.iter().map(|c| c.borrow(py).handle).collect();
        let event_loop = self_.borrow().event_loop.clone();
        let handle = self_.borrow().graph.async_apply_column(
            Arc::new(move |_, values| {
                let future = with_gil_and_pool(|py| {
                    let event_loop = event_loop.clone();
                    let args = PyTuple::new(py, values);
                    let awaitable = function.call1(py, args)?;
                    let awaitable = awaitable.as_ref(py);
                    let locals =
                        pyo3_asyncio::TaskLocals::new(event_loop.as_ref(py)).copy_context(py)?;
                    pyo3_asyncio::into_future_with_locals(&locals, awaitable)
                });

                Box::pin(async {
                    let result = future?.await?;
                    with_gil_and_pool(|py| result.extract::<Value>(py).map_err(DynError::from))
                })
            }),
            universe_ref.handle,
            column_handles,
            properties.trace(py)?,
        )?;
        Column::new(universe, handle)
    }

    pub fn unsafe_map_column_numba(
        self_: &PyCell<Self>,
        table: &Table,
        function: &PyAny,
        properties: EvalProperties,
    ) -> PyResult<Py<Column>> {
        unsafe { numba::unsafe_map_column(self_, table, function, properties) }
    }

    pub fn expression_column(
        self_: &PyCell<Self>,
        table: &Table,
        expression: &PyExpression,
        properties: EvalProperties,
    ) -> PyResult<Py<Column>> {
        let py = self_.py();
        let universe = table.universe.as_ref(py);
        let universe_ref = universe.borrow();
        check_identity(self_, &universe_ref.scope, "scope mismatch")?;
        let column_handles = table.columns.iter().map(|c| c.borrow(py).handle).collect();
        let wrapper = if expression.gil {
            BatchWrapper::WithGil
        } else {
            BatchWrapper::None
        };
        let handle = self_.borrow().graph.expression_column(
            wrapper,
            expression.inner.clone(),
            universe_ref.handle,
            column_handles,
            properties.trace(py)?,
        )?;
        Column::new(universe, handle)
    }

    #[allow(clippy::unused_self)]
    pub fn filter_universe(
        self_: &PyCell<Self>,
        universe: PyRef<Universe>,
        column: &Column,
    ) -> PyResult<Py<Universe>> {
        check_identity(self_, &universe.scope, "scope mismatch")?;
        check_identity(&column.universe, &universe, "universe mismatch")?;
        let new_universe_handle = self_
            .borrow()
            .graph
            .filter_universe(universe.handle, column.handle)?;
        Universe::new(self_, new_universe_handle)
    }

    #[pyo3(signature = (*universes))]
    pub fn intersect_universe<'py>(
        self_: &'py PyCell<Self>,
        universes: &'py PyTuple,
    ) -> PyResult<Py<Universe>> {
        let universe_handles = universes
            .into_iter()
            .map(|universe| {
                let universe: &PyCell<Universe> = universe.downcast()?;
                let universe = universe.borrow();
                check_identity(self_, &universe.scope, "scope mismatch")?;
                Ok(universe.handle)
            })
            .collect::<PyResult<_>>()?;
        let universe_handle = self_.borrow().graph.intersect_universe(universe_handles)?;
        Universe::new(self_, universe_handle)
    }

    #[pyo3(signature = (*universes))]
    pub fn union_universe<'py>(
        self_: &'py PyCell<Self>,
        universes: &'py PyTuple,
    ) -> PyResult<Py<Universe>> {
        let universe_handles = universes
            .into_iter()
            .map(|universe| {
                let universe: &PyCell<Universe> = universe.downcast()?;
                let universe = universe.borrow();
                check_identity(self_, &universe.scope, "scope mismatch")?;
                Ok(universe.handle)
            })
            .collect::<PyResult<_>>()?;
        let universe_handle = self_.borrow().graph.union_universe(universe_handles)?;
        Universe::new(self_, universe_handle)
    }

    pub fn venn_universes(
        self_: &PyCell<Self>,
        left_universe: &PyCell<Universe>,
        right_universe: &PyCell<Universe>,
    ) -> PyResult<VennUniverses> {
        let left_universe_handle = left_universe.borrow().handle;
        let right_universe_handle = right_universe.borrow().handle;
        let handles = self_
            .borrow()
            .graph
            .venn_universes(left_universe_handle, right_universe_handle)?;
        Ok(VennUniverses {
            scope: self_.into(),
            venn_universes_handle: handles,
        })
    }

    pub fn concat(
        self_: &PyCell<Self>,
        #[pyo3(from_py_with = "from_py_iterable")] universes: Vec<PyRef<Universe>>,
    ) -> PyResult<Concat> {
        let universe_handles = universes
            .into_iter()
            .map(|universe| universe.handle)
            .collect();
        let concat_handle = self_.borrow().graph.concat(universe_handles)?;
        let universe_handle = self_.borrow().graph.concat_universe(concat_handle)?;
        let universe = Universe::new(self_, universe_handle)?;
        Ok(Concat {
            scope: self_.into(),
            handle: concat_handle,
            universe,
        })
    }

    pub fn flatten(self_: &PyCell<Self>, flatten_column: PyRef<Column>) -> PyResult<Flatten> {
        let (new_universe_handle, flattened_column_handle, flatten_handle) =
            self_.borrow().graph.flatten(flatten_column.handle)?;
        let new_universe = Universe::new(self_, new_universe_handle)?;
        let flattened_column =
            Column::new(new_universe.as_ref(self_.py()), flattened_column_handle)?;
        Ok(Flatten {
            scope: self_.into(),
            universe: new_universe,
            flattened_column,
            handle: flatten_handle,
        })
    }

    pub fn sort(
        self_: &PyCell<Self>,
        key: PyRef<Column>,
        instance: PyRef<Column>,
    ) -> PyResult<(Py<Column>, Py<Column>)> {
        let (prev_column_handle, next_column_handle) =
            self_.borrow().graph.sort(key.handle, instance.handle)?;
        let py = self_.py();
        let universe = key.universe.as_ref(py);
        let prev_column = Column::new(universe, prev_column_handle)?;
        let next_column = Column::new(universe, next_column_handle)?;

        Ok((prev_column, next_column))
    }

    pub fn reindex_universe<'py>(
        self_: &'py PyCell<Self>,
        py: Python<'py>,
        column: &'py PyCell<Column>,
    ) -> PyResult<Py<Universe>> {
        check_identity(
            self_,
            &column.borrow().universe.borrow(py).scope,
            "scope mismatch",
        )?;

        let universe_handle = self_
            .borrow()
            .graph
            .reindex_universe(column.borrow().handle)?;
        Universe::new(self_, universe_handle)
    }

    pub fn reindex_column<'py>(
        self_: &'py PyCell<Self>,
        py: Python<'py>,
        column_to_reindex: &'py PyCell<Column>,
        reindexing_column: &'py PyCell<Column>,
        reindexing_universe: &'py PyCell<Universe>,
    ) -> PyResult<Py<Column>> {
        check_identity(
            self_,
            &column_to_reindex.borrow().universe.borrow(py).scope,
            "scope mismatch",
        )?;

        check_identity(
            self_,
            &reindexing_column.borrow().universe.borrow(py).scope,
            "scope mismatch",
        )?;

        check_identity(self_, &reindexing_universe.borrow().scope, "scope mismatch")?;

        let new_column_handle = self_.borrow().graph.reindex_column(
            column_to_reindex.borrow().handle,
            reindexing_column.borrow().handle,
            reindexing_universe.borrow().handle,
        )?;

        Column::new(reindexing_universe, new_column_handle)
    }

    pub fn restrict_column<'py>(
        self_: &'py PyCell<Self>,
        universe: &'py PyCell<Universe>,
        column: &'py PyCell<Column>,
    ) -> PyResult<Py<Column>> {
        let universe_ref = universe.borrow();
        check_identity(self_, &universe_ref.scope, "scope mismatch")?;
        let column_ref = column.borrow();
        let column_universe = column_ref.universe.as_ref(self_.py());
        check_identity(self_, &column_universe.borrow().scope, "scope mismatch")?;
        let new_column_handle = self_
            .borrow()
            .graph
            .restrict_column(universe_ref.handle, column_ref.handle)?;
        Column::new(universe, new_column_handle)
    }

    pub fn override_column_universe<'py>(
        self_: &'py PyCell<Self>,
        universe: &'py PyCell<Universe>,
        column: &'py PyCell<Column>,
    ) -> PyResult<Py<Column>> {
        let universe_ref = universe.borrow();
        check_identity(self_, &universe_ref.scope, "scope mismatch")?;
        let column_ref = column.borrow();
        let column_universe = column_ref.universe.as_ref(self_.py());
        check_identity(self_, &column_universe.borrow().scope, "scope mismatch")?;
        let handle = self_
            .borrow()
            .graph
            .override_column_universe(universe_ref.handle, column_ref.handle)?;
        Column::new(universe, handle)
    }

    pub fn table<'py>(
        self_: &'py PyCell<Self>,
        universe: &'py PyCell<Universe>,
        columns: &'py PyAny,
    ) -> PyResult<Table> {
        check_identity(self_, &universe.borrow().scope, "scope mismatch")?;
        let columns = columns
            .iter()?
            .map(|column| {
                let column: &PyCell<Column> = column?.extract()?;
                Self::restrict_column(self_, universe, column)
            })
            .collect::<PyResult<_>>()?;
        Table::new(universe, columns)
    }

    #[pyo3(signature = (table, requested_columns, set_id = false))]
    pub fn group_by(
        self_: &PyCell<Self>,
        table: PyRef<Table>,
        #[pyo3(from_py_with = "from_py_iterable")] requested_columns: Vec<PyRef<Column>>,
        set_id: bool,
    ) -> PyResult<Grouper> {
        let py = self_.py();
        let self_ref = self_.borrow();
        let universe = table.universe.borrow(py);
        check_identity(self_, &universe.scope, "scope mismatch")?;
        let column_handles: Vec<_> = table.columns.iter().map(|c| c.borrow(py).handle).collect();
        let requested_column_handles: Vec<_> =
            requested_columns.into_iter().map(|c| c.handle).collect();

        if requested_column_handles
            .iter()
            .any(|handle| !column_handles.contains(handle))
        {
            return Err(PyValueError::new_err(
                "requested columns must be a subset of input table columns",
            ));
        }

        let grouper_handle = if set_id {
            if column_handles.len() != 1 {
                return Err(PyValueError::new_err("expected exactly one column"));
            }
            self_ref.graph.group_by_id(
                universe.handle,
                column_handles[0],
                requested_column_handles,
            )?
        } else {
            self_ref
                .graph
                .group_by(universe.handle, column_handles, requested_column_handles)?
        };
        Ok(Grouper {
            scope: self_.into(),
            handle: grouper_handle,
        })
    }

    pub fn ix(
        self_: &PyCell<Self>,
        keys_column: PyRef<Column>,
        input_universe: PyRef<Universe>,
        strict: bool,
        optional: bool,
    ) -> PyResult<Ixer> {
        let self_ref = self_.borrow();
        check_identity(self_, &input_universe.scope, "scope mismatch")?;
        let keys_column_handle = keys_column.handle;
        let ix_key_policy = IxKeyPolicy::from_strict_optional(strict, optional)?;
        let ixer_handle =
            self_ref
                .graph
                .ix(keys_column_handle, input_universe.handle, ix_key_policy)?;
        Ok(Ixer {
            scope: self_.into(),
            handle: ixer_handle,
        })
    }

    #[pyo3(signature = (left_table, right_table, assign_id = false, left_ear = false, right_ear = false))]
    pub fn join(
        self_: &PyCell<Self>,
        py: Python,
        left_table: PyRef<Table>,
        right_table: PyRef<Table>,
        assign_id: bool,
        left_ear: bool,
        right_ear: bool,
    ) -> PyResult<Joiner> {
        let self_ref = self_.borrow();
        let left_universe = left_table.universe.borrow(py);
        check_identity(self_, &left_universe.scope, "scope mismatch")?;
        let right_universe = right_table.universe.borrow(py);
        check_identity(self_, &right_universe.scope, "scope mismatch")?;
        let left_column_handles = left_table
            .columns
            .iter()
            .map(|c| c.borrow(py).handle)
            .collect();
        let right_column_handles = right_table
            .columns
            .iter()
            .map(|c| c.borrow(py).handle)
            .collect();
        let join_type = JoinType::from_assign_left_right(assign_id, left_ear, right_ear)?;
        let joiner_handle = self_ref.graph.join(
            left_universe.handle,
            left_column_handles,
            right_universe.handle,
            right_column_handles,
            join_type,
        )?;
        let universe_handle = self_ref.graph.joiner_universe(joiner_handle)?;
        let universe = Universe::new(self_, universe_handle)?;
        Ok(Joiner {
            scope: self_.into(),
            handle: joiner_handle,
            universe,
        })
    }

    fn complex_columns(
        self_: &PyCell<Self>,
        #[pyo3(from_py_with = "from_py_iterable")] inputs: Vec<&PyCell<ComplexColumn>>,
    ) -> PyResult<Vec<Py<Column>>> {
        let py = self_.py();
        let mut engine_complex_columns = Vec::new();
        let mut output_universes = Vec::new();
        for input in inputs {
            engine_complex_columns.push(ComplexColumn::to_engine(input));
            output_universes.extend(ComplexColumn::output_universe(input));
        }
        let columns = self_
            .borrow()
            .graph
            .complex_columns(engine_complex_columns)?
            .into_iter()
            .zip_eq(output_universes)
            .map(|(column_handle, universe)| Column::new(universe.as_ref(py), column_handle))
            .collect::<PyResult<_>>()?;
        Ok(columns)
    }

    pub fn debug_universe<'py>(
        self_: &'py PyCell<Self>,
        name: String,
        universe: &'py PyCell<Universe>,
    ) -> PyResult<()> {
        check_identity(self_, &universe.borrow().scope, "scope mismatch")?;
        Ok(self_
            .borrow()
            .graph
            .debug_universe(name, universe.borrow().handle)?)
    }

    pub fn debug_column<'py>(
        self_: &'py PyCell<Self>,
        name: String,
        column: &'py PyCell<Column>,
    ) -> PyResult<()> {
        let py = self_.py();
        check_identity(
            self_,
            &column.borrow().universe.borrow(py).scope,
            "scope mismatch",
        )?;
        Ok(self_
            .borrow()
            .graph
            .debug_column(name, column.borrow().handle)?)
    }

    pub fn update_rows<'py>(
        self_: &'py PyCell<Self>,
        universe: &'py PyCell<Universe>,
        column: &'py PyCell<Column>,
        updates: &'py PyCell<Column>,
    ) -> PyResult<Py<Column>> {
        let py = self_.py();

        check_identity(self_, &universe.borrow().scope, "scope mismatch")?;
        check_identity(
            self_,
            &column.borrow().universe.borrow(py).scope,
            "scope mismatch",
        )?;
        check_identity(
            self_,
            &updates.borrow().universe.borrow(py).scope,
            "scope mismatch",
        )?;

        let handle = self_.borrow().graph.update_rows(
            universe.borrow().handle,
            column.borrow().handle,
            updates.borrow().handle,
        )?;

        Column::new(universe, handle)
    }

    pub fn output_table<'py>(
        self_: &'py PyCell<Self>,
        table: &'py PyCell<Table>,
        data_sink: &PyCell<DataStorage>,
        data_format: &PyCell<DataFormat>,
    ) -> PyResult<()> {
        let py = self_.py();

        let sink_impl = data_sink.borrow().construct_writer(py)?;
        let format_impl = data_format.borrow().construct_formatter(py)?;

        let mut column_handles = Vec::new();
        for column in &table.borrow().columns {
            column_handles.push(column.borrow(py).handle);
        }

        let universe_handle = table.borrow().universe.borrow(py).handle;
        self_.borrow().graph.output_table(
            sink_impl,
            format_impl,
            universe_handle,
            column_handles,
        )?;

        Ok(())
    }

    pub fn subscribe_table(
        self_: &PyCell<Self>,
        table: &Table,
        on_change: Py<PyAny>,
        on_end: Py<PyAny>,
    ) -> PyResult<()> {
        let py = self_.py();
        let universe = table.universe.as_ref(py);
        let universe_ref = universe.borrow();
        check_identity(self_, &universe_ref.scope, "scope mismatch")?;

        let column_handles = table.columns.iter().map(|c| c.borrow(py).handle).collect();
        self_.borrow().graph.subscribe_column(
            BatchWrapper::WithGil,
            Box::new(move |key, values, time, diff| {
                with_gil_and_pool(|py| {
                    let py_key = key.to_object(py);
                    let py_values = PyTuple::new(py, values).to_object(py);
                    let py_time = time.to_object(py);
                    let py_diff = diff.to_object(py);

                    let args = PyTuple::new(py, [py_key, py_values, py_time, py_diff]);
                    on_change.call(py, args, None)?;
                    Ok(())
                })
            }),
            Box::new(move || {
                with_gil_and_pool(|py| {
                    on_end.call0(py)?;
                    Ok(())
                })
            }),
            universe_ref.handle,
            column_handles,
        )?;
        Ok(())
    }

    pub fn probe_universe(
        self_: &PyCell<Self>,
        universe: &PyCell<Universe>,
        operator_id: usize,
    ) -> PyResult<()> {
        self_
            .borrow()
            .graph
            .probe_universe(universe.borrow().handle, operator_id)?;
        Ok(())
    }

    pub fn probe_column(
        self_: &PyCell<Self>,
        column: &PyCell<Column>,
        operator_id: usize,
    ) -> PyResult<()> {
        self_
            .borrow()
            .graph
            .probe_column(column.borrow().handle, operator_id)?;
        Ok(())
    }
}

#[pyclass(module = "pathway.engine", frozen)]
pub struct Ixer {
    scope: Py<Scope>,
    handle: IxerHandle,
}

#[pymethods]
impl Ixer {
    fn ix_column(self_: PyRef<Self>, column: PyRef<Column>, py: Python) -> PyResult<Py<Column>> {
        check_identity(
            &self_.scope,
            &column.universe.borrow(py).scope,
            "scope mismatch",
        )?;
        let column_handle = self_
            .scope
            .borrow(py)
            .graph
            .ix_column(self_.handle, column.handle)?;
        Column::new(Ixer::universe(self_, py)?.as_ref(py), column_handle)
    }

    #[getter]
    fn universe(self_: PyRef<Self>, py: Python) -> PyResult<Py<Universe>> {
        let universe_handle = self_.scope.borrow(py).graph.ixer_universe(self_.handle)?;
        Universe::new(self_.scope.as_ref(py), universe_handle)
    }
}
#[pyclass(module = "pathway.engine", frozen)]
pub struct Grouper {
    scope: Py<Scope>,
    handle: GrouperHandle,
}

#[pymethods]
impl Grouper {
    fn input_column(self_: PyRef<Self>, column: PyRef<Column>, py: Python) -> PyResult<Py<Column>> {
        check_identity(
            &self_.scope,
            &column.universe.borrow(py).scope,
            "scope mismatch",
        )?;
        let column_handle = self_
            .scope
            .borrow(py)
            .graph
            .grouper_input_column(self_.handle, column.handle)?;
        Column::new(Grouper::universe(self_, py)?.as_ref(py), column_handle)
    }

    fn count_column(self_: PyRef<Self>, py: Python) -> PyResult<Py<Column>> {
        let column_handle = self_
            .scope
            .borrow(py)
            .graph
            .grouper_count_column(self_.handle)?;
        Column::new(Grouper::universe(self_, py)?.as_ref(py), column_handle)
    }

    fn reducer_column(
        self_: PyRef<Self>,
        reducer: Reducer,
        column: PyRef<Column>,
        py: Python,
    ) -> PyResult<Py<Column>> {
        check_identity(
            &self_.scope,
            &column.universe.borrow(py).scope,
            "scope mismatch",
        )?;
        let column_handle = self_.scope.borrow(py).graph.grouper_reducer_column(
            self_.handle,
            reducer,
            column.handle,
        )?;
        Column::new(Grouper::universe(self_, py)?.as_ref(py), column_handle)
    }

    fn reducer_ix_column(
        self_: PyRef<Self>,
        reducer: Reducer,
        ixer: PyRef<Ixer>,
        column: PyRef<Column>,
        py: Python,
    ) -> PyResult<Py<Column>> {
        check_identity(
            &self_.scope,
            &column.universe.borrow(py).scope,
            "scope mismatch",
        )?;
        let column_handle = self_.scope.borrow(py).graph.grouper_reducer_column_ix(
            self_.handle,
            reducer,
            ixer.handle,
            column.handle,
        )?;
        Column::new(Grouper::universe(self_, py)?.as_ref(py), column_handle)
    }

    #[getter]
    fn universe(self_: PyRef<Self>, py: Python) -> PyResult<Py<Universe>> {
        let universe_handle = self_
            .scope
            .borrow(py)
            .graph
            .grouper_universe(self_.handle)?;
        Universe::new(self_.scope.as_ref(py), universe_handle)
    }
}

#[pyclass(module = "pathway.engine", frozen)]
pub struct Joiner {
    scope: Py<Scope>,
    handle: JoinerHandle,
    #[pyo3(get)]
    universe: Py<Universe>,
}

#[pymethods]
impl Joiner {
    fn select_left_column(self_: PyRef<Self>, column: PyRef<Column>) -> PyResult<Py<Column>> {
        let py = self_.py();
        check_identity(
            &self_.scope,
            &column.universe.borrow(py).scope,
            "scope mismatch",
        )?;
        let column_handle = self_
            .scope
            .borrow(py)
            .graph
            .joiner_left_column(self_.handle, column.handle)?;
        Column::new(self_.universe.as_ref(py), column_handle)
    }

    fn select_right_column(self_: PyRef<Self>, column: PyRef<Column>) -> PyResult<Py<Column>> {
        let py = self_.py();
        check_identity(
            &self_.scope,
            &column.universe.borrow(py).scope,
            "scope mismatch",
        )?;
        let column_handle = self_
            .scope
            .borrow(py)
            .graph
            .joiner_right_column(self_.handle, column.handle)?;
        Column::new(self_.universe.as_ref(py), column_handle)
    }
}

type CapturedData<T> = Arc<Mutex<Vec<T>>>;
type CapturedUniverseData = CapturedData<(Key, isize)>;
type CapturedColumnData = CapturedData<(Key, Value, isize)>;
type CapturedTableData = (CapturedUniverseData, Vec<CapturedColumnData>);

fn capture_table_data(
    py: Python,
    graph: &dyn Graph,
    table: PyRef<Table>,
) -> PyResult<CapturedTableData> {
    let universe_data = Arc::new(Mutex::new(Vec::new()));
    {
        let universe_data = universe_data.clone();
        graph.on_universe_data(
            table.universe.borrow(py).handle,
            Box::new(move |key, diff| {
                universe_data.lock().unwrap().push((*key, diff));
                Ok(())
            }),
        )?;
    }
    let mut all_column_data = Vec::new();
    for column in &table.columns {
        let column = column.borrow(py);
        let column_data = Arc::new(Mutex::new(Vec::new()));
        {
            let column_data = column_data.clone();
            graph.on_column_data(
                column.handle,
                Box::new(move |key, data, diff| {
                    column_data.lock().unwrap().push((*key, data.clone(), diff));
                    Ok(())
                }),
            )?;
        }
        all_column_data.push(column_data);
    }
    Ok((universe_data, all_column_data))
}

pub fn make_captured_table(table_data: Vec<CapturedTableData>) -> PyResult<Py<PyDict>> {
    let mut combined_universe_data = Vec::new();
    let mut combined_all_column_data = Vec::new();
    for (universe_data, all_column_data) in table_data {
        combined_universe_data.extend(take(&mut *universe_data.lock().unwrap()));
        combined_all_column_data.resize_with(all_column_data.len(), Vec::new);
        for (combined_column_data, column_data) in
            zip(&mut combined_all_column_data, all_column_data)
        {
            combined_column_data.extend(take(&mut *column_data.lock().unwrap()));
        }
    }
    consolidate(&mut combined_universe_data);
    for column_data in &mut combined_all_column_data {
        consolidate_updates(column_data);
        assert_eq!(column_data.len(), combined_universe_data.len());
    }
    with_gil_and_pool(|py| {
        let dict = PyDict::new(py);
        for (i, (key, diff)) in combined_universe_data.into_iter().enumerate() {
            assert_eq!(diff, 1);
            let tuple = PyTuple::new(
                py,
                combined_all_column_data.iter().map(|column_data| {
                    let (data_key, ref data, diff) = column_data[i];
                    assert_eq!(data_key, key);
                    assert_eq!(diff, 1);
                    data.clone()
                }),
            );
            dict.set_item(key, tuple)?;
        }
        Ok(dict.into())
    })
}

#[pyfunction]
#[allow(clippy::too_many_arguments)]
#[pyo3(signature = (
    logic,
    event_loop,
    stats_monitor = None,
    ignore_asserts = false,
    monitoring_level = MonitoringLevel::None,
    with_http_server = false,
    persistence_config = None
))]
pub fn run_with_new_graph(
    py: Python,
    logic: PyObject,
    event_loop: PyObject,
    stats_monitor: Option<PyObject>,
    ignore_asserts: bool,
    monitoring_level: MonitoringLevel,
    with_http_server: bool,
    persistence_config: Option<PersistenceConfig>,
) -> PyResult<Vec<Py<PyDict>>> {
    defer! {
        log::logger().flush();
    }
    let config =
        config_from_env().map_err(|msg| PyErr::from_type(ENGINE_ERROR_TYPE.as_ref(py), msg))?;
    let persistence_config = {
        if let Some(persistence_config) = persistence_config {
            Some(persistence_config.prepare(py)?)
        } else {
            None
        }
    };
    let results: Vec<Vec<_>> = run_with_wakeup_receiver(py, |wakeup_receiver| {
        py.allow_threads(|| {
            run_with_new_dataflow_graph(
                move |graph| {
                    let thread_state = PythonThreadState::new();

                    let captured_tables = Python::with_gil(|py| {
                        let our_scope = PyCell::new(py, Scope::new(None, event_loop.clone()))?;
                        let tables: Vec<PyRef<Table>> =
                            our_scope.borrow().graph.scoped(graph, || {
                                let args = PyTuple::new(py, [our_scope]);
                                from_py_iterable(logic.as_ref(py).call1(args)?)
                            })?;
                        our_scope.borrow().clear_caches();
                        tables
                            .into_iter()
                            .map(|table| capture_table_data(py, graph, table))
                            .try_collect()
                    })?;
                    Ok((thread_state, captured_tables))
                },
                |(_thread_state, captured_tables)| captured_tables,
                config,
                wakeup_receiver,
                stats_monitor,
                ignore_asserts,
                monitoring_level,
                with_http_server,
                persistence_config,
            )
        })
    })??;
    let mut captured_tables = Vec::new();
    for result in results {
        captured_tables.resize_with(result.len(), Vec::new);
        for (i, table) in result.into_iter().enumerate() {
            captured_tables[i].push(table);
        }
    }
    captured_tables
        .into_iter()
        .map(make_captured_table)
        .collect()
}

#[pyclass(module = "pathway.engine", frozen)]
pub struct Concat {
    scope: Py<Scope>,
    handle: ConcatHandle,
    #[pyo3(get)]
    universe: Py<Universe>,
}

#[pymethods]
impl Concat {
    fn concat_column(
        self_: PyRef<Self>,
        #[pyo3(from_py_with = "from_py_iterable")] columns: Vec<PyRef<Column>>,
    ) -> PyResult<Py<Column>> {
        let py = self_.py();
        for column in &columns {
            check_identity(
                &self_.scope,
                &column.universe.borrow(py).scope,
                "scope mismatch",
            )?;
        }
        let column_handles = columns.into_iter().map(|column| column.handle).collect();
        let column_handle = self_
            .scope
            .borrow(py)
            .graph
            .concat_column(self_.handle, column_handles)?;
        Column::new(self_.universe.as_ref(py), column_handle)
    }
}

#[pyclass(module = "pathway.engine", frozen)]
pub struct Flatten {
    scope: Py<Scope>,
    #[pyo3(get)]
    universe: Py<Universe>,
    flattened_column: Py<Column>,
    handle: FlattenHandle,
}

#[pymethods]
impl Flatten {
    fn get_flattened_column(self_: PyRef<Self>) -> PyResult<Py<Column>> {
        let py = self_.py();
        let column = self_.flattened_column.borrow(py);
        Column::new(column.universe.as_ref(py), column.handle)
    }

    fn explode_column(self_: PyRef<Self>, column: PyRef<Column>) -> PyResult<Py<Column>> {
        let py = self_.py();
        check_identity(
            &self_.scope,
            &column.universe.borrow(py).scope,
            "scope mismatch",
        )?;
        let column_handle = self_
            .scope
            .borrow(py)
            .graph
            .explode(self_.handle, column.handle)?;
        Column::new(self_.universe.as_ref(py), column_handle)
    }
}

#[pyfunction]
#[pyo3(signature = (*values, optional = false))]
pub fn ref_scalar(values: &PyTuple, optional: bool) -> PyResult<Option<Key>> {
    if optional && values.iter().any(PyAny::is_none) {
        return Ok(None);
    }
    let key = Key::for_values(&from_py_iterable(values)?);
    Ok(Some(key))
}

#[pyfunction]
pub fn unsafe_make_pointer(value: KeyImpl) -> Key {
    Key(value)
}

#[pyclass(module = "pathway.engine", frozen)]
pub struct AwsS3Settings {
    bucket_name: String,
    region: s3::region::Region,
    access_key: Option<String>,
    secret_access_key: Option<String>,
    with_path_style: bool,
}

#[pymethods]
impl AwsS3Settings {
    #[new]
    #[pyo3(signature = (
        bucket_name,
        access_key = None,
        secret_access_key = None,
        with_path_style = false,
        region = None,
        endpoint = None,
    ))]
    fn new(
        bucket_name: String,
        access_key: Option<String>,
        secret_access_key: Option<String>,
        with_path_style: bool,
        region: Option<String>,
        endpoint: Option<String>,
    ) -> PyResult<Self> {
        Ok(AwsS3Settings {
            bucket_name,
            region: Self::aws_region(region, endpoint)?,
            access_key,
            secret_access_key,
            with_path_style,
        })
    }
}

impl AwsS3Settings {
    fn aws_region(
        region: Option<String>,
        endpoint: Option<String>,
    ) -> PyResult<s3::region::Region> {
        if let Some(endpoint) = endpoint {
            Ok(s3::region::Region::Custom {
                region: region.unwrap_or(endpoint.clone()),
                endpoint,
            })
        } else if let Some(region) = region {
            region
                .parse()
                .map_err(|_| PyValueError::new_err("Incorrect AWS region"))
        } else {
            Err(PyValueError::new_err(
                "At least one of { region, endpoint } must be defined",
            ))
        }
    }
}

impl AwsS3Settings {
    fn construct_private_bucket(&self) -> PyResult<S3Bucket> {
        let credentials = s3::creds::Credentials::new(
            Some(&self.access_key.clone().ok_or(PyRuntimeError::new_err(
                "access key must be specified for a private bucket",
            ))?),
            Some(
                &self
                    .secret_access_key
                    .clone()
                    .ok_or(PyRuntimeError::new_err(
                        "secret access key must be specified for a private bucket",
                    ))?,
            ),
            None,
            None,
            None,
        )
        .map_err(|err| {
            PyRuntimeError::new_err(format!("Unable to form credentials to AWS storage: {err}"))
        })?;

        S3Bucket::new(&self.bucket_name, self.region.clone(), credentials).map_err(|err| {
            PyRuntimeError::new_err(format!("Failed to connect to private AWS bucket: {err}"))
        })
    }

    fn construct_public_bucket(&self) -> PyResult<S3Bucket> {
        S3Bucket::new_public(&self.bucket_name, self.region.clone()).map_err(|err| {
            PyRuntimeError::new_err(format!("Failed to connect to public AWS bucket: {err}"))
        })
    }

    fn construct_bucket(&self) -> PyResult<S3Bucket> {
        let has_access_key = self.access_key.is_some();
        let has_secret_access_key = self.secret_access_key.is_some();
        if has_access_key != has_secret_access_key {
            warn!("Only one of access_key and secret_access_key is specified. Trying to connect to a public bucket.");
        }

        let mut bucket = {
            if has_access_key && has_secret_access_key {
                self.construct_private_bucket()?
            } else {
                self.construct_public_bucket()?
            }
        };

        if self.with_path_style {
            bucket = bucket.with_path_style();
        }

        Ok(bucket)
    }
}

#[pyclass(module = "pathway.engine", frozen)]
pub struct ElasticSearchAuth {
    auth_type: String,
    username: Option<String>,
    password: Option<String>,
    bearer: Option<String>,
    apikey_id: Option<String>,
    apikey: Option<String>,
}

#[pymethods]
impl ElasticSearchAuth {
    #[new]
    #[pyo3(signature = (
        auth_type,
        username = None,
        password = None,
        bearer = None,
        apikey_id = None,
        apikey = None,
    ))]
    fn new(
        auth_type: String,
        username: Option<String>,
        password: Option<String>,
        bearer: Option<String>,
        apikey_id: Option<String>,
        apikey: Option<String>,
    ) -> Self {
        ElasticSearchAuth {
            auth_type,
            username,
            password,
            bearer,
            apikey_id,
            apikey,
        }
    }
}

impl ElasticSearchAuth {
    fn as_client_auth(&self) -> PyResult<ESCredentials> {
        match self.auth_type.as_ref() {
            "basic" => {
                let username = self.username.as_ref().ok_or_else(|| {
                    PyValueError::new_err("For basic auth username should be specified")
                })?;
                let password = self.password.as_ref().ok_or_else(|| {
                    PyValueError::new_err("For basic auth password should be specified")
                })?;
                Ok(ESCredentials::Basic(
                    username.to_string(),
                    password.to_string(),
                ))
            }
            "bearer" => {
                let bearer = self.bearer.as_ref().ok_or_else(|| {
                    PyValueError::new_err("For bearer auth bearer should be specified")
                })?;
                Ok(ESCredentials::Bearer(bearer.to_string()))
            }
            "apikey" => {
                let apikey_id = self.apikey_id.as_ref().ok_or_else(|| {
                    PyValueError::new_err("For API Key auth apikey_id should be specified")
                })?;
                let apikey = self.apikey.as_ref().ok_or_else(|| {
                    PyValueError::new_err("For API Key auth apikey should be specified")
                })?;
                Ok(ESCredentials::ApiKey(
                    apikey_id.to_string(),
                    apikey.to_string(),
                ))
            }
            _ => Err(PyValueError::new_err("Unsupported type of auth")),
        }
    }
}

#[pyclass(module = "pathway.engine", frozen)]
pub struct ElasticSearchParams {
    host: String,
    index_name: String,
    auth: Py<ElasticSearchAuth>,
    max_batch_size: Option<usize>,
}

#[pymethods]
impl ElasticSearchParams {
    #[new]
    fn new(
        host: String,
        index_name: String,
        auth: Py<ElasticSearchAuth>,
        max_batch_size: Option<usize>,
    ) -> Self {
        ElasticSearchParams {
            host,
            index_name,
            auth,
            max_batch_size,
        }
    }
}

impl ElasticSearchParams {
    fn client(&self, py: pyo3::Python) -> PyResult<Elasticsearch> {
        let creds = self.auth.borrow(py).as_client_auth()?;

        let url = Url::parse(&self.host)
            .map_err(|e| PyValueError::new_err(format!("Failed to parse node URL: {e:?}")))?;
        let conn_pool = SingleNodeConnectionPool::new(url);

        let transport = TransportBuilder::new(conn_pool)
            .auth(creds)
            .disable_proxy()
            .build()
            .map_err(|e| {
                PyValueError::new_err(format!(
                    "Failed to build ES transfer with the given params: {e:?}"
                ))
            })?;

        Ok(Elasticsearch::new(transport))
    }
}

#[derive(Clone, Debug)]
#[pyclass(module = "pathway.engine", frozen)]
pub struct DataStorage {
    storage_type: String,
    path: Option<String>,
    rdkafka_settings: Option<HashMap<String, String>>,
    topic: Option<String>,
    connection_string: Option<String>,
    csv_parser_settings: Option<Py<CsvParserSettings>>,
    mode: ConnectorMode,
    aws_s3_settings: Option<Py<AwsS3Settings>>,
    elasticsearch_params: Option<Py<ElasticSearchParams>>,
    parallel_readers: Option<usize>,
    python_subject: Option<Py<PythonSubject>>,
    persistent_id: Option<ExternalPersistentId>,
}

#[derive(Clone, Debug)]
#[pyclass(module = "pathway.engine", frozen)]
pub struct PersistenceConfig {
    metadata_storage: DataStorage,
    stream_storage: DataStorage,
}

#[pymethods]
impl PersistenceConfig {
    #[new]
    #[pyo3(signature = (
        *,
        metadata_storage,
        stream_storage,
    ))]
    fn new(metadata_storage: DataStorage, stream_storage: DataStorage) -> Self {
        Self {
            metadata_storage,
            stream_storage,
        }
    }
}

impl PersistenceConfig {
    fn prepare(self, py: pyo3::Python) -> PyResult<PersistenceManagerOuterConfig> {
        Ok(PersistenceManagerOuterConfig::new(
            self.metadata_storage
                .construct_metadata_storage_config(py)?,
            self.stream_storage.construct_stream_storage_config(py)?,
        ))
    }
}

#[pyclass(module = "pathway.engine", frozen)]
#[derive(Clone)]
pub struct PythonSubject {
    pub start: Py<PyAny>,
    pub read: Py<PyAny>,
    pub is_internal: bool,
}

#[pymethods]
impl PythonSubject {
    #[new]
    #[pyo3(signature = (start, read, is_internal))]
    fn new(start: Py<PyAny>, read: Py<PyAny>, is_internal: bool) -> Self {
        Self {
            start,
            read,
            is_internal,
        }
    }
}

#[pyclass(module = "pathway.engine")]
#[derive(Clone)]
pub struct ValueField {
    #[pyo3(get)]
    name: String,
    #[pyo3(get)]
    type_: Type,
    #[pyo3(get)]
    default: Option<Value>,
}

impl ValueField {
    fn as_inner_schema_field(&self) -> InnerSchemaField {
        InnerSchemaField::new(self.type_, self.default.clone())
    }
}

#[pymethods]
impl ValueField {
    #[new]
    fn new(name: String, type_: Type) -> Self {
        ValueField {
            name,
            type_,
            default: None,
        }
    }

    fn set_default(&mut self, value: Value) {
        self.default = Some(value);
    }
}

#[pyclass(module = "pathway.engine", frozen)]
pub struct DataFormat {
    format_type: String,
    key_field_names: Option<Vec<String>>,
    #[pyo3(get)]
    value_fields: Vec<Py<ValueField>>,
    delimiter: Option<char>,
    table_name: Option<String>,
    column_paths: Option<HashMap<String, String>>,
    field_absence_is_error: bool,
}

#[pymethods]
impl DataStorage {
    #[new]
    #[pyo3(signature = (
        storage_type,
        path = None,
        rdkafka_settings = None,
        topic = None,
        connection_string = None,
        csv_parser_settings = None,
        mode = ConnectorMode::SimpleStreaming,
        aws_s3_settings = None,
        elasticsearch_params = None,
        parallel_readers = None,
        python_subject = None,
        persistent_id = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        storage_type: String,
        path: Option<String>,
        rdkafka_settings: Option<HashMap<String, String>>,
        topic: Option<String>,
        connection_string: Option<String>,
        csv_parser_settings: Option<Py<CsvParserSettings>>,
        mode: ConnectorMode,
        aws_s3_settings: Option<Py<AwsS3Settings>>,
        elasticsearch_params: Option<Py<ElasticSearchParams>>,
        parallel_readers: Option<usize>,
        python_subject: Option<Py<PythonSubject>>,
        persistent_id: Option<ExternalPersistentId>,
    ) -> Self {
        DataStorage {
            storage_type,
            path,
            rdkafka_settings,
            topic,
            connection_string,
            csv_parser_settings,
            mode,
            aws_s3_settings,
            elasticsearch_params,
            parallel_readers,
            python_subject,
            persistent_id,
        }
    }
}

#[pymethods]
#[allow(clippy::needless_pass_by_value)]
impl DataFormat {
    #[new]
    #[pyo3(signature = (
        format_type,
        key_field_names,
        value_fields,
        delimiter = None,
        table_name = None,
        column_paths = None,
        field_absence_is_error = true,
    ))]
    fn new(
        format_type: String,
        key_field_names: Option<Vec<String>>,
        value_fields: Vec<Py<ValueField>>,
        delimiter: Option<char>,
        table_name: Option<String>,
        column_paths: Option<HashMap<String, String>>,
        field_absence_is_error: bool,
    ) -> Self {
        DataFormat {
            format_type,
            key_field_names,
            value_fields,
            delimiter,
            table_name,
            column_paths,
            field_absence_is_error,
        }
    }
}

#[derive(Clone)]
#[pyclass(module = "pathway.engine", frozen)]
pub struct CsvParserSettings {
    pub delimiter: u8,
    pub quote: u8,
    pub escape: Option<u8>,
    pub enable_double_quote_escapes: bool,
    pub enable_quoting: bool,
    pub comment_character: Option<u8>,
}

#[pymethods]
impl CsvParserSettings {
    #[new]
    #[pyo3(signature = (
        delimiter = ',',
        quote = '"', // "
        escape = None,
        enable_double_quote_escapes = true,
        enable_quoting = true,
        comment_character = None,
    ))]
    pub fn new(
        delimiter: char,
        quote: char,
        escape: Option<char>,
        enable_double_quote_escapes: bool,
        enable_quoting: bool,
        comment_character: Option<char>,
    ) -> PyResult<CsvParserSettings> {
        let mut comment_character_ascii: Option<u8> = None;
        if let Some(comment_character) = comment_character {
            comment_character_ascii = Some(u8::try_from(comment_character).map_err(|_| {
                PyValueError::new_err(
                    "Comment character, if specified, should be an ASCII character",
                )
            })?);
        }

        Ok(CsvParserSettings {
            delimiter: u8::try_from(delimiter).map_err(|_| {
                PyValueError::new_err("Delimiter, if specified, should be an ASCII character")
            })?,
            quote: u8::try_from(quote).map_err(|_| {
                PyValueError::new_err("Quote, if specified, should be an ASCII character")
            })?,
            escape: escape.map(|escape| escape as u8),
            enable_double_quote_escapes,
            enable_quoting,
            comment_character: comment_character_ascii,
        })
    }
}

impl CsvParserSettings {
    fn build_csv_reader_builder(&self) -> CsvReaderBuilder {
        let mut builder = CsvReaderBuilder::new();
        builder
            .delimiter(self.delimiter)
            .quote(self.quote)
            .escape(self.escape)
            .double_quote(self.enable_double_quote_escapes)
            .quoting(self.enable_quoting)
            .comment(self.comment_character)
            .has_headers(false);
        builder
    }
}

impl DataStorage {
    fn path(&self) -> PyResult<&str> {
        let path = self
            .path
            .as_ref()
            .ok_or_else(|| PyValueError::new_err("For fs storage, path must be specified"))?
            .as_str();
        Ok(path)
    }

    fn connection_string(&self) -> PyResult<&str> {
        let connection_string = self
            .connection_string
            .as_ref()
            .ok_or_else(|| {
                PyValueError::new_err("For postgres storage, connection string must be specified")
            })?
            .as_str();
        Ok(connection_string)
    }

    fn s3_bucket(&self, py: pyo3::Python) -> PyResult<S3Bucket> {
        let bucket = self
            .aws_s3_settings
            .as_ref()
            .ok_or_else(|| {
                PyValueError::new_err("For AWS storage, aws_s3_settings must be specified")
            })?
            .borrow(py)
            .construct_bucket()?;
        Ok(bucket)
    }

    fn kafka_client_config(&self) -> PyResult<ClientConfig> {
        let rdkafka_settings = self.rdkafka_settings.as_ref().ok_or_else(|| {
            PyValueError::new_err("For kafka input, rdkafka_settings must be specified")
        })?;

        let mut client_config = ClientConfig::new();
        client_config.set("ssl.ca.location", "probe");
        for (key, value) in rdkafka_settings {
            client_config.set(key, value);
        }

        Ok(client_config)
    }

    fn kafka_topic(&self) -> PyResult<&str> {
        let topic = self
            .topic
            .as_ref()
            .ok_or_else(|| PyValueError::new_err("For kafka input, topic must be specified"))?;

        Ok(topic)
    }

    fn build_csv_parser_settings(&self, py: pyo3::Python) -> CsvReaderBuilder {
        match &self.csv_parser_settings {
            Some(parser_settings) => parser_settings.borrow(py).build_csv_reader_builder(),
            None => {
                let mut builder = CsvReaderBuilder::new();
                builder.has_headers(false);
                builder
            }
        }
    }

    fn internal_persistent_id(&self) -> Option<PersistentId> {
        self.persistent_id.clone().map(|external_persistent_id| {
            let mut hasher = Hasher::default();
            hasher.update(external_persistent_id.as_bytes());
            hasher.digest128()
        })
    }

    fn construct_reader(&self, py: pyo3::Python) -> PyResult<(Box<dyn ReaderBuilder>, usize)> {
        match self.storage_type.as_ref() {
            "fs" => {
                let storage =
                    FilesystemReader::new(self.path()?, self.mode, self.internal_persistent_id())?;
                Ok((Box::new(storage), 1))
            }
            "s3" => {
                let storage = S3LinesReader::new(
                    self.s3_bucket(py)?,
                    self.path()?,
                    self.mode.is_polling_enabled(),
                    self.internal_persistent_id(),
                );
                Ok((Box::new(storage), 1))
            }
            "s3_csv" => {
                let storage = S3CsvReader::new(
                    self.s3_bucket(py)?,
                    self.path()?,
                    self.build_csv_parser_settings(py),
                    self.mode.is_polling_enabled(),
                    self.internal_persistent_id(),
                );
                Ok((Box::new(storage), 1))
            }
            "csv" => {
                let reader = CsvFilesystemReader::new(
                    self.path()?,
                    self.build_csv_parser_settings(py),
                    self.mode,
                    self.internal_persistent_id(),
                )?;
                Ok((Box::new(reader), 1))
            }
            "kafka" => {
                let client_config = self.kafka_client_config()?;

                let consumer: BaseConsumer = client_config.create().map_err(|e| {
                    PyValueError::new_err(format!("Creating Kafka consumer failed: {e}"))
                })?;

                let topic = self.kafka_topic()?;
                consumer.subscribe(&[topic]).map_err(|e| {
                    PyIOError::new_err(format!("Subscription to Kafka topic failed: {e}"))
                })?;

                let reader =
                    KafkaReader::new(consumer, topic.to_string(), self.internal_persistent_id());
                Ok((Box::new(reader), self.parallel_readers.unwrap_or(256)))
            }
            "python" => {
                let subject = self.python_subject.clone().ok_or_else(|| {
                    PyValueError::new_err(
                        "For Python connector, python_subject should be specified",
                    )
                })?;

                if subject.borrow(py).is_internal && self.persistent_id.is_some() {
                    return Err(PyValueError::new_err(
                        "Python connectors marked internal can't have persistent id",
                    ));
                }

                let reader = PythonReaderBuilder::new(subject, self.internal_persistent_id());
                Ok((Box::new(reader), 1))
            }
            other => Err(PyValueError::new_err(format!(
                "Unknown data source {other:?}"
            ))),
        }
    }

    fn construct_stream_storage_config(&self, py: pyo3::Python) -> PyResult<StreamStorageConfig> {
        match self.storage_type.as_ref() {
            "fs" => Ok(StreamStorageConfig::Filesystem(self.path()?.into())),
            "s3" => {
                let bucket = self.s3_bucket(py)?;
                let path = self.path()?;
                Ok(StreamStorageConfig::S3 {
                    bucket,
                    root_path: path.into(),
                })
            }
            other => Err(PyValueError::new_err(format!(
                "Unsupported snapshot storage format: {other:?}"
            ))),
        }
    }

    fn construct_metadata_storage_config(
        &self,
        _py: pyo3::Python,
    ) -> PyResult<MetadataStorageConfig> {
        // _py is not removed, because we will need it anyway with adding more storages

        match self.storage_type.as_ref() {
            "fs" => Ok(MetadataStorageConfig::Filesystem(self.path()?.into())),
            other => Err(PyValueError::new_err(format!(
                "Unsupported metadata storage format: {other:?}"
            ))),
        }
    }

    fn elasticsearch_client_params<'py>(
        &'py self,
        py: pyo3::Python<'py>,
    ) -> PyResult<PyRef<ElasticSearchParams>> {
        Ok(self
            .elasticsearch_params
            .as_ref()
            .ok_or_else(|| {
                PyValueError::new_err(
                    "For elastic search output, elasticsearch_params section must be specified",
                )
            })?
            .borrow(py))
    }

    fn construct_writer(&self, py: pyo3::Python) -> PyResult<Box<dyn Writer>> {
        match self.storage_type.as_ref() {
            "fs" => {
                let path = self.path()?;
                let storage = {
                    let file = File::create(path);
                    match file {
                        Ok(f) => {
                            let buf_writer = BufWriter::new(f);
                            FileWriter::new(buf_writer)
                        }
                        Err(_) => {
                            return Err(PyIOError::new_err("Filesystem operation (create) failed"))
                        }
                    }
                };
                Ok(Box::new(storage))
            }
            "kafka" => {
                let client_config = self.kafka_client_config()?;

                let producer: ThreadedProducer<DefaultProducerContext> =
                    match client_config.create() {
                        Ok(producer) => producer,
                        Err(_) => return Err(PyIOError::new_err("Producer creation failed")),
                    };

                let topic = self.kafka_topic()?;
                let writer = KafkaWriter::new(producer, topic.to_string());

                Ok(Box::new(writer))
            }
            "postgres" => {
                let connection_string = self.connection_string()?;
                let storage = match Client::connect(connection_string, NoTls) {
                    Ok(client) => PsqlWriter::new(client),
                    Err(e) => {
                        return Err(PyIOError::new_err(format!(
                            "Failed to establish PostgreSQL connection: {e:?}"
                        )))
                    }
                };
                Ok(Box::new(storage))
            }
            "elasticsearch" => {
                let elasticsearch_client_params = self.elasticsearch_client_params(py)?;
                let client = elasticsearch_client_params.client(py)?;
                let index_name = elasticsearch_client_params.index_name.clone();
                let max_batch_size = elasticsearch_client_params.max_batch_size;

                let writer = ElasticSearchWriter::new(client, index_name, max_batch_size);
                Ok(Box::new(writer))
            }
            "null" => Ok(Box::new(NullWriter::new())),
            other => Err(PyValueError::new_err(format!(
                "Unknown data sink {other:?}"
            ))),
        }
    }
}

impl DataFormat {
    fn value_field_names(&self, py: pyo3::Python) -> Vec<String> {
        // TODO: schema support is to be added here
        let mut value_field_names = Vec::new();
        for field in &self.value_fields {
            value_field_names.push(field.borrow(py).name.clone());
        }
        value_field_names
    }

    fn construct_dsv_settings(&self, py: pyo3::Python) -> PyResult<DsvSettings> {
        let Some(delimiter) = &self.delimiter else {
            return Err(PyValueError::new_err(
                "For dsv format, delimiter must be specified",
            ));
        };

        Ok(DsvSettings::new(
            self.key_field_names.clone(),
            self.value_field_names(py),
            *delimiter,
        ))
    }

    fn table_name(&self) -> PyResult<String> {
        match &self.table_name {
            Some(table_name) => Ok(table_name.to_string()),
            None => Err(PyValueError::new_err(
                "For postgres format, table name should be specified",
            )),
        }
    }

    fn schema(&self, py: pyo3::Python) -> HashMap<String, InnerSchemaField> {
        let mut types = HashMap::new();
        for field in &self.value_fields {
            let borrowed_field = field.borrow(py);
            types.insert(
                borrowed_field.name.clone(),
                borrowed_field.as_inner_schema_field(),
            );
        }
        types
    }

    fn construct_parser(&self, py: pyo3::Python) -> PyResult<Box<dyn Parser>> {
        match self.format_type.as_ref() {
            "dsv" => {
                let settings = self.construct_dsv_settings(py)?;
                Ok(settings.parser(self.schema(py)))
            }
            "debezium" => {
                let parser = DebeziumMessageParser::new(
                    self.key_field_names.clone(),
                    self.value_field_names(py),
                    DebeziumMessageParser::standard_separator(),
                );
                Ok(Box::new(parser))
            }
            "jsonlines" => {
                let parser = JsonLinesParser::new(
                    self.key_field_names.clone(),
                    self.value_field_names(py),
                    self.column_paths.clone().unwrap_or_default(),
                    self.field_absence_is_error,
                    self.schema(py),
                );
                Ok(Box::new(parser))
            }
            "identity" => Ok(Box::new(IdentityParser::new())),
            _ => Err(PyValueError::new_err("Unknown data format")),
        }
    }

    fn construct_formatter(&self, py: pyo3::Python) -> PyResult<Box<dyn Formatter>> {
        match self.format_type.as_ref() {
            "dsv" => {
                let settings = self.construct_dsv_settings(py)?;
                Ok(settings.formatter())
            }
            "sql" => {
                let formatter =
                    PsqlUpdatesFormatter::new(self.table_name()?, self.value_field_names(py));
                Ok(Box::new(formatter))
            }
            "sql_snapshot" => {
                let maybe_formatter = PsqlSnapshotFormatter::new(
                    self.table_name()?,
                    self.key_field_names
                        .clone()
                        .ok_or_else(|| PyValueError::new_err("Primary key must be specified"))?,
                    self.value_field_names(py),
                );
                match maybe_formatter {
                    Ok(formatter) => Ok(Box::new(formatter)),
                    Err(e) => Err(PyValueError::new_err(format!(
                        "Incorrect formatter parameters: {e:?}"
                    ))),
                }
            }
            "jsonlines" => {
                let formatter = JsonLinesFormatter::new(self.value_field_names(py));
                Ok(Box::new(formatter))
            }
            "null" => {
                let formatter = NullFormatter::new();
                Ok(Box::new(formatter))
            }
            _ => Err(PyValueError::new_err("Unknown data format")),
        }
    }
}

#[pyclass(module = "pathway.engine", frozen)]
#[derive(Clone)]
pub struct EvalProperties {
    #[allow(unused)]
    dtype: Py<PyAny>,
    trace: Option<Py<Trace>>,
}

#[pymethods]
impl EvalProperties {
    #[new]
    #[pyo3(signature = (
        dtype,
        trace = None,
    ))]
    fn new(dtype: Py<PyAny>, trace: Option<Py<Trace>>) -> Self {
        Self { dtype, trace }
    }
}

impl EvalProperties {
    fn trace(&self, py: Python) -> PyResult<EngineTrace> {
        self.trace
            .clone()
            .map_or(Ok(EngineTrace::Empty), |t| t.extract(py))
    }
}

#[pyclass(module = "pathway.engine", frozen)]
#[derive(Clone)]
pub struct ConnectorProperties {
    #[pyo3(get)]
    commit_duration_ms: Option<u64>,
    #[allow(unused)]
    #[pyo3(get)]
    unsafe_trusted_ids: bool,
    #[allow(unused)]
    #[pyo3(get)]
    append_only: bool,
}

#[pymethods]
impl ConnectorProperties {
    #[new]
    #[pyo3(signature = (
        commit_duration_ms = None,
        unsafe_trusted_ids = false,
        append_only = false
    ))]
    fn new(commit_duration_ms: Option<u64>, unsafe_trusted_ids: bool, append_only: bool) -> Self {
        Self {
            commit_duration_ms,
            unsafe_trusted_ids,
            append_only,
        }
    }
}

#[pyclass(module = "pathway.engine", frozen)]
#[derive(Clone)]
pub struct Trace {
    #[pyo3(get)]
    line: String,
    #[pyo3(get)]
    file_name: String,
    #[pyo3(get)]
    line_number: u32,
    #[pyo3(get)]
    function: String,
}

#[pymethods]
impl Trace {
    #[new]
    #[pyo3(signature = (
        line,
        file_name,
        line_number,
        function,
    ))]
    fn new(line: String, file_name: String, line_number: u32, function: String) -> Self {
        Self {
            line,
            file_name,
            line_number,
            function,
        }
    }
}

impl<'source> FromPyObject<'source> for EngineTrace {
    fn extract(obj: &'source PyAny) -> PyResult<Self> {
        let Trace {
            file_name,
            line_number,
            line,
            function,
        } = obj.extract::<Trace>()?;
        Ok(Self::Frame {
            file_name,
            line,
            line_number,
            function,
        })
    }
}

impl ToPyObject for EngineTrace {
    fn to_object(&self, py: Python<'_>) -> PyObject {
        self.clone().into_py(py)
    }
}

impl IntoPy<PyObject> for EngineTrace {
    fn into_py(self, py: Python<'_>) -> PyObject {
        match self {
            Self::Empty => py.None(),
            Self::Frame {
                line,
                file_name,
                line_number,
                function,
            } => Trace {
                line,
                file_name,
                line_number,
                function,
            }
            .into_py(py),
        }
    }
}

struct WakeupHandler<'py> {
    py: Python<'py>,
    _fd: OwnedFd,
    set_wakeup_fd: &'py PyAny,
    old_wakeup_fd: &'py PyAny,
}

impl<'py> WakeupHandler<'py> {
    fn new(py: Python<'py>, fd: OwnedFd) -> PyResult<Option<Self>> {
        let signal_module = py.import("signal")?;
        let set_wakeup_fd = signal_module.getattr("set_wakeup_fd")?;
        let args = PyTuple::new(py, [fd.as_raw_fd()]);
        let old_wakeup_fd = set_wakeup_fd.call1(args);
        if let Err(ref error) = old_wakeup_fd {
            if error.is_instance_of::<PyValueError>(py) {
                // We are not the main thread. This means we can ignore signal handling.
                return Ok(None);
            }
        }
        let old_wakeup_fd = old_wakeup_fd?;
        let res = Some(Self {
            py,
            _fd: fd,
            set_wakeup_fd,
            old_wakeup_fd,
        });
        py.check_signals()?;
        Ok(res)
    }
}

impl<'py> Drop for WakeupHandler<'py> {
    fn drop(&mut self) {
        let py = self.py;
        let args = PyTuple::new(py, [self.old_wakeup_fd]);
        self.set_wakeup_fd
            .call1(args)
            .expect("restoring the wakeup fd should not fail");
    }
}

fn run_with_wakeup_receiver<R>(
    py: Python,
    logic: impl FnOnce(Option<WakeupReceiver>) -> R,
) -> PyResult<R> {
    let wakeup_pipe = pipe(ReaderType::Blocking, WriterType::NonBlocking)?;
    let wakeup_handler = WakeupHandler::new(py, wakeup_pipe.writer)?;
    let mut wakeup_reader = File::from(wakeup_pipe.reader);
    let (wakeup_sender, wakeup_receiver): (_, WakeupReceiver) = crossbeam_channel::unbounded();
    let wakeup_thread = thread::Builder::new()
        .name("pathway:signal_wakeup".to_string())
        .spawn(move || loop {
            let amount = wakeup_reader
                .read(&mut [0; 1024])
                .expect("reading from the wakeup pipe should not fail");
            if amount == 0 {
                break;
            }

            #[allow(clippy::redundant_closure_for_method_calls)]
            wakeup_sender
                .send(Box::new(|| {
                    Python::with_gil(|py| py.check_signals()).map_err(DynError::from)
                }))
                .unwrap_or(());
        })?;
    defer! {
        // Drop the handler first to close the writer end of the pipe.
        drop(wakeup_handler);
        wakeup_thread.join().unwrap()
    }
    Ok(logic(Some(wakeup_receiver)))
}

#[pymodule]
#[pyo3(name = "engine")]
fn module(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    logging::init();

    m.add_class::<BasePointer>()?;
    m.add_class::<PyReducer>()?;
    m.add_class::<PyUnaryOperator>()?;
    m.add_class::<PyBinaryOperator>()?;
    m.add_class::<PyExpression>()?;
    m.add_class::<PathwayType>()?;
    m.add_class::<PyConnectorMode>()?;
    m.add_class::<PyMonitoringLevel>()?;
    m.add_class::<Universe>()?;
    m.add_class::<Column>()?;
    m.add_class::<Table>()?;
    m.add_class::<Computer>()?;
    m.add_class::<Scope>()?;
    m.add_class::<Joiner>()?;
    m.add_class::<Grouper>()?;
    m.add_class::<Ixer>()?;
    m.add_class::<Context>()?;
    m.add_class::<VennUniverses>()?;
    m.add_class::<Concat>()?;
    m.add_class::<Flatten>()?;

    m.add_class::<AwsS3Settings>()?;
    m.add_class::<ElasticSearchParams>()?;
    m.add_class::<ElasticSearchAuth>()?;
    m.add_class::<CsvParserSettings>()?;
    m.add_class::<ValueField>()?;
    m.add_class::<DataStorage>()?;
    m.add_class::<DataFormat>()?;
    m.add_class::<PersistenceConfig>()?;
    m.add_class::<PythonSubject>()?;

    m.add_class::<ConnectorProperties>()?;
    m.add_class::<EvalProperties>()?;
    m.add_class::<Trace>()?;

    m.add_function(wrap_pyfunction!(run_with_new_graph, m)?)?;
    m.add_function(wrap_pyfunction!(ref_scalar, m)?)?;
    #[allow(clippy::unsafe_removed_from_name)] // false positive
    m.add_function(wrap_pyfunction!(unsafe_make_pointer, m)?)?;

    m.add("MissingValueError", &*MISSING_VALUE_ERROR_TYPE)?;
    m.add("EngineError", &*ENGINE_ERROR_TYPE)?;
    m.add("EngineErrorWithTrace", &*ENGINE_ERROR_WITH_TRACE_TYPE)?;

    Ok(())
}
