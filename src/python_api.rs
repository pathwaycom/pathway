// pyo3 macros seem to trigger this
#![allow(clippy::used_underscore_binding)]
// `PyRef`s need to be passed by value
#![allow(clippy::needless_pass_by_value)]

use crate::engine::{graph::SubscribeCallbacksBuilder, Computer as EngineComputer, Expressions};
use csv::ReaderBuilder as CsvReaderBuilder;
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
use once_cell::sync::Lazy;
use postgres::{Client, NoTls};
use pyo3::exceptions::{
    PyBaseException, PyException, PyIOError, PyIndexError, PyKeyError, PyRuntimeError, PyTypeError,
    PyValueError, PyZeroDivisionError,
};
use pyo3::marker::Ungil;
use pyo3::prelude::*;
use pyo3::pyclass::CompareOp;
use pyo3::sync::GILOnceCell;
use pyo3::types::{PyBool, PyBytes, PyDict, PyFloat, PyInt, PyString, PyTuple, PyType};
use pyo3::{AsPyPointer, PyTypeInfo};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::producer::{DefaultProducerContext, ThreadedProducer};
use rdkafka::ClientConfig;
use rusqlite::Connection as SqliteConnection;
use rusqlite::OpenFlags as SqliteOpenFlags;
use s3::bucket::Bucket as S3Bucket;
use scopeguard::defer;
use send_wrapper::SendWrapper;
use serde_json::Value as JsonValue;
use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufWriter, Read};
use std::mem::take;
use std::os::unix::prelude::*;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time;

use self::threads::PythonThreadState;
use crate::connectors::data_format::{
    DebeziumDBType, DebeziumMessageParser, DsvSettings, Formatter, IdentityParser,
    InnerSchemaField, JsonLinesFormatter, JsonLinesParser, NullFormatter, Parser,
    PsqlSnapshotFormatter, PsqlUpdatesFormatter, TransparentParser,
};
use crate::connectors::data_storage::{
    ConnectorMode, CsvFilesystemReader, DataEventType, ElasticSearchWriter, FileWriter,
    FilesystemReader, KafkaReader, KafkaWriter, NullWriter, PsqlWriter, PythonReaderBuilder,
    ReadMethod, ReaderBuilder, S3CsvReader, S3GenericReader, SqliteReader, Writer,
};
use crate::connectors::snapshot::Event as SnapshotEvent;
use crate::connectors::{PersistenceMode, SessionType, SnapshotAccess};
use crate::engine::dataflow::config_from_env;
use crate::engine::error::{DynError, DynResult, Trace as EngineTrace};
use crate::engine::graph::ScopedContext;
use crate::engine::progress_reporter::MonitoringLevel;
use crate::engine::reduce::StatefulCombineFn;
use crate::engine::time::DateTime;
use crate::engine::ReducerData;
use crate::engine::{
    run_with_new_dataflow_graph, BatchWrapper, ColumnHandle, ColumnPath,
    ColumnProperties as EngineColumnProperties, DataRow, DateTimeNaive, DateTimeUtc, Duration,
    ExpressionData, IxKeyPolicy, JoinType, Key, KeyImpl, PointerExpression, Reducer, ScopedGraph,
    TableHandle, TableProperties as EngineTableProperties, Type, UniverseHandle, Value,
};
use crate::engine::{AnyExpression, Context as EngineContext};
use crate::engine::{BoolExpression, Error as EngineError};
use crate::engine::{ComplexColumn as EngineComplexColumn, WakeupReceiver};
use crate::engine::{DateTimeNaiveExpression, DateTimeUtcExpression, DurationExpression};
use crate::engine::{Expression, IntExpression};
use crate::engine::{FloatExpression, Graph};
use crate::engine::{LegacyTable as EngineLegacyTable, StringExpression};
use crate::persistence::config::{
    ConnectorWorkerPair, MetadataStorageConfig, PersistenceManagerOuterConfig, StreamStorageConfig,
};
use crate::persistence::{ExternalPersistentId, IntoPersistentId, PersistentId};
use crate::pipe::{pipe, ReaderType, WriterType};
use s3::creds::Credentials as AwsCredentials;

mod logging;
mod numba;
pub mod threads;

pub fn with_gil_and_pool<R>(f: impl FnOnce(Python) -> R + Ungil) -> R {
    Python::with_gil(|py| py.with_pool(f))
}

const S3_PATH_PREFIX: &str = "s3://";
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
        Ok(ob.extract::<PyRef<Pointer>>()?.0)
    }
}

impl ToPyObject for Key {
    fn to_object(&self, py: Python<'_>) -> PyObject {
        Pointer(*self).into_py(py)
    }
}

impl IntoPy<PyObject> for Key {
    fn into_py(self, py: Python<'_>) -> PyObject {
        Pointer(self).into_py(py)
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

fn value_json_from_py_any(ob: &PyAny) -> PyResult<Value> {
    let py = ob.py();
    let json_str = get_convert_python_module(py)
        .call_method1("_json_dumps", (ob,))?
        .extract()?;
    let json: JsonValue =
        serde_json::from_str(json_str).map_err(|_| PyValueError::new_err("malformed json"))?;
    Ok(Value::from(json))
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
        } else if PyBytes::is_exact_type_of(ob) {
            Ok(Value::from(
                ob.downcast::<PyBytes>()
                    .expect("type conversion should work for bytes")
                    .as_bytes(),
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
        } else if Pointer::is_exact_type_of(ob) {
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
        } else if let Ok(bytes) = ob.downcast::<PyBytes>() {
            Ok(Value::Bytes(bytes.as_bytes().into()))
        } else if let Ok(t) = ob.extract::<Vec<Self>>() {
            Ok(Value::from(t.as_slice()))
        } else if let Ok(dict) = ob.downcast::<PyDict>() {
            value_json_from_py_any(dict)
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
            } else if type_name == "Json" {
                return value_json_from_py_any(ob.getattr("value")?);
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

fn json_to_py_object(py: Python<'_>, json: &JsonValue) -> PyObject {
    get_convert_python_module(py)
        .call_method1("_parse_to_json", (json.to_string(),))
        .unwrap()
        .into_py(py)
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
            Self::Bytes(b) => PyBytes::new(py, b).into(),
            Self::Tuple(t) => PyTuple::new(py, t.iter()).into(),
            Self::IntArray(a) => PyArray::from_array(py, a).into(),
            Self::FloatArray(a) => PyArray::from_array(py, a).into(),
            Self::DateTimeNaive(dt) => dt.into_py(py),
            Self::DateTimeUtc(dt) => dt.into_py(py),
            Self::Duration(d) => d.into_py(py),
            Self::Json(j) => json_to_py_object(py, j),
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
        Ok(ob.extract::<PyRef<PyReducer>>()?.0.clone())
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

impl<'source> FromPyObject<'source> for ReadMethod {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        Ok(ob.extract::<PyRef<PyReadMethod>>()?.0)
    }
}

impl IntoPy<PyObject> for ReadMethod {
    fn into_py(self, py: Python<'_>) -> PyObject {
        PyReadMethod(self).into_py(py)
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

impl<'source> FromPyObject<'source> for SessionType {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        Ok(ob.extract::<PyRef<PySessionType>>()?.0)
    }
}

impl IntoPy<PyObject> for SessionType {
    fn into_py(self, py: Python<'_>) -> PyObject {
        PySessionType(self).into_py(py)
    }
}

impl<'source> FromPyObject<'source> for DataEventType {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        Ok(ob.extract::<PyRef<PyDataEventType>>()?.0)
    }
}

impl IntoPy<PyObject> for DataEventType {
    fn into_py(self, py: Python<'_>) -> PyObject {
        PyDataEventType(self).into_py(py)
    }
}

impl<'source> FromPyObject<'source> for DebeziumDBType {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        Ok(ob.extract::<PyRef<PyDebeziumDBType>>()?.0)
    }
}

impl IntoPy<PyObject> for DebeziumDBType {
    fn into_py(self, py: Python<'_>) -> PyObject {
        PyDebeziumDBType(self).into_py(py)
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

fn engine_tables_from_py_iterable(iterable: &PyAny) -> PyResult<Vec<EngineLegacyTable>> {
    let py = iterable.py();
    iterable
        .iter()?
        .map(|table| {
            let table: PyRef<LegacyTable> = table?.extract()?;
            Ok(table.to_engine(py))
        })
        .collect()
}

#[pyclass(module = "pathway.engine", frozen)]
pub struct Pointer(Key);

#[pymethods]
impl Pointer {
    pub fn __str__(&self) -> String {
        self.0.to_string()
    }

    pub fn __repr__(&self) -> String {
        format!("Pointer(\"{}\")", self.0.to_string().escape_default())
    }

    pub fn __hash__(&self) -> usize {
        self.0 .0 as usize
    }

    pub fn __int__(&self) -> KeyImpl {
        self.0 .0
    }

    pub fn __richcmp__(&self, other: &PyAny, op: CompareOp) -> Py<PyAny> {
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

    #[classmethod]
    pub fn __class_getitem__<'py>(
        cls: &'py PyType,
        #[allow(unused)] item: &'py PyAny,
    ) -> &'py PyType {
        cls
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

    #[staticmethod]
    fn sorted_tuple(skip_nones: bool) -> Reducer {
        Reducer::SortedTuple { skip_nones }
    }

    #[staticmethod]
    fn tuple(skip_nones: bool) -> Reducer {
        Reducer::Tuple { skip_nones }
    }

    #[classattr]
    pub const UNIQUE: Reducer = Reducer::Unique;

    #[classattr]
    pub const COUNT: Reducer = Reducer::Count;

    #[classattr]
    pub const ANY: Reducer = Reducer::Any;

    #[staticmethod]
    fn stateful_many(combine: Py<PyAny>) -> Reducer {
        let combine_fn: StatefulCombineFn = Arc::new(move |state, values| {
            with_gil_and_pool(|py| {
                let combine = combine.as_ref(py);
                let state = state.to_object(py);
                let values = values.to_object(py);
                combine
                    .call1((state, values))
                    .unwrap_or_else(|e| {
                        Python::with_gil(|py| e.print(py));
                        panic!("python error");
                    })
                    .extract()
                    .unwrap_or_else(|e| {
                        Python::with_gil(|py| e.print(py));
                        panic!("python error");
                    })
            })
        });
        Reducer::Stateful { combine_fn }
    }
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
    #[pyo3(signature = (function, *args))]
    fn unsafe_numba_apply(function: &PyAny, args: Vec<PyRef<PyExpression>>) -> PyResult<Self> {
        let args = args
            .into_iter()
            .map(|expr| expr.inner.clone())
            .collect_vec();
        let num_args = args.len();
        let inner = unsafe { numba::get_numba_expression(function, args.into(), num_args) }?;
        Ok(Self::new(inner, false))
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
            _ => None,
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
            (Op::TrueDiv, Tp::Duration, Tp::Int) => {
                Some(binary_op!(DurationE::TrueDivByInt, lhs, rhs))
            }
            (Op::Mul, Tp::Duration, Tp::Float) => Some(binary_op!(DurationE::MulByFloat, lhs, rhs)),
            (Op::Mul, Tp::Float, Tp::Duration) => Some(binary_op!(DurationE::MulByFloat, rhs, lhs)),
            (Op::TrueDiv, Tp::Duration, Tp::Float) => {
                Some(binary_op!(DurationE::DivByFloat, lhs, rhs))
            }
            (Op::FloorDiv, Tp::Duration, Tp::Duration) => {
                Some(binary_op!(IntExpression::DurationFloorDiv, lhs, rhs))
            }
            (Op::TrueDiv, Tp::Duration, Tp::Duration) => {
                Some(binary_op!(FloatExpression::DurationTrueDiv, lhs, rhs))
            }
            (Op::Mod, Tp::Duration, Tp::Duration) => Some(binary_op!(DurationE::Mod, lhs, rhs)),
            (Op::MatMul, Tp::Array, Tp::Array) => Some(binary_op!(AnyE::MatMul, lhs, rhs)),
            (Op::Eq, Tp::Tuple, Tp::Tuple) => Some(binary_op!(BoolE::TupleEq, lhs, rhs)),
            (Op::Ne, Tp::Tuple, Tp::Tuple) => Some(binary_op!(BoolE::TupleNe, lhs, rhs)),
            (Op::Lt, Tp::Tuple, Tp::Tuple) => Some(binary_op!(BoolE::TupleLt, lhs, rhs)),
            (Op::Le, Tp::Tuple, Tp::Tuple) => Some(binary_op!(BoolE::TupleLe, lhs, rhs)),
            (Op::Gt, Tp::Tuple, Tp::Tuple) => Some(binary_op!(BoolE::TupleGt, lhs, rhs)),
            (Op::Ge, Tp::Tuple, Tp::Tuple) => Some(binary_op!(BoolE::TupleGe, lhs, rhs)),
            _ => None,
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
            _ => None,
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
            _ => None,
        }
    }

    #[staticmethod]
    fn convert_optional(expr: &PyExpression, source_type: Type, target_type: Type) -> Option<Self> {
        type Tp = Type;
        match (source_type, target_type) {
            (Tp::Json, Tp::Int | Tp::Float | Tp::Bool | Tp::String) => {
                Some(unary_op!(AnyExpression::JsonToOptional, expr, target_type))
            }
            _ => None,
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

    #[staticmethod]
    fn json_get_item_checked(
        expr: &PyExpression,
        index: &PyExpression,
        default: &PyExpression,
    ) -> Self {
        Self::new(
            Arc::new(Expression::Any(AnyExpression::JsonGetItem(
                expr.inner.clone(),
                index.inner.clone(),
                default.inner.clone(),
            ))),
            expr.gil || index.gil || default.gil,
        )
    }

    #[staticmethod]
    fn json_get_item_unchecked(expr: &PyExpression, index: &PyExpression) -> Self {
        Self::new(
            Arc::new(Expression::Any(AnyExpression::JsonGetItem(
                expr.inner.clone(),
                index.inner.clone(),
                Arc::new(Expression::Any(AnyExpression::Const(Value::from(
                    serde_json::Value::Null,
                )))),
            ))),
            expr.gil || index.gil,
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
    date_time_naive_timestamp_ns,
    IntExpression::DateTimeNaiveTimestampNs
);
binary_expr!(
    date_time_naive_timestamp,
    FloatExpression::DateTimeNaiveTimestamp
);
unary_expr!(date_time_naive_weekday, IntExpression::DateTimeNaiveWeekday);
binary_expr!(date_time_naive_strptime, DateTimeNaiveExpression::Strptime);
binary_expr!(
    date_time_naive_strftime,
    StringExpression::DateTimeNaiveStrftime
);
binary_expr!(
    date_time_naive_from_timestamp,
    DateTimeNaiveExpression::FromTimestamp
);
binary_expr!(
    date_time_naive_from_float_timestamp,
    DateTimeNaiveExpression::FromFloatTimestamp
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
unary_expr!(
    date_time_utc_timestamp_ns,
    IntExpression::DateTimeUtcTimestampNs
);
binary_expr!(
    date_time_utc_timestamp,
    FloatExpression::DateTimeUtcTimestamp
);
unary_expr!(date_time_utc_weekday, IntExpression::DateTimeUtcWeekday);
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
    #[classattr]
    pub const JSON: Type = Type::Json;
    #[classattr]
    pub const TUPLE: Type = Type::Tuple;
}

#[pyclass(module = "pathway.engine", frozen, name = "ReadMethod")]
pub struct PyReadMethod(ReadMethod);

#[pymethods]
impl PyReadMethod {
    #[classattr]
    pub const BY_LINE: ReadMethod = ReadMethod::ByLine;
    #[classattr]
    pub const FULL: ReadMethod = ReadMethod::Full;
}

#[pyclass(module = "pathway.engine", frozen, name = "ConnectorMode")]
pub struct PyConnectorMode(ConnectorMode);

#[pymethods]
impl PyConnectorMode {
    #[classattr]
    pub const STATIC: ConnectorMode = ConnectorMode::Static;
    #[classattr]
    pub const STREAMING: ConnectorMode = ConnectorMode::Streaming;

    pub fn __eq__(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

#[pyclass(module = "pathway.engine", frozen, name = "SessionType")]
pub struct PySessionType(SessionType);

#[pymethods]
impl PySessionType {
    #[classattr]
    pub const NATIVE: SessionType = SessionType::Native;
    #[classattr]
    pub const UPSERT: SessionType = SessionType::Upsert;
}

#[pyclass(module = "pathway.engine", frozen, name = "DataEventType")]
pub struct PyDataEventType(DataEventType);

#[pymethods]
impl PyDataEventType {
    #[classattr]
    pub const INSERT: DataEventType = DataEventType::Insert;
    #[classattr]
    pub const DELETE: DataEventType = DataEventType::Delete;
    #[classattr]
    pub const UPSERT: DataEventType = DataEventType::Upsert;
}

#[pyclass(module = "pathway.engine", frozen, name = "DebeziumDBType")]
pub struct PyDebeziumDBType(DebeziumDBType);

#[pymethods]
impl PyDebeziumDBType {
    #[classattr]
    pub const POSTGRES: DebeziumDBType = DebeziumDBType::Postgres;
    #[classattr]
    pub const MONGO_DB: DebeziumDBType = DebeziumDBType::MongoDB;
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
pub struct LegacyTable {
    #[pyo3(get)] // ?
    universe: Py<Universe>,

    #[pyo3(get)] // ?
    columns: Vec<Py<Column>>,
}

#[pymethods]
impl LegacyTable {
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
            "<LegacyTable universe={:?} columns=[{}]>",
            self.universe.borrow(py).handle,
            self.columns.iter().format_with(", ", |column, f| {
                f(&format_args!("{:?}", column.borrow(py).handle))
            })
        )
    }
}

impl LegacyTable {
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

    fn from_engine(scope: &PyCell<Scope>, table: EngineLegacyTable) -> Self {
        let (universe_handle, column_handles) = table;
        Self::from_handles(scope, universe_handle, column_handles).unwrap()
    }
}

#[pyclass(module = "pathway.engine", frozen)]
pub struct Table {
    scope: Py<Scope>,
    handle: TableHandle,
}

impl Table {
    fn new(scope: &PyCell<Scope>, handle: TableHandle) -> PyResult<Py<Self>> {
        let py = scope.py();
        if let Some(table) = scope.borrow().tables.borrow().get(&handle) {
            return Ok(table.clone());
        }
        let res = Py::new(
            py,
            Self {
                scope: scope.into(),
                handle,
            },
        )?;
        scope
            .borrow()
            .tables
            .borrow_mut()
            .insert(handle, res.clone());
        Ok(res)
    }
}

impl<'source> FromPyObject<'source> for ColumnPath {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        if ob.getattr("is_key").is_ok_and(|is_key| {
            is_key
                .extract::<&PyBool>()
                .expect("is_key field in ColumnPath should be bool")
                .is_true()
        }) {
            Ok(Self::Key)
        } else if let Ok(path) = ob.getattr("path").and_then(PyAny::extract) {
            Ok(Self::ValuePath(path))
        } else {
            Err(PyTypeError::new_err(format!(
                "can't convert {} to ColumnPath",
                ob.get_type().name()?
            )))
        }
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
pub struct Scope {
    #[pyo3(get)]
    parent: Option<Py<Self>>,

    graph: SendWrapper<ScopedGraph>,

    // empty_universe: Lazy<Py<Universe>>,
    universes: RefCell<HashMap<UniverseHandle, Py<Universe>>>,
    columns: RefCell<HashMap<ColumnHandle, Py<Column>>>,
    tables: RefCell<HashMap<TableHandle, Py<Table>>>,
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
            tables: RefCell::new(HashMap::new()),
            persistent_ids: RefCell::new(HashSet::new()),
            event_loop,
        }
    }

    fn clear_caches(&self) {
        self.universes.borrow_mut().clear();
        self.columns.borrow_mut().clear();
        self.tables.borrow_mut().clear();
    }
}

#[pymethods]
impl Scope {
    pub fn empty_table(
        self_: &PyCell<Self>,
        properties: ConnectorProperties,
    ) -> PyResult<Py<Table>> {
        let column_properties = properties.column_properties();
        let table_handle = self_
            .borrow()
            .graph
            .empty_table(Arc::new(EngineTableProperties::flat(column_properties)))?;
        Table::new(self_, table_handle)
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
        properties: ColumnProperties,
    ) -> PyResult<Py<Column>> {
        check_identity(self_, &universe.borrow().scope, "scope mismatch")?;
        let handle =
            self_
                .borrow()
                .graph
                .static_column(universe.borrow().handle, values, properties.0)?;
        Column::new(universe, handle)
    }

    pub fn static_table(
        self_: &PyCell<Self>,
        #[pyo3(from_py_with = "from_py_iterable")] data: Vec<DataRow>,
        properties: ConnectorProperties,
    ) -> PyResult<Py<Table>> {
        let column_properties = properties.column_properties();
        let handle = self_.borrow().graph.static_table(
            data,
            Arc::new(EngineTableProperties::flat(column_properties)),
        )?;
        Table::new(self_, handle)
    }

    pub fn connector_table(
        self_: &PyCell<Self>,
        data_source: &PyCell<DataStorage>,
        data_format: &PyCell<DataFormat>,
        properties: ConnectorProperties,
    ) -> PyResult<Py<Table>> {
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

        let column_properties = properties.column_properties();

        let table_handle = self_.borrow().graph.connector_table(
            reader_impl,
            parser_impl,
            properties
                .commit_duration_ms
                .map(time::Duration::from_millis),
            parallel_readers,
            Arc::new(EngineTableProperties::flat(column_properties)),
            persistent_id.as_ref(),
        )?;
        Table::new(self_, table_handle)
    }

    #[allow(clippy::type_complexity)]
    #[pyo3(signature = (iterated, iterated_with_universe, extra, logic, *, limit = None))]
    pub fn iterate(
        self_: &PyCell<Self>,
        #[pyo3(from_py_with = "engine_tables_from_py_iterable")] iterated: Vec<EngineLegacyTable>,
        #[pyo3(from_py_with = "engine_tables_from_py_iterable")] iterated_with_universe: Vec<
            EngineLegacyTable,
        >,
        #[pyo3(from_py_with = "engine_tables_from_py_iterable")] extra: Vec<EngineLegacyTable>,
        logic: &PyAny,
        limit: Option<u32>,
    ) -> PyResult<(Vec<Py<LegacyTable>>, Vec<Py<LegacyTable>>)> {
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
                        .map(|table| LegacyTable::from_engine(scope, table))
                        .collect::<Vec<_>>();
                    let iterated_with_universe = iterated_with_universe
                        .into_iter()
                        .map(|table| LegacyTable::from_engine(scope, table))
                        .collect::<Vec<_>>();
                    let extra = extra
                        .into_iter()
                        .map(|table| LegacyTable::from_engine(scope, table))
                        .collect::<Vec<_>>();
                    let (result, result_with_universe): (&PyAny, &PyAny) = logic
                        .call1((scope, iterated, iterated_with_universe, extra))?
                        .extract()?;
                    let result = result
                        .iter()?
                        .map(|table| {
                            let table: PyRef<LegacyTable> = table?.extract()?;
                            Ok(table.to_engine(py))
                        })
                        .collect::<PyResult<_>>()?;
                    let result_with_universe = result_with_universe
                        .iter()?
                        .map(|table| {
                            let table: PyRef<LegacyTable> = table?.extract()?;
                            Ok(table.to_engine(py))
                        })
                        .collect::<PyResult<_>>()?;
                    Ok((result, result_with_universe))
                })
            }),
        )?;
        let result = result
            .into_iter()
            .map(|table| Py::new(py, LegacyTable::from_engine(self_, table)))
            .collect::<PyResult<_>>()?;
        let result_with_universe = result_with_universe
            .into_iter()
            .map(|table| Py::new(py, LegacyTable::from_engine(self_, table)))
            .collect::<PyResult<_>>()?;
        Ok((result, result_with_universe))
    }

    pub fn map_column(
        self_: &PyCell<Self>,
        table: &LegacyTable,
        function: Py<PyAny>,
        properties: ColumnProperties,
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
            properties.0,
            EngineTrace::Empty,
        )?;
        Column::new(universe, handle)
    }

    pub fn async_apply_table(
        self_: &PyCell<Self>,
        table: PyRef<Table>,
        #[pyo3(from_py_with = "from_py_iterable")] column_paths: Vec<ColumnPath>,
        function: Py<PyAny>,
        properties: TableProperties,
    ) -> PyResult<Py<Table>> {
        let event_loop = self_.borrow().event_loop.clone();
        let table_handle = self_.borrow().graph.async_apply_table(
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
            table.handle,
            column_paths,
            properties.0,
            EngineTrace::Empty,
        )?;
        Table::new(self_, table_handle)
    }

    pub fn expression_table(
        self_: &PyCell<Self>,
        table: &Table,
        #[pyo3(from_py_with = "from_py_iterable")] column_paths: Vec<ColumnPath>,
        #[pyo3(from_py_with = "from_py_iterable")] expressions: Vec<(
            PyRef<PyExpression>,
            TableProperties,
        )>,
    ) -> PyResult<Py<Table>> {
        let gil = expressions
            .iter()
            .any(|(expression, _properties)| expression.gil);
        let wrapper = if gil {
            BatchWrapper::WithGil
        } else {
            BatchWrapper::None
        };
        let expressions: Vec<ExpressionData> = expressions
            .into_iter()
            .map(|(expression, properties)| {
                ExpressionData::new(expression.inner.clone(), properties.0)
            })
            .collect();
        let table_handle = self_.borrow().graph.expression_table(
            table.handle,
            column_paths,
            expressions,
            wrapper,
        )?;
        Table::new(self_, table_handle)
    }

    pub fn columns_to_table(
        self_: &PyCell<Self>,
        universe: &PyCell<Universe>,
        #[pyo3(from_py_with = "from_py_iterable")] columns: Vec<PyRef<Column>>,
    ) -> PyResult<Py<Table>> {
        let column_handles = columns.into_iter().map(|column| column.handle).collect();
        let table_handle = self_
            .borrow()
            .graph
            .columns_to_table(universe.borrow().handle, column_handles)?;
        Table::new(self_, table_handle)
    }

    pub fn table_column(
        self_: &PyCell<Self>,
        universe: &PyCell<Universe>,
        table: PyRef<Table>,
        column_path: ColumnPath,
    ) -> PyResult<Py<Column>> {
        let handle = self_.borrow().graph.table_column(
            universe.borrow().handle,
            table.handle,
            column_path,
        )?;
        Column::new(universe, handle)
    }

    pub fn table_universe(self_: &PyCell<Self>, table: PyRef<Table>) -> PyResult<Py<Universe>> {
        let universe_handle = self_.borrow().graph.table_universe(table.handle)?;
        Universe::new(self_, universe_handle)
    }

    pub fn table_properties(
        self_: &PyCell<Self>,
        table: PyRef<Table>,
    ) -> PyResult<Py<TableProperties>> {
        let py = self_.py();
        let properties = self_.borrow().graph.table_properties(table.handle)?;
        TableProperties::new(py, properties)
    }

    pub fn flatten_table_storage(
        self_: &PyCell<Self>,
        table: PyRef<Table>,
        #[pyo3(from_py_with = "from_py_iterable")] column_paths: Vec<ColumnPath>,
    ) -> PyResult<Py<Table>> {
        let table_handle = self_
            .borrow()
            .graph
            .flatten_table_storage(table.handle, column_paths)?;
        Table::new(self_, table_handle)
    }

    pub fn filter_table(
        self_: &PyCell<Self>,
        table: PyRef<Table>,
        filtering_column_path: ColumnPath,
        table_properties: TableProperties,
    ) -> PyResult<Py<Table>> {
        let new_table_handle = self_.borrow().graph.filter_table(
            table.handle,
            filtering_column_path,
            table_properties.0,
        )?;
        Table::new(self_, new_table_handle)
    }

    pub fn forget(
        self_: &PyCell<Self>,
        table: PyRef<Table>,
        threshold_column_path: ColumnPath,
        current_time_column_path: ColumnPath,
        mark_forgetting_records: bool,
        table_properties: TableProperties,
    ) -> PyResult<Py<Table>> {
        let new_table_handle = self_.borrow().graph.forget(
            table.handle,
            threshold_column_path,
            current_time_column_path,
            mark_forgetting_records,
            table_properties.0,
        )?;
        Table::new(self_, new_table_handle)
    }

    pub fn forget_immediately(
        self_: &PyCell<Self>,
        table: PyRef<Table>,
        table_properties: TableProperties,
    ) -> PyResult<Py<Table>> {
        let new_table_handle = self_
            .borrow()
            .graph
            .forget_immediately(table.handle, table_properties.0)?;
        Table::new(self_, new_table_handle)
    }

    pub fn filter_out_results_of_forgetting(
        self_: &PyCell<Self>,
        table: PyRef<Table>,
        table_properties: TableProperties,
    ) -> PyResult<Py<Table>> {
        let new_table_handle = self_
            .borrow()
            .graph
            .filter_out_results_of_forgetting(table.handle, table_properties.0)?;
        Table::new(self_, new_table_handle)
    }

    pub fn freeze(
        self_: &PyCell<Self>,
        table: PyRef<Table>,
        threshold_column_path: ColumnPath,
        current_time_column_path: ColumnPath,
        table_properties: TableProperties,
    ) -> PyResult<Py<Table>> {
        let new_table_handle = self_.borrow().graph.freeze(
            table.handle,
            threshold_column_path,
            current_time_column_path,
            table_properties.0,
        )?;
        Table::new(self_, new_table_handle)
    }

    pub fn gradual_broadcast(
        self_: &PyCell<Self>,
        input_table: PyRef<Table>,
        threshold_table: PyRef<Table>,
        lower_path: ColumnPath,
        value_path: ColumnPath,
        upper_path: ColumnPath,
        table_properties: TableProperties,
    ) -> PyResult<Py<Table>> {
        let new_table_handle = self_.borrow().graph.gradual_broadcast(
            input_table.handle,
            threshold_table.handle,
            lower_path,
            value_path,
            upper_path,
            table_properties.0,
        )?;
        Table::new(self_, new_table_handle)
    }

    pub fn buffer(
        self_: &PyCell<Self>,
        table: PyRef<Table>,
        threshold_column_path: ColumnPath,
        current_time_column_path: ColumnPath,
        table_properties: TableProperties,
    ) -> PyResult<Py<Table>> {
        let new_table_handle = self_.borrow().graph.buffer(
            table.handle,
            threshold_column_path,
            current_time_column_path,
            table_properties.0,
        )?;
        Table::new(self_, new_table_handle)
    }

    pub fn intersect_tables(
        self_: &PyCell<Self>,
        table: PyRef<Table>,
        #[pyo3(from_py_with = "from_py_iterable")] tables: Vec<PyRef<Table>>,
        table_properties: TableProperties,
    ) -> PyResult<Py<Table>> {
        let table_handles = tables.into_iter().map(|table| table.handle).collect();
        let result_table_handle = self_.borrow().graph.intersect_tables(
            table.handle,
            table_handles,
            table_properties.0,
        )?;
        Table::new(self_, result_table_handle)
    }

    pub fn subtract_table(
        self_: &PyCell<Self>,
        left_table: PyRef<Table>,
        right_table: PyRef<Table>,
        table_properties: TableProperties,
    ) -> PyResult<Py<Table>> {
        let result_table_handle = self_.borrow().graph.subtract_table(
            left_table.handle,
            right_table.handle,
            table_properties.0,
        )?;
        Table::new(self_, result_table_handle)
    }

    pub fn concat_tables(
        self_: &PyCell<Self>,
        #[pyo3(from_py_with = "from_py_iterable")] tables: Vec<PyRef<Table>>,
        table_properties: TableProperties,
    ) -> PyResult<Py<Table>> {
        let table_handles = tables.into_iter().map(|table| table.handle).collect();
        let table_handle = self_
            .borrow()
            .graph
            .concat_tables(table_handles, table_properties.0)?;
        Table::new(self_, table_handle)
    }

    pub fn flatten_table(
        self_: &PyCell<Self>,
        table: PyRef<Table>,
        flatten_column_path: ColumnPath,
        table_properties: TableProperties,
    ) -> PyResult<Py<Table>> {
        let new_table_handle = self_.borrow().graph.flatten_table(
            table.handle,
            flatten_column_path,
            table_properties.0,
        )?;
        Table::new(self_, new_table_handle)
    }

    pub fn sort_table(
        self_: &PyCell<Self>,
        table: PyRef<Table>,
        key_column_path: ColumnPath,
        instance_column_path: ColumnPath,
        table_properties: TableProperties,
    ) -> PyResult<Py<Table>> {
        let new_table_handle = self_.borrow().graph.sort_table(
            table.handle,
            key_column_path,
            instance_column_path,
            table_properties.0,
        )?;
        Table::new(self_, new_table_handle)
    }

    pub fn reindex_table(
        self_: &PyCell<Self>,
        table: PyRef<Table>,
        reindexing_column_path: ColumnPath,
        table_properties: TableProperties,
    ) -> PyResult<Py<Table>> {
        let result_table_handle = self_.borrow().graph.reindex_table(
            table.handle,
            reindexing_column_path,
            table_properties.0,
        )?;
        Table::new(self_, result_table_handle)
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

    pub fn restrict_table(
        self_: &PyCell<Self>,
        orig_table: PyRef<Table>,
        new_table: PyRef<Table>,
        table_properties: TableProperties,
    ) -> PyResult<Py<Table>> {
        let result_table_handle = self_.borrow().graph.restrict_or_override_table_universe(
            orig_table.handle,
            new_table.handle,
            false,
            table_properties.0,
        )?;
        Table::new(self_, result_table_handle)
    }

    pub fn override_table_universe(
        self_: &PyCell<Self>,
        orig_table: PyRef<Table>,
        new_table: PyRef<Table>,
        table_properties: TableProperties,
    ) -> PyResult<Py<Table>> {
        let result_table_handle = self_.borrow().graph.restrict_or_override_table_universe(
            orig_table.handle,
            new_table.handle,
            true,
            table_properties.0,
        )?;
        Table::new(self_, result_table_handle)
    }

    pub fn table<'py>(
        self_: &'py PyCell<Self>,
        universe: &'py PyCell<Universe>,
        columns: &'py PyAny,
    ) -> PyResult<LegacyTable> {
        check_identity(self_, &universe.borrow().scope, "scope mismatch")?;
        let columns = columns
            .iter()?
            .map(|column| {
                let column: &PyCell<Column> = column?.extract()?;
                Self::restrict_column(self_, universe, column)
            })
            .collect::<PyResult<_>>()?;
        LegacyTable::new(universe, columns)
    }

    pub fn group_by_table(
        self_: &PyCell<Self>,
        table: PyRef<Table>,
        #[pyo3(from_py_with = "from_py_iterable")] grouping_columns_paths: Vec<ColumnPath>,
        #[pyo3(from_py_with = "from_py_iterable")] reducers: Vec<(Reducer, Vec<ColumnPath>)>,
        set_id: bool,
        table_properties: TableProperties,
    ) -> PyResult<Py<Table>> {
        let reducers = reducers
            .into_iter()
            .map(|(reducer, paths)| ReducerData::new(reducer, paths))
            .collect();
        let table_handle = self_.borrow().graph.group_by_table(
            table.handle,
            grouping_columns_paths,
            reducers,
            set_id,
            table_properties.0,
        )?;
        Table::new(self_, table_handle)
    }

    pub fn ix_table(
        self_: &PyCell<Self>,
        to_ix_table: PyRef<Table>,
        key_table: PyRef<Table>,
        key_column_path: ColumnPath,
        optional: bool,
        strict: bool,
        table_properties: TableProperties,
    ) -> PyResult<Py<Table>> {
        let ix_key_policy = IxKeyPolicy::from_strict_optional(strict, optional)?;
        let result_table_handle = self_.borrow().graph.ix_table(
            to_ix_table.handle,
            key_table.handle,
            key_column_path,
            ix_key_policy,
            table_properties.0,
        )?;
        Table::new(self_, result_table_handle)
    }

    #[pyo3(signature = (left_table, right_table, left_column_paths, right_column_paths, table_properties, assign_id = false, left_ear = false, right_ear = false))]
    #[allow(clippy::too_many_arguments)]
    pub fn join_tables(
        self_: &PyCell<Self>,
        left_table: PyRef<Table>,
        right_table: PyRef<Table>,
        #[pyo3(from_py_with = "from_py_iterable")] left_column_paths: Vec<ColumnPath>,
        #[pyo3(from_py_with = "from_py_iterable")] right_column_paths: Vec<ColumnPath>,
        table_properties: TableProperties,
        assign_id: bool,
        left_ear: bool,
        right_ear: bool,
    ) -> PyResult<Py<Table>> {
        let join_type = JoinType::from_assign_left_right(assign_id, left_ear, right_ear)?;
        let table_handle = self_.borrow().graph.join_tables(
            left_table.handle,
            right_table.handle,
            left_column_paths,
            right_column_paths,
            join_type,
            table_properties.0,
        )?;
        Table::new(self_, table_handle)
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
        table: &'py PyCell<Table>,
    ) -> PyResult<()> {
        check_identity(self_, &table.borrow().scope, "scope mismatch")?;
        Ok(self_
            .borrow()
            .graph
            .debug_universe(name, table.borrow().handle)?)
    }

    pub fn debug_column<'py>(
        self_: &'py PyCell<Self>,
        name: String,
        table: &'py PyCell<Table>,
        column_path: ColumnPath,
    ) -> PyResult<()> {
        check_identity(self_, &table.borrow().scope, "scope mismatch")?;
        Ok(self_
            .borrow()
            .graph
            .debug_column(name, table.borrow().handle, column_path)?)
    }

    pub fn update_rows_table(
        self_: &PyCell<Self>,
        table: PyRef<Table>,
        update: PyRef<Table>,
        table_properties: TableProperties,
    ) -> PyResult<Py<Table>> {
        let result_table_handle = self_.borrow().graph.update_rows_table(
            table.handle,
            update.handle,
            table_properties.0,
        )?;

        Table::new(self_, result_table_handle)
    }

    pub fn update_cells_table(
        self_: &PyCell<Self>,
        table: PyRef<Table>,
        update: PyRef<Table>,
        #[pyo3(from_py_with = "from_py_iterable")] column_paths: Vec<ColumnPath>,
        #[pyo3(from_py_with = "from_py_iterable")] update_paths: Vec<ColumnPath>,
        table_properties: TableProperties,
    ) -> PyResult<Py<Table>> {
        let result_table_handle = self_.borrow().graph.update_cells_table(
            table.handle,
            update.handle,
            column_paths,
            update_paths,
            table_properties.0,
        )?;

        Table::new(self_, result_table_handle)
    }

    pub fn output_table(
        self_: &PyCell<Self>,
        table: PyRef<Table>,
        #[pyo3(from_py_with = "from_py_iterable")] column_paths: Vec<ColumnPath>,
        data_sink: &PyCell<DataStorage>,
        data_format: &PyCell<DataFormat>,
    ) -> PyResult<()> {
        let py = self_.py();

        let sink_impl = data_sink.borrow().construct_writer(py)?;
        let format_impl = data_format.borrow().construct_formatter(py)?;

        self_
            .borrow()
            .graph
            .output_table(sink_impl, format_impl, table.handle, column_paths)?;

        Ok(())
    }

    pub fn subscribe_table(
        self_: &PyCell<Self>,
        table: PyRef<Table>,
        #[pyo3(from_py_with = "from_py_iterable")] column_paths: Vec<ColumnPath>,
        skip_persisted_batch: bool,
        on_change: Py<PyAny>,
        on_time_end: Py<PyAny>,
        on_end: Py<PyAny>,
    ) -> PyResult<()> {
        let callbacks = SubscribeCallbacksBuilder::new()
            .wrapper(BatchWrapper::WithGil)
            .on_data(Box::new(move |key, values, time, diff| {
                with_gil_and_pool(|py| {
                    on_change.call1(py, (key, PyTuple::new(py, values), time, diff))?;
                    Ok(())
                })
            }))
            .on_time_end(Box::new(move |new_time| {
                with_gil_and_pool(|py| {
                    on_time_end.call1(py, (new_time,))?;
                    Ok(())
                })
            }))
            .on_end(Box::new(move || {
                with_gil_and_pool(|py| {
                    on_end.call0(py)?;
                    Ok(())
                })
            }))
            .build();
        self_.borrow().graph.subscribe_table(
            table.handle,
            column_paths,
            callbacks,
            skip_persisted_batch,
        )?;
        Ok(())
    }

    pub fn probe_table(
        self_: &PyCell<Self>,
        table: PyRef<Table>,
        operator_id: usize,
    ) -> PyResult<()> {
        self_
            .borrow()
            .graph
            .probe_table(table.handle, operator_id)?;
        Ok(())
    }
}

type CapturedTableData = Arc<Mutex<Vec<DataRow>>>;

fn capture_table_data(
    graph: &dyn Graph,
    table: PyRef<Table>,
    column_paths: Vec<ColumnPath>,
) -> PyResult<CapturedTableData> {
    let table_data = Arc::new(Mutex::new(Vec::new()));
    {
        let table_data = table_data.clone();
        let callbacks = SubscribeCallbacksBuilder::new()
            .on_data(Box::new(move |key, values, time, diff| {
                assert!(diff == 1 || diff == -1);
                table_data.lock().unwrap().push(DataRow::from_engine(
                    key,
                    Vec::from(values),
                    time,
                    diff,
                ));
                Ok(())
            }))
            .build();
        graph.subscribe_table(table.handle, column_paths, callbacks, false)?;
    }
    Ok(table_data)
}

pub fn make_captured_table(table_data: Vec<CapturedTableData>) -> PyResult<Vec<DataRow>> {
    let mut combined_table_data = Vec::new();
    for single_table_data in table_data {
        combined_table_data.extend(take(&mut *single_table_data.lock().unwrap()));
    }
    Ok(combined_table_data)
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
) -> PyResult<Vec<Vec<DataRow>>> {
    defer! {
        log::logger().flush();
    }
    let (config, num_workers) =
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
                        let tables: Vec<(PyRef<Table>, Vec<ColumnPath>)> =
                            our_scope.borrow().graph.scoped(graph, || {
                                let args = PyTuple::new(py, [our_scope]);
                                from_py_iterable(logic.as_ref(py).call1(args)?)
                            })?;
                        our_scope.borrow().clear_caches();
                        tables
                            .into_iter()
                            .map(|(table, paths)| capture_table_data(graph, table, paths))
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
                num_workers,
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
    bucket_name: Option<String>,
    region: s3::region::Region,
    access_key: Option<String>,
    secret_access_key: Option<String>,
    with_path_style: bool,
    profile: Option<String>,
}

#[pymethods]
impl AwsS3Settings {
    #[new]
    #[pyo3(signature = (
        bucket_name = None,
        access_key = None,
        secret_access_key = None,
        with_path_style = false,
        region = None,
        endpoint = None,
        profile = None,
    ))]
    fn new(
        bucket_name: Option<String>,
        access_key: Option<String>,
        secret_access_key: Option<String>,
        with_path_style: bool,
        region: Option<String>,
        endpoint: Option<String>,
        profile: Option<String>,
    ) -> PyResult<Self> {
        Ok(AwsS3Settings {
            bucket_name,
            region: Self::aws_region(region, endpoint)?,
            access_key,
            secret_access_key,
            with_path_style,
            profile,
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
    fn final_bucket_name(&self, deduced_name: Option<&str>) -> PyResult<String> {
        if let Some(bucket_name) = &self.bucket_name {
            Ok(bucket_name.to_string())
        } else if let Some(bucket_name) = deduced_name {
            Ok(bucket_name.to_string())
        } else {
            Err(PyRuntimeError::new_err(
                "bucket_name not specified and isn't in the s3 path",
            ))
        }
    }

    fn construct_private_bucket(&self, deduced_name: Option<&str>) -> PyResult<S3Bucket> {
        let credentials = AwsCredentials::new(
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

        self.construct_bucket_with_credentials(credentials, deduced_name)
    }

    fn construct_bucket_with_credentials(
        &self,
        credentials: AwsCredentials,
        deduced_name: Option<&str>,
    ) -> PyResult<S3Bucket> {
        S3Bucket::new(
            &self.final_bucket_name(deduced_name)?,
            self.region.clone(),
            credentials,
        )
        .map_err(|err| {
            PyRuntimeError::new_err(format!("Failed to connect to private AWS bucket: {err}"))
        })
    }

    fn construct_public_bucket(&self, deduced_name: Option<&str>) -> PyResult<S3Bucket> {
        S3Bucket::new_public(&self.final_bucket_name(deduced_name)?, self.region.clone()).map_err(
            |err| PyRuntimeError::new_err(format!("Failed to connect to public AWS bucket: {err}")),
        )
    }

    fn deduce_bucket_and_path(s3_path: &str) -> (Option<String>, Option<String>) {
        if !s3_path.starts_with(S3_PATH_PREFIX) {
            return (None, Some(s3_path.to_string()));
        }
        let bucket_and_path = &s3_path[S3_PATH_PREFIX.len()..];
        let bucket_and_path_tokenized: Vec<&str> = bucket_and_path.split('/').collect();

        let bucket = bucket_and_path_tokenized[0];
        let path = bucket_and_path_tokenized[1..].join("/");

        (Some(bucket.to_string()), Some(path))
    }

    fn construct_bucket(&self, name_override: Option<&str>) -> PyResult<S3Bucket> {
        let has_access_key = self.access_key.is_some();
        let has_secret_access_key = self.secret_access_key.is_some();
        if has_access_key != has_secret_access_key {
            warn!("Only one of access_key and secret_access_key is specified. Trying to connect to a public bucket.");
        }

        let mut bucket = {
            if has_access_key && has_secret_access_key {
                self.construct_private_bucket(name_override)?
            } else {
                let aws_credentials = AwsCredentials::from_sts_env("aws-creds")
                    .or_else(|_| AwsCredentials::from_env())
                    .or_else(|_| AwsCredentials::from_profile(self.profile.as_deref()))
                    .or_else(|_| AwsCredentials::from_instance_metadata());

                // first, try to deduce credentials from various sources
                if let Ok(credentials) = aws_credentials {
                    self.construct_bucket_with_credentials(credentials, name_override)?
                } else {
                    // if there are no credentials, treat the bucket as a public
                    self.construct_public_bucket(name_override)?
                }
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
}

#[pymethods]
impl ElasticSearchParams {
    #[new]
    fn new(host: String, index_name: String, auth: Py<ElasticSearchAuth>) -> Self {
        ElasticSearchParams {
            host,
            index_name,
            auth,
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
#[pyclass(module = "pathway.engine", frozen, get_all)]
pub struct DataStorage {
    storage_type: String,
    path: Option<String>,
    rdkafka_settings: Option<HashMap<String, String>>,
    topic: Option<String>,
    connection_string: Option<String>,
    csv_parser_settings: Option<Py<CsvParserSettings>>,
    mode: ConnectorMode,
    read_method: ReadMethod,
    aws_s3_settings: Option<Py<AwsS3Settings>>,
    elasticsearch_params: Option<Py<ElasticSearchParams>>,
    parallel_readers: Option<usize>,
    python_subject: Option<Py<PythonSubject>>,
    persistent_id: Option<ExternalPersistentId>,
    max_batch_size: Option<usize>,
    object_pattern: String,
    mock_events: Option<HashMap<(ExternalPersistentId, usize), Vec<SnapshotEvent>>>,
    table_name: Option<String>,
    column_names: Option<Vec<String>>,
}

#[pyclass(module = "pathway.engine", frozen, name = "PersistenceMode")]
pub struct PyPersistenceMode(PersistenceMode);

#[pymethods]
impl PyPersistenceMode {
    #[classattr]
    pub const REALTIME_REPLAY: PersistenceMode = PersistenceMode::RealtimeReplay;
    #[classattr]
    pub const SPEEDRUN_REPLAY: PersistenceMode = PersistenceMode::SpeedrunReplay;
    #[classattr]
    pub const BATCH: PersistenceMode = PersistenceMode::Batch;
    #[classattr]
    pub const PERSISTING: PersistenceMode = PersistenceMode::Persisting;
    #[classattr]
    pub const UDF_CACHING: PersistenceMode = PersistenceMode::UdfCaching;
}

impl<'source> FromPyObject<'source> for PersistenceMode {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        Ok(ob.extract::<PyRef<PyPersistenceMode>>()?.0)
    }
}

impl IntoPy<PyObject> for PersistenceMode {
    fn into_py(self, py: Python<'_>) -> PyObject {
        PyPersistenceMode(self).into_py(py)
    }
}

#[pyclass(module = "pathway.engine", frozen, name = "SnapshotAccess")]
pub struct PySnapshotAccess(SnapshotAccess);

#[pymethods]
impl PySnapshotAccess {
    #[classattr]
    pub const REPLAY: SnapshotAccess = SnapshotAccess::Replay;
    #[classattr]
    pub const RECORD: SnapshotAccess = SnapshotAccess::Record;
    #[classattr]
    pub const FULL: SnapshotAccess = SnapshotAccess::Full;
}

impl<'source> FromPyObject<'source> for SnapshotAccess {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        Ok(ob.extract::<PyRef<PySnapshotAccess>>()?.0)
    }
}

impl IntoPy<PyObject> for SnapshotAccess {
    fn into_py(self, py: Python<'_>) -> PyObject {
        PySnapshotAccess(self).into_py(py)
    }
}

#[derive(Clone, Debug)]
#[pyclass(module = "pathway.engine", frozen)]
pub struct PersistenceConfig {
    snapshot_interval: ::std::time::Duration,
    metadata_storage: DataStorage,
    stream_storage: DataStorage,
    snapshot_access: SnapshotAccess,
    persistence_mode: PersistenceMode,
    continue_after_replay: bool,
}

#[pymethods]
impl PersistenceConfig {
    #[new]
    #[pyo3(signature = (
        *,
        snapshot_interval_ms,
        metadata_storage,
        stream_storage,
        snapshot_access = SnapshotAccess::Full,
        persistence_mode = PersistenceMode::Batch,
        continue_after_replay = true,
    ))]
    fn new(
        snapshot_interval_ms: u64,
        metadata_storage: DataStorage,
        stream_storage: DataStorage,
        snapshot_access: SnapshotAccess,
        persistence_mode: PersistenceMode,
        continue_after_replay: bool,
    ) -> Self {
        Self {
            snapshot_interval: ::std::time::Duration::from_millis(snapshot_interval_ms),
            metadata_storage,
            stream_storage,
            snapshot_access,
            persistence_mode,
            continue_after_replay,
        }
    }
}

impl PersistenceConfig {
    fn prepare(self, py: pyo3::Python) -> PyResult<PersistenceManagerOuterConfig> {
        Ok(PersistenceManagerOuterConfig::new(
            self.snapshot_interval,
            self.metadata_storage
                .construct_metadata_storage_config(py)?,
            self.stream_storage.construct_stream_storage_config(py)?,
            self.snapshot_access,
            self.persistence_mode,
            self.continue_after_replay,
        ))
    }
}

impl<'source> FromPyObject<'source> for SnapshotEvent {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        Ok(ob.extract::<PyRef<PySnapshotEvent>>()?.0.clone())
    }
}

impl IntoPy<PyObject> for SnapshotEvent {
    fn into_py(self, py: Python<'_>) -> PyObject {
        PySnapshotEvent(self).into_py(py)
    }
}

#[pyclass(module = "pathway.engine", frozen, name = "SnapshotEvent")]
pub struct PySnapshotEvent(SnapshotEvent);

#[pymethods]
impl PySnapshotEvent {
    #[staticmethod]
    pub fn insert(key: Key, values: Vec<Value>) -> SnapshotEvent {
        SnapshotEvent::Insert(key, values)
    }
    #[staticmethod]
    pub fn delete(key: Key, values: Vec<Value>) -> SnapshotEvent {
        SnapshotEvent::Delete(key, values)
    }
    #[staticmethod]
    pub fn advance_time(timestamp: u64) -> SnapshotEvent {
        SnapshotEvent::AdvanceTime(timestamp)
    }
    #[classattr]
    pub const FINISHED: SnapshotEvent = SnapshotEvent::Finished;
}

#[pyclass(module = "pathway.engine", frozen)]
#[derive(Clone)]
pub struct PythonSubject {
    pub start: Py<PyAny>,
    pub read: Py<PyAny>,
    pub end: Py<PyAny>,
    pub is_internal: bool,
    pub deletions_enabled: bool,
}

#[pymethods]
impl PythonSubject {
    #[new]
    #[pyo3(signature = (start, read, end, is_internal, deletions_enabled))]
    fn new(
        start: Py<PyAny>,
        read: Py<PyAny>,
        end: Py<PyAny>,
        is_internal: bool,
        deletions_enabled: bool,
    ) -> Self {
        Self {
            start,
            read,
            end,
            is_internal,
            deletions_enabled,
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

#[pyclass(module = "pathway.engine", frozen, get_all)]
pub struct DataFormat {
    format_type: String,
    key_field_names: Option<Vec<String>>,
    value_fields: Vec<Py<ValueField>>,
    delimiter: Option<char>,
    table_name: Option<String>,
    column_paths: Option<HashMap<String, String>>,
    field_absence_is_error: bool,
    parse_utf8: bool,
    debezium_db_type: DebeziumDBType,
    session_type: SessionType,
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
        mode = ConnectorMode::Streaming,
        read_method = ReadMethod::ByLine,
        aws_s3_settings = None,
        elasticsearch_params = None,
        parallel_readers = None,
        python_subject = None,
        persistent_id = None,
        max_batch_size = None,
        object_pattern = "*".to_string(),
        mock_events = None,
        table_name = None,
        column_names = None,
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
        read_method: ReadMethod,
        aws_s3_settings: Option<Py<AwsS3Settings>>,
        elasticsearch_params: Option<Py<ElasticSearchParams>>,
        parallel_readers: Option<usize>,
        python_subject: Option<Py<PythonSubject>>,
        persistent_id: Option<ExternalPersistentId>,
        max_batch_size: Option<usize>,
        object_pattern: String,
        mock_events: Option<HashMap<(ExternalPersistentId, usize), Vec<SnapshotEvent>>>,
        table_name: Option<String>,
        column_names: Option<Vec<String>>,
    ) -> Self {
        DataStorage {
            storage_type,
            path,
            rdkafka_settings,
            topic,
            connection_string,
            csv_parser_settings,
            mode,
            read_method,
            aws_s3_settings,
            elasticsearch_params,
            parallel_readers,
            python_subject,
            persistent_id,
            max_batch_size,
            object_pattern,
            mock_events,
            table_name,
            column_names,
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
        parse_utf8 = true,
        debezium_db_type = DebeziumDBType::Postgres,
        session_type = SessionType::Native,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        format_type: String,
        key_field_names: Option<Vec<String>>,
        value_fields: Vec<Py<ValueField>>,
        delimiter: Option<char>,
        table_name: Option<String>,
        column_paths: Option<HashMap<String, String>>,
        field_absence_is_error: bool,
        parse_utf8: bool,
        debezium_db_type: DebeziumDBType,
        session_type: SessionType,
    ) -> Self {
        DataFormat {
            format_type,
            key_field_names,
            value_fields,
            delimiter,
            table_name,
            column_paths,
            field_absence_is_error,
            parse_utf8,
            debezium_db_type,
            session_type,
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
            .ok_or_else(|| PyValueError::new_err("For fs/s3 storage, path must be specified"))?
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
        let (bucket_name, _) = AwsS3Settings::deduce_bucket_and_path(self.path()?);
        let bucket = self
            .aws_s3_settings
            .as_ref()
            .ok_or_else(|| {
                PyValueError::new_err("For AWS storage, aws_s3_settings must be specified")
            })?
            .borrow(py)
            .construct_bucket(bucket_name.as_deref())?;
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
        self.persistent_id
            .clone()
            .map(IntoPersistentId::into_persistent_id)
    }

    fn construct_reader(&self, py: pyo3::Python) -> PyResult<(Box<dyn ReaderBuilder>, usize)> {
        match self.storage_type.as_ref() {
            "fs" => {
                let storage = FilesystemReader::new(
                    self.path()?,
                    self.mode,
                    self.internal_persistent_id(),
                    self.read_method,
                    &self.object_pattern,
                )
                .map_err(|e| {
                    PyIOError::new_err(format!("Failed to initialize Filesystem reader: {e}"))
                })?;
                Ok((Box::new(storage), 1))
            }
            "s3" => {
                let (_, deduced_path) = AwsS3Settings::deduce_bucket_and_path(self.path()?);
                let storage = S3GenericReader::new(
                    self.s3_bucket(py)?,
                    deduced_path.unwrap_or(self.path()?.to_string()),
                    self.mode.is_polling_enabled(),
                    self.internal_persistent_id(),
                    self.read_method,
                )
                .map_err(|e| PyRuntimeError::new_err(format!("Creating S3 reader failed: {e}")))?;
                Ok((Box::new(storage), 1))
            }
            "s3_csv" => {
                let (_, deduced_path) = AwsS3Settings::deduce_bucket_and_path(self.path()?);
                let storage = S3CsvReader::new(
                    self.s3_bucket(py)?,
                    deduced_path.unwrap_or(self.path()?.to_string()),
                    self.build_csv_parser_settings(py),
                    self.mode.is_polling_enabled(),
                    self.internal_persistent_id(),
                )
                .map_err(|e| PyRuntimeError::new_err(format!("Creating S3 reader failed: {e}")))?;
                Ok((Box::new(storage), 1))
            }
            "csv" => {
                let reader = CsvFilesystemReader::new(
                    self.path()?,
                    self.build_csv_parser_settings(py),
                    self.mode,
                    self.internal_persistent_id(),
                    &self.object_pattern,
                )
                .map_err(|e| {
                    PyIOError::new_err(format!("Failed to initialize CsvFilesystem reader: {e}"))
                })?;
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
            "sqlite" => {
                let connection = SqliteConnection::open_with_flags(
                    self.path()?,
                    SqliteOpenFlags::SQLITE_OPEN_READ_ONLY | SqliteOpenFlags::SQLITE_OPEN_NO_MUTEX,
                )
                .map_err(|e| {
                    PyRuntimeError::new_err(format!("Failed to open Sqlite connection: {e}"))
                })?;
                let table_name = self.table_name.clone().ok_or_else(|| {
                    PyValueError::new_err("For Sqlite connector, table_name should be specified")
                })?;
                let column_names = self.column_names.clone().ok_or_else(|| {
                    PyValueError::new_err("For Sqlite connector, column_names should be specified")
                })?;
                let reader = SqliteReader::new(connection, table_name, column_names);
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
            "mock" => {
                let mut events = HashMap::<ConnectorWorkerPair, Vec<SnapshotEvent>>::new();
                for ((external_persistent_id, worker_id), es) in self.mock_events.as_ref().unwrap()
                {
                    let internal_persistent_id =
                        external_persistent_id.clone().into_persistent_id();
                    events.insert((internal_persistent_id, *worker_id), es.clone());
                }
                Ok(StreamStorageConfig::Mock(events))
            }
            other => Err(PyValueError::new_err(format!(
                "Unsupported snapshot storage format: {other:?}"
            ))),
        }
    }

    fn construct_metadata_storage_config(
        &self,
        py: pyo3::Python,
    ) -> PyResult<MetadataStorageConfig> {
        match self.storage_type.as_ref() {
            "fs" => Ok(MetadataStorageConfig::Filesystem(self.path()?.into())),
            "s3" => {
                let bucket = self.s3_bucket(py)?;
                let path = self.path()?;
                Ok(MetadataStorageConfig::S3 {
                    bucket,
                    root_path: path.into(),
                })
            }
            "mock" => Ok(MetadataStorageConfig::Mock),
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
                    Ok(client) => PsqlWriter::new(client, self.max_batch_size),
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
                let max_batch_size = self.max_batch_size;

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
                    self.debezium_db_type,
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
                    self.session_type,
                );
                Ok(Box::new(parser))
            }
            "identity" => Ok(Box::new(IdentityParser::new(
                self.value_field_names(py),
                self.parse_utf8,
                self.session_type,
            ))),
            "transparent" => Ok(Box::new(TransparentParser::new(self.value_fields.len()))),
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
pub struct ColumnProperties(Arc<EngineColumnProperties>);

#[pymethods]
impl ColumnProperties {
    #[new]
    #[pyo3(signature = (
        dtype,
        trace = None,
        append_only = false
    ))]
    fn new(
        py: Python,
        dtype: Py<PyAny>,
        trace: Option<Py<Trace>>,
        append_only: bool,
    ) -> PyResult<Py<Self>> {
        let trace = trace
            .clone()
            .map_or(Ok(EngineTrace::Empty), |t| t.extract(py))?;
        let inner = Arc::new(EngineColumnProperties {
            append_only,
            dtype: dtype.extract(py)?,
            trace,
        });
        let res = Py::new(py, Self(inner))?;
        Ok(res)
    }
}

#[pyclass(module = "pathway.engine", frozen, subclass)]
#[derive(Clone)]
pub struct TableProperties(Arc<EngineTableProperties>);

impl TableProperties {
    fn new(py: Python, inner: Arc<EngineTableProperties>) -> PyResult<Py<Self>> {
        let res = Py::new(py, Self(inner))?;
        Ok(res)
    }
}

#[pymethods]
impl TableProperties {
    #[staticmethod]
    fn column(py: Python, column_properties: ColumnProperties) -> PyResult<Py<Self>> {
        let inner = Arc::new(EngineTableProperties::Column(column_properties.0));
        TableProperties::new(py, inner)
    }

    #[staticmethod]
    fn from_column_properties(
        py: Python,
        #[pyo3(from_py_with = "from_py_iterable")] column_properties: Vec<(
            ColumnPath,
            ColumnProperties,
        )>,
    ) -> PyResult<Py<Self>> {
        let column_properties: Vec<_> = column_properties
            .into_iter()
            .map(|(path, props)| (path, props.0))
            .collect();

        let table_properties = EngineTableProperties::from_paths(
            column_properties
                .into_iter()
                .map(|(path, column_properties)| {
                    (path, EngineTableProperties::Column(column_properties))
                })
                .collect(),
        )?;

        TableProperties::new(py, Arc::new(table_properties))
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
    #[pyo3(get)]
    column_properties: Vec<ColumnProperties>,
}

#[pymethods]
impl ConnectorProperties {
    #[new]
    #[pyo3(signature = (
        commit_duration_ms = None,
        unsafe_trusted_ids = false,
        column_properties = vec![]
    ))]
    fn new(
        commit_duration_ms: Option<u64>,
        unsafe_trusted_ids: bool,
        #[pyo3(from_py_with = "from_py_iterable")] column_properties: Vec<ColumnProperties>,
    ) -> Self {
        Self {
            commit_duration_ms,
            unsafe_trusted_ids,
            column_properties,
        }
    }
}

impl ConnectorProperties {
    fn column_properties(&self) -> Vec<Arc<EngineColumnProperties>> {
        self.column_properties.iter().map(|p| p.0.clone()).collect()
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

    m.add_class::<Pointer>()?;
    m.add_class::<PyReducer>()?;
    m.add_class::<PyUnaryOperator>()?;
    m.add_class::<PyBinaryOperator>()?;
    m.add_class::<PyExpression>()?;
    m.add_class::<PathwayType>()?;
    m.add_class::<PyConnectorMode>()?;
    m.add_class::<PySessionType>()?;
    m.add_class::<PyDataEventType>()?;
    m.add_class::<PyDebeziumDBType>()?;
    m.add_class::<PyReadMethod>()?;
    m.add_class::<PyMonitoringLevel>()?;
    m.add_class::<Universe>()?;
    m.add_class::<Column>()?;
    m.add_class::<LegacyTable>()?;
    m.add_class::<Table>()?;
    m.add_class::<DataRow>()?;
    m.add_class::<Computer>()?;
    m.add_class::<Scope>()?;
    m.add_class::<Context>()?;

    m.add_class::<AwsS3Settings>()?;
    m.add_class::<ElasticSearchParams>()?;
    m.add_class::<ElasticSearchAuth>()?;
    m.add_class::<CsvParserSettings>()?;
    m.add_class::<ValueField>()?;
    m.add_class::<DataStorage>()?;
    m.add_class::<DataFormat>()?;
    m.add_class::<PersistenceConfig>()?;
    m.add_class::<PythonSubject>()?;
    m.add_class::<PyPersistenceMode>()?;
    m.add_class::<PySnapshotAccess>()?;
    m.add_class::<PySnapshotEvent>()?;

    m.add_class::<ConnectorProperties>()?;
    m.add_class::<ColumnProperties>()?;
    m.add_class::<TableProperties>()?;
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
