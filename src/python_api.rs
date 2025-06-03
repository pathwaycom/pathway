// Copyright © 2024 Pathway

// `PyRef`s need to be passed by value
#![allow(clippy::needless_pass_by_value)]

use crate::async_runtime::create_async_tokio_runtime;
use crate::engine::graph::{
    ErrorLogHandle, ExportedTable, OperatorProperties, SubscribeCallbacks,
    SubscribeCallbacksBuilder, SubscribeConfig,
};
use crate::engine::license::{Error as LicenseError, License};
use crate::engine::{
    Computer as EngineComputer, Expressions, PyObjectWrapper as InternalPyObjectWrapper,
    ShardPolicy, TotalFrontier,
};
use crate::persistence::frontier::OffsetAntichain;

use async_nats::connect as nats_connect;
use async_nats::Client as NatsClient;
use async_nats::Subscriber as NatsSubscriber;
use azure_storage::StorageCredentials as AzureStorageCredentials;
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
use log::{info, warn};
use mongodb::sync::Client as MongoClient;
use ndarray;
use numpy::{PyArray, PyReadonlyArrayDyn};
use once_cell::sync::Lazy;
use postgres::{Client, NoTls};
use pyo3::exceptions::{
    PyBaseException, PyException, PyIOError, PyIndexError, PyKeyError, PyNotImplementedError,
    PyRuntimeError, PyTypeError, PyValueError, PyZeroDivisionError,
};
use pyo3::prelude::*;
use pyo3::pyclass::CompareOp;
use pyo3::sync::GILOnceCell;
use pyo3::types::{PyBool, PyBytes, PyDict, PyFloat, PyInt, PyString, PyTuple, PyType};
use pyo3::{intern, AsPyPointer, PyTypeInfo};
use pyo3_log::ResetHandle;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::producer::{DefaultProducerContext, ThreadedProducer};
use rdkafka::{ClientConfig, Offset as KafkaOffset, TopicPartitionList};
use rusqlite::Connection as SqliteConnection;
use rusqlite::OpenFlags as SqliteOpenFlags;
use s3::bucket::Bucket as S3Bucket;
use scopeguard::defer;
use send_wrapper::SendWrapper;
use serde_json::Value as JsonValue;
use std::borrow::Borrow;
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

use self::external_index_wrappers::{
    PyBruteForceKnnMetricKind, PyExternalIndexData, PyExternalIndexQuery, PyUSearchMetricKind,
};
use self::threads::PythonThreadState;

use crate::connectors::data_format::{
    BsonFormatter, DebeziumDBType, DebeziumMessageParser, DsvSettings, Formatter,
    IdentityFormatter, IdentityParser, InnerSchemaField, JsonLinesFormatter, JsonLinesParser,
    KeyGenerationPolicy, NullFormatter, Parser, PsqlSnapshotFormatter, PsqlUpdatesFormatter,
    SingleColumnFormatter, TransparentParser,
};
use crate::connectors::data_lake::arrow::construct_schema as construct_arrow_schema;
use crate::connectors::data_lake::buffering::{
    AppendOnlyColumnBuffer, ColumnBuffer, SnapshotColumnBuffer,
};
use crate::connectors::data_lake::iceberg::{
    IcebergBatchWriter, IcebergDBParams, IcebergTableParams,
};
use crate::connectors::data_lake::{DeltaBatchWriter, MaintenanceMode};
use crate::connectors::data_storage::{
    new_csv_filesystem_reader, new_filesystem_reader, new_s3_csv_reader, new_s3_generic_reader,
    ConnectorMode, DeltaTableReader, ElasticSearchWriter, FileWriter, IcebergReader, KafkaReader,
    KafkaWriter, LakeWriter, MessageQueueTopic, MongoWriter, NatsReader, NatsWriter, NullWriter,
    ObjectDownloader, PsqlWriter, PythonConnectorEventType, PythonReaderBuilder, RdkafkaWatermark,
    ReadError, ReadMethod, ReaderBuilder, SqlWriterInitMode, SqliteReader, WriteError, Writer,
};
use crate::connectors::scanner::S3Scanner;
use crate::connectors::synchronization::ConnectorGroupDescriptor;
use crate::connectors::{PersistenceMode, SessionType, SnapshotAccess};
use crate::engine::dataflow::Config;
use crate::engine::error::{DataError, DynError, DynResult, Trace as EngineTrace};
use crate::engine::graph::ScopedContext;
use crate::engine::progress_reporter::MonitoringLevel;
use crate::engine::reduce::StatefulCombineFn;
use crate::engine::time::DateTime;
use crate::engine::Config as EngineTelemetryConfig;
use crate::engine::Timestamp;
use crate::engine::{
    run_with_new_dataflow_graph, BatchWrapper, ColumnHandle, ColumnPath,
    ColumnProperties as EngineColumnProperties, DataRow, DateTimeNaive, DateTimeUtc, Duration,
    ExpressionData, IxKeyPolicy, JoinData, JoinType, Key, KeyImpl, PointerExpression, Reducer,
    ReducerData, ScopedGraph, TableHandle, TableProperties as EngineTableProperties, Type,
    UniverseHandle, Value,
};
use crate::engine::{AnyExpression, Context as EngineContext};
use crate::engine::{BoolExpression, Error as EngineError};
use crate::engine::{ComplexColumn as EngineComplexColumn, WakeupReceiver};
use crate::engine::{DateTimeNaiveExpression, DateTimeUtcExpression, DurationExpression};
use crate::engine::{Expression, IntExpression};
use crate::engine::{FloatExpression, Graph};
use crate::engine::{LegacyTable as EngineLegacyTable, StringExpression};
use crate::persistence::config::{
    ConnectorWorkerPair, PersistenceManagerOuterConfig, PersistentStorageConfig,
};
use crate::persistence::input_snapshot::Event as SnapshotEvent;
use crate::persistence::{IntoPersistentId, UniqueName};
use crate::pipe::{pipe, ReaderType, WriterType};
use crate::python_api::external_index_wrappers::PyExternalIndexFactory;
use crate::timestamp::current_unix_timestamp_ms;

use s3::creds::Credentials as AwsCredentials;

mod external_index_wrappers;
mod logging;
pub mod threads;

static CONVERT: GILOnceCell<Py<PyModule>> = GILOnceCell::new();

fn get_convert_python_module(py: Python<'_>) -> &Bound<'_, PyModule> {
    CONVERT
        .get_or_init(py, || {
            PyModule::import_bound(py, "pathway.internals.utils.convert")
                .unwrap()
                .unbind()
        })
        .bind(py)
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

impl<'py> FromPyObject<'py> for Key {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
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

fn value_from_python_datetime(ob: &Bound<PyAny>) -> PyResult<Value> {
    let py = ob.py();
    let (timestamp_ns, is_tz_aware) = get_convert_python_module(py)
        .call_method1(intern!(py, "_datetime_to_rust"), (ob,))?
        .extract::<(i64, bool)>()?;
    if is_tz_aware {
        Ok(Value::DateTimeUtc(DateTimeUtc::new(timestamp_ns)))
    } else {
        Ok(Value::DateTimeNaive(DateTimeNaive::new(timestamp_ns)))
    }
}

fn value_from_python_timedelta(ob: &Bound<PyAny>) -> PyResult<Value> {
    let py = ob.py();
    let duration_ns = get_convert_python_module(py)
        .call_method1(intern!(py, "_timedelta_to_rust"), (ob,))?
        .extract::<i64>()?;
    Ok(Value::Duration(Duration::new(duration_ns)))
}

fn value_from_pandas_timestamp(ob: &Bound<PyAny>) -> PyResult<Value> {
    let py = ob.py();
    let (timestamp, is_tz_aware) = get_convert_python_module(py)
        .call_method1(intern!(py, "_pd_timestamp_to_rust"), (ob,))?
        .extract::<(i64, bool)>()?;
    if is_tz_aware {
        Ok(Value::DateTimeUtc(DateTimeUtc::new(timestamp)))
    } else {
        Ok(Value::DateTimeNaive(DateTimeNaive::new(timestamp)))
    }
}

fn value_from_pandas_timedelta(ob: &Bound<PyAny>) -> PyResult<Value> {
    let py = ob.py();
    let duration = get_convert_python_module(py)
        .call_method1(intern!(py, "_pd_timedelta_to_rust"), (ob,))?
        .extract::<i64>()?;
    Ok(Value::Duration(Duration::new(duration)))
}

fn value_json_from_py_any(ob: &Bound<PyAny>) -> PyResult<Value> {
    let py = ob.py();
    let json_str = get_convert_python_module(py).call_method1(intern!(py, "_json_dumps"), (ob,))?;
    let json_str = json_str.downcast::<PyString>()?.to_str()?;
    let json: JsonValue =
        serde_json::from_str(json_str).map_err(|_| PyValueError::new_err("malformed json"))?;
    Ok(Value::from(json))
}

impl ToPyObject for DateTimeNaive {
    fn to_object(&self, py: Python<'_>) -> PyObject {
        get_convert_python_module(py)
            .call_method1(
                intern!(py, "_pd_timestamp_from_naive_ns"),
                (self.timestamp(),),
            )
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
            .call_method1(
                intern!(py, "_pd_timestamp_from_utc_ns"),
                (self.timestamp(),),
            )
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
            .call_method1(intern!(py, "_pd_timedelta_from_ns"), (self.nanoseconds(),))
            .unwrap()
            .into_py(py)
    }
}

impl IntoPy<PyObject> for Duration {
    fn into_py(self, py: Python<'_>) -> PyObject {
        self.to_object(py)
    }
}

fn is_pathway_json(ob: &Bound<PyAny>) -> PyResult<bool> {
    let type_name = ob.get_type().qualname()?;
    Ok(type_name == "Json")
}

fn array_with_proper_dimensions<T>(
    array: ndarray::ArrayD<T>,
    dim: Option<usize>,
) -> Option<ndarray::ArrayD<T>> {
    match dim {
        Some(dim) if array.ndim() == dim => Some(array),
        Some(_) => None,
        None => Some(array),
    }
}

fn extract_datetime(ob: &Bound<PyAny>, type_: &Type) -> PyResult<Value> {
    assert!(matches!(type_, Type::DateTimeNaive | Type::DateTimeUtc));
    let type_name = ob.get_type().qualname()?;
    let value = if type_name == "datetime" {
        value_from_python_datetime(ob)
    } else if matches!(
        type_name.as_ref(),
        "Timestamp" | "DateTimeNaive" | "DateTimeUtc"
    ) {
        value_from_pandas_timestamp(ob)
    } else {
        Err(PyValueError::new_err(format!(
            "cannot convert {type_name} to DateTime"
        )))
    }?;
    match (&value, type_) {
        (Value::DateTimeNaive(_), Type::DateTimeNaive)
        | (Value::DateTimeUtc(_), Type::DateTimeUtc) => Ok(value),
        (Value::DateTimeNaive(_), Type::DateTimeUtc) => Err(PyValueError::new_err(
            "cannot create DateTimeUtc from a datetime without timezone information. Pass a datetime with timezone information or change the type to DateTimeNaive",
        )),
        (Value::DateTimeUtc(_), Type::DateTimeNaive) => Err(PyValueError::new_err(
            "cannot create DateTimeNaive from a datetime with timezone information. Pass a datetime without timezone information or change the type to DateTimeUtc",
        )),
        _ => unreachable!("value_from_python_datetime and value_from_pandas_timestamp return only DateTimeNaive or DateTimeUtc"),
    }
}

fn extract_int_array(ob: &Bound<PyAny>, dim: Option<usize>) -> Option<ndarray::ArrayD<i64>> {
    let array = if let Ok(array) = ob.extract::<PyReadonlyArrayDyn<i64>>() {
        Some(array.as_array().to_owned())
    } else if let Ok(array) = ob.extract::<PyReadonlyArrayDyn<i32>>() {
        Some(array.as_array().mapv(i64::from))
    } else if let Ok(array) = ob.extract::<PyReadonlyArrayDyn<u32>>() {
        Some(array.as_array().mapv(i64::from))
    } else {
        None
    }?;
    array_with_proper_dimensions(array, dim)
}

#[allow(clippy::cast_precision_loss)]
fn extract_float_array(ob: &Bound<PyAny>, dim: Option<usize>) -> Option<ndarray::ArrayD<f64>> {
    let array = if let Ok(array) = ob.extract::<PyReadonlyArrayDyn<f64>>() {
        array.as_array().to_owned()
    } else if let Ok(array) = ob.extract::<PyReadonlyArrayDyn<f32>>() {
        array.as_array().mapv(f64::from)
    } else {
        extract_int_array(ob, dim).map(|array| array.mapv(|v| v as f64))?
    };
    array_with_proper_dimensions(array, dim)
}

fn py_type_error(ob: &Bound<PyAny>, type_: &Type) -> PyErr {
    PyTypeError::new_err(format!(
        "cannot create an object of type {type_:?} from value {ob}"
    ))
}

pub fn extract_value(ob: &Bound<PyAny>, type_: &Type) -> PyResult<Value> {
    if ob.is_instance_of::<Error>() {
        return Ok(Value::Error);
    }
    let extracted = match type_ {
        Type::Any => ob.extract().ok(),
        Type::Optional(arg) => {
            if ob.is_none() {
                Some(Value::None)
            } else {
                Some(extract_value(ob, arg)?)
            }
        }
        Type::Bool => ob
            .extract::<&PyBool>()
            .ok()
            .map(|b| Value::from(b.is_true())),
        Type::Int => ob.extract::<i64>().ok().map(Value::from),
        Type::Float => ob.extract::<f64>().ok().map(Value::from),
        Type::Pointer => ob.extract::<Key>().ok().map(Value::from),
        Type::String => ob
            .downcast::<PyString>()
            .ok()
            .and_then(|s| s.to_str().ok())
            .map(Value::from),
        Type::Bytes => ob
            .downcast::<PyBytes>()
            .ok()
            .map(|b| Value::from(b.as_bytes())),
        Type::DateTimeNaive | Type::DateTimeUtc => Some(extract_datetime(ob, type_)?),
        Type::Duration => {
            // XXX: check types, not names
            let type_name = ob.get_type().qualname()?;
            if type_name == "timedelta" {
                value_from_python_timedelta(ob).ok()
            } else if matches!(type_name.as_ref(), "Timedelta" | "Duration") {
                value_from_pandas_timedelta(ob).ok()
            } else {
                None
            }
        }
        Type::Array(dim, wrapped) => match wrapped.borrow() {
            Type::Int => Ok(extract_int_array(ob, *dim).map(Value::from)),
            Type::Float => Ok(extract_float_array(ob, *dim).map(Value::from)),
            Type::Any => Ok(extract_int_array(ob, *dim)
                .map(Value::from)
                .or_else(|| extract_float_array(ob, *dim).map(Value::from))),
            wrapped => Err(PyValueError::new_err(format!(
                "{wrapped:?} is invalid type for Array"
            ))),
        }?,
        Type::Json => {
            if is_pathway_json(ob)? {
                value_json_from_py_any(&ob.getattr("value")?).ok()
            } else {
                value_json_from_py_any(ob).ok()
            }
        }
        Type::Tuple(args) => {
            let obs = ob.extract::<Vec<Bound<PyAny>>>()?;
            if obs.len() == args.len() {
                let values: Vec<_> = obs
                    .into_iter()
                    .zip(args.iter())
                    .map(|(ob, type_)| extract_value(&ob, type_))
                    .try_collect()?;
                Some(Value::from(values.as_slice()))
            } else {
                None
            }
        }
        Type::List(arg) => {
            let obs = ob.extract::<Vec<Bound<PyAny>>>()?;
            let values: Vec<_> = obs
                .into_iter()
                .map(|ob| extract_value(&ob, arg))
                .try_collect()?;
            Some(Value::from(values.as_slice()))
        }
        Type::PyObjectWrapper => {
            let value = if let Ok(ob) = ob.extract::<PyObjectWrapper>() {
                ob
            } else {
                PyObjectWrapper::new(ob.clone().unbind())
            };
            Some(Value::from(value.into_internal()))
        }
        Type::Future(arg) => {
            if ob.is_instance_of::<Pending>() {
                Some(Value::Pending)
            } else {
                Some(extract_value(ob, arg)?)
            }
        }
    };
    extracted.ok_or_else(|| py_type_error(ob, type_))
}

impl<'py> FromPyObject<'py> for Value {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let py = ob.py();
        if ob.is_none() {
            Ok(Value::None)
        } else if ob.is_exact_instance_of::<Error>() {
            Ok(Value::Error)
        } else if ob.is_exact_instance_of::<Pending>() {
            Ok(Value::Pending)
        } else if let Ok(s) = ob.downcast_exact::<PyString>() {
            Ok(Value::from(s.to_str()?))
        } else if let Ok(b) = ob.downcast_exact::<PyBytes>() {
            Ok(Value::from(b.as_bytes()))
        } else if ob.is_exact_instance_of::<PyInt>() {
            Ok(Value::Int(
                ob.extract::<i64>()
                    .expect("type conversion should work for int"),
            ))
        } else if ob.is_exact_instance_of::<PyFloat>() {
            Ok(Value::Float(
                ob.extract::<f64>()
                    .expect("type conversion should work for float")
                    .into(),
            ))
        } else if let Ok(b) = ob.downcast_exact::<PyBool>() {
            Ok(Value::Bool(b.is_true()))
        } else if ob.is_exact_instance_of::<Pointer>() {
            Ok(Value::Pointer(
                ob.extract::<Key>()
                    .expect("type conversion should work for Key"),
            ))
        } else if is_pathway_json(ob)? {
            value_json_from_py_any(&ob.getattr(intern!(py, "value"))?)
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
        } else if let Ok(ob) = ob.extract::<PyObjectWrapper>() {
            Ok(Value::from(ob.into_internal()))
        } else {
            // XXX: check types, not names
            let type_name = ob.get_type().qualname()?;
            if type_name == "datetime" {
                return value_from_python_datetime(ob);
            } else if type_name == "timedelta" {
                return value_from_python_timedelta(ob);
            } else if matches!(&*type_name, "Timestamp" | "DateTimeNaive" | "DateTimeUtc") {
                return value_from_pandas_timestamp(ob);
            } else if matches!(&*type_name, "Timedelta" | "Duration") {
                return value_from_pandas_timedelta(ob);
            }

            if let Ok(vec) = ob.extract::<Vec<Bound<PyAny>>>() {
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
        .call_method1(intern!(py, "_parse_to_json"), (json.to_string(),))
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
            Self::Bytes(b) => PyBytes::new_bound(py, b).unbind().into_any(),
            Self::Tuple(t) => PyTuple::new_bound(py, t.iter()).unbind().into_any(),
            Self::IntArray(a) => PyArray::from_array_bound(py, a).unbind().into_any(),
            Self::FloatArray(a) => PyArray::from_array_bound(py, a).unbind().into_any(),
            Self::DateTimeNaive(dt) => dt.into_py(py),
            Self::DateTimeUtc(dt) => dt.into_py(py),
            Self::Duration(d) => d.into_py(py),
            Self::Json(j) => json_to_py_object(py, j),
            Self::Error => ERROR.clone_ref(py).into_py(py),
            Self::PyObjectWrapper(op) => PyObjectWrapper::from_internal(py, op).into_py(py),
            Self::Pending => PENDING.clone_ref(py).into_py(py),
        }
    }
}

impl IntoPy<PyObject> for Value {
    fn into_py(self, py: Python<'_>) -> PyObject {
        self.to_object(py)
    }
}

impl IntoPy<PyObject> for &Value {
    fn into_py(self, py: Python<'_>) -> PyObject {
        self.to_object(py)
    }
}

impl<'py> FromPyObject<'py> for Reducer {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        Ok(ob.extract::<PyRef<PyReducer>>()?.0.clone())
    }
}

impl IntoPy<PyObject> for Reducer {
    fn into_py(self, py: Python<'_>) -> PyObject {
        PyReducer(self).into_py(py)
    }
}

impl<'py> FromPyObject<'py> for Type {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        Ok(ob.extract::<PyRef<PathwayType>>()?.0.clone())
    }
}

impl IntoPy<PyObject> for Type {
    fn into_py(self, py: Python<'_>) -> PyObject {
        PathwayType(self).into_py(py)
    }
}

impl<'py> FromPyObject<'py> for ReadMethod {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        Ok(ob.extract::<PyRef<PyReadMethod>>()?.0)
    }
}

impl IntoPy<PyObject> for ReadMethod {
    fn into_py(self, py: Python<'_>) -> PyObject {
        PyReadMethod(self).into_py(py)
    }
}

impl<'py> FromPyObject<'py> for ConnectorMode {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        Ok(ob.extract::<PyRef<PyConnectorMode>>()?.0)
    }
}

impl IntoPy<PyObject> for ConnectorMode {
    fn into_py(self, py: Python<'_>) -> PyObject {
        PyConnectorMode(self).into_py(py)
    }
}

impl<'py> FromPyObject<'py> for SessionType {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        Ok(ob.extract::<PyRef<PySessionType>>()?.0)
    }
}

impl IntoPy<PyObject> for SessionType {
    fn into_py(self, py: Python<'_>) -> PyObject {
        PySessionType(self).into_py(py)
    }
}

impl<'py> FromPyObject<'py> for PythonConnectorEventType {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        Ok(ob.extract::<PyRef<PyPythonConnectorEventType>>()?.0)
    }
}

impl IntoPy<PyObject> for PythonConnectorEventType {
    fn into_py(self, py: Python<'_>) -> PyObject {
        PyPythonConnectorEventType(self).into_py(py)
    }
}

impl<'py> FromPyObject<'py> for DebeziumDBType {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        Ok(ob.extract::<PyRef<PyDebeziumDBType>>()?.0)
    }
}

impl IntoPy<PyObject> for DebeziumDBType {
    fn into_py(self, py: Python<'_>) -> PyObject {
        PyDebeziumDBType(self).into_py(py)
    }
}

impl<'py> FromPyObject<'py> for KeyGenerationPolicy {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        Ok(ob.extract::<PyRef<PyKeyGenerationPolicy>>()?.0)
    }
}

impl IntoPy<PyObject> for KeyGenerationPolicy {
    fn into_py(self, py: Python<'_>) -> PyObject {
        PyKeyGenerationPolicy(self).into_py(py)
    }
}

impl<'py> FromPyObject<'py> for MonitoringLevel {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        Ok(ob.extract::<PyRef<PyMonitoringLevel>>()?.0)
    }
}

impl IntoPy<PyObject> for MonitoringLevel {
    fn into_py(self, py: Python<'_>) -> PyObject {
        PyMonitoringLevel(self).into_py(py)
    }
}

impl<'py> FromPyObject<'py> for SqlWriterInitMode {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        Ok(ob.extract::<PyRef<PySqlWriterInitMode>>()?.0)
    }
}

impl IntoPy<PyObject> for SqlWriterInitMode {
    fn into_py(self, py: Python<'_>) -> PyObject {
        PySqlWriterInitMode(self).into_py(py)
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
                return PyErr::from_type_bound(ENGINE_ERROR_WITH_TRACE_TYPE.bind(py).clone(), args);
            }
            let exception_type = match error {
                EngineError::DataError(ref error) => match error {
                    DataError::TypeMismatch { .. } => PyTypeError::type_object_bound(py),
                    DataError::DuplicateKey(_)
                    | DataError::ValueMissing
                    | DataError::KeyMissingInOutputTable(_)
                    | DataError::KeyMissingInInputTable(_) => PyKeyError::type_object_bound(py),
                    DataError::DivisionByZero => PyZeroDivisionError::type_object_bound(py),
                    DataError::ParseError(_) | DataError::ValueError(_) => {
                        PyValueError::type_object_bound(py)
                    }
                    DataError::IndexOutOfBounds => PyIndexError::type_object_bound(py),
                    _ => ENGINE_ERROR_TYPE.bind(py).clone(),
                },
                EngineError::IterationLimitTooSmall
                | EngineError::InconsistentColumnProperties
                | EngineError::IdInTableProperties => PyValueError::type_object_bound(py),
                EngineError::ReaderFailed(ReadError::Py(e)) => return e,
                EngineError::OtherWorkerPanic => OTHER_WORKER_ERROR.bind(py).clone(),
                _ => ENGINE_ERROR_TYPE.bind(py).clone(),
            };
            let message = error.to_string();
            PyErr::from_type_bound(exception_type, message)
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

fn from_py_iterable<'py, T>(iterable: &Bound<'py, PyAny>) -> PyResult<Vec<T>>
where
    T: FromPyObject<'py>,
{
    iterable.iter()?.map(|obj| obj?.extract()).collect()
}

fn engine_tables_from_py_iterable(iterable: &Bound<PyAny>) -> PyResult<Vec<EngineLegacyTable>> {
    iterable
        .iter()?
        .map(|table| {
            let table: PyRef<LegacyTable> = table?.extract()?;
            Ok(table.to_engine())
        })
        .collect()
}

pub fn generic_alias_class_getitem<'py>(
    cls: &Bound<'py, PyType>,
    item: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyAny>> {
    static GENERIC_ALIAS: GILOnceCell<Py<PyAny>> = GILOnceCell::new();

    let py = cls.py();
    GENERIC_ALIAS
        .get_or_try_init(py, || -> PyResult<_> {
            Ok(py.import_bound("types")?.getattr("GenericAlias")?.unbind())
        })?
        .bind(py)
        .call1((cls, item))
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

    pub fn __richcmp__(&self, other: &Bound<PyAny>, op: CompareOp) -> Py<PyAny> {
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
        cls: &Bound<'py, PyType>,
        item: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        generic_alias_class_getitem(cls, item)
    }
}

#[pyclass(module = "pathway.engine", frozen)]
#[derive(Clone)]
struct PyObjectWrapper {
    #[pyo3(get)]
    value: PyObject,
    serializer: Option<PyObject>,
}

#[pymethods]
impl PyObjectWrapper {
    #[new]
    #[pyo3(signature = (value))]
    fn new(value: PyObject) -> Self {
        Self {
            value,
            serializer: None,
        }
    }

    #[staticmethod]
    #[pyo3(signature = (value, *, serializer=None), name="_create_with_serializer")]
    fn create_with_serializer(value: PyObject, serializer: Option<PyObject>) -> Self {
        Self { value, serializer }
    }

    pub fn __repr__(&self) -> String {
        format!("PyObjectWrapper({})", self.value)
    }

    #[classmethod]
    pub fn __class_getitem__<'py>(
        cls: &Bound<'py, PyType>,
        item: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        generic_alias_class_getitem(cls, item)
    }

    fn __getnewargs__(&self) -> (PyObject,) {
        (self.value.clone(),)
    }
}

impl PyObjectWrapper {
    fn into_internal(self) -> InternalPyObjectWrapper {
        InternalPyObjectWrapper::new(self.value, self.serializer)
    }

    fn from_internal(py: Python<'_>, ob: &InternalPyObjectWrapper) -> Self {
        Self::create_with_serializer(ob.get_inner(py), ob.get_serializer(py))
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
        Reducer::Stateful {
            combine_fn: wrap_stateful_combine(combine),
        }
    }

    #[classattr]
    pub const LATEST: Reducer = Reducer::Latest;

    #[classattr]
    pub const EARLIEST: Reducer = Reducer::Earliest;
}

fn wrap_stateful_combine(combine: Py<PyAny>) -> StatefulCombineFn {
    Arc::new(move |state, values| {
        Python::with_gil(|py| Ok(combine.bind(py).call1((state, values))?.extract()?))
    })
}

#[pyclass(module = "pathway.engine", frozen, name = "ConnectorGroupDescriptor")]
struct PyConnectorGroupDescriptor(ConnectorGroupDescriptor);

#[pymethods]
impl PyConnectorGroupDescriptor {
    #[new]
    fn new(name: String, column_index: usize, max_difference: Value) -> Self {
        Self(ConnectorGroupDescriptor {
            name,
            column_index,
            max_difference,
        })
    }
}

impl<'py> FromPyObject<'py> for ConnectorGroupDescriptor {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        Ok(ob.extract::<PyRef<PyConnectorGroupDescriptor>>()?.0.clone())
    }
}

impl IntoPy<PyObject> for ConnectorGroupDescriptor {
    fn into_py(self, py: Python<'_>) -> PyObject {
        PyConnectorGroupDescriptor(self).into_py(py)
    }
}

#[pyclass(module = "pathway.engine", frozen, name = "ReducerData")]
struct PyReducerData(ReducerData);

#[pymethods]
impl PyReducerData {
    #[new]
    fn new(
        reducer: Reducer,
        skip_errors: bool,
        column_paths: Vec<ColumnPath>,
        trace: Option<EngineTrace>,
    ) -> Self {
        Self(ReducerData {
            reducer,
            skip_errors,
            column_paths,
            trace: trace.unwrap_or(EngineTrace::Empty),
        })
    }
}

impl<'py> FromPyObject<'py> for ReducerData {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        Ok(ob.extract::<PyRef<PyReducerData>>()?.0.clone())
    }
}

#[pyclass(module = "pathway.engine", frozen, name = "ExpressionData")]
struct PyExpressionData(ExpressionData);

#[pymethods]
impl PyExpressionData {
    #[new]
    fn new(
        expression: &PyExpression,
        properties: TableProperties,
        append_only: bool,
        deterministic: bool,
    ) -> Self {
        Self(ExpressionData {
            expression: expression.inner.clone(),
            properties: properties.0,
            append_only,
            deterministic,
            gil: expression.gil,
        })
    }
}

impl<'py> FromPyObject<'py> for ExpressionData {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        Ok(ob.extract::<PyRef<PyExpressionData>>()?.0.clone())
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

impl<'py> FromPyObject<'py> for UnaryOperator {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
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

impl<'py> FromPyObject<'py> for BinaryOperator {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
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
    fn r#const(ob: &Bound<PyAny>, type_: Type) -> PyResult<Self> {
        let value = extract_value(ob, &type_)?;
        Ok(Self::new(Arc::new(Expression::new_const(value)), false))
    }

    #[staticmethod]
    fn argument(index: usize) -> Self {
        Self::new(
            Arc::new(Expression::Any(AnyExpression::Argument(index))),
            false,
        )
    }

    #[staticmethod]
    #[pyo3(signature = (function, *args, dtype, propagate_none=false))]
    fn apply(
        function: Py<PyAny>,
        args: Vec<PyRef<PyExpression>>,
        dtype: Type,
        propagate_none: bool,
    ) -> Self {
        let args = args
            .into_iter()
            .map(|expr| expr.inner.clone())
            .collect_vec();
        let func = Box::new(move |input: &[Value]| {
            Python::with_gil(|py| -> DynResult<Value> {
                let args = PyTuple::new_bound(py, input);
                let result = function.call1(py, args)?;
                Ok(extract_value(result.bind(py), &dtype)?)
            })
        });
        let expression = if propagate_none {
            AnyExpression::OptionalApply(func, args.into())
        } else {
            AnyExpression::Apply(func, args.into())
        };
        Self::new(Arc::new(Expression::Any(expression)), true)
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
            (Op::MatMul, Tp::Array(_, _), Tp::Array(_, _)) => {
                Some(binary_op!(AnyE::MatMul, lhs, rhs))
            }
            (Op::Eq, Tp::Tuple(_) | Tp::List(_), Tp::Tuple(_) | Tp::List(_)) => {
                Some(binary_op!(BoolE::TupleEq, lhs, rhs))
            }
            (Op::Ne, Tp::Tuple(_) | Tp::List(_), Tp::Tuple(_) | Tp::List(_)) => {
                Some(binary_op!(BoolE::TupleNe, lhs, rhs))
            }
            (Op::Lt, Tp::Tuple(_) | Tp::List(_), Tp::Tuple(_) | Tp::List(_)) => {
                Some(binary_op!(BoolE::TupleLt, lhs, rhs))
            }
            (Op::Le, Tp::Tuple(_) | Tp::List(_), Tp::Tuple(_) | Tp::List(_)) => {
                Some(binary_op!(BoolE::TupleLe, lhs, rhs))
            }
            (Op::Gt, Tp::Tuple(_) | Tp::List(_), Tp::Tuple(_) | Tp::List(_)) => {
                Some(binary_op!(BoolE::TupleGt, lhs, rhs))
            }
            (Op::Ge, Tp::Tuple(_) | Tp::List(_), Tp::Tuple(_) | Tp::List(_)) => {
                Some(binary_op!(BoolE::TupleGe, lhs, rhs))
            }
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
    fn convert(
        expr: &PyExpression,
        default: &PyExpression,
        source_type: Type,
        target_type: Type,
        unwrap: bool,
    ) -> Option<Self> {
        type Tp = Type;
        match (&source_type, &target_type) {
            (Tp::Json, Tp::Int | Tp::Float | Tp::Bool | Tp::String) => Some(Self::new(
                Arc::new(Expression::Any(AnyExpression::JsonToValue(
                    expr.inner.clone(),
                    default.inner.clone(),
                    target_type,
                    unwrap,
                ))),
                expr.gil || default.gil,
            )),
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
    #[pyo3(signature = (*args, optional = false, instance = None))]
    fn pointer_from(
        args: Vec<PyRef<PyExpression>>,
        optional: bool,
        instance: Option<&PyExpression>,
    ) -> Self {
        let gil = args.iter().any(|a| a.gil);
        let args = args
            .into_iter()
            .map(|expr| expr.inner.clone())
            .collect_vec();
        let expr = match (optional, instance) {
            (false, None) => Arc::new(Expression::Pointer(PointerExpression::PointerFrom(
                args.into(),
            ))),
            (false, Some(instance)) => Arc::new(Expression::Pointer(
                PointerExpression::PointerWithInstanceFrom(args.into(), instance.inner.clone()),
            )),
            (true, None) => Arc::new(Expression::Any(AnyExpression::OptionalPointerFrom(
                args.into(),
            ))),
            (true, Some(instance)) => Arc::new(Expression::Any(
                AnyExpression::OptionalPointerWithInstanceFrom(args.into(), instance.inner.clone()),
            )),
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
unary_expr!(int_abs, IntExpression::Abs);
unary_expr!(float_abs, FloatExpression::Abs);
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
binary_expr!(to_duration, DurationExpression::FromTimeUnit);
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
binary_expr!(fill_error, AnyExpression::FillError);

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
    #[staticmethod]
    #[pyo3(signature = (dim, wrapped))]
    pub fn array(dim: Option<usize>, wrapped: Type) -> Type {
        Type::Array(dim, wrapped.into())
    }
    #[classattr]
    pub const JSON: Type = Type::Json;
    #[staticmethod]
    #[pyo3(signature = (*args))]
    pub fn tuple(args: Vec<Type>) -> Type {
        Type::Tuple(args.into())
    }
    #[staticmethod]
    pub fn list(arg: Type) -> Type {
        Type::List(arg.into())
    }
    #[classattr]
    pub const BYTES: Type = Type::Bytes;
    #[classattr]
    pub const PY_OBJECT_WRAPPER: Type = Type::PyObjectWrapper;
    #[staticmethod]
    pub fn optional(wrapped: Type) -> Type {
        Type::Optional(wrapped.into())
    }
    #[staticmethod]
    pub fn future(wrapped: Type) -> Type {
        Type::Future(wrapped.into())
    }
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

#[pyclass(module = "pathway.engine", frozen, name = "PythonConnectorEventType")]
pub struct PyPythonConnectorEventType(PythonConnectorEventType);

#[pymethods]
impl PyPythonConnectorEventType {
    #[classattr]
    pub const INSERT: PythonConnectorEventType = PythonConnectorEventType::Insert;
    #[classattr]
    pub const DELETE: PythonConnectorEventType = PythonConnectorEventType::Delete;
    #[classattr]
    pub const EXTERNAL_OFFSET: PythonConnectorEventType = PythonConnectorEventType::ExternalOffset;
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

#[pyclass(module = "pathway.engine", frozen, name = "KeyGenerationPolicy")]
pub struct PyKeyGenerationPolicy(KeyGenerationPolicy);

#[pymethods]
impl PyKeyGenerationPolicy {
    #[classattr]
    pub const ALWAYS_AUTOGENERATE: KeyGenerationPolicy = KeyGenerationPolicy::AlwaysAutogenerate;
    #[classattr]
    pub const PREFER_MESSAGE_KEY: KeyGenerationPolicy = KeyGenerationPolicy::PreferMessageKey;
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

#[pyclass(module = "pathway.engine", frozen, name = "SqlWriterInitMode")]
pub struct PySqlWriterInitMode(SqlWriterInitMode);

#[pymethods]
impl PySqlWriterInitMode {
    #[classattr]
    pub const DEFAULT: SqlWriterInitMode = SqlWriterInitMode::Default;
    #[classattr]
    pub const CREATE_IF_NOT_EXISTS: SqlWriterInitMode = SqlWriterInitMode::CreateIfNotExists;
    #[classattr]
    pub const REPLACE: SqlWriterInitMode = SqlWriterInitMode::Replace;
}

#[pyclass(module = "pathway.engine", frozen)]
pub struct Universe {
    scope: Py<Scope>,
    handle: UniverseHandle,
}

impl Universe {
    fn new<'py>(scope: &Bound<'py, Scope>, handle: UniverseHandle) -> PyResult<Bound<'py, Self>> {
        let py = scope.py();
        if let Some(universe) = scope.borrow().universes.borrow().get(&handle) {
            return Ok(universe.bind(py).clone());
        }
        let res = Bound::new(
            py,
            Self {
                scope: scope.clone().unbind(),
                handle,
            },
        )?;
        scope
            .borrow()
            .universes
            .borrow_mut()
            .insert(handle, res.clone().unbind());
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
    fn output_universe(self_: &Bound<Self>) -> Option<Py<Universe>> {
        if let Ok(_column) = self_.downcast_exact::<Column>() {
            None
        } else if let Ok(computer) = self_.downcast_exact::<Computer>() {
            let computer = computer.get();
            computer.is_output.then(|| computer.universe.clone())
        } else {
            unreachable!("Unknown ComplexColumn subclass");
        }
    }

    fn to_engine(self_: &Bound<Self>) -> EngineComplexColumn {
        if let Ok(column) = self_.downcast_exact::<Column>() {
            EngineComplexColumn::Column(column.borrow().handle)
        } else if let Ok(computer) = self_.downcast_exact::<Computer>() {
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
    fn new<'py>(
        universe: &Bound<'py, Universe>,
        handle: ColumnHandle,
    ) -> PyResult<Bound<'py, Self>> {
        let py = universe.py();
        let universe_ref = universe.borrow();
        let scope = universe_ref.scope.borrow(py);
        if let Some(column) = scope.columns.borrow().get(&handle) {
            let column = column.bind(py).clone();
            assert!(column.get().universe.is(universe));
            return Ok(column);
        }
        let res = Bound::new(
            py,
            (
                Self {
                    universe: universe.clone().unbind(),
                    handle,
                },
                ComplexColumn,
            ),
        )?;
        scope
            .columns
            .borrow_mut()
            .insert(handle, res.clone().unbind());
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
        universe: Bound<Universe>,
        #[pyo3(from_py_with = "from_py_iterable")] columns: Vec<Py<Column>>,
    ) -> PyResult<Self> {
        let py = universe.py();
        for column in &columns {
            check_identity(&column.borrow(py).universe, &universe, "universe mismatch")?;
        }
        Ok(Self {
            universe: universe.unbind(),
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
    fn to_engine(&self) -> (UniverseHandle, Vec<ColumnHandle>) {
        let universe = self.universe.get();
        let column_handles = self.columns.iter().map(|c| c.get().handle).collect();
        (universe.handle, column_handles)
    }

    fn from_handles<'py>(
        scope: &Bound<'py, Scope>,
        universe_handle: UniverseHandle,
        column_handles: impl IntoIterator<Item = ColumnHandle>,
    ) -> PyResult<Bound<'py, Self>> {
        let py = scope.py();
        let universe = Universe::new(scope, universe_handle)?;
        let columns = column_handles
            .into_iter()
            .map(|column_handle| Ok(Column::new(&universe, column_handle)?.unbind()))
            .collect::<PyResult<_>>()?;
        Bound::new(py, Self::new(universe, columns)?)
    }

    fn from_engine<'py>(
        scope: &Bound<'py, Scope>,
        table: EngineLegacyTable,
    ) -> PyResult<Bound<'py, Self>> {
        let (universe_handle, column_handles) = table;
        Self::from_handles(scope, universe_handle, column_handles)
    }
}

#[pyclass(module = "pathway.engine", frozen)]
pub struct Table {
    scope: Py<Scope>,
    handle: TableHandle,
}

impl Table {
    fn new(scope: &Bound<Scope>, handle: TableHandle) -> PyResult<Py<Self>> {
        let py = scope.py();
        if let Some(table) = scope.borrow().tables.borrow().get(&handle) {
            return Ok(table.clone_ref(py));
        }
        let res = Py::new(
            py,
            Self {
                scope: scope.clone().unbind(),
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

#[pyclass(module = "pathway.engine", frozen)]
pub struct ErrorLog {
    scope: Py<Scope>,
    handle: ErrorLogHandle,
}

impl ErrorLog {
    fn new(scope: &Bound<Scope>, handle: ErrorLogHandle) -> PyResult<Py<Self>> {
        let py = scope.py();
        if let Some(error_log) = scope.borrow().error_logs.borrow().get(&handle) {
            return Ok(error_log.clone_ref(py));
        }
        let res = Py::new(
            py,
            Self {
                scope: scope.clone().unbind(),
                handle,
            },
        )?;
        scope
            .borrow()
            .error_logs
            .borrow_mut()
            .insert(handle, res.clone());
        Ok(res)
    }
}

// submodule to make sure no other code can create instances of `Error`
mod error {
    use once_cell::sync::Lazy;
    use pyo3::prelude::*;

    struct InnerError;
    #[pyclass(module = "pathway.engine", frozen)]
    pub struct Error(InnerError);

    #[pymethods]
    impl Error {
        #[allow(clippy::unused_self)]
        fn __repr__(&self) -> &'static str {
            "Error"
        }
    }

    pub static ERROR: Lazy<Py<Error>> = Lazy::new(|| {
        Python::with_gil(|py| {
            Py::new(py, Error(InnerError)).expect("creating ERROR should not fail")
        })
    });
}
use error::{Error, ERROR};

mod pending {
    use once_cell::sync::Lazy;
    use pyo3::prelude::*;

    struct InnerPending;
    #[pyclass(module = "pathway.engine", frozen)]
    pub struct Pending(InnerPending);

    #[pymethods]
    impl Pending {
        #[allow(clippy::unused_self)]
        fn __repr__(&self) -> &'static str {
            "Pending"
        }
    }

    pub static PENDING: Lazy<Py<Pending>> = Lazy::new(|| {
        Python::with_gil(|py| {
            Py::new(py, Pending(InnerPending)).expect("creating PENDING should not fail")
        })
    });
}
use pending::{Pending, PENDING};

impl<'py> FromPyObject<'py> for ColumnPath {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let py = ob.py();
        if ob.getattr(intern!(py, "is_key")).is_ok_and(|is_key| {
            is_key
                .extract::<&PyBool>()
                .expect("is_key field in ColumnPath should be bool")
                .is_true()
        }) {
            Ok(Self::Key)
        } else if let Ok(path) = ob
            .getattr(intern!(py, "path"))
            .and_then(|path| path.extract())
        {
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
        PyErr::new_type_bound(
            py,
            "pathway.engine.MissingValueError",
            None,
            Some(&PyBaseException::type_object_bound(py)),
            None,
        )
        .expect("creating MissingValueError type should not fail")
    })
});

static ENGINE_ERROR_TYPE: Lazy<Py<PyType>> = Lazy::new(|| {
    Python::with_gil(|py| {
        PyErr::new_type_bound(
            py,
            "pathway.engine.EngineError",
            None,
            Some(&PyException::type_object_bound(py)),
            None,
        )
        .expect("creating EngineError type should not fail")
    })
});

static ENGINE_ERROR_WITH_TRACE_TYPE: Lazy<Py<PyType>> = Lazy::new(|| {
    Python::with_gil(|py| {
        PyErr::new_type_bound(
            py,
            "pathway.engine.EngineErrorWithTrace",
            None,
            Some(&PyException::type_object_bound(py)),
            None,
        )
        .expect("creating EngineErrorWithTrace type should not fail")
    })
});

static OTHER_WORKER_ERROR: Lazy<Py<PyType>> = Lazy::new(|| {
    Python::with_gil(|py| {
        PyErr::new_type_bound(
            py,
            "pathway.engine.OtherWorkerError",
            None,
            Some(&PyException::type_object_bound(py)),
            None,
        )
        .expect("creating OtherWorkerError type should not fail")
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
                context.get(column, row, args).ok_or_else(|| {
                    PyErr::from_type_bound(MISSING_VALUE_ERROR_TYPE.bind(py).clone(), ())
                })
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
        let context = Bound::new(py, Context(SendWrapper::new(ScopedContext::default())))?;
        let mut all_args = Vec::with_capacity(args.len() + 1);
        all_args.push(context.to_object(py));
        all_args.extend(args.iter().map(|value| value.to_object(py)));
        let res = context.borrow().0.scoped(
            engine_context,
            || self.fun.bind(py).call1(PyTuple::new_bound(py, all_args)), // FIXME
        );
        match res {
            Ok(value) => Ok(Some(value.extract()?)),
            Err(error) => {
                if error.is_instance_bound(py, MISSING_VALUE_ERROR_TYPE.bind(py)) {
                    Ok(None)
                } else {
                    Err(error)
                }
            }
        }
    }

    fn to_engine(self_: &Bound<Self>) -> EngineComplexColumn {
        let py = self_.py();
        let self_ref = self_.borrow();
        let computer: Py<Self> = self_.clone().unbind();
        let engine_computer = if self_ref.is_method {
            let data_column_handle = self_ref
                .data_column
                .as_ref()
                .map(|data_column| data_column.borrow(py).handle);
            EngineComputer::Method {
                logic: Box::new(move |engine_context, args| {
                    let engine_context = SendWrapper::new(engine_context);
                    Ok(Python::with_gil(|py| {
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
                    Ok(Python::with_gil(|py| {
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
    license: Option<License>,
    graph: SendWrapper<ScopedGraph>,
    is_persisted: bool,

    // empty_universe: Lazy<Py<Universe>>,
    universes: RefCell<HashMap<UniverseHandle, Py<Universe>>>,
    columns: RefCell<HashMap<ColumnHandle, Py<Column>>>,
    tables: RefCell<HashMap<TableHandle, Py<Table>>>,
    error_logs: RefCell<HashMap<ErrorLogHandle, Py<ErrorLog>>>,
    unique_names: RefCell<HashSet<UniqueName>>,
    event_loop: PyObject,
    total_connectors: RefCell<usize>,
}

impl Scope {
    fn new(
        parent: Option<Py<Self>>,
        event_loop: PyObject,
        license: Option<License>,
        is_persisted: bool,
    ) -> Self {
        Scope {
            parent,
            license,
            is_persisted,
            graph: SendWrapper::new(ScopedGraph::new()),
            universes: RefCell::new(HashMap::new()),
            columns: RefCell::new(HashMap::new()),
            tables: RefCell::new(HashMap::new()),
            error_logs: RefCell::new(HashMap::new()),
            unique_names: RefCell::new(HashSet::new()),
            event_loop,
            total_connectors: RefCell::new(0),
        }
    }

    fn clear_caches(&self) {
        self.universes.borrow_mut().clear();
        self.columns.borrow_mut().clear();
        self.tables.borrow_mut().clear();
        self.error_logs.borrow_mut().clear();
    }

    fn register_unique_name(&self, unique_name: Option<&UniqueName>) -> PyResult<()> {
        if let Some(unique_name) = &unique_name {
            let is_unique_id = self
                .unique_names
                .borrow_mut()
                .insert((*unique_name).to_string());
            if !is_unique_id {
                return Err(PyValueError::new_err(format!(
                    "Unique name '{unique_name}' used more than once"
                )));
            }
        }
        Ok(())
    }
}

#[pymethods]
impl Scope {
    #[getter]
    pub fn worker_index(&self) -> usize {
        self.graph.worker_index()
    }

    #[getter]
    pub fn worker_count(&self) -> usize {
        self.graph.worker_count()
    }

    #[getter]
    pub fn thread_count(&self) -> usize {
        self.graph.thread_count()
    }

    #[getter]
    pub fn process_count(&self) -> usize {
        self.graph.process_count()
    }

    pub fn empty_table(
        self_: &Bound<Self>,
        properties: ConnectorProperties,
    ) -> PyResult<Py<Table>> {
        let column_properties = properties.column_properties();
        let table_handle = self_
            .borrow()
            .graph
            .empty_table(Arc::new(EngineTableProperties::flat(column_properties)))?;
        Table::new(self_, table_handle)
    }

    pub fn static_universe<'py>(
        self_: &Bound<'py, Self>,
        #[pyo3(from_py_with = "from_py_iterable")] keys: Vec<Key>,
    ) -> PyResult<Bound<'py, Universe>> {
        let handle = self_.borrow().graph.static_universe(keys)?;
        Universe::new(self_, handle)
    }

    pub fn static_column<'py>(
        self_: &Bound<'py, Self>,
        universe: &Bound<'py, Universe>,
        #[pyo3(from_py_with = "from_py_iterable")] values: Vec<(Key, Value)>,
        properties: ColumnProperties,
    ) -> PyResult<Bound<'py, Column>> {
        check_identity(self_, &universe.get().scope, "scope mismatch")?;
        let handle =
            self_
                .borrow()
                .graph
                .static_column(universe.get().handle, values, properties.0)?;
        Column::new(universe, handle)
    }

    pub fn static_table(
        self_: &Bound<Self>,
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
        self_: &Bound<Self>,
        data_source: &Bound<DataStorage>,
        data_format: &Bound<DataFormat>,
        properties: &Bound<ConnectorProperties>,
    ) -> PyResult<Py<Table>> {
        let py = self_.py();

        let unique_name = properties.borrow().unique_name.clone();
        self_.borrow().register_unique_name(unique_name.as_ref())?;
        let connector_index = *self_.borrow().total_connectors.borrow();
        *self_.borrow().total_connectors.borrow_mut() += 1;
        let (reader_impl, parallel_readers) = data_source.borrow().construct_reader(
            py,
            &data_format.borrow(),
            connector_index,
            self_.borrow().worker_index(),
            self_.borrow().license.as_ref(),
            self_.borrow().is_persisted,
        )?;

        let parser_impl = data_format.borrow().construct_parser(py)?;

        let column_properties = properties.borrow().column_properties();
        let table_handle = self_.borrow().graph.connector_table(
            reader_impl,
            parser_impl,
            properties
                .borrow()
                .commit_duration_ms
                .map(time::Duration::from_millis),
            parallel_readers,
            Arc::new(EngineTableProperties::flat(column_properties)),
            unique_name.as_ref(),
            properties.borrow().synchronization_group.borrow().as_ref(),
        )?;
        Table::new(self_, table_handle)
    }

    #[allow(clippy::type_complexity)]
    #[pyo3(signature = (iterated, iterated_with_universe, extra, logic, *, limit = None))]
    pub fn iterate<'py>(
        self_: &Bound<'py, Self>,
        #[pyo3(from_py_with = "engine_tables_from_py_iterable")] iterated: Vec<EngineLegacyTable>,
        #[pyo3(from_py_with = "engine_tables_from_py_iterable")] iterated_with_universe: Vec<
            EngineLegacyTable,
        >,
        #[pyo3(from_py_with = "engine_tables_from_py_iterable")] extra: Vec<EngineLegacyTable>,
        logic: &Bound<'py, PyAny>,
        limit: Option<u32>,
    ) -> PyResult<(Vec<Bound<'py, LegacyTable>>, Vec<Bound<'py, LegacyTable>>)> {
        let py = self_.py();
        let (result, result_with_universe) = self_.borrow().graph.iterate(
            iterated,
            iterated_with_universe,
            extra,
            limit,
            Box::new(|graph, iterated, iterated_with_universe, extra| {
                let scope = Bound::new(
                    py,
                    Scope::new(
                        Some(self_.clone().unbind()),
                        self_.borrow().event_loop.clone(),
                        None,
                        false,
                    ),
                )?;
                scope.borrow().graph.scoped(graph, || {
                    let iterated = iterated
                        .into_iter()
                        .map(|table| LegacyTable::from_engine(&scope, table))
                        .collect::<PyResult<Vec<_>>>()?;
                    let iterated_with_universe = iterated_with_universe
                        .into_iter()
                        .map(|table| LegacyTable::from_engine(&scope, table))
                        .collect::<PyResult<Vec<_>>>()?;
                    let extra = extra
                        .into_iter()
                        .map(|table| LegacyTable::from_engine(&scope, table))
                        .collect::<PyResult<Vec<_>>>()?;
                    let (result, result_with_universe): (Bound<PyAny>, Bound<PyAny>) = logic
                        .call1((scope, iterated, iterated_with_universe, extra))?
                        .extract()?;
                    let result = result
                        .iter()?
                        .map(|table| {
                            let table: PyRef<LegacyTable> = table?.extract()?;
                            Ok(table.to_engine())
                        })
                        .collect::<PyResult<_>>()?;
                    let result_with_universe = result_with_universe
                        .iter()?
                        .map(|table| {
                            let table: PyRef<LegacyTable> = table?.extract()?;
                            Ok(table.to_engine())
                        })
                        .collect::<PyResult<_>>()?;
                    Ok((result, result_with_universe))
                })
            }),
        )?;
        let result = result
            .into_iter()
            .map(|table| LegacyTable::from_engine(self_, table))
            .collect::<PyResult<_>>()?;
        let result_with_universe = result_with_universe
            .into_iter()
            .map(|table| LegacyTable::from_engine(self_, table))
            .collect::<PyResult<_>>()?;
        Ok((result, result_with_universe))
    }

    pub fn map_column<'py>(
        self_: &Bound<'py, Self>,
        table: &LegacyTable,
        function: Py<PyAny>,
        properties: ColumnProperties,
    ) -> PyResult<Bound<'py, Column>> {
        let py = self_.py();
        let universe = table.universe.bind(py);
        let universe_ref = universe.get();
        check_identity(self_, &universe_ref.scope, "scope mismatch")?;
        let column_handles = table.columns.iter().map(|c| c.borrow(py).handle).collect();
        let handle = self_.borrow().graph.expression_column(
            BatchWrapper::WithGil,
            Arc::new(Expression::Any(AnyExpression::Apply(
                Box::new(move |input| {
                    Python::with_gil(|py| -> DynResult<_> {
                        let inputs = PyTuple::new_bound(py, input);
                        Ok(function.call1(py, (inputs,))?.extract::<Value>(py)?)
                    })
                }),
                Expressions::Arguments(0..table.columns.len()),
            ))),
            universe_ref.handle,
            column_handles,
            properties.0,
        )?;
        Column::new(universe, handle)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn async_apply_table(
        self_: &Bound<Self>,
        table: PyRef<Table>,
        #[pyo3(from_py_with = "from_py_iterable")] column_paths: Vec<ColumnPath>,
        function: Py<PyAny>,
        propagate_none: bool,
        append_only_or_deterministic: bool,
        properties: TableProperties,
        dtype: Type,
    ) -> PyResult<Py<Table>> {
        let dtype = Arc::new(dtype);
        let event_loop = self_.borrow().event_loop.clone();
        let table_handle = self_.borrow().graph.async_apply_table(
            Arc::new(move |_, values: &[Value]| {
                if propagate_none && values.iter().any(|a| matches!(a, Value::None)) {
                    return Box::pin(futures::future::ok(Value::None));
                }
                let future = Python::with_gil(|py| {
                    let event_loop = event_loop.clone();
                    let args = PyTuple::new_bound(py, values);
                    let awaitable = function.call1(py, args)?;
                    let awaitable = awaitable.into_bound(py);
                    let locals = pyo3_asyncio::TaskLocals::new(event_loop.into_bound(py))
                        .copy_context(py)?;
                    pyo3_asyncio::into_future_with_locals(&locals, awaitable)
                });

                Box::pin({
                    let dtype = dtype.clone();
                    async {
                        let result = future?.await?;
                        Python::with_gil(move |py| Ok(extract_value(result.bind(py), &dtype)?))
                    }
                })
            }),
            table.handle,
            column_paths,
            properties.0,
            append_only_or_deterministic,
        )?;
        Table::new(self_, table_handle)
    }

    pub fn expression_table(
        self_: &Bound<Self>,
        table: &Table,
        #[pyo3(from_py_with = "from_py_iterable")] column_paths: Vec<ColumnPath>,
        #[pyo3(from_py_with = "from_py_iterable")] expressions: Vec<ExpressionData>,
        append_only_or_deterministic: bool,
    ) -> PyResult<Py<Table>> {
        let gil = expressions
            .iter()
            .any(|expression_data| expression_data.gil);
        let wrapper = if gil {
            BatchWrapper::WithGil
        } else {
            BatchWrapper::None
        };
        let table_handle = self_.borrow().graph.expression_table(
            table.handle,
            column_paths,
            expressions,
            wrapper,
            append_only_or_deterministic,
        )?;
        Table::new(self_, table_handle)
    }

    pub fn columns_to_table(
        self_: &Bound<Self>,
        universe: &Bound<Universe>,
        #[pyo3(from_py_with = "from_py_iterable")] columns: Vec<PyRef<Column>>,
    ) -> PyResult<Py<Table>> {
        let column_handles = columns.into_iter().map(|column| column.handle).collect();
        let table_handle = self_
            .borrow()
            .graph
            .columns_to_table(universe.borrow().handle, column_handles)?;
        Table::new(self_, table_handle)
    }

    pub fn table_column<'py>(
        self_: &Bound<'py, Self>,
        universe: &Bound<'py, Universe>,
        table: PyRef<Table>,
        column_path: ColumnPath,
    ) -> PyResult<Bound<'py, Column>> {
        let handle = self_.borrow().graph.table_column(
            universe.borrow().handle,
            table.handle,
            column_path,
        )?;
        Column::new(universe, handle)
    }

    pub fn table_universe<'py>(
        self_: &Bound<'py, Self>,
        table: &Table,
    ) -> PyResult<Bound<'py, Universe>> {
        let universe_handle = self_.borrow().graph.table_universe(table.handle)?;
        Universe::new(self_, universe_handle)
    }

    pub fn table_properties(
        self_: &Bound<Self>,
        table: PyRef<Table>,
        path: ColumnPath,
    ) -> PyResult<Py<TableProperties>> {
        let py = self_.py();
        let properties = self_.borrow().graph.table_properties(table.handle, &path)?;
        TableProperties::new(py, properties)
    }

    pub fn flatten_table_storage(
        self_: &Bound<Self>,
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
        self_: &Bound<Self>,
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

    pub fn remove_retractions_from_table(
        self_: &Bound<Self>,
        table: PyRef<Table>,
        table_properties: TableProperties,
    ) -> PyResult<Py<Table>> {
        let new_table_handle = self_
            .borrow()
            .graph
            .remove_retractions_from_table(table.handle, table_properties.0)?;
        Table::new(self_, new_table_handle)
    }

    pub fn forget(
        self_: &Bound<Self>,
        table: PyRef<Table>,
        threshold_column_path: ColumnPath,
        current_time_column_path: ColumnPath,
        instance_column_path: ColumnPath,
        mark_forgetting_records: bool,
        table_properties: TableProperties,
    ) -> PyResult<Py<Table>> {
        let new_table_handle = self_.borrow().graph.forget(
            table.handle,
            threshold_column_path,
            current_time_column_path,
            instance_column_path,
            mark_forgetting_records,
            table_properties.0,
        )?;
        Table::new(self_, new_table_handle)
    }

    pub fn forget_immediately(
        self_: &Bound<Self>,
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
        self_: &Bound<Self>,
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
        self_: &Bound<Self>,
        table: PyRef<Table>,
        threshold_column_path: ColumnPath,
        current_time_column_path: ColumnPath,
        instance_column_path: ColumnPath,
        table_properties: TableProperties,
    ) -> PyResult<Py<Table>> {
        let new_table_handle = self_.borrow().graph.freeze(
            table.handle,
            threshold_column_path,
            current_time_column_path,
            instance_column_path,
            table_properties.0,
        )?;
        Table::new(self_, new_table_handle)
    }

    pub fn gradual_broadcast(
        self_: &Bound<Self>,
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

    pub fn use_external_index_as_of_now(
        self_: &Bound<Self>,
        index: &PyExternalIndexData,
        queries: &PyExternalIndexQuery,
        table_properties: TableProperties,
        external_index_factory: PyExternalIndexFactory,
    ) -> PyResult<Py<Table>> {
        let new_table_handle = self_.borrow().graph.use_external_index_as_of_now(
            index.to_external_index_data(),
            queries.to_external_index_query(),
            table_properties.0,
            external_index_factory.inner.make_instance()?,
        )?;
        Table::new(self_, new_table_handle)
    }

    pub fn buffer(
        self_: &Bound<Self>,
        table: PyRef<Table>,
        threshold_column_path: ColumnPath,
        current_time_column_path: ColumnPath,
        instance_column_path: ColumnPath,
        table_properties: TableProperties,
    ) -> PyResult<Py<Table>> {
        let new_table_handle = self_.borrow().graph.buffer(
            table.handle,
            threshold_column_path,
            current_time_column_path,
            instance_column_path,
            table_properties.0,
        )?;
        Table::new(self_, new_table_handle)
    }

    pub fn intersect_tables(
        self_: &Bound<Self>,
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
        self_: &Bound<Self>,
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
        self_: &Bound<Self>,
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
        self_: &Bound<Self>,
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
        self_: &Bound<Self>,
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
        self_: &Bound<Self>,
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
        self_: &Bound<'py, Self>,
        universe: &Bound<'py, Universe>,
        column: &Bound<'py, Column>,
    ) -> PyResult<Bound<'py, Column>> {
        check_identity(self_, &universe.get().scope, "scope mismatch")?;
        let column_universe = column.get().universe.bind(self_.py());
        check_identity(self_, &column_universe.borrow().scope, "scope mismatch")?;
        let new_column_handle = self_
            .borrow()
            .graph
            .restrict_column(universe.get().handle, column.get().handle)?;
        Column::new(universe, new_column_handle)
    }

    pub fn restrict_table(
        self_: &Bound<Self>,
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
        self_: &Bound<Self>,
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
        self_: &Bound<'py, Self>,
        universe: &Bound<'py, Universe>,
        columns: &Bound<'py, PyAny>,
    ) -> PyResult<LegacyTable> {
        check_identity(self_, &universe.borrow().scope, "scope mismatch")?;
        let columns = columns
            .iter()?
            .map(|column| {
                let column = column?;
                let column = column.downcast()?;
                let restricted = Self::restrict_column(self_, universe, column)?;
                Ok(restricted.unbind())
            })
            .collect::<PyResult<_>>()?;
        LegacyTable::new(universe.clone(), columns)
    }

    #[pyo3(signature = (table, grouping_columns_paths, last_column_is_instance, reducers, set_id, table_properties))]
    pub fn group_by_table(
        self_: &Bound<Self>,
        table: PyRef<Table>,
        #[pyo3(from_py_with = "from_py_iterable")] grouping_columns_paths: Vec<ColumnPath>,
        last_column_is_instance: bool,
        #[pyo3(from_py_with = "from_py_iterable")] reducers: Vec<ReducerData>,
        set_id: bool,
        table_properties: TableProperties,
    ) -> PyResult<Py<Table>> {
        let table_handle = self_.borrow().graph.group_by_table(
            table.handle,
            grouping_columns_paths,
            ShardPolicy::from_last_column_is_instance(last_column_is_instance),
            reducers,
            set_id,
            table_properties.0,
        )?;
        Table::new(self_, table_handle)
    }

    #[pyo3(signature = (table, grouping_columns_paths, reduced_column_paths, combine, unique_name, table_properties))]
    pub fn deduplicate(
        self_: &Bound<Self>,
        table: PyRef<Table>,
        #[pyo3(from_py_with = "from_py_iterable")] grouping_columns_paths: Vec<ColumnPath>,
        #[pyo3(from_py_with = "from_py_iterable")] reduced_column_paths: Vec<ColumnPath>,
        combine: Py<PyAny>,
        unique_name: Option<UniqueName>,
        table_properties: TableProperties,
    ) -> PyResult<Py<Table>> {
        let table_handle = self_.borrow().graph.deduplicate(
            table.handle,
            grouping_columns_paths,
            reduced_column_paths,
            wrap_stateful_combine(combine),
            unique_name.as_ref(),
            table_properties.0,
        )?;
        Table::new(self_, table_handle)
    }

    pub fn ix_table(
        self_: &Bound<Self>,
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

    #[pyo3(signature = (left_table, right_table, left_column_paths, right_column_paths, *, last_column_is_instance, table_properties, assign_id = false, left_ear = false, right_ear = false))]
    #[allow(clippy::too_many_arguments)]
    #[allow(clippy::fn_params_excessive_bools)]
    pub fn join_tables(
        self_: &Bound<Self>,
        left_table: PyRef<Table>,
        right_table: PyRef<Table>,
        #[pyo3(from_py_with = "from_py_iterable")] left_column_paths: Vec<ColumnPath>,
        #[pyo3(from_py_with = "from_py_iterable")] right_column_paths: Vec<ColumnPath>,
        last_column_is_instance: bool,
        table_properties: TableProperties,
        assign_id: bool,
        left_ear: bool,
        right_ear: bool,
    ) -> PyResult<Py<Table>> {
        let join_type = JoinType::from_assign_left_right(assign_id, left_ear, right_ear)?;
        let table_handle = self_.borrow().graph.join_tables(
            JoinData::new(left_table.handle, left_column_paths),
            JoinData::new(right_table.handle, right_column_paths),
            ShardPolicy::from_last_column_is_instance(last_column_is_instance),
            join_type,
            table_properties.0,
        )?;
        Table::new(self_, table_handle)
    }

    fn complex_columns<'py>(
        self_: &Bound<'py, Self>,
        #[pyo3(from_py_with = "from_py_iterable")] inputs: Vec<Bound<'py, ComplexColumn>>,
    ) -> PyResult<Vec<Bound<'py, Column>>> {
        let py = self_.py();
        let mut engine_complex_columns = Vec::new();
        let mut output_universes = Vec::new();
        for input in inputs {
            engine_complex_columns.push(ComplexColumn::to_engine(&input));
            output_universes.extend(ComplexColumn::output_universe(&input));
        }
        let columns = self_
            .borrow()
            .graph
            .complex_columns(engine_complex_columns)?
            .into_iter()
            .zip_eq(output_universes)
            .map(|(column_handle, universe)| Column::new(universe.bind(py), column_handle))
            .collect::<PyResult<_>>()?;
        Ok(columns)
    }

    pub fn debug_table<'py>(
        self_: &'py Bound<Self>,
        name: String,
        table: &'py Bound<Table>,
        columns: Vec<(String, ColumnPath)>,
    ) -> PyResult<()> {
        check_identity(self_, &table.borrow().scope, "scope mismatch")?;
        Ok(self_
            .borrow()
            .graph
            .debug_table(name, table.borrow().handle, columns)?)
    }

    pub fn update_rows_table(
        self_: &Bound<Self>,
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
        self_: &Bound<Self>,
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
        self_: &Bound<Self>,
        table: PyRef<Table>,
        #[pyo3(from_py_with = "from_py_iterable")] column_paths: Vec<ColumnPath>,
        data_sink: &Bound<DataStorage>,
        data_format: &Bound<DataFormat>,
        unique_name: Option<UniqueName>,
        sort_by_indices: Option<Vec<usize>>,
    ) -> PyResult<()> {
        let py = self_.py();

        self_.borrow().register_unique_name(unique_name.as_ref())?;
        let sink_impl = data_sink
            .borrow()
            .construct_writer(py, &data_format.borrow())?;
        let format_impl = data_format.borrow().construct_formatter(py)?;

        self_.borrow().graph.output_table(
            sink_impl,
            format_impl,
            table.handle,
            column_paths,
            unique_name,
            sort_by_indices,
        )?;

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn subscribe_table(
        self_: &Bound<Self>,
        table: PyRef<Table>,
        #[pyo3(from_py_with = "from_py_iterable")] column_paths: Vec<ColumnPath>,
        skip_persisted_batch: bool,
        skip_errors: bool,
        on_change: Py<PyAny>,
        on_time_end: Py<PyAny>,
        on_end: Py<PyAny>,
        unique_name: Option<UniqueName>,
        sort_by_indices: Option<Vec<usize>>,
    ) -> PyResult<()> {
        self_.borrow().register_unique_name(unique_name.as_ref())?;
        let callbacks = build_subscribe_callback(on_change, on_time_end, on_end);
        self_.borrow().graph.subscribe_table(
            table.handle,
            column_paths,
            callbacks,
            SubscribeConfig {
                skip_persisted_batch,
                skip_errors,
                skip_pending: true,
            },
            unique_name,
            sort_by_indices,
        )?;
        Ok(())
    }

    pub fn set_operator_properties(
        self_: &Bound<Self>,
        operator_id: usize,
        depends_on_error_log: bool,
    ) -> PyResult<()> {
        Ok(self_
            .borrow()
            .graph
            .set_operator_properties(OperatorProperties {
                id: operator_id,
                depends_on_error_log,
            })?)
    }

    pub fn set_error_log(self_: &Bound<Self>, error_log: Option<PyRef<ErrorLog>>) -> PyResult<()> {
        if let Some(error_log) = error_log.as_ref() {
            check_identity(self_, &error_log.scope, "scope mismatch")?;
        }
        Ok(self_
            .borrow()
            .graph
            .set_error_log(error_log.map(|error_log| error_log.handle))?)
    }

    pub fn error_log(
        self_: &Bound<Self>,
        properties: ConnectorProperties,
    ) -> PyResult<(Py<Table>, Py<ErrorLog>)> {
        let column_properties = properties.column_properties();
        let (table_handle, error_log_handle) = self_
            .borrow()
            .graph
            .error_log(Arc::new(EngineTableProperties::flat(column_properties)))?;
        Ok((
            Table::new(self_, table_handle)?,
            ErrorLog::new(self_, error_log_handle)?,
        ))
    }

    pub fn probe_table(
        self_: &Bound<Self>,
        table: PyRef<Table>,
        operator_id: usize,
    ) -> PyResult<()> {
        self_
            .borrow()
            .graph
            .probe_table(table.handle, operator_id)?;
        Ok(())
    }

    pub fn export_table(
        self_: &Bound<Self>,
        table: PyRef<Table>,
        column_paths: Vec<ColumnPath>,
    ) -> PyResult<PyExportedTable> {
        let exported_table = self_
            .borrow()
            .graph
            .export_table(table.handle, column_paths)?;
        Ok(PyExportedTable::new(exported_table))
    }

    pub fn import_table(self_: &Bound<Self>, table: &PyExportedTable) -> PyResult<Py<Table>> {
        let table_handle = self_.borrow().graph.import_table(table.inner.clone())?;
        Table::new(self_, table_handle)
    }

    pub fn remove_value_from_table(
        self_: &Bound<Self>,
        table: PyRef<Table>,
        #[pyo3(from_py_with = "from_py_iterable")] column_paths: Vec<ColumnPath>,
        value: Value,
        table_properties: TableProperties,
    ) -> PyResult<Py<Table>> {
        let new_table_handle = self_.borrow().graph.remove_value_from_table(
            table.handle,
            column_paths,
            value,
            table_properties.0,
        )?;
        Table::new(self_, new_table_handle)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn async_transformer(
        self_: &Bound<Self>,
        table: PyRef<Table>,
        #[pyo3(from_py_with = "from_py_iterable")] column_paths: Vec<ColumnPath>,
        on_change: Py<PyAny>,
        on_time_end: Py<PyAny>,
        on_end: Py<PyAny>,
        data_source: &Bound<DataStorage>,
        data_format: &Bound<DataFormat>,
        properties: ConnectorProperties,
        skip_errors: bool,
    ) -> PyResult<Py<Table>> {
        let py = self_.py();

        let callbacks = build_subscribe_callback(on_change, on_time_end, on_end);
        let connector_index = *self_.borrow().total_connectors.borrow();
        *self_.borrow().total_connectors.borrow_mut() += 1;
        let (reader_impl, parallel_readers) = data_source.borrow().construct_reader(
            py,
            &data_format.borrow(),
            connector_index,
            self_.borrow().worker_index(),
            self_.borrow().license.as_ref(),
            false,
        )?;
        assert_eq!(parallel_readers, 1); // python connector that has parallel_readers == 1 has to be used

        let parser_impl = data_format.borrow().construct_parser(py)?;
        let commit_duration = properties
            .commit_duration_ms
            .map(time::Duration::from_millis);
        let column_properties = properties.column_properties();

        let table_handle = self_.borrow().graph.async_transformer(
            table.handle,
            column_paths,
            callbacks,
            reader_impl,
            parser_impl,
            commit_duration,
            Arc::new(EngineTableProperties::flat(column_properties)),
            skip_errors,
        )?;
        Table::new(self_, table_handle)
    }
}

fn build_subscribe_callback(
    on_change: Py<PyAny>,
    on_time_end: Py<PyAny>,
    on_end: Py<PyAny>,
) -> SubscribeCallbacks {
    SubscribeCallbacksBuilder::new()
        .wrapper(BatchWrapper::WithGil)
        .on_data(Box::new(move |key, values, time, diff| {
            Python::with_gil(|py| {
                on_change.call1(py, (key, PyTuple::new_bound(py, values), time, diff))?;
                Ok(())
            })
        }))
        .on_time_end(Box::new(move |new_time| {
            Python::with_gil(|py| {
                on_time_end.call1(py, (new_time,))?;
                Ok(())
            })
        }))
        .on_end(Box::new(move || {
            Python::with_gil(|py| {
                on_end.call0(py)?;
                Ok(())
            })
        }))
        .build()
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
                table_data.lock().unwrap().push(DataRow::from_engine(
                    key,
                    Vec::from(values),
                    time,
                    diff,
                ));
                Ok(())
            }))
            .build();
        graph.subscribe_table(
            table.handle,
            column_paths,
            callbacks,
            SubscribeConfig {
                skip_persisted_batch: false,
                skip_errors: false,
                skip_pending: false,
            },
            None,
            None,
        )?;
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
    *,
    stats_monitor = None,
    ignore_asserts = false,
    monitoring_level = MonitoringLevel::None,
    with_http_server = false,
    persistence_config = None,
    license_key = None,
    monitoring_server = None,
    trace_parent = None,
    run_id = None,
    terminate_on_error = true,
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
    license_key: Option<String>,
    monitoring_server: Option<String>,
    trace_parent: Option<String>,
    run_id: Option<String>,
    terminate_on_error: bool,
) -> PyResult<Vec<Vec<DataRow>>> {
    LOGGING_RESET_HANDLE.reset();
    defer! {
        log::logger().flush();
    }
    let config = Config::from_env().map_err(|msg| {
        PyErr::from_type_bound(ENGINE_ERROR_TYPE.bind(py).clone(), msg.to_string())
    })?;
    let license = License::new(license_key)?;
    let persistence_config = {
        if let Some(persistence_config) = persistence_config {
            let persistence_config = persistence_config.prepare(py)?;
            persistence_config.validate(&license)?;
            Some(persistence_config)
        } else {
            None
        }
    };
    let is_persisted = persistence_config.is_some();
    let telemetry_config =
        EngineTelemetryConfig::create(&license, run_id, monitoring_server, trace_parent)?;
    let results: Vec<Vec<_>> = run_with_wakeup_receiver(py, |wakeup_receiver| {
        let scope_license = license.clone();
        py.allow_threads(|| {
            run_with_new_dataflow_graph(
                move |graph| {
                    let thread_state = PythonThreadState::new();

                    let captured_tables = Python::with_gil(|py| {
                        let our_scope = &Bound::new(
                            py,
                            Scope::new(
                                None,
                                event_loop.clone(),
                                Some(scope_license.clone()),
                                is_persisted,
                            ),
                        )?;
                        let tables: Vec<(PyRef<Table>, Vec<ColumnPath>)> =
                            our_scope.borrow().graph.scoped(graph, || {
                                from_py_iterable(&logic.bind(py).call1((our_scope,))?)
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
                &license,
                telemetry_config,
                terminate_on_error,
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
pub fn ref_scalar(values: &Bound<PyTuple>, optional: bool) -> PyResult<Option<Key>> {
    if optional && values.iter().any(|v| v.is_none()) {
        return Ok(None);
    }
    let key = Key::for_values(&from_py_iterable(values)?);
    Ok(Some(key))
}

#[pyfunction]
#[pyo3(signature = (*values, instance, optional = false))]
pub fn ref_scalar_with_instance(
    values: &Bound<PyTuple>,
    instance: Value,
    optional: bool,
) -> PyResult<Option<Key>> {
    if optional && (values.iter().any(|v| v.is_none()) || matches!(instance, Value::None)) {
        return Ok(None);
    }
    let mut values_with_instance: Vec<Value> = from_py_iterable(values)?;
    values_with_instance.push(instance);
    let key = ShardPolicy::LastKeyColumn.generate_key(&values_with_instance);
    Ok(Some(key))
}

#[pyfunction]
pub fn unsafe_make_pointer(value: KeyImpl) -> Key {
    Key(value)
}

#[pyfunction]
#[pyo3(signature = (value), name="serialize")]
pub fn serialize(py: Python, value: Value) -> PyResult<Py<PyBytes>> {
    let bytes = bincode::serialize(&value)
        .map_err(|e| PyValueError::new_err(format!("failed to serialize: {e}")))?;
    Ok(PyBytes::new_bound(py, &bytes).into())
}

#[pyfunction]
#[pyo3(signature = (bytes), name="deserialize")]
pub fn deserialize(bytes: &[u8]) -> PyResult<Value> {
    let value: Value = bincode::deserialize(bytes)
        .map_err(|e| PyValueError::new_err(format!("failed to deserialize: {e}")))?;
    Ok(value)
}

#[derive(Clone, Debug)]
#[pyclass(module = "pathway.engine", frozen)]
pub struct AzureBlobStorageSettings {
    account: String,
    password: String,
    container: String,
}

#[pymethods]
impl AzureBlobStorageSettings {
    #[new]
    #[pyo3(signature = (account, password, container))]
    fn new(account: String, password: String, container: String) -> Self {
        Self {
            account,
            password,
            container,
        }
    }
}

impl AzureBlobStorageSettings {
    fn credentials(&self) -> AzureStorageCredentials {
        AzureStorageCredentials::access_key(self.account.clone(), self.password.clone())
    }
}

#[pyclass(module = "pathway.engine", frozen)]
pub struct AwsS3Settings {
    bucket_name: Option<String>,
    region: s3::region::Region,
    access_key: Option<String>,
    secret_access_key: Option<String>,
    with_path_style: bool,
    profile: Option<String>,
    session_token: Option<String>,
}

#[pymethods]
impl AwsS3Settings {
    #[new]
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        bucket_name = None,
        access_key = None,
        secret_access_key = None,
        with_path_style = false,
        region = None,
        endpoint = None,
        profile = None,
        session_token = None,
    ))]
    fn new(
        bucket_name: Option<String>,
        access_key: Option<String>,
        secret_access_key: Option<String>,
        with_path_style: bool,
        region: Option<String>,
        endpoint: Option<String>,
        profile: Option<String>,
        session_token: Option<String>,
    ) -> PyResult<Self> {
        Ok(AwsS3Settings {
            bucket_name,
            region: Self::aws_region(region, endpoint)?,
            access_key,
            secret_access_key,
            with_path_style,
            profile,
            session_token,
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
    snapshot_maintenance_on_output: bool,
    aws_s3_settings: Option<Py<AwsS3Settings>>,
    elasticsearch_params: Option<Py<ElasticSearchParams>>,
    parallel_readers: Option<usize>,
    python_subject: Option<Py<PythonSubject>>,
    unique_name: Option<UniqueName>,
    max_batch_size: Option<usize>,
    object_pattern: String,
    mock_events: Option<HashMap<(UniqueName, usize), Vec<SnapshotEvent>>>,
    table_name: Option<String>,
    header_fields: Vec<(String, usize)>,
    key_field_index: Option<usize>,
    min_commit_frequency: Option<u64>,
    downloader_threads_count: Option<usize>,
    database: Option<String>,
    start_from_timestamp_ms: Option<i64>,
    namespace: Option<Vec<String>>,
    sql_writer_init_mode: SqlWriterInitMode,
    topic_name_index: Option<usize>,
    partition_columns: Option<Vec<String>>,
    backfilling_thresholds: Option<Vec<BackfillingThreshold>>,
    azure_blob_storage_settings: Option<AzureBlobStorageSettings>,
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
    pub const SELECTIVE_PERSISTING: PersistenceMode = PersistenceMode::SelectivePersisting;
    #[classattr]
    pub const UDF_CACHING: PersistenceMode = PersistenceMode::UdfCaching;
    #[classattr]
    pub const OPERATOR_PERSISTING: PersistenceMode = PersistenceMode::OperatorPersisting;
}

impl<'py> FromPyObject<'py> for PersistenceMode {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
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
    #[classattr]
    pub const OFFSETS_ONLY: SnapshotAccess = SnapshotAccess::OffsetsOnly;
}

impl<'py> FromPyObject<'py> for SnapshotAccess {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
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
    backend: DataStorage,
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
        backend,
        snapshot_access = SnapshotAccess::Full,
        persistence_mode = PersistenceMode::Batch,
        continue_after_replay = true,
    ))]
    fn new(
        snapshot_interval_ms: u64,
        backend: DataStorage,
        snapshot_access: SnapshotAccess,
        persistence_mode: PersistenceMode,
        continue_after_replay: bool,
    ) -> Self {
        Self {
            snapshot_interval: ::std::time::Duration::from_millis(snapshot_interval_ms),
            backend,
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
            self.backend.construct_persistent_storage_config(py)?,
            self.snapshot_access,
            self.persistence_mode,
            self.continue_after_replay,
        ))
    }
}

#[derive(Clone, Debug, Default)]
#[pyclass(module = "pathway.engine", frozen, get_all)]
pub struct TelemetryConfig {
    logging_servers: Vec<String>,
    tracing_servers: Vec<String>,
    metrics_servers: Vec<String>,
    service_name: Option<String>,
    service_version: Option<String>,
    service_namespace: Option<String>,
    service_instance_id: Option<String>,
    run_id: String,
    license_key: Option<String>,
}

#[pymethods]
impl TelemetryConfig {
    #[staticmethod]
    #[pyo3(signature = (
        *,
        run_id = None,
        license_key = None,
        monitoring_server = None,
    ))]
    fn create(
        run_id: Option<String>,
        license_key: Option<String>,
        monitoring_server: Option<String>,
    ) -> PyResult<TelemetryConfig> {
        let license = License::new(license_key)?;
        let config = EngineTelemetryConfig::create(&license, run_id, monitoring_server, None)?;
        Ok(config.into())
    }
}

impl From<EngineTelemetryConfig> for TelemetryConfig {
    fn from(config: EngineTelemetryConfig) -> Self {
        match config {
            EngineTelemetryConfig::Enabled(config) => Self {
                logging_servers: config.logging_servers,
                tracing_servers: config.tracing_servers,
                metrics_servers: config.metrics_servers,
                service_name: Some(config.service_name),
                service_version: Some(config.service_version),
                service_namespace: Some(config.service_namespace),
                service_instance_id: Some(config.service_instance_id),
                run_id: config.run_id,
                license_key: Some(config.license_key),
            },
            EngineTelemetryConfig::Disabled => Self::default(),
        }
    }
}

impl<'py> FromPyObject<'py> for SnapshotEvent {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
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
    pub fn advance_time(timestamp: Timestamp) -> SnapshotEvent {
        SnapshotEvent::AdvanceTime(timestamp, OffsetAntichain::new())
    }
    #[classattr]
    pub const FINISHED: SnapshotEvent = SnapshotEvent::Finished;
}

#[pyclass(module = "pathway.engine", frozen)]
#[derive(Clone)]
pub struct PythonSubject {
    pub start: Py<PyAny>,
    pub read: Py<PyAny>,
    pub seek: Py<PyAny>,
    pub on_persisted_run: Py<PyAny>,
    pub end: Py<PyAny>,
    pub is_internal: bool,
    pub deletions_enabled: bool,
}

#[pymethods]
impl PythonSubject {
    #[new]
    #[pyo3(signature = (start, read, seek, on_persisted_run, end, is_internal, deletions_enabled))]
    fn new(
        start: Py<PyAny>,
        read: Py<PyAny>,
        seek: Py<PyAny>,
        on_persisted_run: Py<PyAny>,
        end: Py<PyAny>,
        is_internal: bool,
        deletions_enabled: bool,
    ) -> Self {
        Self {
            start,
            read,
            seek,
            on_persisted_run,
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
    pub name: String,
    #[pyo3(get)]
    pub type_: Type,
    #[pyo3(get)]
    pub default: Option<Value>,
    #[pyo3(get)]
    pub metadata: Option<String>,
}

impl ValueField {
    fn as_inner_schema_field(&self) -> InnerSchemaField {
        InnerSchemaField::new(self.type_.clone(), self.default.clone())
    }
}

#[pymethods]
impl ValueField {
    #[new]
    #[pyo3(signature = (name, type_))]
    fn new(name: String, type_: Type) -> Self {
        ValueField {
            name,
            type_,
            default: None,
            metadata: None,
        }
    }

    fn set_default(&mut self, ob: &Bound<PyAny>) -> PyResult<()> {
        self.default = Some(extract_value(ob, &self.type_)?);
        Ok(())
    }

    fn set_metadata(&mut self, ob: &Bound<PyString>) -> PyResult<()> {
        self.metadata = Some(ob.extract()?);
        Ok(())
    }
}

#[derive(Clone, Debug)]
#[pyclass(module = "pathway.engine", frozen, get_all)]
pub struct BackfillingThreshold {
    pub field: String,
    pub threshold: Value,
    pub comparison_op: String, // TODO: enum?
}

#[pymethods]
impl BackfillingThreshold {
    #[new]
    #[pyo3(signature = (field, threshold, comparison_op))]
    fn new(field: String, threshold: Value, comparison_op: String) -> PyResult<Self> {
        let allowed_comparison_ops = vec![">", "<", ">=", "<=", "==", "!="];
        if !allowed_comparison_ops.contains(&comparison_op.as_str()) {
            return Err(PyValueError::new_err(format!("Unknown 'comparison_op': only {} are supported, but '{comparison_op}' was specified.", allowed_comparison_ops.into_iter().map(|x| format!("'{x}'")).format(", "))));
        }
        Ok(BackfillingThreshold {
            field,
            threshold,
            comparison_op,
        })
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
    value_field_index: Option<usize>,
    key_generation_policy: KeyGenerationPolicy,
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
        snapshot_maintenance_on_output = false,
        aws_s3_settings = None,
        elasticsearch_params = None,
        parallel_readers = None,
        python_subject = None,
        unique_name = None,
        max_batch_size = None,
        object_pattern = "*".to_string(),
        mock_events = None,
        table_name = None,
        header_fields = Vec::new(),
        key_field_index = None,
        min_commit_frequency = None,
        downloader_threads_count = None,
        database = None,
        start_from_timestamp_ms = None,
        namespace = None,
        sql_writer_init_mode = SqlWriterInitMode::Default,
        topic_name_index = None,
        partition_columns = None,
        backfilling_thresholds = None,
        azure_blob_storage_settings = None,
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
        snapshot_maintenance_on_output: bool,
        aws_s3_settings: Option<Py<AwsS3Settings>>,
        elasticsearch_params: Option<Py<ElasticSearchParams>>,
        parallel_readers: Option<usize>,
        python_subject: Option<Py<PythonSubject>>,
        unique_name: Option<UniqueName>,
        max_batch_size: Option<usize>,
        object_pattern: String,
        mock_events: Option<HashMap<(UniqueName, usize), Vec<SnapshotEvent>>>,
        table_name: Option<String>,
        header_fields: Vec<(String, usize)>,
        key_field_index: Option<usize>,
        min_commit_frequency: Option<u64>,
        downloader_threads_count: Option<usize>,
        database: Option<String>,
        start_from_timestamp_ms: Option<i64>,
        namespace: Option<Vec<String>>,
        sql_writer_init_mode: SqlWriterInitMode,
        topic_name_index: Option<usize>,
        partition_columns: Option<Vec<String>>,
        backfilling_thresholds: Option<Vec<BackfillingThreshold>>,
        azure_blob_storage_settings: Option<AzureBlobStorageSettings>,
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
            snapshot_maintenance_on_output,
            aws_s3_settings,
            elasticsearch_params,
            parallel_readers,
            python_subject,
            unique_name,
            max_batch_size,
            object_pattern,
            mock_events,
            table_name,
            header_fields,
            key_field_index,
            min_commit_frequency,
            downloader_threads_count,
            database,
            start_from_timestamp_ms,
            namespace,
            sql_writer_init_mode,
            topic_name_index,
            partition_columns,
            backfilling_thresholds,
            azure_blob_storage_settings,
        }
    }

    #[pyo3(signature = ())]
    fn delta_s3_storage_options(&self, py: pyo3::Python) -> PyResult<HashMap<String, String>> {
        let (bucket_name, _) = S3Scanner::deduce_bucket_and_path(self.path()?);
        let s3_settings = self
            .aws_s3_settings
            .as_ref()
            .ok_or_else(|| {
                PyValueError::new_err("S3 connection settings weren't specified for S3 data source")
            })?
            .borrow(py);

        let mut storage_options = HashMap::new();
        storage_options.insert("AWS_S3_ALLOW_UNSAFE_RENAME".to_string(), "True".to_string());

        let virtual_hosted_style_request_flag = {
            // Virtually hosted-style requests are mutually exclusive with path-style requests
            if s3_settings.with_path_style {
                "False".to_string()
            } else {
                "True".to_string()
            }
        };
        storage_options.insert(
            "AWS_VIRTUAL_HOSTED_STYLE_REQUEST".to_string(),
            virtual_hosted_style_request_flag,
        );
        storage_options.insert(
            "AWS_BUCKET_NAME".to_string(),
            s3_settings.final_bucket_name(bucket_name.as_deref())?,
        );

        if let Some(access_key) = &s3_settings.access_key {
            storage_options.insert("AWS_ACCESS_KEY_ID".to_string(), access_key.to_string());
        }
        if let Some(secret_access_key) = &s3_settings.secret_access_key {
            storage_options.insert(
                "AWS_SECRET_ACCESS_KEY".to_string(),
                secret_access_key.to_string(),
            );
        }
        if let Some(session_token) = &s3_settings.session_token {
            storage_options.insert("AWS_SESSION_TOKEN".to_string(), session_token.to_string());
        }
        if let Some(profile) = &s3_settings.profile {
            storage_options.insert("AWS_PROFILE".to_string(), profile.to_string());
        }

        if let s3::Region::Custom { endpoint, region } = &s3_settings.region {
            if endpoint.starts_with("https://") || endpoint.starts_with("http://") {
                storage_options.insert("AWS_ENDPOINT_URL".to_string(), endpoint.to_string());
            } else {
                storage_options.insert(
                    "AWS_ENDPOINT_URL".to_string(),
                    format!("https://{endpoint}"),
                );
            }
            storage_options.insert("AWS_ALLOW_HTTP".to_string(), "True".to_string());
            storage_options.insert("AWS_STORAGE_ALLOW_HTTP".to_string(), "True".to_string());
            if region != endpoint {
                storage_options.insert("AWS_REGION".to_string(), region.to_string());
            }
        } else {
            storage_options.insert("AWS_REGION".to_string(), s3_settings.region.to_string());
        }

        Ok(storage_options)
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
        value_field_index = None,
        key_generation_policy = KeyGenerationPolicy::PreferMessageKey,
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
        value_field_index: Option<usize>,
        key_generation_policy: KeyGenerationPolicy,
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
            value_field_index,
            key_generation_policy,
        }
    }

    fn is_native_session_used(&self) -> bool {
        matches!(self.session_type, SessionType::Native)
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
            .flexible(true)
            .has_headers(false);
        builder
    }
}

impl DataStorage {
    fn extract_string_field<'a>(
        field: &'a Option<String>,
        error_message: &'static str,
    ) -> PyResult<&'a str> {
        let value = field
            .as_ref()
            .ok_or_else(|| PyValueError::new_err(error_message))?
            .as_str();
        Ok(value)
    }

    fn path(&self) -> PyResult<&str> {
        Self::extract_string_field(&self.path, "For fs/s3 storage, path must be specified")
    }

    fn table_name(&self) -> PyResult<&str> {
        Self::extract_string_field(
            &self.table_name,
            "For MongoDB, the 'table_name' field must be specified",
        )
    }

    fn database(&self) -> PyResult<&str> {
        Self::extract_string_field(
            &self.database,
            "For MongoDB, the 'database' field must be specified",
        )
    }

    fn connection_string(&self) -> PyResult<&str> {
        Self::extract_string_field(
            &self.connection_string,
            "For Postgres and MongoDB, the 'connection_string' field must be specified",
        )
    }

    fn azure_blob_storage_settings(&self) -> PyResult<AzureBlobStorageSettings> {
        let value = self
            .azure_blob_storage_settings
            .as_ref()
            .ok_or_else(|| {
                PyValueError::new_err(
                    "For Azure Blob Storage, 'azure_blob_storage_settings' field must be specified",
                )
            })?
            .clone();
        Ok(value)
    }

    fn s3_bucket(&self, py: pyo3::Python) -> PyResult<S3Bucket> {
        let (bucket_name, _) = S3Scanner::deduce_bucket_and_path(self.path()?);
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

    fn downloader_threads_count(&self) -> PyResult<usize> {
        if let Some(count) = self.downloader_threads_count {
            Ok(count)
        } else {
            let estimated = std::thread::available_parallelism().map_err(|e| {
                PyRuntimeError::new_err(format!(
                    "Failed to estimate the number of parallel downloaders to use: {e}"
                ))
            })?;
            let result: usize = std::convert::Into::<usize>::into(estimated);
            info!("S3 downloader defaults to {estimated} threads");
            Ok(result)
        }
    }

    fn iceberg_s3_storage_options(&self, py: pyo3::Python) -> HashMap<String, String> {
        let Some(ref settings) = self.aws_s3_settings else {
            return HashMap::new();
        };
        let settings = settings.borrow(py);
        let mut props = HashMap::new();
        if let Some(access_key) = &settings.access_key {
            props.insert(
                ::iceberg::io::S3_ACCESS_KEY_ID.to_string(),
                access_key.to_string(),
            );
        }
        if let Some(secret_access_key) = &settings.secret_access_key {
            props.insert(
                ::iceberg::io::S3_SECRET_ACCESS_KEY.to_string(),
                secret_access_key.to_string(),
            );
        }
        if let Some(session_token) = &settings.session_token {
            props.insert(
                ::iceberg::io::S3_SESSION_TOKEN.to_string(),
                session_token.to_string(),
            );
        }
        if settings.with_path_style {
            props.insert(
                ::iceberg::io::S3_PATH_STYLE_ACCESS.to_string(),
                "true".to_string(),
            );
        }
        if settings.profile.is_some() {
            warn!("Profile is specified in AWS Credentials, however this kind of authorization is not supported by the Iceberg connector");
        }
        if let s3::Region::Custom { endpoint, region } = &settings.region {
            if endpoint.starts_with("https://") || endpoint.starts_with("http://") {
                props.insert(::iceberg::io::S3_ENDPOINT.to_string(), endpoint.to_string());
            } else {
                props.insert(
                    ::iceberg::io::S3_ENDPOINT.to_string(),
                    format!("https://{endpoint}"),
                );
            }
            if region != endpoint {
                props.insert(::iceberg::io::S3_REGION.to_string(), region.to_string());
            }
        } else {
            props.insert(
                ::iceberg::io::S3_REGION.to_string(),
                settings.region.to_string(),
            );
        }
        props
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

        // If the starting timestamp is given, the positions
        // within the topic partitions will be reset lazily
        if self.start_from_timestamp_ms.is_some() {
            client_config.set("auto.offset.reset", "earliest");
        }

        Ok(client_config)
    }

    fn kafka_or_nats_topic(&self) -> PyResult<MessageQueueTopic> {
        if let Some(topic) = &self.topic {
            if self.topic_name_index.is_some() {
                Err(PyValueError::new_err(
                    "Either 'topic' or 'topic_name_index' must be defined, but not both",
                ))
            } else {
                Ok(MessageQueueTopic::Fixed(topic.to_string()))
            }
        } else if let Some(topic_name_index) = self.topic_name_index {
            Ok(MessageQueueTopic::Dynamic(topic_name_index))
        } else {
            Err(PyValueError::new_err(
                "Either 'topic' or 'topic_name_index' must be defined, but none is",
            ))
        }
    }

    fn kafka_or_nats_fixed_topic(&self) -> PyResult<String> {
        let topic = self.kafka_or_nats_topic()?;
        match topic {
            MessageQueueTopic::Fixed(t) => Ok(t),
            MessageQueueTopic::Dynamic(_) => Err(PyValueError::new_err(
                "Dynamic topics aren't supported for Kafka and NATS readers",
            )),
        }
    }

    fn build_csv_parser_settings(&self, py: pyo3::Python) -> CsvReaderBuilder {
        if let Some(parser_settings) = &self.csv_parser_settings {
            parser_settings.borrow(py).build_csv_reader_builder()
        } else {
            let mut builder = CsvReaderBuilder::new();
            builder.has_headers(false);
            builder
        }
    }

    fn construct_fs_reader(&self, is_persisted: bool) -> PyResult<(Box<dyn ReaderBuilder>, usize)> {
        let storage = new_filesystem_reader(
            self.path()?,
            self.mode,
            self.read_method,
            &self.object_pattern,
            is_persisted,
        )
        .map_err(|e| PyIOError::new_err(format!("Failed to initialize Filesystem reader: {e}")))?;
        Ok((Box::new(storage), 1))
    }

    fn construct_s3_reader(
        &self,
        py: pyo3::Python,
        is_persisted: bool,
    ) -> PyResult<(Box<dyn ReaderBuilder>, usize)> {
        let (_, deduced_path) = S3Scanner::deduce_bucket_and_path(self.path()?);
        let storage = new_s3_generic_reader(
            self.s3_bucket(py)?,
            deduced_path,
            self.mode,
            self.read_method,
            self.downloader_threads_count()?,
            is_persisted,
        )
        .map_err(|e| PyRuntimeError::new_err(format!("Creating S3 reader failed: {e}")))?;
        Ok((Box::new(storage), 1))
    }

    fn construct_s3_csv_reader(
        &self,
        py: pyo3::Python,
        is_persisted: bool,
    ) -> PyResult<(Box<dyn ReaderBuilder>, usize)> {
        let (_, deduced_path) = S3Scanner::deduce_bucket_and_path(self.path()?);
        let storage = new_s3_csv_reader(
            self.s3_bucket(py)?,
            deduced_path,
            self.build_csv_parser_settings(py),
            self.mode,
            self.downloader_threads_count()?,
            is_persisted,
        )
        .map_err(|e| PyRuntimeError::new_err(format!("Creating S3 reader failed: {e}")))?;
        Ok((Box::new(storage), 1))
    }

    fn construct_csv_reader(
        &self,
        py: pyo3::Python,
        is_persisted: bool,
    ) -> PyResult<(Box<dyn ReaderBuilder>, usize)> {
        let reader = new_csv_filesystem_reader(
            self.path()?,
            self.build_csv_parser_settings(py),
            self.mode,
            &self.object_pattern,
            is_persisted,
        )
        .map_err(|e| {
            PyIOError::new_err(format!("Failed to initialize CsvFilesystem reader: {e}"))
        })?;
        Ok((Box::new(reader), 1))
    }

    /// Returns the total number of partitions for a Kafka topic
    fn total_partitions_for_topic(consumer: &BaseConsumer, topic: &str) -> PyResult<usize> {
        let metadata = consumer
            .fetch_metadata(Some(topic), KafkaReader::default_timeout())
            .map_err(|e| PyIOError::new_err(format!("Failed to fetch topic metadata: {e}")))?;
        if let Some(topic) = metadata.topics().iter().find(|t| t.name() == topic) {
            Ok(topic.partitions().len())
        } else {
            Err(PyIOError::new_err(format!("Topic '{topic}' not found")))
        }
    }

    /// Returns an array of partition watermarks.
    /// Used to handle cases where a later call to `offsets_for_times`
    /// might return `KafkaOffset::End` for some partitions, allowing for graceful handling.
    /// Also used in static mode to identify the boundaries of the data chunk that needs to be read.
    fn kafka_partition_watermarks(
        consumer: &BaseConsumer,
        topic: &str,
        total_partitions: usize,
    ) -> PyResult<Vec<RdkafkaWatermark>> {
        let mut next_used_offset_per_partition = Vec::with_capacity(total_partitions);
        for partition_idx in 0..total_partitions {
            let (start_offset, next_offset) = consumer
                .fetch_watermarks(
                    topic,
                    partition_idx.try_into().unwrap(),
                    KafkaReader::default_timeout(),
                )
                .map_err(|e| {
                    PyIOError::new_err(format!(
                        "Failed to fetch watermarks for ({topic}, {partition_idx}): {e}"
                    ))
                })?;
            next_used_offset_per_partition.push(RdkafkaWatermark::new(start_offset, next_offset));
        }
        Ok(next_used_offset_per_partition)
    }

    fn kafka_seek_positions_for_timestamp(
        consumer: &BaseConsumer,
        topic: &str,
        total_partitions: usize,
        start_from_timestamp_ms: i64,
        watermarks: &[RdkafkaWatermark],
    ) -> PyResult<HashMap<i32, KafkaOffset>> {
        let mut seek_positions = HashMap::new();
        let mut tpl = TopicPartitionList::new();
        for partition_idx in 0..total_partitions {
            tpl.add_partition_offset(
                topic,
                partition_idx.try_into().unwrap(),
                KafkaOffset::Offset(start_from_timestamp_ms),
            )
            .expect("Failed to add partition offset");
        }

        let offsets = consumer
            .offsets_for_times(tpl, KafkaReader::default_timeout())
            .map_err(|e| {
                PyIOError::new_err(format!("Failed to fetch offsets for the timestamp: {e}"))
            })?;

        // We could have done a simple `consumer.assign` here, but it would damage the automatic consumer rebalance
        // So we act differently: we pass the seek positions to consumer, and it seeks lazily
        for element in offsets.elements() {
            assert_eq!(element.topic(), topic);
            let offset = match element.offset() {
                KafkaOffset::Invalid => {
                    return Err(PyRuntimeError::new_err(format!(
                        "rdkafka returned invalid offset, details: {offsets:?}"
                    )))
                }
                KafkaOffset::End => {
                    let partition_idx: usize = element.partition().try_into().unwrap();
                    info!("Partition {partition_idx} doesn't have messages with timestamp greater than {start_from_timestamp_ms}. All new messages will be read.");
                    KafkaOffset::Offset(watermarks[partition_idx].high)
                }
                offset => offset,
            };
            info!(
                "Adding a lazy seek position for ({topic}, {}) to ({:?})",
                element.partition(),
                offset
            );
            seek_positions.insert(element.partition(), offset);
        }
        Ok(seek_positions)
    }

    fn construct_kafka_reader(&self) -> PyResult<(Box<dyn ReaderBuilder>, usize)> {
        let client_config = self.kafka_client_config()?;

        let consumer: BaseConsumer = client_config
            .create()
            .map_err(|e| PyValueError::new_err(format!("Creating Kafka consumer failed: {e}")))?;

        let topic = &self.kafka_or_nats_fixed_topic()?;
        consumer
            .subscribe(&[topic])
            .map_err(|e| PyIOError::new_err(format!("Subscription to Kafka topic failed: {e}")))?;

        let total_partitions = Self::total_partitions_for_topic(&consumer, topic)?;
        let watermarks = Self::kafka_partition_watermarks(&consumer, topic, total_partitions)?;

        let mut seek_positions = HashMap::new();
        if let Some(start_from_timestamp_ms) = self.start_from_timestamp_ms {
            let current_timestamp = current_unix_timestamp_ms();
            if start_from_timestamp_ms > current_timestamp.try_into().unwrap() {
                warn!("The timestamp {start_from_timestamp_ms} is greater than the current timestamp {current_timestamp}. All new entries will be read.");
            }
            seek_positions = Self::kafka_seek_positions_for_timestamp(
                &consumer,
                topic,
                total_partitions,
                start_from_timestamp_ms,
                &watermarks,
            )?;
        }
        let reader = KafkaReader::new(
            consumer,
            topic.to_string(),
            seek_positions,
            watermarks,
            self.mode,
        );
        Ok((Box::new(reader), self.parallel_readers.unwrap_or(256)))
    }

    fn construct_python_reader(
        &self,
        py: pyo3::Python,
        data_format: &DataFormat,
    ) -> PyResult<(Box<dyn ReaderBuilder>, usize)> {
        let subject = self.python_subject.clone().ok_or_else(|| {
            PyValueError::new_err("For Python connector, python_subject should be specified")
        })?;

        if subject.borrow(py).is_internal && self.unique_name.is_some() {
            return Err(PyValueError::new_err(
                "Python connectors marked internal can't have unique names",
            ));
        }

        let reader = PythonReaderBuilder::new(subject, data_format.value_fields_type_map(py));
        Ok((Box::new(reader), 1))
    }

    fn construct_sqlite_reader(
        &self,
        py: pyo3::Python,
        data_format: &DataFormat,
    ) -> PyResult<(Box<dyn ReaderBuilder>, usize)> {
        let connection = SqliteConnection::open_with_flags(
            self.path()?,
            SqliteOpenFlags::SQLITE_OPEN_READ_ONLY | SqliteOpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to open Sqlite connection: {e}")))?;
        let table_name = self.table_name.clone().ok_or_else(|| {
            PyValueError::new_err("For Sqlite connector, table_name should be specified")
        })?;

        let reader = SqliteReader::new(
            connection,
            table_name,
            data_format.value_fields_type_map(py).into_iter().collect(),
        );
        Ok((Box::new(reader), 1))
    }

    fn object_downloader(&self, py: pyo3::Python) -> PyResult<ObjectDownloader> {
        if self.aws_s3_settings.is_some() {
            Ok(ObjectDownloader::S3(Box::new(self.s3_bucket(py)?)))
        } else {
            Ok(ObjectDownloader::Local)
        }
    }

    fn delta_storage_options(&self, py: pyo3::Python) -> PyResult<HashMap<String, String>> {
        if self.aws_s3_settings.is_some() {
            self.delta_s3_storage_options(py)
        } else {
            Ok(HashMap::new())
        }
    }

    fn construct_deltalake_reader(
        &self,
        py: pyo3::Python,
        data_format: &DataFormat,
        license: Option<&License>,
    ) -> PyResult<(Box<dyn ReaderBuilder>, usize)> {
        if let Some(license) = license {
            license.check_entitlements(["deltalake"])?;
        }
        let backfilling_thresholds = self.backfilling_thresholds.clone().unwrap_or_default();

        if self.start_from_timestamp_ms.is_some() && !backfilling_thresholds.is_empty() {
            return Err(PyValueError::new_err("The simultaneous use of 'start_from_timestamp_ms' and 'backfilling_thresholds' is not supported."));
        }
        let reader = DeltaTableReader::new(
            self.path()?,
            self.object_downloader(py)?,
            self.delta_storage_options(py)?,
            data_format.value_fields_type_map(py),
            self.mode,
            self.start_from_timestamp_ms,
            data_format.key_field_names.is_some(),
            backfilling_thresholds,
        )
        .map_err(|e| PyIOError::new_err(format!("Failed to connect to DeltaLake: {e}")))?;
        Ok((Box::new(reader), 1))
    }

    fn construct_nats_reader(
        &self,
        connector_index: usize,
        worker_index: usize,
    ) -> PyResult<(Box<dyn ReaderBuilder>, usize)> {
        let uri = self.path()?;
        let topic: String = self.kafka_or_nats_fixed_topic()?.to_string();
        let runtime = create_async_tokio_runtime()?;
        let subscriber = runtime.block_on(async {
            let consumer_queue = format!("pathway-reader-{connector_index}");
            let client = nats_connect(uri)
                .await
                .map_err(|e| PyIOError::new_err(format!("Failed to connect to NATS: {e}")))?;
            let subscriber = client
                .queue_subscribe(topic.clone(), consumer_queue) // Kafka "consumer group" equivalent to enable parallel reads
                .await
                .map_err(|e| {
                    PyIOError::new_err(format!("Failed to subscribe to NATS topic: {e}"))
                })?;
            Ok::<NatsSubscriber, PyErr>(subscriber)
        })?;
        let reader = NatsReader::new(runtime, subscriber, worker_index, topic);
        Ok((Box::new(reader), 32))
    }

    fn construct_iceberg_reader(
        &self,
        py: pyo3::Python,
        data_format: &DataFormat,
        license: Option<&License>,
    ) -> PyResult<(Box<dyn ReaderBuilder>, usize)> {
        if data_format.key_field_names.is_none() {
            return Err(PyValueError::new_err(
                "Iceberg reader requires explicit primary key fields specification",
            ));
        }

        if let Some(license) = license {
            license.check_entitlements(["iceberg"])?;
        }

        let uri = self.path()?;
        let warehouse = &self.database;
        let table_name = self.table_name()?;
        let namespace = self
            .namespace
            .clone()
            .ok_or_else(|| PyValueError::new_err("Namespace must be specified"))?;
        let mut value_fields = Vec::new();
        for field in &data_format.value_fields {
            value_fields.push(field.borrow(py).clone());
        }

        let db_params = IcebergDBParams::new(
            uri.to_string(),
            warehouse.clone(),
            namespace,
            self.iceberg_s3_storage_options(py),
        );
        let table_params =
            IcebergTableParams::new(table_name.to_string(), &value_fields).map_err(|e| {
                PyIOError::new_err(format!(
                    "Unable to create table params for Iceberg reader: {e}"
                ))
            })?;
        let reader = IcebergReader::new(
            &db_params,
            &table_params,
            data_format.value_fields_type_map(py),
            self.mode,
        )
        .map_err(|e| {
            PyIOError::new_err(format!("Unable to start data lake input connector: {e}"))
        })?;

        Ok((Box::new(reader), 1))
    }

    fn construct_reader(
        &self,
        py: pyo3::Python,
        data_format: &DataFormat,
        connector_index: usize,
        worker_index: usize,
        license: Option<&License>,
        is_persisted: bool,
    ) -> PyResult<(Box<dyn ReaderBuilder>, usize)> {
        match self.storage_type.as_ref() {
            "fs" => self.construct_fs_reader(is_persisted),
            "s3" => self.construct_s3_reader(py, is_persisted),
            "s3_csv" => self.construct_s3_csv_reader(py, is_persisted),
            "csv" => self.construct_csv_reader(py, is_persisted),
            "kafka" => self.construct_kafka_reader(),
            "python" => self.construct_python_reader(py, data_format),
            "sqlite" => self.construct_sqlite_reader(py, data_format),
            "deltalake" => self.construct_deltalake_reader(py, data_format, license),
            "nats" => self.construct_nats_reader(connector_index, worker_index),
            "iceberg" => self.construct_iceberg_reader(py, data_format, license),
            other => Err(PyValueError::new_err(format!(
                "Unknown data source {other:?}"
            ))),
        }
    }

    fn construct_persistent_storage_config(
        &self,
        py: pyo3::Python,
    ) -> PyResult<PersistentStorageConfig> {
        match self.storage_type.as_ref() {
            "fs" => Ok(PersistentStorageConfig::Filesystem(self.path()?.into())),
            "s3" => {
                let bucket = self.s3_bucket(py)?;
                let path = self.path()?;
                Ok(PersistentStorageConfig::S3 {
                    bucket: Box::new(bucket),
                    root_path: path.into(),
                })
            }
            "azure" => {
                let path = self.path()?;
                let azure_settings = self.azure_blob_storage_settings()?;
                Ok(PersistentStorageConfig::Azure {
                    credentials: azure_settings.credentials(),
                    account: azure_settings.account,
                    container: azure_settings.container,
                    root_path: path.into(),
                })
            }
            "mock" => {
                let mut events = HashMap::<ConnectorWorkerPair, Vec<SnapshotEvent>>::new();
                for ((unique_name, worker_id), es) in self.mock_events.as_ref().unwrap() {
                    let internal_persistent_id = unique_name.clone().into_persistent_id();
                    events.insert((internal_persistent_id, *worker_id), es.clone());
                }
                Ok(PersistentStorageConfig::Mock(events))
            }
            other => Err(PyValueError::new_err(format!(
                "Unsupported persistent storage format: {other:?}"
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

    fn construct_fs_writer(&self) -> PyResult<Box<dyn Writer>> {
        let path = self.path()?;
        let storage = {
            let file = File::create(path);
            match file {
                Ok(f) => {
                    let buf_writer = BufWriter::new(f);
                    FileWriter::new(buf_writer, path.to_string())
                }
                Err(_) => return Err(PyIOError::new_err("Filesystem operation (create) failed")),
            }
        };
        Ok(Box::new(storage))
    }

    fn construct_kafka_writer(&self) -> PyResult<Box<dyn Writer>> {
        let client_config = self.kafka_client_config()?;

        let producer: ThreadedProducer<DefaultProducerContext> = match client_config.create() {
            Ok(producer) => producer,
            Err(_) => return Err(PyIOError::new_err("Producer creation failed")),
        };

        let topic = self.kafka_or_nats_topic()?;
        let writer = KafkaWriter::new(
            producer,
            topic,
            self.header_fields.clone(),
            self.key_field_index,
        );

        Ok(Box::new(writer))
    }

    fn construct_postgres_writer(
        &self,
        py: pyo3::Python,
        data_format: &DataFormat,
    ) -> PyResult<Box<dyn Writer>> {
        let connection_string = self.connection_string()?;
        let storage = match Client::connect(connection_string, NoTls) {
            Ok(client) => PsqlWriter::new(
                client,
                self.max_batch_size,
                self.snapshot_maintenance_on_output,
                self.table_name()?,
                &data_format.value_fields_type_map(py),
                &data_format.key_field_names,
                self.sql_writer_init_mode,
            )
            .map_err(|e| {
                PyIOError::new_err(format!("Unable to initialize PostgreSQL table: {e}"))
            })?,
            Err(e) => {
                return Err(PyIOError::new_err(format!(
                    "Failed to establish PostgreSQL connection: {e:?}"
                )))
            }
        };
        Ok(Box::new(storage))
    }

    fn construct_elasticsearch_writer(&self, py: pyo3::Python) -> PyResult<Box<dyn Writer>> {
        let elasticsearch_client_params = self.elasticsearch_client_params(py)?;
        let client = elasticsearch_client_params.client(py)?;
        let index_name = elasticsearch_client_params.index_name.clone();
        let max_batch_size = self.max_batch_size;

        let writer = ElasticSearchWriter::new(client, index_name, max_batch_size);
        Ok(Box::new(writer))
    }

    fn construct_deltalake_writer(
        &self,
        py: pyo3::Python,
        data_format: &DataFormat,
    ) -> PyResult<Box<dyn Writer>> {
        let path = self.path()?;
        let mut value_fields = Vec::new();
        let partition_columns = self.partition_columns.clone().unwrap_or_default();
        for field in &data_format.value_fields {
            value_fields.push(field.borrow(py).clone());
        }

        let table_type = if self.snapshot_maintenance_on_output {
            MaintenanceMode::Snapshot
        } else {
            MaintenanceMode::StreamOfChanges
        };
        let batch_writer = DeltaBatchWriter::new(
            path,
            &value_fields,
            self.delta_storage_options(py)?,
            partition_columns,
            table_type,
        )
        .map_err(|e| {
            let error_text = format!("Unable to create DeltaTable writer: {e}");
            match e {
                WriteError::DeltaTableSchemaMismatch(_) => PyTypeError::new_err(error_text),
                _ => PyIOError::new_err(error_text),
            }
        })?;

        let schema = construct_arrow_schema(&value_fields, &batch_writer, table_type)
            .map_err(|e| PyIOError::new_err(format!("Failed to construct table schema: {e}")))?;
        let buffer: Box<dyn ColumnBuffer> = if self.snapshot_maintenance_on_output {
            Box::new(
                SnapshotColumnBuffer::new_for_delta_table(
                    path,
                    self.delta_storage_options(py)?,
                    &value_fields,
                    Arc::new(schema),
                )
                .map_err(|e| {
                    PyIOError::new_err(format!("Failed to create snapshot writer: {e}"))
                })?,
            )
        } else {
            Box::new(AppendOnlyColumnBuffer::new(Arc::new(schema)))
        };
        let writer = LakeWriter::new(
            Box::new(batch_writer),
            buffer,
            self.min_commit_frequency.map(time::Duration::from_millis),
        )
        .map_err(|e| {
            PyIOError::new_err(format!("Unable to start data lake output connector: {e}"))
        })?;
        Ok(Box::new(writer))
    }

    fn construct_iceberg_writer(
        &self,
        py: pyo3::Python,
        data_format: &DataFormat,
    ) -> PyResult<Box<dyn Writer>> {
        if self.snapshot_maintenance_on_output {
            return Err(PyNotImplementedError::new_err(
                "Snapshot mode is not implemented for Apache Iceberg output",
            ));
        }

        let uri = self.path()?;
        let warehouse = &self.database;
        let table_name = self.table_name()?;
        let namespace = self
            .namespace
            .clone()
            .ok_or_else(|| PyValueError::new_err("Namespace must be specified"))?;
        let mut value_fields = Vec::new();
        for field in &data_format.value_fields {
            value_fields.push(field.borrow(py).clone());
        }

        let db_params = IcebergDBParams::new(
            uri.to_string(),
            warehouse.clone(),
            namespace,
            self.iceberg_s3_storage_options(py),
        );
        let table_params =
            IcebergTableParams::new(table_name.to_string(), &value_fields).map_err(|e| {
                PyIOError::new_err(format!(
                    "Unable to create table params for Iceberg writer: {e}"
                ))
            })?;
        let batch_writer = IcebergBatchWriter::new(&db_params, &table_params).map_err(|e| {
            PyIOError::new_err(format!(
                "Unable to create batch writer for Iceberg writer: {e}"
            ))
        })?;
        let schema = construct_arrow_schema(
            &value_fields,
            &batch_writer,
            MaintenanceMode::StreamOfChanges, // Snapshot mode is not implemented for Iceberg
        )
        .map_err(|e| PyIOError::new_err(format!("Failed to construct table schema: {e}")))?;
        let buffer = AppendOnlyColumnBuffer::new(Arc::new(schema));
        let writer = LakeWriter::new(
            Box::new(batch_writer),
            Box::new(buffer),
            self.min_commit_frequency.map(time::Duration::from_millis),
        )
        .map_err(|e| {
            PyIOError::new_err(format!("Unable to start data lake output connector: {e}"))
        })?;
        Ok(Box::new(writer))
    }

    fn construct_nats_writer(&self) -> PyResult<Box<dyn Writer>> {
        let uri = self.path()?;
        let topic = self.kafka_or_nats_topic()?;
        let runtime = create_async_tokio_runtime()?;
        let client = runtime.block_on(async {
            let client = nats_connect(uri)
                .await
                .map_err(|e| PyIOError::new_err(format!("Failed to connect to NATS: {e}")))?;
            Ok::<NatsClient, PyErr>(client)
        })?;
        let writer = NatsWriter::new(runtime, client, topic, self.header_fields.clone());
        Ok(Box::new(writer))
    }

    fn construct_mongodb_writer(&self) -> PyResult<Box<dyn Writer>> {
        let uri = self.connection_string()?;
        let client = MongoClient::with_uri_str(uri)
            .map_err(|e| PyIOError::new_err(format!("Failed to connect to MongoDB: {e}")))?;
        let database = client.database(self.database()?);
        let collection = database.collection(self.table_name()?);
        let writer = MongoWriter::new(collection, self.max_batch_size);
        Ok(Box::new(writer))
    }

    fn construct_writer(
        &self,
        py: pyo3::Python,
        data_format: &DataFormat,
    ) -> PyResult<Box<dyn Writer>> {
        match self.storage_type.as_ref() {
            "fs" => self.construct_fs_writer(),
            "kafka" => self.construct_kafka_writer(),
            "postgres" => self.construct_postgres_writer(py, data_format),
            "elasticsearch" => self.construct_elasticsearch_writer(py),
            "deltalake" => self.construct_deltalake_writer(py, data_format),
            "mongodb" => self.construct_mongodb_writer(),
            "null" => Ok(Box::new(NullWriter::new())),
            "nats" => self.construct_nats_writer(),
            "iceberg" => self.construct_iceberg_writer(py, data_format),
            other => Err(PyValueError::new_err(format!(
                "Unknown data sink {other:?}"
            ))),
        }
    }
}

impl DataFormat {
    pub fn value_fields_type_map(&self, py: pyo3::Python) -> HashMap<String, Type> {
        let mut result = HashMap::with_capacity(self.value_fields.len());
        for field in &self.value_fields {
            let name = field.borrow(py).name.clone();
            let type_ = field.borrow(py).type_.clone();
            result.insert(name, type_);
        }
        result
    }

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

    fn schema(&self, py: pyo3::Python) -> PyResult<HashMap<String, InnerSchemaField>> {
        let mut types = HashMap::new();
        for field in &self.value_fields {
            let borrowed_field = field.borrow(py);
            types.insert(
                borrowed_field.name.clone(),
                borrowed_field.as_inner_schema_field(),
            );
        }
        for name in self.key_field_names.as_ref().unwrap_or(&vec![]) {
            if !types.contains_key(name) {
                return Err(PyValueError::new_err(format!(
                    "key field {name} not found in schema"
                )));
            }
        }
        Ok(types)
    }

    fn construct_parser(&self, py: pyo3::Python) -> PyResult<Box<dyn Parser>> {
        match self.format_type.as_ref() {
            "dsv" => {
                let settings = self.construct_dsv_settings(py)?;
                Ok(settings.parser(self.schema(py)?)?)
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
                    self.schema(py)?,
                    self.session_type,
                )?;
                Ok(Box::new(parser))
            }
            "identity" => Ok(Box::new(IdentityParser::new(
                self.value_field_names(py),
                self.parse_utf8,
                self.key_generation_policy,
                self.session_type,
            ))),
            "transparent" => Ok(Box::new(TransparentParser::new(
                self.key_field_names.clone(),
                self.value_field_names(py),
                self.schema(py)?,
                self.session_type,
            )?)),
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
                let key_field_names = self
                    .key_field_names
                    .clone()
                    .filter(|k| !k.is_empty())
                    .ok_or_else(|| PyValueError::new_err("Primary key must be specified"))?;
                let maybe_formatter = PsqlSnapshotFormatter::new(
                    self.table_name()?,
                    key_field_names,
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
            "single_column" => {
                let index = self
                    .value_field_index
                    .ok_or_else(|| PyValueError::new_err("Payload column not specified"))?;
                let formatter = SingleColumnFormatter::new(index);
                Ok(Box::new(formatter))
            }
            "identity" => {
                let formatter = IdentityFormatter::new();
                Ok(Box::new(formatter))
            }
            "bson" => {
                let formatter = BsonFormatter::new(self.value_field_names(py));
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
        let trace = trace.map_or(Ok(EngineTrace::Empty), |t| t.extract(py))?;
        let inner = Arc::new(EngineColumnProperties {
            append_only,
            dtype: dtype.extract(py)?,
            trace: Arc::new(trace),
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
        trace: Option<Py<Trace>>,
    ) -> PyResult<Py<Self>> {
        let column_properties: Vec<_> = column_properties
            .into_iter()
            .map(|(path, props)| (path, props.0))
            .collect();
        let trace = trace.map_or(Ok(EngineTrace::Empty), |t| t.extract(py))?;
        let table_properties = EngineTableProperties::from_paths(
            column_properties
                .into_iter()
                .map(|(path, column_properties)| {
                    (path, EngineTableProperties::Column(column_properties))
                })
                .collect(),
            &Arc::new(trace),
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
    #[pyo3(get)]
    unique_name: Option<UniqueName>,
    #[pyo3(get)]
    synchronization_group: Option<ConnectorGroupDescriptor>,
}

#[pymethods]
impl ConnectorProperties {
    #[new]
    #[pyo3(signature = (
        commit_duration_ms = None,
        unsafe_trusted_ids = false,
        column_properties = vec![],
        unique_name = None,
        synchronization_group = None,
    ))]
    fn new(
        commit_duration_ms: Option<u64>,
        unsafe_trusted_ids: bool,
        #[pyo3(from_py_with = "from_py_iterable")] column_properties: Vec<ColumnProperties>,
        unique_name: Option<String>,
        synchronization_group: Option<ConnectorGroupDescriptor>,
    ) -> Self {
        Self {
            commit_duration_ms,
            unsafe_trusted_ids,
            column_properties,
            unique_name,
            synchronization_group,
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

impl<'py> FromPyObject<'py> for EngineTrace {
    fn extract_bound(obj: &Bound<'py, PyAny>) -> PyResult<Self> {
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

// submodule to make sure no other code can create instances of `Done`
mod done {
    use once_cell::sync::Lazy;
    use pyo3::prelude::*;

    struct InnerDone;

    #[pyclass(module = "pathway.engine", frozen)]
    pub struct Done(InnerDone);

    pub static DONE: Lazy<Py<Done>> = Lazy::new(|| {
        Python::with_gil(|py| Py::new(py, Done(InnerDone)).expect("creating DONE should not fail"))
    });
}
use done::{Done, DONE};

#[pymethods]
impl Done {
    pub fn __hash__(self_: &Bound<Self>) -> usize {
        // mimic the default Python hash
        (self_.as_ptr() as usize).rotate_right(4)
    }

    pub fn __richcmp__(self_: &Bound<Self>, other: &Bound<PyAny>, op: CompareOp) -> Py<PyAny> {
        let py = other.py();
        if other.is_instance_of::<Self>() {
            assert!(self_.is(other));
            op.matches(Ordering::Equal).into_py(py)
        } else if other.is_instance_of::<PyInt>() {
            op.matches(Ordering::Greater).into_py(py)
        } else {
            py.NotImplemented()
        }
    }
}

impl<'py, T> FromPyObject<'py> for TotalFrontier<T>
where
    T: FromPyObject<'py>,
{
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        if ob.is_instance_of::<Done>() {
            Ok(TotalFrontier::Done)
        } else {
            Ok(TotalFrontier::At(ob.extract()?))
        }
    }
}

impl<T> IntoPy<PyObject> for TotalFrontier<T>
where
    T: IntoPy<PyObject>,
{
    fn into_py(self, py: Python<'_>) -> PyObject {
        match self {
            Self::At(i) => i.into_py(py),
            Self::Done => DONE.clone_ref(py).into_py(py),
        }
    }
}

impl<T> ToPyObject for TotalFrontier<T>
where
    T: ToPyObject,
{
    fn to_object(&self, py: Python<'_>) -> PyObject {
        match self {
            Self::At(i) => i.to_object(py),
            Self::Done => DONE.clone_ref(py).into_py(py),
        }
    }
}

#[pyclass(module = "pathway.engine", frozen, name = "ExpportedTable")]
pub struct PyExportedTable {
    inner: Arc<dyn ExportedTable>,
}

impl PyExportedTable {
    fn new(inner: Arc<dyn ExportedTable>) -> Self {
        Self { inner }
    }
}

#[pymethods]
impl PyExportedTable {
    fn frontier(&self) -> TotalFrontier<Timestamp> {
        self.inner.frontier()
    }

    fn snapshot_at(&self, frontier: TotalFrontier<Timestamp>) -> Vec<(Key, Vec<Value>)> {
        self.inner.snapshot_at(frontier)
    }

    fn failed(&self) -> bool {
        self.inner.failed()
    }
}

#[allow(clippy::struct_field_names)]
struct WakeupHandler<'py> {
    _fd: OwnedFd,
    set_wakeup_fd: Bound<'py, PyAny>,
    old_wakeup_fd: Bound<'py, PyAny>,
}

impl<'py> WakeupHandler<'py> {
    fn new(py: Python<'py>, fd: OwnedFd) -> PyResult<Option<Self>> {
        let signal_module = py.import_bound(intern!(py, "signal"))?;
        let set_wakeup_fd = signal_module.getattr(intern!(py, "set_wakeup_fd"))?;
        let old_wakeup_fd = set_wakeup_fd.call1((fd.as_raw_fd(),));
        if let Err(ref error) = old_wakeup_fd {
            if error.is_instance_of::<PyValueError>(py) {
                // We are not the main thread. This means we can ignore signal handling.
                return Ok(None);
            }
        }
        let old_wakeup_fd = old_wakeup_fd?;
        let res = Some(Self {
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
        self.set_wakeup_fd
            .call1((&self.old_wakeup_fd,))
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

static LOGGING_RESET_HANDLE: Lazy<ResetHandle> = Lazy::new(logging::init);

impl From<LicenseError> for PyErr {
    fn from(error: LicenseError) -> Self {
        let message = error.to_string();
        PyRuntimeError::new_err(message)
    }
}

#[pyfunction]
#[pyo3(signature = (
    *,
    license_key,
    entitlements,
))]
fn check_entitlements(license_key: Option<String>, entitlements: Vec<String>) -> PyResult<()> {
    License::new(license_key)?.check_entitlements(entitlements)?;
    Ok(())
}

#[pymodule]
#[pyo3(name = "engine")]
fn engine(_py: Python<'_>, m: &Bound<PyModule>) -> PyResult<()> {
    // Initialize the logging
    let _ = Lazy::force(&LOGGING_RESET_HANDLE);

    // Enable S3 support in DeltaLake library
    deltalake::aws::register_handlers(None);

    m.add_class::<Pointer>()?;
    m.add_class::<PyObjectWrapper>()?;
    m.add_class::<PyReducer>()?;
    m.add_class::<PyReducerData>()?;
    m.add_class::<PyUnaryOperator>()?;
    m.add_class::<PyBinaryOperator>()?;
    m.add_class::<PyExpression>()?;
    m.add_class::<PyExpressionData>()?;
    m.add_class::<PathwayType>()?;
    m.add_class::<PyConnectorMode>()?;
    m.add_class::<PySessionType>()?;
    m.add_class::<PyPythonConnectorEventType>()?;
    m.add_class::<PyDebeziumDBType>()?;
    m.add_class::<PyKeyGenerationPolicy>()?;
    m.add_class::<PyReadMethod>()?;
    m.add_class::<PyMonitoringLevel>()?;
    m.add_class::<PySqlWriterInitMode>()?;
    m.add_class::<Universe>()?;
    m.add_class::<Column>()?;
    m.add_class::<LegacyTable>()?;
    m.add_class::<Table>()?;
    m.add_class::<DataRow>()?;
    m.add_class::<Computer>()?;
    m.add_class::<Scope>()?;
    m.add_class::<Context>()?;

    m.add_class::<AwsS3Settings>()?;
    m.add_class::<AzureBlobStorageSettings>()?;
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
    m.add_class::<PyConnectorGroupDescriptor>()?;
    m.add_class::<TelemetryConfig>()?;
    m.add_class::<BackfillingThreshold>()?;

    m.add_class::<ConnectorProperties>()?;
    m.add_class::<ColumnProperties>()?;
    m.add_class::<TableProperties>()?;
    m.add_class::<Trace>()?;
    m.add_class::<Done>()?;
    m.add_class::<PyExportedTable>()?;
    m.add_class::<Error>()?;
    m.add_class::<Pending>()?;

    m.add_class::<PyExternalIndexFactory>()?;
    m.add_class::<PyExternalIndexData>()?;
    m.add_class::<PyExternalIndexQuery>()?;
    m.add_class::<PyUSearchMetricKind>()?;
    m.add_class::<PyBruteForceKnnMetricKind>()?;

    m.add_function(wrap_pyfunction!(run_with_new_graph, m)?)?;
    m.add_function(wrap_pyfunction!(ref_scalar, m)?)?;
    m.add_function(wrap_pyfunction!(ref_scalar_with_instance, m)?)?;
    #[allow(clippy::unsafe_removed_from_name)] // false positive
    m.add_function(wrap_pyfunction!(unsafe_make_pointer, m)?)?;
    m.add_function(wrap_pyfunction!(check_entitlements, m)?)?;
    m.add_function(wrap_pyfunction!(deserialize, m)?)?;
    m.add_function(wrap_pyfunction!(serialize, m)?)?;

    m.add("MissingValueError", &*MISSING_VALUE_ERROR_TYPE)?;
    m.add("EngineError", &*ENGINE_ERROR_TYPE)?;
    m.add("EngineErrorWithTrace", &*ENGINE_ERROR_WITH_TRACE_TYPE)?;
    m.add("OtherWorkerError", &*OTHER_WORKER_ERROR)?;

    m.add("DONE", &*DONE)?;
    m.add("ERROR", &*ERROR)?;
    m.add("PENDING", &*PENDING)?;

    Ok(())
}
