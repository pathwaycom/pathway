// Copyright © 2026 Pathway

use pyo3::prelude::*;

use std::fmt::{self, Display};

use pyo3::intern;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyBytes, PyModule};
use serde::{ser::Error, Deserialize, Serialize};

use super::error::DynResult;

static PICKLE: PyOnceLock<Py<PyModule>> = PyOnceLock::new();

fn get_pickle_module(py: Python<'_>) -> &Bound<'_, PyModule> {
    PICKLE
        .get_or_init(py, || py.import("pickle").unwrap().unbind())
        .bind(py)
}

fn python_dumps<'py>(
    serializer: &Bound<'py, PyAny>,
    object: &Py<PyAny>,
) -> PyResult<Bound<'py, PyBytes>> {
    let py = serializer.py();
    serializer
        .call_method1(intern!(py, "dumps"), (object,))?
        .extract()
        .map_err(Into::into)
}

fn python_loads<'py>(serializer: &Bound<'py, PyAny>, bytes: &[u8]) -> PyResult<Bound<'py, PyAny>> {
    let py = serializer.py();
    serializer.call_method1(intern!(py, "loads"), (bytes,))
}

#[derive(Debug)]
pub struct PyObjectWrapper {
    object: Py<PyAny>,
    serializer: Option<Py<PyAny>>,
}

impl PyObjectWrapper {
    pub fn new(object: Py<PyAny>, serializer: Option<Py<PyAny>>) -> Self {
        Self { object, serializer }
    }

    pub fn get_inner(&self, py: Python<'_>) -> Py<PyAny> {
        self.object.clone_ref(py)
    }

    pub fn get_serializer(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.serializer
            .as_ref()
            .map(|serializer| serializer.clone_ref(py))
    }

    pub fn as_bytes(&self) -> DynResult<Vec<u8>> {
        Ok(Python::attach(|py| {
            let serializer = match self.serializer.as_ref() {
                Some(serializer) => serializer.bind(py),
                None => get_pickle_module(py),
            };
            python_dumps(serializer, &self.object)?.extract()
        })?)
    }

    pub fn from_bytes(bytes: &[u8], serializer: Option<Py<PyAny>>) -> DynResult<Self> {
        let object = Python::attach(|py| -> PyResult<_> {
            let serializer = match serializer.as_ref() {
                Some(serializer) => serializer.bind(py),
                None => get_pickle_module(py),
            };
            let object = python_loads(serializer, bytes)?;
            Ok(object.unbind())
        })?;
        Ok(Self::new(object, serializer))
    }
}

impl Display for PyObjectWrapper {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(fmt, "PyObjectWrapper({})", self.object)
    }
}

#[derive(Serialize, Deserialize)]
struct PyObjectWrapperIntermediate {
    object: Vec<u8>,
    serializer: Vec<u8>,
}

impl Serialize for PyObjectWrapper {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let own_serializer_bytes = match self.serializer.as_ref() {
            Some(serializer) => {
                Python::attach(|py| python_dumps(get_pickle_module(py), serializer)?.extract())
            }
            None => Ok(vec![]),
        }
        .map_err(S::Error::custom)?;
        let intermediate = PyObjectWrapperIntermediate {
            object: self.as_bytes().map_err(S::Error::custom)?,
            serializer: own_serializer_bytes,
        };
        intermediate.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for PyObjectWrapper {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let intermediate = PyObjectWrapperIntermediate::deserialize(deserializer)?;
        let own_serializer: Option<Py<PyAny>> = if intermediate.serializer.is_empty() {
            None
        } else {
            Some(Python::attach(|py| {
                python_loads(get_pickle_module(py), &intermediate.serializer)
                    .map_err(serde::de::Error::custom)
                    .map(Bound::unbind)
            })?)
        };
        Self::from_bytes(&intermediate.object, own_serializer).map_err(serde::de::Error::custom)
    }
}
