// Copyright © 2024 Pathway

use std::fmt::{self, Display};

use pyo3::{sync::GILOnceCell, PyAny, PyObject, Python, ToPyObject};
use serde::{ser::Error, Deserialize, Serialize};

use super::error::DynResult;

static PICKLE: GILOnceCell<PyObject> = GILOnceCell::new();

fn get_pickle_module(py: Python<'_>) -> &PyAny {
    PICKLE
        .get_or_init(py, || py.import("pickle").unwrap().to_object(py))
        .as_ref(py)
}

fn python_dumps(serializer: &PyAny, object: &PyObject) -> DynResult<Vec<u8>> {
    Ok(serializer.call_method1("dumps", (object,))?.extract()?)
}

fn python_loads(serializer: &PyAny, bytes: &[u8]) -> DynResult<PyObject> {
    Ok(serializer.call_method1("loads", (bytes,))?.into())
}

#[derive(Debug)]
pub struct PyObjectWrapper {
    object: PyObject,
    serializer: Option<PyObject>,
}

impl PyObjectWrapper {
    pub fn new(object: PyObject, serializer: Option<PyObject>) -> Self {
        Self { object, serializer }
    }

    pub fn get_inner(&self, py: Python<'_>) -> PyObject {
        self.object.clone_ref(py)
    }

    pub fn get_serializer(&self, py: Python<'_>) -> Option<PyObject> {
        self.serializer
            .as_ref()
            .map(|serializer| serializer.clone_ref(py))
    }

    pub fn as_bytes(&self) -> DynResult<Vec<u8>> {
        Python::with_gil(|py| {
            let serializer = match self.serializer.as_ref() {
                Some(serializer) => serializer.as_ref(py),
                None => get_pickle_module(py),
            };
            python_dumps(serializer, &self.object)
        })
    }

    pub fn from_bytes(bytes: &[u8], serializer: Option<PyObject>) -> DynResult<Self> {
        let object = Python::with_gil(|py| {
            let serializer = match serializer.as_ref() {
                Some(serializer) => serializer.as_ref(py),
                None => get_pickle_module(py),
            };
            python_loads(serializer, bytes)
        });
        Ok(Self::new(object?, serializer))
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
        let own_serializer_bytes: DynResult<_> = match self.serializer.as_ref() {
            Some(serializer) => {
                Python::with_gil(|py| python_dumps(get_pickle_module(py), serializer))
            }
            None => Ok(vec![]),
        };
        let intermediate = PyObjectWrapperIntermediate {
            object: self.as_bytes().map_err(S::Error::custom)?,
            serializer: own_serializer_bytes.map_err(S::Error::custom)?,
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
        let own_serializer: Option<PyObject> = if intermediate.serializer.is_empty() {
            None
        } else {
            Some(Python::with_gil(|py| {
                python_loads(get_pickle_module(py), &intermediate.serializer)
                    .map_err(serde::de::Error::custom)
            })?)
        };
        Self::from_bytes(&intermediate.object, own_serializer).map_err(serde::de::Error::custom)
    }
}
