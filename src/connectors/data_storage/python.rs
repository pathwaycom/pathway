// Copyright © 2026 Pathway

use pyo3::exceptions::PyValueError;
use pyo3::types::PyBytes;
use std::any::type_name;
use std::borrow::Borrow;
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use log::warn;

use crate::connectors::{Offset, OffsetKey, OffsetValue};
use crate::engine::error::limit_length;
use crate::engine::error::STANDARD_OBJECT_LENGTH_LIMIT;
use crate::engine::{Type, Value};
use crate::persistence::frontier::OffsetAntichain;
use crate::persistence::UniqueName;
use crate::python_api::extract_value;
use crate::python_api::threads::PythonThreadState;
use crate::python_api::PythonSubject;

use super::{
    CommitPossibility, ConversionError, DataEventType, PythonConnectorEventType, ReadError,
    ReadResult, Reader, ReaderBuilder, ReaderContext, SpecialEvent, StorageType, ValuesMap,
};
use pyo3::prelude::*;

pub struct PythonReaderBuilder {
    subject: Py<PythonSubject>,
    schema: HashMap<String, Type>,
}

pub struct PythonReader {
    subject: Py<PythonSubject>,
    schema: HashMap<String, Type>,
    total_entries_read: u64,
    current_external_offset: Arc<[u8]>,
    is_initialized: bool,
    is_finished: bool,

    #[allow(unused)]
    python_thread_state: PythonThreadState,
}

impl PythonReaderBuilder {
    pub fn new(subject: Py<PythonSubject>, schema: HashMap<String, Type>) -> Self {
        Self { subject, schema }
    }
}

impl ReaderBuilder for PythonReaderBuilder {
    fn build(self: Box<Self>) -> Result<Box<dyn Reader>, ReadError> {
        let python_thread_state = PythonThreadState::new();
        let Self { subject, schema } = *self;

        Ok(Box::new(PythonReader {
            subject,
            schema,
            python_thread_state,
            total_entries_read: 0,
            is_initialized: false,
            is_finished: false,
            current_external_offset: vec![].into(),
        }))
    }

    fn short_description(&self) -> Cow<'static, str> {
        type_name::<Self>().into()
    }

    fn name(&self, unique_name: Option<&UniqueName>) -> String {
        if let Some(unique_name) = unique_name {
            unique_name.clone()
        } else {
            let desc = self.short_description();
            desc.split("::").last().unwrap().replace("Builder", "")
        }
    }

    fn is_internal(&self) -> bool {
        self.subject.get().is_internal
    }

    fn storage_type(&self) -> StorageType {
        StorageType::Python
    }
}

impl PythonReader {
    fn conversion_error(
        ob: &Bound<PyAny>,
        name: String,
        type_: Type,
        err: &PyErr,
    ) -> ConversionError {
        let value_repr = limit_length(format!("{ob}"), STANDARD_OBJECT_LENGTH_LIMIT);
        ConversionError::new(value_repr, name, type_, Some(err.to_string()))
    }

    fn current_offset(&self) -> Offset {
        (
            OffsetKey::Empty,
            OffsetValue::PythonCursor {
                total_entries_read: self.total_entries_read,
                raw_external_offset: self.current_external_offset.clone(),
            },
        )
    }
}

const PW_OFFSET_FIELD_NAME: &str = "_pw_offset";

impl Reader for PythonReader {
    fn seek(&mut self, frontier: &OffsetAntichain) -> Result<(), ReadError> {
        Python::with_gil(|py| {
            self.subject
                .borrow(py)
                .on_persisted_run
                .call0(py)
                .map_err(ReadError::Py)
        })?;

        let offset_value = frontier.get_offset(&OffsetKey::Empty);
        let Some(OffsetValue::PythonCursor {
            total_entries_read,
            raw_external_offset,
        }) = offset_value
        else {
            if offset_value.is_some() {
                warn!("Incorrect type of offset value in Python frontier: {offset_value:?}");
            }
            return Ok(());
        };

        self.total_entries_read = *total_entries_read;
        if !raw_external_offset.is_empty() {
            Python::with_gil(|py| {
                let data: Vec<u8> = raw_external_offset.to_vec();
                let py_external_offset = PyBytes::new(py, &data).unbind().into_any();
                self.subject
                    .borrow(py)
                    .seek
                    .call1(py, (py_external_offset.borrow(),))
                    .map_err(ReadError::Py)
            })?;
            self.current_external_offset = raw_external_offset.clone();
        }

        Ok(())
    }

    fn read(&mut self) -> Result<ReadResult, ReadError> {
        if !self.is_initialized {
            Python::with_gil(|py| self.subject.borrow(py).start.call0(py))?;
            self.is_initialized = true;
        }
        if self.is_finished {
            return Ok(ReadResult::Finished);
        }

        Python::with_gil(|py| {
            let (py_event, key, objects): (
                PythonConnectorEventType,
                Option<Value>,
                HashMap<String, Py<PyAny>>,
            ) = self
                .subject
                .borrow(py)
                .read
                .call0(py)?
                .extract(py)
                .map_err(ReadError::Py)?;

            let event = match py_event {
                PythonConnectorEventType::Insert => DataEventType::Insert,
                PythonConnectorEventType::Delete => DataEventType::Delete,
                PythonConnectorEventType::ExternalOffset => {
                    let py_external_offset =
                        objects.get(PW_OFFSET_FIELD_NAME).unwrap_or_else(|| {
                            panic!(
                                "In ExternalOffset event '{PW_OFFSET_FIELD_NAME}' must be present"
                            )
                        });
                    self.current_external_offset = py_external_offset
                        .extract::<Vec<u8>>(py)
                        .expect("ExternalOffset must be bytes")
                        .into();
                    return Ok(ReadResult::Data(
                        ReaderContext::Empty,
                        self.current_offset(),
                    ));
                }
            };

            let key = key.map(|key| vec![key]);
            let mut values = HashMap::with_capacity(objects.len());
            for (name, ob) in objects {
                let dtype = self.schema.get(&name).unwrap_or(&Type::Any); // Any for special values
                let value = extract_value(ob.bind(py), dtype).map_err(|err| {
                    Box::new(Self::conversion_error(
                        ob.bind(py),
                        name.clone(),
                        dtype.clone(),
                        &err,
                    ))
                });
                values.insert(name, value);
            }
            let values: ValuesMap = values.into();

            if event != DataEventType::Insert && !self.subject.borrow(py).deletions_enabled {
                return Err(ReadError::Py(PyValueError::new_err(
                    "Trying to modify a row in the Python connector but deletions_enabled is set to False.",
                )));
            }

            if let Some(special_value) = values.get_special() {
                match special_value {
                    SpecialEvent::Finish => {
                        self.is_finished = true;
                        self.subject.borrow(py).end.call0(py)?;
                        return Ok(ReadResult::Finished);
                    }
                    SpecialEvent::EnableAutocommits => {
                        return Ok(ReadResult::FinishedSource {
                            commit_possibility: CommitPossibility::Possible,
                        })
                    }
                    SpecialEvent::DisableAutocommits => {
                        return Ok(ReadResult::FinishedSource {
                            commit_possibility: CommitPossibility::Forbidden,
                        })
                    }
                    SpecialEvent::Commit => {}
                }
            }
            // We use simple sequential offset because Python connector is single threaded, as
            // by default.
            //
            // If it's changed, add worker_id to the offset.
            self.total_entries_read += 1;
            Ok(ReadResult::Data(
                ReaderContext::from_diff(event, key, values),
                self.current_offset(),
            ))
        })
    }

    fn storage_type(&self) -> StorageType {
        StorageType::Python
    }
}
