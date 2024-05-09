// Copyright © 2024 Pathway

use std::sync::Arc;

use pyo3::{
    pyclass, pymethods, FromPyObject, IntoPy, Py, PyAny, PyObject, PyRef, PyResult, Python,
};
use usearch::ffi::MetricKind;

use crate::engine::external_index_wrappers::{ExternalIndexData, ExternalIndexQuery};
use crate::external_integration::tantivy_integration::TantivyIndexFactory;
use crate::external_integration::usearch_integration::{USearchKNNIndexFactory, USearchMetricKind};
use crate::external_integration::ExternalIndexFactory;
use crate::{engine::ColumnPath, python_api::Table};

#[derive(Clone)]
#[pyclass(module = "pathway.engine", frozen, name = "ExternalIndexFactory")]
pub struct PyExternalIndexFactory {
    pub inner: Arc<dyn ExternalIndexFactory>,
}

// expose method creating USearchKNNIndexFactory to python
#[pymethods]
impl PyExternalIndexFactory {
    #[staticmethod]
    fn usearch_knn_factory(
        dimensions: usize,
        reserved_space: usize,
        metric: USearchMetricKind,
        connectivity: usize,
        expansion_add: usize,
        expansion_search: usize,
    ) -> PyExternalIndexFactory {
        PyExternalIndexFactory {
            inner: Arc::new(USearchKNNIndexFactory::new(
                dimensions,
                reserved_space,
                metric.0,
                connectivity,
                expansion_add,
                expansion_search,
            )),
        }
    }

    #[staticmethod]
    fn tantivy_factory(ram_budget: usize, in_memory_index: bool) -> PyExternalIndexFactory {
        PyExternalIndexFactory {
            inner: Arc::new(TantivyIndexFactory::new(ram_budget, in_memory_index)),
        }
    }
}

#[pyclass(module = "pathway.engine", frozen, name = "ExternalIndexData")]
pub struct PyExternalIndexData {
    pub table: Py<Table>,
    pub data_column: ColumnPath,
    pub filter_data_column: Option<ColumnPath>,
}

#[pymethods]
impl PyExternalIndexData {
    #[new]
    fn new(
        table: Py<Table>,
        data_column: ColumnPath,
        filter_data_column: Option<ColumnPath>,
    ) -> PyExternalIndexData {
        PyExternalIndexData {
            table,
            data_column,
            filter_data_column,
        }
    }
}

impl PyExternalIndexData {
    pub fn to_external_index_data(&self) -> ExternalIndexData {
        ExternalIndexData {
            table: self.table.get().handle,
            data_column: self.data_column.clone(),
            filter_data_column: self.filter_data_column.clone(),
        }
    }
}

#[pyclass(module = "pathway.engine", frozen, name = "ExternalIndexQuery")]
pub struct PyExternalIndexQuery {
    pub table: Py<Table>,
    pub query_column: ColumnPath,
    pub limit_column: Option<ColumnPath>,
    pub filter_column: Option<ColumnPath>,
}

#[pymethods]
impl PyExternalIndexQuery {
    #[new]
    fn new(
        table: Py<Table>,
        query_column: ColumnPath,
        limit_column: Option<ColumnPath>,
        filter_column: Option<ColumnPath>,
    ) -> PyExternalIndexQuery {
        PyExternalIndexQuery {
            table,
            query_column,
            limit_column,
            filter_column,
        }
    }
}

impl PyExternalIndexQuery {
    pub fn to_external_index_query(&self) -> ExternalIndexQuery {
        ExternalIndexQuery {
            table: self.table.get().handle,
            query_column: self.query_column.clone(),
            limit_column: self.limit_column.clone(),
            filter_column: self.filter_column.clone(),
        }
    }
}

#[pyclass(module = "pathway.engine", frozen, name = "USearchMetricKind")]
pub struct PyUSearchMetricKind(USearchMetricKind);

#[pymethods]
impl PyUSearchMetricKind {
    #[classattr]
    pub const IP: USearchMetricKind = USearchMetricKind(MetricKind::IP);
    #[classattr]
    pub const L2SQ: USearchMetricKind = USearchMetricKind(MetricKind::L2sq);
    #[classattr]
    pub const COS: USearchMetricKind = USearchMetricKind(MetricKind::Cos);
    #[classattr]
    pub const PEARSON: USearchMetricKind = USearchMetricKind(MetricKind::Pearson);
    #[classattr]
    pub const HAVERSINE: USearchMetricKind = USearchMetricKind(MetricKind::Haversine);
    #[classattr]
    pub const DIVERGENCE: USearchMetricKind = USearchMetricKind(MetricKind::Divergence);
    #[classattr]
    pub const HAMMING: USearchMetricKind = USearchMetricKind(MetricKind::Hamming);
    #[classattr]
    pub const TANIMOTO: USearchMetricKind = USearchMetricKind(MetricKind::Tanimoto);
    #[classattr]
    pub const SORENSEN: USearchMetricKind = USearchMetricKind(MetricKind::Sorensen);
}

impl<'source> FromPyObject<'source> for USearchMetricKind {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        Ok(ob.extract::<PyRef<PyUSearchMetricKind>>()?.0)
    }
}

impl IntoPy<PyObject> for USearchMetricKind {
    fn into_py(self, py: Python<'_>) -> PyObject {
        PyUSearchMetricKind(self).into_py(py)
    }
}
