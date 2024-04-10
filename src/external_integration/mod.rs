// Copyright © 2024 Pathway

use std::ops::Deref;
use std::{collections::HashMap, rc::Rc, sync::Arc};

use glob::Pattern;
use itertools::Itertools;
use jmespath::functions::{ArgumentType, CustomFunction, Signature};
use jmespath::{self, Context, ErrorReason, JmespathError, Rcvar, Runtime, ToJmespath, Variable};
use usearch::ffi::{IndexOptions, MetricKind, ScalarKind};
use usearch::{new_index, Index};

use differential_dataflow::difference::Abelian;

use crate::engine::dataflow::operators::external_index::Index as IndexTrait;
use crate::engine::error::DynResult;
use crate::engine::report_error::{ReportError, UnwrapWithReporter};
use crate::engine::{ColumnPath, Key, Value};

/* common parts */

pub trait ExternalIndex {
    fn add(&mut self, key: Key, data: Value, filter_data: Option<Value>) -> DynResult<()>;
    fn remove(&mut self, key: Key) -> DynResult<()>;
    fn search(
        &self,
        data: &Value,
        limit: Option<&Value>,
        filter: Option<&Value>,
    ) -> DynResult<Value>;
}

pub trait ExternalIndexFactory: Send + Sync {
    fn make_instance(&self) -> Box<dyn ExternalIndex>;
}

pub struct IndexDerivedImpl {
    inner: Box<dyn ExternalIndex>,
    error_reporter: Box<dyn ReportError>,
    data_accessor: Accessor,
    filter_data_accessor: OptionAccessor,
    query_accessor: Accessor,
    query_limit_accessor: OptionAccessor,
    query_filter_accessor: OptionAccessor,
}

impl IndexDerivedImpl {
    pub fn new(
        inner: Box<dyn ExternalIndex>,
        error_reporter: Box<dyn ReportError>,
        data_accessor: Accessor,
        filter_data_accessor: OptionAccessor,
        query_accessor: Accessor,
        query_limit_accessor: OptionAccessor,
        query_filter_accessor: OptionAccessor,
    ) -> IndexDerivedImpl {
        IndexDerivedImpl {
            inner,
            error_reporter,
            data_accessor,
            filter_data_accessor,
            query_accessor,
            query_limit_accessor,
            query_filter_accessor,
        }
    }
}

pub trait CanBeRetraction {
    fn is_retraction(&self) -> bool;
}

impl CanBeRetraction for isize {
    fn is_retraction(&self) -> bool {
        self < &0
    }
}

pub type Accessor = Box<dyn Fn(&Value) -> Value>;
pub type OptionAccessor = Box<dyn Fn(&Value) -> Option<Value>>;

// combine accessors with index, handle access errors
impl<R: Abelian + CanBeRetraction> IndexTrait<Key, Value, R, Value, Value> for IndexDerivedImpl {
    fn take_update(&mut self, k: Key, v: Value, diff: R) {
        if diff.is_zero() {
            return;
        }

        if diff.is_retraction() {
            self.inner
                .remove(k)
                .unwrap_with_reporter(&self.error_reporter);
        } else {
            let vec_as_val = (self.data_accessor)(&v);
            let filter_data = (self.filter_data_accessor)(&v);
            self.inner
                .add(k, vec_as_val, filter_data)
                .unwrap_with_reporter(&self.error_reporter);
        }
    }

    fn search(&self, v: &Value) -> Value {
        let vec_as_val = (self.query_accessor)(v);
        let query_response_limit = (self.query_limit_accessor)(v);
        let filter = (self.query_filter_accessor)(v);

        let results = self
            .inner
            .search(&vec_as_val, query_response_limit.as_ref(), filter.as_ref())
            .unwrap_with_reporter(&self.error_reporter);
        Value::Tuple(Arc::new([v.clone(), results]))
    }
}

/* utils */
// Create a new Runtime and register the builtin JMESPath functions.
struct JMESPathFilterWithGlobPattern {
    pub runtime: Runtime,
}

impl JMESPathFilterWithGlobPattern {
    pub fn new() -> Self {
        let mut runtime = Runtime::new();
        runtime.register_builtin_functions();

        runtime.register_function(
            "globmatch",
            Box::new(CustomFunction::new(
                Signature::new(vec![ArgumentType::String, ArgumentType::String], None),
                Box::new(|args: &[Rcvar], _: &mut Context| {
                    let pattern = Pattern::new(args[0].as_string().ok_or(JmespathError::new(
                        "",
                        0,
                        ErrorReason::Parse("arg[0] not present / not a string".to_string()),
                    ))?)
                    .map_err(|_| {
                        JmespathError::new(
                            "",
                            0,
                            ErrorReason::Parse("glob parsing error".to_string()),
                        )
                    })?;
                    let path = args[1].as_string().ok_or(JmespathError::new(
                        "",
                        0,
                        ErrorReason::Parse("arg[1] not present / not a string".to_string()),
                    ))?;
                    Ok(Rc::new(jmespath::Variable::Bool(pattern.matches(path))))
                }),
            )),
        );

        Self { runtime }
    }
}

// functions converting ColumnPaths into accessors
pub fn make_accessor(path: ColumnPath, error_reporter: impl ReportError + 'static) -> Accessor {
    Box::new(move |x| {
        path.extract_from_value(x)
            .unwrap_with_reporter(&error_reporter)
    })
}

pub fn make_option_accessor(
    path: Option<ColumnPath>,
    error_reporter: impl ReportError + 'static,
) -> OptionAccessor {
    match path {
        Some(path) => Box::new(move |x| {
            Some(
                path.extract_from_value(x)
                    .unwrap_with_reporter(&error_reporter),
            )
        }),
        None => Box::new(|_x| None),
    }
}

/* usearch knn */

#[derive(Clone, Copy)]
pub struct USearchMetricKind(pub MetricKind);

pub struct USearchKNNIndex {
    next_id: u64,
    id_to_key_map: HashMap<u64, Key>,
    key_to_id_map: HashMap<Key, u64>,
    filter_data_map: HashMap<Key, Variable>,
    jmespath_runtime: JMESPathFilterWithGlobPattern,
    index: Arc<Index>,
    return_distance: bool,
}

pub struct SingleMatch {
    key: Key,
    distance: f64,
}

// helper methods
impl USearchKNNIndex {
    fn get_noncolliding_u64_id(&mut self, key: Key) -> u64 {
        if let Some(ret) = self.key_to_id_map.get(&key) {
            return *ret;
        }
        let id = self.next_id;
        self.next_id += 1;
        self.id_to_key_map.insert(id, key);
        self.key_to_id_map.insert(key, id);
        id
    }

    fn _search(&self, vector: &[f64], limit: usize) -> DynResult<Vec<SingleMatch>> {
        let matches = self.index.search(vector, limit)?;
        Ok(matches
            .keys
            .into_iter()
            .zip(matches.distances)
            .map(|(k, d)| SingleMatch {
                key: self.id_to_key_map[&k],
                distance: f64::from(d),
            })
            .collect())
    }
}

// index methods
impl ExternalIndex for USearchKNNIndex {
    fn add(&mut self, key: Key, data: Value, filter_data: Option<Value>) -> DynResult<()> {
        let key_id = self.get_noncolliding_u64_id(key);

        if let Some(f_data_un) = filter_data {
            self.filter_data_map
                .insert(key, f_data_un.as_json()?.to_jmespath()?.deref().clone());
        };

        let vector: Vec<f64> = (data.as_tuple()?.iter().map(Value::as_float).try_collect())?;

        if self.index.size() + 1 > self.index.capacity() {
            assert!(self.index.reserve(2 * self.index.capacity()).is_ok());
        }

        Ok(self.index.add(key_id, &vector)?)
    }

    fn remove(&mut self, key: Key) -> DynResult<()> {
        let key_id = self
            .key_to_id_map
            .remove(&key)
            .ok_or(crate::engine::Error::KeyMissingInUniverse(key))?;
        self.id_to_key_map.remove(&key_id);
        self.filter_data_map.remove(&key);
        self.index.remove(key_id)?;
        Ok(())
    }

    fn search(
        &self,
        data: &Value,
        limit: Option<&Value>,
        filter: Option<&Value>,
    ) -> DynResult<Value> {
        let limit = if let Some(wrapped) = limit {
            usize::try_from(wrapped.as_int()?)?
        } else {
            1
        };

        let vector: Vec<f64> = (data.as_tuple()?.iter().map(Value::as_float).try_collect())?;

        if filter.is_none() {
            return Ok(Value::Tuple(
                self._search(&vector, limit)?
                    .into_iter()
                    .map(|sm| {
                        if self.return_distance {
                            Value::Tuple(Arc::new([Value::from(sm.key), Value::from(sm.distance)]))
                        } else {
                            Value::from(sm.key)
                        }
                    })
                    .collect(),
            ));
        }

        let filter_str = filter.as_ref().unwrap().as_string()?.to_string();
        let expr = self.jmespath_runtime.runtime.compile(&filter_str)?;
        let mut upper_bound = limit;
        let filtered = loop {
            let results = self._search(&vector, upper_bound)?;
            let res_len = results.len();

            let res_with_expr: Vec<(SingleMatch, Rc<Variable>)> = results
                .into_iter()
                .map(|sm| expr.search(&self.filter_data_map[&sm.key]).map(|r| (sm, r)))
                .try_collect()?;

            let to_filter: Vec<(SingleMatch, bool)> = res_with_expr
                .into_iter()
                .map(|(k, e)| match e.as_boolean() {
                    Some(ret) => Ok((k, ret)),
                    None => Err(crate::engine::Error::ValueError(
                        "jmespath filter expression did not return a boolean value".to_string(),
                    )),
                })
                .try_collect()?;

            let mut filtered: Vec<SingleMatch> = to_filter
                .into_iter()
                .filter_map(|(k, e)| if e { Some(k) } else { None })
                .collect();

            if filtered.len() >= limit {
                filtered.truncate(limit);
                break filtered;
            }

            if res_len < upper_bound {
                break filtered;
            }
            upper_bound *= 2;
        };

        Ok(Value::Tuple(
            filtered
                .into_iter()
                .map(|sm| {
                    if self.return_distance {
                        Value::Tuple(Arc::new([Value::from(sm.key), Value::from(sm.distance)]))
                    } else {
                        Value::from(sm.key)
                    }
                })
                .collect(),
        ))
    }
}

// index factory structure
pub struct USearchKNNIndexFactory {
    dimensions: usize,
    reserved_space: usize,
    metric: MetricKind,
    connectivity: usize,
    expansion_add: usize,
    expansion_search: usize,
    return_distance: bool,
}

impl USearchKNNIndexFactory {
    pub fn new(
        dimensions: usize,
        reserved_space: usize,
        metric: MetricKind,
        connectivity: usize,
        expansion_add: usize,
        expansion_search: usize,
        return_distance: bool,
    ) -> USearchKNNIndexFactory {
        USearchKNNIndexFactory {
            dimensions,
            reserved_space,
            metric,
            connectivity,
            expansion_add,
            expansion_search,
            return_distance,
        }
    }
}

// implement make_instance method, which then is used to produce instance of the index for each worker / operator
impl ExternalIndexFactory for USearchKNNIndexFactory {
    fn make_instance(&self) -> Box<dyn ExternalIndex> {
        let options = IndexOptions {
            dimensions: self.dimensions,
            metric: self.metric,
            quantization: ScalarKind::F16,
            connectivity: self.connectivity,
            expansion_add: self.expansion_add,
            expansion_search: self.expansion_search,
            multi: false,
        };

        let index = new_index(&options).unwrap();
        assert!(index.reserve(self.reserved_space).is_ok());

        Box::new(USearchKNNIndex {
            next_id: 1,
            id_to_key_map: HashMap::new(),
            key_to_id_map: HashMap::new(),
            filter_data_map: HashMap::new(),
            jmespath_runtime: JMESPathFilterWithGlobPattern::new(),
            return_distance: self.return_distance,
            index: Arc::from(index),
        })
    }
}
