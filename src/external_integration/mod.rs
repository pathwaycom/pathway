// Copyright © 2024 Pathway
pub mod tantivy_integration;
pub mod usearch_integration;
use std::ops::Deref;
use std::{collections::HashMap, rc::Rc, sync::Arc};

use glob::Pattern;
use itertools::Itertools;
use jmespath::functions::{ArgumentType, CustomFunction, Signature};
use jmespath::{self, Context, ErrorReason, JmespathError, Rcvar, Runtime, ToJmespath, Variable};

use differential_dataflow::difference::Abelian;

use crate::engine::dataflow::operators::external_index::Index as IndexTrait;
use crate::engine::error::DynResult;
use crate::engine::report_error::{ReportError, UnwrapWithReporter};
use crate::engine::{ColumnPath, Error, Key, Value};

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
    fn make_instance(&self) -> Result<Box<dyn ExternalIndex>, Error>;
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

//

struct KeyToU64IdMapper {
    next_id: u64,
    id_to_key_map: HashMap<u64, Key>,
    key_to_id_map: HashMap<Key, u64>,
}

impl KeyToU64IdMapper {
    fn new() -> KeyToU64IdMapper {
        KeyToU64IdMapper {
            next_id: 1,
            id_to_key_map: HashMap::new(),
            key_to_id_map: HashMap::new(),
        }
    }

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

    fn get_key_for_id(&self, id: u64) -> Key {
        self.id_to_key_map[&id]
    }

    fn remove_key(&mut self, key: Key) -> DynResult<u64> {
        let key_id = self
            .key_to_id_map
            .remove(&key)
            .ok_or(crate::engine::DataError::MissingKey(key))?;
        self.id_to_key_map.remove(&key_id);
        Ok(key_id)
    }
}

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

pub struct KeyScoreMatch {
    key: Key,
    score: Option<f64>,
}

impl KeyScoreMatch {
    fn key(&self) -> Key {
        self.key
    }

    fn into_value(self) -> Value {
        if let Some(score) = self.score {
            Value::Tuple(Arc::new([Value::from(self.key), Value::from(score)]))
        } else {
            Value::from(self.key)
        }
    }
}

/*data unpacking types, so that we can define unpacking from values to types by setting generic types*/

pub trait Unpack<Type> {
    fn unpack(self) -> DynResult<Type>;
}

// to vector of floats
impl Unpack<Vec<f64>> for Value {
    fn unpack(self) -> DynResult<Vec<f64>> {
        self.as_tuple()?.iter().map(Value::as_float).try_collect()
    }
}

// to JMESPath Variable
impl Unpack<Variable> for Value {
    fn unpack(self) -> DynResult<Variable> {
        //can I do that without deref.clone()?
        Ok(self.as_json()?.to_jmespath()?.deref().clone())
    }
}

// to String
impl Unpack<String> for Value {
    fn unpack(self) -> DynResult<String> {
        Ok(self.as_string()?.to_string())
    }
}

pub trait NonFilteringExternalIndex<DataType, QueryType> {
    fn add(&mut self, key: Key, data: DataType) -> DynResult<()>;
    fn remove(&mut self, key: Key) -> DynResult<()>;
    fn search(&self, data: &QueryType, limit: Option<usize>) -> DynResult<Vec<KeyScoreMatch>>;
}

pub struct DerivedFilteredSearchIndex<DataType, QueryType> {
    // needed for derived filtering
    jmespath_runtime: JMESPathFilterWithGlobPattern,
    filter_data_map: HashMap<Key, Variable>,
    // needed to be an index
    inner: Box<dyn NonFilteringExternalIndex<DataType, QueryType>>,
}

impl<DataType, QueryType> DerivedFilteredSearchIndex<DataType, QueryType> {
    pub fn new(
        index: Box<dyn NonFilteringExternalIndex<DataType, QueryType>>,
    ) -> DerivedFilteredSearchIndex<DataType, QueryType> {
        DerivedFilteredSearchIndex {
            jmespath_runtime: JMESPathFilterWithGlobPattern::new(),
            filter_data_map: HashMap::new(),
            inner: index,
        }
    }
}

impl<DataType, QueryType> ExternalIndex for DerivedFilteredSearchIndex<DataType, QueryType>
where
    Value: Unpack<DataType> + Unpack<QueryType>,
{
    fn add(&mut self, key: Key, data: Value, filter_data: Option<Value>) -> DynResult<()> {
        if let Some(f_data_un) = filter_data {
            self.filter_data_map.insert(key, f_data_un.unpack()?);
        };
        let data_point: DataType = data.unpack()?;
        self.inner.add(key, data_point)
    }

    fn remove(&mut self, key: Key) -> DynResult<()> {
        self.inner.remove(key)?;
        self.filter_data_map.remove(&key);
        Ok(())
    }

    fn search(
        &self,
        data: &Value,
        limit: Option<&Value>,
        filter: Option<&Value>,
    ) -> DynResult<Value> {
        //TODOs: replace with default limit elsewhere, adjust for distance-in-result (when other PR is merged)
        let limit = if let Some(wrapped) = limit {
            usize::try_from(wrapped.as_int()?)?
        } else {
            1
        };

        // maybe pass query as value and remove clone?
        let query_point: QueryType = data.clone().unpack()?;
        if filter.is_none() {
            return Ok(Value::Tuple(
                self.inner
                    .search(&query_point, Some(limit))?
                    .into_iter()
                    .map(KeyScoreMatch::into_value)
                    .collect(),
            ));
        }

        let filter_as_string: String = filter.cloned().unwrap().unpack()?;
        let expr = self.jmespath_runtime.runtime.compile(&filter_as_string)?;
        let mut upper_bound = limit;

        let filtered = loop {
            let results: Vec<KeyScoreMatch> = self.inner.search(&query_point, Some(upper_bound))?;
            let res_len = results.len();
            let res_with_expr: Vec<(KeyScoreMatch, Rc<Variable>)> = results
                .into_iter()
                .map(|sm| {
                    expr.search(&self.filter_data_map[&sm.key()])
                        .map(|r| (sm, r))
                })
                .try_collect()?;

            let to_filter: Vec<(KeyScoreMatch, bool)> = res_with_expr
                .into_iter()
                .map(|(k, e)| match e.as_boolean() {
                    Some(ret) => Ok((k, ret)),
                    None => Err(crate::engine::DataError::ValueError(
                        "jmespath filter expression did not return a boolean value".to_string(),
                    )),
                })
                .try_collect()?;

            let mut filtered: Vec<KeyScoreMatch> = to_filter
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
                .map(KeyScoreMatch::into_value)
                .collect(),
        ))
    }
}
