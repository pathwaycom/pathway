// Copyright © 2024 Pathway
pub mod tantivy_integration;
pub mod usearch_integration;
use std::ops::Deref;
use std::{collections::HashMap, rc::Rc, sync::Arc};

use glob::Pattern;
use itertools::{Either, Itertools};
use jmespath::functions::{ArgumentType, CustomFunction, Signature};
use jmespath::{
    self, Context, ErrorReason, Expression, JmespathError, Rcvar, Runtime, ToJmespath, Variable,
};

use differential_dataflow::difference::Abelian;

use crate::engine::dataflow::operators::external_index::Index as IndexTrait;
use crate::engine::error::DynResult;
use crate::engine::report_error::{
    LogError, ReportError, UnwrapWithErrorLogger, UnwrapWithReporter,
};
use crate::engine::{ColumnPath, DataError, Error, Key, Value};

type PendingQueryEntry<'a, QType> = (&'a Key, (&'a QType, usize, usize, &'a Expression<'a>));

pub struct AddDataEntry {
    key: Key,
    data: Value,
    filter_data: Option<Value>,
}

pub struct QueryEntry {
    key: Key,
    data: Value,
    limit: Option<Value>,
    filter: Option<Value>,
}

pub trait ExternalIndex {
    fn add(&mut self, add_data: Vec<AddDataEntry>) -> Vec<(Key, DynResult<()>)>;
    fn remove(&mut self, keys: Vec<Key>) -> Vec<(Key, DynResult<()>)>;
    fn search(&self, query_data: &[QueryEntry]) -> Vec<(Key, DynResult<Value>)>;
}

pub trait ExternalIndexFactory: Send + Sync {
    fn make_instance(&self) -> Result<Box<dyn ExternalIndex>, Error>;
}

pub struct IndexDerivedImpl {
    inner: Box<dyn ExternalIndex>,
    error_logger: Box<dyn LogError>,
    data_accessor: Accessor,
    filter_data_accessor: OptionAccessor,
    query_accessor: Accessor,
    query_limit_accessor: OptionAccessor,
    query_filter_accessor: OptionAccessor,
}

impl IndexDerivedImpl {
    pub fn new(
        inner: Box<dyn ExternalIndex>,
        error_logger: Box<dyn LogError>,
        data_accessor: Accessor,
        filter_data_accessor: OptionAccessor,
        query_accessor: Accessor,
        query_limit_accessor: OptionAccessor,
        query_filter_accessor: OptionAccessor,
    ) -> IndexDerivedImpl {
        IndexDerivedImpl {
            inner,
            error_logger,
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
impl<R: Abelian + CanBeRetraction> IndexTrait<Key, Value, R, Key, Value, Value>
    for IndexDerivedImpl
{
    fn take_updates(&mut self, data: Vec<(Key, Value, R)>) {
        let filtered_data: Vec<(Key, Value, Option<Value>, R)> = data
            .into_iter()
            .filter(|(_, _, diff)| !diff.is_zero())
            .filter_map(|(k, v, diff)| {
                let data = (self.data_accessor)(&v);
                let filter_data = (self.filter_data_accessor)(&v);
                let contains_errors = [Some(&data), filter_data.as_ref()]
                    .into_iter()
                    .flatten()
                    .contains(&Value::Error);
                if contains_errors {
                    self.error_logger.log_error(DataError::ErrorInIndexUpdate);
                    None
                } else {
                    Some((k, data, filter_data, diff))
                }
            })
            .collect();

        let (to_insert, to_remove): (Vec<AddDataEntry>, Vec<Key>) = filtered_data
            .into_iter()
            .partition_map(|(k, data, filter_data, diff)| {
                if diff.is_retraction() {
                    Either::Right(k)
                } else {
                    Either::Left(AddDataEntry {
                        key: k,
                        data,
                        filter_data,
                    })
                }
            });

        for (_key, res) in self.inner.remove(to_remove) {
            res.unwrap_or_log(self.error_logger.as_ref(), ());
        }

        for (_key, res) in self.inner.add(to_insert) {
            res.unwrap_or_log(self.error_logger.as_ref(), ());
        }
    }

    fn search(&self, queries: Vec<(Key, Value, R)>) -> Vec<(Key, Value, R)> {
        let (to_query_without_errors, errors): (Vec<QueryEntry>, Vec<Key>) =
            queries.iter().partition_map(|(k, v, _diff)| {
                let vec_as_val = (self.query_accessor)(v);
                let query_response_limit = (self.query_limit_accessor)(v);
                let filter = (self.query_filter_accessor)(v);

                let contains_errors = [
                    Some(&vec_as_val),
                    query_response_limit.as_ref(),
                    filter.as_ref(),
                ]
                .into_iter()
                .flatten()
                .contains(&Value::Error);
                if contains_errors {
                    self.error_logger.log_error(DataError::ErrorInIndexSearch);
                    Either::Right(k)
                } else {
                    Either::Left(QueryEntry {
                        key: *k,
                        data: vec_as_val,
                        limit: query_response_limit,
                        filter,
                    })
                }
            });

        let maybe_error_responses = self.inner.search(&to_query_without_errors);
        let mut responses = HashMap::with_capacity(queries.len());
        for (key, maybe_result) in maybe_error_responses {
            responses.insert(
                key,
                maybe_result.unwrap_or_log(self.error_logger.as_ref(), Value::Error),
            );
        }

        for err in errors {
            responses.insert(err, Value::Error);
        }

        queries
            .into_iter()
            .map(|(k, v, diff)| {
                (
                    k,
                    Value::Tuple(Arc::new([v, responses.remove(&k).unwrap_or(Value::Error)])),
                    diff,
                )
            })
            .collect()
    }
}

/* utils */

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
            .ok_or(DataError::MissingKey(key))?;
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
    score: f64,
}

impl KeyScoreMatch {
    fn key(&self) -> Key {
        self.key
    }

    fn into_value(self) -> Value {
        Value::Tuple(Arc::new([Value::from(self.key), Value::from(self.score)]))
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
    fn add(&mut self, batch: Vec<(Key, DataType)>) -> Vec<(Key, DynResult<()>)>;
    fn remove(&mut self, keys: Vec<Key>) -> Vec<(Key, DynResult<()>)>;
    fn search(
        &self,
        queries: &[(Key, QueryType, usize)],
    ) -> Vec<(Key, DynResult<Vec<KeyScoreMatch>>)>;
}

pub struct DerivedFilteredSearchIndex<DataType, QueryType> {
    // needed for derived filtering
    jmespath_runtime: JMESPathFilterWithGlobPattern,
    filter_data_map: HashMap<Key, Variable>,
    // needed to be an index
    inner: Box<dyn NonFilteringExternalIndex<DataType, QueryType>>,
}

impl<DataType, QueryType> DerivedFilteredSearchIndex<DataType, QueryType>
where
    Value: Unpack<DataType> + Unpack<QueryType>,
{
    pub fn new(
        index: Box<dyn NonFilteringExternalIndex<DataType, QueryType>>,
    ) -> DerivedFilteredSearchIndex<DataType, QueryType> {
        DerivedFilteredSearchIndex {
            jmespath_runtime: JMESPathFilterWithGlobPattern::new(),
            filter_data_map: HashMap::new(),
            inner: index,
        }
    }

    fn handle_filter_and_unpack_data(&mut self, entry: AddDataEntry) -> DynResult<DataType> {
        if let Some(f_data_un) = entry.filter_data {
            self.filter_data_map.insert(entry.key, f_data_un.unpack()?);
        };
        entry.data.unpack()
    }

    fn make_query_tuple(
        &self,
        query: &QueryEntry,
    ) -> DynResult<(QueryType, usize, Option<Expression>)> {
        let limit = if let Some(wrapped) = &query.limit {
            usize::try_from(wrapped.as_int()?)?
        } else {
            1
        };
        let query_point: QueryType = query.data.clone().unpack()?;
        let filter = if query.filter.is_none() || query.filter == Some(Value::None) {
            None
        } else {
            let filter_as_string: String = query.filter.clone().unwrap().unpack()?;
            Some(self.jmespath_runtime.runtime.compile(&filter_as_string)?)
        };
        Ok((query_point, limit, filter))
    }

    fn filter_results(
        &self,
        results: Vec<KeyScoreMatch>,
        expr: &Expression,
    ) -> DynResult<Vec<KeyScoreMatch>> {
        let res_with_expr: Vec<(KeyScoreMatch, Rc<Variable>)> = results
            .into_iter()
            .map(|sm| {
                expr.search(&self.filter_data_map[&sm.key()])
                    .map(|r| (sm, r))
            })
            .try_collect()?;

        Ok(res_with_expr
            .into_iter()
            .filter_map(|(k, e)| match e.as_boolean() {
                Some(true) => Some(Ok(k)),
                Some(false) => None,
                None => Some(Err(DataError::ValueError(
                    "jmespath filter expression did not return a boolean value".to_string(),
                ))),
            })
            .try_collect()?)
    }

    fn retain_unfinished_queries<'a>(
        &self,
        pending: &[PendingQueryEntry<'a, QueryType>],
        answers: Vec<(Key, DynResult<Vec<KeyScoreMatch>>)>,
        responses: &mut Vec<(Key, DynResult<Value>)>,
    ) -> Vec<PendingQueryEntry<'a, QueryType>> {
        pending
            .iter()
            .zip(answers)
            .filter_map(
                |((key, (query, limit, current_upper_bound, expr)), (key2, results))| {
                    assert!(**key == key2);
                    match results {
                        Err(error) => {
                            responses.push((**key, Err(error)));
                            None
                        }
                        Ok(results) => {
                            let res_len = results.len();
                            let filtered = self.filter_results(results, expr);
                            match filtered {
                                Err(error) => {
                                    responses.push((*(*key), Err(error)));
                                    None
                                }
                                Ok(mut filtered) => {
                                    if &filtered.len() >= limit || &res_len < current_upper_bound {
                                        filtered.truncate(*limit);
                                        responses.push((
                                            *(*key),
                                            Ok(Value::Tuple(
                                                filtered
                                                    .into_iter()
                                                    .map(KeyScoreMatch::into_value)
                                                    .collect(),
                                            )),
                                        ));
                                        return None;
                                    }
                                    Some((*key, (*query, *limit, 2 * current_upper_bound, *expr)))
                                }
                            }
                        }
                    }
                },
            )
            .collect()
    }
}

impl<DataType, QueryType> ExternalIndex for DerivedFilteredSearchIndex<DataType, QueryType>
where
    Value: Unpack<DataType> + Unpack<QueryType>,
    QueryType: Clone,
{
    fn add(&mut self, add_data: Vec<AddDataEntry>) -> Vec<(Key, DynResult<()>)> {
        let mut batch = Vec::with_capacity(add_data.len());
        let mut error_rows = Vec::with_capacity(add_data.len());
        for entry in add_data {
            let key = entry.key;
            let maybe_data_point = self.handle_filter_and_unpack_data(entry);
            match maybe_data_point {
                Ok(data_point) => {
                    batch.push((key, data_point));
                }
                Err(error) => {
                    error_rows.push((key, Err(error)));
                }
            }
        }
        let mut ret = self.inner.add(batch);
        ret.extend(error_rows);
        ret
    }

    fn remove(&mut self, keys: Vec<Key>) -> Vec<(Key, DynResult<()>)> {
        for key in &keys {
            self.filter_data_map.remove(key);
        }
        self.inner.remove(keys)
    }

    fn search(&self, query_data: &[QueryEntry]) -> Vec<(Key, DynResult<Value>)> {
        let (queries_without_errors, queries_with_errors): (Vec<_>, Vec<_>) = query_data
            .iter()
            .map(|query| (query.key, self.make_query_tuple(query)))
            .partition_map(|(key, maybe_query)| match maybe_query {
                Ok(query) => Either::Left((key, query)),
                Err(error) => Either::Right((key, error)),
            });

        let (filtering_queries, regular_queries): (Vec<_>, Vec<_>) = queries_without_errors
            .into_iter()
            .partition_map(|(k, (q, lim, f))| {
                if let Some(f) = f {
                    Either::Left((k, q, lim, f))
                } else {
                    Either::Right((k, q, lim))
                }
            });

        let maybe_error_responses = self.inner.search(&regular_queries);
        let mut responses = Vec::with_capacity(query_data.len());
        for (key, error) in queries_with_errors {
            responses.push((key, Err(error)));
        }

        for (key, response) in maybe_error_responses {
            match response {
                Ok(response) => {
                    responses.push((
                        key,
                        Ok(Value::Tuple(
                            response
                                .into_iter()
                                .map(KeyScoreMatch::into_value)
                                .collect(),
                        )),
                    ));
                }
                Err(error) => {
                    responses.push((key, Err(error)));
                }
            }
        }

        let mut pending = Vec::with_capacity(filtering_queries.len());
        for (key, query, limit, filter) in &filtering_queries {
            pending.push((key, (query, *limit, *limit, filter)));
        }

        while !pending.is_empty() {
            let queries: Vec<_> = pending
                .iter()
                .map(|(key, (query, _, current_upper_bound, _))| {
                    (*(*key), (*query).clone(), *current_upper_bound)
                })
                .collect();

            pending = self.retain_unfinished_queries(
                &pending,
                self.inner.search(&queries),
                &mut responses,
            );
        }
        responses
    }
}
