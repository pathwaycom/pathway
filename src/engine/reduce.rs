// Copyright © 2024 Pathway

use ndarray::prelude::*;

use cfg_if::cfg_if;
use differential_dataflow::difference::{Multiply, Semigroup};
use differential_dataflow::ExchangeData;
use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};
use std::any::type_name;
use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::{cmp::Reverse, sync::Arc};

use super::{error::DynResult, DataError, Key, Value};

pub type StatefulCombineFn =
    Arc<dyn Fn(Option<&Value>, Vec<(Vec<Value>, isize)>) -> DynResult<Option<Value>> + Send + Sync>;

fn take_first_value<T>(v: Vec<T>) -> T {
    v.into_iter()
        .next()
        .expect("at least one element should be present")
}

#[derive(Clone)]
pub enum Reducer {
    Count,
    CountDistinct,
    FloatSum { strict: bool },
    IntSum,
    ArraySum { strict: bool },
    Unique,
    Min,
    ArgMin,
    Max,
    ArgMax,
    SortedTuple { skip_nones: bool },
    Tuple { skip_nones: bool },
    Any,
    Stateful { combine_fn: StatefulCombineFn },
    Earliest,
    Latest,
}

pub trait SemigroupState: Sized {
    fn init(key: Key, values: Vec<Value>) -> DynResult<Self>;

    fn empty() -> Self;

    fn finish(self) -> Value;
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ErrorStateWrapper<State> {
    state: State,
    error_count: isize,
}

impl<State> ErrorStateWrapper<State>
where
    State: SemigroupState,
{
    pub fn init(key: Key, values: Vec<Value>) -> DynResult<Self> {
        Ok(Self {
            state: State::init(key, values)?,
            error_count: 0,
        })
    }

    pub fn init_error() -> Self {
        Self {
            state: State::empty(),
            error_count: 1,
        }
    }

    pub fn finish(self) -> Value {
        if self.error_count != 0 {
            Value::Error
        } else {
            self.state.finish()
        }
    }
}

impl<State> Semigroup for ErrorStateWrapper<State>
where
    State: Semigroup,
{
    fn is_zero(&self) -> bool {
        self.state.is_zero() && self.error_count.is_zero()
    }

    fn plus_equals(&mut self, rhs: &Self) {
        self.state.plus_equals(&rhs.state);
        self.error_count.plus_equals(&rhs.error_count);
    }
}

impl<State> Multiply<isize> for ErrorStateWrapper<State>
where
    State: Multiply<isize, Output = State>,
{
    type Output = Self;
    fn multiply(self, rhs: &isize) -> Self::Output {
        let state = self.state.multiply(rhs);
        let error_count = self.error_count * rhs;
        Self { state, error_count }
    }
}

pub struct SemigroupReducer<R> {
    phantom: PhantomData<R>,
}

impl<R> Default for SemigroupReducer<R> {
    fn default() -> Self {
        Self {
            phantom: PhantomData,
        }
    }
}

pub trait ReducerImpl: 'static {
    type State: ExchangeData;

    fn init(&self, key: &Key, values: &[Value]) -> DynResult<Self::State>;

    fn combine<'a>(
        &self,
        values: impl IntoIterator<Item = (&'a Self::State, NonZeroUsize)>,
    ) -> DynResult<Value>;
}

pub trait UnaryReducerImpl: 'static {
    type State: ExchangeData;

    fn init_unary(&self, key: &Key, value: &Value) -> DynResult<Self::State>;

    fn combine<'a>(
        &self,
        values: impl IntoIterator<Item = (&'a Self::State, NonZeroUsize)>,
    ) -> DynResult<Value>;
}

impl<T: UnaryReducerImpl> ReducerImpl for T {
    type State = <Self as UnaryReducerImpl>::State;
    fn init(&self, key: &Key, values: &[Value]) -> DynResult<Self::State> {
        self.init_unary(key, &values[0])
    }

    fn combine<'a>(
        &self,
        values: impl IntoIterator<Item = (&'a Self::State, NonZeroUsize)>,
    ) -> DynResult<Value> {
        <Self as UnaryReducerImpl>::combine(self, values)
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct IntSumState {
    count: isize,
    sum: i64,
}

impl Semigroup for IntSumState {
    fn is_zero(&self) -> bool {
        self.count.is_zero() && self.sum.is_zero()
    }

    fn plus_equals(&mut self, rhs: &Self) {
        self.count.plus_equals(&rhs.count);
        self.sum.plus_equals(&rhs.sum);
    }
}

impl Multiply<isize> for IntSumState {
    type Output = Self;
    fn multiply(self, rhs: &isize) -> Self::Output {
        let count = self.count * rhs;
        let sum = self.sum * i64::try_from(*rhs).unwrap();
        Self { count, sum }
    }
}

impl SemigroupState for IntSumState {
    fn init(key: Key, values: Vec<Value>) -> DynResult<Self> {
        match take_first_value(values) {
            Value::Int(i) => Ok(Self { count: 1, sum: i }),
            value => Err(DataError::ReducerInitializationError {
                reducer_type: type_name::<Self>().to_string(),
                value: value.clone(),
                source_key: key,
            }
            .into()),
        }
    }

    fn empty() -> Self {
        Self { count: 0, sum: 0 }
    }

    fn finish(self) -> Value {
        Value::Int(self.sum)
    }
}

fn neumeier_summation_step(sum: f64, compensation: f64, value: f64) -> (f64, f64) {
    let new_sum = sum + value;
    let delta_compensation = if sum.abs() > value.abs() {
        (sum - new_sum) + value
    } else {
        (value - new_sum) + sum
    };
    (new_sum, compensation + delta_compensation)
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct FloatSumState {
    count: isize,
    sum: OrderedFloat<f64>,
    compensation: OrderedFloat<f64>,
}

impl Semigroup for FloatSumState {
    fn is_zero(&self) -> bool {
        self.count.is_zero()
    }

    fn plus_equals(&mut self, rhs: &Self) {
        self.count.plus_equals(&rhs.count);
        let (sum, compensation) =
            neumeier_summation_step(*self.sum, *self.compensation + *rhs.compensation, *rhs.sum);
        self.compensation = compensation.into();
        self.sum = sum.into();
    }
}

impl Multiply<isize> for FloatSumState {
    type Output = Self;
    #[allow(clippy::cast_precision_loss)]
    fn multiply(self, rhs: &isize) -> Self::Output {
        let count = self.count * rhs;
        let sum = self.sum * *rhs as f64;
        let compensation = self.compensation * *rhs as f64;
        Self {
            count,
            sum,
            compensation,
        }
    }
}

impl SemigroupState for FloatSumState {
    fn init(key: Key, values: Vec<Value>) -> DynResult<Self> {
        match take_first_value(values) {
            Value::Float(f) => Ok(Self {
                count: 1,
                sum: f,
                compensation: 0.0.into(),
            }),
            value => Err(DataError::ReducerInitializationError {
                reducer_type: type_name::<Self>().to_string(),
                value: value.clone(),
                source_key: key,
            }
            .into()),
        }
    }

    fn empty() -> Self {
        Self {
            count: 0,
            sum: 0.0.into(),
            compensation: 0.0.into(),
        }
    }

    fn finish(self) -> Value {
        let sum = if self.count == 0 {
            OrderedFloat(0.0)
        } else {
            self.sum + self.compensation
        };
        Value::Float(sum)
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ArraySumState {
    count: isize,
    sum: Value,
}

impl Semigroup for ArraySumState {
    fn is_zero(&self) -> bool {
        self.count.is_zero()
    }

    fn plus_equals(&mut self, rhs: &Self) {
        self.count.plus_equals(&rhs.count);
        self.sum = add_ndarrays(CowNdArray::new(&self.sum, 1), CowNdArray::new(&rhs.sum, 1))
            .unwrap()
            .into();
    }
}

impl Multiply<isize> for ArraySumState {
    type Output = Self;
    #[allow(clippy::cast_precision_loss)]
    fn multiply(self, rhs: &isize) -> Self::Output {
        if *rhs == 1 {
            return self;
        }
        let count = self.count * rhs;
        let sum = CowNdArray::new(&self.sum, *rhs)
            .expect("value contains only IntArray and FloatArray")
            .into();
        Self { count, sum }
    }
}

impl SemigroupState for ArraySumState {
    fn init(key: Key, values: Vec<Value>) -> DynResult<Self> {
        let value = take_first_value(values);
        if matches!(value, Value::IntArray(_) | Value::FloatArray(_)) {
            Ok(Self {
                count: 1,
                sum: value,
            })
        } else {
            Err(DataError::ReducerInitializationError {
                reducer_type: type_name::<Self>().to_string(),
                value: value.clone(),
                source_key: key,
            }
            .into())
        }
    }

    fn empty() -> Self {
        Self {
            count: 0,
            sum: Value::None,
        }
    }

    fn finish(self) -> Value {
        self.sum
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct AppendOnlyState<State>(State);

trait ShouldBeReplaced {
    fn should_be_replaced(&self, rhs: &Self) -> bool;
}

impl<S> Semigroup for AppendOnlyState<S>
where
    S: ShouldBeReplaced + ExchangeData,
{
    fn is_zero(&self) -> bool {
        false
    }

    fn plus_equals(&mut self, rhs: &Self) {
        if self.0.should_be_replaced(&rhs.0) {
            self.0 = rhs.0.clone();
        }
    }
}

impl<S> Multiply<isize> for AppendOnlyState<S> {
    type Output = Self;
    fn multiply(self, _rhs: &isize) -> Self::Output {
        self
    }
}

impl<S> SemigroupState for AppendOnlyState<S>
where
    S: SemigroupState,
{
    fn init(key: Key, values: Vec<Value>) -> DynResult<Self> {
        Ok(Self(S::init(key, values)?))
    }

    fn empty() -> Self {
        Self(S::empty())
    }

    fn finish(self) -> Value {
        self.0.finish()
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct MinState {
    value: Value,
}

pub type AppendOnlyMinState = AppendOnlyState<MinState>;

impl ShouldBeReplaced for MinState {
    fn should_be_replaced(&self, rhs: &Self) -> bool {
        self.value > rhs.value
    }
}

impl SemigroupState for MinState {
    fn init(_key: Key, values: Vec<Value>) -> DynResult<Self> {
        Ok(Self {
            value: take_first_value(values),
        })
    }

    fn empty() -> Self {
        Self { value: Value::None }
    }

    fn finish(self) -> Value {
        self.value.clone()
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct MaxState {
    value: Value,
}

pub type AppendOnlyMaxState = AppendOnlyState<MaxState>;

impl ShouldBeReplaced for MaxState {
    fn should_be_replaced(&self, rhs: &Self) -> bool {
        self.value < rhs.value
    }
}

impl SemigroupState for MaxState {
    fn init(_key: Key, values: Vec<Value>) -> DynResult<Self> {
        Ok(Self {
            value: take_first_value(values),
        })
    }

    fn empty() -> Self {
        Self { value: Value::None }
    }

    fn finish(self) -> Value {
        self.value
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ArgMinState {
    value: Value,
    key: Value,
}

pub type AppendOnlyArgMinState = AppendOnlyState<ArgMinState>;

impl ShouldBeReplaced for ArgMinState {
    fn should_be_replaced(&self, rhs: &Self) -> bool {
        (&self.value, &self.key) > (&rhs.value, &rhs.key)
    }
}

impl SemigroupState for ArgMinState {
    fn init(_key: Key, values: Vec<Value>) -> DynResult<Self> {
        let mut values = values.into_iter();
        Ok(Self {
            value: values.next().expect("value should be present"),
            key: values.next().expect("key should be present"),
        })
    }

    fn empty() -> Self {
        Self {
            value: Value::None,
            key: Value::None,
        }
    }

    fn finish(self) -> Value {
        self.key
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ArgMaxState {
    value: Value,
    key: Value,
}

pub type AppendOnlyArgMaxState = AppendOnlyState<ArgMaxState>;

impl ShouldBeReplaced for ArgMaxState {
    fn should_be_replaced(&self, rhs: &Self) -> bool {
        (&self.value, Reverse(&self.key)) < (&rhs.value, Reverse(&rhs.key))
    }
}

impl SemigroupState for ArgMaxState {
    fn init(_key: Key, values: Vec<Value>) -> DynResult<Self> {
        let mut values = values.into_iter();
        Ok(Self {
            value: values.next().expect("value should be present"),
            key: values.next().expect("key should be present"),
        })
    }

    fn empty() -> Self {
        Self {
            value: Value::None,
            key: Value::None,
        }
    }

    fn finish(self) -> Value {
        self.key
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct AnyState {
    key: Key,
    value: Value,
}

pub type AppendOnlyAnyState = AppendOnlyState<AnyState>;

impl ShouldBeReplaced for AnyState {
    fn should_be_replaced(&self, rhs: &Self) -> bool {
        (&self.key, &self.value) > (&rhs.key, &rhs.value)
    }
}

impl SemigroupState for AnyState {
    fn init(key: Key, values: Vec<Value>) -> DynResult<Self> {
        Ok(Self {
            key: key.salted_with(SALT),
            value: take_first_value(values),
        })
    }

    fn empty() -> Self {
        Self {
            key: Key(SALT), // any key is valid
            value: Value::None,
        }
    }

    fn finish(self) -> Value {
        self.value
    }
}

#[derive(Debug, Clone, Copy)]
pub struct CountReducer;

#[derive(Debug, Clone, Copy)]
pub struct FloatSumReducer;

impl UnaryReducerImpl for FloatSumReducer {
    type State = OrderedFloat<f64>;

    fn init_unary(&self, key: &Key, value: &Value) -> DynResult<Self::State> {
        match value {
            Value::Float(f) => Ok(*f),
            value => Err(DataError::ReducerInitializationError {
                reducer_type: type_name::<Self>().to_string(),
                value: value.clone(),
                source_key: *key,
            }
            .into()),
        }
    }

    fn combine<'a>(
        &self,
        values: impl IntoIterator<Item = (&'a Self::State, NonZeroUsize)>,
    ) -> DynResult<Value> {
        let mut sum = 0.0;
        let mut compensation = 0.0;
        for (value, cnt) in values {
            #[allow(clippy::cast_precision_loss)]
            let (new_sum, new_compensation) =
                neumeier_summation_step(sum, compensation, **value * cnt.get() as f64);
            compensation = new_compensation;
            sum = new_sum;
        }
        Ok(Value::Float((sum + compensation).into()))
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ArraySumReducer;

#[derive(Debug)]
enum CowNdArray<'a> {
    IntArray(CowArray<'a, i64, IxDyn>),
    FloatArray(CowArray<'a, f64, IxDyn>),
}

impl<'a> CowNdArray<'a> {
    fn new(value: &'a Value, cnt: isize) -> DynResult<Self> {
        match value {
            #[allow(clippy::cast_precision_loss)]
            Value::IntArray(array) => {
                if cnt == 1 {
                    Ok(Self::IntArray(CowArray::from(&**array)))
                } else {
                    Ok(Self::IntArray(CowArray::from(
                        &**array * i64::try_from(cnt).unwrap(),
                    )))
                }
            }
            #[allow(clippy::cast_precision_loss)]
            Value::FloatArray(array) => {
                if cnt == 1 {
                    Ok(Self::FloatArray(CowArray::from(&**array)))
                } else {
                    Ok(Self::FloatArray(CowArray::from(&**array * cnt as f64)))
                }
            }
            value => Err(DataError::TypeMismatch {
                expected: "Array",
                value: value.clone(),
            }
            .into()),
        }
    }
}

fn add_ndarrays<'a>(
    lhs: DynResult<CowNdArray<'a>>,
    rhs: DynResult<CowNdArray<'a>>,
) -> DynResult<CowNdArray<'a>> {
    match (lhs?, rhs?) {
        (CowNdArray::IntArray(lhs), CowNdArray::IntArray(rhs)) => Ok(CowNdArray::IntArray(
            CowArray::from(lhs.into_owned() + &rhs),
        )),
        (CowNdArray::FloatArray(lhs), CowNdArray::FloatArray(rhs)) => Ok(CowNdArray::FloatArray(
            CowArray::from(lhs.into_owned() + &rhs),
        )),
        _ => Err(DataError::MixingTypesInNpSum.into()),
    }
}

impl<'a> From<CowNdArray<'a>> for Value {
    fn from(state: CowNdArray<'a>) -> Self {
        match state {
            CowNdArray::IntArray(a) => Self::from(a.into_owned()),
            CowNdArray::FloatArray(a) => Self::from(a.into_owned()),
        }
    }
}

impl UnaryReducerImpl for ArraySumReducer {
    type State = Value;

    fn init_unary(&self, _key: &Key, value: &Value) -> DynResult<Self::State> {
        Ok(value.clone())
    }

    fn combine<'a>(
        &self,
        values: impl IntoIterator<Item = (&'a Self::State, NonZeroUsize)>,
    ) -> DynResult<Value> {
        Ok(values
            .into_iter()
            .map(|(value, cnt)| CowNdArray::new(value, isize::try_from(cnt.get()).unwrap()))
            .reduce(add_ndarrays)
            .expect("values should not be empty")?
            .into())
    }
}

#[derive(Debug, Clone, Copy)]
pub struct UniqueReducer;

impl UnaryReducerImpl for UniqueReducer {
    type State = Value;

    fn init_unary(&self, _key: &Key, value: &Value) -> DynResult<Self::State> {
        Ok(value.clone())
    }

    fn combine<'a>(
        &self,
        values: impl IntoIterator<Item = (&'a Self::State, NonZeroUsize)>,
    ) -> DynResult<Value> {
        let mut values = values.into_iter();
        let (state, _cnt) = values.next().unwrap();
        if let Some((next_value, _next_cnt)) = values.next() {
            Err(DataError::MoreThanOneValueInUniqueReducer {
                value_1: state.clone(),
                value_2: next_value.clone(),
            }
            .into())
        } else {
            Ok(state.clone())
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct MinReducer;

impl UnaryReducerImpl for MinReducer {
    type State = Value;

    fn init_unary(&self, _key: &Key, value: &Value) -> DynResult<Self::State> {
        Ok(value.clone())
    }

    fn combine<'a>(
        &self,
        values: impl IntoIterator<Item = (&'a Self::State, NonZeroUsize)>,
    ) -> DynResult<Value> {
        Ok(values
            .into_iter()
            .map(|(val, _cnt)| val)
            .min()
            .unwrap()
            .clone())
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ArgMinReducer;

impl ReducerImpl for ArgMinReducer {
    type State = (Value, Value);

    fn init(&self, _key: &Key, values: &[Value]) -> DynResult<Self::State> {
        Ok((values[0].clone(), values[1].clone()))
    }

    fn combine<'a>(
        &self,
        values: impl IntoIterator<Item = (&'a Self::State, NonZeroUsize)>,
    ) -> DynResult<Value> {
        Ok(values
            .into_iter()
            .map(|(val, _cnt)| val)
            .min()
            .unwrap()
            .clone()
            .1)
    }
}

cfg_if! {
    if #[cfg(feature="yolo-id32")] {
        const SALT: u32 = 0xDE_AD_BE_EF_u32;
    } else if #[cfg(feature="yolo-id64")] {
        const SALT: u64 = 0xDE_AD_BE_EF_DE_AD_BE_EF_u64;
    } else {
        const SALT: u128 = 0xDE_AD_BE_EF_DE_AD_BE_EF_DE_AD_BE_EF_DE_AD_BE_EF_u128;
    }
}

pub struct AnyReducer;

impl UnaryReducerImpl for AnyReducer {
    type State = (Key, Value);

    fn init_unary(&self, key: &Key, value: &Value) -> DynResult<Self::State> {
        Ok((*key, value.clone()))
    }

    fn combine<'a>(
        &self,
        values: impl IntoIterator<Item = (&'a Self::State, NonZeroUsize)>,
    ) -> DynResult<Value> {
        Ok(values
            .into_iter()
            .map(|(val, _cnt)| val)
            .min_by_key(|(key, value)| (key.salted_with(SALT), value))
            .unwrap()
            .clone()
            .1)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct MaxReducer;

impl UnaryReducerImpl for MaxReducer {
    type State = Value;

    fn init_unary(&self, _key: &Key, value: &Value) -> DynResult<Self::State> {
        Ok(value.clone())
    }

    fn combine<'a>(
        &self,
        values: impl IntoIterator<Item = (&'a Self::State, NonZeroUsize)>,
    ) -> DynResult<Value> {
        Ok(values
            .into_iter()
            .map(|(val, _cnt)| val)
            .max()
            .unwrap()
            .clone())
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ArgMaxReducer;

impl ReducerImpl for ArgMaxReducer {
    type State = (Value, Value);

    fn init(&self, _key: &Key, values: &[Value]) -> DynResult<Self::State> {
        Ok((values[0].clone(), values[1].clone()))
    }

    fn combine<'a>(
        &self,
        values: impl IntoIterator<Item = (&'a Self::State, NonZeroUsize)>,
    ) -> DynResult<Value> {
        Ok(values
            .into_iter()
            .map(|(val, _cnt)| val)
            .max_by_key(|(value, key)| (value, Reverse(key)))
            .unwrap()
            .clone()
            .1)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct SortedTupleReducer {
    skip_nones: bool,
}

impl SortedTupleReducer {
    pub fn new(skip_nones: bool) -> Self {
        Self { skip_nones }
    }
}

impl UnaryReducerImpl for SortedTupleReducer {
    type State = Vec<Value>;

    fn init_unary(&self, _key: &Key, value: &Value) -> DynResult<Self::State> {
        if *value == Value::None && self.skip_nones {
            Ok(vec![])
        } else {
            Ok(vec![value.clone()])
        }
    }

    fn combine<'a>(
        &self,
        values: impl IntoIterator<Item = (&'a Self::State, NonZeroUsize)>,
    ) -> DynResult<Value> {
        let mut result: Vec<Value> = values
            .into_iter()
            .flat_map(|(state, cnt)| {
                state
                    .iter()
                    .flat_map(move |v| std::iter::repeat_n(v, cnt.get()))
                    .cloned()
            })
            .collect();
        result.sort();
        Ok(result.as_slice().into())
    }
}

#[derive(Debug, Clone, Copy)]
pub struct TupleReducer {
    skip_nones: bool,
}

impl TupleReducer {
    pub fn new(skip_nones: bool) -> Self {
        Self { skip_nones }
    }
}

impl ReducerImpl for TupleReducer {
    type State = Vec<(Option<Value>, Key, Value)>;

    fn init(&self, key: &Key, values: &[Value]) -> DynResult<Self::State> {
        if values[0] == Value::None && self.skip_nones {
            Ok(vec![])
        } else if values.len() > 1 {
            Ok(vec![(Some(values[1].clone()), *key, values[0].clone())])
        } else {
            Ok(vec![(None, *key, values[0].clone())])
        }
    }

    fn combine<'a>(
        &self,
        values: impl IntoIterator<Item = (&'a Self::State, NonZeroUsize)>,
    ) -> DynResult<Value> {
        let mut result: Vec<_> = values
            .into_iter()
            .flat_map(|(state, cnt)| {
                state
                    .iter()
                    .flat_map(move |v| std::iter::repeat_n(v, cnt.get()))
                    .cloned()
            })
            .collect();
        result.sort();
        Ok(result
            .into_iter()
            .map(|(_, _, value)| value)
            .collect::<Vec<Value>>()
            .as_slice()
            .into())
    }
}

#[derive(Debug, Clone, Copy)]
pub struct CountDistinctReducer;

#[derive(Clone)]
pub struct StatefulReducer {
    combine_fn: StatefulCombineFn,
}

impl StatefulReducer {
    pub fn new(combine_fn: StatefulCombineFn) -> Self {
        Self { combine_fn }
    }

    pub fn combine(
        &self,
        state: Option<&Value>,
        data: Vec<(Vec<Value>, isize)>,
    ) -> DynResult<Option<Value>> {
        (self.combine_fn)(state, data)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct LatestReducer;

#[derive(Debug, Clone, Copy)]
pub struct EarliestReducer;
