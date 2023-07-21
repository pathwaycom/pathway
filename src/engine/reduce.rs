use ndarray::prelude::*;

use cfg_if::cfg_if;
use differential_dataflow::{
    difference::{Multiply, Semigroup},
    ExchangeData,
};
use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};
use std::cmp::Reverse;
use std::iter::repeat;
use std::num::NonZeroUsize;
use std::ops::Add;

use super::{Key, Value};

#[derive(Debug, Clone, Copy)]
pub enum Reducer {
    Sum,
    IntSum,
    Unique,
    Min,
    ArgMin,
    Max,
    ArgMax,
    SortedTuple,
    Any,
}

pub trait SemigroupReducerImpl: 'static {
    type State: ExchangeData + Semigroup + Multiply<isize>;

    fn init(&self, key: &Key, value: &Value) -> Option<Self::State>;

    fn finish(&self, state: Self::State) -> Value;
}

pub trait ReducerImpl: 'static {
    type State: ExchangeData;

    fn init(&self, key: &Key, value: &Value) -> Option<Self::State>;

    fn combine<'a>(
        &self,
        values: impl IntoIterator<Item = (&'a Self::State, NonZeroUsize)>,
    ) -> Self::State;

    fn neutral(&self) -> Option<Value>;

    fn finish(&self, state: Self::State) -> Value;
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

impl IntSumState {
    pub fn single(val: i64) -> Self {
        Self { count: 1, sum: val }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct IntSumReducer;

impl SemigroupReducerImpl for IntSumReducer {
    type State = IntSumState;

    fn init(&self, _key: &Key, value: &Value) -> Option<Self::State> {
        match value {
            Value::Int(i) => Some(IntSumState::single(*i)),
            _ => panic!("unsupported type for int_sum"),
        }
    }

    fn finish(&self, state: Self::State) -> Value {
        Value::Int(state.sum)
    }
}
#[derive(Debug, Clone, Copy)]
pub struct SumReducer;

#[derive(Debug)]
enum SumState<'a> {
    Int(i64),
    Float(f64),
    IntArray(CowArray<'a, i64, IxDyn>),
    FloatArray(CowArray<'a, f64, IxDyn>),
}

impl<'a> SumState<'a> {
    fn new(value: &'a Value, cnt: NonZeroUsize) -> Self {
        match value {
            Value::Int(i) => Self::Int(i * i64::try_from(cnt.get()).unwrap()),
            #[allow(clippy::cast_precision_loss)]
            Value::Float(OrderedFloat(f)) => Self::Float(f * cnt.get() as f64),
            Value::IntArray(array) => {
                if cnt.get() == 1 {
                    Self::IntArray(CowArray::from(&**array))
                } else {
                    Self::IntArray(CowArray::from(&**array * i64::try_from(cnt.get()).unwrap()))
                }
            }
            #[allow(clippy::cast_precision_loss)]
            Value::FloatArray(array) => {
                if cnt.get() == 1 {
                    Self::FloatArray(CowArray::from(&**array))
                } else {
                    Self::FloatArray(CowArray::from(&**array * cnt.get() as f64))
                }
            }
            _ => panic!("unsupported type for sum"),
        }
    }
}

impl<'a> Add for SumState<'a> {
    type Output = Self;

    fn add(self, rhs: Self) -> Self {
        match (self, rhs) {
            (Self::Int(lhs), Self::Int(rhs)) => Self::Int(lhs + rhs),
            (Self::Float(lhs), Self::Float(rhs)) => Self::Float(lhs + rhs),
            (Self::IntArray(lhs), Self::IntArray(rhs)) => {
                Self::IntArray(CowArray::from(lhs.into_owned() + &rhs))
            }
            (Self::FloatArray(lhs), Self::FloatArray(rhs)) => {
                Self::FloatArray(CowArray::from(lhs.into_owned() + &rhs))
            }
            _ => panic!("mixing types in sum is not allowed"),
        }
    }
}

impl<'a> From<SumState<'a>> for Value {
    fn from(state: SumState<'a>) -> Self {
        match state {
            SumState::Int(i) => Self::from(i),
            SumState::Float(f) => Self::from(f),
            SumState::IntArray(a) => Self::from(a.into_owned()),
            SumState::FloatArray(a) => Self::from(a.into_owned()),
        }
    }
}

impl ReducerImpl for SumReducer {
    type State = Value;

    fn neutral(&self) -> Option<Value> {
        Some(Value::Int(0)) // XXX
    }

    fn init(&self, _key: &Key, value: &Value) -> Option<Self::State> {
        Some(value.clone())
    }

    fn combine<'a>(
        &self,
        values: impl IntoIterator<Item = (&'a Self::State, NonZeroUsize)>,
    ) -> Self::State {
        values
            .into_iter()
            .map(|(value, cnt)| SumState::new(value, cnt))
            .reduce(|a, b| a + b)
            .expect("values should not be empty")
            .into()
    }

    fn finish(&self, state: Self::State) -> Value {
        state
    }
}

#[derive(Debug, Clone, Copy)]
pub struct UniqueReducer;

impl ReducerImpl for UniqueReducer {
    type State = Option<Value>;

    fn neutral(&self) -> Option<Value> {
        None
    }

    fn init(&self, _key: &Key, value: &Value) -> Option<Self::State> {
        Some(Some(value.clone()))
    }

    fn combine<'a>(
        &self,
        values: impl IntoIterator<Item = (&'a Self::State, NonZeroUsize)>,
    ) -> Self::State {
        let mut values = values.into_iter();
        let (state, _cnt) = values.next().unwrap();
        assert!(
            values.next().is_none(),
            "More than one distinct value passed to the unique reducer."
        );
        state.clone()
    }

    fn finish(&self, state: Self::State) -> Value {
        match state {
            Some(v) => v,
            None => Value::None,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct MinReducer;

impl ReducerImpl for MinReducer {
    type State = Value;

    fn neutral(&self) -> Option<Value> {
        None
    }

    fn init(&self, _key: &Key, value: &Value) -> Option<Self::State> {
        Some(value.clone())
    }

    fn combine<'a>(
        &self,
        values: impl IntoIterator<Item = (&'a Self::State, NonZeroUsize)>,
    ) -> Self::State {
        values
            .into_iter()
            .map(|(val, _cnt)| val)
            .min()
            .unwrap()
            .clone()
    }

    fn finish(&self, state: Self::State) -> Value {
        state
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ArgMinReducer;

impl ReducerImpl for ArgMinReducer {
    type State = (Value, Key);

    fn neutral(&self) -> Option<Value> {
        None
    }

    fn init(&self, key: &Key, value: &Value) -> Option<Self::State> {
        Some((value.clone(), *key))
    }

    fn combine<'a>(
        &self,
        values: impl IntoIterator<Item = (&'a Self::State, NonZeroUsize)>,
    ) -> Self::State {
        values
            .into_iter()
            .map(|(val, _cnt)| val)
            .min()
            .unwrap()
            .clone()
    }

    fn finish(&self, state: Self::State) -> Value {
        Value::Pointer(state.1)
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

impl ReducerImpl for AnyReducer {
    type State = (Key, Value);

    fn neutral(&self) -> Option<Value> {
        None
    }

    fn init(&self, key: &Key, value: &Value) -> Option<Self::State> {
        Some((*key, value.clone()))
    }

    fn combine<'a>(
        &self,
        values: impl IntoIterator<Item = (&'a Self::State, NonZeroUsize)>,
    ) -> Self::State {
        values
            .into_iter()
            .map(|(val, _cnt)| val)
            .min_by_key(|(key, value)| (key.salted_with(SALT), value))
            .unwrap()
            .clone()
    }

    fn finish(&self, state: Self::State) -> Value {
        state.1
    }
}

#[derive(Debug, Clone, Copy)]
pub struct MaxReducer;

impl ReducerImpl for MaxReducer {
    type State = Value;

    fn neutral(&self) -> Option<Value> {
        None
    }

    fn init(&self, _key: &Key, value: &Value) -> Option<Self::State> {
        Some(value.clone())
    }

    fn combine<'a>(
        &self,
        values: impl IntoIterator<Item = (&'a Self::State, NonZeroUsize)>,
    ) -> Self::State {
        values
            .into_iter()
            .map(|(val, _cnt)| val)
            .max()
            .unwrap()
            .clone()
    }

    fn finish(&self, state: Self::State) -> Value {
        state
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ArgMaxReducer;

impl ReducerImpl for ArgMaxReducer {
    type State = (Value, Key);

    fn neutral(&self) -> Option<Value> {
        None
    }

    fn init(&self, key: &Key, value: &Value) -> Option<Self::State> {
        Some((value.clone(), *key))
    }

    fn combine<'a>(
        &self,
        values: impl IntoIterator<Item = (&'a Self::State, NonZeroUsize)>,
    ) -> Self::State {
        values
            .into_iter()
            .map(|(val, _cnt)| val)
            .max_by_key(|(value, key)| (value, Reverse(key)))
            .unwrap()
            .clone()
    }

    fn finish(&self, state: Self::State) -> Value {
        Value::Pointer(state.1)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct SortedTupleReducer;

impl ReducerImpl for SortedTupleReducer {
    type State = Vec<Value>;

    fn init(&self, _key: &Key, value: &Value) -> Option<Self::State> {
        Some(vec![value.clone()])
    }

    fn combine<'a>(
        &self,
        values: impl IntoIterator<Item = (&'a Self::State, NonZeroUsize)>,
    ) -> Self::State {
        values
            .into_iter()
            .flat_map(|(state, cnt)| {
                state
                    .iter()
                    .flat_map(move |v| repeat(v).take(cnt.get()))
                    .cloned()
            })
            .collect()
    }

    fn neutral(&self) -> Option<Value> {
        Some(Value::from(&[][..]))
    }

    fn finish(&self, mut state: Self::State) -> Value {
        state.sort();
        state.as_slice().into()
    }
}
