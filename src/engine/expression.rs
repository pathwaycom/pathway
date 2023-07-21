#![allow(clippy::let_underscore_untyped)] // seems to trigger on Derivative-generated code
#![allow(clippy::module_name_repetitions)]

use arcstr::ArcStr;
use log::warn;
use ndarray::{ArrayD, Axis};
use num_integer::Integer;
use ordered_float::OrderedFloat;
use std::ops::{Deref, Range};
use std::sync::Arc;

use derivative::Derivative;
use itertools::Itertools;
use smallvec::SmallVec;

use super::error::{DynError, DynResult};
use super::time::{DateTime, DateTimeNaive, DateTimeUtc, Duration};
use super::{Error, Key, Value};

#[derive(Debug)]
pub enum Expressions {
    Explicit(SmallVec<[Arc<Expression>; 2]>),
    AllArguments,
    Arguments(Range<usize>),
}

#[derive(Debug)]
pub enum MaybeOwnedValues<'a> {
    Owned(SmallVec<[Value; 2]>),
    Borrowed(&'a [Value]),
}

impl<'a> Deref for MaybeOwnedValues<'a> {
    type Target = [Value];

    fn deref(&self) -> &[Value] {
        match self {
            Self::Owned(values) => values,
            Self::Borrowed(values) => values,
        }
    }
}

impl Expressions {
    pub fn eval<'v>(&self, values: &'v [Value]) -> DynResult<MaybeOwnedValues<'v>> {
        match self {
            Self::Explicit(exprs) => Ok(MaybeOwnedValues::Owned(
                exprs.iter().map(|e| e.eval(values)).try_collect()?,
            )),
            Self::Arguments(range) => Ok(MaybeOwnedValues::Borrowed(&values[range.clone()])),
            Self::AllArguments => Ok(MaybeOwnedValues::Borrowed(values)),
        }
    }
}

fn maybe_argument_slice(source: &[Arc<Expression>]) -> Option<Range<usize>> {
    let mut source = source.iter();
    let first = source.next()?;
    let Expression::Any(AnyExpression::Argument(first_index)) = **first else { return None };
    let mut next_index = first_index + 1;
    for current in source {
        let Expression::Any(AnyExpression::Argument(current_index)) = **current else { return None };
        if current_index != next_index {
            return None;
        }
        next_index = current_index + 1;
    }
    Some(first_index..next_index)
}

impl From<Vec<Arc<Expression>>> for Expressions {
    fn from(source: Vec<Arc<Expression>>) -> Self {
        if let Some(slice) = maybe_argument_slice(&source) {
            return Self::Arguments(slice);
        }

        Self::Explicit(source.into())
    }
}

#[derive(Derivative)]
#[derivative(Debug)]
pub enum AnyExpression {
    Argument(usize),
    Const(Value),
    Apply(
        #[derivative(Debug = "ignore")] Box<dyn Fn(&[Value]) -> DynResult<Value> + Send + Sync>,
        Expressions,
    ),
    IfElse(Arc<Expression>, Arc<Expression>, Arc<Expression>),
    OptionalPointerFrom(Expressions),
    MakeTuple(Expressions),
    TupleGetItemChecked(Arc<Expression>, Arc<Expression>, Arc<Expression>),
    TupleGetItemUnchecked(Arc<Expression>, Arc<Expression>),
}

#[derive(Debug)]
pub enum BoolExpression {
    Const(bool),
    IsNone(Arc<Expression>),
    Not(Arc<Expression>),
    And(Arc<Expression>, Arc<Expression>),
    Or(Arc<Expression>, Arc<Expression>),
    Xor(Arc<Expression>, Arc<Expression>),
    IntEq(Arc<Expression>, Arc<Expression>),
    IntNe(Arc<Expression>, Arc<Expression>),
    IntLt(Arc<Expression>, Arc<Expression>),
    IntLe(Arc<Expression>, Arc<Expression>),
    IntGt(Arc<Expression>, Arc<Expression>),
    IntGe(Arc<Expression>, Arc<Expression>),
    FloatEq(Arc<Expression>, Arc<Expression>),
    FloatNe(Arc<Expression>, Arc<Expression>),
    FloatLt(Arc<Expression>, Arc<Expression>),
    FloatLe(Arc<Expression>, Arc<Expression>),
    FloatGt(Arc<Expression>, Arc<Expression>),
    FloatGe(Arc<Expression>, Arc<Expression>),
    StringEq(Arc<Expression>, Arc<Expression>),
    StringNe(Arc<Expression>, Arc<Expression>),
    StringLt(Arc<Expression>, Arc<Expression>),
    StringLe(Arc<Expression>, Arc<Expression>),
    StringGt(Arc<Expression>, Arc<Expression>),
    StringGe(Arc<Expression>, Arc<Expression>),
    PtrEq(Arc<Expression>, Arc<Expression>),
    PtrNe(Arc<Expression>, Arc<Expression>),
    DateTimeNaiveEq(Arc<Expression>, Arc<Expression>),
    DateTimeNaiveNe(Arc<Expression>, Arc<Expression>),
    DateTimeNaiveLt(Arc<Expression>, Arc<Expression>),
    DateTimeNaiveLe(Arc<Expression>, Arc<Expression>),
    DateTimeNaiveGt(Arc<Expression>, Arc<Expression>),
    DateTimeNaiveGe(Arc<Expression>, Arc<Expression>),
    DateTimeUtcEq(Arc<Expression>, Arc<Expression>),
    DateTimeUtcNe(Arc<Expression>, Arc<Expression>),
    DateTimeUtcLt(Arc<Expression>, Arc<Expression>),
    DateTimeUtcLe(Arc<Expression>, Arc<Expression>),
    DateTimeUtcGt(Arc<Expression>, Arc<Expression>),
    DateTimeUtcGe(Arc<Expression>, Arc<Expression>),
    DurationEq(Arc<Expression>, Arc<Expression>),
    DurationNe(Arc<Expression>, Arc<Expression>),
    DurationLt(Arc<Expression>, Arc<Expression>),
    DurationLe(Arc<Expression>, Arc<Expression>),
    DurationGt(Arc<Expression>, Arc<Expression>),
    DurationGe(Arc<Expression>, Arc<Expression>),
    Eq(Arc<Expression>, Arc<Expression>),
    Ne(Arc<Expression>, Arc<Expression>),
    CastFromFloat(Arc<Expression>),
    CastFromInt(Arc<Expression>),
    CastFromString(Arc<Expression>),
}

#[derive(Debug)]
pub enum IntExpression {
    Const(i64),
    Neg(Arc<Expression>),
    Add(Arc<Expression>, Arc<Expression>),
    Sub(Arc<Expression>, Arc<Expression>),
    Mul(Arc<Expression>, Arc<Expression>),
    FloorDiv(Arc<Expression>, Arc<Expression>),
    Mod(Arc<Expression>, Arc<Expression>),
    Pow(Arc<Expression>, Arc<Expression>),
    Lshift(Arc<Expression>, Arc<Expression>),
    Rshift(Arc<Expression>, Arc<Expression>),
    And(Arc<Expression>, Arc<Expression>),
    Or(Arc<Expression>, Arc<Expression>),
    Xor(Arc<Expression>, Arc<Expression>),
    DateTimeNaiveNanosecond(Arc<Expression>),
    DateTimeNaiveMicrosecond(Arc<Expression>),
    DateTimeNaiveMillisecond(Arc<Expression>),
    DateTimeNaiveSecond(Arc<Expression>),
    DateTimeNaiveMinute(Arc<Expression>),
    DateTimeNaiveHour(Arc<Expression>),
    DateTimeNaiveDay(Arc<Expression>),
    DateTimeNaiveMonth(Arc<Expression>),
    DateTimeNaiveYear(Arc<Expression>),
    DateTimeNaiveTimestamp(Arc<Expression>),
    DateTimeUtcNanosecond(Arc<Expression>),
    DateTimeUtcMicrosecond(Arc<Expression>),
    DateTimeUtcMillisecond(Arc<Expression>),
    DateTimeUtcSecond(Arc<Expression>),
    DateTimeUtcMinute(Arc<Expression>),
    DateTimeUtcHour(Arc<Expression>),
    DateTimeUtcDay(Arc<Expression>),
    DateTimeUtcMonth(Arc<Expression>),
    DateTimeUtcYear(Arc<Expression>),
    DateTimeUtcTimestamp(Arc<Expression>),
    DurationFloorDiv(Arc<Expression>, Arc<Expression>),
    DurationNanoseconds(Arc<Expression>),
    DurationMicroseconds(Arc<Expression>),
    DurationMilliseconds(Arc<Expression>),
    DurationSeconds(Arc<Expression>),
    DurationMinutes(Arc<Expression>),
    DurationHours(Arc<Expression>),
    DurationDays(Arc<Expression>),
    DurationWeeks(Arc<Expression>),
    CastFromBool(Arc<Expression>),
    CastFromFloat(Arc<Expression>),
    CastFromString(Arc<Expression>),
}

#[derive(Debug)]
pub enum FloatExpression {
    Const(f64),
    Neg(Arc<Expression>),
    Add(Arc<Expression>, Arc<Expression>),
    Sub(Arc<Expression>, Arc<Expression>),
    Mul(Arc<Expression>, Arc<Expression>),
    FloorDiv(Arc<Expression>, Arc<Expression>),
    TrueDiv(Arc<Expression>, Arc<Expression>),
    IntTrueDiv(Arc<Expression>, Arc<Expression>),
    Mod(Arc<Expression>, Arc<Expression>),
    Pow(Arc<Expression>, Arc<Expression>),
    DurationTrueDiv(Arc<Expression>, Arc<Expression>),
    CastFromBool(Arc<Expression>),
    CastFromInt(Arc<Expression>),
    CastFromString(Arc<Expression>),
}

#[derive(Debug)]
pub enum StringExpression {
    Add(Arc<Expression>, Arc<Expression>),
    Mul(Arc<Expression>, Arc<Expression>),
    CastFromBool(Arc<Expression>),
    CastFromFloat(Arc<Expression>),
    CastFromInt(Arc<Expression>),
    DateTimeNaiveStrftime(Arc<Expression>, Arc<Expression>),
    DateTimeUtcStrftime(Arc<Expression>, Arc<Expression>),
}

#[derive(Debug)]
pub enum PointerExpression {
    PointerFrom(Expressions),
}

#[derive(Debug)]
pub enum DateTimeNaiveExpression {
    AddDuration(Arc<Expression>, Arc<Expression>),
    SubDuration(Arc<Expression>, Arc<Expression>),
    Strptime(Arc<Expression>, Arc<Expression>),
    FromUtc(Arc<Expression>, Arc<Expression>),
    Round(Arc<Expression>, Arc<Expression>),
    Floor(Arc<Expression>, Arc<Expression>),
    FromTimestamp(Arc<Expression>, Arc<Expression>),
}

#[derive(Debug)]
pub enum DateTimeUtcExpression {
    AddDuration(Arc<Expression>, Arc<Expression>),
    SubDuration(Arc<Expression>, Arc<Expression>),
    Strptime(Arc<Expression>, Arc<Expression>),
    FromNaive(Arc<Expression>, Arc<Expression>),
    Round(Arc<Expression>, Arc<Expression>),
    Floor(Arc<Expression>, Arc<Expression>),
}

#[derive(Debug)]
pub enum DurationExpression {
    Neg(Arc<Expression>),
    Add(Arc<Expression>, Arc<Expression>),
    Sub(Arc<Expression>, Arc<Expression>),
    MulByInt(Arc<Expression>, Arc<Expression>),
    DivByInt(Arc<Expression>, Arc<Expression>),
    Mod(Arc<Expression>, Arc<Expression>),
    DateTimeNaiveSub(Arc<Expression>, Arc<Expression>),
    DateTimeUtcSub(Arc<Expression>, Arc<Expression>),
}

#[derive(Derivative)]
#[derivative(Debug)]
pub enum Expression {
    Bool(BoolExpression),
    Int(IntExpression),
    Float(FloatExpression),
    Pointer(PointerExpression),
    String(StringExpression),
    DateTimeNaive(DateTimeNaiveExpression),
    DateTimeUtc(DateTimeUtcExpression),
    Duration(DurationExpression),
    Any(AnyExpression),
}

fn get_ndarray_element<T>(array: &ArrayD<T>, index: i64) -> DynResult<Option<Value>>
where
    T: Clone,
    Value: From<T>,
    Value: From<ArrayD<T>>,
{
    let length = i64::try_from(array.shape()[0])?;
    let index = if index < 0 { index + length } else { index };
    if index < 0 || length <= index {
        return Ok(None);
    }
    let index_usize = usize::try_from(index).unwrap();
    let row = array.index_axis(Axis(0), index_usize);
    if row.shape().is_empty() {
        Ok(Some(Value::from(row.first().unwrap().clone())))
    } else {
        Ok(Some(Value::from(row.to_owned())))
    }
}

fn get_tuple_element(tuple: &Arc<[Value]>, index: i64) -> DynResult<Option<Value>> {
    let length = i64::try_from(tuple.len())?;
    let index = if index < 0 { index + length } else { index };
    if index < 0 || length <= index {
        return Ok(None);
    }
    let index_usize = usize::try_from(index).unwrap();
    Ok(tuple.get(index_usize).cloned())
}

fn get_array_element(
    expr: &Arc<Expression>,
    index: &Arc<Expression>,
    values: &[Value],
) -> DynResult<Option<Value>> {
    let index = index.eval_as_int(values)?;
    let value = expr.eval(values)?;
    match value {
        Value::IntArray(array) => get_ndarray_element(&array, index),
        Value::FloatArray(array) => get_ndarray_element(&array, index),
        Value::Tuple(tuple) => get_tuple_element(&tuple, index),
        _ => Err(DynError::from(Error::ValueError(format!(
            "Can't get element at index {index} out of {value:?}"
        )))),
    }
}

impl AnyExpression {
    pub fn eval(&self, values: &[Value]) -> DynResult<Value> {
        match self {
            Self::Argument(i) => Ok(values.get(*i).ok_or(Error::IndexOutOfBounds)?.clone()), // XXX: oob
            Self::Const(v) => Ok(v.clone()),
            Self::Apply(f, args) => Ok(f(&args.eval(values)?)?),
            Self::IfElse(if_, then, else_) => Ok(if if_.eval_as_bool(values)? {
                then.eval(values)?
            } else {
                else_.eval(values)?
            }),
            Self::OptionalPointerFrom(args) => {
                let args = args.eval(values)?;
                if args.iter().any(|a| matches!(a, Value::None)) {
                    Ok(Value::None)
                } else {
                    Ok(Value::from(Key::for_values(&args)))
                }
            }
            Self::MakeTuple(args) => {
                let args = args.eval(values)?;
                let args_arc: Arc<[Value]> = (*args).into();
                Ok(Value::Tuple(args_arc))
            }
            Self::TupleGetItemChecked(tuple, index, default) => {
                if let Some(entry) = get_array_element(tuple, index, values)? {
                    Ok(entry)
                } else {
                    Ok(default.eval(values)?)
                }
            }
            Self::TupleGetItemUnchecked(tuple, index) => {
                if let Some(entry) = get_array_element(tuple, index, values)? {
                    Ok(entry)
                } else {
                    Err(DynError::from(Error::IndexOutOfBounds))
                }
            }
        }
    }
}

impl BoolExpression {
    #[allow(clippy::too_many_lines)]
    pub fn eval(&self, values: &[Value]) -> DynResult<bool> {
        match self {
            Self::Const(c) => Ok(*c),
            Self::IsNone(e) => Ok(matches!(e.eval(values)?, Value::None)),
            Self::Not(e) => Ok(!e.eval_as_bool(values)?),
            Self::And(lhs, rhs) => Ok(lhs.eval_as_bool(values)? && rhs.eval_as_bool(values)?),
            Self::Or(lhs, rhs) => Ok(lhs.eval_as_bool(values)? || rhs.eval_as_bool(values)?),
            Self::Xor(lhs, rhs) => Ok(lhs.eval_as_bool(values)? ^ rhs.eval_as_bool(values)?),
            Self::IntEq(lhs, rhs) => Ok(lhs.eval_as_int(values)? == rhs.eval_as_int(values)?),
            Self::IntNe(lhs, rhs) => Ok(lhs.eval_as_int(values)? != rhs.eval_as_int(values)?),
            Self::IntLt(lhs, rhs) => Ok(lhs.eval_as_int(values)? < rhs.eval_as_int(values)?),
            Self::IntLe(lhs, rhs) => Ok(lhs.eval_as_int(values)? <= rhs.eval_as_int(values)?),
            Self::IntGt(lhs, rhs) => Ok(lhs.eval_as_int(values)? > rhs.eval_as_int(values)?),
            Self::IntGe(lhs, rhs) => Ok(lhs.eval_as_int(values)? >= rhs.eval_as_int(values)?),
            #[allow(clippy::float_cmp)]
            Self::FloatEq(lhs, rhs) => Ok(lhs.eval_as_float(values)? == rhs.eval_as_float(values)?),
            #[allow(clippy::float_cmp)]
            Self::FloatNe(lhs, rhs) => Ok(lhs.eval_as_float(values)? != rhs.eval_as_float(values)?),
            Self::FloatLt(lhs, rhs) => Ok(lhs.eval_as_float(values)? < rhs.eval_as_float(values)?),
            Self::FloatLe(lhs, rhs) => Ok(lhs.eval_as_float(values)? <= rhs.eval_as_float(values)?),
            Self::FloatGt(lhs, rhs) => Ok(lhs.eval_as_float(values)? > rhs.eval_as_float(values)?),
            Self::FloatGe(lhs, rhs) => Ok(lhs.eval_as_float(values)? >= rhs.eval_as_float(values)?),
            Self::StringEq(lhs, rhs) => {
                Ok(*lhs.eval_as_string(values)? == *rhs.eval_as_string(values)?)
            }
            Self::StringNe(lhs, rhs) => {
                Ok(*lhs.eval_as_string(values)? != *rhs.eval_as_string(values)?)
            }
            Self::StringLt(lhs, rhs) => {
                Ok(*lhs.eval_as_string(values)? < *rhs.eval_as_string(values)?)
            }
            Self::StringLe(lhs, rhs) => {
                Ok(*lhs.eval_as_string(values)? <= *rhs.eval_as_string(values)?)
            }
            Self::StringGt(lhs, rhs) => {
                Ok(*lhs.eval_as_string(values)? > *rhs.eval_as_string(values)?)
            }
            Self::StringGe(lhs, rhs) => {
                Ok(*lhs.eval_as_string(values)? >= *rhs.eval_as_string(values)?)
            }
            Self::PtrEq(lhs, rhs) => {
                Ok(lhs.eval_as_pointer(values)? == rhs.eval_as_pointer(values)?)
            }
            Self::PtrNe(lhs, rhs) => {
                Ok(lhs.eval_as_pointer(values)? != rhs.eval_as_pointer(values)?)
            }
            Self::DateTimeNaiveEq(lhs, rhs) => {
                Ok(lhs.eval_as_date_time_naive(values)? == rhs.eval_as_date_time_naive(values)?)
            }
            Self::DateTimeNaiveNe(lhs, rhs) => {
                Ok(lhs.eval_as_date_time_naive(values)? != rhs.eval_as_date_time_naive(values)?)
            }
            Self::DateTimeNaiveLt(lhs, rhs) => {
                Ok(lhs.eval_as_date_time_naive(values)? < rhs.eval_as_date_time_naive(values)?)
            }
            Self::DateTimeNaiveLe(lhs, rhs) => {
                Ok(lhs.eval_as_date_time_naive(values)? <= rhs.eval_as_date_time_naive(values)?)
            }
            Self::DateTimeNaiveGt(lhs, rhs) => {
                Ok(lhs.eval_as_date_time_naive(values)? > rhs.eval_as_date_time_naive(values)?)
            }
            Self::DateTimeNaiveGe(lhs, rhs) => {
                Ok(lhs.eval_as_date_time_naive(values)? >= rhs.eval_as_date_time_naive(values)?)
            }
            Self::DateTimeUtcEq(lhs, rhs) => {
                Ok(lhs.eval_as_date_time_utc(values)? == rhs.eval_as_date_time_utc(values)?)
            }
            Self::DateTimeUtcNe(lhs, rhs) => {
                Ok(lhs.eval_as_date_time_utc(values)? != rhs.eval_as_date_time_utc(values)?)
            }
            Self::DateTimeUtcLt(lhs, rhs) => {
                Ok(lhs.eval_as_date_time_utc(values)? < rhs.eval_as_date_time_utc(values)?)
            }
            Self::DateTimeUtcLe(lhs, rhs) => {
                Ok(lhs.eval_as_date_time_utc(values)? <= rhs.eval_as_date_time_utc(values)?)
            }
            Self::DateTimeUtcGt(lhs, rhs) => {
                Ok(lhs.eval_as_date_time_utc(values)? > rhs.eval_as_date_time_utc(values)?)
            }
            Self::DateTimeUtcGe(lhs, rhs) => {
                Ok(lhs.eval_as_date_time_utc(values)? >= rhs.eval_as_date_time_utc(values)?)
            }
            Self::DurationEq(lhs, rhs) => {
                Ok(lhs.eval_as_duration(values)? == rhs.eval_as_duration(values)?)
            }
            Self::DurationNe(lhs, rhs) => {
                Ok(lhs.eval_as_duration(values)? != rhs.eval_as_duration(values)?)
            }
            Self::DurationLt(lhs, rhs) => {
                Ok(lhs.eval_as_duration(values)? < rhs.eval_as_duration(values)?)
            }
            Self::DurationLe(lhs, rhs) => {
                Ok(lhs.eval_as_duration(values)? <= rhs.eval_as_duration(values)?)
            }
            Self::DurationGt(lhs, rhs) => {
                Ok(lhs.eval_as_duration(values)? > rhs.eval_as_duration(values)?)
            }
            Self::DurationGe(lhs, rhs) => {
                Ok(lhs.eval_as_duration(values)? >= rhs.eval_as_duration(values)?)
            }
            Self::Eq(lhs, rhs) => Ok(lhs.eval(values)? == rhs.eval(values)?),
            Self::Ne(lhs, rhs) => Ok(lhs.eval(values)? != rhs.eval(values)?),
            Self::CastFromInt(e) => Ok(e.eval_as_int(values)? != 0),
            Self::CastFromFloat(e) => Ok(e.eval_as_float(values)? != 0.0),
            Self::CastFromString(e) => Ok(!e.eval_as_string(values)?.is_empty()),
        }
    }
}

impl IntExpression {
    pub fn eval(&self, values: &[Value]) -> DynResult<i64> {
        match self {
            Self::Const(c) => Ok(*c),
            Self::Neg(e) => Ok(-e.eval_as_int(values)?),
            Self::Add(lhs, rhs) => Ok(lhs.eval_as_int(values)? + rhs.eval_as_int(values)?),
            Self::Sub(lhs, rhs) => Ok(lhs.eval_as_int(values)? - rhs.eval_as_int(values)?),
            Self::Mul(lhs, rhs) => Ok(lhs.eval_as_int(values)? * rhs.eval_as_int(values)?),
            Self::FloorDiv(lhs, rhs) => {
                let rhs_val = rhs.eval_as_int(values)?;
                if rhs_val == 0 {
                    Err(DynError::from(Error::DivisionByZero))
                } else {
                    Ok(Integer::div_floor(&lhs.eval_as_int(values)?, &rhs_val))
                }
            }
            Self::Mod(lhs, rhs) => {
                let rhs_val = rhs.eval_as_int(values)?;
                if rhs_val == 0 {
                    Err(DynError::from(Error::DivisionByZero))
                } else {
                    Ok(Integer::mod_floor(&lhs.eval_as_int(values)?, &rhs_val))
                }
            }
            #[allow(clippy::cast_possible_truncation)]
            #[allow(clippy::cast_sign_loss)]
            Self::Pow(lhs, rhs) => Ok(lhs
                .eval(values)?
                .as_int()?
                .pow(rhs.eval_as_int(values)? as u32)),
            Self::Lshift(lhs, rhs) => Ok(lhs.eval_as_int(values)? << rhs.eval_as_int(values)?),
            Self::Rshift(lhs, rhs) => Ok(lhs.eval_as_int(values)? >> rhs.eval_as_int(values)?),
            Self::And(lhs, rhs) => Ok(lhs.eval_as_int(values)? & rhs.eval_as_int(values)?),
            Self::Or(lhs, rhs) => Ok(lhs.eval_as_int(values)? | rhs.eval_as_int(values)?),
            Self::Xor(lhs, rhs) => Ok(lhs.eval_as_int(values)? ^ rhs.eval_as_int(values)?),
            Self::DateTimeNaiveNanosecond(e) => Ok(e.eval_as_date_time_naive(values)?.nanosecond()),
            Self::DateTimeNaiveMicrosecond(e) => {
                Ok(e.eval_as_date_time_naive(values)?.microsecond())
            }
            Self::DateTimeNaiveMillisecond(e) => {
                Ok(e.eval_as_date_time_naive(values)?.millisecond())
            }
            Self::DateTimeNaiveSecond(e) => Ok(e.eval_as_date_time_naive(values)?.second()),
            Self::DateTimeNaiveMinute(e) => Ok(e.eval_as_date_time_naive(values)?.minute()),
            Self::DateTimeNaiveHour(e) => Ok(e.eval_as_date_time_naive(values)?.hour()),
            Self::DateTimeNaiveDay(e) => Ok(e.eval_as_date_time_naive(values)?.day()),
            Self::DateTimeNaiveMonth(e) => Ok(e.eval_as_date_time_naive(values)?.month()),
            Self::DateTimeNaiveYear(e) => Ok(e.eval_as_date_time_naive(values)?.year()),
            Self::DateTimeNaiveTimestamp(e) => Ok(e.eval_as_date_time_naive(values)?.timestamp()),
            Self::DateTimeUtcNanosecond(e) => Ok(e.eval_as_date_time_utc(values)?.nanosecond()),
            Self::DateTimeUtcMicrosecond(e) => Ok(e.eval_as_date_time_utc(values)?.microsecond()),
            Self::DateTimeUtcMillisecond(e) => Ok(e.eval_as_date_time_utc(values)?.millisecond()),
            Self::DateTimeUtcSecond(e) => Ok(e.eval_as_date_time_utc(values)?.second()),
            Self::DateTimeUtcMinute(e) => Ok(e.eval_as_date_time_utc(values)?.minute()),
            Self::DateTimeUtcHour(e) => Ok(e.eval_as_date_time_utc(values)?.hour()),
            Self::DateTimeUtcDay(e) => Ok(e.eval_as_date_time_utc(values)?.day()),
            Self::DateTimeUtcMonth(e) => Ok(e.eval_as_date_time_utc(values)?.month()),
            Self::DateTimeUtcYear(e) => Ok(e.eval_as_date_time_utc(values)?.year()),
            Self::DateTimeUtcTimestamp(e) => Ok(e.eval_as_date_time_utc(values)?.timestamp()),
            Self::DurationFloorDiv(lhs, rhs) => {
                match lhs.eval_as_duration(values)? / rhs.eval_as_duration(values)? {
                    Ok(result) => Ok(result),
                    Err(err) => Err(DynError::from(err)),
                }
            }
            Self::DurationNanoseconds(e) => Ok(e.eval_as_duration(values)?.nanoseconds()),
            Self::DurationMicroseconds(e) => Ok(e.eval_as_duration(values)?.microseconds()),
            Self::DurationMilliseconds(e) => Ok(e.eval_as_duration(values)?.milliseconds()),
            Self::DurationSeconds(e) => Ok(e.eval_as_duration(values)?.seconds()),
            Self::DurationMinutes(e) => Ok(e.eval_as_duration(values)?.minutes()),
            Self::DurationHours(e) => Ok(e.eval_as_duration(values)?.hours()),
            Self::DurationDays(e) => Ok(e.eval_as_duration(values)?.days()),
            Self::DurationWeeks(e) => Ok(e.eval_as_duration(values)?.weeks()),
            #[allow(clippy::cast_possible_truncation)]
            Self::CastFromFloat(e) => Ok(e.eval_as_float(values)? as i64),
            Self::CastFromBool(e) => Ok(i64::from(e.eval_as_bool(values)?)),
            Self::CastFromString(e) => {
                let val = e.eval(values)?;
                let val_str = val.as_string()?.trim();
                val_str.parse().map_err(|_| {
                    DynError::from(Error::ParseError(format!(
                        "Cannot cast to int from {val_str}.",
                    )))
                })
            }
        }
    }
}

impl FloatExpression {
    pub fn eval(&self, values: &[Value]) -> DynResult<f64> {
        match self {
            Self::Const(c) => Ok(*c),
            Self::Neg(e) => Ok(-e.eval_as_float(values)?),
            Self::Add(lhs, rhs) => Ok(lhs.eval_as_float(values)? + rhs.eval_as_float(values)?),
            Self::Sub(lhs, rhs) => Ok(lhs.eval_as_float(values)? - rhs.eval_as_float(values)?),
            Self::Mul(lhs, rhs) => Ok(lhs.eval_as_float(values)? * rhs.eval_as_float(values)?),
            Self::FloorDiv(lhs, rhs) => {
                let rhs_val = rhs.eval_as_float(values)?;
                if rhs_val == 0.0f64 {
                    Err(DynError::from(Error::DivisionByZero))
                } else {
                    Ok((lhs.eval_as_float(values)? / rhs_val).floor())
                }
            }
            Self::TrueDiv(lhs, rhs) => {
                let rhs_val = rhs.eval_as_float(values)?;
                if rhs_val == 0.0f64 {
                    Err(DynError::from(Error::DivisionByZero))
                } else {
                    Ok(lhs.eval_as_float(values)? / rhs_val)
                }
            }
            Self::Mod(lhs, rhs) => {
                /*
                Implementation the same as the one in Cpython
                https://github.com/python/cpython/blob/main/Objects/floatobject.c#L640
                */
                let lhs_val = lhs.eval_as_float(values)?;
                let rhs_val = rhs.eval_as_float(values)?;
                if rhs_val == 0.0f64 {
                    return Err(DynError::from(Error::DivisionByZero));
                }
                let mut modulo = lhs_val % rhs_val;
                if modulo == 0.0f64 {
                    modulo = modulo.copysign(rhs_val);
                } else if (rhs_val < 0.0f64) != (modulo < 0.0f64) {
                    modulo += rhs_val;
                }
                Ok(modulo)
            }
            Self::Pow(lhs, rhs) => {
                let result = lhs.eval_as_float(values)?.powf(rhs.eval_as_float(values)?);
                if result.is_infinite() {
                    warn!("overflow encountered in power.");
                }
                Ok(result)
            }
            #[allow(clippy::cast_precision_loss)]
            Self::IntTrueDiv(lhs, rhs) => {
                let rhs_val = rhs.eval_as_int(values)?;
                if rhs_val == 0 {
                    Err(DynError::from(Error::DivisionByZero))
                } else {
                    Ok(lhs.eval_as_int(values)? as f64 / rhs_val as f64)
                }
            }
            Self::DurationTrueDiv(lhs, rhs) => match lhs
                .eval_as_duration(values)?
                .true_div(rhs.eval_as_duration(values)?)
            {
                Ok(result) => Ok(result),
                Err(err) => Err(DynError::from(err)),
            },
            Self::CastFromBool(e) => Ok(if e.eval_as_bool(values)? { 1.0 } else { 0.0 }),
            #[allow(clippy::cast_precision_loss)]
            Self::CastFromInt(e) => Ok(e.eval_as_int(values)? as f64),
            Self::CastFromString(e) => {
                let val = e.eval(values)?;
                let val_str = val.as_string()?.trim();
                val_str.parse().map_err(|_| {
                    DynError::from(Error::ParseError(format!(
                        "Cannot cast to float from {val}."
                    )))
                })
            }
        }
    }
}

impl PointerExpression {
    pub fn eval(&self, values: &[Value]) -> DynResult<Key> {
        match self {
            Self::PointerFrom(args) => Ok(Key::for_values(&args.eval(values)?)),
        }
    }
}

impl StringExpression {
    pub fn eval(&self, values: &[Value]) -> DynResult<ArcStr> {
        match self {
            Self::Add(lhs, rhs) => {
                let lhs = lhs.eval_as_string(values)?;
                let rhs = rhs.eval_as_string(values)?;
                if lhs.is_empty() {
                    Ok(rhs)
                } else if rhs.is_empty() {
                    Ok(lhs)
                } else {
                    Ok(ArcStr::from(([lhs, rhs]).concat()))
                }
            }
            Self::Mul(lhs, rhs) => {
                let repeat = rhs.eval_as_int(values)?;
                if repeat < 0 {
                    Ok(ArcStr::new())
                } else {
                    let repeat = usize::try_from(repeat).unwrap();
                    let result = ArcStr::repeat(&lhs.eval_as_string(values)?, repeat);
                    Ok(result)
                }
            }
            Self::CastFromInt(e) => Ok(e.eval_as_int(values)?.to_string().into()),
            Self::CastFromFloat(e) => Ok(e.eval_as_float(values)?.to_string().into()),
            Self::CastFromBool(e) => Ok(if e.eval_as_bool(values)? {
                arcstr::literal!("True")
            } else {
                arcstr::literal!("False")
            }),
            Self::DateTimeNaiveStrftime(e, fmt) => Ok(ArcStr::from(
                e.eval_as_date_time_naive(values)?
                    .strftime(&fmt.eval_as_string(values)?),
            )),
            Self::DateTimeUtcStrftime(e, fmt) => Ok(ArcStr::from(
                e.eval_as_date_time_utc(values)?
                    .strftime(&fmt.eval_as_string(values)?),
            )),
        }
    }
}

impl DateTimeNaiveExpression {
    pub fn eval(&self, values: &[Value]) -> DynResult<DateTimeNaive> {
        match self {
            Self::AddDuration(lhs, rhs) => {
                Ok(lhs.eval_as_date_time_naive(values)? + rhs.eval_as_duration(values)?)
            }
            Self::SubDuration(lhs, rhs) => {
                Ok(lhs.eval_as_date_time_naive(values)? - rhs.eval_as_duration(values)?)
            }
            Self::Strptime(e, fmt) => {
                match DateTimeNaive::strptime(
                    &e.eval_as_string(values)?,
                    &fmt.eval_as_string(values)?,
                ) {
                    Ok(result) => Ok(result),
                    Err(err) => Err(DynError::from(err)),
                }
            }
            Self::FromUtc(expr, timezone) => {
                match expr
                    .eval_as_date_time_utc(values)?
                    .to_naive_in_timezone(&timezone.eval_as_string(values)?)
                {
                    Ok(result) => Ok(result),
                    Err(err) => Err(DynError::from(err)),
                }
            }
            Self::Round(expr, duration) => Ok(expr
                .eval_as_date_time_naive(values)?
                .round(duration.eval_as_duration(values)?)),
            Self::Floor(expr, duration) => Ok(expr
                .eval_as_date_time_naive(values)?
                .truncate(duration.eval_as_duration(values)?)),
            Self::FromTimestamp(expr, unit) => {
                match DateTimeNaive::from_timestamp(
                    expr.eval_as_int(values)?,
                    &unit.eval_as_string(values)?,
                ) {
                    Ok(result) => Ok(result),
                    Err(err) => Err(DynError::from(err)),
                }
            }
        }
    }
}

impl DateTimeUtcExpression {
    pub fn eval(&self, values: &[Value]) -> DynResult<DateTimeUtc> {
        match self {
            Self::AddDuration(lhs, rhs) => {
                Ok(lhs.eval_as_date_time_utc(values)? + rhs.eval_as_duration(values)?)
            }
            Self::SubDuration(lhs, rhs) => {
                Ok(lhs.eval_as_date_time_utc(values)? - rhs.eval_as_duration(values)?)
            }
            Self::Strptime(e, fmt) => {
                match DateTimeUtc::strptime(
                    &e.eval_as_string(values)?,
                    &fmt.eval_as_string(values)?,
                ) {
                    Ok(result) => Ok(result),
                    Err(err) => Err(DynError::from(err)),
                }
            }
            Self::FromNaive(expr, from_timezone) => {
                match expr
                    .eval_as_date_time_naive(values)?
                    .to_utc_from_timezone(&from_timezone.eval_as_string(values)?)
                {
                    Ok(result) => Ok(result),
                    Err(err) => Err(DynError::from(err)),
                }
            }
            Self::Round(expr, duration) => Ok(expr
                .eval_as_date_time_utc(values)?
                .round(duration.eval_as_duration(values)?)),
            Self::Floor(expr, duration) => Ok(expr
                .eval_as_date_time_utc(values)?
                .truncate(duration.eval_as_duration(values)?)),
        }
    }
}

impl DurationExpression {
    pub fn eval(&self, values: &[Value]) -> DynResult<Duration> {
        match self {
            Self::Neg(e) => Ok(-e.eval_as_duration(values)?),
            Self::Add(lhs, rhs) => {
                Ok(lhs.eval_as_duration(values)? + rhs.eval_as_duration(values)?)
            }
            Self::Sub(lhs, rhs) => {
                Ok(lhs.eval_as_duration(values)? - rhs.eval_as_duration(values)?)
            }
            Self::MulByInt(lhs, rhs) => {
                Ok(lhs.eval_as_duration(values)? * rhs.eval_as_int(values)?)
            }
            Self::DivByInt(lhs, rhs) => {
                match lhs.eval_as_duration(values)? / rhs.eval_as_int(values)? {
                    Ok(result) => Ok(result),
                    Err(err) => Err(DynError::from(err)),
                }
            }
            Self::Mod(lhs, rhs) => {
                match lhs.eval_as_duration(values)? % rhs.eval_as_duration(values)? {
                    Ok(result) => Ok(result),
                    Err(err) => Err(DynError::from(err)),
                }
            }
            Self::DateTimeNaiveSub(lhs, rhs) => {
                Ok(lhs.eval_as_date_time_naive(values)? - rhs.eval_as_date_time_naive(values)?)
            }
            Self::DateTimeUtcSub(lhs, rhs) => {
                Ok(lhs.eval_as_date_time_utc(values)? - rhs.eval_as_date_time_utc(values)?)
            }
        }
    }
}

impl Expression {
    pub fn eval(&self, values: &[Value]) -> DynResult<Value> {
        match self {
            Self::Bool(expr) => Ok(Value::from(expr.eval(values)?)),
            Self::Int(expr) => Ok(Value::from(expr.eval(values)?)),
            Self::Float(expr) => Ok(Value::from(expr.eval(values)?)),
            Self::Pointer(expr) => Ok(Value::from(expr.eval(values)?)),
            Self::String(expr) => Ok(Value::from(expr.eval(values)?)),
            Self::DateTimeNaive(expr) => Ok(Value::from(expr.eval(values)?)),
            Self::DateTimeUtc(expr) => Ok(Value::from(expr.eval(values)?)),
            Self::Duration(expr) => Ok(Value::from(expr.eval(values)?)),
            Self::Any(expr) => Ok(expr.eval(values)?),
        }
    }

    #[cold]
    #[inline(never)]
    fn type_error(&self, expected: &'static str) -> DynError {
        let actual = match self {
            Self::Bool(_) => "bool",
            Self::Int(_) => "int",
            Self::Float(_) => "float",
            Self::Pointer(_) => "pointer",
            Self::String(_) => "string",
            Self::DateTimeNaive(_) => "DateTimeNaive",
            Self::DateTimeUtc(_) => "DateTimeUtc",
            Self::Duration(_) => "Duration",
            Self::Any(_) => "unknown type",
        };
        DynError::from(Error::ColumnTypeMismatch { expected, actual })
    }

    pub fn eval_as_bool(&self, values: &[Value]) -> DynResult<bool> {
        match self {
            Self::Bool(expr) => expr.eval(values),
            Self::Any(expr) => expr.eval(values)?.as_bool(),
            _ => Err(self.type_error("bool")),
        }
    }

    pub fn eval_as_int(&self, values: &[Value]) -> DynResult<i64> {
        match self {
            Self::Int(expr) => expr.eval(values),
            Self::Any(expr) => expr.eval(values)?.as_int(),
            _ => Err(self.type_error("int")),
        }
    }

    pub fn eval_as_float(&self, values: &[Value]) -> DynResult<f64> {
        match self {
            Self::Float(expr) => expr.eval(values),
            Self::Any(expr) => expr.eval(values)?.as_float(),
            _ => Err(self.type_error("float")),
        }
    }

    pub fn eval_as_pointer(&self, values: &[Value]) -> DynResult<Key> {
        match self {
            Self::Pointer(expr) => expr.eval(values),
            Self::Any(expr) => expr.eval(values)?.as_pointer(),
            _ => Err(self.type_error("pointer")),
        }
    }

    pub fn eval_as_string(&self, values: &[Value]) -> DynResult<ArcStr> {
        match self {
            Self::String(expr) => expr.eval(values),
            Self::Any(expr) => Ok(expr.eval(values)?.as_string()?.clone()),
            _ => Err(self.type_error("string")),
        }
    }

    pub fn eval_as_date_time_naive(&self, values: &[Value]) -> DynResult<DateTimeNaive> {
        match self {
            Self::DateTimeNaive(expr) => expr.eval(values),
            Self::Any(expr) => expr.eval(values)?.as_date_time_naive(),
            _ => Err(self.type_error("DateTimeNaive")),
        }
    }

    pub fn eval_as_date_time_utc(&self, values: &[Value]) -> DynResult<DateTimeUtc> {
        match self {
            Self::DateTimeUtc(expr) => expr.eval(values),
            Self::Any(expr) => expr.eval(values)?.as_date_time_utc(),
            _ => Err(self.type_error("DateTimeUtc")),
        }
    }

    pub fn eval_as_duration(&self, values: &[Value]) -> DynResult<Duration> {
        match self {
            Self::Duration(expr) => expr.eval(values),
            Self::Any(expr) => expr.eval(values)?.as_duration(),
            _ => Err(self.type_error("Duration")),
        }
    }

    pub fn new_const(value: Value) -> Self {
        match value {
            Value::Bool(b) => Self::Bool(BoolExpression::Const(b)),
            Value::Int(i) => Self::Int(IntExpression::Const(i)),
            Value::Float(OrderedFloat(f)) => Self::Float(FloatExpression::Const(f)),
            other => Self::Any(AnyExpression::Const(other)),
        }
    }
}

impl From<BoolExpression> for Expression {
    fn from(expr: BoolExpression) -> Self {
        Self::Bool(expr)
    }
}

impl From<IntExpression> for Expression {
    fn from(expr: IntExpression) -> Self {
        Self::Int(expr)
    }
}

impl From<FloatExpression> for Expression {
    fn from(expr: FloatExpression) -> Self {
        Self::Float(expr)
    }
}

impl From<PointerExpression> for Expression {
    fn from(expr: PointerExpression) -> Self {
        Self::Pointer(expr)
    }
}

impl From<StringExpression> for Expression {
    fn from(expr: StringExpression) -> Self {
        Self::String(expr)
    }
}

impl From<DateTimeNaiveExpression> for Expression {
    fn from(expr: DateTimeNaiveExpression) -> Self {
        Self::DateTimeNaive(expr)
    }
}

impl From<DateTimeUtcExpression> for Expression {
    fn from(expr: DateTimeUtcExpression) -> Self {
        Self::DateTimeUtc(expr)
    }
}

impl From<DurationExpression> for Expression {
    fn from(expr: DurationExpression) -> Self {
        Self::Duration(expr)
    }
}

impl From<AnyExpression> for Expression {
    fn from(expr: AnyExpression) -> Self {
        Self::Any(expr)
    }
}
