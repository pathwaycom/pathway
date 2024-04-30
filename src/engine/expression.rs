// Copyright © 2024 Pathway

#![allow(clippy::module_name_repetitions)]

use arcstr::ArcStr;
use log::warn;
use ndarray::{ArrayD, Axis, LinalgScalar};
use num_integer::Integer;
use ordered_float::OrderedFloat;
use std::cmp::Ordering;
use std::ops::{Deref, Range};
use std::sync::Arc;

use derivative::Derivative;
use itertools::Itertools;
use smallvec::SmallVec;

use super::error::{DataError, DynError, DynResult};
use super::time::{DateTime, DateTimeNaive, DateTimeUtc, Duration};
use super::value::SimpleType;
use super::{Key, Type, Value};
use crate::engine::ShardPolicy;
use crate::mat_mul::mat_mul;

#[derive(Debug)]
pub enum Expressions {
    Explicit(SmallVec<[Arc<Expression>; 2]>),
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
            Self::Arguments(range) => {
                let values = &values[range.clone()];
                if values.contains(&Value::Error) {
                    Err(DataError::ErrorInValue.into())
                } else {
                    Ok(MaybeOwnedValues::Borrowed(values))
                }
            }
        }
    }
}

fn maybe_argument_slice(source: &[Arc<Expression>]) -> Option<Range<usize>> {
    let mut source = source.iter();
    let first = source.next()?;
    let Expression::Any(AnyExpression::Argument(first_index)) = **first else {
        return None;
    };
    let mut next_index = first_index + 1;
    for current in source {
        let Expression::Any(AnyExpression::Argument(current_index)) = **current else {
            return None;
        };
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
    OptionalApply(
        #[derivative(Debug = "ignore")] Box<dyn Fn(&[Value]) -> DynResult<Value> + Send + Sync>,
        Expressions,
    ),
    IfElse(Arc<Expression>, Arc<Expression>, Arc<Expression>),
    OptionalPointerFrom(Expressions),
    OptionalPointerWithInstanceFrom(Expressions, Arc<Expression>),
    MakeTuple(Expressions),
    TupleGetItemChecked(Arc<Expression>, Arc<Expression>, Arc<Expression>),
    TupleGetItemUnchecked(Arc<Expression>, Arc<Expression>),
    JsonGetItem(Arc<Expression>, Arc<Expression>, Arc<Expression>),
    JsonToOptional(Arc<Expression>, Type),
    ParseStringToInt(Arc<Expression>, bool),
    ParseStringToFloat(Arc<Expression>, bool),
    ParseStringToBool(Arc<Expression>, Vec<String>, Vec<String>, bool),
    Unwrap(Arc<Expression>),
    CastToOptionalIntFromOptionalFloat(Arc<Expression>),
    CastToOptionalFloatFromOptionalInt(Arc<Expression>),
    MatMul(Arc<Expression>, Arc<Expression>),
    FillError(Arc<Expression>, Arc<Expression>),
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
    PtrLe(Arc<Expression>, Arc<Expression>),
    PtrLt(Arc<Expression>, Arc<Expression>),
    PtrGe(Arc<Expression>, Arc<Expression>),
    PtrGt(Arc<Expression>, Arc<Expression>),
    BoolEq(Arc<Expression>, Arc<Expression>),
    BoolNe(Arc<Expression>, Arc<Expression>),
    BoolLe(Arc<Expression>, Arc<Expression>),
    BoolLt(Arc<Expression>, Arc<Expression>),
    BoolGe(Arc<Expression>, Arc<Expression>),
    BoolGt(Arc<Expression>, Arc<Expression>),
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
    TupleEq(Arc<Expression>, Arc<Expression>),
    TupleNe(Arc<Expression>, Arc<Expression>),
    TupleLe(Arc<Expression>, Arc<Expression>),
    TupleLt(Arc<Expression>, Arc<Expression>),
    TupleGe(Arc<Expression>, Arc<Expression>),
    TupleGt(Arc<Expression>, Arc<Expression>),
    CastFromFloat(Arc<Expression>),
    CastFromInt(Arc<Expression>),
    CastFromString(Arc<Expression>),
}

#[derive(Debug)]
pub enum IntExpression {
    Const(i64),
    Neg(Arc<Expression>),
    Abs(Arc<Expression>),
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
    DateTimeNaiveTimestampNs(Arc<Expression>),
    DateTimeNaiveWeekday(Arc<Expression>),
    DateTimeUtcNanosecond(Arc<Expression>),
    DateTimeUtcMicrosecond(Arc<Expression>),
    DateTimeUtcMillisecond(Arc<Expression>),
    DateTimeUtcSecond(Arc<Expression>),
    DateTimeUtcMinute(Arc<Expression>),
    DateTimeUtcHour(Arc<Expression>),
    DateTimeUtcDay(Arc<Expression>),
    DateTimeUtcMonth(Arc<Expression>),
    DateTimeUtcYear(Arc<Expression>),
    DateTimeUtcTimestampNs(Arc<Expression>),
    DateTimeUtcWeekday(Arc<Expression>),
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
    Abs(Arc<Expression>),
    Add(Arc<Expression>, Arc<Expression>),
    Sub(Arc<Expression>, Arc<Expression>),
    Mul(Arc<Expression>, Arc<Expression>),
    FloorDiv(Arc<Expression>, Arc<Expression>),
    TrueDiv(Arc<Expression>, Arc<Expression>),
    IntTrueDiv(Arc<Expression>, Arc<Expression>),
    Mod(Arc<Expression>, Arc<Expression>),
    Pow(Arc<Expression>, Arc<Expression>),
    DurationTrueDiv(Arc<Expression>, Arc<Expression>),
    DateTimeNaiveTimestamp(Arc<Expression>, Arc<Expression>),
    DateTimeUtcTimestamp(Arc<Expression>, Arc<Expression>),
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
    ToString(Arc<Expression>),
}

#[derive(Debug)]
pub enum PointerExpression {
    PointerFrom(Expressions),
    PointerWithInstanceFrom(Expressions, Arc<Expression>),
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
    FromFloatTimestamp(Arc<Expression>, Arc<Expression>),
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
    TrueDivByInt(Arc<Expression>, Arc<Expression>),
    MulByFloat(Arc<Expression>, Arc<Expression>),
    DivByFloat(Arc<Expression>, Arc<Expression>),
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
        _ => Err(DynError::from(DataError::ValueError(format!(
            "Can't get element at index {index} out of {value:?}"
        )))),
    }
}

fn get_json_item(
    expr: &Arc<Expression>,
    index: &Arc<Expression>,
    values: &[Value],
) -> DynResult<Option<Value>> {
    let index = index.eval(values)?;
    let value = expr.eval(values)?;
    let json = value.as_json()?;
    let json = match index {
        Value::Int(index) => match usize::try_from(index) {
            Ok(index) => json.get(index),
            Err(_) => None,
        },
        Value::String(index) => json.get(index.as_str()),
        _ => {
            return Err(DynError::from(DataError::ValueError(format!(
                "json index must be string or integer, got {index}"
            ))))
        }
    };
    Ok(json.map(|json| Value::from(json.clone())))
}

fn mat_mul_wrapper<T>(lhs: &ArrayD<T>, rhs: &ArrayD<T>) -> DynResult<Value>
where
    T: LinalgScalar,
    Value: From<ArrayD<T>>,
{
    if let Some(result) = mat_mul(&lhs.view(), &rhs.view()) {
        Ok(result.into())
    } else {
        let msg = format!(
            "can't multiply arrays of shapes {:?} and {:?}",
            lhs.shape(),
            rhs.shape()
        );
        Err(DynError::from(DataError::ValueError(msg)))
    }
}

fn are_tuples_equal(lhs: &Arc<[Value]>, rhs: &Arc<[Value]>) -> DynResult<bool> {
    let mut result = lhs.len() == rhs.len();
    for (val_l, val_r) in lhs.iter().zip(rhs.iter()) {
        let result_single = match (val_l, val_r) {
            (Value::Tuple(val_l), Value::Tuple(val_r)) => Ok(are_tuples_equal(val_l, val_r)?),
            #[allow(clippy::cast_precision_loss)]
            (Value::Float(val_l), Value::Int(val_r)) => Ok(val_l.eq(&(*val_r as f64))),
            #[allow(clippy::cast_precision_loss)]
            (Value::Int(val_l), Value::Float(val_r)) => Ok(OrderedFloat(*val_l as f64).eq(val_r)),
            (val, Value::None) | (Value::None, val) => Ok(val == &Value::None),
            (val_l, val_r) => {
                let type_l = val_l.simple_type();
                let type_r = val_r.simple_type();
                if type_l == type_r {
                    Ok(val_l.eq(val_r))
                } else {
                    let msg = format!(
                        "equality not supported between instances of '{type_l:?}' and '{type_r:?}'",
                    );
                    Err(DynError::from(DataError::ValueError(msg)))
                }
            }
        }?;
        result &= result_single;
    }
    Ok(result)
}

fn compare_tuples(lhs: &Arc<[Value]>, rhs: &Arc<[Value]>) -> DynResult<Ordering> {
    let mut result = Ordering::Equal;
    for (val_l, val_r) in lhs.iter().zip(rhs.iter()) {
        let cmp = match (val_l, val_r) {
            (Value::Tuple(val_l), Value::Tuple(val_r)) => Ok(compare_tuples(val_l, val_r)?),
            #[allow(clippy::cast_precision_loss)]
            (Value::Float(val_l), Value::Int(val_r)) => Ok(val_l.cmp(&(*val_r as f64).into())),
            #[allow(clippy::cast_precision_loss)]
            (Value::Int(val_l), Value::Float(val_r)) => Ok(OrderedFloat(*val_l as f64).cmp(val_r)),
            (val_l, val_r) => {
                let type_l = val_l.simple_type();
                let type_r = val_r.simple_type();
                let is_incomparable_type = [
                    SimpleType::Json,
                    SimpleType::IntArray,
                    SimpleType::FloatArray,
                ]
                .contains(&type_l);
                if type_l != type_r || is_incomparable_type {
                    let msg = format!(
                        "comparison not supported between instances of '{type_l:?}' and '{type_r:?}'",
                    );
                    Err(DynError::from(DataError::ValueError(msg)))
                } else {
                    Ok(val_l.cmp(val_r))
                }
            }
        }?;
        if result == Ordering::Equal {
            result = cmp;
        }
    }
    if result == Ordering::Equal {
        Ok(lhs.len().cmp(&rhs.len()))
    } else {
        Ok(result)
    }
}

impl AnyExpression {
    #[allow(clippy::too_many_lines)]
    pub fn eval(&self, values: &[Value]) -> DynResult<Value> {
        let res = match self {
            Self::Argument(i) => values
                .get(*i)
                .ok_or(DataError::IndexOutOfBounds)?
                .clone()
                .into_result()?,
            Self::Const(v) => v.clone(),
            Self::Apply(f, args) => f(&args.eval(values)?)?,
            Self::OptionalApply(f, args) => {
                let args = args.eval(values)?;
                if args.iter().any(|a| matches!(a, Value::None)) {
                    Value::None
                } else {
                    f(&args)?
                }
            }
            Self::IfElse(if_, then, else_) => {
                if if_.eval_as_bool(values)? {
                    then.eval(values)?
                } else {
                    else_.eval(values)?
                }
            }
            Self::OptionalPointerFrom(args) => {
                let args = args.eval(values)?;
                if args.iter().any(|a| matches!(a, Value::None)) {
                    Value::None
                } else {
                    Value::from(Key::for_values(&args))
                }
            }
            Self::OptionalPointerWithInstanceFrom(args, instance) => {
                let mut args = args.eval(values)?.to_vec();
                args.push(instance.eval(values)?);
                if args.iter().any(|a| matches!(a, Value::None)) {
                    Value::None
                } else {
                    Value::from(ShardPolicy::LastKeyColumn.generate_key(&args))
                }
            }
            Self::MakeTuple(args) => {
                let args = args.eval(values)?;
                let args_arc: Arc<[Value]> = (*args).into();
                Value::Tuple(args_arc)
            }
            Self::TupleGetItemChecked(tuple, index, default) => {
                if let Some(entry) = get_array_element(tuple, index, values)? {
                    entry
                } else {
                    default.eval(values)?
                }
            }
            Self::TupleGetItemUnchecked(tuple, index) => {
                if let Some(entry) = get_array_element(tuple, index, values)? {
                    Ok(entry)
                } else {
                    Err(DynError::from(DataError::IndexOutOfBounds))
                }?
            }
            Self::JsonGetItem(tuple, index, default) => {
                if let Some(entry) = get_json_item(tuple, index, values)? {
                    entry
                } else {
                    default.eval(values)?
                }
            }
            Self::ParseStringToInt(e, optional) => {
                let val = e.eval_as_string(values)?;
                let val_str = val.trim();
                let parse_result = val_str.parse().map(Value::Int);
                if *optional {
                    parse_result.unwrap_or(Value::None)
                } else {
                    parse_result.map_err(|e| {
                        DynError::from(DataError::ParseError(format!(
                            "cannot parse {val:?} to int: {e}"
                        )))
                    })?
                }
            }
            Self::ParseStringToFloat(e, optional) => {
                let val = e.eval_as_string(values)?;
                let val_str = val.trim();
                let parse_result = val_str.parse().map(Value::Float);
                if *optional {
                    parse_result.unwrap_or(Value::None)
                } else {
                    parse_result.map_err(|e| {
                        DynError::from(DataError::ParseError(format!(
                            "cannot parse {val:?} to float: {e}"
                        )))
                    })?
                }
            }
            Self::ParseStringToBool(e, true_list, false_list, optional) => {
                let val = e.eval_as_string(values)?;
                let val_str = val.trim().to_lowercase();
                if true_list.contains(&val_str) {
                    Ok(Value::Bool(true))
                } else if false_list.contains(&val_str) {
                    Ok(Value::Bool(false))
                } else if *optional {
                    Ok(Value::None)
                } else {
                    Err(DynError::from(DataError::ParseError(format!(
                        "cannot parse {val:?} to bool"
                    ))))
                }?
            }
            Self::CastToOptionalIntFromOptionalFloat(expr) => match expr.eval(values)? {
                #[allow(clippy::cast_possible_truncation)]
                Value::Float(f) => Ok((*f as i64).into()),
                Value::None => Ok(Value::None),
                val => Err(DynError::from(DataError::ValueError(format!(
                    "Cannot cast to int from {val:?}"
                )))),
            }?,
            Self::CastToOptionalFloatFromOptionalInt(expr) => match expr.eval(values)? {
                #[allow(clippy::cast_precision_loss)]
                Value::Int(i) => Ok((i as f64).into()),
                Value::None => Ok(Value::None),
                val => Err(DynError::from(DataError::ValueError(format!(
                    "Cannot cast to float from {val:?}"
                )))),
            }?,
            Self::JsonToOptional(expr, type_) => match expr.eval(values)? {
                Value::Json(json) => {
                    if json.is_null() {
                        Ok(Value::None)
                    } else {
                        let val = match type_ {
                            Type::Int => json.as_i64().map(Value::from),
                            Type::Float => json.as_f64().map(Value::from),
                            Type::Bool => json.as_bool().map(Value::from),
                            Type::String => json.as_str().map(Value::from),
                            _ => {
                                return Err(DynError::from(DataError::ValueError(format!(
                                    "Cannot convert json {json} to {type_:?}"
                                ))))
                            }
                        };
                        val.ok_or_else(|| {
                            DynError::from(DataError::ValueError(format!(
                                "Cannot convert json {json} to {type_:?}"
                            )))
                        })
                    }
                }
                Value::None => Ok(Value::None),
                val => Err(DynError::from(DataError::ValueError(format!(
                    "Expected Json or None, found {val:?}"
                )))),
            }?,
            Self::MatMul(lhs, rhs) => {
                let lhs_val = lhs.eval(values)?;
                let rhs_val = rhs.eval(values)?;
                match (lhs_val, rhs_val) {
                    (Value::FloatArray(lhs), Value::FloatArray(rhs)) => mat_mul_wrapper(&lhs, &rhs),
                    (Value::IntArray(lhs), Value::IntArray(rhs)) => mat_mul_wrapper(&lhs, &rhs),
                    (lhs_val, rhs_val) => {
                        let lhs_type = lhs_val.simple_type();
                        let rhs_type = rhs_val.simple_type();
                        Err(DynError::from(DataError::ValueError(format!(
                            "can't perform matrix multiplication on {lhs_type:?} and {rhs_type:?}",
                        ))))
                    }
                }?
            }
            Self::Unwrap(e) => {
                let val = e.eval(values)?;
                if val == Value::None {
                    Err(DynError::from(DataError::ValueError(String::from(
                        "cannot unwrap if there is None value",
                    ))))
                } else {
                    Ok(val)
                }?
            }
            Self::FillError(e, replacement) => {
                e.eval(values).or_else(|_| replacement.eval(values))?
            }
        };
        debug_assert!(!matches!(res, Value::Error));
        Ok(res)
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
            Self::PtrLe(lhs, rhs) => {
                Ok(lhs.eval_as_pointer(values)? <= rhs.eval_as_pointer(values)?)
            }
            Self::PtrLt(lhs, rhs) => {
                Ok(lhs.eval_as_pointer(values)? < rhs.eval_as_pointer(values)?)
            }
            Self::PtrGe(lhs, rhs) => {
                Ok(lhs.eval_as_pointer(values)? >= rhs.eval_as_pointer(values)?)
            }
            Self::PtrGt(lhs, rhs) => {
                Ok(lhs.eval_as_pointer(values)? > rhs.eval_as_pointer(values)?)
            }
            Self::BoolEq(lhs, rhs) => Ok(lhs.eval_as_bool(values)? == rhs.eval_as_bool(values)?),
            Self::BoolNe(lhs, rhs) => Ok(lhs.eval_as_bool(values)? != rhs.eval_as_bool(values)?),
            Self::BoolLe(lhs, rhs) => Ok(lhs.eval_as_bool(values)? <= rhs.eval_as_bool(values)?),
            Self::BoolLt(lhs, rhs) => Ok(!(lhs.eval_as_bool(values)?) & rhs.eval_as_bool(values)?),
            Self::BoolGe(lhs, rhs) => Ok(lhs.eval_as_bool(values)? >= rhs.eval_as_bool(values)?),
            Self::BoolGt(lhs, rhs) => Ok(lhs.eval_as_bool(values)? & !(rhs.eval_as_bool(values)?)),
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
            Self::TupleEq(lhs, rhs) => Ok(are_tuples_equal(
                &lhs.eval_as_tuple(values)?,
                &rhs.eval_as_tuple(values)?,
            )?),
            Self::TupleNe(lhs, rhs) => Ok(!are_tuples_equal(
                &lhs.eval_as_tuple(values)?,
                &rhs.eval_as_tuple(values)?,
            )?),
            Self::TupleLe(lhs, rhs) => Ok(compare_tuples(
                &lhs.eval_as_tuple(values)?,
                &rhs.eval_as_tuple(values)?,
            )?
            .is_le()),
            Self::TupleLt(lhs, rhs) => Ok(compare_tuples(
                &lhs.eval_as_tuple(values)?,
                &rhs.eval_as_tuple(values)?,
            )?
            .is_lt()),
            Self::TupleGe(lhs, rhs) => Ok(compare_tuples(
                &lhs.eval_as_tuple(values)?,
                &rhs.eval_as_tuple(values)?,
            )?
            .is_ge()),
            Self::TupleGt(lhs, rhs) => Ok(compare_tuples(
                &lhs.eval_as_tuple(values)?,
                &rhs.eval_as_tuple(values)?,
            )?
            .is_gt()),
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
            Self::Abs(e) => Ok(e.eval_as_int(values)?.abs()),
            Self::Add(lhs, rhs) => Ok(lhs.eval_as_int(values)? + rhs.eval_as_int(values)?),
            Self::Sub(lhs, rhs) => Ok(lhs.eval_as_int(values)? - rhs.eval_as_int(values)?),
            Self::Mul(lhs, rhs) => Ok(lhs.eval_as_int(values)? * rhs.eval_as_int(values)?),
            Self::FloorDiv(lhs, rhs) => {
                let rhs_val = rhs.eval_as_int(values)?;
                if rhs_val == 0 {
                    Err(DynError::from(DataError::DivisionByZero))
                } else {
                    Ok(Integer::div_floor(&lhs.eval_as_int(values)?, &rhs_val))
                }
            }
            Self::Mod(lhs, rhs) => {
                let rhs_val = rhs.eval_as_int(values)?;
                if rhs_val == 0 {
                    Err(DynError::from(DataError::DivisionByZero))
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
            Self::DateTimeNaiveTimestampNs(e) => Ok(e.eval_as_date_time_naive(values)?.timestamp()),
            Self::DateTimeNaiveWeekday(e) => Ok(e.eval_as_date_time_naive(values)?.weekday()),
            Self::DateTimeUtcNanosecond(e) => Ok(e.eval_as_date_time_utc(values)?.nanosecond()),
            Self::DateTimeUtcMicrosecond(e) => Ok(e.eval_as_date_time_utc(values)?.microsecond()),
            Self::DateTimeUtcMillisecond(e) => Ok(e.eval_as_date_time_utc(values)?.millisecond()),
            Self::DateTimeUtcSecond(e) => Ok(e.eval_as_date_time_utc(values)?.second()),
            Self::DateTimeUtcMinute(e) => Ok(e.eval_as_date_time_utc(values)?.minute()),
            Self::DateTimeUtcHour(e) => Ok(e.eval_as_date_time_utc(values)?.hour()),
            Self::DateTimeUtcDay(e) => Ok(e.eval_as_date_time_utc(values)?.day()),
            Self::DateTimeUtcMonth(e) => Ok(e.eval_as_date_time_utc(values)?.month()),
            Self::DateTimeUtcYear(e) => Ok(e.eval_as_date_time_utc(values)?.year()),
            Self::DateTimeUtcTimestampNs(e) => Ok(e.eval_as_date_time_utc(values)?.timestamp()),
            Self::DateTimeUtcWeekday(e) => Ok(e.eval_as_date_time_utc(values)?.weekday()),
            Self::DurationFloorDiv(lhs, rhs) => {
                Ok((lhs.eval_as_duration(values)? / rhs.eval_as_duration(values)?)?)
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
                    DynError::from(DataError::ParseError(format!(
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
            Self::Abs(e) => Ok(e.eval_as_float(values)?.abs()),
            Self::Add(lhs, rhs) => Ok(lhs.eval_as_float(values)? + rhs.eval_as_float(values)?),
            Self::Sub(lhs, rhs) => Ok(lhs.eval_as_float(values)? - rhs.eval_as_float(values)?),
            Self::Mul(lhs, rhs) => Ok(lhs.eval_as_float(values)? * rhs.eval_as_float(values)?),
            Self::FloorDiv(lhs, rhs) => {
                let rhs_val = rhs.eval_as_float(values)?;
                if rhs_val == 0.0f64 {
                    Err(DynError::from(DataError::DivisionByZero))
                } else {
                    Ok((lhs.eval_as_float(values)? / rhs_val).floor())
                }
            }
            Self::TrueDiv(lhs, rhs) => {
                let rhs_val = rhs.eval_as_float(values)?;
                if rhs_val == 0.0f64 {
                    Err(DynError::from(DataError::DivisionByZero))
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
                    return Err(DynError::from(DataError::DivisionByZero));
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
                    Err(DynError::from(DataError::DivisionByZero))
                } else {
                    Ok(lhs.eval_as_int(values)? as f64 / rhs_val as f64)
                }
            }
            Self::DateTimeNaiveTimestamp(e, unit) => Ok(e
                .eval_as_date_time_naive(values)?
                .timestamp_in_unit(&unit.eval_as_string(values)?)?),
            Self::DateTimeUtcTimestamp(e, unit) => Ok(e
                .eval_as_date_time_utc(values)?
                .timestamp_in_unit(&unit.eval_as_string(values)?)?),
            Self::DurationTrueDiv(lhs, rhs) => Ok(lhs
                .eval_as_duration(values)?
                .true_div(rhs.eval_as_duration(values)?)?),
            Self::CastFromBool(e) => Ok(if e.eval_as_bool(values)? { 1.0 } else { 0.0 }),
            #[allow(clippy::cast_precision_loss)]
            Self::CastFromInt(e) => Ok(e.eval_as_int(values)? as f64),
            Self::CastFromString(e) => {
                let val = e.eval(values)?;
                let val_str = val.as_string()?.trim();
                val_str.parse().map_err(|_| {
                    DynError::from(DataError::ParseError(format!(
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
            Self::PointerWithInstanceFrom(args, instance) => {
                let mut args = args.eval(values)?.to_vec();
                args.push(instance.eval(values)?);
                Ok(ShardPolicy::LastKeyColumn.generate_key(&args))
            }
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
            Self::ToString(e) => {
                let val = e.eval(values)?;
                Ok(match val {
                    Value::String(s) => s,
                    _ => val.to_string().into(),
                })
            }
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
            Self::Strptime(e, fmt) => Ok(DateTimeNaive::strptime(
                &e.eval_as_string(values)?,
                &fmt.eval_as_string(values)?,
            )?),
            Self::FromUtc(expr, timezone) => Ok(expr
                .eval_as_date_time_utc(values)?
                .to_naive_in_timezone(&timezone.eval_as_string(values)?)?),
            Self::Round(expr, duration) => Ok(expr
                .eval_as_date_time_naive(values)?
                .round(duration.eval_as_duration(values)?)),
            Self::Floor(expr, duration) => Ok(expr
                .eval_as_date_time_naive(values)?
                .truncate(duration.eval_as_duration(values)?)),
            Self::FromTimestamp(expr, unit) => Ok(DateTimeNaive::from_timestamp(
                expr.eval_as_int(values)?,
                &unit.eval_as_string(values)?,
            )?),
            Self::FromFloatTimestamp(expr, unit) => Ok(DateTimeNaive::from_timestamp_f64(
                expr.eval_as_float(values)?,
                &unit.eval_as_string(values)?,
            )?),
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
            Self::Strptime(e, fmt) => Ok(DateTimeUtc::strptime(
                &e.eval_as_string(values)?,
                &fmt.eval_as_string(values)?,
            )?),
            Self::FromNaive(expr, from_timezone) => Ok(expr
                .eval_as_date_time_naive(values)?
                .to_utc_from_timezone(&from_timezone.eval_as_string(values)?)?),
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
                Ok((lhs.eval_as_duration(values)? / rhs.eval_as_int(values)?)?)
            }
            Self::TrueDivByInt(lhs, rhs) => Ok(lhs
                .eval_as_duration(values)?
                .true_div_by_i64(rhs.eval_as_int(values)?)?),
            Self::MulByFloat(lhs, rhs) => {
                Ok(lhs.eval_as_duration(values)? * rhs.eval_as_float(values)?)
            }
            Self::DivByFloat(lhs, rhs) => {
                Ok((lhs.eval_as_duration(values)? / rhs.eval_as_float(values)?)?)
            }
            Self::Mod(lhs, rhs) => {
                Ok((lhs.eval_as_duration(values)? % rhs.eval_as_duration(values)?)?)
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
        DynError::from(DataError::ColumnTypeMismatch { expected, actual })
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

    pub fn eval_as_tuple(&self, values: &[Value]) -> DynResult<Arc<[Value]>> {
        match self {
            Self::Any(expr) => expr.eval(values)?.as_tuple().cloned(),
            _ => Err(self.type_error("Tuple")),
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
