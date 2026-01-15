// Copyright © 2026 Pathway

#![allow(clippy::module_name_repetitions)]

use arcstr::ArcStr;
use log::warn;
use ndarray::{ArrayD, Axis, LinalgScalar};
use num_integer::Integer;
use ordered_float::OrderedFloat;
use std::cmp::Ordering;
use std::ops::{Deref, Range};
use std::sync::Arc;
use std::vec::IntoIter;

use derivative::Derivative;
use itertools::Itertools;
use smallvec::SmallVec;

use super::error::{DataError, DynError, DynResult};
use super::time::{DateTime, DateTimeNaive, DateTimeUtc, Duration};
use super::value::Kind;
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

impl Deref for MaybeOwnedValues<'_> {
    type Target = [Value];

    fn deref(&self) -> &[Value] {
        match self {
            Self::Owned(values) => values,
            Self::Borrowed(values) => values,
        }
    }
}

impl Expressions {
    pub fn eval<'v>(&self, values: &[&'v [Value]]) -> Vec<DynResult<MaybeOwnedValues<'v>>> {
        match self {
            Self::Explicit(exprs) => {
                let mut iters: Vec<_> = exprs.iter().map(|e| e.eval(values).into_iter()).collect();
                // transpose from expression results in batch form to a batch of expression results
                (0..values.len())
                    .map(|_| {
                        Ok(MaybeOwnedValues::Owned(
                            iters
                                .iter_mut()
                                .map(|expr_j_res| expr_j_res.next().unwrap())
                                .try_collect()?,
                        ))
                    })
                    .collect()
            }
            Self::Arguments(range) => values
                .iter()
                .map(|values_row| {
                    let values_row = &values_row[range.clone()];
                    if values_row.contains(&Value::Error) {
                        Err(DataError::ErrorInValue.into())
                    } else {
                        Ok(MaybeOwnedValues::Borrowed(values_row))
                    }
                })
                .collect(),
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
        #[derivative(Debug = "ignore")]
        Box<dyn Fn(&[&[Value]]) -> Vec<DynResult<Value>> + Send + Sync>,
        Expressions,
    ),
    IfElse(Arc<Expression>, Arc<Expression>, Arc<Expression>),
    OptionalPointerFrom(Expressions),
    OptionalPointerWithInstanceFrom(Expressions, Arc<Expression>),
    MakeTuple(Expressions),
    TupleGetItemChecked(Arc<Expression>, Arc<Expression>, Arc<Expression>),
    TupleGetItemUnchecked(Arc<Expression>, Arc<Expression>),
    JsonGetItem(Arc<Expression>, Arc<Expression>, Arc<Expression>),
    JsonToValue(Arc<Expression>, Arc<Expression>, Type, bool),
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
    FromTimeUnit(Arc<Expression>, Arc<Expression>),
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

fn get_array_element(value: Value, index: i64) -> DynResult<Option<Value>> {
    match value {
        Value::IntArray(array) => get_ndarray_element(&array, index),
        Value::FloatArray(array) => get_ndarray_element(&array, index),
        Value::Tuple(tuple) => get_tuple_element(&tuple, index),
        _ => Err(DynError::from(DataError::ValueError(format!(
            "Can't get element at index {index} out of {value:?}"
        )))),
    }
}

fn get_json_item(value: &Value, index: Value) -> DynResult<Option<Value>> {
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
                let type_l = val_l.kind();
                let type_r = val_r.kind();
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
                let type_l = val_l.kind();
                let type_r = val_r.kind();
                let is_incomparable_type =
                    [Kind::Json, Kind::IntArray, Kind::FloatArray].contains(&type_l);
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

fn unwrap(val: Value) -> DynResult<Value> {
    match val {
        Value::None => Err(DynError::from(DataError::ValueError(
            "cannot unwrap if there is None value".into(),
        ))),
        _ => Ok(val),
    }
}

fn nullary_expr<F, T>(values: &[&[Value]], f: &F) -> Vec<DynResult<T>>
where
    F: Fn() -> T,
{
    values.iter().map(|_v| Ok(f())).collect()
}

fn unary_expr<F, T, U>(expr: &Expression, values: &[&[Value]], f: F) -> Vec<DynResult<U>>
where
    Expression: EvalAs<T>,
    F: Fn(T) -> U + 'static,
{
    expr.eval_as(values)
        .into_iter()
        .map(|v| Ok(f(v?)))
        .collect()
}

fn unary_expr_err<F, T, U>(expr: &Expression, values: &[&[Value]], f: &F) -> Vec<DynResult<U>>
where
    Expression: EvalAs<T>,
    F: Fn(T) -> DynResult<U>,
{
    expr.eval_as(values).into_iter().map(|v| f(v?)).collect()
}

fn binary_expr<F, T, U, V>(
    lhs: &Expression,
    rhs: &Expression,
    values: &[&[Value]],
    f: F,
) -> Vec<DynResult<V>>
where
    Expression: EvalAs<T>,
    Expression: EvalAs<U>,
    F: Fn(T, U) -> V + 'static,
{
    let lhs = lhs.eval_as(values);
    let rhs = rhs.eval_as(values);
    lhs.into_iter()
        .zip(rhs)
        .map(|(l, r)| Ok(f(l?, r?)))
        .collect()
}

fn binary_expr_err<F, T, U, V>(
    lhs: &Expression,
    rhs: &Expression,
    values: &[&[Value]],
    f: F,
) -> Vec<DynResult<V>>
where
    Expression: EvalAs<T>,
    Expression: EvalAs<U>,
    F: Fn(T, U) -> DynResult<V>,
{
    let lhs = lhs.eval_as(values);
    let rhs = rhs.eval_as(values);
    lhs.into_iter().zip(rhs).map(|(l, r)| f(l?, r?)).collect()
}

fn ternary_expr_err<F, T, U, V, W>(
    expr_1: &Expression,
    expr_2: &Expression,
    expr_3: &Expression,
    values: &[&[Value]],
    f: F,
) -> Vec<DynResult<W>>
where
    Expression: EvalAs<T>,
    Expression: EvalAs<U>,
    Expression: EvalAs<V>,
    F: Fn(T, U, V) -> DynResult<W> + 'static,
{
    let expr_1_values = expr_1.eval_as(values);
    let expr_2_values = expr_2.eval_as(values);
    let expr_3_values = expr_3.eval_as(values);
    expr_1_values
        .into_iter()
        .zip(expr_2_values)
        .zip(expr_3_values)
        .map(|((v1, v2), v3)| f(v1?, v2?, v3?))
        .collect()
}

fn multi_expr<F, T>(exprs: &Expressions, values: &[&[Value]], f: F) -> Vec<DynResult<T>>
where
    F: Fn(MaybeOwnedValues) -> T + 'static,
{
    exprs.eval(values).into_iter().map(|v| Ok(f(v?))).collect()
}

fn multi_expression_with_additional_expression<F, T, U>(
    exprs: &Expressions,
    expr: &Arc<Expression>,
    values: &[&[Value]],
    f: F,
) -> Vec<DynResult<U>>
where
    Expression: EvalAs<T>,
    F: Fn(MaybeOwnedValues, T) -> U + 'static,
{
    exprs
        .eval(values)
        .into_iter()
        .zip(expr.eval_as(values))
        .map(|(v, a)| Ok(f(v?, a?)))
        .collect()
}

impl AnyExpression {
    #[allow(clippy::too_many_lines)]
    pub fn eval(&self, values: &[&[Value]]) -> Vec<DynResult<Value>> {
        let res = match self {
            Self::Argument(i) => values
                .iter()
                .map(|values_row| {
                    values_row
                        .get(*i)
                        .ok_or(DataError::IndexOutOfBounds)?
                        .clone()
                        .into_result()
                })
                .collect(),
            Self::Const(v) => nullary_expr(values, &|| v.clone()),
            Self::Apply(f, args) => {
                let args = args.eval(values);
                let mask: Vec<_> = args.iter().map(Result::is_ok).collect();
                let mut args_ok = Vec::new();
                let mut args_err = Vec::new();
                for arg_i in args {
                    match arg_i {
                        Ok(arg_i) => args_ok.push(arg_i),
                        Err(arg_i) => args_err.push(arg_i),
                    }
                }
                let args_ok: Vec<_> = args_ok.iter().map(Deref::deref).collect();
                let mut result_ok = f(&args_ok).into_iter();
                let mut result_err = args_err.into_iter();
                mask.into_iter()
                    .map(|mask_i| {
                        if mask_i {
                            result_ok.next().unwrap()
                        } else {
                            Err(result_err.next().unwrap())
                        }
                    })
                    .collect()
            }
            Self::IfElse(if_, then, else_) => {
                let mask: Vec<DynResult<bool>> = if_.eval_as(values);
                let mut then_values = Vec::new();
                let mut else_values = Vec::new();
                for (i, mask_i) in mask.iter().enumerate() {
                    match mask_i {
                        Ok(true) => then_values.push(values[i]),
                        Ok(false) => else_values.push(values[i]),
                        _ => {}
                    }
                }
                let mut then_res = then.eval(&then_values).into_iter();
                let mut else_res = else_.eval(&else_values).into_iter();
                let mut res = Vec::with_capacity(values.len());
                for mask_i in mask {
                    match mask_i {
                        Ok(true) => {
                            res.push(then_res.next().unwrap());
                        }
                        Ok(false) => {
                            res.push(else_res.next().unwrap());
                        }
                        Err(e) => {
                            res.push(Err(e));
                        }
                    }
                }
                res
            }
            Self::OptionalPointerFrom(args) => multi_expr(args, values, |args| {
                if args.iter().any(|a| matches!(a, Value::None)) {
                    Value::None
                } else {
                    Value::from(Key::for_values(&args))
                }
            }),
            Self::OptionalPointerWithInstanceFrom(args, instance) => {
                multi_expression_with_additional_expression(
                    args,
                    instance,
                    values,
                    |args, instance| {
                        let mut args = args.to_vec();
                        args.push(instance);
                        if args.iter().any(|a| matches!(a, Value::None)) {
                            Value::None
                        } else {
                            Value::from(ShardPolicy::LastKeyColumn.generate_key(&args))
                        }
                    },
                )
            }
            Self::MakeTuple(args) => multi_expr(args, values, |args| Value::Tuple((*args).into())),
            Self::TupleGetItemChecked(tuple, index, default) => ternary_expr_err(
                tuple,
                index,
                default,
                values,
                |tuple: Value, index: i64, default: Value| {
                    Ok(get_array_element(tuple, index)?.unwrap_or(default))
                },
            ),
            Self::TupleGetItemUnchecked(tuple, index) => {
                binary_expr_err(tuple, index, values, |tuple, index| {
                    get_array_element(tuple, index)?
                        .ok_or_else(|| DynError::from(DataError::IndexOutOfBounds))
                })
            }
            Self::JsonGetItem(tuple, index, default) => {
                ternary_expr_err(tuple, index, default, values, |tuple, index, default| {
                    Ok(get_json_item(&tuple, index)?.unwrap_or(default))
                })
            }
            Self::ParseStringToInt(e, optional) => unary_expr_err(e, values, &|v: ArcStr| {
                let parse_result = v.trim().parse().map(Value::Int);
                if *optional {
                    Ok(parse_result.unwrap_or(Value::None))
                } else {
                    parse_result.map_err(|e| {
                        DynError::from(DataError::ParseError(format!(
                            "cannot parse {v:?} to int: {e}"
                        )))
                    })
                }
            }),
            Self::ParseStringToFloat(e, optional) => unary_expr_err(e, values, &|v: ArcStr| {
                let parse_result = v.trim().parse().map(Value::Float);
                if *optional {
                    Ok(parse_result.unwrap_or(Value::None))
                } else {
                    parse_result.map_err(|e| {
                        DynError::from(DataError::ParseError(format!(
                            "cannot parse {v:?} to float: {e}"
                        )))
                    })
                }
            }),
            Self::ParseStringToBool(e, true_list, false_list, optional) => {
                unary_expr_err(e, values, &|v: ArcStr| {
                    let val_str = v.trim().to_lowercase();
                    if true_list.contains(&val_str) {
                        Ok(Value::Bool(true))
                    } else if false_list.contains(&val_str) {
                        Ok(Value::Bool(false))
                    } else if *optional {
                        Ok(Value::None)
                    } else {
                        Err(DynError::from(DataError::ParseError(format!(
                            "cannot parse {v:?} to bool"
                        ))))
                    }
                })
            }
            Self::CastToOptionalIntFromOptionalFloat(expr) => {
                unary_expr_err(expr, values, &|v| match v {
                    #[allow(clippy::cast_possible_truncation)]
                    Value::Float(f) => Ok((*f as i64).into()),
                    Value::None => Ok(Value::None),
                    val => Err(DynError::from(DataError::ValueError(format!(
                        "Cannot cast to int from {val:?}"
                    )))),
                })
            }
            Self::CastToOptionalFloatFromOptionalInt(expr) => {
                unary_expr_err(expr, values, &|v| match v {
                    #[allow(clippy::cast_precision_loss)]
                    Value::Int(i) => Ok((i as f64).into()),
                    Value::None => Ok(Value::None),
                    val => Err(DynError::from(DataError::ValueError(format!(
                        "Cannot cast to float from {val:?}"
                    )))),
                })
            }
            Self::JsonToValue(expr, default, type_, unwrap_) => {
                binary_expr_err(expr, default, values, |v: Value, default: Value| {
                    let result = match v {
                        Value::Json(json) => {
                            if json.is_null() {
                                Ok(default)
                            } else {
                                match type_ {
                                    Type::Int => json.as_i64().map(Value::from),
                                    Type::Float => json.as_f64().map(Value::from),
                                    Type::Bool => json.as_bool().map(Value::from),
                                    Type::String => json.as_str().map(Value::from),
                                    _ => {
                                        return Err(DynError::from(DataError::ValueError(
                                            format!("Cannot convert json {json} to {type_:?}"),
                                        )));
                                    }
                                }
                                .ok_or_else(|| {
                                    DynError::from(DataError::ValueError(format!(
                                        "Cannot convert json {json} to {type_:?}"
                                    )))
                                })
                            }
                        }
                        Value::None => Ok(default),
                        val => {
                            return Err(DynError::from(DataError::ValueError(format!(
                                "Expected Json or None, found {val:?}"
                            ))));
                        }
                    };
                    if *unwrap_ {
                        unwrap(result?)
                    } else {
                        result
                    }
                })
            }
            Self::MatMul(lhs, rhs) => {
                binary_expr_err(lhs, rhs, values, |lhs, rhs| match (lhs, rhs) {
                    (Value::FloatArray(lhs), Value::FloatArray(rhs)) => mat_mul_wrapper(&lhs, &rhs),
                    (Value::IntArray(lhs), Value::IntArray(rhs)) => mat_mul_wrapper(&lhs, &rhs),
                    (lhs_val, rhs_val) => {
                        let lhs_type = lhs_val.kind();
                        let rhs_type = rhs_val.kind();
                        Err(DynError::from(DataError::ValueError(format!(
                            "can't perform matrix multiplication on {lhs_type:?} and {rhs_type:?}",
                        ))))
                    }
                })
            }
            Self::Unwrap(e) => unary_expr_err(e, values, &|v| unwrap(v)),
            Self::FillError(e, replacement) => {
                let result = e.eval(values);
                let to_reevaluate: Vec<_> = values
                    .iter()
                    .zip_eq(result.iter())
                    .filter_map(|(v, r)| if r.is_err() { Some(*v) } else { None })
                    .collect();
                let mut replacement_result = replacement.eval(&to_reevaluate).into_iter();
                result
                    .into_iter()
                    .map(|r| r.or_else(|_| replacement_result.next().unwrap()))
                    .collect()
            }
        };
        for entry in res.iter().flatten() {
            debug_assert!(!matches!(entry, Value::Error));
        }
        res
    }
}

fn logical_operator(
    lhs: &Expression,
    rhs: &Expression,
    values: &[&[Value]],
    rhs_if: bool,
    mut on_true: impl FnMut(&mut IntoIter<DynResult<bool>>) -> DynResult<bool>,
    mut on_false: impl FnMut(&mut IntoIter<DynResult<bool>>) -> DynResult<bool>,
) -> Vec<DynResult<bool>> {
    let lhs_res: Vec<DynResult<bool>> = lhs.eval_as(values);
    let to_evaluate: Vec<_> = values
        .iter()
        .zip_eq(lhs_res.iter())
        .filter_map(|(v, l)| {
            if l.is_ok() && *l.as_ref().unwrap() == rhs_if {
                Some(*v)
            } else {
                None
            }
        })
        .collect();
    let rhs_res: Vec<DynResult<bool>> = rhs.eval_as(&to_evaluate);
    let mut rhs_res_iter = rhs_res.into_iter();
    lhs_res
        .into_iter()
        .map(|l| match l {
            Ok(true) => on_true(&mut rhs_res_iter),
            Ok(false) => on_false(&mut rhs_res_iter),
            e => e,
        })
        .collect()
}

impl BoolExpression {
    #[allow(clippy::too_many_lines)]
    pub fn eval(&self, values: &[&[Value]]) -> Vec<DynResult<bool>> {
        match self {
            Self::Const(c) => nullary_expr(values, &|| *c),
            Self::IsNone(e) => unary_expr(e, values, |v| matches!(v, Value::None)),
            Self::Not(e) => unary_expr(e, values, |v: bool| !v),
            Self::And(lhs, rhs) => logical_operator(
                lhs,
                rhs,
                values,
                true,
                |r| r.next().unwrap(),
                |_r| Ok(false),
            ),
            Self::Or(lhs, rhs) => logical_operator(
                lhs,
                rhs,
                values,
                false,
                |_r| Ok(true),
                |r| r.next().unwrap(),
            ),
            Self::Xor(lhs, rhs) => binary_expr(lhs, rhs, values, |l: bool, r: bool| l ^ r),
            Self::IntEq(lhs, rhs) => binary_expr(lhs, rhs, values, |l: i64, r: i64| l == r),
            Self::IntNe(lhs, rhs) => binary_expr(lhs, rhs, values, |l: i64, r: i64| l != r),
            Self::IntLt(lhs, rhs) => binary_expr(lhs, rhs, values, |l: i64, r: i64| l < r),
            Self::IntLe(lhs, rhs) => binary_expr(lhs, rhs, values, |l: i64, r: i64| l <= r),
            Self::IntGt(lhs, rhs) => binary_expr(lhs, rhs, values, |l: i64, r: i64| l > r),
            Self::IntGe(lhs, rhs) => binary_expr(lhs, rhs, values, |l: i64, r: i64| l >= r),
            #[allow(clippy::float_cmp)]
            Self::FloatEq(lhs, rhs) => binary_expr(lhs, rhs, values, |l: f64, r: f64| l == r),
            #[allow(clippy::float_cmp)]
            Self::FloatNe(lhs, rhs) => binary_expr(lhs, rhs, values, |l: f64, r: f64| l != r),
            Self::FloatLt(lhs, rhs) => binary_expr(lhs, rhs, values, |l: f64, r: f64| l < r),
            Self::FloatLe(lhs, rhs) => binary_expr(lhs, rhs, values, |l: f64, r: f64| l <= r),
            Self::FloatGt(lhs, rhs) => binary_expr(lhs, rhs, values, |l: f64, r: f64| l > r),
            Self::FloatGe(lhs, rhs) => binary_expr(lhs, rhs, values, |l: f64, r: f64| l >= r),
            Self::StringEq(lhs, rhs) => {
                binary_expr(lhs, rhs, values, |l: ArcStr, r: ArcStr| l == r)
            }
            Self::StringNe(lhs, rhs) => {
                binary_expr(lhs, rhs, values, |l: ArcStr, r: ArcStr| l != r)
            }
            Self::StringLt(lhs, rhs) => binary_expr(lhs, rhs, values, |l: ArcStr, r: ArcStr| l < r),
            Self::StringLe(lhs, rhs) => {
                binary_expr(lhs, rhs, values, |l: ArcStr, r: ArcStr| l <= r)
            }
            Self::StringGt(lhs, rhs) => binary_expr(lhs, rhs, values, |l: ArcStr, r: ArcStr| l > r),
            Self::StringGe(lhs, rhs) => {
                binary_expr(lhs, rhs, values, |l: ArcStr, r: ArcStr| l >= r)
            }
            Self::PtrEq(lhs, rhs) => binary_expr(lhs, rhs, values, |l: Key, r: Key| l == r),
            Self::PtrNe(lhs, rhs) => binary_expr(lhs, rhs, values, |l: Key, r: Key| l != r),
            Self::PtrLe(lhs, rhs) => binary_expr(lhs, rhs, values, |l: Key, r: Key| l <= r),
            Self::PtrLt(lhs, rhs) => binary_expr(lhs, rhs, values, |l: Key, r: Key| l < r),
            Self::PtrGe(lhs, rhs) => binary_expr(lhs, rhs, values, |l: Key, r: Key| l >= r),
            Self::PtrGt(lhs, rhs) => binary_expr(lhs, rhs, values, |l: Key, r: Key| l > r),
            Self::BoolEq(lhs, rhs) => binary_expr(lhs, rhs, values, |l: bool, r: bool| l == r),
            Self::BoolNe(lhs, rhs) => binary_expr(lhs, rhs, values, |l: bool, r: bool| l != r),
            Self::BoolLe(lhs, rhs) => binary_expr(lhs, rhs, values, |l: bool, r: bool| l <= r),
            #[allow(clippy::bool_comparison)]
            Self::BoolLt(lhs, rhs) => binary_expr(lhs, rhs, values, |l: bool, r: bool| l < r),
            Self::BoolGe(lhs, rhs) => binary_expr(lhs, rhs, values, |l: bool, r: bool| l >= r),
            #[allow(clippy::bool_comparison)]
            Self::BoolGt(lhs, rhs) => binary_expr(lhs, rhs, values, |l: bool, r: bool| l > r),
            Self::DateTimeNaiveEq(lhs, rhs) => {
                binary_expr(lhs, rhs, values, |l: DateTimeNaive, r: DateTimeNaive| {
                    l == r
                })
            }
            Self::DateTimeNaiveNe(lhs, rhs) => {
                binary_expr(lhs, rhs, values, |l: DateTimeNaive, r: DateTimeNaive| {
                    l != r
                })
            }
            Self::DateTimeNaiveLt(lhs, rhs) => {
                binary_expr(lhs, rhs, values, |l: DateTimeNaive, r: DateTimeNaive| l < r)
            }
            Self::DateTimeNaiveLe(lhs, rhs) => {
                binary_expr(lhs, rhs, values, |l: DateTimeNaive, r: DateTimeNaive| {
                    l <= r
                })
            }
            Self::DateTimeNaiveGt(lhs, rhs) => {
                binary_expr(lhs, rhs, values, |l: DateTimeNaive, r: DateTimeNaive| l > r)
            }
            Self::DateTimeNaiveGe(lhs, rhs) => {
                binary_expr(lhs, rhs, values, |l: DateTimeNaive, r: DateTimeNaive| {
                    l >= r
                })
            }
            Self::DateTimeUtcEq(lhs, rhs) => {
                binary_expr(lhs, rhs, values, |l: DateTimeUtc, r: DateTimeUtc| l == r)
            }
            Self::DateTimeUtcNe(lhs, rhs) => {
                binary_expr(lhs, rhs, values, |l: DateTimeUtc, r: DateTimeUtc| l != r)
            }
            Self::DateTimeUtcLt(lhs, rhs) => {
                binary_expr(lhs, rhs, values, |l: DateTimeUtc, r: DateTimeUtc| l < r)
            }
            Self::DateTimeUtcLe(lhs, rhs) => {
                binary_expr(lhs, rhs, values, |l: DateTimeUtc, r: DateTimeUtc| l <= r)
            }
            Self::DateTimeUtcGt(lhs, rhs) => {
                binary_expr(lhs, rhs, values, |l: DateTimeUtc, r: DateTimeUtc| l > r)
            }
            Self::DateTimeUtcGe(lhs, rhs) => {
                binary_expr(lhs, rhs, values, |l: DateTimeUtc, r: DateTimeUtc| l >= r)
            }
            Self::DurationEq(lhs, rhs) => {
                binary_expr(lhs, rhs, values, |l: Duration, r: Duration| l == r)
            }
            Self::DurationNe(lhs, rhs) => {
                binary_expr(lhs, rhs, values, |l: Duration, r: Duration| l != r)
            }
            Self::DurationLt(lhs, rhs) => {
                binary_expr(lhs, rhs, values, |l: Duration, r: Duration| l < r)
            }
            Self::DurationLe(lhs, rhs) => {
                binary_expr(lhs, rhs, values, |l: Duration, r: Duration| l <= r)
            }
            Self::DurationGt(lhs, rhs) => {
                binary_expr(lhs, rhs, values, |l: Duration, r: Duration| l > r)
            }
            Self::DurationGe(lhs, rhs) => {
                binary_expr(lhs, rhs, values, |l: Duration, r: Duration| l >= r)
            }
            Self::Eq(lhs, rhs) => binary_expr(lhs, rhs, values, |l: Value, r: Value| l == r),
            Self::Ne(lhs, rhs) => binary_expr(lhs, rhs, values, |l: Value, r: Value| l != r),
            Self::TupleEq(lhs, rhs) => {
                binary_expr_err(lhs, rhs, values, |l: Arc<[Value]>, r: Arc<[Value]>| {
                    are_tuples_equal(&l, &r)
                })
            }
            Self::TupleNe(lhs, rhs) => {
                binary_expr_err(lhs, rhs, values, |l: Arc<[Value]>, r: Arc<[Value]>| {
                    Ok(!are_tuples_equal(&l, &r)?)
                })
            }
            Self::TupleLe(lhs, rhs) => {
                binary_expr_err(lhs, rhs, values, |l: Arc<[Value]>, r: Arc<[Value]>| {
                    Ok(compare_tuples(&l, &r)?.is_le())
                })
            }
            Self::TupleLt(lhs, rhs) => {
                binary_expr_err(lhs, rhs, values, |l: Arc<[Value]>, r: Arc<[Value]>| {
                    Ok(compare_tuples(&l, &r)?.is_lt())
                })
            }
            Self::TupleGe(lhs, rhs) => {
                binary_expr_err(lhs, rhs, values, |l: Arc<[Value]>, r: Arc<[Value]>| {
                    Ok(compare_tuples(&l, &r)?.is_ge())
                })
            }
            Self::TupleGt(lhs, rhs) => {
                binary_expr_err(lhs, rhs, values, |l: Arc<[Value]>, r: Arc<[Value]>| {
                    Ok(compare_tuples(&l, &r)?.is_gt())
                })
            }
            Self::CastFromInt(e) => unary_expr(e, values, |v: i64| v != 0),
            Self::CastFromFloat(e) => unary_expr(e, values, |v: f64| v != 0.0),
            Self::CastFromString(e) => unary_expr(e, values, |v: ArcStr| !v.is_empty()),
        }
    }
}

impl IntExpression {
    pub fn eval(&self, values: &[&[Value]]) -> Vec<DynResult<i64>> {
        match self {
            Self::Const(c) => nullary_expr(values, &|| *c),
            Self::Neg(e) => unary_expr(e, values, |v: i64| -v),
            Self::Abs(e) => unary_expr(e, values, |v: i64| v.abs()),
            Self::Add(lhs, rhs) => binary_expr(lhs, rhs, values, |l: i64, r: i64| l + r),
            Self::Sub(lhs, rhs) => binary_expr(lhs, rhs, values, |l: i64, r: i64| l - r),
            Self::Mul(lhs, rhs) => binary_expr(lhs, rhs, values, |l: i64, r: i64| l * r),
            Self::FloorDiv(lhs, rhs) => binary_expr_err(lhs, rhs, values, |l: i64, r: i64| {
                if r == 0 {
                    Err(DynError::from(DataError::DivisionByZero))
                } else {
                    Ok(Integer::div_floor(&l, &r))
                }
            }),
            Self::Mod(lhs, rhs) => binary_expr_err(lhs, rhs, values, |l: i64, r: i64| {
                if r == 0 {
                    Err(DynError::from(DataError::DivisionByZero))
                } else {
                    Ok(Integer::mod_floor(&l, &r))
                }
            }),
            #[allow(clippy::cast_possible_truncation)]
            #[allow(clippy::cast_sign_loss)]
            Self::Pow(lhs, rhs) => binary_expr(lhs, rhs, values, |l: i64, r: i64| l.pow(r as u32)),
            Self::Lshift(lhs, rhs) => binary_expr(lhs, rhs, values, |l: i64, r: i64| l << r),
            Self::Rshift(lhs, rhs) => binary_expr(lhs, rhs, values, |l: i64, r: i64| l >> r),
            Self::And(lhs, rhs) => binary_expr(lhs, rhs, values, |l: i64, r: i64| l & r),
            Self::Or(lhs, rhs) => binary_expr(lhs, rhs, values, |l: i64, r: i64| l | r),
            Self::Xor(lhs, rhs) => binary_expr(lhs, rhs, values, |l: i64, r: i64| l ^ r),
            Self::DateTimeNaiveNanosecond(e) => {
                unary_expr(e, values, |v: DateTimeNaive| v.nanosecond())
            }
            Self::DateTimeNaiveMicrosecond(e) => {
                unary_expr(e, values, |v: DateTimeNaive| v.microsecond())
            }
            Self::DateTimeNaiveMillisecond(e) => {
                unary_expr(e, values, |v: DateTimeNaive| v.millisecond())
            }
            Self::DateTimeNaiveSecond(e) => unary_expr(e, values, |v: DateTimeNaive| v.second()),
            Self::DateTimeNaiveMinute(e) => unary_expr(e, values, |v: DateTimeNaive| v.minute()),
            Self::DateTimeNaiveHour(e) => unary_expr(e, values, |v: DateTimeNaive| v.hour()),
            Self::DateTimeNaiveDay(e) => unary_expr(e, values, |v: DateTimeNaive| v.day()),
            Self::DateTimeNaiveMonth(e) => unary_expr(e, values, |v: DateTimeNaive| v.month()),
            Self::DateTimeNaiveYear(e) => unary_expr(e, values, |v: DateTimeNaive| v.year()),
            Self::DateTimeNaiveTimestampNs(e) => {
                unary_expr(e, values, |v: DateTimeNaive| v.timestamp())
            }
            Self::DateTimeNaiveWeekday(e) => unary_expr(e, values, |v: DateTimeNaive| v.weekday()),
            Self::DateTimeUtcNanosecond(e) => {
                unary_expr(e, values, |v: DateTimeUtc| v.nanosecond())
            }
            Self::DateTimeUtcMicrosecond(e) => {
                unary_expr(e, values, |v: DateTimeUtc| v.microsecond())
            }
            Self::DateTimeUtcMillisecond(e) => {
                unary_expr(e, values, |v: DateTimeUtc| v.millisecond())
            }
            Self::DateTimeUtcSecond(e) => unary_expr(e, values, |v: DateTimeUtc| v.second()),
            Self::DateTimeUtcMinute(e) => unary_expr(e, values, |v: DateTimeUtc| v.minute()),
            Self::DateTimeUtcHour(e) => unary_expr(e, values, |v: DateTimeUtc| v.hour()),
            Self::DateTimeUtcDay(e) => unary_expr(e, values, |v: DateTimeUtc| v.day()),
            Self::DateTimeUtcMonth(e) => unary_expr(e, values, |v: DateTimeUtc| v.month()),
            Self::DateTimeUtcYear(e) => unary_expr(e, values, |v: DateTimeUtc| v.year()),
            Self::DateTimeUtcTimestampNs(e) => {
                unary_expr(e, values, |v: DateTimeUtc| v.timestamp())
            }
            Self::DateTimeUtcWeekday(e) => unary_expr(e, values, |v: DateTimeUtc| v.weekday()),
            Self::DurationFloorDiv(lhs, rhs) => {
                binary_expr_err(lhs, rhs, values, |l: Duration, r: Duration| {
                    if r.is_zero() {
                        Err(DynError::from(DataError::DivisionByZero))
                    } else {
                        Ok(l / r)
                    }
                })
            }
            Self::DurationNanoseconds(e) => unary_expr(e, values, |v: Duration| v.nanoseconds()),
            Self::DurationMicroseconds(e) => unary_expr(e, values, |v: Duration| v.microseconds()),
            Self::DurationMilliseconds(e) => unary_expr(e, values, |v: Duration| v.milliseconds()),
            Self::DurationSeconds(e) => unary_expr(e, values, |v: Duration| v.seconds()),
            Self::DurationMinutes(e) => unary_expr(e, values, |v: Duration| v.minutes()),
            Self::DurationHours(e) => unary_expr(e, values, |v: Duration| v.hours()),
            Self::DurationDays(e) => unary_expr(e, values, |v: Duration| v.days()),
            Self::DurationWeeks(e) => unary_expr(e, values, |v: Duration| v.weeks()),
            #[allow(clippy::cast_possible_truncation)]
            Self::CastFromFloat(e) => unary_expr(e, values, |v: f64| v as i64),
            Self::CastFromBool(e) => unary_expr(e, values, |v: bool| i64::from(v)),
            Self::CastFromString(e) => unary_expr_err(e, values, &|v: ArcStr| {
                v.trim().parse().map_err(|_| {
                    DynError::from(DataError::ParseError(format!(
                        "Cannot cast to int from {v}.",
                    )))
                })
            }),
        }
    }
}

impl FloatExpression {
    pub fn eval(&self, values: &[&[Value]]) -> Vec<DynResult<f64>> {
        match self {
            Self::Const(c) => nullary_expr(values, &|| *c),
            Self::Neg(e) => unary_expr(e, values, |v: f64| -v),
            Self::Abs(e) => unary_expr(e, values, |v: f64| v.abs()),
            Self::Add(lhs, rhs) => binary_expr(lhs, rhs, values, |l: f64, r: f64| l + r),
            Self::Sub(lhs, rhs) => binary_expr(lhs, rhs, values, |l: f64, r: f64| l - r),
            Self::Mul(lhs, rhs) => binary_expr(lhs, rhs, values, |l: f64, r: f64| l * r),
            Self::FloorDiv(lhs, rhs) => binary_expr_err(lhs, rhs, values, |l: f64, r: f64| {
                if r == 0.0f64 {
                    Err(DynError::from(DataError::DivisionByZero))
                } else {
                    Ok((l / r).floor())
                }
            }),
            Self::TrueDiv(lhs, rhs) => binary_expr_err(lhs, rhs, values, |l: f64, r: f64| {
                if r == 0.0f64 {
                    Err(DynError::from(DataError::DivisionByZero))
                } else {
                    Ok(l / r)
                }
            }),
            Self::Mod(lhs, rhs) => {
                binary_expr_err(lhs, rhs, values, |l: f64, r: f64| {
                    /*
                    Implementation the same as the one in Cpython
                    https://github.com/python/cpython/blob/main/Objects/floatobject.c#L640
                    */
                    if r == 0.0f64 {
                        Err(DynError::from(DataError::DivisionByZero))
                    } else {
                        let mut modulo = l % r;
                        if modulo == 0.0f64 {
                            modulo = modulo.copysign(r);
                        } else if (r < 0.0f64) != (modulo < 0.0f64) {
                            modulo += r;
                        }
                        Ok(modulo)
                    }
                })
            }
            Self::Pow(lhs, rhs) => binary_expr(lhs, rhs, values, |l: f64, r: f64| {
                let result = l.powf(r);
                if result.is_infinite() {
                    warn!("overflow encountered in power.");
                }
                result
            }),
            #[allow(clippy::cast_precision_loss)]
            Self::IntTrueDiv(lhs, rhs) => binary_expr_err(lhs, rhs, values, |l: i64, r: i64| {
                if r == 0 {
                    Err(DynError::from(DataError::DivisionByZero))
                } else {
                    Ok((l as f64) / (r as f64))
                }
            }),
            Self::DateTimeNaiveTimestamp(e, unit) => {
                binary_expr_err(e, unit, values, |e: DateTimeNaive, unit: ArcStr| {
                    Ok(e.timestamp_in_unit(&unit)?)
                })
            }
            Self::DateTimeUtcTimestamp(e, unit) => {
                binary_expr_err(e, unit, values, |e: DateTimeUtc, unit: ArcStr| {
                    Ok(e.timestamp_in_unit(&unit)?)
                })
            }
            Self::DurationTrueDiv(lhs, rhs) => {
                binary_expr_err(lhs, rhs, values, |l: Duration, r: Duration| {
                    if r.is_zero() {
                        Err(DynError::from(DataError::DivisionByZero))
                    } else {
                        Ok(l.true_div(r))
                    }
                })
            }
            Self::CastFromBool(e) => unary_expr(e, values, |v| if v { 1.0 } else { 0.0 }),
            #[allow(clippy::cast_precision_loss)]
            Self::CastFromInt(e) => unary_expr(e, values, |v: i64| v as f64),
            Self::CastFromString(e) => unary_expr_err(e, values, &|v: ArcStr| {
                v.trim().parse().map_err(|_| {
                    DynError::from(DataError::ParseError(format!(
                        "Cannot cast to float from {v}.",
                    )))
                })
            }),
        }
    }
}

impl PointerExpression {
    pub fn eval(&self, values: &[&[Value]]) -> Vec<DynResult<Key>> {
        match self {
            Self::PointerFrom(args) => multi_expr(args, values, |args| Key::for_values(&args)),
            Self::PointerWithInstanceFrom(args, instance) => {
                multi_expression_with_additional_expression(
                    args,
                    instance,
                    values,
                    |args, instance| {
                        let mut args = args.to_vec();
                        args.push(instance);
                        ShardPolicy::LastKeyColumn.generate_key(&args)
                    },
                )
            }
        }
    }
}

impl StringExpression {
    pub fn eval(&self, values: &[&[Value]]) -> Vec<DynResult<ArcStr>> {
        match self {
            Self::Add(lhs, rhs) => binary_expr(lhs, rhs, values, |l: ArcStr, r: ArcStr| {
                if l.is_empty() {
                    r
                } else if r.is_empty() {
                    l
                } else {
                    ArcStr::from(([l, r]).concat())
                }
            }),
            Self::Mul(lhs, rhs) => binary_expr(lhs, rhs, values, |l: ArcStr, repeat: i64| {
                if repeat < 0 {
                    ArcStr::new()
                } else {
                    let repeat = usize::try_from(repeat).unwrap();
                    ArcStr::repeat(&l, repeat)
                }
            }),
            Self::CastFromInt(e) => unary_expr(e, values, |v: i64| v.to_string().into()),
            Self::CastFromFloat(e) => unary_expr(e, values, |v: f64| v.to_string().into()),
            Self::CastFromBool(e) => unary_expr(e, values, |v| {
                if v {
                    arcstr::literal!("True")
                } else {
                    arcstr::literal!("False")
                }
            }),
            Self::DateTimeNaiveStrftime(e, fmt) => {
                binary_expr(e, fmt, values, |e: DateTimeNaive, fmt: ArcStr| {
                    ArcStr::from(e.strftime(&fmt))
                })
            }
            Self::DateTimeUtcStrftime(e, fmt) => {
                binary_expr(e, fmt, values, |e: DateTimeUtc, fmt: ArcStr| {
                    ArcStr::from(e.strftime(&fmt))
                })
            }
            Self::ToString(e) => unary_expr(e, values, |v| match v {
                Value::String(s) => s,
                v => v.to_string().into(),
            }),
        }
    }
}

impl DateTimeNaiveExpression {
    pub fn eval(&self, values: &[&[Value]]) -> Vec<DynResult<DateTimeNaive>> {
        match self {
            Self::AddDuration(lhs, rhs) => {
                binary_expr(lhs, rhs, values, |l: DateTimeNaive, r: Duration| l + r)
            }
            Self::SubDuration(lhs, rhs) => {
                binary_expr(lhs, rhs, values, |l: DateTimeNaive, r: Duration| l - r)
            }
            Self::Strptime(e, fmt) => binary_expr_err(e, fmt, values, |e: ArcStr, fmt: ArcStr| {
                Ok(DateTimeNaive::strptime(&e, &fmt)?)
            }),
            Self::FromUtc(expr, timezone) => binary_expr_err(
                expr,
                timezone,
                values,
                |expr: DateTimeUtc, timezone: ArcStr| Ok(expr.to_naive_in_timezone(&timezone)?),
            ),
            Self::Round(expr, duration) => binary_expr(
                expr,
                duration,
                values,
                |expr: DateTimeNaive, duration: Duration| expr.round(duration),
            ),
            Self::Floor(expr, duration) => binary_expr(
                expr,
                duration,
                values,
                |expr: DateTimeNaive, duration: Duration| expr.truncate(duration),
            ),
            Self::FromTimestamp(expr, unit) => {
                binary_expr_err(expr, unit, values, |expr: i64, unit: ArcStr| {
                    Ok(DateTimeNaive::from_timestamp(expr, &unit)?)
                })
            }
            Self::FromFloatTimestamp(expr, unit) => {
                binary_expr_err(expr, unit, values, |expr: f64, unit: ArcStr| {
                    Ok(DateTimeNaive::from_timestamp_f64(expr, &unit)?)
                })
            }
        }
    }
}

impl DateTimeUtcExpression {
    pub fn eval(&self, values: &[&[Value]]) -> Vec<DynResult<DateTimeUtc>> {
        match self {
            Self::AddDuration(lhs, rhs) => {
                binary_expr(lhs, rhs, values, |l: DateTimeUtc, r: Duration| l + r)
            }
            Self::SubDuration(lhs, rhs) => {
                binary_expr(lhs, rhs, values, |l: DateTimeUtc, r: Duration| l - r)
            }
            Self::Strptime(e, fmt) => binary_expr_err(e, fmt, values, |e: ArcStr, fmt: ArcStr| {
                Ok(DateTimeUtc::strptime(&e, &fmt)?)
            }),
            Self::FromNaive(expr, to_timezone) => binary_expr_err(
                expr,
                to_timezone,
                values,
                |expr: DateTimeNaive, to_timezone: ArcStr| {
                    Ok(expr.to_utc_from_timezone(&to_timezone)?)
                },
            ),
            Self::Round(expr, duration) => binary_expr(
                expr,
                duration,
                values,
                |expr: DateTimeUtc, duration: Duration| expr.round(duration),
            ),
            Self::Floor(expr, duration) => binary_expr(
                expr,
                duration,
                values,
                |expr: DateTimeUtc, duration: Duration| expr.truncate(duration),
            ),
        }
    }
}

impl DurationExpression {
    pub fn eval(&self, values: &[&[Value]]) -> Vec<DynResult<Duration>> {
        match self {
            Self::FromTimeUnit(val, unit) => {
                binary_expr_err(val, unit, values, |val: i64, unit: ArcStr| {
                    Ok(Duration::new_with_unit(val, &unit)?)
                })
            }
            Self::Neg(e) => unary_expr(e, values, |v: Duration| -v),
            Self::Add(lhs, rhs) => binary_expr(lhs, rhs, values, |l: Duration, r: Duration| l + r),
            Self::Sub(lhs, rhs) => binary_expr(lhs, rhs, values, |l: Duration, r: Duration| l - r),
            Self::MulByInt(lhs, rhs) => binary_expr(lhs, rhs, values, |l: Duration, r: i64| l * r),
            Self::DivByInt(lhs, rhs) => binary_expr_err(lhs, rhs, values, |l: Duration, r: i64| {
                if r == 0 {
                    Err(DynError::from(DataError::DivisionByZero))
                } else {
                    Ok(l / r)
                }
            }),
            Self::TrueDivByInt(lhs, rhs) => {
                binary_expr_err(lhs, rhs, values, |l: Duration, r: i64| {
                    if r == 0 {
                        Err(DynError::from(DataError::DivisionByZero))
                    } else {
                        Ok(l.true_div_by_i64(r))
                    }
                })
            }
            Self::MulByFloat(lhs, rhs) => {
                binary_expr(lhs, rhs, values, |l: Duration, r: f64| l * r)
            }
            Self::DivByFloat(lhs, rhs) => {
                binary_expr_err(lhs, rhs, values, |l: Duration, r: f64| {
                    if r == 0.0 {
                        Err(DynError::from(DataError::DivisionByZero))
                    } else {
                        Ok(l / r)
                    }
                })
            }
            Self::Mod(lhs, rhs) => binary_expr_err(lhs, rhs, values, |l: Duration, r: Duration| {
                if r.is_zero() {
                    Err(DynError::from(DataError::DivisionByZero))
                } else {
                    Ok(l % r)
                }
            }),
            Self::DateTimeNaiveSub(lhs, rhs) => {
                binary_expr(lhs, rhs, values, |l: DateTimeNaive, r: DateTimeNaive| l - r)
            }
            Self::DateTimeUtcSub(lhs, rhs) => {
                binary_expr(lhs, rhs, values, |l: DateTimeUtc, r: DateTimeUtc| l - r)
            }
        }
    }
}

impl Expression {
    pub fn eval(&self, values: &[&[Value]]) -> Vec<DynResult<Value>> {
        self.eval_as(values)
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

    pub fn new_const(value: Value) -> Self {
        match value {
            Value::Bool(b) => Self::Bool(BoolExpression::Const(b)),
            Value::Int(i) => Self::Int(IntExpression::Const(i)),
            Value::Float(OrderedFloat(f)) => Self::Float(FloatExpression::Const(f)),
            other => Self::Any(AnyExpression::Const(other)),
        }
    }
}

trait EvalAs<T> {
    fn eval_as(&self, values: &[&[Value]]) -> Vec<DynResult<T>>;
}

impl EvalAs<Value> for Expression {
    fn eval_as(&self, values: &[&[Value]]) -> Vec<DynResult<Value>> {
        match self {
            Self::Bool(_) => unary_expr(self, values, |v: bool| Value::from(v)),
            Self::Int(_) => unary_expr(self, values, |v: i64| Value::from(v)),
            Self::Float(_) => unary_expr(self, values, |v: f64| Value::from(v)),
            Self::Pointer(_) => unary_expr(self, values, |v: Key| Value::from(v)),
            Self::String(_) => unary_expr(self, values, |v: ArcStr| Value::from(v)),
            Self::DateTimeNaive(_) => unary_expr(self, values, |v: DateTimeNaive| Value::from(v)),
            Self::DateTimeUtc(_) => unary_expr(self, values, |v: DateTimeUtc| Value::from(v)),
            Self::Duration(_) => unary_expr(self, values, |v: Duration| Value::from(v)),
            Self::Any(expr) => expr.eval(values),
        }
    }
}

impl EvalAs<bool> for Expression {
    fn eval_as(&self, values: &[&[Value]]) -> Vec<DynResult<bool>> {
        match self {
            Self::Bool(expr) => expr.eval(values),
            Self::Any(_) => unary_expr_err(self, values, &|v: Value| v.as_bool()),
            _ => values
                .iter()
                .map(|_| Err(self.type_error("bool")))
                .collect(),
        }
    }
}

impl EvalAs<i64> for Expression {
    fn eval_as(&self, values: &[&[Value]]) -> Vec<DynResult<i64>> {
        match self {
            Self::Int(expr) => expr.eval(values),
            Self::Any(_) => unary_expr_err(self, values, &|v: Value| v.as_int()),
            _ => values.iter().map(|_| Err(self.type_error("int"))).collect(),
        }
    }
}

impl EvalAs<f64> for Expression {
    fn eval_as(&self, values: &[&[Value]]) -> Vec<DynResult<f64>> {
        match self {
            Self::Float(expr) => expr.eval(values),
            Self::Any(_) => unary_expr_err(self, values, &|v: Value| v.as_float()),
            _ => values
                .iter()
                .map(|_| Err(self.type_error("float")))
                .collect(),
        }
    }
}

impl EvalAs<ArcStr> for Expression {
    fn eval_as(&self, values: &[&[Value]]) -> Vec<DynResult<ArcStr>> {
        match self {
            Self::String(expr) => expr.eval(values),
            Self::Any(_) => unary_expr_err(self, values, &|v: Value| v.as_string().cloned()),
            _ => values
                .iter()
                .map(|_| Err(self.type_error("string")))
                .collect(),
        }
    }
}

impl EvalAs<Key> for Expression {
    fn eval_as(&self, values: &[&[Value]]) -> Vec<DynResult<Key>> {
        match self {
            Self::Pointer(expr) => expr.eval(values),
            Self::Any(_) => unary_expr_err(self, values, &|v: Value| v.as_pointer()),
            _ => values
                .iter()
                .map(|_| Err(self.type_error("pointer")))
                .collect(),
        }
    }
}

impl EvalAs<DateTimeNaive> for Expression {
    fn eval_as(&self, values: &[&[Value]]) -> Vec<DynResult<DateTimeNaive>> {
        match self {
            Self::DateTimeNaive(expr) => expr.eval(values),
            Self::Any(_) => unary_expr_err(self, values, &|v: Value| v.as_date_time_naive()),
            _ => values
                .iter()
                .map(|_| Err(self.type_error("DateTimeNaive")))
                .collect(),
        }
    }
}

impl EvalAs<DateTimeUtc> for Expression {
    fn eval_as(&self, values: &[&[Value]]) -> Vec<DynResult<DateTimeUtc>> {
        match self {
            Self::DateTimeUtc(expr) => expr.eval(values),
            Self::Any(_) => unary_expr_err(self, values, &|v: Value| v.as_date_time_utc()),
            _ => values
                .iter()
                .map(|_| Err(self.type_error("DateTimeUtc")))
                .collect(),
        }
    }
}

impl EvalAs<Duration> for Expression {
    fn eval_as(&self, values: &[&[Value]]) -> Vec<DynResult<Duration>> {
        match self {
            Self::Duration(expr) => expr.eval(values),
            Self::Any(_) => unary_expr_err(self, values, &|v: Value| v.as_duration()),
            _ => values
                .iter()
                .map(|_| Err(self.type_error("Duration")))
                .collect(),
        }
    }
}

impl EvalAs<Arc<[Value]>> for Expression {
    fn eval_as(&self, values: &[&[Value]]) -> Vec<DynResult<Arc<[Value]>>> {
        match self {
            Self::Any(_) => unary_expr_err(self, values, &|v: Value| Ok(v.as_tuple()?.clone())),
            _ => values
                .iter()
                .map(|_| Err(self.type_error("Tuple")))
                .collect(),
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
