#![deny(unsafe_op_in_unsafe_fn)]

use std::mem::transmute;
use std::sync::Arc;

use pyo3::prelude::*;

use crate::engine::{AnyExpression, Expression, Expressions, Value};

macro_rules! input_type {
    (i64, $val:expr) => {
        i64::try_from($val.as_int()?).unwrap() // XXX
    };
    (f64, $val:expr) => {
        $val.as_float()?
    };
}

macro_rules! output_type {
    (i64, $val:expr) => {
        Value::from(i64::from($val))
    };
    (f64, $val:expr) => {
        Value::from($val)
    };
}

macro_rules! unary_function {
    ($addr:expr, $args:expr, $type0:tt, $ret:tt) => {{
        let function_ptr: unsafe extern "C" fn($type0) -> $ret = unsafe { transmute($addr) };
        Expression::Any(AnyExpression::Apply(
            Box::new(move |values| {
                let val = input_type!($type0, values[0]);
                let result = unsafe { function_ptr(val) };
                Ok(output_type!($ret, result))
            }),
            $args,
        ))
    }};
}

macro_rules! binary_function {
    ($addr:expr, $args:expr, $type0:tt, $type1: tt, $ret:tt) => {{
        let function_ptr: unsafe extern "C" fn($type0, $type1) -> $ret =
            unsafe { transmute($addr) };
        Expression::Any(AnyExpression::Apply(
            Box::new(move |values| {
                let left = input_type!($type0, values[0]);
                let right = input_type!($type1, values[1]);
                let result = unsafe { function_ptr(left, right) };
                Ok(output_type!($ret, result))
            }),
            $args,
        ))
    }};
}

pub unsafe fn get_numba_expression(
    function: &PyAny,
    args: Expressions,
    num_args: usize,
) -> PyResult<Arc<Expression>> {
    let address = function.getattr("address")?.extract::<usize>()? as *const ();

    let expression = match num_args {
        2 => match function.getattr("_sig")?.str()?.to_str()? {
            "(int64, int64) -> int64" => binary_function!(address, args, i64, i64, i64),
            "(int64, int64) -> float64" => binary_function!(address, args, i64, i64, f64),
            "(int64, float64) -> int64" => binary_function!(address, args, i64, f64, i64),
            "(int64, float64) -> float64" => binary_function!(address, args, i64, f64, f64),
            "(float64, int64) -> int64" => binary_function!(address, args, f64, i64, i64),
            "(float64, int64) -> float64" => binary_function!(address, args, f64, i64, f64),
            "(float64, float64) -> int64" => binary_function!(address, args, f64, f64, i64),
            "(float64, float64) -> float64" => binary_function!(address, args, f64, f64, f64),
            other => unimplemented!("unimplemented signature {:?}", other),
        },
        1 => match function.getattr("_sig")?.str()?.to_str()? {
            "(int64,) -> int64" => unary_function!(address, args, i64, i64),
            "(int64,) -> float64" => unary_function!(address, args, i64, f64),
            "(float64,) -> int64" => unary_function!(address, args, f64, i64),
            "(float64,) -> float64" => unary_function!(address, args, f64, f64),
            other => unimplemented!("unimplemented signature {:?}", other),
        },
        _ => unimplemented!(),
    };
    Ok(Arc::new(expression))
}
