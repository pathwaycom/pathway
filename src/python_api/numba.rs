#![deny(unsafe_op_in_unsafe_fn)]

use std::mem::transmute;
use std::sync::Arc;

use pyo3::prelude::*;

use pyo3::PyCell;

use crate::engine::{AnyExpression, BatchWrapper, Expression, Expressions, Graph, Value};

use super::EvalProperties;
use super::{Column, Scope, Table};

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
    ($addr:expr, $type0:tt, $ret:tt) => {{
        let function_ptr: unsafe extern "C" fn($type0) -> $ret = unsafe { transmute($addr) };
        Expression::Any(AnyExpression::Apply(
            Box::new(move |values| {
                let val = input_type!($type0, values[0]);
                let result = unsafe { function_ptr(val) };
                Ok(output_type!($ret, result))
            }),
            Expressions::AllArguments,
        ))
    }};
}

macro_rules! binary_function {
    ($addr:expr, $type0:tt, $type1: tt, $ret:tt) => {{
        let function_ptr: unsafe extern "C" fn($type0, $type1) -> $ret =
            unsafe { transmute($addr) };
        Expression::Any(AnyExpression::Apply(
            Box::new(move |values| {
                let left = input_type!($type0, values[0]);
                let right = input_type!($type1, values[1]);
                let result = unsafe { function_ptr(left, right) };
                Ok(output_type!($ret, result))
            }),
            Expressions::AllArguments,
        ))
    }};
}

pub unsafe fn unsafe_map_column(
    self_: &PyCell<Scope>,
    table: &Table,
    function: &PyAny,
    properties: EvalProperties,
) -> PyResult<Py<Column>> {
    let py = self_.py();

    let address = function.getattr("address")?.extract::<usize>()? as *const ();

    match table.columns.as_slice() {
        [left_column, right_column] => {
            let expression = match function.getattr("_sig")?.str()?.to_str()? {
                "(int64, int64) -> int64" => binary_function!(address, i64, i64, i64),
                "(int64, int64) -> float64" => binary_function!(address, i64, i64, f64),
                "(int64, float64) -> int64" => binary_function!(address, i64, f64, i64),
                "(int64, float64) -> float64" => binary_function!(address, i64, f64, f64),
                "(float64, int64) -> int64" => binary_function!(address, f64, i64, i64),
                "(float64, int64) -> float64" => binary_function!(address, f64, i64, f64),
                "(float64, float64) -> int64" => binary_function!(address, f64, f64, i64),
                "(float64, float64) -> float64" => binary_function!(address, f64, f64, f64),
                other => unimplemented!("unimplemented signature {:?}", other),
            };

            let left_column = left_column.borrow(py);
            let right_column = right_column.borrow(py);

            let universe = left_column.universe.as_ref(py);

            let handle = self_.borrow().graph.expression_column(
                BatchWrapper::None,
                Arc::new(expression),
                universe.borrow().handle,
                vec![left_column.handle, right_column.handle],
                properties.trace(py)?,
            )?;
            Column::new(universe, handle)
        }
        [column] => {
            let expression = match function.getattr("_sig")?.str()?.to_str()? {
                "(int64,) -> int64" => unary_function!(address, i64, i64),
                "(int64,) -> float64" => unary_function!(address, i64, f64),
                "(float64,) -> int64" => unary_function!(address, f64, i64),
                "(float64,) -> float64" => unary_function!(address, f64, f64),
                other => unimplemented!("unimplemented signature {:?}", other),
            };
            let column = column.borrow(py);

            let universe = column.universe.as_ref(py);

            let handle = self_.borrow().graph.expression_column(
                BatchWrapper::None,
                Arc::new(expression),
                universe.borrow().handle,
                vec![column.handle],
                properties.trace(py)?,
            )?;
            Column::new(universe, handle)
        }
        _ => unimplemented!(),
    }
}
