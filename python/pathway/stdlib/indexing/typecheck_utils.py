# Copyright © 2024 Pathway
from typing import Tuple

import pathway.internals as pw
from pathway.internals import dtype as dt
from pathway.internals.type_interpreter import eval_type


def check_column_reference_type(
    parameters: list[
        Tuple[str, Tuple[pw.ColumnExpression, dt.DType | tuple[dt.DType, ...]]]
    ]
) -> None:

    failed = []
    for name, (expr, types) in parameters:
        expr_type = eval_type(expr)
        if isinstance(types, tuple):
            single_type = types[0]
            valid = any(dt.dtype_issubclass(expr_type, dtype) for dtype in types)
        else:
            single_type = types
            valid = dt.dtype_issubclass(eval_type(expr), single_type)
        if not valid:
            failed.append((name, (expr_type, single_type)))

    if failed:
        msg = "Some columns have types incompatible with expected types: " + ", ".join(
            f"{name} should be compatible with type {dtype!r} but is of type {expr_type!r}"
            for (name, (expr_type, dtype)) in failed
        )

        raise TypeError(msg)
