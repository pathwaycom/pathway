# Copyright © 2024 Pathway
from typing import Tuple

import pathway.internals as pw
from pathway.internals import dtype as dt
from pathway.internals.type_interpreter import eval_type


def check_column_reference_type(
    parameters: list[Tuple[str, Tuple[pw.ColumnExpression, dt.DType]]]
) -> None:

    failed = [
        (name, (expr, type))
        for (name, (expr, type)) in parameters
        if not dt.dtype_issubclass(eval_type(expr), type)
    ]
    if failed:
        msg = "Some columns have types incompatible with expected types: " + ", ".join(
            f"{name} should be compatible with type {type!r} but is of type {eval_type(expr)!r}"
            for (name, (expr, type)) in failed
        )

        raise TypeError(msg)
