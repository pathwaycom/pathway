# Copyright © 2024 Pathway

from __future__ import annotations

import itertools
from collections import defaultdict
from collections.abc import Iterable
from typing import TYPE_CHECKING

from pathway.internals import dtype as dt, expression as expr

if TYPE_CHECKING:
    from pathway.internals.table import Table
    from pathway.internals.trace import Trace

from pathway.internals.expression_visitor import ExpressionVisitor


class ExpressionFormatter(ExpressionVisitor):
    table_numbers: dict[Table, int]

    def __init__(self):
        self.table_counter = itertools.count(start=1)
        self.table_numbers = defaultdict(lambda: next(self.table_counter))

    def table_infos(self):
        for tab, cnt in self.table_numbers.items():
            trace: Trace = tab._source.operator.trace
            yield f"<table{cnt}>", trace.user_frame

    def print_table_infos(self):
        return "\n".join(
            f"{name} created in {frame.filename}:{frame.line_number}"
            for name, frame in self.table_infos()
        )

    def eval_column_val(self, expression: expr.ColumnReference):
        from pathway.internals.thisclass import ThisMetaclass, left, right, this

        if isinstance(expression._table, ThisMetaclass):
            if expression._table == this:
                prefix = "this"
            elif expression._table == left:
                prefix = "left"
            elif expression._table == right:
                prefix = "right"
            else:
                prefix = f"{expression._table}"

            return f"pathway.{prefix}.{expression._name}"
        else:
            return f"<table{self.table_numbers[expression._table]}>.{expression._name}"

    def eval_unary_op(self, expression: expr.ColumnUnaryOpExpression):
        symbol = getattr(expression._operator, "_symbol", expression._operator.__name__)
        uexpr = self.eval_expression(expression._expr)
        return f"({symbol}{uexpr})"

    def eval_binary_op(self, expression: expr.ColumnBinaryOpExpression):
        symbol = getattr(expression._operator, "_symbol", expression._operator.__name__)
        lexpr = self.eval_expression(expression._left)
        rexpr = self.eval_expression(expression._right)
        return f"({lexpr} {symbol} {rexpr})"

    def eval_const(self, expression: expr.ColumnConstExpression):
        return repr(expression._val)

    def eval_reducer(self, expression: expr.ReducerExpression):
        args = self._eval_args_kwargs(expression._args, expression._kwargs)
        name = expression._reducer.name
        return f"pathway.reducers.{name}({args})"

    def eval_apply(self, expression: expr.ApplyExpression):
        args = self._eval_args_kwargs(expression._args, expression._kwargs)
        return f"pathway.apply({expression._fun.__name__}, {args})"

    def eval_async_apply(self, expression: expr.ApplyExpression):
        args = self._eval_args_kwargs(expression._args, expression._kwargs)
        return f"pathway.apply_async({expression._fun.__name__}, {args})"

    def eval_pointer(self, expression: expr.PointerExpression):
        kwargs: dict[str, expr.ColumnExpression] = {}
        if expression._instance is not None:
            kwargs["instance"] = expression._instance
        if expression._optional:
            import pathway.internals.expression as expr

            kwargs["optional"] = expr.ColumnConstExpression(True)
        args = self._eval_args_kwargs(expression._args, kwargs)
        return f"<table{self.table_numbers[expression._table]}>.pointer_from({args})"

    def eval_call(self, expression: expr.ColumnCallExpression):
        args = self._eval_args_kwargs(expression._args)
        return self.eval_expression(expression._col_expr) + f"({args})"

    def eval_cast(self, expression: expr.CastExpression):
        uexpr = self.eval_expression(expression._expr)
        return f"pathway.cast({_type_name(expression._return_type)}, {uexpr})"

    def eval_convert(self, expression: expr.ConvertExpression):
        uexpr = self.eval_expression(expression._expr)
        dtype = dt.unoptionalize(expression._return_type)
        return f"pathway.as_{_type_name(dtype).lower()}({uexpr})"

    def eval_declare(self, expression: expr.DeclareTypeExpression):
        uexpr = self.eval_expression(expression._expr)
        return f"pathway.declare_type({_type_name(expression._return_type)}, {uexpr})"

    def eval_coalesce(self, expression: expr.CoalesceExpression):
        args = self._eval_args_kwargs(expression._args)
        return f"pathway.coalesce({args})"

    def eval_require(self, expression: expr.RequireExpression):
        args = self._eval_args_kwargs((expression._val, *expression._args))
        return f"pathway.require({args})"

    def eval_ifelse(self, expression: expr.IfElseExpression):
        args = self._eval_args_kwargs(
            (expression._if, expression._then, expression._else)
        )
        return f"pathway.if_else({args})"

    def eval_not_none(self, expression: expr.IsNotNoneExpression):
        args = self._eval_args_kwargs((expression._expr,))
        return f"{args}.is_not_none())"

    def eval_none(self, expression: expr.IsNoneExpression):
        args = self._eval_args_kwargs((expression._expr,))
        return f"{args}.is_none())"

    def eval_method_call(self, expression: expr.MethodCallExpression):
        object_ = self.eval_expression(expression._args_used_for_repr[0])
        args = self._eval_args_kwargs(expression._args_used_for_repr[1:])
        return f"({object_}).{expression._name}({args})"

    def _eval_args_kwargs(
        self,
        args: Iterable[expr.ColumnExpression] = (),
        kwargs: dict[str, expr.ColumnExpression] = {},
    ):
        return ", ".join(
            itertools.chain(
                (self.eval_expression(arg) for arg in args),
                (
                    key + "=" + self.eval_expression(value)
                    for key, value in kwargs.items()
                ),
            )
        )

    def eval_make_tuple(self, expression: expr.MakeTupleExpression):
        args = self._eval_args_kwargs(expression._args)
        return f"pathway.make_tuple({args})"

    def eval_get(self, expression: expr.GetExpression):
        object = self.eval_expression(expression._object)
        args = [expression._index]

        if expression._check_if_exists:
            args += [expression._default]
        args_formatted = self._eval_args_kwargs(args)
        if expression._check_if_exists:
            return f"({object}).get({args_formatted})"
        else:
            return f"({object})[{args_formatted}]"

    def eval_unwrap(self, expression: expr.UnwrapExpression):
        uexpr = self.eval_expression(expression._expr)
        return f"pathway.unwrap({uexpr})"

    def eval_fill_error(self, expression: expr.FillErrorExpression):
        args = self._eval_args_kwargs((expression._expr, expression._replacement))
        return f"pathway.fill_error({args})"

    def eval_fully_async_apply(self, expression: expr.FullyAsyncApplyExpression):
        args = self._eval_args_kwargs(expression._args, expression._kwargs)
        return f"pathway.apply_fully_async({expression._fun.__name__}, {args})"


def get_expression_info(expression: expr.ColumnExpression) -> str:
    printer = ExpressionFormatter()
    expression_str = f"\t{printer.eval_expression(expression)}\n"
    expression_info = ""

    frame = expression._trace.user_frame
    if frame is not None:
        expression_info = f"called in {frame.filename}:{frame.line_number}\n"

    tabnames = printer.print_table_infos()
    if tabnames != "":
        tabnames = "with tables:\n\t" + tabnames + "\n"

    return expression_str + expression_info + tabnames


def _type_name(return_type):
    from pathway.internals import dtype as dt

    if isinstance(return_type, str) or isinstance(return_type, dt.DType):
        return repr(return_type)
    else:
        return return_type.__name__
