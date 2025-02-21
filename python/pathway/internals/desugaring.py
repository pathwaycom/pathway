# Copyright © 2024 Pathway

from __future__ import annotations

from abc import abstractmethod
from collections.abc import Iterable, Mapping
from functools import wraps
from typing import TYPE_CHECKING, Any, TypeVar

from pathway.internals import expression as expr
from pathway.internals.expression_visitor import IdentityTransform
from pathway.internals.helpers import function_spec, with_optional_kwargs
from pathway.internals.row_transformer import RowTransformer

if TYPE_CHECKING:
    from pathway.internals import groupbys, table, thisclass


class DesugaringTransform(IdentityTransform):
    def eval_tuple_of_maybe_expressions(self, expression: tuple, **kwargs):
        result = [self.eval_expression(e) for e in expression]

        if any(isinstance(e, expr.ColumnExpression) for e in result):
            return expr.MakeTupleExpression(*result)

        return expression

    def eval_any(self, expression, **kwargs):
        if isinstance(expression, tuple):
            return self.eval_tuple_of_maybe_expressions(expression, **kwargs)
        return expression


class ThisDesugaring(DesugaringTransform):
    substitution: dict[thisclass.ThisMetaclass, table.Joinable]

    def __init__(self, substitution: dict[thisclass.ThisMetaclass, table.Joinable]):
        self.substitution = substitution

    def eval_column_val(
        self, expression: expr.ColumnReference, **kwargs
    ) -> expr.ColumnReference:
        from pathway.internals import thisclass

        # Below we want to break moment we reach the fix-point
        # desugaring is hard, since it can desugar delayed-op-on-this to identical delayed-op-on-this
        # (due to interval join desugaring pw.this to pw.this, which can break desugaring of delayed ix)
        # and delayed-ops are hard to compare.
        # But any nontrivial step reduces `depth` of delayed ops, or lands us in `pw.Table` world where
        # comparisons make sense.
        while True:
            expression = expression.table[expression.name]  # magic desugaring slices
            prev = expression.table
            table = self._desugar_table(expression.table)
            expression = table[expression.name]
            if prev == table or (
                isinstance(prev, thisclass.ThisMetaclass)
                and isinstance(table, thisclass.ThisMetaclass)
                and prev._delay_depth() == table._delay_depth()
            ):
                break
        return expression

    def eval_pointer(
        self, expression: expr.PointerExpression, **kwargs
    ) -> expr.PointerExpression:
        args = [self.eval_expression(arg, **kwargs) for arg in expression._args]
        instance = expression._instance
        optional = expression._optional
        desugared_table = self._desugar_table(expression._table)
        from pathway.internals.table import Table

        assert isinstance(desugared_table, Table)

        return expr.PointerExpression(
            desugared_table,
            *args,
            optional=optional,
            instance=instance,
        )

    def _desugar_table(
        self, table: table.Joinable | thisclass.ThisMetaclass
    ) -> table.Joinable:
        from pathway.internals import thisclass

        if not isinstance(table, thisclass.ThisMetaclass):
            return table
        return table._eval_substitution(self.substitution)


class SubstitutionDesugaring(DesugaringTransform):
    substitution: dict[expr.InternalColRef, expr.ColumnExpression]

    def __init__(self, substitution: dict[expr.InternalColRef, expr.ColumnExpression]):
        self.substitution = substitution

    def eval_column_val(  # type: ignore[override]
        self, expression: expr.ColumnReference, **kwargs
    ) -> expr.ColumnExpression:
        return self.substitution.get(expression._to_internal(), expression)


class TableSubstitutionDesugaring(DesugaringTransform):
    """Maps all references to tables according to `table_substitution` dictionary."""

    def __init__(
        self,
        table_substitution: dict[table.TableLike, table.Table],
    ):
        self._table_substitution = table_substitution

    def eval_column_val(
        self, expression: expr.ColumnReference, **kwargs
    ) -> expr.ColumnReference:
        target_table = self._table_substitution.get(expression.table)
        if target_table is None:
            return super().eval_column_val(expression, **kwargs)
        else:
            return target_table[expression.name]


class TableReplacementWithNoneDesugaring(IdentityTransform):
    """Replaces all references to `table` with None."""

    def __init__(self, table):
        self._table = table

    def eval_column_val(  # type: ignore[override]
        self, expression: expr.ColumnReference, **kwargs
    ) -> expr.ColumnExpression:
        if expression.table is self._table:
            return expr.ColumnConstExpression(None)
        else:
            return super().eval_column_val(expression, **kwargs)

    def eval_require(
        self, expression: expr.RequireExpression, **kwargs
    ) -> expr.RequireExpression:
        val = self.eval_expression(expression._val, **kwargs)
        args = [self.eval_expression(arg, **kwargs) for arg in expression._args]
        for arg in args:
            if isinstance(arg, expr.ColumnConstExpression):
                if arg._val is None:
                    return expr.ColumnConstExpression(None)  # type: ignore
        return expr.RequireExpression(val, *args)


class TableCallbackDesugaring(DesugaringTransform):
    table_like: table.TableLike | groupbys.GroupedJoinable

    def __init__(self, table_like: table.TableLike | groupbys.GroupedJoinable):
        self.table_like = table_like

    @abstractmethod
    def callback(self, *args, **kwargs):
        pass

    def eval_call(  # type:ignore[override]
        self, expression: expr.ColumnCallExpression, **kwargs
    ) -> expr.ColumnReference:
        args: dict[str, expr.ColumnExpression] = {
            f"arg_{index}": self.eval_expression(arg, **kwargs)
            for index, arg in enumerate(expression._args)
        }

        from pathway.internals import type_interpreter

        method_call_transformer = RowTransformer.method_call_transformer(
            len(args), dtype=type_interpreter.eval_type(expression)
        )
        method_call_input = self.callback(method=expression._col_expr, **args)
        call_result = method_call_transformer(table=method_call_input)
        table = call_result.table
        return table.result


class TableSelectDesugaring(TableCallbackDesugaring):
    table_like: table.Joinable

    def __init__(self, table_like: table.Joinable):
        from pathway.internals import table

        assert isinstance(table_like, table.Joinable)
        super().__init__(table_like)

    def callback(self, *args, **kwargs):
        return self.table_like.select(*args, **kwargs)


class TableReduceDesugaring(TableCallbackDesugaring):
    table_like: groupbys.GroupedJoinable

    def __init__(self, table_like: groupbys.GroupedJoinable):
        from pathway.internals import groupbys

        assert isinstance(table_like, groupbys.GroupedJoinable)
        super().__init__(table_like)

    def callback(self, *args, **kwargs):
        return self.table_like.reduce(*args, **kwargs)

    def eval_reducer(
        self, expression: expr.ReducerExpression, **kwargs
    ) -> expr.ReducerExpression:
        select_desugar = TableSelectDesugaring(self.table_like._joinable_to_group)
        args = [
            select_desugar.eval_expression(arg, **kwargs) for arg in expression._args
        ]
        return expr.ReducerExpression(expression._reducer, *args)


class RestrictUniverseDesugaring(DesugaringTransform):
    table_like: table.TableLike
    cache: dict[table.Table, table.Table]

    def __init__(self, table_like: table.TableLike) -> None:
        self.table_like = table_like
        self.cache = {}

    def eval_column_val(
        self, expression: expr.ColumnReference, **kwargs
    ) -> expr.ColumnReference:
        from pathway.internals.thisclass import ThisMetaclass

        if isinstance(expression.table, ThisMetaclass):
            return expression
        if self.table_like._universe.is_subset_of(
            expression.table._universe
        ) and not self.table_like._universe.is_equal_to(expression.table._universe):
            if expression.table not in self.cache:
                self.cache[expression.table] = expression.table.restrict(
                    self.table_like
                )
            return expr.ColumnReference(
                _column=expression._column,
                _table=self.cache[expression.table],
                _name=expression.name,
            )
        else:
            return super().eval_column_val(expression, **kwargs)


ColExprT = TypeVar("ColExprT", bound=expr.ColumnExpression)


def _desugar_this_arg(
    substitution: dict[thisclass.ThisMetaclass, table.Joinable],
    expression: ColExprT,
) -> ColExprT:
    return ThisDesugaring(substitution).eval_expression(expression)


def _desugar_this_args(
    substitution: dict[thisclass.ThisMetaclass, table.Joinable],
    args: Iterable[ColExprT],
) -> tuple[ColExprT, ...]:
    ret: list[ColExprT] = []
    from pathway.internals import thisclass

    for arg in args:
        if isinstance(arg, thisclass.ThisMetaclass):
            assert issubclass(arg, thisclass.iter_guard)
            evaled_table = arg._eval_substitution(substitution)
            ret.extend(evaled_table)
        else:
            ret.append(_desugar_this_arg(substitution, arg))

    return tuple(ret)


def _desugar_this_kwargs(
    substitution: dict[thisclass.ThisMetaclass, table.Joinable],
    kwargs: Mapping[str, ColExprT],
) -> dict[str, ColExprT]:
    from pathway.internals import thisclass

    new_kwargs = {
        name: arg
        for name, arg in kwargs.items()
        if not name.startswith(thisclass.KEY_GUARD)
    }
    for name, arg in kwargs.items():
        if name.startswith(thisclass.KEY_GUARD):
            assert isinstance(arg, thisclass.ThisMetaclass)
            evaled_table = arg._eval_substitution(substitution)
            new_kwargs.update(evaled_table)
    return {
        name: _desugar_this_arg(substitution, arg) for name, arg in new_kwargs.items()
    }


def combine_args_kwargs(
    args: Iterable[expr.ColumnReference],
    kwargs: Mapping[str, Any],
    exclude_columns: set[str] | None = None,
) -> dict[str, expr.ColumnExpression]:
    all_args = {}

    def add(name, expression):
        if exclude_columns is not None and name in exclude_columns:
            return
        if name in all_args:
            raise ValueError(f"Duplicate expression value given for {name}")
        if name == "id":
            raise ValueError("Can't use 'id' as a column name")
        if not isinstance(expression, expr.ColumnExpression):
            expression = expr.ColumnConstExpression(expression)
        all_args[name] = expression

    for expression in args:
        add(expr.smart_name(expression), expression)
    for name, expression in kwargs.items():
        add(name, expression)

    return all_args


class DesugaringContext:
    _substitution: dict[thisclass.ThisMetaclass, table.Joinable] = {}

    @property
    @abstractmethod
    def _desugaring(self) -> DesugaringTransform:
        pass


@with_optional_kwargs
def desugar(func, **kwargs):
    fn_spec = function_spec(func)
    substitution_param = kwargs.get("substitution", {})

    @wraps(func)
    def wrapper(*args, **kwargs):
        named_args = {**dict(zip(fn_spec.arg_names, args)), **kwargs}
        assert len(named_args) > 0
        first_arg = next(iter(named_args.values()))
        desugaring_context = (
            first_arg if isinstance(first_arg, DesugaringContext) else None
        )

        this_substitution = {}
        if desugaring_context is not None:
            this_substitution.update(desugaring_context._substitution)

        for key, value in substitution_param.items():
            assert isinstance(value, str)
            this_substitution[key] = named_args[value]

        args = _desugar_this_args(this_substitution, args)
        kwargs = _desugar_this_kwargs(this_substitution, kwargs)

        if desugaring_context is not None:
            args = tuple(
                desugaring_context._desugaring.eval_expression(arg) for arg in args
            )
            kwargs = {
                key: desugaring_context._desugaring.eval_expression(value)
                for key, value in kwargs.items()
            }
        return func(*args, **kwargs)

    return wrapper
