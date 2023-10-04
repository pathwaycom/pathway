# Copyright © 2023 Pathway

from __future__ import annotations

import warnings
from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable
from functools import cached_property
from typing import TYPE_CHECKING, ClassVar

from pathway.internals import api, apply_with_type, asynchronous
from pathway.internals import column as clmn
from pathway.internals import dtype as dt
from pathway.internals import environ
from pathway.internals import expression as expr
from pathway.internals import universe as univ
from pathway.internals.column_path import ColumnPath
from pathway.internals.column_properties import ColumnProperties
from pathway.internals.expression_printer import get_expression_info
from pathway.internals.expression_visitor import ExpressionVisitor, IdentityTransform
from pathway.internals.graph_runner.path_storage import Storage
from pathway.internals.graph_runner.scope_context import ScopeContext
from pathway.internals.operator_mapping import (
    common_dtype_in_binary_operator,
    get_binary_expression,
    get_binary_operators_mapping_optionals,
    get_cast_operators_mapping,
    get_convert_operators_mapping,
    get_unary_expression,
    tuple_handling_operators,
)

if TYPE_CHECKING:
    from pathway.internals.graph_runner.state import ScopeState


def column_eval_properties(column: clmn.Column) -> api.EvalProperties:
    props = column.properties
    return api.EvalProperties(
        trace=column.trace.to_engine(),
        dtype=props.dtype,
        append_only=props.append_only,
    )


class ExpressionEvaluator(ABC):
    _context_map: ClassVar[dict[type[clmn.Context], type[ExpressionEvaluator]]] = {}
    context_type: ClassVar[type[clmn.Context]]
    scope: api.Scope
    state: ScopeState
    scope_context: ScopeContext

    def __init__(
        self,
        context: clmn.Context,
        scope: api.Scope,
        state: ScopeState,
        scope_context: ScopeContext,
    ) -> None:
        self.context = context
        self.scope = scope
        self.state = state
        self.scope_context = scope_context
        # assert isinstance(context, self.context_type)
        # FIXME uncomment when there is a single version of RowwiseEvaluator
        self._initialize_from_context()

    def __init_subclass__(cls, /, context_type, **kwargs):
        super().__init_subclass__(**kwargs)
        cls.context_type = context_type
        assert context_type not in cls._context_map
        cls._context_map[context_type] = cls

    def _initialize_from_context(self):
        pass

    @property
    def output_universe(self) -> api.Universe:
        return self.state.get_universe(self.context.universe)

    @classmethod
    def for_context(cls, context: clmn.Context) -> type[ExpressionEvaluator]:
        return cls._context_map[type(context)]

    @abstractmethod
    def eval(self, column: clmn.ColumnWithExpression) -> api.Column:
        pass

    def expression_type(
        self,
        expression: clmn.ColumnExpression,
    ):
        return self.context.expression_type(expression)

    def run(self, output_storage: Storage, *input_storages: Storage) -> api.Table:
        raise NotImplementedError()

    def _flatten_table_storage(
        self,
        output_storage: Storage,
        input_storage: Storage,
    ) -> api.Table:
        paths = [
            input_storage.get_path(column) for column in output_storage.get_columns()
        ]

        engine_flattened_table = self.scope.flatten_table_storage(
            self.state.get_table(input_storage), paths
        )
        return engine_flattened_table

    def flatten_table_storage_if_needed(self, output_storage: Storage):
        if output_storage.flattened_output is not None:
            flattened_storage = self._flatten_table_storage(
                output_storage.flattened_output, output_storage
            )
            self.state.set_table(output_storage.flattened_output, flattened_storage)

    def flatten_tables(
        self, output_storage: Storage, *input_storages: Storage
    ) -> tuple[api.Table, ...]:
        if output_storage.flattened_inputs is not None:
            assert len(input_storages) == len(output_storage.flattened_inputs)
            engine_input_tables = []
            for input_storage, flattened_storage in zip(
                input_storages, output_storage.flattened_inputs
            ):
                flattened_engine_storage = self._flatten_table_storage(
                    flattened_storage, input_storage
                )
                engine_input_tables.append(flattened_engine_storage)
        else:
            engine_input_tables = [
                self.state.get_table(storage) for storage in input_storages
            ]
        return tuple(engine_input_tables)


class RowwiseEvalState:
    _dependencies: dict[api.Column, int]

    def __init__(self) -> None:
        self._dependencies = {}

    def dependency(self, column: api.Column) -> int:
        return self._dependencies.setdefault(column, len(self._dependencies))

    @property
    def columns(self) -> list[api.Column]:
        return list(self._dependencies.keys())


class DependencyReference:
    index: int
    column: api.Column

    def __init__(self, index: int, column: api.Column):
        self.index = index
        self.column = column

    def __call__(self, vals):
        return vals[self.index]


class TypeVerifier(IdentityTransform):
    def eval_expression(  # type: ignore[override]
        self, expression: expr.ColumnExpression, **kwargs
    ) -> expr.ColumnExpression:
        expression = super().eval_expression(expression, **kwargs)

        dtype = expression._dtype

        def test_type(val):
            if not dtype.is_value_compatible(val):
                raise TypeError(f"Value {val} is not of type {dtype}.")
            return val

        ret = apply_with_type(test_type, dtype, expression)
        ret._dtype = dtype
        return ret


class RowwiseEvaluator(
    ExpressionEvaluator, ExpressionVisitor, context_type=clmn.RowwiseContext
):
    def eval_expression(
        self,
        expression: expr.ColumnExpression,
        eval_state: RowwiseEvalState | None = None,
        **kwargs,
    ) -> api.Expression:
        assert eval_state is not None
        assert not kwargs
        return super().eval_expression(expression, eval_state=eval_state, **kwargs)

    def eval_dependency(
        self,
        column: api.Column,
        eval_state: RowwiseEvalState | None,
    ):
        assert eval_state is not None
        index = eval_state.dependency(column)
        return api.Expression.argument(index)

    def eval(self, column: clmn.ColumnWithExpression) -> api.Column:
        return self._column_from_expression(
            column.expression, column_eval_properties(column)
        )

    def _column_from_expression(
        self,
        expression: expr.ColumnExpression,
        eval_properties: api.EvalProperties | None = None,
    ) -> api.Column:
        expression = self.context.expression_with_type(expression)
        if environ.runtime_typechecking:
            expression = TypeVerifier().eval_expression(expression)
        if eval_properties is None:
            eval_properties = api.EvalProperties(dtype=expression._dtype)
        if isinstance(expression, expr.NumbaApplyExpression):
            return self.numba_compile_numbaapply(expression, eval_properties)

        eval_state = RowwiseEvalState()
        api_expression = self.eval_expression(expression, eval_state=eval_state)
        table = self.scope.table(self.output_universe, eval_state.columns)
        return self.scope.expression_column(
            table=table, expression=api_expression, properties=eval_properties
        )

    def numba_compile_numbaapply(
        self,
        expression: expr.NumbaApplyExpression,
        eval_properties: api.EvalProperties | None = None,
    ):
        raise NotImplementedError(
            "numba_apply currently not supported in join or groupby."
        )

    def eval_column_val(
        self,
        expression: expr.ColumnReference,
        eval_state: RowwiseEvalState | None = None,
    ):
        column = self._dereference(expression)
        return self.eval_dependency(column, eval_state=eval_state)

    def eval_unary_op(
        self,
        expression: expr.ColumnUnaryOpExpression,
        eval_state: RowwiseEvalState | None = None,
    ):
        operator_fun = expression._operator
        arg = self.eval_expression(expression._expr, eval_state=eval_state)
        operand_dtype = expression._expr._dtype
        if (
            result_expression := get_unary_expression(arg, operator_fun, operand_dtype)
        ) is not None:
            return result_expression

        return api.Expression.apply(operator_fun, arg)

    def eval_binary_op(
        self,
        expression: expr.ColumnBinaryOpExpression,
        eval_state: RowwiseEvalState | None = None,
    ):
        operator_fun = expression._operator
        left_expression = expression._left
        right_expression = expression._right

        left_dtype = left_expression._dtype
        right_dtype = right_expression._dtype

        if (
            dtype := common_dtype_in_binary_operator(left_dtype, right_dtype)
        ) is not None:
            dtype = dt.wrap(dtype)
            left_dtype = dtype
            right_dtype = dtype
            left_expression = expr.CastExpression(dtype, left_expression)
            right_expression = expr.CastExpression(dtype, right_expression)

        left = self.eval_expression(left_expression, eval_state=eval_state)
        right = self.eval_expression(right_expression, eval_state=eval_state)

        if (
            result_expression := get_binary_expression(
                left,
                right,
                operator_fun,
                left_dtype,
                right_dtype,
            )
        ) is not None:
            return result_expression

        left_dtype_unoptionalized, right_dtype_unoptionalized = dt.unoptionalize_pair(
            left_dtype,
            right_dtype,
        )

        if (
            dtype_and_handler := get_binary_operators_mapping_optionals(
                operator_fun, left_dtype_unoptionalized, right_dtype_unoptionalized
            )
        ) is not None:
            return dtype_and_handler[1](left, right)

        operator_symbol = getattr(
            expression._operator, "_symbol", expression._operator.__name__
        )

        expression_info = get_expression_info(expression)
        if (
            isinstance(left_dtype, dt.Tuple)
            and isinstance(right_dtype, dt.Tuple)
            and expression._operator in tuple_handling_operators
        ):
            warnings.warn(
                f"Pathway does not natively support operator {operator_symbol} "
                + "on Tuple types. "
                + "It refers to the following expression:\n"
                + expression_info
                + "The evaluation will be performed in Python, which may slow down your computations. "
                + "Try specifying the types or expressing the computation differently.",
            )
            return api.Expression.apply(operator_fun, left, right)
        else:
            # this path should be covered by TypeInterpreter
            raise TypeError(
                f"Pathway does not support using binary operator {expression._operator.__name__}"
                + f" on columns of types {left_dtype}, {right_dtype}."
                + "It refers to the following expression:\n"
                + expression_info
            )

    def eval_const(
        self,
        expression: expr.ColumnConstExpression,
        eval_state: RowwiseEvalState | None = None,
    ):
        return api.Expression.const(expression._val)

    def eval_call(
        self,
        expression: expr.ColumnCallExpression,
        eval_state: RowwiseEvalState | None = None,
    ):
        # At this point all column calls should be desugared and gone
        raise RuntimeError("RowwiseEvaluator encountered ColumnCallExpression")

    def eval_apply(
        self,
        expression: expr.ApplyExpression,
        eval_state: RowwiseEvalState | None = None,
    ):
        fun, args = self._prepare_positional_apply(
            fun=expression._fun, args=expression._args, kwargs=expression._kwargs
        )
        return api.Expression.apply(
            fun,
            *(self.eval_expression(arg, eval_state=eval_state) for arg in args),
        )

    def eval_async_apply(
        self,
        expression: expr.AsyncApplyExpression,
        eval_state: RowwiseEvalState | None = None,
    ):
        raise NotImplementedError()

    def eval_numbaapply(
        self,
        expression: expr.NumbaApplyExpression,
        eval_state: RowwiseEvalState | None = None,
    ):
        raise NotImplementedError()

    def eval_cast(
        self,
        expression: expr.CastExpression,
        eval_state: RowwiseEvalState | None = None,
    ):
        arg = self.eval_expression(expression._expr, eval_state=eval_state)
        source_type = expression._expr._dtype
        target_type = expression._return_type

        if (
            dt.dtype_equivalence(target_type, source_type)
            or dt.dtype_equivalence(dt.Optional(source_type), target_type)
            or (source_type == dt.NONE and isinstance(target_type, dt.Optional))
            or target_type == dt.ANY
        ):
            return arg  # then cast is noop
        if (
            result_expression := get_cast_operators_mapping(
                arg, source_type, target_type
            )
        ) is not None:
            return result_expression

        raise TypeError(
            f"Pathway doesn't support casting {source_type} to {target_type}."
        )

    def eval_convert(
        self,
        expression: expr.ConvertExpression,
        eval_state: RowwiseEvalState | None = None,
    ):
        arg = self.eval_expression(expression._expr, eval_state=eval_state)
        source_type = expression._expr._dtype
        target_type = expression._return_type

        if (
            dt.dtype_equivalence(target_type, source_type)
            or dt.dtype_equivalence(dt.Optional(source_type), target_type)
            or (source_type == dt.NONE and isinstance(target_type, dt.Optional))
            or target_type == dt.ANY
        ):
            return arg
        if (
            result_expression := get_convert_operators_mapping(
                arg, source_type, target_type
            )
        ) is not None:
            return result_expression

        raise TypeError(
            f"Pathway doesn't support converting {source_type} to {target_type}."
        )

    def eval_declare(
        self,
        expression: expr.DeclareTypeExpression,
        eval_state: RowwiseEvalState | None = None,
    ):
        return self.eval_expression(expression._expr, eval_state=eval_state)

    def eval_coalesce(
        self,
        expression: expr.CoalesceExpression,
        eval_state: RowwiseEvalState | None = None,
    ):
        dtype = self.expression_type(expression)
        args: list[api.Expression] = []
        for expr_arg in expression._args:
            if not isinstance(dtype, dt.Optional) and isinstance(
                expr_arg._dtype, dt.Optional
            ):
                arg_dtype = dt.Optional(dtype)
            else:
                arg_dtype = dtype
            arg_expr = expr.CastExpression(arg_dtype, expr_arg)
            args.append(self.eval_expression(arg_expr, eval_state=eval_state))

        res = args[-1]
        for arg in reversed(args[:-1]):
            res = api.Expression.if_else(api.Expression.is_none(arg), res, arg)

        return res

    def eval_require(
        self,
        expression: expr.RequireExpression,
        eval_state: RowwiseEvalState | None = None,
    ):
        val = self.eval_expression(expression._val, eval_state=eval_state)
        args = [
            self.eval_expression(arg, eval_state=eval_state) for arg in expression._args
        ]

        res = val
        for arg in reversed(args):
            res = api.Expression.if_else(
                api.Expression.is_none(arg),
                api.Expression.const(None),
                res,
            )

        return res

    def eval_ifelse(
        self,
        expression: expr.IfElseExpression,
        eval_state: RowwiseEvalState | None = None,
    ):
        dtype = expression._dtype
        if_ = self.eval_expression(expression._if, eval_state=eval_state)
        then = self.eval_expression(
            expr.CastExpression(dtype, expression._then),
            eval_state=eval_state,
        )
        else_ = self.eval_expression(
            expr.CastExpression(dtype, expression._else),
            eval_state=eval_state,
        )

        return api.Expression.if_else(if_, then, else_)

    def eval_not_none(
        self,
        expression: expr.IsNotNoneExpression,
        eval_state: RowwiseEvalState | None = None,
    ):
        return api.Expression.unary_expression(
            api.Expression.is_none(
                self.eval_expression(expression._expr, eval_state=eval_state)
            ),
            api.UnaryOperator.INV,
            api.PathwayType.BOOL,
        )

    def eval_none(
        self,
        expression: expr.IsNoneExpression,
        eval_state: RowwiseEvalState | None = None,
    ):
        return api.Expression.is_none(
            self.eval_expression(expression._expr, eval_state=eval_state)
        )

    def eval_reducer(
        self,
        expression: expr.ReducerExpression,
        eval_state: RowwiseEvalState | None = None,
    ):
        raise RuntimeError("RowwiseEvaluator encountered ReducerExpression")

    def eval_count(
        self,
        expression: expr.CountExpression,
        eval_state: RowwiseEvalState | None = None,
    ):
        raise RuntimeError("RowwiseEvaluator encountered CountExpression")

    def eval_pointer(
        self,
        expression: expr.PointerExpression,
        eval_state: RowwiseEvalState | None = None,
    ):
        expressions = [
            self.eval_expression(
                arg,
                eval_state=eval_state,
            )
            for arg in expression._args
        ]
        optional = expression._optional
        return api.Expression.pointer_from(*expressions, optional=optional)

    def eval_make_tuple(
        self,
        expression: expr.MakeTupleExpression,
        eval_state: RowwiseEvalState | None = None,
    ):
        expressions = [
            self.eval_expression(arg, eval_state=eval_state) for arg in expression._args
        ]
        return api.Expression.make_tuple(*expressions)

    def eval_get(
        self,
        expression: expr.GetExpression,
        eval_state: RowwiseEvalState | None = None,
    ):
        object = self.eval_expression(expression._object, eval_state=eval_state)
        index = self.eval_expression(expression._index, eval_state=eval_state)
        default = self.eval_expression(expression._default, eval_state=eval_state)
        object_dtype = expression._object._dtype

        if object_dtype == dt.JSON:
            if expression._check_if_exists:
                return api.Expression.json_get_item_checked(object, index, default)
            else:
                return api.Expression.json_get_item_unchecked(object, index)
        else:
            assert not object_dtype.equivalent_to(dt.Optional(dt.JSON))
            if expression._check_if_exists:
                return api.Expression.sequence_get_item_checked(object, index, default)
            else:
                return api.Expression.sequence_get_item_unchecked(object, index)

    def eval_method_call(
        self,
        expression: expr.MethodCallExpression,
        eval_state: RowwiseEvalState | None = None,
    ):
        dtypes = tuple([arg._dtype for arg in expression._args])
        if (dtypes_and_handler := expression.get_function(dtypes)) is not None:
            new_dtypes, _, handler = dtypes_and_handler

            expressions = [
                self.eval_expression(
                    expr.CastExpression(dtype, arg),
                    eval_state=eval_state,
                )
                for dtype, arg in zip(new_dtypes, expression._args)
            ]
            return handler(*expressions)
        raise AttributeError(
            f"Column of type {dtypes[0]} has no attribute {expression._name}."
        )

    def eval_unwrap(
        self,
        expression: expr.UnwrapExpression,
        eval_state: RowwiseEvalState | None = None,
    ):
        val = self.eval_expression(expression._expr, eval_state=eval_state)
        return api.Expression.unwrap(val)

    def _dereference(self, expression: expr.ColumnReference):
        column = self.state.get_column(expression._column)
        if expression._column.universe == self.context.universe:
            return column
        elif self.context.universe.is_equal_to(expression._column.universe):
            return self.scope.override_column_universe(self.output_universe, column)
        else:
            assert self.context.universe.is_subset_of(expression._column.universe)
            return self.scope.restrict_column(self.output_universe, column)

    def _prepare_positional_apply(
        self,
        fun: Callable,
        args: tuple[expr.ColumnExpression, ...],
        kwargs: dict[str, expr.ColumnExpression],
    ) -> tuple[Callable, tuple[expr.ColumnExpression, ...]]:
        if kwargs:
            args_len = len(args)
            kwarg_names = list(kwargs.keys())

            def wrapped(*all_values):
                arg_values = all_values[:args_len]
                kwarg_values = dict(zip(kwarg_names, all_values[args_len:]))
                return fun(*arg_values, **kwarg_values)

            return wrapped, (*args, *kwargs.values())
        else:
            return fun, args


class AutotuplingRowwiseEvalState:
    _dependencies: dict[clmn.Column, int]
    _storages: dict[Storage, api.Table]

    def __init__(self) -> None:
        self._dependencies = {}
        self._storages = {}

    def dependency(self, column: clmn.Column) -> int:
        return self._dependencies.setdefault(column, len(self._dependencies))

    @property
    def columns(self) -> list[clmn.Column]:
        return list(self._dependencies.keys())

    def get_temporary_table(self, storage: Storage) -> api.Table:
        return self._storages[storage]

    def set_temporary_table(self, storage: Storage, table: api.Table) -> None:
        self._storages[storage] = table

    @property
    def storages(self) -> list[Storage]:
        return list(self._storages.keys())


class AutotuplingRowwiseEvaluator(RowwiseEvaluator, context_type=None):
    def run(
        self,
        output_storage: Storage,
        *input_storages: Storage,
        old_path: ColumnPath | None = ColumnPath.EMPTY,
    ) -> api.Table:
        [input_storage] = input_storages
        engine_input_table = self.state.get_table(input_storage)

        expressions = []
        eval_state = AutotuplingRowwiseEvalState()
        if old_path is not None:  # keep old columns if they are needed
            placeholder_column = clmn.MaterializedColumn(
                self.context.universe, ColumnProperties(dtype=dt.ANY)
            )
            expressions.append(
                (
                    self.eval_dependency(placeholder_column, eval_state=eval_state),
                    api.EvalProperties(dtype=placeholder_column.dtype),
                )
            )
            input_storage = input_storage.with_updated_paths(
                {placeholder_column: old_path}
            )

        for column in output_storage.get_columns():
            if input_storage.has_column(column):
                continue
            assert isinstance(column, clmn.ColumnWithExpression)
            expression = column.expression
            expression = self.context.expression_with_type(expression)
            eval_properties = column_eval_properties(column)

            engine_expression = self.eval_expression(expression, eval_state=eval_state)  # type: ignore
            expressions.append((engine_expression, eval_properties))

        # START temporary solution for eval_async_apply
        for intermediate_storage in eval_state.storages:
            [column] = intermediate_storage.get_columns()
            engine_input_table = self.scope.override_table_universe(
                eval_state.get_temporary_table(intermediate_storage),
                engine_input_table,
            )
            input_storage = Storage.merge_storages(
                self.context.universe, input_storage, intermediate_storage
            )
        # END temporary solution for eval_async_apply

        paths = [input_storage.get_path(dep) for dep in eval_state.columns]
        return self.scope.expression_table(engine_input_table, paths, expressions)

    def eval_dependency(
        self,
        column: clmn.Column,  # type: ignore
        eval_state: AutotuplingRowwiseEvalState | None,  # type: ignore
    ):
        assert eval_state is not None
        index = eval_state.dependency(column)
        return api.Expression.argument(index)

    def run_subexpressions(
        self,
        expressions: Iterable[expr.ColumnExpression],
    ) -> tuple[list[clmn.Column], Storage, api.Table]:
        output_columns: list[clmn.Column] = []

        eval_state = AutotuplingRowwiseEvalState()
        for expression in expressions:
            self.eval_expression(expression, eval_state=eval_state)  # type: ignore

            output_column = clmn.ColumnWithExpression(
                self.context, self.context.universe, expression
            )
            output_columns.append(output_column)

        output_storage = Storage.flat(self.context.universe, output_columns)
        input_storage = self.state.get_storage(self.context.universe)
        engine_output_table = self.run(output_storage, input_storage, old_path=None)
        return (output_columns, output_storage, engine_output_table)

    def eval_column_val(
        self,
        expression: expr.ColumnReference,
        eval_state: AutotuplingRowwiseEvalState | None = None,  # type: ignore
    ):
        return self.eval_dependency(expression._column, eval_state=eval_state)

    def eval_async_apply(
        self,
        expression: expr.AsyncApplyExpression,
        eval_state: AutotuplingRowwiseEvalState | None = None,  # type: ignore
    ):
        fun, args = self._prepare_positional_apply(
            fun=asynchronous.coerce_async(expression._fun),
            args=expression._args,
            kwargs=expression._kwargs,
        )

        columns, input_storage, engine_input_table = self.run_subexpressions(args)
        paths = [input_storage.get_path(column) for column in columns]
        engine_table = self.scope.async_apply_table(
            engine_input_table,
            paths,
            fun,
            properties=api.EvalProperties(
                dtype=expression._dtype,
                trace=expression._trace.to_engine(),
            ),
        )
        tmp_column = clmn.MaterializedColumn(
            self.context.universe, ColumnProperties(dtype=expression._dtype)
        )
        output_storage = Storage.flat(self.context.universe, [tmp_column])
        assert eval_state is not None
        eval_state.set_temporary_table(output_storage, engine_table)
        return self.eval_dependency(tmp_column, eval_state=eval_state)

    def eval_numbaapply(
        self,
        expression: expr.NumbaApplyExpression,
        eval_state: AutotuplingRowwiseEvalState | None = None,  # type: ignore
    ):
        assert not expression._kwargs
        return api.Expression.unsafe_numba_apply(
            expression._fun,
            *(
                self.eval_expression(arg, eval_state=eval_state)  # type: ignore
                for arg in expression._args
            ),
        )


class TableRestrictedRowwiseEvaluator(
    RowwiseEvaluator, context_type=clmn.TableRestrictedRowwiseContext
):
    context: clmn.TableRestrictedRowwiseContext

    def _dereference(self, expression: expr.ColumnReference):
        if expression.table != self.context.table:
            raise ValueError("invalid expression in restricted context")
        return super()._dereference(expression)


class FilterEvaluator(ExpressionEvaluator, context_type=clmn.FilterContext):
    context: clmn.FilterContext

    def eval(self, column: clmn.ColumnWithExpression) -> api.Column:
        raise NotImplementedError()

    @property
    def output_universe(self) -> api.Universe:
        raise NotImplementedError()

    def run(self, output_storage: Storage, *input_storages: Storage) -> api.Table:
        [input_storage] = input_storages
        filtering_column_path = input_storage.get_path(self.context.filtering_column)
        return self.scope.filter_table(
            self.state.get_table(input_storage), filtering_column_path
        )


class ForgetEvaluator(ExpressionEvaluator, context_type=clmn.ForgetContext):
    context: clmn.ForgetContext

    def eval(self, column: clmn.ColumnWithExpression) -> api.Column:
        raise NotImplementedError()

    @property
    def output_universe(self) -> api.Universe:
        raise NotImplementedError()

    def run(self, output_storage: Storage, *input_storages: Storage) -> api.Table:
        [input_storage] = input_storages
        threshold_column_path = input_storage.get_path(self.context.threshold_column)
        time_column_path = input_storage.get_path(self.context.time_column)

        return self.scope.forget(
            self.state.get_table(input_storage),
            threshold_column_path,
            time_column_path,
        )


class FreezeEvaluator(ExpressionEvaluator, context_type=clmn.FreezeContext):
    context: clmn.FreezeContext

    def eval(self, column: clmn.ColumnWithExpression) -> api.Column:
        raise NotImplementedError()

    @property
    def output_universe(self) -> api.Universe:
        raise NotImplementedError()

    def run(self, output_storage: Storage, *input_storages: Storage) -> api.Table:
        [input_storage] = input_storages
        threshold_column_path = input_storage.get_path(self.context.threshold_column)
        time_column_path = input_storage.get_path(self.context.time_column)

        return self.scope.freeze(
            self.state.get_table(input_storage),
            threshold_column_path,
            time_column_path,
        )


class BufferEvaluator(ExpressionEvaluator, context_type=clmn.BufferContext):
    context: clmn.BufferContext

    def eval(self, column: clmn.ColumnWithExpression) -> api.Column:
        raise NotImplementedError()

    @property
    def output_universe(self) -> api.Universe:
        raise NotImplementedError()

    def run(self, output_storage: Storage, *input_storages: Storage) -> api.Table:
        [input_storage] = input_storages
        threshold_column_path = input_storage.get_path(self.context.threshold_column)
        time_column_path = input_storage.get_path(self.context.time_column)

        return self.scope.buffer(
            self.state.get_table(input_storage),
            threshold_column_path,
            time_column_path,
        )


class IntersectEvaluator(ExpressionEvaluator, context_type=clmn.IntersectContext):
    context: clmn.IntersectContext

    def eval(self, column: clmn.ColumnWithExpression) -> api.Column:
        raise NotImplementedError()

    @cached_property
    def output_universe(self) -> api.Universe:
        raise NotImplementedError()

    def run(self, output_storage: Storage, *input_storages: Storage) -> api.Table:
        engine_tables = [self.state.get_table(storage) for storage in input_storages]
        return self.scope.intersect_tables(engine_tables[0], engine_tables[1:])


class RestrictEvaluator(ExpressionEvaluator, context_type=clmn.RestrictContext):
    context: clmn.RestrictContext

    def eval(self, column: clmn.ColumnWithExpression) -> api.Column:
        raise NotImplementedError()

    def run(self, output_storage: Storage, *input_storages: Storage) -> api.Table:
        engine_tables = [self.state.get_table(storage) for storage in input_storages]
        [orig_universe_storage, new_universe_storage] = engine_tables
        return self.scope.restrict_table(orig_universe_storage, new_universe_storage)


class DifferenceEvaluator(ExpressionEvaluator, context_type=clmn.DifferenceContext):
    context: clmn.DifferenceContext

    def eval(self, column: clmn.ColumnWithExpression) -> api.Column:
        raise NotImplementedError()

    @cached_property
    def output_universe(self) -> api.Universe:
        raise NotImplementedError()

    def run(self, output_storage: Storage, *input_storages: Storage) -> api.Table:
        [left_storage, right_storage] = input_storages
        return self.scope.subtract_table(
            self.state.get_table(left_storage), self.state.get_table(right_storage)
        )


class ReindexEvaluator(ExpressionEvaluator, context_type=clmn.ReindexContext):
    context: clmn.ReindexContext

    def eval(self, column: clmn.ColumnWithExpression) -> api.Column:
        raise NotImplementedError()

    @cached_property
    def output_universe(self) -> api.Universe:
        raise NotImplementedError()

    def run(self, output_storage: Storage, *input_storages: Storage) -> api.Table:
        [input_storage] = input_storages
        reindexing_column_path = input_storage.get_path(self.context.reindex_column)
        return self.scope.reindex_table(
            self.state.get_table(input_storage), reindexing_column_path
        )


class IxEvaluator(ExpressionEvaluator, context_type=clmn.IxContext):
    context: clmn.IxContext
    key_column: api.Column
    input_universe: api.Universe
    ixer: api.Ixer

    def _initialize_from_context(self):
        self.key_column = self.state.get_column(self.context.key_column)
        self.input_universe = self.state.get_universe(self.context.orig_universe)
        self.ixer = self.scope.ix(
            self.key_column,
            self.input_universe,
            strict=True,
            optional=self.context.optional,
        )

    def eval(self, column: clmn.ColumnWithExpression) -> api.Column:
        column_to_ix = self.state.get_column(column.dereference())
        return self.ixer.ix_column(column_to_ix)


class PromiseSameUniverseEvaluator(
    ExpressionEvaluator, context_type=clmn.PromiseSameUniverseContext
):
    def eval(self, column: clmn.ColumnWithExpression) -> api.Column:
        raise NotImplementedError()

    def run(self, output_storage: Storage, *input_storages: Storage) -> api.Table:
        engine_tables = [self.state.get_table(storage) for storage in input_storages]
        [orig_universe_storage, new_universe_storage] = engine_tables
        return self.scope.override_table_universe(
            orig_universe_storage, new_universe_storage
        )


class HavingEvaluator(ExpressionEvaluator, context_type=clmn.HavingContext):
    context: clmn.HavingContext
    ixer: api.Ixer

    def _initialize_from_context(self):
        keys_col: api.Column = self.state.get_column(self.context.key_column)
        orig_universe: api.Universe = self.state.get_universe(
            self.context.orig_universe
        )
        self.ixer = self.scope.ix(keys_col, orig_universe, strict=False, optional=False)

    def eval(self, column: clmn.ColumnWithExpression) -> api.Column:
        assert isinstance(column.expression, expr.ColumnReference)
        col_to_ix = self.state.get_column(column.dereference())
        ret = self.ixer.ix_column(col_to_ix)
        return ret

    @cached_property
    def output_universe(self) -> api.Universe:
        return self.ixer.universe


class JoinEvaluator(RowwiseEvaluator, context_type=clmn.JoinContext):
    context: clmn.JoinContext

    @cached_property
    def output_universe(self) -> api.Universe:
        raise NotImplementedError()

    def get_join_storage(
        self,
        universe: univ.Universe,
        left_input_storage: Storage,
        right_input_storage: Storage,
    ) -> Storage:
        left_id_storage = Storage(
            self.context.left_table._universe,
            {
                self.context.left_table._id_column: ColumnPath.EMPTY,
            },
        )
        right_id_storage = Storage(
            self.context.right_table._universe,
            {
                self.context.right_table._id_column: ColumnPath.EMPTY,
            },
        )
        return Storage.merge_storages(
            universe,
            left_id_storage,
            left_input_storage.restrict_to_table(self.context.left_table),
            right_id_storage,
            right_input_storage.restrict_to_table(self.context.right_table),
        )

    def run_join(self, universe: univ.Universe, *input_storages: Storage) -> Storage:
        [left_input_storage, right_input_storage] = input_storages
        output_storage = self.get_join_storage(
            universe, left_input_storage, right_input_storage
        )
        left_paths = [
            left_input_storage.get_path(column)
            for column in self.context.on_left.columns
        ]
        right_paths = [
            right_input_storage.get_path(column)
            for column in self.context.on_right.columns
        ]
        output_engine_table = self.scope.join_tables(
            self.state.get_table(left_input_storage),
            self.state.get_table(right_input_storage),
            left_paths,
            right_paths,
            self.context.assign_id,
            self.context.left_ear,
            self.context.right_ear,
        )
        self.state.set_table(output_storage, output_engine_table)

        return output_storage

    def run(self, output_storage: Storage, *input_storages: Storage) -> api.Table:
        joined_storage = self.run_join(self.context.universe, *input_storages)
        rowwise_evaluator = AutotuplingRowwiseEvaluator(
            clmn.RowwiseContext(self.context.universe),
            self.scope,
            self.state,
            self.scope_context,
        )
        if self.context.assign_id:
            old_path = ColumnPath((1,))
        else:
            old_path = None
        return rowwise_evaluator.run(output_storage, joined_storage, old_path=old_path)


class JoinRowwiseEvaluator(RowwiseEvaluator, context_type=clmn.JoinRowwiseContext):
    context: clmn.JoinRowwiseContext


class AutotuplingJoinRowwiseEvaluator(AutotuplingRowwiseEvaluator, context_type=""):
    context: clmn.JoinRowwiseContext


class GroupedEvaluator(RowwiseEvaluator, context_type=clmn.GroupedContext):
    context: clmn.GroupedContext
    grouper: api.Grouper

    def _initialize_from_context(self):
        universe = self.state.get_universe(self.context.inner_context.universe)
        table = api.LegacyTable(
            universe, self.state.get_columns(self.context.grouping_columns.values())
        )
        requested_columns = self.state.get_columns(
            self.context.grouping_columns[col]
            for col in self.context.requested_grouping_columns
            if not self.scope_context.skip_column(col.to_column())
        )
        self.grouper = self.scope.group_by(
            table, requested_columns, self.context.set_id
        )

    def _dereference(self, expression: expr.ColumnReference):
        input_column = self.context.grouping_columns.get(
            expression._to_original_internal()
        )
        if input_column is None:
            return super()._dereference(expression)
        else:
            return self.grouper.input_column(self.state.get_column(input_column))

    def eval_count(
        self,
        expression: expr.CountExpression,
        eval_state: RowwiseEvalState | None = None,
    ):
        column = self.grouper.count_column()
        return self.eval_dependency(column, eval_state=eval_state)

    def eval_reducer(
        self,
        expression: expr.ReducerExpression,
        eval_state: RowwiseEvalState | None = None,
    ):
        args = expression._args + expression._reducer.additional_args_from_context(
            self.context
        )

        context = self.context.table._context
        input_columns = [self.context.table._eval(arg, context) for arg in args]
        arg_columns = [
            RowwiseEvaluator(context, self.scope, self.state, self.scope_context).eval(
                input_column
            )
            for input_column in input_columns
        ]

        engine_reducer = expression._reducer.engine_reducer(
            [arg._dtype for arg in expression._args]
        )
        column = self.grouper.reducer_column(engine_reducer, arg_columns)
        return self.eval_dependency(column, eval_state=eval_state)

    @cached_property
    def output_universe(self) -> api.Universe:
        return self.grouper.universe


class UpdateRowsEvaluator(ExpressionEvaluator, context_type=clmn.UpdateRowsContext):
    context: clmn.UpdateRowsContext

    def eval(self, column: clmn.ColumnWithExpression) -> api.Column:
        raise NotImplementedError()

    @cached_property
    def output_universe(self) -> api.Universe:
        raise NotImplementedError()

    def run(self, output_storage: Storage, *input_storages: Storage) -> api.Table:
        engine_input_tables = self.flatten_tables(output_storage, *input_storages)
        [input_table, update_input_table] = engine_input_tables
        return self.scope.update_rows_table(input_table, update_input_table)


class UpdateCellsEvaluator(UpdateRowsEvaluator, context_type=clmn.UpdateCellsContext):
    context: clmn.UpdateCellsContext

    def eval(self, column: clmn.ColumnWithExpression) -> api.Column:
        raise NotImplementedError()

    @property
    def output_universe(self) -> api.Universe:
        raise NotImplementedError()

    def run(self, output_storage: Storage, *input_storages: Storage) -> api.Table:
        if len(input_storages) == 1:
            input_storage, update_input_storage = input_storages[0], input_storages[0]
        else:
            [input_storage, update_input_storage] = input_storages
        paths = []
        update_paths = []

        for column in output_storage.get_columns():
            if column in input_storage.get_columns():
                continue
            assert isinstance(column, clmn.ColumnWithReference)
            if column.expression.name in self.context.updates:
                paths.append(input_storage.get_path(column.expression._column))
                update_paths.append(
                    update_input_storage.get_path(
                        self.context.updates[column.expression.name]
                    )
                )

        return self.scope.update_cells_table(
            self.state.get_table(input_storage),
            self.state.get_table(update_input_storage),
            paths,
            update_paths,
        )


class ConcatUnsafeEvaluator(ExpressionEvaluator, context_type=clmn.ConcatUnsafeContext):
    context: clmn.ConcatUnsafeContext

    def eval(self, column: clmn.ColumnWithExpression) -> api.Column:
        raise NotImplementedError()

    @cached_property
    def output_universe(self) -> api.Universe:
        raise NotImplementedError()

    def run(self, output_storage: Storage, *input_storages: Storage) -> api.Table:
        engine_input_tables = self.flatten_tables(output_storage, *input_storages)
        engine_table = self.scope.concat_tables(engine_input_tables)
        return engine_table


class FlattenEvaluator(ExpressionEvaluator, context_type=clmn.FlattenContext):
    context: clmn.FlattenContext

    def eval(self, column: clmn.ColumnWithExpression) -> api.Column:
        raise NotImplementedError()

    @cached_property
    def output_universe(self) -> api.Universe:
        raise NotImplementedError()

    def run(self, output_storage: Storage, *input_storages: Storage) -> api.Table:
        [input_storage] = input_storages
        flatten_column_path = input_storage.get_path(self.context.flatten_column)
        return self.scope.flatten_table(
            self.state.get_table(input_storage), flatten_column_path
        )


class SortingEvaluator(ExpressionEvaluator, context_type=clmn.SortingContext):
    context: clmn.SortingContext

    def eval(self, column: clmn.ColumnWithExpression) -> api.Column:
        raise NotImplementedError()

    def run(self, output_storage: Storage, *input_storages: Storage) -> api.Table:
        [input_storage] = input_storages
        key_column_path = input_storage.get_path(self.context.key_column)
        instance_column_path = input_storage.get_path(self.context.instance_column)
        return self.scope.sort_table(
            self.state.get_table(input_storage), key_column_path, instance_column_path
        )
