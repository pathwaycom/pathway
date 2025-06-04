# Copyright © 2024 Pathway

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING, ClassVar

from pathway.engine import ExternalIndexData, ExternalIndexQuery
from pathway.internals import (
    api,
    column as clmn,
    dtype as dt,
    expression as expr,
    universe as univ,
)
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
    get_convert_operator,
    get_unary_expression,
)
from pathway.internals.schema import schema_from_types
from pathway.internals.udfs import udf

if TYPE_CHECKING:
    from pathway.internals.graph_runner.state import ScopeState


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
        assert isinstance(context, self.context_type)
        self._initialize_from_context()

    def __init_subclass__(cls, /, context_type, **kwargs):
        super().__init_subclass__(**kwargs)
        cls.context_type = context_type
        assert context_type not in cls._context_map
        cls._context_map[context_type] = cls

    def _initialize_from_context(self):
        pass

    @classmethod
    def for_context(cls, context: clmn.Context) -> type[ExpressionEvaluator]:
        return cls._context_map[type(context)]

    def column_properties(self, column: clmn.Column) -> api.ColumnProperties:
        props = column.properties
        return api.ColumnProperties(
            trace=column.trace.to_engine(),
            dtype=props.dtype.to_engine(),
            append_only=props.append_only,
        )

    def expression_type(self, expression: expr.ColumnExpression) -> dt.DType:
        return self.context.expression_type(expression)

    @abstractmethod
    def run(self, output_storage: Storage) -> api.Table:
        raise NotImplementedError()

    def maybe_flatten_table(
        self,
        output_storage: Storage,
    ) -> api.Table:
        input_storage = self.state.get_storage(output_storage._universe)
        table = self.state.get_table(output_storage._universe)
        if output_storage is input_storage:
            return table
        assert output_storage.is_flat
        paths = []
        for i, column in enumerate(output_storage.get_columns()):
            assert output_storage.get_path(column) == ColumnPath((i,))
            paths.append(input_storage.get_path(column))
        return self.scope.flatten_table_storage(table, paths)

    def flatten_table_storage_if_needed(self, output_storage: Storage):
        if output_storage.flattened_output is not None:
            flattened_storage = self.maybe_flatten_table(
                output_storage.flattened_output
            )
            self.state.set_table(output_storage.flattened_output, flattened_storage)

    def maybe_flatten_tables(self, output_storage: Storage) -> tuple[api.Table, ...]:
        engine_input_tables = []
        for flattened_storage in output_storage.maybe_flattened_inputs.values():
            flattened_engine_storage = self.maybe_flatten_table(flattened_storage)
            engine_input_tables.append(flattened_engine_storage)
        return tuple(engine_input_tables)

    def _table_properties(self, storage: Storage) -> api.TableProperties:
        properties = []
        for column in storage.get_columns():
            if not isinstance(column, clmn.ExternalMaterializedColumn):
                properties.append(
                    (storage.get_path(column), self.column_properties(column))
                )
        return api.TableProperties.from_column_properties(
            properties, self.context.trace.to_engine()
        )


class RowwiseEvalState:
    _dependencies: dict[clmn.Column, int]
    _storages: dict[Storage, api.Table]
    _deterministic: bool
    _locally_deterministic: bool

    def __init__(self) -> None:
        self._dependencies = {}
        self._storages = {}
        self._deterministic = True
        self._locally_deterministic = True

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

    def set_non_deterministic(self) -> None:
        self._locally_deterministic = False
        self._deterministic = False

    @property
    def deterministic(self) -> bool:
        return self._deterministic

    @property
    def locally_deterministic(self) -> bool:
        return self._locally_deterministic

    def reset_locally_deterministic(self) -> None:
        self._locally_deterministic = True


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

        from pathway.internals.operator import RowTransformerOperator

        if isinstance(expression, expr.ColumnReference):
            if isinstance(
                expression._column.lineage.source.operator, RowTransformerOperator
            ):
                return expression

        dtype = expression._dtype

        @udf(return_type=dtype, deterministic=True)
        def test_type(val):
            if not dtype.is_value_compatible(val):
                raise TypeError(f"Value {val} is not of type {dtype}.")
            return val

        ret = test_type(expression)
        assert isinstance(ret, expr.ApplyExpression)
        ret._check_for_disallowed_types = False
        ret._dtype = dtype

        return ret


class RowwiseEvaluator(
    ExpressionEvaluator, ExpressionVisitor, context_type=clmn.RowwiseContext
):
    def run(
        self,
        output_storage: Storage,
        old_path: ColumnPath | None = ColumnPath.EMPTY,
        disable_runtime_typechecking: bool = False,
    ) -> api.Table:
        input_storage = self.state.get_storage(self.context.universe)
        engine_input_table = self.state.get_table(input_storage._universe)
        if output_storage.has_only_references:
            return engine_input_table

        expressions = []
        eval_state = RowwiseEvalState()

        if (
            old_path is not None and not output_storage.has_only_new_columns
        ):  # keep old columns if they are needed
            placeholder_column = clmn.MaterializedColumn(
                self.context.universe,
                ColumnProperties(dtype=dt.ANY, append_only=input_storage.append_only),
            )
            expressions.append(
                api.ExpressionData(
                    self.eval_dependency(placeholder_column, eval_state=eval_state),
                    self.scope.table_properties(engine_input_table, old_path),
                    append_only=input_storage.append_only,
                    deterministic=True,
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
            if (
                self.scope_context.runtime_typechecking
                and not disable_runtime_typechecking
            ):
                expression = TypeVerifier().eval_expression(expression)
            properties = api.TableProperties.column(self.column_properties(column))

            eval_state.reset_locally_deterministic()
            engine_expression = self.eval_expression(expression, eval_state=eval_state)
            append_only = column.properties.append_only
            deterministic = eval_state.locally_deterministic
            assert ColumnPath((len(expressions),)) == output_storage.get_path(column)
            expressions.append(
                api.ExpressionData(
                    engine_expression, properties, append_only, deterministic
                )
            )

        # START temporary solution for eval_async_apply
        for intermediate_storage in eval_state.storages:
            properties = self._table_properties(intermediate_storage)
            # restrict instead of override because of edge case in fully async UDF
            # with missing rows.
            engine_input_table = self.scope.restrict_table(
                engine_input_table,
                eval_state.get_temporary_table(intermediate_storage),
                properties,
            )
            input_storage = Storage.merge_storages(
                self.context.universe, intermediate_storage, input_storage
            )
        # END temporary solution for eval_async_apply

        paths = [input_storage.get_path(dep) for dep in eval_state.columns]

        return self.scope.expression_table(
            engine_input_table,
            paths,
            expressions,
            eval_state.deterministic or input_storage.append_only,
        )

    def run_subexpressions(
        self,
        expressions: Iterable[expr.ColumnExpression],
    ) -> tuple[list[clmn.Column], Storage, api.Table]:
        output_columns: list[clmn.Column] = []

        eval_state = RowwiseEvalState()
        for expression in expressions:
            self.eval_expression(expression, eval_state=eval_state)

            output_column = clmn.ColumnWithExpression(
                self.context, self.context.universe, expression
            )
            output_columns.append(output_column)

        output_storage = Storage.flat(self.context.universe, output_columns)
        engine_output_table = self.run(
            output_storage, old_path=None, disable_runtime_typechecking=True
        )  # checks already added in the main call to run
        return (output_columns, output_storage, engine_output_table)

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
        column: clmn.Column,
        eval_state: RowwiseEvalState | None,
    ):
        assert eval_state is not None
        index = eval_state.dependency(column)
        return api.Expression.argument(index)

    def eval_column_val(
        self,
        expression: expr.ColumnReference,
        eval_state: RowwiseEvalState | None = None,
    ):
        return self.eval_dependency(expression._column, eval_state=eval_state)

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

        return api.Expression.apply(
            operator_fun, arg, dtype=expression._dtype.to_engine()
        )

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

        expression_info = get_expression_info(expression)
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
        return api.Expression.const(expression._val, expression._dtype.to_engine())

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
        if not expression._deterministic:
            assert eval_state is not None
            eval_state.set_non_deterministic()
        return api.Expression.apply(
            fun,
            *(self.eval_expression(arg, eval_state=eval_state) for arg in args),
            propagate_none=expression._propagate_none,
            dtype=expression._dtype.to_engine(),
            max_batch_size=expression._max_batch_size,
        )

    def eval_async_apply(
        self,
        expression: expr.AsyncApplyExpression,
        eval_state: RowwiseEvalState | None = None,
    ):
        fun, args = self._prepare_positional_apply(
            fun=expression._fun,
            args=expression._args,
            kwargs=expression._kwargs,
        )

        columns, input_storage, engine_input_table = self.run_subexpressions(args)
        tmp_column = clmn.MaterializedColumn(
            self.context.universe, ColumnProperties(dtype=expression._dtype)
        )
        output_storage = Storage.flat(self.context.universe, [tmp_column])
        paths = [input_storage.get_path(column) for column in columns]
        engine_table = self.scope.async_apply_table(
            engine_input_table,
            paths,
            fun,
            expression._propagate_none,
            expression._deterministic or input_storage.append_only,
            self._table_properties(output_storage),
            expression._dtype.to_engine(),
        )

        assert eval_state is not None
        eval_state.set_temporary_table(output_storage, engine_table)
        return self.eval_dependency(tmp_column, eval_state=eval_state)

    def eval_fully_async_apply(
        self,
        expression: expr.FullyAsyncApplyExpression,
        eval_state: RowwiseEvalState | None = None,
    ):
        fun, args = self._prepare_positional_apply(
            fun=expression._fun,
            args=expression._args,
            kwargs=expression._kwargs,
        )

        columns, input_storage, engine_input_table = self.run_subexpressions(args)
        tmp_column = clmn.MaterializedColumn(
            self.context.universe, ColumnProperties(dtype=expression._dtype)
        )
        status_column = clmn.MaterializedColumn(
            self.context.universe, ColumnProperties(dt.STR)
        )
        from pathway.stdlib.utils.async_transformer import (
            _ASYNC_STATUS_COLUMN,
            _BaseAsyncTransformer,
        )

        output_columns = {
            "result": tmp_column,
            _ASYNC_STATUS_COLUMN: status_column,
        }
        schema = schema_from_types(result=expression._dtype)

        class Transformer(_BaseAsyncTransformer, output_schema=schema):
            async def invoke(self, **kwargs) -> dict:
                args = []
                for i, (name, arg) in enumerate(kwargs.items()):
                    if arg is api.ERROR:
                        return dict(result=api.ERROR)
                    if arg is None and expression._propagate_none:
                        return dict(result=None)
                    assert f"{i}" == name
                    args.append(arg)
                try:
                    return dict(result=await fun(*args))
                except (
                    Exception
                ):  # FIXME: remove when AsyncTransformer returns `api.ERROR` for failure
                    self._connector._logger.error(
                        "Exception in fully_async_udf:", exc_info=True
                    )
                    return dict(result=api.ERROR)

        transformer = Transformer(expression.autocommit_duration_ms)

        ordered_output_columns = [
            output_columns[name]
            for name in transformer.wrapped_output_schema._dtypes().keys()
        ]
        output_storage = Storage.flat(self.context.universe, ordered_output_columns)
        paths = [input_storage.get_path(column) for column in columns]
        datasource = transformer._get_datasource()
        engine_table = self.scope.async_transformer(
            engine_input_table,
            paths,
            transformer._connector.on_subscribe_change,
            transformer._connector.on_subscribe_time_end,
            transformer._connector.on_subscribe_end,
            datasource.datastorage,
            datasource.dataformat,
            datasource.connector_properties,
            skip_errors=False,
        )

        assert eval_state is not None
        eval_state.set_temporary_table(output_storage, engine_table)
        return self.eval_dependency(tmp_column, eval_state=eval_state)

    def eval_cast(
        self,
        expression: expr.CastExpression,
        eval_state: RowwiseEvalState | None = None,
    ):
        arg = self.eval_expression(expression._expr, eval_state=eval_state)
        source_type = expression._expr._dtype
        target_type = expression._return_type
        source_type = dt.normalize_pointers(source_type)
        target_type = dt.normalize_pointers(target_type)

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
        expr = self.eval_expression(expression._expr, eval_state=eval_state)
        default = self.eval_expression(expression._default, eval_state=eval_state)
        unwrap = expression._unwrap
        source_type = expression._expr._dtype
        target_type = expression._return_type

        if (
            dt.dtype_equivalence(target_type, source_type)
            or dt.dtype_equivalence(dt.Optional(source_type), target_type)
            or (source_type == dt.NONE and isinstance(target_type, dt.Optional))
            or target_type == dt.ANY
        ):
            return expr

        if (
            result_expression := get_convert_operator(
                expr, default, source_type, target_type, unwrap
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
            arg_dtype: dt.DType
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
        none_expr = api.Expression.const(
            None, dt.Optional(expression._dtype).to_engine()
        )
        for arg in reversed(args):
            res = api.Expression.if_else(
                api.Expression.is_none(arg),
                none_expr,
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
        if expression._instance is not None:
            instance = self.eval_expression(expression._instance, eval_state=eval_state)
        else:
            instance = None
        optional = expression._optional
        return api.Expression.pointer_from(
            *expressions,
            optional=optional,
            instance=instance,
        )

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

    def eval_fill_error(
        self,
        expression: expr.FillErrorExpression,
        eval_state: RowwiseEvalState | None = None,
    ):
        dtype = expression._dtype
        ret = self.eval_expression(
            expr.CastExpression(dtype, expression._expr),
            eval_state=eval_state,
        )
        replacement = self.eval_expression(
            expr.CastExpression(dtype, expression._replacement),
            eval_state=eval_state,
        )

        return api.Expression.fill_error(ret, replacement)

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


class TableRestrictedRowwiseEvaluator(
    RowwiseEvaluator, context_type=clmn.TableRestrictedRowwiseContext
):
    context: clmn.TableRestrictedRowwiseContext

    def eval_column_val(
        self,
        expression: expr.ColumnReference,
        eval_state: RowwiseEvalState | None = None,
    ):
        if expression.table != self.context.table:
            raise ValueError("invalid expression in restricted context")
        return super().eval_column_val(expression, eval_state)


class FilterEvaluator(ExpressionEvaluator, context_type=clmn.FilterContext):
    context: clmn.FilterContext

    def run(self, output_storage: Storage) -> api.Table:
        input_storage = self.state.get_storage(self.context.input_universe())
        filtering_column_path = input_storage.get_path(self.context.filtering_column)
        properties = self._table_properties(output_storage)
        return self.scope.filter_table(
            self.state.get_table(input_storage._universe),
            filtering_column_path,
            properties,
        )


class ForgetEvaluator(ExpressionEvaluator, context_type=clmn.ForgetContext):
    context: clmn.ForgetContext

    def run(self, output_storage: Storage) -> api.Table:
        input_storage = self.state.get_storage(self.context.input_universe())
        threshold_column_path = input_storage.get_path(self.context.threshold_column)
        time_column_path = input_storage.get_path(self.context.time_column)
        instance_column_path = input_storage.get_path(self.context.instance_column)
        properties = self._table_properties(output_storage)

        return self.scope.forget(
            self.state.get_table(input_storage._universe),
            threshold_column_path,
            time_column_path,
            instance_column_path,
            self.context.mark_forgetting_records,
            properties,
        )


class GradualBroadcastEvaluator(
    ExpressionEvaluator, context_type=clmn.GradualBroadcastContext
):
    context: clmn.GradualBroadcastContext

    def run(self, output_storage: Storage) -> api.Table:
        threshold_storage = self.state.get_storage(self.context.value_column.universe)
        lower_path = threshold_storage.get_path(self.context.lower_column)
        value_path = threshold_storage.get_path(self.context.value_column)
        upper_path = threshold_storage.get_path(self.context.upper_column)
        properties = self._table_properties(output_storage)

        return self.scope.gradual_broadcast(
            self.state.get_table(self.context.input_universe()),
            self.state.get_table(threshold_storage._universe),
            lower_path,
            value_path,
            upper_path,
            properties,
        )


class ExternalIndexAsOfNowEvaluator(
    ExpressionEvaluator, context_type=clmn.ExternalIndexAsOfNowContext
):
    context: clmn.ExternalIndexAsOfNowContext

    def run(self, output_storage: Storage) -> api.Table:
        index_storage = self.state.get_storage(self.context.index_universe())
        index_path = index_storage.get_path(self.context.index_column)
        index_filter_data_path = (
            index_storage.get_path(self.context.index_filter_data_column)
            if self.context.index_filter_data_column is not None
            else None
        )
        queries_storage = self.state.get_storage(self.context.query_universe())
        query_path = queries_storage.get_path(self.context.query_column)
        query_response_limit_path = (
            queries_storage.get_path(self.context.query_response_limit_column)
            if self.context.query_response_limit_column is not None
            else None
        )
        query_filter_path = (
            queries_storage.get_path(self.context.query_filter_column)
            if self.context.query_filter_column is not None
            else None
        )

        properties = self._table_properties(output_storage)

        index = ExternalIndexData(
            self.state.get_table(self.context.index_universe()),
            index_path,
            index_filter_data_path,
        )
        queries = ExternalIndexQuery(
            self.state.get_table(self.context.query_universe()),
            query_path,
            query_response_limit_path,
            query_filter_path,
        )

        return self.scope.use_external_index_as_of_now(
            index=index,
            queries=queries,
            table_properties=properties,
            external_index_factory=self.context.index_factory,
        )


class ForgetImmediatelyEvaluator(
    ExpressionEvaluator, context_type=clmn.ForgetImmediatelyContext
):
    context: clmn.ForgetImmediatelyContext

    def run(self, output_storage: Storage) -> api.Table:
        properties = self._table_properties(output_storage)

        return self.scope.forget_immediately(
            self.state.get_table(self.context.input_universe()),
            properties,
        )


class FilterOutForgettingContext(
    ExpressionEvaluator, context_type=clmn.FilterOutForgettingContext
):
    context: clmn.FilterOutForgettingContext

    def run(self, output_storage: Storage) -> api.Table:
        properties = self._table_properties(output_storage)

        return self.scope.filter_out_results_of_forgetting(
            self.state.get_table(self.context.input_universe()), properties
        )


class FreezeEvaluator(ExpressionEvaluator, context_type=clmn.FreezeContext):
    context: clmn.FreezeContext

    def run(self, output_storage: Storage) -> api.Table:
        input_storage = self.state.get_storage(self.context.input_universe())
        threshold_column_path = input_storage.get_path(self.context.threshold_column)
        time_column_path = input_storage.get_path(self.context.time_column)
        instance_column_path = input_storage.get_path(self.context.instance_column)
        properties = self._table_properties(output_storage)

        return self.scope.freeze(
            self.state.get_table(input_storage._universe),
            threshold_column_path,
            time_column_path,
            instance_column_path,
            properties,
        )


class BufferEvaluator(ExpressionEvaluator, context_type=clmn.BufferContext):
    context: clmn.BufferContext

    def run(self, output_storage: Storage) -> api.Table:
        input_storage = self.state.get_storage(self.context.input_universe())
        threshold_column_path = input_storage.get_path(self.context.threshold_column)
        time_column_path = input_storage.get_path(self.context.time_column)
        instance_column_path = input_storage.get_path(self.context.instance_column)
        properties = self._table_properties(output_storage)

        return self.scope.buffer(
            self.state.get_table(input_storage._universe),
            threshold_column_path,
            time_column_path,
            instance_column_path,
            properties,
        )


class IntersectEvaluator(ExpressionEvaluator, context_type=clmn.IntersectContext):
    context: clmn.IntersectContext

    def run(self, output_storage: Storage) -> api.Table:
        engine_tables = self.state.get_tables(self.context.universe_dependencies())
        properties = self._table_properties(output_storage)
        return self.scope.intersect_tables(
            self.maybe_flatten_table(
                output_storage.maybe_flattened_inputs["input_storage"]
            ),
            engine_tables[1:],
            properties,
        )


class RestrictEvaluator(ExpressionEvaluator, context_type=clmn.RestrictContext):
    context: clmn.RestrictContext

    def run(self, output_storage: Storage) -> api.Table:
        properties = self._table_properties(output_storage)
        return self.scope.restrict_table(
            self.maybe_flatten_table(
                output_storage.maybe_flattened_inputs["orig_storage"]
            ),
            self.maybe_flatten_table(
                output_storage.maybe_flattened_inputs["new_storage"]
            ),
            properties,
        )


class DifferenceEvaluator(ExpressionEvaluator, context_type=clmn.DifferenceContext):
    context: clmn.DifferenceContext

    def run(self, output_storage: Storage) -> api.Table:
        properties = self._table_properties(output_storage)
        return self.scope.subtract_table(
            self.maybe_flatten_table(
                output_storage.maybe_flattened_inputs["input_storage"]
            ),
            self.state.get_table(self.context.right.universe),
            properties,
        )


class ReindexEvaluator(ExpressionEvaluator, context_type=clmn.ReindexContext):
    context: clmn.ReindexContext

    def run(self, output_storage: Storage) -> api.Table:
        input_storage = self.state.get_storage(self.context.input_universe())
        reindexing_column_path = input_storage.get_path(self.context.reindex_column)
        properties = self._table_properties(output_storage)
        return self.scope.reindex_table(
            self.state.get_table(input_storage._universe),
            reindexing_column_path,
            properties,
        )


class IxEvaluator(ExpressionEvaluator, context_type=clmn.IxContext):
    context: clmn.IxContext

    def run(self, output_storage: Storage) -> api.Table:
        key_storage = output_storage.maybe_flattened_inputs["new_storage"]
        key_column_path = key_storage.get_path(self.context.key_column)
        properties = self._table_properties(output_storage)
        return self.scope.ix_table(
            self.maybe_flatten_table(
                output_storage.maybe_flattened_inputs["orig_storage"]
            ),
            self.maybe_flatten_table(key_storage),
            key_column_path,
            optional=self.context.optional,
            strict=True,
            table_properties=properties,
        )


class PromiseSameUniverseEvaluator(
    ExpressionEvaluator, context_type=clmn.PromiseSameUniverseContext
):
    context: clmn.PromiseSameUniverseContext

    def run(self, output_storage: Storage) -> api.Table:
        properties = self._table_properties(output_storage)
        return self.scope.override_table_universe(
            self.maybe_flatten_table(
                output_storage.maybe_flattened_inputs["orig_storage"]
            ),
            self.maybe_flatten_table(
                output_storage.maybe_flattened_inputs["new_storage"]
            ),
            properties,
        )


class PromiseSameUniverseAsOfNowEvaluator(
    ExpressionEvaluator, context_type=clmn.PromiseSameUniverseAsOfNowContext
):
    context: clmn.PromiseSameUniverseContext

    def run(self, output_storage: Storage) -> api.Table:
        orig_storage = output_storage.maybe_flattened_inputs["orig_storage"]
        original_table_with_forgetting = self.scope.forget_immediately(
            self.maybe_flatten_table(orig_storage),
            self._table_properties(orig_storage),
        )
        new_universe_storage = output_storage.maybe_flattened_inputs["new_storage"]
        destination_universe_table_with_forgetting = self.scope.forget_immediately(
            self.maybe_flatten_table(new_universe_storage),
            self._table_properties(new_universe_storage),
        )

        properties = self._table_properties(output_storage)
        table_with_merged_universes = self.scope.override_table_universe(
            original_table_with_forgetting,
            destination_universe_table_with_forgetting,
            properties,
        )

        return self.scope.filter_out_results_of_forgetting(
            table_with_merged_universes, properties
        )


class HavingEvaluator(ExpressionEvaluator, context_type=clmn.HavingContext):
    context: clmn.HavingContext

    def run(self, output_storage: Storage) -> api.Table:
        key_storage = self.state.get_storage(self.context.key_column.universe)
        key_column_path = key_storage.get_path(self.context.key_column)
        properties = self._table_properties(output_storage)
        return self.scope.ix_table(
            self.maybe_flatten_table(
                output_storage.maybe_flattened_inputs["input_storage"]
            ),
            self.state.get_table(key_storage._universe),
            key_column_path,
            optional=False,
            strict=False,
            table_properties=properties,
        )


class JoinEvaluator(ExpressionEvaluator, context_type=clmn.JoinContext):
    context: clmn.JoinContext

    def run_join(self, universe: univ.Universe, output_storage: Storage) -> None:
        left_input_storage = output_storage.maybe_flattened_inputs["left_storage"]
        right_input_storage = output_storage.maybe_flattened_inputs["right_storage"]
        join_storage = output_storage.maybe_flattened_inputs["join_storage"]
        left_paths = [
            left_input_storage.get_path(column)
            for column in self.context.on_left.columns
        ]
        right_paths = [
            right_input_storage.get_path(column)
            for column in self.context.on_right.columns
        ]
        properties = self._table_properties(join_storage)
        output_engine_table = self.scope.join_tables(
            self.maybe_flatten_table(left_input_storage),
            self.maybe_flatten_table(right_input_storage),
            left_paths,
            right_paths,
            last_column_is_instance=self.context.last_column_is_instance,
            table_properties=properties,
            assign_id=self.context.assign_id,
            left_ear=self.context.left_ear,
            right_ear=self.context.right_ear,
        )
        self.state.set_table(join_storage, output_engine_table)

    def run(self, output_storage: Storage) -> api.Table:
        self.run_join(self.context.universe, output_storage)
        rowwise_evaluator = RowwiseEvaluator(
            clmn.RowwiseContext(self.context.id_column),
            self.scope,
            self.state,
            self.scope_context,
        )
        if self.context.assign_id:
            old_path = ColumnPath((1,))
        else:
            old_path = None
        return rowwise_evaluator.run(
            output_storage,
            old_path=old_path,
            disable_runtime_typechecking=True,
        )


class JoinRowwiseEvaluator(RowwiseEvaluator, context_type=clmn.JoinRowwiseContext):
    context: clmn.JoinRowwiseContext


@dataclass
class ReducerData:
    reducer: api.Reducer
    input_columns: tuple[clmn.Column, ...]


class GroupedEvaluator(ExpressionEvaluator, context_type=clmn.GroupedContext):
    context: clmn.GroupedContext

    def run(
        self,
        output_storage: Storage,
    ) -> api.Table:
        input_storage = self.state.get_storage(self.context.inner_context.universe)
        reducers = []
        for column in output_storage.get_columns():
            assert isinstance(column, clmn.ColumnWithExpression)
            expression = column.expression
            expression = self.context.expression_with_type(expression)
            reducer_data = self.eval_expression(expression)
            inner_paths = [
                input_storage.get_path(input_column)
                for input_column in reducer_data.input_columns
            ]
            reducers.append(
                api.ReducerData(
                    reducer=reducer_data.reducer,
                    skip_errors=self.context.skip_errors,
                    column_paths=inner_paths,
                    trace=column.trace.to_engine(),
                )
            )

        groupby_columns_paths = [
            input_storage.get_path(col_ref._column)
            for col_ref in self.context.grouping_columns
        ]

        properties = self._table_properties(output_storage)

        reduced_engine_table = self.scope.group_by_table(
            self.state.get_table(input_storage._universe),
            groupby_columns_paths,
            self.context.last_column_is_instance,
            reducers,
            self.context.set_id,
            properties,
        )

        return reduced_engine_table

    def eval_expression(self, expression) -> ReducerData:
        impl: dict[type, Callable] = {
            expr.ColumnReference: self.eval_column_val,
            expr.ReducerExpression: self.eval_reducer,
        }
        if type(expression) not in impl:
            raise RuntimeError(f"GroupedEvaluator encountered {type(expression)}")
        return impl[type(expression)](expression)

    def _reducer_data(
        self,
        reducer: api.Reducer,
        expressions: Iterable[expr.ColumnExpression],
    ) -> ReducerData:
        input_columns = []
        for input_expression in expressions:
            assert isinstance(input_expression, expr.ColumnReference)
            input_columns.append(input_expression._column)
        return ReducerData(reducer, tuple(input_columns))

    def eval_column_val(
        self,
        expression: expr.ColumnReference,
    ) -> ReducerData:
        return self._reducer_data(api.Reducer.UNIQUE, [expression])

    def eval_reducer(
        self,
        expression: expr.ReducerExpression,
    ) -> ReducerData:
        args = expression._args + expression._reducer.additional_args_from_context(
            self.context
        )
        engine_reducer = expression._reducer.engine_reducer(
            [arg._dtype for arg in expression._args]
        )
        return self._reducer_data(engine_reducer, args)


class DeduplicateEvaluator(ExpressionEvaluator, context_type=clmn.DeduplicateContext):
    context: clmn.DeduplicateContext

    def run(
        self,
        output_storage: Storage,
    ) -> api.Table:
        input_storage = self.state.get_storage(self.context.input_universe())
        instance_paths = [
            input_storage.get_path(column) for column in self.context.instance
        ]
        reduced_columns_paths = [input_storage.get_path(self.context.value)]
        for column in output_storage.get_columns():
            assert isinstance(column, clmn.ColumnWithReference)
            path = input_storage.get_path(column.expression._column)
            reduced_columns_paths.append(path)

        properties = self._table_properties(output_storage)

        def is_different_with_state(
            state: tuple[api.Value, ...] | None,
            rows: Iterable[tuple[list[api.Value], int]],
        ) -> tuple[api.Value, ...] | None:
            for [col, *cols], difference in rows:
                if difference <= 0 or col is api.ERROR:
                    continue
                state_val = state[0] if state is not None else None
                if state_val is None or self.context.acceptor(col, state_val):
                    state = (col, *cols)
            return state

        return self.scope.deduplicate(
            self.state.get_table(input_storage._universe),
            instance_paths,
            reduced_columns_paths,
            is_different_with_state,
            self.context.unique_name,
            properties,
        )


class UpdateRowsEvaluator(ExpressionEvaluator, context_type=clmn.UpdateRowsContext):
    context: clmn.UpdateRowsContext

    def run(self, output_storage: Storage) -> api.Table:
        engine_input_tables = self.maybe_flatten_tables(output_storage)
        [input_table, update_input_table] = engine_input_tables
        properties = self._table_properties(output_storage)
        return self.scope.update_rows_table(input_table, update_input_table, properties)


class UpdateCellsEvaluator(ExpressionEvaluator, context_type=clmn.UpdateCellsContext):
    context: clmn.UpdateCellsContext

    def run(self, output_storage: Storage) -> api.Table:
        left_storage = output_storage.maybe_flattened_inputs["left_storage"]
        right_storage = output_storage.maybe_flattened_inputs["right_storage"]
        paths = []
        update_paths = []

        for column in output_storage.get_columns():
            if column in left_storage.get_columns():
                continue
            assert isinstance(column, clmn.ColumnWithReference)
            if column.expression.name in self.context.updates:
                paths.append(left_storage.get_path(column.expression._column))
                update_paths.append(
                    right_storage.get_path(self.context.updates[column.expression.name])
                )

        properties = self._table_properties(output_storage)

        return self.scope.update_cells_table(
            self.maybe_flatten_table(left_storage),
            self.maybe_flatten_table(right_storage),
            paths,
            update_paths,
            properties,
        )


class ConcatUnsafeEvaluator(ExpressionEvaluator, context_type=clmn.ConcatUnsafeContext):
    context: clmn.ConcatUnsafeContext

    def run(self, output_storage: Storage) -> api.Table:
        engine_input_tables = self.maybe_flatten_tables(output_storage)
        properties = self._table_properties(output_storage)
        engine_table = self.scope.concat_tables(engine_input_tables, properties)
        return engine_table


class FlattenEvaluator(ExpressionEvaluator, context_type=clmn.FlattenContext):
    context: clmn.FlattenContext

    def run(self, output_storage: Storage) -> api.Table:
        input_storage = self.state.get_storage(self.context.orig_universe)
        flatten_column_path = input_storage.get_path(self.context.flatten_column)
        properties = self._table_properties(output_storage)
        return self.scope.flatten_table(
            self.state.get_table(input_storage._universe),
            flatten_column_path,
            properties,
        )


class SortingEvaluator(ExpressionEvaluator, context_type=clmn.SortingContext):
    context: clmn.SortingContext

    def run(self, output_storage: Storage) -> api.Table:
        input_storage = self.state.get_storage(self.context.universe)
        key_column_path = input_storage.get_path(self.context.key_column)
        instance_column_path = input_storage.get_path(self.context.instance_column)
        properties = self._table_properties(output_storage)
        return self.scope.sort_table(
            self.state.get_table(input_storage._universe),
            key_column_path,
            instance_column_path,
            properties,
        )


class SetSchemaContextEvaluator(
    ExpressionEvaluator, context_type=clmn.SetSchemaContext
):
    context: clmn.SetSchemaContext

    def run(self, output_storage: Storage) -> api.Table:
        return self.state.get_table(self.context.universe)


class FilterOutValueEvaluator(
    ExpressionEvaluator, context_type=clmn.FilterOutValueContext
):
    context: clmn.FilterOutValueContext

    def run(self, output_storage: Storage) -> api.Table:
        input_storage = self.state.get_storage(self.context.input_universe())
        column_paths = []
        for column in output_storage.get_columns():
            assert isinstance(column, clmn.ColumnWithReference)
            path = input_storage.get_path(column.expression._column)
            column_paths.append(path)
        properties = self._table_properties(output_storage)
        return self.scope.remove_value_from_table(
            self.state.get_table(input_storage._universe),
            column_paths,
            self.context.value_to_filter_out,
            properties,
        )


class RemoveRetractionsEvaluator(
    ExpressionEvaluator, context_type=clmn.RemoveRetractionsContext
):
    context: clmn.RemoveRetractionsContext

    def run(self, output_storage: Storage) -> api.Table:
        input_storage = self.state.get_storage(self.context.input_universe())
        properties = self._table_properties(output_storage)
        return self.scope.remove_retractions_from_table(
            self.state.get_table(input_storage._universe),
            properties,
        )


class AsyncTransformerEvaluator(
    ExpressionEvaluator, context_type=clmn.AsyncTransformerContext
):
    context: clmn.AsyncTransformerContext

    def run(self, output_storage: Storage) -> api.Table:
        input_storage = self.state.get_storage(self.context.input_universe())
        column_paths = [
            input_storage.get_path(column) for column in self.context.input_columns
        ]
        return self.scope.async_transformer(
            self.state.get_table(input_storage._universe),
            column_paths,
            self.context.on_change,
            self.context.on_time_end,
            self.context.on_end,
            self.context.datasource.datastorage,
            self.context.datasource.dataformat,
            self.context.datasource.connector_properties,
            skip_errors=True,
        )
