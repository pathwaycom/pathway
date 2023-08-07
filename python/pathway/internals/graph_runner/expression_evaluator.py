# Copyright © 2023 Pathway

from __future__ import annotations

import warnings
from abc import ABC, abstractmethod
from functools import cached_property
from typing import (
    TYPE_CHECKING,
    Callable,
    ClassVar,
    Dict,
    List,
    Optional,
    Tuple,
    Type,
    cast,
    get_origin,
)

import pathway.internals.column as clmn
import pathway.internals.expression as expr
from pathway.internals import _reducers, api, asynchronous
from pathway.internals.dtype import unoptionalize
from pathway.internals.expression_printer import get_expression_info
from pathway.internals.expression_visitor import ExpressionVisitor
from pathway.internals.graph_runner.operator_mapping import (
    get_binary_expression,
    get_binary_operators_mapping_optionals,
    get_cast_operators_mapping,
    get_unary_expression,
    tuple_handling_operators,
)
from pathway.internals.graph_runner.scope_context import ScopeContext

if TYPE_CHECKING:
    from pathway.internals.graph_runner.state import ScopeState


def column_eval_properties(column: clmn.ColumnWithContext) -> api.EvalProperties:
    return api.EvalProperties(dtype=column.dtype, trace=column.trace.to_engine())


class ExpressionEvaluator(ABC):
    _context_map: ClassVar[Dict[Type[clmn.Context], Type[ExpressionEvaluator]]] = {}
    context_type: ClassVar[Type[clmn.Context]]
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

    @property
    def output_universe(self) -> api.Universe:
        return self.state.get_universe(self.context.universe)

    @classmethod
    def for_context(cls, context: clmn.Context) -> Type[ExpressionEvaluator]:
        return cls._context_map[type(context)]

    @abstractmethod
    def eval(self, column: clmn.ColumnWithExpression) -> api.Column:
        pass

    def expression_type(self, expression: clmn.ColumnExpression):
        return self.context.expression_type(expression)


class RowwiseEvalState:
    _dependencies: Dict[api.Column, int]

    def __init__(self) -> None:
        self._dependencies = {}

    def dependency(self, column: api.Column) -> int:
        return self._dependencies.setdefault(column, len(self._dependencies))

    @property
    def columns(self) -> List[api.Column]:
        return list(self._dependencies.keys())


class DependencyReference:
    index: int
    column: api.Column

    def __init__(self, index: int, column: api.Column):
        self.index = index
        self.column = column

    def __call__(self, vals):
        return vals[self.index]


class RowwiseEvaluator(
    ExpressionEvaluator, ExpressionVisitor, context_type=clmn.RowwiseContext
):
    def eval_dependency(
        self, column: api.Column, eval_state: Optional[RowwiseEvalState]
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
        eval_properties: Optional[api.EvalProperties] = None,
    ) -> api.Column:
        if eval_properties is None:
            eval_properties = api.EvalProperties(dtype=self.expression_type(expression))
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
        eval_properties: Optional[api.EvalProperties] = None,
    ):
        assert not expression._kwargs
        if eval_properties is None:
            eval_properties = api.EvalProperties(dtype=self.expression_type(expression))
        columns = [self._column_from_expression(arg) for arg in expression._args]
        table = self.scope.table(self.output_universe, columns)
        return self.scope.unsafe_map_column_numba(
            table=table,
            function=expression._fun,
            properties=eval_properties,
        )

    def eval_column_val(
        self,
        expression: expr.ColumnReference,
        eval_state: Optional[RowwiseEvalState] = None,
    ):
        column = self._dereference(expression)
        return self.eval_dependency(column, eval_state=eval_state)

    def eval_unary_op(
        self,
        expression: expr.ColumnUnaryOpExpression,
        eval_state: Optional[RowwiseEvalState] = None,
    ):
        operator_fun = expression._operator
        arg = self.eval_expression(expression._expr, eval_state=eval_state)
        operand_dtype = self.expression_type(expression._expr)
        if (
            result_expression := get_unary_expression(arg, operator_fun, operand_dtype)
        ) is not None:
            return result_expression

        return api.Expression.apply(operator_fun, arg)

    def eval_binary_op(
        self,
        expression: expr.ColumnBinaryOpExpression,
        eval_state: Optional[RowwiseEvalState] = None,
    ):
        operator_fun = expression._operator
        left_expression = expression._left
        right_expression = expression._right

        left_dtype = self.expression_type(left_expression)
        right_dtype = self.expression_type(right_expression)

        # TODO: casting of Optional[int] to Optional[float]
        if left_dtype == int and right_dtype in [float, Optional[float]]:
            left_dtype = float
            left_expression = expr.CastExpression(left_dtype, left_expression)
        if left_dtype in [float, Optional[float]] and right_dtype == int:
            right_dtype = float
            right_expression = expr.CastExpression(right_dtype, right_expression)

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

        left_dtype_unoptionalized, right_dtype_unoptionalized = unoptionalize(
            left_dtype,
            right_dtype,
        )

        if (
            dtype_and_handler := get_binary_operators_mapping_optionals(
                operator_fun, left_dtype_unoptionalized, right_dtype_unoptionalized
            )
        ) is not None:
            return dtype_and_handler[1](left, right)

        if (
            result_expression := get_binary_expression(
                left,
                right,
                operator_fun,
                left_dtype_unoptionalized,
                right_dtype_unoptionalized,
            )
        ) is not None:
            return result_expression

        operator_symbol = getattr(
            expression._operator, "_symbol", expression._operator.__name__
        )

        expression_info = get_expression_info(expression)
        if (
            get_origin(left_dtype) == get_origin(Tuple)
            and get_origin(right_dtype) == get_origin(Tuple)
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
            raise RuntimeError(
                f"Pathway does not support using binary operator {expression._operator.__name__}"
                + f" on columns of types {left_dtype}, {right_dtype}."
                + "It refers to the following expression:\n"
                + expression_info
            )

    def eval_const(
        self,
        expression: expr.ColumnConstExpression,
        eval_state: Optional[RowwiseEvalState] = None,
    ):
        return api.Expression.const(expression._val)

    def eval_call(
        self,
        expression: expr.ColumnCallExpression,
        eval_state: Optional[RowwiseEvalState] = None,
    ):
        # At this point all column calls should be desugared and gone
        raise RuntimeError("RowwiseEvaluator encountered ColumnCallExpression")

    def eval_apply(
        self,
        expression: expr.ApplyExpression,
        eval_state: Optional[RowwiseEvalState] = None,
    ):
        fun, args = self._prepare_positional_apply(
            fun=expression._fun, args=expression._args, kwargs=expression._kwargs
        )
        return api.Expression.apply(
            fun, *(self.eval_expression(arg, eval_state=eval_state) for arg in args)
        )

    def eval_async_apply(
        self,
        expression: expr.AsyncApplyExpression,
        eval_state: Optional[RowwiseEvalState] = None,
    ):
        fun, args = self._prepare_positional_apply(
            fun=asynchronous.coerce_async(expression._fun),
            args=expression._args,
            kwargs=expression._kwargs,
        )

        columns = [self._column_from_expression(arg) for arg in args]
        table = self.scope.table(self.output_universe, columns)
        column = self.scope.async_map_column(
            table,
            fun,
            properties=api.EvalProperties(
                dtype=self.expression_type(expression),
                trace=expression._trace.to_engine(),
            ),
        )
        return self.eval_dependency(column, eval_state=eval_state)

    def eval_numbaapply(
        self,
        expression: expr.NumbaApplyExpression,
        eval_state: Optional[RowwiseEvalState] = None,
    ):
        column = self.numba_compile_numbaapply(expression)
        return self.eval_dependency(column, eval_state=eval_state)

    def eval_cast(
        self,
        expression: expr.CastExpression,
        eval_state: Optional[RowwiseEvalState] = None,
    ):
        arg = self.eval_expression(expression._expr, eval_state=eval_state)
        key = (
            self.expression_type(expression._expr),
            self.expression_type(expression),
        )
        if key[0] == key[1]:
            return arg
        if (result_expression := get_cast_operators_mapping(arg, *key)) is not None:
            return result_expression

        key = unoptionalize(key[0], key[1])

        if (result_expression := get_cast_operators_mapping(arg, *key)) is not None:
            return result_expression

        raise RuntimeError(
            f"Pathway doesn't support type conversion between {key[0]} and {key[1]}"
        )

    def eval_declare(
        self,
        expression: expr.DeclareTypeExpression,
        eval_state: Optional[RowwiseEvalState] = None,
    ):
        return self.eval_expression(expression._expr, eval_state=eval_state)

    def eval_coalesce(
        self,
        expression: expr.CoalesceExpression,
        eval_state: Optional[RowwiseEvalState] = None,
    ):
        args = [
            self.eval_expression(arg, eval_state=eval_state) for arg in expression._args
        ]

        res = args[-1]
        for arg in reversed(args[:-1]):
            res = api.Expression.if_else(api.Expression.is_none(arg), res, arg)

        return res

    def eval_require(
        self,
        expression: expr.RequireExpression,
        eval_state: Optional[RowwiseEvalState] = None,
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
        eval_state: Optional[RowwiseEvalState] = None,
    ):
        if_ = self.eval_expression(expression._if, eval_state=eval_state)
        then = self.eval_expression(expression._then, eval_state=eval_state)
        else_ = self.eval_expression(expression._else, eval_state=eval_state)

        return api.Expression.if_else(if_, then, else_)

    def eval_reducer(
        self,
        expression: expr.ReducerExpression,
        eval_state: Optional[RowwiseEvalState] = None,
    ):
        raise RuntimeError("RowwiseEvaluator encountered ReducerExpression")

    def eval_reducer_ix(
        self,
        expression: expr.ReducerIxExpression,
        eval_state: Optional[RowwiseEvalState] = None,
    ):
        raise RuntimeError("RowwiseEvaluator encountered ReducerIxExpression")

    def eval_ix(
        self,
        expression: expr.ColumnIxExpression,
        eval_state: Optional[RowwiseEvalState] = None,
    ):
        keys_col: api.Column = self._column_from_expression(expression._keys_expression)
        colref = expression._column_expression

        input_universe = self.state.get_universe(colref.table._universe)
        ixer = self.scope.ix(
            keys_col, input_universe, strict=True, optional=expression._optional
        )

        input_column = self.state.get_column(expression._column)
        return self.eval_dependency(ixer.ix_column(input_column), eval_state=eval_state)

    def eval_pointer(
        self,
        expression: expr.PointerExpression,
        eval_state: Optional[RowwiseEvalState] = None,
    ):
        expressions = [
            self.eval_expression(arg, eval_state=eval_state) for arg in expression._args
        ]
        optional = expression._optional
        return api.Expression.pointer_from(*expressions, optional=optional)

    def eval_make_tuple(
        self,
        expression: expr.MakeTupleExpression,
        eval_state: Optional[RowwiseEvalState] = None,
    ):
        expressions = [
            self.eval_expression(arg, eval_state=eval_state) for arg in expression._args
        ]
        return api.Expression.make_tuple(*expressions)

    def eval_sequence_get(
        self,
        expression: expr.SequenceGetExpression,
        eval_state: Optional[RowwiseEvalState] = None,
    ):
        object = self.eval_expression(expression._object, eval_state=eval_state)
        index = self.eval_expression(expression._index, eval_state=eval_state)
        default = self.eval_expression(expression._default, eval_state=eval_state)
        if expression._check_if_exists:
            return api.Expression.sequence_get_item_checked(object, index, default)
        else:
            return api.Expression.sequence_get_item_unchecked(object, index)

    def eval_method_call(
        self,
        expression: expr.MethodCallExpression,
        eval_state: Optional[RowwiseEvalState] = None,
    ):
        expressions = [
            self.eval_expression(arg, eval_state=eval_state) for arg in expression._args
        ]
        dtypes = tuple([self.expression_type(arg) for arg in expression._args])
        if (handler := expression._fun_mapping.get(dtypes)) is not None:
            return handler(*expressions)
        raise AttributeError(
            f"Column of type {dtypes[0]} has no attribute {expression._name}."
        )

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
        args: Tuple[expr.ColumnExpression, ...],
        kwargs: Dict[str, expr.ColumnExpression],
    ) -> Tuple[Callable, Tuple[expr.ColumnExpression, ...]]:
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

    def _dereference(self, expression: expr.ColumnReference):
        if expression.table != self.context.table:
            raise ValueError("invalid expression in restricted context")
        return super()._dereference(expression)


class FilterEvaluator(ExpressionEvaluator, context_type=clmn.FilterContext):
    context: clmn.FilterContext

    def _initialize_from_context(self):
        filtering_column = self.state.get_column(self.context.filtering_column)
        universe_to_filter = self.state.get_universe(self.context.universe_to_filter)
        self.filtered_universe = self.scope.filter_universe(
            universe_to_filter, filtering_column
        )

    def eval(self, column: clmn.ColumnWithExpression) -> api.Column:
        column_to_filter = self.state.get_column(column.dereference())
        return self.scope.restrict_column(self.filtered_universe, column_to_filter)

    @property
    def output_universe(self) -> api.Universe:
        return self.filtered_universe


class IntersectEvaluator(ExpressionEvaluator, context_type=clmn.IntersectContext):
    context: clmn.IntersectContext

    def eval(self, column: clmn.ColumnWithExpression) -> api.Column:
        column_to_restrict = self.state.get_column(column.dereference())
        return self.scope.restrict_column(self.output_universe, column_to_restrict)

    @cached_property
    def output_universe(self) -> api.Universe:
        return self.scope.intersect_universe(
            *(
                self.state.get_universe(universe)
                for universe in self.context.intersecting_universes
            ),
        )


class DifferenceEvaluator(ExpressionEvaluator, context_type=clmn.DifferenceContext):
    context: clmn.DifferenceContext

    def eval(self, column: clmn.ColumnWithExpression) -> api.Column:
        column_to_restrict = self.state.get_column(column.dereference())
        return self.scope.restrict_column(self.output_universe, column_to_restrict)

    @cached_property
    def output_universe(self) -> api.Universe:
        return self.scope.venn_universes(
            left_universe=self.state.get_universe(self.context.left),
            right_universe=self.state.get_universe(self.context.right),
        ).only_left()


class ReindexEvaluator(ExpressionEvaluator, context_type=clmn.ReindexContext):
    context: clmn.ReindexContext
    reindexing_column: api.Column

    def _initialize_from_context(self):
        self.reindexing_column = self.state.get_column(self.context.reindex_column)

    def eval(self, column: clmn.ColumnWithExpression) -> api.Column:
        column_to_reindex = self.state.get_column(column.dereference())
        return self.scope.reindex_column(
            column_to_reindex, self.reindexing_column, self.output_universe
        )

    @cached_property
    def output_universe(self) -> api.Universe:
        return self.scope.reindex_universe(self.reindexing_column)


class PromiseSameUniverseEvaluator(
    ExpressionEvaluator, context_type=clmn.PromiseSameUniverseContext
):
    def eval(self, column: clmn.ColumnWithExpression) -> api.Column:
        column_to_override = self.state.get_column(column.dereference())
        return self.scope.override_column_universe(
            self.output_universe, column_to_override
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
    joiner: api.Joiner
    left_table: api.Table
    right_table: api.Table

    def _initialize_from_context(self):
        left_table = self.state.get_context_table(self.context.on_left)
        right_table = self.state.get_context_table(self.context.on_right)
        self.joiner = self.scope.join(
            left_table,
            right_table,
            self.context.assign_id,
            self.context.left_ear,
            self.context.right_ear,
        )

    def _dereference(self, expression: expr.ColumnReference):
        input_column = self.state.get_column(expression._column)
        if expression.table == self.context.left_table:
            return self.joiner.select_left_column(input_column)
        elif expression.table == self.context.right_table:
            return self.joiner.select_right_column(input_column)
        else:
            return super()._dereference(expression)

    @cached_property
    def output_universe(self) -> api.Universe:
        return self.joiner.universe


class JoinFilterEvaluator(JoinEvaluator, context_type=clmn.JoinFilterContext):
    context: clmn.JoinFilterContext  # noqa

    def _dereference(self, expression: expr.ColumnReference):
        ret = super()._dereference(expression)
        return self.scope.restrict_column(self.output_universe, ret)

    @cached_property
    def output_universe(self) -> api.Universe:
        return self.state.get_universe(self.context.universe)


class GroupedEvaluator(RowwiseEvaluator, context_type=clmn.GroupedContext):
    context: clmn.GroupedContext
    grouper: api.Grouper

    def _initialize_from_context(self):
        universe = self.state.get_universe(self.context.inner_context.universe)
        table = api.Table(
            universe, self.state.get_columns(self.context.grouping_columns.values())
        )
        requested_columns = self.state.get_columns(
            (
                self.context.grouping_columns[col]
                for col in self.context.requested_grouping_columns
                if not self.scope_context.skip_column(col.to_column())
            )
        )
        self.grouper = self.scope.group_by(
            table, requested_columns, self.context.set_id
        )

    def map_reducer(self, reducer: Callable) -> api.Reducer:
        api_reducers = {
            # XXX
            _reducers._argmin: api.Reducer.ARG_MIN,
            _reducers._min: api.Reducer.MIN,
            _reducers._argmax: api.Reducer.ARG_MAX,
            _reducers._max: api.Reducer.MAX,
            _reducers._sum: api.Reducer.SUM,
            _reducers._int_sum: api.Reducer.INT_SUM,
            _reducers._sorted_tuple: api.Reducer.SORTED_TUPLE,
            _reducers._max_int: api.Reducer.MAX,
            _reducers._min_int: api.Reducer.MIN,
            _reducers._npsum: api.Reducer.SUM,
            _reducers._unique: api.Reducer.UNIQUE,
            _reducers._any: api.Reducer.ANY,
        }
        return api_reducers[reducer]

    def _dereference(self, expression: expr.ColumnReference):
        input_column = self.context.grouping_columns.get(
            expression._to_original_internal()
        )
        if input_column is None:
            return super()._dereference(expression)
        else:
            return self.grouper.input_column(self.state.get_column(input_column))

    def eval_reducer(
        self,
        expression: expr.ReducerExpression,
        eval_state: Optional[RowwiseEvalState] = None,
    ):
        args: List[api.Column] = []
        for arg in expression._args:
            context = self.context.table._context
            input_column = self.context.table._eval(arg, context)
            column = RowwiseEvaluator(
                context, self.scope, self.state, self.scope_context
            ).eval(input_column)
            args.append(column)
        if expression._reducer is _reducers._count:
            column = self.grouper.count_column()
        else:
            [arg_column] = args
            reducer = self.map_reducer(expression._reducer)
            column = self.grouper.reducer_column(reducer, arg_column)
        return self.eval_dependency(column, eval_state)

    def eval_reducer_ix(
        self,
        expression: expr.ReducerIxExpression,
        eval_state: Optional[RowwiseEvalState] = None,
    ):
        [arg] = expression._args
        reducer = self.map_reducer(expression._reducer)
        rowwise_context = self.context.table._context
        column = self.context.table._eval(arg, rowwise_context)
        rowwise = RowwiseEvaluator(
            rowwise_context, self.scope, self.state, self.scope_context
        )

        ix_expr: expr.ColumnIxExpression = cast(
            expr.ColumnIxExpression, column.expression
        )

        keys_col: api.Column = rowwise._column_from_expression(ix_expr._keys_expression)

        input_column = rowwise.state.get_column(ix_expr._column)

        ixer = rowwise.scope.ix(
            keys_col, input_column.universe, strict=True, optional=ix_expr._optional
        )

        result_column = self.grouper.reducer_ix_column(reducer, ixer, input_column)
        return self.eval_dependency(result_column, eval_state)

    @cached_property
    def output_universe(self) -> api.Universe:
        return self.grouper.universe


class UpdateRowsEvaluator(ExpressionEvaluator, context_type=clmn.UpdateRowsContext):
    context: clmn.UpdateRowsContext

    def eval(self, column: clmn.ColumnWithExpression) -> api.Column:
        assert isinstance(column.expression, expr.ColumnReference)
        name = column.expression.name
        updates = self.context.updates[name]

        evaluated_updates = self.state.get_column(updates)
        evaluated_column = self.state.get_column(column.dereference())
        return self.scope.update_rows(
            self.output_universe, evaluated_column, evaluated_updates
        )

    @cached_property
    def output_universe(self) -> api.Universe:
        return self.scope.union_universe(
            *(
                self.state.get_universe(universe)
                for universe in self.context.union_universes
            )
        )


class ConcatUnsafeEvaluator(ExpressionEvaluator, context_type=clmn.ConcatUnsafeContext):
    context: clmn.ConcatUnsafeContext
    concat: api.Concat

    def _initialize_from_context(self):
        self.concat = self.scope.concat(
            self.state.get_universe(universe)
            for universe in self.context.union_universes
        )

    def eval(self, column: clmn.ColumnWithExpression) -> api.Column:
        assert isinstance(column.expression, expr.ColumnReference)
        name = column.expression.name
        updates = [update[name] for update in self.context.updates]

        evaluated_updates = [self.state.get_column(update) for update in updates]
        evaluated_column = self.state.get_column(column.dereference())
        return self.concat.concat_column([evaluated_column, *evaluated_updates])

    @cached_property
    def output_universe(self) -> api.Universe:
        return self.concat.universe


class FlattenEvaluator(ExpressionEvaluator, context_type=clmn.FlattenContext):
    context: clmn.FlattenContext
    flatten: api.Flatten
    inner_evaluator: RowwiseEvaluator

    def _initialize_from_context(self):
        flatten_column = self.state.get_column(self.context.flatten_column)
        self.flatten = self.scope.flatten(flatten_column)
        self.inner_evaluator = RowwiseEvaluator(
            self.context.inner_context, self.scope, self.state, self.scope_context
        )
        self.state.set_column(
            self.context.flatten_result_column, self.flatten.get_flattened_column()
        )

    def eval(self, column: clmn.ColumnWithExpression) -> api.Column:
        column_evaluated = self.inner_evaluator.eval(column)
        return self.flatten.explode_column(column_evaluated)

    @cached_property
    def output_universe(self) -> api.Universe:
        return self.flatten.universe


class SortingEvaluator(RowwiseEvaluator, context_type=clmn.SortingContext):
    context: clmn.SortingContext

    def _initialize_from_context(self):
        key_column = self.eval(self.context.key_column)
        instance_column = self.eval(self.context.instance_column)
        prev_column, next_column = self.scope.sort(key_column, instance_column)
        self.state.set_column(self.context.prev_column, prev_column)
        self.state.set_column(self.context.next_column, next_column)
