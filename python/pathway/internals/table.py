# Copyright © 2024 Pathway

from __future__ import annotations

import functools
import warnings
from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, Any, Generic, TypeVar, cast, overload

import pathway.internals.column as clmn
import pathway.internals.expression as expr
from pathway.engine import ExternalIndexFactory
from pathway.internals import api, dtype as dt, groupbys, thisclass, universes
from pathway.internals.api import Value
from pathway.internals.arg_handlers import (
    arg_handler,
    groupby_handler,
    reduce_args_handler,
    select_args_handler,
)
from pathway.internals.decorators import contextualized_operator
from pathway.internals.desugaring import (
    RestrictUniverseDesugaring,
    combine_args_kwargs,
    desugar,
)
from pathway.internals.expression_visitor import collect_tables
from pathway.internals.helpers import SetOnceProperty, StableSet
from pathway.internals.joins import Joinable, JoinResult
from pathway.internals.operator import DebugOperator, OutputHandle
from pathway.internals.operator_input import OperatorInput
from pathway.internals.parse_graph import G
from pathway.internals.runtime_type_check import check_arg_types
from pathway.internals.schema import Schema, schema_from_columns, schema_from_types
from pathway.internals.table_like import TableLike
from pathway.internals.table_slice import TableSlice
from pathway.internals.trace import trace_user_frame
from pathway.internals.type_interpreter import TypeInterpreterState
from pathway.internals.universe import Universe
from pathway.internals.universe_solver import UniverseSolver

if TYPE_CHECKING:
    from pathway.internals.datasink import DataSink
    from pathway.internals.interactive import LiveTable


TSchema = TypeVar("TSchema", bound=Schema)
TTable = TypeVar("TTable", bound="Table[Any]")
T = TypeVar("T", bound=api.Value)


class Table(
    Joinable,
    OperatorInput,
    Generic[TSchema],
):
    """Collection of named columns over identical universes.

    Example:

    >>> import pathway as pw
    >>> t1 = pw.debug.table_from_markdown('''
    ... age | owner | pet
    ... 10  | Alice | dog
    ... 9   | Bob   | dog
    ... 8   | Alice | cat
    ... 7   | Bob   | dog
    ... ''')
    >>> isinstance(t1, pw.Table)
    True
    """

    if TYPE_CHECKING:
        from pathway.stdlib.ordered import diff  # type: ignore[misc]
        from pathway.stdlib.statistical import interpolate  # type: ignore[misc]
        from pathway.stdlib.temporal import (  # type: ignore[misc]
            asof_join,
            asof_join_left,
            asof_join_outer,
            asof_join_right,
            asof_now_join,
            asof_now_join_inner,
            asof_now_join_left,
            inactivity_detection,
            interval_join,
            interval_join_inner,
            interval_join_left,
            interval_join_outer,
            interval_join_right,
            window_join,
            window_join_inner,
            window_join_left,
            window_join_outer,
            window_join_right,
            windowby,
        )
        from pathway.stdlib.viz import (  # type: ignore[misc]
            _repr_mimebundle_,
            plot,
            show,
        )

    _columns: dict[str, clmn.Column]
    _schema: type[Schema]
    _id_column: clmn.IdColumn
    _rowwise_context: clmn.RowwiseContext
    _source: SetOnceProperty[OutputHandle] = SetOnceProperty()
    """Lateinit by operator."""

    def __init__(
        self,
        _columns: Mapping[str, clmn.Column],
        _context: clmn.Context,
        _schema: type[Schema] | None = None,
    ):
        if _schema is None:
            _schema = schema_from_columns(_columns, _context.id_column)
        super().__init__(_context)
        self._columns = dict(_columns)
        self._schema = _schema
        self._id_column = _context.id_column
        assert dt.wrap(self._id_column.dtype) == dt.wrap(self._schema.id_type)
        self._substitution = {thisclass.this: self}
        self._rowwise_context = clmn.RowwiseContext(self._id_column)

    @property
    def id(self) -> expr.ColumnReference:
        """Get reference to pseudocolumn containing id's of a table.

        Example:

        >>> import pathway as pw
        >>> t1 = pw.debug.table_from_markdown('''
        ... age | owner | pet
        ... 10  | Alice | dog
        ... 9   | Bob   | dog
        ... 8   | Alice | cat
        ... 7   | Bob   | dog
        ... ''')
        >>> t2 = t1.select(ids = t1.id)
        >>> t2.typehints()['ids']
        <class 'pathway.engine.Pointer'>
        >>> pw.debug.compute_and_print(t2.select(test=t2.id == t2.ids), include_id=False)
        test
        True
        True
        True
        True
        """
        return expr.ColumnReference(_table=self, _column=self._id_column, _name="id")

    def column_names(self):
        return self.keys()

    def keys(self):
        return self._columns.keys()

    def _get_column(self, name: str) -> clmn.Column:
        return self._columns[name]

    def _ipython_key_completions_(self):
        return list(self.column_names())

    def __dir__(self):
        return list(super().__dir__()) + list(self.column_names())

    @property
    def _C(self) -> TSchema:
        return self.C  # type: ignore

    @property
    def schema(self) -> type[Schema]:
        """Get schema of the table.

        Example:

        >>> import pathway as pw
        >>> t1 = pw.debug.table_from_markdown('''
        ... age | owner | pet
        ... 10  | Alice | dog
        ... 9   | Bob   | dog
        ... 8   | Alice | cat
        ... 7   | Bob   | dog
        ... ''')
        >>> t1.schema
        <pathway.Schema types={'age': <class 'int'>, 'owner': <class 'str'>, 'pet': <class 'str'>}, \
id_type=<class 'pathway.engine.Pointer'>>
        >>> t1.typehints()['age']
        <class 'int'>
        """
        return self._schema

    @property
    def is_append_only(self) -> bool:
        return all([col.properties.append_only for col in self._columns.values()])

    def _get_colref_by_name(self, name, exception_type) -> expr.ColumnReference:
        name = self._column_deprecation_rename(name)
        if name == "id":
            return self.id
        if name not in self.keys():
            raise exception_type(f"Table has no column with name {name}.")
        return expr.ColumnReference(
            _table=self, _column=self._get_column(name), _name=name
        )

    @overload
    def __getitem__(self, args: str | expr.ColumnReference) -> expr.ColumnReference: ...

    @overload
    def __getitem__(self, args: list[str | expr.ColumnReference]) -> Table: ...

    @trace_user_frame
    def __getitem__(
        self, args: str | expr.ColumnReference | list[str | expr.ColumnReference]
    ) -> expr.ColumnReference | Table:
        """Get columns by name.

        Warning:
            - Does not allow repetitions of columns.
            - Fails if tries to access nonexistent column.

        Args:
            names: a singe column name or list of columns names to be extracted from `self`.

        Returns:
            Table with specified columns, or column expression (if single argument given).
            Instead of column names, column references are valid here.

        Example:

        >>> import pathway as pw
        >>> t1 = pw.debug.table_from_markdown('''
        ... age | owner | pet
        ... 10  | Alice | dog
        ... 9   | Bob   | dog
        ... 8   | Alice | cat
        ... 7   | Bob   | dog
        ... ''')
        >>> t2 = t1[["age", "pet"]]
        >>> t2 = t1[["age", t1.pet]]
        >>> pw.debug.compute_and_print(t2, include_id=False)
        age | pet
        7   | dog
        8   | cat
        9   | dog
        10  | dog
        """
        if isinstance(args, expr.ColumnReference):
            if (args.table is not self) and not isinstance(
                args.table, thisclass.ThisMetaclass
            ):
                raise ValueError(
                    "Table.__getitem__ argument has to be a ColumnReference to the same table or pw.this, or a string "
                    + "(or a list of those)."
                )
            return self._get_colref_by_name(args.name, KeyError)
        elif isinstance(args, str):
            return self._get_colref_by_name(args, KeyError)
        else:
            return self.select(*[self[name] for name in args])

    @staticmethod
    def _get_universe_solver() -> UniverseSolver:
        return G.universe_solver

    @trace_user_frame
    @staticmethod
    @check_arg_types
    def from_columns(
        *args: expr.ColumnReference, **kwargs: expr.ColumnReference
    ) -> Table:
        """Build a table from columns.

        All columns must have the same ids. Columns' names must be pairwise distinct.

        Args:
            args: List of columns.
            kwargs: Columns with their new names.

        Returns:
            Table: Created table.


        Example:

        >>> import pathway as pw
        >>> t1 = pw.Table.empty(age=float, pet=float)
        >>> t2 = pw.Table.empty(foo=float, bar=float).with_universe_of(t1)
        >>> t3 = pw.Table.from_columns(t1.pet, qux=t2.foo)
        >>> pw.debug.compute_and_print(t3, include_id=False)
        pet | qux
        """
        all_args = cast(
            dict[str, expr.ColumnReference], combine_args_kwargs(args, kwargs)
        )
        if not all_args:
            raise ValueError("Table.from_columns() cannot have empty arguments list")
        else:
            arg = next(iter(all_args.values()))
            table: Table = arg.table
            for arg in all_args.values():
                if not table._universe.is_equal_to(arg.table._universe):
                    raise ValueError(
                        "Universes of all arguments of Table.from_columns() have to be equal.\n"
                        + "Consider using Table.promise_universes_are_equal() to assert it.\n"
                        + "(However, untrue assertion might result in runtime errors.)"
                    )
            return table.select(*args, **kwargs)

    @trace_user_frame
    @check_arg_types
    def concat_reindex(self, *tables: Table) -> Table:
        """Concatenate contents of several tables.

        This is similar to PySpark union. All tables must have the same schema. Each row is reindexed.

        Args:
            tables: List of tables to concatenate. All tables must have the same schema.

        Returns:
            Table: The concatenated table. It will have new, synthetic ids.

        Example:

        >>> import pathway as pw
        >>> t1 = pw.debug.table_from_markdown('''
        ...   | pet
        ... 1 | Dog
        ... 7 | Cat
        ... ''')
        >>> t2 = pw.debug.table_from_markdown('''
        ...   | pet
        ... 1 | Manul
        ... 8 | Octopus
        ... ''')
        >>> t3 = t1.concat_reindex(t2)
        >>> pw.debug.compute_and_print(t3, include_id=False)
        pet
        Cat
        Dog
        Manul
        Octopus
        """
        all_tables: list[Table] = [self, *tables]
        all_tables = [table.update_id_type(dt.ANY_POINTER) for table in all_tables]
        reindexed = [
            table.with_id_from(table.id, i) for i, table in enumerate(all_tables)
        ]
        universes.promise_are_pairwise_disjoint(*reindexed)
        concatenated = Table.concat(*reindexed)
        return concatenated.update_id_type(
            dt.ANY_POINTER,
            id_append_only=concatenated._id_column.properties.append_only,
        )

    @trace_user_frame
    @staticmethod
    @check_arg_types
    def empty(**kwargs: dt.DType) -> Table:
        """Creates an empty table with a schema specified by kwargs.

        Args:
            kwargs: Dict whose keys are column names and values are column types.

        Returns:
            Table: Created empty table.


        Example:

        >>> import pathway as pw
        >>> t1 = pw.Table.empty(age=float, pet=float)
        >>> pw.debug.compute_and_print(t1, include_id=False)
        age | pet
        """
        from pathway.internals import table_io

        ret = table_io.empty_from_schema(schema_from_types(None, **kwargs))
        ret._universe.register_as_empty(no_warn=True)
        return ret

    @trace_user_frame
    @desugar
    @arg_handler(handler=select_args_handler)
    @contextualized_operator
    def select(self, *args: expr.ColumnReference, **kwargs: Any) -> Table:
        """Build a new table with columns specified by kwargs.

        Output columns' names are keys(kwargs). values(kwargs) can be raw values, boxed
        values, columns. Assigning to id reindexes the table.


        Args:
            args: Column references.
            kwargs: Column expressions with their new assigned names.


        Returns:
            Table: Created table.


        Example:

        >>> import pathway as pw
        >>> t1 = pw.debug.table_from_markdown('''
        ... pet
        ... Dog
        ... Cat
        ... ''')
        >>> t2 = t1.select(animal=t1.pet, desc="fluffy")
        >>> pw.debug.compute_and_print(t2, include_id=False)
        animal | desc
        Cat    | fluffy
        Dog    | fluffy
        """
        new_columns = []

        all_args = combine_args_kwargs(args, kwargs)

        for new_name, expression in all_args.items():
            self._validate_expression(expression)
            column = self._eval(expression)
            new_columns.append((new_name, column))

        return self._with_same_universe(*new_columns)

    @trace_user_frame
    def __add__(self, other: Table) -> Table:
        """Build a union of `self` with `other`.

        Semantics: Returns a table C, such that
            - C.columns == self.columns + other.columns
            - C.id == self.id == other.id

        Args:
            other: The other table. `self.id` must be equal `other.id` and
            `self.columns` and `other.columns` must be disjoint (or overlapping names
            are THE SAME COLUMN)

        Returns:
            Table: Created table.


        Example:

        >>> import pathway as pw
        >>> t1 = pw.debug.table_from_markdown('''
        ...    pet
        ... 1  Dog
        ... 7  Cat
        ... ''')
        >>> t2 = pw.debug.table_from_markdown('''
        ...    age
        ... 1   10
        ... 7    3
        ... ''')
        >>> t3 = t1 + t2
        >>> pw.debug.compute_and_print(t3, include_id=False)
        pet | age
        Cat | 3
        Dog | 10
        """
        if not self._universe.is_equal_to(other._universe):
            raise ValueError(
                "Universes of all arguments of Table.__add__() have to be equal.\n"
                + "Consider using Table.promise_universes_are_equal() to assert it.\n"
                + "(However, untrue assertion might result in runtime errors.)"
            )
        return self.select(*self, *other)

    @property
    def slice(self) -> TableSlice:
        """Creates a collection of references to self columns.
        Supports basic column manipulation methods.

        Example:

        >>> import pathway as pw
        >>> t1 = pw.debug.table_from_markdown('''
        ... age | owner | pet
        ... 10  | Alice | dog
        ... 9   | Bob   | dog
        ... 8   | Alice | cat
        ... 7   | Bob   | dog
        ... ''')
        >>> t1.slice.without("age")
        TableSlice({'owner': <table1>.owner, 'pet': <table1>.pet})
        """
        return TableSlice(dict(**self), self)

    @trace_user_frame
    @desugar
    @check_arg_types
    def filter(self, filter_expression: expr.ColumnExpression) -> Table[TSchema]:
        """Filter a table according to `filter_expression` condition.


        Args:
            filter_expression: `ColumnExpression` that specifies the filtering condition.

        Returns:
            Table: Result has the same schema as `self` and its ids are subset of `self.id`.


        Example:

        >>> import pathway as pw
        >>> vertices = pw.debug.table_from_markdown('''
        ... label outdegree
        ...     1         3
        ...     7         0
        ... ''')
        >>> filtered = vertices.filter(vertices.outdegree == 0)
        >>> pw.debug.compute_and_print(filtered, include_id=False)
        label | outdegree
        7     | 0
        """
        filter_type = self.eval_type(filter_expression)
        if filter_type != dt.BOOL:
            raise TypeError(
                f"Filter argument of Table.filter() has to be bool, found {filter_type}."
            )
        ret = self._filter(filter_expression)
        if (
            filter_col := expr.get_column_filtered_by_is_none(filter_expression)
        ) is not None and filter_col.table == self:
            name = filter_col.name
            dtype = self._columns[name].dtype
            ret = ret.update_types(**{name: dt.unoptionalize(dtype)})
        return ret

    @trace_user_frame
    @desugar
    @check_arg_types
    def split(
        self, split_expression: expr.ColumnExpression
    ) -> tuple[Table[TSchema], Table[TSchema]]:
        """Split a table according to `split_expression` condition.


        Args:
            split_expression: `ColumnExpression` that specifies the split condition.

        Returns:
           positive_table, negative_table: tuple of tables,
           with the same schemas as `self` and with ids that are subsets of `self.id`,
           and provably disjoint.


        Example:

        >>> import pathway as pw
        >>> vertices = pw.debug.table_from_markdown('''
        ... label outdegree
        ...     1         3
        ...     7         0
        ... ''')
        >>> positive, negative = vertices.split(vertices.outdegree == 0)
        >>> pw.debug.compute_and_print(positive, include_id=False)
        label | outdegree
        7     | 0
        >>> pw.debug.compute_and_print(negative, include_id=False)
        label | outdegree
        1     | 3
        """
        positive = self.filter(split_expression)
        negative = self.filter(~split_expression)
        universes.promise_are_pairwise_disjoint(positive, negative)
        universes.promise_are_equal(
            self, Table.concat(positive, negative)
        )  # TODO: add API method for this
        return positive, negative

    @contextualized_operator
    def _filter(self, filter_expression: expr.ColumnExpression) -> Table[TSchema]:
        self._validate_expression(filter_expression)
        filtering_column = self._eval(filter_expression)
        assert self._universe == filtering_column.universe

        context = clmn.FilterContext(filtering_column, self._id_column)

        return self._table_with_context(context)

    @trace_user_frame
    @desugar
    @check_arg_types
    @contextualized_operator
    def _external_index_as_of_now(
        self,
        query_table: Table,
        *,
        index_column: expr.ColumnExpression,
        query_column: expr.ColumnExpression,
        index_factory: ExternalIndexFactory,
        res_type: dt.DType = dt.List(dt.Tuple(dt.ANY_POINTER, float)),
        query_responses_limit_column: expr.ColumnExpression | None = None,
        index_filter_data_column: expr.ColumnExpression | None = None,
        query_filter_column: expr.ColumnExpression | None = None,
    ) -> Table:
        ev_query_responses_limit_column = (
            query_table._eval(query_responses_limit_column)
            if query_responses_limit_column is not None
            else None
        )
        ev_index_filter_data_column = (
            self._eval(index_filter_data_column)
            if index_filter_data_column is not None
            else None
        )
        ev_query_filter_column = (
            query_table._eval(query_filter_column)
            if query_filter_column is not None
            else None
        )
        context = clmn.ExternalIndexAsOfNowContext(
            _index_id_column=self._id_column,
            _query_id_column=query_table._id_column,
            index_table=self,
            query_table=query_table,
            index_column=self._eval(index_column),
            query_column=query_table._eval(query_column),
            index_factory=index_factory,
            query_response_limit_column=ev_query_responses_limit_column,
            index_filter_data_column=ev_index_filter_data_column,
            query_filter_column=ev_query_filter_column,
            res_type=res_type,
        )
        return Table(
            _columns={"_pw_index_reply": context.index_reply}, _context=context
        )

    @trace_user_frame
    @desugar
    @check_arg_types
    def _gradual_broadcast(
        self,
        threshold_table,
        lower_column,
        value_column,
        upper_column,
    ) -> Table:
        return self + self.__gradual_broadcast(
            threshold_table, lower_column, value_column, upper_column
        )

    @trace_user_frame
    @desugar
    @check_arg_types
    @contextualized_operator
    def __gradual_broadcast(
        self,
        threshold_table,
        lower_column,
        value_column,
        upper_column,
    ):
        context = clmn.GradualBroadcastContext(
            self._id_column,
            threshold_table._eval(lower_column),
            threshold_table._eval(value_column),
            threshold_table._eval(upper_column),
        )

        return Table(_columns={"apx_value": context.apx_value_column}, _context=context)

    @trace_user_frame
    @desugar
    @check_arg_types
    @contextualized_operator
    def _forget(
        self,
        threshold_column: expr.ColumnExpression,
        time_column: expr.ColumnExpression,
        mark_forgetting_records: bool,
        instance_column: expr.ColumnExpression | None = None,
    ) -> Table:
        if instance_column is None:
            instance_column = expr.ColumnConstExpression(None)
        context = clmn.ForgetContext(
            self._id_column,
            self._eval(threshold_column),
            self._eval(time_column),
            self._eval(instance_column),
            mark_forgetting_records,
        )
        return self._table_with_context(context)

    @trace_user_frame
    @desugar
    @check_arg_types
    @contextualized_operator
    def _forget_immediately(
        self,
    ) -> Table:
        context = clmn.ForgetImmediatelyContext(self._id_column)
        return self._table_with_context(context)

    @trace_user_frame
    @desugar
    @check_arg_types
    @contextualized_operator
    def _filter_out_results_of_forgetting(
        self,
    ) -> Table:
        # The output universe is a superset of input universe because forgetting entries
        # are filtered out. At each point in time, the set of keys with +1 diff can be
        # bigger than a set of keys with +1 diff in an input table.
        context = clmn.FilterOutForgettingContext(self._id_column)
        return self._table_with_context(context)

    @trace_user_frame
    @desugar
    @check_arg_types
    @contextualized_operator
    def _freeze(
        self,
        threshold_column: expr.ColumnExpression,
        time_column: expr.ColumnExpression,
        instance_column: expr.ColumnExpression | None = None,
    ) -> Table:
        # FIXME: freeze can be incorrect if the input is not append-only
        # we may produce insertion but never produce deletion
        if instance_column is None:
            instance_column = expr.ColumnConstExpression(None)
        context = clmn.FreezeContext(
            self._id_column,
            self._eval(threshold_column),
            self._eval(time_column),
            self._eval(instance_column),
        )
        return self._table_with_context(context)

    @trace_user_frame
    @desugar
    @check_arg_types
    @contextualized_operator
    def _buffer(
        self,
        threshold_column: expr.ColumnExpression,
        time_column: expr.ColumnExpression,
        instance_column: expr.ColumnExpression | None = None,
    ) -> Table:
        if instance_column is None:
            instance_column = expr.ColumnConstExpression(None)
        context = clmn.BufferContext(
            self._id_column,
            self._eval(threshold_column),
            self._eval(time_column),
            self._eval(instance_column),
        )
        return self._table_with_context(context)

    @contextualized_operator
    @check_arg_types
    def difference(self, other: Table) -> Table[TSchema]:
        r"""Restrict self universe to keys not appearing in the other table.

        Args:
            other: table with ids to remove from self.

        Returns:
            Table: table with restricted universe, with the same set of columns


        Example:

        >>> import pathway as pw
        >>> t1 = pw.debug.table_from_markdown('''
        ...   | age  | owner  | pet
        ... 1 | 10   | Alice  | 1
        ... 2 | 9    | Bob    | 1
        ... 3 | 8    | Alice  | 2
        ... ''')
        >>> t2 = pw.debug.table_from_markdown('''
        ...   | cost
        ... 2 | 100
        ... 3 | 200
        ... 4 | 300
        ... ''')
        >>> t3 = t1.difference(t2)
        >>> pw.debug.compute_and_print(t3, include_id=False)
        age | owner | pet
        10  | Alice | 1
        """
        context = clmn.DifferenceContext(
            left=self._id_column,
            right=other._id_column,
        )
        return self._table_with_context(context)

    @check_arg_types
    def intersect(self, *tables: Table) -> Table[TSchema]:
        """Restrict self universe to keys appearing in all of the tables.

        Args:
            tables: tables keys of which are used to restrict universe.

        Returns:
            Table: table with restricted universe, with the same set of columns


        Example:

        >>> import pathway as pw
        >>> t1 = pw.debug.table_from_markdown('''
        ...   | age  | owner  | pet
        ... 1 | 10   | Alice  | 1
        ... 2 | 9    | Bob    | 1
        ... 3 | 8    | Alice  | 2
        ... ''')
        >>> t2 = pw.debug.table_from_markdown('''
        ...   | cost
        ... 2 | 100
        ... 3 | 200
        ... 4 | 300
        ... ''')
        >>> t3 = t1.intersect(t2)
        >>> pw.debug.compute_and_print(t3, include_id=False)
        age | owner | pet
        8   | Alice | 2
        9   | Bob   | 1
        """
        if len(tables) == 0:
            warnings.warn("Empty argument list for Table.intersect().", stacklevel=5)
            return self
        all_args: list[Table] = [self, *tables]
        intersecting_universes = [tab._universe for tab in all_args]
        universe = self._get_universe_solver().get_intersection(*intersecting_universes)
        if universe.is_equal_to(self._universe):
            warnings.warn("Unnecessary call to Table.intersect().", stacklevel=5)
        for tab in tables:
            if universe.is_equal_to(tab._universe):
                warnings.warn(
                    "Table.intersect() can be replaced with Table.restrict() operation.",
                    stacklevel=5,
                )
                break
        return self._intersect(*tables)

    @contextualized_operator
    def _intersect(self, *tables: Table) -> Table[TSchema]:
        intersecting_ids = (
            self._id_column,
            *(table._id_column for table in tables),
        )
        context = clmn.IntersectContext(
            intersecting_ids=intersecting_ids,
        )

        return self._table_with_context(context)

    @trace_user_frame
    @check_arg_types
    def restrict(self, other: TableLike) -> Table[TSchema]:
        """Restrict self universe to keys appearing in other.

        Args:
            other: table which universe is used to restrict universe of self.

        Returns:
            Table: table with restricted universe, with the same set of columns


        Example:

        >>> import pathway as pw
        >>> t1 = pw.debug.table_from_markdown(
        ...     '''
        ...   | age  | owner  | pet
        ... 1 | 10   | Alice  | 1
        ... 2 | 9    | Bob    | 1
        ... 3 | 8    | Alice  | 2
        ... '''
        ... )
        >>> t2 = pw.debug.table_from_markdown(
        ...     '''
        ...   | cost
        ... 2 | 100
        ... 3 | 200
        ... '''
        ... )
        >>> t2.promise_universe_is_subset_of(t1)
        <pathway.Table schema={'cost': <class 'int'>}>
        >>> t3 = t1.restrict(t2)
        >>> pw.debug.compute_and_print(t3, include_id=False)
        age | owner | pet
        8   | Alice | 2
        9   | Bob   | 1
        """
        if self._universe == other._universe:
            warnings.warn("Identical universes for Table.restrict().", stacklevel=5)
            return self
        if not other._universe.is_subset_of(self._universe):
            raise ValueError(
                "Table.restrict(): other universe has to be a subset of self universe."
                + "Consider using Table.promise_universe_is_subset_of() to assert it."
            )
        if other._universe.is_equal_to(self._universe):
            warnings.warn(
                "Unnecessary call to Table.restrict(), consider using Table.with_universe_of().",
                stacklevel=5,
            )
        return self._restrict(other)

    @contextualized_operator
    def _restrict(self, other: TableLike) -> Table[TSchema]:
        context = clmn.RestrictContext(self._id_column, other._id_column)

        columns = {
            name: self._wrap_column_in_context(context, column, name)
            for name, column in self._columns.items()
        }

        return Table(
            _columns=columns,
            _context=context,
        )

    @contextualized_operator
    @check_arg_types
    def copy(self) -> Table[TSchema]:
        """Returns a copy of a table.

        Example:

        >>> import pathway as pw
        >>> t1 = pw.debug.table_from_markdown('''
        ... age | owner | pet
        ... 10  | Alice | dog
        ... 9   | Bob   | dog
        ... 8   | Alice | cat
        ... 7   | Bob   | dog
        ... ''')
        >>> t2 = t1.copy()
        >>> pw.debug.compute_and_print(t2, include_id=False)
        age | owner | pet
        7   | Bob   | dog
        8   | Alice | cat
        9   | Bob   | dog
        10  | Alice | dog
        >>> t1 is t2
        False
        """

        return self._copy_as(type(self))

    def _copy_as(self, table_type: type[TTable], /, **kwargs) -> TTable:
        columns = {
            name: self._wrap_column_in_context(self._rowwise_context, column, name)
            for name, column in self._columns.items()
        }

        return table_type(_columns=columns, _context=self._rowwise_context, **kwargs)

    @trace_user_frame
    @desugar
    @arg_handler(handler=groupby_handler)
    @check_arg_types
    def groupby(
        self,
        *args: expr.ColumnReference,
        id: expr.ColumnReference | None = None,
        sort_by: expr.ColumnReference | None = None,
        _filter_out_results_of_forgetting: bool = False,
        instance: expr.ColumnReference | None = None,
        _skip_errors: bool = True,
        _is_window: bool = False,
    ) -> groupbys.GroupedTable:
        """Groups table by columns from args.

        Note:
            Usually followed by `.reduce()` that aggregates the result and returns a table.

        Args:
            args: columns to group by.
            id: if provided, is the column used to set id's of the rows of the result
            sort_by: if provided, column values are used as sorting keys for particular reducers
            instance: optional argument describing partitioning of the data into separate instances

        Returns:
            GroupedTable: Groupby object.

        Example:

        >>> import pathway as pw
        >>> t1 = pw.debug.table_from_markdown('''
        ... age | owner | pet
        ... 10  | Alice | dog
        ... 9   | Bob   | dog
        ... 8   | Alice | cat
        ... 7   | Bob   | dog
        ... ''')
        >>> t2 = t1.groupby(t1.pet, t1.owner).reduce(t1.owner, t1.pet, ageagg=pw.reducers.sum(t1.age))
        >>> pw.debug.compute_and_print(t2, include_id=False)
        owner | pet | ageagg
        Alice | cat | 8
        Alice | dog | 10
        Bob   | dog | 16
        """
        if instance is not None:
            args = (*args, instance)
        if id is not None:
            if len(args) == 0:
                args = (id,)
            elif len(args) > 1:
                raise ValueError(
                    "Table.groupby() cannot have id argument when grouping by multiple columns."
                )
            elif args[0]._column != id._column:
                raise ValueError(
                    "Table.groupby() received id argument and is grouped by a single column,"
                    + " but the arguments are not equal.\n"
                    + "Consider using <table>.groupby(id=...), skipping the positional argument."
                )

        for arg in args:
            if not isinstance(arg, expr.ColumnReference):
                if isinstance(arg, str):
                    raise ValueError(
                        f"Expected a ColumnReference, found a string. Did you mean <table>.{arg}"
                        + f" instead of {repr(arg)}?"
                    )
                else:
                    raise ValueError(
                        "All Table.groupby() arguments have to be a ColumnReference."
                    )

        self._check_for_disallowed_types(*args)
        return groupbys.GroupedTable.create(
            table=self,
            grouping_columns=args,
            last_column_is_instance=instance is not None,
            set_id=id is not None,
            sort_by=sort_by,
            _filter_out_results_of_forgetting=_filter_out_results_of_forgetting,
            _skip_errors=_skip_errors,
            _is_window=_is_window,
        )

    @trace_user_frame
    @desugar
    @arg_handler(handler=reduce_args_handler)
    def reduce(
        self, *args: expr.ColumnReference, **kwargs: expr.ColumnExpression
    ) -> Table:
        """Reduce a table to a single row.

        Equivalent to `self.groupby().reduce(*args, **kwargs)`.

        Args:
            args: reducer to reduce the table with
            kwargs: reducer to reduce the table with. Its key is the new name of a column.

        Returns:
            Table: Reduced table.

        Example:

        >>> import pathway as pw
        >>> t1 = pw.debug.table_from_markdown('''
        ... age | owner | pet
        ... 10  | Alice | dog
        ... 9   | Bob   | dog
        ... 8   | Alice | cat
        ... 7   | Bob   | dog
        ... ''')
        >>> t2 = t1.reduce(ageagg=pw.reducers.argmin(t1.age))
        >>> pw.debug.compute_and_print(t2, include_id=False) # doctest: +ELLIPSIS
        ageagg
        ^...
        >>> t3 = t2.select(t1.ix(t2.ageagg).age, t1.ix(t2.ageagg).pet)
        >>> pw.debug.compute_and_print(t3, include_id=False)
        age | pet
        7   | dog
        """
        return self.groupby().reduce(*args, **kwargs)

    @trace_user_frame
    @desugar
    @check_arg_types
    @contextualized_operator
    def deduplicate(
        self,
        *,
        value: expr.ColumnExpression | Value,
        instance: expr.ColumnExpression | None = None,
        acceptor: Callable[[T, T], bool],
        name: str | None = None,
    ) -> Table:
        """Deduplicates rows in `self` on `value` column using acceptor function.

        It keeps rows which where accepted by the acceptor function.
        Acceptor operates on two arguments - *CURRENT* value and *PREVIOUS* value.

        Args:
            value: column expression used for deduplication.
            instance: Grouping column. For rows with different
                values in this column, deduplication will be performed separately.
                Defaults to None.
            acceptor: callback telling whether two values are different.
            name:  An identifier, under which the state of the table
                will be persisted or ``None``, if there is no need to persist the state of this table.
                When a program restarts, it restores the state for all input tables according to what
                was saved for their ``name``. This way it's possible to configure the start of
                computations from the moment they were terminated last time.

        Returns:
            Table: the result of deduplication.

        Example:

        >>> import pathway as pw
        >>> table = pw.debug.table_from_markdown(
        ...     '''
        ...     val | __time__
        ...      1  |     2
        ...      2  |     4
        ...      3  |     6
        ...      4  |     8
        ... '''
        ... )
        >>>
        >>> def acceptor(new_value, old_value) -> bool:
        ...     return new_value >= old_value + 2
        ...
        >>>
        >>> result = table.deduplicate(value=pw.this.val, acceptor=acceptor)
        >>> pw.debug.compute_and_print_update_stream(result, include_id=False)
        val | __time__ | __diff__
        1   | 2        | 1
        1   | 6        | -1
        3   | 6        | 1
        >>>
        >>> table = pw.debug.table_from_markdown(
        ...     '''
        ...     val | instance | __time__
        ...      1  |     1    |     2
        ...      2  |     1    |     4
        ...      3  |     2    |     6
        ...      4  |     1    |     8
        ...      4  |     2    |     8
        ...      5  |     1    |    10
        ... '''
        ... )
        >>>
        >>> def acceptor(new_value, old_value) -> bool:
        ...     return new_value >= old_value + 2
        ...
        >>>
        >>> result = table.deduplicate(
        ...     value=pw.this.val, instance=pw.this.instance, acceptor=acceptor
        ... )
        >>> pw.debug.compute_and_print_update_stream(result, include_id=False)
        val | instance | __time__ | __diff__
        1   | 1        | 2        | 1
        3   | 2        | 6        | 1
        1   | 1        | 8        | -1
        4   | 1        | 8        | 1
        """
        if instance is None:
            instance = expr.ColumnConstExpression(None)
        if not isinstance(value, expr.ColumnExpression):
            _value: expr.ColumnExpression = expr.ColumnConstExpression(value)
        else:
            _value = value
        self._validate_expression(_value)
        self._validate_expression(instance)
        self._check_for_disallowed_types(_value, instance)
        value_col = self._eval(_value)
        instance_col = self._eval(instance)

        context = clmn.DeduplicateContext(
            value_col,
            (instance_col,),
            acceptor,
            self._id_column,
            name,
        )

        return self._table_with_context(context)

    @trace_user_frame
    def ix(
        self,
        expression: expr.ColumnExpression,
        *,
        optional: bool = False,
        context=None,
        allow_misses: bool = False,
    ) -> Table:
        """Reindexes the table using expression values as keys. Uses keys from context, or tries to infer
        proper context from the expression.
        If optional is True, then None in expression values result in None values in the result columns.
        Missing values in table keys result in RuntimeError.
        If ``allow_misses`` is set to True, they result in None value on the output.

        Context can be anything that allows for `select` or `reduce`, or `pathway.this` construct
        (latter results in returning a delayed operation, and should be only used when using `ix` inside
        join().select() or groupby().reduce() sequence).

        Returns:
            Reindexed table with the same set of columns.

        Example:

        >>> import pathway as pw
        >>> t_animals = pw.debug.table_from_markdown('''
        ...   | epithet    | genus
        ... 1 | upupa      | epops
        ... 2 | acherontia | atropos
        ... 3 | bubo       | scandiacus
        ... 4 | dynastes   | hercules
        ... ''')
        >>> t_birds = pw.debug.table_from_markdown('''
        ...   | desc
        ... 2 | hoopoe
        ... 4 | owl
        ... ''')
        >>> ret = t_birds.select(t_birds.desc, latin=t_animals.ix(t_birds.id).genus)
        >>> pw.debug.compute_and_print(ret, include_id=False)
        desc   | latin
        hoopoe | atropos
        owl    | hercules
        """

        if context is None:
            all_tables = collect_tables(expression)
            if len(all_tables) == 0:
                context = thisclass.this
            elif all(tab == all_tables[0] for tab in all_tables):
                context = all_tables[0]
        if context is None:
            for tab in all_tables:
                if not isinstance(tab, Table):
                    raise ValueError("Table expected here.")
            if len(all_tables) == 0:
                raise ValueError("Const value provided.")
            context = all_tables[0]
            for tab in all_tables:
                assert context._universe.is_equal_to(tab._universe)
        if isinstance(context, groupbys.GroupedJoinable):
            context = thisclass.this
        if isinstance(context, thisclass.ThisMetaclass):
            return context._delayed_op(
                lambda table, expression: self.ix(
                    expression=expression,
                    optional=optional,
                    context=table,
                    allow_misses=allow_misses,
                ),
                expression=expression,
                qualname=f"{self}.ix(...)",
                name="ix",
            )
        restrict_universe = RestrictUniverseDesugaring(context)
        expression = restrict_universe.eval_expression(expression)
        key_tab = context.select(tmp=expression)
        key_col = key_tab.tmp
        key_dtype = key_tab.eval_type(key_col)
        supertype = dt.ANY_POINTER
        if optional:
            supertype = dt.Optional(supertype)
        if not dt.dtype_issubclass(key_dtype, supertype):
            raise TypeError(
                f"Pathway supports indexing with Pointer type only. The type used was {key_dtype}."
            )
        supertype = self._id_column.dtype
        if optional:
            supertype = dt.Optional(supertype)
        if not dt.dtype_issubclass(key_dtype, supertype):
            raise TypeError(
                "Indexing a table with a Pointer type with probably mismatched primary keys."
                + f" Type used was {key_dtype}. Indexed id type was {supertype}."
            )
        if optional and isinstance(key_dtype, dt.Optional):
            self_ = self.update_types(
                **{name: dt.Optional(self.typehints()[name]) for name in self.keys()}
            )
        else:
            self_ = self
        if allow_misses:
            subset = self_._having(key_col)
            new_key_col = key_tab.restrict(subset).tmp
            fill = key_tab.difference(subset).select(
                **{name: None for name in self.column_names()}
            )
            return Table.concat(
                self_._ix(new_key_col, optional=optional), fill
            ).with_universe_of(key_tab)

        else:
            return self_._ix(key_col, optional)

    @contextualized_operator
    def _ix(
        self,
        key_expression: expr.ColumnReference,
        optional: bool,
    ) -> Table:
        key_column = key_expression._column

        context = clmn.IxContext(
            key_column, key_expression._table._id_column, self._id_column, optional
        )

        return self._table_with_context(context)

    def __lshift__(self, other: Table) -> Table:
        """Alias to update_cells method.

        Updates cells of `self`, breaking ties in favor of the values in `other`.

        Semantics:
            - result.columns == self.columns
            - result.id == self.id
            - conflicts are resolved preferring other's values

        Requires:
            - other.columns ⊆ self.columns
            - other.id ⊆ self.id

        Args:
            other:  the other table.

        Returns:
            Table: `self` updated with cells form `other`.

        Example:

        >>> import pathway as pw
        >>> t1 = pw.debug.table_from_markdown('''
        ...   | age | owner | pet
        ... 1 | 10  | Alice | 1
        ... 2 | 9   | Bob   | 1
        ... 3 | 8   | Alice | 2
        ... ''')
        >>> t2 = pw.debug.table_from_markdown('''
        ...   | age | owner | pet
        ... 1 | 10  | Alice | 30
        ... ''')
        >>> pw.universes.promise_is_subset_of(t2, t1)
        >>> t3 = t1 << t2
        >>> pw.debug.compute_and_print(t3, include_id=False)
        age | owner | pet
        8   | Alice | 2
        9   | Bob   | 1
        10  | Alice | 30
        """
        return self.update_cells(other, _stacklevel=2)

    @trace_user_frame
    @check_arg_types
    def concat(self, *others: Table[TSchema]) -> Table[TSchema]:
        """Concats `self` with every `other` ∊ `others`.

        Semantics:
        - result.columns == self.columns == other.columns
        - result.id == self.id ∪ other.id

        if self.id and other.id collide, throws an exception.

        Requires:
        - other.columns == self.columns
        - self.id disjoint with other.id

        Args:
            other:  the other table.

        Returns:
            Table: The concatenated table. Id's of rows from original tables are preserved.

        Example:

        >>> import pathway as pw
        >>> t1 = pw.debug.table_from_markdown('''
        ...   | age | owner | pet
        ... 1 | 10  | Alice | 1
        ... 2 | 9   | Bob   | 1
        ... 3 | 8   | Alice | 2
        ... ''')
        >>> t2 = pw.debug.table_from_markdown('''
        ...    | age | owner | pet
        ... 11 | 11  | Alice | 30
        ... 12 | 12  | Tom   | 40
        ... ''')
        >>> pw.universes.promise_are_pairwise_disjoint(t1, t2)
        >>> t3 = t1.concat(t2)
        >>> pw.debug.compute_and_print(t3, include_id=False)
        age | owner | pet
        8   | Alice | 2
        9   | Bob   | 1
        10  | Alice | 1
        11  | Alice | 30
        12  | Tom   | 40
        """
        for other in others:
            if other.keys() != self.keys():
                self_keys = set(self.keys())
                other_keys = set(other.keys())
                missing_keys = self_keys - other_keys
                superfluous_keys = other_keys - self_keys
                raise ValueError(
                    "columns do not match in the argument of Table.concat()."
                    + (
                        f" Missing columns: {missing_keys}."
                        if missing_keys is not None
                        else ""
                    )
                    + (
                        f" Superfluous columns: {superfluous_keys}."
                        if superfluous_keys is not None
                        else ""
                    )
                )
        schema = {}
        all_args: list[Table] = [self, *others]

        for key in self.keys():
            schema[key] = _types_lca_with_error(
                *[arg.schema._dtypes()[key] for arg in all_args],
                function_name="a concat",
                pointers=False,
                key=key,
            )
        id_type = _types_lca_with_error(
            *[arg.schema._id_dtype for arg in all_args],
            function_name="a concat",
            pointers=True,
        )

        return Table._concat(
            *[tab.cast_to_types(**schema).update_id_type(id_type) for tab in all_args]
        )

    @trace_user_frame
    @contextualized_operator
    def _concat(self, *others: Table[TSchema]) -> Table[TSchema]:
        union_ids = (self._id_column, *(other._id_column for other in others))
        if not self._get_universe_solver().query_are_disjoint(
            *(c.universe for c in union_ids)
        ):
            raise ValueError(
                "Universes of the arguments of Table.concat() have to be disjoint.\n"
                + "Consider using Table.promise_universes_are_disjoint() to assert it.\n"
                + "(However, untrue assertion might result in runtime errors.)"
            )
        context = clmn.ConcatUnsafeContext(
            union_ids=union_ids,
            updates=tuple(
                {col_name: other._columns[col_name] for col_name in self.keys()}
                for other in others
            ),
        )
        return self._table_with_context(context)

    @trace_user_frame
    @check_arg_types
    def update_cells(self, other: Table, _stacklevel: int = 1) -> Table:
        """Updates cells of `self`, breaking ties in favor of the values in `other`.

        Semantics:
            - result.columns == self.columns
            - result.id == self.id
            - conflicts are resolved preferring other's values

        Requires:
            - other.columns ⊆ self.columns
            - other.id ⊆ self.id

        Args:
            other:  the other table.

        Returns:
            Table: `self` updated with cells form `other`.

        Example:

        >>> import pathway as pw
        >>> t1 = pw.debug.table_from_markdown('''
        ...   | age | owner | pet
        ... 1 | 10  | Alice | 1
        ... 2 | 9   | Bob   | 1
        ... 3 | 8   | Alice | 2
        ... ''')
        >>> t2 = pw.debug.table_from_markdown('''
        ...     age | owner | pet
        ... 1 | 10  | Alice | 30
        ... ''')
        >>> pw.universes.promise_is_subset_of(t2, t1)
        >>> t3 = t1.update_cells(t2)
        >>> pw.debug.compute_and_print(t3, include_id=False)
        age | owner | pet
        8   | Alice | 2
        9   | Bob   | 1
        10  | Alice | 30
        """
        if names := (set(other.keys()) - set(self.keys())):
            raise ValueError(
                f"Columns of the argument in Table.update_cells() not present in the updated table: {list(names)}."
            )

        if self._universe == other._universe:
            warnings.warn(
                "Key sets of self and other in update_cells are the same."
                + " Using with_columns instead of update_cells.",
                stacklevel=_stacklevel + 4,
            )
            return self.with_columns(*(other[name] for name in other))

        schema = {}
        for key in other.keys():
            schema[key] = _types_lca_with_error(
                self.schema._dtypes()[key],
                other.schema._dtypes()[key],
                function_name="an update_cells",
                pointers=False,
            )

        return Table._update_cells(
            self.cast_to_types(**schema), other.cast_to_types(**schema)
        )

    @trace_user_frame
    @contextualized_operator
    @check_arg_types
    def _update_cells(self, other: Table) -> Table:
        if not other._universe.is_subset_of(self._universe):
            raise ValueError(
                "Universe of the argument of Table.update_cells() needs to be "
                + "a subset of the universe of the updated table.\n"
                + "Consider using Table.promise_is_subset_of() to assert this.\n"
                + "(However, untrue assertion might result in runtime errors.)"
            )
        context = clmn.UpdateCellsContext(
            left=self._id_column,
            right=other._id_column,
            updates={name: other._columns[name] for name in other.keys()},
        )
        return self._table_with_context(context)

    @trace_user_frame
    @check_arg_types
    def update_rows(self, other: Table[TSchema]) -> Table[TSchema]:
        """Updates rows of `self`, breaking ties in favor for the rows in `other`.

        Semantics:
        - result.columns == self.columns == other.columns
        - result.id == self.id ∪ other.id

        Requires:
        - other.columns == self.columns

        Args:
            other:  the other table.

        Returns:
            Table: `self` updated with rows form `other`.

        Example:

        >>> import pathway as pw
        >>> t1 = pw.debug.table_from_markdown('''
        ...   | age | owner | pet
        ... 1 | 10  | Alice | 1
        ... 2 | 9   | Bob   | 1
        ... 3 | 8   | Alice | 2
        ... ''')
        >>> t2 = pw.debug.table_from_markdown('''
        ...    | age | owner | pet
        ... 1  | 10  | Alice | 30
        ... 12 | 12  | Tom   | 40
        ... ''')
        >>> t3 = t1.update_rows(t2)
        >>> pw.debug.compute_and_print(t3, include_id=False)
        age | owner | pet
        8   | Alice | 2
        9   | Bob   | 1
        10  | Alice | 30
        12  | Tom   | 40
        """
        if other.keys() != self.keys():
            raise ValueError(
                "Columns do not match between argument of Table.update_rows() and the updated table."
            )
        if self._universe.is_subset_of(other._universe):
            warnings.warn(
                "Universe of self is a subset of universe of other in update_rows. Returning other.",
                stacklevel=5,
            )
            return other

        schema = {}

        for key in self.keys():
            schema[key] = _types_lca_with_error(
                self.schema._dtypes()[key],
                other.schema._dtypes()[key],
                function_name="an update_rows",
                pointers=False,
            )

        id_type = _types_lca_with_error(
            self._id_column.dtype,
            other._id_column.dtype,
            function_name="an update_rows",
            pointers=True,
        )

        if other._universe.is_subset_of(self._universe):
            return Table._update_cells(
                self.cast_to_types(**schema), other.cast_to_types(**schema)
            )
        else:
            return Table._update_rows(
                self.cast_to_types(**schema).update_id_type(id_type),
                other.cast_to_types(**schema).update_id_type(id_type),
            )

    @trace_user_frame
    @contextualized_operator
    @check_arg_types
    def _update_rows(self, other: Table[TSchema]) -> Table[TSchema]:
        union_ids = (self._id_column, other._id_column)
        context = clmn.UpdateRowsContext(
            updates={col_name: other._columns[col_name] for col_name in self.keys()},
            union_ids=union_ids,
        )
        return self._table_with_context(context)

    @trace_user_frame
    @desugar
    def with_columns(self, *args: expr.ColumnReference, **kwargs: Any) -> Table:
        """Updates columns of `self`, according to args and kwargs.
        See `table.select` specification for evaluation of args and kwargs.

        Example:

        >>> import pathway as pw
        >>> t1 = pw.debug.table_from_markdown('''
        ...   | age | owner | pet
        ... 1 | 10  | Alice | 1
        ... 2 | 9   | Bob   | 1
        ... 3 | 8   | Alice | 2
        ... ''')
        >>> t2 = pw.debug.table_from_markdown('''
        ...   | owner | pet | size
        ... 1 | Tom   | 1   | 10
        ... 2 | Bob   | 1   | 9
        ... 3 | Tom   | 2   | 8
        ... ''')
        >>> t3 = t1.with_columns(*t2)
        >>> pw.debug.compute_and_print(t3, include_id=False)
        age | owner | pet | size
        8   | Tom   | 2   | 8
        9   | Bob   | 1   | 9
        10  | Tom   | 1   | 10
        """
        other = self.select(*args, **kwargs)
        columns = dict(self)
        columns.update(other)
        return self.select(**columns)

    @trace_user_frame
    @desugar
    @check_arg_types
    def with_id(self, new_index: expr.ColumnReference) -> Table:
        """Set new ids based on another column containing id-typed values.

        To generate ids based on arbitrary valued columns, use `with_id_from`.

        Values assigned must be row-wise unique.
        The uniqueness is not checked by pathway. Failing to provide unique ids can
        cause unexpected errors downstream.

        Args:
            new_id: column to be used as the new index.

        Returns:
            Table with updated ids.

        Example:

        >>> import pytest; pytest.xfail("with_id is hard to test")
        >>> import pathway as pw
        >>> t1 = pw.debug.table_from_markdown('''
        ...   | age | owner | pet
        ... 1 | 10  | Alice | 1
        ... 2 | 9   | Bob   | 1
        ... 3 | 8   | Alice | 2
        ... ''')
        >>> t2 = pw.debug.table_from_markdown('''
        ...   | new_id
        ... 1 | 2
        ... 2 | 3
        ... 3 | 4
        ... ''')
        >>> t3 = t1.promise_universe_is_subset_of(t2).with_id(t2.new_id)
        >>> pw.debug.compute_and_print(t3)
            age  owner  pet
        ^2   10  Alice    1
        ^3    9    Bob    1
        ^4    8  Alice    2
        """
        return self._with_new_index(new_index)

    @trace_user_frame
    @desugar
    @check_arg_types
    def with_id_from(
        self,
        *args: expr.ColumnExpression | Value,
        instance: expr.ColumnReference | None = None,
    ) -> Table:
        """Compute new ids based on values in columns.
        Ids computed from `columns` must be row-wise unique.
        The uniqueness is not checked by pathway. Failing to provide unique ids can
        cause unexpected errors downstream.

        Args:
            columns:  columns to be used as primary keys.

        Returns:
            Table: `self` updated with recomputed ids.

        Example:

        >>> import pathway as pw
        >>> t1 = pw.debug.table_from_markdown('''
        ...    | age | owner  | pet
        ...  1 | 10  | Alice  | 1
        ...  2 | 9   | Bob    | 1
        ...  3 | 8   | Alice  | 2
        ... ''')
        >>> t2 = t1 + t1.select(old_id=t1.id)
        >>> t3 = t2.with_id_from(t2.age)
        >>> pw.debug.compute_and_print(t3) # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
             | age | owner | pet | old_id
        ^... | 8   | Alice | 2   | ^...
        ^... | 9   | Bob   | 1   | ^...
        ^... | 10  | Alice | 1   | ^...
        >>> t4 = t3.select(t3.age, t3.owner, t3.pet, same_as_old=(t3.id == t3.old_id),
        ...     same_as_new=(t3.id == t3.pointer_from(t3.age)))
        >>> pw.debug.compute_and_print(t4) # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
             | age | owner | pet | same_as_old | same_as_new
        ^... | 8   | Alice | 2   | False       | True
        ^... | 9   | Bob   | 1   | False       | True
        ^... | 10  | Alice | 1   | False       | True
        """
        # new_index should be a column, so a little workaround
        new_index = self.select(
            ref_column=self.pointer_from(*args, instance=instance)
        ).ref_column

        return self._with_new_index(
            new_index=new_index,
        )

    @trace_user_frame
    @contextualized_operator
    @check_arg_types
    def _with_new_index(
        self,
        new_index: expr.ColumnExpression,
    ) -> Table:
        self._validate_expression(new_index)
        index_type = self.eval_type(new_index)
        if not isinstance(index_type, dt.Pointer):
            raise TypeError(
                f"Pathway supports reindexing Tables with Pointer type only. The type used was {index_type}."
            )
        reindex_column = self._eval(new_index)
        assert self._universe == reindex_column.universe

        context = clmn.ReindexContext(reindex_column)

        return self._table_with_context(context)

    @trace_user_frame
    @desugar
    @contextualized_operator
    @check_arg_types
    def rename_columns(self, **kwargs: str | expr.ColumnReference) -> Table:
        """Rename columns according to kwargs.

        Columns not in keys(kwargs) are not changed. New name of a column must not be `id`.

        Args:
            kwargs:  mapping from old column names to new names.

        Returns:
            Table: `self` with columns renamed.

        Example:

        >>> import pathway as pw
        >>> t1 = pw.debug.table_from_markdown('''
        ... age | owner | pet
        ... 10  | Alice | 1
        ... 9   | Bob   | 1
        ... 8   | Alice | 2
        ... ''')
        >>> t2 = t1.rename_columns(years_old=t1.age, animal=t1.pet)
        >>> pw.debug.compute_and_print(t2, include_id=False)
        owner | years_old | animal
        Alice | 8         | 2
        Alice | 10        | 1
        Bob   | 9         | 1
        """
        mapping: dict[str, str] = {}
        for new_name, old_name_col in kwargs.items():
            if isinstance(old_name_col, expr.ColumnReference):
                old_name = old_name_col.name
            else:
                old_name = old_name_col
            if old_name not in self._columns:
                raise ValueError(f"Column {old_name} does not exist in a given table.")
            mapping[new_name] = old_name
        renamed_columns = self._columns.copy()
        for new_name, old_name in mapping.items():
            renamed_columns.pop(old_name)
        for new_name, old_name in mapping.items():
            renamed_columns[new_name] = self._columns[old_name]

        columns_wrapped = {
            name: self._wrap_column_in_context(
                self._rowwise_context,
                column,
                mapping[name] if name in mapping else name,
            )
            for name, column in renamed_columns.items()
        }
        return self._with_same_universe(*columns_wrapped.items())

    @check_arg_types
    def rename_by_dict(
        self, names_mapping: dict[str | expr.ColumnReference, str]
    ) -> Table:
        """Rename columns according to a dictionary.

        Columns not in keys(kwargs) are not changed. New name of a column must not be `id`.

        Args:
            names_mapping: mapping from old column names to new names.

        Returns:
            Table: `self` with columns renamed.

        Example:

        >>> import pathway as pw
        >>> t1 = pw.debug.table_from_markdown('''
        ... age | owner | pet
        ... 10  | Alice | 1
        ... 9   | Bob   | 1
        ... 8   | Alice | 2
        ... ''')
        >>> t2 = t1.rename_by_dict({"age": "years_old", t1.pet: "animal"})
        >>> pw.debug.compute_and_print(t2, include_id=False)
        owner | years_old | animal
        Alice | 8         | 2
        Alice | 10        | 1
        Bob   | 9         | 1
        """
        return self.rename_columns(
            **{new_name: self[old_name] for old_name, new_name in names_mapping.items()}
        )

    @check_arg_types
    def with_prefix(self, prefix: str) -> Table:
        """Rename columns by adding prefix to each name of column.

        Example:

        >>> import pathway as pw
        >>> t1 = pw.debug.table_from_markdown('''
        ... age | owner | pet
        ... 10  | Alice | 1
        ... 9   | Bob   | 1
        ... 8   | Alice | 2
        ... ''')
        >>> t2 = t1.with_prefix("u_")
        >>> pw.debug.compute_and_print(t2, include_id=False)
        u_age | u_owner | u_pet
        8     | Alice   | 2
        9     | Bob     | 1
        10    | Alice   | 1
        """
        return self.rename_by_dict({name: prefix + name for name in self.keys()})

    @check_arg_types
    def with_suffix(self, suffix: str) -> Table:
        """Rename columns by adding suffix to each name of column.

        Example:

        >>> import pathway as pw
        >>> t1 = pw.debug.table_from_markdown('''
        ... age | owner | pet
        ... 10  | Alice | 1
        ... 9   | Bob   | 1
        ... 8   | Alice | 2
        ... ''')
        >>> t2 = t1.with_suffix("_current")
        >>> pw.debug.compute_and_print(t2, include_id=False)
        age_current | owner_current | pet_current
        8           | Alice         | 2
        9           | Bob           | 1
        10          | Alice         | 1
        """
        return self.rename_by_dict({name: name + suffix for name in self.keys()})

    @trace_user_frame
    @check_arg_types
    def rename(
        self,
        names_mapping: dict[str | expr.ColumnReference, str] | None = None,
        **kwargs: expr.ColumnExpression,
    ) -> Table:
        """Rename columns according either a dictionary or kwargs.

        If a mapping is provided using a dictionary, ``rename_by_dict`` will be used.
        Otherwise, ``rename_columns`` will be used with kwargs.
        Columns not in keys(kwargs) are not changed. New name of a column must not be ``id``.

        Args:
            names_mapping: mapping from old column names to new names.
            kwargs:  mapping from old column names to new names.

        Returns:
            Table: `self` with columns renamed.
        """
        if names_mapping is not None:
            return self.rename_by_dict(names_mapping=names_mapping)
        return self.rename_columns(**kwargs)

    @trace_user_frame
    @desugar
    @contextualized_operator
    @check_arg_types
    def without(self, *columns: str | expr.ColumnReference) -> Table:
        """Selects all columns without named column references.

        Args:
            columns: columns to be dropped provided by `table.column_name` notation.

        Returns:
            Table: `self` without specified columns.

        Example:

        >>> import pathway as pw
        >>> t1 = pw.debug.table_from_markdown('''
        ... age  | owner  | pet
        ...  10  | Alice  | 1
        ...   9  | Bob    | 1
        ...   8  | Alice  | 2
        ... ''')
        >>> t2 = t1.without(t1.age, pw.this.pet)
        >>> pw.debug.compute_and_print(t2, include_id=False)
        owner
        Alice
        Alice
        Bob
        """
        new_columns = self._columns.copy()
        for col in columns:
            if isinstance(col, expr.ColumnReference):
                new_columns.pop(col.name)
            else:
                assert isinstance(col, str)
                new_columns.pop(col)
        columns_wrapped = {
            name: self._wrap_column_in_context(self._rowwise_context, column, name)
            for name, column in new_columns.items()
        }
        return self._with_same_universe(*columns_wrapped.items())

    @trace_user_frame
    @contextualized_operator
    @check_arg_types
    def _with_schema(self, schema: type[Schema]) -> Table:
        """Returns updated table with a forced schema on it."""
        if schema.keys() != self.schema.keys():
            raise ValueError(
                "Table.with_schema() argument has to have the same column names as in the table."
            )
        context = clmn.SetSchemaContext(
            _id_column=self._id_column,
            _new_properties={
                self[name]._to_internal(): schema.column_properties(name)
                for name in self.column_names()
            },
            _id_column_props=schema.__universe_properties__,
        )
        return self._table_with_context(context)

    @trace_user_frame
    @check_arg_types
    def update_types(self, **kwargs: Any) -> Table:
        """Updates types in schema. Has no effect on the runtime."""

        for name in kwargs.keys():
            if name not in self.keys():
                raise ValueError(
                    "Table.update_types() argument name has to be an existing table column name."
                )
        new_schema = self.schema.with_types(**kwargs)
        for name in kwargs.keys():
            left = new_schema._dtypes()[name]
            right = self.schema._dtypes()[name]
            if not (
                dt.dtype_issubclass(left, right) or dt.dtype_issubclass(right, left)
            ):
                raise TypeError(
                    f"Cannot change type from {right} to {left}.\n"
                    + "Table.update_types() should be used only for type narrowing or type extending."
                )
        return self._with_schema(new_schema)

    @trace_user_frame
    @check_arg_types
    def update_id_type(self, id_type, *, id_append_only: bool | None = None) -> Table:
        id_type = dt.wrap(id_type)
        assert isinstance(id_type, dt.Pointer)
        return self._with_schema(
            self.schema.with_id_type(id_type, append_only=id_append_only)
        )

    @check_arg_types
    def cast_to_types(self, **kwargs: Any) -> Table:
        """Casts columns to types."""

        for name in kwargs.keys():
            if name not in self.keys():
                raise ValueError(
                    "Table.cast_to_types() argument name has to be an existing table column name."
                )
        from pathway.internals.common import cast

        return self.with_columns(
            **{key: cast(val, self[key]) for key, val in kwargs.items()}
        )

    @contextualized_operator
    @check_arg_types
    def _having(self, indexer: expr.ColumnReference) -> Table[TSchema]:
        context = clmn.HavingContext(
            orig_id_column=self._id_column,
            key_column=indexer._column,
            key_id_column=indexer._table._id_column,
        )
        return self._table_with_context(context)

    @trace_user_frame
    @check_arg_types
    def with_universe_of(self, other: TableLike) -> Table:
        """Returns a copy of self with exactly the same universe as others.

        Semantics: Required precondition self.universe == other.universe
        Used in situations where Pathway cannot deduce equality of universes, but
        those are equal as verified during runtime.

        Example:

        >>> import pathway as pw
        >>> t1 = pw.debug.table_from_markdown('''
        ...   | pet
        ... 1 | Dog
        ... 7 | Cat
        ... ''')
        >>> t2 = pw.debug.table_from_markdown('''
        ...   | age
        ... 1 | 10
        ... 7 | 3
        ... 8 | 100
        ... ''')
        >>> t3 = t2.filter(pw.this.age < 30).with_universe_of(t1)
        >>> t4 = t1 + t3
        >>> pw.debug.compute_and_print(t4, include_id=False)
        pet | age
        Cat | 3
        Dog | 10
        """
        if self._universe == other._universe:
            return self.copy()
        universes.promise_are_equal(self, other)
        return self._unsafe_promise_universe(other)

    @trace_user_frame
    @check_arg_types
    def _unsafe_with_universe_of_as_of_now(self, other: TableLike) -> Table:
        """Returns a copy of self with exactly the same universe as other.

        Semantics: Required precondition self.universe == other.universe
        Used in situations where Pathway cannot deduce equality of universes, but
        those are equal as verified during runtime. Stricter than ``with_universe_of``.
        Both universes have to have updates to the same keys at the same processing time.

        """
        if self._universe == other._universe:
            return self.copy()
        universes.promise_are_equal(self, other)
        return self._unsafe_promise_universe_as_of_now(other)

    @trace_user_frame
    @desugar
    @check_arg_types
    def flatten(
        self,
        to_flatten: expr.ColumnReference,
        *,
        origin_id: str | None = None,
    ) -> Table:
        """Performs a flatmap operation on a column or expression given as a first
        argument. Datatype of this column or expression has to be iterable or Json array.
        Other columns of the table are duplicated as many times as the length of the iterable.

        It is possible to get ids of source rows by passing `origin_id` argument, which is
        a new name of the column with the source ids.

        Example:

        >>> import pathway as pw
        >>> t1 = pw.debug.table_from_markdown('''
        ...   | pet  |  age
        ... 1 | Dog  |   2
        ... 7 | Cat  |   5
        ... ''')
        >>> t2 = t1.flatten(t1.pet)
        >>> pw.debug.compute_and_print(t2, include_id=False)
        pet | age
        C   | 5
        D   | 2
        a   | 5
        g   | 2
        o   | 2
        t   | 5
        """
        if origin_id is None:
            intermediate_table = self
        else:
            intermediate_table = self.with_columns(**{origin_id: thisclass.this.id})

        return intermediate_table._flatten(to_flatten.name)

    @contextualized_operator
    def _flatten(
        self,
        flatten_name: str,
    ) -> Table:
        flatten_column = self._columns[flatten_name]

        context = clmn.FlattenContext(
            orig_universe=self._universe,
            flatten_column=flatten_column,
        )

        columns = {
            name: self._wrap_column_in_context(context, column, name)
            for name, column in self._columns.items()
            if name != flatten_name
        }

        return Table(
            _columns={
                flatten_name: context.flatten_result_column,
                **columns,
            },
            _context=context,
        )

    @trace_user_frame
    @desugar
    @contextualized_operator
    @check_arg_types
    def sort(
        self,
        key: expr.ColumnExpression,
        instance: expr.ColumnExpression | None = None,
    ) -> Table:
        """
        Sorts a table by the specified keys.

        Args:
            table : pw.Table
                The table to be sorted.
            key (ColumnExpression[int | float | datetime | str | bytes]):
                An expression to sort by.
            instance : ColumnReference or None
                An expression with instance. Rows are sorted within an instance.
                ``prev`` and ``next`` columns will only point to rows that have the same instance.

        Returns:
            pw.Table: The sorted table. Contains two columns: ``prev`` and ``next``, containing the pointers
            to the previous and next rows.

        Example:

        >>> import pathway as pw
        >>> table = pw.debug.table_from_markdown('''
        ... name     | age | score
        ... Alice    | 25  | 80
        ... Bob      | 20  | 90
        ... Charlie  | 30  | 80
        ... ''')
        >>> table = table.with_id_from(pw.this.name)
        >>> table += table.sort(key=pw.this.age)
        >>> pw.debug.compute_and_print(table, include_id=True)
                    | name    | age | score | prev        | next
        ^GBSDEEW... | Alice   | 25  | 80    | ^EDPSSB1... | ^DS9AT95...
        ^EDPSSB1... | Bob     | 20  | 90    |             | ^GBSDEEW...
        ^DS9AT95... | Charlie | 30  | 80    | ^GBSDEEW... |
        >>> table = pw.debug.table_from_markdown('''
        ... name     | age | score
        ... Alice    | 25  | 80
        ... Bob      | 20  | 90
        ... Charlie  | 30  | 80
        ... David    | 35  | 90
        ... Eve      | 15  | 80
        ... ''')
        >>> table = table.with_id_from(pw.this.name)
        >>> table += table.sort(key=pw.this.age, instance=pw.this.score)
        >>> pw.debug.compute_and_print(table, include_id=True)
                    | name    | age | score | prev        | next
        ^GBSDEEW... | Alice   | 25  | 80    | ^T0B95XH... | ^DS9AT95...
        ^EDPSSB1... | Bob     | 20  | 90    |             | ^RT0AZWX...
        ^DS9AT95... | Charlie | 30  | 80    | ^GBSDEEW... |
        ^RT0AZWX... | David   | 35  | 90    | ^EDPSSB1... |
        ^T0B95XH... | Eve     | 15  | 80    |             | ^GBSDEEW...
        """
        instance = clmn.ColumnExpression._wrap(instance)
        self._check_for_disallowed_types(key, instance)
        context = clmn.SortingContext(
            self._eval(key),
            self._eval(instance),
            self._id_column.dtype,
        )
        return Table(
            _columns={
                "prev": context.prev_column,
                "next": context.next_column,
            },
            _context=context,
        )

    def _set_source(self, source: OutputHandle):
        self._source = source
        if not hasattr(self._id_column, "lineage"):
            self._id_column.lineage = clmn.ColumnLineage(name="id", source=source)
        for name, column in self._columns.items():
            if not hasattr(column, "lineage"):
                column.lineage = clmn.ColumnLineage(name=name, source=source)
        universe = self._universe
        if not hasattr(universe, "lineage"):
            universe.lineage = clmn.Lineage(source=source)

    @contextualized_operator
    def _unsafe_promise_universe(self, other: TableLike) -> Table:
        context = clmn.PromiseSameUniverseContext(self._id_column, other._id_column)
        return self._table_with_context(context)

    @contextualized_operator
    def _unsafe_promise_universe_as_of_now(self, other: TableLike) -> Table:
        """Updates the universe of ``self`` to the universe of ``other``.
        Stricter than _unsafe_promise_universe. Both universes have
        to have updates to the same keys at the same processing time."""

        context = clmn.PromiseSameUniverseAsOfNowContext(
            self._id_column, other._id_column
        )
        return self._table_with_context(context)

    def _validate_expression(self, expression: expr.ColumnExpression):
        for dep in expression._dependencies_above_reducer():
            if self._universe != dep._column.universe:
                raise ValueError(
                    f"You cannot use {dep.to_column_expression()} in this context."
                    + " Its universe is different than the universe of the table the method"
                    + " was called on. You can use <table1>.with_universe_of(<table2>)"
                    + " to assign universe of <table2> to <table1> if you're sure their"
                    + " sets of keys are equal."
                )

    def _check_for_disallowed_types(self, *expressions: expr.ColumnExpression):
        for expression in expressions:
            dtype = self.eval_type(expression)
            if isinstance(dtype, dt.Future):
                raise TypeError(
                    f"Using column of type {dtype.typehint} is not allowed here."
                    + " Consider applying `await_futures()` to the table first."
                )

    def _wrap_column_in_context(
        self,
        context: clmn.Context,
        column: clmn.Column,
        name: str,
        lineage: clmn.Lineage | None = None,
    ) -> clmn.Column:
        """Contextualize column by wrapping it in expression."""
        expression = expr.ColumnReference(_table=self, _column=column, _name=name)
        return expression._column_with_expression_cls(
            context=context,
            universe=context.universe,
            expression=expression,
            lineage=lineage,
        )

    def _table_with_context(self, context: clmn.Context) -> Table:
        columns = {
            name: self._wrap_column_in_context(context, column, name)
            for name, column in self._columns.items()
        }

        return Table(
            _columns=columns,
            _context=context,
        )

    @functools.cached_property
    def _table_restricted_context(self) -> clmn.TableRestrictedRowwiseContext:
        return clmn.TableRestrictedRowwiseContext(self._id_column, self)

    def _eval(
        self, expression: expr.ColumnExpression, context: clmn.Context | None = None
    ) -> clmn.ColumnWithExpression:
        """Desugar expression and wrap it in given context."""
        if context is None:
            context = self._rowwise_context
        column = expression._column_with_expression_cls(
            context=context,
            universe=context.universe,
            expression=expression,
        )
        return column

    @classmethod
    def _from_schema(cls: type[TTable], schema: type[Schema]) -> TTable:
        universe = Universe()
        context = clmn.MaterializedContext(universe, schema.universe_properties)
        columns = {
            name: clmn.MaterializedColumn(
                universe,
                schema.column_properties(name),
            )
            for name in schema.column_names()
        }
        return cls(_columns=columns, _schema=schema, _context=context)

    def __repr__(self) -> str:
        id_dtype = self._id_column.dtype
        if id_dtype == dt.ANY_POINTER:
            return f"<pathway.Table schema={dict(self.typehints())}>"
        else:
            return f"<pathway.Table schema={dict(self.typehints())} id_type={id_dtype.typehint}>"

    def _with_same_universe(
        self,
        *columns: tuple[str, clmn.Column],
        schema: type[Schema] | None = None,
    ) -> Table:
        return Table(
            _columns=dict(columns),
            _schema=schema,
            _context=self._rowwise_context,
        )

    def _sort_columns_by_other(self, other: Table):
        assert self.keys() == other.keys()
        self._columns = {name: self._columns[name] for name in other.keys()}

    def _operator_dependencies(self) -> StableSet[Table]:
        return StableSet([self])

    def debug(self, name: str):
        G.add_operator(
            lambda id: DebugOperator(name, id),
            lambda operator: operator(self),
        )
        return self

    def to(self, sink: DataSink) -> None:
        from pathway.internals import table_io

        table_io.table_to_datasink(self, sink)

    def _materialize(self, universe: Universe):
        context = clmn.MaterializedContext(universe, self._id_column.properties)
        columns = {
            name: clmn.MaterializedColumn(universe, column.properties)
            for (name, column) in self._columns.items()
        }
        return Table(
            _columns=columns,
            _schema=self.schema.with_id_type(context.id_column_type()),
            _context=context,
        )

    @trace_user_frame
    def pointer_from(
        self, *args: Any, optional=False, instance: expr.ColumnReference | None = None
    ):
        """Pseudo-random hash of its argument. Produces pointer types. Applied column-wise.

        Example:

        >>> import pathway as pw
        >>> t1 = pw.debug.table_from_markdown('''
        ...    age  owner  pet
        ... 1   10  Alice  dog
        ... 2    9    Bob  dog
        ... 3    8  Alice  cat
        ... 4    7    Bob  dog''')
        >>> g = t1.groupby(t1.owner).reduce(refcol = t1.pointer_from(t1.owner)) # g.id == g.refcol
        >>> pw.debug.compute_and_print(g.select(test = (g.id == g.refcol)), include_id=False)
        test
        True
        True
        """
        # XXX verify types for the table primary_keys
        return expr.PointerExpression(
            self,
            *args,
            instance=instance,
            optional=optional,
        )

    @trace_user_frame
    def ix_ref(
        self,
        *args: expr.ColumnExpression | Value,
        optional: bool = False,
        context=None,
        instance: expr.ColumnReference | None = None,
        allow_misses: bool = False,
    ):
        """Reindexes the table using expressions as primary keys.
        Uses keys from context, or tries to infer proper context from the expression.
        If ``optional`` is True, then None in expression values result in None values in the result columns.
        Missing values in table keys result in RuntimeError.
        If ``allow_misses`` is set to True, they result in None value on the output.

        Context can be anything that allows for `select` or `reduce`, or `pathway.this` construct
        (latter results in returning a delayed operation, and should be only used when using `ix` inside
        join().select() or groupby().reduce() sequence).


        Args:
            args: Column references.

        Returns:
            Row: indexed row.

        Example:

        >>> import pathway as pw
        >>> t1 = pw.debug.table_from_markdown('''
        ... name   | pet
        ... Alice  | dog
        ... Bob    | cat
        ... Carole | cat
        ... David  | dog
        ... ''')
        >>> t2 = t1.with_id_from(pw.this.name)
        >>> t2 = t2.select(*pw.this, new_value=pw.this.ix_ref("Alice").pet)
        >>> pw.debug.compute_and_print(t2, include_id=False)
        name   | pet | new_value
        Alice  | dog | dog
        Bob    | cat | dog
        Carole | cat | dog
        David  | dog | dog

        Tables obtained by a groupby/reduce scheme always have primary keys:

        >>> import pathway as pw
        >>> t1 = pw.debug.table_from_markdown('''
        ... name   | pet
        ... Alice  | dog
        ... Bob    | cat
        ... Carole | cat
        ... David  | cat
        ... ''')
        >>> t2 = t1.groupby(pw.this.pet).reduce(pw.this.pet, count=pw.reducers.count())
        >>> t3 = t1.select(*pw.this, new_value=t2.ix_ref(t1.pet).count)
        >>> pw.debug.compute_and_print(t3, include_id=False)
        name   | pet | new_value
        Alice  | dog | 1
        Bob    | cat | 3
        Carole | cat | 3
        David  | cat | 3

        Single-row tables can be accessed via `ix_ref()`:

        >>> import pathway as pw
        >>> t1 = pw.debug.table_from_markdown('''
        ... name   | pet
        ... Alice  | dog
        ... Bob    | cat
        ... Carole | cat
        ... David  | cat
        ... ''')
        >>> t2 = t1.reduce(count=pw.reducers.count())
        >>> t3 = t1.select(*pw.this, new_value=t2.ix_ref(context=t1).count)
        >>> pw.debug.compute_and_print(t3, include_id=False)
        name   | pet | new_value
        Alice  | dog | 4
        Bob    | cat | 4
        Carole | cat | 4
        David  | cat | 4
        """
        return self.ix(
            self.pointer_from(*args, optional=optional, instance=instance),
            optional=optional,
            context=context,
            allow_misses=allow_misses,
        )

    @trace_user_frame
    @contextualized_operator
    def remove_errors(self) -> Table[TSchema]:
        """Filters out rows that contain errors.

        Example:

        >>> import pathway as pw
        >>> t1 = pw.debug.table_from_markdown(
        ...     '''
        ...     a | b
        ...     3 | 3
        ...     4 | 0
        ...     5 | 5
        ...     6 | 2
        ... '''
        ... )
        >>> t2 = t1.with_columns(x=pw.this.a // pw.this.b)
        >>> res = t2.remove_errors()
        >>> pw.debug.compute_and_print(res, include_id=False, terminate_on_error=False)
        a | b | x
        3 | 3 | 1
        5 | 5 | 1
        6 | 2 | 3
        """
        context = clmn.FilterOutValueContext(self._id_column, api.ERROR)
        return self._table_with_context(context)

    def await_futures(self) -> Table[TSchema]:
        """Waits for the results of asynchronous computation.

        It strips the ``Future`` wrapper from table columns where applicable. In practice,
        it filters out the ``Pending`` values and produces a column with a data type that
        was the argument of `Future`.

        Columns of `Future` data type are produced by fully asynchronous UDFs. Columns of
        this type can be propagated further, but can't be used in most expressions
        (e.g. arithmetic operations). You can wait for their results using this method
        and later use the results in expressions you want.

        Example:

        >>> import pathway as pw
        >>> import asyncio
        >>>
        >>> t = pw.debug.table_from_markdown(
        ...     '''
        ...     a | b
        ...     1 | 2
        ...     3 | 4
        ...     5 | 6
        ... '''
        ... )
        >>>
        >>> @pw.udf(executor=pw.udfs.fully_async_executor())
        ... async def long_running_async_function(a: int, b: int) -> int:
        ...     c = a * b
        ...     await asyncio.sleep(0.1 * c)
        ...     return c
        ...
        >>>
        >>> result = t.with_columns(res=long_running_async_function(pw.this.a, pw.this.b))
        >>> print(result.schema)
        id          | a   | b   | res
        ANY_POINTER | INT | INT | Future(INT)
        >>>
        >>> awaited_result = result.await_futures()
        >>> print(awaited_result.schema)
        id          | a   | b   | res
        ANY_POINTER | INT | INT | INT
        >>> pw.debug.compute_and_print(awaited_result, include_id=False)
        a | b | res
        1 | 2 | 2
        3 | 4 | 12
        5 | 6 | 30
        """
        result = self._await_futures()
        new_dtypes = {}
        for name, column in result._columns.items():
            if isinstance(column.dtype, dt.Future):
                new_dtypes[name] = column.dtype.wrapped
        new_schema = self.schema.with_types(**new_dtypes)
        return result._with_schema(new_schema)

    @trace_user_frame
    @contextualized_operator
    def _await_futures(self) -> Table[TSchema]:
        context = clmn.FilterOutValueContext(self._id_column, api.PENDING)
        return self._table_with_context(context)

    @contextualized_operator
    def _remove_retractions(self) -> Table[TSchema]:
        context = clmn.RemoveRetractionsContext(self._id_column)
        return self._table_with_context(context)

    @contextualized_operator
    def _async_transformer(self, context: clmn.AsyncTransformerContext) -> Table:
        columns = {
            name: clmn.ColumnWithoutExpression(context, context.universe, dtype)
            for name, dtype in context.schema._dtypes().items()
        }
        return Table(_columns=columns, _context=context)

    def _subtables(self) -> StableSet[Table]:
        return StableSet([self])

    def _substitutions(
        self,
    ) -> tuple[Table, dict[expr.InternalColRef, expr.ColumnExpression]]:
        return self, {}

    def typehints(self) -> Mapping[str, Any]:
        """
        Return the types of the columns as a dictionary.

        Example:

        >>> import pathway as pw
        >>> t1 = pw.debug.table_from_markdown('''
        ... age | owner | pet
        ... 10  | Alice | dog
        ... 9   | Bob   | dog
        ... 8   | Alice | cat
        ... 7   | Bob   | dog
        ... ''')
        >>> t1.typehints()
        mappingproxy({'age': <class 'int'>, 'owner': <class 'str'>, 'pet': <class 'str'>})
        """
        return self.schema.typehints()

    def eval_type(self, expression: expr.ColumnExpression) -> dt.DType:
        return (
            self._rowwise_context._get_type_interpreter()
            .eval_expression(expression, state=TypeInterpreterState())
            ._dtype
        )

    def _auto_live(self) -> Table:
        """Make self automatically live in interactive mode"""
        from pathway.internals.interactive import is_interactive_mode_enabled

        if is_interactive_mode_enabled():
            return self.live()
        else:
            return self

    def live(self) -> LiveTable[TSchema]:
        from pathway.internals.interactive import LiveTable

        warnings.warn("live tables are an experimental feature", stacklevel=2)

        return LiveTable._create(self)


@overload
def groupby(
    grouped: Table,
    *args: expr.ColumnReference,
    id: expr.ColumnReference | None = None,
    sort_by: expr.ColumnReference | None = None,
    _filter_out_results_of_forgetting: bool = False,
    instance: expr.ColumnReference | None = None,
) -> groupbys.GroupedTable: ...


@overload
def groupby(
    grouped: JoinResult,
    *args: expr.ColumnReference,
    id: expr.ColumnReference | None = None,
) -> groupbys.GroupedJoinResult: ...


def groupby(
    grouped: Table | JoinResult, *args, id: expr.ColumnReference | None = None, **kwargs
):
    """Groups join result by columns from args.

    Note:
        Usually followed by `.reduce()` that aggregates the result and returns a table.

    Args:
        grouped: ``JoinResult`` to group by.
        args: columns to group by.
        id: if provided, is the column used to set id's of the rows of the result
        **kwargs: extra arguments, see respective documentation for ``Table.groupby`` and ``JoinResult.groupby``

    Returns:
        Groupby object of ``GroupedJoinResult`` or ``GroupedTable`` type.

    Example:

    >>> import pathway as pw
    >>> t1 = pw.debug.table_from_markdown('''
    ... age | owner | pet
    ... 10  | Alice | dog
    ... 9   | Bob   | dog
    ... 8   | Alice | cat
    ... 7   | Bob   | dog
    ... ''')
    >>> t2 = pw.groupby(t1, t1.pet, t1.owner).reduce(
    ...     t1.owner, t1.pet, ageagg=pw.reducers.sum(t1.age)
    ... )
    >>> pw.debug.compute_and_print(t2, include_id=False)
    owner | pet | ageagg
    Alice | cat | 8
    Alice | dog | 10
    Bob   | dog | 16
    >>> t3 = pw.debug.table_from_markdown('''
    ...    cost  owner  pet
    ... 1   100  Alice    1
    ... 2    90    Bob    1
    ... 3    80  Alice    2
    ... ''')
    >>> t4 = pw.debug.table_from_markdown('''
    ...     cost  owner  pet size
    ... 11   100  Alice    3    M
    ... 12    90    Bob    1    L
    ... 13    80    Tom    1   XL
    ... ''')
    >>> join_result = t3.join(t4, t3.owner==t4.owner)
    >>> result = pw.groupby(join_result, pw.this.owner).reduce(
    ...     pw.this.owner, pairs=pw.reducers.count()
    ... )
    >>> pw.debug.compute_and_print(result, include_id=False)
    owner | pairs
    Alice | 2
    Bob   | 1
    """
    return grouped.groupby(*args, id=id, **kwargs)


def _types_lca_with_error(
    *dtypes, function_name: str, pointers: bool, key: str | None = None
):
    try:
        return dt.types_lca_many(
            *dtypes,
            raising=True,
        )
    except TypeError:
        msg = (
            f"Incompatible types for {function_name} operation.\n"
            + f"The types are: {dtypes}. "
            + (f"Affected column: '{key}'. " if key is not None else "")
        )
        if pointers:
            msg += (
                "You might try casting the id type to pw.Pointer to circumvent this, "
                + "but this is most probably an error."
            )
        else:
            msg += (
                "You might try casting the expressions to Any type to circumvent this, "
                + "but this is most probably an error."
            )
        raise TypeError(msg)
