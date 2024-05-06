# Copyright © 2024 Pathway


from __future__ import annotations

import itertools
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

import sqlglot
import sqlglot.expressions as sql_expr
from sqlglot.errors import OptimizeError
from sqlglot.optimizer import qualify_columns

from pathway.internals import expression as expr, if_else, reducers, table, thisclass
from pathway.internals.desugaring import TableSubstitutionDesugaring
from pathway.internals.expression_visitor import IdentityTransform
from pathway.internals.runtime_type_check import check_arg_types
from pathway.internals.shadows import operator

_tmp_table_cnt = itertools.count()

if TYPE_CHECKING:
    ContextType = dict[str, table.Table]


class ReducerDetector(IdentityTransform):
    contains_reducers: bool

    def __init__(self):
        self.contains_reducers = False

    def eval_reducer(
        self, expression: expr.ReducerExpression, **kwargs
    ) -> expr.ReducerExpression:
        self.contains_reducers = True
        return super().eval_reducer(expression, **kwargs)


_expression_handlers: dict[type[sql_expr.Expression], Callable] = {}


def _run(node: sql_expr.Expression, context: ContextType) -> Any:
    handler = _expression_handlers.get(type(node))
    if handler is None:
        raise NotImplementedError(f"{node.sql()} not supported.")
    return handler(node, context)


def register(nodetype):
    def wrapper(func):
        def inner(node, context):
            assert isinstance(node, nodetype), nodetype
            return func(node, context)

        _expression_handlers[nodetype] = inner
        return inner

    return wrapper


@register(nodetype=sql_expr.If)
def _if(
    node: sql_expr.If, context: ContextType
) -> tuple[expr.ColumnExpression, expr.ColumnExpression]:
    return _run(node.this, context), _run(node.args.pop("true"), context)


@register(nodetype=sql_expr.Case)
def _case(node: sql_expr.Case, context: ContextType) -> expr.IfElseExpression:
    args = []
    for arg in node.args.pop("ifs"):
        args.extend(_run(arg, context))

    if (default_field := node.args.pop("default", None)) is not None:
        args.append(_run(default_field, context))
    else:
        args.append(None)
    assert len(args) >= 3, "Wrong number of arguments."
    while len(args) >= 3:
        _else = args.pop()
        _then = args.pop()
        _if = args.pop()
        args.append(if_else(_if, _then, _else))

    [ret] = args

    return ret


@register(nodetype=sql_expr.Between)
def _between(node: sql_expr.Between, context: ContextType) -> expr.ReducerExpression:
    middle = _run(node.this, context)
    low = _run(node.args.pop("low"), context)
    high = _run(node.args.pop("high"), context)
    return (middle >= low) & (middle <= high)


@register(nodetype=sql_expr.Max)
def _max(node: sql_expr.Max, context: ContextType) -> expr.ReducerExpression:
    return reducers.max(_run(node.this, context))


@register(nodetype=sql_expr.Min)
def _min(node: sql_expr.Min, context: ContextType) -> expr.ReducerExpression:
    return reducers.min(_run(node.this, context))


@register(nodetype=sql_expr.Sum)
def _sum(node: sql_expr.Sum, context: ContextType) -> expr.ReducerExpression:
    return reducers.sum(_run(node.this, context))


@register(nodetype=sql_expr.Avg)
def _avg(node: sql_expr.Avg, context: ContextType) -> expr.ColumnExpression:
    return reducers.avg(_run(node.this, context))


@register(nodetype=sql_expr.Count)
def _count(node: sql_expr.Count, context: ContextType) -> expr.ReducerExpression:
    return reducers.count()


@register(nodetype=sql_expr.Group)
def _group(node: sql_expr.Group, context: ContextType) -> list[expr.ColumnExpression]:
    return [_run(e, context) for e in node.expressions]


@register(nodetype=sql_expr.Star)
def _star(node: sql_expr.Star, context: ContextType):
    [ret] = thisclass.this.__iter__()
    return ret


@register(nodetype=sql_expr.Where)
def _where(node: sql_expr.Where, context: ContextType):
    return _run(node.this, context)


@register(nodetype=sql_expr.Having)
def _having(node: sql_expr.Having, context: ContextType):
    return _run(node.this, context)


@register(nodetype=sql_expr.Null)
def _null(node: sql_expr.Null, context: ContextType):
    return None


@register(nodetype=sql_expr.Boolean)
def _boolean(node: sql_expr.Boolean, context: ContextType) -> bool:
    return node.this


@register(nodetype=sql_expr.Identifier)
def _identifier(node: sql_expr.Identifier, context: ContextType):
    return node.this


@register(nodetype=sql_expr.Column)
def _column(node: sql_expr.Column, context: ContextType) -> expr.ColumnReference:
    tab = node.table
    colname = _identifier(node.this, context)
    if tab == "":
        return thisclass.this[colname]
    else:
        return context[tab][colname]


@register(nodetype=sql_expr.With)
def _with(node: sql_expr.With, context: ContextType) -> list[dict[str, table.Table]]:
    return [_cte(e, context) for e in node.expressions]


@register(nodetype=sql_expr.CTE)
def _cte(node: sql_expr.CTE, context: ContextType) -> dict[str, table.Table]:
    ret, context = _run(node.this, context)
    return {node.alias: ret}


@register(nodetype=sql_expr.TableAlias)
def _tablealias(node: sql_expr.TableAlias, context: ContextType):
    return _identifier(node.this, context)


@register(nodetype=sql_expr.Alias)
def _alias(
    node: sql_expr.Alias, context: ContextType
) -> dict[str, expr.ColumnExpression]:
    return {node.alias: _run(node.this, context)}


@register(nodetype=sql_expr.Literal)
def _literal(node: sql_expr.Literal, context: ContextType) -> str | int | float:
    if node.is_string:
        return node.this
    else:
        try:
            return int(node.this)
        except ValueError:
            return float(node.this)


@register(nodetype=sql_expr.Paren)
def _paren(node: sql_expr.Paren, context: ContextType) -> expr.ColumnExpression:
    return _run(node.this, context)


@register(nodetype=sql_expr.Add)
def _add(node: sql_expr.Add, context: ContextType) -> expr.ColumnBinaryOpExpression:
    return _run(node.this, context) + _run(node.expression, context)


@register(nodetype=sql_expr.Div)
def _div(node: sql_expr.Div, context: ContextType) -> expr.ColumnBinaryOpExpression:
    return _run(node.this, context) / _run(node.expression, context)


@register(nodetype=sql_expr.IntDiv)
def _intDiv(
    node: sql_expr.IntDiv, context: ContextType
) -> expr.ColumnBinaryOpExpression:
    return _run(node.this, context) // _run(node.expression, context)


@register(nodetype=sql_expr.Mul)
def _mul(node: sql_expr.Mul, context: ContextType) -> expr.ColumnBinaryOpExpression:
    return _run(node.this, context) * _run(node.expression, context)


@register(nodetype=sql_expr.Sub)
def _sub(node: sql_expr.Sub, context: ContextType) -> expr.ColumnBinaryOpExpression:
    return _run(node.this, context) - _run(node.expression, context)


@register(nodetype=sql_expr.Mod)
def _mod(node: sql_expr.Mod, context: ContextType) -> expr.ColumnBinaryOpExpression:
    return _run(node.this, context) % _run(node.expression, context)


@register(nodetype=sql_expr.Neg)
def _neg(node: sql_expr.Neg, context: ContextType) -> expr.ColumnUnaryOpExpression:
    return -_run(node.this, context)


@register(nodetype=sql_expr.And)
def _and(node: sql_expr.And, context: ContextType) -> expr.ColumnBinaryOpExpression:
    return _run(node.this, context) & _run(node.expression, context)


@register(nodetype=sql_expr.Or)
def _or(node: sql_expr.Or, context: ContextType) -> expr.ColumnBinaryOpExpression:
    return _run(node.this, context) | _run(node.expression, context)


@register(nodetype=sql_expr.Not)
def _not(node: sql_expr.Not, context: ContextType) -> expr.ColumnUnaryOpExpression:
    return ~_run(node.this, context)


@register(nodetype=sql_expr.LT)
def _lt(node: sql_expr.LT, context: ContextType) -> expr.ColumnBinaryOpExpression:
    return _run(node.this, context) < _run(node.expression, context)


@register(nodetype=sql_expr.GT)
def _gt(node: sql_expr.GT, context: ContextType) -> expr.ColumnBinaryOpExpression:
    return _run(node.this, context) > _run(node.expression, context)


@register(nodetype=sql_expr.LTE)
def _lte(node: sql_expr.LTE, context: ContextType) -> expr.ColumnBinaryOpExpression:
    return _run(node.this, context) <= _run(node.expression, context)


@register(nodetype=sql_expr.GTE)
def _gte(node: sql_expr.GTE, context: ContextType) -> expr.ColumnBinaryOpExpression:
    return _run(node.this, context) >= _run(node.expression, context)


@register(nodetype=sql_expr.EQ)
def _eq(node: sql_expr.EQ, context: ContextType) -> expr.ColumnBinaryOpExpression:
    return _run(node.this, context) == _run(node.expression, context)


@register(nodetype=sql_expr.Is)
def _is(node: sql_expr.Is, context: ContextType) -> expr.ColumnBinaryOpExpression:
    return _run(node.this, context) == _run(node.expression, context)


@register(nodetype=sql_expr.NEQ)
def _neq(node: sql_expr.NEQ, context: ContextType) -> expr.ColumnBinaryOpExpression:
    return _run(node.this, context) != _run(node.expression, context)


@register(nodetype=sql_expr.From)
def _from(node: sql_expr.From, context: ContextType) -> tuple[table.Table, ContextType]:
    tabs = []
    for expression in node.expressions:
        tab, context = _run(expression, context)
        tabs.append(tab)
    ret = tabs[0]
    for tab in tabs[1:]:
        ret = ret.join(tab)
    return ret, context


@register(nodetype=sql_expr.Subquery)
def _subquery(
    node: sql_expr.Subquery, context: ContextType
) -> tuple[table.Table, ContextType]:
    context = _with_block(node, context)
    tab, _ = _run(node.args.pop("this"), context)
    tab, context = _alias_block(node, tab, context)
    if node.args.pop("pivots", []) != []:
        raise NotImplementedError("PIVOTS not supported")
    _check_work_done(node)
    return tab, context


@register(nodetype=sql_expr.Table)
def _table(
    node: sql_expr.Table, context: ContextType
) -> tuple[table.Joinable, ContextType]:
    name = _identifier(node.args.pop("this"), context)
    tab = context[name]
    tab, context = _alias_block(node, tab, context)
    joined_tab, context = _joins_block(node, tab, context)

    if node.args.pop("pivots", []) != []:
        raise NotImplementedError("PIVOTS not supported")
    _check_work_done(node)
    return joined_tab, context


@register(nodetype=sql_expr.Union)
def _union(
    node: sql_expr.Union, context: ContextType
) -> tuple[table.Table, ContextType]:
    orig_context = context
    context = _with_block(node, context)
    left, _ = _run(node.args.pop("this"), context)
    right, _ = _run(node.args.pop("expression"), context)
    ret = left.concat_reindex(right)
    if node.args.pop("distinct"):
        ret = ret.groupby(*thisclass.this).reduce(*thisclass.this)
    assert node.args.pop("expressions", []) == []
    _check_work_done(node)
    return ret, orig_context


@register(nodetype=sql_expr.Intersect)
def _intersect(
    node: sql_expr.Intersect, context: ContextType
) -> tuple[table.Table, ContextType]:
    orig_context = context
    context = _with_block(node, context)
    left, _ = _run(node.args.pop("this"), context)
    right, _ = _run(node.args.pop("expression"), context)

    left = left.groupby(*thisclass.this).reduce(*thisclass.this)
    right = right.groupby(*thisclass.this).reduce(*thisclass.this)
    ret = left.intersect(right)
    assert node.args.pop("distinct")
    assert node.args.pop("expressions", []) == []
    _check_work_done(node)
    return ret, orig_context


@register(nodetype=sql_expr.Join)
def _join(node: sql_expr.Join, _context: ContextType) -> Callable:
    def _wrap(left_tab: table.Joinable, context: ContextType):
        right_tab, context = _run(node.args.pop("this"), context)
        assert isinstance(right_tab, table.Joinable)
        if (on_field := node.args.pop("on", None)) is not None:

            def _rec(
                op: sql_expr.And | sql_expr.EQ,
            ) -> list[expr.ColumnBinaryOpExpression]:
                if isinstance(op, sql_expr.And):
                    return _rec(op.this) + _rec(op.expression)
                else:
                    return [_run(op, context)]

            on_all = _rec(on_field)

            def _test(e: expr.ColumnExpression) -> bool:
                if not isinstance(e, expr.ColumnBinaryOpExpression):
                    return False
                if e._operator != operator.eq:
                    return False
                left_side = e._left
                if not isinstance(left_side, expr.ColumnReference):
                    return False
                right_side = e._right
                if not isinstance(right_side, expr.ColumnReference):
                    return False
                return (
                    left_side.table in left_tab._subtables()
                    and right_side.table in right_tab._subtables()
                )

            on = []
            postfilter = []
            for e in on_all:
                if _test(e):
                    on.append(e)
                else:
                    postfilter.append(e)

        elif using_field := node.args.pop("using", None):
            on = []
            for arg in using_field:
                name = _identifier(arg)
            on.append(thisclass.left[name] == thisclass.right[name])
            postfilter = []
        else:
            on = []
            postfilter = []

        node.args.pop("kind", None)
        side = node.args.pop("side", "")
        _check_work_done(node)

        if side == "OUTER":
            # TODO we should properly handle those cases
            assert (
                len(postfilter) == 0
            ), "Not supported ON clause for OUTER JOIN, if possible use WHERE"
            return left_tab.join_outer(right_tab, *on), context
        elif side == "LEFT":
            assert (
                len(postfilter) == 0
            ), "Not supported ON clause for LEFT JOIN, if possible use WHERE"
            return left_tab.join_left(right_tab, *on), context
        elif side == "RIGHT":
            assert (
                len(postfilter) == 0
            ), "Not supported ON clause for RIGHT JOIN, if possible use WHERE"
            return left_tab.join_left(right_tab, *on), context
        else:
            assert side in ["INNER", ""]
            ret = left_tab.join(right_tab, *on)
            for fil in postfilter:
                ret = ret.filter(fil)
            return ret, context

    return _wrap


class _ReducersGatherer(IdentityTransform):
    gathered_reducers: dict[str, expr.ColumnExpression]

    def __init__(self) -> None:
        self.count = itertools.count(0)
        self.gathered_reducers = {}

    def add_expression(self, expression: expr.ColumnExpression) -> expr.ColumnReference:
        name = f"_pw_having_{next(self.count)}"
        self.gathered_reducers[name] = expression
        return thisclass.this[name]

    def eval_column_val(self, expression: expr.ColumnReference, **kwargs):
        if isinstance(expression.table, thisclass.ThisMetaclass):
            return super().eval_column_val(expression, **kwargs)
        else:
            return self.add_expression(expression)

    def eval_reducer(self, expression: expr.ReducerExpression, **kwargs):
        return self.add_expression(expression)


class _HavingHelper(IdentityTransform):
    tab: table.Table

    def __init__(self, tab):
        self.tab = tab

    def eval_column_val(
        self, expression: expr.ColumnReference, **kwargs
    ) -> expr.ColumnReference:
        if isinstance(expression.table, thisclass.ThisMetaclass):
            try:
                return self.tab[expression.name]
            except KeyError:
                pass
        return super().eval_column_val(expression, **kwargs)


def _all_nonnested_subqueries(node):
    def prune_fn(_self, _parent, _key):
        return isinstance(_self, sql_expr.Subquery)

    return [
        _self
        for _self, _parent, _key in node.dfs(prune=prune_fn)
        if prune_fn(_self, _parent, _key)
    ]


# mutates `field`
def _process_field_for_subqueries(field, tab, context, orig_context, agg_fun):
    context_subqueries = {**context}
    tab_joined = tab
    for subquery in _all_nonnested_subqueries(field):
        try:
            subquery_tab, _ = _subquery(subquery, orig_context)
        except KeyError:
            raise SyntaxError("Correlated subqueries not supported.")
        tabname = f"__pathway__tmp__table__name__{next(_tmp_table_cnt)}"
        context_subqueries[tabname] = subquery_tab
        [colexpr] = subquery_tab
        subquery.replace(sqlglot.parse_one(f"{agg_fun}({tabname}.{colexpr.name})"))
        tab_joined = tab_joined.join(subquery_tab, id=tab_joined.id)

    return tab_joined, context_subqueries


@register(nodetype=sql_expr.Select)
def _select(
    node: sql_expr.Select, context: ContextType
) -> tuple[table.Table, ContextType]:
    orig_context = context

    # WITH block
    context = _with_block(node, context)

    # FROM block
    tab, context = _from(node.args.pop("from"), context)

    tab, context = _joins_block(node, tab, context)

    # GROUP block
    if (group_field := node.args.pop("group", None)) is not None:
        groupby = _group(group_field, context)
    else:
        groupby = None

    # args building
    expr_args = []
    expr_kwargs = {}
    for e in node.args.pop("expressions"):
        ret = _run(e, context)
        if isinstance(ret, dict):
            expr_kwargs.update(ret)
        else:
            expr_args.append(ret)

    # WHERE block
    if (where_field := node.args.pop("where", None)) is not None:
        # mutates `where_field`
        tab_joined_where, context_subqueries_where = _process_field_for_subqueries(
            where_field, tab, context, orig_context, ""
        )

        tab_filter_where = tab_joined_where.select(
            filter_col=_where(where_field, context_subqueries_where)
        ).with_universe_of(tab)
        tab_filtered = tab.filter(tab_filter_where.filter_col)
        table_replacer = TableSubstitutionDesugaring({tab: tab_filtered})
        expr_args = [table_replacer.eval_expression(e) for e in expr_args]
        if groupby is not None:
            groupby = [table_replacer.eval_expression(e) for e in groupby]
        expr_kwargs = {
            name: table_replacer.eval_expression(e) for name, e in expr_kwargs.items()
        }
        tab = tab_filtered

    # HAVING block
    if (having_field := node.args.pop("having", None)) is not None:
        if groupby is None:
            groupby = []

    _check_work_done(node)

    # maybe we have implicit GROUP BY
    if groupby is None:
        detector = ReducerDetector()
        for arg in expr_args:
            detector.eval_expression(arg)
        for arg in expr_kwargs.values():
            detector.eval_expression(arg)
        if detector.contains_reducers:
            groupby = []

    if groupby is None:
        result = tab.select(*expr_args, **expr_kwargs)
        return result, orig_context

    if having_field is not None:
        # mutates `having_field`
        tab, context_subqueries_having = _process_field_for_subqueries(
            having_field, tab, context, orig_context, "MIN"
        )
        having_expr = _having(having_field, context_subqueries_having)
        gatherer = _ReducersGatherer()
        having_expr = gatherer.eval_expression(having_expr)
        expr_kwargs = {**expr_kwargs, **gatherer.gathered_reducers}

    grouped = tab.groupby(*groupby)
    result = grouped.reduce(*expr_args, **expr_kwargs)
    if having_field is None:
        return result, orig_context

    having_col = _HavingHelper(result).eval_expression(having_expr)
    result = result.filter(having_col).without(
        *[thisclass.this[name] for name in gatherer.gathered_reducers.keys()]
    )
    return result, orig_context


@check_arg_types
def sql(query: str, **kwargs: table.Table) -> table.Table:
    r'''
    Run a SQL query on Pathway tables.

    Args:
        query: the SQL query to execute.
        kwargs: the association name: table used for the execution of the SQL query. \
            Each name:table pair links a Pathway table to a table name used in the SQL query.

    Example:

    >>> import pathway as pw
    >>> t = pw.debug.table_from_markdown(
    ...     """
    ...       A  | B
    ...       1  | 2
    ...       4  | 3
    ...       4  | 7
    ...     """
    ... )
    >>> ret = pw.sql("SELECT * FROM tab WHERE A<B", tab=t)
    >>> pw.debug.compute_and_print(ret, include_id=False)
    A | B
    1 | 2
    4 | 7

    Supported SQL keywords and operations:
    SELECT, WHERE, boolean expressions, arithmetic operations, \
    GROUP BY, HAVING, AS (alias), UNION, INTERSECTION, JOIN, and WITH.

    Table and column names are case-sensitive.

    Specificities of Pathway:
    - `id` is a reserved key word for columns, every Pathway table has a special column \
    `id`. This column is not captured by `*` expressions in SQL.
    - Order of columns might not be preserved with respect to SELECT query.
    - Pathway reducers (pw.count, pw.sum, etc.) aggregate over None values, while SQL \
    aggregation functions (COUNT, SUM, etc.) skip NULL values.
    - UNION requires matching column names.
    - INTERSECT requires matching column names.

    Limited support:
    - Subqueries are supported but fragile -- they depend on a set of query rewriting routines \
    from the `sqlglot <https://github.com/tobymao/sqlglot>`_ library.
    - Additionally, using the `id` column in subqueries is fragile.
    - LIKE, ANY, ALL, EXISTS are not supported, or only supported in a very weak state.

    Unsupported operations:
    - ordering operations: ORDER BY, LIMIT, SELECT TOP
    - INSERT INTO (Pathway tables are immutable)
    - Pathway does not support anonymous columns: they might work but we do not guarantee their behavior.
    - INTERSECT does not support INTERSECT ALL.
    - COALESCE, IFNULL are not supported.
    - FULL JOIN and NATURAL JOIN are not supported.
    - CAST is not supported

    '''

    kwargs = {name: tab.copy() for name, tab in kwargs.items()}
    root: sql_expr.Expression = sqlglot.parse_one(query)
    if root is None:
        raise RuntimeError(f"parsing {query} failed")
    try:
        root = qualify_columns.qualify_columns(
            root,
            {name: tab.schema.typehints() for name, tab in kwargs.items()},
        )
    except OptimizeError:
        pass
    tab, _ = _run(root, kwargs)
    return tab


def _with_block(node: sql_expr.Expression, context: ContextType) -> ContextType:
    if (with_field := node.args.pop("with", None)) is not None:
        new_context = context.copy()
        for context_update in _with(with_field, context):
            new_context.update(context_update)
        return new_context
    else:
        return context


def _joins_block(
    node: sql_expr.Expression, tab: table.Joinable, context: ContextType
) -> tuple[table.Joinable, ContextType]:
    if (joins_field := node.args.pop("joins", None)) is not None:
        for arg in joins_field:
            fun = _join(arg, context)
            tab, context = fun(tab, context)
    return tab, context


def _alias_block(
    node: sql_expr.Expression, tab: table.Table, context: ContextType
) -> tuple[table.Table, ContextType]:
    if (alias_field := node.args.pop("alias", None)) is not None:
        alias = _run(alias_field, context)
        assert isinstance(alias, str)
        context = context.copy()
        tab = tab.copy()
        context[alias] = tab
    return tab, context


def _check_work_done(node: sql_expr.Expression) -> None:
    for key, obj in node.args.items():
        if obj is None:
            continue
        try:
            repr = obj.sql()
        except AttributeError:
            repr = str(obj)
        raise NotImplementedError(f"{key}: {repr} not supported.")
