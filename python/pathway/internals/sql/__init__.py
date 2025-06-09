from pathway.internals import table
from pathway.internals.runtime_type_check import check_arg_types
from pathway.optional_import import optional_imports


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

    with optional_imports("sql"):
        import sqlglot
        import sqlglot.expressions as sql_expr
        from sqlglot.errors import OptimizeError
        from sqlglot.optimizer import qualify_columns

        from pathway.internals.sql.processing import _run

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
