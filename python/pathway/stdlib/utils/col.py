# Copyright © 2024 Pathway
from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import overload

import pathway.internals as pw
from pathway.internals import dtype as dt
from pathway.internals.runtime_type_check import check_arg_types
from pathway.internals.trace import trace_user_frame


@overload
def unpack_col(
    column: pw.ColumnReference, *unpacked_columns: pw.ColumnReference | str
) -> pw.Table: ...


@overload
def unpack_col(
    column: pw.ColumnReference,
    *,
    schema: type[pw.Schema],
) -> pw.Table: ...


@check_arg_types
@trace_user_frame
def unpack_col(
    column: pw.ColumnReference,
    *unpacked_columns: pw.ColumnReference | str,
    schema: type[pw.Schema] | None = None,
) -> pw.Table:
    """Unpacks multiple columns from a single column.

    Arguments unpacked_columns and schema are mutually exclusive

    Input:
    - column: Column expression of column containing some sequences
    - unpacked_columns: list of names of output columns
    - schema: Schema of new columns

    Output:
    - Table with columns named by "unpacked_columns" argument

    Examples:

    >>> import pathway as pw
    >>> t1 = pw.debug.table_from_markdown(
    ... '''
    ...   | colA   | colB | colC
    ... 1 | Alice  | 25   | dog
    ... 2 | Bob    | 32   | cat
    ... 3 | Carole | 28   | dog
    ... ''')
    >>> t2 = t1.select(user = pw.make_tuple(pw.this.colA, pw.this.colB, pw.this.colC))
    >>> pw.debug.compute_and_print(t2, include_id=False)
    user
    ('Alice', 25, 'dog')
    ('Bob', 32, 'cat')
    ('Carole', 28, 'dog')
    >>> unpack_table = pw.utils.col.unpack_col(t2.user, "name", "age", "pet")
    >>> pw.debug.compute_and_print(unpack_table, include_id=False)
    name   | age | pet
    Alice  | 25  | dog
    Bob    | 32  | cat
    Carole | 28  | dog
    >>> class SomeSchema(pw.Schema):
    ...     name: str
    ...     age: int
    ...     pet: str
    >>> unpack_table = pw.utils.col.unpack_col(t2.user, schema=SomeSchema)
    >>> pw.debug.compute_and_print(unpack_table, include_id=False)
    name   | age | pet
    Alice  | 25  | dog
    Bob    | 32  | cat
    Carole | 28  | dog
    """

    if (schema is None) == (len(unpacked_columns) == 0):
        raise ValueError(
            "exactly one of the parameters `schema` or `unpacked_columns` must be provided"
        )

    if schema is not None:
        unpacked_columns = tuple(schema.column_names())
    colrefs = [pw.this[unpacked_column] for unpacked_column in unpacked_columns]
    kw = {colref.name: column[i] for i, colref in enumerate(colrefs)}
    result = column.table.select(**kw)
    if schema is not None:
        result = result.update_types(**schema.typehints())
    return result


@check_arg_types
@trace_user_frame
def unpack_col_dict(
    column: pw.ColumnReference,
    schema: type[pw.Schema],
) -> pw.Table:
    """Unpacks columns from a json object

    Input:
    - column: Column expression of column containing some pw.Json with an object
    - schema: Schema for columns to extract

    Output:
    - Table with columns given by the schema

    Example:
    >>> import pathway as pw
    >>> t = pw.debug.table_from_rows(
    ...     schema=pw.schema_from_types(data=pw.Json),
    ...     rows=[
    ...         ({"field_a": 13, "field_b": "foo", "field_c": False},),
    ...         ({"field_a": 17, "field_c": True, "field_d": 3.4},)
    ...     ]
    ... )
    >>> class DataSchema(pw.Schema):
    ...     field_a: int
    ...     field_b: str | None
    ...     field_c: bool
    ...     field_d: float | None
    >>> t2 = pw.utils.col.unpack_col_dict(t.data, schema=DataSchema)
    >>> pw.debug.compute_and_print(t2, include_id=False)
    field_a | field_b | field_c | field_d
    13      | foo     | False   |
    17      |         | True    | 3.4
    """
    typehints = schema._dtypes()

    def _convert_from_json(name: str, col: pw.ColumnExpression):
        _type = dt.unoptionalize(typehints[name])
        is_optional = isinstance(typehints[name], dt.Optional)
        result: pw.ColumnExpression

        def _optional(
            col: pw.ColumnExpression,
            op: Callable[[pw.ColumnExpression], pw.ColumnExpression],
        ) -> pw.ColumnExpression:
            if is_optional:
                return pw.if_else(col == pw.Json.NULL, None, op(col))
            else:
                return op(col)

        match _type:
            case dt.JSON:
                result = col
            case dt.BOOL:
                result = col.as_bool()
            case dt.FLOAT:
                result = col.as_float()
            case dt.INT:
                result = col.as_int()
            case dt.STR:
                result = col.as_str()
            case dt.DATE_TIME_NAIVE:
                result = _optional(
                    col,
                    lambda col: pw.unwrap(col.as_str()).dt.strptime(
                        "%Y-%m-%dT%H:%M:%S.%f"
                    ),
                )
            case dt.DATE_TIME_UTC:
                result = _optional(
                    col,
                    lambda col: pw.unwrap(col.as_str()).dt.strptime(
                        "%Y-%m-%dT%H:%M:%S.%f%z"
                    ),
                )
            case dt.DURATION:
                result = _optional(
                    col, lambda col: pw.unwrap(col.as_int()).dt.to_duration("ns")
                )
            case _:
                raise TypeError(
                    f"Unsupported conversion from pw.Json to {typehints[name]}"
                )

        return result if is_optional else pw.unwrap(result)

    colrefs = [pw.this[column_name] for column_name in schema.column_names()]
    kw = {
        colref.name: _convert_from_json(colref.name, column.get(colref.name))
        for colref in colrefs
    }
    result = column.table.select(**kw).update_types(**schema)
    return result


# TODO: generalize to apply on groupby: https://github.com/navalgo/IoT-Pathway/issues/1919
@check_arg_types
@trace_user_frame
def multiapply_all_rows(
    *cols: pw.ColumnReference,
    fun: Callable[..., list[Sequence]],
    result_col_names: list[str | pw.ColumnReference],
) -> pw.Table:
    """Applies a function to all the data in selected columns at once, returning multiple columns.
    This transformer is meant to be run infrequently on a relativelly small tables.

    Input:
    - cols: list of columns to which function will be applied
    - fun: function taking lists of columns and returning a corresponding list of outputs.
    - result_col_names: names of the output columns

    Output:
    - Table indexed with original indices with columns named by "result_col_names" argument
    containing results of the apply

    Example:

    >>> import pathway as pw
    >>> table = pw.debug.table_from_markdown(
    ... '''
    ...   | colA | colB
    ... 1 | 1    | 10
    ... 2 | 2    | 20
    ... 3 | 3    | 30
    ... ''')
    >>> def add_total_sum(col1, col2):
    ...    sum_all = sum(col1) + sum(col2)
    ...    return [x + sum_all for x in col1], [x + sum_all for x in col2]
    >>> result = pw.utils.col.multiapply_all_rows(
    ...    table.colA, table.colB, fun=add_total_sum, result_col_names=["res1", "res2"]
    ... )
    >>> pw.debug.compute_and_print(result, include_id=False)
    res1 | res2
    67   | 76
    68   | 86
    69   | 96
    """
    assert len(cols) > 0
    table = cols[0].table
    assert all([col.table == table for col in cols[1:]])

    def zip_cols(id, *cols):
        return (id, *cols)

    tmp = table.select(id_and_cols=pw.apply(zip_cols, table.id, *cols))
    reduced = tmp.reduce(ids_and_cols=pw.reducers.sorted_tuple(tmp.id_and_cols))

    def fun_wrapped(ids_and_cols):
        ids, *cols = zip(*ids_and_cols)
        res = fun(*cols)
        return tuple(zip(ids, *res))

    applied = reduced.select(ids_and_res=pw.apply(fun_wrapped, reduced.ids_and_cols))
    flatted = applied.flatten(pw.this.ids_and_res)
    result = unpack_col(flatted.ids_and_res, "idd", *result_col_names).update_types(
        idd=pw.Pointer
    )
    result = result.with_id(result.idd).without(pw.this.idd)
    return result.with_universe_of(table)


@check_arg_types
@trace_user_frame
def apply_all_rows(
    *cols: pw.ColumnReference,
    fun: Callable[..., Sequence],
    result_col_name: str | pw.ColumnReference,
) -> pw.Table:
    """Applies a function to all the data in selected columns at once, returning a single column.
    This transformer is meant to be run infrequently on a relativelly small tables.

    Input:
    - cols: list of columns to which function will be applied
    - fun: function taking lists of columns and returning a corresponding list of outputs.
    - result_col_name: name of the output column

    Output:
    - Table indexed with original indices with a single column named by "result_col_name" argument
    containing results of the apply

    Example:

    >>> import pathway as pw
    >>> table = pw.debug.table_from_markdown(
    ... '''
    ...   | colA | colB
    ... 1 | 1    | 10
    ... 2 | 2    | 20
    ... 3 | 3    | 30
    ... ''')
    >>> def add_total_sum(col1, col2):
    ...    sum_all = sum(col1) + sum(col2)
    ...    return [x + sum_all for x in col1]
    >>> result = pw.utils.col.apply_all_rows(
    ...    table.colA, table.colB, fun=add_total_sum, result_col_name="res"
    ... )
    >>> pw.debug.compute_and_print(result, include_id=False)
    res
    67
    68
    69
    """

    def fun_wrapped(*cols):
        return [fun(*cols)]

    return multiapply_all_rows(
        *cols, fun=fun_wrapped, result_col_names=[result_col_name]
    )


@check_arg_types
@trace_user_frame
def groupby_reduce_majority(
    column_group: pw.ColumnReference, column_val: pw.ColumnReference
):
    """Finds a majority in column_val for every group in column_group.

    Workaround for missing majority reducer.

    Example:

    >>> import pathway as pw
    >>> table = pw.debug.table_from_markdown(
    ... '''
    ...   | group | vote
    ... 0 | 1     | pizza
    ... 1 | 1     | pizza
    ... 2 | 1     | hotdog
    ... 3 | 2     | hotdog
    ... 4 | 2     | pasta
    ... 5 | 2     | pasta
    ... 6 | 2     | pasta
    ... ''')
    >>> result = pw.utils.col.groupby_reduce_majority(table.group, table.vote)
    >>> pw.debug.compute_and_print(result, include_id=False)
    group | majority
    1     | pizza
    2     | pasta
    """
    table = column_group.table
    column_val = table[column_val]  # in case its pw.this reference
    column_val_name = column_val.name
    column_group_name = column_group.name
    counts = table.groupby(column_group, column_val).reduce(
        column_group, column_val, _pw_special_count=pw.reducers.count()
    )
    res = counts.groupby(counts[column_group_name]).reduce(
        counts[column_group_name],
        majority=counts.ix(
            pw.reducers.argmax(counts._pw_special_count), context=pw.this
        )[column_val_name],
    )

    return res
