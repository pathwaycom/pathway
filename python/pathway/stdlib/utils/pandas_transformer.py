# Copyright © 2024 Pathway

from __future__ import annotations

import pandas as pd

import pathway.internals as pw
from pathway.debug import table_from_pandas
from pathway.internals import schema
from pathway.internals.api import Pointer, ref_scalar
from pathway.internals.helpers import FunctionSpec, function_spec
from pathway.stdlib.utils.col import unpack_col


def _table_columns(table: pw.Table):
    return (table[name] for name in table.column_names())


def _create_input_table(*input: pw.Table):
    def as_tuple(*args):
        return args

    result = []
    for idx, table in enumerate(input):
        tupled_cols = table.select(
            all_cols=pw.apply(as_tuple, table.id, *_table_columns(table))
        )
        reduced = tupled_cols.reduce(
            **{f"_{idx}": pw.reducers.sorted_tuple(tupled_cols.all_cols)}
        )
        result.append(reduced)

    def _add_tables(first: pw.Table, *tables: pw.Table) -> pw.Table:
        for table in tables:
            first += table.with_universe_of(first)
        return first

    return _add_tables(*result)


def _argument_index(func_spec: FunctionSpec, arg: str | int | None) -> int | None:
    if isinstance(arg, str):
        try:
            return func_spec.arg_names.index(arg)
        except ValueError:
            raise ValueError(f"wrong output universe. No argument of name: {arg}")
    elif isinstance(arg, int):
        if arg < 0 or arg >= len(func_spec.arg_names):
            raise ValueError("wrong output universe. Index out of range")
    return arg


def _pandas_transformer(
    *inputs: pw.Table,
    func_spec: FunctionSpec,
    output_schema: type[schema.Schema],
    output_universe: str | int | None,
) -> pw.Table:
    output_universe_arg_index = _argument_index(func_spec, output_universe)
    func = func_spec.func

    def process_pandas_output(
        result: pd.DataFrame | pd.Series, pandas_input: list[pd.DataFrame] = []
    ):
        if isinstance(result, pd.Series):
            result = pd.DataFrame(result)

        result.columns = output_schema.column_names()  # type: ignore

        if output_universe_arg_index is not None and not result.index.equals(
            pandas_input[output_universe_arg_index].index
        ):
            raise ValueError(
                "resulting universe does not match the universe of the indicated argument"
            )
        else:
            if not result.index.is_unique:
                raise ValueError("index of resulting DataFrame must be unique")
            index_as_series = result.index.to_series()
            if not index_as_series.map(lambda x: isinstance(x, Pointer)).all():
                new_index = index_as_series.map(lambda x: ref_scalar(x))
                result.reindex(new_index)
            assert result.index.is_unique

        return result

    if len(func_spec.arg_names) == 0:
        result = func()
        result = process_pandas_output(result)
        output = table_from_pandas(result)
    else:
        input_table = _create_input_table(*inputs)

        def wrapper(*input_tables, inputs=inputs):
            pandas_input = []
            for idx, table in enumerate(input_tables):
                df = pd.DataFrame(table)
                df.set_index(df.columns[0], inplace=True)
                df.columns = inputs[idx].schema.column_names()
                pandas_input.append(df)

            result = func(*pandas_input)
            result = process_pandas_output(result, pandas_input)
            result.insert(0, "_id", result.index)
            return tuple(result.apply(tuple, axis=1))

        applied = input_table.select(
            all_cols=pw.apply(wrapper, *_table_columns(input_table))
        )
        flattened = applied.flatten(pw.this.all_cols)
        output = unpack_col(
            flattened.all_cols, pw.this._id, *output_schema.column_names()
        ).update_types(_id=pw.Pointer)
        output = output.with_id(output._id).without(pw.this._id)

    if output_universe_arg_index is not None:
        output = output.with_universe_of(inputs[output_universe_arg_index])

    output = output.update_types(**output_schema.typehints())

    return output


def pandas_transformer(
    output_schema: type[schema.Schema], output_universe: str | int | None = None
):
    """Decorator that turns python function operating on pandas.DataFrame into pathway transformer.

    Input universes are converted into input DataFrame indexes.
    The resulting index is treated as the output universe, so it must maintain uniqueness
    and be of integer type.

    Args:
        output_schema: Schema of a resulting table.
        output_universe: Index or name of an argument whose universe will be used \
        in resulting table. Defaults to `None`.
    Returns:
        Transformer that can be applied on Pathway tables.


    Example:

    >>> import pathway as pw
    >>> input = pw.debug.table_from_markdown(
    ...     '''
    ...     | foo  | bar
    ... 0   | 10   | 100
    ... 1   | 20   | 200
    ... 2   | 30   | 300
    ... '''
    ... )
    >>> class Output(pw.Schema):
    ...     sum: int
    >>> @pw.pandas_transformer(output_schema=Output)
    ... def sum_cols(t: pd.DataFrame) -> pd.DataFrame:
    ...     return pd.DataFrame(t.sum(axis=1))
    >>> output = sum_cols(input)
    >>> pw.debug.compute_and_print(output, include_id=False)
    sum
    110
    220
    330
    """

    def decorator(func):
        func_specs = function_spec(func)

        def wrapper(*args):
            return _pandas_transformer(
                *args,
                func_spec=func_specs,
                output_schema=output_schema,
                output_universe=output_universe,
            )

        return wrapper

    return decorator
