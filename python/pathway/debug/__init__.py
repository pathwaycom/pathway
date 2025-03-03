# Copyright © 2024 Pathway
"""Methods and classes for debugging Pathway computation.

Typical use:

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
from __future__ import annotations

import functools
import io
import itertools
import re
from collections.abc import Iterable
from os import PathLike
from warnings import warn

import pandas as pd

from pathway import persistence
from pathway.internals import Json, api, parse_graph
from pathway.internals.config import get_pathway_config
from pathway.internals.datasource import DataSourceOptions, PandasDataSource
from pathway.internals.fingerprints import fingerprint
from pathway.internals.graph_runner import GraphRunner
from pathway.internals.monitoring import MonitoringLevel
from pathway.internals.runtime_type_check import check_arg_types
from pathway.internals.schema import Schema, schema_from_pandas
from pathway.internals.table import Table
from pathway.internals.table_io import table_from_datasource
from pathway.internals.trace import trace_user_frame
from pathway.io._utils import read_schema
from pathway.io.python import ConnectorSubject, read


@check_arg_types
def _compute_tables(
    *tables: Table, _stacklevel: int = 1, **kwargs
) -> list[api.CapturedStream]:
    captured = GraphRunner(
        parse_graph.G,
        debug=True,
        monitoring_level=MonitoringLevel.NONE,
        _stacklevel=_stacklevel + 1,
        **kwargs,
    ).run_tables(*tables)
    return captured


def table_to_dicts(
    table: Table,
    **kwargs,
) -> tuple[list[api.Pointer], dict[str, dict[api.Pointer, api.Value]]]:
    """
    Runs the computations needed to get the contents of the Pathway Table and converts it
    to a dictionary representation, where each column is mapped to its respective values.

    Args:
        table (Table): The Pathway Table to be converted.
        **kwargs: Additional keyword arguments to customize the behavior of the function.
                  - terminate_on_error (bool): If True, the function will terminate execution upon
                  encountering an error during the squashing of updates. Defaults to True.

    Returns:
        tuple: A tuple containing two elements: 1) list of keys (pointers) that represent
        the rows in the table, and 2) a dictionary where each key is a column name,
        and the value is another dictionary mapping row keys (pointers) to their respective
        values in that column.
    """
    captured = _compute_tables(table, **kwargs)[0]
    output_data = api.squash_updates(
        captured, terminate_on_error=kwargs.get("terminate_on_error", True)
    )
    keys = list(output_data.keys())
    columns = {
        name: {key: output_data[key][index] for key in keys}
        for index, name in enumerate(table._columns.keys())
    }
    return keys, columns


@functools.total_ordering
class _NoneAwareComparisonWrapper:
    def __init__(self, inner):
        if isinstance(inner, dict | Json):
            self.inner = str(inner)
        else:
            self.inner = inner

    def __eq__(self, other):
        if not isinstance(other, _NoneAwareComparisonWrapper):
            return NotImplemented
        return self.inner == other.inner

    def __lt__(self, other):
        if not isinstance(other, _NoneAwareComparisonWrapper):
            return NotImplemented
        if self.inner is None:
            return other.inner is not None
        if other.inner is None:
            return False
        return self.inner < other.inner


def _compute_and_print_internal(
    *tables: Table,
    squash_updates: bool,
    include_id: bool,
    short_pointers: bool,
    n_rows: int | None,
    _stacklevel: int = 1,
    **kwargs,
) -> None:
    captured = _compute_tables(*tables, _stacklevel=_stacklevel + 1, **kwargs)
    if get_pathway_config().process_id != "0":
        return
    for table, captured_single in zip(tables, captured, strict=True):
        _compute_and_print_single(
            table,
            captured_single,
            squash_updates=squash_updates,
            include_id=include_id,
            short_pointers=short_pointers,
            n_rows=n_rows,
            terminate_on_error=kwargs.get("terminate_on_error", True),
        )


def _compute_and_print_single(
    table: Table,
    captured: api.CapturedStream,
    *,
    squash_updates: bool,
    include_id: bool,
    short_pointers: bool,
    n_rows: int | None,
    terminate_on_error: bool,
) -> None:
    columns = list(table._columns.keys())
    if squash_updates:
        output_data = list(
            api.squash_updates(captured, terminate_on_error=terminate_on_error).items()
        )
    else:
        columns.extend([api.TIME_PSEUDOCOLUMN, api.DIFF_PSEUDOCOLUMN])
        output_data = []
        for row in captured:
            output_data.append((row.key, tuple(row.values) + (row.time, row.diff)))

    if not columns and not include_id:
        return

    if include_id or len(columns) > 1:
        none = ""
    else:
        none = "None"

    def _format(x):
        if x is None:
            return none
        if isinstance(x, api.Pointer) and short_pointers:
            s = str(x)
            if len(s) > 8:
                s = s[:8] + "..."
            return s
        return str(x)

    if squash_updates:

        def _key(row: tuple[api.Pointer, tuple[api.Value, ...]]):
            return tuple(_NoneAwareComparisonWrapper(value) for value in row[1]) + (
                row[0],
            )

    else:
        # sort by time and diff first if there is no squashing
        def _key(row: tuple[api.Pointer, tuple[api.Value, ...]]):
            return (
                row[1][-2:]
                + tuple(_NoneAwareComparisonWrapper(value) for value in row[1])
                + (row[0],)
            )

    try:
        output_data = sorted(output_data, key=_key)
    except (ValueError, TypeError):
        pass  # Some values (like arrays, PyObjectWrapper) cannot be sorted this way, so just don't sort them.
    output_data_truncated = itertools.islice(output_data, n_rows)
    data = []
    if include_id:
        name = "" if columns else "id"
        data.append([name] + columns)
    else:
        data.append(columns)
    for key, values in output_data_truncated:
        formatted_row = []
        if include_id:
            formatted_row.append(_format(key))
        formatted_row.extend(_format(value) for value in values)
        data.append(formatted_row)
    max_lens = [max(len(row[i]) for row in data) for i in range(len(data[0]))]
    max_lens[-1] = 0
    for formatted_row in data:
        formatted = " | ".join(
            value.ljust(max_len) for value, max_len in zip(formatted_row, max_lens)
        )
        print(formatted.rstrip())


@check_arg_types
@trace_user_frame
def compute_and_print(
    *tables: Table,
    include_id=True,
    short_pointers=True,
    n_rows: int | None = None,
    **kwargs,
) -> None:
    """A function running the computations and printing the table.

    Args:
        tables: tables to be computed and printed
        include_id: whether to show ids of rows
        short_pointers: whether to shorten printed ids
        n_rows: number of rows to print, if None whole table will be printed
    """
    _compute_and_print_internal(
        *tables,
        squash_updates=True,
        include_id=include_id,
        short_pointers=short_pointers,
        n_rows=n_rows,
        _stacklevel=7,
        **kwargs,
    )


@check_arg_types
@trace_user_frame
def compute_and_print_update_stream(
    *tables: Table,
    include_id=True,
    short_pointers=True,
    n_rows: int | None = None,
    **kwargs,
) -> None:
    """A function running the computations and printing the update stream of the table.

    Args:
        tables: tables for which the update stream is to be computed and printed
        include_id: whether to show ids of rows
        short_pointers: whether to shorten printed ids
        n_rows: number of rows to print, if None whole update stream will be printed
    """
    _compute_and_print_internal(
        *tables,
        squash_updates=False,
        include_id=include_id,
        short_pointers=short_pointers,
        n_rows=n_rows,
        _stacklevel=7,
        **kwargs,
    )


def _dtype_to_pandas(dtype):
    if dtype == (int | None):
        return pd.Int64Dtype()
    else:
        return None


@check_arg_types
@trace_user_frame
def table_to_pandas(table: Table, *, include_id: bool = True):
    keys, columns = table_to_dicts(table)
    series_dict = {}
    for name in columns:
        dtype = _dtype_to_pandas(table.schema.typehints()[name])
        if include_id:
            series = pd.Series(columns[name], dtype=dtype)
        else:
            # we need to remove original keys, otherwise pandas will use them to create index
            series = pd.Series(list(columns[name].values()), dtype=dtype)
        series_dict[name] = series
    index = keys if include_id else None
    res = pd.DataFrame(series_dict, index=index)
    return res


def _validate_dataframe(df: pd.DataFrame, stacklevel: int = 1) -> None:
    for pseudocolumn in api.PANDAS_PSEUDOCOLUMNS:
        if pseudocolumn in df.columns:
            if not pd.api.types.is_integer_dtype(df[pseudocolumn].dtype):
                raise ValueError(f"Column {pseudocolumn} has to contain integers only.")
    if api.TIME_PSEUDOCOLUMN in df.columns:
        if any(df[api.TIME_PSEUDOCOLUMN] < 0):
            raise ValueError(
                f"Column {api.TIME_PSEUDOCOLUMN} cannot contain negative times."
            )
        if any(df[api.TIME_PSEUDOCOLUMN] % 2 == 1):
            warn(
                "timestamps are required to be even; all timestamps will be doubled",
                stacklevel=stacklevel + 1,
            )
            df[api.TIME_PSEUDOCOLUMN] = 2 * df[api.TIME_PSEUDOCOLUMN]

    if api.DIFF_PSEUDOCOLUMN in df.columns:
        if any((df[api.DIFF_PSEUDOCOLUMN] != 1) & (df[api.DIFF_PSEUDOCOLUMN] != -1)):
            raise ValueError(
                f"Column {api.DIFF_PSEUDOCOLUMN} can only have 1 and -1 values."
            )


@check_arg_types
@trace_user_frame
def table_from_rows(
    schema: type[Schema],
    rows: list[tuple],
    unsafe_trusted_ids: bool = False,
    is_stream=False,
) -> Table:
    """A function for creating a table from a list of tuples. Each tuple should describe
    one row of the input data (or stream), matching provided schema.

    If ``is_stream`` is set to ``True``, each tuple representing a row should contain
    two additional columns, the first indicating the time of arrival of particular row
    and the second indicating whether the row should be inserted (1) or deleted (-1).
    """

    kwargs: dict[str, list] = {}
    colnames = schema.column_names()
    if is_stream:
        colnames += ["__time__", "__diff__"]
    for colname in colnames:
        kwargs[colname] = []
    for row in rows:
        for colname, entry in zip(colnames, list(row)):
            kwargs[colname].append(entry)
    df = pd.DataFrame.from_dict(kwargs)
    return table_from_pandas(
        df, unsafe_trusted_ids=unsafe_trusted_ids, schema=schema, _stacklevel=5
    )


@check_arg_types
@trace_user_frame
def table_from_pandas(
    df: pd.DataFrame,
    id_from: list[str] | None = None,
    unsafe_trusted_ids: bool = False,
    schema: type[Schema] | None = None,
    _stacklevel: int = 1,
    _new_universe: bool = False,
) -> Table:
    """A function for creating a table from a pandas DataFrame. If it contains a special
    column ``__time__``, rows will be split into batches with timestamps from the column.
    A special column ``__diff__`` can be used to set an event type - with ``1`` treated
    as inserting the row and ``-1`` as removing it.
    """
    if id_from is not None and schema is not None:
        raise ValueError("parameters `schema` and `id_from` are mutually exclusive")

    ordinary_columns_names = [
        column for column in df.columns if column not in api.PANDAS_PSEUDOCOLUMNS
    ]
    if schema is None:
        schema = schema_from_pandas(
            df, id_from=id_from, exclude_columns=api.PANDAS_PSEUDOCOLUMNS
        )
    elif set(ordinary_columns_names) != set(schema.column_names()):
        raise ValueError("schema does not match given dataframe")

    _validate_dataframe(df, stacklevel=_stacklevel + 4)

    if id_from is None and schema is not None:
        id_from = schema.primary_key_columns()

    if id_from is None:
        ids_df = pd.DataFrame({"id": df.index})
        ids_df.index = df.index
    else:
        ids_df = df[id_from].copy()

    for column in api.PANDAS_PSEUDOCOLUMNS:
        if column in df.columns:
            ids_df[column] = df[column]

    as_hashes = [fingerprint(x) for x in ids_df.to_dict(orient="records")]
    key = fingerprint((unsafe_trusted_ids, sorted(as_hashes)))

    ret: Table = table_from_datasource(
        PandasDataSource(
            schema=schema,
            data=df.copy(),
            data_source_options=DataSourceOptions(
                unsafe_trusted_ids=unsafe_trusted_ids,
            ),
        )
    )
    from pathway.internals.parse_graph import G

    if not _new_universe:
        if key in G.static_tables_cache:
            ret = ret.with_universe_of(G.static_tables_cache[key])
        else:
            G.static_tables_cache[key] = ret

    return ret


def _markdown_to_pandas(table_def: str, split_on_whitespace: bool = True):
    table_def = table_def.lstrip("\n")
    if split_on_whitespace:
        sep = r"(?:\s*\|\s*)|\s+"
    else:
        sep = r"\s*\|\s*"
    header = table_def.partition("\n")[0].strip()
    column_names = re.split(sep, header)
    for index, name in enumerate(column_names):
        if name in ("", "id"):
            index_col = index
            break
    else:
        index_col = None
    return pd.read_table(
        io.StringIO(table_def),
        sep=sep,
        index_col=index_col,
        engine="python",
        na_values=("", "None", "NaN", "nan", "NA", "NULL"),
        keep_default_na=False,
    ).convert_dtypes()


def table_from_markdown(
    table_def,
    id_from=None,
    unsafe_trusted_ids=False,
    schema: type[Schema] | None = None,
    *,
    _stacklevel: int = 1,
    split_on_whitespace: bool = True,
    _new_universe: bool = False,
) -> Table:
    """A function for creating a table from its definition in markdown. If it contains a special
    column ``__time__``, rows will be split into batches with timestamps from the column.
    A special column ``__diff__`` can be used to set an event type - with ``1`` treated
    as inserting the row and ``-1`` as removing it.

    By default, it splits on whitespaces. To get a table containing
    strings with whitespaces, use with ``split_on_whitespace = False``.
    """
    df = _markdown_to_pandas(table_def, split_on_whitespace)
    return table_from_pandas(
        df,
        id_from=id_from,
        unsafe_trusted_ids=unsafe_trusted_ids,
        schema=schema,
        _stacklevel=_stacklevel + 1,
        _new_universe=_new_universe,
    )


@check_arg_types
def table_from_parquet(
    path: str | PathLike,
    id_from=None,
    unsafe_trusted_ids=False,
    _stacklevel: int = 1,
) -> Table:
    """
    Reads a Parquet file into a pandas DataFrame and then converts that into a Pathway table.
    """

    df = pd.read_parquet(path)
    return table_from_pandas(
        df, id_from=None, unsafe_trusted_ids=False, _stacklevel=_stacklevel + 3
    )


@check_arg_types
def table_to_parquet(table: Table, filename: str | PathLike):
    """
    Converts a Pathway Table into a pandas DataFrame and then writes it to Parquet
    """
    df = table_to_pandas(table)
    df = df.reset_index()
    df = df.drop(["index"], axis=1)
    return df.to_parquet(filename)


class _EmptyConnectorSubject(ConnectorSubject):
    def run(self):
        pass


class StreamGenerator:
    _unique_name = itertools.count()
    events: dict[tuple[str, int], list[api.SnapshotEvent]] = {}

    def _get_next_unique_name(self) -> str:
        return str(f"_stream_generator_{next(self._unique_name)}")

    def _advance_time_for_all_workers(
        self, unique_name: str, workers: Iterable[int], timestamp: int
    ):
        for worker in workers:
            self.events[(unique_name, worker)].append(
                api.SnapshotEvent.advance_time(timestamp)
            )

    def _table_from_dict(
        self,
        batches: dict[int, dict[int, list[tuple[int, api.Pointer, list[api.Value]]]]],
        schema: type[Schema],
        _stacklevel: int = 1,
    ) -> Table:
        """A function that creates a table from a mapping of timestamps to batches. Each batch
        is a mapping from worker id to list of rows processed in this batch by this worker,
        and each row is tuple (diff, key, values).

        Note: unless you need to specify timestamps and keys, consider using
        `table_from_list_of_batches` and `table_from_list_of_batches_by_workers`.

        Args:
            batches: dictionary with specified batches to be put in the table
            schema: schema of the table
        """
        unique_name = self._get_next_unique_name()
        workers = {worker for batch in batches.values() for worker in batch}
        for worker in workers:
            self.events[(unique_name, worker)] = []

        timestamps = set(batches.keys())

        if any(timestamp for timestamp in timestamps if timestamp < 0):
            raise ValueError("negative timestamp cannot be used")
        elif any(timestamp for timestamp in timestamps if timestamp == 0):
            warn(
                "rows with timestamp 0 are only backfilled and are not processed by output connectors"
            )

        if any(timestamp for timestamp in timestamps if timestamp % 2 == 1):
            warn(
                "timestamps are required to be even; all timestamps will be doubled",
                stacklevel=_stacklevel + 1,
            )
            batches = {2 * timestamp: batches[timestamp] for timestamp in batches}

        for timestamp in sorted(batches):
            self._advance_time_for_all_workers(unique_name, workers, timestamp)
            batch = batches[timestamp]
            for worker, changes in batch.items():
                for diff, key, values in changes:
                    if diff == 1:
                        event = api.SnapshotEvent.insert(key, values)
                        self.events[(unique_name, worker)] += [event] * diff
                    elif diff == -1:
                        event = api.SnapshotEvent.delete(key, values)
                        self.events[(unique_name, worker)] += [event] * (-diff)
                    else:
                        raise ValueError("only diffs of 1 and -1 are supported")

        return read(
            _EmptyConnectorSubject(datasource_name="debug.stream-generator"),
            name=unique_name,
            schema=schema,
        )

    def table_from_list_of_batches_by_workers(
        self,
        batches: list[dict[int, list[dict[str, api.Value]]]],
        schema: type[Schema],
        _stacklevel: int = 1,
    ) -> Table:
        """A function that creates a table from a list of batches, where each batch is a mapping
        from worker id to a list of rows processed by this worker in this batch.
        Each row is a mapping from column name to a value.

        Args:
            batches: list of batches to be put in the table
            schema: schema of the table
        """
        key = itertools.count()
        schema, api_schema = read_schema(schema)
        value_fields: list[api.ValueField] = api_schema["value_fields"]

        def next_key() -> api.Pointer:
            api_key = api.ref_scalar(next(key))
            return api_key

        def add_diffs_and_keys(list_of_values: list[dict[str, api.Value]]):
            return [
                (1, next_key(), [values[field.name] for field in value_fields])
                for values in list_of_values
            ]

        formatted_batches: dict[
            int, dict[int, list[tuple[int, api.Pointer, list[api.Value]]]]
        ] = {}
        timestamp = itertools.count(2, 2)

        for batch in batches:
            changes = {worker: add_diffs_and_keys(batch[worker]) for worker in batch}
            formatted_batches[next(timestamp)] = changes

        return self._table_from_dict(
            formatted_batches, schema, _stacklevel=_stacklevel + 1
        )

    def table_from_list_of_batches(
        self,
        batches: list[list[dict[str, api.Value]]],
        schema: type[Schema],
        _stacklevel: int = 1,
    ) -> Table:
        """A function that creates a table from a list of batches, where each batch is a list of
        rows in this batch. Each row is a mapping from column name to a value.

        Args:
            batches: list of batches to be put in the table
            schema: schema of the table
        """
        batches_by_worker = [{0: batch} for batch in batches]
        return self.table_from_list_of_batches_by_workers(
            batches_by_worker, schema, _stacklevel=_stacklevel + 1
        )

    def table_from_pandas(
        self,
        df: pd.DataFrame,
        id_from: list[str] | None = None,
        unsafe_trusted_ids: bool = False,
        schema: type[Schema] | None = None,
        _stacklevel: int = 1,
    ) -> Table:
        """A function for creating a table from a pandas DataFrame. If the DataFrame
        contains a column ``_time``, rows will be split into batches with timestamps from ``_time`` column.
        Then ``_worker`` column will be interpreted as the id of a worker which will process the row and
        ``_diff`` column as an event type with ``1`` treated as inserting row and ``-1`` as removing.
        """
        if schema is None:
            schema = schema_from_pandas(
                df, exclude_columns={"_time", "_diff", "_worker"}
            )
        schema, api_schema = read_schema(schema)
        value_fields: list[api.ValueField] = api_schema["value_fields"]

        if "_time" not in df:
            df["_time"] = [2] * len(df)
        if "_worker" not in df:
            df["_worker"] = [0] * len(df)
        if "_diff" not in df:
            df["_diff"] = [1] * len(df)

        batches: dict[
            int, dict[int, list[tuple[int, api.Pointer, list[api.Value]]]]
        ] = {}

        ids = api.ids_from_pandas(
            df, api.ConnectorProperties(unsafe_trusted_ids=unsafe_trusted_ids), id_from
        )

        for row_index in range(len(df)):
            row = df.iloc[row_index]
            time = row["_time"]
            key = ids[df.index[row_index]]
            worker = row["_worker"]

            if time not in batches:
                batches[time] = {}

            if worker not in batches[time]:
                batches[time][worker] = []

            values = []
            for value_field in value_fields:
                column = value_field.name
                value = api.denumpify(row[column])
                values.append(value)
            diff = row["_diff"]

            batches[time][worker].append((diff, key, values))

        return self._table_from_dict(batches, schema, _stacklevel=_stacklevel + 1)

    def table_from_markdown(
        self,
        table: str,
        id_from: list[str] | None = None,
        unsafe_trusted_ids: bool = False,
        schema: type[Schema] | None = None,
        _stacklevel: int = 1,
    ) -> Table:
        """A function for creating a table from its definition in markdown. If it
        contains a column ``_time``, rows will be split into batches with timestamps from ``_time`` column.
        Then ``_worker`` column will be interpreted as the id of a worker which will process the row and
        ``_diff`` column as an event type - with ``1`` treated as inserting row and ``-1`` as removing.
        """
        df = _markdown_to_pandas(table)
        return self.table_from_pandas(
            df, id_from, unsafe_trusted_ids, schema, _stacklevel=_stacklevel + 1
        )

    def persistence_config(self) -> persistence.Config | None:
        """Returns a persistece config to be used during run. Needs to be passed to ``pw.run``
        so that tables created using StreamGenerator are filled with data.
        """

        if len(self.events) == 0:
            return None
        return persistence.Config(
            persistence.Backend.mock(self.events),
            snapshot_access=api.SnapshotAccess.REPLAY,
            persistence_mode=api.PersistenceMode.SPEEDRUN_REPLAY,
        )
