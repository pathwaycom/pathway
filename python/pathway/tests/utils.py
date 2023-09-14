# Copyright © 2023 Pathway

from __future__ import annotations

import collections
import multiprocessing
import pathlib
import platform
import re
import sys
import time
from dataclasses import dataclass
from typing import Callable, Tuple, Union

import numpy as np
import pandas as pd
import pytest

import pathway as pw
from pathway.debug import _markdown_to_pandas, parse_to_table, table_from_pandas
from pathway.internals import api, datasource
from pathway.internals.graph_runner import GraphRunner
from pathway.internals.schema import is_subschema, schema_from_columns
from pathway.internals.table import Table

try:
    import numba  # noqa

    _numba_missing = False
except ImportError:
    _numba_missing = True

xfail_on_darwin = pytest.mark.xfail(
    platform.system() == "Darwin",
    reason="can't do pw.run() from custom process on Darwin",
)
xfail_no_numba = pytest.mark.xfail(_numba_missing, reason="unable to import numba")


def assert_equal_tables(t0: api.CapturedTable, t1: api.CapturedTable):
    assert t0 == t1


def make_value_hashable(val: api.Value):
    if isinstance(val, np.ndarray):
        return (type(val), val.dtype, val.shape, str(val))
    else:
        return val


def make_row_hashable(row: Tuple[api.Value, ...]):
    return tuple(make_value_hashable(val) for val in row)


def assert_equal_tables_wo_index(t0: api.CapturedTable, t1: api.CapturedTable):
    assert collections.Counter(
        make_row_hashable(row) for row in t0.values()
    ) == collections.Counter(make_row_hashable(row) for row in t1.values())


class CsvLinesNumberChecker:
    def __init__(self, path, n_lines):
        self.path = path
        self.n_lines = n_lines

    def __call__(self):
        result = pd.read_csv(self.path).sort_index()
        return len(result) == self.n_lines


def expect_csv_checker(expected, output_path, usecols=("k", "v"), index_col=("k")):
    expected = (
        pw.debug._markdown_to_pandas(expected)
        .set_index(index_col, drop=False)
        .sort_index()
    )

    def checker():
        result = (
            pd.read_csv(output_path, usecols=[*usecols, *index_col])
            .convert_dtypes()
            .set_index(index_col, drop=False)
            .sort_index()
        )
        return expected.equals(result)

    return checker


@dataclass(frozen=True)
class TestDataSource(datasource.DataSource):
    __test__ = False


def run_graph_and_validate_result(verifier: Callable, assert_schemas=True):
    def inner(table: Table, expected: Table, **kwargs):
        table_schema_dict = table.schema.as_dict()
        expected_schema_dict = expected.schema.as_dict()
        columns_schema_dict = schema_from_columns(table._columns).as_dict()
        if assert_schemas:
            if columns_schema_dict != table_schema_dict:
                raise RuntimeError(
                    f"Output schema validation error, columns {columns_schema_dict} vs table {table_schema_dict}"  # noqa
                )

            if not (
                is_subschema(table.schema, expected.schema)
                and is_subschema(expected.schema, table.schema)
            ):
                raise RuntimeError(
                    f"Output schema validation error, table {table_schema_dict} vs expected {expected_schema_dict}"  # noqa
                )
        else:
            assert columns_schema_dict != table_schema_dict or not (
                is_subschema(table.schema, expected.schema)
                and is_subschema(expected.schema, table.schema)
            ), "wo_types is not needed"

        if list(table.column_names()) != list(expected.column_names()):
            raise RuntimeError(
                f"Mismatched column names, {list(table.column_names())} vs {list(expected.column_names())}"
            )

        print("We will do GraphRunner with the following kwargs: ", kwargs)

        [captured_table, captured_expected] = GraphRunner(
            table._source.graph, monitoring_level=pw.MonitoringLevel.NONE, **kwargs
        ).run_tables(table, expected)
        return verifier(captured_table, captured_expected)

    return inner


def T(*args, format="markdown", **kwargs):
    if format == "pandas":
        return table_from_pandas(*args, **kwargs)
    assert format == "markdown"
    return parse_to_table(*args, **kwargs)


def remove_ansi_escape_codes(msg: str) -> str:
    """Removes color codes from messages."""
    # taken from https://stackoverflow.com/a/14693789
    return re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])").sub("", msg)


assert_table_equality = run_graph_and_validate_result(assert_equal_tables)

assert_table_equality_wo_index = run_graph_and_validate_result(
    assert_equal_tables_wo_index
)

assert_table_equality_wo_types = run_graph_and_validate_result(
    assert_equal_tables, assert_schemas=False
)

assert_table_equality_wo_index_types = run_graph_and_validate_result(
    assert_equal_tables_wo_index, assert_schemas=False
)


def run(**kwargs):
    kwargs.setdefault("debug", True)
    kwargs.setdefault("monitoring_level", pw.MonitoringLevel.NONE)
    pw.run(**kwargs)


def run_all(**kwargs):
    kwargs.setdefault("debug", True)
    kwargs.setdefault("monitoring_level", pw.MonitoringLevel.NONE)
    pw.run_all(**kwargs)


def wait_result_with_checker(checker, timeout_sec, target=run, args=(), kwargs={}):
    p = multiprocessing.Process(target=target, args=args, kwargs=kwargs)
    p.start()
    started_at = time.time()

    succeeded = False
    for _ in range(timeout_sec):
        time.sleep(1.0)
        try:
            succeeded = checker()
            if succeeded:
                print(
                    "Correct result obtained after {} seconds".format(
                        time.time() - started_at
                    ),
                    file=sys.stderr,
                )
                break
        except Exception:
            pass

    if "persistence_config" in kwargs:
        time.sleep(5.0)  # allow a little gap to persist state

    p.terminate()
    p.join()

    return succeeded


def write_csv(path: Union[str, pathlib.Path], table_def: str, *, sep=","):
    df = _markdown_to_pandas(table_def)
    df.to_csv(path, sep=sep, encoding="utf-8")


def write_lines(path: Union[str, pathlib.Path], data: str):
    with open(path, "w+") as f:
        f.writelines(data)
