# Copyright © 2023 Pathway

from __future__ import annotations

import collections
import multiprocessing
import os
import pathlib
import platform
import re
import time
from dataclasses import dataclass
from typing import Callable, Type, Union

import pytest

import pathway as pw
from pathway.debug import _markdown_to_pandas, parse_to_table, table_from_pandas
from pathway.internals import api, datasource
from pathway.internals.decorators import table_from_datasource
from pathway.internals.graph_runner import GraphRunner
from pathway.internals.schema import Schema, is_subschema, schema_from_columns
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


def assert_equal_tables_wo_index(t0: api.CapturedTable, t1: api.CapturedTable):
    assert collections.Counter(t0.values()) == collections.Counter(t1.values())


@dataclass(frozen=True)
class TestDataSource(datasource.DataSource):
    __test__ = False

    _schema: Type[Schema]

    @property
    def schema(self) -> Type[Schema]:
        return self._schema


def run_graph_and_validate_result(verifier: Callable, assert_schemas=True):
    def inner(table: Table, expected: Table):
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
        [captured_table, captured_expected] = GraphRunner(
            table._source.graph, monitoring_level=pw.MonitoringLevel.NONE
        ).run_tables(table, expected)
        return verifier(captured_table, captured_expected)

    return inner


def table_from_schema(schema):
    return table_from_datasource(TestDataSource(schema))


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


def wait_result_with_checker(checker, timeout_sec, target=run):
    p = multiprocessing.Process(target=target, args=())
    p.start()

    succeeded = False
    for _ in range(timeout_sec):
        time.sleep(1.0)
        try:
            succeeded = checker()
            if succeeded:
                break
        except Exception:
            pass

    if os.environ.get("PATHWAY_PERSISTENT_STORAGE"):
        time.sleep(3.0)  # allow a little gap to persist state

    p.terminate()
    p.join()

    return succeeded


def write_csv(path: Union[str, pathlib.Path], table_def: str, *, sep=","):
    df = _markdown_to_pandas(table_def)
    df.to_csv(path, sep=sep, encoding="utf-8")


def write_lines(path: Union[str, pathlib.Path], data: str):
    with open(path, "w+") as f:
        f.writelines(data)
