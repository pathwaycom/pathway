# Copyright © 2024 Pathway

import base64
import json
import os
import pathlib
import pickle
import re
import socket
import sqlite3
import sys
import threading
import time
from typing import Any, Optional
from unittest import mock

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest
import yaml
from dateutil import tz
from deltalake import DeltaTable, write_deltalake
from fs import open_fs

import pathway as pw
from pathway.internals import api
from pathway.internals.api import SessionType
from pathway.internals.parse_graph import G
from pathway.io.airbyte.logic import _PathwayAirbyteDestination
from pathway.tests.test_persistence import only_with_license_key
from pathway.tests.utils import (
    AIRBYTE_FAKER_CONNECTION_REL_PATH,
    CountDifferentTimestampsCallback,
    CsvLinesNumberChecker,
    FileLinesNumberChecker,
    T,
    assert_sets_equality_from_path,
    assert_table_equality,
    assert_table_equality_wo_index,
    needs_multiprocessing_fork,
    run,
    run_all,
    wait_result_with_checker,
    write_csv,
    write_lines,
)
from pathway.third_party.airbyte_serverless.sources import (
    DockerAirbyteSource,
    VenvAirbyteSource,
)


def start_streaming_inputs(inputs_path, n_files, stream_interval, data_format):
    os.mkdir(inputs_path)

    def stream_inputs():
        for i in range(n_files):
            file_path = inputs_path / f"{i}.csv"
            if data_format == "json":
                payload = {"k": str(i), "v": i}
                with open(file_path, "w") as streamed_file:
                    json.dump(payload, streamed_file)
            elif data_format == "csv":
                data = """
                    k | v
                    {} | {}
                """.format(
                    i, i
                )
                write_csv(file_path, data)
            elif data_format == "plaintext":
                with open(file_path, "w") as f:
                    f.write(f"{i}")
            else:
                raise ValueError(f"Unknown format: {data_format}")

            time.sleep(stream_interval)

    inputs_thread = threading.Thread(target=stream_inputs, daemon=True)
    inputs_thread.start()


def test_python_connector():
    class TestSubject(pw.io.python.ConnectorSubject):
        def run(self):
            self.next_json({"key": 1, "genus": "upupa", "epithet": "epops"})
            self.next_str(
                json.dumps({"key": 2, "genus": "acherontia", "epithet": "atropos"})
            )
            self.next_bytes(
                json.dumps(
                    {"key": 3, "genus": "bubo", "epithet": "scandiacus"}
                ).encode()
            )

    class InputSchema(pw.Schema):
        key: int = pw.column_definition(primary_key=True)
        genus: str
        epithet: str

    table = pw.io.python.read(
        TestSubject(),
        schema=InputSchema,
    )

    assert_table_equality(
        table,
        T(
            """
                key | genus      | epithet
                1   | upupa      | epops
                2   | acherontia | atropos
                3   | bubo       | scandiacus
            """,
            id_from=["key"],
        ),
    )


def test_python_connector_on_stop():
    class TestSubject(pw.io.python.ConnectorSubject):
        stopped: bool = False

        def run(self):
            pass

        def on_stop(self):
            self.stopped = True

    subject = TestSubject()

    class InputSchema(pw.Schema):
        pass

    pw.io.python.read(subject, schema=InputSchema)
    run_all()

    assert subject.stopped


def test_python_connector_encoding():
    class TestSubject(pw.io.python.ConnectorSubject):
        def run(self):
            self.next_json({"key": 1, "foo": "🙃"})

    class InputSchema(pw.Schema):
        key: int = pw.column_definition(primary_key=True)
        foo: str

    table = pw.io.python.read(
        TestSubject(),
        schema=InputSchema,
    )

    assert_table_equality(
        table,
        T(
            """
                key | foo
                1   | 🙃
            """,
            id_from=["key"],
        ),
    )


def test_python_connector_no_primary_key():
    class InputSchema(pw.Schema):
        x: int
        y: int

    class TestSubject(pw.io.python.ConnectorSubject):
        def run(self):
            self.next_json({"x": 1, "y": 1})
            self.next_json({"x": 2, "y": 2})

    table = pw.io.python.read(TestSubject(), schema=InputSchema, format="json")

    assert_table_equality_wo_index(
        table,
        T(
            """
            x | y
            1 | 1
            2 | 2
            """
        ),
    )


def test_python_connector_raw():
    class TestSubject(pw.io.python.ConnectorSubject):
        def run(self):
            self.next_str("lorem")
            self.next_str("ipsum")

    table = pw.io.python.read(TestSubject(), format="raw")

    assert_table_equality_wo_index(
        table,
        T(
            """
                | data
            1   | lorem
            2   | ipsum
            """
        ),
    )


def test_python_connector_commits(tmp_path: pathlib.Path):
    data = [{"k": 1, "v": "foo"}, {"k": 2, "v": "bar"}, {"k": 3, "v": "baz"}]
    output_path = tmp_path / "output.csv"

    class TestSubject(pw.io.python.ConnectorSubject):
        def run(self):
            for row in data:
                self.next_json(row)

    class InputSchema(pw.Schema):
        k: int = pw.column_definition(primary_key=True)
        v: str

    table = pw.io.python.read(TestSubject(), schema=InputSchema)

    pw.io.csv.write(table, str(output_path))

    run_all()

    result = pd.read_csv(output_path, usecols=["k", "v"], index_col=["k"]).sort_index()
    expected = pd.DataFrame(data).set_index("k")
    assert result.equals(expected)


def test_python_connector_remove():
    class TestSubject(pw.io.python.ConnectorSubject):
        def run(self):
            self._add(
                api.ref_scalar(1),
                json.dumps({"key": 1, "genus": "upupa", "epithet": "epops"}).encode(),
            )
            self._remove(
                api.ref_scalar(1),
                json.dumps({"key": 1, "genus": "upupa", "epithet": "epops"}).encode(),
            )
            self._add(
                api.ref_scalar(3),
                json.dumps(
                    {"key": 3, "genus": "bubo", "epithet": "scandiacus"}
                ).encode(),
            )

    class InputSchema(pw.Schema):
        key: int
        genus: str
        epithet: str

    table = pw.io.python.read(
        TestSubject(),
        schema=InputSchema,
    )

    assert_table_equality_wo_index(
        table,
        T(
            """
                key | genus      | epithet
                3   | bubo       | scandiacus
            """,
        ),
    )


def test_python_connector_deletions_disabled():
    class TestSubject(pw.io.python.ConnectorSubject):
        def run(self):
            self._add(
                api.ref_scalar(1),
                json.dumps({"key": 1, "genus": "upupa", "epithet": "epops"}).encode(),
            )
            self._add(
                api.ref_scalar(3),
                json.dumps(
                    {"key": 3, "genus": "bubo", "epithet": "scandiacus"}
                ).encode(),
            )

        @property
        def _deletions_enabled(self) -> bool:
            return False

    class InputSchema(pw.Schema):
        key: int
        genus: str
        epithet: str

    table = pw.io.python.read(
        TestSubject(),
        schema=InputSchema,
    )

    assert_table_equality_wo_index(
        table,
        T(
            """
                key | genus      | epithet
                1   | upupa      | epops
                3   | bubo       | scandiacus
            """,
        ),
    )


def test_python_connector_deletions_disabled_logs_error_on_delete():
    class TestSubject(pw.io.python.ConnectorSubject):
        def run(self):
            self._add(
                api.ref_scalar(1),
                json.dumps({"key": 1, "genus": "upupa", "epithet": "epops"}).encode(),
            )
            self._remove(
                api.ref_scalar(1),
                json.dumps({"key": 1, "genus": "upupa", "epithet": "epops"}).encode(),
            )
            self._add(
                api.ref_scalar(3),
                json.dumps(
                    {"key": 3, "genus": "bubo", "epithet": "scandiacus"}
                ).encode(),
            )

        @property
        def _deletions_enabled(self) -> bool:
            return False

    class InputSchema(pw.Schema):
        key: int
        genus: str
        epithet: str

    pw.io.python.read(
        TestSubject(),
        schema=InputSchema,
    )

    with pytest.raises(
        ValueError,
        match="Trying to delete a row in .* but deletions_enabled is set to False",
    ):
        run_all()


def test_python_connector_deletions_disabled_logs_error_on_upsert():
    class TestSubject(pw.io.python.ConnectorSubject):
        def run(self):
            self._add(
                api.ref_scalar(1),
                json.dumps({"key": 1, "genus": "upupa", "epithet": "epops"}).encode(),
            )

        @property
        def _deletions_enabled(self) -> bool:
            return False

        @property
        def _session_type(self) -> SessionType:
            return SessionType.UPSERT

    class InputSchema(pw.Schema):
        key: int
        genus: str
        epithet: str

    pw.io.python.read(
        TestSubject(),
        schema=InputSchema,
    )

    with pytest.raises(
        ValueError,
        match=r"Trying to modify a row in .* but deletions_enabled is set to False",
    ):
        run_all()


def test_csv_static_read_write(tmp_path: pathlib.Path):
    data = """
        k | v
        1 | foo
        2 | bar
        3 | baz
    """
    input_path = tmp_path / "input.csv"
    output_path = tmp_path / "output.csv"

    write_csv(input_path, data)

    class InputSchema(pw.Schema):
        k: int = pw.column_definition(primary_key=True)
        v: str

    table = pw.io.csv.read(str(input_path), schema=InputSchema, mode="static")

    pw.io.csv.write(table, str(output_path))

    run_all()

    result = pd.read_csv(output_path, usecols=["k", "v"], index_col=["k"]).sort_index()
    expected = pd.read_csv(input_path, usecols=["k", "v"], index_col=["k"]).sort_index()
    assert result.equals(expected)


def test_csv_static_exotic_column_name(tmp_path: pathlib.Path):
    data = """
        #key    | @value
        1       | foo
        2       | bar
        3       | baz
    """
    input_path = tmp_path / "input.csv"
    write_csv(input_path, data)

    class InputSchema(pw.Schema):
        key: int = pw.column_definition(primary_key=True, name="#key")
        value: str = pw.column_definition(primary_key=True, name="@value")

    input = pw.io.csv.read(
        str(input_path),
        schema=InputSchema,
        mode="static",
    )
    result = input.select(pw.this["#key"], pw.this["@value"])
    expected = T(data)

    assert_table_equality_wo_index(input, expected)
    assert_table_equality_wo_index(result, expected)


def test_csv_default_values(tmp_path: pathlib.Path):
    data = """
        k | v
        a | 42
        b | 43
        c |
    """
    input_path = tmp_path / "input.csv"
    write_csv(input_path, data)

    class InputSchema(pw.Schema):
        k: str = pw.column_definition(primary_key=True)
        v: int = pw.column_definition(default_value=0)

    table = pw.io.csv.read(
        str(input_path),
        schema=InputSchema,
        mode="static",
    )

    assert_table_equality(
        table,
        T(
            """
                k | v
                a | 42
                b | 43
                c | 0
            """
        ).with_id_from(pw.this.k),
    )


def test_csv_skip_column(tmp_path: pathlib.Path):
    data = """
        k | a   | b
        1 | foo | a
        2 | bar | b
        3 | baz | c
    """
    input_path = tmp_path / "input.csv"
    write_csv(input_path, data)
    table = pw.io.csv.read(
        input_path,
        schema=pw.schema_builder(
            columns={
                "k": pw.column_definition(primary_key=True, dtype=int),
                "b": pw.column_definition(dtype=str),
            }
        ),
        mode="static",
    )
    assert_table_equality(
        table,
        T(
            """
            k   | b
            1   | a
            2   | b
            3   | c
            """,
            id_from=["k"],
        ),
    )


def test_id_hashing_across_connectors(tmp_path: pathlib.Path):
    csv_data = """
        key | value
        1   | foo
        2   | bar
        3   | baz
    """
    write_csv(tmp_path / "input.csv", csv_data)

    json_data = """
        {"key": 1, "value": "foo"}
        {"key": 2, "value": "bar"}
        {"key": 3, "value": "baz"}
    """
    write_lines(tmp_path / "input.json", json_data)

    class TestSubject(pw.io.python.ConnectorSubject):
        def run(self):
            self.next_json({"key": 1, "value": "foo"})
            self.next_json({"key": 2, "value": "bar"})
            self.next_json({"key": 3, "value": "baz"})

    class TestSchema(pw.Schema):
        key: int = pw.column_definition(primary_key=True)
        value: str

    table_csv = pw.io.csv.read(
        tmp_path / "input.csv",
        schema=TestSchema,
        mode="static",
    )
    table_json = pw.io.jsonlines.read(
        tmp_path / "input.json",
        schema=TestSchema,
        mode="static",
    )
    table_py = pw.io.python.read(
        subject=TestSubject(),
        schema=TestSchema,
    )
    table_static = T(csv_data, id_from=["key"])

    assert_table_equality(table_static, table_json)
    assert_table_equality(table_static, table_py)
    assert_table_equality(table_static, table_csv)


def test_json_default_values(tmp_path: pathlib.Path):
    data = """
        {"k": "a", "b": 1, "c": "foo" }
        {"k": "b", "b": 2, "c": null }
        {"k": "c" }
    """
    input_path = tmp_path / "input.csv"
    write_lines(input_path, data)

    class InputSchema(pw.Schema):
        k: str = pw.column_definition(primary_key=True)
        b: int = pw.column_definition(default_value=0)
        c: str | None = pw.column_definition(default_value="default")

    table = pw.io.jsonlines.read(
        str(input_path),
        schema=InputSchema,
        mode="static",
    )

    assert_table_equality(
        table,
        T(
            """
                k   | b   | c
                a   | 1   | foo
                b   | 2   |
                c   | 0   | default
            """
        ).with_id_from(pw.this.k),
    )


def test_subscribe():
    class TestSubject(pw.io.python.ConnectorSubject):
        def run(self):
            self.next(data="foo")

    table = pw.io.python.read(TestSubject(), format="raw")

    root = mock.Mock()

    pw.io.subscribe(
        table,
        on_change=root.on_change,
        on_time_end=root.on_time_end,
        on_end=root.on_end,
    )

    run()

    root.assert_has_calls(
        [
            mock.call.on_change(
                key=mock.ANY, row={"data": "foo"}, time=mock.ANY, is_addition=True
            ),
            mock.call.on_time_end(mock.ANY),
            mock.call.on_end(),
        ]
    )


def test_python_write():
    class TestSubject(pw.io.python.ConnectorSubject):
        def run(self):
            self.next(data="foo")

    root = mock.Mock()

    class Observer(pw.io.python.ConnectorObserver):
        def on_change(self, key, row, time, is_addition):
            root.on_change(key=key, row=row, time=time, is_addition=is_addition)

        def on_time_end(self, time):
            root.on_time_end(time=time)

        def on_end(self):
            root.on_end()

    table = pw.io.python.read(TestSubject(), format="raw")
    pw.io.python.write(table, Observer())
    run()

    root.assert_has_calls(
        [
            mock.call.on_change(
                key=mock.ANY, row={"data": "foo"}, time=mock.ANY, is_addition=True
            ),
            mock.call.on_time_end(time=mock.ANY),
            mock.call.on_end(),
        ]
    )


def test_python_write_on_change_only():
    class TestSubject(pw.io.python.ConnectorSubject):
        def run(self):
            self.next(data="foo")

    on_change = mock.Mock()

    class Observer(pw.io.python.ConnectorObserver):
        def on_change(self, key, row, time, is_addition):
            on_change(key=key, row=row, time=time, is_addition=is_addition)

    table = pw.io.python.read(TestSubject(), format="raw")
    observer = Observer()
    pw.io.python.write(table, observer)
    run_all()

    on_change.assert_called_once_with(
        key=mock.ANY, row={"data": "foo"}, time=mock.ANY, is_addition=True
    )


def test_fs_raw(tmp_path: pathlib.Path):
    input_path = tmp_path / "input.txt"
    write_lines(input_path, "foo\nbar\nbaz")

    table = pw.io.fs.read(str(input_path), format="raw", mode="static").update_types(
        data=str
    )

    assert_table_equality_wo_index(
        table,
        T(
            """
            data
            foo
            bar
            baz
        """,
        ),
    )
    pw.debug.compute_and_print(table)


@pytest.mark.flaky(reruns=2)
@needs_multiprocessing_fork
def test_csv_directory(tmp_path: pathlib.Path):
    inputs_path = tmp_path / "inputs/"
    os.mkdir(inputs_path)

    data = """
        k | v
        a | 42
    """
    write_csv(inputs_path / "1.csv", data)

    data = """
        k | v
        b | 43
    """
    write_csv(inputs_path / "2.csv", data)

    class InputSchema(pw.Schema):
        k: str = pw.column_definition(primary_key=True)
        v: int

    table = pw.io.csv.read(
        inputs_path,
        schema=InputSchema,
        mode="streaming",
        autocommit_duration_ms=10,
    )

    output_path = tmp_path / "output.csv"
    pw.io.csv.write(table, output_path)

    wait_result_with_checker(CsvLinesNumberChecker(output_path, 2), 30)


@pytest.mark.flaky(reruns=2)
@needs_multiprocessing_fork
def test_csv_streaming(tmp_path: pathlib.Path):
    inputs_path = tmp_path / "inputs/"
    start_streaming_inputs(inputs_path, 5, 1.0, "csv")

    class InputSchema(pw.Schema):
        k: str = pw.column_definition(primary_key=True)
        v: int

    table = pw.io.csv.read(
        str(inputs_path),
        schema=InputSchema,
        mode="streaming",
        autocommit_duration_ms=10,
    )

    output_path = tmp_path / "output.csv"
    pw.io.csv.write(table, str(output_path))

    wait_result_with_checker(CsvLinesNumberChecker(output_path, 5), 30)


@pytest.mark.flaky(reruns=2)
@needs_multiprocessing_fork
def test_json_streaming(tmp_path: pathlib.Path):
    inputs_path = tmp_path / "inputs/"
    start_streaming_inputs(inputs_path, 5, 1.0, "json")

    class InputSchema(pw.Schema):
        k: str = pw.column_definition(primary_key=True)
        v: int

    table = pw.io.jsonlines.read(
        str(inputs_path),
        schema=InputSchema,
        mode="streaming",
        autocommit_duration_ms=10,
    )

    output_path = tmp_path / "output.csv"
    pw.io.csv.write(table, str(output_path))

    wait_result_with_checker(CsvLinesNumberChecker(output_path, 5), 30)


@pytest.mark.flaky(reruns=2)
@needs_multiprocessing_fork
def test_plaintext_streaming(tmp_path: pathlib.Path):
    inputs_path = tmp_path / "inputs/"
    start_streaming_inputs(inputs_path, 5, 1.0, "plaintext")

    table = pw.io.plaintext.read(
        str(inputs_path),
        mode="streaming",
        autocommit_duration_ms=10,
    )

    output_path = tmp_path / "output.csv"
    pw.io.csv.write(table, str(output_path))

    wait_result_with_checker(CsvLinesNumberChecker(output_path, 5), 30)


@pytest.mark.flaky(reruns=2)
@needs_multiprocessing_fork
def test_csv_streaming_fs_alias(tmp_path: pathlib.Path):
    inputs_path = tmp_path / "inputs/"
    start_streaming_inputs(inputs_path, 5, 1.0, "csv")

    class InputSchema(pw.Schema):
        k: str = pw.column_definition(primary_key=True)
        v: int

    table = pw.io.fs.read(
        str(inputs_path),
        schema=InputSchema,
        mode="streaming",
        autocommit_duration_ms=10,
        format="csv",
    )

    output_path = tmp_path / "output.csv"
    pw.io.csv.write(table, str(output_path))

    wait_result_with_checker(CsvLinesNumberChecker(output_path, 5), 30)


@pytest.mark.flaky(reruns=2)
@needs_multiprocessing_fork
def test_json_streaming_fs_alias(tmp_path: pathlib.Path):
    inputs_path = tmp_path / "inputs/"
    start_streaming_inputs(inputs_path, 5, 1.0, "json")

    class InputSchema(pw.Schema):
        k: str = pw.column_definition(primary_key=True)
        v: int

    table = pw.io.fs.read(
        str(inputs_path),
        schema=InputSchema,
        mode="streaming",
        autocommit_duration_ms=10,
        format="json",
    )

    output_path = tmp_path / "output.csv"
    pw.io.csv.write(table, str(output_path))

    wait_result_with_checker(CsvLinesNumberChecker(output_path, 5), 30)


@pytest.mark.flaky(reruns=2)
@needs_multiprocessing_fork
def test_plaintext_streaming_fs_alias(tmp_path: pathlib.Path):
    inputs_path = tmp_path / "inputs/"
    start_streaming_inputs(inputs_path, 5, 1.0, "plaintext")

    table = pw.io.fs.read(
        str(inputs_path),
        mode="streaming",
        autocommit_duration_ms=10,
        format="plaintext",
    )

    output_path = tmp_path / "output.csv"
    pw.io.csv.write(table, str(output_path))

    wait_result_with_checker(CsvLinesNumberChecker(output_path, 5), 30)


def test_pathway_type_mapping():
    import inspect

    from pathway.internals.api import PathwayType
    from pathway.io._utils import _PATHWAY_TYPE_MAPPING

    assert all(
        t == _PATHWAY_TYPE_MAPPING[t].to_engine()
        for _, t in inspect.getmembers(PathwayType)
        if isinstance(t, PathwayType)
    )


def test_json_optional_values(tmp_path: pathlib.Path):
    data = """
{"k": "a", "v": 1}
{"k": "b", "v": 2, "w": 512}
    """
    input_path = tmp_path / "input.csv"
    write_lines(input_path, data)

    class InputSchema(pw.Schema):
        k: str = pw.column_definition(primary_key=True)
        v: int = pw.column_definition(default_value=0)
        w: int = pw.column_definition(default_value=1024)

    table = pw.io.jsonlines.read(
        str(input_path),
        schema=InputSchema,
        mode="static",
    )

    assert_table_equality(
        table,
        T(
            """
                k | v | w
                a | 1 | 1024
                b | 2 | 512
            """
        ).with_id_from(pw.this.k),
    )


def test_json_optional_values_with_paths(tmp_path: pathlib.Path):
    data = """
{"k": "a", "v": 1}
{"k": "b", "v": 2, "w": 512}
    """
    input_path = tmp_path / "input.csv"
    write_lines(input_path, data)

    class InputSchema(pw.Schema):
        k: str = pw.column_definition(primary_key=True)
        v: int = pw.column_definition(default_value=0)
        w: int = pw.column_definition(default_value=1024)

    table = pw.io.jsonlines.read(
        str(input_path),
        schema=InputSchema,
        mode="static",
        json_field_paths={"w": "/q/w/e/r/t/y/u"},
    )

    assert_table_equality(
        table,
        T(
            """
                k | v | w
                a | 1 | 1024
                b | 2 | 1024
            """
        ).with_id_from(pw.this.k),
    )


def test_table_from_pandas_schema():
    class DfSchema(pw.Schema):
        a: float

    table = pw.debug.table_from_markdown(
        """
        a
        0.0
        """,
        schema=DfSchema,
    )
    expected = pw.debug.table_from_markdown(
        """
        a
        0.0
        """
    ).update_types(a=float)

    assert_table_equality(table, expected)


def test_table_from_pandas_wrong_schema():
    with pytest.raises(ValueError):
        pw.debug.table_from_markdown(
            """
                | foo | bar
            1   | 32  | 32
            """,
            schema=pw.schema_builder({"foo": pw.column_definition()}),
        )


def test_table_from_pandas_wrong_params():
    with pytest.raises(ValueError):
        pw.debug.table_from_markdown(
            """
                | foo | bar
            1   | 32  | 32
            """,
            id_from=["foo"],
            schema=pw.schema_builder(
                {"foo": pw.column_definition(), "bar": pw.column_definition()}
            ),
        )


def test_table_from_pandas_modify_dataframe():
    df = pd.DataFrame({"a": [1, 2, 3]})
    table = pw.debug.table_from_pandas(df)
    df["a"] = [3, 4, 5]

    assert_table_equality(
        table,
        T(
            """
            a
            1
            2
            3
            """
        ),
    )


def test_python_connector_persistence(tmp_path: pathlib.Path):
    persistent_storage_path = tmp_path / "PStorage"
    input_path = tmp_path / "input.txt"
    output_path = tmp_path / "output.txt"

    class TestSubject(pw.io.python.ConnectorSubject):
        def __init__(self, items):
            super().__init__()
            self.items = items

        def run(self):
            for item in self.items:
                self.next_str(item)

    def run_computation(py_connector_input, fs_connector_input):
        G.clear()
        write_lines(input_path, "\n".join(fs_connector_input))
        table_py = pw.io.python.read(
            TestSubject(py_connector_input), format="raw", name="1"
        )
        table_csv = pw.io.plaintext.read(input_path, name="2", mode="static")
        table_joined = table_py.join(table_csv, table_py.data == table_csv.data).select(
            table_py.data
        )
        pw.io.csv.write(table_joined, output_path)
        run(
            persistence_config=pw.persistence.Config(
                pw.persistence.Backend.filesystem(persistent_storage_path),
            )
        )

    # We have "one" in Python connector and "one" in plaintext connector
    # They will form this one pair.
    run_computation(["one", "two"], ["one", "three"])
    result = pd.read_csv(output_path)
    assert set(result["data"]) == {"one"}

    # In case of non-persistent run we have an empty table on the left and four-element
    # table on the right. They join can't form any pairs in this case.
    #
    # But we are running in persistent mode and the table ["one", "two"] from the
    # previous run is persisted. We also have the first two elements of the plaintext
    # table persisted, so in this run we will do the join of ["one", "two"] (persisted
    # Python) with ["two", "four"] (new plaintext). It will produce one line: "two".
    run_computation([], ["one", "three", "two", "four"])
    result = pd.read_csv(output_path)
    assert set(result["data"]) == {"two"}

    # Now we add two additional elements to the Python-connector table. Now the table in
    # connector is ["one", "two", "three", "four"], where ["one", "two"] are old
    # persisted elements, while ["three", "four"] are new.
    #
    # We don't have any new elements in the second, plaintext table, but since it's
    # persisted, and join has the new results, it will produce ["three", "four"] on the
    # output.
    run_computation(["three", "four"], ["one", "three", "two", "four"])
    result = pd.read_csv(output_path)
    assert set(result["data"]) == {"three", "four"}


def test_no_pstorage(tmp_path: pathlib.Path):
    input_path = tmp_path / "input.txt"
    output_path = tmp_path / "input.txt"
    path = tmp_path / "NotPStorage"
    write_lines(path, "hello")
    write_lines(input_path, "hello")

    table = pw.io.plaintext.read(input_path)
    pw.io.csv.write(table, output_path)
    with pytest.raises(
        api.EngineError,
        match="persistence backend failed: target object should be a directory",
    ):
        run(
            persistence_config=pw.persistence.Config(
                pw.persistence.Backend.filesystem(path),
            )
        )


def test_name_not_assigned_autogenerate(tmp_path: pathlib.Path):
    input_path = tmp_path / "input.txt"
    write_lines(input_path, "test_data")
    pstorage_path = tmp_path / "PStrorage"

    write_lines(input_path, "test_data")

    table = pw.io.plaintext.read(input_path, mode="static")
    pw.io.csv.write(table, tmp_path / "output.txt")
    run(
        persistence_config=pw.persistence.Config(
            pw.persistence.Backend.filesystem(pstorage_path)
        )
    )


def test_no_persistent_storage_has_name(tmp_path: pathlib.Path):
    input_path = tmp_path / "input.txt"
    write_lines(input_path, "test_data")

    table = pw.io.plaintext.read(input_path, name="1", mode="static")
    pw.io.csv.write(table, tmp_path / "output.txt")
    run()


def test_duplicated_name(tmp_path: pathlib.Path):
    pstorage_path = tmp_path / "PStorage"
    input_path = tmp_path / "input_first.txt"
    input_path_2 = tmp_path / "input_second.txt"
    output_path = tmp_path / "output.txt"

    write_lines(input_path, "hello")
    write_lines(input_path_2, "world")

    table_1 = pw.io.plaintext.read(input_path, name="one")
    table_2 = pw.io.plaintext.read(input_path_2, name="one")
    pw.universes.promise_are_pairwise_disjoint(table_1, table_2)
    table_concat = table_1.concat(table_2)
    pw.io.csv.write(table_concat, output_path)

    with pytest.raises(
        ValueError,
        match="Unique name 'one' used more than once",
    ):
        run(
            persistence_config=pw.persistence.Config(
                pw.persistence.Backend.filesystem(pstorage_path)
            )
        )


def test_duplicated_name_between_input_and_output(tmp_path: pathlib.Path):
    pstorage_path = tmp_path / "PStorage"
    input_path = tmp_path / "input_first.txt"
    input_path_2 = tmp_path / "input_second.txt"
    output_path = tmp_path / "output.txt"

    write_lines(input_path, "hello")
    write_lines(input_path_2, "world")

    table = pw.io.plaintext.read(input_path, name="one")
    pw.io.jsonlines.write(table, output_path, name="one")

    with pytest.raises(
        ValueError,
        match="Unique name 'one' used more than once",
    ):
        run(
            persistence_config=pw.persistence.Config(
                pw.persistence.Backend.filesystem(pstorage_path)
            )
        )


def test_duplicated_name_between_input_and_subscribe(tmp_path: pathlib.Path):
    pstorage_path = tmp_path / "PStorage"
    input_path = tmp_path / "input_first.txt"
    input_path_2 = tmp_path / "input_second.txt"

    write_lines(input_path, "hello")
    write_lines(input_path_2, "world")

    table = pw.io.plaintext.read(input_path, name="one")
    pw.io.subscribe(table, lambda **kwargs: print(kwargs), name="one")

    with pytest.raises(
        ValueError,
        match="Unique name 'one' used more than once",
    ):
        run(
            persistence_config=pw.persistence.Config(
                pw.persistence.Backend.filesystem(pstorage_path)
            )
        )


@pytest.mark.skipif(sys.platform != "linux", reason="/dev/full is Linux-only")
def test_immediate_connector_errors():
    class TestSubject(pw.io.python.ConnectorSubject):
        should_finish: threading.Event
        timed_out: bool

        def __init__(self):
            super().__init__()
            self.finish = threading.Event()
            self.timed_out = False

        def run(self):
            self.next_str("manul")
            if not self.finish.wait(timeout=10):
                self.timed_out = True

    subject = TestSubject()
    table = pw.io.python.read(subject, format="raw", autocommit_duration_ms=10)
    pw.io.csv.write(table, "/dev/full")
    with pytest.raises(api.EngineError, match="No space left on device"):
        run_all()
    subject.finish.set()

    assert not subject.timed_out


def run_replacement_test(
    streaming_target,
    input_format,
    expected_output_lines,
    tmp_path,
    monkeypatch,
    inputs_path_override=None,
    has_only_file_replacements=False,
):
    monkeypatch.setenv("PATHWAY_PERSISTENT_STORAGE", str(tmp_path / "PStorage"))
    inputs_path = inputs_path_override or (tmp_path / "inputs")
    os.mkdir(tmp_path / "inputs")

    class InputSchema(pw.Schema):
        key: int = pw.column_definition(primary_key=True)
        value: str

    table = pw.io.fs.read(
        inputs_path,
        format=input_format,
        schema=InputSchema,
        mode="streaming",
        autocommit_duration_ms=1,
        with_metadata=True,
    )

    output_path = tmp_path / "output.csv"
    pw.io.jsonlines.write(table, str(output_path))

    inputs_thread = threading.Thread(target=streaming_target, daemon=True)
    inputs_thread.start()

    wait_result_with_checker(
        FileLinesNumberChecker(output_path, expected_output_lines), 30
    )

    parsed_rows = []
    with open(output_path) as f:
        for row in f:
            parsed_row = json.loads(row)
            parsed_rows.append(parsed_row)
    parsed_rows.sort(key=lambda row: (row["time"], row["diff"]))

    key_metadata = {}
    time_removed = {}
    for parsed_row in parsed_rows:
        key = parsed_row["key"]
        metadata = parsed_row["_metadata"]
        file_name = metadata["path"]
        is_insertion = parsed_row["diff"] == 1
        timestamp = parsed_row["time"]

        if is_insertion:
            if has_only_file_replacements and file_name in time_removed:
                # If there are only replacement and the file has been removed
                # already, then we need to check that the insertion and its'
                # removal were consolidated, i.e. happened in the same timestamp
                assert time_removed[file_name] == timestamp
            key_metadata[key] = metadata
        else:
            # Check that the metadata for the deleted object corresponds to the
            # initially reported metadata
            assert key_metadata[key] == metadata
            time_removed[file_name] = timestamp


@pytest.mark.flaky(reruns=2)
@needs_multiprocessing_fork
def test_simple_replacement_with_removal(tmp_path: pathlib.Path, monkeypatch):
    def stream_inputs():
        time.sleep(1)
        first_line = {"key": 1, "value": "one"}
        second_line = {"key": 2, "value": "two"}
        write_lines(tmp_path / "inputs/input1.jsonlines", json.dumps(first_line))
        time.sleep(1)
        write_lines(tmp_path / "inputs/input2.jsonlines", json.dumps(second_line))
        time.sleep(1)
        os.remove(tmp_path / "inputs/input1.jsonlines")

    run_replacement_test(
        streaming_target=stream_inputs,
        input_format="json",
        expected_output_lines=3,
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
    )


@pytest.mark.flaky(reruns=2)
@needs_multiprocessing_fork
def test_simple_insert_consolidation(tmp_path: pathlib.Path, monkeypatch):
    def stream_inputs():
        time.sleep(1)
        first_line = {"key": 1, "value": "one"}
        second_line = {"key": 2, "value": "two"}
        write_lines(tmp_path / "inputs/input1.jsonlines", json.dumps(first_line))
        time.sleep(1)
        write_lines(tmp_path / "inputs/input1.jsonlines", json.dumps(second_line))
        time.sleep(1)
        write_lines(tmp_path / "inputs/input1.jsonlines", json.dumps(first_line))
        time.sleep(1)

    run_replacement_test(
        streaming_target=stream_inputs,
        input_format="json",
        expected_output_lines=5,
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
        has_only_file_replacements=True,
    )


@pytest.mark.flaky(reruns=2)
@needs_multiprocessing_fork
def test_simple_replacement_on_file(tmp_path: pathlib.Path, monkeypatch):
    def stream_inputs():
        time.sleep(1)
        first_line = {"key": 1, "value": "one"}
        second_line = {"key": 2, "value": "two"}
        third_line = {"key": 3, "value": "three"}
        write_lines(tmp_path / "inputs/input.jsonlines", json.dumps(first_line))
        time.sleep(1)
        write_lines(tmp_path / "inputs/input.jsonlines", json.dumps(second_line))
        time.sleep(1)
        os.remove(tmp_path / "inputs/input.jsonlines")
        time.sleep(1)
        write_lines(tmp_path / "inputs/input.jsonlines", json.dumps(third_line))

    run_replacement_test(
        streaming_target=stream_inputs,
        input_format="json",
        expected_output_lines=5,
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
        inputs_path_override=tmp_path / "inputs/input.jsonlines",
    )


@pytest.mark.flaky(reruns=2)
@needs_multiprocessing_fork
def test_simple_replacement(tmp_path: pathlib.Path, monkeypatch):
    def stream_inputs():
        time.sleep(1)
        first_line = {"key": 1, "value": "one"}
        second_line = {"key": 2, "value": "two"}
        third_line = {"key": 3, "value": "three"}
        write_lines(tmp_path / "inputs/input1.jsonlines", json.dumps(first_line))
        time.sleep(1)
        write_lines(tmp_path / "inputs/input2.jsonlines", json.dumps(second_line))
        time.sleep(1)
        write_lines(tmp_path / "inputs/input1.jsonlines", json.dumps(third_line))

    run_replacement_test(
        streaming_target=stream_inputs,
        input_format="json",
        expected_output_lines=4,
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
        has_only_file_replacements=True,
    )


@pytest.mark.flaky(reruns=2)
@needs_multiprocessing_fork
def test_last_file_replacement_json(tmp_path: pathlib.Path, monkeypatch):
    def stream_inputs():
        time.sleep(1)
        first_line = {"key": 1, "value": "one"}
        second_line = {"key": 2, "value": "two"}
        third_line = {"key": 3, "value": "three"}
        write_lines(tmp_path / "inputs/input1.jsonlines", json.dumps(first_line))
        time.sleep(1)
        write_lines(tmp_path / "inputs/input2.jsonlines", json.dumps(second_line))
        time.sleep(1)
        write_lines(tmp_path / "inputs/input2.jsonlines", json.dumps(third_line))

    run_replacement_test(
        streaming_target=stream_inputs,
        input_format="json",
        expected_output_lines=4,
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
        has_only_file_replacements=True,
    )


@pytest.mark.flaky(reruns=2)
@needs_multiprocessing_fork
def test_last_file_replacement_csv(tmp_path: pathlib.Path, monkeypatch):
    def stream_inputs():
        time.sleep(1)
        first_file = """
            key | value
            1   | one
        """
        second_file = """
            key | value
            2   | two
        """
        third_file = """
            key | value
            3   | three
        """
        write_csv(tmp_path / "inputs/input1.jsonlines", first_file)
        time.sleep(1)
        write_csv(tmp_path / "inputs/input2.jsonlines", second_file)
        time.sleep(1)
        write_csv(tmp_path / "inputs/input2.jsonlines", third_file)

    run_replacement_test(
        streaming_target=stream_inputs,
        input_format="csv",
        expected_output_lines=4,
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
        has_only_file_replacements=True,
    )


@pytest.mark.flaky(reruns=2)
@needs_multiprocessing_fork
def test_file_removal_autogenerated_key(tmp_path: pathlib.Path, monkeypatch):
    def stream_inputs():
        time.sleep(1)
        first_line = {"key": 1, "value": "one"}
        second_line = {"key": 2, "value": "two"}
        write_lines(tmp_path / "inputs/input1.jsonlines", json.dumps(first_line))
        time.sleep(1)
        write_lines(tmp_path / "inputs/input2.jsonlines", json.dumps(second_line))
        time.sleep(1)
        os.remove(tmp_path / "inputs/input1.jsonlines")

    run_replacement_test(
        streaming_target=stream_inputs,
        input_format="json",
        expected_output_lines=3,
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
    )


@pytest.mark.flaky(reruns=2)
@needs_multiprocessing_fork
def test_simple_replacement_autogenerated_key(tmp_path: pathlib.Path, monkeypatch):
    def stream_inputs():
        time.sleep(1)
        first_line = {"key": 1, "value": "one"}
        second_line = {"key": 2, "value": "two"}
        third_line = {"key": 3, "value": "three"}
        write_lines(tmp_path / "inputs/input1.jsonlines", json.dumps(first_line))
        time.sleep(1)
        write_lines(tmp_path / "inputs/input2.jsonlines", json.dumps(second_line))
        time.sleep(1)
        write_lines(tmp_path / "inputs/input1.jsonlines", json.dumps(third_line))

    run_replacement_test(
        streaming_target=stream_inputs,
        input_format="json",
        expected_output_lines=4,
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
        has_only_file_replacements=True,
    )


def test_bytes_read(tmp_path: pathlib.Path):
    input_path = tmp_path / "input.txt"
    input_full_contents = "abc\n\ndef\nghi"
    output_path = tmp_path / "output.json"
    write_lines(input_path, input_full_contents)

    table = pw.io.fs.read(
        input_path,
        format="binary",
        mode="static",
        autocommit_duration_ms=1000,
    )
    pw.io.jsonlines.write(table, output_path)
    run()

    with open(output_path) as f:
        result = json.load(f)
        assert result["data"] == base64.b64encode(
            (input_full_contents + "\n").encode("utf-8")
        ).decode("utf-8")


def test_binary_data_in_subscribe(tmp_path: pathlib.Path):
    input_path = tmp_path / "input.txt"
    input_full_contents = "abc\n\ndef\nghi"
    write_lines(input_path, input_full_contents)

    table = pw.io.fs.read(
        input_path,
        format="binary",
        mode="static",
        autocommit_duration_ms=1000,
    )

    rows = []

    def on_change(key, row, time, is_addition):
        rows.append(row)

    def on_end(*args, **kwargs):
        pass

    pw.io.subscribe(table, on_change=on_change, on_end=on_end)
    run()

    assert rows == [
        {
            "data": (input_full_contents + "\n").encode("utf-8"),
        }
    ]


def test_bool_values_parsing_in_csv(tmp_path: pathlib.Path):
    input_path = tmp_path / "input.csv"
    output_path = tmp_path / "output.csv"

    various_formats_input = """key,value
1,true
2,TRUE
3,T
4,1
5,t
6,FALSE
7,false
8,f
9,F
10,0"""
    write_lines(input_path, various_formats_input)

    class InputSchema(pw.Schema):
        key: int
        value: bool

    table = pw.io.fs.read(
        input_path,
        format="csv",
        mode="static",
        schema=InputSchema,
    )
    pw.io.csv.write(table, output_path)
    run()
    result = pd.read_csv(
        output_path, usecols=["key", "value"], index_col=["key"]
    ).sort_index()
    assert list(result["value"]) == [True] * 5 + [False] * 5


def test_text_file_read_in_full(tmp_path: pathlib.Path):
    input_path = tmp_path / "input.txt"
    input_full_contents = "abc\n\ndef\nghi"
    output_path = tmp_path / "output.json"
    write_lines(input_path, input_full_contents)

    table = pw.io.fs.read(
        input_path,
        format="plaintext_by_file",
        mode="static",
        autocommit_duration_ms=1000,
    )
    pw.io.jsonlines.write(table, output_path)
    run()

    with open(output_path) as f:
        result = json.load(f)
        assert result["data"] == input_full_contents


def test_text_files_directory_read_in_full(tmp_path: pathlib.Path):
    inputs_path = tmp_path / "inputs"
    os.mkdir(inputs_path)

    input_contents_1 = "abc\n\ndef\nghi"
    input_contents_2 = "ttt\nppp\nqqq"
    input_contents_3 = "zzz\nyyy\n\nxxx"
    write_lines(inputs_path / "input1.txt", input_contents_1)
    write_lines(inputs_path / "input2.txt", input_contents_2)
    write_lines(inputs_path / "input3.txt", input_contents_3)

    output_path = tmp_path / "output.json"
    table = pw.io.fs.read(
        inputs_path,
        format="plaintext_by_file",
        mode="static",
        autocommit_duration_ms=1000,
    )
    pw.io.jsonlines.write(table, output_path)
    run()

    output_lines = []
    with open(output_path) as f:
        for line in f.readlines():
            output_lines.append(json.loads(line)["data"])

    assert len(output_lines) == 3
    output_lines.sort()
    assert output_lines[0] == input_contents_1
    assert output_lines[1] == input_contents_2
    assert output_lines[2] == input_contents_3


@pytest.mark.parametrize(
    "snapshot_access", [api.SnapshotAccess.FULL, api.SnapshotAccess.OFFSETS_ONLY]
)
def test_persistent_subscribe(snapshot_access, tmp_path):
    pstorage_dir = tmp_path / "PStorage"
    input_path = tmp_path / "input.csv"

    class TestSchema(pw.Schema):
        k: int = pw.column_definition(primary_key=True)
        v: str

    data = """
        k | v
        1 | foo
    """

    write_csv(input_path, data)
    table = pw.io.csv.read(
        str(input_path),
        schema=TestSchema,
        name="1",
        mode="static",
    )

    root = mock.Mock()
    pw.io.subscribe(table, on_change=root.on_change, on_end=root.on_end)
    pw.run(
        persistence_config=pw.persistence.Config(
            pw.persistence.Backend.filesystem(pstorage_dir),
            snapshot_access=snapshot_access,
        ),
    )

    root.assert_has_calls(
        [
            mock.call.on_change(
                key=mock.ANY,
                row={"k": 1, "v": "foo"},
                time=mock.ANY,
                is_addition=True,
            ),
            mock.call.on_end(),
        ]
    )
    assert root.on_change.call_count == 1
    assert root.on_end.call_count == 1

    G.clear()

    data = """
        k | v
        1 | foo
        2 | bar
    """

    write_csv(input_path, data)
    table = pw.io.csv.read(
        str(input_path),
        schema=TestSchema,
        name="1",
        mode="static",
    )

    root = mock.Mock()
    pw.io.subscribe(table, on_change=root.on_change, on_end=root.on_end)
    run(
        persistence_config=pw.persistence.Config(
            pw.persistence.Backend.filesystem(pstorage_dir),
            snapshot_access=snapshot_access,
        ),
    )
    root.assert_has_calls(
        [
            mock.call.on_change(
                key=mock.ANY,
                row={"k": 2, "v": "bar"},
                time=mock.ANY,
                is_addition=True,
            ),
            mock.call.on_end(),
        ]
    )
    assert root.on_change.call_count == 1
    assert root.on_end.call_count == 1


def test_objects_pattern(tmp_path: pathlib.Path):
    inputs_dir = tmp_path / "inputs"
    os.mkdir(inputs_dir)

    input_contents_1 = "a\nb\nc"
    input_contents_2 = "d\ne\nf\ng"
    write_lines(inputs_dir / "input.txt", input_contents_1)
    write_lines(inputs_dir / "input.dat", input_contents_2)

    output_path = tmp_path / "output.csv"
    table = pw.io.fs.read(
        inputs_dir,
        format="plaintext",
        mode="static",
        object_pattern="*.txt",
    )
    pw.io.csv.write(table, output_path)
    run()
    result = pd.read_csv(output_path).sort_index()
    assert set(result["data"]) == {"a", "b", "c"}

    G.clear()
    table = pw.io.fs.read(
        inputs_dir,
        format="plaintext",
        mode="static",
        object_pattern="*.dat",
    )
    pw.io.csv.write(table, output_path)
    run()
    result = pd.read_csv(output_path).sort_index()
    assert set(result["data"]) == {"d", "e", "f", "g"}


class CollectValuesCallback(pw.io.OnChangeCallback):
    values: list[int]

    def __init__(self, expected, column_name):
        self.values = []
        self.expected = expected
        self.column_name = column_name

    def __call__(self, key, row, time: int, is_addition):
        self.values.append(row[self.column_name])

    def on_end(self):
        assert sorted(self.values) == sorted(self.expected)


def test_replay(tmp_path: pathlib.Path):
    replay_dir = tmp_path / "test_replay"

    class TimeColumnInputSchema(pw.Schema):
        number: int

    def run_graph(
        persistence_mode,
        expected: list[int],
        value_function_offset=0,
        generate_rows=0,
        continue_after_replay=True,
        snapshot_access=api.SnapshotAccess.FULL,
    ):
        G.clear()

        value_functions = {
            "number": lambda x: 2 * (x + value_function_offset) + 1,
        }

        t = pw.demo.generate_custom_stream(
            value_functions,
            schema=TimeColumnInputSchema,
            nb_rows=generate_rows,
            input_rate=15,
            autocommit_duration_ms=50,
            name="1",
        )

        callback = CollectValuesCallback(expected, "number")

        pw.io.subscribe(t, callback, callback.on_end)

        run(
            persistence_config=pw.persistence.Config(
                pw.persistence.Backend.filesystem(replay_dir),
                persistence_mode=persistence_mode,
                continue_after_replay=continue_after_replay,
                snapshot_access=snapshot_access,
            )
        )

    # First run to persist data in local storage
    expected = [2 * x + 1 for x in range(15)]
    run_graph(api.PersistenceMode.PERSISTING, expected, generate_rows=15)

    # In Persistency there should not be any data in output connector
    run_graph(api.PersistenceMode.PERSISTING, [])

    run_graph(api.PersistenceMode.BATCH, expected)

    run_graph(api.PersistenceMode.SPEEDRUN_REPLAY, expected)

    # With continue_after_replay=False, we should not generate new rows
    run_graph(
        api.PersistenceMode.SPEEDRUN_REPLAY,
        expected,
        generate_rows=15,
        continue_after_replay=False,
    )

    # Generate rows, but don't record them
    expected = [2 * x + 1 for x in range(30)]
    run_graph(
        api.PersistenceMode.SPEEDRUN_REPLAY,
        expected,
        generate_rows=15,
        value_function_offset=15,
        snapshot_access=api.SnapshotAccess.REPLAY,
    )

    # Check that the rows weren't recorded
    expected = [2 * x + 1 for x in range(15)]
    run_graph(api.PersistenceMode.SPEEDRUN_REPLAY, expected)

    # Without replay (and with empty input connector), there are no rows
    run_graph(
        api.PersistenceMode.SPEEDRUN_REPLAY,
        [],
        snapshot_access=api.SnapshotAccess.RECORD,
    )

    # Generate rows and record them (but don't replay saved data)
    expected = [2 * x + 1 for x in range(15, 25)]
    run_graph(
        api.PersistenceMode.SPEEDRUN_REPLAY,
        expected,
        generate_rows=10,
        value_function_offset=15,
        snapshot_access=api.SnapshotAccess.RECORD,
    )

    # Check that the rows were recorded
    expected = [2 * x + 1 for x in range(25)]
    run_graph(api.PersistenceMode.SPEEDRUN_REPLAY, expected)


def test_replay_timestamps(tmp_path: pathlib.Path):
    replay_dir = tmp_path / "test_replay_timestamps"

    class TimeColumnInputSchema(pw.Schema):
        number: int
        parity: int

    value_functions = {
        "number": lambda x: x + 1,
        "parity": lambda x: (x + 1) % 2,
    }

    def run_graph(
        persistence_mode,
        expected_count: int | None = None,
        generate_rows=0,
        continue_after_replay=True,
        snapshot_access=api.SnapshotAccess.FULL,
    ) -> int:
        G.clear()

        t = pw.demo.generate_custom_stream(
            value_functions,
            schema=TimeColumnInputSchema,
            nb_rows=generate_rows,
            input_rate=15,
            autocommit_duration_ms=50,
            name="1",
        )

        callback = CountDifferentTimestampsCallback(expected_count)

        pw.io.subscribe(t, callback, callback.on_end)

        run(
            persistence_config=pw.persistence.Config(
                pw.persistence.Backend.filesystem(replay_dir),
                persistence_mode=persistence_mode,
                continue_after_replay=continue_after_replay,
                snapshot_access=snapshot_access,
            )
        )

        return len(callback.timestamps)

    # Workaround for demo.generate_custom_stream sometimes putting two rows in the same batch:
    # when generating rows we count number of different timestamp, and then during replay in Speedrun mode
    # we expect the number of different timestamps to be the same as when generating data.

    # First run to persist data in local storage
    n_timestamps = run_graph(api.PersistenceMode.PERSISTING, generate_rows=15)

    # In Persistency there should not be any data in output connector
    run_graph(api.PersistenceMode.PERSISTING, 0)

    # In Batch every row should have the same timestamp
    run_graph(api.PersistenceMode.BATCH, 1)

    # In Speedrun we should have the same number of timestamps as when generating data
    run_graph(api.PersistenceMode.SPEEDRUN_REPLAY, n_timestamps)


def test_metadata_column_identity(tmp_path: pathlib.Path):
    inputs_path = tmp_path / "inputs"
    os.mkdir(inputs_path)

    input_contents_1 = "abc\n\ndef\nghi"
    input_contents_2 = "ttt\nppp\nqqq"
    input_contents_3 = "zzz\nyyy\n\nxxx"
    write_lines(inputs_path / "input1.txt", input_contents_1)
    write_lines(inputs_path / "input2.txt", input_contents_2)
    write_lines(inputs_path / "input3.txt", input_contents_3)

    output_path = tmp_path / "output.json"
    table = pw.io.fs.read(
        inputs_path,
        with_metadata=True,
        format="plaintext_by_file",
        mode="static",
        autocommit_duration_ms=1000,
    )
    pw.io.jsonlines.write(table, output_path)
    run()

    metadata_file_names = []
    with open(output_path) as f:
        for line in f.readlines():
            metadata_file_names.append(json.loads(line)["_metadata"]["path"])

    assert len(metadata_file_names) == 3, metadata_file_names
    metadata_file_names.sort()
    assert metadata_file_names[0].endswith("input1.txt")
    assert metadata_file_names[1].endswith("input2.txt")
    assert metadata_file_names[2].endswith("input3.txt")


def test_metadata_column_regular_parser(tmp_path: pathlib.Path):
    inputs_path = tmp_path / "inputs"
    os.mkdir(inputs_path)

    input_contents_1 = json.dumps({"a": 1, "b": 10})
    input_contents_2 = json.dumps({"a": 2, "b": 20})
    write_lines(inputs_path / "input1.txt", input_contents_1)
    write_lines(inputs_path / "input2.txt", input_contents_2)

    class InputSchema(pw.Schema):
        a: int
        b: int

    output_path = tmp_path / "output.json"
    table = pw.io.fs.read(
        inputs_path,
        with_metadata=True,
        schema=InputSchema,
        format="json",
        mode="static",
        autocommit_duration_ms=1000,
    )
    pw.io.jsonlines.write(table, output_path)
    run()

    metadata_file_names = []
    with open(output_path) as f:
        for line in f.readlines():
            metadata_file_names.append(json.loads(line)["_metadata"]["path"])

    assert len(metadata_file_names) == 2, metadata_file_names
    metadata_file_names.sort()
    assert metadata_file_names[0].endswith("input1.txt")
    assert metadata_file_names[1].endswith("input2.txt")


def test_mock_snapshot_reader():
    class InputSchema(pw.Schema):
        number: int

    events = {
        ("1", 0): [
            api.SnapshotEvent.advance_time(2),
            api.SnapshotEvent.insert(api.ref_scalar(0), [1]),
            api.SnapshotEvent.insert(api.ref_scalar(1), [1]),
            api.SnapshotEvent.advance_time(4),
            api.SnapshotEvent.insert(api.ref_scalar(2), [4]),
            api.SnapshotEvent.delete(api.ref_scalar(0), [1]),
            api.SnapshotEvent.FINISHED,
        ]
    }

    t = pw.demo.generate_custom_stream(
        {},
        schema=InputSchema,
        nb_rows=0,
        input_rate=15,
        autocommit_duration_ms=50,
        name="1",
    )

    on_change = mock.Mock()
    pw.io.subscribe(t, on_change=on_change)

    run(
        persistence_config=pw.persistence.Config(
            pw.persistence.Backend.mock(events),
            persistence_mode=api.PersistenceMode.SPEEDRUN_REPLAY,
            snapshot_access=api.SnapshotAccess.REPLAY,
        )
    )

    on_change.assert_has_calls(
        [
            mock.call.on_change(
                key=api.ref_scalar(0),
                row={"number": 1},
                time=2,
                is_addition=True,
            ),
            mock.call.on_change(
                key=api.ref_scalar(1),
                row={"number": 1},
                time=2,
                is_addition=True,
            ),
            mock.call.on_change(
                key=api.ref_scalar(2),
                row={"number": 4},
                time=4,
                is_addition=True,
            ),
            mock.call.on_change(
                key=api.ref_scalar(0),
                row={"number": 1},
                time=4,
                is_addition=False,
            ),
        ],
        any_order=True,
    )
    assert on_change.call_count == 4


def test_stream_generator_from_list():
    class InputSchema(pw.Schema):
        number: int

    stream_generator = pw.debug.StreamGenerator()

    events = [
        [{"number": 1}, {"number": 2}, {"number": 5}],
        [{"number": 4}, {"number": 4}],
    ]

    t = stream_generator.table_from_list_of_batches(events, InputSchema)
    on_change = mock.Mock()
    pw.io.subscribe(t, on_change=on_change)

    run(persistence_config=stream_generator.persistence_config())

    timestamps = {call.kwargs["time"] for call in on_change.mock_calls}
    assert len(timestamps) == 2

    on_change.assert_has_calls(
        [
            mock.call.on_change(
                key=mock.ANY,
                row={"number": 1},
                time=min(timestamps),
                is_addition=True,
            ),
            mock.call.on_change(
                key=mock.ANY,
                row={"number": 2},
                time=min(timestamps),
                is_addition=True,
            ),
            mock.call.on_change(
                key=mock.ANY,
                row={"number": 5},
                time=min(timestamps),
                is_addition=True,
            ),
            mock.call.on_change(
                key=mock.ANY,
                row={"number": 4},
                time=max(timestamps),
                is_addition=True,
            ),
            mock.call.on_change(
                key=mock.ANY,
                row={"number": 4},
                time=max(timestamps),
                is_addition=True,
            ),
        ],
        any_order=True,
    )
    assert on_change.call_count == 5


def test_stream_generator_from_list_multiple_workers(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("PATHWAY_THREADS", "2")
    stream_generator = pw.debug.StreamGenerator()

    class InputSchema(pw.Schema):
        number: int

    events = [
        {0: [{"number": 1}, {"number": 2}], 1: [{"number": 5}]},
        {0: [{"number": 4}], 1: [{"number": 4}]},
    ]

    t = stream_generator.table_from_list_of_batches_by_workers(events, InputSchema)
    on_change = mock.Mock()
    pw.io.subscribe(t, on_change=on_change)

    run(persistence_config=stream_generator.persistence_config())

    timestamps = {call.kwargs["time"] for call in on_change.mock_calls}
    assert len(timestamps) == 2

    on_change.assert_has_calls(
        [
            mock.call.on_change(
                key=mock.ANY,
                row={"number": 1},
                time=min(timestamps),
                is_addition=True,
            ),
            mock.call.on_change(
                key=mock.ANY,
                row={"number": 2},
                time=min(timestamps),
                is_addition=True,
            ),
            mock.call.on_change(
                key=mock.ANY,
                row={"number": 5},
                time=min(timestamps),
                is_addition=True,
            ),
            mock.call.on_change(
                key=mock.ANY,
                row={"number": 4},
                time=max(timestamps),
                is_addition=True,
            ),
            mock.call.on_change(
                key=mock.ANY,
                row={"number": 4},
                time=max(timestamps),
                is_addition=True,
            ),
        ],
        any_order=True,
    )
    assert on_change.call_count == 5


@pytest.mark.filterwarnings("ignore:timestamps are required to be even")
def test_stream_generator_from_markdown():
    stream_generator = pw.debug.StreamGenerator()
    t = stream_generator.table_from_markdown(
        """
       | colA | colB | _time
    1  | 1    | 2    | 1
    5  | 2    | 3    | 1
    10 | 5    | 1    | 2
    """
    )
    on_change = mock.Mock()
    pw.io.subscribe(t, on_change=on_change)

    run(persistence_config=stream_generator.persistence_config())

    on_change.assert_has_calls(
        [
            mock.call.on_change(
                key=api.ref_scalar(1),
                row={"colA": 1, "colB": 2},
                time=2,
                is_addition=True,
            ),
            mock.call.on_change(
                key=api.ref_scalar(5),
                row={"colA": 2, "colB": 3},
                time=2,
                is_addition=True,
            ),
            mock.call.on_change(
                key=api.ref_scalar(10),
                row={"colA": 5, "colB": 1},
                time=4,
                is_addition=True,
            ),
        ],
        any_order=True,
    )
    assert on_change.call_count == 3


def test_stream_generator_from_markdown_with_diffs():
    stream_generator = pw.debug.StreamGenerator()
    t = stream_generator.table_from_markdown(
        """
       | colA | colB | _time | _diff
    1  | 1    | 2    | 2     | 1
    5  | 2    | 3    | 2     | 1
    1  | 1    | 2    | 4     | -1
    10 | 5    | 1    | 4     | 1
    3  | 1    | 1    | 4     | 1
    10 | 5    | 1    | 8     | -1
    """
    )

    expected = pw.debug.table_from_markdown(
        """
       | colA | colB
    5  | 2    | 3
    3  | 1    | 1
    """
    )

    assert_table_equality(
        t, expected, persistence_config=stream_generator.persistence_config()
    )


def test_stream_generator_two_tables_multiple_workers(monkeypatch: pytest.MonkeyPatch):
    stream_generator = pw.debug.StreamGenerator()
    monkeypatch.setenv("PATHWAY_THREADS", "4")

    class InputSchema(pw.Schema):
        colA: int
        colB: int

    t1 = stream_generator.table_from_markdown(
        """
    colA | colB | _time | _worker
    1    | 2    | 2     | 0
    2    | 3    | 2     | 1
    5    | 1    | 4     | 2
    3    | 5    | 6     | 3
    7    | 4    | 8     | 0
    """
    )

    t2 = stream_generator._table_from_dict(
        {
            2: {0: [(1, api.ref_scalar(0), [1, 4])]},
            4: {2: [(1, api.ref_scalar(1), [3, 7])]},
            8: {0: [(1, api.ref_scalar(2), [2, 2])]},
        },
        InputSchema,
    )

    t3 = (
        t1.join(t2, t1.colA == t2.colA)
        .select(colA=pw.left.colA, left=pw.left.colB, right=pw.right.colB)
        .with_columns(sum=pw.this.left + pw.this.right)
    )

    on_change = mock.Mock()
    pw.io.subscribe(t3, on_change=on_change)

    run(persistence_config=stream_generator.persistence_config())

    on_change.assert_has_calls(
        [
            mock.call.on_change(
                key=mock.ANY,
                row={"colA": 1, "left": 2, "right": 4, "sum": 6},
                time=2,
                is_addition=True,
            ),
            mock.call.on_change(
                key=mock.ANY,
                row={"colA": 3, "left": 5, "right": 7, "sum": 12},
                time=6,
                is_addition=True,
            ),
            mock.call.on_change(
                key=mock.ANY,
                row={"colA": 2, "left": 3, "right": 2, "sum": 5},
                time=8,
                is_addition=True,
            ),
        ],
        any_order=True,
    )
    assert on_change.call_count == 3


def test_python_connector_upsert_raw(tmp_path: pathlib.Path):
    output_path = tmp_path / "output.csv"

    class TestSubject(pw.io.python.ConnectorSubject):
        @property
        def _session_type(self) -> SessionType:
            return SessionType.UPSERT

        def run(self):
            self._add(api.ref_scalar(0), b"one")
            time.sleep(5e-2)
            self._remove(api.ref_scalar(0), b"")
            time.sleep(5e-2)
            self._add(api.ref_scalar(0), b"two")
            time.sleep(5e-2)
            self._add(api.ref_scalar(0), b"three")
            self.close()

    table = pw.io.python.read(TestSubject(), format="raw", autocommit_duration_ms=10)
    pw.io.csv.write(table, output_path)
    run()

    result = pd.read_csv(output_path)
    assert len(result) == 5
    assert_sets_equality_from_path(output_path, {"three,1"})


def test_python_connector_upsert_remove_raw(tmp_path: pathlib.Path):
    output_path = tmp_path / "output.csv"

    class TestSubject(pw.io.python.ConnectorSubject):
        @property
        def _session_type(self) -> SessionType:
            return SessionType.UPSERT

        def run(self):
            self._add(api.ref_scalar(0), b"one")
            time.sleep(5e-2)
            self._remove(api.ref_scalar(0), b"")

    table = pw.io.python.read(TestSubject(), format="raw", autocommit_duration_ms=10)
    pw.io.csv.write(table, output_path)
    run()

    result = pd.read_csv(output_path)
    assert len(result) == 2
    assert_sets_equality_from_path(output_path, set())


def test_python_connector_removal_by_key(tmp_path: pathlib.Path):
    output_path = tmp_path / "output.csv"

    class TestSubject(pw.io.python.ConnectorSubject):
        @property
        def _session_type(self) -> SessionType:
            return SessionType.UPSERT

        def run(self):
            self._add(api.ref_scalar(0), b"one")
            time.sleep(5e-2)
            self._remove(api.ref_scalar(0), b"")  # Note: we don't pass an actual value
            self.close()

    table = pw.io.python.read(TestSubject(), format="raw", autocommit_duration_ms=10)
    pw.io.csv.write(table, output_path)
    run()

    result = pd.read_csv(output_path)
    assert len(result) == 2
    assert_sets_equality_from_path(output_path, set())


def test_python_connector_upsert_json(tmp_path: pathlib.Path):
    output_path = tmp_path / "output.csv"

    class TestSubject(pw.io.python.ConnectorSubject):
        @property
        def _session_type(self) -> SessionType:
            return SessionType.UPSERT

        def run(self):
            self._add(
                api.ref_scalar(0),
                json.dumps({"word": "one", "digit": 1}).encode("utf-8"),
            )
            time.sleep(5e-2)
            self._add(
                api.ref_scalar(0),
                json.dumps({"word": "two", "digit": 2}).encode("utf-8"),
            )
            time.sleep(5e-2)
            self._add(
                api.ref_scalar(0),
                json.dumps({"word": "three", "digit": 3}).encode("utf-8"),
            )
            self.close()

    class InputSchema(pw.Schema):
        word: str
        digit: int

    table = pw.io.python.read(
        TestSubject(), format="json", schema=InputSchema, autocommit_duration_ms=10
    )
    pw.io.csv.write(table, output_path)
    run()

    result = pd.read_csv(output_path)
    assert len(result) == 5
    assert_sets_equality_from_path(output_path, {"three,3,1"})


def test_python_connector_upsert_remove_json(tmp_path: pathlib.Path):
    output_path = tmp_path / "output.csv"

    class TestSubject(pw.io.python.ConnectorSubject):
        @property
        def _session_type(self) -> SessionType:
            return SessionType.UPSERT

        def run(self):
            self._add(
                api.ref_scalar(0),
                json.dumps({"word": "one", "digit": 1}).encode("utf-8"),
            )
            time.sleep(5e-2)
            self._remove_inner(
                api.ref_scalar(0),
                {},
            )

    class InputSchema(pw.Schema):
        word: str
        digit: int

    table = pw.io.python.read(
        TestSubject(), format="json", schema=InputSchema, autocommit_duration_ms=10
    )
    pw.io.csv.write(table, output_path)
    run()

    result = pd.read_csv(output_path)
    assert len(result) == 2
    assert_sets_equality_from_path(output_path, set())


def test_python_connector_immediate_upsert(tmp_path: pathlib.Path):
    output_path = tmp_path / "output.csv"

    class InputSchema(pw.Schema):
        a: int

    class TestSubject(pw.io.python.ConnectorSubject):
        @property
        def _session_type(self) -> SessionType:
            return SessionType.UPSERT

        def run(self):
            self._disable_commits()
            self._add_inner(api.ref_scalar(0), dict(a=3))
            self._add_inner(api.ref_scalar(0), dict(a=1))
            self._add_inner(api.ref_scalar(0), dict(a=4))
            self._add_inner(api.ref_scalar(0), dict(a=6))
            self._add_inner(api.ref_scalar(0), dict(a=2))
            self._enable_commits()

    table = pw.io.python.read(TestSubject(), schema=InputSchema)
    pw.io.csv.write(table, output_path)
    run()

    result = pd.read_csv(output_path)
    assert len(result) == 1
    assert_sets_equality_from_path(output_path, {"2,1"})


def test_python_connector_metadata():
    class TestSubject(pw.io.python.ConnectorSubject):
        @property
        def _with_metadata(self) -> bool:
            return True

        def run(self):
            def encode(obj):
                return json.dumps(obj).encode()

            self._add(api.ref_scalar(1), b"foo", encode({"createdAt": 1701273920}))
            self._add(api.ref_scalar(2), b"bar", encode({"createdAt": 1701273912}))
            self._add(api.ref_scalar(3), b"baz", encode({"createdAt": 1701283942}))

    class OutputSchema(pw.Schema):
        _metadata: pw.Json
        data: Any

    table: pw.Table = pw.io.python.read(TestSubject(), format="raw")
    result = table.select(
        pw.this.data, createdAt=pw.this._metadata["createdAt"].as_int()
    )

    table.schema.assert_matches_schema(OutputSchema)
    assert_table_equality(
        T(
            """
                | data | createdAt
            1   | foo  | 1701273920
            2   | bar  | 1701273912
            3   | baz  | 1701283942
            """,
        ).update_types(
            data=str,
            createdAt=Optional[int],
        ),
        result,
    )


def test_python_connector_values():
    class TestSubject(pw.io.python.ConnectorSubject):
        def run(self):
            self.next(a=pd.Timestamp("2021-03-21T18:34:12"), b="abc".encode())
            self.next(a=pd.Timestamp("2022-04-01T11:12:12"), b="def".encode())
            self.next(a=pd.Timestamp("2023-01-01T00:00:00"), b="x".encode())

    class InputSchema(pw.Schema):
        a: pw.DateTimeNaive
        b: bytes

    result = pw.io.python.read(TestSubject(), schema=InputSchema)

    @pw.udf
    def encode(data: str) -> bytes:
        return data.encode()

    expected = T(
        """
        a                   | b
        2021-03-21T18:34:12 | abc
        2022-04-01T11:12:12 | def
        2023-01-01T00:00:00 | x
    """
    ).select(a=pw.this.a.dt.strptime("%Y-%m-%dT%H:%M:%S"), b=encode(pw.this.b))
    assert_table_equality_wo_index(result, expected)


def test_python_connector_defaults():
    class TestSubject(pw.io.python.ConnectorSubject):
        def run(self):
            self.next(a=1, b="one")
            self.next(a=2)
            self.next(b="three")

    class InputSchema(pw.Schema):
        a: int = pw.column_definition(default_value=0)
        b: str = pw.column_definition(default_value="default")

    result = pw.io.python.read(TestSubject(), schema=InputSchema)

    expected = T(
        """
        a | b
        1 | one
        2 | default
        0 | three
    """
    )
    assert_table_equality_wo_index(result, expected)


@pytest.mark.flaky(reruns=2)
@needs_multiprocessing_fork
def test_sqlite(tmp_path: pathlib.Path):
    database_name = tmp_path / "test.db"
    output_path = tmp_path / "output.csv"

    connection = sqlite3.connect(database_name)
    cursor = connection.cursor()
    cursor.execute(
        """
        CREATE TABLE users (
            id INTEGER,
            login TEXT,
            name TEXT
        )
        """
    )
    cursor.execute("INSERT INTO users (id, login, name) VALUES (1, 'alice', 'Alice')")
    cursor.execute("INSERT INTO users (id, login, name) VALUES (2, 'bob1999', 'Bob')")
    connection.commit()

    def stream_target():
        wait_result_with_checker(FileLinesNumberChecker(output_path, 2), 5, target=None)
        connection = sqlite3.connect(database_name)
        cursor = connection.cursor()
        cursor.execute(
            """
            INSERT INTO users (id, login, name) VALUES (3, 'ch123', 'Charlie')"""
        )
        connection.commit()

        wait_result_with_checker(FileLinesNumberChecker(output_path, 3), 2, target=None)
        cursor = connection.cursor()
        cursor.execute("UPDATE users SET name = 'Bob Smith' WHERE id = 2")
        connection.commit()

        wait_result_with_checker(FileLinesNumberChecker(output_path, 5), 2, target=None)
        cursor = connection.cursor()
        cursor.execute("DELETE FROM users WHERE id = 3")
        connection.commit()

    class InputSchema(pw.Schema):
        id: int
        login: str
        name: str

    table = pw.io.sqlite.read(
        database_name, "users", InputSchema, autocommit_duration_ms=1
    )
    pw.io.jsonlines.write(table, output_path)

    inputs_thread = threading.Thread(target=stream_target, daemon=True)
    inputs_thread.start()

    wait_result_with_checker(FileLinesNumberChecker(output_path, 6), 30)

    events = []
    with open(output_path) as f:
        for row in f:
            events.append(json.loads(row))

    events.sort(key=lambda event: (event["time"], event["diff"], event["name"]))
    events_truncated = []
    for event in events:
        events_truncated.append([event["name"], event["diff"]])

    assert events_truncated == [
        ["Alice", 1],
        ["Bob", 1],
        ["Charlie", 1],
        ["Bob", -1],
        ["Bob Smith", 1],
        ["Charlie", -1],
    ]


def test_apply_bytes_full_cycle(tmp_path: pathlib.Path):
    input_path = tmp_path / "input.txt"
    input_full_contents = "abc\n\ndef\nghi"
    output_path = tmp_path / "output.json"
    write_lines(input_path, input_full_contents)

    def duplicate(b):
        return b + b

    table = pw.io.fs.read(
        input_path,
        format="binary",
        mode="static",
        autocommit_duration_ms=1000,
    )
    table = table.select(data=pw.apply(duplicate, pw.this.data))
    pw.io.jsonlines.write(table, output_path)
    run()

    with open(output_path) as f:
        result = json.load(f)
        assert result["data"] == base64.b64encode(
            ((input_full_contents + "\n") * 2).encode("utf-8")
        ).decode("utf-8")


def test_server_fail_on_port_wrong_range():
    class InputSchema(pw.Schema):
        k: int
        v: int

    queries, response_writer = pw.io.http.rest_connector(
        host="127.0.0.1",
        port=-1,
        schema=InputSchema,
        delete_completed_queries=False,
    )
    response_writer(queries.select(query_id=queries.id, result=pw.this.v))

    with pytest.raises(OverflowError, match="port must be 0-65535."):
        pw.run(monitoring_level=pw.MonitoringLevel.NONE)


def test_server_fail_on_incorrect_port_type():
    class InputSchema(pw.Schema):
        k: int
        v: int

    with pytest.raises(TypeError):
        pw.io.http.rest_connector(
            host="127.0.0.1",
            port=("8080",),
            schema=InputSchema,
            delete_completed_queries=False,
        )


def test_server_fail_on_unparsable_port():
    class InputSchema(pw.Schema):
        k: int
        v: int

    with pytest.raises(ValueError):
        pw.io.http.rest_connector(
            host="127.0.0.1",
            port="abc",
            schema=InputSchema,
            delete_completed_queries=False,
        )


def test_server_fail_on_incorrect_host():
    class InputSchema(pw.Schema):
        k: int
        v: int

    queries, response_writer = pw.io.http.rest_connector(
        host="127.0.0.1xx",
        port=23213,
        schema=InputSchema,
        delete_completed_queries=False,
    )
    response_writer(queries.select(query_id=queries.id, result=pw.this.v))

    with pytest.raises(socket.gaierror):
        pw.run(monitoring_level=pw.MonitoringLevel.NONE)


def test_kafka_incorrect_host(tmp_path: pathlib.Path):
    table = pw.io.kafka.read(
        rdkafka_settings={"bootstrap.servers": "kafka:9092"},
        topic="test_0",
        format="raw",
        autocommit_duration_ms=100,
    )
    pw.io.csv.write(table, str(tmp_path / "output.csv"))

    with pytest.raises(
        OSError,
        match="Subscription error: Local: Unknown group",
    ):
        pw.run()


def test_kafka_incorrect_rdkafka_param(tmp_path: pathlib.Path):
    table = pw.io.kafka.read(
        rdkafka_settings={"bootstrap_servers": "kafka:9092"},  # "_" instead of "."
        topic="test_0",
        format="raw",
        autocommit_duration_ms=100,
    )
    pw.io.csv.write(table, str(tmp_path / "output.csv"))

    with pytest.raises(
        ValueError,
        match="No such configuration property",
    ):
        pw.run()


def test_server_fail_on_duplicate_route():
    port = int(os.environ.get("PATHWAY_MONITORING_HTTP_PORT", "20000")) + 10005

    class InputSchema(pw.Schema):
        k: int
        v: int

    webserver = pw.io.http.PathwayWebserver(host="127.0.0.1", port=port)

    queries, response_writer = pw.io.http.rest_connector(
        webserver=webserver,
        route="/uppercase",
        schema=InputSchema,
        delete_completed_queries=False,
    )
    response_writer(queries.select(query_id=queries.id, result=pw.this.v))

    with pytest.raises(RuntimeError, match="Added route will never be executed"):
        _ = pw.io.http.rest_connector(
            webserver=webserver,
            route="/uppercase",
            schema=InputSchema,
            delete_completed_queries=False,
        )


def test_pyfilesystem_simple(tmp_path: pathlib.Path):
    zip_path = (
        pathlib.Path("/".join(__file__.split("/")[:-1])) / "data" / "pyfs-testdata.zip"
    )
    output_path = tmp_path / "output.txt"

    with open_fs("zip://" + str(zip_path)) as source:
        table = pw.io.pyfilesystem.read(
            source,
            mode="static",
            with_metadata=True,
        )
        pw.io.jsonlines.write(table, output_path)
        pw.run(monitoring_level=pw.MonitoringLevel.NONE)

    paths = set()
    names = set()
    with open(output_path, "r") as f:
        for line in f:
            data = json.loads(line)
            assert "_metadata" in data
            assert "data" in data
            assert "diff" in data
            assert "time" in data
            metadata = data["_metadata"]
            paths.add(metadata["path"])
            names.add(metadata["name"])
            assert metadata["size"] == len(base64.b64decode(data["data"]))

    assert names == set(["a.txt", "b.txt"])
    assert paths == set(["projects/a.txt", "projects/b.txt"])


@pytest.mark.flaky(reruns=4)
@needs_multiprocessing_fork
def test_pyfilesystem_streaming(tmp_path: pathlib.Path):
    inputs_path = tmp_path / "inputs"
    os.mkdir(inputs_path)
    output_path = tmp_path / "output.txt"

    def stream_inputs():
        first_line = {"key": 1, "value": "one"}
        second_line = {"key": 2, "value": "two"}
        write_lines(inputs_path / "input1.jsonlines", json.dumps(first_line))
        wait_result_with_checker(
            FileLinesNumberChecker(output_path, 1), 30, target=None
        )
        write_lines(inputs_path / "input1.jsonlines", json.dumps(second_line))
        wait_result_with_checker(
            FileLinesNumberChecker(output_path, 3), 30, target=None
        )
        write_lines(inputs_path / "input1.jsonlines", json.dumps(first_line))
        wait_result_with_checker(
            FileLinesNumberChecker(output_path, 5), 30, target=None
        )
        os.remove(inputs_path / "input1.jsonlines")
        wait_result_with_checker(
            FileLinesNumberChecker(output_path, 6), 30, target=None
        )
        write_lines(inputs_path / "input2.jsonlines", json.dumps(first_line))

    t = threading.Thread(target=stream_inputs, daemon=True)
    t.start()

    with open_fs(os.fspath(inputs_path)) as source:
        table = pw.io.pyfilesystem.read(
            source,
            with_metadata=True,
            refresh_interval=0.1,
        )
        pw.io.jsonlines.write(table, output_path)
        wait_result_with_checker(FileLinesNumberChecker(output_path, 7), 30)

    t.join()


def test_airbyte_stream_state():
    # Actual github state used for commits
    # not_commits is a mock to check that one stream won't overwrite another
    destination = _PathwayAirbyteDestination(print, print)
    assert destination._state == {}
    destination._write(
        "_airbyte_states",
        [
            {
                "_airbyte_raw_id": "4450e616-fb46-458e-aa78-16c1f631dbfb",
                "_airbyte_job_started_at": "2024-03-15T16:01:07.374909",
                "_airbyte_slice_started_at": "2024-03-15T16:01:07.374909",
                "_airbyte_extracted_at": None,
                "_airbyte_loaded_at": "2024-03-15T16:01:13.809156",
                "_airbyte_data": '{"type": "STREAM", "stream": {"stream_descriptor": {"name": "commits", "namespace": null}, "stream_state": {"pathwaycom/pathway": {"main": {"created_at": "2024-03-15T11:13:53Z"}}}}}',  # noqa
            }
        ],
    )
    assert destination._state == {
        "commits": {
            "pathwaycom/pathway": {"main": {"created_at": "2024-03-15T11:13:53Z"}}
        },
    }
    destination._write(
        "_airbyte_states",
        [
            {
                "_airbyte_raw_id": "4450e616-fb46-458e-aa78-16c1f631dbfb",
                "_airbyte_job_started_at": "2024-03-15T16:01:07.374909",
                "_airbyte_slice_started_at": "2024-03-15T16:01:07.374909",
                "_airbyte_extracted_at": None,
                "_airbyte_loaded_at": "2024-03-15T16:01:13.809156",
                "_airbyte_data": '{"type": "STREAM", "stream": {"stream_descriptor": {"name": "not_commits", "namespace": null}, "stream_state": {"pathwaycom/pathway": {"main": {"created_at": "2024-03-15T11:13:54Z"}}}}}',  # noqa
            }
        ],
    )
    assert destination._state == {
        "commits": {
            "pathwaycom/pathway": {"main": {"created_at": "2024-03-15T11:13:53Z"}}
        },
        "not_commits": {
            "pathwaycom/pathway": {"main": {"created_at": "2024-03-15T11:13:54Z"}}
        },
    }
    # Check that the state for a stream is updated
    destination._write(
        "_airbyte_states",
        [
            {
                "_airbyte_raw_id": "4450e616-fb46-458e-aa78-16c1f631dbfb",
                "_airbyte_job_started_at": "2024-03-15T16:01:07.374909",
                "_airbyte_slice_started_at": "2024-03-15T16:01:07.374909",
                "_airbyte_extracted_at": None,
                "_airbyte_loaded_at": "2024-03-15T16:01:13.809156",
                "_airbyte_data": '{"type": "STREAM", "stream": {"stream_descriptor": {"name": "commits", "namespace": null}, "stream_state": {"pathwaycom/pathway": {"main": {"created_at": "2024-03-15T11:13:55Z"}}}}}',  # noqa
            }
        ],
    )
    assert destination._state == {
        "commits": {
            "pathwaycom/pathway": {"main": {"created_at": "2024-03-15T11:13:55Z"}}
        },
        "not_commits": {
            "pathwaycom/pathway": {"main": {"created_at": "2024-03-15T11:13:54Z"}}
        },
    }


def test_airbyte_global_state():
    destination = _PathwayAirbyteDestination(print, print)
    assert destination._state == {}
    assert destination._shared_state is None
    destination._write(
        "_airbyte_states",
        [
            {
                "_airbyte_raw_id": "4450e616-fb46-458e-aa78-16c1f631dbfb",
                "_airbyte_job_started_at": "2024-03-15T16:01:07.374909",
                "_airbyte_slice_started_at": "2024-03-15T16:01:07.374909",
                "_airbyte_extracted_at": None,
                "_airbyte_loaded_at": "2024-03-15T16:01:13.809156",
                "_airbyte_data": '{"type": "GLOBAL", "global": {"stream_states": [{"stream_descriptor": {"name": "commits"}, "stream_state": {"pathwaycom/pathway": {"main": {"created_at": "2024-08-29T22:00:10Z"}}}}]}}',  # noqa
            }
        ],
    )
    assert destination._state == {
        "commits": {
            "pathwaycom/pathway": {"main": {"created_at": "2024-08-29T22:00:10Z"}}
        },
    }
    assert destination._shared_state is None
    destination._write(
        "_airbyte_states",
        [
            {
                "_airbyte_raw_id": "4450e616-fb46-458e-aa78-16c1f631dbfb",
                "_airbyte_job_started_at": "2024-03-15T16:01:07.374909",
                "_airbyte_slice_started_at": "2024-03-15T16:01:07.374909",
                "_airbyte_extracted_at": None,
                "_airbyte_loaded_at": "2024-03-15T16:01:13.809156",
                "_airbyte_data": '{"type": "GLOBAL", "global": {"shared_state": "shared_test", "stream_states": [{"stream_descriptor": {"name": "commits"}, "stream_state": {"pathwaycom/pathway": {"main": {"created_at": "2024-08-29T22:00:10Z"}}}}]}}',  # noqa
            }
        ],
    )
    assert destination._state == {
        "commits": {
            "pathwaycom/pathway": {"main": {"created_at": "2024-08-29T22:00:10Z"}}
        },
    }
    assert destination._shared_state == "shared_test"
    destination._write(
        "_airbyte_states",
        [
            {
                "_airbyte_raw_id": "4450e616-fb46-458e-aa78-16c1f631dbfb",
                "_airbyte_job_started_at": "2024-03-15T16:01:07.374909",
                "_airbyte_slice_started_at": "2024-03-15T16:01:07.374909",
                "_airbyte_extracted_at": None,
                "_airbyte_loaded_at": "2024-03-15T16:01:13.809156",
                "_airbyte_data": '{"type": "GLOBAL", "global": {"stream_states": [{"stream_descriptor": {"name": "commits"}, "stream_state": {"pathwaycom/pathway": {"main": {"created_at": "2024-08-29T22:00:10Z"}}}}]}}',  # noqa
            }
        ],
    )
    assert destination._state == {
        "commits": {
            "pathwaycom/pathway": {"main": {"created_at": "2024-08-29T22:00:10Z"}}
        },
    }
    assert destination._shared_state is None


def test_airbyte_legacy_state():
    destination = _PathwayAirbyteDestination(lambda x: print(x), lambda x: print(x))
    assert destination._state == {}
    destination._write(
        "_airbyte_states",
        [
            {
                "_airbyte_raw_id": "b677b191-e0f5-4852-965c-c76e47127c99",
                "_airbyte_job_started_at": "2024-03-17T19:19:45.269451",
                "_airbyte_slice_started_at": "2024-03-17T19:19:45.269451",
                "_airbyte_extracted_at": None,
                "_airbyte_loaded_at": "2024-03-17T19:19:50.820893",
                "_airbyte_data": '{"data": {"commits": {"pathwaycom/pathway": {"main": {"created_at": "2024-03-15T18:24:20Z"}}}}}',  # noqa
            }
        ],
    )
    assert destination._state == {
        "commits": {
            "pathwaycom/pathway": {"main": {"created_at": "2024-03-15T18:24:20Z"}}
        },
    }


def test_subdirectories(tmp_path: pathlib.Path):
    nested_inputs_path = (
        tmp_path / "nested_level_1" / "nested_level_2" / "nested_level_3"
    )
    os.makedirs(nested_inputs_path)
    output_path = tmp_path / "output.json"
    write_lines(nested_inputs_path / "a.txt", "a\nb\nc")

    table = pw.io.plaintext.read(tmp_path / "nested_level_1", mode="static")
    pw.io.jsonlines.write(table, output_path)
    pw.run()

    assert FileLinesNumberChecker(output_path, 3)()


def test_glob_pattern(tmp_path: pathlib.Path):
    nested_inputs_path = (
        tmp_path / "nested_level_1" / "nested_level_2" / "nested_level_3"
    )
    os.makedirs(nested_inputs_path)
    output_path = tmp_path / "output.json"
    write_lines(nested_inputs_path / "a.txt", "a\nb\nc")
    write_lines(nested_inputs_path / "b.txt", "d\ne\nf\ng")

    table = pw.io.plaintext.read(tmp_path / "nested_level_1/**/b.txt", mode="static")
    pw.io.jsonlines.write(table, output_path)
    pw.run()

    assert FileLinesNumberChecker(output_path, 4)()


def test_glob_pattern_recurse_subdirs(tmp_path: pathlib.Path):
    os.makedirs(tmp_path / "input" / "foo" / "level2")
    write_lines(tmp_path / "input" / "foo" / "level2" / "a.txt", "a\nb\nc")
    write_lines(tmp_path / "input" / "f1.txt", "d\ne\nf\ng")
    write_lines(tmp_path / "input" / "bar.txt", "h\ni\nj\nk\nl")
    output_path = tmp_path / "output.json"

    table = pw.io.plaintext.read(tmp_path / "input/f*", mode="static")
    pw.io.jsonlines.write(table, output_path)
    pw.run()

    assert FileLinesNumberChecker(output_path, 7)()


def test_glob_pattern_nothing_matched(tmp_path: pathlib.Path):
    os.makedirs(tmp_path / "input" / "foo" / ".level2")
    write_lines(tmp_path / "input" / "foo" / ".level2" / ".a.txt", "a\nb\nc")
    write_lines(tmp_path / "input" / "f1.txt", "d\ne\nf\ng")
    write_lines(tmp_path / "input" / "bar.txt", "h\ni\nj\nk\nl")
    output_path = tmp_path / "output.json"

    table = pw.io.plaintext.read(tmp_path / "input/f", mode="static")
    pw.io.jsonlines.write(table, output_path)
    pw.run()

    assert FileLinesNumberChecker(output_path, 0)()


def test_raw_kafka_raises_no_value_specified_for_key():
    table = pw.Table.empty(data=bytes, _metadata=dict)
    with pytest.raises(
        ValueError,
        match="'value' must be specified if 'key' is not None",
    ):
        pw.io.kafka.write(
            table,
            topic_name="test",
            rdkafka_settings={},
            format="raw",
            key=table.data,
        )


def test_raw_kafka_raises_no_column_selected():
    table = pw.Table.empty(data=bytes, _metadata=dict)
    with pytest.raises(
        ValueError,
        match="'raw' format without explicit 'value' specification can only be used with single-column tables",
    ):
        pw.io.kafka.write(
            table,
            topic_name="test",
            rdkafka_settings={},
            format="raw",
        )


def test_raw_kafka_raises_wrong_type():
    table = pw.Table.empty(data=bytes, _metadata=dict)
    with pytest.raises(
        ValueError,
        match="The key column should be of the type 'BYTES'",
    ):
        pw.io.kafka.write(
            table,
            topic_name="test",
            rdkafka_settings={},
            format="raw",
            key=table._metadata,
            value=table.data,
        )

    with pytest.raises(
        ValueError,
        match="The value column should be of the type 'BYTES'",
    ):
        pw.io.kafka.write(
            table,
            topic_name="test",
            rdkafka_settings={},
            format="raw",
            value=table._metadata,
        )


def test_non_ascii_characters(tmp_path: pathlib.Path):
    output_path = tmp_path / "output.csv"

    @pw.udf
    def replace_some_chars(a: str) -> str:
        return a.replace("b", "\n")

    t = pw.debug.table_from_markdown(
        """
        data
        aba
        ąęćśż
        قطة
    """
    ).select(data=replace_some_chars(pw.this.data))
    pw.io.csv.write(t, output_path)

    run_all()

    result = pd.read_csv(output_path)
    answers = set(result["data"])
    expected = ["a\\na", "ąęćśż", "قطة"]
    for word in expected:
        assert word in answers


@only_with_license_key
def test_deltalake_simple(tmp_path: pathlib.Path):
    data = """
        k | v
        1 | foo
        2 | bar
        3 | baz
    """
    input_path = tmp_path / "input.csv"
    output_path = tmp_path / "output"
    write_csv(input_path, data)

    class InputSchema(pw.Schema):
        k: int = pw.column_definition(primary_key=True)
        v: str

    table = pw.io.csv.read(str(input_path), schema=InputSchema, mode="static")
    pw.io.deltalake.write(table, str(output_path))
    run_all()

    delta_table = DeltaTable(output_path)
    pd_table_from_delta = (
        delta_table.to_pandas().drop("time", axis=1).drop("diff", axis=1)
    )

    assert_table_equality(
        table,
        pw.debug.table_from_pandas(
            pd_table_from_delta,
            schema=InputSchema,
        ),
    )


@pytest.mark.parametrize("min_commit_frequency", [None, 60_000])
@only_with_license_key
def test_deltalake_append(min_commit_frequency, tmp_path: pathlib.Path):
    data = """
        k | v
        1 | foo
        2 | bar
        3 | baz
    """
    input_path = tmp_path / "input.csv"
    output_path = tmp_path / "output"
    write_csv(input_path, data)

    class InputSchema(pw.Schema):
        k: int = pw.column_definition(primary_key=True)
        v: str

    def iteration():
        G.clear()
        table = pw.io.csv.read(str(input_path), schema=InputSchema, mode="static")
        pw.io.deltalake.write(
            table,
            str(output_path),
            min_commit_frequency=min_commit_frequency,
        )
        run_all()

    iteration()
    iteration()

    delta_table = DeltaTable(output_path)
    pd_table_from_delta = delta_table.to_pandas()
    assert pd_table_from_delta.shape[0] == 6


@needs_multiprocessing_fork
@pytest.mark.parametrize("env_vars", [None, {"''": "\"''''\"\""}, {"KEY": "VALUE"}])
@pytest.mark.xfail(reason="fails randomly")
def test_airbyte_local_run(env_vars, tmp_path_with_airbyte_config):
    table = pw.io.airbyte.read(
        tmp_path_with_airbyte_config / AIRBYTE_FAKER_CONNECTION_REL_PATH,
        ["users"],
        mode="static",
        execution_type="local",
        env_vars=env_vars,
    )

    with open(
        tmp_path_with_airbyte_config / AIRBYTE_FAKER_CONNECTION_REL_PATH, "r"
    ) as f:
        config = yaml.safe_load(f)["source"]
    airbyte_source = pw.io.airbyte._construct_local_source(
        config,
        streams=["users"],
        env_vars=env_vars,
    )
    assert isinstance(airbyte_source, VenvAirbyteSource)

    output_path = tmp_path_with_airbyte_config / "table.jsonl"
    pw.io.jsonlines.write(table, output_path)
    run_all()
    total_lines = 0
    with open(output_path, "r") as f:
        for _ in f:
            total_lines += 1
    assert total_lines == 500


@needs_multiprocessing_fork
@pytest.mark.parametrize("env_vars", [None, {"''": "\"''''\"\""}, {"KEY": "VALUE"}])
@pytest.mark.xfail(reason="fails randomly")
def test_airbyte_local_docker_run(env_vars, tmp_path_with_airbyte_config):
    table = pw.io.airbyte.read(
        tmp_path_with_airbyte_config / AIRBYTE_FAKER_CONNECTION_REL_PATH,
        ["users"],
        mode="static",
        execution_type="local",
        env_vars=env_vars,
        enforce_method="docker",
    )

    with open(
        tmp_path_with_airbyte_config / AIRBYTE_FAKER_CONNECTION_REL_PATH, "r"
    ) as f:
        config = yaml.safe_load(f)["source"]
    airbyte_source = pw.io.airbyte._construct_local_source(
        config,
        streams=["users"],
        env_vars=env_vars,
        enforce_method="docker",
    )
    assert isinstance(airbyte_source, DockerAirbyteSource)

    output_path = tmp_path_with_airbyte_config / "table.jsonl"
    pw.io.jsonlines.write(table, output_path)
    run_all()
    total_lines = 0
    with open(output_path, "r") as f:
        for _ in f:
            total_lines += 1
    assert total_lines == 500


@only_with_license_key
@pytest.mark.parametrize("has_primary_key", [True, False])
def test_deltalake_roundtrip(has_primary_key: bool, tmp_path: pathlib.Path):
    data = """
        k | v
        1 | foo
        2 | bar
        3 | baz
    """
    input_path = tmp_path / "input.csv"
    lake_path = tmp_path / "lake"
    output_path = tmp_path / "output.csv"
    write_csv(input_path, data)

    class InputSchema(pw.Schema):
        k: int = pw.column_definition(primary_key=has_primary_key)
        v: str

    table = pw.io.csv.read(str(input_path), schema=InputSchema, mode="static")
    pw.io.deltalake.write(table, str(lake_path))
    run_all()

    G.clear()
    table = pw.io.deltalake.read(str(lake_path), schema=InputSchema, mode="static")
    pw.io.csv.write(table, output_path)
    run_all()

    final = pd.read_csv(output_path, usecols=["k", "v"], index_col=["k"]).sort_index()
    original = pd.read_csv(input_path, usecols=["k", "v"], index_col=["k"]).sort_index()
    assert final.equals(original)


@pytest.mark.parametrize(
    "snapshot_access", [api.SnapshotAccess.FULL, api.SnapshotAccess.OFFSETS_ONLY]
)
@only_with_license_key
def test_deltalake_recovery(snapshot_access, tmp_path: pathlib.Path):
    data = [{"k": 1, "v": "one"}, {"k": 2, "v": "two"}, {"k": 3, "v": "three"}]
    df = pd.DataFrame(data).set_index("k")
    lake_path = str(tmp_path / "lake")
    output_path = str(tmp_path / "output.csv")
    write_deltalake(lake_path, df)

    class InputSchema(pw.Schema):
        k: int = pw.column_definition(primary_key=True)
        v: str

    def run_pathway_program(expected_key_set, expected_diff=1):
        table = pw.io.deltalake.read(lake_path, schema=InputSchema, mode="static")
        pw.io.csv.write(table, output_path)

        persistence_config = pw.persistence.Config(
            pw.persistence.Backend.filesystem(tmp_path / "PStorage"),
            snapshot_access=snapshot_access,
        )
        run_all(persistence_config=persistence_config)
        try:
            result = pd.read_csv(output_path)
            assert set(result["k"]) == expected_key_set
            assert set(result["diff"]) == {expected_diff}
        except pd.errors.EmptyDataError:
            assert expected_key_set == {}
        G.clear()

    run_pathway_program({1, 2, 3})

    # The second run: only two added rows must be read
    additional_data = [{"k": 7, "v": "seven"}, {"k": 8, "v": "eight"}]
    df = pd.DataFrame(additional_data).set_index("k")
    write_deltalake(lake_path, df, mode="append")
    run_pathway_program({7, 8})

    # The third run: add three more rows
    additional_data = [
        {"k": 4, "v": "four"},
        {"k": 5, "v": "five"},
        {"k": 6, "v": "six"},
    ]
    df = pd.DataFrame(additional_data).set_index("k")
    write_deltalake(lake_path, df, mode="append")
    run_pathway_program({4, 5, 6})

    # The fourth run: optimize the storage, and then write some data
    DeltaTable(lake_path).optimize()
    additional_data = [{"k": 9, "v": "nine"}]
    df = pd.DataFrame(additional_data).set_index("k")
    write_deltalake(lake_path, df, mode="append")
    run_pathway_program({9})

    # The fifth run: only reorganize the data according to Z-order
    DeltaTable(lake_path).optimize.z_order("k")
    run_pathway_program({})

    # The sixth run: add some data on top of the reordered table
    additional_data = [{"k": 10, "v": "ten"}]
    df = pd.DataFrame(additional_data).set_index("k")
    write_deltalake(lake_path, df, mode="append")
    run_pathway_program({10})

    # The seventh run: remove some data from the table
    condition = "k > 4 and k < 8"
    table = DeltaTable(lake_path)
    table.delete(condition)
    run_pathway_program({5, 6, 7}, -1)


@only_with_license_key
def test_deltalake_read_after_modification(tmp_path):
    data = [
        {"k": 1, "v": "one"},
        {"k": 2, "v": "two"},
        {"k": 3, "v": "three"},
        {"k": 4, "v": "four"},
        {"k": 5, "v": "five"},
        {"k": 6, "v": "six"},
    ]
    df = pd.DataFrame(data).set_index("k")
    lake_path = str(tmp_path / "lake")
    output_path = str(tmp_path / "output.csv")
    write_deltalake(lake_path, df)

    condition = "k > 1 and k < 5"
    table = DeltaTable(lake_path)
    table.delete(condition)

    class InputSchema(pw.Schema):
        k: int = pw.column_definition(primary_key=True)
        v: str

    table = pw.io.deltalake.read(lake_path, schema=InputSchema, mode="static")
    pw.io.csv.write(table, output_path)
    pw.run()

    result = pd.read_csv(output_path)
    assert set(result["k"]) == {1, 5, 6}


@needs_multiprocessing_fork
@only_with_license_key
def test_streaming_from_deltalake(tmp_path):
    lake_path = str(tmp_path / "lake")
    output_path = tmp_path / "output.csv"

    class InputSchema(pw.Schema):
        k: int = pw.column_definition(primary_key=True)
        v: str

    data = [{"k": 0, "v": ""}]
    df = pd.DataFrame(data).set_index("k")
    write_deltalake(lake_path, df, mode="append")

    def create_new_versions(start_idx, end_idx):
        for idx in range(start_idx, end_idx):
            data = [{"k": idx, "v": "a" * idx}]
            df = pd.DataFrame(data).set_index("k")
            write_deltalake(lake_path, df, mode="append")
            time.sleep(1.0)

    t = threading.Thread(target=create_new_versions, args=(1, 10))
    t.start()
    table = pw.io.deltalake.read(
        lake_path, schema=InputSchema, autocommit_duration_ms=10
    )
    pw.io.csv.write(table, output_path)
    wait_result_with_checker(CsvLinesNumberChecker(output_path, 10), 30)


@needs_multiprocessing_fork
@pytest.mark.parametrize("enforce_method", ["venv", "docker"])
@pytest.mark.xfail(reason="fails randomly")
def test_airbyte_persistence(enforce_method, tmp_path_with_airbyte_config):
    output_path = tmp_path_with_airbyte_config / "table.jsonl"
    pstorage_path = tmp_path_with_airbyte_config / "PStorage"

    def run_pathway_program(n_expected_records):
        table = pw.io.airbyte.read(
            tmp_path_with_airbyte_config / AIRBYTE_FAKER_CONNECTION_REL_PATH,
            ["users"],
            mode="static",
            execution_type="local",
            enforce_method=enforce_method,
        )
        pw.io.jsonlines.write(table, output_path)
        run_all(
            persistence_config=pw.persistence.Config(
                backend=pw.persistence.Backend.filesystem(pstorage_path),
                snapshot_access=api.SnapshotAccess.OFFSETS_ONLY,
            )
        )

        total_lines = 0
        with open(output_path, "r") as f:
            for _ in f:
                total_lines += 1
        assert total_lines == n_expected_records
        G.clear()

    run_pathway_program(500)
    run_pathway_program(0)


@needs_multiprocessing_fork
def test_airbyte_persistence_error_message(tmp_path_with_airbyte_config):
    output_path = tmp_path_with_airbyte_config / "table.jsonl"
    pstorage_path = tmp_path_with_airbyte_config / "PStorage"
    table = pw.io.airbyte.read(
        tmp_path_with_airbyte_config / AIRBYTE_FAKER_CONNECTION_REL_PATH,
        streams=["users", "purchases"],
        mode="static",
    )
    pw.io.jsonlines.write(table, output_path)
    with pytest.raises(
        RuntimeError,
        match="Persistence in airbyte connector is supported only for the case of a single stream. "
        "Please use several airbyte connectors with one stream per connector to persist the state.",
    ):
        run_all(
            persistence_config=pw.persistence.Config(
                backend=pw.persistence.Backend.filesystem(pstorage_path),
                snapshot_access=api.SnapshotAccess.OFFSETS_ONLY,
            )
        )


@needs_multiprocessing_fork
def test_persistence_one_worker_has_no_committed_timestamp(tmp_path):
    # This test only makes sense when there are at least two Pathway workers
    input_path = tmp_path / "input.csv"
    output_path = tmp_path / "output.csv"
    persistent_storage_path = tmp_path / "PStorage"
    data = """
        k | v
        1 | foo
        2 | bar
        3 | baz
    """
    write_csv(input_path, data)

    def run_identity_transformation():
        G.clear()

        class InputSchema(pw.Schema):
            k: int = pw.column_definition(primary_key=True)
            v: str

        table = pw.io.csv.read(input_path, schema=InputSchema, mode="static")
        pw.io.csv.write(table, output_path)
        persistence_config = pw.persistence.Config(
            pw.persistence.Backend.filesystem(persistent_storage_path),
        )
        run_all(persistence_config=persistence_config)

    run_identity_transformation()
    result = pd.read_csv(output_path, usecols=["k", "v"], index_col=["k"]).sort_index()
    expected = pd.read_csv(input_path, usecols=["k", "v"], index_col=["k"]).sort_index()
    assert result.equals(expected)

    # Remove metadata saved by the worker `0`
    for file in persistent_storage_path.iterdir():
        if file.is_file() and file.match("*-0-[01]"):
            file.unlink()

    # Even though there are other workers that still have the advanced time,
    # we must run the whole process again
    run_identity_transformation()
    result = pd.read_csv(output_path, usecols=["k", "v"], index_col=["k"]).sort_index()
    expected = pd.read_csv(input_path, usecols=["k", "v"], index_col=["k"]).sort_index()
    assert result.equals(expected)


@only_with_license_key
def test_deltalake_no_primary_key(tmp_path: pathlib.Path):
    data = [{"k": 1, "v": "one"}, {"k": 2, "v": "two"}, {"k": 3, "v": "three"}]
    df = pd.DataFrame(data).set_index("k")
    lake_path = str(tmp_path / "lake")
    output_path = str(tmp_path / "output.csv")
    write_deltalake(lake_path, df)

    class InputSchema(pw.Schema):
        k: int
        v: str

    table = pw.io.deltalake.read(lake_path, schema=InputSchema)
    pw.io.jsonlines.write(table, output_path)
    with pytest.raises(
        OSError,
        match=(
            "Failed to connect to DeltaLake: explicit primary key specification is "
            "required for non-append-only tables"
        ),
    ):
        pw.run()


def test_iceberg_no_primary_key():
    class InputSchema(pw.Schema):
        k: int
        v: str

    with pytest.raises(
        ValueError,
        match="Iceberg reader requires explicit primary key fields specification",
    ):
        pw.io.iceberg.read(
            catalog_uri="http://localhost:8181",
            namespace=["app"],
            table_name="test",
            schema=InputSchema,
        )


@pytest.mark.parametrize("data_format", ["delta", "json"])
@only_with_license_key("data_format", ["delta"])
def test_py_object_wrapper_serialization(tmp_path: pathlib.Path, data_format):
    input_path = tmp_path / "input.jsonl"
    auxiliary_path = tmp_path / "auxiliary-storage"
    output_path = tmp_path / "output.jsonl"
    input_path.write_text("test")

    table = pw.io.plaintext.read(input_path, mode="static")
    table = table.select(
        data=pw.this.data, fun=pw.wrap_py_object(len, serializer=pickle)  # type: ignore
    )
    if data_format == "delta":
        pw.io.deltalake.write(table, auxiliary_path)
    elif data_format == "json":
        pw.io.jsonlines.write(table, auxiliary_path)
    else:
        raise ValueError(f"Unknown data format: {data_format}")
    run_all()
    G.clear()

    class InputSchema(pw.Schema):
        data: str = pw.column_definition(primary_key=True)
        fun: pw.PyObjectWrapper

    @pw.udf
    def use_python_object(a: pw.PyObjectWrapper, x: str) -> int:
        return a.value(x)

    if data_format == "delta":
        table = pw.io.deltalake.read(auxiliary_path, schema=InputSchema, mode="static")
    elif data_format == "json":
        table = pw.io.jsonlines.read(auxiliary_path, schema=InputSchema, mode="static")
    else:
        raise ValueError(f"Unknown data format: {data_format}")
    table = table.select(len=use_python_object(pw.this.fun, pw.this.data))
    pw.io.jsonlines.write(table, output_path)
    run_all()

    with open(output_path, "r") as f:
        data = json.load(f)
        assert data["len"] == 4


@pytest.mark.parametrize("data_format", ["delta", "json"])
@only_with_license_key("data_format", ["delta"])
def test_different_types_serialization(tmp_path: pathlib.Path, data_format):
    input_path = tmp_path / "input.jsonl"
    auxiliary_path = tmp_path / "auxiliary-storage"
    input_path.write_text("test")

    column_values = {
        "boolean": True,
        "integer": 123,
        "double": -5.6,
        "string": "abcdef",
        "binary_data": b"fedcba",
        "datetime_naive": pw.DateTimeNaive(year=2025, month=1, day=17),
        "datetime_utc_aware": pw.DateTimeUtc(year=2025, month=1, day=17, tz=tz.UTC),
        "duration": pw.Duration(days=5),
        "ints": np.array([[[1, 2], [3, 4]], [[5, 6], [7, 8]]], dtype=int),
        "floats": np.array([[1.1, 2.2], [3.3, 4.4]], dtype=float),
        "ints_flat": np.array([9, 9, 9], dtype=int),
        "floats_flat": np.array([1.1, 2.2, 3.3], dtype=float),
        "json_data": pw.Json.parse('{"a": 15, "b": "hello"}'),
        "tuple_data": (b"world", True),
        "list_data": ("lorem", None, "ipsum"),
    }
    table = pw.io.plaintext.read(input_path, mode="static")
    table = table.select(
        data=pw.this.data,
        **column_values,
    )
    table = table.update_types(
        ints=np.ndarray[None, int],
        floats=np.ndarray[None, float],
        ints_flat=np.ndarray[None, int],
        floats_flat=np.ndarray[None, float],
        tuple_data=tuple[bytes, bool],
        list_data=list[str | None],
    )
    if data_format == "delta":
        pw.io.deltalake.write(table, auxiliary_path)
    elif data_format == "json":
        pw.io.jsonlines.write(table, auxiliary_path)
    else:
        raise ValueError(f"Unknown data format: {data_format}")

    run_all()
    G.clear()

    class InputSchema(pw.Schema):
        data: str = pw.column_definition(primary_key=True)
        boolean: bool
        integer: int
        double: float
        string: str
        binary_data: bytes
        datetime_naive: pw.DateTimeNaive
        datetime_utc_aware: pw.DateTimeUtc
        duration: pw.Duration
        ints: np.ndarray[None, int]
        floats: np.ndarray[None, float]
        ints_flat: np.ndarray[None, int]
        floats_flat: np.ndarray[None, float]
        json_data: pw.Json
        tuple_data: tuple[bytes, bool]
        list_data: list[str | None]

    class Checker:
        def __init__(self):
            self.n_processed_rows = 0

        def __call__(self, key, row, time, is_addition):
            self.n_processed_rows += 1
            for field, expected_value in column_values.items():
                if isinstance(expected_value, np.ndarray):
                    assert row[field].shape == expected_value.shape
                    assert (row[field] == expected_value).all()
                else:
                    assert row[field] == expected_value

    if data_format == "delta":
        table = pw.io.deltalake.read(auxiliary_path, schema=InputSchema, mode="static")
    elif data_format == "json":
        table = pw.io.jsonlines.read(auxiliary_path, schema=InputSchema, mode="static")
    else:
        raise ValueError(f"Unknown data format: {data_format}")

    checker = Checker()
    pw.io.subscribe(table, on_change=checker)
    run_all()
    assert checker.n_processed_rows == 1


@only_with_license_key
def test_deltalake_start_from_timestamp(tmp_path: pathlib.Path):
    data_first_run = """
        k | v
        1 | foo
        2 | bar
        3 | baz
    """
    data_second_run = """
        k | v
        4 | foo
        5 | bar
        6 | baz
    """
    data_third_run = """
        k | v
        7 | foo
        8 | bar
        9 | baz
    """
    input_path = tmp_path / "input.csv"
    output_path = tmp_path / "output"
    temp_reread_path = tmp_path / "reread.jsonl"

    class InputSchema(pw.Schema):
        k: int = pw.column_definition(primary_key=True)
        v: str

    def write_data(data: str):
        write_csv(input_path, data)
        G.clear()
        table = pw.io.csv.read(input_path, schema=InputSchema, mode="static")
        pw.io.deltalake.write(table, output_path)
        run_all()

    def check_entries_count(start_from_timestamp_ms: int, expected_entries: int):
        G.clear()
        table = pw.io.deltalake.read(
            output_path,
            schema=InputSchema,
            mode="static",
            start_from_timestamp_ms=start_from_timestamp_ms,
        )
        pw.io.jsonlines.write(table, temp_reread_path)
        run_all()
        n_rows = 0
        with open(temp_reread_path, "r") as f:
            for _ in f:
                n_rows += 1
        assert n_rows == expected_entries

    start_time_9 = int(time.time() * 1000)
    time.sleep(0.025)
    write_data(data_first_run)
    time.sleep(0.025)

    start_time_6 = int(time.time() * 1000)
    time.sleep(0.025)
    write_data(data_second_run)
    time.sleep(0.025)

    start_time_3 = int(time.time() * 1000)
    time.sleep(0.025)
    write_data(data_third_run)
    time.sleep(0.025)

    start_time_0 = int(time.time() * 1000)

    check_entries_count(start_time_0, 0)
    check_entries_count(start_time_3, 3)
    check_entries_count(start_time_6, 6)
    check_entries_count(start_time_9, 9)


def test_message_queue_topic_name_error(tmp_path: pathlib.Path):
    input_path = tmp_path / "input.jsonl"

    class InputSchema(pw.Schema):
        k: int = pw.column_definition(primary_key=True)
        v: str

    table = pw.io.jsonlines.read(input_path, schema=InputSchema)
    with pytest.raises(
        ValueError,
        match="The topic name column must have a string type, however <class 'int'> is used",
    ):
        pw.io.nats.write(table, "nats://nats:4448", topic=table.k, format="json")
    with pytest.raises(
        ValueError,
        match="The topic name column must have a string type, however <class 'int'> is used",
    ):
        pw.io.kafka.write(table, {}, topic_name=table.k, format="json")


def test_output_column_sorting_by_references(tmp_path: pathlib.Path):
    data = """
        k | v   | vv
        1 | foo | bar
        2 | bar | bar
        3 | baz | baz
    """
    input_path = tmp_path / "input.csv"
    output_path = tmp_path / "output.jsonl"
    write_csv(input_path, data)

    class InputSchema(pw.Schema):
        k: int = pw.column_definition(primary_key=True)
        v: str
        vv: str

    class TestCallback:
        def __init__(self):
            self.target_column_values = []

        def __call__(self, key, row, time, is_addition):
            self.target_column_values.append(row["k"])

    def check_indices_in_output(callback: TestCallback, expected_indices: list[int]):
        run_all()
        indices_in_output = []
        with open(output_path, "r") as f:
            for line in f:
                data = json.loads(line)
                indices_in_output.append(data["k"])
        assert indices_in_output == expected_indices
        assert callback.target_column_values == expected_indices
        G.clear()

    on_change_callback = TestCallback()
    table = pw.io.csv.read(input_path, schema=InputSchema, mode="static")
    pw.io.fs.write(table, output_path, format="json", sort_by=[table.v])
    pw.io.subscribe(table, on_change_callback, sort_by=[table.v])
    check_indices_in_output(on_change_callback, [2, 3, 1])

    on_change_callback = TestCallback()
    table = pw.io.csv.read(input_path, schema=InputSchema, mode="static")
    pw.io.fs.write(table, output_path, format="json", sort_by=[table.vv, table.v])
    pw.io.subscribe(table, on_change_callback, sort_by=[table.vv, table.v])
    check_indices_in_output(on_change_callback, [2, 1, 3])

    on_change_callback = TestCallback()
    table = pw.io.csv.read(input_path, schema=InputSchema, mode="static")
    pw.io.fs.write(table, output_path, format="json", sort_by=[table.k])
    pw.io.subscribe(table, on_change_callback, sort_by=[table.k])
    check_indices_in_output(on_change_callback, [1, 2, 3])


def test_output_column_sorting_foreign_columns_error_fs(tmp_path: pathlib.Path):
    input_path_1 = tmp_path / "input_1.csv"
    input_path_2 = tmp_path / "input_2.csv"
    output_path = tmp_path / "output.jsonl"

    class InputSchema(pw.Schema):
        k: int = pw.column_definition(primary_key=True)
        v: str
        vv: str

    table_1 = pw.io.csv.read(input_path_1, schema=InputSchema, mode="static")
    table_2 = pw.io.csv.read(input_path_2, schema=InputSchema, mode="static")

    with pytest.raises(
        ValueError,
        match="The column <table1>.v doesn't belong to the target table *",
    ):
        pw.io.fs.write(table_1, output_path, format="json", sort_by=[table_2.v])


def test_output_column_sorting_foreign_columns_error_subscribe(tmp_path: pathlib.Path):
    input_path_1 = tmp_path / "input_1.csv"
    input_path_2 = tmp_path / "input_2.csv"

    class InputSchema(pw.Schema):
        k: int = pw.column_definition(primary_key=True)
        v: str
        vv: str

    table_1 = pw.io.csv.read(input_path_1, schema=InputSchema, mode="static")
    table_2 = pw.io.csv.read(input_path_2, schema=InputSchema, mode="static")
    with pytest.raises(
        ValueError,
        match="The column <table1>.v doesn't belong to the target table *",
    ):
        pw.io.subscribe(table_1, lambda **kwargs: print(kwargs), sort_by=[table_2.v])


@only_with_license_key
def test_deltalake_partition_columns(tmp_path: pathlib.Path):
    data = """
        k | v
        1 | foo
        2 | bar
        3 | baz
    """
    input_path = tmp_path / "input.csv"
    output_path = tmp_path / "output"
    write_csv(input_path, data)

    class InputSchema(pw.Schema):
        k: int = pw.column_definition(primary_key=True)
        v: str

    table = pw.io.csv.read(str(input_path), schema=InputSchema, mode="static")
    pw.io.deltalake.write(table, str(output_path), partition_columns=[table.k])
    run_all()

    delta_table = DeltaTable(output_path)
    assert delta_table._table.metadata().partition_columns == ["k"]
    pd_table_from_delta = (
        delta_table.to_pandas().drop("time", axis=1).drop("diff", axis=1)
    )

    assert_table_equality(
        table,
        pw.debug.table_from_pandas(
            pd_table_from_delta,
            schema=InputSchema,
        ),
    )


@only_with_license_key
def test_deltalake_partition_columns_unknown(tmp_path: pathlib.Path):
    input_path_1 = tmp_path / "input_1.csv"
    input_path_2 = tmp_path / "input_2.csv"
    output_path = tmp_path / "output"

    class InputSchema(pw.Schema):
        k: int = pw.column_definition(primary_key=True)
        v: str
        vv: str

    table_1 = pw.io.csv.read(input_path_1, schema=InputSchema, mode="static")
    table_2 = pw.io.csv.read(input_path_2, schema=InputSchema, mode="static")
    with pytest.raises(
        ValueError,
        match="The suggested partition column <table1>.v doesn't belong to the table *",
    ):
        pw.io.deltalake.write(table_1, output_path, partition_columns=[table_2.v])


@only_with_license_key
def test_deltalake_schema_mismatch(tmp_path: pathlib.Path):
    data_1 = """
        k | v
        1 | foo
        2 | bar
        3 | baz
    """
    data_2 = """
        k | vv
        1 | foo
        2 | bar
        3 | baz
    """
    data_3 = """
        k | v
        1 | 1
        2 | 2
        3 | 3
    """
    input_path_1 = tmp_path / "input_1.csv"
    input_path_2 = tmp_path / "input_2.csv"
    input_path_3 = tmp_path / "input_3.csv"
    output_path = tmp_path / "output"
    write_csv(input_path_1, data_1)
    write_csv(input_path_2, data_2)
    write_csv(input_path_3, data_3)

    class InputSchema_1(pw.Schema):
        k: int = pw.column_definition(primary_key=True)
        v: str

    table = pw.io.csv.read(input_path_1, schema=InputSchema_1, mode="static")
    pw.io.deltalake.write(table, output_path)
    run_all()

    class InputSchema_2(pw.Schema):
        k: int = pw.column_definition(primary_key=True)
        vv: str

    G.clear()
    table = pw.io.csv.read(input_path_2, schema=InputSchema_2, mode="static")
    pw.io.deltalake.write(table, output_path)
    expected_message = (
        "Unable to create DeltaTable writer: "
        "delta table schema mismatch: "
        'Fields in the provided schema that aren\'t present in the existing table: ["vv"]; '
        'Fields in the existing table that aren\'t present in the provided schema: ["v"]'
    )
    with pytest.raises(TypeError, match=re.escape(expected_message)):
        run_all()

    class InputSchema_3(pw.Schema):
        k: int = pw.column_definition(primary_key=True)
        v: int

    G.clear()
    table = pw.io.csv.read(input_path_3, schema=InputSchema_3, mode="static")
    pw.io.deltalake.write(table, output_path)
    expected_message = (
        "Unable to create DeltaTable writer: "
        "delta table schema mismatch: "
        'Fields with mismatching types: [field "v": data type differs (existing table=string, schema=long)]'
    )
    with pytest.raises(TypeError, match=re.escape(expected_message)):
        run_all()


@only_with_license_key
def test_deltalake_schema_mismatch_with_optionality_and_metadata(
    tmp_path: pathlib.Path,
):
    data = """
        k | v
        1 | one
        2 | two
        3 | three
    """
    input_path = tmp_path / "input.csv"
    lake_path = tmp_path / "output"
    write_csv(input_path, data)

    lake_initial = [{"k": 0, "v": "zero", "time": 0, "diff": 1}]
    df = pd.DataFrame(lake_initial).set_index("k")
    schema = pa.schema(
        [
            pa.field("k", pa.int64(), nullable=False),
            pa.field("v", pa.string(), nullable=True, metadata={"description": "test"}),
            pa.field("time", pa.int64(), nullable=False),
            pa.field("diff", pa.int64(), nullable=False),
        ]
    )
    write_deltalake(lake_path, df, schema=schema)

    class InputSchema(pw.Schema):
        k: int = pw.column_definition(primary_key=True)
        v: str

    table = pw.io.csv.read(input_path, schema=InputSchema, mode="static")
    pw.io.deltalake.write(table, lake_path)
    expected_message = (
        "Unable to create DeltaTable writer: "
        "delta table schema mismatch: "
        'Fields with mismatching types: [field "v": nullability differs (existing table=true, schema=false)'
        ', metadata differs (existing table={"description": String("test")}, schema={})]'
    )
    with pytest.raises(TypeError, match=re.escape(expected_message)):
        run_all()
