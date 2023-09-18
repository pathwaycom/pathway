# Copyright © 2023 Pathway

import asyncio
import json
import os
import pathlib
import random
import sys
import threading
import time
from typing import Any, Dict
from unittest import mock

import pandas as pd
import pytest

import pathway as pw
from pathway.internals import api
from pathway.internals.parse_graph import G
from pathway.tests.utils import (
    CsvLinesNumberChecker,
    T,
    assert_table_equality,
    assert_table_equality_wo_index,
    assert_table_equality_wo_index_types,
    assert_table_equality_wo_types,
    run_all,
    wait_result_with_checker,
    write_csv,
    write_lines,
    xfail_on_darwin,
)


def start_streaming_inputs(inputs_path, n_files, stream_interval, data_format):
    os.mkdir(inputs_path)

    def stream_inputs():
        for i in range(n_files):
            file_path = inputs_path / "{}.csv".format(i)
            if data_format == "json":
                payload = {"k": i, "v": i}
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
                    f.write("{}".format(i))
            else:
                raise ValueError("Unknown format: {}".format(data_format))

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

    class TestSubject(pw.python.ConnectorSubject):
        def run(self):
            self.next_json({"x": 1, "y": 1})
            self.next_json({"x": 2, "y": 2})

    variants = []

    variants.append(pw.python.read(TestSubject(), schema=InputSchema, format="json"))
    variants.append(
        pw.python.read(
            TestSubject(),
            primary_key=[],
            value_columns=["x", "y"],
            types={"x": pw.Type.INT, "y": pw.Type.INT},
            format="json",
        )
    )
    variants.append(
        pw.python.read(
            TestSubject(),
            primary_key=None,
            value_columns=["x", "y"],
            types={"x": pw.Type.INT, "y": pw.Type.INT},
            format="json",
        )
    )

    for table in variants:
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
    class TestSubject(pw.python.ConnectorSubject):
        def run(self):
            self.next_str("lorem")
            self.next_str("ipsum")

    table = pw.python.read(TestSubject(), format="raw")

    assert_table_equality_wo_index_types(
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

    table_csv = pw.io.csv.read(
        tmp_path / "input.csv",
        id_columns=["key"],
        value_columns=["value"],
        types={"key": pw.Type.INT, "value": pw.Type.STRING},
        mode="static",
    )
    table_json = pw.io.jsonlines.read(
        tmp_path / "input.json",
        primary_key=["key"],
        value_columns=["value"],
        types={"key": pw.Type.INT, "value": pw.Type.STRING},
        mode="static",
    )
    table_py = pw.io.python.read(
        subject=TestSubject(),
        primary_key=["key"],
        value_columns=["value"],
        types={"key": pw.Type.INT, "value": pw.Type.STRING},
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
        c: str = pw.column_definition(default_value="default")

    table = pw.io.jsonlines.read(
        str(input_path),
        schema=InputSchema,
        mode="static",
    )

    assert_table_equality_wo_types(
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


def test_deprecated_schema_compatiblity(tmp_path: pathlib.Path):
    data = """
        a | b | c  | d
        a | 1 | 42 | foo
        b | 2 | 43 | bar
        c | 3 |    | 42
    """
    input_path = tmp_path / "input.csv"
    write_csv(input_path, data)

    class InputSchema(pw.Schema):
        a: str = pw.column_definition(primary_key=True)
        b: int = pw.column_definition(primary_key=True)
        c: int = pw.column_definition(default_value=0)
        d: Any = pw.column_definition()
        e: Any

    table1 = pw.io.csv.read(
        str(input_path),
        schema=InputSchema,
        mode="static",
    )
    table2 = pw.io.csv.read(
        str(input_path),
        id_columns=["a", "b"],
        value_columns=[
            "a",
            "c",
            "d",
            "e",
        ],
        types={
            "a": pw.Type.STRING,
            "b": pw.Type.INT,
            "c": pw.Type.INT,
            "d": pw.Type.ANY,
        },
        default_values={"c": 0},
        mode="static",
    )

    assert table1.schema == table2.schema
    assert_table_equality(table1, table2)


def test_subscribe():
    class TestSubject(pw.io.python.ConnectorSubject):
        def run(self):
            self.next_str("foo")

    table = pw.io.python.read(TestSubject(), format="raw")

    root = mock.Mock()
    on_change, on_end = mock.Mock(), mock.Mock()
    root.on_change, root.on_end = on_change, on_end

    pw.io.subscribe(table, on_change=on_change, on_end=on_end)

    run_all()

    threads_num = min(int(os.environ.get("PATHWAY_THREADS", 1)), 8)
    root.assert_has_calls(
        [
            mock.call.on_change(
                key=mock.ANY, row={"data": "foo"}, time=mock.ANY, is_addition=True
            ),
            *[mock.call.on_end()] * threads_num,
        ]
    )


def test_async_transformer():
    class OutputSchema(pw.Schema):
        ret: int

    class TestAsyncTransformer(pw.AsyncTransformer, output_schema=OutputSchema):
        async def invoke(self, value: int) -> Dict[str, Any]:
            await asyncio.sleep(random.uniform(0, 0.1))
            return dict(ret=value + 1)

    input_table = T(
        """
            | value
        1   | 1
        2   | 2
        3   | 3
        """
    )

    result = TestAsyncTransformer(input_table=input_table).result

    assert result._universe.is_subset_of(input_table._universe)

    assert_table_equality(
        result,
        T(
            """
            | ret
        1   | 2
        2   | 3
        3   | 4
        """
        ),
    )


@pytest.mark.xfail(reason="stil fails randomly")
def test_async_transformer_idempotency():
    class OutputSchema(pw.Schema):
        ret: int

    class TestAsyncTransformer(pw.AsyncTransformer, output_schema=OutputSchema):
        async def invoke(self, value: int) -> Dict[str, Any]:
            await asyncio.sleep(random.uniform(0, 0.1))
            return dict(ret=value + 1)

    input_table = T(
        """
            | value
        1   | 1
        2   | 2
        3   | 3
        """
    )

    result = TestAsyncTransformer(input_table=input_table).result
    expected = T(
        """
            | ret
        1   | 2
        2   | 3
        3   | 4
        """
    )

    assert result._universe.is_subset_of(input_table._universe)

    # check if state is cleared between runs
    assert_table_equality(result, expected)
    assert_table_equality(result, expected)


def test_async_transformer_filter_failures():
    class OutputSchema(pw.Schema):
        ret: int

    class TestAsyncTransformer(pw.AsyncTransformer, output_schema=OutputSchema):
        async def invoke(self, value: int) -> Dict[str, Any]:
            await asyncio.sleep(random.uniform(0, 0.1))
            if value == 2:
                raise Exception
            return dict(ret=value + 1)

    input_table = T(
        """
            | value
        1   | 1
        2   | 2
        3   | 3
        """
    )

    result = TestAsyncTransformer(input_table=input_table).result

    assert result._universe.is_subset_of(input_table._universe)

    assert_table_equality(
        result,
        T(
            """
            | ret
        1   | 2
        3   | 4
        """
        ),
    )


def test_async_transformer_assert_schema_error():
    class OutputSchema(pw.Schema):
        ret: int

    class TestAsyncTransformer(pw.AsyncTransformer, output_schema=OutputSchema):
        async def invoke(self, value: int) -> Dict[str, Any]:
            await asyncio.sleep(random.uniform(0, 0.1))
            return dict(foo=value + 1)

    input_table = T(
        """
            | value
        1   | 1
        2   | 2
        """
    )

    result = TestAsyncTransformer(input_table=input_table).result

    assert result._universe.is_subset_of(input_table._universe)

    assert_table_equality(result, pw.Table.empty(ret=int))


def test_async_transformer_disk_cache(tmp_path: pathlib.Path):
    cache_dir = tmp_path / "test_cache"
    counter = mock.Mock()

    def pipeline():
        G.clear()

        class OutputSchema(pw.Schema):
            ret: int

        class TestAsyncTransformer(pw.AsyncTransformer, output_schema=OutputSchema):
            async def invoke(self, value: int) -> Dict[str, Any]:
                counter()
                await asyncio.sleep(random.uniform(0, 0.1))
                return dict(ret=value + 1)

        input = T(
            """
                | value
            1   | 1
            2   | 2
            3   | 3
            """
        )
        expected = T(
            """
                | ret
            1   | 2
            2   | 3
            3   | 4
            """
        )

        result = (
            TestAsyncTransformer(input_table=input)
            .with_options(cache_strategy=pw.asynchronous.DiskCache())
            .result
        )

        assert_table_equality(
            result,
            expected,
            persistence_config=pw.io.PersistenceConfig.single_backend(
                pw.io.PersistentStorageBackend.filesystem(cache_dir),
            ),
        )

    # run twice to check if cache is used
    pipeline()
    pipeline()
    assert os.path.exists(cache_dir)
    assert counter.call_count == 3


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


@xfail_on_darwin(reason="running pw.run from separate process not supported")
@pytest.mark.xdist_group(name="streaming_tests")
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
        str(inputs_path),
        schema=InputSchema,
        mode="streaming",
        autocommit_duration_ms=1000,
    )

    output_path = tmp_path / "output.csv"
    pw.io.csv.write(table, str(output_path))

    assert wait_result_with_checker(CsvLinesNumberChecker(output_path, 2), 30)


@xfail_on_darwin(reason="running pw.run from separate process not supported")
@pytest.mark.xdist_group(name="streaming_tests")
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
        autocommit_duration_ms=1000,
    )

    output_path = tmp_path / "output.csv"
    pw.io.csv.write(table, str(output_path))

    assert wait_result_with_checker(CsvLinesNumberChecker(output_path, 5), 30)


@xfail_on_darwin(reason="running pw.run from separate process not supported")
@pytest.mark.xdist_group(name="streaming_tests")
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
        autocommit_duration_ms=1000,
    )

    output_path = tmp_path / "output.csv"
    pw.io.csv.write(table, str(output_path))

    assert wait_result_with_checker(CsvLinesNumberChecker(output_path, 5), 30)


@xfail_on_darwin(reason="running pw.run from separate process not supported")
@pytest.mark.xdist_group(name="streaming_tests")
def test_plaintext_streaming(tmp_path: pathlib.Path):
    inputs_path = tmp_path / "inputs/"
    start_streaming_inputs(inputs_path, 5, 1.0, "plaintext")

    table = pw.io.plaintext.read(
        str(inputs_path),
        mode="streaming",
        autocommit_duration_ms=1000,
    )

    output_path = tmp_path / "output.csv"
    pw.io.csv.write(table, str(output_path))

    assert wait_result_with_checker(CsvLinesNumberChecker(output_path, 5), 30)


@xfail_on_darwin(reason="running pw.run from separate process not supported")
@pytest.mark.xdist_group(name="streaming_tests")
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
        autocommit_duration_ms=1000,
        format="csv",
    )

    output_path = tmp_path / "output.csv"
    pw.io.csv.write(table, str(output_path))

    assert wait_result_with_checker(CsvLinesNumberChecker(output_path, 5), 30)


@xfail_on_darwin(reason="running pw.run from separate process not supported")
@pytest.mark.xdist_group(name="streaming_tests")
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
        autocommit_duration_ms=1000,
        format="json",
    )

    output_path = tmp_path / "output.csv"
    pw.io.csv.write(table, str(output_path))

    assert wait_result_with_checker(CsvLinesNumberChecker(output_path, 5), 30)


@xfail_on_darwin(reason="running pw.run from separate process not supported")
@pytest.mark.xdist_group(name="streaming_tests")
def test_plaintext_streaming_fs_alias(tmp_path: pathlib.Path):
    inputs_path = tmp_path / "inputs/"
    start_streaming_inputs(inputs_path, 5, 1.0, "plaintext")

    table = pw.io.fs.read(
        str(inputs_path),
        mode="streaming",
        autocommit_duration_ms=1000,
        format="plaintext",
    )

    output_path = tmp_path / "output.csv"
    pw.io.csv.write(table, str(output_path))

    assert wait_result_with_checker(CsvLinesNumberChecker(output_path, 5), 30)


def test_pathway_type_mapping():
    import inspect

    from pathway.internals.api import _TYPES_TO_ENGINE_MAPPING, PathwayType
    from pathway.io._utils import _PATHWAY_TYPE_MAPPING

    assert all(
        t in _PATHWAY_TYPE_MAPPING and t in _TYPES_TO_ENGINE_MAPPING.values()
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

    class TestSubject(pw.python.ConnectorSubject):
        def __init__(self, items):
            super().__init__()
            self.items = items

        def run(self):
            for item in self.items:
                self.next_str(item)

    def run_computation(py_connector_input, fs_connector_input):
        G.clear()
        write_lines(input_path, "\n".join(fs_connector_input))
        table_py = pw.python.read(
            TestSubject(py_connector_input), format="raw", persistent_id="1"
        )
        table_csv = pw.io.plaintext.read(input_path, persistent_id="2", mode="static")
        table_joined = table_py.join(table_csv, table_py.data == table_csv.data).select(
            table_py.data
        )
        pw.io.csv.write(table_joined, output_path)
        pw.run(
            persistence_config=pw.io.PersistenceConfig.single_backend(
                pw.io.PersistentStorageBackend.filesystem(persistent_storage_path),
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
        match="persistent metadata backend failed: target object should be a directory",
    ):
        pw.run(
            persistence_config=pw.io.PersistenceConfig.single_backend(
                pw.io.PersistentStorageBackend.filesystem(path),
            )
        )


def test_persistent_id_not_assigned(tmp_path: pathlib.Path):
    input_path = tmp_path / "input.txt"
    write_lines(input_path, "test_data")
    pstorage_path = tmp_path / "PStrorage"

    table = pw.io.plaintext.read(input_path)
    pw.io.csv.write(table, tmp_path / "output.txt")
    with pytest.raises(
        ValueError,
        match="persistent storage is configured, but persistent id is not assigned for FileSystem reader",
    ):
        pw.run(
            persistence_config=pw.io.PersistenceConfig.single_backend(
                pw.io.PersistentStorageBackend.filesystem(pstorage_path)
            )
        )


def test_no_persistent_storage(tmp_path: pathlib.Path):
    input_path = tmp_path / "input.txt"
    write_lines(input_path, "test_data")

    table = pw.io.plaintext.read(input_path, persistent_id="1")
    pw.io.csv.write(table, tmp_path / "output.txt")
    with pytest.raises(
        ValueError,
        match="persistent id 1 is assigned, but no persistent storage is configured",
    ):
        pw.run()


def test_duplicated_persistent_id(tmp_path: pathlib.Path):
    pstorage_path = tmp_path / "PStorage"
    input_path = tmp_path / "input_first.txt"
    input_path_2 = tmp_path / "input_second.txt"
    output_path = tmp_path / "output.txt"

    write_lines(input_path, "hello")
    write_lines(input_path_2, "world")

    table_1 = pw.io.plaintext.read(input_path, persistent_id="one")
    table_2 = pw.io.plaintext.read(input_path_2, persistent_id="one")
    pw.universes.promise_are_pairwise_disjoint(table_1, table_2)
    table_concat = table_1.concat(table_2)
    pw.io.csv.write(table_concat, output_path)

    with pytest.raises(
        ValueError,
        match="Persistent ID 'one' used more than once",
    ):
        pw.run(
            persistence_config=pw.io.PersistenceConfig.single_backend(
                pw.io.PersistentStorageBackend.filesystem(pstorage_path)
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
    pw.csv.write(table, "/dev/full")
    with pytest.raises(api.EngineError, match="No space left on device"):
        run_all()
    subject.finish.set()

    assert not subject.timed_out


@xfail_on_darwin(reason="running pw.run from separate process not supported")
def test_simple_forgetting(tmp_path: pathlib.Path):
    os.environ["PATHWAY_PERSISTENT_STORAGE"] = str(tmp_path / "PStorage")
    inputs_path = tmp_path / "inputs"
    os.mkdir(inputs_path)

    def stream_inputs():
        time.sleep(1)
        first_line = {"key": 1, "value": "one"}
        second_line = {"key": 2, "value": "two"}
        write_lines(inputs_path / "input1.jsonlines", json.dumps(first_line))
        time.sleep(1)
        write_lines(inputs_path / "input2.jsonlines", json.dumps(second_line))
        time.sleep(1)
        os.remove(inputs_path / "input1.jsonlines")

    class InputSchema(pw.Schema):
        key: int = pw.column_definition(primary_key=True)
        value: str

    table = pw.io.fs.read(
        inputs_path,
        format="json",
        schema=InputSchema,
        mode="streaming_with_deletions",
        autocommit_duration_ms=10,
    )

    output_path = tmp_path / "output.csv"
    pw.io.csv.write(table, str(output_path))

    inputs_thread = threading.Thread(target=stream_inputs, daemon=True)
    inputs_thread.start()

    assert wait_result_with_checker(CsvLinesNumberChecker(output_path, 3), 30)


@xfail_on_darwin(reason="running pw.run from separate process not supported")
def test_simple_replacement(tmp_path: pathlib.Path):
    os.environ["PATHWAY_PERSISTENT_STORAGE"] = str(tmp_path / "PStorage")
    inputs_path = tmp_path / "inputs"
    os.mkdir(inputs_path)

    def stream_inputs():
        time.sleep(1)
        first_line = {"key": 1, "value": "one"}
        second_line = {"key": 2, "value": "two"}
        third_line = {"key": 3, "value": "three"}
        write_lines(inputs_path / "input1.jsonlines", json.dumps(first_line))
        time.sleep(1)
        write_lines(inputs_path / "input2.jsonlines", json.dumps(second_line))
        time.sleep(1)
        write_lines(inputs_path / "input1.jsonlines", json.dumps(third_line))

    class InputSchema(pw.Schema):
        key: int = pw.column_definition(primary_key=True)
        value: str

    table = pw.io.fs.read(
        inputs_path,
        format="json",
        schema=InputSchema,
        mode="streaming_with_deletions",
        autocommit_duration_ms=10,
    )

    output_path = tmp_path / "output.csv"
    pw.io.csv.write(table, str(output_path))

    inputs_thread = threading.Thread(target=stream_inputs, daemon=True)
    inputs_thread.start()

    assert wait_result_with_checker(CsvLinesNumberChecker(output_path, 4), 30)


@xfail_on_darwin(reason="running pw.run from separate process not supported")
def test_simple_forgetting_autogenerated_key(tmp_path: pathlib.Path):
    os.environ["PATHWAY_PERSISTENT_STORAGE"] = str(tmp_path / "PStorage")
    inputs_path = tmp_path / "inputs"
    os.mkdir(inputs_path)

    def stream_inputs():
        time.sleep(1)
        first_line = {"key": 1, "value": "one"}
        second_line = {"key": 2, "value": "two"}
        write_lines(inputs_path / "input1.jsonlines", json.dumps(first_line))
        time.sleep(1)
        write_lines(inputs_path / "input2.jsonlines", json.dumps(second_line))
        time.sleep(1)
        os.remove(inputs_path / "input1.jsonlines")

    class InputSchema(pw.Schema):
        key: int
        value: str

    table = pw.io.fs.read(
        inputs_path,
        format="json",
        schema=InputSchema,
        mode="streaming_with_deletions",
        autocommit_duration_ms=10,
    )

    output_path = tmp_path / "output.csv"
    pw.io.csv.write(table, str(output_path))

    inputs_thread = threading.Thread(target=stream_inputs, daemon=True)
    inputs_thread.start()

    assert wait_result_with_checker(CsvLinesNumberChecker(output_path, 3), 30)


@xfail_on_darwin(reason="running pw.run from separate process not supported")
def test_simple_replacement_autogenerated_key(tmp_path: pathlib.Path):
    os.environ["PATHWAY_PERSISTENT_STORAGE"] = str(tmp_path / "PStorage")
    inputs_path = tmp_path / "inputs"
    os.mkdir(inputs_path)

    def stream_inputs():
        time.sleep(1)
        first_line = {"key": 1, "value": "one"}
        second_line = {"key": 2, "value": "two"}
        third_line = {"key": 3, "value": "three"}
        write_lines(inputs_path / "input1.jsonlines", json.dumps(first_line))
        time.sleep(1)
        write_lines(inputs_path / "input2.jsonlines", json.dumps(second_line))
        time.sleep(1)
        write_lines(inputs_path / "input1.jsonlines", json.dumps(third_line))

    class InputSchema(pw.Schema):
        key: int
        value: str

    table = pw.io.fs.read(
        inputs_path,
        format="json",
        schema=InputSchema,
        mode="streaming_with_deletions",
        autocommit_duration_ms=10,
    )

    output_path = tmp_path / "output.csv"
    pw.io.csv.write(table, str(output_path))

    inputs_thread = threading.Thread(target=stream_inputs, daemon=True)
    inputs_thread.start()

    assert wait_result_with_checker(CsvLinesNumberChecker(output_path, 4), 30)


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
    pw.run()

    with open(output_path, "r") as f:
        result = json.load(f)
        assert result["data"] == [ord(c) for c in input_full_contents]


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
    pw.run()

    assert rows == [
        {
            "data": input_full_contents.encode("utf-8"),
        }
    ]
