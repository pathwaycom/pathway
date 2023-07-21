# Copyright © 2023 Pathway

import asyncio
import json
import os
import pathlib
import random
import threading
import time
from typing import Any, Dict
from unittest import mock

import pandas as pd
import pytest

import pathway as pw
from pathway.internals.parse_graph import G
from pathway.tests.utils import (
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


def get_number_of_lines_checker(
    output_path, n_lines, usecols=("k", "v"), index_col=("k")
):
    def checker():
        result = pd.read_csv(
            output_path, usecols=usecols, index_col=index_col
        ).sort_index()
        return len(result) == n_lines

    return checker


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


def get_stream_test_folder_contents(root_path):
    report_parts = []
    inputs_dir = os.listdir(root_path / "inputs")
    for input_filename in inputs_dir:
        report_parts.append("Input file: {}".format(str(input_filename)))
    try:
        with open(root_path / "output", "r") as f:
            output_report = f.read()
    except Exception:
        output_report = "<FILE DOES NOT EXIST>"
    report_parts.append("\nOutput: {}".format(output_report))
    return "\n".join(report_parts)


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
        d = pw.column_definition()

    table1 = pw.io.csv.read(
        str(input_path),
        schema=InputSchema,
        mode="static",
    )
    table2 = pw.io.csv.read(
        str(input_path),
        id_columns=["a", "b"],
        value_columns=["c", "d"],
        types={"a": pw.Type.STRING, "b": pw.Type.INT, "c": pw.Type.INT},
        default_values={"c": 0},
        mode="static",
    )

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
    os.environ["PATHWAY_PERSISTENT_STORAGE"] = str(cache_dir)

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

        assert_table_equality(result, expected)

    # run twice to check if cache is used
    pipeline()
    pipeline()
    assert os.path.exists(cache_dir)
    assert counter.call_count == 3


@xfail_on_darwin(reason="running pw.run from separate process not supported")
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

    assert wait_result_with_checker(
        get_number_of_lines_checker(output_path, 2), 30
    ), get_stream_test_folder_contents(tmp_path)


@xfail_on_darwin(reason="running pw.run from separate process not supported")
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

    assert wait_result_with_checker(
        get_number_of_lines_checker(output_path, 5), 30
    ), get_stream_test_folder_contents(tmp_path)


@xfail_on_darwin(reason="running pw.run from separate process not supported")
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

    assert wait_result_with_checker(
        get_number_of_lines_checker(output_path, 5), 30
    ), get_stream_test_folder_contents(tmp_path)


@xfail_on_darwin(reason="running pw.run from separate process not supported")
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

    assert wait_result_with_checker(
        get_number_of_lines_checker(output_path, 5, ("data",), ("data",)),
        30,
    ), get_stream_test_folder_contents(tmp_path)


@xfail_on_darwin(reason="running pw.run from separate process not supported")
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

    assert wait_result_with_checker(
        get_number_of_lines_checker(output_path, 5), 30
    ), get_stream_test_folder_contents(tmp_path)


@xfail_on_darwin(reason="running pw.run from separate process not supported")
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

    assert wait_result_with_checker(
        get_number_of_lines_checker(output_path, 5), 30
    ), get_stream_test_folder_contents(tmp_path)


@xfail_on_darwin(reason="running pw.run from separate process not supported")
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

    assert wait_result_with_checker(
        get_number_of_lines_checker(output_path, 5, ("data",), ("data",)),
        30,
    ), get_stream_test_folder_contents(tmp_path)
