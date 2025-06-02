# Copyright © 2024 Pathway


import asyncio
import os
import pathlib
import random
import re
import time
from typing import Any
from unittest import mock

import numpy as np
import numpy.typing as npt
import pandas as pd
import pytest

import pathway as pw
from pathway.internals import api
from pathway.internals.parse_graph import G
from pathway.tests.utils import (
    FileLinesNumberChecker,
    T,
    assert_stream_split_into_groups,
    assert_table_equality,
    needs_multiprocessing_fork,
    run,
    wait_result_with_checker,
    write_csv,
)
from pathway.udfs import InMemoryCache


def test_simple(monkeypatch):
    monkeypatch.delenv("PATHWAY_PERSISTENT_STORAGE", raising=False)

    class OutputSchema(pw.Schema):
        ret: int

    class TestAsyncTransformer(pw.AsyncTransformer, output_schema=OutputSchema):
        async def invoke(self, value: int) -> dict[str, Any]:
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

    result = TestAsyncTransformer(input_table=input_table).successful

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


def test_file_io(monkeypatch, tmp_path: pathlib.Path):
    monkeypatch.delenv("PATHWAY_PERSISTENT_STORAGE", raising=False)

    class InputSchema(pw.Schema):
        value: int

    class OutputSchema(pw.Schema):
        ret: int

    class TestAsyncTransformer(pw.AsyncTransformer, output_schema=OutputSchema):
        async def invoke(self, value: int) -> dict[str, Any]:
            await asyncio.sleep(random.uniform(0, 0.1))
            return dict(ret=value + 1)

    input_table_str = """
            | value
        1   | 1
        2   | 2
        3   | 3
        """
    input_path = tmp_path / "input.csv"
    output_path = tmp_path / "output.csv"
    write_csv(input_path, input_table_str)

    input_table = pw.io.csv.read(input_path, schema=InputSchema, mode="static")
    result = TestAsyncTransformer(input_table=input_table).successful
    pw.io.csv.write(result, output_path)

    pstorage_dir = tmp_path / "PStorage"
    persistence_config = pw.persistence.Config(
        backend=pw.persistence.Backend.filesystem(pstorage_dir),
        persistence_mode=api.PersistenceMode.UDF_CACHING,
    )

    run(
        persistence_config=persistence_config,
    )


@needs_multiprocessing_fork
def test_idempotency(monkeypatch):
    monkeypatch.delenv("PATHWAY_PERSISTENT_STORAGE", raising=False)

    class OutputSchema(pw.Schema):
        ret: int

    class TestAsyncTransformer(pw.AsyncTransformer, output_schema=OutputSchema):
        async def invoke(self, value: int) -> dict[str, Any]:
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

    result = TestAsyncTransformer(input_table=input_table).successful
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


def test_filter_failures(monkeypatch):
    monkeypatch.delenv("PATHWAY_PERSISTENT_STORAGE", raising=False)

    class OutputSchema(pw.Schema):
        ret: int

    class TestAsyncTransformer(pw.AsyncTransformer, output_schema=OutputSchema):
        async def invoke(self, value: int) -> dict[str, Any]:
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

    result = TestAsyncTransformer(input_table=input_table).successful

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


def test_assert_schema_error(monkeypatch):
    monkeypatch.delenv("PATHWAY_PERSISTENT_STORAGE", raising=False)

    class OutputSchema(pw.Schema):
        ret: int

    class TestAsyncTransformer(pw.AsyncTransformer, output_schema=OutputSchema):
        async def invoke(self, value: int) -> dict[str, Any]:
            await asyncio.sleep(random.uniform(0, 0.1))
            return dict(foo=value + 1)

    input_table = T(
        """
            | value
        1   | 1
        2   | 2
        """
    )

    result = TestAsyncTransformer(input_table=input_table).successful

    assert result._universe.is_subset_of(input_table._universe)

    assert_table_equality(result, pw.Table.empty(ret=int))


def test_disk_cache(tmp_path: pathlib.Path):
    cache_dir = tmp_path / "test_cache"
    counter = mock.Mock()

    def pipeline():
        G.clear()

        class OutputSchema(pw.Schema):
            ret: int

        class TestAsyncTransformer(pw.AsyncTransformer, output_schema=OutputSchema):
            async def invoke(self, value: int) -> dict[str, Any]:
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

        result = TestAsyncTransformer(input_table=input).successful

        assert_table_equality(
            result,
            expected,
            persistence_config=pw.persistence.Config(
                pw.persistence.Backend.filesystem(cache_dir),
                persistence_mode=pw.PersistenceMode.SELECTIVE_PERSISTING,
            ),
        )

    # run twice to check if cache is used
    pipeline()
    pipeline()
    assert os.path.exists(cache_dir)
    assert counter.call_count == 3


def test_with_instance():
    class OutputSchema(pw.Schema):
        ret: float

    class TestAsyncTransformer(pw.AsyncTransformer, output_schema=OutputSchema):
        async def invoke(self, value: float, instance: int) -> dict[str, Any]:
            await asyncio.sleep(value)
            return dict(ret=value)

    input_table = T(
        """
        value | instance
         1.3  |     1
         1.1  |     1
         0.0  |     2
         0.5  |     2
         1.0  |     3
         0.1  |     3
    """
    )

    result = TestAsyncTransformer(
        input_table=input_table, instance=pw.this.instance
    ).successful

    assert_stream_split_into_groups(
        result,
        T(
            """
        ret | __time__
        1.3 |     2
        1.1 |     2
        0.0 |     4
        0.5 |     4
        1.0 |     6
        0.1 |     6
    """
        ),
    )


def test_with_instance_work_after_restart():
    class OutputSchema(pw.Schema):
        ret: float

    class TestAsyncTransformer(pw.AsyncTransformer, output_schema=OutputSchema):
        async def invoke(self, value: float, instance: int) -> dict[str, Any]:
            if value == 1.3:
                raise ValueError("incorrect value")
            await asyncio.sleep(value)
            return dict(ret=value)

    input_table = T(
        """
        value | instance
         1.3  |     1
         0.0  |     2
         0.5  |     2
         1.0  |     3
         0.1  |     3
         1.1  |     1
    """
    )

    expected = T(
        """
        _async_status | ret | __time__
          -FAILURE-   |     |     2
          -SUCCESS-   | 0.0 |     4
          -SUCCESS-   | 0.5 |     4
          -SUCCESS-   | 1.0 |     6
          -SUCCESS-   | 0.1 |     6
          -FAILURE-   |     |     2

    """
    )

    result = TestAsyncTransformer(
        input_table=input_table, instance=pw.this.instance
    ).finished

    assert_stream_split_into_groups(result, expected)
    assert_stream_split_into_groups(result, expected)


def test_fails_whole_instance():
    class OutputSchema(pw.Schema):
        ret: float

    class TestAsyncTransformer(pw.AsyncTransformer, output_schema=OutputSchema):
        async def invoke(self, value: float, instance: int) -> dict[str, Any]:
            if value == 1.1:
                raise ValueError("incorrect value")
            await asyncio.sleep(value)
            return dict(ret=value)

    input_table = T(
        """
        value | instance
         1.3  |     1
         1.1  |     1
         0.0  |     2
         0.5  |     2
         1.0  |     3
         0.1  |     3
    """
    )

    result = TestAsyncTransformer(
        input_table=input_table, instance=pw.this.instance
    ).finished

    assert_stream_split_into_groups(
        result,
        T(
            """
        _async_status | ret | __time__
          -FAILURE-   |     |     2
          -FAILURE-   |     |     2
          -SUCCESS-   | 0.0 |     4
          -SUCCESS-   | 0.5 |     4
          -SUCCESS-   | 1.0 |     6
          -SUCCESS-   | 0.1 |     6
    """
        ),
    )


def test_fails_on_too_many_columns():
    class OutputSchema(pw.Schema):
        ret: int

    class TestAsyncTransformer(pw.AsyncTransformer, output_schema=OutputSchema):
        async def invoke(self, a: int) -> dict[str, Any]:
            await asyncio.sleep(a)
            return dict(ret=a)

    input_table = T(
        """
        a | b
        1 | 2
    """
    )

    with pytest.raises(
        TypeError,
        match="Input table has a column 'b' but it is not present on the argument list of the invoke method.",
    ):
        TestAsyncTransformer(input_table=input_table)


def test_fails_on_not_enough_columns():
    class OutputSchema(pw.Schema):
        ret: int

    class TestAsyncTransformer(pw.AsyncTransformer, output_schema=OutputSchema):
        async def invoke(self, a: int, b: int) -> dict[str, Any]:
            await asyncio.sleep(a)
            return dict(ret=a + b)

    input_table = T(
        """
        a
        1
    """
    )

    with pytest.raises(
        TypeError,
        match="Column 'b' is present on the argument list of the invoke"
        + " method but it is not present in the input_table.",
    ):
        TestAsyncTransformer(input_table=input_table)


def test_failed():
    class OutputSchema(pw.Schema):
        ret: float

    class TestAsyncTransformer(pw.AsyncTransformer, output_schema=OutputSchema):
        async def invoke(self, value: float) -> dict[str, Any]:
            if value == 1.1:
                raise ValueError("incorrect value")
            await asyncio.sleep(value)
            return dict(ret=value)

    input_table = T(
        """
        value
         1.3
         1.1
    """
    )

    result = TestAsyncTransformer(input_table=input_table).failed

    assert_table_equality(
        result,
        T(
            """
          | ret
        1 |
    """
        ).update_types(ret=float | None),
    )


def test_consistent_when_instance_for_key_changes():
    class OutputSchema(pw.Schema):
        value: float

    class TestAsyncTransformer(pw.AsyncTransformer, output_schema=OutputSchema):
        async def invoke(self, value: float, instance: int) -> dict[str, Any]:
            await asyncio.sleep(value)
            return dict(value=value)

    input_table = T(
        """
          | value | instance | __time__ | __diff__
        1 |  2.0  |     1    |     2    |     1
        1 |  2.0  |     1    |     4    |    -1
        1 |  0.2  |     2    |     4    |     1
    """
    )

    transformer = TestAsyncTransformer(
        input_table=input_table, instance=pw.this.instance
    )

    assert_table_equality(
        transformer.successful,
        T(
            """
          | value
        1 |  0.2
        """
        ),
    )


def test_requires_hashable_instance():
    class OutputSchema(pw.Schema):
        value: float

    class TestAsyncTransformer(pw.AsyncTransformer, output_schema=OutputSchema):
        async def invoke(self, value: float, instance: int) -> dict[str, Any]:
            return dict(value=value)

    input_table = T(
        """
          | value | instance
        1 |  2.0  |    1
    """
    )

    @pw.udf
    def foo(a: int) -> npt.NDArray[np.float64]:
        return np.ones(a)

    input_table = input_table.with_columns(instance=foo(pw.this.instance))

    with pytest.raises(
        ValueError,
        match=re.escape(
            "You can't use a column of type Array(0, FLOAT) as instance"
            + " in AsyncTransformer because it is unhashable."
        ),
    ):
        TestAsyncTransformer(input_table=input_table, instance=pw.this.instance)


def test_error_is_logged(caplog):
    class OutputSchema(pw.Schema):
        ret: int

    class TestAsyncTransformer(pw.AsyncTransformer, output_schema=OutputSchema):
        async def invoke(self, value: float) -> dict[str, Any]:
            if value == 11:
                raise ValueError("incorrect value 11")
            return dict(ret=value)

    input_table = T(
        """
        value
          1
          2
         11
    """
    )
    transformer = TestAsyncTransformer(input_table=input_table)
    expected = T(
        """
        ret
         1
         2
    """
    )
    assert_table_equality(transformer.successful, expected)
    assert any(
        isinstance(record.exc_text, str) and "incorrect value 11" in record.exc_text
        for record in caplog.records
    )


@needs_multiprocessing_fork
def test_commits_even_if_blocked_on_processing(tmp_path):
    # can sometimes succeed even if it shouldn't
    # then all values are read to `a` before `TestAsyncTransformer` produces last valid value
    # to test the correct behavior, `a` should still read values, when `b` inserts
    # blocking calls to AsyncTransformer. Then the transformer is blocked but its output
    # should still advance time.
    output_path = tmp_path / "output.csv"

    class OutputSchema(pw.Schema):
        value: float

    a = pw.demo.range_stream(nb_rows=4)
    b = pw.demo.range_stream(nb_rows=4)

    class TestAsyncTransformer(pw.AsyncTransformer, output_schema=OutputSchema):
        async def invoke(self, value: int) -> dict[str, Any]:
            if value > 1:
                time.sleep(100)  # simulates a blocking operation
            return dict(value=value)

    b = TestAsyncTransformer(input_table=b).successful
    res = a.join(b).select(l=pw.left.value + 1, r=pw.right.value + 1)

    pw.io.csv.write(res, output_path)
    wait_result_with_checker(FileLinesNumberChecker(output_path, 9), 30)
    # 9 = 4 x 2 + 1 (4 from a, 2 non-blocked from b, 1 header)


@needs_multiprocessing_fork
def test_commits_even_if_nothing_to_process(tmp_path):
    output_path = tmp_path / "output.csv"

    class HangingSubject(pw.io.python.ConnectorSubject):
        def run(self):
            self.next(value=0)
            self.next(value=1)
            time.sleep(100)  # simulates waiting for more files

    class InputSchema(pw.Schema):
        value: int

    a = pw.demo.range_stream(nb_rows=4)
    b = pw.io.python.read(
        HangingSubject(),
        schema=InputSchema,
    )

    class OutputSchema(pw.Schema):
        value: float

    class TestAsyncTransformer(pw.AsyncTransformer, output_schema=OutputSchema):
        async def invoke(self, value: int) -> dict[str, Any]:
            return dict(value=value)

    b = TestAsyncTransformer(input_table=b).successful
    res = a.join(b).select(l=pw.left.value + 1, r=pw.right.value + 1)

    pw.io.csv.write(res, output_path)
    wait_result_with_checker(FileLinesNumberChecker(output_path, 9), 30)
    # 9 = 4 x 2 + 1 (4 from a, 2 non-blocked from b, 1 header)
    # checks if values from stream `a` are joined with `b` after all
    # values are inserted to `b` from AsyncTransformer


def test_simple_memory_cache(monkeypatch):
    monkeypatch.delenv("PATHWAY_PERSISTENT_STORAGE", raising=False)

    class OutputSchema(pw.Schema):
        ret: int

    class TestAsyncTransformer(pw.AsyncTransformer, output_schema=OutputSchema):
        async def invoke(self, value: int) -> dict[str, Any]:
            await asyncio.sleep(random.uniform(0, 0.1))
            return dict(ret=value + 1)

    input_table = T(
        """
            | value
        1   | 1
        2   | 1
        3   | 3
        """
    )

    result = (
        TestAsyncTransformer(input_table=input_table)
        .with_options(cache_strategy=InMemoryCache())
        .successful
    )

    assert result._universe.is_subset_of(input_table._universe)

    assert_table_equality(
        result,
        T(
            """
            | ret
        1   | 2
        2   | 2
        3   | 4
        """
        ),
    )


def test_datetimes():
    class OutputSchema(pw.Schema):
        ret: pw.DateTimeNaive

    class TestAsyncTransformer(pw.AsyncTransformer, output_schema=OutputSchema):
        async def invoke(self, value: int) -> dict[str, Any]:
            await asyncio.sleep(random.uniform(0, 0.1))
            return dict(
                ret=pw.DateTimeNaive("2025-05-28 17:00:00") + pd.Timedelta(days=value)
            )

    input_table = T(
        """
            | value
        1   | 1
        2   | 2
        3   | 3
        """
    )

    result = TestAsyncTransformer(input_table=input_table).successful

    assert_table_equality(
        result,
        T(
            """
            | ret
        1   | 2025-05-29 17:00:00
        2   | 2025-05-30 17:00:00
        3   | 2025-05-31 17:00:00
        """,
            split_on_whitespace=False,
        ).select(ret=pw.this.ret.dt.strptime("%Y-%m-%d %H:%M:%S")),
    )
