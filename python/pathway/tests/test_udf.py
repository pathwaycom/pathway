# Copyright © 2024 Pathway

from __future__ import annotations

import asyncio
import os
import pathlib
from unittest import mock

import pathway as pw
from pathway.tests.utils import T, assert_table_equality


def test_udf():
    @pw.udf
    def inc(a: int) -> int:
        return a + 1

    input = pw.debug.table_from_markdown(
        """
        a
        1
        2
        3
        """
    )

    result = input.select(ret=inc(pw.this.a))

    assert_table_equality(
        result,
        T(
            """
            ret
            2
            3
            4
            """,
        ),
    )


def test_udf_class():
    class Inc(pw.UDFSync):
        def __init__(self, inc) -> None:
            super().__init__()
            self.inc = inc

        def __wrapped__(self, a: int) -> int:
            return a + self.inc

    input = pw.debug.table_from_markdown(
        """
        a
        1
        2
        3
        """
    )

    inc = Inc(2)
    result = input.select(ret=inc(pw.this.a))

    assert_table_equality(
        result,
        T(
            """
            ret
            3
            4
            5
            """,
        ),
    )


def test_udf_async_options(tmp_path: pathlib.Path):
    cache_dir = tmp_path / "test_cache"

    counter = mock.Mock()

    @pw.udf_async(cache_strategy=pw.internals.asynchronous.DiskCache())
    async def inc(x: int) -> int:
        counter()
        return x + 5

    input = T(
        """
        foo
        1
        2
        3
        """
    )
    result = input.select(ret=inc(pw.this.foo))
    expected = T(
        """
        ret
        6
        7
        8
        """
    )

    # run twice to check if cache is used
    assert_table_equality(
        result,
        expected,
        persistence_config=pw.persistence.Config.simple_config(
            pw.persistence.Backend.filesystem(cache_dir),
        ),
    )
    assert_table_equality(
        result,
        expected,
        persistence_config=pw.persistence.Config.simple_config(
            pw.persistence.Backend.filesystem(cache_dir),
        ),
    )
    assert os.path.exists(cache_dir)
    assert counter.call_count == 3


def test_udf_async():
    @pw.udf_async
    async def inc(a: int) -> int:
        await asyncio.sleep(0.1)
        return a + 3

    input = pw.debug.table_from_markdown(
        """
        a
        1
        2
        3
        """
    )

    result = input.select(ret=inc(pw.this.a))

    assert_table_equality(
        result,
        T(
            """
            ret
            4
            5
            6
            """,
        ),
    )


def test_udf_async_class():
    class Inc(pw.UDFAsync):
        def __init__(self, inc, **kwargs) -> None:
            super().__init__(**kwargs)
            self.inc = inc

        async def __wrapped__(self, a: int) -> int:
            await asyncio.sleep(0.1)
            return a + self.inc

    input = pw.debug.table_from_markdown(
        """
        a
        1
        2
        3
        """
    )

    inc = Inc(40)
    result = input.select(ret=inc(pw.this.a))

    assert_table_equality(
        result,
        T(
            """
            ret
            41
            42
            43
            """,
        ),
    )
