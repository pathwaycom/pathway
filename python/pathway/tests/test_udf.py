# Copyright © 2024 Pathway

from __future__ import annotations

import asyncio
import os
import pathlib
import re
import sys
import threading
import time
import warnings
from typing import Optional
from unittest import mock

import pytest

import pathway as pw
from pathway.internals import api
from pathway.internals.udfs.executors import Executor
from pathway.tests.utils import (
    T,
    assert_stream_equality,
    assert_table_equality,
    assert_table_equality_wo_index,
    assert_table_equality_wo_types,
    run_all,
    warns_here,
    xfail_on_multiple_threads,
)


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
    class Inc(pw.UDF):
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


def get_async_executor(fully_async: bool) -> Executor:
    if fully_async:
        return pw.udfs.fully_async_executor()
    else:
        return pw.udfs.async_executor()


@pytest.mark.parametrize("fully_async", [True, False])
def test_udf_async_options(tmp_path: pathlib.Path, fully_async):
    cache_dir = tmp_path / "test_cache"

    counter = mock.Mock()

    @pw.udf(
        executor=get_async_executor(fully_async), cache_strategy=pw.udfs.DiskCache()
    )
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
    if fully_async:
        result = result.await_futures()
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
        persistence_config=pw.persistence.Config(
            pw.persistence.Backend.filesystem(cache_dir),
        ),
    )
    assert_table_equality(
        result,
        expected,
        persistence_config=pw.persistence.Config(
            pw.persistence.Backend.filesystem(cache_dir),
        ),
    )
    assert os.path.exists(cache_dir)
    assert counter.call_count == 3


@pytest.mark.parametrize("fully_async", [True, False])
@pytest.mark.skipif(sys.version_info < (3, 11), reason="test requires asyncio.Barrier")
def test_udf_async(fully_async):
    barrier = asyncio.Barrier(3)  # type: ignore[attr-defined]
    # mypy complains because of versions lower than 3.11

    @pw.udf(executor=get_async_executor(fully_async))
    async def inc(a: int) -> int:
        await barrier.wait()
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

    if fully_async:
        result = result.await_futures()

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


def test_udf_sync():
    barrier = threading.Barrier(3, timeout=1)

    @pw.udf
    def inc(a: int) -> int:
        barrier.wait()
        return a + 3

    input = pw.debug.table_from_markdown(
        """
        a
        1
        2
        3
        """
    )

    input.select(ret=inc(pw.this.a))

    with pytest.raises(threading.BrokenBarrierError):
        run_all()


@pytest.mark.parametrize("fully_async", [True, False])
def test_udf_sync_with_async_executor(fully_async):
    barrier = threading.Barrier(3, timeout=10)

    @pw.udf(executor=get_async_executor(fully_async))
    def inc(a: int) -> int:
        barrier.wait()
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

    if fully_async:
        result = result.await_futures()

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


@pytest.mark.parametrize("fully_async", [True, False])
def test_udf_async_class(fully_async):
    class Inc(pw.UDF):
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

    inc = Inc(40, executor=get_async_executor(fully_async))
    result = input.select(ret=inc(pw.this.a))
    if fully_async:
        result = result.await_futures()

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


@pytest.mark.parametrize("fully_async", [True, False])
def test_udf_propagate_none(fully_async):
    internal_add = mock.Mock()

    @pw.udf(executor=get_async_executor(fully_async), propagate_none=True)
    def add(a: int, b: int) -> int:
        assert a is not None
        assert b is not None
        internal_add()
        return a + b

    input = T(
        """
        a | b
        1 | 6
        2 |
          | 8
        """
    )

    result = input.select(ret=add(pw.this.a, pw.this.b))
    if fully_async:
        result = result.await_futures()

    assert_table_equality(
        result,
        T(
            """
            ret
            7
            None
            None
            """,
        ),
    )
    internal_add.assert_called_once()


@pytest.mark.parametrize("sync", [True, False])
def test_udf_make_deterministic(sync: bool) -> None:
    internal_inc = mock.Mock()

    if sync:

        @pw.udf
        def inc(a: int) -> int:
            internal_inc(a)
            return a + 1

    else:

        @pw.udf
        async def inc(a: int) -> int:
            await asyncio.sleep(a / 10)
            internal_inc(a)
            return a + 1

    input = T(
        """
          | a | __time__ | __diff__
        1 | 1 |     2    |     1
        2 | 2 |     2    |     1
        2 | 2 |     4    |    -1
        3 | 3 |     6    |     1
        4 | 1 |     8    |     1
        3 | 3 |     8    |    -1
        3 | 4 |     8    |     1
        """
    )

    result = input.select(ret=inc(pw.this.a))

    assert_table_equality(
        result,
        T(
            """
              | ret
            1 | 2
            3 | 5
            4 | 2
            """,
        ),
    )
    internal_inc.assert_has_calls(
        [mock.call(1), mock.call(2), mock.call(3), mock.call(1), mock.call(4)],
        any_order=True,
    )
    assert internal_inc.call_count == 5


@pytest.mark.parametrize("sync", [True, False])
def test_udf_make_deterministic_2(sync: bool) -> None:
    counter = mock.Mock()
    if sync:

        @pw.udf
        def foo(a: int) -> int:
            counter(a)
            return a

    else:

        @pw.udf
        async def foo(a: int) -> int:
            await asyncio.sleep(a / 10)
            counter(a)
            return a

    input = T(
        """
        a | __time__ | __diff__
        1 |     2    |     1
        1 |     4    |    -1
        1 |     6    |     1
        1 |     8    |    -1
        1 |    10    |     1
    """,
        id_from=["a"],
    )

    res = input.select(a=foo(pw.this.a))

    assert_stream_equality(
        res,
        T(
            """
            a | __time__ | __diff__
            1 |     2    |     1
            1 |     4    |    -1
            1 |     6    |     1
            1 |     8    |    -1
            1 |    10    |     1
        """,
            id_from=["a"],
        ),
    )
    counter.assert_has_calls(
        [mock.call(1), mock.call(1), mock.call(1)],
        any_order=True,
    )
    assert counter.call_count == 3


@xfail_on_multiple_threads
def test_udf_cache(monkeypatch, tmp_path: pathlib.Path):
    monkeypatch.delenv("PATHWAY_PERSISTENT_STORAGE", raising=False)
    internal_inc = mock.Mock()

    @pw.udf(deterministic=True, cache_strategy=pw.udfs.DiskCache())
    def inc(a: int) -> int:
        internal_inc(a)
        return a + 1

    input = T(
        """
        a
        1
        2
        2
        3
        1
        """
    )

    result = input.select(ret=inc(pw.this.a))

    pstorage_dir = tmp_path / "PStorage"
    persistence_config = pw.persistence.Config(
        backend=pw.persistence.Backend.filesystem(pstorage_dir),
        persistence_mode=api.PersistenceMode.UDF_CACHING,
    )
    assert_table_equality(
        result,
        T(
            """
            ret
            2
            3
            3
            4
            2
            """,
        ),
        persistence_config=persistence_config,
    )
    internal_inc.assert_has_calls(
        [mock.call(1), mock.call(2), mock.call(3)], any_order=True
    )
    assert internal_inc.call_count == 3


def test_udf_cache_too_small_size_limit(monkeypatch, tmp_path: pathlib.Path):
    monkeypatch.delenv("PATHWAY_PERSISTENT_STORAGE", raising=False)

    @pw.udf(deterministic=True, cache_strategy=pw.udfs.DiskCache(size_limit=10))
    def inc(a: int) -> int:
        return a + 1

    input = T(
        """
        a
        1
        """
    )
    input.select(ret=inc(pw.this.a))

    pstorage_dir = tmp_path / "PStorage"
    persistence_config = pw.persistence.Config.simple_config(
        backend=pw.persistence.Backend.filesystem(pstorage_dir),
        persistence_mode=api.PersistenceMode.UDF_CACHING,
    )
    with pytest.raises(KeyError):
        run_all(persistence_config=persistence_config)


@pytest.mark.parametrize("sync", [True, False])
def test_udf_deterministic_not_stored(monkeypatch, tmp_path: pathlib.Path, sync):
    monkeypatch.delenv("PATHWAY_PERSISTENT_STORAGE", raising=False)
    internal_inc = mock.Mock()

    if sync:

        @pw.udf(deterministic=True)
        def inc(a: int) -> int:
            internal_inc(a)
            return a + 1

    else:

        @pw.udf(deterministic=True)
        async def inc(a: int) -> int:
            await asyncio.sleep(a / 10)
            internal_inc(a)
            return a + 1

    input = T(
        """
        a
        1
        2
        2
        3
        1
        """
    )

    result = input.select(ret=inc(pw.this.a))

    pstorage_dir = tmp_path / "PStorage"
    persistence_config = pw.persistence.Config(
        backend=pw.persistence.Backend.filesystem(pstorage_dir),
        persistence_mode=api.PersistenceMode.UDF_CACHING,
    )
    assert_table_equality(
        result,
        T(
            """
            ret
            2
            3
            3
            4
            2
            """,
        ),
        persistence_config=persistence_config,
    )
    internal_inc.assert_has_calls(
        [mock.call(1), mock.call(2), mock.call(3), mock.call(2), mock.call(1)],
        any_order=True,
    )
    assert internal_inc.call_count == 5


@pytest.mark.parametrize("fully_async", [True, False])
def test_async_udf_propagate_none(fully_async):
    internal_add = mock.Mock()

    @pw.udf(propagate_none=True, executor=get_async_executor(fully_async))
    async def add(a: int, b: int) -> int:
        assert a is not None
        assert b is not None
        internal_add()
        return a + b

    input = T(
        """
        a | b
        1 | 6
        2 |
          | 8
        """
    )

    result = input.select(ret=add(pw.this.a, pw.this.b))
    if fully_async:
        result = result.await_futures()

    assert_table_equality(
        result,
        T(
            """
            ret
            7
            None
            None
            """,
        ),
    )
    internal_add.assert_called_once()


@pytest.mark.parametrize("fully_async", [True, False])
def test_async_udf_with_none(fully_async):
    internal_add = mock.Mock()

    @pw.udf(executor=get_async_executor(fully_async))
    async def add(a: int, b: int) -> int:
        internal_add()
        if a is None:
            return b
        if b is None:
            return a
        return a + b

    input = T(
        """
        a | b
        1 | 6
        2 |
          | 8
        """
    )

    result = input.select(ret=add(pw.this.a, pw.this.b))
    if fully_async:
        result = result.await_futures()

    assert_table_equality(
        result,
        T(
            """
            ret
            7
            2
            8
            """,
        ),
    )
    assert internal_add.call_count == 3


@pytest.mark.parametrize("fully_async", [True, False])
def test_udf_timeout(fully_async):
    if fully_async:
        executor = pw.udfs.fully_async_executor(timeout=0.1)
    else:
        executor = pw.udfs.async_executor(timeout=0.1)

    @pw.udf(executor=executor)
    async def inc(a: int) -> int:
        await asyncio.sleep(2)
        return a + 1

    input = pw.debug.table_from_markdown(
        """
        a
        1
        """
    )

    input.select(ret=inc(pw.this.a))
    expected: type[Exception]
    if fully_async:
        expected = api.EngineError
    elif sys.version_info < (3, 11):
        expected = asyncio.exceptions.TimeoutError
    else:
        expected = TimeoutError
    with pytest.raises(expected):
        run_all()


@pytest.mark.parametrize("fully_async", [True, False])
def test_udf_too_fast_for_timeout(fully_async):
    if fully_async:
        executor = pw.udfs.fully_async_executor(timeout=10.0)
    else:
        executor = pw.udfs.async_executor(timeout=10.0)

    @pw.udf(executor=executor)
    async def inc(a: int) -> int:
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
    if fully_async:
        result = result.await_futures()
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


@pytest.mark.parametrize("sync", ["sync", "async", "fully_async"])
def test_udf_in_memory_cache(sync: str) -> None:
    internal_inc = mock.Mock()

    if sync == "sync":

        @pw.udf(cache_strategy=pw.udfs.InMemoryCache())
        def inc(a: int) -> int:
            internal_inc(a)
            return a + 1

    else:

        @pw.udf(
            cache_strategy=pw.udfs.InMemoryCache(),
            executor=get_async_executor(sync == "fully_async"),
        )
        async def inc(a: int) -> int:
            await asyncio.sleep(a / 10)
            internal_inc(a)
            return a + 1

    input = pw.debug.table_from_markdown(
        """
        a
        1
        2
        1
        2
        3
    """
    )
    result = input.select(ret=inc(pw.this.a))
    if sync == "fully_async":
        result = result.await_futures()
    expected = T(
        """
        ret
        2
        3
        2
        3
        4
        """
    )
    assert_table_equality(result, expected)
    internal_inc.assert_has_calls(
        [mock.call(1), mock.call(2), mock.call(3)], any_order=True
    )
    assert internal_inc.call_count == 3

    assert_table_equality(result, expected)
    assert internal_inc.call_count == 3  # count did not change


@pytest.mark.parametrize("sync", ["sync", "async", "fully_async"])
def test_udf_in_memory_cache_with_limit(sync: str) -> None:
    internal_inc = mock.Mock()

    if sync == "sync":

        @pw.udf(cache_strategy=pw.udfs.InMemoryCache(max_size=0))
        def inc(a: int) -> int:
            internal_inc(a)
            return a + 1

    else:

        @pw.udf(
            cache_strategy=pw.udfs.InMemoryCache(max_size=0),
            executor=get_async_executor(sync == "fully_async"),
        )
        async def inc(a: int) -> int:
            await asyncio.sleep(a / 10)
            internal_inc(a)
            return a + 1

    input = pw.debug.table_from_markdown(
        """
        a | __time__
        1 |     2
        1 |     4
        1 |     6
    """
    )
    result = input.select(ret=inc(pw.this.a))
    if sync == "fully_async":
        result = result.await_futures()
    expected = T(
        """
        ret
        2
        2
        2
        """
    )
    assert_table_equality(result, expected)
    internal_inc.assert_has_calls([mock.call(1), mock.call(1), mock.call(1)])
    assert internal_inc.call_count == 3


@pytest.mark.parametrize(
    "sync",
    [
        "sync",
        "async",
        pytest.param(
            "fully_async",
            marks=pytest.mark.xfail(
                sys.platform != "linux", reason="InMemoryCache uses incompatible loop"
            ),
        ),
    ],
)
def test_udf_in_memory_cache_multiple_places(sync: bool) -> None:
    internal_inc = mock.Mock()

    if sync == "sync":

        @pw.udf(cache_strategy=pw.udfs.InMemoryCache())
        def inc(a: int) -> int:
            internal_inc(a)
            return a + 1

    else:

        @pw.udf(
            cache_strategy=pw.udfs.InMemoryCache(),
            executor=get_async_executor(sync == "fully_async"),
        )
        async def inc(a: int) -> int:
            internal_inc(a)
            return a + 1

    input = pw.debug.table_from_markdown(
        """
        a
        1
        2
        1
        2
        3
    """
    )
    result = input.with_columns(ret=inc(pw.this.a))
    result = result.with_columns(ret_2=inc(pw.this.a))
    if sync == "fully_async":
        result = result.await_futures()
    expected = T(
        """
        a | ret | ret_2
        1 |  2  |   2
        2 |  3  |   3
        1 |  2  |   2
        2 |  3  |   3
        3 |  4  |   4
        """
    )
    assert_table_equality(result, expected)
    internal_inc.assert_has_calls(
        [mock.call(1), mock.call(2), mock.call(3)], any_order=True
    )
    assert internal_inc.call_count == 3


def test_udf_warn_on_too_specific_return_type() -> None:
    @pw.udf(return_type=int)
    def f(a: int) -> Optional[int]:
        return a + 1

    msg = (
        "The value of return_type parameter (<class 'int'>) is inconsistent with UDF's"
        + " return type annotation (typing.Optional[int])."
    )
    with warns_here(Warning, match=re.escape(msg)):
        f(pw.this.a)


def test_udf_dont_warn_on_broader_return_type() -> None:
    @pw.udf(return_type=Optional[int])
    def f(a: int) -> int:
        return a + 1

    with warnings.catch_warnings():
        warnings.simplefilter("error")
        f(pw.this.a)


@pytest.mark.parametrize("sync", ["sync", "async", "fully_async"])
def test_cast_on_return(sync: str) -> None:
    if sync == "sync":

        @pw.udf()
        def f(a: int) -> float:
            return a

    else:

        @pw.udf(executor=get_async_executor(sync == "fully_async"))
        async def f(a: int) -> float:
            return a

    t = pw.debug.table_from_markdown(
        """
        a |  b
        1 | 1.5
        2 | 2.5
        3 | 3.5
    """
    ).with_columns(a=f(pw.this.a))

    if sync == "fully_async":
        t = t.await_futures()
    res = t.select(c=pw.this.a + pw.this.b)
    expected = pw.debug.table_from_markdown(
        """
        c
        2.5
        4.5
        6.5
    """
    )
    assert_table_equality(res, expected)


@xfail_on_multiple_threads
def test_append_only_non_deterministic_not_storing_results():
    class Schema(pw.Schema, append_only=True):
        # lie that a table is append only to check if values aren't stored
        a: int

    t = pw.debug.table_from_markdown(
        """
          | a | __time__ | __diff__
        1 | 1 |     2    |     1
        2 | 2 |     4    |     1
        1 | 1 |     6    |    -1
        3 | 3 |     8    |     1
        3 | 3 |    10    |    -1
    """,
        schema=Schema,
    )

    count = 0

    @pw.udf
    def f(a: int) -> int:
        nonlocal count
        count += 1
        return count

    result = t.with_columns(b=f(pw.this.a))

    [rows] = pw.debug._compute_tables(result)
    assert sorted([row.values[0] for row in rows]) == [1, 1, 2, 3, 3]
    assert sorted([row.values[1] for row in rows]) == [1, 2, 3, 4, 5]


@xfail_on_multiple_threads
def test_append_only_results_stored_forever_column_append_only_storage_not_append_only():
    class Schema(pw.Schema):
        a: int = pw.column_definition(append_only=True)
        b: int

    t = pw.debug.table_from_markdown(
        """
          | a | b | __time__ | __diff__
        1 | 1 | 2 |     2    |     1
        2 | 2 | 3 |     4    |     1
        1 | 1 | 2 |     6    |    -1
        1 | 1 | 4 |     6    |     1
        3 | 3 | 5 |     8    |     1
        3 | 3 | 5 |    10    |    -1
        3 | 3 | 6 |    10    |     1
    """,
        schema=Schema,
    )

    count = 0

    @pw.udf
    def f(a: int) -> int:
        nonlocal count
        count += 1
        return count

    result = t.select(a=f(pw.this.a), b=pw.this.b)

    expected = pw.debug.table_from_markdown(
        """
          | a | b | __time__ | __diff__
        1 | 1 | 2 |     2    |     1
        2 | 2 | 3 |     4    |     1
        1 | 1 | 2 |     6    |    -1
        1 | 1 | 4 |     6    |     1
        3 | 3 | 5 |     8    |     1
        3 | 3 | 5 |    10    |    -1
        3 | 3 | 6 |    10    |     1
    """,
        _new_universe=True,
    )

    assert_stream_equality(result, expected)


@xfail_on_multiple_threads
def test_append_only_results_stored_temporarily_column_not_append_only_storage_not_append_only():
    class Schema(pw.Schema):
        a: int
        b: int

    t = pw.debug.table_from_markdown(
        """
          | a | b | __time__ | __diff__
        1 | 1 | 2 |     2    |     1
        2 | 2 | 3 |     4    |     1
        1 | 1 | 2 |     6    |    -1
        1 | 1 | 4 |     6    |     1
        3 | 3 | 5 |     8    |     1
        3 | 3 | 5 |    10    |    -1
        3 | 3 | 6 |    10    |     1
    """,
        schema=Schema,
    )

    count = 0

    @pw.udf
    def f(a: int) -> int:
        nonlocal count
        count += 1
        return count

    result = t.select(a=f(pw.this.a), b=pw.this.b)

    expected = pw.debug.table_from_markdown(
        """
          | a | b | __time__ | __diff__
        1 | 1 | 2 |     2    |     1
        2 | 2 | 3 |     4    |     1
        1 | 1 | 2 |     6    |    -1
        1 | 3 | 4 |     6    |     1
        3 | 4 | 5 |     8    |     1
        3 | 4 | 5 |    10    |    -1
        3 | 5 | 6 |    10    |     1
    """,
        _new_universe=True,
    )

    assert_stream_equality(result, expected)


def test_fully_async_udf():
    @pw.udf(executor=pw.udfs.fully_async_executor())
    async def inc(a: int) -> int:
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

    assert_table_equality_wo_types(
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


def test_fully_async_udf_propagation_allowed():
    @pw.udf(executor=pw.udfs.fully_async_executor())
    async def inc(a: int) -> int:
        return a + 1

    input = pw.debug.table_from_markdown(
        """
        a
        1
        2
        3
        """
    )

    t = input.with_columns(ret=inc(pw.this.a))
    result = t.select(a=pw.this.a + 2, b=pw.this.ret)

    assert_table_equality_wo_types(
        result,
        T(
            """
            a | b
            3 | 2
            4 | 3
            5 | 4
            """,
        ),
    )


@pytest.mark.skipif(
    sys.version_info < (3, 11), reason="_asyncio.Future arg not printed"
)
def test_future_dtype_disallowed_expression():
    @pw.udf(executor=pw.udfs.fully_async_executor())
    async def inc(a: int) -> int:
        return a + 1

    input = pw.debug.table_from_markdown(
        """
        a
        1
        2
        3
        """
    )

    msg = "Pathway does not support using binary operator add on columns of types _asyncio.Future[int], <class 'int'>."
    with pytest.raises(TypeError, match=re.escape(msg)):
        input.select(ret=inc(pw.this.a) + 1)


def table_with_future_ret() -> pw.Table:
    @pw.udf(executor=pw.udfs.fully_async_executor())
    async def inc(a: int) -> int:
        return a + 1

    input = pw.debug.table_from_markdown(
        """
        a
        1
        2
        3
        """
    )
    return input.with_columns(ret=inc(pw.this.a))


@pytest.mark.skipif(
    sys.version_info < (3, 11), reason="_asyncio.Future arg not printed"
)
def test_future_dtype_disallowed_reduce():
    t = table_with_future_ret()
    msg = (
        "Cannot perform pathway.reducers.sum when column of type _asyncio.Future[int] is involved."
        + " Consider applying `await_futures()` to the table used here"
    )
    with pytest.raises(TypeError, match=re.escape(msg)):
        t.reduce(s=pw.reducers.sum(pw.this.ret))


@pytest.mark.skipif(
    sys.version_info < (3, 11), reason="_asyncio.Future arg not printed"
)
def test_future_dtype_disallowed_in_groupby():
    t = table_with_future_ret()
    msg = (
        "Using column of type _asyncio.Future[int] is not allowed here."
        + " Consider applying `await_futures()` to the table first."
    )
    with pytest.raises(TypeError, match=re.escape(msg)):
        t.groupby(pw.this.ret)


@pytest.mark.skipif(
    sys.version_info < (3, 11), reason="_asyncio.Future arg not printed"
)
def test_future_dtype_disallowed_in_sort_key():
    t = table_with_future_ret()
    msg = (
        "Using column of type _asyncio.Future[int] is not allowed here."
        + " Consider applying `await_futures()` to the table first."
    )
    with pytest.raises(TypeError, match=re.escape(msg)):
        t.sort(pw.this.ret)


@pytest.mark.skipif(
    sys.version_info < (3, 11), reason="_asyncio.Future arg not printed"
)
def test_future_dtype_disallowed_in_sort_instance():
    t = table_with_future_ret()
    msg = (
        "Using column of type _asyncio.Future[int] is not allowed here."
        + " Consider applying `await_futures()` to the table first."
    )
    with pytest.raises(TypeError, match=re.escape(msg)):
        t.sort(pw.this.a, instance=pw.this.ret)


@pytest.mark.skipif(
    sys.version_info < (3, 11), reason="_asyncio.Future arg not printed"
)
def test_future_dtype_disallowed_in_deduplicate():
    t = table_with_future_ret()

    def acceptor(new_value, old_value) -> bool:
        return new_value >= old_value + 2

    msg = (
        "Using column of type _asyncio.Future[int] is not allowed here."
        + " Consider applying `await_futures()` to the table first."
    )
    with pytest.raises(TypeError, match=re.escape(msg)):
        t.deduplicate(value=pw.this.ret, acceptor=acceptor)


@pytest.mark.skipif(
    sys.version_info < (3, 11), reason="_asyncio.Future arg not printed"
)
def test_future_dtype_disallowed_in_deduplicate_instance():
    t = table_with_future_ret()

    def acceptor(new_value, old_value) -> bool:
        return new_value >= old_value + 2

    msg = (
        "Using column of type _asyncio.Future[int] is not allowed here."
        + " Consider applying `await_futures()` to the table first."
    )
    with pytest.raises(TypeError, match=re.escape(msg)):
        t.deduplicate(value=pw.this.a, instance=pw.this.ret, acceptor=acceptor)


@pytest.mark.skipif(
    sys.version_info < (3, 11), reason="_asyncio.Future arg not printed"
)
def test_future_dtype_disallowed_in_expressions():
    t = table_with_future_ret()
    msg = (
        "Cannot perform pathway.pointer_from when column of type _asyncio.Future[int] is involved."
        + " Consider applying `await_futures()` to the table used here."
    )
    with pytest.raises(TypeError, match=re.escape(msg)):
        t.select(p=t.pointer_from(t.ret))

    msg = "Cannot perform pathway.if_else on columns of types _asyncio.Future[int] and <class 'int'>."
    with pytest.raises(TypeError, match=re.escape(msg)):
        t.select(p=pw.if_else(t.a > 2, t.ret, 2))

    msg = (
        "Cannot perform pathway.make_tuple when column of type _asyncio.Future[int] is involved."
        + " Consider applying `await_futures()` to the table used here."
    )
    with pytest.raises(TypeError, match=re.escape(msg)):
        t.select(p=pw.make_tuple(t.ret, 2))

    msg = (
        "Cannot perform pathway.is_none when column of type _asyncio.Future[int] is involved."
        + " Consider applying `await_futures()` to the table used here."
    )
    with pytest.raises(TypeError, match=re.escape(msg)):
        t.select(p=t.ret.is_none())

    msg = (
        "Cannot perform pathway.is_not_none when column of type _asyncio.Future[int] is involved."
        + " Consider applying `await_futures()` to the table used here."
    )
    with pytest.raises(TypeError, match=re.escape(msg)):
        t.select(p=t.ret.is_not_none())

    @pw.udf
    def foo(a: int) -> int:
        return a - 1

    @pw.udf
    async def bar(a: int) -> int:
        return a - 1

    msg = (
        "Cannot perform pathway.apply when column of type _asyncio.Future[int] is involved."
        + " Consider applying `await_futures()` to the table used here."
    )
    with pytest.raises(TypeError, match=re.escape(msg)):
        t.select(p=foo(t.ret))

    msg = (
        "Cannot perform pathway.apply_async when column of type _asyncio.Future[int] is involved."
        + " Consider applying `await_futures()` to the table used here."
    )
    with pytest.raises(TypeError, match=re.escape(msg)):
        t.select(p=bar(t.ret))


@pytest.mark.skipif(
    sys.version_info < (3, 11), reason="_asyncio.Future arg not printed"
)
def test_future_dtype_disallowed_in_expressions_2():
    @pw.udf(executor=pw.udfs.fully_async_executor())
    async def inc(a: int | None) -> int | None:
        if a is None:
            return None
        return a + 1

    input = pw.debug.table_from_markdown(
        """
        a | b
        1 | 1
        2 | 1
        3 | 1
          | 1
        """
    )
    t = input.with_columns(ret=inc(pw.this.a))

    msg = (
        "Cannot perform pathway.coalesce when column of type _asyncio.Future[int | None] is involved."
        + " Consider applying `await_futures()` to the table used here."
    )
    with pytest.raises(TypeError, match=re.escape(msg)):
        t.select(p=pw.coalesce(t.ret, t.b))

    msg = (
        "Cannot perform pathway.require when column of type _asyncio.Future[int | None] is involved."
        + " Consider applying `await_futures()` to the table used here."
    )
    with pytest.raises(TypeError, match=re.escape(msg)):
        t.select(p=pw.require(t.ret, t.a))


def test_fully_async_udf_expression_allowed_after_await():
    result = table_with_future_ret().await_futures().select(ret=pw.this.ret + 2)

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


def test_fully_async_udf_reducer_allowed_after_await():
    result = (
        table_with_future_ret().await_futures().reduce(s=pw.reducers.sum(pw.this.ret))
    )

    assert_table_equality_wo_index(
        result,
        T(
            """
            s
            9
            """,
        ),
    )


def test_fully_async_udf_chaining():
    @pw.udf(executor=pw.udfs.fully_async_executor())
    async def inc(a: int) -> int:
        print(a)
        return a + 1

    input = pw.debug.table_from_markdown(
        """
        a
        1
        2
        3
        """
    )

    result = input.select(ret=inc(inc(pw.this.a)))

    assert_table_equality_wo_types(
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


@pytest.mark.parametrize("fully_async", [True, False])
def test_fully_async_udf_error_propagation(fully_async):

    @pw.udf(executor=get_async_executor(fully_async))
    async def inc(a: int) -> int:
        return a + 1

    input = pw.debug.table_from_markdown(
        """
        a | b
        1 | 1
        2 | 0
        3 | 1
        """
    )

    result = input.select(ret=inc(pw.this.a // pw.this.b))
    if fully_async:
        result = result.await_futures()
    result = result.select(ret=pw.fill_error(pw.this.ret, -1))

    assert_table_equality(
        result,
        T(
            """
            ret
             2
            -1
             4
            """,
        ),
        terminate_on_error=False,
    )


def test_fully_async_udf_first_result_after_deletion_and_next_insertion():
    class InputSchema(pw.Schema):
        a: int

    class InputSubject(pw.io.python.ConnectorSubject):
        def run(self):
            time.sleep(2)
            self._add_inner(api.ref_scalar(3), dict(a=10))
            time.sleep(0.2)
            self._remove_inner(api.ref_scalar(3), dict(a=10))
            time.sleep(0.2)
            self._add_inner(api.ref_scalar(3), dict(a=12))

    @pw.udf
    def foo(a: int) -> int:
        return a + 1

    @pw.udf(executor=pw.udfs.fully_async_executor(autocommit_duration_ms=10))
    async def bar(a: int) -> int:
        time.sleep(0.5)
        return a + 2

    t = pw.io.python.read(InputSubject(), schema=InputSchema, autocommit_duration_ms=10)
    res = t.select(x=foo(pw.this.a), y=bar(pw.this.a))
    expected = pw.debug.table_from_markdown(
        """
          |  x |  y
        3 | 13 | 14
    """
    )
    assert_table_equality_wo_types(res, expected)
