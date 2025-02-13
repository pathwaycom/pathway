# Copyright © 2024 Pathway

from __future__ import annotations

import collections
import functools
import inspect
import multiprocessing
import os
import pathlib
import re
import sys
import threading
import time
import uuid
from abc import abstractmethod
from collections.abc import Callable, Generator, Hashable, Iterable, Mapping
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass
from typing import Any, TypeVar

import numpy as np
import pandas as pd
import pytest

import pathway as pw
from pathway.debug import _markdown_to_pandas, table_from_markdown, table_from_pandas
from pathway.internals import api, datasource
from pathway.internals.config import get_pathway_config
from pathway.internals.graph_runner import GraphRunner
from pathway.internals.schema import is_subschema, schema_from_columns
from pathway.internals.table import Table

needs_multiprocessing_fork = pytest.mark.xfail(
    sys.platform != "linux",
    reason="multiprocessing needs to use fork() for pw.run() to work",
)

xfail_on_multiple_threads = pytest.mark.xfail(
    os.getenv("PATHWAY_THREADS", "1") != "1", reason="multiple threads"
)

only_standard_build = pytest.mark.xfail(
    pw.__version__.endswith("+enterprise"),
    reason="only works on standard build",
)


def xfail_on_arg(
    arg_name=None,
    xfail_values=None,
    condition: bool = False,
    reason: str = "",
):
    if callable(arg_name):
        test_func = arg_name

        @functools.wraps(test_func)
        def wrapper(*args, **kwargs):
            if condition:
                pytest.xfail(reason)
            return test_func(*args, **kwargs)

        return wrapper

    def decorator(test_func):
        @functools.wraps(test_func)
        def wrapper(*args, **kwargs):
            sig = inspect.signature(test_func)
            bound_args = sig.bind(*args, **kwargs)
            bound_args.apply_defaults()

            if arg_name and arg_name in bound_args.arguments:
                if bound_args.arguments[arg_name] in xfail_values and condition:
                    pytest.xfail(reason)

            return test_func(*args, **kwargs)

        return wrapper

    return decorator


only_with_license_key = functools.partial(
    xfail_on_arg,
    condition=get_pathway_config().license_key is None,
    reason="works only with a proper license key",
)


AIRBYTE_FAKER_CONNECTION_REL_PATH = "connections/faker.yaml"


def skip_on_multiple_workers() -> None:
    if os.environ.get("PATHWAY_THREADS", "1") != "1":
        pytest.skip()


class ExceptionAwareThread(threading.Thread):
    def run(self):
        self._exception = None
        try:
            if self._target is not None:  # type: ignore
                self._result = self._target(*self._args, **self._kwargs)  # type: ignore
        except Exception as e:
            self._exception = e
        finally:
            del self._target, self._args, self._kwargs  # type: ignore

    def join(self, timeout=None):
        super().join(timeout)
        if self._exception:
            raise self._exception
        return self._result


class UniquePortDispenser:
    """
    Tests are run simultaneously by several workers.
    Since they involve running a web server, they shouldn't interfere, so they
    should occupy distinct ports.
    This class automates unique port assignments for different tests.
    """

    range_start: int
    worker_range_size: int
    step_size: int
    next_available_port: int | None
    lock: threading.Lock

    def __init__(
        self,
        range_start: int = 12345,
        worker_range_size: int = 1000,
        step_size: int = 1,
    ):
        # the main worker is sometimes 'master', sometimes just not defined
        pytest_worker_id = os.environ.get("PYTEST_XDIST_WORKER", "master")
        if pytest_worker_id == "master":
            worker_id = 0
        elif pytest_worker_id.startswith("gw"):
            worker_id = int(pytest_worker_id[2:])
        else:
            raise ValueError(f"Unknown xdist worker id: {pytest_worker_id}")

        self.step_size = step_size
        self.range_start = range_start + worker_id * worker_range_size
        self.worker_range_size = worker_range_size
        self.next_available_port = None
        self.lock = threading.Lock()

    def get_unique_port(self, testrun_uid: str) -> int:
        with self.lock:
            if self.next_available_port is None:
                self.next_available_port = (
                    hash(testrun_uid) % self.worker_range_size + self.range_start
                )
            port = self.next_available_port
            self.next_available_port += self.step_size
            if self.next_available_port >= self.range_start + self.worker_range_size:
                self.next_available_port = self.range_start
        return port


@dataclass(order=True)
class DiffEntry:
    key: api.Pointer
    order: int
    insertion: bool
    row: dict[str, api.Value]

    @staticmethod
    def create(
        pk_table: pw.Table,
        pk_columns: dict[str, api.Value],
        order: int,
        insertion: bool,
        row: dict[str, api.Value],
        instance: api.Value | None = None,
    ) -> DiffEntry:
        key = DiffEntry.create_id_from(pk_table, pk_columns, instance=instance)
        return DiffEntry(key, order, insertion, row)

    def final_cleanup_entry(self):
        return DiffEntry(self.key, self.order + 1, False, self.row)

    @staticmethod
    def create_id_from(
        pk_table: pw.Table,
        pk_columns: dict[str, api.Value],
        instance: api.Value | None = None,
    ) -> api.Pointer:
        values = list(pk_columns.values())
        if instance is None:
            return api.ref_scalar(*values)
        else:
            return api.ref_scalar_with_instance(*values, instance=instance)


# This class is an abstract subclass of OnChangeCallback, which takes a list of entries
# representing a stream, groups them by key, and orders them by (order, insertion);
# Such organized representation of a stream is kept in `state`.
#
# Remarks: the orders associated with any fixed key may differ from the times in the stream
# (as it's difficult to impose precise times to be present in the engine);
# the requirement is that for a fixed key, the ordering (by order, insertion) of entries
# should be the same as the same as what we expect to see in the output
class CheckKeyEntriesInStreamCallback(pw.io._subscribe.OnChangeCallback):
    state: collections.defaultdict[api.Pointer, collections.deque[DiffEntry]]

    def __init__(self, state_list: Iterable[DiffEntry]):
        super().__init__()
        state_list = sorted(state_list)
        self.state = collections.defaultdict(lambda: collections.deque())
        for entry in state_list:
            self.state[entry.key].append(entry)

    @abstractmethod
    def __call__(
        self,
        key: api.Pointer,
        row: dict[str, api.Value],
        time: int,
        is_addition: bool,
    ) -> Any:
        pass


class CheckKeyConsistentInStreamCallback(CheckKeyEntriesInStreamCallback):
    def __call__(
        self,
        key: api.Pointer,
        row: dict[str, api.Value],
        time: int,
        is_addition: bool,
    ) -> Any:
        q = self.state.get(key)
        assert (
            q
        ), f"Got unexpected entry {key=} {row=} {time=} {is_addition=}, expected entries= {self.state!r}"

        while True:
            entry = q.popleft()
            if (is_addition, row) == (entry.insertion, entry.row):
                if not q:
                    self.state.pop(key)
                break
            else:
                assert (
                    q
                ), f"Skipping over entries emptied the set of expected entries for {key=} and state = {self.state!r}"

    def on_end(self):
        assert not self.state, f"Non empty final state = {self.state!r}"


# this callback does not verify the order of entries, only that all of them were present
class CheckStreamEntriesEqualityCallback(CheckKeyEntriesInStreamCallback):
    def __call__(
        self,
        key: api.Pointer,
        row: dict[str, api.Value],
        time: int,
        is_addition: bool,
    ) -> Any:
        q = self.state.get(key)
        assert (
            q
        ), f"Got unexpected entry {key=} {row=} {time=} {is_addition=}, expected entries= {self.state!r}"

        entry = q.popleft()
        assert (is_addition, row) == (
            entry.insertion,
            entry.row,
        ), f"Got unexpected entry {key=} {row=} {time=} {is_addition=}, expected entries= {self.state!r}"
        if not q:
            self.state.pop(key)

    def on_end(self):
        assert not self.state, f"Non empty final state = {self.state!r}"


# assert_key_entries_in_stream_consistent verifies for each key, whether:
# - a sequence of updates in the table is a subsequence
# of the sequence of updates defined in expected
# - the final entry for both stream and list is the same
def assert_key_entries_in_stream_consistent(expected: list[DiffEntry], table: pw.Table):
    callback = CheckKeyConsistentInStreamCallback(expected)
    pw.io.subscribe(table, callback, callback.on_end)


def assert_stream_equal(expected: list[DiffEntry], table: pw.Table):
    callback = CheckStreamEntriesEqualityCallback(expected)
    pw.io.subscribe(table, callback, callback.on_end)


def assert_equal_tables(
    t0: api.CapturedStream, t1: api.CapturedStream, **kwargs
) -> None:
    assert api.squash_updates(t0, **kwargs) == api.squash_updates(t1, **kwargs)


def make_value_hashable(val: api.Value):
    if isinstance(val, np.ndarray):
        return (type(val), val.dtype, val.shape, str(val))
    elif isinstance(val, pw.Json):
        return (type(val), repr(val))
    else:
        return val


def make_row_hashable(row: tuple[api.Value, ...]):
    return tuple(make_value_hashable(val) for val in row)


def assert_equal_tables_wo_index(
    s0: api.CapturedStream, s1: api.CapturedStream, **kwargs
) -> None:
    t0 = api.squash_updates(s0, **kwargs)
    t1 = api.squash_updates(s1, **kwargs)
    assert collections.Counter(
        make_row_hashable(row) for row in t0.values()
    ) == collections.Counter(make_row_hashable(row) for row in t1.values())


def assert_equal_streams(
    t0: api.CapturedStream, t1: api.CapturedStream, **kwargs
) -> None:
    def transform(row: api.DataRow) -> Hashable:
        t = (row.key,) + tuple(row.values) + (row.time, row.diff)
        return make_row_hashable(t)

    assert collections.Counter(transform(row) for row in t0) == collections.Counter(
        transform(row) for row in t1
    )


def assert_equal_streams_wo_index(
    t0: api.CapturedStream, t1: api.CapturedStream, **kwargs
) -> None:
    def transform(row: api.DataRow) -> Hashable:
        t = tuple(row.values) + (row.time, row.diff)
        return make_row_hashable(t)

    assert collections.Counter(transform(row) for row in t0) == collections.Counter(
        transform(row) for row in t1
    )


def assert_split_into_time_groups(
    t0: api.CapturedStream,
    t1: api.CapturedStream,
    transform: Callable[[api.DataRow], tuple[Hashable, int]],
) -> None:
    result: list[tuple[Any, int]] = [transform(row) for row in t0]
    expected: list[tuple[Any, int]] = [transform(row) for row in t1]
    assert len(result) == len(expected)
    expected_counter = collections.Counter(row[0] for row in expected)
    for key, count in expected_counter.items():
        if count != 1:
            raise ValueError(
                "This utility function does not support cases where the count of (value, diff)"
                + f" pair is !=1, but the count of {key} is {count}."
            )
    result.sort()
    expected.sort()
    expected_to_result_time: dict[int, int] = {}
    for (res_val, res_time), (ex_val, ex_time) in zip(result, expected):
        assert res_val == ex_val
        if ex_time not in expected_to_result_time:
            expected_to_result_time[ex_time] = res_time
        if res_time != expected_to_result_time[ex_time]:
            raise AssertionError(
                f"Expected {res_val} to have time {expected_to_result_time[ex_time]}"
                + f" but it has time {res_time}."
            )


def assert_streams_in_time_groups(
    t0: api.CapturedStream, t1: api.CapturedStream, **kwargs
) -> None:
    def transform(row: api.DataRow) -> tuple[Hashable, int]:
        t = (row.key, *row.values, row.diff)
        return make_row_hashable(t), row.time

    assert_split_into_time_groups(t0, t1, transform)


def assert_streams_in_time_groups_wo_index(
    t0: api.CapturedStream, t1: api.CapturedStream, **kwargs
) -> None:
    def transform(row: api.DataRow) -> tuple[Hashable, int]:
        t = (*row.values, row.diff)
        return make_row_hashable(t), row.time

    assert_split_into_time_groups(t0, t1, transform)


class CsvLinesNumberChecker:
    def __init__(self, path, n_lines):
        self.path = path
        self.n_lines = n_lines

    def __call__(self):
        try:
            result = pd.read_csv(self.path).sort_index()
        except Exception:
            return False
        print(
            f"Actual (expected) lines number: {len(result)} ({self.n_lines})",
            file=sys.stderr,
        )
        return len(result) == self.n_lines

    def provide_information_on_failure(self):
        if not self.path.exists():
            return f"{self.path} does not exist"
        with open(self.path) as f:
            return f"Final output contents:\n{f.read()}"


class FileLinesNumberChecker:
    def __init__(self, path, n_lines):
        self.path = path
        self.n_lines = n_lines

    def __call__(self):
        if not self.path.exists():
            return False
        n_lines_actual = 0
        with open(self.path) as f:
            for row in f:
                n_lines_actual += 1
        print(
            f"Actual (expected) lines number: {n_lines_actual} ({self.n_lines})",
            file=sys.stderr,
        )
        return n_lines_actual == self.n_lines

    def provide_information_on_failure(self):
        if not self.path.exists():
            return f"{self.path} does not exist"
        with open(self.path) as f:
            return f"Final output contents:\n{f.read()}"


def expect_csv_checker(expected, output_path, usecols=("k", "v"), index_col=("k")):
    expected = (
        pw.debug._markdown_to_pandas(expected)
        .set_index(index_col, drop=False)
        .sort_index()
    )

    def checker():
        try:
            result = (
                pd.read_csv(output_path, usecols=[*usecols, *index_col])
                .convert_dtypes()
                .set_index(index_col, drop=False)
                .sort_index()
            )
        except Exception:
            return False
        return expected.equals(result)

    return checker


class CsvPathwayChecker:
    def __init__(
        self,
        expected: str,
        output_path: pathlib.Path,
        *,
        id_from: list[str] | None = None,
    ) -> None:
        self.expected = expected
        self.output_path = output_path
        self.id_from = id_from

    def __call__(self):
        try:
            ex = pw.debug.table_from_markdown(self.expected)
            dfs = []
            for entry in os.listdir(self.output_path):
                dfs.append(pd.read_csv(self.output_path / entry))
            df = pd.concat(dfs, ignore_index=True).rename(
                columns={"time": "__time__", "diff": "__diff__"}
            )
            res = pw.debug.table_from_pandas(df, id_from=self.id_from)
            assert_table_equality_wo_index(res, ex)
        except Exception as exception:
            self.exception = exception
            return False
        return True

    def provide_information_on_failure(self):
        return self.exception


class LogicChecker:
    def __init__(self, logic: Callable) -> None:
        self.logic = logic

    def __call__(self):
        try:
            self.logic()
        except Exception as exception:
            self.exception = exception
            return False
        return True

    def provide_information_on_failure(self):
        return self.exception


@dataclass(frozen=True)
class TestDataSource(datasource.DataSource):
    __test__ = False

    def is_bounded(self) -> bool:
        raise NotImplementedError()

    def is_append_only(self) -> bool:
        return False


def apply_defaults_for_run_kwargs(kwargs):
    kwargs.setdefault("debug", True)
    kwargs.setdefault("monitoring_level", pw.MonitoringLevel.NONE)


def run_graph_and_validate_result(verifier: Callable, assert_schemas=True):
    def assert_schemas_the_same(table: Table, expected: Table):
        table_schema_dict = table.schema.typehints()
        expected_schema_dict = expected.schema.typehints()
        columns_schema_dict = schema_from_columns(
            table._columns, table._id_column
        ).typehints()
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
            # assert table.schema._id_dtype == expected.schema._id_dtype
        else:
            assert columns_schema_dict != table_schema_dict or not (
                is_subschema(table.schema, expected.schema)
                and is_subschema(expected.schema, table.schema)
            ), "wo_types is not needed"

        if list(table.column_names()) != list(expected.column_names()):
            raise RuntimeError(
                f"Mismatched column names, {list(table.column_names())} vs {list(expected.column_names())}"
            )

    def inner(
        table: Table | tuple[Table, ...], expected: Table | tuple[Table, ...], **kwargs
    ):
        if isinstance(table, Table):
            table = (table,)
        if isinstance(expected, Table):
            expected = (expected,)
        for t, ex in zip(table, expected, strict=True):
            assert_schemas_the_same(t, ex)

        apply_defaults_for_run_kwargs(kwargs)
        print("We will do GraphRunner with the following kwargs: ", kwargs)

        captured = GraphRunner(
            table[0]._source.graph, _stacklevel=2, **kwargs
        ).run_tables(*table, *expected)
        n = len(expected)
        captured_tables, captured_expected = captured[:n], captured[n:]
        for captured_t, captured_ex in zip(captured_tables, captured_expected):
            verifier(
                captured_t,
                captured_ex,
                terminate_on_error=kwargs.get("terminate_on_error", True),
            )

    return inner


def T(*args, format="markdown", _stacklevel=1, **kwargs):
    if format == "pandas":
        return table_from_pandas(*args, **kwargs, _stacklevel=_stacklevel + 1)
    assert format == "markdown"
    return table_from_markdown(*args, **kwargs, _stacklevel=_stacklevel + 1)


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

assert_stream_equality = run_graph_and_validate_result(assert_equal_streams)

assert_stream_equality_wo_index = run_graph_and_validate_result(
    assert_equal_streams_wo_index
)

assert_stream_equality_wo_types = run_graph_and_validate_result(
    assert_equal_streams, assert_schemas=False
)

assert_stream_equality_wo_index_types = run_graph_and_validate_result(
    assert_equal_streams_wo_index, assert_schemas=False
)

assert_stream_split_into_groups = run_graph_and_validate_result(
    assert_streams_in_time_groups
)

assert_stream_split_into_groups_wo_index = run_graph_and_validate_result(
    assert_streams_in_time_groups_wo_index
)

assert_stream_split_into_groups_wo_types = run_graph_and_validate_result(
    assert_streams_in_time_groups, assert_schemas=False
)

assert_stream_split_into_groups_wo_index_types = run_graph_and_validate_result(
    assert_streams_in_time_groups_wo_index, assert_schemas=False
)


def run(**kwargs):
    apply_defaults_for_run_kwargs(kwargs)
    pw.run(**kwargs)


def run_all(**kwargs):
    apply_defaults_for_run_kwargs(kwargs)
    pw.run_all(**kwargs)


def wait_result_with_checker(
    checker: Callable[[], bool],
    timeout_sec: float,
    *,
    step: float = 0.1,
    target: Callable[..., None] | None = run,
    processes: int = 1,
    first_port: int | None = None,
    args: Iterable[Any] = (),
    kwargs: Mapping[str, Any] = {},
) -> None:
    handles: list[multiprocessing.Process] = []
    try:
        if target is not None:
            assert (
                multiprocessing.get_start_method() == "fork"
            ), "multiprocessing does not use fork(), pw.run() will not work"

            if processes != 1:
                assert first_port is not None
                run_id = uuid.uuid4()

                def target_wrapped(process_id, *args, **kwargs):
                    os.environ["PATHWAY_PROCESSES"] = str(processes)
                    os.environ["PATHWAY_FIRST_PORT"] = str(first_port)
                    os.environ["PATHWAY_PROCESS_ID"] = str(process_id)
                    os.environ["PATHWAY_RUN_ID"] = str(run_id)
                    target(*args, **kwargs)

                for process_id in range(processes):
                    p = multiprocessing.Process(
                        target=target_wrapped, args=(process_id, *args), kwargs=kwargs
                    )
                    p.start()
                    handles.append(p)
            else:
                target_wrapped = target
                p = multiprocessing.Process(target=target, args=args, kwargs=kwargs)
                p.start()
                handles.append(p)

        succeeded = False
        start_time = time.monotonic()
        while True:
            time.sleep(step)

            elapsed = time.monotonic() - start_time
            if elapsed >= timeout_sec:
                print("Timed out", file=sys.stderr)
                break

            succeeded = checker()
            if succeeded:
                print(
                    f"Correct result obtained after {elapsed:.1f} seconds",
                    file=sys.stderr,
                )
                break

            if target is not None and not any(handle.is_alive() for handle in handles):
                print("All processes are done", file=sys.stderr)
                assert all(handle.exitcode == 0 for handle in handles)
                break

        if not succeeded:
            provide_information_on_failure: Callable[[], str] | None = getattr(
                checker, "provide_information_on_failure", None
            )
            if provide_information_on_failure is not None:
                details = provide_information_on_failure()
            else:
                details = "(no details)"
            print(f"Checker failed: {details}", file=sys.stderr)
            raise AssertionError(details)
    finally:
        if target is not None:
            if "persistence_config" in kwargs:
                time.sleep(5.0)  # allow a little gap to persist state

            for p in handles:
                p.terminate()
                p.join()


def write_csv(path: str | pathlib.Path, table_def: str, **kwargs):
    df = _markdown_to_pandas(table_def)
    df.to_csv(path, encoding="utf-8", **kwargs)


def write_lines(path: str | pathlib.Path, data: str | list[str]):
    if isinstance(data, str):
        data = [data]
    data = [row + "\n" for row in data]
    with open(path, "w+") as f:
        f.writelines(data)


def read_lines(path: str | pathlib.Path) -> list[str]:
    with open(path) as f:
        return f.readlines()


def get_aws_s3_settings():
    return pw.io.s3.AwsS3Settings(
        bucket_name="aws-integrationtest",
        access_key=os.environ["AWS_S3_ACCESS_KEY"],
        secret_access_key=os.environ["AWS_S3_SECRET_ACCESS_KEY"],
        region="eu-central-1",
    )


def get_minio_settings():
    return pw.io.minio.MinIOSettings(
        bucket_name="minio-integrationtest",
        access_key=os.environ["MINIO_S3_ACCESS_KEY"],
        secret_access_key=os.environ["MINIO_S3_SECRET_ACCESS_KEY"],
        endpoint="minio-api.deploys.pathway.com",
    )


# Callback class for checking whether number of distinct timestamps of
# rows is equal to expected
class CountDifferentTimestampsCallback(pw.io.OnChangeCallback):
    timestamps: set[int]

    def __init__(self, expected: int | None = None):
        self.timestamps = set()
        self.expected = expected

    def __call__(self, key, row, time: int, is_addition):
        self.timestamps.add(time)

    def on_end(self):
        if self.expected is not None:
            assert len(self.timestamps) == self.expected


C = TypeVar("C", bound=Callable)


@contextmanager
def warns_here(
    expected_warning: type[Warning] | tuple[type[Warning], ...] = Warning,
    *,
    match: str | re.Pattern[str] | None = None,
) -> Generator[pytest.WarningsRecorder, None, None]:
    frame = sys._getframe(2)
    code = frame.f_code
    first_line = frame.f_lineno
    del frame
    file_name = code.co_filename
    function_lines = {
        line for (_start, _end, line) in code.co_lines() if line is not None
    }
    del code

    with pytest.warns(expected_warning, match=match) as context:
        yield context

    def matches(warning) -> bool:
        if not isinstance(warning.message, expected_warning):
            return False
        if match is not None and not re.search(match, str(warning.message)):
            return False
        if warning.filename != file_name:
            return False
        if warning.lineno < first_line:
            return False
        if warning.lineno not in function_lines:
            return False
        return True

    if not any(matches(warning) for warning in context):
        raise AssertionError(
            "No matched warning caused by expected source line.\n"
            "All warnings:\n"
            + "\n".join(f"  {warning}" for warning in context)
            + f"\nExpected: {file_name!r}, line in "
            f"{list(line for line in function_lines if line >= first_line)}"
        )


def deprecated_call_here(
    *, match: str | re.Pattern[str] | None = None
) -> AbstractContextManager[pytest.WarningsRecorder]:
    return warns_here((DeprecationWarning, PendingDeprecationWarning), match=match)


def consolidate(df: pd.DataFrame) -> pd.DataFrame:
    values = None
    for column in df.columns:
        if column in ["time", "diff"]:
            continue
        if values is None:
            values = df[column].astype(str)
        else:
            values = values + "," + df[column].astype(str)
    df["_all_values"] = values

    total = {}
    for _, row in df.iterrows():
        value = row["_all_values"]
        if value not in total:
            total[value] = 0
        total[value] += row["diff"]

    for i in range(df.shape[0]):
        value = df.at[i, "_all_values"]
        df.at[i, "diff"] = total[value]
        total[value] = 0
    return df[df["diff"] != 0].drop(columns=["_all_values"])


def combine_columns(df: pd.DataFrame) -> pd.Series:
    result = None
    for column in df.columns:
        if column == "time":
            continue
        if result is None:
            result = df[column].astype(str)
        else:
            result += "," + df[column].astype(str)
    assert result is not None
    return result


def assert_sets_equality_from_path(path: pathlib.Path, expected: set[str]) -> None:
    try:
        result = combine_columns(consolidate(pd.read_csv(path)))
    except pd.errors.EmptyDataError:
        result = pd.Series([])
    assert set(result) == expected
