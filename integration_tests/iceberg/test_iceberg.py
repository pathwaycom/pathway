import datetime
import json
import multiprocessing
import os
import pickle
import threading
import time
import typing
import uuid

import pandas as pd
import pytest
from dateutil import tz
from pyiceberg.catalog import load_catalog
from pyiceberg.catalog.glue import GlueCatalog
from pyiceberg.exceptions import ServerError
from pyiceberg.schema import Schema as PyIcebergSchema
from pyiceberg.types import (
    DateType,
    DecimalType,
    FixedType,
    FloatType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
    StructType,
    TimeType,
    UUIDType,
)

import pathway as pw
from pathway.internals.parse_graph import G
from pathway.tests.utils import (
    FileLinesNumberChecker,
    get_aws_s3_settings,
    run,
    wait_result_with_checker,
)

INPUT_CONTENTS_1 = """{"user_id": 1, "name": "John"}
{"user_id": 2, "name": "Jane"}
{"user_id": 3, "name": "Alice"}
{"user_id": 4, "name": "Bob"}"""
INPUT_CONTENTS_2 = """{"user_id": 5, "name": "Peter"}
{"user_id": 6, "name": "Jake"}
{"user_id": 7, "name": "Dora"}
{"user_id": 8, "name": "Barbara"}"""
INPUT_CONTENTS_3 = """{"user_id": 9, "name": "Anna"}
{"user_id": 10, "name": "Paul"}
{"user_id": 11, "name": "Steve"}
{"user_id": 12, "name": "Sarah"}"""

LOCAL_BACKEND_NAME = "local"
GLUE_BACKEND_NAME = "glue"


def _get_catalog(backend: str, s3_path: str | None = None):
    if backend == LOCAL_BACKEND_NAME:
        return pw.io.iceberg.RestCatalog(
            uri=os.environ.get("ICEBERG_CATALOG_URI", "http://iceberg:8181"),
        )
    elif backend == GLUE_BACKEND_NAME:
        assert s3_path is not None, "s3_path is required for Glue catalog"
        return pw.io.iceberg.GlueCatalog(
            warehouse=s3_path,
            aws_settings=get_aws_s3_settings(),
        )
    else:
        raise ValueError(f"unknown backend: '{backend}'")


INPUT_CONTENTS = {
    1: INPUT_CONTENTS_1,
    2: INPUT_CONTENTS_2,
    3: INPUT_CONTENTS_3,
}


GLUE_CATALOG = GlueCatalog(
    name="default",
    **{"glue.region": "eu-central-1"},
)


def _get_pandas_table(backend: str, table_name: str) -> pd.DataFrame:
    if backend == LOCAL_BACKEND_NAME:
        catalog = load_catalog(
            name="default",
            uri=os.environ.get("ICEBERG_CATALOG_URI", "http://iceberg:8181"),
        )
        iceberg_table = catalog.load_table(table_name)
    elif backend == GLUE_BACKEND_NAME:
        iceberg_table = GLUE_CATALOG.load_table(table_name)
    else:
        raise ValueError(f"unknown backend: '{backend}'")

    scan = iceberg_table.scan()
    return scan.to_pandas()


class IcebergEntriesCountChecker:
    def __init__(self, backend: str, table_name: str, expected_count: int):
        self.backend = backend
        self.table_name = table_name
        self.expected_count = expected_count

    def __call__(self) -> bool:
        try:
            table = _get_pandas_table(self.backend, self.table_name)
            return len(table) == self.expected_count
        except Exception:
            return False

    def provide_information_on_failure(self) -> str:
        try:
            table = _get_pandas_table(self.backend, self.table_name)
            unique_keys = ", ".join(table["user_id"].astype(str))
            failure_report_parts = [
                f"Table length is: {len(table)}.",
                f"Expected count is {self.expected_count}.",
                f"Keys: {unique_keys}.",
            ]
            return " ".join(failure_report_parts)
        except Exception as e:
            return f"Failed to read the table. Exception: {e}"


@pytest.mark.parametrize("backend", [LOCAL_BACKEND_NAME, GLUE_BACKEND_NAME])
def test_iceberg_read_after_write(backend, tmp_path, s3_path):
    input_path = tmp_path / "input.txt"
    output_path = tmp_path / "output.txt"
    pstorage_path = tmp_path / "pstorage"
    full_s3_path = f"s3://aws-integrationtest/{s3_path}"
    table_name = uuid.uuid4().hex
    namespace = uuid.uuid4().hex

    def run_single_iteration(seq_number: int):
        input_path.write_text(INPUT_CONTENTS[seq_number])
        name_for_id_from_new_part = {}
        for input_line in INPUT_CONTENTS[seq_number].splitlines():
            data = json.loads(input_line)
            name_for_id_from_new_part[data["user_id"]] = data["name"]

        class InputSchema(pw.Schema):
            user_id: int = pw.column_definition(primary_key=True)
            name: str

        # Place some data into the Iceberg table
        table = pw.io.jsonlines.read(
            input_path,
            schema=InputSchema,
            mode="static",
        )
        pw.io.iceberg.write(
            table,
            catalog=_get_catalog(backend, s3_path=full_s3_path),
            namespace=[namespace],
            table_name=table_name,
        )
        run()

        # Read the data from the Iceberg table via Pathway
        G.clear()
        table = pw.io.iceberg.read(
            catalog=_get_catalog(backend, s3_path=full_s3_path),
            namespace=[namespace],
            table_name=table_name,
            mode="static",
            schema=InputSchema,
        )
        pw.io.jsonlines.write(table, output_path)
        persistence_config = pw.persistence.Config(
            pw.persistence.Backend.filesystem(pstorage_path),
        )
        run(persistence_config=persistence_config)

        name_for_id = {}
        with open(output_path, "r") as f:
            for line in f:
                data = json.loads(line)
                name_for_id[data["user_id"]] = data["name"]
        assert name_for_id == name_for_id_from_new_part

    for seq_number in range(1, len(INPUT_CONTENTS) + 1):
        run_single_iteration(seq_number)


@pytest.mark.flaky(reruns=5)
@pytest.mark.parametrize("backend", [LOCAL_BACKEND_NAME, GLUE_BACKEND_NAME])
def test_iceberg_several_runs(backend, tmp_path, s3_path):
    input_path = tmp_path / "input.txt"
    full_s3_path = f"s3://aws-integrationtest/{s3_path}"
    table_name = uuid.uuid4().hex
    namespace = uuid.uuid4().hex
    all_ids = set()
    all_names = set()

    def run_single_iteration(seq_number: int):
        input_path.write_text(INPUT_CONTENTS[seq_number])
        for input_line in INPUT_CONTENTS[seq_number].splitlines():
            data = json.loads(input_line)
            all_ids.add(data["user_id"])
            all_names.add(data["name"])

        class InputSchema(pw.Schema):
            user_id: int
            name: str

        G.clear()
        table = pw.io.jsonlines.read(
            input_path,
            schema=InputSchema,
            mode="static",
        )
        pw.io.iceberg.write(
            table,
            catalog=_get_catalog(backend, s3_path=full_s3_path),
            namespace=[namespace],
            table_name=table_name,
        )
        run()

        pandas_table = _get_pandas_table(backend, f"{namespace}.{table_name}")
        assert pandas_table.shape == (4 * seq_number, 4)
        assert set(pandas_table["user_id"]) == all_ids
        assert set(pandas_table["name"]) == all_names
        assert set(pandas_table["diff"]) == {1}
        assert len(set(pandas_table["time"])) == seq_number

    # The first run includes the table creation.
    # The second and the following runs check that the case with the created table also works correctly.
    for seq_number in range(1, len(INPUT_CONTENTS) + 1):
        run_single_iteration(seq_number)


@pytest.mark.parametrize("backend", [LOCAL_BACKEND_NAME, GLUE_BACKEND_NAME])
def test_iceberg_streaming(backend, tmp_path, s3_path):
    inputs_path = tmp_path / "inputs"
    output_path = tmp_path / "output.jsonl"
    full_s3_path = f"s3://aws-integrationtest/{s3_path}"
    inputs_path.mkdir()
    table_name = uuid.uuid4().hex
    namespace = uuid.uuid4().hex

    def stream_inputs():
        for i in range(1, len(INPUT_CONTENTS) + 1):
            input_path = inputs_path / f"{i}.txt"
            input_path.write_text(INPUT_CONTENTS[i])
            wait_result_with_checker(
                FileLinesNumberChecker(output_path, 4 * i),
                30,
                target=None,
            )

    class InputSchema(pw.Schema):
        user_id: int
        name: str

    table = pw.io.jsonlines.read(
        inputs_path,
        schema=InputSchema,
        mode="streaming",
    )
    # It's much quicker to check the file to make sure entries are ingested,
    # rather than to query the catalog
    pw.io.jsonlines.write(table, output_path)
    pw.io.iceberg.write(
        table,
        catalog=_get_catalog(backend, s3_path=full_s3_path),
        namespace=[namespace],
        table_name=table_name,
        min_commit_frequency=1000,
    )

    t = threading.Thread(target=stream_inputs)
    t.start()
    wait_result_with_checker(
        IcebergEntriesCountChecker(
            backend, f"{namespace}.{table_name}", 4 * len(INPUT_CONTENTS)
        ),
        timeout_sec=100,
    )

    all_ids = set()
    all_names = set()
    for i in range(1, len(INPUT_CONTENTS) + 1):
        for input_line in INPUT_CONTENTS[i].splitlines():
            data = json.loads(input_line)
            all_ids.add(data["user_id"])
            all_names.add(data["name"])

    pandas_table = _get_pandas_table(backend, f"{namespace}.{table_name}")
    assert pandas_table.shape == (4 * len(INPUT_CONTENTS), 4)
    assert set(pandas_table["user_id"]) == all_ids
    assert set(pandas_table["name"]) == all_names
    assert set(pandas_table["diff"]) == {1}
    assert len(set(pandas_table["time"])) == len(INPUT_CONTENTS)


@pytest.mark.parametrize("backend", [LOCAL_BACKEND_NAME, GLUE_BACKEND_NAME])
def test_py_object_wrapper_in_iceberg(backend, tmp_path, s3_path):
    input_path = tmp_path / "input.jsonl"
    output_path = tmp_path / "output.jsonl"
    full_s3_path = f"s3://aws-integrationtest/{s3_path}"
    table_name = uuid.uuid4().hex
    namespace = uuid.uuid4().hex
    input_path.write_text("test")

    table = pw.io.plaintext.read(input_path, mode="static")
    table = table.select(
        data=pw.this.data,
        fun=pw.wrap_py_object(len, serializer=pickle),  # type: ignore
    )
    pw.io.iceberg.write(
        table,
        catalog=_get_catalog(backend, s3_path=full_s3_path),
        namespace=[namespace],
        table_name=table_name,
    )
    run()
    G.clear()

    class InputSchema(pw.Schema):
        data: str = pw.column_definition(primary_key=True)
        fun: pw.PyObjectWrapper

    @pw.udf
    def use_python_object(a: pw.PyObjectWrapper, x: str) -> int:
        return a.value(x)

    table = pw.io.iceberg.read(
        catalog=_get_catalog(backend, s3_path=full_s3_path),
        namespace=[namespace],
        table_name=table_name,
        mode="static",
        schema=InputSchema,
    )
    table = table.select(len=use_python_object(pw.this.fun, pw.this.data))
    pw.io.jsonlines.write(table, output_path)
    run()

    with open(output_path, "r") as f:
        data = json.load(f)
        assert data["len"] == 4


@pytest.mark.parametrize("backend", [LOCAL_BACKEND_NAME, GLUE_BACKEND_NAME])
def test_iceberg_different_types_serialization(backend, tmp_path, s3_path):
    input_path = tmp_path / "input.jsonl"
    full_s3_path = f"s3://aws-integrationtest/{s3_path}"
    table_name = uuid.uuid4().hex
    namespace = uuid.uuid4().hex
    input_path.write_text("test")

    column_values = {
        "boolean": True,
        "integer": 123,
        "double": -5.6,
        "string": "abcdef",
        "binary_data": b"fedcba",
        "datetime_naive": pw.DateTimeNaive(year=2025, month=1, day=17),
        "duration": pw.Duration(days=5),
        "json_data": pw.Json.parse('{"a": 15, "b": "hello"}'),
    }
    # Glue/Hive has no type that preserves timezone on read-back; iceberg-rust 0.8+
    # rejects Timestamptz/TimestamptzNs at write time instead of silently dropping the
    # zone (apache/iceberg-rust#1925). Exercise the tz-aware roundtrip only on REST.
    if backend != GLUE_BACKEND_NAME:
        column_values["datetime_utc_aware"] = pw.DateTimeUtc(
            year=2025, month=1, day=17, tz=tz.UTC
        )
    table = pw.io.plaintext.read(input_path, mode="static")
    table = table.select(
        data=pw.this.data,
        **column_values,
    )
    pw.io.iceberg.write(
        table,
        catalog=_get_catalog(backend, s3_path=full_s3_path),
        namespace=[namespace],
        table_name=table_name,
        timestamp_unit="ns" if backend == GLUE_BACKEND_NAME else "us",
    )
    run()
    G.clear()

    class InputSchemaBase(pw.Schema):
        data: str = pw.column_definition(primary_key=True)
        boolean: bool
        integer: int
        double: float
        string: str
        binary_data: bytes
        datetime_naive: pw.DateTimeNaive
        duration: pw.Duration
        json_data: pw.Json

    if backend == GLUE_BACKEND_NAME:
        InputSchema = InputSchemaBase
    else:

        class InputSchema(InputSchemaBase):  # type: ignore[no-redef]
            datetime_utc_aware: pw.DateTimeUtc

    class Checker:
        def __init__(self):
            self.n_processed_rows = 0

        def __call__(self, key, row, time, is_addition):
            self.n_processed_rows += 1
            for field, expected_value in column_values.items():
                assert row[field] == expected_value

    table = pw.io.iceberg.read(
        catalog=_get_catalog(backend, s3_path=full_s3_path),
        namespace=[namespace],
        table_name=table_name,
        mode="static",
        schema=InputSchema,
    )
    checker = Checker()
    pw.io.subscribe(table, on_change=checker)
    run()
    assert checker.n_processed_rows == 1


@pytest.mark.parametrize("backend", [LOCAL_BACKEND_NAME, GLUE_BACKEND_NAME])
def test_iceberg_static_read_empty_table(backend, tmp_path, s3_path):
    """A static-mode read on a freshly created (empty) Iceberg table must
    finish promptly with zero rows. The docs say static mode "will only
    consider the available data and ingest all of it in one commit"; for an
    empty table the "available data" set is just empty.
    """
    input_path = tmp_path / "input.txt"
    output_path = tmp_path / "output.jsonl"
    full_s3_path = f"s3://aws-integrationtest/{s3_path}"
    table_name = uuid.uuid4().hex
    namespace = uuid.uuid4().hex

    class InputSchema(pw.Schema):
        user_id: int = pw.column_definition(primary_key=True)
        name: str

    # Empty input creates the table (writer's ensure_table runs at sink
    # construction) but commits no data files, so the table has no current
    # snapshot.
    input_path.write_text("")
    table = pw.io.jsonlines.read(input_path, schema=InputSchema, mode="static")
    pw.io.iceberg.write(
        table,
        catalog=_get_catalog(backend, s3_path=full_s3_path),
        namespace=[namespace],
        table_name=table_name,
    )
    run()
    G.clear()

    table = pw.io.iceberg.read(
        catalog=_get_catalog(backend, s3_path=full_s3_path),
        namespace=[namespace],
        table_name=table_name,
        mode="static",
        schema=InputSchema,
    )
    pw.io.jsonlines.write(table, output_path)
    # Run pw.run() in a forked subprocess so a hang shows up as a stuck
    # process instead of a stuck pytest worker. wait_result_with_checker is
    # not strict enough here: the FileSystem writer flushes the (empty)
    # output at startup, so any line-count checker would already report
    # success while the iceberg reader thread is still spinning.
    ctx = multiprocessing.get_context("fork")
    proc = ctx.Process(target=run)
    proc.start()
    try:
        proc.join(timeout=20)
        is_alive = proc.is_alive()
    finally:
        if proc.is_alive():
            proc.terminate()
            proc.join()
    assert not is_alive, (
        "pw.run() did not finish in time: static read of an empty "
        "Iceberg table hangs."
    )
    assert proc.exitcode == 0, f"pw.run() failed: exitcode={proc.exitcode}"
    if output_path.exists():
        assert output_path.read_text() == ""


def _count_iceberg_snapshots(backend: str, qualified_name: str) -> int:
    if backend == LOCAL_BACKEND_NAME:
        catalog = load_catalog(
            name="default",
            uri=os.environ.get("ICEBERG_CATALOG_URI", "http://iceberg:8181"),
        )
        iceberg_table = catalog.load_table(qualified_name)
    elif backend == GLUE_BACKEND_NAME:
        iceberg_table = GLUE_CATALOG.load_table(qualified_name)
    else:
        raise ValueError(f"unknown backend: '{backend}'")
    return len(iceberg_table.metadata.snapshots)


@pytest.mark.parametrize("backend", [LOCAL_BACKEND_NAME, GLUE_BACKEND_NAME])
def test_iceberg_min_commit_frequency_rate_limits_after_first_window(
    backend, tmp_path, s3_path
):
    """The docstring states min_commit_frequency is "the minimum time
    interval between two data commits in storage". It must therefore
    rate-limit commits across the entire run, not only the initial window.

    Streaming small minibatches faster than the configured interval should
    produce roughly one snapshot per interval, not one snapshot per
    minibatch.
    """
    inputs_dir = tmp_path / "inputs"
    output_path = tmp_path / "output.jsonl"
    inputs_dir.mkdir()
    full_s3_path = f"s3://aws-integrationtest/{s3_path}"
    table_name = uuid.uuid4().hex
    namespace = uuid.uuid4().hex

    n_files = 24
    file_period_sec = 0.5
    min_commit_frequency_ms = 5_000

    def stream_inputs():
        for i in range(1, n_files + 1):
            (inputs_dir / f"{i}.txt").write_text(
                json.dumps({"user_id": i, "name": f"u{i}"})
            )
            time.sleep(file_period_sec)

    class InputSchema(pw.Schema):
        user_id: int
        name: str

    table = pw.io.jsonlines.read(
        inputs_dir,
        schema=InputSchema,
        mode="streaming",
        autocommit_duration_ms=200,
    )
    pw.io.jsonlines.write(table, output_path)
    pw.io.iceberg.write(
        table,
        catalog=_get_catalog(backend, s3_path=full_s3_path),
        namespace=[namespace],
        table_name=table_name,
        min_commit_frequency=min_commit_frequency_ms,
    )

    t = threading.Thread(target=stream_inputs)
    t.start()
    wait_result_with_checker(
        FileLinesNumberChecker(output_path, n_files),
        timeout_sec=int(n_files * file_period_sec) + 10,
    )

    snapshots = _count_iceberg_snapshots(backend, f"{namespace}.{table_name}")
    expected_runtime_sec = n_files * file_period_sec
    # Floor of how many `min_commit_frequency` intervals fit in the run,
    # plus 1 for the leading partial interval, plus 1 slack for any commit
    # at shutdown.
    max_expected_snapshots = (
        int(expected_runtime_sec * 1000 / min_commit_frequency_ms) + 2
    )
    assert snapshots <= max_expected_snapshots, (
        f"Expected at most {max_expected_snapshots} snapshots in "
        f"{expected_runtime_sec:.0f}s with min_commit_frequency="
        f"{min_commit_frequency_ms}ms, observed {snapshots}. "
        "min_commit_frequency only rate-limits the first commit; subsequent "
        "commits are not throttled."
    )


# ----------------------------------------------------------------------------
# Primitive-type tests for the Iceberg writer's existing-table reconciliation
# (`int`, `float`, `decimal`, `uuid`, `fixed`) and the reader's expanded arrow
# coverage (`date`, `uuid`, `fixed`).
#
# All currently parameterized on the local backend only — the type-handling
# code paths are backend-agnostic, so `tabulario/iceberg-rest` exercises the
# full plumbing without burning Glue / S3 quota.
# ----------------------------------------------------------------------------


def _local_catalog():
    return load_catalog(
        name="default",
        uri=os.environ.get("ICEBERG_CATALOG_URI", "http://iceberg:8181"),
    )


def _retry_catalog_op(op, *, attempts: int = 5, base_delay: float = 0.2):
    """Run a local-catalog mutation, retrying transient server errors with
    exponential backoff. The `tabulario/iceberg-rest` backend (a SQLite-backed
    JDBC catalog) intermittently answers ``500 ServerError``
    (`UncheckedSQLException: Unknown failure`) when many parallel test workers
    create namespaces/tables at once. These are not schema problems — the same
    request succeeds on retry — so we only swallow `ServerError` and let every
    other exception (e.g. a genuinely invalid schema) propagate immediately."""
    for attempt in range(attempts):
        try:
            return op()
        except ServerError:
            if attempt == attempts - 1:
                raise
            time.sleep(base_delay * (2**attempt))


def _create_table_with_schema(namespace: str, table_name: str, schema: PyIcebergSchema):
    """Pre-create an Iceberg table on the local catalog with a specific schema,
    so Pathway will hit the existing-table reconciliation path on write."""
    catalog = _local_catalog()
    _retry_catalog_op(lambda: catalog.create_namespace(namespace))
    _retry_catalog_op(lambda: catalog.create_table(f"{namespace}.{table_name}", schema))


def _input_value_field_id_offset() -> int:
    """Field-id offset for Pathway-added `time`/`diff` columns. Schema field ids
    must be unique within an Iceberg schema; we leave room for them after the
    user columns by starting from a known base."""
    return 100


def _existing_table_schema(*user_fields: NestedField) -> PyIcebergSchema:
    """Wrap the caller's column field(s) with the Pathway-added `time`/`diff`
    metadata columns. Their field ids start past `_input_value_field_id_offset`
    so they never collide with the user field ids (which the caller numbers from
    1 upward, including the primary-key column)."""
    offset = _input_value_field_id_offset()
    return PyIcebergSchema(
        *user_fields,
        NestedField(offset + 1, "time", LongType(), required=False),
        NestedField(offset + 2, "diff", LongType(), required=False),
    )


def _register_single_row_write(tmp_path, namespace: str, table_name: str, **columns):
    """Register a one-row Iceberg write into `namespace.table_name`. The row holds
    `data="test"` plus the given columns, which may be literals (`i32=42`) or
    Pathway expressions (`payload=pw.apply_with_type(...)`). Does not call `run()`,
    so error tests can wrap the run in `pytest.raises`."""
    input_path = tmp_path / "input.txt"
    input_path.write_text("test")
    table = pw.io.plaintext.read(input_path, mode="static")
    table = table.select(data=pw.this.data, **columns)
    pw.io.iceberg.write(
        table,
        catalog=_get_catalog(LOCAL_BACKEND_NAME),
        namespace=[namespace],
        table_name=table_name,
    )


def _read_back_column(namespace: str, table_name: str, schema, column: str) -> list:
    """Read `namespace.table_name` back in static mode and return the values
    observed for `column` (one entry per row, in addition order)."""
    table = pw.io.iceberg.read(
        catalog=_get_catalog(LOCAL_BACKEND_NAME),
        namespace=[namespace],
        table_name=table_name,
        mode="static",
        schema=schema,
    )
    seen: list = []
    pw.io.subscribe(
        table,
        on_change=lambda key, row, time, is_addition: seen.append(row[column]),
    )
    run()
    return seen


def _run_expecting_error(*required_substrings: str) -> str:
    """Run the graph, assert it raises, and that the error message contains every
    `required_substrings` entry. Returns the message so callers can add their own
    (e.g. case-insensitive or OR-style) assertions."""
    with pytest.raises(Exception) as excinfo:
        run()
    msg = str(excinfo.value)
    for substring in required_substrings:
        assert substring in msg, f"expected {substring!r} in error message, got: {msg}"
    return msg


def test_iceberg_write_int32_existing_column(tmp_path):
    namespace = uuid.uuid4().hex
    table_name = uuid.uuid4().hex
    _create_table_with_schema(
        namespace,
        table_name,
        _existing_table_schema(
            NestedField(1, "data", StringType(), required=True),
            NestedField(2, "i32", IntegerType(), required=False),
        ),
    )
    _register_single_row_write(tmp_path, namespace, table_name, i32=42)
    run()

    pdf = _get_pandas_table(LOCAL_BACKEND_NAME, f"{namespace}.{table_name}")
    assert list(pdf["i32"]) == [42]
    # The arrow type roundtrip should preserve int32 — pyarrow surfaces this as
    # int32 dtype, whereas a regular Pathway int column would surface as int64.
    assert pdf["i32"].dtype.name == "int32"


def test_iceberg_write_int32_overflow_errors(tmp_path):
    namespace = uuid.uuid4().hex
    table_name = uuid.uuid4().hex
    _create_table_with_schema(
        namespace,
        table_name,
        _existing_table_schema(
            NestedField(1, "data", StringType(), required=True),
            NestedField(2, "i32", IntegerType(), required=False),
        ),
    )
    # 2^33 — fits in i64 but not i32; the writer must refuse rather than truncate.
    _register_single_row_write(tmp_path, namespace, table_name, i32=1 << 33)
    msg = _run_expecting_error()
    assert (
        "does not fit" in msg or "Int32" in msg
    ), f"expected an integer-narrowing-overflow error, got: {msg}"


def test_iceberg_write_float32_existing_column(tmp_path):
    namespace = uuid.uuid4().hex
    table_name = uuid.uuid4().hex
    _create_table_with_schema(
        namespace,
        table_name,
        _existing_table_schema(
            NestedField(1, "data", StringType(), required=True),
            NestedField(2, "f32", FloatType(), required=False),
        ),
    )
    _register_single_row_write(tmp_path, namespace, table_name, f32=1.5)
    run()

    pdf = _get_pandas_table(LOCAL_BACKEND_NAME, f"{namespace}.{table_name}")
    assert pdf["f32"].dtype.name == "float32"
    assert pytest.approx(float(pdf["f32"][0]), rel=1e-6) == 1.5


def test_iceberg_decimal_roundtrip_via_string(tmp_path):
    """Pre-existing decimal column + Pathway str → write encodes the string as
    decimal, read back as str preserves the exact textual value."""
    namespace = uuid.uuid4().hex
    table_name = uuid.uuid4().hex
    _create_table_with_schema(
        namespace,
        table_name,
        _existing_table_schema(
            NestedField(1, "data", StringType(), required=True),
            NestedField(
                2, "amount", DecimalType(precision=10, scale=2), required=False
            ),
        ),
    )
    _register_single_row_write(tmp_path, namespace, table_name, amount="123.45")
    run()
    G.clear()

    # Read back via Pathway as str → expect lossless decimal text.
    class InputSchema(pw.Schema):
        data: str = pw.column_definition(primary_key=True)
        amount: str

    seen = _read_back_column(namespace, table_name, InputSchema, "amount")
    assert seen == ["123.45"]


def test_iceberg_uuid_roundtrip_via_string(tmp_path):
    namespace = uuid.uuid4().hex
    table_name = uuid.uuid4().hex
    _create_table_with_schema(
        namespace,
        table_name,
        _existing_table_schema(
            NestedField(1, "data", StringType(), required=True),
            NestedField(2, "uid", UUIDType(), required=False),
        ),
    )
    user_uuid = "550e8400-e29b-41d4-a716-446655440000"
    _register_single_row_write(tmp_path, namespace, table_name, uid=user_uuid)
    run()
    G.clear()

    class InputSchema(pw.Schema):
        data: str = pw.column_definition(primary_key=True)
        uid: str

    seen = _read_back_column(namespace, table_name, InputSchema, "uid")
    assert seen == [user_uuid]


def test_iceberg_uuid_invalid_string_errors(tmp_path):
    namespace = uuid.uuid4().hex
    table_name = uuid.uuid4().hex
    _create_table_with_schema(
        namespace,
        table_name,
        _existing_table_schema(
            NestedField(1, "data", StringType(), required=True),
            NestedField(2, "uid", UUIDType(), required=False),
        ),
    )
    _register_single_row_write(tmp_path, namespace, table_name, uid="not-a-uuid")
    msg = _run_expecting_error()
    assert "UUID" in msg or "uuid" in msg, f"expected a UUID-parse error, got: {msg}"


def test_iceberg_fixed_roundtrip_via_bytes(tmp_path):
    namespace = uuid.uuid4().hex
    table_name = uuid.uuid4().hex
    _create_table_with_schema(
        namespace,
        table_name,
        _existing_table_schema(
            NestedField(1, "data", StringType(), required=True),
            NestedField(2, "blob", FixedType(8), required=False),
        ),
    )
    payload = bytes(range(8))
    _register_single_row_write(tmp_path, namespace, table_name, blob=payload)
    run()
    G.clear()

    class InputSchema(pw.Schema):
        data: str = pw.column_definition(primary_key=True)
        blob: bytes

    seen = _read_back_column(namespace, table_name, InputSchema, "blob")
    assert seen == [payload]


def test_iceberg_fixed_wrong_length_errors(tmp_path):
    namespace = uuid.uuid4().hex
    table_name = uuid.uuid4().hex
    _create_table_with_schema(
        namespace,
        table_name,
        _existing_table_schema(
            NestedField(1, "data", StringType(), required=True),
            NestedField(2, "blob", FixedType(8), required=False),
        ),
    )
    # 3 bytes into a fixed(8) column — the writer must reject the length.
    _register_single_row_write(tmp_path, namespace, table_name, blob=b"\x01\x02\x03")
    msg = _run_expecting_error()
    assert (
        "expected" in msg or "length" in msg
    ), f"expected a length-mismatch error, got: {msg}"


def test_iceberg_read_date_as_datetime_naive(tmp_path):
    """Pre-create a table with an Iceberg `date` column and a row, read with
    Pathway DateTimeNaive — value should be the calendar day at midnight."""
    namespace = uuid.uuid4().hex
    table_name = uuid.uuid4().hex
    _create_table_with_schema(
        namespace,
        table_name,
        PyIcebergSchema(
            NestedField(1, "id", IntegerType(), required=True),
            NestedField(2, "d", DateType(), required=False),
        ),
    )
    # Insert a row via pyarrow / pyiceberg directly (Pathway can't write `date`
    # since it has no native date-only type and we deliberately don't add one).
    import pyarrow as pa

    catalog = _local_catalog()
    table = catalog.load_table(f"{namespace}.{table_name}")
    arrow_table = pa.table(
        {
            "id": pa.array([1], type=pa.int32()),
            "d": pa.array([datetime.date(2025, 1, 17)], type=pa.date32()),
        },
        schema=table.schema().as_arrow(),
    )
    table.append(arrow_table)

    class InputSchema(pw.Schema):
        id: int = pw.column_definition(primary_key=True)
        d: pw.DateTimeNaive

    seen = _read_back_column(namespace, table_name, InputSchema, "d")
    assert seen == [pw.DateTimeNaive(year=2025, month=1, day=17)]


def test_iceberg_time_roundtrip_via_duration(tmp_path):
    """Iceberg `time` is microseconds since midnight; Pathway expresses it as
    Duration (same convention as the Postgres TIME mapping). Roundtrip both
    write (Pathway Duration → Iceberg time) and read (Iceberg time → Pathway
    Duration)."""
    namespace = uuid.uuid4().hex
    table_name = uuid.uuid4().hex
    _create_table_with_schema(
        namespace,
        table_name,
        _existing_table_schema(
            NestedField(1, "data", StringType(), required=True),
            NestedField(2, "tod", TimeType(), required=False),
        ),
    )
    # 12:34:56.789012 — pick a non-trivial duration so we'd notice ms/us mixups.
    payload = pw.Duration(hours=12, minutes=34, seconds=56, microseconds=789_012)
    _register_single_row_write(tmp_path, namespace, table_name, tod=payload)
    run()
    G.clear()

    class InputSchema(pw.Schema):
        data: str = pw.column_definition(primary_key=True)
        tod: pw.Duration

    seen = _read_back_column(namespace, table_name, InputSchema, "tod")
    assert seen == [payload]


def test_iceberg_time_wrong_pathway_type_errors(tmp_path):
    """Existing `time` column + non-Duration value (e.g. plain int) → clear
    type-mismatch error rather than silent coercion."""
    namespace = uuid.uuid4().hex
    table_name = uuid.uuid4().hex
    _create_table_with_schema(
        namespace,
        table_name,
        _existing_table_schema(
            NestedField(1, "data", StringType(), required=True),
            NestedField(2, "tod", TimeType(), required=False),
        ),
    )
    # Pathway `int`, not `Duration`. The override map only kicks in for
    # Duration, so the writer falls back to the default type-mapping for `int`,
    # producing Arrow `Int64`. The parquet write then refuses Int64 vs Time64.
    _register_single_row_write(tmp_path, namespace, table_name, tod=42)
    msg = _run_expecting_error()
    # With the writer preflight in place this surfaces as an
    # `IcebergColumnTypeMismatch` naming the column, the Pathway type and
    # the iceberg type. Older versions returned the arrow-level
    # "Time64 / Int64 / Incompatible" stack — still accepted to keep the
    # check backward-compatible with a wider range of error wordings.
    assert (
        "tod" in msg or "Time64" in msg or "Int64" in msg or "Incompatible" in msg
    ), f"expected a clear schema-mismatch error, got: {msg}"


def test_iceberg_tuple_new_table_roundtrip(tmp_path):
    """Pathway-created table with a `tuple[int, str]` column. Pathway emits
    the column as Iceberg `struct<[0]: long, [1]: string>` and reads it back
    as a Pathway tuple. Field IDs and names are entirely under our control,
    so the round-trip should be exact."""
    namespace = uuid.uuid4().hex
    table_name = uuid.uuid4().hex
    # Plain Python literal `(7, "hello")` would be inferred as `tuple[Any, Any]`,
    # which the iceberg writer rejects. Force the element types with
    # `apply_with_type`.
    _register_single_row_write(
        tmp_path,
        namespace,
        table_name,
        payload=pw.apply_with_type(
            lambda _d: (7, "hello"), tuple[int, str], pw.this.data
        ),
    )
    run()
    G.clear()

    class InputSchema(pw.Schema):
        data: str = pw.column_definition(primary_key=True)
        payload: tuple[int, str]

    seen = _read_back_column(namespace, table_name, InputSchema, "payload")
    assert seen == [(7, "hello")]


def test_iceberg_tuple_existing_struct_column_roundtrip(tmp_path):
    """Existing iceberg `struct<a: long, b: string>` column + Pathway
    `tuple[int, str]`: the writer's reconciliation override pulls the
    existing struct's *names* (`a`, `b`) into the arrow batch so the
    parquet write stays aligned with the table's field IDs. Tuple position
    0 binds to field `a`, position 1 to `b`."""
    namespace = uuid.uuid4().hex
    table_name = uuid.uuid4().hex
    _create_table_with_schema(
        namespace,
        table_name,
        _existing_table_schema(
            NestedField(1, "data", StringType(), required=True),
            NestedField(
                2,
                "payload",
                StructType(
                    NestedField(3, "a", LongType(), required=False),
                    NestedField(4, "b", StringType(), required=False),
                ),
                required=False,
            ),
        ),
    )
    _register_single_row_write(
        tmp_path,
        namespace,
        table_name,
        payload=pw.apply_with_type(
            lambda _d: (42, "world"), tuple[int, str], pw.this.data
        ),
    )
    run()
    G.clear()

    class InputSchema(pw.Schema):
        data: str = pw.column_definition(primary_key=True)
        payload: tuple[int, str]

    seen = _read_back_column(namespace, table_name, InputSchema, "payload")
    assert seen == [(42, "world")]


def test_iceberg_tuple_nested_in_list(tmp_path):
    """Recursion check: `list[tuple[int, str]]`. Tests that the type
    machinery composes through list element types."""
    namespace = uuid.uuid4().hex
    table_name = uuid.uuid4().hex
    _register_single_row_write(
        tmp_path,
        namespace,
        table_name,
        rows=pw.apply_with_type(
            lambda _d: [(1, "a"), (2, "b"), (3, "c")],
            list[tuple[int, str]],
            pw.this.data,
        ),
    )
    run()
    G.clear()

    class InputSchema(pw.Schema):
        data: str = pw.column_definition(primary_key=True)
        rows: list[tuple[int, str]]

    seen = _read_back_column(namespace, table_name, InputSchema, "rows")
    assert len(seen) == 1
    # `pw.io.subscribe` materializes the list as a Python tuple of tuples.
    assert tuple(seen[0]) == ((1, "a"), (2, "b"), (3, "c"))


def test_iceberg_tuple_with_inner_narrow_types(tmp_path):
    """Existing iceberg `struct<i32: int, f32: float, dec: decimal(10,2),
    uid: uuid, fix: fixed(8), tod: time>` + Pathway
    `tuple[int, float, str, str, bytes, Duration]`. Exercises that the
    existing-column reconciliation override propagates **recursively** into
    nested struct fields: each inner Iceberg type's arrow shape arrives at
    `array_for_type` via the override, and the Pathway value at the matching
    tuple position must convert into that arrow type the same way as a
    top-level column would (int32 narrowing, decimal text → unscaled i128,
    UUID hex parse, length-checked bytes, Duration → Time64(µs))."""
    namespace = uuid.uuid4().hex
    table_name = uuid.uuid4().hex
    _create_table_with_schema(
        namespace,
        table_name,
        _existing_table_schema(
            NestedField(1, "data", StringType(), required=True),
            NestedField(
                2,
                "payload",
                StructType(
                    NestedField(3, "i32", IntegerType(), required=False),
                    NestedField(4, "f32", FloatType(), required=False),
                    NestedField(
                        5, "dec", DecimalType(precision=10, scale=2), required=False
                    ),
                    NestedField(6, "uid", UUIDType(), required=False),
                    NestedField(7, "fix", FixedType(8), required=False),
                    NestedField(8, "tod", TimeType(), required=False),
                ),
                required=False,
            ),
        ),
    )
    user_uuid = "550e8400-e29b-41d4-a716-446655440000"
    fix_payload = bytes(range(1, 9))
    tod_payload = pw.Duration(hours=12, minutes=34, seconds=56, microseconds=789_012)
    payload_value = (42, 1.5, "123.45", user_uuid, fix_payload, tod_payload)

    _register_single_row_write(
        tmp_path,
        namespace,
        table_name,
        payload=pw.apply_with_type(
            lambda _d: payload_value,
            tuple[int, float, str, str, bytes, pw.Duration],
            pw.this.data,
        ),
    )
    run()
    G.clear()

    class InputSchema(pw.Schema):
        data: str = pw.column_definition(primary_key=True)
        payload: tuple[int, float, str, str, bytes, pw.Duration]

    seen = _read_back_column(namespace, table_name, InputSchema, "payload")
    assert len(seen) == 1
    roundtrip = seen[0]
    assert roundtrip[0] == 42
    # Float32 cast is lossy in general but 1.5 is exactly representable.
    assert roundtrip[1] == pytest.approx(1.5, rel=1e-6)
    assert roundtrip[2] == "123.45"
    assert roundtrip[3] == user_uuid
    assert roundtrip[4] == fix_payload
    assert roundtrip[5] == tod_payload


def test_iceberg_write_date_existing_column_truncates_time_of_day(tmp_path):
    """Iceberg `date` ↔ Pathway `DateTimeNaive`: writes drop the time-of-day
    component silently (Pathway has no date-only type, mirrors the Postgres
    DATE convention). Verify a non-midnight timestamp lands on the correct
    calendar day."""
    namespace = uuid.uuid4().hex
    table_name = uuid.uuid4().hex
    _create_table_with_schema(
        namespace,
        table_name,
        _existing_table_schema(
            NestedField(1, "data", StringType(), required=True),
            NestedField(2, "d", DateType(), required=False),
        ),
    )
    # Pick mid-day on purpose — the assertion checks that the time-of-day part
    # is dropped on write.
    payload = pw.DateTimeNaive(
        year=2025, month=1, day=17, hour=14, minute=30, second=42
    )
    _register_single_row_write(tmp_path, namespace, table_name, d=payload)
    run()
    G.clear()

    class InputSchema(pw.Schema):
        data: str = pw.column_definition(primary_key=True)
        d: pw.DateTimeNaive

    seen = _read_back_column(namespace, table_name, InputSchema, "d")
    assert len(seen) == 1
    # Time-of-day silently dropped: 14:30:42 → 00:00:00 on the same day.
    assert seen[0] == pw.DateTimeNaive(year=2025, month=1, day=17)


def test_iceberg_ndarray_roundtrip(tmp_path):
    """Pathway `np.ndarray` is encoded as Iceberg
    `struct<shape: list<long>, elements: list<…>>`, the same shape Delta uses.
    Round-trip a 2-D int array end-to-end through Pathway-created table."""
    import numpy as np

    namespace = uuid.uuid4().hex
    table_name = uuid.uuid4().hex
    payload = np.array([[1, 2, 3], [4, 5, 6]], dtype=np.int64)
    _register_single_row_write(
        tmp_path,
        namespace,
        table_name,
        arr=pw.apply_with_type(
            lambda _d: payload,
            np.ndarray[typing.Any, int],  # type: ignore[type-var]
            pw.this.data,
        ),
    )
    run()
    G.clear()

    class InputSchema(pw.Schema):
        data: str = pw.column_definition(primary_key=True)
        arr: np.ndarray[typing.Any, int]  # type: ignore[type-var]

    seen = _read_back_column(namespace, table_name, InputSchema, "arr")
    assert len(seen) == 1
    np.testing.assert_array_equal(seen[0], payload)


def test_iceberg_ndarray_float_roundtrip(tmp_path):
    """Same as above but with float elements — exercises the
    `list<double>` element-list branch."""
    import numpy as np

    namespace = uuid.uuid4().hex
    table_name = uuid.uuid4().hex
    payload = np.array([[1.5, 2.5], [3.5, 4.5], [5.5, 6.5]], dtype=np.float64)
    _register_single_row_write(
        tmp_path,
        namespace,
        table_name,
        arr=pw.apply_with_type(
            lambda _d: payload,
            np.ndarray[typing.Any, float],  # type: ignore[type-var]
            pw.this.data,
        ),
    )
    run()
    G.clear()

    class InputSchema(pw.Schema):
        data: str = pw.column_definition(primary_key=True)
        arr: np.ndarray[typing.Any, float]  # type: ignore[type-var]

    seen = _read_back_column(namespace, table_name, InputSchema, "arr")
    assert len(seen) == 1
    np.testing.assert_array_equal(seen[0], payload)


def test_iceberg_ndarray_existing_struct_column_roundtrip(tmp_path):
    """User pre-creates the iceberg table with the same
    `struct<shape: list<long>, elements: list<long>>` shape Pathway would emit;
    Pathway then writes an `np.ndarray` into it through the existing-column
    path (not the new-table emit). Field-name matching pulls the destination's
    `shape` / `elements` names into the parquet output."""
    import numpy as np
    from pyiceberg.types import ListType

    namespace = uuid.uuid4().hex
    table_name = uuid.uuid4().hex
    _create_table_with_schema(
        namespace,
        table_name,
        _existing_table_schema(
            NestedField(1, "data", StringType(), required=True),
            NestedField(
                2,
                "arr",
                # Match the shape Pathway's `iceberg_type::Type::Array` emits:
                # inner list elements declared nullable (`element_required=False`)
                # because Pathway's arrow_data_type produces nullable inner-list
                # elements; otherwise iceberg-rust would refuse the batch on
                # nullability mismatch (see `iceberg.rs::Type::Array`).
                StructType(
                    # `required=True` on the shape / elements fields matches
                    # what Pathway's arrow_data_type for Type::Array emits
                    # (non-null shape and elements; the inner list element is
                    # nullable in both Pathway and our iceberg_type, which is
                    # why `element_required=False`).
                    NestedField(
                        3,
                        "shape",
                        ListType(
                            element_id=4,
                            element_type=LongType(),
                            element_required=False,
                        ),
                        required=True,
                    ),
                    NestedField(
                        5,
                        "elements",
                        ListType(
                            element_id=6,
                            element_type=LongType(),
                            element_required=False,
                        ),
                        required=True,
                    ),
                ),
                required=False,
            ),
        ),
    )
    payload = np.array([[10, 20, 30], [40, 50, 60]], dtype=np.int64)
    _register_single_row_write(
        tmp_path,
        namespace,
        table_name,
        arr=pw.apply_with_type(
            lambda _d: payload,
            np.ndarray[typing.Any, int],  # type: ignore[type-var]
            pw.this.data,
        ),
    )
    run()
    G.clear()

    class InputSchema(pw.Schema):
        data: str = pw.column_definition(primary_key=True)
        arr: np.ndarray[typing.Any, int]  # type: ignore[type-var]

    seen = _read_back_column(namespace, table_name, InputSchema, "arr")
    assert len(seen) == 1
    np.testing.assert_array_equal(seen[0], payload)


# ----------------------------------------------------------------------------
# Preflight-check tests. Each verifies that a misconfiguration surfaces at
# `pw.run()` time with a message that names the column / type involved,
# rather than as a per-row parse error or a deep-stack parquet failure.
# ----------------------------------------------------------------------------


def test_iceberg_preflight_read_column_not_in_iceberg_schema(tmp_path):
    """User's Pathway schema declares a column that doesn't exist in the
    iceberg table. The connector should refuse at startup with a message
    naming the missing column, instead of letting the parser emit a per-row
    error during reads."""
    namespace = uuid.uuid4().hex
    table_name = uuid.uuid4().hex
    _create_table_with_schema(
        namespace,
        table_name,
        _existing_table_schema(
            NestedField(1, "user_id", LongType(), required=True),
            NestedField(2, "name", StringType(), required=False),
        ),
    )
    # Pre-populate so the reader actually has rows to walk through.
    input_path = tmp_path / "input.jsonl"
    input_path.write_text('{"user_id": 1, "name": "A"}\n{"user_id": 2, "name": "B"}\n')

    class WriteSchema(pw.Schema):
        user_id: int = pw.column_definition(primary_key=True)
        name: str

    table = pw.io.jsonlines.read(input_path, schema=WriteSchema, mode="static")
    pw.io.iceberg.write(
        table,
        catalog=_get_catalog(LOCAL_BACKEND_NAME),
        namespace=[namespace],
        table_name=table_name,
    )
    run()
    G.clear()

    class InputSchema(pw.Schema):
        user_id: int = pw.column_definition(primary_key=True)
        name: str
        # `email` is NOT in the iceberg table.
        email: str

    table = pw.io.iceberg.read(
        catalog=_get_catalog(LOCAL_BACKEND_NAME),
        namespace=[namespace],
        table_name=table_name,
        mode="static",
        schema=InputSchema,
    )
    pw.io.jsonlines.write(table, tmp_path / "output.jsonl")
    msg = _run_expecting_error("email")
    # Anything that signals "this is a schema mismatch" rather than a
    # row-level parse error: the message should call out the iceberg side,
    # the schema, or say the column is not found.
    assert (
        "iceberg" in msg.lower()
        or "schema" in msg.lower()
        or "not found" in msg.lower()
    ), f"error should clearly identify the schema mismatch, got: {msg}"


def test_iceberg_preflight_writer_pathway_table_missing_required_iceberg_column(
    tmp_path,
):
    """Existing iceberg table has a required (non-null) column that the user's
    Pathway table doesn't produce. Currently this surfaces inside the parquet
    writer as an obscure arrow nullability / "field id not found" error at the
    first attempt to flush; the message doesn't name the column the user
    needs to add. Should be caught at writer construction with a message
    naming the missing column."""
    namespace = uuid.uuid4().hex
    table_name = uuid.uuid4().hex
    _create_table_with_schema(
        namespace,
        table_name,
        _existing_table_schema(
            NestedField(1, "data", StringType(), required=True),
            # `important` is required (non-null) — Pathway must supply it.
            NestedField(2, "important", LongType(), required=True),
        ),
    )
    # `important` is deliberately not produced — the writer should detect this
    # at construction time.
    _register_single_row_write(tmp_path, namespace, table_name)
    # The helper asserts the missing required column is named; here we also
    # require the message to flag that the column is required / non-null.
    msg = _run_expecting_error("important")
    assert (
        "required" in msg.lower()
        or "not-null" in msg.lower()
        or "non-null" in msg.lower()
    ), f"error should mention that the column is required / non-null, got: {msg}"


def test_iceberg_preflight_writer_struct_tuple_arity_mismatch(tmp_path):
    """Existing iceberg `struct<…>` column has 3 fields but the user's
    Pathway `tuple[…]` declares only 2 (or vice versa). Currently fails
    inside the parquet writer as an arrow / field-id error; should be
    caught at writer construction naming the column and the mismatched
    arities."""
    namespace = uuid.uuid4().hex
    table_name = uuid.uuid4().hex
    _create_table_with_schema(
        namespace,
        table_name,
        _existing_table_schema(
            NestedField(1, "data", StringType(), required=True),
            # Destination struct has 3 fields:
            NestedField(
                2,
                "payload",
                StructType(
                    NestedField(3, "a", LongType(), required=False),
                    NestedField(4, "b", StringType(), required=False),
                    NestedField(5, "c", LongType(), required=False),
                ),
                required=False,
            ),
        ),
    )
    # User declares `tuple[int, str]` — arity 2 — vs destination arity 3.
    _register_single_row_write(
        tmp_path,
        namespace,
        table_name,
        payload=pw.apply_with_type(
            lambda _d: (42, "hi"), tuple[int, str], pw.this.data
        ),
    )
    msg = _run_expecting_error("payload")
    assert (
        "2" in msg and "3" in msg
    ), f"error should mention both the user's arity (2) and the destination's arity (3), got: {msg}"


def test_iceberg_preflight_writer_type_mismatch_with_destination(tmp_path):
    """User declares a Pathway column whose type has no encoding into the
    destination iceberg column type — e.g. Pathway `int` writing into iceberg
    `string`. Currently fails inside `array_for_type` per row with a
    `TypeMismatchWithSchema(Int(...), Utf8)` error or a parquet writer
    failure; should be caught at writer construction naming the column and
    both types."""
    namespace = uuid.uuid4().hex
    table_name = uuid.uuid4().hex
    _create_table_with_schema(
        namespace,
        table_name,
        _existing_table_schema(
            NestedField(1, "data", StringType(), required=True),
            # Destination is `string`; user will declare Pathway `int`.
            NestedField(2, "label", StringType(), required=False),
        ),
    )
    # Pathway `int` ↔ iceberg `string` is not a defined conversion.
    _register_single_row_write(tmp_path, namespace, table_name, label=42)
    msg = _run_expecting_error("label")
    # Should mention both Pathway and iceberg sides of the type clash.
    assert "int" in msg.lower(), f"error should mention Pathway 'int', got: {msg}"
    assert "string" in msg.lower(), f"error should mention iceberg 'string', got: {msg}"


def test_iceberg_preflight_reader_tuple_arity_vs_struct(tmp_path):
    """User declares Pathway `tuple[int, str]` (arity 2) for an iceberg
    struct with 3 fields. Currently `convert_arrow_struct_array` emits a
    per-row conversion error 'struct arity 3 != tuple arity 2'; the user
    sees a flood of identical row-level errors rather than a single
    schema-mismatch message at startup."""
    namespace = uuid.uuid4().hex
    table_name = uuid.uuid4().hex
    _create_table_with_schema(
        namespace,
        table_name,
        _existing_table_schema(
            NestedField(1, "data", StringType(), required=True),
            NestedField(
                2,
                "payload",
                StructType(
                    NestedField(3, "a", LongType(), required=False),
                    NestedField(4, "b", StringType(), required=False),
                    NestedField(5, "c", LongType(), required=False),
                ),
                required=False,
            ),
        ),
    )
    # Pre-populate with a matching tuple so the reader walks at least one row.
    _register_single_row_write(
        tmp_path,
        namespace,
        table_name,
        payload=pw.apply_with_type(
            lambda _d: (1, "x", 2), tuple[int, str, int], pw.this.data
        ),
    )
    run()
    G.clear()

    # Now read with a mismatched tuple arity.
    class InputSchema(pw.Schema):
        data: str = pw.column_definition(primary_key=True)
        payload: tuple[int, str]  # arity 2; iceberg has 3 fields

    table = pw.io.iceberg.read(
        catalog=_get_catalog(LOCAL_BACKEND_NAME),
        namespace=[namespace],
        table_name=table_name,
        mode="static",
        schema=InputSchema,
    )
    pw.io.jsonlines.write(table, tmp_path / "output.jsonl")
    msg = _run_expecting_error("payload")
    assert "2" in msg and "3" in msg, f"error should mention both arities, got: {msg}"


def test_iceberg_preflight_reader_tuple_inner_type_mismatch(tmp_path):
    """User declares Pathway `tuple[int, int]` (arity matches) for an iceberg
    `struct<a: long, b: string>`. The inner type at position 1 (`int` vs
    `string`) has no defined decoding, so the reader rejects it at startup
    with a message naming the column, instead of emitting per-row conversion
    errors while `pw.run()` silently produces no output."""
    namespace = uuid.uuid4().hex
    table_name = uuid.uuid4().hex
    _create_table_with_schema(
        namespace,
        table_name,
        _existing_table_schema(
            NestedField(1, "data", StringType(), required=True),
            NestedField(
                2,
                "payload",
                StructType(
                    NestedField(3, "a", LongType(), required=False),
                    NestedField(4, "b", StringType(), required=False),
                ),
                required=False,
            ),
        ),
    )
    # Pre-populate with a positionally-matching tuple so the table has a row.
    _register_single_row_write(
        tmp_path,
        namespace,
        table_name,
        payload=pw.apply_with_type(lambda _d: (1, "x"), tuple[int, str], pw.this.data),
    )
    run()
    G.clear()

    # Read declaring the second tuple position as `int`, which has no decoding
    # from the iceberg `string` field; the arities still match so this is not
    # caught by the arity preflight.
    class InputSchema(pw.Schema):
        data: str = pw.column_definition(primary_key=True)
        payload: tuple[int, int]

    table = pw.io.iceberg.read(
        catalog=_get_catalog(LOCAL_BACKEND_NAME),
        namespace=[namespace],
        table_name=table_name,
        mode="static",
        schema=InputSchema,
    )
    pw.io.jsonlines.write(table, tmp_path / "output.jsonl")
    _run_expecting_error("payload")


def test_iceberg_preflight_reader_type_mismatch_with_iceberg_column(tmp_path):
    """User declares Pathway `int` for an iceberg `string` column. The
    per-row reader emits conversion errors but the pipeline silently
    produces no output. Should be caught at startup with a message naming
    the column and both types."""
    namespace = uuid.uuid4().hex
    table_name = uuid.uuid4().hex
    _create_table_with_schema(
        namespace,
        table_name,
        _existing_table_schema(
            NestedField(1, "data", StringType(), required=True),
            # `label` is a string in iceberg.
            NestedField(2, "label", StringType(), required=False),
        ),
    )
    # Populate.
    input_path = tmp_path / "input.jsonl"
    input_path.write_text('{"data": "row1", "label": "x"}\n')

    class WriteSchema(pw.Schema):
        data: str = pw.column_definition(primary_key=True)
        label: str

    table = pw.io.jsonlines.read(input_path, schema=WriteSchema, mode="static")
    pw.io.iceberg.write(
        table,
        catalog=_get_catalog(LOCAL_BACKEND_NAME),
        namespace=[namespace],
        table_name=table_name,
    )
    run()
    G.clear()

    class InputSchema(pw.Schema):
        data: str = pw.column_definition(primary_key=True)
        # User declares Pathway `int` — incompatible with iceberg `string`.
        label: int

    table = pw.io.iceberg.read(
        catalog=_get_catalog(LOCAL_BACKEND_NAME),
        namespace=[namespace],
        table_name=table_name,
        mode="static",
        schema=InputSchema,
    )
    pw.io.jsonlines.write(table, tmp_path / "output.jsonl")
    msg = _run_expecting_error("label")
    assert "int" in msg.lower(), f"error should mention Pathway 'int', got: {msg}"
    assert "string" in msg.lower(), f"error should mention iceberg 'string', got: {msg}"


def test_iceberg_preflight_writer_pathway_has_extra_column(tmp_path):
    """The user's Pathway table has a column that the destination iceberg
    table does not declare. Currently either silently dropped or fails
    obscurely inside the parquet writer. Should be caught at writer
    construction so the user can see they're producing data nothing will
    consume."""
    namespace = uuid.uuid4().hex
    table_name = uuid.uuid4().hex
    _create_table_with_schema(
        namespace,
        table_name,
        _existing_table_schema(
            NestedField(1, "data", StringType(), required=True),
        ),
    )
    # `extra` is NOT in the iceberg table.
    _register_single_row_write(
        tmp_path, namespace, table_name, extra="silently dropped"
    )
    msg = _run_expecting_error("extra")
    assert (
        "iceberg" in msg.lower() or "schema" in msg.lower() or "not in" in msg.lower()
    ), f"error should clearly flag the schema mismatch, got: {msg}"


def test_iceberg_preflight_writer_timestamp_unit_mismatch(tmp_path):
    """User picks `timestamp_unit="ns"` but the destination column is
    Iceberg `timestamp` (microsecond). Pathway emits arrow
    `Timestamp(Nanosecond, None)`, iceberg-rust expects
    `Timestamp(Microsecond, None)` — the writer fails inside parquet with
    an arrow type-mismatch. Should be caught at writer construction with
    both unit choices made explicit.

    (Picking this direction because `tabulario/iceberg-rest` doesn't yet
    create the V3 `timestamp_ns` type at table-creation time.)
    """
    from pyiceberg.types import TimestampType

    namespace = uuid.uuid4().hex
    table_name = uuid.uuid4().hex
    _create_table_with_schema(
        namespace,
        table_name,
        _existing_table_schema(
            NestedField(1, "data", StringType(), required=True),
            # Destination is `timestamp` (microsecond); user will pick `ns`.
            NestedField(2, "tnaive", TimestampType(), required=False),
        ),
    )
    # The write carries a `timestamp_unit`, so it can't go through the shared
    # single-row helper.
    input_path = tmp_path / "input.txt"
    input_path.write_text("test")
    table = pw.io.plaintext.read(input_path, mode="static")
    table = table.select(
        data=pw.this.data,
        tnaive=pw.DateTimeNaive(year=2025, month=1, day=17),
    )
    pw.io.iceberg.write(
        table,
        catalog=_get_catalog(LOCAL_BACKEND_NAME),
        namespace=[namespace],
        table_name=table_name,
        timestamp_unit="ns",  # mismatches destination's microsecond precision
    )
    # The error must name the column and, deliberately, mention `timestamp_unit`
    # explicitly — not just an arrow panic that happens to contain "us" inside
    # "using" and "ns" inside "Timestamp(ns)".
    _run_expecting_error("tnaive", "timestamp_unit")


# NOTE: an earlier draft tested writing `Optional[T]` into a required iceberg
# column. That turns out to be fine in practice — Pathway's arrow batch is
# nullable but contains no actual nulls, and iceberg-rust accepts the write
# (Arrow spec: a nullable column with no nulls is a valid producer of a
# required parquet field). The data-dependent failure (an actual null arrives
# at a required column) is by definition not catchable in a preflight.


def test_iceberg_preflight_writer_struct_inner_type_mismatch(tmp_path):
    """Writer-side recursion: Pathway `tuple[int, int]` (arity matches)
    written into iceberg `struct<a: long, b: string>` — the second tuple
    position is int but the destination wants string. Should be caught at
    writer construction naming the column and the inner-field mismatch,
    rather than panicking inside the parquet writer."""
    namespace = uuid.uuid4().hex
    table_name = uuid.uuid4().hex
    _create_table_with_schema(
        namespace,
        table_name,
        _existing_table_schema(
            NestedField(1, "data", StringType(), required=True),
            NestedField(
                2,
                "payload",
                StructType(
                    NestedField(3, "a", LongType(), required=False),
                    NestedField(4, "b", StringType(), required=False),
                ),
                required=False,
            ),
        ),
    )
    _register_single_row_write(
        tmp_path,
        namespace,
        table_name,
        payload=pw.apply_with_type(
            lambda _d: (1, 2),
            tuple[int, int],  # second position is int but destination is string
            pw.this.data,
        ),
    )
    _run_expecting_error("payload")
