import json
import multiprocessing
import pathlib
import threading
import time
from uuid import uuid4

import numpy as np
import pandas as pd
import pytest

import pathway as pw
from pathway.internals.parse_graph import G
from pathway.tests.utils import CsvLinesNumberChecker, wait_result_with_checker

from .utils import RABBITMQ_STREAM_URI

WAIT_TIMEOUT_SECS = 30


# --- Parametrized read/write test ---


@pytest.mark.parametrize("format", ["plaintext", "raw", "json"])
@pytest.mark.parametrize("mode", ["streaming", "static"])
@pytest.mark.parametrize("with_headers", [False, True])
def test_rabbitmq_read_write(rabbitmq_context, tmp_path, format, mode, with_headers):
    output_file = tmp_path / "output.txt"
    n_messages = 3

    class JsonSchema(pw.Schema):
        name: str
        age: int

    schema = JsonSchema if format == "json" else None

    # Produce messages: via pw.io.rabbitmq.write when headers are tested,
    # otherwise via the test helper.
    if with_headers:
        G.clear()
        table = pw.debug.table_from_markdown(
            """
            name   | age
            Alice  | 30
            Bob    | 25
            Carol  | 35
            """
        )
        pw.io.rabbitmq.write(
            table,
            uri=RABBITMQ_STREAM_URI,
            stream_name=rabbitmq_context.stream_name,
            format="json",
            headers=[table.age],
        )
        pw.run()
        # Write always produces JSON; override format for read-back
        schema = JsonSchema
        format = "json"
    else:
        for i in range(n_messages):
            if format == "json":
                rabbitmq_context.send(json.dumps({"name": f"user{i}", "age": 20 + i}))
            else:
                rabbitmq_context.send(f"message-{i}")

    if mode == "static":
        time.sleep(1)

    G.clear()
    table = pw.io.rabbitmq.read(
        uri=RABBITMQ_STREAM_URI,
        stream_name=rabbitmq_context.stream_name,
        schema=schema,
        format=format,
        mode=mode,
    )
    pw.io.csv.write(table, output_file)

    wait_result_with_checker(
        CsvLinesNumberChecker(output_file, n_messages),
        WAIT_TIMEOUT_SECS,
    )


# --- Metadata test ---


def test_rabbitmq_metadata(rabbitmq_context, tmp_path: pathlib.Path):
    """Test with_metadata=True produces _metadata column with all expected fields."""
    output_file = tmp_path / "output.txt"

    # Phase 1: write messages with headers so application_properties are set
    G.clear()
    table = pw.debug.table_from_markdown(
        """
        name   | age
        Alice  | 30
        Bob    | 25
        """
    )
    pw.io.rabbitmq.write(
        table,
        uri=RABBITMQ_STREAM_URI,
        stream_name=rabbitmq_context.stream_name,
        format="json",
        headers=[table.age],
    )
    pw.run()

    # Phase 2: read back with metadata
    G.clear()

    class InputSchema(pw.Schema):
        name: str
        age: int

    table = pw.io.rabbitmq.read(
        uri=RABBITMQ_STREAM_URI,
        stream_name=rabbitmq_context.stream_name,
        schema=InputSchema,
        format="json",
        with_metadata=True,
    )
    pw.io.csv.write(table, output_file)

    wait_result_with_checker(
        CsvLinesNumberChecker(output_file, 2),
        WAIT_TIMEOUT_SECS,
    )

    result = pd.read_csv(output_file)
    assert "_metadata" in result.columns, "Expected _metadata column"
    for _, row in result.iterrows():
        metadata = json.loads(row["_metadata"])
        # Core fields always present
        assert "offset" in metadata
        assert "stream_name" in metadata
        assert metadata["stream_name"] == rabbitmq_context.stream_name
        # AMQP 1.0 property fields exist (may be null)
        for field in [
            "message_id",
            "correlation_id",
            "content_type",
            "content_encoding",
            "subject",
            "reply_to",
            "priority",
            "durable",
        ]:
            assert field in metadata, f"Expected '{field}' in metadata"
        # Application properties contain pathway_time, pathway_diff, and user header
        assert "application_properties" in metadata
        app_props = metadata["application_properties"]
        assert "pathway_time" in app_props
        assert "pathway_diff" in app_props
        assert app_props["pathway_diff"] in ("1", "-1")
        assert "age" in app_props


# --- Timestamp filter test ---


def test_rabbitmq_start_from_timestamp(rabbitmq_context, tmp_path: pathlib.Path):
    """Test start_from_timestamp_ms filters messages by server-side timestamp."""
    output_file_all = tmp_path / "output_all.txt"
    output_file_filtered = tmp_path / "output_filtered.txt"

    # Send two messages with a clear time gap between them
    rabbitmq_context.send('{"value": "msg1"}')
    time.sleep(3)
    ts_between = int(time.time() * 1000)
    time.sleep(3)
    rabbitmq_context.send('{"value": "msg2"}')
    time.sleep(1)

    # Run 1: no timestamp filter — should read both messages
    G.clear()

    class InputSchema(pw.Schema):
        value: str

    table = pw.io.rabbitmq.read(
        uri=RABBITMQ_STREAM_URI,
        stream_name=rabbitmq_context.stream_name,
        schema=InputSchema,
        format="json",
        mode="static",
    )
    pw.io.csv.write(table, output_file_all)

    wait_result_with_checker(
        CsvLinesNumberChecker(output_file_all, 2),
        WAIT_TIMEOUT_SECS,
    )

    # Run 2: timestamp between the two messages — should read only msg2
    G.clear()

    table = pw.io.rabbitmq.read(
        uri=RABBITMQ_STREAM_URI,
        stream_name=rabbitmq_context.stream_name,
        schema=InputSchema,
        format="json",
        mode="static",
        start_from="timestamp",
        start_from_timestamp_ms=ts_between,
    )
    pw.io.csv.write(table, output_file_filtered)

    wait_result_with_checker(
        CsvLinesNumberChecker(output_file_filtered, 1),
        WAIT_TIMEOUT_SECS,
    )


# --- Start from end test ---


def test_rabbitmq_start_from_end(rabbitmq_context, tmp_path: pathlib.Path):
    """Test start_from='end' skips existing messages and only reads new ones."""
    output_file = tmp_path / "output.txt"
    n_old = 3
    n_new = 2

    # Pre-populate with messages that should be skipped
    for i in range(n_old):
        rabbitmq_context.send(f"old-{i}")

    G.clear()
    table = pw.io.rabbitmq.read(
        uri=RABBITMQ_STREAM_URI,
        stream_name=rabbitmq_context.stream_name,
        format="plaintext",
        start_from="end",
    )
    pw.io.csv.write(table, output_file)

    def send_new_messages():
        # Wait for the pipeline to start
        time.sleep(3)
        for i in range(n_new):
            rabbitmq_context.send(f"new-{i}")

    sender = threading.Thread(target=send_new_messages, daemon=True)
    sender.start()

    wait_result_with_checker(
        CsvLinesNumberChecker(output_file, n_new),
        WAIT_TIMEOUT_SECS,
    )


def test_rabbitmq_start_from_end_static_empty(rabbitmq_context, tmp_path: pathlib.Path):
    """Test that start_from='end' in static mode produces an empty table.

    A background thread continuously streams data, but none of it should appear
    in the output because the reader starts past the tail and immediately finishes.
    """
    output_file = tmp_path / "output.txt"

    # Pre-populate with messages that should be skipped
    for i in range(5):
        rabbitmq_context.send(f"old-{i}")
    time.sleep(1)

    G.clear()
    table = pw.io.rabbitmq.read(
        uri=RABBITMQ_STREAM_URI,
        stream_name=rabbitmq_context.stream_name,
        format="plaintext",
        mode="static",
        start_from="end",
    )
    pw.io.csv.write(table, output_file)

    stop = threading.Event()

    def background_streamer():
        while not stop.is_set():
            rabbitmq_context.send("should-not-appear")
            time.sleep(0.1)

    sender = threading.Thread(target=background_streamer, daemon=True)
    sender.start()

    p = multiprocessing.Process(target=pw.run)
    p.start()
    time.sleep(10)
    stop.set()
    sender.join(timeout=5)
    p.kill()
    p.join()

    if output_file.exists() and output_file.stat().st_size > 0:
        result = pd.read_csv(output_file)
        assert len(result) == 0, f"Expected empty table, got {len(result)} rows"


# --- JSON field paths test ---


def test_rabbitmq_json_field_paths(rabbitmq_context, tmp_path: pathlib.Path):
    """Test json_field_paths extracts nested JSON fields."""
    output_file = tmp_path / "output.txt"

    G.clear()
    rabbitmq_context.send(json.dumps({"user": {"name": "Alice", "age": 30}}))
    rabbitmq_context.send(json.dumps({"user": {"name": "Bob", "age": 25}}))
    rabbitmq_context.send(json.dumps({"user": {"name": "Carol", "age": 35}}))

    class InputSchema(pw.Schema):
        name: str
        age: int

    table = pw.io.rabbitmq.read(
        uri=RABBITMQ_STREAM_URI,
        stream_name=rabbitmq_context.stream_name,
        schema=InputSchema,
        format="json",
        json_field_paths={"name": "/user/name", "age": "/user/age"},
    )
    pw.io.csv.write(table, output_file)

    wait_result_with_checker(
        CsvLinesNumberChecker(output_file, 3),
        WAIT_TIMEOUT_SECS,
    )

    result = pd.read_csv(output_file)
    names = sorted(result["name"].tolist())
    ages = sorted(result["age"].tolist())
    assert names == ["Alice", "Bob", "Carol"]
    assert ages == [25, 30, 35]


# --- Dynamic topics test ---


def test_rabbitmq_dynamic_topics(rabbitmq_context, tmp_path: pathlib.Path):
    """Test writing to different streams based on a column value."""
    stream_1 = f"rmq-dyn-{uuid4()}"
    stream_2 = f"rmq-dyn-{uuid4()}"
    output_file = tmp_path / "output.txt"

    rabbitmq_context.create_stream(stream_1)
    rabbitmq_context.create_stream(stream_2)
    try:
        # Phase 1: write rows to different streams based on a column
        G.clear()
        table = pw.debug.table_from_markdown(
            f"""
            k  | v   | stream_name
            0  | foo | {stream_1}
            1  | bar | {stream_2}
            2  | baz | {stream_1}
            """
        )
        pw.io.rabbitmq.write(
            table,
            uri=RABBITMQ_STREAM_URI,
            stream_name=table.stream_name,
            format="json",
        )
        pw.run()

        time.sleep(2)

        # Phase 2: read from each stream and verify
        G.clear()

        class InputSchema(pw.Schema):
            k: int
            v: str
            stream_name: str

        t1 = pw.io.rabbitmq.read(
            uri=RABBITMQ_STREAM_URI,
            stream_name=stream_1,
            schema=InputSchema,
            format="json",
        )
        t2 = pw.io.rabbitmq.read(
            uri=RABBITMQ_STREAM_URI,
            stream_name=stream_2,
            schema=InputSchema,
            format="json",
        )
        combined = pw.Table.concat_reindex(t1, t2)
        pw.io.csv.write(combined, output_file)

        wait_result_with_checker(
            CsvLinesNumberChecker(output_file, 3),
            WAIT_TIMEOUT_SECS,
        )

        result = pd.read_csv(output_file)
        ks_by_stream = {}
        for _, row in result.iterrows():
            ks_by_stream.setdefault(row["stream_name"], []).append(row["k"])
        assert sorted(ks_by_stream[stream_1]) == [0, 2]
        assert sorted(ks_by_stream[stream_2]) == [1]
    finally:
        rabbitmq_context.delete_stream(stream_1)
        rabbitmq_context.delete_stream(stream_2)


# --- Persistence test ---


@pytest.mark.parametrize("mode", ["streaming", "static"])
def test_rabbitmq_persistence(rabbitmq_context, tmp_path: pathlib.Path, mode):
    """Test that persistence preserves offsets across restarts.

    Each run adds new messages and reads only the newly added ones,
    verifying that seek() correctly resumes from the last committed offset.
    """
    persistence_dir = tmp_path / "PStorage"
    runs = [3, 4, 2]

    total_seen = 0
    for run_idx, n_new in enumerate(runs):
        for i in range(n_new):
            rabbitmq_context.send(f'{{"run": {run_idx}, "i": {i}}}')

        if mode == "static":
            time.sleep(1)

        output_file = tmp_path / f"output_{run_idx}.txt"
        G.clear()

        class InputSchema(pw.Schema):
            run: int
            i: int

        table = pw.io.rabbitmq.read(
            uri=RABBITMQ_STREAM_URI,
            stream_name=rabbitmq_context.stream_name,
            schema=InputSchema,
            format="json",
            mode=mode,
            name="persist-source",
        )
        pw.io.csv.write(table, output_file)

        persistence_config = pw.persistence.Config(
            pw.persistence.Backend.filesystem(persistence_dir),
        )

        wait_result_with_checker(
            CsvLinesNumberChecker(output_file, n_new),
            WAIT_TIMEOUT_SECS,
            kwargs={"persistence_config": persistence_config},
        )

        total_seen += n_new

    # Final run without persistence: should see all messages from the beginning
    if mode == "static":
        time.sleep(1)

    output_all = tmp_path / "output_all.txt"
    G.clear()

    class InputSchema(pw.Schema):
        run: int
        i: int

    table = pw.io.rabbitmq.read(
        uri=RABBITMQ_STREAM_URI,
        stream_name=rabbitmq_context.stream_name,
        schema=InputSchema,
        format="json",
        mode=mode,
    )
    pw.io.csv.write(table, output_all)

    wait_result_with_checker(
        CsvLinesNumberChecker(output_all, total_seen),
        WAIT_TIMEOUT_SECS,
    )


# --- Invalid persisted offset test ---


def test_rabbitmq_invalid_persisted_offset(rabbitmq_context, tmp_path: pathlib.Path):
    """Test that seek() detects a persisted offset beyond the stream's last offset.

    Run 1 writes messages and reads them with persistence. Then the stream is
    deleted and recreated empty, so the saved offset is invalid. Run 2 must fail.
    """
    persistence_dir = tmp_path / "PStorage"
    output_1 = tmp_path / "output_1.txt"
    output_2 = tmp_path / "output_2.txt"
    stream_name = rabbitmq_context.stream_name

    # Send messages and read with persistence
    for i in range(5):
        rabbitmq_context.send(f'{{"v": {i}}}')
    time.sleep(1)

    G.clear()

    class InputSchema(pw.Schema):
        v: int

    table = pw.io.rabbitmq.read(
        uri=RABBITMQ_STREAM_URI,
        stream_name=stream_name,
        schema=InputSchema,
        format="json",
        mode="static",
        name="invalid-offset-source",
    )
    pw.io.csv.write(table, output_1)
    pw.run(
        persistence_config=pw.persistence.Config(
            pw.persistence.Backend.filesystem(persistence_dir),
        )
    )

    result_1 = pd.read_csv(output_1)
    assert len(result_1) == 5

    # Delete and recreate the stream so the saved offset is invalid
    rabbitmq_context.delete_stream(stream_name)
    rabbitmq_context.create_stream(stream_name)
    rabbitmq_context.send('{"v": 99}')
    time.sleep(1)

    # Run 2: should fail because persisted offset exceeds stream's last offset
    G.clear()

    table = pw.io.rabbitmq.read(
        uri=RABBITMQ_STREAM_URI,
        stream_name=stream_name,
        schema=InputSchema,
        format="json",
        mode="static",
        name="invalid-offset-source",
    )
    pw.io.csv.write(table, output_2)

    with pytest.raises(Exception, match="persisted offset .* exceeds"):
        pw.run(
            persistence_config=pw.persistence.Config(
                pw.persistence.Backend.filesystem(persistence_dir),
            )
        )


# --- Round-trip header test ---


class SimpleObject:
    """Picklable object for PyObjectWrapper round-trip testing."""

    def __init__(self, a):
        self.a = a

    def __eq__(self, other):
        return self.a == other.a


class HeaderRoundTripSchema(pw.Schema):
    s_val: str
    i_val: int
    f_val: float
    b_val: bool
    json_val: pw.Json
    bytes_val: bytes
    dt_naive_val: pw.DateTimeNaive
    dt_utc_val: pw.DateTimeUtc
    dur_val: pw.Duration
    opt_val: int | None
    tuple_val: tuple[int, str]
    list_val: list[int]
    ndarray_int_val: np.ndarray
    ndarray_float_val: np.ndarray
    pyobj_val: pw.PyObjectWrapper[SimpleObject]


def test_rabbitmq_header_round_trip(rabbitmq_context, tmp_path: pathlib.Path):
    """Test that all Pathway types survive a write→read round-trip through headers.

    Values are JSON-encoded as AMQP strings on write and appear in
    _metadata.application_properties on read. json.loads recovers the JSON value.
    Types that map natively to JSON (bool, int, float, str, None, Json) are
    fully round-trippable. Other types (bytes, DateTime, Duration, ndarray,
    Pointer, PyObjectWrapper) are serialized following the same encoding as
    pw.io.jsonlines.write.
    """
    import base64

    output_file = tmp_path / "output.txt"

    G.clear()
    table = pw.debug.table_from_rows(
        HeaderRoundTripSchema,
        [
            (
                "hello",  # str
                42,  # int
                3.14,  # float
                True,  # bool
                pw.Json({"key": "value", "nested": [1, 2]}),  # Json
                b"\xde\xad\xbe\xef",  # bytes
                pw.DateTimeNaive("2025-03-14T10:13:00"),  # DateTimeNaive
                pw.DateTimeUtc("2025-03-14T10:13:00+00:00"),  # DateTimeUtc
                pd.Timedelta("1 days 2 hours 3 seconds"),  # Duration
                7,  # Optional[int] - populated
                (1, "abc"),  # tuple
                [10, 20, 30],  # list
                np.array([1, 2, 3]),  # ndarray int
                np.array([1.1, 2.2]),  # ndarray float
                pw.wrap_py_object(SimpleObject(42)),  # PyObjectWrapper
            ),
            (
                "",  # str empty
                -1,  # int negative
                0.0,  # float zero
                False,  # bool
                pw.Json(None),  # Json null
                b"",  # bytes empty
                pw.DateTimeNaive("2026-01-01T00:00:00"),
                pw.DateTimeUtc("2026-01-01T00:00:00+00:00"),
                pd.Timedelta("0"),  # Duration zero
                None,  # Optional[int] - null
                (0, ""),  # tuple
                [],  # list empty
                np.array([], dtype=np.int64),  # ndarray empty
                np.array([], dtype=np.float64),  # ndarray empty
                pw.wrap_py_object(SimpleObject(0)),
            ),
        ],
    )
    pw.io.rabbitmq.write(
        table,
        uri=RABBITMQ_STREAM_URI,
        stream_name=rabbitmq_context.stream_name,
        format="json",
        headers=[
            table.s_val,
            table.i_val,
            table.f_val,
            table.b_val,
            table.json_val,
            table.bytes_val,
            table.dt_naive_val,
            table.dt_utc_val,
            table.dur_val,
            table.opt_val,
            table.tuple_val,
            table.list_val,
            table.ndarray_int_val,
            table.ndarray_float_val,
            table.pyobj_val,
        ],
    )
    pw.run()

    time.sleep(1)

    G.clear()
    table = pw.io.rabbitmq.read(
        uri=RABBITMQ_STREAM_URI,
        stream_name=rabbitmq_context.stream_name,
        format="plaintext",
        mode="static",
        with_metadata=True,
    )
    pw.io.csv.write(table, output_file)

    wait_result_with_checker(
        CsvLinesNumberChecker(output_file, 2),
        WAIT_TIMEOUT_SECS,
    )

    result = pd.read_csv(output_file)
    for _, row in result.iterrows():
        metadata = json.loads(row["_metadata"])
        app_props = metadata["application_properties"]

        # All header names must be present
        for field in HeaderRoundTripSchema.column_names():
            assert field in app_props, f"Missing header '{field}'"

        # JSON-native types: exact round-trip via json.loads
        s = json.loads(app_props["s_val"])
        assert isinstance(s, str)

        i = json.loads(app_props["i_val"])
        assert isinstance(i, int)

        f = json.loads(app_props["f_val"])
        assert isinstance(f, float)

        b = json.loads(app_props["b_val"])
        assert isinstance(b, bool)

        j = json.loads(app_props["json_val"])
        assert isinstance(j, (dict, type(None)))

        opt = json.loads(app_props["opt_val"])
        assert isinstance(opt, (int, type(None)))

        # bytes: base64-encoded string
        bytes_str = json.loads(app_props["bytes_val"])
        assert isinstance(bytes_str, str)
        base64.b64decode(bytes_str)  # must not raise

        # DateTimeNaive / DateTimeUtc: string representation
        dt_naive_str = json.loads(app_props["dt_naive_val"])
        assert isinstance(dt_naive_str, str)

        dt_utc_str = json.loads(app_props["dt_utc_val"])
        assert isinstance(dt_utc_str, str)

        # Duration: integer (nanoseconds)
        dur_val = json.loads(app_props["dur_val"])
        assert isinstance(dur_val, int)

        # Tuple: JSON array
        tuple_val = json.loads(app_props["tuple_val"])
        assert isinstance(tuple_val, list)

        # List: JSON array
        list_val = json.loads(app_props["list_val"])
        assert isinstance(list_val, list)

        # ndarray int: JSON object with @shape and @elements
        ndarray_int = json.loads(app_props["ndarray_int_val"])
        assert isinstance(ndarray_int, dict)
        assert "shape" in ndarray_int
        assert "elements" in ndarray_int

        # ndarray float: JSON object with @shape and @elements
        ndarray_float = json.loads(app_props["ndarray_float_val"])
        assert isinstance(ndarray_float, dict)
        assert "shape" in ndarray_float
        assert "elements" in ndarray_float

        # PyObjectWrapper: base64-encoded bincode string
        pyobj_str = json.loads(app_props["pyobj_val"])
        assert isinstance(pyobj_str, str)
        base64.b64decode(pyobj_str)  # must not raise


# --- Streaming test ---


def test_rabbitmq_streaming_live(rabbitmq_context, tmp_path: pathlib.Path):
    """Test that the connector correctly picks up messages arriving during runtime.

    A background thread sends messages one by one with delays, while the Pathway
    pipeline runs and reads them. Each message is sent only after the previous
    ones have appeared in the output.
    """
    output_file = tmp_path / "output.txt"
    n_messages = 10

    G.clear()
    table = pw.io.rabbitmq.read(
        uri=RABBITMQ_STREAM_URI,
        stream_name=rabbitmq_context.stream_name,
        format="plaintext",
    )
    pw.io.csv.write(table, output_file)

    def streaming_sender():
        # Wait for the pipeline to start and create the output file
        time.sleep(3)
        for i in range(n_messages):
            rabbitmq_context.send(f"live-{i}")
            # Wait until this message appears in the output before sending the next
            wait_result_with_checker(
                CsvLinesNumberChecker(output_file, i + 1),
                WAIT_TIMEOUT_SECS,
                target=None,
            )

    sender_thread = threading.Thread(target=streaming_sender, daemon=True)
    sender_thread.start()

    # pw.run() is started here via the default target= parameter
    wait_result_with_checker(
        CsvLinesNumberChecker(output_file, n_messages),
        WAIT_TIMEOUT_SECS,
    )
