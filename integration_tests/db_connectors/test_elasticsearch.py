import datetime
import json
import os
import pathlib
import subprocess
import sys
import textwrap
import time

import pytest
from utils import ELASTICSEARCH_URL, elasticsearch_now_ms as _now_ms

import pathway as pw
from pathway.internals.parse_graph import G
from pathway.tests.utils import (
    FileLinesNumberChecker,
    read_jsonlines,
    run,
    wait_result_with_checker,
)

# ---------------------------------------------------------------------------
# Streaming worker run in a fresh subprocess, mirroring the MongoDB tests:
# subprocess.Popen (not fork) avoids inheriting Rust/tokio global state that a
# prior pw.run() leaves behind in the test process.
# ---------------------------------------------------------------------------
_STREAMING_WORKER_SCRIPT = textwrap.dedent(
    """\
    import json
    import sys

    cfg = json.loads(sys.argv[1])

    import pathway as pw
    from pathway.internals.parse_graph import G
    from pathway.tests.utils import run

    type_map = {"str": str, "int": int, "float": float, "bool": bool}
    columns = {
        name: pw.column_definition(dtype=type_map[t])
        for name, t in cfg["columns"].items()
    }
    InputSchema = pw.schema_builder(columns=columns)

    G.clear()
    persistence_config = None
    read_kwargs = dict(
        host=cfg["host"],
        auth=pw.io.elasticsearch.ElasticSearchAuth.basic("admin", "admin"),
        index_name=cfg["index_name"],
        schema=InputSchema,
        timestamp_column=cfg["timestamp_column"],
        id_column=cfg["id_column"],
        max_transaction_duration=__import__("datetime").timedelta(
            milliseconds=cfg["max_transaction_duration_ms"]
        ),
        mode="streaming",
        poll_interval=__import__("datetime").timedelta(milliseconds=cfg["poll_interval_ms"]),
        autocommit_duration_ms=100,
    )
    if cfg.get("name"):
        read_kwargs["name"] = cfg["name"]
    if cfg.get("read_batch_size") is not None:
        read_kwargs["read_batch_size"] = cfg["read_batch_size"]
    table = pw.io.elasticsearch.read(**read_kwargs)
    pw.io.jsonlines.write(table, cfg["output_path"])

    if cfg.get("pstorage_path"):
        persistence_config = pw.persistence.Config(
            backend=pw.persistence.Backend.filesystem(cfg["pstorage_path"])
        )
    run(persistence_config=persistence_config)
    """
)


def _start_streaming_worker(
    *,
    index_name: str,
    columns: dict[str, str],
    timestamp_column: str,
    id_column: str,
    output_path: pathlib.Path,
    max_transaction_duration_ms: int,
    poll_interval_ms: int = 200,
    pstorage_path: pathlib.Path | None = None,
    name: str | None = None,
    read_batch_size: int | None = None,
) -> subprocess.Popen:
    cfg = json.dumps(
        {
            "host": ELASTICSEARCH_URL,
            "index_name": index_name,
            "columns": columns,
            "timestamp_column": timestamp_column,
            "id_column": id_column,
            "output_path": str(output_path),
            "max_transaction_duration_ms": max_transaction_duration_ms,
            "poll_interval_ms": poll_interval_ms,
            "pstorage_path": str(pstorage_path) if pstorage_path else None,
            "name": name,
            "read_batch_size": read_batch_size,
        }
    )
    return subprocess.Popen(
        [sys.executable, "-c", _STREAMING_WORKER_SCRIPT, cfg],
        env=os.environ,
    )


def _wait_and_terminate(
    checker: FileLinesNumberChecker,
    timeout_sec: float,
    proc: subprocess.Popen,
    *,
    double_check_interval: float | None = None,
    persistence_flush_sec: float = 5.0,
) -> None:
    """Poll checker until it passes, sleep for persistence flush, then kill proc."""
    start = time.monotonic()
    try:
        while True:
            time.sleep(0.1)
            elapsed = time.monotonic() - start
            if elapsed >= timeout_sec:
                raise AssertionError(
                    f"Timed out after {timeout_sec}s. "
                    f"{checker.provide_information_on_failure()}"
                )
            if checker():
                if double_check_interval is not None:
                    time.sleep(double_check_interval)
                    if not checker():
                        raise AssertionError(
                            f"Double-check failed. "
                            f"{checker.provide_information_on_failure()}"
                        )
                break
            if proc.poll() is not None:
                assert (
                    proc.returncode == 0
                ), f"Subprocess exited with code {proc.returncode}"
                break
        time.sleep(persistence_flush_sec)
    finally:
        if proc.poll() is None:
            proc.terminate()
            proc.wait()


_TIMESTAMP_PROPERTIES = {
    "ts": {"type": "long"},
    "doc_id": {"type": "keyword"},
}


def _output_rows(output_path: pathlib.Path, fields: list[str]) -> list[dict]:
    """Read jsonlines output, projecting to the given fields plus the diff."""
    rows = []
    for record in read_jsonlines(output_path):
        row = {field: record[field] for field in fields}
        row["diff"] = record["diff"]
        rows.append(row)
    return rows


def _static_read_doc_ids(
    elasticsearch,
    output_path: pathlib.Path,
    documents: list[dict],
    read_batch_size: int,
) -> list[dict]:
    """Index ``documents`` (each a ``{"doc_id", "ts"}`` dict) into a fresh index,
    read it once in static mode with the given ``read_batch_size``, and return the
    delivered ``doc_id``/``diff`` rows."""
    index_name = elasticsearch.generate_index_name()
    elasticsearch.create_index(index_name, _TIMESTAMP_PROPERTIES)
    elasticsearch.index_documents(index_name, documents, id_field="doc_id")

    class InputSchema(pw.Schema):
        doc_id: str
        ts: int

    G.clear()
    table = pw.io.elasticsearch.read(
        host=ELASTICSEARCH_URL,
        auth=pw.io.elasticsearch.ElasticSearchAuth.basic("admin", "admin"),
        index_name=index_name,
        schema=InputSchema,
        timestamp_column="ts",
        id_column="doc_id",
        max_transaction_duration=datetime.timedelta(milliseconds=1000),
        mode="static",
        read_batch_size=read_batch_size,
    )
    pw.io.jsonlines.write(table, output_path)
    run()
    return _output_rows(output_path, ["doc_id"])


# A minimal worker that just builds and runs the reader; used to capture the
# startup mapping-validation warnings the connector logs to stderr. Run in a
# subprocess so an index whose mapping makes the connector fail (and retry
# forever) can be killed without affecting the test process.
_MAPPING_WORKER_SCRIPT = textwrap.dedent(
    """\
    import json
    import sys

    cfg = json.loads(sys.argv[1])

    import pathway as pw
    from pathway.internals.parse_graph import G
    from pathway.tests.utils import run

    class InputSchema(pw.Schema):
        doc_id: str
        ts: int

    G.clear()
    table = pw.io.elasticsearch.read(
        host=cfg["host"],
        auth=pw.io.elasticsearch.ElasticSearchAuth.basic("admin", "admin"),
        index_name=cfg["index_name"],
        schema=InputSchema,
        timestamp_column="ts",
        id_column="doc_id",
        max_transaction_duration=__import__("datetime").timedelta(milliseconds=1000),
        mode="static",
    )
    pw.io.jsonlines.write(table, cfg["output_path"])
    run()
    """
)


def _mapping_validation_stderr(
    index_name: str,
    output_path: pathlib.Path,
    stderr_path: pathlib.Path,
    *,
    timeout_sec: float = 60.0,
) -> str:
    """Run the reader against ``index_name`` in a subprocess and return everything
    it wrote to stderr up to the point where either the startup mapping warning
    appeared, the run finished, or the timeout elapsed."""
    cfg = json.dumps(
        {
            "host": ELASTICSEARCH_URL,
            "index_name": index_name,
            "output_path": str(output_path),
        }
    )
    with open(stderr_path, "w") as stderr_file:
        proc = subprocess.Popen(
            [sys.executable, "-c", _MAPPING_WORKER_SCRIPT, cfg],
            env=os.environ,
            stdout=subprocess.DEVNULL,
            stderr=stderr_file,
        )
        try:
            start = time.monotonic()
            while time.monotonic() - start < timeout_sec:
                time.sleep(0.2)
                captured = stderr_path.read_text()
                if "WARNING:Elasticsearch" in captured:
                    break
                if proc.poll() is not None:
                    break
        finally:
            if proc.poll() is None:
                proc.terminate()
                proc.wait()
    return stderr_path.read_text()


def test_elasticsearch_warns_when_id_column_is_not_sortable(tmp_path, elasticsearch):
    """A polling field Elasticsearch cannot sort by — ``id_column`` mapped as
    ``text`` — is reported with a warning at startup, so the misconfiguration is
    diagnosable rather than failing opaquely."""
    index_name = elasticsearch.generate_index_name()
    elasticsearch.create_index(
        index_name, {"ts": {"type": "long"}, "doc_id": {"type": "text"}}
    )
    elasticsearch.index_documents(
        index_name, [{"doc_id": "a", "ts": _now_ms()}], id_field="doc_id"
    )

    stderr = _mapping_validation_stderr(
        index_name, tmp_path / "out.jsonl", tmp_path / "stderr.txt"
    )
    assert "id_column" in stderr and "doc_id" in stderr, stderr
    assert "text" in stderr and "sortable" in stderr, stderr


def test_elasticsearch_warns_when_timestamp_column_is_not_numeric(
    tmp_path, elasticsearch
):
    """A ``timestamp_column`` mapped as ``keyword`` (not numeric) is reported with
    a warning at startup, since the connector reads and watermarks it as an
    integer."""
    index_name = elasticsearch.generate_index_name()
    elasticsearch.create_index(
        index_name, {"ts": {"type": "keyword"}, "doc_id": {"type": "keyword"}}
    )
    elasticsearch.index_documents(
        index_name, [{"doc_id": "a", "ts": "1000"}], id_field="doc_id"
    )

    stderr = _mapping_validation_stderr(
        index_name, tmp_path / "out.jsonl", tmp_path / "stderr.txt"
    )
    assert "timestamp_column" in stderr and "ts" in stderr, stderr
    assert "numeric" in stderr, stderr


def test_elasticsearch_well_mapped_index_emits_no_field_warning(
    tmp_path, elasticsearch
):
    """A correctly mapped index (numeric ``timestamp_column``, keyword
    ``id_column``) triggers none of the mapping-validation warnings."""
    index_name = elasticsearch.generate_index_name()
    elasticsearch.create_index(index_name, _TIMESTAMP_PROPERTIES)
    elasticsearch.index_documents(
        index_name, [{"doc_id": "a", "ts": _now_ms()}], id_field="doc_id"
    )

    stderr = _mapping_validation_stderr(
        index_name, tmp_path / "out.jsonl", tmp_path / "stderr.txt"
    )
    assert "WARNING:Elasticsearch" not in stderr, stderr


def test_elasticsearch_static_read_delivers_all_rows(tmp_path, elasticsearch):
    """In static mode every document in the index is delivered exactly once."""
    index_name = elasticsearch.generate_index_name()
    elasticsearch.create_index(
        index_name,
        {**_TIMESTAMP_PROPERTIES, "message": {"type": "keyword"}},
    )

    base_ts = _now_ms()
    documents = [
        {"doc_id": f"d{i}", "ts": base_ts + i, "message": f"m{i}"} for i in range(10)
    ]
    elasticsearch.index_documents(index_name, documents, id_field="doc_id")

    output_path = tmp_path / "output.jsonl"

    class InputSchema(pw.Schema):
        doc_id: str
        ts: int
        message: str

    G.clear()
    table = pw.io.elasticsearch.read(
        host=ELASTICSEARCH_URL,
        auth=pw.io.elasticsearch.ElasticSearchAuth.basic("admin", "admin"),
        index_name=index_name,
        schema=InputSchema,
        timestamp_column="ts",
        id_column="doc_id",
        max_transaction_duration=datetime.timedelta(milliseconds=1000),
        mode="static",
    )
    pw.io.jsonlines.write(table, output_path)
    run()

    rows = _output_rows(output_path, ["doc_id", "ts", "message"])
    assert all(row["diff"] == 1 for row in rows)
    delivered = {(row["doc_id"], row["ts"], row["message"]) for row in rows}
    expected = {(d["doc_id"], d["ts"], d["message"]) for d in documents}
    assert len(rows) == len(documents)
    assert delivered == expected


def test_elasticsearch_write_splits_batch_larger_than_max_content_length(
    tmp_path, elasticsearch
):
    """A minibatch whose serialized ``_bulk`` body would exceed the cluster's
    ``http.max_content_length`` is split across several bulk requests, so every row
    is indexed instead of the whole write failing with ``413 Request Entity Too
    Large``. Reading a large file line by line and writing it to Elasticsearch in a
    single static run pushes the entire input through one minibatch, which is the
    case that overflows the limit before the fix.
    """
    index_name = elasticsearch.generate_index_name()
    # ``payload`` is stored but not indexed: the test only needs the documents to
    # be large and to land in the index, not to be searchable, and skipping the
    # analysis of hundreds of megabytes of text keeps the write fast.
    elasticsearch.create_index(
        index_name,
        {
            "doc_id": {"type": "keyword"},
            "ts": {"type": "long"},
            "payload": {"type": "text", "index": False},
        },
    )

    # Size the input off the live limit so a single un-split bulk request would be
    # rejected: ~1.4x the cap, spread over rows large enough to keep the row count
    # (and JSON-formatting overhead) small.
    max_content_length = elasticsearch.max_content_length()
    payload_size = 700_000
    blob = "x" * payload_size
    n_rows = (max_content_length + max_content_length // 2) // payload_size

    input_path = tmp_path / "big_input.jsonl"
    with open(input_path, "w") as f:
        for i in range(n_rows):
            f.write(
                json.dumps({"doc_id": f"d{i}", "ts": 1000 + i, "payload": blob}) + "\n"
            )

    class InputSchema(pw.Schema):
        doc_id: str
        ts: int
        payload: str

    G.clear()
    table = pw.io.fs.read(
        str(input_path), format="json", schema=InputSchema, mode="static"
    )
    pw.io.elasticsearch.write(
        table=table,
        host=ELASTICSEARCH_URL,
        auth=pw.io.elasticsearch.ElasticSearchAuth.basic("admin", "admin"),
        index_name=index_name,
    )
    run()

    assert elasticsearch.document_count(index_name) == n_rows


@pytest.mark.parametrize(
    ("make_documents", "read_batch_size"),
    [
        # Many distinct timestamps, backlog far larger than read_batch_size: read in
        # bounded pages.
        pytest.param(
            lambda base: [{"doc_id": f"d{i}", "ts": base + i} for i in range(50)],
            7,
            id="distinct_timestamps_paginated",
        ),
        # A single timestamp holding more documents than read_batch_size: the query
        # must grow to read the whole bucket.
        pytest.param(
            lambda base: [{"doc_id": f"d{i}", "ts": base} for i in range(25)],
            10,
            id="single_timestamp_exceeds_batch",
        ),
        # Cold start, a single timestamp holding 20x read_batch_size documents: the
        # query grows through several steps until the whole bucket is read at once.
        pytest.param(
            lambda base: [{"doc_id": f"d{i}", "ts": base} for i in range(1000)],
            50,
            id="single_timestamp_20x_cold_start",
        ),
    ],
)
def test_elasticsearch_static_delivers_every_document_once(
    tmp_path, elasticsearch, make_documents, read_batch_size
):
    """In static mode every document is delivered exactly once regardless of how the
    backlog is shaped relative to ``read_batch_size`` — many distinct timestamps
    paged across fetches, or a single hot timestamp far larger than the batch that
    the connector must grow the query to read in full. No loss, no duplicates."""
    documents = make_documents(_now_ms())
    rows = _static_read_doc_ids(
        elasticsearch, tmp_path / "output.jsonl", documents, read_batch_size
    )
    delivered = [row["doc_id"] for row in rows]
    assert all(row["diff"] == 1 for row in rows)
    assert len(delivered) == len(set(delivered)), delivered
    assert set(delivered) == {d["doc_id"] for d in documents}


def test_elasticsearch_streaming_concurrent_writes_no_miss_no_duplicate(
    tmp_path, elasticsearch
):
    """Concurrent writes that land inside ``max_transaction_duration`` — including
    a row committed *late* with an older timestamp than already-delivered rows —
    are all delivered, and none are delivered twice."""
    index_name = elasticsearch.generate_index_name()
    elasticsearch.create_index(index_name, _TIMESTAMP_PROPERTIES)

    output_path = tmp_path / "output.jsonl"
    columns = {"doc_id": "str", "ts": "int"}

    # A generous window keeps every write of the test inside the live edge, so the
    # overlap re-read (and its deduplication) is continuously exercised.
    max_transaction_duration_ms = 60_000

    proc = _start_streaming_worker(
        index_name=index_name,
        columns=columns,
        timestamp_column="ts",
        id_column="doc_id",
        output_path=output_path,
        max_transaction_duration_ms=max_transaction_duration_ms,
    )

    expected_ids: set[str] = set()
    try:
        base_ts = _now_ms()
        # First wave.
        first_wave = [{"doc_id": f"a{i}", "ts": base_ts + i} for i in range(5)]
        elasticsearch.index_documents(index_name, first_wave, id_field="doc_id")
        expected_ids.update(str(d["doc_id"]) for d in first_wave)
        wait_result_with_checker(
            FileLinesNumberChecker(output_path, len(expected_ids)), 120, target=None
        )

        # Second wave, including a "late" writer whose timestamp predates rows
        # that were already delivered — must still be picked up.
        late_ts = base_ts - 5_000
        second_wave = [{"doc_id": "late", "ts": late_ts}] + [
            {"doc_id": f"b{i}", "ts": _now_ms() + i} for i in range(5)
        ]
        elasticsearch.index_documents(index_name, second_wave, id_field="doc_id")
        expected_ids.update(str(d["doc_id"]) for d in second_wave)
        wait_result_with_checker(
            FileLinesNumberChecker(output_path, len(expected_ids)),
            120,
            target=None,
            double_check_interval=3.0,
        )
    finally:
        time.sleep(2.0)
        if proc.poll() is None:
            proc.terminate()
            proc.wait()

    rows = _output_rows(output_path, ["doc_id", "ts"])
    assert all(row["diff"] == 1 for row in rows)
    delivered_ids = [row["doc_id"] for row in rows]
    # No duplicates.
    assert len(delivered_ids) == len(set(delivered_ids)), delivered_ids
    # No missed rows.
    assert set(delivered_ids) == expected_ids


def test_elasticsearch_streaming_detects_late_arrival_below_max_timestamp(
    tmp_path, elasticsearch
):
    """A row committed with a timestamp *below* the current maximum (but inside the
    overlap window) is detected and delivered, even though it does not move the
    maximum timestamp — only the overlap window's row count changes. This exercises
    the ``(max_timestamp, overlap_count)`` change detection: counting only rows at
    the maximum would miss this row and never deliver it."""
    index_name = elasticsearch.generate_index_name()
    elasticsearch.create_index(index_name, _TIMESTAMP_PROPERTIES)
    output_path = tmp_path / "output.jsonl"
    columns = {"doc_id": "str", "ts": "int"}

    # A wide window so every write of the test stays inside the overlap window.
    max_transaction_duration_ms = 60_000

    proc = _start_streaming_worker(
        index_name=index_name,
        columns=columns,
        timestamp_column="ts",
        id_column="doc_id",
        output_path=output_path,
        max_transaction_duration_ms=max_transaction_duration_ms,
    )

    expected_ids: set[str] = set()
    try:
        base_ts = _now_ms()
        # Establish the maximum timestamp at base_ts + 4.
        first_wave = [{"doc_id": f"a{i}", "ts": base_ts + i} for i in range(5)]
        elasticsearch.index_documents(index_name, first_wave, id_field="doc_id")
        expected_ids.update(str(d["doc_id"]) for d in first_wave)
        wait_result_with_checker(
            FileLinesNumberChecker(output_path, len(expected_ids)), 120, target=None
        )

        # A row strictly below the current maximum (base_ts + 4), still inside the
        # window. It leaves the maximum timestamp unchanged, so only the window's
        # overlap count moves — the connector must still pick it up.
        late = {"doc_id": "late_below_max", "ts": base_ts + 2}
        elasticsearch.index_documents(index_name, [late], id_field="doc_id")
        expected_ids.add("late_below_max")
        wait_result_with_checker(
            FileLinesNumberChecker(output_path, len(expected_ids)),
            120,
            target=None,
            double_check_interval=3.0,
        )
    finally:
        time.sleep(2.0)
        if proc.poll() is None:
            proc.terminate()
            proc.wait()

    rows = _output_rows(output_path, ["doc_id"])
    delivered = [row["doc_id"] for row in rows]
    assert len(delivered) == len(set(delivered)), delivered
    assert set(delivered) == expected_ids


def _run_two_run_persistence(
    elasticsearch,
    tmp_path,
    *,
    batch1: list[dict],
    batch2: list[dict],
    max_transaction_duration_ms: int,
    read_batch_size: int | None = None,
) -> list[dict]:
    """Drive two streaming workers sharing one persistent storage and source name.

    ``batch1`` is indexed before run 1; run 1 delivers it and persists its offset
    (the watermark *and* the still-open overlap window of already-delivered rows).
    ``batch2`` is indexed between the runs. Returns the ``doc_id``/``diff`` rows
    delivered by run 2, which the caller checks against ``batch2``."""
    index_name = elasticsearch.generate_index_name()
    elasticsearch.create_index(index_name, _TIMESTAMP_PROPERTIES)
    columns = {"doc_id": "str", "ts": "int"}
    pstorage_path = tmp_path / "PStorage"

    def start(output_path: pathlib.Path) -> subprocess.Popen:
        return _start_streaming_worker(
            index_name=index_name,
            columns=columns,
            timestamp_column="ts",
            id_column="doc_id",
            output_path=output_path,
            max_transaction_duration_ms=max_transaction_duration_ms,
            read_batch_size=read_batch_size,
            pstorage_path=pstorage_path,
            name="es_persistence_source",
        )

    # --- Run 1: deliver batch1 and persist the offset before stopping. ---
    elasticsearch.index_documents(index_name, batch1, id_field="doc_id")
    output_1 = tmp_path / "output_1.jsonl"
    p1 = start(output_1)
    # The flush wait (default 5s) lets the offset (watermark plus the overlap
    # window of delivered-but-unsettled rows) reach persistent storage before kill.
    _wait_and_terminate(FileLinesNumberChecker(output_1, len(batch1)), 120, p1)
    run1_ids = {row["doc_id"] for row in _output_rows(output_1, ["doc_id"])}
    assert run1_ids == {str(d["doc_id"]) for d in batch1}

    # --- Documents written between the runs. ---
    if batch2:
        elasticsearch.index_documents(index_name, batch2, id_field="doc_id")

    # --- Run 2: resumes from the persisted offset; must deliver exactly batch2. ---
    output_2 = tmp_path / "output_2.jsonl"
    p2 = start(output_2)
    if batch2:
        _wait_and_terminate(
            FileLinesNumberChecker(output_2, len(batch2)),
            120,
            p2,
            double_check_interval=3.0,
        )
    else:
        # Nothing new to deliver: run several poll cycles, then confirm run 2
        # produced no output — i.e. it did not re-deliver any already-seen row.
        try:
            time.sleep(8.0)
        finally:
            if p2.poll() is None:
                p2.terminate()
                p2.wait()

    if not output_2.exists():
        return []
    return _output_rows(output_2, ["doc_id"])


def _persist_case_forward_shift(base: int) -> tuple[list[dict], list[dict]]:
    """batch2 carries strictly newer timestamps, moving the global maximum forward."""
    batch1 = [{"doc_id": f"r1_{i}", "ts": base + i} for i in range(4)]
    batch2 = [{"doc_id": f"r2_{i}", "ts": base + 10_000 + i} for i in range(3)]
    return batch1, batch2


def _persist_case_middle_insert_same_max(base: int) -> tuple[list[dict], list[dict]]:
    """The global maximum does not move; batch2 is inserted into the middle of the
    still-open window, below the maximum already seen by run 1."""
    batch1 = [{"doc_id": f"r1_{i}", "ts": base + i * 10} for i in range(6)]
    batch2 = [{"doc_id": f"mid_{i}", "ts": base + 25 + i} for i in range(3)]
    return batch1, batch2


def _persist_case_middle_insert_then_shift(base: int) -> tuple[list[dict], list[dict]]:
    """batch2 both inserts into the still-open window (below run 1's maximum) and
    pushes the global maximum forward, so the older rows settle out of the window."""
    batch1 = [{"doc_id": f"r1_{i}", "ts": base + i * 10} for i in range(6)]
    middle = [{"doc_id": f"mid_{i}", "ts": base + 25 + i} for i in range(2)]
    higher = [{"doc_id": f"hi_{i}", "ts": base + 10_000 + i} for i in range(3)]
    return batch1, middle + higher


def _persist_case_resume_hot_bucket(base: int) -> tuple[list[dict], list[dict]]:
    """batch2 piles many documents onto run 1's last-seen timestamp — far more than
    one read page — without moving the global maximum, so on resume the query must
    grow to read the whole bucket."""
    batch1 = [{"doc_id": f"r1_{i}", "ts": base + i} for i in range(4)]
    batch2 = [{"doc_id": f"hot_{i}", "ts": base + 3} for i in range(30)]
    return batch1, batch2


def _persist_case_resume_hot_bucket_then_shift(
    base: int,
) -> tuple[list[dict], list[dict]]:
    """Like ``resume_hot_bucket`` but batch2 also adds far-future documents, so after
    the over-sized bucket is read the global maximum jumps and that bucket settles
    out of the window within the same run."""
    batch1 = [{"doc_id": f"r1_{i}", "ts": base + i} for i in range(4)]
    hot = [{"doc_id": f"hot_{i}", "ts": base + 3} for i in range(30)]
    future = [{"doc_id": f"fut_{i}", "ts": base + 20_000 + i} for i in range(3)]
    return batch1, hot + future


def _persist_case_empty_delta(base: int) -> tuple[list[dict], list[dict]]:
    """Nothing is written between the runs: run 2 must deliver nothing at all."""
    batch1 = [{"doc_id": f"r1_{i}", "ts": base + i} for i in range(4)]
    return batch1, []


@pytest.mark.parametrize(
    ("make_batches", "max_transaction_duration_ms", "read_batch_size"),
    [
        pytest.param(
            _persist_case_forward_shift, 2_000, None, id="second_batch_shifts_max"
        ),
        pytest.param(
            _persist_case_middle_insert_same_max,
            60_000,
            None,
            id="middle_insert_max_unchanged",
        ),
        pytest.param(
            _persist_case_middle_insert_then_shift,
            2_000,
            None,
            id="middle_insert_then_shift",
        ),
        pytest.param(
            _persist_case_resume_hot_bucket,
            60_000,
            10,
            id="resume_hot_bucket_exceeds_page",
        ),
        pytest.param(
            _persist_case_resume_hot_bucket_then_shift,
            2_000,
            10,
            id="resume_hot_bucket_then_settle",
        ),
        pytest.param(
            _persist_case_empty_delta, 2_000, None, id="empty_delta_no_redelivery"
        ),
    ],
)
def test_elasticsearch_streaming_persistence_delivers_only_delta(
    tmp_path,
    elasticsearch,
    make_batches,
    max_transaction_duration_ms,
    read_batch_size,
):
    """Restart/resume across two workers sharing one persistent storage: run 1
    delivers ``batch1`` and persists its offset (watermark plus the still-open
    overlap window). After a restart, run 2 must deliver exactly ``batch2`` — every
    newly written document once, with no re-delivery of rows already delivered by
    run 1 and no loss — no matter how ``batch2`` relates to ``batch1``: newer
    timestamps, insertions into the still-open window with or without a later
    global-maximum shift, a hot timestamp bucket larger than one read page (with and
    without a subsequent shift that settles it), or no new documents at all."""
    base = _now_ms()
    batch1, batch2 = make_batches(base)
    rows = _run_two_run_persistence(
        elasticsearch,
        tmp_path,
        batch1=batch1,
        batch2=batch2,
        max_transaction_duration_ms=max_transaction_duration_ms,
        read_batch_size=read_batch_size,
    )

    assert all(row["diff"] == 1 for row in rows)
    delivered = sorted(str(row["doc_id"]) for row in rows)
    expected = sorted(str(d["doc_id"]) for d in batch2)
    # Run 2 delivered exactly the inter-run delta...
    assert delivered == expected, delivered
    # ...and nothing twice.
    assert len(delivered) == len(set(delivered)), delivered
