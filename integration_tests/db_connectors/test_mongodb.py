import json
import os
import pathlib
import struct
import subprocess
import sys
import textwrap
import time
from typing import Literal

import lz4.block
import pytest
from utils import MONGODB_BASE_NAME, MONGODB_CONNECTION_STRING, MongoDBContext

import pathway as pw
from pathway.internals.parse_graph import G
from pathway.tests.utils import (
    ExceptionAwareThread,
    FileLinesNumberChecker,
    read_jsonlines,
    run,
    wait_result_with_checker,
)

pytestmark = [
    pytest.mark.xdist_group("mongodb"),
    pytest.mark.flaky(reruns=2),
]

# ---------------------------------------------------------------------------
# Script run inside a fresh subprocess for streaming persistence tests.
# Using subprocess.Popen (not fork) avoids inheriting Rust/tokio global state
# that pw.run() leaves behind in the test process.
# ---------------------------------------------------------------------------
_STREAMING_WORKER_SCRIPT = textwrap.dedent(
    """\
    import json
    import sys

    cfg = json.loads(sys.argv[1])

    import pathway as pw
    from pathway.internals.parse_graph import G
    from pathway.tests.utils import run

    class InputSchema(pw.Schema):
        product: str
        qty: int

    G.clear()
    table = pw.io.mongodb.read(
        connection_string=cfg["connection_string"],
        database=cfg["database"],
        collection=cfg["collection"],
        schema=InputSchema,
        mode="streaming",
        name="mongodb_streaming_persistence_source",
    )
    pw.io.jsonlines.write(table, cfg["output_path"])
    persistence_config = pw.persistence.Config(
        backend=pw.persistence.Backend.filesystem(cfg["pstorage_path"])
    )
    run(persistence_config=persistence_config)
    """
)


def _start_streaming_worker(
    output_path: pathlib.Path,
    pstorage_path: pathlib.Path,
    input_collection: str,
) -> subprocess.Popen:
    """Start a fresh Python subprocess running the streaming persistence worker."""
    cfg = json.dumps(
        {
            "connection_string": MONGODB_CONNECTION_STRING,
            "database": MONGODB_BASE_NAME,
            "collection": input_collection,
            "output_path": str(output_path),
            "pstorage_path": str(pstorage_path),
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
    while True:
        time.sleep(0.1)
        elapsed = time.monotonic() - start
        if elapsed >= timeout_sec:
            proc.terminate()
            proc.wait()
            raise AssertionError(
                f"Timed out after {timeout_sec}s. "
                f"{checker.provide_information_on_failure()}"
            )
        if checker():
            if double_check_interval is not None:
                time.sleep(double_check_interval)
                if not checker():
                    proc.terminate()
                    proc.wait()
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
    proc.terminate()
    proc.wait()


def write_items_with_connector(
    *,
    mongodb: MongoDBContext,
    test_items: list[dict],
    input_path: pathlib.Path,
    schema: type[pw.Schema],
    output_collection: str,
    output_table_type: Literal["stream_of_changes", "snapshot"],
    persistence_config: pw.persistence.Config | None = None,
) -> list[dict]:
    G.clear()
    with open(input_path, "w") as f:
        for test_item in test_items:
            f.write(json.dumps(test_item) + "\n")
    table = pw.io.jsonlines.read(input_path, schema=schema, mode="static")
    pw.io.mongodb.write(
        table,
        connection_string=MONGODB_CONNECTION_STRING,
        database=MONGODB_BASE_NAME,
        collection=output_collection,
        output_table_type=output_table_type,
    )
    pw.run(persistence_config=persistence_config)

    result = mongodb.get_collection(output_collection, schema.column_names())
    result.sort(key=lambda item: (item["name"], item["available"]))
    return result


def check_special_fields(
    mongodb: MongoDBContext, output_collection: str, *, are_expected: bool
):
    full_collection = mongodb.get_full_collection(output_collection)
    for document in full_collection:
        time_in_document = "time" in document
        diff_in_document = "diff" in document
        assert time_in_document == are_expected, document
        assert diff_in_document == are_expected, document


def test_mongodb_stream_of_changes(tmp_path, mongodb):
    class InputSchema(pw.Schema):
        name: str
        count: int
        price: float
        available: bool

    input_path = tmp_path / "input.txt"
    output_collection = mongodb.generate_collection_name()
    test_items = [
        {"name": "Milk", "count": 500, "price": 1.5, "available": False},
        {"name": "Water", "count": 600, "price": 0.5, "available": True},
    ]
    result = write_items_with_connector(
        mongodb=mongodb,
        test_items=test_items,
        input_path=input_path,
        schema=InputSchema,
        output_collection=output_collection,
        output_table_type="stream_of_changes",
    )
    assert result == test_items
    check_special_fields(mongodb, output_collection, are_expected=True)

    new_test_items = [{"name": "Milk", "count": 500, "price": 1.5, "available": True}]
    result = write_items_with_connector(
        mongodb=mongodb,
        test_items=new_test_items,
        input_path=input_path,
        schema=InputSchema,
        output_collection=output_collection,
        output_table_type="stream_of_changes",
    )
    expected_result = [
        {"name": "Milk", "count": 500, "price": 1.5, "available": False},
        {"name": "Milk", "count": 500, "price": 1.5, "available": True},
        {"name": "Water", "count": 600, "price": 0.5, "available": True},
    ]
    assert result == expected_result
    check_special_fields(mongodb, output_collection, are_expected=True)


def test_mongodb_snapshot(tmp_path, mongodb):
    class InputSchema(pw.Schema):
        name: str = pw.column_definition(primary_key=True)
        count: int
        price: float
        available: bool

    input_path = tmp_path / "input.txt"
    output_collection = mongodb.generate_collection_name()

    test_items = [
        {"name": "Milk", "count": 500, "price": 1.5, "available": False},
        {"name": "Water", "count": 600, "price": 0.5, "available": True},
    ]
    result = write_items_with_connector(
        mongodb=mongodb,
        test_items=test_items,
        input_path=input_path,
        schema=InputSchema,
        output_collection=output_collection,
        output_table_type="snapshot",
    )
    assert result == test_items
    check_special_fields(mongodb, output_collection, are_expected=False)

    new_test_items = [{"name": "Milk", "count": 500, "price": 1.5, "available": True}]
    result = write_items_with_connector(
        mongodb=mongodb,
        test_items=new_test_items,
        input_path=input_path,
        schema=InputSchema,
        output_collection=output_collection,
        output_table_type="snapshot",
    )
    expected_result = [
        {"name": "Milk", "count": 500, "price": 1.5, "available": True},
        {"name": "Water", "count": 600, "price": 0.5, "available": True},
    ]
    assert result == expected_result
    check_special_fields(mongodb, output_collection, are_expected=False)


def test_mongodb_snapshot_remove(tmp_path, mongodb):
    class InputSchema(pw.Schema):
        name: str = pw.column_definition(primary_key=True)
        count: int
        price: float
        available: bool

    input_path = tmp_path / "input.txt"
    pstorage_path = tmp_path / "PStorage"
    persistence_config = pw.persistence.Config(
        backend=pw.persistence.Backend.filesystem(pstorage_path)
    )

    output_collection = mongodb.generate_collection_name()
    test_items = [
        {"name": "Milk", "count": 500, "price": 1.5, "available": False},
        {"name": "Water", "count": 600, "price": 0.5, "available": True},
    ]
    result = write_items_with_connector(
        mongodb=mongodb,
        test_items=test_items,
        input_path=input_path,
        schema=InputSchema,
        output_collection=output_collection,
        output_table_type="snapshot",
        persistence_config=persistence_config,
    )
    assert result == test_items
    check_special_fields(mongodb, output_collection, are_expected=False)

    test_items = [
        {"name": "Water", "count": 600, "price": 0.5, "available": True},
    ]
    result = write_items_with_connector(
        mongodb=mongodb,
        test_items=test_items,
        input_path=input_path,
        schema=InputSchema,
        output_collection=output_collection,
        output_table_type="snapshot",
        persistence_config=persistence_config,
    )
    assert result == test_items
    check_special_fields(mongodb, output_collection, are_expected=False)


# ---------------------------------------------------------------------------
# Persistence plans for test_mongodb_read_persistence
#
# Each plan describes:
#   initial   – documents to insert into MongoDB before the first run
#   changes   – sequence of (op, filter, doc) tuples applied between the runs
#               op is "insert", "delete", or "replace"
#   run1_expected – list of {"product", "qty", "diff"} dicts after the first run
#   run2_expected – list of {"product", "qty", "diff"} dicts after the second run
# ---------------------------------------------------------------------------
_READ_PERSISTENCE_PLANS = [
    pytest.param(
        {
            "initial": [
                {"product": "Apple", "qty": 10},
                {"product": "Banana", "qty": 5},
            ],
            "changes": [
                ("insert", None, {"product": "Cherry", "qty": 20}),
                ("insert", None, {"product": "Durian", "qty": 2}),
            ],
            "run1_expected": [
                {"product": "Apple", "qty": 10, "diff": 1},
                {"product": "Banana", "qty": 5, "diff": 1},
            ],
            "run2_expected": [
                {"product": "Cherry", "qty": 20, "diff": 1},
                {"product": "Durian", "qty": 2, "diff": 1},
            ],
        },
        id="inserts_only",
    ),
    pytest.param(
        {
            "initial": [
                {"product": "Apple", "qty": 10},
                {"product": "Banana", "qty": 5},
                {"product": "Cherry", "qty": 20},
            ],
            "changes": [
                ("delete", {"product": "Banana"}, None),
            ],
            "run1_expected": [
                {"product": "Apple", "qty": 10, "diff": 1},
                {"product": "Banana", "qty": 5, "diff": 1},
                {"product": "Cherry", "qty": 20, "diff": 1},
            ],
            "run2_expected": [
                {"product": "Banana", "qty": 5, "diff": -1},
            ],
        },
        id="deletes_only",
    ),
    pytest.param(
        {
            "initial": [
                {"product": "Apple", "qty": 10},
                {"product": "Banana", "qty": 5},
            ],
            "changes": [
                ("replace", {"product": "Apple"}, {"product": "Apple", "qty": 99}),
            ],
            "run1_expected": [
                {"product": "Apple", "qty": 10, "diff": 1},
                {"product": "Banana", "qty": 5, "diff": 1},
            ],
            "run2_expected": [
                {"product": "Apple", "qty": 10, "diff": -1},
                {"product": "Apple", "qty": 99, "diff": 1},
            ],
        },
        id="replacements_only",
    ),
    pytest.param(
        {
            "initial": [
                {"product": "Apple", "qty": 10},
                {"product": "Banana", "qty": 5},
                {"product": "Cherry", "qty": 20},
            ],
            "changes": [
                ("insert", None, {"product": "Durian", "qty": 2}),
                ("delete", {"product": "Banana"}, None),
                ("replace", {"product": "Cherry"}, {"product": "Cherry", "qty": 18}),
            ],
            "run1_expected": [
                {"product": "Apple", "qty": 10, "diff": 1},
                {"product": "Banana", "qty": 5, "diff": 1},
                {"product": "Cherry", "qty": 20, "diff": 1},
            ],
            "run2_expected": [
                {"product": "Banana", "qty": 5, "diff": -1},
                {"product": "Cherry", "qty": 20, "diff": -1},
                {"product": "Cherry", "qty": 18, "diff": 1},
                {"product": "Durian", "qty": 2, "diff": 1},
            ],
        },
        id="mixed",
    ),
    pytest.param(
        {
            "initial": [
                {"product": "Apple", "qty": 10},
                {"product": "Banana", "qty": 5},
                {"product": "Cherry", "qty": 20},
                {"product": "Durian", "qty": 2},
                {"product": "Elderberry", "qty": 15},
                {"product": "Fig", "qty": 12},
                {"product": "Grape", "qty": 30},
                {"product": "Honeydew", "qty": 8},
                {"product": "Jackfruit", "qty": 3},
                {"product": "Kiwi", "qty": 25},
            ],
            "changes": [
                ("insert", None, {"product": "Lemon", "qty": 18}),
                ("delete", {"product": "Banana"}, None),
                ("replace", {"product": "Apple"}, {"product": "Apple", "qty": 99}),
                ("insert", None, {"product": "Mango", "qty": 7}),
                ("delete", {"product": "Durian"}, None),
                ("replace", {"product": "Cherry"}, {"product": "Cherry", "qty": 22}),
                ("delete", {"product": "Grape"}, None),
                ("insert", None, {"product": "Nectarine", "qty": 14}),
                ("replace", {"product": "Kiwi"}, {"product": "Kiwi", "qty": 11}),
                ("insert", None, {"product": "Orange", "qty": 6}),
            ],
            "run1_expected": [
                {"product": "Apple", "qty": 10, "diff": 1},
                {"product": "Banana", "qty": 5, "diff": 1},
                {"product": "Cherry", "qty": 20, "diff": 1},
                {"product": "Durian", "qty": 2, "diff": 1},
                {"product": "Elderberry", "qty": 15, "diff": 1},
                {"product": "Fig", "qty": 12, "diff": 1},
                {"product": "Grape", "qty": 30, "diff": 1},
                {"product": "Honeydew", "qty": 8, "diff": 1},
                {"product": "Jackfruit", "qty": 3, "diff": 1},
                {"product": "Kiwi", "qty": 25, "diff": 1},
            ],
            "run2_expected": [
                {"product": "Apple", "qty": 10, "diff": -1},
                {"product": "Apple", "qty": 99, "diff": 1},
                {"product": "Banana", "qty": 5, "diff": -1},
                {"product": "Cherry", "qty": 20, "diff": -1},
                {"product": "Cherry", "qty": 22, "diff": 1},
                {"product": "Durian", "qty": 2, "diff": -1},
                {"product": "Grape", "qty": 30, "diff": -1},
                {"product": "Kiwi", "qty": 11, "diff": 1},
                {"product": "Kiwi", "qty": 25, "diff": -1},
                {"product": "Lemon", "qty": 18, "diff": 1},
                {"product": "Mango", "qty": 7, "diff": 1},
                {"product": "Nectarine", "qty": 14, "diff": 1},
                {"product": "Orange", "qty": 6, "diff": 1},
            ],
        },
        id="long_mixed",
    ),
    pytest.param(
        {
            "initial": [
                {"product": "Apple", "qty": 10},
                {"product": "Banana", "qty": 5},
            ],
            "changes": [],
            "run1_expected": [
                {"product": "Apple", "qty": 10, "diff": 1},
                {"product": "Banana", "qty": 5, "diff": 1},
            ],
            "run2_expected": [],
        },
        id="no_changes",
    ),
]


def _extract_row(r: dict) -> dict:
    return {"product": r["product"], "qty": r["qty"], "diff": r["diff"]}


def _sort_rows(rows: list[dict]) -> list[dict]:
    return sorted(rows, key=lambda r: (r["product"], r["qty"], r["diff"]))


@pytest.mark.parametrize("plan", _READ_PERSISTENCE_PLANS)
def test_mongodb_streaming_persistence(tmp_path, mongodb, plan):
    """Two-run persistence test for pw.io.mongodb.read in streaming mode.

    Run 1: the full collection snapshot appears in the output (all diff=+1).
    Run 2: only the delta since the previous run appears in the output.

    Each run is a fresh subprocess.Popen so no Rust/tokio global state from
    prior pw.run() calls (or daemon threads) in the test process is inherited.
    """
    pstorage_path = tmp_path / "PStorage"
    input_collection = mongodb.generate_collection_name()

    for doc in plan["initial"]:
        mongodb.insert_document(input_collection, doc)

    # --- Run 1: full collection must appear in the streaming output ---
    output_path_1 = tmp_path / "output_1.jsonl"
    run1_expected = plan["run1_expected"]
    p1 = _start_streaming_worker(output_path_1, pstorage_path, input_collection)
    try:
        _wait_and_terminate(
            FileLinesNumberChecker(output_path_1, len(run1_expected)), 30, p1
        )
    finally:
        if p1.poll() is None:
            p1.terminate()
            p1.wait()

    assert _sort_rows(
        [_extract_row(r) for r in read_jsonlines(output_path_1)]
    ) == _sort_rows(run1_expected), f"Run 1: expected {run1_expected}"

    # Apply changes to the MongoDB collection between the two runs.
    for op, filter_doc, new_doc in plan["changes"]:
        if op == "insert":
            mongodb.insert_document(input_collection, new_doc)
        elif op == "delete":
            mongodb.delete_document(input_collection, filter_doc)
        elif op == "replace":
            mongodb.replace_document(input_collection, filter_doc, new_doc)

    # --- Run 2: only the delta since Run 1 must appear ---
    output_path_2 = tmp_path / "output_2.jsonl"
    run2_expected = plan["run2_expected"]
    p2 = _start_streaming_worker(output_path_2, pstorage_path, input_collection)
    try:
        if run2_expected:
            _wait_and_terminate(
                FileLinesNumberChecker(output_path_2, len(run2_expected)), 30, p2
            )
        else:
            # no_changes: pre-create the output file so the checker can verify it
            # stays empty. double_check_interval gives the engine time to confirm
            # no events arrive before we declare success.
            output_path_2.touch()
            _wait_and_terminate(
                FileLinesNumberChecker(output_path_2, 0),
                30,
                p2,
                double_check_interval=3.0,
            )
    finally:
        if p2.poll() is None:
            p2.terminate()
            p2.wait()

    assert _sort_rows(
        [_extract_row(r) for r in read_jsonlines(output_path_2)]
    ) == _sort_rows(run2_expected), f"Run 2: expected {run2_expected}"


@pytest.mark.parametrize("plan", _READ_PERSISTENCE_PLANS)
def test_mongodb_read_persistence(tmp_path, mongodb, plan):
    """Two-run persistence test for pw.io.mongodb.read in static mode.

    Run 1: the full collection is reflected in the output (all diff=+1).
    Run 2: only the delta since the previous run appears in the output.
    """

    class InputSchema(pw.Schema):
        product: str
        qty: int

    pstorage_path = tmp_path / "PStorage"
    persistence_config = pw.persistence.Config(
        backend=pw.persistence.Backend.filesystem(pstorage_path)
    )
    input_collection = mongodb.generate_collection_name()

    for doc in plan["initial"]:
        mongodb.insert_document(input_collection, doc)

    def run_read(output_path: pathlib.Path) -> None:
        G.clear()
        table = pw.io.mongodb.read(
            connection_string=MONGODB_CONNECTION_STRING,
            database=MONGODB_BASE_NAME,
            collection=input_collection,
            schema=InputSchema,
            mode="static",
            name="mongodb_read_persistence_source",
        )
        pw.io.jsonlines.write(table, output_path)
        run(persistence_config=persistence_config)

    # --- Run 1: full collection must be reflected ---
    output_path_1 = tmp_path / "output_1.jsonl"
    run_read(output_path_1)
    run1_rows = read_jsonlines(output_path_1)
    assert _sort_rows([_extract_row(r) for r in run1_rows]) == _sort_rows(
        plan["run1_expected"]
    ), f"Run 1: expected {plan['run1_expected']}, got {run1_rows}"

    # Apply changes to the MongoDB collection between the two runs.
    for op, filter_doc, new_doc in plan["changes"]:
        if op == "insert":
            mongodb.insert_document(input_collection, new_doc)
        elif op == "delete":
            mongodb.delete_document(input_collection, filter_doc)
        elif op == "replace":
            mongodb.replace_document(input_collection, filter_doc, new_doc)

    # --- Run 2: only the delta must appear ---
    output_path_2 = tmp_path / "output_2.jsonl"
    run_read(output_path_2)
    run2_rows = read_jsonlines(output_path_2)
    assert _sort_rows([_extract_row(r) for r in run2_rows]) == _sort_rows(
        plan["run2_expected"]
    ), f"Run 2: expected {plan['run2_expected']}, got {run2_rows}"


def _corrupt_mongodb_resume_token(pstorage_path: pathlib.Path) -> None:
    """Overwrite the MongoDbOplogToken bytes in persistence snapshot files with 0xFF.

    Snapshot files are LZ4-compressed bincode sequences of Event enums.  The
    relevant pattern in the decompressed bytes is:

        OffsetKey::MongoDb      (u32 LE variant 4)  -> 04 00 00 00
        OffsetValue::MongoDbOplogToken (u32 LE variant 11) -> 0b 00 00 00
        token length            (u64 LE)             -> 8 bytes
        token bytes             (Vec<u8>)             -> <length> bytes   <- corrupted

    Replacing the token bytes with 0xFF makes the BSON invalid, so
    bson::from_slice will fail in initialize() and the connector returns
    ReadError::MalformedData.
    """
    # bincode encodes OffsetKey::MongoDb (variant 4) then
    # OffsetValue::MongoDbOplogToken (variant 11) as consecutive u32 LE values.
    PATTERN = b"\x04\x00\x00\x00\x0b\x00\x00\x00"

    streams_dir = pstorage_path / "streams"
    corrupted = False
    for chunk_file in streams_dir.rglob("*"):
        if not chunk_file.is_file():
            continue
        try:
            int(chunk_file.name)  # snapshot chunks are named by numeric id
        except ValueError:
            continue

        raw = chunk_file.read_bytes()
        orig_size = struct.unpack_from("<I", raw)[0]
        data = bytearray(lz4.block.decompress(raw[4:], uncompressed_size=orig_size))

        # Corrupt every occurrence in this chunk (multiple AdvanceTime events
        # may each carry a token; the engine uses the latest one it finds).
        offset = 0
        while True:
            pos = data.find(PATTERN, offset)
            if pos == -1:
                break

            # u64 LE token length immediately follows the 8-byte pattern
            token_len = struct.unpack_from("<Q", data, pos + len(PATTERN))[0]
            token_start = pos + len(PATTERN) + 8
            for i in range(token_start, token_start + token_len):
                data[i] = 0xFF
            corrupted = True
            offset = token_start + token_len

        if corrupted:
            recompressed = lz4.block.compress(bytes(data), store_size=False)
            chunk_file.write_bytes(struct.pack("<I", len(data)) + recompressed)

    if not corrupted:
        raise AssertionError("No MongoDB resume token found in persistence files")


def test_mongodb_invalid_resume_token(tmp_path, mongodb):
    """A corrupted resume token must produce a clear error on the next run.

    After a successful first run the persisted oplog token is overwritten with
    invalid bytes.  The second run must fail rather than silently producing
    wrong output or hanging.
    """

    class InputSchema(pw.Schema):
        product: str
        qty: int

    pstorage_path = tmp_path / "PStorage"
    persistence_config = pw.persistence.Config(
        backend=pw.persistence.Backend.filesystem(pstorage_path)
    )
    input_collection = mongodb.generate_collection_name()
    mongodb.insert_document(input_collection, {"product": "Apple", "qty": 10})
    mongodb.insert_document(input_collection, {"product": "Banana", "qty": 5})

    def run_read(output_path: pathlib.Path) -> None:
        G.clear()
        table = pw.io.mongodb.read(
            connection_string=MONGODB_CONNECTION_STRING,
            database=MONGODB_BASE_NAME,
            collection=input_collection,
            schema=InputSchema,
            mode="static",
            name="mongodb_invalid_token_source",
        )
        pw.io.jsonlines.write(table, output_path)
        run(persistence_config=persistence_config)

    # Run 1: populate persistence with a valid resume token.
    output_path_1 = tmp_path / "output_1.jsonl"
    run_read(output_path_1)
    assert len(read_jsonlines(output_path_1)) == 2

    # Corrupt the persisted token so it is no longer valid BSON.
    _corrupt_mongodb_resume_token(pstorage_path)

    # Run 2: must raise with the specific "corrupt token" message so the user
    # knows to delete the persistence directory rather than getting silent
    # wrong output or a generic crash.
    output_path_2 = tmp_path / "output_2.jsonl"
    with pytest.raises(Exception, match="corrupt and cannot be decoded"):
        run_read(output_path_2)


def test_mongodb_streaming_no_primary_key(tmp_path, mongodb):
    # Schema intentionally has no primary key — Pathway will derive the row key
    # from the MongoDB _id, making this a pure stream-of-changes view.
    class InputSchema(pw.Schema):
        product: str
        quantity: int
        price: float

    output_path = tmp_path / "output.txt"
    input_collection = mongodb.generate_collection_name()

    # Sequence of 10 operations mixing inserts, replacements, and deletes.
    # A replacement in Upsert session mode causes Pathway to retract the old
    # row and emit the new one, producing 2 output lines.
    # An insert or delete produces 1 output line.
    # Cumulative expected line totals are listed after each entry.
    operations = [
        # op,          args,                                            lines_after
        ("insert", {"product": "Apple", "quantity": 10, "price": 1.50}),  # 1
        ("insert", {"product": "Banana", "quantity": 5, "price": 0.80}),  # 2
        ("insert", {"product": "Cherry", "quantity": 20, "price": 3.00}),  # 3
        (
            "replace",
            {"product": "Apple"},
            {"product": "Apple", "quantity": 15, "price": 1.50},
        ),  # 5
        ("insert", {"product": "Durian", "quantity": 2, "price": 5.00}),  # 6
        ("delete", {"product": "Banana"}),  # 7
        ("insert", {"product": "Elderberry", "quantity": 15, "price": 4.50}),  # 8
        (
            "replace",
            {"product": "Cherry"},
            {"product": "Cherry", "quantity": 18, "price": 3.20},
        ),  # 10
        ("delete", {"product": "Durian"}),  # 11
        ("insert", {"product": "Fig", "quantity": 12, "price": 1.80}),  # 12
    ]
    # Cumulative output line counts that follow from the above.
    cumulative_lines = [1, 2, 3, 5, 6, 7, 8, 10, 11, 12]

    def streaming_target():
        for op, expected_lines in zip(operations, cumulative_lines):
            if op[0] == "insert":
                mongodb.insert_document(input_collection, op[1])
            elif op[0] == "replace":
                mongodb.replace_document(input_collection, op[1], op[2])
            elif op[0] == "delete":
                mongodb.delete_document(input_collection, op[1])
            wait_result_with_checker(
                FileLinesNumberChecker(output_path, expected_lines), 30, target=None
            )

    streaming_thread = ExceptionAwareThread(target=streaming_target, daemon=True)
    streaming_thread.start()

    table = pw.io.mongodb.read(
        connection_string=MONGODB_CONNECTION_STRING,
        database=MONGODB_BASE_NAME,
        collection=input_collection,
        schema=InputSchema,
        mode="streaming",
    )
    pw.io.jsonlines.write(table, output_path)
    # Fails with fork
    pathway_thread = ExceptionAwareThread(target=run, daemon=True)
    pathway_thread.start()
    wait_result_with_checker(FileLinesNumberChecker(output_path, 12), 30, target=None)
    streaming_thread.join()
