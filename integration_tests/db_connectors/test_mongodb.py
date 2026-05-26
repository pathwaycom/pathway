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


# ---------------------------------------------------------------------------
# Reserved-column validation for pw.io.mongodb.write.
#
# - `_id` is reserved in both output modes because MongoDB uses `_id` as every
#   document's primary key.
# - `diff` and `time` are reserved in `stream_of_changes` mode because the
#   writer appends those two metadata fields to every document.
#
# These checks happen at construction time so that misconfigurations surface
# before pw.run() rather than as cryptic MongoDB server errors.
# ---------------------------------------------------------------------------
def test_mongodb_write_reserved_column_diff_rejected(tmp_path):
    class InputSchema(pw.Schema):
        name: str
        diff: int

    G.clear()
    input_path = tmp_path / "input.txt"
    with open(input_path, "w") as f:
        f.write(json.dumps({"name": "x", "diff": 1}) + "\n")
    table = pw.io.jsonlines.read(input_path, schema=InputSchema, mode="static")

    with pytest.raises(ValueError, match="diff"):
        pw.io.mongodb.write(
            table,
            connection_string=MONGODB_CONNECTION_STRING,
            database=MONGODB_BASE_NAME,
            collection="unused",
            output_table_type="stream_of_changes",
        )


def test_mongodb_write_reserved_column_time_rejected(tmp_path):
    class InputSchema(pw.Schema):
        name: str
        time: int

    G.clear()
    input_path = tmp_path / "input.txt"
    with open(input_path, "w") as f:
        f.write(json.dumps({"name": "x", "time": 1}) + "\n")
    table = pw.io.jsonlines.read(input_path, schema=InputSchema, mode="static")

    with pytest.raises(ValueError, match="time"):
        pw.io.mongodb.write(
            table,
            connection_string=MONGODB_CONNECTION_STRING,
            database=MONGODB_BASE_NAME,
            collection="unused",
            output_table_type="stream_of_changes",
        )


@pytest.mark.parametrize("mode", ["snapshot", "stream_of_changes"])
def test_mongodb_write_rejects_user_id_column(tmp_path, mode):
    class InputSchema(pw.Schema):
        name: str = pw.column_definition(primary_key=True)
        _id: int

    G.clear()
    input_path = tmp_path / "input.txt"
    with open(input_path, "w") as f:
        f.write(json.dumps({"name": "x", "_id": 1}) + "\n")
    table = pw.io.jsonlines.read(input_path, schema=InputSchema, mode="static")

    with pytest.raises(ValueError, match="_id"):
        pw.io.mongodb.write(
            table,
            connection_string=MONGODB_CONNECTION_STRING,
            database=MONGODB_BASE_NAME,
            collection="unused",
            output_table_type=mode,
        )


def test_mongodb_write_reserved_column_allowed_in_snapshot(tmp_path, mongodb):
    """``diff`` and ``time`` are only reserved in ``stream_of_changes`` mode.
    In ``snapshot`` mode they are regular user columns and must be preserved."""

    class InputSchema(pw.Schema):
        name: str = pw.column_definition(primary_key=True)
        diff: int
        time: int

    G.clear()
    input_path = tmp_path / "input.txt"
    with open(input_path, "w") as f:
        f.write(json.dumps({"name": "Milk", "diff": 42, "time": 99999}) + "\n")
    output_collection = mongodb.generate_collection_name()
    table = pw.io.jsonlines.read(input_path, schema=InputSchema, mode="static")
    pw.io.mongodb.write(
        table,
        connection_string=MONGODB_CONNECTION_STRING,
        database=MONGODB_BASE_NAME,
        collection=output_collection,
        output_table_type="snapshot",
    )
    pw.run()

    rows = mongodb.get_full_collection(output_collection)
    assert len(rows) == 1
    assert rows[0]["diff"] == 42
    assert rows[0]["time"] == 99999


# ---------------------------------------------------------------------------
# `stream_of_changes` mode promises a complete history of modifications: an
# update on the source is forwarded as two events in the same minibatch —
# first the old row with `diff = -1`, then the new row with `diff = 1`.  This
# test uses a MongoDB source and sink, replaces a document, and verifies that
# both events reach the destination collection.
# ---------------------------------------------------------------------------
def test_mongodb_write_stream_of_changes_preserves_retractions(tmp_path, mongodb):
    class InputSchema(pw.Schema):
        product: str
        qty: int

    src_collection = mongodb.generate_collection_name()
    dst_collection = mongodb.generate_collection_name()
    mongodb.insert_document(src_collection, {"product": "apple", "qty": 10})

    output_path = tmp_path / "output.txt"

    def driver():
        class Checker:
            def __init__(self, n):
                self.n = n

            def __call__(self):
                return (
                    mongodb.client[MONGODB_BASE_NAME][dst_collection].count_documents(
                        {}
                    )
                    >= self.n
                )

        wait_result_with_checker(Checker(1), 30, target=None)
        mongodb.replace_document(
            src_collection,
            {"product": "apple"},
            {"product": "apple", "qty": 99},
        )
        wait_result_with_checker(Checker(3), 30, target=None)

    G.clear()
    src = pw.io.mongodb.read(
        connection_string=MONGODB_CONNECTION_STRING,
        database=MONGODB_BASE_NAME,
        collection=src_collection,
        schema=InputSchema,
        mode="streaming",
    )
    pw.io.mongodb.write(
        src,
        connection_string=MONGODB_CONNECTION_STRING,
        database=MONGODB_BASE_NAME,
        collection=dst_collection,
        output_table_type="stream_of_changes",
    )
    pw.io.jsonlines.write(src, output_path)

    drv = ExceptionAwareThread(target=driver, daemon=True)
    drv.start()
    pt = ExceptionAwareThread(target=run, daemon=True)
    pt.start()
    drv.join(timeout=60)

    rows = mongodb.get_full_collection(dst_collection)
    diffs = sorted(r["diff"] for r in rows)
    assert diffs == [-1, 1, 1], f"expected diffs [-1, 1, 1], got {diffs}: rows={rows}"
    neg = [r for r in rows if r["diff"] == -1]
    assert neg and neg[0]["qty"] == 10, f"retraction should carry qty=10: {neg}"


# ---------------------------------------------------------------------------
# BSON type-conversion spot checks that mirror the reference table in
# `290.mongodb.rst`.  These complement the richer round-trip coverage in
# test_mongodb_parsing.py with focused assertions for edge cases.
# ---------------------------------------------------------------------------
def test_mongodb_read_int64_as_float(mongodb):
    """BSON ``Int64`` widens to Pathway ``float`` when the schema declares float."""
    from bson import Int64

    coll = mongodb.generate_collection_name()
    mongodb.client[MONGODB_BASE_NAME][coll].insert_one({"pkey": 1, "v": Int64(42)})

    class S(pw.Schema):
        pkey: int
        v: float

    G.clear()
    t = pw.io.mongodb.read(
        connection_string=MONGODB_CONNECTION_STRING,
        database=MONGODB_BASE_NAME,
        collection=coll,
        schema=S,
        mode="static",
    )
    rows = []

    class Obs(pw.io.python.ConnectorObserver):
        def on_change(self, key, row, time, is_addition):
            if is_addition:
                rows.append(dict(row))

        def on_end(self):
            pass

    pw.io.python.write(t, Obs())
    run()
    assert rows == [{"pkey": 1, "v": 42.0}]


def test_mongodb_read_empty_collection_static(mongodb, tmp_path):
    """Reading an empty collection in static mode yields zero rows without error."""

    class S(pw.Schema):
        x: int

    coll = mongodb.generate_collection_name()
    mongodb.client[MONGODB_BASE_NAME].create_collection(coll)

    G.clear()
    out = tmp_path / "out.jsonl"
    out.write_text("")
    t = pw.io.mongodb.read(
        connection_string=MONGODB_CONNECTION_STRING,
        database=MONGODB_BASE_NAME,
        collection=coll,
        schema=S,
        mode="static",
    )
    pw.io.jsonlines.write(t, out)
    run()
    assert out.read_text() == ""


def test_mongodb_write_snapshot_max_batch_size(mongodb):
    """Every row is delivered even when ``max_batch_size`` forces intermediate
    flushes that split a single minibatch across several MongoDB round-trips."""

    class S(pw.Schema):
        k: int = pw.column_definition(primary_key=True)
        v: int

    coll = mongodb.generate_collection_name()
    G.clear()
    t = pw.debug.table_from_rows(S, [(i, i * 10) for i in range(1, 8)])
    pw.io.mongodb.write(
        t,
        connection_string=MONGODB_CONNECTION_STRING,
        database=MONGODB_BASE_NAME,
        collection=coll,
        output_table_type="snapshot",
        max_batch_size=2,
    )
    run()
    rows = sorted(
        mongodb.client[MONGODB_BASE_NAME][coll].find({}, {"_id": 0}),
        key=lambda r: r["k"],
    )
    assert rows == [{"k": i, "v": i * 10} for i in range(1, 8)]


# ---------------------------------------------------------------------------
# Persistence: catchup must drain *every* event that arrived between runs,
# even when the count exceeds a single MongoDB getMore batch (default
# 101 documents).  In static mode there is no follow-up live stream to fill
# the gap, so any miss here would be permanent data loss.
# ---------------------------------------------------------------------------
def test_mongodb_read_persistence_catchup_large_backlog(tmp_path, mongodb):
    """A large number of events between two static-mode runs must all be
    delivered by a single restart, regardless of the server's getMore batch
    size."""

    class InputSchema(pw.Schema):
        product: str
        qty: int

    pstorage_path = tmp_path / "PStorage"
    persistence_config = pw.persistence.Config(
        backend=pw.persistence.Backend.filesystem(pstorage_path)
    )
    input_collection = mongodb.generate_collection_name()
    mongodb.insert_document(input_collection, {"product": "init", "qty": 0})

    def run_read(output_path: pathlib.Path) -> None:
        G.clear()
        table = pw.io.mongodb.read(
            connection_string=MONGODB_CONNECTION_STRING,
            database=MONGODB_BASE_NAME,
            collection=input_collection,
            schema=InputSchema,
            mode="static",
            name="catchup_backlog_source",
        )
        pw.io.jsonlines.write(table, output_path)
        run(persistence_config=persistence_config)

    output_path_1 = tmp_path / "output_1.jsonl"
    run_read(output_path_1)
    assert len(read_jsonlines(output_path_1)) == 1

    n = 2_500
    coll = mongodb.client[MONGODB_BASE_NAME][input_collection]
    coll.insert_many([{"product": f"p{i}", "qty": i} for i in range(n)])

    output_path_2 = tmp_path / "output_2.jsonl"
    run_read(output_path_2)
    assert len(read_jsonlines(output_path_2)) == n, (
        f"expected {n} delta events on restart, "
        f"got {len(read_jsonlines(output_path_2))}"
    )


# ---------------------------------------------------------------------------
# Persistence: `drop`, `rename`, `dropDatabase`, and `invalidate` events
# permanently terminate a MongoDB change stream — the saved resume token can
# never be extended past them.  Silently continuing would drop any future
# events on a recreated collection with the same name, so the connector
# surfaces a `StreamTerminated` error telling the user to clear the
# persistence directory.
# ---------------------------------------------------------------------------
def test_mongodb_read_raises_on_drop_recreate(tmp_path, mongodb):
    """Dropping and recreating the collection between runs must not silently
    lose events. The second run must raise a clear error instead of producing
    empty output."""

    class InputSchema(pw.Schema):
        product: str
        qty: int

    pstorage_path = tmp_path / "PStorage"
    persistence_config = pw.persistence.Config(
        backend=pw.persistence.Backend.filesystem(pstorage_path)
    )
    input_collection = mongodb.generate_collection_name()
    mongodb.insert_document(input_collection, {"product": "apple", "qty": 10})

    def run_read(output_path: pathlib.Path) -> None:
        G.clear()
        table = pw.io.mongodb.read(
            connection_string=MONGODB_CONNECTION_STRING,
            database=MONGODB_BASE_NAME,
            collection=input_collection,
            schema=InputSchema,
            mode="static",
            name="drop_recreate_source",
        )
        pw.io.jsonlines.write(table, output_path)
        run(persistence_config=persistence_config)

    # Run 1: deliver the initial snapshot.
    output_path_1 = tmp_path / "output_1.jsonl"
    run_read(output_path_1)
    assert len(read_jsonlines(output_path_1)) == 1

    # Drop and recreate the collection with a new document.
    mongodb.client[MONGODB_BASE_NAME][input_collection].drop()
    mongodb.insert_document(input_collection, {"product": "cherry", "qty": 20})

    # Run 2: the saved resume token points before the drop. The connector
    # must detect the stream-terminating event and surface an actionable error
    # rather than returning empty output.
    output_path_2 = tmp_path / "output_2.jsonl"
    with pytest.raises(Exception, match="change stream terminated"):
        run_read(output_path_2)


def test_mongodb_read_raises_on_rename_during_streaming(tmp_path, mongodb):
    """Renaming the watched collection during streaming must stop the pipeline
    with a clear error, not silently swallow the rename event."""

    class InputSchema(pw.Schema):
        product: str
        qty: int

    src_collection = mongodb.generate_collection_name()
    dst_collection = mongodb.generate_collection_name()
    mongodb.insert_document(src_collection, {"product": "apple", "qty": 10})

    pstorage_path = tmp_path / "PStorage"
    output_path = tmp_path / "output.jsonl"

    script = textwrap.dedent(
        """\
        import json, sys
        import pathway as pw
        from pathway.internals.parse_graph import G
        from pathway.tests.utils import run

        cfg = json.loads(sys.argv[1])

        class S(pw.Schema):
            product: str
            qty: int

        G.clear()
        t = pw.io.mongodb.read(
            connection_string=cfg["conn"],
            database=cfg["db"],
            collection=cfg["coll"],
            schema=S,
            mode="streaming",
            name="rename_source",
        )
        pw.io.jsonlines.write(t, cfg["out"])
        persistence_config = pw.persistence.Config(
            backend=pw.persistence.Backend.filesystem(cfg["pstorage"]),
        )
        run(persistence_config=persistence_config)
        """
    )
    cfg = json.dumps(
        {
            "conn": MONGODB_CONNECTION_STRING,
            "db": MONGODB_BASE_NAME,
            "coll": src_collection,
            "out": str(output_path),
            "pstorage": str(pstorage_path),
        }
    )
    proc = subprocess.Popen(
        [sys.executable, "-c", script, cfg],
        env=os.environ,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        wait_result_with_checker(
            FileLinesNumberChecker(output_path, 1), 30, target=None
        )
        mongodb.client[MONGODB_BASE_NAME][src_collection].rename(dst_collection)
        proc.wait(timeout=30)
    except Exception:
        proc.kill()
        proc.wait()
        raise
    stderr = proc.stderr.read().decode() if proc.stderr else ""
    assert (
        proc.returncode != 0
    ), f"subprocess should have failed but exited 0; stderr={stderr}"
    assert (
        "change stream terminated" in stderr
    ), f"expected StreamTerminated message in stderr, got: {stderr}"
    # Cleanup
    mongodb.client[MONGODB_BASE_NAME][dst_collection].drop()


# ---------------------------------------------------------------------------
# Extended BSON type support: ObjectId, Decimal128, RegularExpression, and
# Timestamp aren't part of the original Pathway/BSON conversion table but are
# common in real MongoDB collections.  The reader maps them to the closest
# Pathway scalar type:
#
#   ObjectId           -> str  (24-character lowercase hex)
#   Decimal128         -> str  (canonical decimal string)
#   RegularExpression  -> str  ("/<pattern>/<options>" form)
#   Timestamp          -> int  (the seconds-since-epoch `time` component)
#
# Writes don't produce these BSON types — they emit String/Int values instead —
# so the round-trip preserves the *value* but not the original BSON type tag.
# That's the documented contract for these mappings.
# ---------------------------------------------------------------------------
def _read_one(mongodb, coll: str, schema: type[pw.Schema]):
    """Run mongodb.read in static mode and return the rows seen."""
    G.clear()
    table = pw.io.mongodb.read(
        connection_string=MONGODB_CONNECTION_STRING,
        database=MONGODB_BASE_NAME,
        collection=coll,
        schema=schema,
        mode="static",
    )
    rows: list[dict] = []

    class Obs(pw.io.python.ConnectorObserver):
        def on_change(self, key, row, time, is_addition):
            if is_addition:
                rows.append(dict(row))

        def on_end(self):
            pass

    pw.io.python.write(table, Obs())
    run()
    return rows


def test_mongodb_read_objectid_as_string(mongodb):
    """A native BSON ``ObjectId`` field reads back as the 24-char hex string."""
    from bson import ObjectId

    coll = mongodb.generate_collection_name()
    oid = ObjectId("507f1f77bcf86cd799439011")
    mongodb.client[MONGODB_BASE_NAME][coll].insert_one({"pkey": 1, "v": oid})

    class S(pw.Schema):
        pkey: int
        v: str

    rows = _read_one(mongodb, coll, S)
    assert rows == [{"pkey": 1, "v": "507f1f77bcf86cd799439011"}]


def test_mongodb_read_decimal128_as_string(mongodb):
    """A native BSON ``Decimal128`` field reads back as its canonical decimal string."""
    from bson.decimal128 import Decimal128

    coll = mongodb.generate_collection_name()
    mongodb.client[MONGODB_BASE_NAME][coll].insert_one(
        {"pkey": 1, "v": Decimal128("12345.6789")}
    )

    class S(pw.Schema):
        pkey: int
        v: str

    rows = _read_one(mongodb, coll, S)
    assert rows == [{"pkey": 1, "v": "12345.6789"}]


def test_mongodb_read_decimal128_preserves_precision(mongodb):
    """Decimal128 maps to ``str`` precisely so very long decimals round-trip
    unchanged — verifying that mapping to ``float`` would have lost digits."""
    from bson.decimal128 import Decimal128

    coll = mongodb.generate_collection_name()
    # 34 significant decimal digits — beyond f64's ~15-17 precision.
    long_value = "1.234567890123456789012345678901234"
    mongodb.client[MONGODB_BASE_NAME][coll].insert_one(
        {"pkey": 1, "v": Decimal128(long_value)}
    )

    class S(pw.Schema):
        pkey: int
        v: str

    rows = _read_one(mongodb, coll, S)
    assert rows == [{"pkey": 1, "v": long_value}]


def test_mongodb_read_regex_as_string(mongodb):
    """A native BSON ``RegularExpression`` field reads back as ``/pattern/flags``."""
    import re as _re

    coll = mongodb.generate_collection_name()
    # pymongo accepts a Python regex object and stores it as a BSON Regex.
    mongodb.client[MONGODB_BASE_NAME][coll].insert_one(
        {"pkey": 1, "v": _re.compile(r"^foo.*$", _re.IGNORECASE | _re.MULTILINE)}
    )

    class S(pw.Schema):
        pkey: int
        v: str

    rows = _read_one(mongodb, coll, S)
    # pymongo also sets the `u` (unicode) flag on Python regexes by default;
    # the driver sorts option characters alphabetically.
    assert rows == [{"pkey": 1, "v": "/^foo.*$/imu"}]


def test_mongodb_read_timestamp_as_int(mongodb):
    """A native BSON ``Timestamp`` field reads back as the seconds-since-epoch
    ``time`` component; the ``increment`` companion is dropped."""
    from bson.timestamp import Timestamp as BsonTimestamp

    coll = mongodb.generate_collection_name()
    mongodb.client[MONGODB_BASE_NAME][coll].insert_one(
        {"pkey": 1, "v": BsonTimestamp(time=1_700_000_000, inc=42)}
    )

    class S(pw.Schema):
        pkey: int
        v: int

    rows = _read_one(mongodb, coll, S)
    assert rows == [{"pkey": 1, "v": 1_700_000_000}]


@pytest.mark.parametrize(
    "label,value,schema_type",
    [
        ("objectid", "507f1f77bcf86cd799439011", str),
        ("decimal128", "12345.6789", str),
        ("regex", "/^foo$/i", str),
        ("timestamp", 1_700_000_000, int),
    ],
)
def test_mongodb_extended_types_write_then_read(
    tmp_path, mongodb, label, value, schema_type
):
    """Writing the connector's natural representation of each extended type and
    reading it back through ``pw.io.mongodb.read`` returns the same value.

    Note: the BSON type after the round-trip is ``String``/``Int64`` (not the
    original BSON-extended type), but the schema-declared *value* is preserved.
    """
    output_collection = mongodb.generate_collection_name()

    WriteSchema = pw.schema_builder(
        {
            "pkey": pw.column_definition(primary_key=True, dtype=int),
            "v": pw.column_definition(dtype=schema_type),
        }
    )

    G.clear()
    table = pw.debug.table_from_rows(WriteSchema, [(1, value)])
    pw.io.mongodb.write(
        table,
        connection_string=MONGODB_CONNECTION_STRING,
        database=MONGODB_BASE_NAME,
        collection=output_collection,
        output_table_type="snapshot",
    )
    pw.run()

    # Verify the raw stored value matches what we wrote.
    stored = list(
        mongodb.client[MONGODB_BASE_NAME][output_collection].find({}, {"_id": 0})
    )
    assert stored == [{"pkey": 1, "v": value}], f"raw stored: {stored}"

    # Now read it back through the connector.
    ReadSchema = pw.schema_builder(
        {
            "pkey": pw.column_definition(dtype=int),
            "v": pw.column_definition(dtype=schema_type),
        }
    )

    rows = _read_one(mongodb, output_collection, ReadSchema)
    assert rows == [{"pkey": 1, "v": value}], f"round-tripped: {rows}"


def test_mongodb_extended_types_native_then_read_then_write_then_read(mongodb):
    """End-to-end: native BSON extended types ingested via the reader can be
    fed straight into ``pw.io.mongodb.write`` and read back unchanged.

    This mirrors the real-world pattern of consuming a foreign collection that
    contains ObjectIds and friends, then materializing a derived collection."""
    import re as _re

    from bson import ObjectId
    from bson.decimal128 import Decimal128
    from bson.timestamp import Timestamp as BsonTimestamp

    src = mongodb.generate_collection_name()
    dst = mongodb.generate_collection_name()
    mongodb.client[MONGODB_BASE_NAME][src].insert_one(
        {
            "pkey": 1,
            "oid": ObjectId("507f1f77bcf86cd799439011"),
            "dec": Decimal128("12345.6789"),
            "rgx": _re.compile(r"^foo$", _re.IGNORECASE),
            "ts": BsonTimestamp(time=1_700_000_000, inc=7),
        }
    )

    class S(pw.Schema):
        pkey: int
        oid: str
        dec: str
        rgx: str
        ts: int

    G.clear()
    table = pw.io.mongodb.read(
        connection_string=MONGODB_CONNECTION_STRING,
        database=MONGODB_BASE_NAME,
        collection=src,
        schema=S,
        mode="static",
    )
    pw.io.mongodb.write(
        table,
        connection_string=MONGODB_CONNECTION_STRING,
        database=MONGODB_BASE_NAME,
        collection=dst,
        output_table_type="snapshot",
    )
    run()

    # Read back the destination through the connector.
    rows = _read_one(mongodb, dst, S)
    # pymongo's Python-regex compile path sets the unicode (`u`) flag in
    # addition to the explicit IGNORECASE (`i`) — option characters are
    # alphabetized by the driver.
    assert rows == [
        {
            "pkey": 1,
            "oid": "507f1f77bcf86cd799439011",
            "dec": "12345.6789",
            "rgx": "/^foo$/iu",
            "ts": 1_700_000_000,
        }
    ], f"end-to-end: {rows}"
