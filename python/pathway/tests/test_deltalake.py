# Copyright © 2026 Pathway

import base64
import datetime
import json
import multiprocessing
import os
import pathlib
import re
import time

import pandas as pd
import pyarrow as pa
import pytest
from deltalake import DeltaTable, write_deltalake

import pathway as pw
from pathway.internals import api
from pathway.internals.parse_graph import G
from pathway.io.deltalake import _PATHWAY_COLUMN_META_FIELD
from pathway.tests.utils import (
    CsvLinesNumberChecker,
    ExceptionAwareThread,
    T,
    assert_table_equality,
    needs_multiprocessing_fork,
    only_with_license_key,
    run,
    run_all,
    wait_result_with_checker,
    write_csv,
)


@only_with_license_key
def test_deltalake_simple(tmp_path: pathlib.Path):
    data = """
        k | v
        1 | foo
        2 | bar
        3 | baz
    """
    input_path = tmp_path / "input.csv"
    output_path = tmp_path / "output"
    write_csv(input_path, data)

    class InputSchema(pw.Schema):
        k: int = pw.column_definition(primary_key=True)
        v: str

    table = pw.io.csv.read(str(input_path), schema=InputSchema, mode="static")
    pw.io.deltalake.write(table, str(output_path))
    run_all()

    delta_table = DeltaTable(output_path)
    pd_table_from_delta = (
        delta_table.to_pandas().drop("time", axis=1).drop("diff", axis=1)
    )

    assert_table_equality(
        table,
        pw.debug.table_from_pandas(
            pd_table_from_delta,
            schema=InputSchema,
        ),
    )


@pytest.mark.parametrize("min_commit_frequency", [None, 60_000])
@only_with_license_key
def test_deltalake_append(min_commit_frequency, tmp_path: pathlib.Path):
    data = """
        k | v
        1 | foo
        2 | bar
        3 | baz
    """
    input_path = tmp_path / "input.csv"
    output_path = tmp_path / "output"
    write_csv(input_path, data)

    class InputSchema(pw.Schema):
        k: int = pw.column_definition(primary_key=True)
        v: str

    def iteration():
        G.clear()
        table = pw.io.csv.read(str(input_path), schema=InputSchema, mode="static")
        pw.io.deltalake.write(
            table,
            str(output_path),
            min_commit_frequency=min_commit_frequency,
        )
        run_all()

    iteration()
    iteration()

    delta_table = DeltaTable(output_path)
    pd_table_from_delta = delta_table.to_pandas()
    assert pd_table_from_delta.shape[0] == 6


@only_with_license_key
@pytest.mark.parametrize("has_primary_key", [True, False])
@pytest.mark.parametrize("use_stored_schema", [True, False])
def test_deltalake_roundtrip(
    has_primary_key: bool, use_stored_schema: bool, tmp_path: pathlib.Path
):
    data = """
        k | v
        1 | foo
        2 | bar
        3 | baz
    """
    input_path = tmp_path / "input.csv"
    lake_path = tmp_path / "lake"
    output_path = tmp_path / "output.csv"
    write_csv(input_path, data)

    class InputSchema(pw.Schema):
        k: int = pw.column_definition(primary_key=has_primary_key)
        v: str

    table = pw.io.csv.read(str(input_path), schema=InputSchema, mode="static")
    pw.io.deltalake.write(table, str(lake_path))
    run_all()

    G.clear()
    if use_stored_schema:
        table = pw.io.deltalake.read(lake_path, mode="static")
    else:
        table = pw.io.deltalake.read(lake_path, schema=InputSchema, mode="static")
    pw.io.csv.write(table, output_path)
    run_all()

    final = pd.read_csv(output_path, usecols=["k", "v"], index_col=["k"]).sort_index()
    original = pd.read_csv(input_path, usecols=["k", "v"], index_col=["k"]).sort_index()
    assert final.equals(original)


@pytest.mark.parametrize(
    "snapshot_access", [api.SnapshotAccess.FULL, api.SnapshotAccess.OFFSETS_ONLY]
)
@only_with_license_key
def test_deltalake_recovery(snapshot_access, tmp_path: pathlib.Path):
    data = [{"k": 1, "v": "one"}, {"k": 2, "v": "two"}, {"k": 3, "v": "three"}]
    df = pd.DataFrame(data)
    lake_path = str(tmp_path / "lake")
    output_path = str(tmp_path / "output.csv")
    write_deltalake(lake_path, df)

    class InputSchema(pw.Schema):
        k: int = pw.column_definition(primary_key=True)
        v: str

    def run_pathway_program(expected_key_set, expected_diff=1):
        table = pw.io.deltalake.read(lake_path, schema=InputSchema, mode="static")
        pw.io.csv.write(table, output_path)

        persistence_config = pw.persistence.Config(
            pw.persistence.Backend.filesystem(tmp_path / "PStorage"),
            snapshot_access=snapshot_access,
        )
        run_all(persistence_config=persistence_config)
        try:
            result = pd.read_csv(output_path)
            assert set(result["k"]) == expected_key_set
            assert set(result["diff"]) == {expected_diff}
        except pd.errors.EmptyDataError:
            assert expected_key_set == {}
        G.clear()

    run_pathway_program({1, 2, 3})

    # The second run: only two added rows must be read
    additional_data = [{"k": 7, "v": "seven"}, {"k": 8, "v": "eight"}]
    df = pd.DataFrame(additional_data)
    write_deltalake(lake_path, df, mode="append")
    run_pathway_program({7, 8})

    # The third run: add three more rows
    additional_data = [
        {"k": 4, "v": "four"},
        {"k": 5, "v": "five"},
        {"k": 6, "v": "six"},
    ]
    df = pd.DataFrame(additional_data)
    write_deltalake(lake_path, df, mode="append")
    run_pathway_program({4, 5, 6})

    # The fourth run: optimize the storage, and then write some data
    DeltaTable(lake_path).optimize()
    additional_data = [{"k": 9, "v": "nine"}]
    df = pd.DataFrame(additional_data)
    write_deltalake(lake_path, df, mode="append")
    run_pathway_program({9})

    # The fifth run: only reorganize the data according to Z-order
    DeltaTable(lake_path).optimize.z_order("k")
    run_pathway_program({})

    # The sixth run: add some data on top of the reordered table
    additional_data = [{"k": 10, "v": "ten"}]
    df = pd.DataFrame(additional_data)
    write_deltalake(lake_path, df, mode="append")
    run_pathway_program({10})

    # The seventh run: remove some data from the table
    condition = "k > 4 and k < 8"
    table = DeltaTable(lake_path)
    table.delete(condition)
    run_pathway_program({5, 6, 7}, -1)


@only_with_license_key
def test_deltalake_read_after_modification(tmp_path):
    data = [
        {"k": 1, "v": "one"},
        {"k": 2, "v": "two"},
        {"k": 3, "v": "three"},
        {"k": 4, "v": "four"},
        {"k": 5, "v": "five"},
        {"k": 6, "v": "six"},
    ]
    df = pd.DataFrame(data)
    lake_path = str(tmp_path / "lake")
    output_path = str(tmp_path / "output.csv")
    write_deltalake(lake_path, df)

    condition = "k > 1 and k < 5"
    table = DeltaTable(lake_path)
    table.delete(condition)

    class InputSchema(pw.Schema):
        k: int = pw.column_definition(primary_key=True)
        v: str

    table = pw.io.deltalake.read(lake_path, schema=InputSchema, mode="static")
    pw.io.csv.write(table, output_path)
    pw.run()

    result = pd.read_csv(output_path)
    assert set(result["k"]) == {1, 5, 6}


@needs_multiprocessing_fork
@only_with_license_key
@pytest.mark.parametrize("with_backfilling_thresholds", [False, True])
def test_streaming_from_deltalake(tmp_path, with_backfilling_thresholds):
    lake_path = str(tmp_path / "lake")
    output_path = tmp_path / "output.csv"

    class InputSchema(pw.Schema):
        k: int = pw.column_definition(primary_key=True)
        v: str

    data = [{"k": 0, "v": ""}]
    df = pd.DataFrame(data).set_index("k")
    write_deltalake(lake_path, df, mode="append")

    def create_new_versions(start_idx, end_idx):
        for idx in range(start_idx, end_idx):
            data = [{"k": idx, "v": "a" * idx}]
            df = pd.DataFrame(data).set_index("k")
            write_deltalake(lake_path, df, mode="append")
            wait_result_with_checker(
                CsvLinesNumberChecker(output_path, idx + 1), 30, target=None
            )

    if with_backfilling_thresholds:
        backfilling_thresholds = [
            api.BackfillingThreshold(field="k", comparison_op=">=", threshold=0),
        ]
        table = pw.io.deltalake.read(
            lake_path,
            schema=InputSchema,
            autocommit_duration_ms=10,
            backfilling_thresholds=backfilling_thresholds,
        )
    else:
        table = pw.io.deltalake.read(
            lake_path, schema=InputSchema, autocommit_duration_ms=10
        )
    pw.io.csv.write(table, output_path)

    stream_thread = ExceptionAwareThread(target=create_new_versions, args=(1, 10))
    pathway_process = multiprocessing.Process(target=run)
    try:
        stream_thread.start()
        pathway_process.start()

        # Wait for the scenario to complete
        stream_thread.join()
    finally:
        # Finish Pathway process in any case
        pathway_process.terminate()
        pathway_process.join()


@only_with_license_key
def test_deltalake_no_primary_key(tmp_path: pathlib.Path):
    data = [{"k": 1, "v": "one"}, {"k": 2, "v": "two"}, {"k": 3, "v": "three"}]
    df = pd.DataFrame(data).set_index("k")
    lake_path = str(tmp_path / "lake")
    output_path = str(tmp_path / "output.csv")
    write_deltalake(lake_path, df)

    class InputSchema(pw.Schema):
        k: int
        v: str

    table = pw.io.deltalake.read(lake_path, schema=InputSchema)
    pw.io.jsonlines.write(table, output_path)
    with pytest.raises(
        OSError,
        match=(
            "Failed to connect to DeltaLake: explicit primary key specification is "
            "required for non-append-only tables"
        ),
    ):
        pw.run()


@only_with_license_key
def test_deltalake_start_from_timestamp(tmp_path: pathlib.Path):
    data_first_run = """
        k | v
        1 | foo
        2 | bar
        3 | baz
    """
    data_second_run = """
        k | v
        4 | foo
        5 | bar
        6 | baz
    """
    data_third_run = """
        k | v
        7 | foo
        8 | bar
        9 | baz
    """
    input_path = tmp_path / "input.csv"
    output_path = tmp_path / "output"
    temp_reread_path = tmp_path / "reread.jsonl"

    class InputSchema(pw.Schema):
        k: int = pw.column_definition(primary_key=True)
        v: str

    def write_data(data: str):
        write_csv(input_path, data)
        G.clear()
        table = pw.io.csv.read(input_path, schema=InputSchema, mode="static")
        pw.io.deltalake.write(table, output_path)
        run_all()

    def check_entries_count(start_from_timestamp_ms: int, expected_entries: int):
        G.clear()
        table = pw.io.deltalake.read(
            output_path,
            schema=InputSchema,
            mode="static",
            start_from_timestamp_ms=start_from_timestamp_ms,
        )
        pw.io.jsonlines.write(table, temp_reread_path)
        run_all()
        n_rows = 0
        with open(temp_reread_path, "r") as f:
            for _ in f:
                n_rows += 1
        assert n_rows == expected_entries

    start_time_9 = int(time.time() * 1000)
    time.sleep(0.025)
    write_data(data_first_run)
    time.sleep(0.025)

    start_time_6 = int(time.time() * 1000)
    time.sleep(0.025)
    write_data(data_second_run)
    time.sleep(0.025)

    start_time_3 = int(time.time() * 1000)
    time.sleep(0.025)
    write_data(data_third_run)
    time.sleep(0.025)

    start_time_0 = int(time.time() * 1000)

    check_entries_count(start_time_0, 0)
    check_entries_count(start_time_3, 3)
    check_entries_count(start_time_6, 6)
    check_entries_count(start_time_9, 9)


@only_with_license_key
@pytest.mark.parametrize(
    "partition_column",
    ["i", "f", "s", "b", "tn", "tu", "bin", "io", "dur", "js", "ptr"],
)
def test_deltalake_partition_columns(tmp_path, partition_column):
    """Round-trip every supported Pathway type through ``pw.io.deltalake.write``'s
    partition_columns parameter and back through ``pw.io.deltalake.read``.

    ``ptr`` (Pointer), ``dur`` (Duration), and ``js`` (Json) are all stored as
    primitive Delta types (``string`` / ``long``) in the partition path, so they
    are valid Delta partition keys; the ``parse_partition_values`` branch for
    each must round-trip the value back into the original Pathway type.
    """
    random_bytes_1 = base64.b64encode(os.urandom(20)).decode("utf-8")
    random_bytes_2 = base64.b64encode(os.urandom(20)).decode("utf-8")
    random_bytes_3 = base64.b64encode(os.urandom(20)).decode("utf-8")
    data = [
        {
            "i": 1,
            "f": -0.5,
            "b": True,
            "s": "foo",
            "tn": "2025-05-22T10:57:43.000000000",
            "tu": "2025-03-22T10:57:43.000000000+0000",
            "bin": random_bytes_1,
            "io": 1,
            "dur": 1_000_000,
            "js": {"name": "alice", "n": 1},
        },
        {
            "i": 2,
            "f": -0.5,
            "b": True,
            "s": "bar",
            "tn": "2025-05-21T10:57:43.000000000",
            "tu": "2025-03-23T10:57:43.000000000+0000",
            "bin": random_bytes_2,
            "io": None,
            "dur": 2_000_000,
            "js": {"name": "bob", "n": 2},
        },
        {
            "i": 3,
            "f": 0.5,
            "b": False,
            "s": "foo",
            "tn": "2025-05-22T10:57:43.000000000",
            "tu": "2025-03-24T10:57:43.000000000+0000",
            "bin": random_bytes_3,
            "io": 2,
            "dur": 3_000_000,
            "js": {"name": "carol", "n": 3},
        },
    ]
    input_path = tmp_path / "input.jsonl"
    lake_path = tmp_path / "output"
    reread_path = tmp_path / "reread.csv"
    with open(input_path, "w") as f:
        for row in data:
            f.write(json.dumps(row))
            f.write("\n")

    class WriteSchema(pw.Schema):
        i: int = pw.column_definition(primary_key=True)
        f: float
        b: bool
        s: str
        tn: pw.DateTimeNaive
        tu: pw.DateTimeUtc
        bin: bytes
        io: int | None
        dur: pw.Duration
        js: pw.Json

    class ReadSchema(pw.Schema):
        i: int = pw.column_definition(primary_key=True)
        f: float
        b: bool
        s: str
        tn: pw.DateTimeNaive
        tu: pw.DateTimeUtc
        bin: bytes
        io: int | None
        dur: pw.Duration
        js: pw.Json
        ptr: pw.Pointer

    # Pathway has no notion of a Pointer literal in JSONL, so derive one after read
    # from the auto-generated row id. Every Pathway row already has one — using it
    # here exercises the Pointer-as-partition-column branch end-to-end.
    table = pw.io.jsonlines.read(input_path, schema=WriteSchema, mode="static")
    table = table.with_columns(ptr=pw.this.id)
    pw.io.deltalake.write(table, lake_path, partition_columns=[table[partition_column]])
    run_all()

    delta_table = DeltaTable(lake_path)
    assert delta_table._table.metadata().partition_columns == [partition_column]

    G.clear()
    table = pw.io.deltalake.read(lake_path, schema=ReadSchema, mode="static")
    pw.io.csv.write(table, reread_path)
    run_all()

    result = (
        pd.read_csv(reread_path, index_col=["i"])
        .drop("time", axis=1)
        .drop("diff", axis=1)
    )
    # Compare on the columns that survive a CSV round-trip cleanly. ``js`` becomes
    # the JSON-encoded text in CSV (e.g. `{"n": 1, "name": "alice"}`); ``ptr`` is
    # an opaque ``^…`` string whose exact value depends on Pathway's id hashing so
    # we just confirm three distinct pointer values came back.
    for col in ("f", "b", "s", "tn", "tu", "bin", "io", "dur"):
        assert sorted(result[col].dropna().tolist()) == sorted(
            [row[col] for row in data if row[col] is not None]
        ), f"column {col} mismatch"
    assert result["ptr"].nunique() == 3
    for row, json_str in zip(data, result["js"]):
        assert json.loads(json_str) == row["js"]

    G.clear()
    table = pw.io.deltalake.read(
        lake_path,
        schema=ReadSchema,
        mode="static",
        _backfilling_thresholds=[
            api.BackfillingThreshold(
                field="i",
                comparison_op=">=",
                threshold=1,
            )
        ],
    )
    pw.io.csv.write(table, reread_path)
    run_all()
    result_b = (
        pd.read_csv(reread_path, index_col=["i"])
        .drop("time", axis=1)
        .drop("diff", axis=1)
    )
    # Same content arriving via the backfilling-thresholds path.
    for col in ("f", "b", "s", "tn", "tu", "bin", "io", "dur"):
        assert sorted(result_b[col].dropna().tolist()) == sorted(
            result[col].dropna().tolist()
        )


@only_with_license_key
def test_deltalake_partition_columns_unknown(tmp_path: pathlib.Path):
    input_path_1 = tmp_path / "input_1.csv"
    input_path_2 = tmp_path / "input_2.csv"
    output_path = tmp_path / "output"

    class InputSchema(pw.Schema):
        k: int = pw.column_definition(primary_key=True)
        v: str
        vv: str

    table_1 = pw.io.csv.read(input_path_1, schema=InputSchema, mode="static")
    table_2 = pw.io.csv.read(input_path_2, schema=InputSchema, mode="static")
    with pytest.raises(
        ValueError,
        match="The suggested partition column <table1>.v doesn't belong to the table *",
    ):
        pw.io.deltalake.write(table_1, output_path, partition_columns=[table_2.v])


@only_with_license_key
def test_deltalake_schema_mismatch(tmp_path: pathlib.Path):
    data_1 = """
        k | v
        1 | foo
        2 | bar
        3 | baz
    """
    data_2 = """
        k | vv
        1 | foo
        2 | bar
        3 | baz
    """
    data_3 = """
        k | v
        1 | 1
        2 | 2
        3 | 3
    """
    input_path_1 = tmp_path / "input_1.csv"
    input_path_2 = tmp_path / "input_2.csv"
    input_path_3 = tmp_path / "input_3.csv"
    output_path = tmp_path / "output"
    write_csv(input_path_1, data_1)
    write_csv(input_path_2, data_2)
    write_csv(input_path_3, data_3)

    class InputSchema_1(pw.Schema):
        k: int = pw.column_definition(primary_key=True)
        v: str

    table = pw.io.csv.read(input_path_1, schema=InputSchema_1, mode="static")
    pw.io.deltalake.write(table, output_path)
    run_all()

    class InputSchema_2(pw.Schema):
        k: int = pw.column_definition(primary_key=True)
        vv: str

    G.clear()
    table = pw.io.csv.read(input_path_2, schema=InputSchema_2, mode="static")
    pw.io.deltalake.write(table, output_path)
    expected_message = (
        "Unable to create DeltaTable writer: "
        "delta table schema mismatch: "
        'Fields in the provided schema that aren\'t present in the existing table: ["vv"]; '
        'Fields in the existing table that aren\'t present in the provided schema: ["v"]'
    )
    with pytest.raises(TypeError, match=re.escape(expected_message)):
        run_all()

    class InputSchema_3(pw.Schema):
        k: int = pw.column_definition(primary_key=True)
        v: int

    G.clear()
    table = pw.io.csv.read(input_path_3, schema=InputSchema_3, mode="static")
    pw.io.deltalake.write(table, output_path)
    expected_message = (
        "Unable to create DeltaTable writer: "
        "delta table schema mismatch: "
        'Fields with mismatching types: [field "v": data type differs (existing table=string, schema=long)'
    )
    with pytest.raises(TypeError, match=re.escape(expected_message)):
        run_all()


@only_with_license_key
def test_deltalake_schema_mismatch_with_optionality(
    tmp_path: pathlib.Path,
):
    data = """
        k | v
        1 | one
        2 | two
        3 | three
    """
    input_path = tmp_path / "input.csv"
    lake_path = tmp_path / "output"
    write_csv(input_path, data)

    lake_initial = [{"k": 0, "v": "zero", "time": 0, "diff": 1}]
    df = pd.DataFrame(lake_initial).set_index("k")
    schema = pa.schema(
        [
            pa.field(
                "k",
                pa.int64(),
                nullable=False,
                metadata={
                    _PATHWAY_COLUMN_META_FIELD: (
                        '{"append_only": false, "description": null, "dtype": {"type": "INT"}, '
                        '"name": "k", "primary_key": true}'
                    )
                },
            ),
            pa.field("v", pa.string(), nullable=True, metadata={"description": "test"}),
            pa.field("time", pa.int64(), nullable=False),
            pa.field("diff", pa.int64(), nullable=False),
        ]
    )
    write_deltalake(lake_path, df, schema=schema)

    class InputSchema(pw.Schema):
        k: int = pw.column_definition(primary_key=True)
        v: str

    table = pw.io.csv.read(input_path, schema=InputSchema, mode="static")
    pw.io.deltalake.write(table, lake_path)
    expected_message = (
        "Unable to create DeltaTable writer: "
        "delta table schema mismatch: "
        'Fields with mismatching types: [field "v": nullability differs (existing table=true, schema=false)]'
    )
    with pytest.raises(TypeError, match=re.escape(expected_message)):
        run_all()


def test_deltalake_fails_to_reconstruct_schema(tmp_path: pathlib.Path):
    data = [{"k": 1, "v": "one"}, {"k": 2, "v": "two"}, {"k": 3, "v": "three"}]
    df = pd.DataFrame(data).set_index("k")
    lake_path = tmp_path / "lake"
    write_deltalake(lake_path, df)
    with pytest.raises(
        ValueError,
        match="No Pathway table schema is stored in the given Delta table's metadata",
    ):
        pw.io.deltalake.read(lake_path)


@only_with_license_key
def test_deltalake_schema_custom_metadata_flexibility(tmp_path: pathlib.Path):
    data = """
        k | v
        1 | one
        2 | two
        3 | three
    """
    input_path = tmp_path / "input.csv"
    lake_path = tmp_path / "output"
    write_csv(input_path, data)

    lake_initial = [{"k": 0, "v": "zero", "time": 0, "diff": 1}]
    df = pd.DataFrame(lake_initial).set_index("k")
    schema = pa.schema(
        [
            pa.field("k", pa.int64(), nullable=False),
            pa.field(
                "v", pa.string(), nullable=False, metadata={"description": "test"}
            ),
            pa.field("time", pa.int64(), nullable=False),
            pa.field(
                "diff",
                pa.int64(),
                nullable=False,
                metadata={"description": "special field", "hello": "world"},
            ),
        ]
    )
    write_deltalake(lake_path, df, schema=schema)

    class InputSchema(pw.Schema):
        k: int = pw.column_definition(primary_key=True)
        v: str

    table = pw.io.csv.read(input_path, schema=InputSchema, mode="static")
    pw.io.deltalake.write(table, lake_path)
    run_all()

    delta_table = DeltaTable(lake_path)
    pd_table_from_delta = (
        delta_table.to_pandas().drop("time", axis=1).drop("diff", axis=1)
    )
    assert set(pd_table_from_delta["k"]) == {0, 1, 2, 3}
    assert set(pd_table_from_delta["v"]) == {"zero", "one", "two", "three"}


@only_with_license_key
def test_deltalake_read_external_decimal_columns(tmp_path: pathlib.Path):
    """A Delta ``decimal(p, s)`` column declared as ``float`` in the Pathway schema is
    read by converting the unscaled integer through ``f64``. The mapping is lossy in
    general (binary representation, ~15-17 significant decimal digits of mantissa), so
    only values within that envelope round-trip; the lossless alternative is to declare
    the column as ``str`` (covered by `test_deltalake_read_external_decimal_columns_as_str`).
    """
    import decimal as _decimal

    lake_path = tmp_path / "lake"
    output_path = tmp_path / "out.csv"
    schema = pa.schema(
        [
            pa.field("k", pa.int64(), nullable=False),
            pa.field("price_d128_82", pa.decimal128(8, 2), nullable=False),
            pa.field("price_d128_188", pa.decimal128(18, 8), nullable=False),
        ]
    )
    arrow_table = pa.Table.from_arrays(
        [
            pa.array([1, 2, 3], type=pa.int64()),
            pa.array(
                [
                    _decimal.Decimal("100.50"),
                    _decimal.Decimal("-200.25"),
                    _decimal.Decimal("300.75"),
                ],
                type=pa.decimal128(8, 2),
            ),
            pa.array(
                [
                    _decimal.Decimal("1.00000001"),
                    _decimal.Decimal("999999999.12345678"),
                    _decimal.Decimal("-12345.00000007"),
                ],
                type=pa.decimal128(18, 8),
            ),
        ],
        schema=schema,
    )
    write_deltalake(str(lake_path), arrow_table)

    class InputSchema(pw.Schema):
        k: int = pw.column_definition(primary_key=True)
        price_d128_82: float
        price_d128_188: float

    table = pw.io.deltalake.read(str(lake_path), schema=InputSchema, mode="static")
    pw.io.csv.write(table, str(output_path))
    pw.run()

    result = pd.read_csv(output_path)
    by_k = result.set_index("k")
    assert abs(by_k.loc[1, "price_d128_82"] - 100.50) < 1e-9
    assert abs(by_k.loc[2, "price_d128_82"] - (-200.25)) < 1e-9
    assert abs(by_k.loc[3, "price_d128_82"] - 300.75) < 1e-9
    assert abs(by_k.loc[1, "price_d128_188"] - 1.00000001) < 1e-9
    assert abs(by_k.loc[2, "price_d128_188"] - 999999999.12345678) < 1e-3


@only_with_license_key
def test_deltalake_decimal_column_str_roundtrip(tmp_path: pathlib.Path):
    """End-to-end round-trip: a Delta ``decimal(p, s)`` column produced by an
    external tool can be read into Pathway as ``str``, processed as text, and
    written back into the same Delta column with no precision loss. The
    underlying unscaled integer in the Delta file matches bit-for-bit."""
    import decimal as _decimal

    G.clear()
    src_lake = tmp_path / "src_lake"
    dst_lake = tmp_path / "dst_lake"

    # Seed source with decimal(38, 10), then create a destination table sharing the
    # same schema so the writer's decimal-coercion path can kick in. We seed `dst`
    # with a single throw-away row and then have Pathway overwrite/append the real
    # data; the fact that Pathway's `str` input is materialised as the column's
    # unscaled integer is what we're verifying.
    # Use precision/scale that's expressive enough to test the cast — values that
    # don't round-trip through f64 — but small enough that the values fit cleanly
    # in i64 (deltalake-rs serialises Delta-log min/max stats for very large
    # Decimal128 values via f64, which is itself lossy and a separate issue).
    test_values = [
        _decimal.Decimal("0.1000000000"),
        _decimal.Decimal("-12345.6789012345"),
        _decimal.Decimal("999999.9999999999"),
        _decimal.Decimal("0.0000000000"),
    ]
    src_schema = pa.schema(
        [
            pa.field("k", pa.int64(), nullable=False),
            pa.field("v", pa.decimal128(18, 10), nullable=False),
        ]
    )
    src_arrow = pa.Table.from_arrays(
        [
            pa.array([1, 2, 3, 4], type=pa.int64()),
            pa.array(test_values, type=pa.decimal128(18, 10)),
        ],
        schema=src_schema,
    )
    write_deltalake(str(src_lake), src_arrow)

    # Materialize the destination shell with the SAME schema (decimal(18, 10)) plus
    # Pathway's `time` and `diff` housekeeping columns that the append-only writer
    # appends. Without these, the writer would create a fresh table with a `string`
    # column and the override branch wouldn't fire.
    dst_schema = pa.schema(
        [
            pa.field("k", pa.int64(), nullable=False),
            pa.field("v", pa.decimal128(18, 10), nullable=False),
            pa.field("time", pa.int64(), nullable=False),
            pa.field("diff", pa.int64(), nullable=False),
        ]
    )
    write_deltalake(
        str(dst_lake),
        pa.Table.from_arrays(
            [
                pa.array([], type=pa.int64()),
                pa.array([], type=pa.decimal128(38, 10)),
                pa.array([], type=pa.int64()),
                pa.array([], type=pa.int64()),
            ],
            schema=dst_schema,
        ),
    )

    class InputSchema(pw.Schema):
        k: int = pw.column_definition(primary_key=True)
        v: str

    table = pw.io.deltalake.read(str(src_lake), schema=InputSchema, mode="static")
    pw.io.deltalake.write(table, str(dst_lake))
    pw.run()

    # Read the destination Delta table directly (not via Pathway) to verify the
    # column is still decimal(18, 10) on disk and the values are bit-exact.
    dst_table = DeltaTable(str(dst_lake)).to_pandas().drop(["time", "diff"], axis=1)
    by_k = {row["k"]: row["v"] for _, row in dst_table.iterrows()}
    assert by_k[1] == _decimal.Decimal("0.1000000000")
    assert by_k[2] == _decimal.Decimal("-12345.6789012345")
    assert by_k[3] == _decimal.Decimal("999999.9999999999")
    assert by_k[4] == _decimal.Decimal("0E-10") or by_k[4] == _decimal.Decimal(
        "0.0000000000"
    )


@only_with_license_key
def test_deltalake_decimal_column_str_write_invalid_string(tmp_path: pathlib.Path):
    """Writing a string that is not valid decimal text into an existing Delta
    ``decimal(p, s)`` column fails the write with an error message that names both
    the offending value and the column's precision/scale — enough information for the
    user to find the bad row and the constraint it violated."""
    G.clear()
    dst_lake = tmp_path / "dst_lake"

    # Create destination table with decimal(8, 2) + Pathway's housekeeping columns.
    dst_schema = pa.schema(
        [
            pa.field("k", pa.int64(), nullable=False),
            pa.field("v", pa.decimal128(8, 2), nullable=False),
            pa.field("time", pa.int64(), nullable=False),
            pa.field("diff", pa.int64(), nullable=False),
        ]
    )
    write_deltalake(
        str(dst_lake),
        pa.Table.from_arrays(
            [
                pa.array([], type=pa.int64()),
                pa.array([], type=pa.decimal128(8, 2)),
                pa.array([], type=pa.int64()),
                pa.array([], type=pa.int64()),
            ],
            schema=dst_schema,
        ),
    )

    class InputSchema(pw.Schema):
        k: int = pw.column_definition(primary_key=True)
        v: str

    table = pw.debug.table_from_markdown(
        """
        k | v
        1 | not-a-decimal
        """,
        schema=InputSchema,
    )
    pw.io.deltalake.write(table, str(dst_lake))
    with pytest.raises(Exception) as excinfo:
        pw.run()
    msg = str(excinfo.value)
    assert "decimal(8, 2)" in msg, f"error should name column shape, got: {msg}"
    assert "not-a-decimal" in msg, f"error should name offending value, got: {msg}"


@only_with_license_key
def test_deltalake_read_external_decimal_columns_as_str(tmp_path: pathlib.Path):
    """Declaring a Delta ``decimal(p, s)`` column as ``str`` in the Pathway schema
    bypasses the lossy f64 path and passes the value through as its decimal text
    representation, preserving the full precision of the source column."""
    import decimal as _decimal

    G.clear()
    lake_path = tmp_path / "lake"
    output_path = tmp_path / "out.csv"
    # Pick values that don't round-trip through f64:
    #   - 0.1 (binary-inexact)
    #   - a value with leading zeros after the decimal point
    #   - a 38-digit value that overflows f64's mantissa
    #   - exact zero (verifies the leading-zero padding branch with non-zero unscaled)
    schema = pa.schema(
        [
            pa.field("k", pa.int64(), nullable=False),
            pa.field("v", pa.decimal128(38, 10), nullable=False),
        ]
    )
    test_values = [
        _decimal.Decimal("0.1000000000"),
        _decimal.Decimal("-12345.6789012345"),
        _decimal.Decimal("1234567890123456789012345678.9012345678"),
        _decimal.Decimal("0.0000000000"),
    ]
    arrow_table = pa.Table.from_arrays(
        [
            pa.array([1, 2, 3, 4], type=pa.int64()),
            pa.array(test_values, type=pa.decimal128(38, 10)),
        ],
        schema=schema,
    )
    write_deltalake(str(lake_path), arrow_table)

    class InputSchema(pw.Schema):
        k: int = pw.column_definition(primary_key=True)
        v: str

    table = pw.io.deltalake.read(str(lake_path), schema=InputSchema, mode="static")
    pw.io.csv.write(table, str(output_path))
    pw.run()

    # Read everything as string so pandas's numeric inference doesn't strip trailing zeros.
    result = pd.read_csv(output_path, dtype={"v": str}).set_index("k")
    # decimal(38, 10) → 10 digits after the decimal point, with the unscaled integer
    # serialised exactly. Negative sign and leading-zero padding both survive.
    assert result.loc[1, "v"] == "0.1000000000"
    assert result.loc[2, "v"] == "-12345.6789012345"
    assert result.loc[3, "v"] == "1234567890123456789012345678.9012345678"
    assert result.loc[4, "v"] == "0.0000000000"


@only_with_license_key
def test_deltalake_read_external_temporal_columns(tmp_path: pathlib.Path):
    """A Delta ``date`` column maps onto Pathway ``DateTimeNaive`` / ``DateTimeUtc``
    materialized at midnight on the calendar day (Pathway has no native ``Date``
    type, and midnight is the only mapping that preserves the date losslessly).
    A Delta ``timestamp`` with millisecond precision maps onto the same Pathway
    types with millisecond precision preserved."""
    lake_path = tmp_path / "lake"
    output_path = tmp_path / "out.csv"
    schema = pa.schema(
        [
            pa.field("k", pa.int64(), nullable=False),
            pa.field("d", pa.date32(), nullable=False),
            pa.field("ts_ms", pa.timestamp("ms"), nullable=False),
        ]
    )
    arrow_table = pa.Table.from_arrays(
        [
            pa.array([1, 2, 3], type=pa.int64()),
            pa.array(
                [
                    datetime.date(2025, 1, 1),
                    datetime.date(2025, 1, 2),
                    datetime.date(2025, 6, 30),
                ],
                type=pa.date32(),
            ),
            pa.array(
                [
                    datetime.datetime(2025, 1, 1, 12, 30, 45, 123000),
                    datetime.datetime(2025, 1, 2, 0, 0, 0, 0),
                    datetime.datetime(2025, 6, 30, 23, 59, 59, 999000),
                ],
                type=pa.timestamp("ms"),
            ),
        ],
        schema=schema,
    )
    write_deltalake(str(lake_path), arrow_table)

    class InputSchema(pw.Schema):
        k: int = pw.column_definition(primary_key=True)
        d: pw.DateTimeNaive
        ts_ms: pw.DateTimeNaive

    table = pw.io.deltalake.read(str(lake_path), schema=InputSchema, mode="static")
    pw.io.csv.write(table, str(output_path))
    pw.run()

    result = pd.read_csv(output_path)
    assert sorted(result["k"].tolist()) == [1, 2, 3]
    # Dates land at midnight; the third row's date is 2025-06-30
    assert any("2025-06-30" in str(d) for d in result["d"])
    # Millisecond timestamp preserves sub-second precision in `ts_ms`
    assert any("12:30:45.123" in str(t) for t in result["ts_ms"])


@only_with_license_key
@pytest.mark.parametrize(
    "k_arrow_type,v_arrow_type",
    [
        (pa.int8(), pa.float64()),
        (pa.int16(), pa.float64()),
        (pa.int32(), pa.float64()),
        (pa.int64(), pa.float32()),
        (pa.int64(), pa.float64()),  # baseline that already worked
    ],
)
def test_deltalake_read_narrow_numeric_columns(
    tmp_path: pathlib.Path, k_arrow_type, v_arrow_type
):
    """Delta integer columns of any standard width (``byte`` / ``short`` / ``integer`` /
    ``long``) read into Pathway ``int`` (widened to ``i64``); 32-bit and 16-bit Delta
    floats read into Pathway ``float`` (widened to ``f64``). Both widenings are exact
    — Pathway's i64 / f64 fully contain the narrower types — so the round-trip through
    Pathway is value-preserving."""
    lake_path = tmp_path / "lake"
    output_path = tmp_path / "output.csv"
    schema = pa.schema(
        [
            pa.field("k", k_arrow_type, nullable=False),
            pa.field("v", v_arrow_type, nullable=False),
        ]
    )
    arrow_table = pa.Table.from_arrays(
        [
            pa.array([1, 2, 3], type=k_arrow_type),
            pa.array([1.5, 2.5, 3.5], type=v_arrow_type),
        ],
        schema=schema,
    )
    write_deltalake(str(lake_path), arrow_table)

    class InputSchema(pw.Schema):
        k: int = pw.column_definition(primary_key=True)
        v: float

    table = pw.io.deltalake.read(str(lake_path), schema=InputSchema, mode="static")
    pw.io.csv.write(table, str(output_path))
    pw.run()

    result = pd.read_csv(output_path)
    assert sorted(result["k"].tolist()) == [1, 2, 3]
    assert sorted(result["v"].tolist()) == [1.5, 2.5, 3.5]


@only_with_license_key
def test_deltalake_backfilling_thresholds_recovery(tmp_path: pathlib.Path):
    """`_backfilling_thresholds` reads the snapshot at construction time. Combined
    with persistence, the reader emits each backfilled row exactly once across
    restarts: a re-run with the same input emits nothing, and a re-run after new
    commits emits only the new commits' rows. Delta versions are atomic and
    persisted offsets are committed at version boundaries, so resuming at the
    next version after the saved one is sufficient — there is no partial-version
    state to reconstruct."""
    lake_path = tmp_path / "lake"
    output_path = tmp_path / "output.csv"
    pstorage = tmp_path / "pstorage"
    persistence_config = pw.persistence.Config(
        pw.persistence.Backend.filesystem(pstorage)
    )

    class InputSchema(pw.Schema):
        k: int = pw.column_definition(primary_key=True)
        v: str

    write_deltalake(
        str(lake_path),
        pd.DataFrame([{"k": i, "v": f"v_{i}"} for i in range(3)]),
    )

    def run_once():
        if output_path.exists():
            output_path.unlink()
        G.clear()
        table = pw.io.deltalake.read(
            str(lake_path),
            schema=InputSchema,
            mode="static",
            _backfilling_thresholds=[
                api.BackfillingThreshold(field="k", comparison_op=">=", threshold=0),
            ],
        )
        pw.io.csv.write(table, str(output_path))
        run_all(persistence_config=persistence_config)
        try:
            return pd.read_csv(output_path)
        except pd.errors.EmptyDataError:
            return pd.DataFrame()

    out1 = run_once()
    assert sorted(out1["k"].tolist()) == [0, 1, 2]

    out2 = run_once()
    # Without the seek fix, this is [0, 1, 2] — backfilling re-emits every row.
    assert out2.empty or out2["k"].tolist() == []

    write_deltalake(
        str(lake_path),
        pd.DataFrame([{"k": 3, "v": "v_3"}, {"k": 4, "v": "v_4"}]),
        mode="append",
    )
    out3 = run_once()
    assert sorted(out3["k"].tolist()) == [3, 4]


@only_with_license_key
def test_deltalake_backfilling_thresholds(tmp_path: pathlib.Path):
    input_path = tmp_path / "input"
    output_path = tmp_path / "output.csv"
    lake_initial = [
        {"k": 0, "v": "zero"},
        {"k": 1, "v": "one"},
        {"k": 2, "v": "two"},
        {"k": 3, "v": "three"},
    ]
    df = pd.DataFrame(lake_initial)
    write_deltalake(input_path, df)

    class InputSchema(pw.Schema):
        k: int = pw.column_definition(primary_key=True)
        v: str

    table = pw.io.deltalake.read(
        input_path,
        InputSchema,
        mode="static",
        _backfilling_thresholds=[
            api.BackfillingThreshold(
                field="k",
                comparison_op=">",
                threshold=1,
            )
        ],
    )
    pw.io.csv.write(table, output_path)
    run_all()
    result = pd.read_csv(output_path, usecols=["v"])
    assert set(result["v"]) == {"two", "three"}


def test_deltalake_snapshot_mode(tmp_path):
    first_payload = """
        k | v
        1 | foo
        2 | bar
        3 | baz
    """

    second_payload = """
        k | v
        1 | one
        2 | two
        3 | three
    """

    third_payload = """
        k | v
        1 | one
        4 | four
    """

    fourth_payload = """
        k | v
        5 | five
        7 | seven
    """

    input_path = tmp_path / "input.csv"
    output_path = tmp_path / "output"
    output_path_2 = tmp_path / "output_2"
    pstorage_path = tmp_path / "pstorage"
    persistence_config = pw.persistence.Config(
        pw.persistence.Backend.filesystem(pstorage_path)
    )

    def run_reread(data):
        G.clear()
        write_csv(input_path, data)

        class InputSchema(pw.Schema):
            k: int = pw.column_definition(primary_key=True)
            v: str

        table = pw.io.csv.read(input_path, schema=InputSchema, mode="static")
        pw.io.deltalake.write(table, output_path, output_table_type="snapshot")
        run_all(persistence_config=persistence_config)

        delta_table = DeltaTable(output_path)
        pd_table_from_delta = delta_table.to_pandas().drop("_id", axis=1)
        assert_table_equality(
            table,
            pw.debug.table_from_pandas(
                pd_table_from_delta,
                schema=InputSchema,
            ),
        )

        G.clear()
        table = pw.io.deltalake.read(output_path, mode="static")
        pw.io.csv.write(table, output_path_2)
        run_all()

        assert_table_equality(
            table,
            T(data, id_from=["k"]),
        )

    run_reread(first_payload)
    run_reread(second_payload)
    run_reread(third_payload)
    run_reread(fourth_payload)


def test_wrong_delta_optimizer_rule(tmp_path):
    input_path = tmp_path / "input"
    output_path = tmp_path / "output"

    class InputSchema(pw.Schema):
        key: int = pw.column_definition(primary_key=True)
        value: str
        date: str

    table = pw.io.jsonlines.read(input_path, mode="static", schema=InputSchema)
    with pytest.raises(
        ValueError,
        match=(
            "Optimization is based on the column '<table1>.key', which is not a "
            "partition column. Please include this column in partition_columns."
        ),
    ):
        pw.io.deltalake.write(
            table,
            output_path,
            partition_columns=[table.date],
            table_optimizer=pw.io.deltalake.TableOptimizer(
                tracked_column=table.key,
                time_format="%Y-%m-%d",
                quick_access_window=datetime.timedelta(days=7),
                compression_frequency=datetime.timedelta(hours=1),
            ),
        )


def test_delta_optimizer_rule(tmp_path):
    input_path = tmp_path / "input.jsonl"
    output_path = tmp_path / "output_lake"

    def run_identity_program(data):
        G.clear()

        class InputSchema(pw.Schema):
            key: int = pw.column_definition(primary_key=True)
            value: str
            date: str

        with open(input_path, "w") as f:
            for row in data:
                f.write(json.dumps(row))
                f.write("\n")

        table = pw.io.jsonlines.read(input_path, mode="static", schema=InputSchema)
        pw.io.deltalake.write(
            table,
            output_path,
            partition_columns=[table.date],
            table_optimizer=pw.io.deltalake.TableOptimizer(
                tracked_column=table.date,
                time_format="%Y-%m-%d",
                quick_access_window=datetime.timedelta(days=7),
                compression_frequency=datetime.timedelta(hours=1),
                remove_old_checkpoints=True,
            ),
        )
        pw.io.csv.write(table, tmp_path / "output.csv")
        run_all()

    today_date = datetime.date.today()
    remote_date = today_date - datetime.timedelta(days=21)

    data = [
        {
            "key": 1,
            "value": "one",
            "date": today_date.isoformat(),
        },
        {
            "key": 2,
            "value": "two",
            "date": remote_date.isoformat(),
        },
    ]
    run_identity_program(data)
    delta_table = DeltaTable(output_path)
    pd_table_from_delta = delta_table.to_pandas()
    assert pd_table_from_delta.shape[0] == 2
    assert len(delta_table.files()) == 2

    data = [
        {
            "key": 3,
            "value": "three",
            "date": remote_date.isoformat(),
        },
    ]
    run_identity_program(data)
    delta_table = DeltaTable(output_path)
    pd_table_from_delta = delta_table.to_pandas()
    assert pd_table_from_delta.shape[0] == 3
    # Same as in the previous run thanks to the compression
    assert len(delta_table.files()) == 2

    raw_history = delta_table.history()
    operations_history = []
    for item in raw_history:
        operations_history.append(item["operation"])

    expected_operations_history = [
        "VACUUM END",
        "VACUUM START",
        "OPTIMIZE",
        "WRITE",
        "WRITE",
        "CREATE TABLE",
    ]
    assert operations_history == expected_operations_history


def test_delta_snapshot_mode_rewind(tmp_path):
    input_path = tmp_path / "input.jsonl"
    delta_table_path = tmp_path / "delta"
    output_path = tmp_path / "output.jsonl"
    pstorage_path = tmp_path / "PStorage"

    row_1 = {
        "key": 1,
        "value": "one",
    }
    row_2 = {
        "key": 2,
        "value": "two",
    }
    row_3 = {
        "key": 3,
        "value": "three",
    }

    class InputSchema(pw.Schema):
        key: int = pw.column_definition(primary_key=True)
        value: str

    def update_delta_table_snapshot(rows: list[dict]):
        G.clear()
        with open(input_path, "w") as f:
            for row in rows:
                f.write(json.dumps(row))
                f.write("\n")
        table = pw.io.jsonlines.read(input_path, schema=InputSchema, mode="static")
        pw.io.deltalake.write(table, delta_table_path, output_table_type="snapshot")
        run_all(
            persistence_config=pw.persistence.Config(
                backend=pw.persistence.Backend.filesystem(pstorage_path)
            )
        )

    def from_delta_table_to_file(start_from_timestamp_ms: int | None) -> list[dict]:
        G.clear()
        table = pw.io.deltalake.read(
            delta_table_path,
            schema=InputSchema,
            mode="static",
            start_from_timestamp_ms=start_from_timestamp_ms,
        )
        pw.io.jsonlines.write(table, output_path)
        run_all()
        result = []
        with open(output_path, "r") as f:
            for row in f:
                parsed_row = json.loads(row)
                if start_from_timestamp_ms is None:
                    assert parsed_row["diff"] == 1
                result.append(parsed_row)
        return result

    time_start_1 = int(time.time() * 1000)
    time.sleep(0.05)
    update_delta_table_snapshot([row_1, row_2, row_3])
    assert len(from_delta_table_to_file(None)) == 3

    time_start_2 = int(time.time() * 1000)
    time.sleep(0.05)
    update_delta_table_snapshot([row_1, row_2])
    assert len(from_delta_table_to_file(None)) == 2

    time_start_3 = int(time.time() * 1000)
    time.sleep(0.05)
    update_delta_table_snapshot([row_1, row_3])
    assert len(from_delta_table_to_file(None)) == 2

    time_start_4 = int(time.time() * 1000)
    time.sleep(0.05)
    update_delta_table_snapshot([row_2])
    assert len(from_delta_table_to_file(None)) == 1
    time.sleep(0.05)
    time_start_5 = int(time.time() * 1000)

    # We start with an empty set.
    # Then, we move to [1, 2, 3]. It's 3 actions.
    # Then, we move to [1, 2] via just one action: -1. It's 4 actions in total.
    # Then, we move to [1, 3], which takes -2, +3. It's 6 actions in total.
    # Then, we move to [2], which takes -1, -3, +2. It's 9 actions.
    assert len(from_delta_table_to_file(time_start_1)) == 9

    # We the state at `time_start_2` corresponds to the snapshot with 3 elements.
    # Then we apply all diffs, so the size of the log is the same as in the previous
    # case.
    assert len(from_delta_table_to_file(time_start_2)) == 9

    # We start with [1, 2]. It's 2 events.
    # Then, we do -2, +3 to advance to [1, 3]. It's 4 events.
    # Then, we do -1, -3, +2 to advance to [2]. It's 7 events.
    assert len(from_delta_table_to_file(time_start_3)) == 7

    # We start with [1, 3]. It's 2 events.
    # Then, we do -1, -3, +2 to advance to [2]. It's 5 events.
    assert len(from_delta_table_to_file(time_start_4)) == 5

    # There are no events following after `time_start_5`, so we take the snapshot
    # that is actual at this point of time. It's just one action: [2]
    assert len(from_delta_table_to_file(time_start_5)) == 1
