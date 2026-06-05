import datetime
import json
import os

import pytest
from utils import EntryCountChecker

import pathway as pw
from pathway.tests.utils import ExceptionAwareThread, wait_result_with_checker

# ClickHouse insertion via the native protocol is synchronous and immediately
# queryable over HTTP, but a freshly started server may still be warming up, so
# the checkers below poll with a generous timeout.
WAIT_TIMEOUT_SECONDS = 60


@pytest.mark.parametrize("reserved_column", ["time", "diff"])
def test_clickhouse_write_rejects_reserved_column(reserved_column):
    """``time`` and ``diff`` are reserved output columns in stream-of-changes
    mode, so an input table that already has one must be rejected eagerly when
    ``write`` is called, with a message that names the offending column.
    """
    table = pw.debug.table_from_markdown(
        f"""
         | data | {reserved_column}
        1 | Hello | 5
        """
    )
    with pytest.raises(ValueError, match=reserved_column):
        pw.io.clickhouse.write(
            table,
            connection_string="tcp://localhost:9000/default",
            table_name="irrelevant",
        )


def _write_static_table(table, clickhouse, table_name, n_expected, **write_kwargs):
    write_kwargs.setdefault("init_mode", "create_if_not_exists")
    pw.io.clickhouse.write(
        table,
        connection_string=clickhouse.connection_string,
        table_name=table_name,
        **write_kwargs,
    )
    checker = EntryCountChecker(
        n_expected, clickhouse, table_name=table_name, column_names=["diff"]
    )
    wait_result_with_checker(checker, WAIT_TIMEOUT_SECONDS)


def test_clickhouse_scalar_roundtrip(clickhouse):
    """Scalar columns of every basic type round-trip, and the connector appends
    the ``diff`` and ``time`` metadata columns. All rows of a static input are
    insertions, so every ``diff`` is ``1``.
    """
    table = pw.debug.table_from_markdown(
        """
        name  | count | price | available
        Water | 1     | 0.89  | True
        Milk  | 2     | 1.65  | True
        Eggs  | 10    | 4.5   | False
        """
    )
    with clickhouse.temp_table() as table_name:
        _write_static_table(table, clickhouse, table_name, 3)

        rows = clickhouse.get_table_contents(
            table_name, ["name", "count", "price", "available"], sort_by="name"
        )
        assert rows == [
            {"name": "Eggs", "count": 10, "price": 4.5, "available": False},
            {"name": "Milk", "count": 2, "price": 1.65, "available": True},
            {"name": "Water", "count": 1, "price": 0.89, "available": True},
        ]

        diffs = clickhouse.get_table_contents(table_name, ["diff"])
        assert [row["diff"] for row in diffs] == [1, 1, 1]

        times = clickhouse.get_table_contents(table_name, ["time"])
        assert all(row["time"] >= 0 for row in times)


def test_clickhouse_optional_columns_store_null(clickhouse):
    """Optional columns are created as ``Nullable`` and ``None`` values are
    stored as SQL ``NULL``, round-tripping back to ``None``.
    """
    seed = pw.debug.table_from_markdown(
        """
          | k
        1 | 1
        2 | 2
        """
    )

    @pw.udf
    def maybe_int(k: int) -> int | None:
        return None if k % 2 == 0 else k * 10

    @pw.udf
    def maybe_str(k: int) -> str | None:
        return None if k % 2 == 1 else f"value-{k}"

    table = seed.select(k=pw.this.k, oi=maybe_int(pw.this.k), os=maybe_str(pw.this.k))
    with clickhouse.temp_table() as table_name:
        _write_static_table(table, clickhouse, table_name, 2)
        rows = clickhouse.get_table_contents(table_name, ["k", "oi", "os"], sort_by="k")
        assert rows == [
            {"k": 1, "oi": 10, "os": None},
            {"k": 2, "oi": None, "os": "value-2"},
        ]


def test_clickhouse_datetime_and_duration_roundtrip(clickhouse):
    """``DateTimeUtc`` and ``DateTimeNaive`` are stored as ``DateTime64(9)`` and
    preserve the exact nanosecond instant; ``Duration`` is stored as a 64-bit
    nanosecond count.
    """
    seed = pw.debug.table_from_markdown(
        """
          | k
        1 | 1
        2 | 2
        """
    )

    @pw.udf
    def f_utc(k: int) -> pw.DateTimeUtc:
        return pw.DateTimeUtc(
            datetime.datetime(2025, 6, 27, 0, 30, k, tzinfo=datetime.timezone.utc)
        )

    @pw.udf
    def f_naive(k: int) -> pw.DateTimeNaive:
        return pw.DateTimeNaive(datetime.datetime(2025, 6, 27, 0, 30, k))

    @pw.udf
    def f_dur(k: int) -> pw.Duration:
        return pw.Duration(seconds=k, microseconds=500000)

    table = seed.select(
        k=pw.this.k,
        ts_utc=f_utc(pw.this.k),
        ts_naive=f_naive(pw.this.k),
        dur=f_dur(pw.this.k),
    )
    with clickhouse.temp_table() as table_name:
        _write_static_table(table, clickhouse, table_name, 2)
        rows = clickhouse.get_table_contents(
            table_name,
            [
                "k",
                "toUnixTimestamp64Nano(ts_utc) AS ts_utc_ns",
                "toUnixTimestamp64Nano(ts_naive) AS ts_naive_ns",
                "dur",
            ],
            sort_by="k",
        )
        for k in (1, 2):
            expected_instant = datetime.datetime(
                2025, 6, 27, 0, 30, k, tzinfo=datetime.timezone.utc
            )
            expected_ns = int(expected_instant.timestamp()) * 1_000_000_000
            row = rows[k - 1]
            assert row["ts_utc_ns"] == expected_ns
            # A naive datetime is interpreted as a UTC wall-clock instant, so it
            # carries the same nanosecond value as the UTC column here.
            assert row["ts_naive_ns"] == expected_ns
            assert row["dur"] == k * 1_000_000_000 + 500_000_000


def test_clickhouse_bytes_json_pointer_roundtrip(clickhouse):
    """``bytes`` round-trips exactly (stored as a byte-valued ``String``),
    ``JSON`` is stored as its serialized text, and a pointer column is stored as
    its canonical string form.
    """
    seed = pw.debug.table_from_markdown(
        """
          | k
        1 | 1
        """
    )

    @pw.udf
    def f_bytes(k: int) -> bytes:
        return bytes([0, 1, 2, 255])

    @pw.udf
    def f_json(k: int) -> pw.Json:
        return pw.Json({"k": "v", "n": 1})

    table = seed.select(
        bts=f_bytes(pw.this.k),
        j=f_json(pw.this.k),
        ptr=pw.this.id,
    )
    with clickhouse.temp_table() as table_name:
        _write_static_table(table, clickhouse, table_name, 1)
        rows = clickhouse.get_table_contents(
            table_name, ["hex(bts) AS bts_hex", "j", "ptr"]
        )
        assert len(rows) == 1
        row = rows[0]
        assert row["bts_hex"].lower() == bytes([0, 1, 2, 255]).hex()
        assert json.loads(row["j"]) == {"k": "v", "n": 1}
        assert isinstance(row["ptr"], str) and row["ptr"].startswith("^")


def test_clickhouse_default_init_mode_uses_existing_table(clickhouse):
    """With ``init_mode="default"`` the connector does not create the table and
    appends to a table the user created beforehand.
    """
    with clickhouse.temp_table() as table_name:
        clickhouse.execute_sql(
            f"CREATE TABLE {table_name} "
            "(data String, time Int64, diff Int8) ENGINE = MergeTree ORDER BY tuple()"
        )
        table = pw.debug.table_from_markdown(
            """
              | data
            1 | Hello
            2 | World
            """
        )
        _write_static_table(table, clickhouse, table_name, 2, init_mode="default")
        rows = clickhouse.get_table_contents(table_name, ["data"], sort_by="data")
        assert [row["data"] for row in rows] == ["Hello", "World"]


def test_clickhouse_init_mode_replace_recreates_table(clickhouse):
    """``init_mode="replace"`` drops and recreates the table, so a second run
    does not accumulate the rows of the first.
    """
    first = pw.debug.table_from_markdown(
        """
          | data
        1 | a
        2 | b
        3 | c
        """
    )
    with clickhouse.temp_table() as table_name:
        _write_static_table(first, clickhouse, table_name, 3, init_mode="replace")

        pw.internals.parse_graph.G.clear()

        second = pw.debug.table_from_markdown(
            """
              | data
            1 | x
            """
        )
        _write_static_table(second, clickhouse, table_name, 1, init_mode="replace")
        rows = clickhouse.get_table_contents(table_name, ["data"])
        assert [row["data"] for row in rows] == ["x"]


def test_clickhouse_streaming_appends_all_changes(clickhouse, tmp_path):
    """In streaming mode every change is appended. A single-group running sum
    produces a retraction (``diff = -1``) of the previous value followed by an
    insertion (``diff = 1``) of the new one on each update, and all of these
    rows are present in the output in addition to the very first insertion.
    """

    class InputSchema(pw.Schema):
        value: int

    inputs_path = tmp_path / "inputs"
    os.mkdir(inputs_path)

    values = [1, 2, 3]
    # Expected number of rows after each input is delivered: the first value is
    # a single insertion, and every later value retracts the old sum and inserts
    # the new one (two extra rows each).
    expected_counts = [1, 3, 5]

    with clickhouse.temp_table() as table_name:

        def stream_inputs() -> None:
            for file_idx, value in enumerate(values):
                input_path = inputs_path / f"{file_idx}.json"
                with open(input_path, "w") as f:
                    f.write(json.dumps({"value": value}))
                checker = EntryCountChecker(
                    expected_counts[file_idx],
                    clickhouse,
                    table_name=table_name,
                    column_names=["diff"],
                )
                wait_result_with_checker(checker, WAIT_TIMEOUT_SECONDS, target=None)

        table = pw.io.jsonlines.read(
            inputs_path, schema=InputSchema, autocommit_duration_ms=100
        )
        total = table.reduce(s=pw.reducers.sum(pw.this.value))
        pw.io.clickhouse.write(
            total,
            connection_string=clickhouse.connection_string,
            table_name=table_name,
            init_mode="create_if_not_exists",
        )

        t = ExceptionAwareThread(target=stream_inputs)
        t.start()
        checker = EntryCountChecker(
            expected_counts[-1],
            clickhouse,
            table_name=table_name,
            column_names=["diff"],
        )
        wait_result_with_checker(checker, WAIT_TIMEOUT_SECONDS)
        t.join()

        rows = clickhouse.get_table_contents(table_name, ["s", "diff", "time"])
        assert len(rows) == 5

        insertions = [row for row in rows if row["diff"] == 1]
        retractions = [row for row in rows if row["diff"] == -1]
        assert len(insertions) == 3
        assert len(retractions) == 2

        # The running sums that were inserted are 1, 3, 6; the retracted ones are
        # the intermediate 1 and 3.
        assert sorted(row["s"] for row in insertions) == [1, 3, 6]
        assert sorted(row["s"] for row in retractions) == [1, 3]

        # Summing the diffs per value reconstructs the final logical state: the
        # intermediate sums net to zero (inserted then retracted) and only the
        # full sum survives with a net diff of +1.
        net_by_sum: dict[int, int] = {}
        for row in rows:
            net_by_sum[row["s"]] = net_by_sum.get(row["s"], 0) + row["diff"]
        assert net_by_sum == {1: 0, 3: 0, 6: 1}

        assert all(row["time"] >= 0 for row in rows)
