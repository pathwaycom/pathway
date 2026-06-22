import copy
import datetime
import json
import os

import pytest
from utils import QUEST_DB_HOST, QUEST_DB_LINE_PORT, EntryCountChecker, RowCountChecker

import pathway as pw
from pathway.tests.utils import ExceptionAwareThread, wait_result_with_checker

QUESTDB_CONNECTION_STRING = f"http::addr={QUEST_DB_HOST}:{QUEST_DB_LINE_PORT};"


def test_questdb_use_column_requires_designated_timestamp():
    """``designated_timestamp_policy="use_column"`` requires the
    ``designated_timestamp`` parameter to be provided, as the docstring states.

    The error must be raised eagerly when ``write`` is called and must name the
    ``designated_timestamp`` parameter the user controls -- not an internal
    engine field -- so the user knows what to fix.
    """
    table = pw.debug.table_from_markdown(
        """
         | data
        1 | Hello
        """
    )
    with pytest.raises(ValueError, match="designated_timestamp"):
        pw.io.questdb.write(
            table,
            connection_string=QUESTDB_CONNECTION_STRING,
            table_name="irrelevant",
            designated_timestamp_policy="use_column",
        )


def test_questdb_designated_timestamp_must_be_datetime():
    """The ``designated_timestamp`` column must have ``DateTimeNaive`` or
    ``DateTimeUtc`` type, as the docstring states.

    Passing a column of any other type must be rejected eagerly when ``write``
    is called, with a message that names the ``designated_timestamp`` parameter
    the user controls.
    """
    table = pw.debug.table_from_markdown(
        """
         | data | ts
        1 | Hello | 5
        """
    )
    with pytest.raises(ValueError, match="designated_timestamp"):
        pw.io.questdb.write(
            table,
            connection_string=QUESTDB_CONNECTION_STRING,
            table_name="irrelevant",
            designated_timestamp=table.ts,
        )


def test_questdb_serializes_complex_types_per_documentation(questdb):
    """The complex Pathway types are stored as the documentation describes:
    ``bytes`` as base64 strings, ``Duration`` as nanosecond longs, ``JSON`` as
    serialized JSON strings, and ``tuple``/``np.ndarray`` as JSON strings (a
    JSON array for tuples, and a ``{"shape": ..., "elements": ...}`` object for
    arrays).
    """
    import base64

    import numpy as np

    table_name = questdb.random_table_name()
    seed = pw.debug.table_from_markdown(
        """
           | k
         1 | 1
        """
    )

    @pw.udf
    def f_bytes(k: int) -> bytes:
        return bytes([0, 1, 2])

    @pw.udf
    def f_dur(k: int) -> pw.Duration:
        return pw.Duration(seconds=1)

    @pw.udf
    def f_json(k: int) -> pw.Json:
        return pw.Json({"k": "v"})

    @pw.udf
    def f_tuple(k: int) -> tuple[int, ...]:
        return (1, 2, 3)

    @pw.udf
    def f_arr(k: int) -> np.ndarray:
        return np.array([[1.0, 2.0], [3.0, 4.0]])

    table = seed.select(
        bts=f_bytes(pw.this.k),
        dur=f_dur(pw.this.k),
        j=f_json(pw.this.k),
        tup=f_tuple(pw.this.k),
        arr=f_arr(pw.this.k),
    )
    pw.io.questdb.write(
        table,
        connection_string=QUESTDB_CONNECTION_STRING,
        table_name=table_name,
    )

    checker = EntryCountChecker(1, questdb, table_name=table_name, column_names=["bts"])
    wait_result_with_checker(checker, 30)

    rows = questdb.get_table_contents(table_name, ["bts", "dur", "j", "tup", "arr"])
    assert len(rows) == 1
    row = rows[0]
    assert row["bts"] == base64.b64encode(bytes([0, 1, 2])).decode()
    assert row["dur"] == 1_000_000_000
    assert json.loads(row["j"]) == {"k": "v"}
    assert json.loads(row["tup"]) == [1, 2, 3]
    assert json.loads(row["arr"]) == {"shape": [2, 2], "elements": [1.0, 2.0, 3.0, 4.0]}


@pytest.mark.parametrize(
    "designated_timestamp_policy", ["use_now", "use_pathway_time", "use_column"]
)
def test_questdb_output_stream(designated_timestamp_policy, tmp_path, questdb):
    class InputSchema(pw.Schema):
        name: str
        count: int
        price: float
        available: bool
        updated_at: pw.DateTimeUtc

    table_name = questdb.random_table_name()
    inputs_path = tmp_path / "inputs"
    os.mkdir(inputs_path)
    input_items = [
        {
            "name": "Water",
            "count": 1,
            "price": 0.89,
            "available": True,
            "updated_at": "2025-06-27T00:30:00+00:00",
        },
        {
            "name": "Milk",
            "count": 1,
            "price": 1.65,
            "available": True,
            "updated_at": "2025-06-27T00:30:01+00:00",
        },
        {
            "name": "Eggs",
            "count": 10,
            "price": 4.5,
            "available": False,
            "updated_at": "2025-06-27T00:30:02+00:00",
        },
    ]

    times_original_flat = []
    input_items_without_timestamp = []
    for item in input_items:
        item_copy = copy.copy(item)
        updated_at = item_copy.pop("updated_at")
        input_items_without_timestamp.append(item_copy)
        times_original_flat.append(
            datetime.datetime.fromisoformat(updated_at).replace(tzinfo=None)  # type: ignore
        )

    def stream_inputs(test_items: list[dict]) -> None:
        for file_idx, test_item in enumerate(test_items):
            input_path = inputs_path / f"{file_idx}.json"
            with open(input_path, "w") as f:
                f.write(json.dumps(test_item))
            checker = EntryCountChecker(
                file_idx + 1, questdb, table_name=table_name, column_names=["name"]
            )
            wait_result_with_checker(checker, 30, target=None)

    table = pw.io.jsonlines.read(
        inputs_path, schema=InputSchema, autocommit_duration_ms=200
    )
    extra_params = {}
    if designated_timestamp_policy == "use_column":
        extra_params["designated_timestamp"] = table.updated_at
    else:
        extra_params["designated_timestamp_policy"] = designated_timestamp_policy
    pw.io.questdb.write(
        table,
        connection_string=QUESTDB_CONNECTION_STRING,
        table_name=table_name,
        **extra_params,
    )

    t = ExceptionAwareThread(target=stream_inputs, args=(input_items,))
    t.start()
    checker = EntryCountChecker(
        len(input_items), questdb, table_name=table_name, column_names=["name"]
    )
    wait_result_with_checker(checker, 30)
    table_reread = questdb.get_table_contents(
        table_name,
        ["name", "count", "price", "available"],
        sort_by="price",
    )
    assert table_reread == input_items_without_timestamp
    time_column = (
        "timestamp" if designated_timestamp_policy == "use_column" else "updated_at"
    )
    times_reread = questdb.get_table_contents(
        table_name, [time_column], sort_by=time_column
    )
    times_reread_flat = []
    for time_reread in times_reread:
        times_reread_flat.append(time_reread[time_column])
    assert times_reread_flat == times_original_flat


def test_questdb_writer_ingests_500k_rows_within_30s(tmp_path, questdb):
    """The QuestDB sink must ingest a 500,000-row batch within 30 seconds.

    The connector accumulates rows in the ILP buffer and sends them in bulk, so
    half a million rows is comfortably within reach. This guards against two
    regressions that make a multi-row minibatch fail outright (and therefore
    blow the time budget): forgetting to begin each ILP row with ``table()``
    (the second row of a minibatch then errors), and buffering an entire
    minibatch into a single flush (which overruns the questdb-rs buffer cap on
    large batches). A single static CSV file is used as the input so the whole
    batch is handed to the writer in large minibatches.
    """
    n_rows = 500_000

    class InputSchema(pw.Schema):
        k: int
        name: str
        value: float
        flag: bool

    # A directory with one CSV file, read in streaming mode so the run process
    # stays alive while we poll (and is terminated by the checker on success or
    # timeout). The deadline in wait_result_with_checker bounds the wait: a
    # broken writer (e.g. one that errors on multi-row minibatches) fails the
    # test at ~30s instead of after the whole run.
    input_dir = tmp_path / "inputs"
    input_dir.mkdir()
    with open(input_dir / "data.csv", "w") as f:
        f.write("k,name,value,flag\n")
        for i in range(n_rows):
            f.write(f"{i},item_{i},{i * 0.5},{'True' if i % 2 else 'False'}\n")

    table_name = questdb.random_table_name()
    table = pw.io.csv.read(str(input_dir), schema=InputSchema)
    pw.io.questdb.write(
        table,
        connection_string=QUESTDB_CONNECTION_STRING,
        table_name=table_name,
        designated_timestamp_policy="use_now",
    )
    wait_result_with_checker(RowCountChecker(n_rows, questdb, table_name), 30)
