import copy
import datetime
import json
import os
import threading
import time

import pytest
from utils import QUEST_DB_HOST, QUEST_DB_LINE_PORT, QuestDBContext

import pathway as pw
from pathway.tests.utils import wait_result_with_checker

QUESTDB_CONNECTION_STRING = f"http::addr={QUEST_DB_HOST}:{QUEST_DB_LINE_PORT};"


class EntryCountChecker:

    def __init__(
        self,
        n_expected_entries: int,
        questdb: QuestDBContext,
        table_name: str,
        column_names: list[str],
    ):
        self.n_expected_entries = n_expected_entries
        self.questdb = questdb
        self.table_name = table_name
        self.column_names = column_names

    def __call__(self):
        try:
            table_contents = self.questdb.get_table_contents(
                self.table_name, self.column_names
            )
        except Exception:
            return False
        return len(table_contents) == self.n_expected_entries


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
        file_idx = 0
        for test_item in test_items:
            time.sleep(1.5)
            file_idx += 1
            input_path = inputs_path / f"{file_idx}.json"
            with open(input_path, "w") as f:
                f.write(json.dumps(test_item))

    table = pw.io.jsonlines.read(
        inputs_path, schema=InputSchema, autocommit_duration_ms=200
    )
    table_name = questdb.random_table_name()
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

    t = threading.Thread(target=stream_inputs, args=(input_items,))
    t.start()
    checker = EntryCountChecker(len(input_items), questdb, table_name, ["name"])
    wait_result_with_checker(checker, 15)
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
