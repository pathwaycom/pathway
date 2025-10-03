import json
from collections import defaultdict
from typing import Any

import pytest

import pathway as pw
from pathway.internals.api import Pointer, SessionType
from pathway.tests.utils import run_all


class KeyDataColumnSchema(pw.Schema):
    key: str = pw.column_definition(primary_key=True)
    data: str


class SnapshotObserver(pw.io.python.ConnectorObserver):

    def __init__(self):
        self.snapshots = defaultdict(list[str])

    def on_change(
        self, key: Pointer, row: dict[str, Any], time: int, is_addition: bool
    ) -> None:
        assert is_addition  # unpack_snapshot has only additions
        self.snapshots[time].append(row["data"])

    @property
    def time_ordered_snapshots(self):
        snapshots = []
        snapshots_with_time = []
        for time, snapshot in self.snapshots.items():
            snapshots_with_time.append([time, snapshot])
        snapshots_with_time.sort()
        for _, snapshot in snapshots_with_time:
            snapshot.sort()
            snapshots.append(snapshot)
        return snapshots


class SnapshotChangeStreamer(pw.io.python.ConnectorSubject):

    def __init__(
        self,
        transactions: list[list[dict]],
        session_type: SessionType = SessionType.NATIVE,
        datasource_name: str = "snapshot-change-streamer",
    ):
        super().__init__(datasource_name)
        self.transactions = transactions
        self.session_type = session_type
        self.snapshot: dict[str, str] = {}

    @property
    def _session_type(self) -> SessionType:
        return self.session_type

    def run(self) -> None:
        for transaction in self.transactions:
            for action in transaction:
                action_type = action["type"]
                if action_type == "Upsert":
                    self.upsert(action["key"], action["data"])
                elif action_type == "Remove":
                    self.remove(action["key"])
                else:
                    raise ValueError(f"unknown test scenario action: {action_type}")
            self.commit()

    def create_json_message(self, key: str, data: str) -> bytes:
        return json.dumps(
            {
                "key": key,
                "data": data,
            }
        ).encode("utf-8")

    def upsert(self, key, data) -> None:
        message = self.create_json_message(key, data)
        if self.session_type == SessionType.NATIVE:
            old_data = self.snapshot.get(key)
            if old_data is not None:
                deletion_message = self.create_json_message(key, old_data)
                self._remove(None, deletion_message)
            self._add(None, message)
        else:
            self._add(None, message)
        self.snapshot[key] = data

    def remove(self, key: str) -> None:
        data = self.snapshot.pop(key)
        message = self.create_json_message(key, data)
        self._remove(None, message)


def test_unpack_snapshots_static_table():
    table = pw.debug.table_from_markdown(
        """
        data
        one
        two
        three
        """
    )
    unpacked = table.unpack_snapshots()
    observer = SnapshotObserver()
    pw.io.python.write(unpacked, observer)
    run_all()

    assert observer.time_ordered_snapshots == [["one", "three", "two"]]


def test_unpack_snapshots_dynamically_growing_table():
    table = pw.demo.generate_custom_stream(
        value_generators={
            "key": lambda x: str(x + 1),
            "data": lambda x: str(x + 1),
        },
        schema=KeyDataColumnSchema,
        nb_rows=5,
        input_rate=5,
        autocommit_duration_ms=10,
    )
    unpacked = table.unpack_snapshots()
    observer = SnapshotObserver()
    pw.io.python.write(unpacked, observer)
    run_all()

    assert observer.time_ordered_snapshots == [
        ["1"],
        ["1", "2"],
        ["1", "2", "3"],
        ["1", "2", "3", "4"],
        ["1", "2", "3", "4", "5"],
    ]


@pytest.mark.parametrize("session_type", [SessionType.NATIVE, SessionType.UPSERT])
def test_unpack_snapshots_small_changing_table(session_type):
    scenario = [
        [
            {"type": "Upsert", "key": "1", "data": "one"},
            {"type": "Upsert", "key": "2", "data": "two"},
            {"type": "Upsert", "key": "3", "data": "three"},
        ],
        [{"type": "Upsert", "key": "3", "data": "four"}],
        [{"type": "Remove", "key": "1"}, {"type": "Remove", "key": "2"}],
        [
            {"type": "Upsert", "key": "3", "data": "five"},
            {"type": "Upsert", "key": "1", "data": "six"},
            {"type": "Upsert", "key": "2", "data": "seven"},
        ],
        [
            {"type": "Upsert", "key": "1", "data": "eight"},
            {"type": "Remove", "key": "3"},
        ],
    ]
    table = pw.io.python.read(
        subject=SnapshotChangeStreamer(scenario, session_type=session_type),
        schema=KeyDataColumnSchema,
        format="json",
        autocommit_duration_ms=None,
    )
    unpacked = table.unpack_snapshots()
    observer = SnapshotObserver()
    pw.io.python.write(unpacked, observer)
    run_all()

    assert observer.time_ordered_snapshots == [
        ["one", "three", "two"],
        ["four", "one", "two"],
        ["four"],
        ["five", "seven", "six"],
        ["eight", "seven"],
    ]
