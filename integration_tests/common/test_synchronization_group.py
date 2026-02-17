from __future__ import annotations

import datetime
import sys
import threading
import time
from typing import Any

import pandas as pd

import pathway as pw
from pathway.internals.api import Pointer, SessionType

START_EVENT_TIME = datetime.datetime(year=2025, month=1, day=1)
NO_ADVANCEMENTS_TIME = datetime.datetime(year=2024, month=1, day=1)


class InputSchema(pw.Schema):
    key: str = pw.column_definition(primary_key=True)
    current_time: pw.DateTimeNaive
    origin: str


class BaseStreamingSubject(pw.io.python.ConnectorSubject):
    def __init__(
        self,
        n_entries: int,
        interval: datetime.timedelta,
        origin: str,
        geofences_subject: "BaseStreamingSubject" | None = None,
    ):
        super().__init__()
        self.n_entries = n_entries
        self.interval = interval
        self.origin = origin
        self.geofences_subject = geofences_subject
        self._can_terminate_mutex = threading.Lock()
        self._can_terminate = False

    def report_possible_to_terminate(self):
        with self._can_terminate_mutex:
            self._can_terminate = True

    def wait_until_possible_to_terminate(self):
        while True:
            with self._can_terminate_mutex:
                if self._can_terminate:
                    break
            time.sleep(1.0)

    def last_event_time(self):
        return START_EVENT_TIME + self.n_entries * self.interval


class InsertSubject(BaseStreamingSubject):
    def run(self) -> None:
        current_time = START_EVENT_TIME
        for _ in range(self.n_entries):
            current_time += self.interval
            self.next(
                key=f"{current_time}-{self.origin}",
                current_time=current_time,
                origin=self.origin,
            )
            time.sleep(0.001)
        if self.geofences_subject is None:
            self.wait_until_possible_to_terminate()
        else:
            self.geofences_subject.report_possible_to_terminate()

    @property
    def _deletions_enabled(self) -> bool:
        return False


class UpsertSubject(BaseStreamingSubject):
    def run(self) -> None:
        current_time = START_EVENT_TIME
        for _ in range(self.n_entries):
            current_time += self.interval
            self.next(
                key=self.origin,
                current_time=current_time,
                origin=self.origin,
            )
            time.sleep(0.001)
        self.wait_until_possible_to_terminate()

    @property
    def _session_type(self) -> SessionType:
        return SessionType.UPSERT


def check_output_events(
    concat_events: list[tuple], last_geofence_time: datetime.datetime
):
    rows = pd.DataFrame(concat_events, columns=["origin", "time", "pathway_time"])
    # compute max time for each origin for each batch
    batches_agg = (
        rows.groupby(["pathway_time", "origin"])["time"]
        .max()
        .unstack()
        .sort_values("pathway_time")
    )
    # propagate max time seen so far for each origin separately
    advancements = batches_agg.ffill().cummax()
    # check if geofences are ahead of events until geofences are over
    first_batch_with_event_idx = batches_agg["events"].first_valid_index()
    last_batch_with_geofence_idx = batches_agg["geofences"].last_valid_index()
    advancements_not_over = advancements.loc[
        first_batch_with_event_idx:last_batch_with_geofence_idx  # type: ignore
    ]

    # an information about the exact place where the
    # ordering is broken is valuable for debugging
    is_geofence_leading = (
        advancements_not_over["geofences"] >= advancements_not_over["events"]
    )
    total = len(is_geofence_leading)
    for pos, (idx, ok) in enumerate(is_geofence_leading.items(), start=1):
        if not ok:
            print(
                f"geofence is not leading: {pos}/{total}, index={idx}",
                file=sys.stderr,
            )
    assert is_geofence_leading.all(), "geofence is not leading the stream"
    # in each batch, two sources have difference smaller than the defined interval
    assert (
        (advancements_not_over["geofences"] - advancements_not_over["events"]).abs()
        <= datetime.timedelta(days=1)
    ).all()
    assert len(batches_agg) > 1000


def create_and_run_graph(geofences_subject, events_subject, tmp_path):
    last_geofence_time = geofences_subject.last_event_time()
    events = pw.io.python.read(
        events_subject,
        schema=InputSchema,
        autocommit_duration_ms=10,
        name="events",
    )
    geofences = pw.io.python.read(
        geofences_subject,
        schema=InputSchema,
        autocommit_duration_ms=10,
        name="geofences",
    )
    pw.io.register_input_synchronization_group(
        pw.io.SynchronizedColumn(events.current_time, priority=0),
        pw.io.SynchronizedColumn(
            geofences.current_time,
            priority=1,
            idle_duration=datetime.timedelta(seconds=10),
        ),
        max_difference=datetime.timedelta(days=1),
    )
    events.promise_universes_are_disjoint(geofences)
    concat = events.concat(geofences)

    concat_events = []

    class TestObserver(pw.io.python.ConnectorObserver):

        def on_change(
            self, key: Pointer, row: dict[str, Any], time: int, is_addition: bool
        ) -> None:
            origin = row["origin"]
            current_time = row["current_time"]
            concat_events.append((origin, current_time, time))

    pw.io.python.write(concat, TestObserver())
    pw.io.jsonlines.write(concat, tmp_path / "output.jsonl")
    pw.run()
    check_output_events(concat_events, last_geofence_time)


def test_two_regular_streams(tmp_path):
    geofences_subject = InsertSubject(
        100, interval=datetime.timedelta(days=1), origin="geofences"
    )
    events_subject = InsertSubject(
        1440 * 120,
        interval=datetime.timedelta(minutes=1),
        origin="events",
        geofences_subject=geofences_subject,
    )
    create_and_run_graph(geofences_subject, events_subject, tmp_path)


def test_stream_with_snapshot(tmp_path):
    geofences_subject = UpsertSubject(
        100, interval=datetime.timedelta(days=1), origin="geofences"
    )
    events_subject = InsertSubject(
        1440 * 120,
        interval=datetime.timedelta(minutes=1),
        origin="events",
        geofences_subject=geofences_subject,
    )
    create_and_run_graph(geofences_subject, events_subject, tmp_path)
