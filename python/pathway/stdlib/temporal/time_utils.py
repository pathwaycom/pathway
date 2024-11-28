# Copyright © 2024 Pathway

import datetime
import time
from functools import cache

import pathway.internals as pw
from pathway import io


class TimestampSchema(pw.Schema):
    timestamp_utc: pw.DateTimeUtc


class TimestampSubject(io.python.ConnectorSubject):
    _refresh_rate: datetime.timedelta

    def __init__(self, refresh_rate: datetime.timedelta) -> None:
        super().__init__()
        self._refresh_rate = refresh_rate

    def run(self) -> None:
        while True:
            now_utc = datetime.datetime.now(datetime.timezone.utc)
            self.next(timestamp_utc=now_utc)
            self.commit()
            time.sleep(self._refresh_rate.total_seconds())


@cache
def utc_now(refresh_rate: datetime.timedelta = datetime.timedelta(seconds=60)):
    """
    Provides a continuously updating stream of the current UTC time.

    This function generates a real-time feed of the current UTC timestamp, refreshing
    at a specified interval.

    Args:
        refresh_rate: The interval at which the current
            UTC time is refreshed. Defaults to 60 seconds.

    Returns:
        A table containing a stream of the current UTC timestamps, updated
        according to the specified refresh rate.
    """
    return io.python.read(
        TimestampSubject(refresh_rate=refresh_rate),
        schema=TimestampSchema,
    )


def inactivity_detection(
    event_time_column: pw.ColumnReference,
    allowed_inactivity_period: pw.Duration,
    refresh_rate: pw.Duration = pw.Duration(seconds=1),
    instance: pw.ColumnReference | None = None,
) -> tuple[pw.Table, pw.Table]:
    """Detects periods of inactivity in a given table and identifies when activity resumes.

    This function monitors a stream of events defined by a timestamp column and detects
    inactivity periods that exceed a specified threshold. Additionally, it identifies
    the first event that resumes activity after each period of inactivity.

    Note: Assumes that the ingested timestamps (event_time_column) follow current UTC time
    and that the latency of the system is negligible compared to the `allowed_inactivity_period`.

    Args:
        event_time_column: A reference to the column containing
            UTC timestamps of events in the monitored table.
        allowed_inactivity_period: The maximum allowed period of
            inactivity. If no events occur within this duration, an inactivity
            period is flagged.
        refresh_rate: The frequency at which the current time
            is refreshed for inactivity detection. Defaults to 1 second.
        instance:
            The inactivity periods are computed separately per each `instance` value

    Returns:
        Tuple of tables:
            - **inactivities** (`pw.Table`): A table containing timestamps (`inactive_t`)
              where periods of inactivity begin (i.e., the last timestamp before inactivity
              was detected).
            - **resumed_activities** (`pw.Table`): A table containing the earliest
              timestamps (`resumed_t`) of resumed activity following each period
              of inactivity.
    """

    events_t = event_time_column.table.select(t=event_time_column, instance=instance)

    now_t = utc_now(refresh_rate=refresh_rate)
    latest_t = (
        events_t.groupby(pw.this.instance)
        .reduce(pw.this.instance, latest_t=pw.reducers.max(pw.this.t))
        .filter(
            pw.this.latest_t > datetime.datetime.now(datetime.timezone.utc)
        )  # filter to avoid alerts during backfilling
    )
    inactivities = (
        now_t.asof_now_join(latest_t)
        .select(pw.left.timestamp_utc, pw.right.instance, pw.right.latest_t)
        .filter(pw.this.latest_t + allowed_inactivity_period < pw.this.timestamp_utc)
        .groupby(
            pw.this.latest_t, pw.this.instance
        )  # todo: memoryless alert deduplication
        .reduce(pw.this.latest_t, pw.this.instance)
        .select(instance=pw.this.instance, inactive_t=pw.this.latest_t)
    )

    latest_inactivity = inactivities.groupby(pw.this.instance).reduce(
        pw.this.instance, inactive_t=pw.reducers.latest(pw.this.inactive_t)
    )
    resumed_activities = (
        events_t.asof_now_join(
            latest_inactivity, events_t.instance == latest_inactivity.instance
        )
        .select(pw.left.t, pw.left.instance, pw.right.inactive_t)
        .groupby(
            pw.this.inactive_t, pw.this.instance
        )  # todo: memoryless alert deduplication
        .reduce(pw.this.instance, resumed_t=pw.reducers.min(pw.this.t))
    )
    if instance is None:
        inactivities = inactivities.without(pw.this.instance)
        resumed_activities = resumed_activities.without(pw.this.instance)
    return inactivities, resumed_activities
