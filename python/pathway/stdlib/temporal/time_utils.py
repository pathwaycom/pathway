# Copyright © 2024 Pathway

import datetime
import time
from functools import cache

import pathway.internals as pw
from pathway import io
from pathway.internals.runtime_type_check import check_arg_types
from pathway.internals.trace import trace_user_frame


class TimestampSchema(pw.Schema):
    timestamp_utc: pw.DateTimeUtc


class TimestampSubject(io.python.ConnectorSubject):
    _refresh_rate: datetime.timedelta

    def __init__(self, refresh_rate: datetime.timedelta) -> None:
        super().__init__()
        self._refresh_rate = refresh_rate

    @property
    def _deletions_enabled(self) -> bool:
        return False

    def run(self) -> None:
        while True:
            now_utc = datetime.datetime.now(tz=datetime.timezone.utc)
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


def _get_now_timestamp_utc(for_test_only: pw.Pointer | None) -> pw.DateTimeUtc:
    return pw.DateTimeUtc(datetime.datetime.now(tz=datetime.timezone.utc))


@check_arg_types
@trace_user_frame
def inactivity_detection(
    self: pw.Table,
    allowed_inactivity_period: pw.Duration,
    refresh_rate: pw.Duration = pw.Duration(seconds=1),
    instance: pw.ColumnExpression | None = None,
) -> pw.Table:
    """Monitor append only table additions to detect inactivity periods and
    identify when activity resumes, optionally with `instance` argument.

    This function periodically checks for table additions according to
    the provided refresh rate. It is limited to append only tables since the function
    is mostly intended to monitor input data streams. Inactivity periods that exceed
    the specified threshold are reported. The output table lists the inactivity periods
    with the UTC timestamp of the last detected activity before the threshold was
    exceeded and the UTC timestamp of the first detected activity that ends the
    inactivity period, or `None` if the inactivity period not yet ended.

    Note: the inactivity period limits may differ from the actual values when the
    refresh rate is lower than the table update rate. It is also assumed that the
    system latency is neglectable compared to the specified threshold. When used with
    `instance`, an inactivity period since the stream start (*i.e.* no incoming data)
    is reported with a `None` value in the `instance` column.

    Args:
        allowed_inactivity_period (pw.Duration): maximum allowed inactivity duration.
            If no activity occurs within this duration, an inactivity period is flagged.
        refresh_rate (pw.Duration, optional): frequency with which table activities are
            checked to detect an inactivity period. Defaults to 1 second.
        instance (pw.ColumnExpression | None, optional): group column to detect
            inactivity periods separately. Defaults to None.

    Returns:
        Table: inactivity periods table with `inactivity_timestamp_utc` and
        `resumed_activity_timestamp_utc` columns, optionally `instance` column.
    """
    assert self.is_append_only, "Table must be append only to use inactivity_detection"

    utc_now_table = utc_now(refresh_rate=refresh_rate).reduce(
        timestamp_utc=pw.reducers.latest(pw.this.timestamp_utc)
    )

    @pw.udf(deterministic=True)
    def get_now_timestamp_utc(for_test_only: pw.Pointer) -> pw.DateTimeUtc:
        return _get_now_timestamp_utc(for_test_only)

    latest_activities = (
        self.select(instance=instance, timestamp_utc=get_now_timestamp_utc(pw.this.id))
        .groupby(pw.this.instance)
        .reduce(
            pw.this.instance, timestamp_utc=pw.reducers.latest(pw.this.timestamp_utc)
        )
    )

    start_timestamp_utc = _get_now_timestamp_utc(None)
    latest_inactivities = (
        latest_activities.join_right(utc_now_table)
        .select(pw.left.instance, pw.left.timestamp_utc, now_utc=pw.right.timestamp_utc)
        .with_columns(
            timestamp_utc=pw.coalesce(pw.this.timestamp_utc, start_timestamp_utc)
        )
        .filter(pw.this.timestamp_utc + allowed_inactivity_period < pw.this.now_utc)
        .select(pw.this.instance, inactivity_timestamp_utc=pw.this.timestamp_utc)
    )

    inactivities = (
        latest_inactivities._remove_retractions()
        .groupby(pw.this.instance, pw.this.inactivity_timestamp_utc)
        .reduce(pw.this.instance, pw.this.inactivity_timestamp_utc)
    )

    latest_resumed_activities = (
        inactivities.groupby(pw.this.instance)
        .reduce(
            pw.this.instance,
            inactivity_timestamp_utc=pw.reducers.latest(
                pw.this.inactivity_timestamp_utc
            ),
        )
        .join_inner(latest_activities, pw.left.instance == pw.right.instance)
        .select(
            pw.left.instance,
            pw.left.inactivity_timestamp_utc,
            latest_activity_timestamp_utc=pw.right.timestamp_utc,
        )
        .filter(
            pw.this.inactivity_timestamp_utc < pw.this.latest_activity_timestamp_utc
        )
    )

    resumed_activities = (
        latest_resumed_activities._remove_retractions()
        .groupby(pw.this.instance, pw.this.inactivity_timestamp_utc)
        .reduce(
            pw.this.instance,
            pw.this.inactivity_timestamp_utc,
            resumed_activity_timestamp_utc=pw.reducers.earliest(
                pw.this.latest_activity_timestamp_utc
            ),
        )
    )

    inactivities = inactivities.join_left(
        resumed_activities,
        pw.left.instance == pw.right.instance,
        pw.left.inactivity_timestamp_utc == pw.right.inactivity_timestamp_utc,
    ).select(
        pw.left.instance,
        pw.left.inactivity_timestamp_utc,
        pw.right.resumed_activity_timestamp_utc,
    )

    if instance is None:
        inactivities = inactivities.without(pw.this.instance)

    return inactivities
