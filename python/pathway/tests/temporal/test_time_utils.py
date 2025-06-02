# Copyright © 2024 Pathway

from __future__ import annotations

import datetime
from typing import Callable
from unittest.mock import patch

import pandas as pd

import pathway as pw
from pathway.tests.utils import T, assert_stream_equality_wo_index


def fake_utc_now(total_duration_ms: int) -> Callable[[pw.Duration], pw.Table]:
    def faked(refresh_rate: pw.Duration) -> pw.Table:
        return pw.debug.table_from_rows(
            pw.schema_from_types(timestamp_utc=int),
            [
                (time_ms, time_ms, 1)
                for time_ms in range(
                    0, total_duration_ms, int(refresh_rate.total_seconds() * 1000)
                )
            ],
            is_stream=True,
        ).select(timestamp_utc=pw.this.timestamp_utc.dt.utc_from_timestamp(unit="ms"))

    return faked


@patch("pathway.stdlib.temporal.time_utils._get_now_timestamp_utc")
@patch("pathway.stdlib.temporal.time_utils.utc_now")
def test_no_alert(utc_now_mock, get_now_timestamp_utc_mock):
    t = T(
        """
  | now_utc | __time__
0 |       0 |        0
        """
    )

    now_utc = pw.debug.table_to_pandas(t).now_utc.to_dict()

    utc_now_mock.side_effect = fake_utc_now(1000)
    get_now_timestamp_utc_mock.side_effect = lambda ptr: pd.Timestamp(
        now_utc[ptr] if ptr is not None else 0, unit="ms", tz=datetime.timezone.utc
    )
    inactivities = t.inactivity_detection(
        allowed_inactivity_period=pw.Duration(milliseconds=1000),
        refresh_rate=pw.Duration(milliseconds=100),
    )

    expected_inactivities = pw.Table.empty(
        inactivity_timestamp_utc=pw.DateTimeUtc,
        resumed_activity_timestamp_utc=pw.DateTimeUtc | None,
    )

    assert_stream_equality_wo_index(inactivities, expected_inactivities)


@patch("pathway.stdlib.temporal.time_utils._get_now_timestamp_utc")
@patch("pathway.stdlib.temporal.time_utils.utc_now")
def test_inactivity_detection(utc_now_mock, get_now_timestamp_utc_mock):
    t = T(
        """
  | now_utc | __time__
0 |       0 |        0
1 |      50 |       50
2 |     150 |      150
3 |     200 |      200
4 |     900 |      900
5 |    1000 |     1000
        """
    )

    now_utc = pw.debug.table_to_pandas(t).now_utc.to_dict()

    utc_now_mock.side_effect = fake_utc_now(1400)
    get_now_timestamp_utc_mock.side_effect = lambda ptr: pd.Timestamp(
        now_utc[ptr] if ptr is not None else 0, unit="ms", tz=datetime.timezone.utc
    )
    inactivities = t.inactivity_detection(
        allowed_inactivity_period=pw.Duration(milliseconds=300),
        refresh_rate=pw.Duration(milliseconds=50),
    )

    expected_inactivities = T(
        """
inactivity_timestamp_utc | resumed_activity_timestamp_utc | __time__ | __diff__
                    200  |                                |      550 |        1
                    200  |                                |      900 |       -1
                    200  |                            900 |      900 |        1
                   1000  |                                |     1350 |        1
        """
    ).with_columns(
        inactivity_timestamp_utc=pw.this.inactivity_timestamp_utc.dt.utc_from_timestamp(
            unit="ms"
        ),
        resumed_activity_timestamp_utc=pw.require(
            pw.this.resumed_activity_timestamp_utc.dt.utc_from_timestamp(unit="ms"),
            pw.this.resumed_activity_timestamp_utc,
        ),
    )

    assert_stream_equality_wo_index(inactivities, expected_inactivities)


@patch("pathway.stdlib.temporal.time_utils._get_now_timestamp_utc")
@patch("pathway.stdlib.temporal.time_utils.utc_now")
def test_inactivity_detection_instance(utc_now_mock, get_now_timestamp_utc_mock):
    t = T(
        """
  | instance | now_utc | __time__
0 |        A |       0 |        0
1 |        A |      50 |       50
2 |        A |     150 |      150
3 |        A |     200 |      200
4 |        A |     900 |      900
5 |        A |    1000 |     1000
6 |        B |       0 |        0
7 |        B |     200 |      200
8 |        B |     400 |      400
9 |        B |    1000 |     1000
        """
    )

    now_utc = pw.debug.table_to_pandas(t).now_utc.to_dict()

    utc_now_mock.side_effect = fake_utc_now(1400)
    get_now_timestamp_utc_mock.side_effect = lambda ptr: pd.Timestamp(
        now_utc[ptr] if ptr is not None else 0, unit="ms", tz=datetime.timezone.utc
    )
    inactivities = t.inactivity_detection(
        allowed_inactivity_period=pw.Duration(milliseconds=300),
        refresh_rate=pw.Duration(milliseconds=50),
        instance=pw.this.instance,
    )

    expected_inactivities = (
        T(
            """
instance | inactivity_timestamp_utc | resumed_activity_timestamp_utc | __time__ | __diff__
       A |                      200 |                                |      550 |        1
       A |                      200 |                                |      900 |       -1
       A |                      200 |                           900  |      900 |        1
       A |                     1000 |                                |     1350 |        1
       B |                      400 |                                |      750 |        1
       B |                      400 |                                |     1000 |       -1
       B |                      400 |                          1000  |     1000 |        1
       B |                     1000 |                                |     1350 |        1
        """
        )
        .update_types(instance=str | None)
        .with_columns(
            inactivity_timestamp_utc=pw.this.inactivity_timestamp_utc.dt.utc_from_timestamp(
                unit="ms"
            ),
            resumed_activity_timestamp_utc=pw.require(
                pw.this.resumed_activity_timestamp_utc.dt.utc_from_timestamp(unit="ms"),
                pw.this.resumed_activity_timestamp_utc,
            ),
        )
    )

    assert_stream_equality_wo_index(inactivities, expected_inactivities)


@patch("pathway.stdlib.temporal.time_utils._get_now_timestamp_utc")
@patch("pathway.stdlib.temporal.time_utils.utc_now")
def test_inactivity_detection_empty(utc_now_mock, get_now_timestamp_utc_mock):
    t = pw.Table.empty(value=str)
    utc_now_mock.side_effect = fake_utc_now(500)
    get_now_timestamp_utc_mock.side_effect = lambda _: pd.Timestamp(
        0, unit="ms", tz=datetime.timezone.utc
    )
    inactivities = t.inactivity_detection(
        allowed_inactivity_period=pw.Duration(milliseconds=300),
        refresh_rate=pw.Duration(milliseconds=50),
    )

    expected_inactivities = (
        T(
            """
inactivity_timestamp_utc | resumed_activity_timestamp_utc | __time__ | __diff__
                      0  |                                |      350 |        1
        """
        )
        .update_types(resumed_activity_timestamp_utc=int | None)
        .with_columns(
            inactivity_timestamp_utc=pw.this.inactivity_timestamp_utc.dt.utc_from_timestamp(
                unit="ms"
            ),
            resumed_activity_timestamp_utc=pw.require(
                pw.this.resumed_activity_timestamp_utc.dt.utc_from_timestamp(unit="ms"),
                pw.this.resumed_activity_timestamp_utc,
            ),
        )
    )

    assert_stream_equality_wo_index(inactivities, expected_inactivities)


@patch("pathway.stdlib.temporal.time_utils._get_now_timestamp_utc")
@patch("pathway.stdlib.temporal.time_utils.utc_now")
def test_inactivity_detection_empty_instance(utc_now_mock, get_now_timestamp_utc_mock):
    t = pw.Table.empty(instance=str, value=str)
    utc_now_mock.side_effect = fake_utc_now(500)
    get_now_timestamp_utc_mock.side_effect = lambda _: pd.Timestamp(
        0, unit="ms", tz=datetime.timezone.utc
    )
    inactivities = t.inactivity_detection(
        allowed_inactivity_period=pw.Duration(milliseconds=300),
        refresh_rate=pw.Duration(milliseconds=50),
        instance=pw.this.instance,
    )

    expected_inactivities = (
        T(
            """
instance | inactivity_timestamp_utc | resumed_activity_timestamp_utc | __time__ | __diff__
         |                       0  |                                |      350 |        1
        """
        )
        .update_types(instance=str | None, resumed_activity_timestamp_utc=int | None)
        .with_columns(
            inactivity_timestamp_utc=pw.this.inactivity_timestamp_utc.dt.utc_from_timestamp(
                unit="ms"
            ),
            resumed_activity_timestamp_utc=pw.require(
                pw.this.resumed_activity_timestamp_utc.dt.utc_from_timestamp(unit="ms"),
                pw.this.resumed_activity_timestamp_utc,
            ),
        )
    )

    assert_stream_equality_wo_index(inactivities, expected_inactivities)
