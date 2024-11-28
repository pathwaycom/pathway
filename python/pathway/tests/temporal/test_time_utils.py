# Copyright © 2024 Pathway

from __future__ import annotations

import datetime
from unittest.mock import patch

import pathway as pw
from pathway.tests.utils import T, assert_stream_equality_wo_index


@patch("pathway.stdlib.temporal.time_utils.utc_now")
def test_inactivity_detection_instance(utc_now_mock):
    now = datetime.datetime.now(datetime.timezone.utc)
    now_ms = int((int(now.timestamp() * 1000) // 1000) * 1000) + 100000
    events = T(
        f"""
            | t             | instance | __time__
        1   | {now_ms}      |        A | {now_ms}
        2   | {now_ms+50}   |        A | {now_ms+50}
        3   | {now_ms+150}  |        A | {now_ms+150}
        4   | {now_ms+200}  |        A | {now_ms+200}
        5   | {now_ms+900}  |        A | {now_ms+900}
        6   | {now_ms+1000} |        A | {now_ms+1000}
        7   | {now_ms}      |        B | {now_ms}
        8   | {now_ms+200}  |        B | {now_ms+200}
        9   | {now_ms+400}  |        B | {now_ms+400}
       10   | {now_ms+1000} |        B | {now_ms+1000}



    """
    ).with_columns(t=pw.this.t.dt.utc_from_timestamp(unit="ms"))

    utc_now_mock.side_effect = lambda refresh_rate: pw.debug.table_from_rows(
        pw.schema_from_types(t=int),
        [
            (time_ms, time_ms, 1)
            for time_ms in range(
                now_ms, now_ms + 1400, int(refresh_rate.total_seconds() * 1000)
            )
        ],
        is_stream=True,
    ).select(timestamp_utc=pw.this.t.dt.utc_from_timestamp(unit="ms"))

    inactivities, resumed_activities = pw.temporal.inactivity_detection(
        events.t,
        pw.Duration(milliseconds=300),
        refresh_rate=pw.Duration(milliseconds=50),
        instance=pw.this.instance,
    )

    expected_inactivities = T(
        f"""
             instance | inactive_t    | __time__      | __diff__
                    A | {now_ms+200}  | {now_ms+550}  | 1
                    A | {now_ms+1000} | {now_ms+1350} | 1
                    B | {now_ms+400}  | {now_ms+750}  | 1
                    B | {now_ms+1000} | {now_ms+1350} | 1
        """
    )
    expected_resumes = T(
        f"""
             instance | resumed_t     | __time__      | __diff__
                    A | {now_ms+900}  | {now_ms+900}  | 1
                    B | {now_ms+1000} | {now_ms+1000} | 1
        """
    )
    assert_stream_equality_wo_index(
        (
            inactivities.with_columns(
                inactive_t=pw.cast(int, pw.this.inactive_t.dt.timestamp(unit="ms"))
            ),
            resumed_activities.with_columns(
                resumed_t=pw.cast(int, pw.this.resumed_t.dt.timestamp(unit="ms"))
            ),
        ),
        (expected_inactivities, expected_resumes),
    )


@patch("pathway.stdlib.temporal.time_utils.utc_now")
def test_inactivity_detection(utc_now_mock):
    now = datetime.datetime.now(datetime.timezone.utc)
    now_ms = int((int(now.timestamp() * 1000) // 1000) * 1000) + 100000
    events = T(
        f"""
            | t             | __time__
        1   | {now_ms}      | {now_ms}
        2   | {now_ms+50}   | {now_ms+50}
        3   | {now_ms+150}  | {now_ms+150}
        4   | {now_ms+200}  | {now_ms+200}
        5   | {now_ms+900}  | {now_ms+900}
        6   | {now_ms+1000} | {now_ms+1000}


    """
    ).with_columns(t=pw.this.t.dt.utc_from_timestamp(unit="ms"))

    utc_now_mock.side_effect = lambda refresh_rate: pw.debug.table_from_rows(
        pw.schema_from_types(t=int),
        [
            (time_ms, time_ms, 1)
            for time_ms in range(
                now_ms, now_ms + 1400, int(refresh_rate.total_seconds() * 1000)
            )
        ],
        is_stream=True,
    ).select(timestamp_utc=pw.this.t.dt.utc_from_timestamp(unit="ms"))

    inactivities, resumed_activities = pw.temporal.inactivity_detection(
        events.t,
        pw.Duration(milliseconds=300),
        refresh_rate=pw.Duration(milliseconds=50),
    )

    expected_inactivities = T(
        f"""
            inactive_t    | __time__      | __diff__
            {now_ms+200}  | {now_ms+550}  | 1
            {now_ms+1000} | {now_ms+1350} | 1
        """
    )
    expected_resumes = T(
        f"""
            resumed_t     | __time__      | __diff__
            {now_ms+900}  | {now_ms+900}  | 1
        """
    )
    assert_stream_equality_wo_index(
        (
            inactivities.with_columns(
                inactive_t=pw.cast(int, pw.this.inactive_t.dt.timestamp(unit="ms"))
            ),
            resumed_activities.with_columns(
                resumed_t=pw.cast(int, pw.this.resumed_t.dt.timestamp(unit="ms"))
            ),
        ),
        (expected_inactivities, expected_resumes),
    )
