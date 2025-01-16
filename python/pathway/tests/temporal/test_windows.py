# Copyright © 2024 Pathway

from __future__ import annotations

import datetime
import re
import typing

import pandas as pd
import pytest

import pathway as pw
from pathway.internals import dtype as dt
from pathway.internals.dtype import DATE_TIME_NAIVE, DATE_TIME_UTC
from pathway.tests.utils import T, assert_table_equality_wo_index, warns_here


def test_session_simple():
    t = T(
        """
            | instance |  t |  v
        1   | 0        |  1 |  10
        2   | 0        |  2 |  1
        3   | 0        |  4 |  3
        4   | 0        |  8 |  2
        5   | 0        |  9 |  4
        6   | 0        |  10|  8
        7   | 1        |  1 |  9
        8   | 1        |  2 |  16
    """
    )

    def should_merge(a, b):
        return abs(a - b) <= 1

    gb = t.windowby(
        t.t, window=pw.temporal.session(predicate=should_merge), instance=t.instance
    )
    result = gb.reduce(
        pw.this._pw_instance,
        pw.this._pw_window_start,
        pw.this._pw_window_end,
        min_t=pw.reducers.min(pw.this.t),
        max_v=pw.reducers.max(pw.this.v),
        count=pw.reducers.count(),
    )
    res = T(
        """
        _pw_instance | _pw_window_start | _pw_window_end | min_t | max_v | count
        0            | 1                | 2              | 1     | 10    | 2
        0            | 4                | 4              | 4     | 3     | 1
        0            | 8                | 10             | 8     | 8     | 3
        1            | 1                | 2              | 1     | 16    | 2
    """
    )
    assert_table_equality_wo_index(result, res)


def test_session_simple_this():
    t = T(
        """
        instance | t  |  v
        0        |  1 |  10
        0        |  2 |  1
        0        |  4 |  3
        0        |  8 |  2
        0        |  9 |  4
        0        |  10|  8
        1        |  1 |  9
        1        |  2 |  16
    """
    )

    def should_merge(a, b):
        return abs(a - b) <= 1

    gb = t.windowby(
        pw.this.t,
        window=pw.temporal.session(predicate=should_merge),
        instance=pw.this.instance,
    )
    result = gb.reduce(
        pw.this.instance,
        min_t=pw.reducers.min(pw.this.t),
        max_v=pw.reducers.max(pw.this.v),
    )
    res = T(
        """
        instance | min_t | max_v
        0        | 1     | 10
        0        | 4     | 3
        0        | 8     | 8
        1        | 1     | 16
    """
    )
    assert_table_equality_wo_index(result, res)


def test_session_max_gap():
    t = T(
        """
            | t
        1   |  1
        2   |  1.1
        3   |  1.2
        4   |  3
        5   |  3.4
        6   |  3.5
    """
    )

    gb = t.windowby(t.t, window=pw.temporal.session(max_gap=0.15))
    result = gb.reduce(
        min_t=pw.reducers.min(pw.this.t),
        count=pw.reducers.count(),
    )
    res = T(
        """
        min_t | count
        1     | 3
        3     | 1
        3.4   | 2
    """
    )
    assert_table_equality_wo_index(result, res)


def test_session_max_gap_mixed():
    t = T(
        """
            | t
        1   |  10
        2   |  11
        3   |  12
        4   |  30
        5   |  34
        6   |  35
    """
    )

    gb = t.windowby(t.t, window=pw.temporal.session(max_gap=1.5))
    result = gb.reduce(
        min_t=pw.reducers.min(pw.this.t),
        count=pw.reducers.count(),
    )
    res = T(
        """
        min_t | count
        10    | 3
        30    | 1
        34    | 2
    """
    )
    assert_table_equality_wo_index(result, res)


def test_session_window_creation():
    with pytest.raises(ValueError):
        pw.temporal.session()
    with pytest.raises(ValueError):
        pw.temporal.session(predicate=lambda *_: True, max_gap=1)

    pw.temporal.session(predicate=lambda *_: True)
    pw.temporal.session(max_gap=1)


def test_sliding():
    t = T(
        """
            | instance | t
        1   | 0        |  12
        2   | 0        |  13
        3   | 0        |  14
        4   | 0        |  15
        5   | 0        |  16
        6   | 0        |  17
        7   | 1        |  10
        8   | 1        |  11
    """
    )

    gb = t.windowby(
        t.t, window=pw.temporal.sliding(duration=10, hop=3), instance=t.instance
    )
    result = gb.reduce(
        pw.this._pw_instance,
        pw.this._pw_window_start,
        pw.this._pw_window_end,
        min_t=pw.reducers.min(pw.this.t),
        max_t=pw.reducers.max(pw.this.t),
        count=pw.reducers.count(),
    )
    res = T(
        """
        _pw_instance | _pw_window_start | _pw_window_end | min_t | max_t | count
            0        |     3            |     13         | 12    | 12    | 1
            0        |     6            |     16         | 12    | 15    | 4
            0        |     9            |     19         | 12    | 17    | 6
            0        |     12           |     22         | 12    | 17    | 6
            0        |     15           |     25         | 15    | 17    | 3
            1        |     3            |     13         | 10    | 11    | 2
            1        |     6            |     16         | 10    | 11    | 2
            1        |     9            |     19         | 10    | 11    | 2
            """
    )
    assert_table_equality_wo_index(result, res)


# in the batch mode, we can test close to nothing;
# basically checks whether syntax is not broken
# for more tests see test_windows_stream.py
def test_sliding_compacting():
    t = T(
        """
            | instance | t
        1   | 0        |  12
        2   | 0        |  13
        3   | 0        |  14
        4   | 0        |  15
        5   | 0        |  16
        6   | 0        |  17
        7   | 1        |  10
        8   | 1        |  11
    """
    )

    gb = t.windowby(
        t.t,
        window=pw.temporal.sliding(duration=10, hop=3),
        behavior=pw.temporal.common_behavior(delay=0, cutoff=1, keep_results=False),
        instance=t.instance,
    )

    result = gb.reduce(
        pw.this._pw_instance,
        pw.this._pw_window_start,
        pw.this._pw_window_end,
        min_t=pw.reducers.min(pw.this.t),
        max_t=pw.reducers.max(pw.this.t),
        count=pw.reducers.count(),
    )
    # note that two windows have cutoff that is equal to the time, and they
    # are not present here but are present in the next test
    res = T(
        """
        _pw_instance | _pw_window_start | _pw_window_end | min_t | max_t | count
            0        |     3            |     13         | 12    | 12    | 1
            0        |     6            |     16         | 12    | 15    | 4
            0        |     9            |     19         | 12    | 17    | 6
            0        |     12           |     22         | 12    | 17    | 6
            0        |     15           |     25         | 15    | 17    | 3
            1        |     3            |     13         | 10    | 11    | 2
            1        |     6            |     16         | 10    | 11    | 2
            1        |     9            |     19         | 10    | 11    | 2
            """
    )

    assert_table_equality_wo_index(result, res)


def test_flush_buffer_long_chain_of_operators():
    t = T(
        """
    t
    12
    14
    16
    18
    20
    22
    24
    26
    """
    )

    expected = T(
        """
    t
    12
    14
    16
    18
    20
    22
    24
    26
    """
    )

    for i in range(5):
        gb = t.windowby(
            t.t,
            window=pw.temporal.sliding(duration=2, hop=2, origin=1),
            behavior=pw.temporal.common_behavior(
                delay=8, cutoff=100, keep_results=False
            ),
        )

        t = gb.reduce(
            t=pw.reducers.any(pw.this.t),
        )
    assert_table_equality_wo_index(t, expected)


def test_sliding_origin():
    t = T(
        """
            | t
        1   |  12
        2   |  13
        3   |  14
        4   |  15
        5   |  16
        6   |  17
    """
    )
    gb = t.windowby(t.t, window=pw.temporal.sliding(duration=10, hop=3, origin=13))
    result = gb.reduce(
        pw.this._pw_instance,
        pw.this._pw_window_start,
        pw.this._pw_window_end,
        min_t=pw.reducers.min(pw.this.t),
        max_t=pw.reducers.max(pw.this.t),
        count=pw.reducers.count(),
    )

    res = T(
        """
        _pw_instance | _pw_window_start | _pw_window_end | min_t | max_t | count
                     |     13           |     23         | 13    | 17    | 5
                     |     16           |     26         | 16    | 17    | 2
    """
    )
    assert_table_equality_wo_index(result, res)


def test_sliding_larger_hop():
    t = T(
        """
            | t
        0   |  11
        1   |  12
        2   |  13
        3   |  14
        4   |  15
        5   |  16
        6   |  17
    """
    )

    gb = t.windowby(t.t, window=pw.temporal.sliding(duration=4, hop=6))
    result = gb.reduce(
        pw.this._pw_instance,
        pw.this._pw_window_start,
        pw.this._pw_window_end,
        min_t=pw.reducers.min(pw.this.t),
        max_t=pw.reducers.max(pw.this.t),
        count=pw.reducers.count(),
    )

    res = T(
        """
        _pw_instance | _pw_window_start | _pw_window_end | min_t | max_t | count
                     |     12           |     16         | 12    | 15    | 4
    """
    )
    assert_table_equality_wo_index(result, res)


def test_sliding_larger_hop_mixed():
    t = T(
        """
            | t
        0   |  11.3
        1   |  12.1
        2   |  13.3
        3   |  14.7
        4   |  15.3
        5   |  16.1
        6   |  17.8
    """
    )

    gb = t.windowby(t.t, window=pw.temporal.sliding(duration=4, hop=6))
    result = gb.reduce(
        pw.this._pw_instance,
        pw.this._pw_window_start,
        pw.this._pw_window_end,
        min_t=pw.reducers.min(pw.this.t),
        max_t=pw.reducers.max(pw.this.t),
        count=pw.reducers.count(),
    )

    res = T(
        """
        _pw_instance | _pw_window_start | _pw_window_end | min_t | max_t | count
                     |     12           |     16         | 12.1  | 15.3  | 4
    """
    ).update_types(_pw_window_start=dt.FLOAT, _pw_window_end=dt.FLOAT)
    assert_table_equality_wo_index(result, res)


def test_tumbling():
    t = T(
        """
            | instance | t
        1   | 0        |  12
        2   | 0        |  13
        3   | 0        |  14
        4   | 0        |  15
        5   | 0        |  16
        6   | 0        |  17
        7   | 1        |  12
        8   | 1        |  13
    """
    )

    gb = t.windowby(t.t, window=pw.temporal.tumbling(duration=5), instance=t.instance)
    result = gb.reduce(
        pw.this._pw_instance,
        pw.this._pw_window_start,
        pw.this._pw_window_end,
        min_t=pw.reducers.min(pw.this.t),
        max_t=pw.reducers.max(pw.this.t),
        count=pw.reducers.count(),
    )

    res = T(
        """
    _pw_instance | _pw_window_start | _pw_window_end | min_t | max_t | count
        0        |     10           |     15         | 12    | 14    | 3
        0        |     15           |     20         | 15    | 17    | 3
        1        |     10           |     15         | 12    | 13    | 2
    """
    )
    assert_table_equality_wo_index(result, res)


def test_tumbling_origin():
    t = T(
        """
            | t
        0   |  3
        1   |  12
        2   |  13
        3   |  14
        4   |  15
        5   |  16
        6   |  17
    """
    )

    gb = t.windowby(t.t, window=pw.temporal.tumbling(duration=3, origin=7))
    result = gb.reduce(
        pw.this._pw_instance,
        pw.this._pw_window_start,
        pw.this._pw_window_end,
        min_t=pw.reducers.min(pw.this.t),
        max_t=pw.reducers.max(pw.this.t),
        count=pw.reducers.count(),
    )

    res = T(
        """
    _pw_instance | _pw_window_start | _pw_window_end | min_t | max_t | count
                 |     10           |     13         | 12    | 12    | 1
                 |     13           |     16         | 13    | 15    | 3
                 |     16           |     19         | 16    | 17    | 2
    """
    )
    assert_table_equality_wo_index(result, res)


def test_tumbling_floats():
    n = 100
    t = pw.debug.table_from_pandas(
        pd.DataFrame({"t": [0.1 * (k + 1) for k in range(n)]})
    )

    hop = 0.1
    gb = t.windowby(t.t, window=pw.temporal.tumbling(duration=hop, origin=-hop))
    result = gb.reduce(
        pw.this._pw_instance,
        pw.this._pw_window_start,
        pw.this._pw_window_end,
        count=pw.reducers.count(),
    )
    res_pd = pw.debug.table_to_pandas(result)
    assert res_pd["count"].sum() == n


def test_sliding_floats():
    n = 100
    t = pw.debug.table_from_pandas(
        pd.DataFrame({"t": [0.1 * (k + 1) for k in range(n)]})
    )

    hop = 0.1
    gb = t.windowby(t.t, window=pw.temporal.sliding(hop=hop, ratio=3, origin=-hop))
    result = gb.reduce(
        pw.this._pw_instance,
        pw.this._pw_window_start,
        pw.this._pw_window_end,
        count=pw.reducers.count(),
    )
    res_pd = pw.debug.table_to_pandas(result)
    assert res_pd["count"].sum() == 3 * n


@pytest.mark.parametrize(
    "w",
    [
        pw.temporal.tumbling(duration=2),
        pw.temporal.sliding(hop=1, duration=2),
        pw.temporal.session(predicate=lambda a, b: abs(a - b) <= 2),
    ],
)
def test_windows_smart_cols(w):
    t = T(
        """
           | k | t
         0 | 1 | 1
         1 | 1 | 3
         2 | 1 | 4
         3 | 1 | 6
         4 | 1 | 7
         5 | 2 | -2
         6 | 2 | -1
         7 | 2 | 5
         8 | 2 | 6
         9 | 3 | 0
        10 | 3 | 1
        11 | 3 | 2
        12 | 3 | 3
        13 | 3 | 7
    """
    )
    if w == pw.temporal.tumbling(duration=2):
        expected = T(
            """
        _pw_instance | min_t | max_t | count
              1      | 1     | 1     | 1
              1      | 3     | 3     | 1
              1      | 4     | 4     | 1
              1      | 6     | 7     | 2
              2      | -2    | -1    | 2
              2      | 5     | 5     | 1
              2      | 6     | 6     | 1
              3      | 0     | 1     | 2
              3      | 2     | 3     | 2
              3      | 7     | 7     | 1
            """
        )
    elif w == pw.temporal.sliding(hop=1, duration=2):
        expected = T(
            """
        _pw_instance | min_t | max_t | count
              1      | 1     | 1     | 1
              1      | 1     | 1     | 1
              1      | 3     | 3     | 1
              1      | 3     | 4     | 2
              1      | 4     | 4     | 1
              1      | 6     | 6     | 1
              1      | 6     | 7     | 2
              1      | 7     | 7     | 1
              2      | -2    | -2    | 1
              2      | -2    | -1    | 2
              2      | -1    | -1    | 1
              2      | 5     | 5     | 1
              2      | 5     | 6     | 2
              2      | 6     | 6     | 1
              3      | 0     | 0     | 1
              3      | 0     | 1     | 2
              3      | 1     | 2     | 2
              3      | 2     | 3     | 2
              3      | 3     | 3     | 1
              3      | 7     | 7     | 1
              3      | 7     | 7     | 1

        """
        )
    else:
        expected = T(
            """
        _pw_instance | min_t | max_t | count
                2    | -2    | -1    | 2
                3    | 0     | 3     | 4
                1    | 1     | 7     | 5
                2    | 5     | 6     | 2
                3    | 7     | 7     | 1

        """
        )

    grouped = t.windowby(
        pw.this.t,
        window=w,
        instance=pw.this.k,
    )
    res = grouped.reduce(
        pw.this._pw_instance,
        min_t=pw.reducers.min(pw.this.t),
        max_t=pw.reducers.max(pw.this.t),
        count=pw.reducers.count(),
    )

    assert_table_equality_wo_index(res, expected)


@pytest.mark.parametrize(
    "w",
    [
        pw.temporal.session(max_gap=datetime.timedelta(minutes=10)),
        pw.temporal.tumbling(duration=datetime.timedelta(minutes=30)),
        pw.temporal.sliding(
            hop=datetime.timedelta(minutes=15), duration=datetime.timedelta(minutes=30)
        ),
    ],
)
def test_windows_with_utc_datetimes(w):
    table = pw.debug.table_from_markdown(
        """
      |             t             | a
    1 | 2023-05-15T10:13:00+02:00 | 1
    2 | 2023-05-15T10:14:00+02:00 | 2
    3 | 2023-05-15T10:14:00+02:00 | 3
    4 | 2023-05-15T10:26:00+02:00 | 4
    5 | 2023-05-15T10:31:23+02:00 | 5
    6 | 2023-05-15T11:00:20+02:00 | 6
    """
    )
    if w == pw.temporal.session(max_gap=datetime.timedelta(minutes=10)):
        expected = T(
            """
         | min_a | max_a
       1 |   1   |   3
       2 |   4   |   5
       3 |   6   |   6
        """
        )

    elif w == pw.temporal.tumbling(duration=datetime.timedelta(minutes=30)):
        expected = T(
            """
         | min_a | max_a
       1 |   1   |   4
       2 |   5   |   5
       3 |   6   |   6
        """
        )
    else:
        expected = T(
            """
         | min_a | max_a
       1 |   1   |   3
       2 |   1   |   4
       3 |   4   |   5
       4 |   5   |   5
       5 |   6   |   6
       6 |   6   |   6
        """
        )

    table = table.with_columns(t=pw.this.t.dt.strptime("%Y-%m-%dT%H:%M:%S%z"))
    res = table.windowby(
        pw.this.t,
        window=w,
    ).reduce(min_a=pw.reducers.min(pw.this.a), max_a=pw.reducers.max(pw.this.a))

    assert_table_equality_wo_index(res, expected)


@pytest.mark.parametrize(
    "w",
    [
        pw.temporal.session(max_gap=datetime.timedelta(minutes=10)),
        pw.temporal.tumbling(duration=datetime.timedelta(minutes=30)),
        pw.temporal.sliding(
            hop=datetime.timedelta(minutes=15), duration=datetime.timedelta(minutes=30)
        ),
    ],
)
def test_windows_with_datetimes(w):
    table = pw.debug.table_from_markdown(
        """
      |          t          | a
    1 | 2023-05-15T10:13:00 | 1
    2 | 2023-05-15T10:14:00 | 2
    3 | 2023-05-15T10:14:00 | 3
    4 | 2023-05-15T10:26:00 | 4
    5 | 2023-05-15T10:31:23 | 5
    6 | 2023-05-15T11:00:20 | 6
    """
    )
    if w == pw.temporal.session(max_gap=datetime.timedelta(minutes=10)):
        expected = T(
            """
         | min_a | max_a
       1 |   1   |   3
       2 |   4   |   5
       3 |   6   |   6
        """
        )

    elif w == pw.temporal.tumbling(duration=datetime.timedelta(minutes=30)):
        expected = T(
            """
         | min_a | max_a
       1 |   1   |   4
       2 |   5   |   5
       3 |   6   |   6
        """
        )
    else:
        expected = T(
            """
         | min_a | max_a
       1 |   1   |   3
       2 |   1   |   4
       3 |   4   |   5
       4 |   5   |   5
       5 |   6   |   6
       6 |   6   |   6
        """
        )

    table = table.with_columns(t=pw.this.t.dt.strptime("%Y-%m-%dT%H:%M:%S"))
    res = table.windowby(
        pw.this.t,
        window=w,
    ).reduce(min_a=pw.reducers.min(pw.this.a), max_a=pw.reducers.max(pw.this.a))

    assert_table_equality_wo_index(res, expected)


@pytest.mark.parametrize(
    "dtype,window,error_str",
    [
        (
            int,
            pw.temporal.tumbling(duration=datetime.timedelta(days=1)),
            ", window.hop",
        ),
        (
            int,
            pw.temporal.tumbling(
                duration=datetime.timedelta(days=1),
                origin=datetime.datetime(year=1970, month=1, day=1),
            ),
            ", window.hop, window.origin",
        ),
        (
            DATE_TIME_UTC,
            pw.temporal.sliding(hop=2, duration=3.5),
            ", window.hop, window.duration",
        ),
        (DATE_TIME_NAIVE, pw.temporal.tumbling(duration=1.2), ", window.hop"),
        (
            int,
            pw.temporal.tumbling(duration=datetime.timedelta(days=1)),
            ", window.hop",
        ),
        (DATE_TIME_NAIVE, pw.temporal.session(max_gap=2), ", window.max_gap"),
        (
            DATE_TIME_NAIVE,
            pw.temporal.sliding(hop=2, duration=3.5),
            ", window.hop, window.duration",
        ),
    ],
)
def test_incorrect_args(dtype, window, error_str):
    t1 = pw.Table.empty(a=int, t=dtype)

    with pytest.raises(
        TypeError,
        match=rf"Arguments \(time_expr{error_str}"
        + r"\) have to be of types .* but are of types .*",
    ):
        t1.windowby(t1.t, window=window)


def test_intervals_over():
    t = T(
        """
        | t |  v
    1   | 1 |  10
    2   | 2 |  1
    3   | 3 |  3
    4   | 8 |  2
    5   | 9 |  4
    6   | 10|  8
    7   | 1 |  9
    8   | 2 |  16
    """
    )
    probes = T(
        """
    t
    2
    4
    6
    8
    10
    """
    )
    result = pw.temporal.windowby(
        t,
        t.t,
        window=pw.temporal.intervals_over(
            at=probes.t, lower_bound=-2, upper_bound=1, is_outer=False
        ),
    ).reduce(pw.this._pw_window_location, v=pw.reducers.tuple(pw.this.v))

    df = pd.DataFrame(
        {
            "_pw_window_location": [2, 4, 8, 10],
            "v": [(10, 9, 16, 1, 3), (16, 1, 3), (2, 4), (2, 4, 8)],
        }
    )
    expected = pw.debug.table_from_pandas(
        df,
        schema=pw.schema_from_types(_pw_window_location=int, v=list[int]),
    )
    assert_table_equality_wo_index(result, expected)


def test_intervals_over_with_instance():
    t = T(
        """
        | t |  v  | instance
    1   | 1 |  10 | 1
    2   | 2 |  1  | 1
    3   | 4 |  3  | 1
    4   | 8 |  2  | 1
    5   | 9 |  4  | 2
    6   | 10|  8  | 2
    7   | 1 |  9  | 2
    8   | 2 |  16 | 2
    """
    )
    probes = T(
        """
    t
    2
    6
    10
    """
    )
    result = pw.temporal.windowby(
        t,
        t.t,
        window=pw.temporal.intervals_over(
            at=probes.t, lower_bound=-4, upper_bound=2, is_outer=False
        ),
        instance=pw.this.instance,
    ).reduce(
        pw.this._pw_window_location,
        pw.this._pw_instance,
        v=pw.reducers.tuple(pw.this.v),
    )

    df = pd.DataFrame(
        {
            "_pw_window_location": [2, 2, 6, 6, 10, 10],
            "_pw_instance": [1, 2, 1, 2, 1, 2],
            "v": [(10, 1, 3), (9, 16), (1, 3, 2), (16,), (2,), (4, 8)],
        }
    )
    expected = pw.debug.table_from_pandas(
        df,
        schema=pw.schema_from_types(
            _pw_window_location=int, _pw_instance=int, v=list[int]
        ),
    )
    assert_table_equality_wo_index(result, expected)


def test_intervals_over_works_on_same_table():
    t = T(
        """
        | t
    1   | 1
    2   | 2
    3   | 3
    4   | 4
    5   | 5
    """
    )
    result = pw.temporal.windowby(
        t,
        t.t,
        window=pw.temporal.intervals_over(
            at=t.t, lower_bound=-2, upper_bound=0, is_outer=False
        ),
    ).reduce(pw.this._pw_window_location, v=pw.reducers.sorted_tuple(pw.this.t))

    df = pd.DataFrame(
        {
            "_pw_window_location": [1, 2, 3, 4, 5],
            "v": [(1,), (1, 2), (1, 2, 3), (2, 3, 4), (3, 4, 5)],
        }
    )
    expected = pw.debug.table_from_pandas(
        df,
        schema=pw.schema_from_types(_pw_window_location=int, v=list[int]),
    )
    assert_table_equality_wo_index(result, expected)


def test_intervals_over_outer():
    t = T(
        """
        | t |  v
    1   | 1 |  10
    2   | 2 |  1
    3   | 3 |  3
    4   | 8 |  2
    5   | 9 |  4
    6   | 10|  8
    7   | 1 |  9
    8   | 2 |  16
    """
    )
    probes = T(
        """
    t
    2
    4
    6
    8
    10
    """
    )
    result = pw.temporal.windowby(
        t,
        t.t,
        window=pw.temporal.intervals_over(
            at=probes.t, lower_bound=-2, upper_bound=1, is_outer=True
        ),
    ).reduce(pw.this._pw_window_location, v=pw.reducers.sorted_tuple(pw.this.v))

    df = pd.DataFrame(
        {
            "_pw_window_location": [2, 4, 6, 8, 10],
            "v": [(1, 3, 9, 10, 16), (1, 3, 16), (None,), (2, 4), (2, 4, 8)],
        }
    )
    expected = pw.debug.table_from_pandas(
        df,
        schema=pw.schema_from_types(
            _pw_window_location=int, v=list[typing.Optional[int]]
        ),
    )
    assert_table_equality_wo_index(result, expected)


def test_intervals_over_with_reducer_over_ix():
    values = T(
        """
        | v
    1   | 1
    2   | 2
    3   | 6
    4   | 3
    5   | 9
    6   | 3
    7   | 2
    8   | -5
    9   | 1
    10  | 7
    """
    )
    t = T(
        """
        | t |  ptr
    1   | 1 |  10
    2   | 2 |  1
    3   | 4 |  3
    4   | 8 |  2
    5   | 9 |  4
    6   | 10|  8
    7   | 5 |  9
    8   | 3 |  7
    """
    ).select(pw.this.t, ptr=values.pointer_from(pw.this.ptr))
    probes = pw.debug.table_from_markdown(
        """
    t
    2
    4
    6
    8
    10
    """
    )
    grouped_table = pw.temporal.windowby(
        t,
        t.t,
        window=pw.temporal.intervals_over(
            at=probes.t, lower_bound=-1, upper_bound=1, is_outer=False
        ),
    )
    result = grouped_table.reduce(
        pw.this._pw_window_location, v=pw.reducers.tuple(values.ix(grouped_table.ptr).v)
    )

    df = pd.DataFrame(
        {
            "_pw_window_location": [2, 4, 6, 8, 10],
            "v": [(7, 1, 2), (2, 6, 1), (1,), (2, 3), (3, -5)],
        }
    )
    expected = pw.debug.table_from_pandas(
        df,
        schema=pw.schema_from_types(_pw_window_location=int, v=list[int]),
    )
    assert_table_equality_wo_index(result, expected)


def test_latest_reducer():
    t = T(
        """
        t | a
        1 | 1
        2 | 2
        3 | 3
    """
    )

    msg = re.escape(
        "latest reducer uses processing time to choose elements"
        + " while windowby uses data time to assign entries to windows."
        + " Maybe it is not the behavior you want. To choose elements according"
        + " to their data time, you may use max reducer."
    )
    with warns_here(match=msg):
        res = t.windowby(
            pw.this.t, window=pw.temporal.sliding(hop=1, duration=2)
        ).reduce(t=pw.this._pw_window_start, a=pw.reducers.latest(pw.this.a))
    expected = T(
        """
        t | a
        0 | 1
        1 | 1
        2 | 2
        3 | 3
    """
    )
    assert_table_equality_wo_index(res, expected)
