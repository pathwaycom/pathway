# Copyright © 2023 Pathway

import pathway as pw
import pathway.internals.dtype as dt
from pathway.tests.utils import T


def test_date_time_naive_schema():
    table = T(
        """
      |         t1          |         t2
    0 | 2023-05-15T10:13:00 | 2023-05-15T10:13:23
    """
    )
    fmt = "%Y-%m-%dT%H:%M:%S"
    table_with_datetimes = table.select(
        t1=table.t1.dt.strptime(fmt=fmt), t2=table.t2.dt.strptime(fmt=fmt)
    )
    table_with_datetimes = table_with_datetimes.with_columns(
        diff=pw.this.t1 - pw.this.t2
    )
    assert table_with_datetimes.schema._dtypes() == {
        "t1": dt.DATE_TIME_NAIVE,
        "t2": dt.DATE_TIME_NAIVE,
        "diff": dt.DURATION,
    }


def test_date_time_utc_schema():
    table = T(
        """
      |            t1             |            t2
    0 | 2023-05-15T10:13:00+01:00 | 2023-05-15T10:13:23+01:00
    """
    )
    fmt = "%Y-%m-%dT%H:%M:%S%z"
    table_with_datetimes = table.select(
        t1=table.t1.dt.strptime(fmt=fmt), t2=table.t2.dt.strptime(fmt=fmt)
    )
    table_with_datetimes = table_with_datetimes.with_columns(
        diff=pw.this.t1 - pw.this.t2
    )
    assert table_with_datetimes.schema._dtypes() == {
        "t1": dt.DATE_TIME_UTC,
        "t2": dt.DATE_TIME_UTC,
        "diff": dt.DURATION,
    }
