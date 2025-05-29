# Copyright © 2024 Pathway

import datetime
import operator
import re
from typing import Any

import numpy as np
import pandas as pd
import pytest
from dateutil import tz

import pathway as pw
from pathway.debug import table_from_markdown, table_from_pandas
from pathway.internals import dtype as dt
from pathway.tests.utils import (
    assert_table_equality,
    assert_table_equality_wo_index,
    deprecated_call_here,
    run_all,
)


@pytest.mark.parametrize(
    "method_name,unit",
    [
        ("nanoseconds", 1),
        ("microseconds", 1_000),
        ("milliseconds", 1_000_000),
        ("seconds", 1_000_000_000),
        ("minutes", 60 * 1_000_000_000),
        ("hours", 3600 * 1_000_000_000),
        ("days", 24 * 3600 * 1_000_000_000),
        ("weeks", 7 * 24 * 3600 * 1_000_000_000),
    ],
)
def test_duration(method_name: str, unit: int) -> None:
    df = pd.DataFrame(
        {
            "a": [
                pd.Timedelta(0),
                pd.Timedelta(-1),
                pd.Timedelta(-2),
                pd.Timedelta(1),
                pd.Timedelta(2),
                pd.Timedelta(microseconds=-2),
                pd.Timedelta(microseconds=3),
                pd.Timedelta(milliseconds=-2),
                pd.Timedelta(milliseconds=3),
                pd.Timedelta(seconds=-2),
                pd.Timedelta(seconds=3),
                pd.Timedelta(minutes=-2),
                pd.Timedelta(minutes=3),
                pd.Timedelta(hours=-2),
                pd.Timedelta(hours=3),
                pd.Timedelta(days=-2),
                pd.Timedelta(days=3),
                pd.Timedelta(weeks=-2),
                pd.Timedelta(weeks=3),
                pd.Timedelta(906238033887173888),
                pd.Timedelta(-25028201030208546),
                pd.Timedelta(-560647988758320624),
                pd.Timedelta(21569578082613316),
                pd.Timedelta(461037051895230252),
                pd.Timedelta(888145670672098607),
                pd.Timedelta(-916627150335519587),
                pd.Timedelta(-74827964329550952),
                pd.Timedelta(-126273201490715187),
                pd.Timedelta(125605450924133901),
            ]
        }
    )
    table = table_from_pandas(df)
    table_pw = table.select(a=table.a.dt.__getattribute__(method_name)())
    df_new = pd.DataFrame({"a": (df.a.values / unit).astype(np.int64)})
    table_pd = table_from_pandas(df_new)

    assert_table_equality(table_pw, table_pd)


@pytest.mark.parametrize("is_naive", [True, False])
@pytest.mark.parametrize(
    "method_name",
    [
        "nanosecond",
        "microsecond",
        "millisecond",
        "second",
        "minute",
        "hour",
        "day",
        "month",
        "year",
    ],
)
def test_date_time(method_name: str, is_naive: bool) -> None:
    data = [
        "1960-02-03 08:00:00.000000000",
        "1960-02-03 08:00:00.123456789",
        "2008-02-29 08:00:00.000000000",
        "2023-03-25 12:00:00.000000000",
        "2023-03-25 12:00:00.000000001",
        "2023-03-25 12:00:00.123456789",
        "2023-03-25 16:43:21.000123000",
        "2023-03-25 17:00:01.987000000",
        "2023-03-25 22:59:59.999999999",
        "2023-03-25 23:00:00.000000001",
        "2023-03-25 23:59:59.999999999",
        "2023-03-26 00:00:00.000000001",
        "2023-03-26 12:00:00.000000001",
        "2123-03-26 12:00:00.000000001",
        "2123-03-31 23:00:00.000000001",
    ]
    fmt = "%Y-%m-%d %H:%M:%S.%f"
    if not is_naive:
        data = [entry + "-02:00" for entry in data[:-2]]
        fmt += "%z"
    df = pd.DataFrame({"a": pd.to_datetime(data, format=fmt)})
    if not is_naive:
        df.a = df.a.dt.tz_convert(tz.UTC)
    if method_name == "nanosecond":
        series_new = df.a.dt.nanosecond + df.a.dt.microsecond * 1000
    elif method_name == "millisecond":
        series_new = df.a.dt.microsecond // 1000
    elif method_name == "timestamp":
        series_new = df.a.values.astype(np.int64)
    else:
        series_new = getattr(df.a.dt, method_name)
    df_new = pd.DataFrame({"a": series_new})
    table_pd = table_from_pandas(df_new)

    table = table_from_pandas(pd.DataFrame({"a": pd.to_datetime(data, format=fmt)}))
    table_pw = table.select(a=getattr(table.a.dt, method_name)())
    assert_table_equality(table_pw, table_pd)


@pytest.mark.parametrize("is_naive", [True, False])
def test_timestamp(is_naive: bool) -> None:
    data = [
        "1960-02-03 08:00:00.000000000",
        "1960-02-03 08:00:00.123456789",
        "2008-02-29 08:00:00.000000000",
        "2023-03-25 12:00:00.000000000",
        "2023-03-25 12:00:00.000000001",
        "2023-03-25 12:00:00.123456789",
        "2023-03-25 16:43:21.000123000",
        "2023-03-25 17:00:01.987000000",
        "2023-03-25 22:59:59.999999999",
        "2023-03-25 23:00:00.000000001",
        "2023-03-25 23:59:59.999999999",
        "2023-03-26 00:00:00.000000001",
        "2023-03-26 12:00:00.000000001",
        "2123-03-26 12:00:00.000000001",
        "2123-03-31 23:00:00.000000001",
    ]
    fmt = "%Y-%m-%d %H:%M:%S.%f"
    if not is_naive:
        data = [entry + "-02:00" for entry in data[:-2]]
        fmt += "%z"
    series = pd.to_datetime(pd.Series(data), format=fmt)
    if not is_naive:
        series = series.dt.tz_convert(tz.UTC)
    df = pd.DataFrame({"ns": series.values.astype(np.int64)})
    table_pd = table_from_pandas(df).select(
        nounit=pw.this.ns,
        ns=pw.this.ns / 1,
        us=pw.this.ns / 1000,
        ms=pw.this.ns / 1e6,
        s=pw.this.ns / 1e9,
    )

    table = table_from_pandas(pd.DataFrame({"a": pd.to_datetime(data, format=fmt)}))

    with deprecated_call_here():
        nounit = table.a.dt.timestamp()

    table_pw = table.select(
        nounit=nounit,
        ns=pw.this.a.dt.timestamp(unit="ns"),
        us=pw.this.a.dt.timestamp(unit="us"),
        ms=pw.this.a.dt.timestamp(unit="ms"),
        s=pw.this.a.dt.timestamp(unit="s"),
    )
    assert_table_equality(table_pw, table_pd)


def test_timestamp_without_unit_deprecated() -> None:
    table = table_from_markdown(
        """
        time
          2
    """
    ).select(ts=pw.this.time.dt.from_timestamp(unit="s"))

    with deprecated_call_here(
        match=re.escape(
            "Not specyfying the `unit` argument of the `timestamp()` method is deprecated. "
            "Please specify its value. Without specifying, it will default to 'ns'."
        ),
    ):
        table.select(time=pw.this.ts.dt.timestamp())


@pytest.mark.parametrize("is_naive", [True, False])
@pytest.mark.parametrize(
    "fmt_out",
    [
        "%a",
        "%A",
        "%w",
        "%d",
        "%b",
        "%B",
        "%m",
        "%y",
        "%Y",
        "%H",
        "%I",
        "%p",
        "%M",
        "%S",
        "%f",
        "%z",
        "%Z",
        "%j",
        "%U",
        "%W",
        "%c",
        "%x",
        "%X",
        "%%%Y",
        "%G",
        "%u",
        "%V",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S.%%f",
        "%%H:%%M:%%S",  # test that %%sth are not changed to values
    ],
)
def test_strftime(fmt_out: str, is_naive: bool) -> None:
    if "%Z" in fmt_out:
        pytest.xfail("%Z output different between python and rust chrono")
    if "%z" in fmt_out and is_naive:
        pytest.xfail("%z (timezone) failing for naive timestamp")  # FIXME
    data = [
        "1960-02-03 08:00:00.000000000",
        "2008-02-29 08:00:00.000000000",
        "2023-03-25 12:00:00.000000000",
        "2023-03-25 12:00:00.000000001",
        "2023-03-25 12:00:00.123456789",
        "2023-03-25 16:43:21.000123000",
        "2023-03-25 17:00:01.987000000",
        "2023-03-25 23:59:59.999999999",
        "2023-03-26 01:59:59.999999999",
        "2023-03-26 03:00:00.000000001",
        "2023-03-26 04:00:00.000000001",
        "2023-03-26 12:00:00.000000001",
        "2123-03-26 12:00:00.000000001",
    ]
    fmt_in = "%Y-%m-%d %H:%M:%S.%f"
    if not is_naive:
        data = [entry + "-02:00" for entry in data]
        fmt_in += "%z"
    df = pd.DataFrame({"ts": pd.to_datetime(data, format=fmt_in)})
    if is_naive:
        df_converted = df
    else:
        df_converted = pd.DataFrame({"ts": df.ts.dt.tz_convert(tz.UTC)})
    df_new = pd.DataFrame({"txt": df_converted.ts.dt.strftime(fmt_out)})
    table = table_from_pandas(df)
    fmt_out_pw = fmt_out.replace("%f", "%6f")
    fmt_out_pw = fmt_out_pw.replace("%%6f", "%%f")
    table_pw = table.select(txt=table.ts.dt.strftime(fmt_out_pw))
    table_pd = table_from_pandas(df_new)
    assert_table_equality(table_pw, table_pd)


def test_strftime_with_format_in_column() -> None:
    pairs = [
        ("1960-02-03T12:45:12.000000", "%Y-%m-%d %H:%M:%S"),
        ("2023-03-25T16:43:21.000000", "%Y-%m-%dT%H:%M:%S"),
        ("2023-03-25T16:43:21.567891", "%Y-%m-%dT%H:%M:%S.%6f"),
        ("2023-05-12T11:14:45.000000", "%H:%M:%S %Y-%m-%d"),
    ]
    expected = table_from_pandas(
        pd.DataFrame(
            {
                "date_str": [
                    "1960-02-03 12:45:12",
                    "2023-03-25T16:43:21",
                    "2023-03-25T16:43:21.567891",
                    "11:14:45 2023-05-12",
                ]
            }
        )
    )
    fmt_in = "%Y-%m-%dT%H:%M:%S.%6f"
    pairs_T = list(zip(*pairs))
    df = pd.DataFrame({"ts": pairs_T[0], "fmt": pairs_T[1]})
    table = table_from_pandas(df)
    table_with_datetime = table.with_columns(date=table.ts.dt.strptime(fmt_in))
    res = table_with_datetime.select(
        date_str=table_with_datetime.date.dt.strftime(table_with_datetime.fmt)
    )
    assert_table_equality_wo_index(res, expected)


@pytest.mark.parametrize(
    "data,fmt",
    [
        (["1960-02-03", "2023-03-25", "2023-03-26", "2123-03-26"], "%Y-%m-%d"),
        (["03.02.1960", "25.03.2023", "26.03.2023", "26.03.2123"], "%d.%m.%Y"),
        (["02.03.1960", "03.25.2023", "03.26.2023", "03.26.2123"], "%m.%d.%Y"),
        (["12:34:00", "01:22:12", "13:00:34", "23:59:59"], "%H:%M:%S"),
        (["12:34:00 PM", "01:22:12 AM", "01:00:34 PM", "11:59:59 PM"], "%I:%M:%S %p"),
        (
            ["12:34:00.000000000", "01:22:12.123456789", "13:00:34.111111111"],
            "%H:%M:%S.%f",
        ),
        (["2023-03-25 16:43:21", "2023-03-26 16:43:21"], "%Y-%m-%d %H:%M:%S"),
        (["2023-03-25T16:43:21", "2023-03-26T16:43:21"], "%Y-%m-%dT%H:%M:%S"),
        (["2023-03-25 04:43:21 AM", "2023-03-26 04:43:21 PM"], "%Y-%m-%d %I:%M:%S %p"),
        (
            [
                "1900-01-01 00:00:00.396",
                "1900-01-01 00:00:00.396093123",
                "2023-03-25 16:43:21.123456789",
                "2023-03-26 16:43:21.123456789",
                "2023-03-26 16:43:21.12",
            ],
            "%Y-%m-%d %H:%M:%S.%f",
        ),
        (
            [
                "1900-01-01 %f00:00:00.396",
                "1900-01-01 %f00:00:00.396093123",
                "2023-03-25 %f16:43:21.123456789",
                "2023-03-26 %f16:43:21.123456789",
                "2023-03-26 %f16:43:21.12",
            ],
            "%Y-%m-%d %%f%H:%M:%S.%f",
        ),
    ],
)
def test_strptime_naive(data: list[str], fmt: str) -> None:
    df = pd.DataFrame({"ts": pd.to_datetime(data, format=fmt)})
    table_pd = table_from_pandas(df)
    table = table_from_pandas(pd.DataFrame({"a": data}))
    table_pw = table.select(ts=table.a.dt.strptime(fmt))
    assert_table_equality(table_pw, table_pd)


@pytest.mark.parametrize(
    "data,fmt",
    [
        (["1960-02-03", "2023-03-25", "2023-03-26", "2123-03-26"], "%Y-%m-%d"),
        (["03.02.1960", "25.03.2023", "26.03.2023", "26.03.2123"], "%d.%m.%Y"),
        (["02.03.1960", "03.25.2023", "03.26.2023", "03.26.2123"], "%m.%d.%Y"),
        (["12:34:00", "01:22:12", "13:00:34", "23:59:59"], "%H:%M:%S"),
        (["12:34:00 PM", "01:22:12 AM", "01:00:34 PM", "11:59:59 PM"], "%I:%M:%S %p"),
        (
            ["12:34:00.000000", "01:22:12.12345", "13:00:34.11"],
            "%H:%M:%S.%f",
        ),
        (["2023-03-25 16:43:21", "2023-03-26 16:43:21"], "%Y-%m-%d %H:%M:%S"),
        (["2023-03-25T16:43:21", "2023-03-26T16:43:21"], "%Y-%m-%dT%H:%M:%S"),
        (["2023-03-25 04:43:21 AM", "2023-03-26 04:43:21 PM"], "%Y-%m-%d %I:%M:%S %p"),
        (
            [
                "1900-01-01 00:00:00.396",
                "1900-01-01 00:00:00.396093",
                "2023-03-25 16:43:21.123456",
                "2023-03-26 16:43:21.123456",
                "2023-03-26 16:43:21.12",
            ],
            "%Y-%m-%d %H:%M:%S.%f",
        ),
    ],
)
def test_strptime_naive_with_python_datetime(data: list[str], fmt: str) -> None:
    table = table_from_pandas(pd.DataFrame({"a": data})).select(
        ts=pw.this.a.dt.strptime(fmt)
    )

    @pw.udf
    def parse_datetime(date_str: str) -> dt.DATE_TIME_NAIVE:
        return datetime.datetime.strptime(date_str, fmt)  # type: ignore [return-value]

    expected = table_from_pandas(pd.DataFrame({"a": data})).select(
        ts=parse_datetime(pw.this.a)
    )
    assert_table_equality(table, expected)


@pytest.mark.parametrize(
    "data,fmt",
    [
        (
            ["2023-03-25 16:43:21+0123", "2023-03-26 16:43:21+0123"],
            "%Y-%m-%d %H:%M:%S%z",
        ),
        (
            ["2023-03-25 16:43:21+01:23", "2023-03-26 16:43:21+01:23"],
            "%Y-%m-%d %H:%M:%S%:z",
        ),
        (
            ["2023-03-25T16:43:21+01:23", "2023-03-26T16:43:21+01:23"],
            "%Y-%m-%dT%H:%M:%S%z",
        ),
        (
            ["2023-03-25 04:43:21 AM +01:23", "2023-03-26 04:43:21 PM +01:23"],
            "%Y-%m-%d %I:%M:%S %p %z",
        ),
        (
            [
                "1900-01-01 00:00:00.396-11:05",
                "1900-01-01 00:00:00.396093123-11:05",
                "2023-03-25 16:43:21.123456789-11:05",
                "2023-03-26 16:43:21.123456789-11:05",
                "2023-03-26 16:43:21.12-11:05",
            ],
            "%Y-%m-%d %H:%M:%S.%f%z",
        ),
        (
            [
                "1900%f01-01 00:00:00.396-11:05",
                "1900%f01-01 00:00:00.396093123-11:05",
                "2023%f03-25 16:43:21.123456789-11:05",
                "2023%f03-26 16:43:21.123456789-11:05",
                "2023%f03-26 16:43:21.12-11:05",
            ],
            "%Y%%f%m-%d %H:%M:%S.%f%z",
        ),
    ],
)
def test_strptime_time_zone_aware(data: list[str], fmt: str) -> None:
    pandas_fmt = fmt.replace("%:z", "%z")  # pandas does not support %:z
    df = pd.DataFrame({"ts": pd.to_datetime(data, format=pandas_fmt)})
    table_pd = table_from_pandas(df)
    table = table_from_pandas(pd.DataFrame({"a": data}))
    table_pw = table.select(ts=table.a.dt.strptime(fmt))
    assert_table_equality(table_pw, table_pd)


@pytest.mark.parametrize(
    "data,fmt",
    [
        (
            ["2023-03-25 16:43:21+01:23", "2023-03-26 16:43:21+01:23"],
            "%Y-%m-%d %H:%M:%S%z",
        ),
        (
            ["2023-03-25T16:43:21+01:23", "2023-03-26T16:43:21+01:23"],
            "%Y-%m-%dT%H:%M:%S%z",
        ),
        (
            ["2023-03-25 04:43:21 AM +01:23", "2023-03-26 04:43:21 PM +01:23"],
            "%Y-%m-%d %I:%M:%S %p %z",
        ),
        (
            [
                "1900-01-01 00:00:00.396-11:05",
                "1900-01-01 00:00:00.396093-11:05",
                "2023-03-25 16:43:21.1234-11:05",
                "2023-03-26 16:43:21.12345-11:05",
                "2023-03-26 16:43:21.12-11:05",
            ],
            "%Y-%m-%d %H:%M:%S.%f%z",
        ),
    ],
)
def test_strptime_time_zone_aware_with_python_datetime(
    data: list[str], fmt: str
) -> None:
    table = table_from_pandas(pd.DataFrame({"a": data})).select(
        ts=pw.this.a.dt.strptime(fmt)
    )

    @pw.udf
    def parse_datetime(date_str: str) -> dt.DATE_TIME_UTC:
        return datetime.datetime.strptime(date_str, fmt)  # type: ignore [return-value]

    expected = table_from_pandas(pd.DataFrame({"a": data})).select(
        ts=parse_datetime(pw.this.a)
    )
    assert_table_equality(table, expected)


def test_strptime_with_format_in_column() -> None:
    pairs = [
        ("1960-02-03 12:45:12", "%Y-%m-%d %H:%M:%S"),
        ("2023-03-25T16:43:21", "%Y-%m-%dT%H:%M:%S"),
        ("2023-03-25T16:43:21.567891234", "%Y-%m-%dT%H:%M:%S.%f"),
        ("11:14:45 2023-05-12", "%H:%M:%S %Y-%m-%d"),
    ]
    expected = table_from_markdown(
        """
         |      date_str
       1 | 1960-02-03T12:45:12.000000
       2 | 2023-03-25T16:43:21.000000
       3 | 2023-03-25T16:43:21.567891
       4 | 2023-05-12T11:14:45.000000
    """
    )
    fmt_out = "%Y-%m-%dT%H:%M:%S.%6f"
    pairs_T = list(zip(*pairs))
    df = pd.DataFrame({"ts": pairs_T[0], "fmt": pairs_T[1]})
    table = table_from_pandas(df)
    table_with_datetime = table.select(
        date=table.ts.dt.strptime(table.fmt, contains_timezone=False)
    )
    res = table_with_datetime.select(
        date_str=table_with_datetime.date.dt.strftime(fmt_out)
    )
    assert_table_equality_wo_index(res, expected)


def test_strptime_naive_errors_on_wrong_specifier() -> None:
    table_from_pandas(pd.DataFrame({"a": ["2023-03-26 16:43:21-12"]})).select(
        t=pw.this.a.dt.strptime("%Y-%m-%d %H:%M:%S-%f")
    )
    with pytest.raises(
        ValueError,
        match=re.escape(
            'parse error: cannot use format "%Y-%m-%d %H:%M:%S-%f": '
            'using "%f" without the leading dot is not supported'
        ),
    ):
        run_all()


def test_strptime_naive_errors_on_wrong_format() -> None:
    table_from_pandas(pd.DataFrame({"a": ["2023-03-26T16:43:21.12"]})).select(
        t=pw.this.a.dt.strptime("%Y-%m-%d %H:%M:%S.%f")
    )
    with pytest.raises(
        ValueError,
        match=re.escape(
            'parse error: cannot parse date "2023-03-26T16:43:21.12" '
            'using format "%Y-%m-%d %H:%M:%S%.f"'
        ),
    ):
        run_all()


def test_strptime_utc_errors_on_wrong_specifier() -> None:
    table_from_pandas(pd.DataFrame({"a": ["2023-03-26 16:43:21-12+0100"]})).select(
        t=pw.this.a.dt.strptime("%Y-%m-%d %H:%M:%S-%f%z")
    )
    with pytest.raises(
        ValueError,
        match=re.escape(
            'parse error: cannot use format "%Y-%m-%d %H:%M:%S-%f%z": '
            'using "%f" without the leading dot is not supported'
        ),
    ):
        run_all()


def test_strptime_utc_errors_on_wrong_format() -> None:
    table_from_pandas(pd.DataFrame({"a": ["2023-03-26T16:43:21.12-0100"]})).select(
        t=pw.this.a.dt.strptime("%Y-%m-%d %H:%M:%S.%f%z")
    )
    with pytest.raises(
        ValueError,
        match=re.escape(
            'parse error: cannot parse date "2023-03-26T16:43:21.12-0100" '
            'using format "%Y-%m-%d %H:%M:%S%.f%z"'
        ),
    ):
        run_all()


def test_date_time_naive_to_utc() -> None:
    table = table_from_markdown(
        """
           |         date_string
         1 | 2023-03-25T12:00:00.000000000
         2 | 2023-03-25T23:00:00.000000000
         3 | 2023-03-26T00:00:00.000000000
         4 | 2023-03-26T01:00:00.000000000
         5 | 2023-03-26T01:59:59.999999999
         6 | 2023-03-26T02:00:00.000000000
         7 | 2023-03-26T02:00:00.000000001
         8 | 2023-03-26T02:30:00.000000000
         9 | 2023-03-26T02:59:59.999999999
        10 | 2023-03-26T03:00:00.000000000
        11 | 2023-03-26T03:00:00.000000001
        12 | 2023-03-26T03:30:00.000000000
        13 | 2023-03-26T04:00:00.000000000
        14 | 2023-10-28T23:00:00.000000000
        15 | 2023-10-29T01:00:00.000000000
        16 | 2023-10-29T01:59:59.999999999
        17 | 2023-10-29T02:00:00.000000000
        18 | 2023-10-29T02:00:00.000000001
        19 | 2023-10-29T02:00:30.000000000
        20 | 2023-10-29T02:59:59.999999999
        21 | 2023-10-29T03:00:00.000000000
        22 | 2023-10-29T03:00:00.000000001
        23 | 2023-10-29T03:30:00.000000000
        24 | 2023-10-29T04:00:00.000000000
    """
    )

    expected = table_from_markdown(
        """
           |         date_string
         1 | 2023-03-25T11:00:00.000000000+0000
         2 | 2023-03-25T22:00:00.000000000+0000
         3 | 2023-03-25T23:00:00.000000000+0000
         4 | 2023-03-26T00:00:00.000000000+0000
         5 | 2023-03-26T00:59:59.999999999+0000
         6 | 2023-03-26T01:00:00.000000000+0000
         7 | 2023-03-26T01:00:00.000000000+0000
         8 | 2023-03-26T01:00:00.000000000+0000
         9 | 2023-03-26T01:00:00.000000000+0000
        10 | 2023-03-26T01:00:00.000000000+0000
        11 | 2023-03-26T01:00:00.000000001+0000
        12 | 2023-03-26T01:30:00.000000000+0000
        13 | 2023-03-26T02:00:00.000000000+0000
        14 | 2023-10-28T21:00:00.000000000+0000
        15 | 2023-10-28T23:00:00.000000000+0000
        16 | 2023-10-28T23:59:59.999999999+0000
        17 | 2023-10-29T01:00:00.000000000+0000
        18 | 2023-10-29T01:00:00.000000001+0000
        19 | 2023-10-29T01:00:30.000000000+0000
        20 | 2023-10-29T01:59:59.999999999+0000
        21 | 2023-10-29T02:00:00.000000000+0000
        22 | 2023-10-29T02:00:00.000000001+0000
        23 | 2023-10-29T02:30:00.000000000+0000
        24 | 2023-10-29T03:00:00.000000000+0000
    """
    )
    fmt_in = "%Y-%m-%dT%H:%M:%S.%f"
    fmt_out = "%Y-%m-%dT%H:%M:%S.%f%z"
    table_with_datetime = table.select(t=table.date_string.dt.strptime(fmt_in))
    table_utc = table_with_datetime.select(
        t=table_with_datetime.t.dt.to_utc("Europe/Warsaw")
    )
    res = table_utc.select(date_string=table_utc.t.dt.strftime(fmt_out))

    assert_table_equality(res, expected)


def test_date_time_utc_to_naive() -> None:
    table = table_from_markdown(
        """
           |         date_string
         1 | 2023-03-25T11:00:00.000000000+0000
         2 | 2023-03-25T22:00:00.000000000+0000
         3 | 2023-03-25T23:00:00.000000000+0000
         4 | 2023-03-26T00:00:00.000000000+0000
         5 | 2023-03-26T00:59:59.999999999+0000
         6 | 2023-03-26T01:00:00.000000000+0000
         7 | 2023-03-26T01:00:00.000001000+0000
         8 | 2023-03-26T01:30:00.000000000+0000
         9 | 2023-03-26T02:00:00.000000000+0000
        10 | 2023-10-28T21:00:00.000000000+0000
        11 | 2023-10-28T23:00:00.000000000+0000
        12 | 2023-10-28T23:59:59.999999999+0000
        13 | 2023-10-29T00:00:00.000000000+0000
        14 | 2023-10-29T00:00:00.000001000+0000
        15 | 2023-10-29T00:00:30.000000000+0000
        16 | 2023-10-29T00:59:59.999999999+0000
        17 | 2023-10-29T01:00:00.000000000+0000
        18 | 2023-10-29T01:00:00.000001000+0000
        19 | 2023-10-29T01:00:30.000000000+0000
        20 | 2023-10-29T01:59:59.999999999+0000
        21 | 2023-10-29T02:00:00.000000000+0000
        22 | 2023-10-29T02:00:00.000001000+0000
        23 | 2023-10-29T02:30:00.000000000+0000
        24 | 2023-10-29T03:00:00.000000000+0000
    """
    )

    expected = table_from_markdown(
        """
           |         date_string
         1 | 2023-03-25T12:00:00.000000
         2 | 2023-03-25T23:00:00.000000
         3 | 2023-03-26T00:00:00.000000
         4 | 2023-03-26T01:00:00.000000
         5 | 2023-03-26T01:59:59.999999
         6 | 2023-03-26T03:00:00.000000
         7 | 2023-03-26T03:00:00.000001
         8 | 2023-03-26T03:30:00.000000
         9 | 2023-03-26T04:00:00.000000
        10 | 2023-10-28T23:00:00.000000
        11 | 2023-10-29T01:00:00.000000
        12 | 2023-10-29T01:59:59.999999
        13 | 2023-10-29T02:00:00.000000
        14 | 2023-10-29T02:00:00.000001
        15 | 2023-10-29T02:00:30.000000
        16 | 2023-10-29T02:59:59.999999
        17 | 2023-10-29T02:00:00.000000
        18 | 2023-10-29T02:00:00.000001
        19 | 2023-10-29T02:00:30.000000
        20 | 2023-10-29T02:59:59.999999
        21 | 2023-10-29T03:00:00.000000
        22 | 2023-10-29T03:00:00.000001
        23 | 2023-10-29T03:30:00.000000
        24 | 2023-10-29T04:00:00.000000
    """
    )
    fmt_in = "%Y-%m-%dT%H:%M:%S.%f%z"
    fmt_out = "%Y-%m-%dT%H:%M:%S.%6f"
    table_utc = table.select(t=table.date_string.dt.strptime(fmt_in))
    table_local = table_utc.select(
        t=table_utc.t.dt.to_naive_in_timezone("Europe/Warsaw")
    )
    res = table_local.select(date_string=table_local.t.dt.strftime(fmt_out))

    assert_table_equality(res, expected)


@pytest.mark.parametrize("op", [operator.add, operator.sub])
def test_add_sub_in_timezone(op: Any) -> None:
    pairs = [
        ["2023-03-26 01:00:00", pd.Timedelta(minutes=30)],
        ["2023-03-26 01:00:00", pd.Timedelta(hours=1)],
        ["2023-03-26 01:00:00", pd.Timedelta(minutes=90)],
        ["2023-03-26 01:00:00", pd.Timedelta(hours=2)],
        ["2023-03-26 01:43:00", pd.Timedelta(minutes=16)],
        ["2023-03-26 01:43:00", pd.Timedelta(minutes=17)],
        ["2023-03-26 01:43:00", pd.Timedelta(hours=1)],
        ["2023-03-26 03:02:00", pd.Timedelta(minutes=-2)],
        ["2023-03-26 03:02:00", pd.Timedelta(minutes=-3)],
        ["2023-10-29 01:59:00", pd.Timedelta(minutes=1)],
        ["2023-10-29 01:59:00", pd.Timedelta(hours=1)],
        ["2023-10-29 01:59:00", pd.Timedelta(hours=2)],
        ["2023-10-29 02:00:00", pd.Timedelta(minutes=1)],
        ["2023-10-29 02:00:00", pd.Timedelta(minutes=-1)],
    ]

    expected = table_from_pandas(
        pd.DataFrame(
            {
                "date_string": [
                    "2023-03-26 01:30:00",
                    "2023-03-26 03:00:00",
                    "2023-03-26 03:30:00",
                    "2023-03-26 04:00:00",
                    "2023-03-26 01:59:00",
                    "2023-03-26 03:00:00",
                    "2023-03-26 03:43:00",
                    "2023-03-26 03:00:00",
                    "2023-03-26 01:59:00",
                    "2023-10-29 02:00:00",
                    "2023-10-29 02:59:00",
                    "2023-10-29 02:59:00",
                    "2023-10-29 02:01:00",
                    "2023-10-29 02:59:00",
                ]
            }
        )
    )

    timezone = "Europe/Warsaw"
    fmt = "%Y-%m-%d %H:%M:%S"
    pairs_T = list(zip(*pairs))
    df = pd.DataFrame({"ts": pairs_T[0], "duration": pairs_T[1]})

    table = table_from_pandas(df)
    table = table.with_columns(ts=table.ts.dt.strptime(fmt))

    if op == operator.add:
        res = table.select(
            res=table.ts.dt.add_duration_in_timezone(table.duration, timezone)
        )
    else:
        res = table.select(
            res=table.ts.dt.subtract_duration_in_timezone(-table.duration, timezone)
        )

    table_pw = res.select(date_string=res.res.dt.strftime(fmt))

    assert_table_equality(table_pw, expected)


def test_date_time_sub_in_timezone() -> None:
    table = table_from_markdown(
        """
           |           a         |           b
         1 | 2023-03-26T01:00:00 | 2023-03-26T00:55:00
         2 | 2023-03-26T03:00:00 | 2023-03-26T01:55:00
         3 | 2023-03-26T01:56:00 | 2023-03-26T03:01:00
         4 | 2023-03-26T04:00:00 | 2023-03-26T01:00:00
         5 | 2023-03-26T04:00:00 | 2023-03-26T03:00:00
         6 | 2023-10-29T01:59:00 | 2023-10-29T02:00:00
         7 | 2023-10-29T02:59:00 | 2023-10-29T02:59:00
         8 | 2023-10-29T02:59:00 | 2023-10-29T02:00:00
         9 | 2023-10-29T02:30:00 | 2023-10-29T01:30:00
    """
    )
    expected = table_from_markdown(
        """
           | diff
         1 |   5
         2 |   5
         3 |  -5
         4 | 120
         5 |  60
         6 | -61
         7 |   0
         8 |  59
         9 | 120
        """
    )
    timezone = "Europe/Warsaw"
    fmt = "%Y-%m-%dT%H:%M:%S"
    parsed = table.select(a=table.a.dt.strptime(fmt), b=table.b.dt.strptime(fmt))
    res = parsed.select(
        diff=parsed.a.dt.subtract_date_time_in_timezone(parsed.b, timezone).dt.minutes()
    )
    assert_table_equality(res, expected)


@pytest.mark.parametrize("is_naive", [True, False])
@pytest.mark.parametrize(
    "round_to",
    [
        pd.Timedelta(days=1),
        pd.Timedelta(hours=2),
        pd.Timedelta(hours=1),
        pd.Timedelta(minutes=20),
        pd.Timedelta(minutes=1),
        pd.Timedelta(seconds=1),
        pd.Timedelta(minutes=43),
        pd.Timedelta(seconds=19),
        "D",
        "2H3T",
        "min",
        "S",
        "14L22ms14us",
        "U",
        "N",
    ],
)
@pytest.mark.parametrize("method_name", ["round", "floor"])
def test_date_time_round(
    method_name: str, round_to: pd.Timedelta | str, is_naive: bool
) -> None:
    data = [
        "2020-03-04 11:13:00.345612",
        "2020-03-04 12:13:00.345612",
        "2020-03-04 12:00:00.0",
        "2020-03-04 11:59:59.999999999",
        "2020-03-04 13:22:23.0",
        "2023-05-19 13:56:23.0",
        "2023-05-19 13:56:23.123456789",
        "2023-05-01 09:10:11.121314",
    ]
    fmt = "%Y-%m-%d %H:%M:%S.%f"
    if not is_naive:
        data = [entry + "+00:00" for entry in data]
        fmt += "%z"
    df = pd.DataFrame({"date": data})
    table = table_from_pandas(df)
    table = table.select(date=table.date.dt.strptime(fmt=fmt))
    res = table.select(rounded=table.date.dt.__getattribute__(method_name)(round_to))

    expected = table_from_pandas(
        pd.DataFrame(
            {
                "rounded": pd.to_datetime(df.date, format=fmt).dt.__getattribute__(
                    method_name
                )(round_to)
            }
        )
    )

    assert_table_equality(res, expected)


@pytest.mark.parametrize(
    "method_with_args",
    [
        ("dt.nanosecond",),
        ("dt.seconds",),
        ("dt.strftime", "%Y-%m-%d %H:%M:%S"),
        ("dt.strptime", "%Y-%m-%d %H:%M:%S"),
    ],
)
def test_fail_if_used_with_wrong_type(method_with_args: tuple[str]) -> None:
    method = method_with_args[0]
    namespace, method_name = method.split(".")
    args = method_with_args[1:]
    table = table_from_pandas(pd.DataFrame({"a": [1, 2, 3]}))
    with pytest.raises(AttributeError):
        table.select(
            a=table.a.__getattribute__(namespace).__getattribute__(method_name)(*args)
        )


def test_from_timestamp_ns() -> None:
    fmt = "%Y-%m-%dT%H:%M:%S.%f"
    table = table_from_markdown(
        """
      | timestamp
    1 |    10
    2 | 1685969950453404012
    """
    )
    expected = table_from_markdown(
        """
      | date
    1 | 1970-01-01T00:00:00.000000010
    2 | 2023-06-05T12:59:10.453404012
    """
    ).with_columns(date=pw.this.date.dt.strptime(fmt))
    table = table.select(date=pw.this.timestamp.dt.from_timestamp(unit="ns"))

    assert_table_equality(table, expected)


def test_from_timestamp_us() -> None:
    fmt = "%Y-%m-%dT%H:%M:%S.%f"
    table = table_from_markdown(
        """
      | timestamp
    1 |    10
    2 | 1685969950453404
    """
    )
    expected = table_from_markdown(
        """
      | date
    1 | 1970-01-01T00:00:00.000010000
    2 | 2023-06-05T12:59:10.453404000
    """
    ).with_columns(date=pw.this.date.dt.strptime(fmt))
    table = table.select(date=pw.this.timestamp.dt.from_timestamp(unit="us"))

    assert_table_equality(table, expected)


def test_from_timestamp_ms() -> None:
    fmt = "%Y-%m-%dT%H:%M:%S.%f"
    table = table_from_markdown(
        """
      | timestamp
    1 |    10
    2 | 1685969950453
    """
    )
    expected = table_from_markdown(
        """
      | date
    1 | 1970-01-01T00:00:00.010000000
    2 | 2023-06-05T12:59:10.453000000
    """
    ).with_columns(date=pw.this.date.dt.strptime(fmt))
    table = table.select(date=pw.this.timestamp.dt.from_timestamp(unit="ms"))

    assert_table_equality(table, expected)


def test_from_timestamp_s() -> None:
    fmt = "%Y-%m-%dT%H:%M:%S"
    table = table_from_markdown(
        """
      | timestamp
    1 |    10
    2 | 1685969950
    """
    )
    expected = table_from_markdown(
        """
      | date
    1 | 1970-01-01T00:00:10
    2 | 2023-06-05T12:59:10
    """
    ).with_columns(date=pw.this.date.dt.strptime(fmt))
    table = table.select(date=pw.this.timestamp.dt.from_timestamp(unit="s"))

    assert_table_equality(table, expected)


def test_from_timestamp_s_utc() -> None:
    fmt = "%Y-%m-%dT%H:%M:%S%z"
    table = table_from_markdown(
        """
      | timestamp
    1 |    10
    2 | 1685969950
    """
    )
    expected = table_from_markdown(
        """
      | date
    1 | 1970-01-01T00:00:10+00:00
    2 | 2023-06-05T12:59:10+00:00
    """
    ).with_columns(date=pw.this.date.dt.strptime(fmt))
    table = table.select(date=pw.this.timestamp.dt.utc_from_timestamp(unit="s"))

    assert_table_equality(table, expected)


@pytest.mark.parametrize("is_naive", [True, False])
def test_weekday(is_naive: bool) -> None:
    data = [
        "1960-02-03 08:00:00.000000000",
        "2008-02-29 08:00:00.000000000",
        "2023-03-25 12:00:00.000000000",
        "2023-03-25 12:00:00.000000001",
        "2023-03-25 12:00:00.123456789",
        "2023-03-25 16:43:21.000123000",
        "2023-03-25 17:00:01.987000000",
        "2023-03-25 23:59:59.999999999",
        "2023-03-26 01:59:59.999999999",
        "2023-03-26 03:00:00.000000001",
        "2023-03-26 04:00:00.000000001",
        "2023-03-26 12:00:00.000000001",
        "2123-03-26 12:00:00.000000001",
    ]
    fmt_in = "%Y-%m-%d %H:%M:%S.%f"
    if not is_naive:
        data = [entry + "-02:00" for entry in data]
        fmt_in += "%z"
    df = pd.DataFrame({"ts": pd.to_datetime(data, format=fmt_in)})
    if is_naive:
        df_converted = df
    else:
        df_converted = pd.DataFrame({"ts": df.ts.dt.tz_convert(tz.UTC)})
    df_new = pd.DataFrame({"txt": df_converted.ts.dt.weekday})
    table = table_from_pandas(df)
    table_pw = table.select(txt=table.ts.dt.weekday())
    table_pd = table_from_pandas(df_new)
    assert_table_equality(table_pw, table_pd)


def test_pathway_duration():
    values = [
        (1, ["W"]),
        (1, ["D", "day", "days"]),
        (24, ["h", "hr", "hour", "hours"]),
        (24 * 60, ["m", "min", "minute", "minutes"]),
        (24 * 60 * 60, ["s", "sec", "second", "seconds"]),
        (24 * 60 * 60 * 1000, ["ms", "millisecond", "milliseconds", "millis", "milli"]),
        (
            24 * 60 * 60 * 1000 * 1000,
            ["us", "microsecond", "microsecond", "micros", "micro"],
        ),
        (
            24 * 60 * 60 * 1000 * 1000 * 1000,
            ["ns", "nanosecond", "nanoseconds", "nanos", "nano"],
        ),
    ]

    markdown = "value | unit\n"
    for value, units in values:
        for unit in units:
            markdown += f"{value} | {unit}\n"
    t = table_from_markdown(markdown)

    result = t.select(value=pw.this.value.dt.to_duration(pw.this.unit))

    assert_table_equality(
        result,
        table_from_pandas(
            pd.DataFrame(
                {
                    "value": [
                        pd.Timedelta(f"{v} {u}") for v, units in values for u in units
                    ]
                }
            )
        ),
    )


def test_pathway_duration_from_udf():
    t = table_from_markdown(
        """
        value
        1
    """
    )

    @pw.udf
    def to_duration(a) -> pw.Duration:
        return pw.Duration(days=a)

    result = t.select(value=to_duration(pw.this.value))
    assert_table_equality(
        result, table_from_pandas(pd.DataFrame({"value": [pd.Timedelta(days=1)]}))
    )


def test_pathway_datetimes():
    @pw.udf
    def to_naive(year, month, day) -> pw.DateTimeNaive:
        return pw.DateTimeNaive(year=year, month=month, day=day)

    @pw.udf
    def to_utc(year, month, day) -> pw.DateTimeUtc:
        return pw.DateTimeUtc(year=year, month=month, day=day, tz=tz.UTC)

    t = table_from_markdown(
        """
        year | month | day
        2023 |   8   |  12
    """
    )

    result = t.select(value=to_naive(pw.this.year, pw.this.month, pw.this.day))
    assert_table_equality(
        result,
        table_from_pandas(
            pd.DataFrame({"value": [pd.Timestamp(year=2023, month=8, day=12)]})
        ),
    )

    result = t.select(value=to_utc(pw.this.year, pw.this.month, pw.this.day))
    assert_table_equality(
        result,
        table_from_pandas(
            pd.DataFrame(
                {"value": [pd.Timestamp(year=2023, month=8, day=12, tz=tz.UTC)]}
            )
        ),
    )


def test_python_types():
    pw.DateTimeNaive("2025-05-01T12:00:00")
    with pytest.raises(
        ValueError,
        match=re.escape(
            "DateTimeNaive cannot contain timezone information. Use pw.DateTimeUtc for datetimes with a timezone."
        ),
    ):
        pw.DateTimeNaive("2025-05-01T12:00:00+00:00")
    with pytest.raises(
        ValueError,
        match=re.escape(
            "DateTimeUtc must contain timezone information. Use pw.DateTimeNaive for naive datetimes."
        ),
    ):
        pw.DateTimeUtc("2025-05-01T12:00:00")
    pw.DateTimeUtc("2025-05-01T12:00:00+00:00")
