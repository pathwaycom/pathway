# Copyright © 2024 Pathway

import pandas as pd


class DateTimeNaive(pd.Timestamp):
    """Type for storing datetime without timezone information. Extends `pandas.Timestamp` type."""

    def __new__(cls, *args, **kwargs):
        obj = super().__new__(cls, *args, **kwargs)
        if obj.tz is not None:
            raise ValueError(
                "DateTimeNaive cannot contain timezone information. Use pw.DateTimeUtc for datetimes with a timezone."
            )
        return obj


class DateTimeUtc(pd.Timestamp):
    """Type for storing datetime with default timezone. Extends `pandas.Timestamp` type."""

    def __new__(cls, *args, **kwargs):
        obj = super().__new__(cls, *args, **kwargs)
        if obj.tz is None:
            raise ValueError(
                "DateTimeUtc must contain timezone information. Use pw.DateTimeNaive for naive datetimes."
            )
        return obj


class Duration(pd.Timedelta):
    """Type for storing duration of time. Extends `pandas.Timedelta` type."""

    pass
