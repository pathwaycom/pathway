# Copyright © 2024 Pathway

import pandas as pd


class DateTimeNaive(pd.Timestamp):
    """Type for storing datetime without timezone information. Extends `pandas.Timestamp` type."""

    pass


class DateTimeUtc(pd.Timestamp):
    """Type for storing datetime with default timezone. Extends `pandas.Timestamp` type."""

    pass


class Duration(pd.Timedelta):
    """Type for storing duration of time. Extends `pandas.Timedelta` type."""

    pass
