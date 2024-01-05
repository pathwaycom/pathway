# Copyright © 2024 Pathway

from __future__ import annotations

import datetime


def truncate_to_minutes(time: datetime.datetime) -> datetime.datetime:
    return time - datetime.timedelta(seconds=time.second, microseconds=time.microsecond)
