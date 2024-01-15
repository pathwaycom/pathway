# Copyright © 2024 Pathway

import datetime
from typing import Any, Union

import pandas as pd
from dateutil import tz

from pathway.internals import dtype as dt
from pathway.internals.type_interpreter import eval_type

TimeEventType = Union[int, float, datetime.datetime]
IntervalType = Union[int, float, datetime.timedelta]


def get_default_origin(time_event_type: dt.DType) -> TimeEventType:
    mapping: dict[Any, TimeEventType] = {
        dt.INT: 0,
        dt.FLOAT: 0.0,
        dt.DATE_TIME_NAIVE: pd.Timestamp(year=1973, month=1, day=1, tz=None),
        dt.DATE_TIME_UTC: pd.Timestamp(year=1973, month=1, day=1, tz=tz.UTC),
        # 1973 because it started on Monady and then by default all week-wide windows start on Monday
    }
    return mapping[time_event_type]


def zero_length_interval(interval_type: type[IntervalType]) -> IntervalType:
    if issubclass(interval_type, datetime.timedelta):
        return datetime.timedelta(0)
    elif issubclass(interval_type, int):
        return 0
    elif issubclass(interval_type, float):
        return 0.0
    else:
        raise Exception("unsupported interval type")


def _get_possible_types(type: Any) -> tuple[dt.DType, ...]:
    if type is TimeEventType:
        return (dt.INT, dt.FLOAT, dt.DATE_TIME_NAIVE, dt.DATE_TIME_UTC)
    if type is IntervalType:
        return (dt.INT, dt.FLOAT, dt.DURATION, dt.DURATION)
    raise ValueError("Type has to be either TimeEventType or IntervalType.")


def check_joint_types(parameters: dict[str, tuple[Any, Any]]) -> None:
    """Checks if all parameters have types that allow to execute a function.
    If parameters are {'a': (a, TimeEventType), 'b': (b, IntervalType)} then
    the following pairs of types are allowed for (a, b): (int, int), (float, float),
    (datetime.datetime, datetime.timedelta)
    """

    parameters = {
        name: (variable, expected_type)
        for name, (variable, expected_type) in parameters.items()
        if variable is not None
    }
    types = {name: eval_type(variable) for name, (variable, _) in parameters.items()}
    expected_types = []
    for i in range(len(_get_possible_types(TimeEventType))):
        expected_types.append(
            {
                name: _get_possible_types(expected_type)[i]
                for name, (_variable, expected_type) in parameters.items()
            }
        )
    for ex_types in expected_types:
        if all(
            [
                dt.dtype_issubclass(dtype, ex_dtype)
                for (dtype, ex_dtype) in zip(types.values(), ex_types.values())
            ]
        ):
            break
    else:
        expected_types_string = " or ".join(
            repr(tuple(ex_types.values())) for ex_types in expected_types
        )
        raise TypeError(
            f"Arguments ({', '.join(parameters.keys())}) have to be of types "
            + f"{expected_types_string} but are of types {tuple(types.values())}."
        )
