# Copyright © 2023 Pathway

import datetime
from typing import Any, Dict, Tuple, Union, get_args

from pathway.internals.datetime_types import DateTimeNaive, DateTimeUtc, Duration
from pathway.internals.type_interpreter import eval_type

TimeEventType = Union[int, float, datetime.datetime]
IntervalType = Union[int, float, datetime.timedelta]


def get_default_shift(interval: IntervalType) -> TimeEventType:
    if isinstance(interval, datetime.timedelta):
        return datetime.datetime(1970, 1, 1)
    elif isinstance(interval, int):
        return 0
    else:
        return 0.0


def _get_possible_types(type: Any) -> Any:
    if type is TimeEventType:
        return [int, float, DateTimeNaive, DateTimeUtc]
    if type is IntervalType:
        return [int, float, Duration, Duration]
    return get_args(type)


def check_joint_types(parameters: Dict[str, Tuple[Any, Any]]) -> None:
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
    if not any([types == ex_types for ex_types in expected_types]):
        expected_types_string = " or ".join(
            repr(tuple(ex_types.values())) for ex_types in expected_types
        )
        raise TypeError(
            f"Arguments ({', '.join(parameters.keys())}) have to be of types "
            + f"{expected_types_string} but are of types {tuple(types.values())}."
        )
