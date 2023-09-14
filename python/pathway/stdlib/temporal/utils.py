# Copyright © 2023 Pathway

import datetime
from typing import Any, Dict, Tuple, Union

from pathway.internals import dtype as dt
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


def _get_possible_types(type: Any) -> Tuple[dt.DType, ...]:
    if type is TimeEventType:
        return (dt.INT, dt.FLOAT, dt.DATE_TIME_NAIVE, dt.DATE_TIME_UTC)
    if type is IntervalType:
        return (dt.INT, dt.FLOAT, dt.DURATION, dt.DURATION)
    raise ValueError("Type has to be either TimeEventType or IntervalType.")


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
