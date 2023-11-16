# Copyright © 2023 Pathway

from dataclasses import dataclass

from .utils import IntervalType


class Behavior:
    """
    A superclass of all classes defining temporal behavior.
    """

    pass


@dataclass
class CommonBehavior(Behavior):
    """Defines temporal behavior of windows and temporal joins."""

    delay: IntervalType | None
    cutoff: IntervalType | None
    keep_results: bool


def common_behavior(
    delay: IntervalType | None = None,
    cutoff: IntervalType | None = None,
    keep_results: bool = True,
) -> CommonBehavior:
    """Creates CommonBehavior

    Args:
        delay:
            Optional; for windows, delays initial output by ``delay`` with respect to the
            beginning of the window. Setting it to ``None`` does not enable
            delaying mechanism.

            For interval joins, it delays the time the record is joined by ``delay``.

            Using `delay` is useful when updates are too frequent.
        cutoff:
            Optional; for windows, stops updating windows which end earlier than maximal
            seen time minus ``cutoff``. Setting cutoff to ``None`` does not enable
            cutoff mechanism.

            For interval joins, it ignores entries that are older
            than maximal seen time minus ``cutoff``. This parameter is also used to clear
            memory. It allows to release memory used by entries that won't change.

        keep_results: If set to True, keeps all results of the operator. If set to False,
            keeps only results that are newer than maximal seen time minus ``cutoff``.
            Can't be set to ``False``, when ``cutoff`` is ``None``.
    """
    assert not (cutoff is None and not keep_results)
    return CommonBehavior(delay, cutoff, keep_results)


@dataclass
class ExactlyOnceBehavior(Behavior):
    shift: IntervalType | None


def exactly_once_behavior(shift: IntervalType | None = None):
    """Creates an instance of class ExactlyOnceBehavior, indicating that each non empty
    window should produce exactly one output.

    Args:
        shift: optional, defines the moment in time (``window end + shift``) in which
        the window stops accepting the data and sends the results to the output.
        Setting it to ``None`` is interpreted as ``shift=0``.

    Remark:
        note that setting a non-zero shift and demanding exactly one output results in
        the output being delivered only when the time in the time column reaches
        ``window end + shift``.

    """
    return ExactlyOnceBehavior(shift)
