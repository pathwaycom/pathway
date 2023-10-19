# Copyright © 2023 Pathway

from dataclasses import dataclass

from .utils import IntervalType

# TODO - clarify corner cases (which times are exclusive / inclusive)


@dataclass
class WindowBehavior:
    """Defines temporal behavior of windows and temporal joins."""

    delay: IntervalType | None
    cutoff: IntervalType | None
    keep_results: bool


def window_behavior(
    delay: IntervalType | None = None,
    cutoff: IntervalType | None = None,
    keep_results: bool = True,
) -> WindowBehavior:
    """Creates WindowBehavior

    Args:
        delay: For windows, delays initial output by ``delay`` with respect to the
            beginning of the window. For interval joins, it delays the time the record
            is joined by ``delay``. Using `delay` is useful when updates are too frequent.
        cutoff: For windows, stops updating windows which end earlier than maximal seen
            time minus ``cutoff``. For interval joins, it ignores entries that are older
            than maximal seen time minus ``cutoff``. This parameter is also used to clear
            memory. It allows to release memory used by entries that won't change.
        keep_results: If set to True, keeps all results of the operator. If set to False,
            keeps only results that are newer than maximal seen time minus ``cutoff``.
    """
    return WindowBehavior(delay, cutoff, keep_results)
