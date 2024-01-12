# Copyright © 2024 Pathway

from dataclasses import dataclass

import pathway.internals as pw

from .utils import IntervalType


class Behavior:
    """
    A superclass of all classes defining temporal behavior: its subclasses allow
    to configure several temporal operators to delay outputs, ignore late entries,
    and clean the memory.
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
    """Creates an instance of ``CommonBehavior``, which contains a basic configuration of
    a behavior of temporal operators (like ``windowby`` or ``asof_join``).
    Each temporal operator tracks its own time (defined as a maximum time that arrived to
    the operator) and this configuration tells it that some of its inputs or outputs may
    be delayed or ignored.
    The decisions are based on the current time of the operator and the time associated
    with an input/output entry. Additionally, it allows the operator to free up memory by
    removing parts of internal state that cannot interact with any future input entries.

    Remark: for the sake of temporal behavior, the current time of each operator is
    updated only after it processes all the data that arrived on input. In other words,
    if several new input entries arrived to the system simultaneously, each of those
    entries will be processed using last recorded time, and the recorded time is upda

    Args:
        delay:
            Optional.

            For windows, delays initial output by ``delay`` with respect to the
            beginning of the window. Setting it to ``None`` does not enable
            delaying mechanism.

            For interval joins and asof joins, it delays the time the record is joined by ``delay``.

            Using `delay` is useful when updates are too frequent.
        cutoff:
            Optional.

            For windows, stops updating windows which end earlier than maximal
            seen time minus ``cutoff``. Setting cutoff to ``None`` does not enable
            cutoff mechanism.

            For interval joins and asof joins, it ignores entries that are older
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


def apply_temporal_behavior(
    table: pw.Table, behavior: CommonBehavior | None
) -> pw.Table:
    if behavior is not None:
        if behavior.delay is not None:
            table = table._buffer(pw.this._pw_time + behavior.delay, pw.this._pw_time)
        if behavior.cutoff is not None:
            cutoff_threshold = pw.this._pw_time + behavior.cutoff
            table = table._freeze(cutoff_threshold, pw.this._pw_time)
            table = table._forget(
                cutoff_threshold, pw.this._pw_time, behavior.keep_results
            )
    return table
