# Copyright © 2026 Pathway

from __future__ import annotations

import contextlib
import functools
import linecache
import sys
from collections.abc import Callable
from dataclasses import dataclass
from types import FrameType
from typing import TYPE_CHECKING, ParamSpec, TypeVar

if TYPE_CHECKING:
    from pathway.internals import api

_MARKER_FUNCTION = "_pathway_trace_marker"


def _is_external_file(filename: str) -> bool:
    # Flat chain of substring checks: this predicate runs for every frame
    # examined by ``Trace.from_traceback``, so it must not allocate.
    if "pathway/tests/test_" in filename:
        return True
    return (
        "pathway/tests" not in filename
        and "pathway/internals" not in filename
        and "pathway/io" not in filename
        and "pathway/stdlib" not in filename
        and "pathway/debug" not in filename
        and "@beartype" not in filename
    )


@dataclass(frozen=True)
class Frame:
    filename: str
    line_number: int | None
    line: str | None
    function: str

    def is_external(self) -> bool:
        return _is_external_file(self.filename)


@dataclass(frozen=True)
class Trace:
    user_frame: Frame | None

    @staticmethod
    def from_traceback():
        # The user frame is the newest external frame older than the oldest
        # trace marker on the stack (or the newest external frame overall when
        # no marker is present).  Walking raw frames newest-to-oldest keeps
        # this hot path cheap: no stack materialization, and the source line
        # is read only once, for the single frame chosen.
        frame: FrameType | None = sys._getframe(1)
        candidate: tuple[str, int, str] | None = None
        want_external = True
        while frame is not None:
            code = frame.f_code
            if code.co_name == _MARKER_FUNCTION:
                candidate = None
                want_external = True
            elif want_external and _is_external_file(code.co_filename):
                candidate = (code.co_filename, frame.f_lineno, code.co_name)
                want_external = False
            frame = frame.f_back

        user_frame: Frame | None = None
        if candidate is not None:
            filename, line_number, function = candidate
            user_frame = Frame(
                filename=filename,
                line_number=line_number,
                line=linecache.getline(filename, line_number).strip(),
                function=function,
            )

        return Trace(user_frame=user_frame)

    def to_engine(self) -> api.Trace | None:
        user_frame = self.user_frame
        if (
            user_frame is None
            or user_frame.line_number is None
            or user_frame.line is None
        ):
            return None
        else:
            from pathway.internals import api

            return api.Trace(
                file_name=user_frame.filename,
                line_number=user_frame.line_number,
                line=user_frame.line,
                function=user_frame.function,
            )


def _format_frame(frame: Frame) -> str:
    return f"""Occurred here:
    Line: {frame.line}
    File: {frame.filename}:{frame.line_number}"""


def _reraise_with_user_frame(e: Exception, trace: Trace | None = None) -> None:
    traceback = e.__traceback__
    if traceback is not None:
        traceback = traceback.tb_next

    e = e.with_traceback(traceback)

    if hasattr(e, "_pathway_trace_note"):
        raise e

    if trace is None:
        trace = Trace.from_traceback()

    user_frame = trace.user_frame

    if user_frame is not None:
        add_pathway_trace_note(e, user_frame)

    raise e


def add_pathway_trace_note(e: Exception, frame: Frame) -> None:
    note = _format_frame(frame)
    e._pathway_trace_note = note  # type:ignore[attr-defined]
    if sys.version_info < (3, 11):
        import exceptiongroup  # noqa:F401 enable backport

        e.__notes__ = getattr(e, "__notes__", []) + [note]  # type:ignore[attr-defined]
    else:
        e.add_note(note)


P = ParamSpec("P")
T = TypeVar("T")


def trace_user_frame(func: Callable[P, T]) -> Callable[P, T]:
    @functools.wraps(func)
    def _pathway_trace_marker(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            _reraise_with_user_frame(e)

    return _pathway_trace_marker


@contextlib.contextmanager
def custom_trace(trace: Trace):
    try:
        yield
    except Exception as e:
        _reraise_with_user_frame(e, trace)
