# Copyright © 2024 Pathway

from __future__ import annotations

import contextlib
import functools
import sys
import traceback
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, ParamSpec, TypeVar

if TYPE_CHECKING:
    from pathway.internals import api


@dataclass(frozen=True)
class Frame:
    filename: str
    line_number: int | None
    line: str | None
    function: str

    def is_external(self) -> bool:
        if "pathway/tests/test_" in self.filename:
            return True
        exclude_patterns = [
            "pathway/tests",
            "pathway/internals",
            "pathway/io",
            "pathway/stdlib",
            "pathway/debug",
            "@beartype",
        ]
        return all(pattern not in self.filename for pattern in exclude_patterns)

    def is_marker(self) -> bool:
        return self.function == "_pathway_trace_marker"


@dataclass(frozen=True)
class Trace:
    frames: list[Frame]
    user_frame: Frame | None

    @staticmethod
    def from_traceback():
        frames = [
            Frame(
                filename=e.filename,
                line_number=e.lineno,
                line=e.line,
                function=e.name,
            )
            for e in traceback.extract_stack()[:-1]
        ]

        user_frame: Frame | None = None
        for frame in frames:
            if frame.is_marker():
                break
            elif frame.is_external():
                user_frame = frame

        return Trace(frames=frames, user_frame=user_frame)

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
