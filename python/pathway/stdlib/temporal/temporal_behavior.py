# Copyright © 2023 Pathway

import sys
from dataclasses import dataclass

# TODO - clarify corner cases (which times are exclusive / inclusive)
# defines temporal behavior of a window
# the window delays initial output by `delay`` with respect to the begin of the window
# the window stops updating the output `delay` time after the end of the window
# `keep_results` indicates whether the entry corresponding to the window is cleared
# after time reaches `cutoff`


@dataclass
class WindowBehavior:
    delay: int | None
    cutoff: int | None
    keep_results: bool


def window_behavior(
    delay: int, cutoff: int, keep_results: bool = True
) -> WindowBehavior:
    print("making behavior", file=sys.stderr)

    return WindowBehavior(delay, cutoff, keep_results)
