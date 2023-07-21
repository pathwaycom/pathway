# Copyright © 2023 Pathway


from __future__ import annotations

from enum import Enum


class JoinMode(Enum):
    INNER = 0
    LEFT = 1
    RIGHT = 2
    OUTER = 3
