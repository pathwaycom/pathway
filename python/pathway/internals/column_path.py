# Copyright © 2024 Pathway

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar


@dataclass(eq=True, frozen=True)
class ColumnPath:
    path: tuple[int, ...]
    is_key: bool = False

    EMPTY: ClassVar[ColumnPath]
    KEY: ClassVar[ColumnPath]

    def __add__(self, other: tuple[int, ...]) -> ColumnPath:
        return ColumnPath(self.path + other)

    def __radd__(self, other: tuple[int, ...]) -> ColumnPath:
        return ColumnPath(other + self.path)

    def __len__(self) -> int:
        return len(self.path)


ColumnPath.EMPTY = ColumnPath(())
ColumnPath.KEY = ColumnPath(path=(), is_key=True)
