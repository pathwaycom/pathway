# Copyright © 2024 Pathway

from __future__ import annotations

from abc import abstractmethod
from typing import AbstractSet

import pathway


class OperatorInput:
    """Abstract for all valid operator inputs, like tables, expressions, grouped tables."""

    @abstractmethod
    def _operator_dependencies(self) -> AbstractSet[pathway.Table]:
        pass
