# Copyright © 2024 Pathway


from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathway.internals.joins import Joinable


class ColumnNamespace:
    __wrapped: Joinable

    def __init__(self, wrapped: Joinable):
        self.__wrapped = wrapped

    def __getattr__(self, name):
        return self.__wrapped._get_colref_by_name(name, AttributeError)

    def __getitem__(self, name):
        return self.__wrapped._get_colref_by_name(name, KeyError)
