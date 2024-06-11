# Copyright © 2024 Pathway

from __future__ import annotations

import dataclasses
from collections.abc import Iterable
from dataclasses import dataclass
from functools import cached_property

from pathway.internals.column import Column, IdColumn
from pathway.internals.column_path import ColumnPath
from pathway.internals.table import Table
from pathway.internals.universe import Universe


@dataclass(eq=False, frozen=True)
class Storage:
    _universe: Universe
    _column_paths: dict[Column, ColumnPath]
    flattened_inputs: list[Storage] | None = None
    flattened_output: Storage | None = None
    has_only_references: bool = False
    has_only_new_columns: bool = False

    def get_columns(self) -> Iterable[Column]:
        return self._column_paths.keys()

    def has_column(self, column: Column) -> bool:
        return column in self._column_paths or (
            isinstance(column, IdColumn) and column.universe == self._universe
        )

    def get_path(self, column: Column) -> ColumnPath:
        if column in self._column_paths:
            return self._column_paths[column]
        elif isinstance(column, IdColumn) and column.universe == self._universe:
            return ColumnPath.KEY
        else:
            raise KeyError(f"Column {column} not found in Storage {self}.")

    @cached_property
    def max_depth(self) -> int:
        return max((len(path) for path in self._column_paths.values()), default=0)

    def validate(self) -> None:
        assert len(set(self._column_paths.values())) == len(
            self._column_paths
        ), "Some columns have the same path."

    def with_updated_paths(self, paths: dict[Column, ColumnPath]) -> Storage:
        return dataclasses.replace(self, _column_paths=self._column_paths | paths)

    def with_flattened_output(self, storage: Storage) -> Storage:
        return dataclasses.replace(self, flattened_output=storage)

    def with_flattened_inputs(self, storages: list[Storage] | None = None) -> Storage:
        return dataclasses.replace(self, flattened_inputs=storages)

    def restrict_to_table(self, table: Table) -> Storage:
        table_columns = set(table._columns.values())
        new_column_paths = {}
        for column, path in self._column_paths.items():
            if column in table_columns:
                new_column_paths[column] = path
        return dataclasses.replace(self, _column_paths=new_column_paths)

    def remove_columns_from_table(self, table: Table) -> Storage:
        table_columns = set(table._columns.values())
        new_column_paths = {}
        for column, path in self._column_paths.items():
            if column not in table_columns:
                new_column_paths[column] = path
        return dataclasses.replace(self, _column_paths=new_column_paths)

    @classmethod
    def merge_storages(cls, universe: Universe, *storages: Storage) -> Storage:
        column_paths = {}
        for i, storage in enumerate(storages):
            for column, path in storage._column_paths.items():
                assert path is not None
                column_paths[column] = (i,) + path
        return cls(universe, column_paths)

    @classmethod
    def flat(
        cls, universe: Universe, columns: Iterable[Column], shift: int = 0
    ) -> Storage:
        paths = {}
        for i, column in enumerate(columns):
            paths[column] = ColumnPath((i + shift,))
        return cls(universe, paths)
