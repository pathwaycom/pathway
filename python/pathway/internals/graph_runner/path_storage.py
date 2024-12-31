# Copyright © 2024 Pathway

from __future__ import annotations

import dataclasses
from collections.abc import Iterable
from dataclasses import dataclass
from functools import cached_property

from pathway.internals.column import Column, IdColumn
from pathway.internals.column_path import ColumnPath
from pathway.internals.universe import Universe


@dataclass(eq=False, frozen=True)
class Storage:
    _universe: Universe
    _column_paths: dict[Column, ColumnPath]
    """Paths to columns that are needed downstream. Not all columns in a tuple are present there."""

    _all_columns: dict[ColumnPath, Column]
    """Columns at a given path, all columns present in a tuple are there. Used to
    evaluate the total memory usage of a tuple or stream properties (append-only).
    """

    maybe_flattened_inputs: dict[str, Storage] = dataclasses.field(default_factory=dict)
    flattened_output: Storage | None = None
    has_only_references: bool = False
    has_only_new_columns: bool = False
    is_flat: bool = False

    def __post_init__(self) -> None:
        for path in self._column_paths.values():
            assert path in self._all_columns

    @classmethod
    def new(
        cls, universe: Universe, column_paths: dict[Column, ColumnPath], **kwargs
    ) -> Storage:
        return cls(
            universe,
            column_paths,
            {path: column for column, path in column_paths.items()},
            **kwargs,
        )

    @classmethod
    def one_column_storage(cls, column: Column) -> Storage:
        return cls.new(column.universe, {column: ColumnPath.EMPTY})

    def get_columns(self) -> Iterable[Column]:
        return self._column_paths.keys()

    def get_all_columns(self) -> Iterable[tuple[ColumnPath, Column]]:
        return self._all_columns.items()

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

    @cached_property
    def append_only(self) -> bool:
        return all(
            column.properties.append_only for column in self._all_columns.values()
        )

    def with_updated_paths(
        self, paths: dict[Column, ColumnPath], *, universe: Universe | None = None
    ) -> Storage:
        if universe is None:
            universe = self._universe
        new_column_paths = self._column_paths | paths
        new_all_columns = self._all_columns | {
            path: column for column, path in paths.items()
        }
        return Storage(universe, new_column_paths, new_all_columns)

    def with_flattened_output(self, storage: Storage) -> Storage:
        return dataclasses.replace(self, flattened_output=storage)

    def with_maybe_flattened_inputs(self, storages: dict[str, Storage]) -> Storage:
        return dataclasses.replace(self, maybe_flattened_inputs=storages)

    def with_only_references(self) -> Storage:
        return dataclasses.replace(self, has_only_references=True)

    def with_only_new_columns(self) -> Storage:
        return dataclasses.replace(self, has_only_new_columns=True)

    def restrict_to(
        self, columns: Iterable[Column], require_all: bool = False
    ) -> Storage:
        table_columns = set(columns)
        new_column_paths = {}
        for column, path in self._column_paths.items():
            if column in table_columns:
                new_column_paths[column] = path
        if require_all:
            assert len(table_columns) == len(new_column_paths)
            assert all(column.universe == self._universe for column in new_column_paths)
        # restrict_to doesn't change the _all_columns because these columns are still present in a tuple.
        # It only affects _column_paths which is used to set which columns are visible
        # externally (from methods like get_path).
        # restrict_to performs no operation on the underlying data
        return dataclasses.replace(self, _column_paths=new_column_paths)

    def remove(self, columns: Iterable[Column]) -> Storage:
        table_columns = set(columns)
        new_column_paths = {}
        for column, path in self._column_paths.items():
            if column not in table_columns:
                new_column_paths[column] = path
        return dataclasses.replace(self, _column_paths=new_column_paths)

    @classmethod
    def merge_storages(cls, universe: Universe, *storages: Storage) -> Storage:
        column_paths = {}
        all_columns = {}
        for i, storage in enumerate(storages):
            for column, path in storage._column_paths.items():
                assert path is not None
                column_paths[column] = (i,) + path
            for path, column in storage._all_columns.items():
                all_columns[(i,) + path] = column
        return cls(universe, column_paths, all_columns)

    @classmethod
    def flat(
        cls, universe: Universe, columns: Iterable[Column], shift: int = 0
    ) -> Storage:
        paths = {}
        for i, column in enumerate(columns):
            paths[column] = ColumnPath((i + shift,))
        return cls.new(universe, paths, is_flat=True)

    def with_prefix(self, prefix: tuple[int, ...]) -> Storage:
        new_paths = {}
        for column, path in self._column_paths.items():
            new_paths[column] = prefix + path
        all_columns = {}
        for path, column in self._all_columns.items():
            all_columns[prefix + path] = column
        return Storage(
            self._universe, _column_paths=new_paths, _all_columns=all_columns
        )
