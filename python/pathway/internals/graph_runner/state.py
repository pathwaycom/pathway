# Copyright © 2024 Pathway

from __future__ import annotations

from collections.abc import Callable, Iterable

from pathway.internals import api, column, table, universe
from pathway.internals.graph_runner.path_storage import Storage


class OutOfScopeError(RuntimeError):
    pass


class ScopeState:
    columns: dict[column.Column, api.Column]
    legacy_tables: dict[table.Table, api.LegacyTable]
    universes: dict[universe.Universe, api.Universe]
    computers: list[Callable]
    tables: dict[universe.Universe, api.Table]
    storages: dict[universe.Universe, Storage]
    error_logs: dict[table.Table, api.ErrorLog]

    def __init__(self, scope: api.Scope) -> None:
        self.scope = scope
        self.columns = {}
        self.universes = {}
        self.computers = []
        self.legacy_tables = {}
        self.tables = {}
        self.storages = {}
        self.error_logs = {}

    def extract_universe(self, univ: universe.Universe) -> api.Universe:
        engine_table = self.get_table(univ)
        engine_universe = self.scope.table_universe(engine_table)
        self.set_universe(univ, engine_universe)
        return engine_universe

    def extract_column(self, column: column.Column) -> api.Column:
        univ = column.universe
        storage = self.get_storage(univ)
        if not storage.has_column(column):
            raise OutOfScopeError("column out of scope")
        engine_table = self.get_table(univ)
        if not self.has_universe(univ):
            engine_universe = self.extract_universe(univ)
        else:
            engine_universe = self.get_universe(univ)
        column_path = storage.get_path(column)
        engine_column = self.scope.table_column(
            engine_universe, engine_table, column_path
        )
        self.set_column(column, engine_column)
        return engine_column

    def create_table(self, universe: universe.Universe, storage: Storage) -> None:
        columns: list[api.Column] = []
        for col in storage.get_columns():
            if not isinstance(col, column.ExternalMaterializedColumn):
                columns.append(self.get_column(col))
        engine_table = self.scope.columns_to_table(self.get_universe(universe), columns)
        self.set_table(storage, engine_table)

    def set_column(self, key: column.Column, value: api.Column):
        self.columns[key] = value
        self.set_universe(key.universe, value.universe)

    def get_column(self, key: column.Column) -> api.Column:
        if key not in self.columns:
            return self.extract_column(key)
        column = self.columns[key]
        return column

    def has_column(self, key: column.Column) -> bool:
        try:
            self.get_column(key)
            return True
        except OutOfScopeError:
            return False

    def get_columns(self, columns: Iterable[column.Column]) -> list[api.Column]:
        return [self.get_column(column) for column in columns]

    def set_legacy_tables(
        self,
        keys: Iterable[table.Table],
        values: Iterable[api.LegacyTable],
    ):
        for output, result in zip(keys, values):
            self.set_legacy_table(output, result)

    def set_legacy_table(self, key: table.Table, value: api.LegacyTable):
        self.legacy_tables[key] = value
        for (_, col), evaluated_column in zip(key._columns.items(), value.columns):
            self.set_column(col, evaluated_column)

    def get_legacy_table(self, key: table.Table) -> api.LegacyTable:
        if key in self.legacy_tables:
            return self.legacy_tables[key]
        else:
            universe = self.get_universe(key._universe)
            columns = self.get_columns(key._columns.values())
            table = api.LegacyTable(universe, columns)
            self.legacy_tables[key] = table
            return table

    def has_legacy_table(self, key: table.Table) -> bool:
        try:
            self.get_legacy_table(key)
            return True
        except OutOfScopeError:
            return False

    def get_legacy_tables(self, tables: Iterable[table.Table]) -> list[api.LegacyTable]:
        return [self.get_legacy_table(table) for table in tables]

    def set_universe(self, key: universe.Universe, value: api.Universe):
        if key in self.universes:
            assert self.universes[key] == value, "overriding already computed universe"
            return
        self.universes[key] = value

    def get_universe(self, key: universe.Universe):
        if key not in self.universes:
            return self.extract_universe(key)
        return self.universes[key]

    def has_universe(self, key: universe.Universe) -> bool:
        return key in self.universes

    def add_computer_logic(self, computer_callback: Callable) -> int:
        id = len(self.computers)
        self.computers.append(computer_callback)
        return id

    def get_computer_logic(self, id: int) -> Callable:
        return self.computers[id]

    def get_table(self, key: universe.Universe) -> api.Table:
        if key not in self.tables:
            raise OutOfScopeError("table out of scope")
        return self.tables[key]

    def get_tables(self, keys: Iterable[universe.Universe]) -> list[api.Table]:
        return [self.get_table(key) for key in keys]

    def set_table(self, storage: Storage, table: api.Table) -> None:
        self.tables[storage._universe] = table
        self.storages[storage._universe] = storage

    def get_storage(self, key: universe.Universe) -> Storage:
        if key not in self.storages:
            raise OutOfScopeError("path storage out of scope")
        return self.storages[key]

    def get_storages(self, keys: Iterable[universe.Universe]) -> list[Storage]:
        return [self.get_storage(key) for key in keys]

    def set_error_log(self, table: table.Table, error_log: api.ErrorLog) -> None:
        self.error_logs[table] = error_log

    def get_error_log(self, table: table.Table) -> api.ErrorLog | None:
        # None is returned if the error_log is not used.
        # It was removed by tree shaking and there's no need to put entries in it.
        return self.error_logs.get(table)
