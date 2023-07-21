# Copyright © 2023 Pathway

from __future__ import annotations

from typing import Callable, Dict, Iterable, List

import pathway.internals.graph_runner.expression_evaluator as evaluator
from pathway.internals import api, column, table, universe


class OutOfScopeError(RuntimeError):
    pass


class ScopeState:
    columns: Dict[column.Column, api.Column]
    tables: Dict[table.Table, api.Table]
    universes: Dict[universe.Universe, api.Universe]
    computers: List[Callable]
    evaluators: Dict[column.Context, evaluator.ExpressionEvaluator]

    def __init__(self) -> None:
        self.columns = {}
        self.universes = {}
        self.computers = []
        self.evaluators = {}
        self.tables = {}

    def set_column(self, key: column.Column, value: api.Column):
        self.columns[key] = value
        self.set_universe(key.universe, value.universe)

    def get_column(self, key: column.Column) -> api.Column:
        if key not in self.columns:
            raise OutOfScopeError("column out of scope")
        column = self.columns[key]
        return column

    def has_column(self, key: column.Column) -> bool:
        return key in self.columns

    def get_columns(self, columns: Iterable[column.Column]) -> List[api.Column]:
        return [self.get_column(column) for column in columns]

    def set_tables(
        self,
        keys: Iterable[table.Table],
        values: Iterable[api.Table],
    ):
        for output, result in zip(keys, values):
            self.set_table(output, result)

    def set_table(self, key: table.Table, value: api.Table):
        self.tables[key] = value
        self.set_column(key._id_column, value.universe.id_column)
        for (_, col), evaluated_column in zip(key._columns.items(), value.columns):
            self.set_column(col, evaluated_column)

    def get_table(self, key: table.Table) -> api.Table:
        if key in self.tables:
            return self.tables[key]
        else:
            universe = self.get_universe(key._universe)
            columns = self.get_columns(key._columns.values())
            table = api.Table(universe, columns)
            self.tables[key] = table
            return table

    def has_table(self, key: table.Table) -> bool:
        try:
            self.get_table(key)
            return True
        except OutOfScopeError:
            return False

    def get_tables(self, tables: Iterable[table.Table]) -> List[api.Table]:
        return [self.get_table(table) for table in tables]

    def set_universe(self, key: universe.Universe, value: api.Universe):
        if key in self.universes:
            assert self.universes[key] == value, "overriding already computed universe"
            return
        self.universes[key] = value

    def get_universe(self, key: universe.Universe):
        if key not in self.universes:
            raise OutOfScopeError("universe out of scope")
        return self.universes[key]

    def get_context_table(self, key: column.ContextTable) -> api.Table:
        return api.Table(
            universe=self.get_universe(key.universe),
            columns=[self.get_column(column) for column in key.columns],
        )

    def add_computer_logic(self, computer_callback: Callable) -> int:
        id = len(self.computers)
        self.computers.append(computer_callback)
        return id

    def get_computer_logic(self, id: int) -> Callable:
        return self.computers[id]

    def get_or_create_evaluator(
        self,
        context: column.Context,
        evaluator_factory: Callable[[column.Context], evaluator.ExpressionEvaluator],
    ):
        if context not in self.evaluators:
            evaluator = evaluator_factory(context)
            self.evaluators[context] = evaluator
        return self.evaluators[context]
