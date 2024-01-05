# Copyright © 2024 Pathway

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pathway.internals.row_transformer as rt
from pathway.internals import api, trace, universe
from pathway.internals.graph_runner.operator_handler import OperatorHandler
from pathway.internals.graph_runner.path_storage import Storage
from pathway.internals.graph_runner.state import ScopeState
from pathway.internals.operator import RowTransformerOperator
from pathway.internals.row_transformer_table import (
    InputAttributeTransformerColumn,
    InputMethodTransformerColumn,
    OutputAttributeTransformerColumn,
    ToBeComputedTransformerColumn,
    TransformerColumn,
    TransformerColumnWithDependenecies,
    TransformerTable,
)
from pathway.internals.table import Table


class RowTransformerOperatorHandler(
    OperatorHandler[RowTransformerOperator], operator_type=RowTransformerOperator
):
    def _run(
        self,
        operator: RowTransformerOperator,
        output_storages: dict[Table, Storage],
    ):
        inputs = self._get_inputs(operator)
        result = self.scope.complex_columns(inputs)

        first_col = 0
        for table in operator.output_tables:
            number_of_cols = len(table._columns)
            universe = self.state.get_universe(table._universe)
            cols = result[first_col : first_col + number_of_cols]
            self.state.set_legacy_table(table, api.LegacyTable(universe, cols))
            first_col += number_of_cols
            output_storage = output_storages[table]
            if output_storage.flattened_output is not None:
                output_storage = output_storage.flattened_output
            self.state.create_table(table._universe, output_storage)

    def _get_inputs(self, operator: RowTransformerOperator) -> list[api.ComplexColumn]:
        """Gathers all input tables associated with all transformerClassArgs along with their dependencies."""
        dependency_tables = [
            table.dependency_table for table in operator.transformer_inputs
        ]
        mapping = TransformerColumnIndexMapping.from_tables(
            *operator.transformer_inputs, *dependency_tables
        )

        inputs = []

        for table in operator.transformer_inputs:
            inputs.extend(self.complex_table(table, mapping))
        for table in dependency_tables:
            inputs.extend(self.complex_dependency_table(table, mapping))

        return inputs

    def complex_table(
        self, table: TransformerTable, mapping: TransformerColumnIndexMapping
    ) -> list[api.ComplexColumn]:
        columns: list[api.ComplexColumn] = []

        for column in table.columns:
            input_column: api.ComplexColumn
            if isinstance(column, InputAttributeTransformerColumn):
                input_column = self.state.get_column(column.graph_column)
            elif isinstance(column, InputMethodTransformerColumn):
                input_column = self.input_method_computer(
                    column, table.universe, self.state, mapping
                )
            elif isinstance(column, ToBeComputedTransformerColumn):
                input_column = self.own_attribute_computer(
                    column, table.universe, self.state, mapping
                )
            else:
                raise NotImplementedError()
            columns.append(input_column)

        return columns

    def complex_dependency_table(
        self, table: TransformerTable, mapping
    ) -> list[api.ComplexColumn]:
        columns: list[api.ComplexColumn] = []

        for column in table.columns:
            input_column: api.ComplexColumn

            if isinstance(column, InputAttributeTransformerColumn):
                input_column = self.state.get_column(column.graph_column)
            elif isinstance(column, OutputAttributeTransformerColumn):
                input_column = self.state.get_column(column.graph_column)
            elif isinstance(column, TransformerColumnWithDependenecies):
                input_column = self.input_method_computer(
                    column, table.universe, self.state, mapping
                )
            elif isinstance(column, ToBeComputedTransformerColumn):
                input_column = self.own_attribute_computer(
                    column, table.universe, self.state, mapping
                )
            else:
                continue

            columns.append(input_column)

        return columns

    def input_method_computer(
        self,
        column: TransformerColumn,
        computer_universe: universe.Universe,
        state: ScopeState,
        mapping: TransformerColumnIndexMapping,
    ) -> api.Computer:
        """Creates computer for imported attribute."""
        attribute = column.attribute
        input_column = state.get_column(column.graph_column)

        def func(context: api.Context, *args, mapping=mapping):
            (computer_id, this_row) = context.data
            assert isinstance(computer_id, int)
            computer_logic = state.get_computer_logic(computer_id)
            return computer_logic(context, *args, mapping=mapping, this_row=this_row)

        return api.Computer.from_raising_fun(
            func,
            dtype=attribute.dtype,
            is_output=False,
            is_method=True,
            universe=self.state.get_universe(computer_universe),
            data_column=input_column,
        )

    def own_attribute_computer(
        self,
        column: ToBeComputedTransformerColumn,
        computer_universe: universe.Universe,
        state: ScopeState,
        mapping: TransformerColumnIndexMapping,
    ) -> api.Computer:
        """Creates computer for own attribute."""
        attribute: rt.ToBeComputedAttribute = column.attribute

        def func(context: api.Context, *args, mapping=mapping, this_row=None):
            this_row = this_row if this_row is not None else context.this_row
            row_reference = RowReference(
                column.attribute.class_arg,
                context,
                mapping,
                column.operator.id,
                this_row,
            )
            return attribute.compute(row_reference, *args)

        id = state.add_computer_logic(func)

        return api.Computer.from_raising_fun(
            func,
            dtype=attribute.dtype,
            is_output=attribute.is_output,
            is_method=attribute.is_method,
            universe=self.state.get_universe(computer_universe),
            data=id if attribute.is_method else None,
        )


class TransformerColumnIndexMapping:
    """Maps transformer column coordinates.

    Maps coordinates in the form of (operator_id, table_index, column_name)
    to the index of the corresponding column passed in the input list to the api.
    """

    _mapping: dict[tuple[int, int, str], int]

    def __init__(self) -> None:
        self._mapping = {}

    @classmethod
    def from_tables(cls, *tables: TransformerTable):
        mapping = cls()
        index = 0
        for table in tables:
            for column in table.columns:
                operator_id = column.operator.id
                class_arg_index = column.attribute.class_arg._index
                name = column.attribute.name
                mapping._set((operator_id, class_arg_index, name), index)
                index += 1
        return mapping

    def _set(self, key: tuple[int, int, str], value: int):
        if key in self._mapping:
            assert self._mapping[key] == value
            return
        self._mapping[key] = value

    def get(self, operator: int, table: int, column: str) -> int:
        return self._mapping[(operator, table, column)]


# Helper classes for RowReference.transformer property
@dataclass
class TransformerReferenceIndexer:
    row_reference: RowReference
    table_name: str

    def __getitem__(self, index):
        table_class_arg = self.row_reference._class_arg.transformer.class_args[
            self.table_name
        ]
        return table_class_arg(self.row_reference, index)  # type: ignore[call-arg]

    def pointer_from(self, *args, optional=False):
        # XXX verify
        return api.ref_scalar(*args, optional=optional)


@dataclass
class TransformerReference:
    """Helper class to support `self.transformer.foo_class[id] access."""

    row_reference: RowReference

    @trace.trace_user_frame
    def __getattr__(self, table_name: str):
        if table_name not in self.row_reference._class_arg.transformer.class_args:
            raise AttributeError(f"transformer has no attribute `{table_name}`")
        return TransformerReferenceIndexer(self.row_reference, table_name)


class RowReference:
    """Represents a context for particular row.

    Holds all needed information allowing to translate reference to a column by name into
    proper indexes understood by backend.
    """

    id: api.Pointer

    _context: api.Context
    _class_arg: rt.ClassArgMeta
    _mapping: TransformerColumnIndexMapping
    _operator_id: int
    """Identifies an instance of the row transformer."""

    def __init__(
        self,
        class_arg: rt.ClassArgMeta,
        context: api.Context,
        mapping: TransformerColumnIndexMapping,
        operator_id: int,
        id: api.Pointer,
    ):
        self.id = id
        self._context = context
        self._class_arg = class_arg
        self._mapping = mapping
        self._operator_id = operator_id

    @property
    def transformer(self):
        return TransformerReference(self)

    def _with_class_arg(self, class_arg: rt.ClassArgMeta, id: api.Pointer):
        return RowReference(
            class_arg, self._context, self._mapping, self._operator_id, id
        )

    @trace.trace_user_frame
    def __getattr__(self, name: str):
        try:
            if not self._class_arg._is_attribute(name):
                return self._non_attribute_property(name)

            table_index = self._class_arg._index
            column_index = self._mapping.get(self._operator_id, table_index, name)
            attribute = self._class_arg._get_attribute(name)
        except AttributeError:
            # Reraise with better message
            raise AttributeError(f"`{self.name}` has no attribute `{name}`")

        if attribute.is_method:

            def func(*args):
                return self._context.raising_get(column_index, self.id, *args)

            return func
        else:
            return self._context.raising_get(column_index, self.id)

    def _non_attribute_property(self, name: str) -> Any:
        attr = self._class_arg._get_class_property(name)
        if hasattr(attr, "__get__"):
            return attr.__get__(self)
        else:
            return attr
