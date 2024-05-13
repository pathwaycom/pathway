# Copyright © 2024 Pathway

from __future__ import annotations

from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Collection, Iterable
from dataclasses import dataclass
from functools import cached_property
from itertools import chain
from typing import TYPE_CHECKING, Any, TypeVar

import pathway.internals as pw
import pathway.internals.row_transformer_table as tt
from pathway.internals.arg_tuple import ArgTuple, as_arg_tuple
from pathway.internals.helpers import (
    FunctionSpec,
    SetOnceProperty,
    StableSet,
    fn_arg_tuple,
)
from pathway.internals.operator_input import OperatorInput
from pathway.internals.trace import Trace
from pathway.internals.universe import Universe

if TYPE_CHECKING:
    import pathway.internals.table as tables
    from pathway.internals import row_transformer as rt
    from pathway.internals.datasink import DataSink
    from pathway.internals.datasource import DataSource, StaticDataSource
    from pathway.internals.parse_graph import ParseGraph, Scope
    from pathway.internals.schema import Schema


class InOut(ABC):
    """Abstraction over Operator ends."""

    name: str
    operator: Operator

    def __init__(self, operator: Operator, name: str):
        super().__init__()
        self.name = name
        self.operator = operator

    def label(self):
        return self.name

    @property
    def graph(self):
        return self.operator.graph

    @property
    def id(self):
        return f"{type(self).__name__}_{self.operator.id}_{self.name}"


class InputHandle(InOut):
    """Handle for the input of the Operator."""

    value: OperatorInput

    def __init__(self, operator: Operator, name: str, value: OperatorInput):
        assert isinstance(value, OperatorInput)
        super().__init__(operator, name)
        self.value = value

    @property
    def dependencies(self) -> StableSet[Operator]:
        input_tables = self.value._operator_dependencies()
        return StableSet(table._source.operator for table in input_tables)


class OutputHandle(InOut):
    """Handle for the output of the Operator."""

    value: tables.Table

    def __init__(self, operator: Operator, name, value: tables.Table):
        super().__init__(operator, name)
        self.value = value


class Operator(ABC):
    """Abstraction over operator node.

    Operator holds its inputs (all arguments passed to operator) and outputs (resulting tables).
    Inputs and outputs retain their original order.
    """

    _inputs: dict[str, InputHandle]
    _outputs: dict[str, OutputHandle]
    trace: Trace
    graph: SetOnceProperty[ParseGraph] = SetOnceProperty()
    id: int
    error_log: tables.Table | None

    def __init__(self, id: int) -> None:
        self.id = id
        self._inputs = {}
        self._outputs = {}
        self.trace = Trace.from_traceback()

    @property
    def output_tables(self) -> Iterable[tables.Table]:
        return (output.value for output in self.outputs)

    @cached_property
    def intermediate_and_output_tables(self) -> Iterable[tables.Table]:
        return list(
            chain(
                *(
                    table._id_column.context.intermediate_tables()
                    for table in self.output_tables
                ),
                self.output_tables,
            )
        )

    @property
    def input_tables(self) -> StableSet[tables.Table]:
        return StableSet.union(
            *(i.value._operator_dependencies() for i in self._inputs.values())
        )

    @property
    def inputs(self) -> list[InputHandle]:
        return list(self._inputs.values())

    @property
    def outputs(self) -> list[OutputHandle]:
        return list(self._outputs.values())

    def get_input(self, name: str) -> InputHandle:
        return self._inputs[name]

    def get_output(self, name: str) -> OutputHandle:
        return self._outputs[name]

    def get_table(self, name: str) -> tables.Table:
        return self._outputs[name].value

    def _prepare_inputs(self, inputs: ArgTuple):
        valid_inputs = [
            (name, value)
            for name, value in inputs.items()
            if isinstance(value, OperatorInput)
        ]
        for name, value in valid_inputs:
            input = InputHandle(self, name, value)
            self._inputs[name] = input

    def _prepare_outputs(self, outputs: ArgTuple):
        for name, value in outputs.items():
            assert isinstance(value, pw.Table)
            output = OutputHandle(self, name, value)
            value._set_source(output)
            self._outputs[name] = output

    def set_graph(self, graph: ParseGraph):
        self.graph = graph

    def set_error_log(self, error_log: tables.Table | None) -> None:
        self.error_log = error_log

    def input_operators(self) -> StableSet[Operator]:
        result: StableSet[Operator] = StableSet()
        for handle in self.inputs:
            for dependency in handle.dependencies:
                result.add(dependency)
        return result

    def hard_table_dependencies(self) -> StableSet[tables.Table]:
        return StableSet()

    def operator_type(self) -> str:
        return type(self).__name__

    def label(self) -> str:
        return self.operator_type()

    def __repr__(self) -> str:
        return f"{self.id} [{self.label()}]"

    @cached_property
    def depends_on_error_log(self) -> bool:
        return any(
            dependency.depends_on_error_log
            for handle in self.inputs
            for dependency in handle.dependencies
        )


class OperatorFromDef(Operator, ABC):
    """Abstraction for operators created from python functions."""

    func_spec: FunctionSpec

    def __init__(self, spec: FunctionSpec, id: int) -> None:
        super().__init__(id)
        self.func_spec = spec

    @abstractmethod
    def __call__(self, *args, **kwargs):
        pass

    def operator_type(self) -> str:
        return self.func_spec.func.__name__


class ContextualizedIntermediateOperator(OperatorFromDef):
    """Operator producing tables with `ColumnWithExpression`s that have not been
    evaluated yet.

    `@contextualized_operator` can be used to decorate any function so that
    operator will be created and added to the graph whenever such function is called.
    """

    def __init__(self, func_spec: FunctionSpec, id: int):
        super().__init__(func_spec, id)

    def __call__(self, *args, **kwargs):
        input = fn_arg_tuple(self.func_spec, args, kwargs)
        self._prepare_inputs(input)

        result = self.func_spec.func(*args, **kwargs)
        result = as_arg_tuple(result)
        self._prepare_outputs(result)

        return result.scalar_or_tuple()


class DebugOperator(Operator):
    name: str
    table: tables.Table

    def __init__(self, name: str, id: int):
        super().__init__(id)
        self.name = name

    def __call__(self, table: tables.Table) -> None:
        self._prepare_inputs(as_arg_tuple(table))
        self.table = table

    def label(self):
        return f"debug: {self.name}"

    def hard_table_dependencies(self) -> StableSet[tables.Table]:
        return self.input_tables


TTable = TypeVar("TTable", bound="tables.Table[Any]")


class InputOperator(Operator):
    """Holds a definition of external datasource."""

    datasource: DataSource
    debug_datasource: StaticDataSource | None

    def __init__(
        self,
        datasource: DataSource,
        id: int,
        debug_datasource: StaticDataSource | None = None,
    ) -> None:
        super().__init__(id)
        self.datasource = datasource
        self.debug_datasource = debug_datasource

    def operator_type(self) -> str:
        return f"input({self.datasource.name})"

    def __call__(self, table_cls: type[TTable]) -> TTable:
        result = table_cls._from_schema(self.datasource.get_effective_schema())
        self._prepare_outputs(as_arg_tuple(result))
        return result

    @cached_property
    def depends_on_error_log(self) -> bool:
        from pathway.internals.datasource import ErrorLogDataSource

        return isinstance(self.datasource, ErrorLogDataSource)


class OutputOperator(Operator):
    """Holds a definition of datasink."""

    datasink: DataSink
    table: tables.Table

    def __init__(self, datasink: DataSink, id: int) -> None:
        super().__init__(id)
        self.datasink = datasink

    def __call__(self, table: tables.Table) -> OutputOperator:
        self._prepare_inputs(as_arg_tuple(table))
        self.table = table
        return self

    def hard_table_dependencies(self) -> StableSet[tables.Table]:
        return self.input_tables

    def operator_type(self) -> str:
        return f"output({self.datasink.name})"


@dataclass
class iterate_universe(OperatorInput):
    table: tables.Table

    def _operator_dependencies(self):
        return self.table._operator_dependencies()


class IterateOperator(OperatorFromDef):
    """Corresponds to `iterate` operation."""

    scope: Scope
    """Subscope holding nodes created by iteration logic."""

    iteration_limit: int | None

    iterated: ArgTuple
    iterated_with_universe: ArgTuple
    extra: ArgTuple

    iterated_copy: ArgTuple
    iterated_with_universe_copy: ArgTuple
    extra_copy: ArgTuple

    result_iterated: ArgTuple
    result_iterated_with_universe: ArgTuple

    _universe_mapping: dict[Universe, Universe]

    def __init__(
        self,
        func_spec: FunctionSpec,
        id: int,
        scope: Scope,
        iteration_limit: int | None = None,
    ):
        super().__init__(func_spec, id)
        self.scope = scope
        self.iteration_limit = iteration_limit
        self._universe_mapping = defaultdict(Universe)

    def __call__(self, **kwargs):
        input = as_arg_tuple(kwargs)

        input_copy = ArgTuple.empty()
        iterated_with_universe_copy = ArgTuple.empty()

        # unwrap input and materialize input copy
        for name, arg in input.items():
            if isinstance(arg, pw.Table):
                input_copy[name] = self._copy_input_table(name, arg, unique=False)
            elif isinstance(arg, iterate_universe):
                iterated_with_universe_copy[name] = self._copy_input_table(
                    name, arg.table, unique=True
                )
                input[name] = arg.table
            else:
                raise TypeError(f"{name} has to be a Table instead of {type(arg)}")

        assert all(isinstance(table, pw.Table) for table in input)

        # call iteration logic with copied input and sort result by input order
        raw_result = self.func_spec.func(**input_copy, **iterated_with_universe_copy)
        arg_tuple = as_arg_tuple(raw_result)
        result = arg_tuple.process_input(input)
        if not iterated_with_universe_copy.is_key_subset_of(result):
            raise ValueError(
                "not all arguments marked as iterated returned from iteration"
            )
        for name, table in result.items():
            input_table: pw.Table = input[name]
            assert isinstance(table, pw.Table)
            input_schema = input_table.schema._dtypes()
            result_schema = table.schema._dtypes()
            if input_schema != result_schema:
                raise ValueError(
                    f"output: {result_schema}  of the iterated function does not correspond to the input: {input_schema}"  # noqa
                )
            table._sort_columns_by_other(input_table)

        # designate iterated arguments
        self.iterated_with_universe = input.intersect_keys(iterated_with_universe_copy)
        self.iterated = input.intersect_keys(result).subtract_keys(
            iterated_with_universe_copy
        )
        self.extra = input.subtract_keys(result)

        # do the same for proxied arguments
        self.iterated_with_universe_copy = iterated_with_universe_copy
        self.iterated_copy = input_copy.intersect_keys(result).subtract_keys(
            iterated_with_universe_copy
        )
        self.extra_copy = input_copy.subtract_keys(self.iterated_copy)

        # prepare iteration result
        self.result_iterated_with_universe = result.intersect_keys(
            iterated_with_universe_copy
        )
        self.result_iterated = result.subtract_keys(iterated_with_universe_copy)

        # materialize output
        output = type(arg_tuple).empty()
        for name, table in result.items():
            if name in self.iterated_with_universe_copy:
                universe = Universe()
            elif table._universe == input_copy[name]._universe:
                universe = input[name]._universe
            else:
                raise ValueError(
                    "iterated table not marked as 'iterate_universe' changed its universe"
                )
            output[name] = table._materialize(universe)
        output = output.with_same_order(
            self.result_iterated + self.result_iterated_with_universe
        )

        self._prepare_inputs(input)
        self._prepare_outputs(output)
        return output.to_output()

    def _copy_input_table(self, name: str, table: tables.Table, unique: bool):
        if unique:
            universe = Universe()
        else:
            universe = self._universe_mapping[table._universe]
        table_copy = table._materialize(universe)
        table_copy._set_source(OutputHandle(self, name, table_copy))
        return table_copy

    def hard_table_dependencies(self) -> StableSet[tables.Table]:
        return self.input_tables

    def label(self):
        return f"iterate: {self.func_spec.func.__name__}"


class RowTransformerOperator(Operator):
    """Corresponds to `example_row_transformer(input, ...)`."""

    transformer: rt.RowTransformer
    transformer_inputs: list[tt.TransformerTable]

    def __init__(self, id: int, transformer: rt.RowTransformer) -> None:
        super().__init__(id)
        self.transformer = transformer
        self.transformer_inputs = []

    def __call__(self, tables: dict[str, tables.Table]):
        input_tables, output_tables = self._prepare_tables(tables)

        self.transformer_inputs = list(input_tables.values())
        result = as_arg_tuple(output_tables)

        self._prepare_inputs(as_arg_tuple(tables))
        self._prepare_outputs(result)

        return result.scalar_or_tuple()

    def _prepare_tables(
        self, tables_dict: dict[str, tables.Table]
    ) -> tuple[dict[str, tt.TransformerTable], dict[str, tables.Table]]:
        input_tables: dict[str, tt.TransformerTable] = {}
        output_tables: dict[str, tables.Table] = {}

        for class_arg in self.transformer.class_args.values():
            param_table = tables_dict[class_arg.name]

            input_tables[class_arg.name] = self._prepare_input_table(
                class_arg._attributes.values(), param_table
            )

            output_tables[class_arg.name] = self._prepare_output_table(
                class_arg._output_attributes.values(),
                param_table,
                class_arg.output_schema,
            )

        return input_tables, output_tables

    def _prepare_input_table(
        self,
        attributes: Collection[rt.AbstractAttribute],
        param_table: tables.Table,
    ) -> tt.TransformerTable:
        columns: list[tt.TransformerColumn] = [
            attr.to_transformer_column(self, param_table) for attr in attributes
        ]
        return tt.TransformerTable(param_table._universe, columns=columns)

    def _prepare_output_table(
        self,
        attributes: Collection[rt.AbstractOutputAttribute],
        param_table: tables.Table,
        schema: type[Schema],
    ):
        columns = {
            attr.output_name: attr.to_output_column(param_table._universe)
            for attr in attributes
        }
        return param_table._with_same_universe(
            *columns.items(), schema=schema.with_id_type(param_table.schema.id_type)
        )

    def all_columns(self) -> list[tt.TransformerColumn]:
        columns = []
        for table in self.transformer_inputs:
            for column in table.columns:
                columns.append(column)
        return columns

    def hard_table_dependencies(self) -> StableSet[tables.Table]:
        return self.input_tables

    def label(self):
        return f"transformer: {self.transformer.name}"
