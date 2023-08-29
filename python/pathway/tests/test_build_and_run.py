# Copyright © 2023 Pathway

import os
import pathlib

import pytest

from pathway import (
    ClassArg,
    Table,
    input_attribute,
    output_attribute,
    reducers,
    schema_from_types,
    this,
    transformer,
)
from pathway.debug import _markdown_to_pandas
from pathway.internals import column, datasink, datasource, graph_runner
from pathway.internals.decorators import table_from_datasource
from pathway.internals.graph_runner.state import ScopeState
from pathway.internals.monitoring import MonitoringLevel
from pathway.internals.parse_graph import G
from pathway.internals.schema import schema_from_pandas
from pathway.io import csv
from pathway.tests.utils import T, TestDataSource


def test_process_only_relevant_nodes():
    input1 = Table.empty()
    input1.to(datasink.DataSink())
    input2 = Table.empty()
    output = input2.select()

    def validate(state: ScopeState) -> None:
        assert not state.has_table(input1)
        assert state.has_table(input2)
        assert state.has_table(output)

    graph_runner.GraphRunner(
        G, debug=False, monitoring_level=MonitoringLevel.NONE
    ).run_tables(output, after_build=validate)


def test_process_relevant_nodes_and_debug_nodes():
    input1 = Table.empty()
    input2 = Table.empty()
    input2.debug("input2")
    input3 = Table.empty()

    def validate(state: ScopeState) -> None:
        assert state.has_table(input1)
        assert state.has_table(input2)
        assert not state.has_table(input3)

    graph_runner.GraphRunner(
        G, debug=True, monitoring_level=MonitoringLevel.NONE
    ).run_tables(input1, after_build=validate)


def test_process_output_nodes(tmp_path: pathlib.Path):
    input1 = Table.empty()
    input1.debug("input1")
    input2 = Table.empty()

    file_path = tmp_path / "test_output.csv"
    csv.write(input2, file_path)

    def validate(state: ScopeState) -> None:
        assert not state.has_table(input1)
        assert state.has_table(input2)

    graph_runner.GraphRunner(
        G, debug=False, monitoring_level=MonitoringLevel.NONE
    ).run_outputs(after_build=validate)
    assert os.path.exists(file_path)


def test_process_output_nodes_and_debug_nodes(tmp_path: pathlib.Path):
    input1 = Table.empty()
    input1.debug("input1")
    input2 = Table.empty()
    input3 = Table.empty()

    file_path = tmp_path / "test_output.csv"
    csv.write(input2, file_path)

    def validate(state) -> None:
        assert state.has_table(input1)
        assert state.has_table(input2)
        assert not state.has_table(input3)

    graph_runner.GraphRunner(
        G, debug=True, monitoring_level=MonitoringLevel.NONE
    ).run_outputs(after_build=validate)
    assert os.path.exists(file_path)


def test_process_all_nodes():
    input1 = Table.empty()
    input2 = Table.empty()

    def validate(state: ScopeState) -> None:
        assert state.has_table(input1)
        assert state.has_table(input2)

    graph_runner.GraphRunner(G, monitoring_level=MonitoringLevel.NONE).run_all(
        after_build=validate
    )


def test_debug_datasource():
    df = _markdown_to_pandas(
        """
            | foo
        1   | 42
        """
    )
    input1 = table_from_datasource(
        datasource=TestDataSource(schema_from_types(foo=int)),
        debug_datasource=datasource.PandasDataSource(
            data=df,
            schema=schema_from_pandas(df),
        ),
    )
    input2 = T(
        """
            | foo
        1   | 42
        """
    )

    result, expected = graph_runner.GraphRunner(
        G, debug=True, monitoring_level=MonitoringLevel.NONE
    ).run_tables(input1, input2)

    assert result == expected


def test_debug_datasource_schema_mismatch():
    df = _markdown_to_pandas(
        """
            | foo
        1   | 42
        """
    )
    input = table_from_datasource(
        datasource=TestDataSource(schema_from_types(foo=str)),
        debug_datasource=datasource.PandasDataSource(
            data=df,
            schema=schema_from_pandas(df),
        ),
    )

    with pytest.raises(ValueError):
        graph_runner.GraphRunner(
            G, debug=True, monitoring_level=MonitoringLevel.NONE
        ).run_tables(input)


def test_process_only_relevant_columns():
    input1 = T(
        """
            | foo   | bar   | baz
        1   | 41    | a     | x
        2   | 42    | b     | y
        3   | 43    | c     | z
        """
    )

    input1 = input1.select(*input1)
    filtered = input1.filter(this.foo <= 42)
    result = filtered.select(this.bar)

    def validate(state: ScopeState) -> None:
        assert state.has_column(filtered._get_column("bar"))
        assert state.has_column(result._get_column("bar"))
        assert not state.has_column(filtered._get_column("foo"))
        assert not state.has_column(filtered._get_column("baz"))

    graph_runner.GraphRunner(G, monitoring_level=MonitoringLevel.NONE).run_tables(
        result, after_build=validate
    )


def test_process_columns_of_debug_nodes():
    input = T(
        """
            | foo
        1   | 42
        """
    )
    result = input.select(input.foo)
    result.debug(name="result")

    def validate(state: ScopeState):
        assert state.has_column(input.foo._column)
        assert state.has_column(result.foo._column)

    graph_runner.GraphRunner(
        G, debug=True, monitoring_level=MonitoringLevel.NONE
    ).run_outputs(after_build=validate)


def test_process_row_transformer_columns_if_needed():
    @transformer
    class foo_transformer:
        class table(ClassArg):
            arg = input_attribute()

            @output_attribute
            def ret(self) -> int:
                return self.arg + 1

    input = T(
        """
            | arg   |   foo
        1   | 1     |   1
        2   | 2     |   2
        3   | 3     |   3
        """
    ).select(*this)

    builder = graph_runner.GraphRunner(G, monitoring_level=MonitoringLevel.NONE)

    result1 = foo_transformer(input).table

    def validate(state: ScopeState) -> None:
        assert state.has_column(input._get_column("arg"))
        assert state.has_column(input._get_column("foo"))
        assert state.has_column(result1._get_column("ret"))

    builder.run_tables(result1, after_build=validate)

    result2 = input.select(this.arg)

    def validate(state: ScopeState) -> None:
        assert state.has_column(input._get_column("arg"))
        assert not state.has_column(input._get_column("foo"))
        assert state.has_column(result2._get_column("arg"))

    builder.run_tables(result2, after_build=validate)


def test_groupby_cache():
    table = T(
        """
            | pet  |  owner  | age
        1   | dog  | Alice   | 10
        2   | dog  | Bob     | 9
        3   | cat  | Alice   | 8
        4   | dog  | Bob     | 7
        """
    )

    g1 = table.groupby(table.pet)
    g2 = table.groupby(table.pet)

    g1.reduce(min=reducers.min(table.age))
    g2.reduce(min=reducers.max(table.age))

    assert g1 == g2

    def validate(state: ScopeState):
        groupby_contexts = list(
            ctx
            for ctx in state.evaluators.keys()
            if isinstance(ctx, column.GroupedContext)
        )
        assert len(groupby_contexts) == 1

    graph_runner.GraphRunner(G, monitoring_level=MonitoringLevel.NONE).run_all(
        after_build=validate
    )


def test_groupby_cache_multiple_cols():
    table = T(
        """
            | pet  |  owner  | age
        1   | dog  | Alice   | 10
        2   | dog  | Bob     | 9
        3   | cat  | Alice   | 8
        4   | dog  | Bob     | 7
        """
    )

    g1 = table.groupby(table.pet, this.owner)
    g2 = table.groupby(this.pet, table.owner)

    g1.reduce(min=reducers.min(table.age))
    g2.reduce(min=reducers.max(table.age))

    assert g1 == g2

    def validate(state: ScopeState):
        groupby_contexts = list(
            ctx
            for ctx in state.evaluators.keys()
            if isinstance(ctx, column.GroupedContext)
        )
        assert len(groupby_contexts) == 1

    graph_runner.GraphRunner(G, monitoring_level=MonitoringLevel.NONE).run_all(
        after_build=validate
    )


def test_groupby_cache_similar_tables():
    table = T(
        """
            | pet  |  owner  | age
        1   | dog  | Alice   | 10
        2   | dog  | Bob     | 9
        3   | cat  | Alice   | 8
        4   | dog  | Bob     | 7
        """
    )
    copy = table.select(*this)

    g1 = copy.groupby(table.pet)
    g2 = copy.groupby(copy.pet)

    assert g1 == g2

    g1.reduce(min=reducers.max(table.age))
    g2.reduce(min=reducers.max(copy.age))

    def validate(state: ScopeState):
        groupby_contexts = list(
            ctx
            for ctx in state.evaluators.keys()
            if isinstance(ctx, column.GroupedContext)
        )
        assert len(groupby_contexts) == 1

    graph_runner.GraphRunner(G, monitoring_level=MonitoringLevel.NONE).run_all(
        after_build=validate
    )
