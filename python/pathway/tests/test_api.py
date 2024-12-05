# Copyright © 2024 Pathway

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from pathway.debug import _markdown_to_pandas, schema_from_pandas
from pathway.internals import api, column_path, dtype as dt
from pathway.tests.utils import only_standard_build

from .utils import assert_equal_tables, assert_equal_tables_wo_index

pytestmark = only_standard_build


@pytest.fixture
def event_loop():
    from pathway.internals.graph_runner import async_utils

    with async_utils.new_event_loop() as event_loop:
        yield event_loop


def table_to_legacy(scope, table, column_count):
    universe = scope.table_universe(table)
    return scope.table(
        universe,
        [
            scope.table_column(universe, table, column_path.ColumnPath((i,)))
            for i in range(column_count)
        ],
    )


def static_table_from_md(scope, txt, ptr_columns=(), legacy=True):
    df = _markdown_to_pandas(txt)
    return static_table_from_pandas(scope, df, ptr_columns, legacy)


def static_table_from_pandas(scope, df, ptr_columns=(), legacy=True):
    for column in ptr_columns:
        df[column] = df[column].apply(api.unsafe_make_pointer)

    schema = schema_from_pandas(df)

    columns: list[api.ColumnProperties] = []

    for col in schema.columns().values():
        columns.append(
            api.ColumnProperties(
                dtype=col.dtype.to_engine(),
                append_only=col.append_only,
            )
        )

    connector_properties = api.ConnectorProperties(
        unsafe_trusted_ids=True,
        column_properties=columns,
    )

    table = api.static_table_from_pandas(
        scope,
        df,
        connector_properties,
    )
    if legacy:
        table = table_to_legacy(scope, table, len(df.columns))
    return table


def convert_table(scope, table):
    if isinstance(table, api.LegacyTable):
        new_table = scope.columns_to_table(table.universe, table.columns)
        return (
            new_table,
            [column_path.ColumnPath((i,)) for i in range(len(table.columns))],
        )
    raise NotImplementedError()


def convert_tables(scope, *tables):
    return tuple(convert_table(scope, table) for table in tables)


def test_assert(event_loop):
    def build(s):
        tab1 = static_table_from_md(
            s,
            """
                | a
                0 | 0
                1 | 10
                2 | 20
                """,
        )
        tab1prime = static_table_from_md(
            s,
            """
                | a
                0 | 0
                1 | 10
                2 | 20
                """,
        )
        tab2 = static_table_from_md(
            s,
            """
                    | a
                    0 | 0
                    1 | 1
                    2 | 2
                    """,
        )

        return convert_tables(s, tab1, tab1prime, tab2)

    tab1, tab1prime, tab2 = api.run_with_new_graph(build, event_loop)

    assert_equal_tables(tab1, tab1prime)

    with pytest.raises(AssertionError):
        assert_equal_tables(tab1, tab2)


@pytest.mark.xfail(reason="needs to be adjusted to new API")
def test_simple(event_loop):
    def build(s):
        tab = static_table_from_md(
            s,
            """
        | a
        0 | 0
        1 | 10
        2 | 20
        """,
        )

        filtering_col = s.map_column(
            tab,
            lambda values: values[0] >= 10,
            api.ColumnProperties(dtype=api.PathwayType.BOOL),
        )
        universe = s.filter_universe(tab.universe, filtering_col)
        const_col = s.map_column(
            s.table(universe, []),
            lambda values: 10,
            api.ColumnProperties(dtype=api.PathwayType.INT),
        )
        computed_col = s.map_column(
            s.table(universe, tab.columns + [const_col]),
            lambda values: values[0] + values[1],
            api.ColumnProperties(dtype=api.PathwayType.INT),
        )

        ret_table = s.table(
            universe, tab.columns + [filtering_col, const_col, computed_col]
        )
        expected = static_table_from_md(
            s,
            """
            c0    c1  c2  c3
            1  10  True  10  20
            2  20  True  10  30
            """,
        )

        return convert_tables(s, ret_table, expected)

    ret_table, expected = api.run_with_new_graph(build, event_loop)

    assert_equal_tables(ret_table, expected)


@pytest.mark.xfail(reason="needs to be adjusted to new API")
def test_groupby_simple(event_loop):
    def build(s):
        tab = static_table_from_md(
            s,
            """
            c0    c1   c2  c3
            1  10  True   10  20
            2  20  True   10  30
            3  30  False  10  30
            """,
        )

        groupby_table = s.table(tab.universe, [tab.columns[1], tab.columns[2]])
        grouper = s.group_by(groupby_table, [tab.columns[1], tab.columns[2]])
        cnt = grouper.count_column()
        ret = s.table(
            cnt.universe,
            [
                cnt,
                grouper.input_column(tab.columns[1]),
                grouper.input_column(tab.columns[2]),
            ],
        )
        expected = static_table_from_md(
            s,
            """
        c0     c1  c2
    0   2   True  10
    1   1  False  10
            """,
        )

        return convert_tables(s, ret, expected)

    ret, expected = api.run_with_new_graph(build, event_loop)

    assert_equal_tables_wo_index(ret, expected)


@pytest.mark.xfail(reason="needs to be adjusted to new API")
def test_groupby_reduce(event_loop):
    def build(s):
        tab = static_table_from_md(
            s,
            """
            c0    c1   c2  c3
            1  10  True   10  20
            2  20  True   10  30
            3  30  False  10  30
            """,
        )

        groupby_table = s.table(tab.universe, [tab.columns[1], tab.columns[2]])
        grouper = s.group_by(groupby_table, [tab.columns[1], tab.columns[2]])
        ret = s.table(
            grouper.universe,
            [
                grouper.input_column(tab.columns[1]),
                grouper.input_column(tab.columns[2]),
                grouper.reducer_column(api.Reducer.MIN, [tab.columns[3]]),
                grouper.reducer_column(api.Reducer.INT_SUM, [tab.columns[2]]),
                # grouper.reducer_column(api.Reducer.SORTED_TUPLE, tab.columns[3]), TODO: fix typing issues.
                grouper.count_column(),
                grouper.reducer_column(api.Reducer.ARG_MIN, [tab.columns[3]]),
            ],
        )
        expected = static_table_from_md(
            s,
            """
        c0  c1  c2  c3  c4  c5
    0   True  10  20  20   2   1
    1  False  10  30  10   1   3
            """,
            ptr_columns=["c5"],
        )

        return convert_tables(s, ret, expected)

    ret, expected = api.run_with_new_graph(build, event_loop)

    assert_equal_tables_wo_index(ret, expected)


@pytest.mark.xfail(reason="needs to be adjusted to new API")
def test_groupby_reduce_expression(event_loop):
    """Test case corresponding to

    tab.groupby(tab.c1, tab.c2)
       .reduce(c0 = tab.c1,
               c1 = tab.c2,
               c2 = 1000 + reducers.sum(tab.c0),
               c3 = reducers.sum(tab.c0 * tab.c0),
            )
    """

    def build(s):
        tab = static_table_from_md(
            s,
            """
            c0    c1   c2  c3
            1  10  True   10  20
            2  20  True   10  30
            3  30  False  10  30
            """,
        )
        col_expr_inner = s.map_column(
            tab, lambda x: x[0] * x[0], api.ColumnProperties(dtype=api.PathwayType.INT)
        )
        new_table = s.table(tab.universe, tab.columns + [col_expr_inner])
        groupby_table = s.table(
            new_table.universe, [new_table.columns[1], new_table.columns[2]]
        )
        grouper = s.group_by(groupby_table, [tab.columns[1], tab.columns[2]])

        tmp_tab = s.table(
            grouper.universe,
            [grouper.reducer_column(api.Reducer.INT_SUM, [new_table.columns[0]])],
        )
        col_expr_outer = s.map_column(
            tmp_tab,
            lambda x: 1000 + x[0],
            api.ColumnProperties(dtype=api.PathwayType.INT),
        )

        ret = s.table(
            grouper.universe,
            [
                grouper.input_column(new_table.columns[1]),
                grouper.input_column(new_table.columns[2]),
                col_expr_outer,
                grouper.reducer_column(api.Reducer.INT_SUM, [new_table.columns[4]]),
            ],
        )

        expected = static_table_from_md(
            s,
            """
        c0  c1    c2   c3
    0   True  10  1030  500
    1  False  10  1030  900
            """,
        )

        return convert_tables(s, ret, expected)

    ret, expected = api.run_with_new_graph(build, event_loop)

    assert_equal_tables_wo_index(ret, expected)


@pytest.mark.xfail(reason="needs to be adjusted to new API")
def test_groupby_set_id(event_loop):
    def build(s):
        tab = static_table_from_md(
            s,
            """
            c0    c1   c2  c3
            1  10  True   10  20
            2  20  True   10  30
            3  30  False  10  30
            """,
            ptr_columns=["c3"],
        )

        groupby_table = s.table(tab.universe, [tab.columns[3]])
        grouper = s.group_by(groupby_table, [tab.columns[3]], set_id=True)
        cnt = grouper.count_column()
        ret = s.table(
            cnt.universe,
            [
                cnt,
                grouper.input_column(tab.columns[3]),
            ],
        )
        expected = static_table_from_md(
            s,
            """
                    c0  c3
                20   1  20
                30   2  30
                """,
            ptr_columns=["c3"],
        )

        return convert_tables(s, ret, expected)

    ret, expected = api.run_with_new_graph(build, event_loop)

    assert_equal_tables(ret, expected)


@pytest.mark.xfail(reason="needs to be adjusted to new API")
def test_groupby_requested_columns(event_loop):
    def build(s):
        tab = static_table_from_md(
            s,
            """
                c0    c1   c2  c3
            1   10  True   10  20
            2   20  True   10  30
            3   30  False  10  30
            """,
        )

        groupby_table = s.table(tab.universe, [tab.columns[1], tab.columns[2]])
        grouper = s.group_by(groupby_table, [tab.columns[1]])
        cnt = grouper.count_column()
        with pytest.raises(Exception):
            ret = s.table(
                cnt.universe,
                [
                    cnt,
                    grouper.input_column(tab.columns[2]),
                ],
            )
        ret = s.table(
            cnt.universe,
            [
                cnt,
                grouper.input_column(tab.columns[1]),
            ],
        )
        expected = static_table_from_md(
            s,
            """
                c0  c1
            0   2   True
            1   1   False
            """,
        )

        return convert_tables(s, ret, expected)

    ret, expected = api.run_with_new_graph(build, event_loop)

    assert_equal_tables_wo_index(ret, expected)


@pytest.mark.xfail(reason="needs to be adjusted to new API")
def test_groupby_no_requested_columns(event_loop):
    def build(s):
        tab = static_table_from_md(
            s,
            """
                c0    c1   c2  c3
            1   10  True   10  20
            2   20  True   10  30
            3   30  False  10  30
            """,
        )

        groupby_table = s.table(tab.universe, [tab.columns[1], tab.columns[2]])
        grouper = s.group_by(groupby_table, [])
        cnt = grouper.count_column()
        with pytest.raises(Exception):
            ret = s.table(
                cnt.universe,
                [
                    cnt,
                    grouper.input_column(tab.columns[1]),
                ],
            )
        ret = s.table(
            cnt.universe,
            [
                cnt,
            ],
        )
        expected = static_table_from_md(
            s,
            """
                c0
            0   2
            1   1
            """,
        )

        return convert_tables(s, ret, expected)

    ret, expected = api.run_with_new_graph(build, event_loop)

    assert_equal_tables_wo_index(ret, expected)


@pytest.mark.xfail(reason="needs to be adjusted to new API")
def test_groupby_requested_columns_integrity(event_loop):
    def build(s):
        tab = static_table_from_md(
            s,
            """
                c0    c1   c2  c3
            1   10  True   10  20
            """,
        )

        with pytest.raises(ValueError):
            groupby_table = s.table(tab.universe, [tab.columns[1], tab.columns[2]])
            s.group_by(groupby_table, [tab.columns[3]])

        return []

    api.run_with_new_graph(build, event_loop)


@pytest.mark.xfail(reason="needs to be adjusted to new API")
def test_join_inner_simple(event_loop):
    def build(s):
        left = static_table_from_md(
            s,
            """
            c0    c1   c2  c3
            1  10  True   10  11
            2  20  True   10  12
            3  30  False  10  12
            """,
        )

        right = static_table_from_md(
            s,
            """
            c0   c1
            1  10    1
            2  10   11
            3  30  111
            """,
        )

        joiner = s.join(
            s.table(left.universe, [left.columns[0]]),
            s.table(right.universe, [right.columns[0]]),
        )
        ret = s.table(
            joiner.universe,
            [
                joiner.select_left_column(left.columns[0]),
                joiner.select_left_column(left.columns[3]),
                joiner.select_right_column(right.columns[0]),
                joiner.select_right_column(right.columns[1]),
            ],
        )
        expected = static_table_from_md(
            s,
            """
    c0  c1  c2   c3
    0  10  11  10   11
    1  10  11  10    1
    2  30  12  30  111
            """,
        )

        return convert_tables(s, ret, expected)

    ret, expected = api.run_with_new_graph(build, event_loop)

    assert_equal_tables_wo_index(ret, expected)


@pytest.mark.xfail(reason="needs to be adjusted to new API")
def test_join_on_id(event_loop):
    def build(s):
        left = static_table_from_md(
            s,
            """
            c0
            1  10
            2  20
            3  20
            """,
        )

        right = static_table_from_md(
            s,
            """
            c0
            2  20
            3  30
            4  30
            """,
        )

        joiner = s.join(
            s.table(left.universe, [left.universe.id_column]),
            s.table(right.universe, [right.universe.id_column]),
        )
        ret = s.table(
            joiner.universe,
            [
                joiner.select_left_column(left.columns[0]),
                joiner.select_left_column(left.universe.id_column),
                joiner.select_right_column(right.columns[0]),
            ],
        )
        expected = static_table_from_md(
            s,
            """
    c0  c1  c2
    1  20   2  20
    2  20   3  30
            """,
            ptr_columns=["c1"],
        )

        return convert_tables(s, ret, expected)

    ret, expected = api.run_with_new_graph(build, event_loop)

    assert_equal_tables_wo_index(ret, expected)


@pytest.mark.xfail(reason="needs to be adjusted to new API")
def test_join(event_loop):
    def build(s):
        left = static_table_from_md(
            s,
            """
        c0     c1  c2  c3
        1   1  Alice  10   1
        2   1    Bob   9   2
        3   2  Alice   8   3
        """,
        )
        right = static_table_from_md(
            s,
            """
            c0     c1  c2    c3   c4
        11   3  Alice  10     M   11
        12   1    Bob   9     L   12
        13   1    Tom   8    XL   13
        """,
        )

        joiner = s.join(
            s.table(left.universe, [left.columns[0], left.columns[1]]),
            s.table(right.universe, [right.columns[0], right.columns[1]]),
        )
        ret = s.table(
            joiner.universe,
            [
                joiner.select_left_column(left.columns[3]),
                joiner.select_left_column(left.columns[2]),
                joiner.select_right_column(right.columns[4]),
                joiner.select_right_column(right.columns[1]),
            ],
        )
        expected = static_table_from_md(
            s,
            """
        c0  c1  c2   c3
    1   2   9  12  Bob
            """,
        )

        return convert_tables(s, ret, expected)

    ret, expected = api.run_with_new_graph(build, event_loop)

    assert_equal_tables_wo_index(ret, expected)


@pytest.mark.xfail(reason="needs to be adjusted to new API")
def test_cross_join(event_loop):
    def build(s):
        left = static_table_from_md(
            s,
            """
        | pet | owner | age
        1 |   1 | Alice |  10
        2 |   1 |   Bob |   9
        3 |   2 | Alice |   8
        """,
        )
        right = static_table_from_md(
            s,
            """
        | pet | owner | age | size
        11 |   3 | Alice |  10 |    M
        12 |   1 |   Bob |  9  |    L
        13 |   1 |   Tom |  8  |   XL
        """,
        )

        joiner = s.join(
            s.table(left.universe, []),
            s.table(right.universe, []),
        )
        ret = s.table(
            joiner.universe,
            [
                joiner.select_left_column(left.columns[0]),
                joiner.select_left_column(left.columns[1]),
                joiner.select_right_column(right.columns[3]),
            ],
        )
        expected = static_table_from_md(
            s,
            """
        c0     c1  c2
    1   2  Alice   M
    2   1  Alice   L
    3   1  Alice   M
    4    1    Bob  XL
    5   2  Alice  XL
    6   1  Alice  XL
    7   1    Bob   L
    8   2  Alice   L
    9   1    Bob   M
        """,
        )

        return convert_tables(s, ret, expected)

    ret, expected = api.run_with_new_graph(build, event_loop)

    assert_equal_tables_wo_index(ret, expected)


def test_transformer(event_loop):
    def build(s):
        tab = static_table_from_md(
            s,
            """
        | a  | ptr
        0 | 10 |  -1
        1 | 20 |  0
        2 | 30 |  1
        """,
        )

        def computed_method_fun(c: api.Context, x: int):
            ptr: api.Pointer = c.raising_get(1, c.this_row)  # type: ignore
            if ptr >= 0:
                ptr = api.unsafe_make_pointer(ptr)
                other_a: int = c.raising_get(0, ptr)  # type: ignore
            else:
                other_a = 0
            return x + other_a * 100

        def computed_col(c: api.Context):
            a = c.raising_get(0, c.this_row)
            m = c.raising_get(2, c.this_row, a)
            return m

        trans_input = tab.columns + [
            api.Computer.from_raising_fun(
                computed_method_fun,
                dtype=api.PathwayType.INT,
                is_output=False,
                is_method=True,
                universe=tab.universe,
            ),
            api.Computer.from_raising_fun(
                computed_col,
                dtype=api.PathwayType.INT,
                is_output=True,
                is_method=False,
                universe=tab.universe,
            ),
        ]

        trans_tab = s.table(tab.universe, s.complex_columns(trans_input))
        expected = static_table_from_md(
            s,
            """
                    c0
                0    10
                1  1020
                2  2030
            """,
        )

        return convert_tables(s, trans_tab, expected)

    trans_tab, expected = api.run_with_new_graph(build, event_loop)

    assert_equal_tables(trans_tab, expected)


@pytest.mark.xfail
def test_transformer_loop(event_loop):
    def build(s):
        tab = static_table_from_md(
            s,
            """
        | a
        0 | 10
        1 | 20
        2 | 30
        """,
        )

        def computed_col(c: api.Context):
            # this tries to fetch itself
            a = c.raising_get(1, c.this_row)
            return a

        trans_input = tab.columns + [
            api.Computer.from_raising_fun(
                computed_col,
                dtype=api.PathwayType.INT,
                is_output=True,
                is_method=False,
                universe=tab.universe,
            ),
        ]

        result = s.table(tab.universe, s.complex_columns(trans_input))

        return convert_tables(s, result)

    with pytest.raises(RecursionError):
        api.run_with_new_graph(build, event_loop)


def test_iteration(event_loop):
    def collatz_step(x):
        (x,) = x
        if x == 1:
            return 1.0
        elif x % 2 == 0:
            return x / 2
        else:
            return 3 * x + 1

    def collatz_logic(s: api.Scope, iterated, iterated_with_universe, extra):
        (t,) = iterated
        assert not iterated_with_universe
        assert not extra
        col = s.map_column(
            t, collatz_step, api.ColumnProperties(dtype=api.PathwayType.FLOAT)
        )
        return ([s.table(col.universe, [col])], [])

    def build(s):
        iterated = [
            table_to_legacy(
                s,
                api.static_table_from_pandas(
                    s,
                    pd.DataFrame(
                        index=range(1, 101), data={"c0": np.arange(1.0, 101.0)}
                    ),
                ),
                1,
            )
        ]

        ([result_tab], []) = s.iterate(iterated, [], [], collatz_logic)

        expected = table_to_legacy(
            s,
            api.static_table_from_pandas(
                s, pd.DataFrame(index=range(1, 101), data={"c0": 1.0})
            ),
            1,
        )

        return convert_tables(s, result_tab, expected)

    result_tab, expected = api.run_with_new_graph(build, event_loop)

    assert_equal_tables(result_tab, expected)


def test_iteration_exception(event_loop):
    def logic(s: api.Scope, iterated, iterated_with_universe, extra):
        raise ValueError("manul")

    def build(s):
        with pytest.raises(ValueError, match="manul"):
            iterated = [
                static_table_from_md(
                    s,
                    """
                    a
                    42
                    """,
                )
            ]
            s.iterate(iterated, [], [], logic)

        return []

    api.run_with_new_graph(build, event_loop)


@pytest.mark.parametrize("limit", [2, 10])
def test_iteration_limit(limit, event_loop):
    def logic(s: api.Scope, iterated, iterated_with_universe, extra):
        (t,) = iterated
        assert not iterated_with_universe
        assert not extra
        col = s.map_column(
            t, lambda c: c[0] + 1, api.ColumnProperties(dtype=api.PathwayType.INT)
        )
        return ([s.table(col.universe, [col])], [])

    def build(s: api.Scope):
        values = list(range(10))
        iterated = [
            table_to_legacy(
                s, api.static_table_from_pandas(s, pd.DataFrame(data={"c0": values})), 1
            )
        ]

        ([result_tab], []) = s.iterate(iterated, [], [], logic, limit=limit)

        expected = table_to_legacy(
            s,
            api.static_table_from_pandas(
                s, pd.DataFrame(data={"c0": [v + limit for v in values]})
            ),
            1,
        )

        return convert_tables(s, result_tab, expected)

    result_tab, expected = api.run_with_new_graph(build, event_loop)

    assert_equal_tables(result_tab, expected)


@pytest.mark.parametrize("limit", [0, 1])
def test_iteration_limit_small(limit, event_loop):
    def logic(s: api.Scope, iterated, iterated_with_universe, extra):
        (t,) = iterated
        assert not iterated_with_universe
        assert not extra
        col = s.map_column(
            t, lambda c: c[0] + 1, api.ColumnProperties(dtype=api.PathwayType.INT)
        )
        return ([s.table(col.universe, [col])], [])

    def build(s):
        values = list(range(10))
        iterated = [
            table_to_legacy(
                s, api.static_table_from_pandas(s, pd.DataFrame(data={"c0": values})), 1
            )
        ]

        ([result_tab], []) = s.iterate(iterated, [], [], logic, limit=limit)
        return convert_tables(s, result_tab)

    with pytest.raises(ValueError):
        api.run_with_new_graph(build, event_loop)


@pytest.mark.xfail(reason="needs to be adjusted to new API")
def test_update_rows(event_loop):
    def build(s):
        tab = static_table_from_md(
            s,
            """
        | a
        0 | 10
        1 | 20
        2 | 30
        """,
        )
        inflation = static_table_from_md(
            s,
            """
        |   a
        0 |   0
        2 | 300
        3 | 400
        """,
        )
        universe = s.union_universe(tab.universe, inflation.universe)
        col = s.update_rows(universe, tab.columns[0], inflation.columns[0])
        res = s.table(col.universe, [col])
        expected = static_table_from_md(
            s,
            """
        |   a
        0 |   0
        1 |  20
        2 | 300
        3 | 400
        """,
        )

        return convert_tables(s, res, expected)

    res, expected = api.run_with_new_graph(build, event_loop)

    assert_equal_tables(res, expected)


@pytest.mark.xfail(reason="needs to be adjusted to new API")
def test_intersect(event_loop):
    def build(s):
        t1 = static_table_from_md(
            s,
            """
                | col
            1   | 11
            2   | 12
            3   | 13
            4   | 14
            """,
        )
        t2 = static_table_from_md(
            s,
            """
                | col
            2   | 11
            3   | 11
            4   | 11
            5   | 11
            """,
        )
        t3 = static_table_from_md(
            s,
            """
                | col
            1   | 11
            3   | 11
            4   | 11
            5   | 11
            """,
        )

        universe = s.intersect_universe(t1.universe, t2.universe, t3.universe)
        result = s.table(
            universe=universe,
            columns=[s.restrict_column(universe, c) for c in t1.columns],
        )
        expected = static_table_from_md(
            s,
            """
                    | col
                3   | 13
                4   | 14
                """,
        )

        return convert_tables(s, result, expected)

    result, expected = api.run_with_new_graph(build, event_loop)

    assert_equal_tables(result, expected)


@pytest.mark.xfail(reason="needs to be adjusted to new API")
def test_venn_universes(event_loop):
    def build(s):
        left = static_table_from_md(
            s,
            """
                | col
            1   | 11
            2   | 12
            3   | 13
            4   | 14
            """,
        )
        right = static_table_from_md(
            s,
            """
                | col
            2   | 11
            3   | 11
            4   | 11
            5   | 11
            """,
        )

        universes = s.venn_universes(left.universe, right.universe)

        only_left = s.table(
            universe=universes.only_left(),
            columns=[s.restrict_column(universes.only_left(), c) for c in left.columns],
        )
        expected_only_left = static_table_from_md(
            s,
            """
                | col
            1   | 11
            """,
        )

        only_right = s.table(
            universe=universes.only_right(),
            columns=[
                s.restrict_column(universes.only_right(), c) for c in right.columns
            ],
        )
        expected_only_right = static_table_from_md(
            s,
            """
                | col
            5   | 11
            """,
        )
        both = s.table(
            universe=universes.both(),
            columns=[s.restrict_column(universes.both(), c) for c in left.columns]
            + [s.restrict_column(universes.both(), c) for c in right.columns],
        )
        expected_both = static_table_from_md(
            s,
            """
                | left | right
            2   | 12   | 11
            3   | 13   | 11
            4   | 14   | 11
            """,
        )

        return convert_tables(
            s,
            only_left,
            only_right,
            both,
            expected_only_left,
            expected_only_right,
            expected_both,
        )

    (
        only_left,
        only_right,
        both,
        expected_only_left,
        expected_only_right,
        expected_both,
    ) = api.run_with_new_graph(build, event_loop)

    assert_equal_tables(only_left, expected_only_left)
    assert_equal_tables(only_right, expected_only_right)
    assert_equal_tables(both, expected_both)


@pytest.mark.xfail(reason="needs to be adjusted to new API")
def test_concat(event_loop):
    def build(s):
        left = static_table_from_md(
            s,
            """
                | col
            1   | 11
            2   | 12
            3   | 13
            """,
        )
        right = static_table_from_md(
            s,
            """
                | col
            4   | 14
            5   | 15
            6   | 16
            """,
        )

        concat = s.concat([left.universe, right.universe])

        result = s.table(
            universe=concat.universe,
            columns=[concat.concat_column([left.columns[0], right.columns[0]])],
        )

        expected = static_table_from_md(
            s,
            """
                | col
            1   | 11
            2   | 12
            3   | 13
            4   | 14
            5   | 15
            6   | 16
            """,
        )

        return convert_tables(s, result, expected)

    result, expected = api.run_with_new_graph(build, event_loop)

    assert_equal_tables(result, expected)


def test_concat_fail(event_loop):
    def build(s):
        left = static_table_from_md(
            s,
            """
                | col
            1   | 11
            2   | 12
            3   | 13
            """,
        )
        right = static_table_from_md(
            s,
            """
                | col
            3   | 33
            4   | 14
            5   | 15
            6   | 16
            """,
        )

        result = s.concat([left.universe, right.universe])

        return convert_tables(s, result)

    with pytest.raises(Exception):
        api.run_with_new_graph(build, event_loop)


@pytest.mark.parametrize(
    "value",
    [
        None,
        42,
        42.0,
        "",
        "foo",
        True,
        False,
        (),
        (42,),
        (1, 2, 3),
        np.array([], dtype=int),
        np.array([42]),
        np.array([1, 2, 3]),
        np.array([], dtype=float),
        np.array([42.0]),
        np.array([1.0, 2.0, 3.0]),
    ],
    ids=repr,
)
def test_value_type_via_python(event_loop, value):
    def build(s):
        key = api.ref_scalar()
        universe = s.static_universe([key])
        dtype = dt.wrap(type(value)).to_engine()
        column = s.static_column(
            universe, [(key, value)], properties=api.ColumnProperties(dtype=dtype)
        )
        table = s.table(universe, [column])

        def fun(values):
            [inner] = values
            assert type(value) is type(inner)
            if isinstance(value, np.ndarray):
                assert value.dtype == inner.dtype
                assert np.array_equal(value, inner)
            else:
                assert value == inner
            return inner

        new_column = s.map_column(table, fun, api.ColumnProperties(dtype=dtype))
        new_table = s.table(universe, [new_column])

        return convert_tables(s, table, new_table)

    table, new_table = api.run_with_new_graph(build, event_loop)

    assert_equal_tables_wo_index(
        table, new_table
    )  # wo_index knows how to compare arrays
