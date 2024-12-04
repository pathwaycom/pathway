# Copyright © 2024 Pathway

from __future__ import annotations

import re

import pytest

import pathway as pw
from pathway import demo
from pathway.internals import Schema, api
from pathway.tests.utils import (
    CsvPathwayChecker,
    DiffEntry,
    T,
    assert_key_entries_in_stream_consistent,
    assert_stream_equality,
    assert_stream_equality_wo_index,
    assert_stream_split_into_groups,
    assert_stream_split_into_groups_wo_index,
    run,
)


def test_stream_success():
    class TimeColumnInputSchema(Schema):
        number: int
        parity: int

    value_functions = {
        "number": lambda x: x + 1,
        "parity": lambda x: (x + 1) % 2,
    }

    t = demo.generate_custom_stream(
        value_functions,
        schema=TimeColumnInputSchema,
        nb_rows=15,
        input_rate=15,
        autocommit_duration_ms=50,
    )

    gb = t.groupby(t.parity).reduce(t.parity, cnt=pw.reducers.count())

    list = []
    row: dict[str, api.Value]

    for i in [1, 2]:
        parity = i % 2
        row = {"cnt": 1, "parity": parity}
        list.append(DiffEntry.create(gb, {"parity": parity}, i, True, row))
    for i in range(3, 16):
        parity = i % 2
        row = {"cnt": (i - 1) // 2, "parity": parity}
        list.append(DiffEntry.create(gb, {"parity": parity}, i, False, row))
        row = {"cnt": (i + 1) // 2, "parity": parity}
        list.append(DiffEntry.create(gb, {"parity": parity}, i, True, row))

    assert_key_entries_in_stream_consistent(list, gb)
    run()


def test_subscribe_consistency():
    class TimeColumnInputSchema(Schema):
        number: int
        parity: int
        number_div: int

    value_functions = {
        "number": lambda x: x + 1,
        "number_div": lambda x: (x + 1) // 2,
        "parity": lambda x: (x + 1) % 2,
    }

    t = demo.generate_custom_stream(
        value_functions,
        schema=TimeColumnInputSchema,
        nb_rows=15,
        input_rate=15,
        autocommit_duration_ms=100,
    )

    gb = t.groupby(t.parity).reduce(
        t.parity,
        cnt=pw.reducers.count(),
        max_number=pw.reducers.max(t.number),
        max_div=pw.reducers.max(t.number_div),
    )

    list = []
    row: dict[str, api.Value]

    for i in [1, 2]:
        parity = i % 2
        row = {"cnt": 1, "parity": parity, "max_number": i, "max_div": i // 2}
        list.append(DiffEntry.create(gb, {"parity": parity}, i, True, row))
    for i in range(3, 16):
        parity = i % 2
        row = {
            "cnt": (i - 1) // 2,
            "parity": parity,
            "max_number": i - 2,
            "max_div": (i - 2) // 2,
        }
        list.append(DiffEntry.create(gb, {"parity": parity}, i, False, row))
        row = {
            "cnt": (i + 1) // 2,
            "parity": parity,
            "max_number": i,
            "max_div": i // 2,
        }
        list.append(DiffEntry.create(gb, {"parity": parity}, i, True, row))

    assert_key_entries_in_stream_consistent(list, gb)
    run()


def test_stream_test_util_should_fail_q_none():
    class TimeColumnInputSchema(Schema):
        number: int
        parity: int

    value_functions = {
        "number": lambda x: x + 1,
        "parity": lambda x: (x + 1) % 2,
    }

    t = demo.generate_custom_stream(
        value_functions,
        schema=TimeColumnInputSchema,
        nb_rows=15,
        input_rate=15,
        autocommit_duration_ms=50,
    )

    gb = t.groupby(t.parity).reduce(t.parity, cnt=pw.reducers.count())

    list = []
    row: dict[str, api.Value]

    for i in [1, 2]:
        parity = i % 2
        row = {"cnt": 1, "parity": parity}
        list.append(DiffEntry.create(gb, {"parity": parity}, i, True, row))
    for i in range(3, 7):
        parity = i % 2
        row = {"cnt": (i - 1) // 2, "parity": parity}
        list.append(DiffEntry.create(gb, {"parity": parity}, i + 7, False, row))
        row = {"cnt": (i + 1) // 2, "parity": parity}
        list.append(DiffEntry.create(gb, {"parity": parity}, i + 7, True, row))
    for i in range(7, 16):
        parity = i % 2
        row = {"cnt": (i - 1) // 2, "parity": parity}
        list.append(DiffEntry.create(gb, {"parity": parity}, i - 4, False, row))
        row = {"cnt": (i + 1) // 2, "parity": parity}
        list.append(DiffEntry.create(gb, {"parity": parity}, i - 4, True, row))

    assert_key_entries_in_stream_consistent(list, gb)
    with pytest.raises(AssertionError):
        run()


def test_stream_test_util_should_fail_empty_final_state():
    class TimeColumnInputSchema(Schema):
        number: int
        parity: int

    value_functions = {
        "number": lambda x: x + 1,
        "parity": lambda x: (x + 1) % 2,
    }

    t = demo.generate_custom_stream(
        value_functions,
        schema=TimeColumnInputSchema,
        nb_rows=15,
        input_rate=15,
        autocommit_duration_ms=50,
    )

    gb = t.groupby(t.parity).reduce(t.parity, cnt=pw.reducers.count())

    list = []
    row: dict[str, api.Value]

    for i in [1, 2]:
        parity = i % 2
        row = {"cnt": 1, "parity": parity}
        list.append(DiffEntry.create(gb, {"parity": parity}, i, True, row))
    for i in range(3, 18):
        parity = i % 2
        row = {"cnt": (i - 1) // 2, "parity": parity}
        list.append(DiffEntry.create(gb, {"parity": parity}, i, False, row))
        row = {"cnt": (i + 1) // 2, "parity": parity}
        list.append(DiffEntry.create(gb, {"parity": parity}, i, True, row))

    assert_key_entries_in_stream_consistent(list, gb)
    with pytest.raises(AssertionError):
        run()


def test_assert_stream_equality():
    t = T(
        """
      | a | __time__ | __diff__
    9 | 0 | 2        |    1
    7 | 2 | 4        |    1
    8 | 1 | 4        |    1
    6 | 3 | 6        |    1
    7 | 2 | 6        |   -1
    6 | 4 | 8        |    1
    5 | 4 | 8        |    1
    6 | 3 | 8        |   -1
    """
    )
    expected = T(
        """
      | a | __time__ | __diff__
    9 | 0 | 2        |    1
    8 | 1 | 4        |    1
    7 | 2 | 4        |    1
    6 | 3 | 6        |    1
    7 | 2 | 6        |   -1
    5 | 4 | 8        |    1
    6 | 3 | 8        |   -1
    6 | 4 | 8        |    1
    """
    )
    assert_stream_equality(t, expected)


def test_assert_table_revisions_equality_with_id():
    t = T(
        """
    a | __time__ | __diff__
    0 |    2     |    1
    1 |    4     |    1
    2 |    4     |    1
    3 |    6     |    1
    4 |    8     |    1
    4 |    8     |    1
    """
    )
    expected = T(
        """
    a | __time__ | __diff__
    0 |    2     |    1
    2 |    4     |    1
    1 |    4     |    1
    3 |    6     |    1
    4 |    8     |    1
    4 |    8     |    1
    """
    )
    assert_stream_equality_wo_index(t, expected)


def test_assert_table_revisions_equality_with_id_multiple_workers():
    t = T(
        """
      | a | __time__ | __diff__ | __shard__
    9 | 0 |    2     |    1     |     0
    8 | 1 |    4     |    1     |     0
    7 | 2 |    4     |    1     |     1
    2 | 8 |    4     |    1     |     2
    6 | 3 |    6     |    1     |     3
    7 | 2 |    6     |   -1     |     1
    5 | 4 |    8     |    1     |     0
    6 | 3 |    8     |   -1     |     3
    6 | 4 |    8     |    1     |     3
    2 | 8 |    8     |   -1     |     2
    """
    )
    expected = T(
        """
      | a | __time__ | __diff__
    9 | 0 |    2     |    1
    8 | 1 |    4     |    1
    7 | 2 |    4     |    1
    2 | 8 |    4     |    1
    6 | 3 |    6     |    1
    7 | 2 |    6     |   -1
    5 | 4 |    8     |    1
    6 | 3 |    8     |   -1
    6 | 4 |    8     |    1
    2 | 8 |    8     |   -1
    """
    )
    assert_stream_equality(t, expected)


def test_raises_when_not_equal_1():
    t = T(
        """
      | a | __time__ | __diff__
    9 | 0 |    2     |    1
    8 | 1 |    4     |    1
    7 | 2 |    4     |    1
    """
    )
    expected = T(
        """
      | a | __time__ | __diff__
    9 | 0 |    2     |    1
    """
    )
    with pytest.raises(AssertionError):
        assert_stream_equality(t, expected)


def test_raises_when_not_equal_2():
    t = T(
        """
      | a | __time__ | __diff__
    9 | 0 |    2     |    1
    8 | 1 |    4     |    1
    7 | 2 |    4     |    1
    """
    )
    expected = T(
        """
      | a | __time__ | __diff__
    9 | 0 |    2     |    1
    7 | 2 |    4     |    1
    """
    )
    with pytest.raises(AssertionError):
        assert_stream_equality(t, expected)


def test_raises_when_schemas_dont_match():
    t = T(
        """
      | a | __time__ | __diff__
    9 | 0 |    2     |    1
    8 | 1 |    4     |    1
    7 | 2 |    4     |    1
    """
    )
    expected = T(
        """
      | b | __time__ | __diff__
    9 | 0 |    2     |    1
    8 | 1 |    4     |    1
    7 | 2 |    4     |    1
    """
    )
    with pytest.raises(RuntimeError):
        assert_stream_equality(t, expected)


def test_compute_and_print_update_stream(capsys):
    table_def = """
      | a | __time__ | __diff__
    9 | 0 |    2     |    1
    8 | 1 |    4     |    1
    7 | 2 |    4     |    1
    8 | 1 |    6     |   -1
    8 | 2 |    6     |    1
    """
    expected = """
a | __time__ | __diff__
0 | 2        | 1
1 | 4        | 1
2 | 4        | 1
1 | 6        | -1
2 | 6        | 1
    """
    t = T(table_def)
    pw.debug.compute_and_print_update_stream(t, include_id=False)
    caputed = capsys.readouterr()
    assert caputed.out.strip() == expected.strip()


def test_compute_and_print(capsys):
    table_def = """
      | a | __time__ | __diff__
    9 | 0 |    2     |    1
    8 | 1 |    4     |    1
    7 | 2 |    4     |    1
    8 | 1 |    6     |   -1
    8 | 2 |    6     |    1
    """
    expected = """
a
0
2
2
    """
    t = T(table_def)
    pw.debug.compute_and_print(t, include_id=False)
    caputed = capsys.readouterr()
    assert caputed.out.strip() == expected.strip()


def test_assert_stream_split_into_groups():
    table = T(
        """
    value | __time__ | __diff__
      1   |    12    |     1
      2   |    12    |     1
      3   |    12    |     1
      4   |    12    |     1
      1   |    16    |    -1
      2   |    16    |    -1
      3   |    16    |    -1
      5   |    18    |     1
      6   |    18    |     1
    """,
        id_from=["value"],
    )
    expected = T(
        """
    value | __time__ | __diff__
      1   |     2    |     1
      2   |     2    |     1
      3   |     4    |     1
      4   |     4    |     1
      1   |     6    |    -1
      2   |     6    |    -1
      3   |     8    |    -1
      5   |    10    |     1
      6   |    10    |     1
    """,
        id_from=["value"],
    )

    assert_stream_split_into_groups(table, expected)


def test_assert_stream_split_into_groups_does_not_allow_different_lengths():
    table = T(
        """
    value | __time__
      1   |    12
      2   |    16
    """,
    )
    expected = T(
        """
    value | __time__
      1   |     2
      2   |     4
      3   |     4
    """,
    )

    with pytest.raises(AssertionError, match="assert 2 == 3"):
        assert_stream_split_into_groups(table, expected)


def test_assert_stream_split_into_groups_does_not_allow_different_vales():
    table = T(
        """
    value | __time__
      1   |    12
      2   |    16
    """,
    )
    expected = T(
        """
    value | __time__
      1   |     2
      3   |     4
    """,
    )

    with pytest.raises(AssertionError, match=r"assert \(.*, 2, 1\) == \(.*, 3, 1\)"):
        assert_stream_split_into_groups(table, expected)


def test_assert_stream_split_into_groups_does_not_allow_repetitions():
    table = T(
        """
    value | __time__ | __diff__
      1   |    12    |     1
      1   |    16    |    -1
      1   |    18    |     1
    """,
        id_from=["value"],
    )
    expected = T(
        """
    value | __time__ | __diff__
      1   |     2    |     1
      1   |     4    |    -1
      1   |     6    |     1
    """,
        id_from=["value"],
    )

    with pytest.raises(
        ValueError,
        match=re.escape(
            "This utility function does not support cases where the count of (value, diff) pair is !=1"
        ),
    ):
        assert_stream_split_into_groups(table, expected)


def test_assert_stream_split_into_groups_raises():
    table = T(
        """
    value | __time__ | __diff__
      1   |    12    |     1
      2   |    14    |     1
    """,
        id_from=["value"],
    )
    expected = T(
        """
    value | __time__ | __diff__
      1   |     2    |     1
      2   |     2    |     1
    """,
        id_from=["value"],
    )

    with pytest.raises(
        AssertionError,
        match=r"Expected \(.*, 2, 1\) to have time 12 but it has time 14\.",
    ):
        assert_stream_split_into_groups(table, expected)


def test_assert_stream_split_into_groups_wo_index():
    table = T(
        """
    value | __time__ | __diff__
      3   |    12    |     1
      4   |    12    |     1
      1   |    12    |     1
      2   |    12    |     1
    """
    )
    expected = T(
        """
    value | __time__ | __diff__
      1   |     2    |     1
      2   |     2    |     1
      3   |     4    |     1
      4   |     4    |     1
    """
    )

    assert_stream_split_into_groups_wo_index(table, expected)


def test_assert_stream_split_into_groups_wo_index_raises():
    table = T(
        """
    value | __time__ | __diff__
      1   |    12    |     1
      2   |    14    |     1
    """
    )
    expected = T(
        """
    value | __time__ | __diff__
      1   |     2    |     1
      2   |     2    |     1
    """
    )

    with pytest.raises(
        AssertionError,
        match=re.escape("Expected (2, 1) to have time 12 but it has time 14."),
    ):
        assert_stream_split_into_groups_wo_index(table, expected)


def test_csv_pathway_checker_1(tmp_path):
    path = tmp_path / "output.csv"
    with open(path, "w") as f:
        f.write("a,time,diff\n1,10,1\n2,12,1\n")
    expected_1 = """
    a
    1
    2
    """
    assert CsvPathwayChecker(expected_1, tmp_path)()
    assert CsvPathwayChecker(expected_1, tmp_path)()
    expected_2 = """
    a
    1
    """
    assert not CsvPathwayChecker(expected_2, tmp_path)()
    expected_3 = """
    a
    1
    3
    """
    assert not CsvPathwayChecker(expected_3, tmp_path)()


def test_csv_pathway_checker_2(tmp_path):
    path = tmp_path / "output.csv"
    with open(path, "w") as f:
        f.write("a,b,time,diff\n1,2,10,1\n2,3,12,1\n1,2,12,-1\n1,4,12,1\n")
    expected_1 = """
    a | b
    1 | 4
    2 | 3
    """
    assert CsvPathwayChecker(expected_1, tmp_path, id_from=["a"])()
    assert CsvPathwayChecker(expected_1, tmp_path, id_from=["a"])()
    expected_2 = """
    a | b
    1 | 2
    2 | 3
    """
    assert not CsvPathwayChecker(expected_2, tmp_path, id_from=["a"])()
    expected_3 = """
    a | b
    1 | 2
    1 | 4
    2 | 3
    """
    assert not CsvPathwayChecker(expected_3, tmp_path, id_from=["a"])()


def test_csv_pathway_checker_3(tmp_path):
    path = tmp_path / "output.csv"
    with open(path, "w") as f:
        f.write("a,b,time,diff\n1,2,10,1\n2,3,12,1\n1,2,12,-1\n")
    expected_1 = """
    a | b
    2 | 3
    """
    assert CsvPathwayChecker(expected_1, tmp_path, id_from=["a"])()
    assert CsvPathwayChecker(expected_1, tmp_path, id_from=["a"])()
    expected_2 = """
    a | b
    1 | 2
    2 | 3
    """
    assert not CsvPathwayChecker(expected_2, tmp_path, id_from=["a"])()


def test_csv_pathway_checker_4(tmp_path):
    path = tmp_path / "output.csv"
    with open(path, "w") as f:
        f.write(
            "a,b,time,diff\n1,2,10,1\n2,3,12,1\n2,2,12,-1\n"
        )  # deleting non-existing row
    expected_1 = """
    a | b
    2 | 3
    """
    assert not CsvPathwayChecker(expected_1, tmp_path, id_from=["a"])()
    expected_2 = """
    a | b
    1 | 2
    2 | 3
    """
    assert not CsvPathwayChecker(expected_2, tmp_path, id_from=["a"])()
    expected_3 = """
    a | b
    1 | 2
    2 | 3
    2 | 2
    """
    assert not CsvPathwayChecker(expected_3, tmp_path, id_from=["a"])()
