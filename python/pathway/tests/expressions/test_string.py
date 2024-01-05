# Copyright © 2024 Pathway

import pandas as pd
import pytest

import pathway as pw
from pathway.debug import table_from_pandas
from pathway.tests.utils import T, assert_table_equality, run_all


def test_strip():
    t = table_from_pandas(
        pd.DataFrame({"a": ["   abc", "   def   ", "ab   cd  ", "xy  zt", "zy  "]})
    )
    expected = table_from_pandas(
        pd.DataFrame({"a": ["abc", "def", "ab   cd", "xy  zt", "zy"]})
    )
    result = t.select(a=pw.this.a.str.strip())
    assert_table_equality(result, expected)


def test_count():
    t = T(
        """
          | name
        0 | Alice
        1 | olice
        2 | Hello
        3 | World
        4 | Zoo
     """
    )
    assert_table_equality(
        t.select(count=pw.this.name.str.count("o")),
        T(
            """
          | count
        0 | 0
        1 | 1
        2 | 1
        3 | 1
        4 | 2
        """
        ),
    )
    assert_table_equality(
        t.select(count=pw.this.name.str.count("o", 1)),
        T(
            """
          | count
        0 | 0
        1 | 0
        2 | 1
        3 | 1
        4 | 2
        """
        ),
    )
    assert_table_equality(
        t.select(count=pw.this.name.str.count("o", 0, 3)),
        T(
            """
          | count
        0 | 0
        1 | 1
        2 | 0
        3 | 1
        4 | 2
        """
        ),
    )
    assert_table_equality(
        t.select(count=pw.this.name.str.count("o", end=2)),
        T(
            """
          | count
        0 | 0
        1 | 1
        2 | 0
        3 | 1
        4 | 1
        """
        ),
    )


def test_find():
    t = T(
        """
          | name
        0 | Alice
        1 | olice
        2 | Hello
        3 | World
        4 | Zoo
     """
    )
    assert_table_equality(
        t.select(pos=pw.this.name.str.find("o")),
        T(
            """
          | pos
        0 | -1
        1 | 0
        2 | 4
        3 | 1
        4 | 1
        """
        ),
    )
    assert_table_equality(
        t.select(pos=pw.this.name.str.find("o", 1)),
        T(
            """
          | pos
        0 | -1
        1 | -1
        2 | 4
        3 | 1
        4 | 1
        """
        ),
    )
    assert_table_equality(
        t.select(pos=pw.this.name.str.find("o", 2)),
        T(
            """
          | pos
        0 | -1
        1 | -1
        2 | 4
        3 | -1
        4 | 2
        """
        ),
    )
    assert_table_equality(
        t.select(pos=pw.this.name.str.find("o", 0, 4)),
        T(
            """
          | pos
        0 | -1
        1 | 0
        2 | -1
        3 | 1
        4 | 1
        """
        ),
    )
    assert_table_equality(
        t.select(pos=pw.this.name.str.find("o", end=2)),
        T(
            """
          | pos
        0 | -1
        1 | 0
        2 | -1
        3 | 1
        4 | 1
        """
        ),
    )


def test_rfind():
    t = T(
        """
          | name
        0 | Alice
        1 | olice
        2 | Hello
        3 | World
        4 | Zoo
     """
    )
    assert_table_equality(
        t.select(pos=pw.this.name.str.rfind("o")),
        T(
            """
          | pos
        0 | -1
        1 | 0
        2 | 4
        3 | 1
        4 | 2
        """
        ),
    )
    assert_table_equality(
        t.select(pos=pw.this.name.str.rfind("o", 1)),
        T(
            """
          | pos
        0 | -1
        1 | -1
        2 | 4
        3 | 1
        4 | 2
        """
        ),
    )
    assert_table_equality(
        t.select(pos=pw.this.name.str.rfind("o", 2)),
        T(
            """
          | pos
        0 | -1
        1 | -1
        2 | 4
        3 | -1
        4 | 2
        """
        ),
    )
    assert_table_equality(
        t.select(pos=pw.this.name.str.rfind("o", 0, 4)),
        T(
            """
          | pos
        0 | -1
        1 | 0
        2 | -1
        3 | 1
        4 | 2
        """
        ),
    )
    assert_table_equality(
        t.select(pos=pw.this.name.str.rfind("o", end=2)),
        T(
            """
          | pos
        0 | -1
        1 | 0
        2 | -1
        3 | 1
        4 | 1
        """
        ),
    )


def move_to_pathway_with_the_right_type(list, dtype):
    df = pd.DataFrame({"a": list}, dtype=dtype)
    table = table_from_pandas(df)
    return table


def test_parse_int():
    from_ = ["10", "0", "-1", "-2", "4294967297", "35184372088833"]
    to_ = [10, 0, -1, -2, 2**32 + 1, 2**45 + 1]

    table = move_to_pathway_with_the_right_type(from_, str)
    expected = move_to_pathway_with_the_right_type(to_, int)
    table = table.select(a=pw.this.a.str.parse_int())
    assert_table_equality(table, expected)


def test_parse_float():
    from_ = [
        "10.345",
        "10.999",
        "-1.012",
        "-1.99",
        "-2.01",
        "4294967297",
        "35184372088833",
    ]
    to_ = [
        10.345,
        10.999,
        -1.012,
        -1.99,
        -2.01,
        float(2**32 + 1),
        float(2**45 + 1),
    ]

    table = move_to_pathway_with_the_right_type(from_, str)
    expected = move_to_pathway_with_the_right_type(to_, float)
    table = table.select(a=pw.this.a.str.parse_float())
    assert_table_equality(table, expected)


def test_parse_bool():
    from_ = ["On", "true", "1", "Yes", "off", "False", "0", "no"]
    to_ = [True, True, True, True, False, False, False, False]

    table = move_to_pathway_with_the_right_type(from_, str)
    expected = move_to_pathway_with_the_right_type(to_, bool)
    table = table.select(a=pw.this.a.str.parse_bool())
    assert_table_equality(table, expected)


def test_parse_bool_custom_mapping():
    from_ = ["44", "true", "a", "-5"]
    to_ = [True, False, True, False]

    table = move_to_pathway_with_the_right_type(from_, str)
    expected = move_to_pathway_with_the_right_type(to_, bool)
    table = table.select(
        a=pw.this.a.str.parse_bool(
            true_values=["a", "44", ">"], false_values=["true", "-5"]
        )
    )
    assert_table_equality(table, expected)


def test_parse_int_optional():
    from_ = ["10", "0.5", "-1", "aaaa"]

    table = move_to_pathway_with_the_right_type(from_, str)
    expected = T(
        """
        a
        10
        None
        -1
        None
        """
    )
    table = table.select(a=pw.this.a.str.parse_int(optional=True))
    assert_table_equality(table, expected)


def test_parse_float_exception():
    from_ = ["10.5", "0.5", "4.4.4", "0.5"]

    table = move_to_pathway_with_the_right_type(from_, str)
    table = table.select(a=pw.this.a.str.parse_float(optional=False))
    with pytest.raises(ValueError):
        run_all()


def test_parse_float_optional():
    from_ = ["10.5", "0.5", "4.4.4", "-66"]

    table = move_to_pathway_with_the_right_type(from_, str)
    expected = T(
        """
        a
        10.5
        0.5
        None
        -66
        """
    )
    table = table.select(a=pw.this.a.str.parse_float(optional=True))
    assert_table_equality(table, expected)


def test_parse_bool_exception():
    from_ = ["1", "Truer", "off", "aaaa"]

    table = move_to_pathway_with_the_right_type(from_, str)
    table = table.select(a=pw.this.a.str.parse_bool(optional=False))
    with pytest.raises(ValueError):
        run_all()


def test_parse_bool_optional():
    from_ = ["1", "Truer", "off", "aaaa"]

    table = move_to_pathway_with_the_right_type(from_, str)
    expected = T(
        """
        a
        True
        None
        False
        None
        """
    )
    table = table.select(a=pw.this.a.str.parse_bool(optional=True))
    assert_table_equality(table, expected)


def test_parse_bool_optional_custom_mapping():
    from_ = ["1", "True", "off", "aaaa"]

    table = move_to_pathway_with_the_right_type(from_, str)
    expected = T(
        """
        a
        None
        None
        False
        None
        """
    )
    table = table.select(
        a=pw.this.a.str.parse_bool(
            true_values=["On"], false_values=["Off"], optional=True
        )
    )
    assert_table_equality(table, expected)


def test_parse_int_exception():
    from_ = ["10", "0.5", "-1", "aaaa"]

    table = move_to_pathway_with_the_right_type(from_, str)
    table = table.select(a=pw.this.a.str.parse_int(optional=False))
    with pytest.raises(ValueError):
        run_all()


def test_to_string():
    integers = [10, 0, -1, -2, 2**32 + 1, 2**45 + 1]
    bools = [True, False]
    floats = [
        10.345,
        10.999,
        -1.012,
        -1.99,
        -2.01,
        float(2**32 + 1),
        float(2**45 + 1),
    ]

    integers_table = move_to_pathway_with_the_right_type(integers, int)
    bools_table = move_to_pathway_with_the_right_type(bools, bool)
    floats_table = move_to_pathway_with_the_right_type(floats, float)

    res_integers = integers_table.select(a=pw.this.a.to_string().str.parse_int())
    assert_table_equality(integers_table, res_integers)

    res_bools = bools_table.select(a=pw.this.a.to_string().str.parse_bool())
    assert_table_equality(bools_table, res_bools)

    res_floats = floats_table.select(a=pw.this.a.to_string().str.parse_float())
    assert_table_equality(floats_table, res_floats)


def test_to_string_for_optional_type():
    table = T(
        """
        a
        10
        None
        -1
        -2
        None
        35184372088833
        """
    )
    expected = move_to_pathway_with_the_right_type(
        ["10", "None", "-1", "-2", "None", "35184372088833"], str
    )

    res = table.select(a=pw.this.a.to_string())
    assert_table_equality(res, expected)


def test_to_string_for_datetime_naive():
    t = T(
        """
          | t
        1 | 2019-12-31T23:49:59.999999999
        2 | 2019-12-31T23:49:59.0001
        3 | 2020-03-04T11:13:00.345612
        4 | 2023-03-26T12:00:00.000000001
        """
    )
    expected = T(
        """
          | t
        1 | 2019-12-31T23:49:59.999999999
        2 | 2019-12-31T23:49:59.000100000
        3 | 2020-03-04T11:13:00.345612000
        4 | 2023-03-26T12:00:00.000000001
        """
    )
    assert_table_equality(
        t.select(t=pw.this.t.dt.strptime("%Y-%m-%dT%H:%M:%S.%f").to_string()), expected
    )


def test_to_string_for_datetime_utc():
    t = T(
        """
          | t
        1 | 2019-12-31T23:49:59.999999999+0100
        2 | 2019-12-31T23:49:59.0001+0100
        3 | 2020-03-04T11:13:00.345612+0100
        4 | 2023-03-26T12:00:00.000000001+0100
        """
    )
    expected = T(
        """
          | t
        1 | 2019-12-31T22:49:59.999999999+0000
        2 | 2019-12-31T22:49:59.000100000+0000
        3 | 2020-03-04T10:13:00.345612000+0000
        4 | 2023-03-26T11:00:00.000000001+0000
        """
    )
    assert_table_equality(
        t.select(t=pw.this.t.dt.strptime("%Y-%m-%dT%H:%M:%S.%f%z").to_string()),
        expected,
    )
