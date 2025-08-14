# Copyright © 2024 Pathway

import pytest

import pathway as pw
from pathway.tests.utils import assert_stream_equality_wo_index


def test_forget_instance():
    t = pw.debug.table_from_markdown(
        """
        a |  t | __time__
        1 |  2 |     2
        1 |  3 |     2
        1 |  5 |     4
        1 | 15 |     8
        1 |  7 |    10
        2 |  4 |     2
        2 |  8 |     4
        2 |  9 |     8
        3 |  1 |     6
    """
    )

    res = t._forget(
        pw.this.t + 2,
        pw.this.t,
        mark_forgetting_records=False,
        instance_column=pw.this.a,
    )
    expected = pw.debug.table_from_markdown(
        """
        a |  t | __time__ | __diff__
        1 |  2 |     2    |     1
        1 |  3 |     2    |     1
        1 |  5 |     4    |     1
        1 |  2 |     6    |    -1
        1 |  3 |     6    |    -1
        1 | 15 |     8    |     1
        1 |  5 |    10    |    -1
        2 |  4 |     2    |     1
        2 |  8 |     4    |     1
        2 |  4 |     6    |    -1
        2 |  9 |     8    |     1
        3 |  1 |     6    |     1
    """
    )
    assert_stream_equality_wo_index(res, expected)


@pytest.mark.parametrize("public", [True, False])
def test_forget_no_instance(public: bool):
    t = pw.debug.table_from_markdown(
        """
        a |  t | __time__
        1 |  2 |     2
        1 |  3 |     2
        1 |  5 |     4
        1 | 15 |     8
        1 |  7 |    10
        2 |  4 |     2
        2 |  8 |     4
        2 |  9 |     8
        3 |  1 |     6
    """
    )

    if public:
        res = t.forget(pw.this.t, 2)
    else:
        res = t._forget(
            pw.this.t + 2,
            pw.this.t,
            mark_forgetting_records=False,
        )
    expected = pw.debug.table_from_markdown(
        """
        a |  t | __time__ | __diff__
        1 |  2 |     2    |     1
        1 |  3 |     2    |     1
        1 |  5 |     4    |     1
        1 |  2 |     4    |    -1
        1 |  3 |     6    |    -1
        1 |  5 |     6    |    -1
        1 | 15 |     8    |     1
        2 |  4 |     2    |     1
        2 |  8 |     4    |     1
        2 |  4 |     6    |    -1
        2 |  9 |     8    |     1
        2 |  8 |    10    |    -1
        2 |  9 |    10    |    -1
    """
    )
    assert_stream_equality_wo_index(res, expected)


def test_forget_non_append_only():
    t = pw.debug.table_from_markdown(
        """
          | x | __time__ | __diff__
        1 | 1 |     2    |     1
        2 | 5 |     4    |     1
        3 | 5 |     6    |     1
        1 | 1 |     8    |    -1
        4 | 2 |    10    |     1
        4 | 2 |    12    |    -1
        3 | 5 |    14    |    -1
        5 | 8 |    16    |     1
        6 | 8 |    18    |     1
    """
    )

    res = t._forget(pw.this.x + 2, pw.this.x, mark_forgetting_records=False)
    expected = pw.debug.table_from_markdown(
        """
          | x | __time__ | __diff__
        1 | 1 |     2    |     1
        2 | 5 |     4    |     1
        3 | 5 |     6    |     1
        1 | 1 |     6    |    -1
        3 | 5 |    14    |    -1
        5 | 8 |    16    |     1
        6 | 8 |    18    |     1
        2 | 5 |    18    |    -1
    """
    )
    assert_stream_equality_wo_index(res, expected)


def test_buffer_instance():
    t = pw.debug.table_from_markdown(
        """
        a |  t | __time__
        1 |  2 |     2
        1 |  3 |     2
        1 |  5 |     4
        1 | 15 |     8
        1 |  7 |    10
        2 |  4 |     2
        2 |  8 |     4
        2 |  9 |     8
        3 |  1 |     6
    """
    )

    res = t._buffer(
        pw.this.t + 2,
        pw.this.t,
        instance_column=pw.this.a,
    )
    expected = pw.debug.table_from_markdown(
        """
        a |  t | __time__
        1 |  2 |     4
        1 |  3 |     4
        1 |  5 |     8
        1 | 15 |   18446744073709551614
        1 |  7 |    10
        2 |  4 |     4
        2 |  8 |   18446744073709551614
        2 |  9 |   18446744073709551614
        3 |  1 |   18446744073709551614
    """
    )
    assert_stream_equality_wo_index(res, expected)


@pytest.mark.parametrize("public", [True, False])
def test_buffer_no_instance(public: bool):
    t = pw.debug.table_from_markdown(
        """
        a |  t | __time__
        1 |  2 |     2
        1 |  3 |     2
        1 |  5 |     4
        1 | 15 |     8
        1 |  7 |    10
        2 |  4 |     2
        2 |  8 |     4
        2 |  9 |     8
        3 |  1 |     6
    """
    )
    if public:
        res = t.buffer(pw.this.t, 2)
    else:
        res = t._buffer(
            pw.this.t + 2,
            pw.this.t,
        )
    expected = pw.debug.table_from_markdown(
        """
        a |  t | __time__
        1 |  2 |     2
        1 |  3 |     4
        1 |  5 |     4
        1 | 15 |   18446744073709551614
        1 |  7 |    10
        2 |  4 |     4
        2 |  8 |     8
        2 |  9 |     8
        3 |  1 |     6
    """
    )
    assert_stream_equality_wo_index(res, expected)


def test_buffer_non_append_only():
    t = pw.debug.table_from_markdown(
        """
          | x | __time__ | __diff__
        1 | 1 |     2    |     1
        2 | 5 |     4    |     1
        3 | 5 |     6    |     1
        1 | 1 |     8    |    -1
        4 | 2 |    10    |     1
        4 | 2 |    12    |    -1
        3 | 5 |    14    |    -1
        5 | 8 |    16    |     1
        6 | 8 |    18    |     1
    """
    )

    res = t.buffer(pw.this.x, 2)
    expected = pw.debug.table_from_markdown(
        """
          | x |             __time__ | __diff__
        1 | 1 |                    4 |     1
        1 | 1 |                    8 |    -1
        4 | 2 |                   10 |     1
        4 | 2 |                   12 |    -1
        2 | 5 |                   16 |     1
        5 | 8 | 18446744073709551614 |     1
        6 | 8 | 18446744073709551614 |     1
    """
    )
    assert_stream_equality_wo_index(res, expected)


def test_freeze_instance():
    t = pw.debug.table_from_markdown(
        """
        a |  t | __time__
        1 |  2 |     2
        1 |  3 |     2
        1 |  5 |     6
        1 | 15 |     8
        1 |  7 |    10
        2 |  4 |     2
        2 |  8 |     4
        2 |  5 |     8
        3 |  1 |     6
    """
    )

    res = t._freeze(
        pw.this.t + 2,
        pw.this.t,
        instance_column=pw.this.a,
    )
    expected = pw.debug.table_from_markdown(
        """
        a |  t | __time__
        1 |  2 |     2
        1 |  3 |     2
        1 |  5 |     6
        1 | 15 |     8
        2 |  4 |     2
        2 |  8 |     4
        3 |  1 |     6
    """
    )
    assert_stream_equality_wo_index(res, expected)


@pytest.mark.parametrize("public", [True, False])
def test_freeze_no_instance(public: bool):
    t = pw.debug.table_from_markdown(
        """
        a |  t | __time__
        1 |  2 |     2
        1 |  3 |     2
        1 |  5 |     6
        1 | 15 |     8
        1 |  7 |    10
        2 |  4 |     2
        2 |  8 |     4
        2 |  5 |     8
        3 |  1 |     6
    """
    )

    if public:
        res = t.ignore_late(pw.this.t, 2)
    else:
        res = t._freeze(
            pw.this.t + 2,
            pw.this.t,
        )
    expected = pw.debug.table_from_markdown(
        """
        a |  t | __time__
        1 |  2 |     2
        1 |  3 |     2
        1 | 15 |     8
        2 |  4 |     2
        2 |  8 |     4
    """
    )
    assert_stream_equality_wo_index(res, expected)
