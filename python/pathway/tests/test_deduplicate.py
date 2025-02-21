# Copyright © 2024 Pathway

import pathlib
from unittest import mock

import pytest

import pathway as pw
from pathway.internals.parse_graph import G
from pathway.tests.utils import assert_stream_equality_wo_index, run


def test_deduplicate_keeps_state(tmp_path: pathlib.Path):
    persistence_path = tmp_path / "persistence"
    persistence_config = pw.persistence.Config(
        pw.persistence.Backend.filesystem(persistence_path)
    )
    data_1 = """
    val | __time__
     1  |     2
     2  |     4
     3  |     6
     4  |     8
     5  |    10
     6  |    12
     7  |    14
     8  |    16
     9  |    16
    10  |    16
    12  |    18
    13  |    20
    """
    data_2 = """
    val | __time__
     1  |     0
     2  |     0
     3  |     0
     4  |     0
     5  |     0
     6  |     0
     7  |     0
     8  |     0
     9  |     0
    10  |     0
    12  |     0
    13  |     0
    14  |    22
    15  |    24
    16  |    26
    17  |    28
    18  |    30
    """
    # values with __time__ == 0 simulate the persistence behavior from a regular connector

    def acceptor(new_value, old_value) -> bool:
        return new_value >= old_value + 2

    table = pw.debug.table_from_markdown(data_1)
    result = table.deduplicate(value=pw.this.val, acceptor=acceptor)

    expected_1 = pw.debug.table_from_markdown(
        """
    id | val | __time__ | __diff__
     1 |  1  |     2    |     1
     1 |  1  |     6    |    -1
     1 |  3  |     6    |     1
     1 |  3  |    10    |    -1
     1 |  5  |    10    |     1
     1 |  5  |    14    |    -1
     1 |  7  |    14    |     1
     1 |  7  |    16    |    -1
     1 |  9  |    16    |     1
     1 |  9  |    18    |    -1
     1 | 12  |    18    |     1
    """
    )
    assert_stream_equality_wo_index(
        result, expected_1, persistence_config=persistence_config
    )
    G.clear()

    table = pw.debug.table_from_markdown(data_2)
    result = table.deduplicate(value=pw.this.val, acceptor=acceptor)

    expected_2 = pw.debug.table_from_markdown(
        """
    id | val | __time__ | __diff__
     1 | 12  |     0    |     1
     1 | 12  |    22    |    -1
     1 | 14  |    22    |     1
     1 | 14  |    26    |    -1
     1 | 16  |    26    |     1
     1 | 16  |    30    |    -1
     1 | 18  |    30    |     1
    """
    )
    assert_stream_equality_wo_index(
        result, expected_2, persistence_config=persistence_config
    )


def test_deduplicate_keeps_state_after_two_restarts(tmp_path: pathlib.Path):
    persistence_path = tmp_path / "persistence"
    persistence_config = pw.persistence.Config(
        pw.persistence.Backend.filesystem(persistence_path)
    )
    data_1 = """
    val | __time__
     1  |     2
     2  |     4
     3  |     6
     4  |     8
     5  |    10
     6  |    12
     7  |    14
     8  |    16
     9  |    16
    10  |    16
    12  |    18
    13  |    20
    """
    data_2 = """
    val | __time__
     1  |     0
     2  |     0
     3  |     0
     4  |     0
     5  |     0
     6  |     0
     7  |     0
     8  |     0
     9  |     0
    10  |     0
    12  |     0
    13  |     0
    14  |    22
    15  |    24
    16  |    26
    """
    # values with __time__ == 0 simulate the persistence behavior from a regular connector
    data_3 = """
    val | __time__
     1  |     0
     2  |     0
     3  |     0
     4  |     0
     5  |     0
     6  |     0
     7  |     0
     8  |     0
     9  |     0
    10  |     0
    12  |     0
    13  |     0
    14  |     0
    15  |     0
    16  |     0
    17  |    28
    18  |    30
    """

    def acceptor(new_value, old_value) -> bool:
        return new_value >= old_value + 2

    table = pw.debug.table_from_markdown(data_1)
    result = table.deduplicate(value=pw.this.val, acceptor=acceptor)

    expected_1 = pw.debug.table_from_markdown(
        """
    id | val | __time__ | __diff__
     1 |  1  |     2    |     1
     1 |  1  |     6    |    -1
     1 |  3  |     6    |     1
     1 |  3  |    10    |    -1
     1 |  5  |    10    |     1
     1 |  5  |    14    |    -1
     1 |  7  |    14    |     1
     1 |  7  |    16    |    -1
     1 |  9  |    16    |     1
     1 |  9  |    18    |    -1
     1 | 12  |    18    |     1
    """
    )
    assert_stream_equality_wo_index(
        result, expected_1, persistence_config=persistence_config
    )
    G.clear()

    table = pw.debug.table_from_markdown(data_2)
    result = table.deduplicate(value=pw.this.val, acceptor=acceptor)

    expected_2 = pw.debug.table_from_markdown(
        """
    id | val | __time__ | __diff__
     1 | 12  |     0    |     1
     1 | 12  |    22    |    -1
     1 | 14  |    22    |     1
     1 | 14  |    26    |    -1
     1 | 16  |    26    |     1
    """
    )
    assert_stream_equality_wo_index(
        result, expected_2, persistence_config=persistence_config
    )

    G.clear()

    table = pw.debug.table_from_markdown(data_3)
    result = table.deduplicate(value=pw.this.val, acceptor=acceptor)

    expected_3 = pw.debug.table_from_markdown(
        """
    id | val | __time__ | __diff__
     1 | 16  |     0    |     1
     1 | 16  |    30    |    -1
     1 | 18  |    30    |     1
    """
    )
    assert_stream_equality_wo_index(
        result, expected_3, persistence_config=persistence_config
    )


def test_deduplicate_with_instance_keeps_state(tmp_path: pathlib.Path):
    persistence_path = tmp_path / "persistence"
    persistence_config = pw.persistence.Config(
        pw.persistence.Backend.filesystem(persistence_path)
    )
    data_1 = """
    val | instance | __time__
     1  |     1    |     2
     2  |     2    |     4
     3  |     1    |     6
     4  |     1    |     8
     5  |     1    |     8
     6  |     2    |    10
     6  |     1    |    12
    """
    data_2 = """
    val | instance | __time__
     1  |     1    |     0
     2  |     2    |     0
     3  |     1    |     0
     4  |     1    |     0
     5  |     1    |     0
     6  |     2    |     0
     6  |     1    |     0
    20  |     1    |    16
    13  |     2    |    18
    18  |     1    |    20
    24  |     1    |    22
    """
    # values with __time__ == 0 simulate the persistence behavior from a regular connector

    def acceptor(new_value, old_value) -> bool:
        return new_value >= old_value + 3

    table = pw.debug.table_from_markdown(data_1)
    result = table.deduplicate(
        value=pw.this.val, instance=pw.this.instance, acceptor=acceptor
    )

    expected_1 = pw.debug.table_from_markdown(
        """
    id | val | instance | __time__ | __diff__
     1 |  1  |     1    |     2    |     1
     2 |  2  |     2    |     4    |     1
     1 |  1  |     1    |     8    |    -1
     1 |  4  |     1    |     8    |     1
     2 |  2  |     2    |    10    |    -1
     2 |  6  |     2    |    10    |     1
    """
    )
    assert_stream_equality_wo_index(
        result, expected_1, persistence_config=persistence_config
    )
    G.clear()

    table = pw.debug.table_from_markdown(data_2)
    result = table.deduplicate(
        value=pw.this.val, instance=pw.this.instance, acceptor=acceptor
    )

    expected_2 = pw.debug.table_from_markdown(
        """
    id | val | instance | __time__ | __diff__
     1 |  4  |     1    |     0    |     1
     2 |  6  |     2    |     0    |     1
     1 |  4  |     1    |    16    |    -1
     1 | 20  |     1    |    16    |     1
     2 |  6  |     2    |    18    |    -1
     2 | 13  |     2    |    18    |     1
     1 | 20  |     1    |    22    |    -1
     1 | 24  |     1    |    22    |     1
    """
    )
    assert_stream_equality_wo_index(
        result, expected_2, persistence_config=persistence_config
    )


def test_deduplicate_keeps_state_after_code_change(tmp_path: pathlib.Path):
    persistence_path = tmp_path / "persistence"
    persistence_config = pw.persistence.Config(
        pw.persistence.Backend.filesystem(persistence_path)
    )
    data_1 = """
    val | __time__
     1  |     2
     2  |     4
     3  |     6
     4  |     8
    """
    data_2 = """
    val | __time__
     1  |     0
     2  |     0
     3  |     0
     4  |     0
     5  |    10
     6  |    12
     7  |    14
     8  |    16
    """
    # values with __time__ == 0 simulate the persistence behavior from a regular connector

    def acceptor_1(new_value, old_value) -> bool:
        return new_value >= old_value + 2

    table = pw.debug.table_from_markdown(data_1)
    result = table.deduplicate(value=pw.this.val, acceptor=acceptor_1)

    expected_1 = pw.debug.table_from_markdown(
        """
    id | val | __time__ | __diff__
     1 |  1  |     2    |     1
     1 |  1  |     6    |    -1
     1 |  3  |     6    |     1
    """
    )
    assert_stream_equality_wo_index(
        result, expected_1, persistence_config=persistence_config
    )
    G.clear()

    def acceptor_2(new_value, old_value) -> bool:
        return (
            new_value >= old_value + 4
        )  # note that the required offset is now 4 and it was 2

    table = pw.debug.table_from_markdown(data_2)
    result = table.deduplicate(value=pw.this.val, acceptor=acceptor_2)

    expected_2 = pw.debug.table_from_markdown(
        """
    id | val | __time__ | __diff__
     1 |  3  |     0    |     1
     1 |  3  |    14    |    -1
     1 |  7  |    14    |     1
    """
    )
    assert_stream_equality_wo_index(
        result, expected_2, persistence_config=persistence_config
    )


@pytest.mark.flaky(reruns=2)
def test_deduplicate_keeps_state_with_regular_persistence(tmp_path: pathlib.Path):
    persistence_path = tmp_path / "persistence"
    persistence_config = pw.persistence.Config(
        pw.persistence.Backend.filesystem(persistence_path)
    )

    def run_computation(nb_rows: int, offset: int, expected: list[int]):
        G.clear()

        def acceptor(new_value, old_value) -> bool:
            return new_value >= old_value + 2

        table = pw.demo.range_stream(
            nb_rows, offset=offset, input_rate=25, autocommit_duration_ms=10
        )
        result = table.deduplicate(value=pw.this.value, acceptor=acceptor)

        emit = mock.Mock()

        def on_change(key: pw.Pointer, row: dict, time: int, is_addition: bool):
            if is_addition:
                emit(row["value"])

        pw.io.subscribe(result, on_change)
        run(persistence_config=persistence_config)

        emit.assert_has_calls([mock.call(i) for i in expected])

    run_computation(6, 0, [0, 2, 4])
    run_computation(5, 6, [6, 8, 10])


def test_selective_persistence_name_set(
    tmp_path: pathlib.Path,
):
    persistence_path = tmp_path / "persistence"
    persistence_config = pw.persistence.Config(
        pw.persistence.Backend.filesystem(persistence_path),
        persistence_mode=pw.PersistenceMode.SELECTIVE_PERSISTING,
    )
    data_1 = """
    val | __time__
     1  |     2
     2  |     4
     3  |     6
     4  |     8
     5  |    10
    """
    data_2 = """
    val | __time__
     1  |     2
     2  |     4
     3  |     6
     4  |     8
     5  |    10
     6  |    12
     7  |    14
     8  |    16
     9  |    16
    """

    def acceptor(new_value, old_value) -> bool:
        return new_value >= old_value + 2

    table = pw.debug.table_from_markdown(data_1)
    result = table.deduplicate(value=pw.this.val, acceptor=acceptor, name="foo")

    expected_1 = pw.debug.table_from_markdown(
        """
    id | val | __time__ | __diff__
     1 |  1  |     2    |     1
     1 |  1  |     6    |    -1
     1 |  3  |     6    |     1
     1 |  3  |    10    |    -1
     1 |  5  |    10    |     1
    """
    )
    assert_stream_equality_wo_index(
        result, expected_1, persistence_config=persistence_config
    )
    G.clear()

    table = pw.debug.table_from_markdown(data_2)
    result = table.deduplicate(value=pw.this.val, acceptor=acceptor, name="foo")

    expected_2 = pw.debug.table_from_markdown(
        """
    id | val | __time__ | __diff__
     1 |  5  |     0    |     1
     1 |  5  |    14    |    -1
     1 |  7  |    14    |     1
     1 |  7  |    16    |    -1
     1 |  9  |    16    |     1
    """
    )
    assert_stream_equality_wo_index(
        result, expected_2, persistence_config=persistence_config
    )


@pytest.mark.parametrize(
    "first_id,second_id", [(None, None), ("foo", "bar"), (None, "foo"), ("bar", None)]
)
def test_selective_persistence_no_name_set_or_different_names_set(
    tmp_path: pathlib.Path,
    first_id: str | None,
    second_id: str | None,
):
    persistence_path = tmp_path / "persistence"
    persistence_config = pw.persistence.Config(
        pw.persistence.Backend.filesystem(persistence_path),
        persistence_mode=pw.PersistenceMode.SELECTIVE_PERSISTING,
    )
    data_1 = """
    val | __time__
     1  |     2
     2  |     4
     3  |     6
     4  |     8
     5  |    10
    """
    data_2 = """
    val | __time__
     1  |     2
     2  |     4
     3  |     6
     4  |     8
     5  |    10
     6  |    12
     7  |    14
     8  |    16
     9  |    16
    """

    def acceptor(new_value, old_value) -> bool:
        return new_value >= old_value + 2

    table = pw.debug.table_from_markdown(data_1)
    result = table.deduplicate(value=pw.this.val, acceptor=acceptor, name=first_id)

    expected_1 = pw.debug.table_from_markdown(
        """
    id | val | __time__ | __diff__
     1 |  1  |     2    |     1
     1 |  1  |     6    |    -1
     1 |  3  |     6    |     1
     1 |  3  |    10    |    -1
     1 |  5  |    10    |     1
    """
    )
    assert_stream_equality_wo_index(
        result, expected_1, persistence_config=persistence_config
    )
    G.clear()

    table = pw.debug.table_from_markdown(data_2)
    result = table.deduplicate(value=pw.this.val, acceptor=acceptor, name=second_id)

    expected_2 = pw.debug.table_from_markdown(
        """
    id | val | __time__ | __diff__
     1 |  1  |     2    |     1
     1 |  1  |     6    |    -1
     1 |  3  |     6    |     1
     1 |  3  |    10    |    -1
     1 |  5  |    10    |     1
     1 |  5  |    14    |    -1
     1 |  7  |    14    |     1
     1 |  7  |    16    |    -1
     1 |  9  |    16    |     1
    """
    )
    assert_stream_equality_wo_index(
        result, expected_2, persistence_config=persistence_config
    )


def test_deduplicate_python_tuple():
    t = pw.debug.table_from_markdown(
        """
        a | b | __time__
        1 | 1 |     2
        1 | 2 |     4
        3 | 1 |     6
        3 | 0 |     8
        4 | 2 |    10
        4 | 2 |    12
        4 | 1 |    14
    """
    )

    def acceptor(new_value: tuple, old_value: tuple) -> bool:
        return new_value > old_value

    res = t.deduplicate(value=(pw.this.a, pw.this.b), acceptor=acceptor)
    expected = pw.debug.table_from_markdown(
        """
        a | b | __time__ | __diff__
        1 | 1 |     2    |     1
        1 | 1 |     4    |    -1
        1 | 2 |     4    |     1
        1 | 2 |     6    |    -1
        3 | 1 |     6    |     1
        3 | 1 |    10    |    -1
        4 | 2 |    10    |     1
    """
    )
    assert_stream_equality_wo_index(res, expected)
