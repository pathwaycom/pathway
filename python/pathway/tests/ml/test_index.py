from typing import Any

import numpy as np
import pandas as pd

import pathway as pw
from pathway.stdlib.ml.index import KNNIndex
from pathway.tests.utils import T, assert_table_equality_wo_index


class PointSchema(pw.Schema):
    coords: Any
    is_query: bool


def sort_arrays(arrays: list[np.ndarray]) -> list[tuple[int, ...]]:
    return sorted([tuple(array) for array in arrays])


def get_points() -> list[tuple[tuple[int, ...], bool]]:
    points = [
        (2, 2, 0),
        (3, -2, 0),
        (0, 0, 1),
        (-1, 0, 0),
        (2, -2, 1),
        (1, 2, 0),
        (-1, 1, 1),
        (-3, 1, 0),
        (-2, -3, 1),
        (1, -4, 0),
    ]
    return [(point[:-1], point[-1] == 1) for point in points]


def nn_as_table(
    to_table: list[tuple[tuple[int, ...], tuple[tuple[int, ...]]]]
) -> pw.Table:
    return pw.debug.table_from_pandas(
        pd.DataFrame(
            {
                "coords": [point[0] for point in to_table],
                "nn": [point[1] for point in to_table],
            }
        )
    ).update_types(nn=list[tuple[int, ...]])


def test_all_at_once():
    data = get_points()
    df = pd.DataFrame(
        {
            "coords": [point[0] for point in data],
            "is_query": [point[1] for point in data],
        }
    )
    table = pw.debug.table_from_pandas(df)
    points = table.filter(~pw.this.is_query).without(pw.this.is_query)
    queries = table.filter(pw.this.is_query).without(pw.this.is_query)
    index = KNNIndex(points.coords, points, n_dimensions=2, n_and=5)
    result = queries + index.get_nearest_items(queries.coords, k=2).select(
        nn=pw.apply(sort_arrays, pw.this.coords)
    )
    expected = nn_as_table(
        [
            ((0, 0), ((-1, 0), (1, 2))),
            ((2, -2), ((1, -4), (3, -2))),
            ((-1, 1), ((-3, 1), (-1, 0))),
            ((-2, -3), ((-1, 0), (1, -4))),
        ]
    )
    assert_table_equality_wo_index(result, expected)


def stream_points(with_k: bool = False) -> tuple[pw.Table, pw.Table]:
    points = T(
        """
         x |  y | __time__
         2 |  2 |     1
         3 | -2 |     2
        -1 |  0 |     4
         1 |  2 |     6
        -3 |  1 |     8
         1 | -4 |    10
    """
    ).select(coords=pw.make_tuple(pw.this.x, pw.this.y))
    queries = T(
        """
         x |  y | k | __time__
         0 |  0 | 1 |     3
         2 | -2 | 2 |     5
        -1 |  1 | 3 |     7
        -2 | -3 | 0 |     9
    """
    ).select(coords=pw.make_tuple(pw.this.x, pw.this.y), k=pw.this.k)
    if not with_k:
        queries = queries.without(pw.this.k)
    return points, queries


def test_update_old():
    points, queries = stream_points()
    index = KNNIndex(points.coords, points, n_dimensions=2, n_and=5)
    result = queries + index.get_nearest_items(queries.coords, k=2).select(
        nn=pw.apply(sort_arrays, pw.this.coords)
    )
    expected = nn_as_table(
        [
            ((0, 0), ((-1, 0), (1, 2))),
            ((2, -2), ((1, -4), (3, -2))),
            ((-1, 1), ((-3, 1), (-1, 0))),
            ((-2, -3), ((-1, 0), (1, -4))),
        ]
    )
    assert_table_equality_wo_index(result, expected)


def test_asof_now():
    points, queries = stream_points()
    index = KNNIndex(points.coords, points, n_dimensions=2, n_and=5)
    result = queries + index.get_nearest_items_asof_now(queries.coords, k=2).select(
        nn=pw.apply(sort_arrays, pw.this.coords)
    )
    expected = nn_as_table(
        [
            ((0, 0), ((2, 2), (3, -2))),
            ((2, -2), ((-1, 0), (3, -2))),
            ((-1, 1), ((-1, 0), (1, 2))),
            ((-2, -3), ((-3, 1), (-1, 0))),
        ]
    )
    assert_table_equality_wo_index(result, expected)


def test_update_old_with_variable_k():
    points, queries = stream_points(with_k=True)
    index = KNNIndex(points.coords, points, n_dimensions=2, n_and=5)
    result = queries.without(pw.this.k) + index.get_nearest_items(
        queries.coords, queries.k
    ).with_universe_of(queries).select(nn=pw.apply(sort_arrays, pw.this.coords))
    expected = nn_as_table(
        [
            ((0, 0), ((-1, 0),)),
            ((2, -2), ((1, -4), (3, -2))),
            ((-1, 1), ((-3, 1), (-1, 0), (1, 2))),
            ((-2, -3), ()),
        ]
    )
    assert_table_equality_wo_index(result, expected)


def test_asof_now_with_variable_k():
    points, queries = stream_points(with_k=True)
    index = KNNIndex(points.coords, points, n_dimensions=2, n_and=5)
    result = queries.without(pw.this.k) + index.get_nearest_items_asof_now(
        queries.coords, queries.k
    ).select(nn=pw.apply(sort_arrays, pw.this.coords))
    expected = nn_as_table(
        [
            ((0, 0), ((2, 2),)),
            ((2, -2), ((-1, 0), (3, -2))),
            ((-1, 1), ((-1, 0), (1, 2), (2, 2))),
            ((-2, -3), ()),
        ]
    )
    assert_table_equality_wo_index(result, expected)
