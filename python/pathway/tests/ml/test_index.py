import os

import numpy as np
import pandas as pd
import pytest

import pathway as pw
from pathway.stdlib.ml.index import KNNIndex
from pathway.tests.utils import (
    assert_table_equality_wo_index,
    assert_values_in_stream_consistent,
)


class PointSchema(pw.Schema):
    coords: np.ndarray
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


def test_all_at_once():
    data = get_points()
    df = pd.DataFrame(
        {
            "coords": [np.array(point[0]) for point in data],
            "is_query": [point[1] for point in data],
        }
    )
    table = pw.debug.table_from_pandas(df)
    points = table.filter(~pw.this.is_query).without(pw.this.is_query)
    queries = table.filter(pw.this.is_query).without(pw.this.is_query)
    index = KNNIndex(points.coords, points, n_dimensions=2, n_and=5)
    result = queries + index.get_nearest_items(queries.coords, k=2).with_universe_of(
        queries
    ).select(nn=pw.apply(sort_arrays, pw.this.coords))
    expected = [
        ((0, 0), ((-1, 0), (1, 2))),
        ((2, -2), ((1, -4), (3, -2))),
        ((-1, 1), ((-3, 1), (-1, 0))),
        ((-2, -3), ((-1, 0), (1, -4))),
    ]
    expected_pw = pw.debug.table_from_pandas(
        pd.DataFrame(
            {
                "coords": [np.array(point[0]) for point in expected],
                "nn": [point[1] for point in expected],
            }
        )
    )
    assert_table_equality_wo_index(result, expected_pw)


def stream_points() -> tuple[pw.Table, pw.Table]:
    """Returns (points, queries)."""
    points = get_points()
    value_functions = {
        "coords": lambda i: points[i][0],
        "is_query": lambda i: points[i][1],
    }

    table = pw.demo.generate_custom_stream(
        value_functions,
        schema=PointSchema,
        nb_rows=10,
        autocommit_duration_ms=20,
        input_rate=10,
    )
    return (
        table.filter(~pw.this.is_query).without(pw.this.is_query),
        table.filter(pw.this.is_query).without(pw.this.is_query),
    )


def test_update_old():
    points, queries = stream_points()
    index = KNNIndex(points.coords, points, n_dimensions=2, n_and=5)
    result = queries + index.get_nearest_items(queries.coords, k=2).with_universe_of(
        queries
    ).select(nn=pw.apply(sort_arrays, pw.this.coords))
    expected = [
        ((0, 0), ((-1, 0), (1, 2))),
        ((2, -2), ((1, -4), (3, -2))),
        ((-1, 1), ((-3, 1), (-1, 0))),
        ((-2, -3), ((-1, 0), (1, -4))),
    ]
    assert_values_in_stream_consistent(result, expected)


@pytest.mark.xfail(reason="data isn't split into batches as expected")
def test_asof_now():
    if (
        os.getenv("PATHWAY_THREADS") is not None
        and int(os.getenv("PATHWAY_THREADS")) != 1  # type: ignore[arg-type]
    ):
        pytest.xfail(reason="Order changes when multiple threads are used.")
        # FIXME: waits for proper tool for streaming tests
    points, queries = stream_points()
    index = KNNIndex(points.coords, points, n_dimensions=2, n_and=5)
    result = queries + index.get_nearest_items_asof_now(queries.coords, k=2).select(
        nn=pw.apply(sort_arrays, pw.this.coords)
    )
    expected = [
        ((0, 0), ((2, 2), (3, -2))),
        ((2, -2), ((-1, 0), (3, -2))),
        ((-1, 1), ((-1, 0), (1, 2))),
        ((-2, -3), ((-3, 1), (-1, 0))),
    ]
    assert_values_in_stream_consistent(result, expected)
