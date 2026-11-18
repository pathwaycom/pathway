# Copyright © 2026 Pathway

import typing

import numpy as np
import numpy.typing as npt

import pathway.internals.dtype as dt


def test_identities():
    assert dt.Optional(dt.INT) is dt.Optional(dt.INT)
    assert dt.Pointer() is dt.Pointer()
    assert dt.Tuple(dt.INT, dt.Optional(dt.ANY_POINTER)) is dt.Tuple(
        dt.INT, dt.Optional(dt.ANY_POINTER)
    )
    assert dt.Tuple(dt.INT, ...) is dt.List(dt.INT)
    assert dt.Optional(dt.ANY) is dt.ANY
    assert dt.Optional(dt.Optional(dt.INT)) is dt.Optional(dt.INT)
    assert dt.Array(2, dt.Array(2, dt.INT)) is dt.Array(4, dt.INT)


def test_wrap_ndarray_annotations():
    # must hold on every numpy version: < 2.1 (NDArray shape is Any),
    # 2.1 - 2.4 (shape is a variadic tuple), >= 2.5 (NDArray is a lazy alias)
    assert dt.wrap(np.ndarray) is dt.ANY_ARRAY
    assert dt.wrap(npt.NDArray) is dt.ANY_ARRAY
    assert dt.wrap(npt.NDArray[typing.Any]) is dt.ANY_ARRAY
    assert dt.wrap(npt.NDArray[np.float64]) is dt.Array(n_dim=None, wrapped=dt.FLOAT)
    assert dt.wrap(npt.NDArray[np.int64]) is dt.Array(n_dim=None, wrapped=dt.INT)
    assert dt.wrap(np.ndarray[typing.Any, np.dtype[np.float64]]) is dt.Array(
        n_dim=None, wrapped=dt.FLOAT
    )
    assert dt.wrap(np.ndarray[tuple[int, ...], np.dtype[np.float64]]) is dt.Array(
        n_dim=None, wrapped=dt.FLOAT
    )
    assert dt.wrap(np.ndarray[tuple[int, int], np.dtype[np.int64]]) is dt.Array(
        n_dim=2, wrapped=dt.INT
    )
    assert dt.wrap(dt.ANY_ARRAY.typehint) is dt.ANY_ARRAY
    assert dt.wrap(dt.Array(n_dim=None, wrapped=dt.FLOAT).typehint) is dt.Array(
        n_dim=None, wrapped=dt.FLOAT
    )
