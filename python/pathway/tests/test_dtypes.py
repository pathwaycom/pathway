# Copyright © 2024 Pathway

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
