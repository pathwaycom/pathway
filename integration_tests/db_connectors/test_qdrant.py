# Copyright © 2026 Pathway

import pytest
from utils import QDRANT_VECTOR_DIM, QdrantContext

import pathway as pw
from pathway.internals.parse_graph import G
from pathway.tests.utils import run

_STREAMING_ROWS = [
    {"id": 1, "vector": [1.0, 2.0, 3.0], "title": "a"},
    {"id": 2, "vector": [4.0, 5.0, 6.0], "title": "b"},
    {"id": 3, "vector": [7.0, 8.0, 9.0], "title": "c"},
]


class _InputSchema(pw.Schema):
    id: int = pw.column_definition(primary_key=True)
    vector: list[float]
    title: str


def _write_table(
    qdrant: QdrantContext, collection_name: str, rows, *, is_stream: bool = False
) -> None:
    G.clear()
    table = pw.debug.table_from_rows(_InputSchema, rows, is_stream=is_stream)
    pw.io.qdrant.write(
        table,
        url=qdrant.url,
        collection_name=collection_name,
        vector=table.vector,
    )
    run()


def test_write_basic(qdrant):
    collection_name = qdrant.generate_collection_name()
    qdrant.create_collection(collection_name)

    rows = [
        (1, [1.0, 2.0, 3.0], "a"),
        (2, [4.0, 5.0, 6.0], "b"),
        (3, [7.0, 8.0, 9.0], "c"),
    ]
    _write_table(qdrant, collection_name, rows)

    result = qdrant.query_all(collection_name)
    result.sort(key=lambda r: r["id"])
    assert result == [
        {"id": id_, "vector": vec, "title": title} for id_, vec, title in rows
    ]


def test_write_upsert(qdrant):
    """Rows with an existing primary key should be replaced, not duplicated."""
    collection_name = qdrant.generate_collection_name()
    qdrant.create_collection(collection_name)

    _write_table(
        qdrant, collection_name, [(1, [1.0, 2.0, 3.0], "a"), (2, [4.0, 5.0, 6.0], "b")]
    )
    _write_table(
        qdrant, collection_name, [(1, [9.0, 8.0, 7.0], "z"), (2, [4.0, 5.0, 6.0], "b")]
    )

    result = qdrant.query_all(collection_name)
    result.sort(key=lambda r: r["id"])
    assert result == [
        {"id": 1, "vector": [9.0, 8.0, 7.0], "title": "z"},
        {"id": 2, "vector": [4.0, 5.0, 6.0], "title": "b"},
    ]


def test_write_delete(qdrant):
    """Rows with diff=-1 should be removed from the collection."""
    collection_name = qdrant.generate_collection_name()
    qdrant.create_collection(collection_name)

    # Insert points 1 and 2 at time=2, then delete point 1 at time=4.
    # table_from_rows with is_stream=True expects (...cols..., time, diff) tuples.
    rows = [
        (1, [1.0, 2.0, 3.0], "a", 2, 1),
        (2, [4.0, 5.0, 6.0], "b", 2, 1),
        (1, [1.0, 2.0, 3.0], "a", 4, -1),
    ]
    _write_table(qdrant, collection_name, rows, is_stream=True)

    result = qdrant.query_all(collection_name)
    assert len(result) == 1
    assert result[0]["id"] == 2


def test_write_update_larger_than_batch_size(qdrant):
    """Updating more keys in one minibatch than ``batch_size`` must keep every
    updated point.

    An update arrives as a retraction (``diff=-1``) of the old row followed by an
    insertion (``diff=+1``) of the new row, both with the same primary key. The
    connector promises that within a minibatch the insertion always wins over the
    deletion, so after the update every key must still be present, carrying the
    new payload — never dropped.
    """
    collection_name = qdrant.generate_collection_name()
    qdrant.create_collection(collection_name)

    n = 300  # > default batch_size (256)

    # time=2: insert n points; time=4: update all n (delete old + insert new),
    # all within a single minibatch.
    rows = [(i, [float(i), float(i + 1), float(i + 2)], "old", 2, 1) for i in range(n)]
    rows += [
        (i, [float(i), float(i + 1), float(i + 2)], "old", 4, -1) for i in range(n)
    ]
    rows += [(i, [float(i), float(i + 1), float(i + 2)], "new", 4, 1) for i in range(n)]
    _write_table(qdrant, collection_name, rows, is_stream=True)

    assert qdrant.count(collection_name) == n
    result = qdrant.query_all(collection_name, with_vectors=False)
    assert sorted(r["id"] for r in result) == list(range(n))
    assert all(r["title"] == "new" for r in result)


def test_write_empty(qdrant):
    """Writing an empty table should succeed without errors."""
    collection_name = qdrant.generate_collection_name()
    qdrant.create_collection(collection_name)

    _write_table(qdrant, collection_name, [])

    assert qdrant.query_all(collection_name) == []


def test_write_large_batch(qdrant):
    """Writing many points at once should work correctly."""
    collection_name = qdrant.generate_collection_name()
    qdrant.create_collection(collection_name)

    n = 500
    rows = [(i, [float(i), float(i + 1), float(i + 2)], f"t{i}") for i in range(n)]
    _write_table(qdrant, collection_name, rows)

    assert qdrant.count(collection_name) == n


def test_write_creates_collection_when_missing(qdrant):
    """If the collection does not exist, the connector creates it on first write.

    The auto-created collection uses Cosine distance, under which Qdrant
    normalizes stored vectors, so this asserts on the payload and point count
    rather than on exact vector values.
    """
    collection_name = qdrant.generate_collection_name()
    # Intentionally do NOT create the collection up front.

    rows = [(1, [1.0, 2.0, 3.0], "a"), (2, [4.0, 5.0, 6.0], "b")]
    _write_table(qdrant, collection_name, rows)

    assert qdrant.count(collection_name) == 2
    result = qdrant.query_all(collection_name, with_vectors=False)
    result.sort(key=lambda r: r["id"])
    assert result == [{"id": 1, "title": "a"}, {"id": 2, "title": "b"}]


def test_write_small_batch_size_chunks(qdrant):
    """A small batch_size splits the minibatch into several requests; every
    point must still land in the collection exactly once."""
    collection_name = qdrant.generate_collection_name()
    qdrant.create_collection(collection_name)

    n = 50
    rows = [(i, [float(i), float(i + 1), float(i + 2)], f"t{i}") for i in range(n)]
    G.clear()
    table = pw.debug.table_from_rows(_InputSchema, rows)
    pw.io.qdrant.write(
        table,
        url=qdrant.url,
        collection_name=collection_name,
        vector=table.vector,
        batch_size=7,
    )
    run()

    assert qdrant.count(collection_name) == n


def test_write_invalid_batch_size_raises(qdrant):
    """A non-positive batch_size is rejected up front with a clear ValueError."""
    collection_name = qdrant.generate_collection_name()
    qdrant.create_collection(collection_name)

    G.clear()
    table = pw.debug.table_from_rows(_InputSchema, [(1, [1.0, 2.0, 3.0], "a")])
    with pytest.raises(ValueError, match="batch_size must be a positive integer"):
        pw.io.qdrant.write(
            table,
            url=qdrant.url,
            collection_name=collection_name,
            vector=table.vector,
            batch_size=0,
        )


def test_write_streaming(qdrant):
    """Rows emitted via a Python connector across multiple commits land in Qdrant."""
    collection_name = qdrant.generate_collection_name()
    qdrant.create_collection(collection_name)

    class VectorSubject(pw.io.python.ConnectorSubject):
        def run(self):
            for row in _STREAMING_ROWS:
                self.next_json(row)
                self.commit()

    G.clear()
    table = pw.io.python.read(VectorSubject(), schema=_InputSchema)
    pw.io.qdrant.write(
        table,
        url=qdrant.url,
        collection_name=collection_name,
        vector=table.vector,
    )
    run()

    result = qdrant.query_all(collection_name)
    result.sort(key=lambda r: r["id"])
    assert result == _STREAMING_ROWS


def test_write_dimension_mismatch_raises(qdrant):
    """Inserting a vector whose dimension differs from the collection should raise."""
    collection_name = qdrant.generate_collection_name()
    qdrant.create_collection(collection_name, dimension=QDRANT_VECTOR_DIM)

    wrong_dim_rows = [(1, [1.0, 2.0], "a")]  # dim=2, collection expects dim=3
    with pytest.raises(Exception):
        _write_table(qdrant, collection_name, wrong_dim_rows)


def test_write_vector_wrong_table_raises(qdrant):
    """Passing a vector column from a different table raises a clear ValueError."""
    collection_name = qdrant.generate_collection_name()
    qdrant.create_collection(collection_name)

    G.clear()
    table = pw.debug.table_from_rows(_InputSchema, [(1, [1.0, 2.0, 3.0], "a")])
    other = pw.debug.table_from_rows(_InputSchema, [(2, [4.0, 5.0, 6.0], "b")])
    with pytest.raises(ValueError, match="doesn't belong to the target table"):
        pw.io.qdrant.write(
            table,
            url=qdrant.url,
            collection_name=collection_name,
            vector=other.vector,
        )


def test_write_numpy_vector(qdrant):
    """A 1-D numpy.ndarray vector column is accepted and stored as a list."""
    import numpy as np

    class NumpySchema(pw.Schema):
        id: int = pw.column_definition(primary_key=True)
        vector: np.ndarray

    collection_name = qdrant.generate_collection_name()
    qdrant.create_collection(collection_name)

    G.clear()
    table = pw.debug.table_from_rows(
        NumpySchema,
        [
            (1, np.array([1.0, 2.0, 3.0])),
            (2, np.array([4.0, 5.0, 6.0])),
        ],
    )
    pw.io.qdrant.write(
        table,
        url=qdrant.url,
        collection_name=collection_name,
        vector=table.vector,
    )
    run()

    result = qdrant.query_all(collection_name)
    result.sort(key=lambda r: r["id"])
    assert result == [
        {"id": 1, "vector": [1.0, 2.0, 3.0]},
        {"id": 2, "vector": [4.0, 5.0, 6.0]},
    ]


def test_write_non_contiguous_numpy_vector(qdrant):
    """A non-contiguous 1-D numpy vector must be stored with all its elements.

    A reversed view (``arr[::-1]``) or a column extracted from a 2-D array has a
    non-standard memory layout (negative or non-unit strides) but is still a
    valid 1-D float vector. Every element must reach Qdrant, exactly as for a
    contiguous array — the vector must not be silently truncated.
    """
    import numpy as np

    class NumpySchema(pw.Schema):
        id: int = pw.column_definition(primary_key=True)
        vector: np.ndarray

    collection_name = qdrant.generate_collection_name()
    qdrant.create_collection(collection_name)

    reversed_view = np.array([3.0, 2.0, 1.0])[::-1]  # -> [1, 2, 3], non-contiguous
    column_view = np.array([[10.0, 20.0], [30.0, 40.0], [50.0, 60.0]])[
        :, 1
    ]  # [20,40,60]
    assert not reversed_view.flags["C_CONTIGUOUS"]
    assert not column_view.flags["C_CONTIGUOUS"]

    G.clear()
    table = pw.debug.table_from_rows(
        NumpySchema,
        [(1, reversed_view), (2, column_view)],
    )
    pw.io.qdrant.write(
        table,
        url=qdrant.url,
        collection_name=collection_name,
        vector=table.vector,
    )
    run()

    result = qdrant.query_all(collection_name)
    result.sort(key=lambda r: r["id"])
    assert result == [
        {"id": 1, "vector": [1.0, 2.0, 3.0]},
        {"id": 2, "vector": [20.0, 40.0, 60.0]},
    ]


def test_write_non_finite_vector_raises(qdrant):
    """A vector containing NaN or infinity is rejected with a clear error.

    Qdrant silently accepts non-finite vector components, but a single such point
    makes the whole collection unreadable to standard clients (the values fail to
    deserialize). The connector must reject a non-finite vector up front with an
    actionable error instead of poisoning the collection.
    """
    collection_name = qdrant.generate_collection_name()
    qdrant.create_collection(collection_name)

    for bad_vector in ([float("nan"), 1.0, 2.0], [1.0, float("inf"), 2.0]):
        G.clear()
        table = pw.debug.table_from_rows(_InputSchema, [(1, bad_vector, "a")])
        pw.io.qdrant.write(
            table,
            url=qdrant.url,
            collection_name=collection_name,
            vector=table.vector,
        )
        with pytest.raises(Exception, match="non-finite"):
            run()

    assert qdrant.count(collection_name) == 0


def test_write_non_finite_numpy_vector_raises(qdrant):
    """The non-finite guard also applies to numpy vectors."""
    import numpy as np

    class NumpySchema(pw.Schema):
        id: int = pw.column_definition(primary_key=True)
        vector: np.ndarray

    collection_name = qdrant.generate_collection_name()
    qdrant.create_collection(collection_name)

    G.clear()
    table = pw.debug.table_from_rows(NumpySchema, [(1, np.array([1.0, np.nan, 3.0]))])
    pw.io.qdrant.write(
        table,
        url=qdrant.url,
        collection_name=collection_name,
        vector=table.vector,
    )
    with pytest.raises(Exception, match="non-finite"):
        run()

    assert qdrant.count(collection_name) == 0


def test_write_json_payload(qdrant):
    """pw.Json payload values are stored as plain JSON in the point payload."""
    import json

    class JsonSchema(pw.Schema):
        id: int = pw.column_definition(primary_key=True)
        vector: list[float]
        meta: pw.Json

    collection_name = qdrant.generate_collection_name()
    qdrant.create_collection(collection_name)

    G.clear()
    table = pw.debug.table_from_rows(
        JsonSchema,
        [(1, [1.0, 2.0, 3.0], json.loads('{"category": "A", "score": 5}'))],
    )
    pw.io.qdrant.write(
        table,
        url=qdrant.url,
        collection_name=collection_name,
        vector=table.vector,
    )
    run()

    result = qdrant.query_all(collection_name)
    assert result == [
        {"id": 1, "vector": [1.0, 2.0, 3.0], "meta": {"category": "A", "score": 5}}
    ]
