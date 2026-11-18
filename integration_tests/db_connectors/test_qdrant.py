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


class _DenseSchema(pw.Schema):
    id: int = pw.column_definition(primary_key=True)
    vector: list[float]
    title: str


class _HybridSchema(pw.Schema):
    id: int = pw.column_definition(primary_key=True)
    vector: list[float]
    sparse: list[tuple[int, float]]
    title: str


_HYBRID_ROWS = [
    (1, [1.0, 2.0, 3.0], [(1, 2.0), (5, 1.0)], "a"),
    (2, [4.0, 5.0, 6.0], [(7, 3.0)], "b"),
    (3, [7.0, 8.0, 9.0], [(1, 1.0), (7, 1.0)], "c"),
]


def _write_table(
    qdrant: QdrantContext,
    collection_name: str,
    rows,
    *,
    schema=_DenseSchema,
    is_stream: bool = False,
) -> None:
    G.clear()
    table = pw.debug.table_from_rows(schema, rows, is_stream=is_stream)
    pw.io.qdrant.write(
        table,
        url=qdrant.url,
        collection_name=collection_name,
    )
    run()


def _create_hybrid_collection(qdrant: QdrantContext, collection_name: str) -> None:
    qdrant.create_collection(
        collection_name,
        dense={"vector": QDRANT_VECTOR_DIM},
        sparse=["sparse"],
    )


def test_write_dense_only(qdrant):
    """A collection with a single named dense slot — the common non-hybrid
    case — receives the same-named column as the vector and the rest as
    payload."""
    collection_name = qdrant.generate_collection_name()
    qdrant.create_collection(collection_name, dense={"vector": QDRANT_VECTOR_DIM})

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


def test_write_hybrid_dense_and_sparse(qdrant):
    """Both the dense and the sparse vector land on every point, written in the
    same upsert, and a sparse query scores points by the stored weights."""
    collection_name = qdrant.generate_collection_name()
    _create_hybrid_collection(qdrant, collection_name)

    _write_table(qdrant, collection_name, _HYBRID_ROWS, schema=_HybridSchema)

    result = qdrant.query_all(collection_name)
    result.sort(key=lambda r: r["id"])
    assert result == [
        {"id": id_, "vector": vec, "sparse": sparse, "title": title}
        for id_, vec, sparse, title in _HYBRID_ROWS
    ]

    # Term 5 appears only in row 1.
    only_row_1 = qdrant.sparse_query(collection_name, "sparse", [(5, 1.0)])
    assert [row["id"] for row in only_row_1] == [1]

    # Term 7 appears in rows 2 and 3; row 2 stores the higher weight, so it
    # must be scored first.
    rows_2_and_3 = qdrant.sparse_query(collection_name, "sparse", [(7, 1.0)])
    assert [row["id"] for row in rows_2_and_3] == [2, 3]


def test_write_upsert(qdrant):
    """Rewriting an existing primary key replaces the point — both its dense
    and its sparse vector — instead of duplicating it."""
    collection_name = qdrant.generate_collection_name()
    _create_hybrid_collection(qdrant, collection_name)

    _write_table(
        qdrant,
        collection_name,
        [
            (1, [1.0, 2.0, 3.0], [(1, 1.0)], "a"),
            (2, [4.0, 5.0, 6.0], [(2, 1.0)], "b"),
        ],
        schema=_HybridSchema,
    )
    _write_table(
        qdrant,
        collection_name,
        [
            (1, [9.0, 8.0, 7.0], [(9, 2.0)], "z"),
            (2, [4.0, 5.0, 6.0], [(2, 1.0)], "b"),
        ],
        schema=_HybridSchema,
    )

    result = qdrant.query_all(collection_name)
    result.sort(key=lambda r: r["id"])
    assert result == [
        {"id": 1, "vector": [9.0, 8.0, 7.0], "sparse": [(9, 2.0)], "title": "z"},
        {"id": 2, "vector": [4.0, 5.0, 6.0], "sparse": [(2, 1.0)], "title": "b"},
    ]
    # The old sparse term of row 1 must not match anything anymore.
    assert qdrant.sparse_query(collection_name, "sparse", [(1, 1.0)]) == []


def test_write_update_within_one_minibatch(qdrant):
    """An in-stream update (retraction + insertion of the same key in one
    minibatch) atomically replaces both vectors of the point."""
    collection_name = qdrant.generate_collection_name()
    _create_hybrid_collection(qdrant, collection_name)

    # table_from_rows with is_stream=True expects (...cols..., time, diff).
    rows = [
        (1, [1.0, 2.0, 3.0], [(1, 1.0)], "old", 2, 1),
        (1, [1.0, 2.0, 3.0], [(1, 1.0)], "old", 4, -1),
        (1, [9.0, 8.0, 7.0], [(9, 2.0)], "new", 4, 1),
    ]
    _write_table(qdrant, collection_name, rows, schema=_HybridSchema, is_stream=True)

    result = qdrant.query_all(collection_name)
    assert result == [
        {"id": 1, "vector": [9.0, 8.0, 7.0], "sparse": [(9, 2.0)], "title": "new"}
    ]


def test_write_delete(qdrant):
    """A row deletion removes the whole point, including both its vectors."""
    collection_name = qdrant.generate_collection_name()
    _create_hybrid_collection(qdrant, collection_name)

    # Insert points 1 and 2 at time=2, then delete point 1 at time=4.
    rows = [
        (1, [1.0, 2.0, 3.0], [(1, 1.0)], "a", 2, 1),
        (2, [4.0, 5.0, 6.0], [(2, 1.0)], "b", 2, 1),
        (1, [1.0, 2.0, 3.0], [(1, 1.0)], "a", 4, -1),
    ]
    _write_table(qdrant, collection_name, rows, schema=_HybridSchema, is_stream=True)

    result = qdrant.query_all(collection_name)
    assert len(result) == 1
    assert result[0]["id"] == 2
    # The deleted point's sparse vector must not match anything anymore.
    assert qdrant.sparse_query(collection_name, "sparse", [(1, 1.0)]) == []


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


def test_write_small_batch_size_chunks(qdrant):
    """A small batch_size splits the minibatch into several requests; every
    point must still land in the collection exactly once."""
    collection_name = qdrant.generate_collection_name()
    qdrant.create_collection(collection_name)

    n = 50
    rows = [(i, [float(i), float(i + 1), float(i + 2)], f"t{i}") for i in range(n)]
    G.clear()
    table = pw.debug.table_from_rows(_DenseSchema, rows)
    pw.io.qdrant.write(
        table,
        url=qdrant.url,
        collection_name=collection_name,
        batch_size=7,
    )
    run()

    assert qdrant.count(collection_name) == n


def test_write_invalid_batch_size_raises(qdrant):
    """A non-positive batch_size is rejected up front with a clear ValueError."""
    collection_name = qdrant.generate_collection_name()
    qdrant.create_collection(collection_name)

    G.clear()
    table = pw.debug.table_from_rows(_DenseSchema, [(1, [1.0, 2.0, 3.0], "a")])
    with pytest.raises(ValueError, match="batch_size must be a positive integer"):
        pw.io.qdrant.write(
            table,
            url=qdrant.url,
            collection_name=collection_name,
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
    table = pw.io.python.read(VectorSubject(), schema=_DenseSchema)
    pw.io.qdrant.write(
        table,
        url=qdrant.url,
        collection_name=collection_name,
    )
    run()

    result = qdrant.query_all(collection_name)
    result.sort(key=lambda r: r["id"])
    assert result == _STREAMING_ROWS


def test_write_missing_collection_raises(qdrant):
    """A missing collection fails at startup with an error telling the user to
    create it beforehand — the connector never creates collections."""
    collection_name = qdrant.generate_collection_name()
    # Intentionally do NOT create the collection.

    with pytest.raises(
        Exception,
        match=(f'collection "{collection_name}" does not exist.*create it beforehand'),
    ):
        _write_table(qdrant, collection_name, [(1, [1.0, 2.0, 3.0], "a")])


def test_write_collection_without_vector_slots_raises(qdrant):
    """A collection declaring no vector slots at all fails loudly at startup."""
    collection_name = qdrant.generate_collection_name()
    qdrant.create_collection(collection_name, dense={})

    with pytest.raises(Exception, match="declares no vector slots"):
        _write_table(qdrant, collection_name, [(1, [1.0, 2.0, 3.0], "a")])


def test_write_unnamed_vector_slot_raises(qdrant):
    """A collection configured with a single unnamed vector fails loudly: the
    connector binds vector slots to columns by name, so slots must be named."""
    collection_name = qdrant.generate_collection_name()
    qdrant.create_collection(collection_name, unnamed_dimension=QDRANT_VECTOR_DIM)

    with pytest.raises(Exception, match="single unnamed vector.*named vector slots"):
        _write_table(qdrant, collection_name, [(1, [1.0, 2.0, 3.0], "a")])


def test_write_orphaned_slot_raises(qdrant):
    """A declared slot with no same-named column fails loudly, naming the slot.

    This is also the guard against column typos: the misnamed column would
    silently go to the payload, but the orphaned slot it should have fed makes
    the pipeline fail instead.
    """
    collection_name = qdrant.generate_collection_name()
    qdrant.create_collection(collection_name, dense={"embedding": QDRANT_VECTOR_DIM})

    with pytest.raises(
        Exception,
        match='dense vector slot "embedding".*no column named "embedding"',
    ):
        _write_table(qdrant, collection_name, [(1, [1.0, 2.0, 3.0], "a")])


def test_write_orphaned_sparse_slot_raises(qdrant):
    """An orphaned sparse slot fails just as loudly as an orphaned dense one."""
    collection_name = qdrant.generate_collection_name()
    qdrant.create_collection(
        collection_name, dense={"vector": QDRANT_VECTOR_DIM}, sparse=["bm25"]
    )

    with pytest.raises(
        Exception,
        match='sparse vector slot "bm25".*no column named "bm25"',
    ):
        _write_table(qdrant, collection_name, [(1, [1.0, 2.0, 3.0], "a")])


def test_write_dense_column_bound_to_sparse_slot_raises(qdrant):
    """A list[float] column bound to a sparse slot is a kind mismatch and fails
    loudly, naming the slot and both the expected and the actual type."""
    collection_name = qdrant.generate_collection_name()
    qdrant.create_collection(collection_name, dense={}, sparse=["vector"])

    with pytest.raises(
        Exception,
        match=(
            'sparse vector slot "vector".*'
            r"requires a column \"vector\" of type list\[tuple\[int, float\]\].*"
            r"has type list\[float\]"
        ),
    ):
        _write_table(qdrant, collection_name, [(1, [1.0, 2.0, 3.0], "a")])


def test_write_sparse_column_bound_to_dense_slot_raises(qdrant):
    """A list[tuple[int, float]] column bound to a dense slot is a kind
    mismatch and fails loudly, naming the slot and both types."""
    collection_name = qdrant.generate_collection_name()
    qdrant.create_collection(collection_name, dense={"sparse": QDRANT_VECTOR_DIM})

    class SparseOnlySchema(pw.Schema):
        id: int = pw.column_definition(primary_key=True)
        sparse: list[tuple[int, float]]

    with pytest.raises(
        Exception,
        match=(
            'dense vector slot "sparse".*'
            r"requires a column \"sparse\" of type list\[float\].*"
            r"has type list\[tuple\[int, float\]\]"
        ),
    ):
        _write_table(
            qdrant, collection_name, [(1, [(1, 1.0)])], schema=SparseOnlySchema
        )


def test_write_multivector_slot_raises_not_implemented(qdrant):
    """A collection declaring a multivector slot fails loudly: multivectors are
    not implemented in the connector yet."""
    collection_name = qdrant.generate_collection_name()
    qdrant.create_collection(collection_name, multivector={"vector": QDRANT_VECTOR_DIM})

    with pytest.raises(
        NotImplementedError,
        match='multivector slot "vector".*not supported',
    ):
        _write_table(qdrant, collection_name, [(1, [1.0, 2.0, 3.0], "a")])


def test_write_dimension_mismatch_raises(qdrant):
    """A dense vector whose dimension differs from the slot's configured
    dimension is rejected with an error naming the slot and both dimensions."""
    collection_name = qdrant.generate_collection_name()
    qdrant.create_collection(collection_name, dense={"vector": QDRANT_VECTOR_DIM})

    wrong_dim_rows = [(1, [1.0, 2.0], "a")]  # dim=2, slot expects dim=3
    with pytest.raises(
        Exception,
        match='column "vector" has dimension 2.*slot "vector" expects dimension 3',
    ):
        _write_table(qdrant, collection_name, wrong_dim_rows)


def test_write_sparse_negative_index_raises(qdrant):
    """A sparse pair with a negative index is rejected with a clear error:
    Qdrant sparse indices are unsigned 32-bit integers."""
    collection_name = qdrant.generate_collection_name()
    _create_hybrid_collection(qdrant, collection_name)

    rows = [(1, [1.0, 2.0, 3.0], [(-1, 1.0)], "a")]
    with pytest.raises(Exception, match="index -1.*does not fit the u32 range"):
        _write_table(qdrant, collection_name, rows, schema=_HybridSchema)

    assert qdrant.count(collection_name) == 0


def test_write_sparse_too_large_index_raises(qdrant):
    """A sparse pair with an index above the u32 range is rejected with a clear
    error instead of being silently truncated."""
    collection_name = qdrant.generate_collection_name()
    _create_hybrid_collection(qdrant, collection_name)

    rows = [(1, [1.0, 2.0, 3.0], [(2**32, 1.0)], "a")]
    with pytest.raises(Exception, match="does not fit the u32 range"):
        _write_table(qdrant, collection_name, rows, schema=_HybridSchema)

    assert qdrant.count(collection_name) == 0


def test_write_empty_sparse_vector(qdrant):
    """A row whose sparse column holds no pairs (e.g. a document with no known
    terms) is stored with an empty sparse vector and a working dense vector."""
    collection_name = qdrant.generate_collection_name()
    _create_hybrid_collection(qdrant, collection_name)

    rows: list[tuple] = [(1, [1.0, 2.0, 3.0], [], "a")]
    _write_table(qdrant, collection_name, rows, schema=_HybridSchema)

    result = qdrant.query_all(collection_name)
    assert result == [{"id": 1, "vector": [1.0, 2.0, 3.0], "sparse": [], "title": "a"}]


def test_write_numpy_vector(qdrant):
    """A 1-D numpy.ndarray column feeds a dense slot and is stored as a list."""
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
    )
    run()

    result = qdrant.query_all(collection_name)
    result.sort(key=lambda r: r["id"])
    assert result == [
        {"id": 1, "vector": [1.0, 2.0, 3.0]},
        {"id": 2, "vector": [20.0, 40.0, 60.0]},
    ]


def test_write_non_finite_vector_raises(qdrant):
    """A dense vector containing NaN or infinity is rejected with a clear error.

    Qdrant silently accepts non-finite vector components, but a single such point
    makes the whole collection unreadable to standard clients (the values fail to
    deserialize). The connector must reject a non-finite vector up front with an
    actionable error instead of poisoning the collection.
    """
    collection_name = qdrant.generate_collection_name()
    qdrant.create_collection(collection_name)

    for bad_vector in ([float("nan"), 1.0, 2.0], [1.0, float("inf"), 2.0]):
        G.clear()
        table = pw.debug.table_from_rows(_DenseSchema, [(1, bad_vector, "a")])
        pw.io.qdrant.write(
            table,
            url=qdrant.url,
            collection_name=collection_name,
        )
        with pytest.raises(Exception, match="non-finite"):
            run()

    assert qdrant.count(collection_name) == 0


def test_write_non_finite_sparse_weight_raises(qdrant):
    """The non-finite guard also applies to sparse vector weights."""
    collection_name = qdrant.generate_collection_name()
    _create_hybrid_collection(qdrant, collection_name)

    rows = [(1, [1.0, 2.0, 3.0], [(1, float("nan"))], "a")]
    with pytest.raises(Exception, match="non-finite"):
        _write_table(qdrant, collection_name, rows, schema=_HybridSchema)

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
    )
    run()

    result = qdrant.query_all(collection_name)
    assert result == [
        {"id": 1, "vector": [1.0, 2.0, 3.0], "meta": {"category": "A", "score": 5}}
    ]


def test_write_vector_like_column_without_slot_goes_to_payload(qdrant):
    """A vector-capable column whose name matches no slot is stored in the
    payload, as any other column."""
    collection_name = qdrant.generate_collection_name()
    qdrant.create_collection(collection_name, dense={"vector": QDRANT_VECTOR_DIM})

    class ExtraVectorSchema(pw.Schema):
        id: int = pw.column_definition(primary_key=True)
        vector: list[float]
        extra: list[float]

    G.clear()
    table = pw.debug.table_from_rows(
        ExtraVectorSchema, [(1, [1.0, 2.0, 3.0], [4.0, 5.0])]
    )
    pw.io.qdrant.write(
        table,
        url=qdrant.url,
        collection_name=collection_name,
    )
    run()

    result = qdrant.query_all(collection_name)
    assert result == [{"id": 1, "vector": [1.0, 2.0, 3.0], "extra": [4.0, 5.0]}]
