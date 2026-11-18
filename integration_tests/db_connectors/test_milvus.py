# Copyright © 2026 Pathway

import pytest
from utils import MILVUS_VECTOR_DIM, MilvusContext

import pathway as pw
from pathway.internals.parse_graph import G
from pathway.tests.utils import run

_STREAMING_ROWS = [
    {"id": 1, "vector": [1.0, 2.0, 3.0]},
    {"id": 2, "vector": [4.0, 5.0, 6.0]},
    {"id": 3, "vector": [7.0, 8.0, 9.0]},
]


def _write_table(milvus: MilvusContext, collection_name, rows, *, is_stream=False):
    class InputSchema(pw.Schema):
        id: int = pw.column_definition(primary_key=True)
        vector: list[float]

    G.clear()
    table = pw.debug.table_from_rows(InputSchema, rows, is_stream=is_stream)
    pw.io.milvus.write(
        table,
        uri=milvus.uri,
        collection_name=collection_name,
        primary_key=table.id,
    )
    run()


def test_write_basic(milvus):
    collection_name = milvus.generate_collection_name()
    milvus.create_collection(collection_name)

    rows = [
        (1, [1.0, 2.0, 3.0]),
        (2, [4.0, 5.0, 6.0]),
        (3, [7.0, 8.0, 9.0]),
    ]
    _write_table(milvus, collection_name, rows)

    result = milvus.query_all(collection_name, ["id", "vector"])
    result.sort(key=lambda r: r["id"])
    assert result == [{"id": id_, "vector": vec} for id_, vec in rows]


def test_write_upsert(milvus):
    """Rows with an existing primary key should be replaced, not duplicated."""
    collection_name = milvus.generate_collection_name()
    milvus.create_collection(collection_name)

    _write_table(milvus, collection_name, [(1, [1.0, 2.0, 3.0]), (2, [4.0, 5.0, 6.0])])
    _write_table(milvus, collection_name, [(1, [9.0, 8.0, 7.0]), (2, [4.0, 5.0, 6.0])])

    result = milvus.query_all(collection_name, ["id", "vector"])
    result.sort(key=lambda r: r["id"])
    assert result == [
        {"id": 1, "vector": [9.0, 8.0, 7.0]},
        {"id": 2, "vector": [4.0, 5.0, 6.0]},
    ]


def test_write_delete(milvus):
    """Rows with diff=-1 should be removed from the collection."""
    collection_name = milvus.generate_collection_name()
    milvus.create_collection(collection_name)

    # Insert rows 1 and 2 at time=2, then delete row 1 at time=4.
    # table_from_rows with is_stream=True expects (...cols..., time, diff) tuples.
    rows = [
        (1, [1.0, 2.0, 3.0], 2, 1),
        (2, [4.0, 5.0, 6.0], 2, 1),
        (1, [1.0, 2.0, 3.0], 4, -1),
    ]
    _write_table(milvus, collection_name, rows, is_stream=True)

    result = milvus.query_all(collection_name, ["id", "vector"])
    assert len(result) == 1
    assert result[0]["id"] == 2


def test_write_empty(milvus):
    """Writing an empty table should succeed without errors."""
    collection_name = milvus.generate_collection_name()
    milvus.create_collection(collection_name)

    _write_table(milvus, collection_name, [])

    assert milvus.query_all(collection_name, ["id", "vector"]) == []


def test_write_large_batch(milvus):
    """Writing many rows at once should work correctly."""
    collection_name = milvus.generate_collection_name()
    milvus.create_collection(collection_name)

    n = 500
    rows = [(i, [float(i), float(i + 1), float(i + 2)]) for i in range(n)]
    _write_table(milvus, collection_name, rows)

    assert len(milvus.query_all(collection_name, ["id"])) == n


def test_write_batch_size_chunks_requests(milvus, monkeypatch):
    """A mini-batch larger than ``batch_size`` is split into several upsert
    requests, each no larger than ``batch_size`` rows, and every row still lands.

    Milvus caps a single gRPC message at 64 MiB by default, so a large commit
    sent as one upsert is rejected with ``RESOURCE_EXHAUSTED``. ``batch_size``
    keeps each request bounded regardless of how large the commit is.
    """
    from pymilvus import MilvusClient

    collection_name = milvus.generate_collection_name()
    milvus.create_collection(collection_name)

    upsert_sizes: list[int] = []
    original_upsert = MilvusClient.upsert

    def spy_upsert(self, *args, **kwargs):
        upsert_sizes.append(len(kwargs["data"]))
        return original_upsert(self, *args, **kwargs)

    monkeypatch.setattr(MilvusClient, "upsert", spy_upsert)

    n = 1000
    batch_size = 128
    rows = [(i, [float(i), float(i + 1), float(i + 2)]) for i in range(n)]

    class InputSchema(pw.Schema):
        id: int = pw.column_definition(primary_key=True)
        vector: list[float]

    G.clear()
    table = pw.debug.table_from_rows(InputSchema, rows)
    pw.io.milvus.write(
        table,
        uri=milvus.uri,
        collection_name=collection_name,
        primary_key=table.id,
        batch_size=batch_size,
    )
    run()

    assert len(milvus.query_all(collection_name, ["id"])) == n
    assert upsert_sizes, "no upsert request was issued"
    assert max(upsert_sizes) <= batch_size
    assert sum(upsert_sizes) == n


def test_write_batch_size_must_be_positive(milvus):
    """A non-positive ``batch_size`` is rejected up front with a clear error."""
    collection_name = milvus.generate_collection_name()
    milvus.create_collection(collection_name)

    class InputSchema(pw.Schema):
        id: int = pw.column_definition(primary_key=True)
        vector: list[float]

    G.clear()
    table = pw.debug.table_from_rows(InputSchema, [(1, [1.0, 2.0, 3.0])])
    with pytest.raises(ValueError, match="batch_size must be a positive integer"):
        pw.io.milvus.write(
            table,
            uri=milvus.uri,
            collection_name=collection_name,
            primary_key=table.id,
            batch_size=0,
        )


@pytest.mark.parametrize(
    "rows", [[(1, [1.0, 2.0, 3.0])], []], ids=["nonempty", "empty"]
)
def test_write_missing_collection_raises(milvus, rows):
    """Pointing the connector at a collection that does not exist fails fast with
    a clear error at pipeline construction, rather than surfacing deep inside the
    run — or, for an empty table, silently doing nothing."""

    class InputSchema(pw.Schema):
        id: int = pw.column_definition(primary_key=True)
        vector: list[float]

    G.clear()
    table = pw.debug.table_from_rows(InputSchema, rows)
    with pytest.raises(Exception, match="does not exist"):
        pw.io.milvus.write(
            table,
            uri=milvus.uri,
            collection_name="no_such_collection_xyz",
            primary_key=table.id,
        )


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), float("-inf")])
def test_write_non_finite_vector_raises(milvus, bad):
    """A vector containing NaN or infinity is rejected with a clear error rather
    than silently stored, since a non-finite component corrupts the index and
    makes similarity search meaningless."""
    collection_name = milvus.generate_collection_name()
    milvus.create_collection(collection_name)

    class InputSchema(pw.Schema):
        id: int = pw.column_definition(primary_key=True)
        vector: list[float]

    G.clear()
    table = pw.debug.table_from_rows(InputSchema, [(1, [bad, 1.0, 2.0])])
    pw.io.milvus.write(
        table, uri=milvus.uri, collection_name=collection_name, primary_key=table.id
    )
    with pytest.raises(Exception, match="non-finite"):
        run()

    # Nothing was written: the row was rejected before reaching Milvus.
    assert milvus.query_all(collection_name, ["id"]) == []


def test_write_incompatible_type_raises(milvus):
    """Writing a string to a FLOAT_VECTOR field should raise a clear type error."""
    collection_name = milvus.generate_collection_name()
    milvus.create_collection(collection_name)

    class InputSchema(pw.Schema):
        id: int = pw.column_definition(primary_key=True)
        vector: str  # incompatible with FLOAT_VECTOR

    G.clear()
    table = pw.debug.table_from_rows(InputSchema, [(1, "not_a_vector")])
    pw.io.milvus.write(
        table, uri=milvus.uri, collection_name=collection_name, primary_key=table.id
    )
    with pytest.raises(Exception, match="float_vector"):
        run()


def test_write_dimension_mismatch_raises(milvus):
    """Attempting to insert a vector of the wrong dimension should raise."""
    collection_name = milvus.generate_collection_name()
    milvus.create_collection(collection_name, dimension=MILVUS_VECTOR_DIM)

    wrong_dim_rows = [(1, [1.0, 2.0])]  # dim=2, collection expects dim=3
    with pytest.raises(Exception):
        _write_table(milvus, collection_name, wrong_dim_rows)


def test_write_explicit_primary_key(milvus):
    """Passing primary_key=table.id routes deletes via the named column."""
    collection_name = milvus.generate_collection_name()
    milvus.create_collection(collection_name)

    class InputSchema(pw.Schema):
        id: int = pw.column_definition(primary_key=True)
        vector: list[float]

    rows = [(1, [1.0, 2.0, 3.0]), (2, [4.0, 5.0, 6.0])]
    G.clear()
    table = pw.debug.table_from_rows(InputSchema, rows)
    pw.io.milvus.write(
        table,
        uri=milvus.uri,
        collection_name=collection_name,
        primary_key=table.id,
    )
    run()

    result = milvus.query_all(collection_name, ["id", "vector"])
    result.sort(key=lambda r: r["id"])
    assert result == [{"id": id_, "vector": vec} for id_, vec in rows]


def test_write_primary_key_wrong_table_raises(milvus):
    """Passing a column from a different table raises a clear ValueError."""
    collection_name = milvus.generate_collection_name()
    milvus.create_collection(collection_name)

    class InputSchema(pw.Schema):
        id: int = pw.column_definition(primary_key=True)
        vector: list[float]

    G.clear()
    table = pw.debug.table_from_rows(InputSchema, [(1, [1.0, 2.0, 3.0])])
    other = pw.debug.table_from_rows(InputSchema, [(2, [4.0, 5.0, 6.0])])
    with pytest.raises(ValueError, match="does not belong to the provided table"):
        pw.io.milvus.write(
            table,
            uri=milvus.uri,
            collection_name=collection_name,
            primary_key=other.id,
        )


def test_write_streaming(milvus):
    """Rows emitted via a Python connector across multiple commits land in Milvus."""
    collection_name = milvus.generate_collection_name()
    milvus.create_collection(collection_name)

    class InputSchema(pw.Schema):
        id: int = pw.column_definition(primary_key=True)
        vector: list[float]

    class VectorSubject(pw.io.python.ConnectorSubject):
        def run(self):
            for row in _STREAMING_ROWS:
                self.next_json(row)
                self.commit()

    G.clear()
    table = pw.io.python.read(VectorSubject(), schema=InputSchema)
    pw.io.milvus.write(
        table,
        uri=milvus.uri,
        collection_name=collection_name,
        primary_key=table.id,
    )
    run()

    result = milvus.query_all(collection_name, ["id", "vector"])
    result.sort(key=lambda r: r["id"])
    assert result == _STREAMING_ROWS
