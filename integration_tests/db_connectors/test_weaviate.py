# Copyright © 2026 Pathway

import pytest
from utils import WeaviateContext
from weaviate.util import generate_uuid5

import pathway as pw
from pathway.internals.parse_graph import G
from pathway.tests.utils import run

_STREAMING_ROWS = [
    {"id": 1, "vector": [1.0, 2.0, 3.0]},
    {"id": 2, "vector": [4.0, 5.0, 6.0]},
    {"id": 3, "vector": [7.0, 8.0, 9.0]},
]


def _uuid(key) -> str:
    """The object UUID the connector derives for a primary-key value."""
    return str(generate_uuid5(key))


def _connection_kwargs(weaviate: WeaviateContext) -> dict:
    return {
        "http_host": weaviate.host,
        "http_port": weaviate.http_port,
    }


def _write_table(weaviate, collection_name, rows, *, is_stream=False):
    class InputSchema(pw.Schema):
        id: int = pw.column_definition(primary_key=True)
        vector: list[float]

    G.clear()
    table = pw.debug.table_from_rows(InputSchema, rows, is_stream=is_stream)
    pw.io.weaviate.write(
        table,
        collection_name=collection_name,
        primary_key=table.id,
        vector=table.vector,
        **_connection_kwargs(weaviate),
    )
    run()


def _sorted_by_uuid(rows: list[dict]) -> list[dict]:
    return sorted(rows, key=lambda r: r["uuid"])


def test_write_basic(weaviate):
    collection_name = weaviate.generate_collection_name()
    weaviate.create_collection(collection_name)

    rows = [
        (1, [1.0, 2.0, 3.0]),
        (2, [4.0, 5.0, 6.0]),
        (3, [7.0, 8.0, 9.0]),
    ]
    _write_table(weaviate, collection_name, rows)

    result = _sorted_by_uuid(weaviate.query_all(collection_name))
    expected = _sorted_by_uuid(
        [{"uuid": _uuid(id_), "vector": vec} for id_, vec in rows]
    )
    assert result == expected


def test_write_upsert(weaviate):
    """Rows with an existing primary key should be replaced, not duplicated."""
    collection_name = weaviate.generate_collection_name()
    weaviate.create_collection(collection_name)

    _write_table(
        weaviate, collection_name, [(1, [1.0, 2.0, 3.0]), (2, [4.0, 5.0, 6.0])]
    )
    _write_table(
        weaviate, collection_name, [(1, [9.0, 8.0, 7.0]), (2, [4.0, 5.0, 6.0])]
    )

    result = _sorted_by_uuid(weaviate.query_all(collection_name))
    expected = _sorted_by_uuid(
        [
            {"uuid": _uuid(1), "vector": [9.0, 8.0, 7.0]},
            {"uuid": _uuid(2), "vector": [4.0, 5.0, 6.0]},
        ]
    )
    assert result == expected


def test_write_delete(weaviate):
    """Rows with diff=-1 should be removed from the collection."""
    collection_name = weaviate.generate_collection_name()
    weaviate.create_collection(collection_name)

    # Insert rows 1 and 2 at time=2, then delete row 1 at time=4.
    # table_from_rows with is_stream=True expects (...cols..., time, diff) tuples.
    rows = [
        (1, [1.0, 2.0, 3.0], 2, 1),
        (2, [4.0, 5.0, 6.0], 2, 1),
        (1, [1.0, 2.0, 3.0], 4, -1),
    ]
    _write_table(weaviate, collection_name, rows, is_stream=True)

    result = weaviate.query_all(collection_name)
    assert len(result) == 1
    assert result[0]["uuid"] == _uuid(2)


def test_write_update_within_single_batch(weaviate):
    """A delete + re-insert of the same key in one minibatch keeps the new value."""
    collection_name = weaviate.generate_collection_name()
    weaviate.create_collection(collection_name)

    _write_table(weaviate, collection_name, [(1, [1.0, 2.0, 3.0])])
    # In a single minibatch (same time), retract the old row and add the new one.
    rows = [
        (1, [1.0, 2.0, 3.0], 4, -1),
        (1, [9.0, 9.0, 9.0], 4, 1),
    ]
    _write_table(weaviate, collection_name, rows, is_stream=True)

    result = weaviate.query_all(collection_name)
    assert result == [{"uuid": _uuid(1), "vector": [9.0, 9.0, 9.0]}]


def test_write_empty(weaviate):
    """Writing an empty table should succeed without errors."""
    collection_name = weaviate.generate_collection_name()
    weaviate.create_collection(collection_name)

    _write_table(weaviate, collection_name, [])

    assert weaviate.query_all(collection_name) == []


def test_write_large_batch(weaviate):
    """Writing many rows at once should work correctly."""
    collection_name = weaviate.generate_collection_name()
    weaviate.create_collection(collection_name)

    n = 500
    rows = [(i, [float(i), float(i + 1), float(i + 2)]) for i in range(n)]
    _write_table(weaviate, collection_name, rows)

    assert weaviate.count(collection_name) == n


def test_write_large_high_dim_batch(weaviate):
    """A mini-batch whose vectors exceed the gRPC message-size limit in a single
    request must still be written — the connector splits it across requests."""
    collection_name = weaviate.generate_collection_name()
    weaviate.create_collection(collection_name)

    # 3000 rows x 768 floats x 8 bytes ~= 18 MB, well above Weaviate's default
    # 10 MB gRPC limit if sent as one request.
    dim = 768
    n = 3000
    rows = [(i, [float((i + j) % 10) for j in range(dim)]) for i in range(n)]
    _write_table(weaviate, collection_name, rows)

    assert weaviate.count(collection_name) == n


def test_write_without_vector(weaviate):
    """Omitting the vector column stores only properties."""
    collection_name = weaviate.generate_collection_name()
    weaviate.create_collection(collection_name)

    class InputSchema(pw.Schema):
        id: int = pw.column_definition(primary_key=True)
        title: str

    G.clear()
    table = pw.debug.table_from_rows(InputSchema, [(1, "alpha"), (2, "beta")])
    pw.io.weaviate.write(
        table,
        collection_name=collection_name,
        primary_key=table.id,
        **_connection_kwargs(weaviate),
    )
    run()

    result = _sorted_by_uuid(weaviate.query_all(collection_name, include_vector=False))
    expected = _sorted_by_uuid(
        [
            {"uuid": _uuid(1), "title": "alpha"},
            {"uuid": _uuid(2), "title": "beta"},
        ]
    )
    assert result == expected


def test_write_property_types(weaviate):
    """Each Pathway scalar/list type is stored as the documented Weaviate property
    type. Under Weaviate's auto-schema, integers are stored as ``number`` (returned
    as floats), bytes as a base64 ``text``, and ``list[float]`` as ``number[]``."""
    collection_name = weaviate.generate_collection_name()
    weaviate.create_collection(collection_name)

    class InputSchema(pw.Schema):
        id: int = pw.column_definition(primary_key=True)
        p_int: int
        p_float: float
        p_bool: bool
        p_str: str
        p_bytes: bytes
        p_list: list[float]

    G.clear()
    table = pw.debug.table_from_rows(
        InputSchema,
        [(1, 42, 3.5, True, "hello", b"\x00\x01", [1.0, 2.0])],
    )
    pw.io.weaviate.write(
        table,
        collection_name=collection_name,
        primary_key=table.id,
        **_connection_kwargs(weaviate),
    )
    run()

    result = weaviate.query_all(collection_name, include_vector=False)
    assert result == [
        {
            "uuid": _uuid(1),
            "p_int": 42.0,  # int -> Weaviate `number`
            "p_float": 3.5,
            "p_bool": True,
            "p_str": "hello",
            "p_bytes": "AAE=",  # bytes -> base64 `text`
            "p_list": [1.0, 2.0],  # list[float] -> `number[]`
        }
    ]
    # Verify the inferred Weaviate property types match the documented mapping.
    assert weaviate.property_types(collection_name) == {
        "p_int": "number",
        "p_float": "number",
        "p_bool": "boolean",
        "p_str": "text",
        "p_bytes": "text",
        "p_list": "number[]",
    }


def test_write_extra_properties_preserved(weaviate):
    """Non-vector columns are stored as queryable properties alongside the vector."""
    collection_name = weaviate.generate_collection_name()
    weaviate.create_collection(collection_name)

    class InputSchema(pw.Schema):
        id: int = pw.column_definition(primary_key=True)
        title: str
        vector: list[float]

    G.clear()
    table = pw.debug.table_from_rows(InputSchema, [(1, "alpha", [1.0, 2.0, 3.0])])
    pw.io.weaviate.write(
        table,
        collection_name=collection_name,
        primary_key=table.id,
        vector=table.vector,
        **_connection_kwargs(weaviate),
    )
    run()

    result = weaviate.query_all(collection_name)
    assert result == [{"uuid": _uuid(1), "title": "alpha", "vector": [1.0, 2.0, 3.0]}]


def test_write_streaming(weaviate):
    """Rows emitted via a Python connector across multiple commits land in Weaviate."""
    collection_name = weaviate.generate_collection_name()
    weaviate.create_collection(collection_name)

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
    pw.io.weaviate.write(
        table,
        collection_name=collection_name,
        primary_key=table.id,
        vector=table.vector,
        **_connection_kwargs(weaviate),
    )
    run()

    result = _sorted_by_uuid(weaviate.query_all(collection_name))
    expected = _sorted_by_uuid(
        [{"uuid": _uuid(row["id"]), "vector": row["vector"]} for row in _STREAMING_ROWS]
    )
    assert result == expected


def test_write_reserved_property_name_raises(weaviate):
    """A non-key column named 'vector' (reserved by Weaviate) raises a ValueError."""
    collection_name = weaviate.generate_collection_name()

    class InputSchema(pw.Schema):
        id: int = pw.column_definition(primary_key=True)
        vector: str  # reserved name, and not passed as the vector argument

    G.clear()
    table = pw.debug.table_from_rows(InputSchema, [(1, "x")])
    with pytest.raises(ValueError, match="reserved by Weaviate"):
        pw.io.weaviate.write(
            table,
            collection_name=collection_name,
            primary_key=table.id,
            **_connection_kwargs(weaviate),
        )


def test_write_primary_key_wrong_table_raises(weaviate):
    """Passing a primary_key column from a different table raises a clear ValueError."""
    collection_name = weaviate.generate_collection_name()

    class InputSchema(pw.Schema):
        id: int = pw.column_definition(primary_key=True)
        vector: list[float]

    G.clear()
    table = pw.debug.table_from_rows(InputSchema, [(1, [1.0, 2.0, 3.0])])
    other = pw.debug.table_from_rows(InputSchema, [(2, [4.0, 5.0, 6.0])])
    with pytest.raises(ValueError, match="does not belong to the provided table"):
        pw.io.weaviate.write(
            table,
            collection_name=collection_name,
            primary_key=other.id,
            vector=table.vector,
            **_connection_kwargs(weaviate),
        )


def test_write_vector_with_non_finite_value_raises(weaviate):
    """A vector containing a non-finite component (NaN/Inf) must fail with a clear
    error rather than being silently stored with that component replaced by 0.0
    (JSON has no NaN/Inf, so a naive serialization would corrupt the embedding)."""
    collection_name = weaviate.generate_collection_name()
    weaviate.create_collection(collection_name)

    class InputSchema(pw.Schema):
        id: int = pw.column_definition(primary_key=True)
        vector: list[float]

    G.clear()
    table = pw.debug.table_from_rows(InputSchema, [(1, [1.0, float("nan"), 3.0])])
    pw.io.weaviate.write(
        table,
        collection_name=collection_name,
        primary_key=table.id,
        vector=table.vector,
        **_connection_kwargs(weaviate),
    )
    with pytest.raises(Exception, match="(?i)finite|nan"):
        run()
    # The corrupted object must not have been written.
    assert weaviate.count(collection_name) == 0


def test_write_nonexistent_collection_raises(weaviate):
    """Writing to a collection that does not exist must fail with a clear error
    naming the collection, rather than silently letting Weaviate auto-create a
    differently-configured collection (a typo in ``collection_name`` would
    otherwise route data into a phantom collection unnoticed)."""
    collection_name = weaviate.generate_collection_name()  # deliberately not created

    class InputSchema(pw.Schema):
        id: int = pw.column_definition(primary_key=True)
        vector: list[float]

    G.clear()
    table = pw.debug.table_from_rows(InputSchema, [(1, [1.0, 2.0, 3.0])])
    pw.io.weaviate.write(
        table,
        collection_name=collection_name,
        primary_key=table.id,
        vector=table.vector,
        **_connection_kwargs(weaviate),
    )
    weaviate._created_collections.append(collection_name)  # clean up if auto-created
    with pytest.raises(Exception, match=collection_name):
        run()


def test_write_vector_wrong_table_raises(weaviate):
    """Passing a vector column from a different table raises a clear ValueError."""
    collection_name = weaviate.generate_collection_name()

    class InputSchema(pw.Schema):
        id: int = pw.column_definition(primary_key=True)
        vector: list[float]

    G.clear()
    table = pw.debug.table_from_rows(InputSchema, [(1, [1.0, 2.0, 3.0])])
    other = pw.debug.table_from_rows(InputSchema, [(2, [4.0, 5.0, 6.0])])
    with pytest.raises(ValueError, match="does not belong to the provided table"):
        pw.io.weaviate.write(
            table,
            collection_name=collection_name,
            primary_key=table.id,
            vector=other.vector,
            **_connection_kwargs(weaviate),
        )
