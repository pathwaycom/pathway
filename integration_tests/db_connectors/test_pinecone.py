# Copyright © 2026 Pathway

import numpy as np
import pytest

import pathway as pw
from pathway.internals.parse_graph import G
from pathway.tests.utils import run

_STREAMING_ROWS = [
    {"id": 1, "vector": [1.0, 0.0, 0.0]},
    {"id": 2, "vector": [0.0, 1.0, 0.0]},
    {"id": 3, "vector": [0.0, 0.0, 1.0]},
]


class _VectorSchema(pw.Schema):
    id: int = pw.column_definition(primary_key=True)
    vector: list[float]


def _write_vectors(pinecone, index_name, rows, *, is_stream=False):
    G.clear()
    table = pw.debug.table_from_rows(_VectorSchema, rows, is_stream=is_stream)
    pw.io.pinecone.write(
        table,
        index_name=index_name,
        primary_key=table.id,
        vector=table.vector,
        api_key=pinecone.api_key,
        host=pinecone.control_host,
    )
    run()


def test_write_basic(pinecone):
    index_name = pinecone.generate_index_name()
    pinecone.create_index(index_name)

    rows = [
        (1, [1.0, 0.0, 0.0]),
        (2, [0.0, 1.0, 0.0]),
        (3, [0.0, 0.0, 1.0]),
    ]
    _write_vectors(pinecone, index_name, rows)

    assert pinecone.wait_for_count(index_name, 3) == 3
    fetched = pinecone.fetch(index_name, [1, 2, 3])
    assert set(fetched) == {"1", "2", "3"}


def test_write_preserves_vector_values(pinecone):
    """Vectors round-trip through Pinecone with their original component values."""
    index_name = pinecone.generate_index_name()
    pinecone.create_index(index_name)

    rows = [(1, [1.0, 0.0, 0.0]), (2, [0.0, 0.5, 0.5])]
    _write_vectors(pinecone, index_name, rows)

    assert pinecone.wait_for_count(index_name, 2) == 2
    fetched = pinecone.fetch(index_name, [1, 2])
    assert fetched["1"]["values"] == pytest.approx([1.0, 0.0, 0.0], abs=1e-3)
    assert fetched["2"]["values"] == pytest.approx([0.0, 0.5, 0.5], abs=1e-3)


def test_write_with_metadata(pinecone):
    """Non-primary-key, non-vector columns are stored as record metadata."""
    index_name = pinecone.generate_index_name()
    pinecone.create_index(index_name)

    class InputSchema(pw.Schema):
        id: int = pw.column_definition(primary_key=True)
        vector: list[float]
        title: str
        score: float
        published: bool

    rows = [(1, [1.0, 0.0, 0.0], "First", 0.9, True)]
    G.clear()
    table = pw.debug.table_from_rows(InputSchema, rows)
    pw.io.pinecone.write(
        table,
        index_name=index_name,
        primary_key=table.id,
        vector=table.vector,
        api_key=pinecone.api_key,
        host=pinecone.control_host,
    )
    run()

    assert pinecone.wait_for_count(index_name, 1) == 1
    metadata = pinecone.fetch(index_name, [1])["1"]["metadata"]
    assert metadata["title"] == "First"
    assert metadata["score"] == pytest.approx(0.9)
    assert metadata["published"] is True


def test_write_selected_metadata_columns(pinecone):
    """Only the columns listed in metadata_columns are stored as metadata."""
    index_name = pinecone.generate_index_name()
    pinecone.create_index(index_name)

    class InputSchema(pw.Schema):
        id: int = pw.column_definition(primary_key=True)
        vector: list[float]
        title: str
        secret: str

    rows = [(1, [1.0, 0.0, 0.0], "First", "do-not-store")]
    G.clear()
    table = pw.debug.table_from_rows(InputSchema, rows)
    pw.io.pinecone.write(
        table,
        index_name=index_name,
        primary_key=table.id,
        vector=table.vector,
        metadata_columns=[table.title],
        api_key=pinecone.api_key,
        host=pinecone.control_host,
    )
    run()

    assert pinecone.wait_for_count(index_name, 1) == 1
    metadata = pinecone.fetch(index_name, [1])["1"]["metadata"]
    assert metadata == {"title": "First"}


def test_write_to_namespace(pinecone):
    """Records are written into the requested namespace, not the default one."""
    index_name = pinecone.generate_index_name()
    pinecone.create_index(index_name)

    G.clear()
    table = pw.debug.table_from_rows(
        _VectorSchema, [(1, [1.0, 0.0, 0.0]), (2, [0.0, 1.0, 0.0])]
    )
    pw.io.pinecone.write(
        table,
        index_name=index_name,
        primary_key=table.id,
        vector=table.vector,
        namespace="tenant-a",
        api_key=pinecone.api_key,
        host=pinecone.control_host,
    )
    run()

    assert pinecone.wait_for_count(index_name, 2, namespace="tenant-a") == 2
    assert set(pinecone.fetch(index_name, [1, 2], namespace="tenant-a")) == {"1", "2"}
    # Nothing leaked into the default namespace.
    assert pinecone.fetch(index_name, [1, 2], namespace="") == {}


def test_write_upsert(pinecone):
    """Rows with an existing primary key should be replaced, not duplicated."""
    index_name = pinecone.generate_index_name()
    pinecone.create_index(index_name)

    _write_vectors(pinecone, index_name, [(1, [1.0, 0.0, 0.0]), (2, [0.0, 1.0, 0.0])])
    assert pinecone.wait_for_count(index_name, 2) == 2

    _write_vectors(pinecone, index_name, [(1, [0.0, 0.0, 1.0]), (2, [0.0, 1.0, 0.0])])
    assert pinecone.wait_for_count(index_name, 2) == 2
    fetched = pinecone.fetch(index_name, [1])
    assert fetched["1"]["values"] == pytest.approx([0.0, 0.0, 1.0], abs=1e-3)


def test_write_without_primary_key_uses_internal_key(pinecone):
    """With no ``primary_key`` the internal row key is used as the record id, so
    each row maps to one record and an update upserts in place (no duplicates)."""
    index_name = pinecone.generate_index_name()
    pinecone.create_index(index_name)

    # Insert 3 rows at time=2, then update each (same row key, new vector) at
    # time=4. The internal key is stable across the update, so the count stays 3.
    rows = [
        (1, [1.0, 0.0, 0.0], 2, 1),
        (2, [0.0, 1.0, 0.0], 2, 1),
        (3, [0.0, 0.0, 1.0], 2, 1),
        (1, [1.0, 0.0, 0.0], 4, -1),
        (1, [0.5, 0.5, 0.0], 4, 1),
        (2, [0.0, 1.0, 0.0], 4, -1),
        (2, [0.0, 0.5, 0.5], 4, 1),
        (3, [0.0, 0.0, 1.0], 4, -1),
        (3, [0.5, 0.0, 0.5], 4, 1),
    ]
    G.clear()
    table = pw.debug.table_from_rows(_VectorSchema, rows, is_stream=True)
    pw.io.pinecone.write(
        table,
        index_name=index_name,
        vector=table.vector,
        api_key=pinecone.api_key,
        host=pinecone.control_host,
    )
    run()

    assert pinecone.wait_for_count(index_name, 3) == 3


def test_write_delete(pinecone):
    """Rows with diff=-1 should be removed from the index."""
    index_name = pinecone.generate_index_name()
    pinecone.create_index(index_name)

    # Insert rows 1 and 2 at time=2, then delete row 1 at time=4.
    # table_from_rows with is_stream=True expects (...cols..., time, diff) tuples.
    rows = [
        (1, [1.0, 0.0, 0.0], 2, 1),
        (2, [0.0, 1.0, 0.0], 2, 1),
        (1, [1.0, 0.0, 0.0], 4, -1),
    ]
    _write_vectors(pinecone, index_name, rows, is_stream=True)

    assert pinecone.wait_for_count(index_name, 1) == 1
    assert set(pinecone.fetch(index_name, [1, 2])) == {"2"}


def test_write_delete_more_than_one_request_worth(pinecone):
    """Deleting more ids than fit in one Pinecone delete call removes them all.

    Pinecone caps a single delete at 1000 ids, so removing 1500 rows in one
    minibatch must be split across requests with every record deleted.
    """
    index_name = pinecone.generate_index_name()
    pinecone.create_index(index_name)

    n = 1500
    rows = []
    for i in range(n):
        rows.append((i, [1.0, 0.0, 0.0], 2, 1))  # insert at time=2
        rows.append((i, [1.0, 0.0, 0.0], 4, -1))  # delete at time=4
    _write_vectors(pinecone, index_name, rows, is_stream=True)

    assert pinecone.wait_for_count(index_name, 0) == 0


def test_write_unicode_and_special_characters(pinecone):
    """Ids and metadata with unicode, quotes and control characters round-trip."""
    index_name = pinecone.generate_index_name()
    pinecone.create_index(index_name)

    class InputSchema(pw.Schema):
        doc_id: str = pw.column_definition(primary_key=True)
        vector: list[float]
        title: str

    record_id = 'café "x"\n\t/\\日本'
    title = 'tag "with" \n newline 日本'
    G.clear()
    table = pw.debug.table_from_rows(InputSchema, [(record_id, [1.0, 0.0, 0.0], title)])
    pw.io.pinecone.write(
        table,
        index_name=index_name,
        primary_key=table.doc_id,
        vector=table.vector,
        api_key=pinecone.api_key,
        host=pinecone.control_host,
    )
    run()

    assert pinecone.wait_for_count(index_name, 1) == 1
    fetched = pinecone.fetch(index_name, [record_id])
    assert list(fetched) == [record_id]
    assert fetched[record_id]["metadata"] == {"title": title}


def test_write_empty(pinecone):
    """Writing an empty table should succeed without errors."""
    index_name = pinecone.generate_index_name()
    pinecone.create_index(index_name)

    _write_vectors(pinecone, index_name, [])

    assert pinecone.wait_for_count(index_name, 0) == 0


def test_write_large_batch(pinecone):
    """Writing more rows than a single upsert batch should work correctly."""
    index_name = pinecone.generate_index_name()
    pinecone.create_index(index_name)

    n = 250
    rows = [(i, [float(i), float(i + 1), float(i + 2)]) for i in range(n)]
    _write_vectors(pinecone, index_name, rows)

    assert pinecone.wait_for_count(index_name, n) == n


def test_write_high_dim_large_batch_does_not_overflow_request(pinecone):
    """A big batch of high-dimensional vectors is split to stay under Pinecone's
    2 MB request limit instead of failing with HTTP 413.

    With ``dimension=736`` and ``batch_size=500`` a single upsert request would
    far exceed 2 MB; the connector must split it transparently so every vector
    still lands.
    """
    index_name = pinecone.generate_index_name()
    pinecone.create_index(index_name, dimension=736)

    class HighDimSchema(pw.Schema):
        id: int = pw.column_definition(primary_key=True)
        vector: list[float]

    n = 600
    rows = [(i, [float((i + j) % 7) for j in range(736)]) for i in range(n)]
    G.clear()
    table = pw.debug.table_from_rows(HighDimSchema, rows)
    pw.io.pinecone.write(
        table,
        index_name=index_name,
        primary_key=table.id,
        vector=table.vector,
        api_key=pinecone.api_key,
        host=pinecone.control_host,
        batch_size=500,
    )
    run()

    assert pinecone.wait_for_count(index_name, n) == n


def test_write_streaming(pinecone):
    """Rows emitted via a Python connector across multiple commits land in Pinecone."""
    index_name = pinecone.generate_index_name()
    pinecone.create_index(index_name)

    class VectorSubject(pw.io.python.ConnectorSubject):
        def run(self):
            for row in _STREAMING_ROWS:
                self.next_json(row)
                self.commit()

    G.clear()
    table = pw.io.python.read(VectorSubject(), schema=_VectorSchema)
    pw.io.pinecone.write(
        table,
        index_name=index_name,
        primary_key=table.id,
        vector=table.vector,
        api_key=pinecone.api_key,
        host=pinecone.control_host,
    )
    run()

    assert pinecone.wait_for_count(index_name, 3) == 3
    assert set(pinecone.fetch(index_name, [1, 2, 3])) == {"1", "2", "3"}


def test_write_primary_key_wrong_table_raises(pinecone):
    """Passing a primary_key column from a different table raises a clear ValueError."""
    index_name = pinecone.generate_index_name()

    G.clear()
    table = pw.debug.table_from_rows(_VectorSchema, [(1, [1.0, 0.0, 0.0])])
    other = pw.debug.table_from_rows(_VectorSchema, [(2, [0.0, 1.0, 0.0])])
    with pytest.raises(ValueError, match="does not belong to the provided table"):
        pw.io.pinecone.write(
            table,
            index_name=index_name,
            primary_key=other.id,
            vector=table.vector,
            api_key=pinecone.api_key,
            host=pinecone.control_host,
        )


def test_write_vector_wrong_table_raises(pinecone):
    """Passing a vector column from a different table raises a clear ValueError."""
    index_name = pinecone.generate_index_name()

    G.clear()
    table = pw.debug.table_from_rows(_VectorSchema, [(1, [1.0, 0.0, 0.0])])
    other = pw.debug.table_from_rows(_VectorSchema, [(2, [0.0, 1.0, 0.0])])
    with pytest.raises(ValueError, match="does not belong to the provided table"):
        pw.io.pinecone.write(
            table,
            index_name=index_name,
            primary_key=table.id,
            vector=other.vector,
            api_key=pinecone.api_key,
            host=pinecone.control_host,
        )


def test_write_unsupported_metadata_value_raises_at_runtime(pinecone):
    """A metadata value of an unsupported type is rejected at runtime.

    When the column's static type is unknown (here an unannotated UDF whose
    result type is ``ANY``), the up-front guard cannot reject it, so the engine
    must catch the offending value while flushing and surface a clear error
    naming the column. Statically-typed unsupported columns are rejected earlier,
    at ``write()`` time (see ``test_write_rejects_unsupported_metadata_type``).
    """
    index_name = pinecone.generate_index_name()
    pinecone.create_index(index_name)

    @pw.udf
    def make_blob(x: int):  # no return annotation -> ANY column type
        return b"blob"

    class InputSchema(pw.Schema):
        doc_id: int = pw.column_definition(primary_key=True)
        vector: list[float]

    G.clear()
    table = pw.debug.table_from_rows(InputSchema, [(1, [1.0, 0.0, 0.0])])
    table = table.with_columns(payload=make_blob(table.doc_id))
    pw.io.pinecone.write(
        table,
        index_name=index_name,
        primary_key=table.doc_id,
        vector=table.vector,
        metadata_columns=[table.payload],
        api_key=pinecone.api_key,
        host=pinecone.control_host,
    )
    with pytest.raises(Exception, match="unsupported type"):
        run()


def test_write_metadata_type_coverage(pinecone):
    """Every supported metadata type round-trips per the conversion table."""
    index_name = pinecone.generate_index_name()
    pinecone.create_index(index_name)

    class InputSchema(pw.Schema):
        id: int = pw.column_definition(primary_key=True)
        vector: list[float]
        s: str
        i: int
        f: float
        b: bool
        tags: list[str]

    rows = [(1, [1.0, 0.0, 0.0], "hello", 42, 0.5, True, ["a", "b"])]
    G.clear()
    table = pw.debug.table_from_rows(InputSchema, rows)
    pw.io.pinecone.write(
        table,
        index_name=index_name,
        primary_key=table.id,
        vector=table.vector,
        api_key=pinecone.api_key,
        host=pinecone.control_host,
    )
    run()

    assert pinecone.wait_for_count(index_name, 1) == 1
    md = pinecone.fetch(index_name, [1])["1"]["metadata"]
    assert md["s"] == "hello"
    assert md["i"] == 42
    assert md["f"] == pytest.approx(0.5)
    assert md["b"] is True
    assert list(md["tags"]) == ["a", "b"]


def test_write_numpy_vector(pinecone):
    """A 1-D numpy.ndarray vector column round-trips with its values."""
    index_name = pinecone.generate_index_name()
    pinecone.create_index(index_name)

    class InputSchema(pw.Schema):
        id: int = pw.column_definition(primary_key=True)
        vector: np.ndarray

    rows = [
        (1, np.array([1.0, 0.0, 0.0], dtype=np.float32)),
        (2, np.array([0.0, 0.5, 0.5], dtype=np.float32)),
    ]
    G.clear()
    table = pw.debug.table_from_rows(InputSchema, rows)
    pw.io.pinecone.write(
        table,
        index_name=index_name,
        primary_key=table.id,
        vector=table.vector,
        api_key=pinecone.api_key,
        host=pinecone.control_host,
    )
    run()

    assert pinecone.wait_for_count(index_name, 2) == 2
    fetched = pinecone.fetch(index_name, [1, 2])
    assert fetched["1"]["values"] == pytest.approx([1.0, 0.0, 0.0], abs=1e-3)
    assert fetched["2"]["values"] == pytest.approx([0.0, 0.5, 0.5], abs=1e-3)


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), float("-inf")])
def test_write_rejects_non_finite_vector_value(pinecone, bad):
    """A NaN or infinite vector component is rejected with a clear, named error.

    JSON has no representation for NaN/infinity, so such a value would otherwise
    reach Pinecone as ``null`` and fail with an opaque deserialization error. The
    connector must instead surface a clear error naming the vector column.
    """
    index_name = pinecone.generate_index_name()
    pinecone.create_index(index_name)

    G.clear()
    table = pw.debug.table_from_rows(_VectorSchema, [(1, [bad, 0.0, 0.0])])
    pw.io.pinecone.write(
        table,
        index_name=index_name,
        primary_key=table.id,
        vector=table.vector,
        api_key=pinecone.api_key,
        host=pinecone.control_host,
    )
    with pytest.raises(Exception, match="non-finite"):
        run()


def test_write_rejects_non_finite_metadata_value(pinecone):
    """A NaN or infinite metadata float is rejected with a clear, named error
    instead of being sent to Pinecone as an unstorable ``null``.

    The non-finite value is produced by a UDF (as it would be by a real
    computation such as a division), so it reaches the connector at runtime.
    """
    index_name = pinecone.generate_index_name()
    pinecone.create_index(index_name)

    @pw.udf
    def to_score(x: int) -> float:
        return float("nan")

    class InputSchema(pw.Schema):
        doc_id: int = pw.column_definition(primary_key=True)
        vector: list[float]

    G.clear()
    table = pw.debug.table_from_rows(InputSchema, [(1, [1.0, 0.0, 0.0])])
    table = table.with_columns(score=to_score(table.doc_id))
    pw.io.pinecone.write(
        table,
        index_name=index_name,
        primary_key=table.doc_id,
        vector=table.vector,
        metadata_columns=[table.score],
        api_key=pinecone.api_key,
        host=pinecone.control_host,
    )
    with pytest.raises(Exception, match="non-finite"):
        run()


def test_write_rejects_duplicate_record_id(pinecone):
    """Two distinct rows mapped to the same record id raise a clear error.

    When ``primary_key`` is a non-unique column, two different rows claim the same
    Pinecone record id and would silently overwrite each other. The connector must
    reject this rather than lose data.
    """
    index_name = pinecone.generate_index_name()
    pinecone.create_index(index_name)

    class InputSchema(pw.Schema):
        id: int = pw.column_definition(primary_key=True)
        group: int  # deliberately non-unique, used as the Pinecone record id
        vector: list[float]

    G.clear()
    table = pw.debug.table_from_rows(
        InputSchema,
        [(1, 7, [1.0, 0.0, 0.0]), (2, 7, [0.0, 1.0, 0.0])],
    )
    pw.io.pinecone.write(
        table,
        index_name=index_name,
        primary_key=table.group,
        vector=table.vector,
        api_key=pinecone.api_key,
        host=pinecone.control_host,
    )
    with pytest.raises(Exception, match="uniquely identify"):
        run()


def test_write_string_primary_key(pinecone):
    """A str primary_key column is used verbatim as the Pinecone record id."""
    index_name = pinecone.generate_index_name()
    pinecone.create_index(index_name)

    class InputSchema(pw.Schema):
        doc_id: str = pw.column_definition(primary_key=True)
        vector: list[float]

    rows = [("alpha", [1.0, 0.0, 0.0]), ("beta", [0.0, 1.0, 0.0])]
    G.clear()
    table = pw.debug.table_from_rows(InputSchema, rows)
    pw.io.pinecone.write(
        table,
        index_name=index_name,
        primary_key=table.doc_id,
        vector=table.vector,
        api_key=pinecone.api_key,
        host=pinecone.control_host,
    )
    run()

    assert pinecone.wait_for_count(index_name, 2) == 2
    assert set(pinecone.fetch(index_name, ["alpha", "beta"])) == {"alpha", "beta"}


def test_write_large_one_shot_load(pinecone):
    """A single large load lands entirely, exercising request-size/count limits.

    5000 dim-736 vectors in one minibatch force the connector to split the load
    across many requests; all records must arrive with no limit-exceeded errors.
    """
    index_name = pinecone.generate_index_name()
    pinecone.create_index(index_name, dimension=736)

    class HighDimSchema(pw.Schema):
        id: int = pw.column_definition(primary_key=True)
        vector: list[float]

    n = 5000
    rows = [(i, [float((i + j) % 5) for j in range(736)]) for i in range(n)]
    G.clear()
    table = pw.debug.table_from_rows(HighDimSchema, rows)
    pw.io.pinecone.write(
        table,
        index_name=index_name,
        primary_key=table.id,
        vector=table.vector,
        api_key=pinecone.api_key,
        host=pinecone.control_host,
    )
    run()

    assert pinecone.wait_for_count(index_name, n, timeout=120) == n


# --- Configuration / type guardrails (validated up front, before any request) ---
#
# `pw.io.pinecone.write` rejects misconfigured pipelines and statically-known
# unsupported column types at call time, so the user does not have to wait for a
# bad row to reach the sink and crash a worker at runtime. These tests never
# connect to Pinecone — they assert that `write()` raises before that point.


class _GuardrailSchema(pw.Schema):
    id: int = pw.column_definition(primary_key=True)
    name: str
    score: float
    vector: list[float]
    blob: bytes
    opt_id: int | None
    opt_vector: list[float] | None


def _guardrail_table():
    G.clear()
    return pw.debug.table_from_rows(
        _GuardrailSchema,
        [(1, "a", 0.5, [1.0, 2.0, 3.0], b"x", None, None)],
    )


def test_write_rejects_float_primary_key():
    table = _guardrail_table()
    with pytest.raises(ValueError, match="primary_key column 'score'.*int or str"):
        pw.io.pinecone.write(
            table,
            index_name="i",
            primary_key=table.score,
            vector=table.vector,
            api_key="k",
        )


def test_write_rejects_bytes_primary_key():
    table = _guardrail_table()
    with pytest.raises(ValueError, match="primary_key column 'blob'.*int or str"):
        pw.io.pinecone.write(
            table,
            index_name="i",
            primary_key=table.blob,
            vector=table.vector,
            api_key="k",
        )


def test_write_rejects_nullable_primary_key():
    table = _guardrail_table()
    with pytest.raises(ValueError, match="primary_key column 'opt_id' is nullable"):
        pw.io.pinecone.write(
            table,
            index_name="i",
            primary_key=table.opt_id,
            vector=table.vector,
            api_key="k",
        )


def test_write_rejects_non_numeric_vector():
    table = _guardrail_table()
    with pytest.raises(ValueError, match="vector column 'name'.*list\\[float\\]"):
        pw.io.pinecone.write(
            table,
            index_name="i",
            primary_key=table.id,
            vector=table.name,
            api_key="k",
        )


def test_write_rejects_nullable_vector():
    table = _guardrail_table()
    with pytest.raises(ValueError, match="vector column 'opt_vector' is nullable"):
        pw.io.pinecone.write(
            table,
            index_name="i",
            primary_key=table.id,
            vector=table.opt_vector,
            api_key="k",
        )


def test_write_rejects_unsupported_metadata_type():
    table = _guardrail_table()
    with pytest.raises(
        ValueError, match="metadata column 'blob'.*int, float, bool, str"
    ):
        pw.io.pinecone.write(
            table,
            index_name="i",
            primary_key=table.id,
            vector=table.vector,
            metadata_columns=[table.blob],
            api_key="k",
        )


def test_write_rejects_unsupported_metadata_type_when_inferred():
    """A `bytes` column auto-included as metadata is rejected too."""
    table = _guardrail_table()
    with pytest.raises(ValueError, match="metadata column 'blob'"):
        pw.io.pinecone.write(
            table,
            index_name="i",
            primary_key=table.id,
            vector=table.vector,
            api_key="k",
        )


def test_write_rejects_primary_key_equal_to_vector():
    table = _guardrail_table()
    with pytest.raises(ValueError, match="both reference column 'vector'"):
        pw.io.pinecone.write(
            table,
            index_name="i",
            primary_key=table.vector,
            vector=table.vector,
            api_key="k",
        )


def test_write_rejects_primary_key_as_metadata_column():
    table = _guardrail_table()
    with pytest.raises(ValueError, match="column 'id' is used as the primary_key"):
        pw.io.pinecone.write(
            table,
            index_name="i",
            primary_key=table.id,
            vector=table.vector,
            metadata_columns=[table.id, table.name],
            api_key="k",
        )


def test_write_rejects_vector_as_metadata_column():
    table = _guardrail_table()
    with pytest.raises(ValueError, match="column 'vector' is used as the vector"):
        pw.io.pinecone.write(
            table,
            index_name="i",
            primary_key=table.id,
            vector=table.vector,
            metadata_columns=[table.vector],
            api_key="k",
        )


@pytest.mark.parametrize("batch_size", [0, -1])
def test_write_rejects_nonpositive_batch_size(batch_size):
    table = _guardrail_table()
    with pytest.raises(ValueError, match="batch_size must be a positive integer"):
        pw.io.pinecone.write(
            table,
            index_name="i",
            primary_key=table.id,
            vector=table.vector,
            metadata_columns=[table.name],
            batch_size=batch_size,
            api_key="k",
        )


def test_write_accepts_all_supported_metadata_types():
    """Supported column types pass the up-front guards.

    All metadata kinds Pinecone accepts (int, float, bool, str, list[str], and
    their nullable variants) plus a str primary key and a numeric vector must get
    past validation. The pipeline then targets an unreachable host, so the only
    accepted outcome is a non-``ValueError`` failure (connect error) — a
    ``ValueError`` here would mean a guardrail wrongly rejected a valid schema.
    """

    class AllTypes(pw.Schema):
        id: str = pw.column_definition(primary_key=True)
        vector: list[float]
        an_int: int
        a_float: float
        a_bool: bool
        a_str: str
        a_str_list: list[str]
        an_opt: str | None

    G.clear()
    table = pw.debug.table_from_rows(
        AllTypes, [("a", [1.0, 2.0], 1, 0.5, True, "s", ["x", "y"], None)]
    )
    try:
        pw.io.pinecone.write(
            table,
            index_name="i",
            primary_key=table.id,
            vector=table.vector,
            api_key="k",
            host="http://127.0.0.1:1",
        )
        run()
    except ValueError as e:
        pytest.fail(f"a valid configuration was rejected by a guardrail: {e}")
    except Exception:
        pass  # connect failure is expected — the guards accepted the schema
