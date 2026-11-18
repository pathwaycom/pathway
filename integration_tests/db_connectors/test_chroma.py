# Copyright © 2026 Pathway

import os
import subprocess
import sys

import numpy as np
import pytest
from utils import ChromaContext

import pathway as pw
from pathway.internals.parse_graph import G
from pathway.tests.utils import run

_STREAMING_ROWS = [
    {"id": 1, "vector": [1.0, 2.0, 3.0]},
    {"id": 2, "vector": [4.0, 5.0, 6.0]},
    {"id": 3, "vector": [7.0, 8.0, 9.0]},
]


class _VectorSchema(pw.Schema):
    id: int = pw.column_definition(primary_key=True)
    vector: list[float]


def _write_vectors(chroma: ChromaContext, collection_name, rows, *, is_stream=False):
    G.clear()
    table = pw.debug.table_from_rows(_VectorSchema, rows, is_stream=is_stream)
    pw.io.chroma.write(
        table,
        collection_name=collection_name,
        primary_key=table.id,
        embedding=table.vector,
        host=chroma.host,
        port=chroma.port,
    )
    run()


def test_write_basic(chroma):
    collection_name = chroma.generate_collection_name()
    chroma.create_collection(collection_name)

    rows = [
        (1, [1.0, 2.0, 3.0]),
        (2, [4.0, 5.0, 6.0]),
        (3, [7.0, 8.0, 9.0]),
    ]
    _write_vectors(chroma, collection_name, rows)

    result = chroma.query_all(collection_name, include=("embeddings",))
    assert result == [{"id": str(id_), "embedding": vec} for id_, vec in rows]


def test_write_document_and_metadata(chroma):
    """document and metadata columns travel with each vector."""
    collection_name = chroma.generate_collection_name()
    chroma.create_collection(collection_name)

    class DocSchema(pw.Schema):
        id: int = pw.column_definition(primary_key=True)
        vector: list[float]
        text: str
        rank: int
        score: float
        flag: bool

    rows = [
        (1, [1.0, 2.0, 3.0], "hello", 10, 0.5, True),
        (2, [4.0, 5.0, 6.0], "world", 20, 0.25, False),
    ]
    G.clear()
    table = pw.debug.table_from_rows(DocSchema, rows)
    pw.io.chroma.write(
        table,
        collection_name=collection_name,
        primary_key=table.id,
        embedding=table.vector,
        document=table.text,
        metadata_columns=[table.rank, table.score, table.flag],
        host=chroma.host,
        port=chroma.port,
    )
    run()

    result = chroma.query_all(collection_name)
    assert result == [
        {
            "id": "1",
            "embedding": [1.0, 2.0, 3.0],
            "document": "hello",
            "metadata": {"rank": 10, "score": 0.5, "flag": True},
        },
        {
            "id": "2",
            "embedding": [4.0, 5.0, 6.0],
            "document": "world",
            "metadata": {"rank": 20, "score": 0.25, "flag": False},
        },
    ]


def test_write_type_conversions(chroma):
    """A string primary key is used as the id, and metadata scalars keep their
    Pathway types (str/int/float/bool)."""
    collection_name = chroma.generate_collection_name()
    chroma.create_collection(collection_name)

    class TypedSchema(pw.Schema):
        key: str = pw.column_definition(primary_key=True)
        vector: list[float]
        category: str
        rank: int
        score: float
        flag: bool

    rows = [
        ("doc-a", [1.0, 2.0, 3.0], "news", 1, 0.5, True),
        ("doc-b", [4.0, 5.0, 6.0], "blog", 2, 0.25, False),
    ]
    G.clear()
    table = pw.debug.table_from_rows(TypedSchema, rows)
    pw.io.chroma.write(
        table,
        collection_name=collection_name,
        primary_key=table.key,
        embedding=table.vector,
        metadata_columns=[table.category, table.rank, table.score, table.flag],
        host=chroma.host,
        port=chroma.port,
    )
    run()

    result = chroma.query_all(collection_name, include=("embeddings", "metadatas"))
    assert result == [
        {
            "id": "doc-a",
            "embedding": [1.0, 2.0, 3.0],
            "metadata": {"category": "news", "rank": 1, "score": 0.5, "flag": True},
        },
        {
            "id": "doc-b",
            "embedding": [4.0, 5.0, 6.0],
            "metadata": {"category": "blog", "rank": 2, "score": 0.25, "flag": False},
        },
    ]
    # The metadata scalars must come back as their original Python types, not
    # coerced (e.g. an int must not arrive as a float).
    for record in result:
        meta = record["metadata"]
        assert isinstance(meta["category"], str)
        assert isinstance(meta["rank"], int) and not isinstance(meta["rank"], bool)
        assert isinstance(meta["score"], float)
        assert isinstance(meta["flag"], bool)


def test_write_upsert(chroma):
    """Rows with an existing primary key should be replaced, not duplicated."""
    collection_name = chroma.generate_collection_name()
    chroma.create_collection(collection_name)

    _write_vectors(
        chroma, collection_name, [(1, [1.0, 2.0, 3.0]), (2, [4.0, 5.0, 6.0])]
    )
    _write_vectors(
        chroma, collection_name, [(1, [9.0, 8.0, 7.0]), (2, [4.0, 5.0, 6.0])]
    )

    result = chroma.query_all(collection_name, include=("embeddings",))
    assert result == [
        {"id": "1", "embedding": [9.0, 8.0, 7.0]},
        {"id": "2", "embedding": [4.0, 5.0, 6.0]},
    ]


def test_write_delete(chroma):
    """Rows with diff=-1 should be removed from the collection."""
    collection_name = chroma.generate_collection_name()
    chroma.create_collection(collection_name)

    # Insert rows 1 and 2 at time=2, then delete row 1 at time=4.
    # table_from_rows with is_stream=True expects (...cols..., time, diff) tuples.
    rows = [
        (1, [1.0, 2.0, 3.0], 2, 1),
        (2, [4.0, 5.0, 6.0], 2, 1),
        (1, [1.0, 2.0, 3.0], 4, -1),
    ]
    _write_vectors(chroma, collection_name, rows, is_stream=True)

    result = chroma.query_all(collection_name, include=("embeddings",))
    assert result == [{"id": "2", "embedding": [4.0, 5.0, 6.0]}]


def test_write_without_primary_key_uses_internal_key(chroma):
    """Omitting ``primary_key`` stores each row under its internal Pathway key.

    The ids are opaque engine keys, so the test checks that every row landed with
    the right embedding and that the ids are distinct non-empty strings, rather
    than pinning the exact id values.
    """
    collection_name = chroma.generate_collection_name()
    chroma.create_collection(collection_name)

    rows = [
        (1, [1.0, 2.0, 3.0]),
        (2, [4.0, 5.0, 6.0]),
        (3, [7.0, 8.0, 9.0]),
    ]
    G.clear()
    table = pw.debug.table_from_rows(_VectorSchema, rows)
    pw.io.chroma.write(
        table,
        collection_name=collection_name,
        embedding=table.vector,
        host=chroma.host,
        port=chroma.port,
    )
    run()

    result = chroma.query_all(collection_name, include=("embeddings",))
    assert sorted(record["embedding"] for record in result) == sorted(
        vec for _, vec in rows
    )
    ids = [record["id"] for record in result]
    assert all(isinstance(id_, str) and id_ for id_ in ids)
    assert len(set(ids)) == len(rows)


def test_write_without_primary_key_upsert_and_delete(chroma):
    """Internal-key writes still replace updated rows and drop deleted ones.

    An update arrives as a delete of the old row plus an insert of the new one,
    both sharing the same internal key; the single-writer path the keyless mode
    uses must apply the delete before the upsert so the new value survives.
    """
    collection_name = chroma.generate_collection_name()
    chroma.create_collection(collection_name)

    # Insert rows 1 and 2 at time=2; update row 1's vector and delete row 2 at
    # time=4. table_from_rows(is_stream=True) expects (...cols..., time, diff).
    rows = [
        (1, [1.0, 2.0, 3.0], 2, 1),
        (2, [4.0, 5.0, 6.0], 2, 1),
        (1, [1.0, 2.0, 3.0], 4, -1),
        (1, [9.0, 8.0, 7.0], 4, 1),
        (2, [4.0, 5.0, 6.0], 4, -1),
    ]
    G.clear()
    table = pw.debug.table_from_rows(_VectorSchema, rows, is_stream=True)
    pw.io.chroma.write(
        table,
        collection_name=collection_name,
        embedding=table.vector,
        host=chroma.host,
        port=chroma.port,
    )
    run()

    result = chroma.query_all(collection_name, include=("embeddings",))
    assert [record["embedding"] for record in result] == [[9.0, 8.0, 7.0]]


def test_write_empty(chroma):
    """Writing an empty table should succeed without errors."""
    collection_name = chroma.generate_collection_name()
    chroma.create_collection(collection_name)

    _write_vectors(chroma, collection_name, [])

    assert chroma.count(collection_name) == 0


def test_write_large_batch(chroma):
    """Writing more rows than the server's max batch size still writes them all.

    The connector chunks each upsert to ``get_max_batch_size()``; this batch is
    larger than that limit, so a single un-chunked request would be rejected.
    """
    collection_name = chroma.generate_collection_name()
    chroma.create_collection(collection_name)

    n = 6000
    rows = [(i, [float(i), float(i + 1), float(i + 2)]) for i in range(n)]
    _write_vectors(chroma, collection_name, rows)

    assert chroma.count(collection_name) == n


def test_write_large_high_dimensional_batch(chroma):
    """A big batch of high-dimensional vectors is written without a 413.

    A single upsert chunk is the server's max batch size (thousands of records);
    at this dimensionality a verbose JSON-number-array body for one chunk far
    exceeds the server's request-size limit and is rejected with HTTP 413
    "Payload too large". The connector must keep every request within the limit
    (it sends embeddings in the server's compact binary encoding), so all rows
    land regardless of vector size.
    """
    collection_name = chroma.generate_collection_name()
    chroma.create_collection(collection_name)

    n = 8000  # larger than the server's per-request max batch size
    dim = 768

    class HighDimSchema(pw.Schema):
        id: int = pw.column_definition(primary_key=True)
        vector: np.ndarray

    rng = np.random.default_rng(0)
    rows = [(i, rng.random(dim, dtype=np.float32)) for i in range(n)]
    G.clear()
    table = pw.debug.table_from_rows(HighDimSchema, rows)
    pw.io.chroma.write(
        table,
        collection_name=collection_name,
        primary_key=table.id,
        embedding=table.vector,
        host=chroma.host,
        port=chroma.port,
    )
    run()

    assert chroma.count(collection_name) == n


def test_write_streaming(chroma):
    """Rows emitted via a Python connector across multiple commits land in Chroma."""
    collection_name = chroma.generate_collection_name()
    chroma.create_collection(collection_name)

    class VectorSubject(pw.io.python.ConnectorSubject):
        def run(self):
            for row in _STREAMING_ROWS:
                self.next_json(row)
                self.commit()

    G.clear()
    table = pw.io.python.read(VectorSubject(), schema=_VectorSchema)
    pw.io.chroma.write(
        table,
        collection_name=collection_name,
        primary_key=table.id,
        embedding=table.vector,
        host=chroma.host,
        port=chroma.port,
    )
    run()

    result = chroma.query_all(collection_name, include=("embeddings",))
    assert result == [
        {"id": str(row["id"]), "embedding": row["vector"]} for row in _STREAMING_ROWS
    ]


def test_write_embedding_wrong_type_raises(chroma):
    """A non-vector embedding column should raise a clear type error."""
    collection_name = chroma.generate_collection_name()
    chroma.create_collection(collection_name)

    class BadSchema(pw.Schema):
        id: int = pw.column_definition(primary_key=True)
        vector: str  # not a vector

    G.clear()
    table = pw.debug.table_from_rows(BadSchema, [(1, "not_a_vector")])
    pw.io.chroma.write(
        table,
        collection_name=collection_name,
        primary_key=table.id,
        embedding=table.vector,
        host=chroma.host,
        port=chroma.port,
    )
    with pytest.raises(Exception, match="embedding"):
        run()


def test_write_unsupported_metadata_type_raises(chroma):
    """A non-scalar metadata column should raise a clear type error."""
    collection_name = chroma.generate_collection_name()
    chroma.create_collection(collection_name)

    class MetaSchema(pw.Schema):
        id: int = pw.column_definition(primary_key=True)
        vector: list[float]
        extra: list[float]  # not a scalar metadata type

    G.clear()
    table = pw.debug.table_from_rows(MetaSchema, [(1, [1.0, 2.0, 3.0], [9.0])])
    pw.io.chroma.write(
        table,
        collection_name=collection_name,
        primary_key=table.id,
        embedding=table.vector,
        metadata_columns=[table.extra],
        host=chroma.host,
        port=chroma.port,
    )
    with pytest.raises(Exception, match="metadata"):
        run()


def test_write_primary_key_wrong_table_raises(chroma):
    """Passing a column from a different table raises a clear ValueError."""
    collection_name = chroma.generate_collection_name()
    chroma.create_collection(collection_name)

    G.clear()
    table = pw.debug.table_from_rows(_VectorSchema, [(1, [1.0, 2.0, 3.0])])
    other = pw.debug.table_from_rows(_VectorSchema, [(2, [4.0, 5.0, 6.0])])
    with pytest.raises(ValueError, match="does not belong to the provided table"):
        pw.io.chroma.write(
            table,
            collection_name=collection_name,
            primary_key=other.id,
            embedding=table.vector,
            host=chroma.host,
            port=chroma.port,
        )


def test_write_missing_collection_raises(chroma):
    """Writing to a collection that does not exist raises when the pipeline runs."""
    G.clear()
    table = pw.debug.table_from_rows(_VectorSchema, [(1, [1.0, 2.0, 3.0])])
    pw.io.chroma.write(
        table,
        collection_name="does_not_exist_" + chroma.generate_collection_name(),
        primary_key=table.id,
        embedding=table.vector,
        host=chroma.host,
        port=chroma.port,
    )
    with pytest.raises(Exception, match="does not exist"):
        run()


# Run in a subprocess with PATHWAY_THREADS=N so the keyless write path
# (``single_threaded() == true``) consolidates every record onto one worker. It
# inserts keyed rows, updates a quarter, and deletes a quarter, all without a
# ``primary_key`` so each record's id is the row's internal Pathway key.
_MULTIWORKER_INTERNAL_KEY_WORKER = """
import sys
import numpy as np
import pandas as pd
import pathway as pw
from pathway.internals.parse_graph import G

host, port, name, n_str, dim_str = sys.argv[1:6]
n, dim = int(n_str), int(dim_str)

updated = {k for k in range(n) if k % 4 == 0}
deleted = {k for k in range(n) if k % 4 == 1}

rows = []
for k in range(n):
    rows.append({"k": k, "tag": f"v{k}", "__time__": 2, "__diff__": 1})
for k in sorted(updated):                       # retract old, insert new value
    rows.append({"k": k, "tag": f"v{k}", "__time__": 4, "__diff__": -1})
    rows.append({"k": k, "tag": f"v{k}_upd", "__time__": 4, "__diff__": 1})
for k in sorted(deleted):                        # retract -> record removed
    rows.append({"k": k, "tag": f"v{k}", "__time__": 6, "__diff__": -1})


def emb(k):
    return list(np.random.default_rng(k).standard_normal(dim).astype(float))


df = pd.DataFrame(rows)
df["embedding"] = df["k"].map(emb)

G.clear()
table = pw.debug.table_from_pandas(df).with_id_from(pw.this.k)
pw.io.chroma.write(                              # no primary_key -> internal key
    table,
    collection_name=name,
    embedding=table.embedding,
    document=table.tag,
    host=host,
    port=int(port),
)
pw.run(monitoring_level=pw.MonitoringLevel.NONE)
"""


@pytest.mark.parametrize("workers", [4, 8])
def test_write_without_primary_key_multiworker_applies_updates_and_deletes(
    chroma, tmp_path, workers
):
    """Keyless writes stay correct when Pathway runs with several workers.

    Without ``primary_key`` the record id is the row's internal Pathway key and
    the sink runs single-threaded, so every event for a given key (insert, the
    retract+insert of an update, and a delete) is handled by one writer. The net
    state across N workers must therefore be exact: updates reflected, deletions
    removed, and no duplicate ids.
    """
    collection_name = chroma.generate_collection_name()
    chroma.create_collection(collection_name)

    n, dim = 400, 16
    updated = {k for k in range(n) if k % 4 == 0}
    deleted = {k for k in range(n) if k % 4 == 1}

    worker_script = tmp_path / "mw_internal_key_worker.py"
    worker_script.write_text(_MULTIWORKER_INTERNAL_KEY_WORKER)
    env = dict(os.environ, PATHWAY_THREADS=str(workers))
    result = subprocess.run(
        [
            sys.executable,
            str(worker_script),
            chroma.host,
            str(chroma.port),
            collection_name,
            str(n),
            str(dim),
        ],
        env=env,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, f"worker failed:\n{result.stderr[-3000:]}"

    records = chroma.query_all(collection_name, include=("documents",))
    ids = [record["id"] for record in records]
    assert len(ids) == len(set(ids)) == n - len(deleted)
    tags = sorted(record["document"] for record in records)
    expected_tags = sorted(
        f"v{k}_upd" if k in updated else f"v{k}" for k in range(n) if k not in deleted
    )
    assert tags == expected_tags
