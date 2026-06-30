# Copyright © 2026 Pathway

"""Integration tests for writing vector embeddings to MongoDB Atlas.

MongoDB Atlas is cloud-hosted MongoDB speaking the same wire protocol as a
self-managed deployment, so Pathway's existing ``pw.io.mongodb.write`` connector
already targets it: ``mongodb+srv://`` SRV URIs resolve through the driver's
default ``dns-resolver`` feature, and a vector embedding (a ``numpy`` array
column) is serialized to a BSON array of doubles by ``serialize_value_to_bson``
(``src/connectors/data_format/bson.rs``).

The one capability unique to Atlas is **Atlas Vector Search** — the
``$vectorSearch`` aggregation backed by a ``vectorSearch`` index.  That index is
defined through the regular MongoDB driver (``createSearchIndexes``), so these
tests create it with ``pymongo`` rather than adding connector code.

These tests run against the ``mongodb-atlas`` service defined in
``../../.jenkins/integration_tests/docker-compose-integration.yml`` — the
official ``mongodb/mongodb-atlas-local`` image, the only MongoDB image that ships
the ``mongot`` search process, so ``$vectorSearch`` works in CI without a cloud
Atlas account.  The shared client and the Atlas-specific helpers (vector-index
creation, ``$vectorSearch`` queries) live on the ``atlas`` fixture in
``conftest.py`` / ``AtlasContext`` in ``utils.py``, matching how every other
connector here gets its backing service.
"""

from __future__ import annotations

import os
import subprocess
import sys

import numpy as np
import numpy.typing as npt
import pandas as pd
import pytest
from utils import (
    MONGODB_ATLAS_BASE_NAME as DATABASE,
    MONGODB_ATLAS_VECTOR_DIM as VECTOR_DIM,
    AtlasContext,
)

import pathway as pw
from pathway.internals.parse_graph import G

# The Atlas Local image is heavy (mongod + mongot), and a single shared service
# backs every test, so pin the whole module to one xdist worker.
pytestmark = pytest.mark.xdist_group("mongodb_atlas")


class EmbeddingSchema(pw.Schema):
    doc_id: int
    embedding: npt.NDArray[np.float64]


def _write_vectors(
    atlas: AtlasContext,
    collection: str,
    vectors: dict[int, np.ndarray],
    *,
    output_table_type: str = "snapshot",
) -> None:
    """Stream ``{doc_id: vector}`` rows through ``pw.io.mongodb.write``."""
    items = list(vectors.items())

    class Subject(pw.io.python.ConnectorSubject):
        def run(self):
            for doc_id, vector in items:
                self.next(doc_id=doc_id, embedding=vector)

    G.clear()
    table = pw.io.python.read(Subject(), schema=EmbeddingSchema)
    pw.io.mongodb.write(
        table,
        connection_string=atlas.connection_string,
        database=DATABASE,
        collection=collection,
        output_table_type=output_table_type,
    )
    pw.run(monitoring_level=pw.MonitoringLevel.NONE)


def test_write_736dim_vectors_snapshot(atlas: AtlasContext) -> None:
    """A 736-dim numpy vector lands as a BSON array of 736 doubles."""
    collection = atlas.collection_name()
    rng = np.random.default_rng(42)
    vectors = {i: rng.standard_normal(VECTOR_DIM) for i in range(5)}

    _write_vectors(atlas, collection, vectors, output_table_type="snapshot")

    docs = {d["doc_id"]: d for d in atlas.documents(collection)}
    assert set(docs) == set(vectors)
    for doc_id, vector in vectors.items():
        stored = docs[doc_id]["embedding"]
        assert isinstance(stored, list)
        assert len(stored) == VECTOR_DIM
        assert all(isinstance(x, float) for x in stored)
        np.testing.assert_allclose(stored, vector, rtol=1e-12)
    # snapshot mode keys documents by the Pathway row id, no diff/time fields
    assert "diff" not in docs[0]
    assert "time" not in docs[0]


def test_vector_search_roundtrip(atlas: AtlasContext) -> None:
    """Written vectors are retrievable through an Atlas ``$vectorSearch`` query."""
    collection = atlas.collection_name()
    rng = np.random.default_rng(7)
    corpus = {i: rng.standard_normal(VECTOR_DIM) for i in range(64)}
    target = rng.standard_normal(VECTOR_DIM)
    target_id = 9999
    corpus[target_id] = target

    _write_vectors(atlas, collection, corpus, output_table_type="snapshot")
    atlas.create_vector_index(collection, dimensions=VECTOR_DIM, similarity="cosine")

    # Query with the exact target vector — it must come back first (score ~1.0).
    hits = atlas.vector_search(collection, target, limit=3)
    assert hits, "vector search returned no results"
    assert hits[0]["doc_id"] == target_id
    assert hits[0]["score"] == pytest.approx(1.0, abs=1e-3)


def test_stream_of_changes_vectors(atlas: AtlasContext) -> None:
    """stream_of_changes mode preserves vectors and adds diff/time metadata."""
    collection = atlas.collection_name()
    rng = np.random.default_rng(123)
    vectors = {i: rng.standard_normal(VECTOR_DIM) for i in range(4)}

    _write_vectors(atlas, collection, vectors, output_table_type="stream_of_changes")

    docs = atlas.documents(collection)
    assert len(docs) == len(vectors)
    for doc in docs:
        assert doc["diff"] == 1
        assert "time" in doc
        assert len(doc["embedding"]) == VECTOR_DIM


def test_float32_vectors_are_written_as_doubles(atlas: AtlasContext) -> None:
    """float32 embeddings widen to BSON doubles without loss of dimensionality."""
    collection = atlas.collection_name()
    rng = np.random.default_rng(5)
    vectors = {i: rng.standard_normal(VECTOR_DIM).astype(np.float32) for i in range(3)}

    _write_vectors(atlas, collection, vectors, output_table_type="snapshot")

    docs = {d["doc_id"]: d for d in atlas.documents(collection)}
    for doc_id, vector in vectors.items():
        stored = docs[doc_id]["embedding"]
        assert len(stored) == VECTOR_DIM
        np.testing.assert_allclose(stored, vector.astype(np.float64), rtol=1e-6)


# Worker script for the multi-worker snapshot test. Run as a subprocess under
# `PATHWAY_THREADS=N` so the MongoDB sink (now `single_threaded = false`) shards
# output across N workers. It inserts keyed rows, updates a quarter, and deletes
# a quarter, then writes the net state in snapshot mode.
_MULTIWORKER_SNAPSHOT_WORKER = """
import sys
import numpy as np
import pandas as pd
import pathway as pw
from pathway.internals.parse_graph import G

uri, database, collection, n_str, dim_str = sys.argv[1:6]
n, dim = int(n_str), int(dim_str)

updated = {k for k in range(n) if k % 4 == 0}
deleted = {k for k in range(n) if k % 4 == 1}

rows = []
for k in range(n):
    rows.append({"k": k, "tag": f"v{k}", "__time__": 2, "__diff__": 1})
for k in sorted(updated):                       # retract old, insert new value
    rows.append({"k": k, "tag": f"v{k}", "__time__": 4, "__diff__": -1})
    rows.append({"k": k, "tag": f"v{k}_upd", "__time__": 4, "__diff__": 1})
for k in sorted(deleted):                        # retract -> document removed
    rows.append({"k": k, "tag": f"v{k}", "__time__": 6, "__diff__": -1})

# Deterministic per-key embedding so the parent can verify values too.
def emb(k):
    return list(np.random.default_rng(k).standard_normal(dim))

df = pd.DataFrame(rows)
df["embedding"] = df["k"].map(emb)

G.clear()
table = pw.debug.table_from_pandas(df).with_id_from(pw.this.k)
pw.io.mongodb.write(
    table,
    connection_string=uri,
    database=database,
    collection=collection,
    output_table_type="snapshot",
)
pw.run(monitoring_level=pw.MonitoringLevel.NONE)
"""


def test_large_one_shot_bulk_load(atlas: AtlasContext) -> None:
    """A large single-minibatch write must not hit MongoDB's per-batch limits.

    A static `table_from_pandas` run flushes the whole table in one go. With 120k
    736-dim vectors this crosses both the 48 MB max-message size (~8k vectors)
    and the 100,000-operations-per-batch limit, so it verifies the connector lets
    the driver split the load into valid sub-batches instead of erroring.
    """
    collection = atlas.collection_name()
    n = 120_000
    rng = np.random.default_rng(0)
    df = pd.DataFrame(
        {
            "doc_id": np.arange(n),
            "embedding": list(rng.standard_normal((n, VECTOR_DIM))),
        }
    )

    G.clear()
    table = pw.debug.table_from_pandas(df).with_id_from(pw.this.doc_id)
    pw.io.mongodb.write(
        table,
        connection_string=atlas.connection_string,
        database=DATABASE,
        collection=collection,
        output_table_type="snapshot",
    )
    pw.run(monitoring_level=pw.MonitoringLevel.NONE)

    assert atlas.collection(collection).count_documents({}) == n


@pytest.mark.parametrize("workers", [4, 8])
def test_multiworker_snapshot_applies_updates_and_deletes(
    atlas: AtlasContext, tmp_path, workers: int
) -> None:
    """With the sink parallelized, snapshot output across N workers still lands
    in the exact expected state: updates reflected, deletions removed.

    This is the correctness guard for `MongoWriter::single_threaded() == false`.
    The engine shards output by row key, so every event for a given `_id`
    (insert, the retract+insert of an update, and a delete) is owned by one
    worker — no two workers ever race on the same document.
    """
    collection = atlas.collection_name()
    n, dim = 400, 16
    updated = {k for k in range(n) if k % 4 == 0}
    deleted = {k for k in range(n) if k % 4 == 1}

    worker_script = tmp_path / "mw_worker.py"
    worker_script.write_text(_MULTIWORKER_SNAPSHOT_WORKER)
    env = dict(os.environ, PATHWAY_THREADS=str(workers))
    result = subprocess.run(
        [
            sys.executable,
            str(worker_script),
            atlas.connection_string,
            DATABASE,
            collection,
            str(n),
            str(dim),
        ],
        env=env,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, f"worker failed:\n{result.stderr[-3000:]}"

    docs = {d["k"]: d for d in atlas.documents(collection)}
    expected_keys = {k for k in range(n) if k not in deleted}
    assert set(docs) == expected_keys, (
        f"key set mismatch with {workers} workers: "
        f"missing={sorted(expected_keys - set(docs))[:5]} "
        f"extra={sorted(set(docs) - expected_keys)[:5]}"
    )
    for k, doc in docs.items():
        assert doc["tag"] == (f"v{k}_upd" if k in updated else f"v{k}")
        assert len(doc["embedding"]) == dim
    # deleted documents are genuinely gone, not lingering
    assert not (set(docs) & deleted)
