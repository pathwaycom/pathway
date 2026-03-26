# Copyright © 2026 Pathway
#
# Milvus field-type coverage tests.
#
# Supported Pathway → Milvus mappings (tested as round-trips):
#   int         → INT64
#   float       → DOUBLE
#   str         → VARCHAR
#   bool        → BOOL
#   pw.Json     → JSON
#   list[float] → FLOAT_VECTOR  (covered in test_milvus.py)
#   bytes       → BINARY_VECTOR
#
# Unsupported types raise TypeError before reaching pymilvus (tested below).
# Multi-dimensional numpy arrays raise ValueError (tested below).

import datetime

import pytest
from pymilvus import DataType
from utils import MILVUS_VECTOR_DIM

import pathway as pw
from pathway.internals.parse_graph import G
from pathway.tests.utils import run

_VEC = [1.0, 2.0, 3.0]


def _make_scalar_schema(value_annotation):
    """Build a pw.Schema with id (PK int), value (<annotation>), vec (list[float])."""
    return pw.schema_builder(
        {
            "id": pw.column_definition(dtype=int, primary_key=True),
            "value": pw.column_definition(dtype=value_annotation),
            "vec": pw.column_definition(dtype=list[float]),
        }
    )


def _write_scalar_and_run(milvus, collection_name, schema, value):
    G.clear()
    table = pw.debug.table_from_rows(schema, [(1, value, _VEC)])
    pw.io.milvus.write(
        table,
        uri=milvus.uri,
        collection_name=collection_name,
        primary_key=table.id,
    )
    run()


@pytest.mark.parametrize(
    "pathway_type, milvus_type, milvus_kwargs, value, expected",
    [
        pytest.param(int, DataType.INT64, {}, 42, 42, id="int"),
        pytest.param(float, DataType.DOUBLE, {}, 3.14, 3.14, id="float"),
        pytest.param(
            str, DataType.VARCHAR, {"max_length": 255}, "hello", "hello", id="str"
        ),
        pytest.param(bool, DataType.BOOL, {}, True, True, id="bool"),
        # pw.Json columns accept plain Python dicts/lists; the connector unwraps
        # the internal pw.Json wrapper before forwarding to pymilvus.
        pytest.param(
            pw.Json,
            DataType.JSON,
            {},
            {"key": "val", "n": 1},
            {"key": "val", "n": 1},
            id="json",
        ),
    ],
)
def test_scalar_type_roundtrip(
    milvus, pathway_type, milvus_type, milvus_kwargs, value, expected
):
    """Each Pathway scalar type round-trips through the corresponding Milvus field."""
    collection_name = milvus.generate_collection_name()
    milvus.create_scalar_collection(collection_name, milvus_type, **milvus_kwargs)

    schema = _make_scalar_schema(pathway_type)
    _write_scalar_and_run(milvus, collection_name, schema, value)

    result = milvus.query_all(collection_name, ["id", "value"])
    assert result == [{"id": 1, "value": expected}]


def test_bytes_as_binary_vector(milvus):
    """bytes values are written to a BINARY_VECTOR field and read back correctly."""
    collection_name = milvus.generate_collection_name()
    # BINARY_VECTOR dim is the number of bits; 24 bits = 3 bytes.
    schema = milvus.client.create_schema(auto_id=False, enable_dynamic_field=False)
    schema.add_field("id", DataType.INT64, is_primary=True)
    schema.add_field("vec", DataType.BINARY_VECTOR, dim=24)
    index_params = milvus.client.prepare_index_params()
    index_params.add_index("vec", metric_type="HAMMING", index_type="BIN_FLAT")
    milvus.client.create_collection(
        collection_name, schema=schema, index_params=index_params
    )

    pw_schema = pw.schema_builder(
        {
            "id": pw.column_definition(dtype=int, primary_key=True),
            "vec": pw.column_definition(dtype=bytes),
        }
    )

    data = b"\x01\x02\x03"
    G.clear()
    table = pw.debug.table_from_rows(pw_schema, [(1, data)])
    pw.io.milvus.write(
        table,
        uri=milvus.uri,
        collection_name=collection_name,
        primary_key=table.id,
    )
    run()

    result = milvus.query_all(collection_name, ["id", "vec"])
    assert len(result) == 1
    assert result[0]["id"] == 1
    # pymilvus returns BINARY_VECTOR fields as a list of bytes objects.
    assert result[0]["vec"] == [data]


def test_mixed_types(milvus):
    """A collection with multiple columns of different types round-trips correctly."""
    collection_name = milvus.generate_collection_name()

    schema = milvus.client.create_schema(auto_id=False, enable_dynamic_field=False)
    schema.add_field("id", DataType.INT64, is_primary=True)
    schema.add_field("count", DataType.INT64)
    schema.add_field("score", DataType.DOUBLE)
    schema.add_field("label", DataType.VARCHAR, max_length=255)
    schema.add_field("active", DataType.BOOL)
    schema.add_field("meta", DataType.JSON)
    schema.add_field("embedding", DataType.FLOAT_VECTOR, dim=MILVUS_VECTOR_DIM)
    index_params = milvus.client.prepare_index_params()
    index_params.add_index("embedding", metric_type="COSINE", index_type="FLAT")
    milvus.client.create_collection(
        collection_name, schema=schema, index_params=index_params
    )

    pw_schema = pw.schema_builder(
        {
            "id": pw.column_definition(dtype=int, primary_key=True),
            "count": pw.column_definition(dtype=int),
            "score": pw.column_definition(dtype=float),
            "label": pw.column_definition(dtype=str),
            "active": pw.column_definition(dtype=bool),
            "meta": pw.column_definition(dtype=pw.Json),
            "embedding": pw.column_definition(dtype=list[float]),
        }
    )

    rows = [
        (1, 10, 0.5, "alpha", True, {"x": 1}, [1.0, 2.0, 3.0]),
        (2, 20, 1.5, "beta", False, {"x": 2, "y": "z"}, [4.0, 5.0, 6.0]),
        (3, 30, 2.5, "gamma", True, {}, [7.0, 8.0, 9.0]),
    ]

    G.clear()
    table = pw.debug.table_from_rows(pw_schema, rows)
    pw.io.milvus.write(
        table,
        uri=milvus.uri,
        collection_name=collection_name,
        primary_key=table.id,
    )
    run()

    result = milvus.query_all(
        collection_name,
        ["id", "count", "score", "label", "active", "meta", "embedding"],
    )
    result.sort(key=lambda r: r["id"])
    assert result == [
        {
            "id": 1,
            "count": 10,
            "score": 0.5,
            "label": "alpha",
            "active": True,
            "meta": {"x": 1},
            "embedding": [1.0, 2.0, 3.0],
        },
        {
            "id": 2,
            "count": 20,
            "score": 1.5,
            "label": "beta",
            "active": False,
            "meta": {"x": 2, "y": "z"},
            "embedding": [4.0, 5.0, 6.0],
        },
        {
            "id": 3,
            "count": 30,
            "score": 2.5,
            "label": "gamma",
            "active": True,
            "meta": {},
            "embedding": [7.0, 8.0, 9.0],
        },
    ]


def test_incompatible_type_raises(milvus):
    """Writing a value whose Python type is incompatible with the Milvus field raises."""
    collection_name = milvus.generate_collection_name()
    # The collection expects INT64 for the value field.
    milvus.create_scalar_collection(collection_name, DataType.INT64)

    # Pathway schema declares value as str — incompatible with INT64.
    schema = _make_scalar_schema(str)
    G.clear()
    table = pw.debug.table_from_rows(schema, [(1, "not_an_int", _VEC)])
    pw.io.milvus.write(
        table,
        uri=milvus.uri,
        collection_name=collection_name,
        primary_key=table.id,
    )
    with pytest.raises(Exception):
        run()


def test_unsupported_type_raises_clear_error():
    """_prepare_row raises TypeError with a clear message for unsupported Python types."""
    from pathway.io.milvus import _prepare_row

    with pytest.raises(TypeError, match="unsupported type"):
        _prepare_row({"ts": datetime.date(2026, 1, 1)})


def test_multidimensional_ndarray_raises_clear_error():
    """_prepare_row raises ValueError with a clear message for multi-dimensional ndarrays."""
    import numpy as np

    from pathway.io.milvus import _prepare_row

    with pytest.raises(ValueError, match="2-dimensional"):
        _prepare_row({"vec": np.zeros((3, 4))})
