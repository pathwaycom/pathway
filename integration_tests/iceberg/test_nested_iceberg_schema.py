# Copyright © 2026 Pathway

import datetime
import os
from uuid import uuid4

import pandas as pd
import pyarrow as pa
import pytest
from pyiceberg.catalog import load_catalog

import pathway as pw
from pathway.tests.utils import assert_table_equality_wo_index


@pytest.mark.parametrize("plain_json", [False, True])
@pytest.mark.parametrize("container", ["struct", "map"])
def test_nested_iceberg_native_struct(plain_json, container):
    uri = os.environ.get("ICEBERG_CATALOG_URI", "http://iceberg:8181")
    catalog = load_catalog(name="default", uri=uri)
    namespace = f"nested_{uuid4().hex}"
    catalog.create_namespace(namespace)
    native_type = pa.struct(
        [
            (
                "two",
                pa.struct(
                    [
                        ("content", pa.binary()),
                        ("three", pa.float64()),
                        ("at", pa.timestamp("us", tz="UTC")),
                        ("span", pa.int64()),
                    ]
                ),
            )
        ]
    )
    if container == "map":
        native_type = pa.map_(pa.string(), native_type.field("two").type)
    arrow_schema = pa.schema(
        [pa.field("key", pa.int64(), nullable=False), pa.field("one", native_type)]
    )
    source = catalog.create_table(f"{namespace}.events", schema=arrow_schema)
    try:
        source.append(
            pa.Table.from_pylist(
                [
                    {
                        "key": 1,
                        "one": {
                            "two": {
                                "three": 12.5,
                                "content": b"ok",
                                "at": datetime.datetime(
                                    2024, 1, 2, tzinfo=datetime.timezone.utc
                                ),
                                "span": 123,
                            }
                        },
                    }
                ],
                schema=arrow_schema,
            )
        )

        class Leaf(pw.Schema):
            three: float
            content: bytes
            at: pw.DateTimeUtc
            span: pw.Duration
            currency: str = pw.column_definition(default_value="USD")

        schema = pw.schema_builder(
            {
                "key": pw.column_definition(dtype=int, primary_key=True),
                "one": pw.column_definition(
                    dtype=pw.Json if plain_json else pw.schema_from_types(two=Leaf)
                ),
            }
        )
        table = pw.io.iceberg.read(
            pw.io.iceberg.RestCatalog(uri=uri),
            [namespace],
            "events",
            schema=schema,
            mode="static",
        )
        if plain_json:
            result = table.select(value=table.one.two.three.as_float())
            expected = pw.debug.table_from_rows(
                pw.schema_from_types(value=float | None), [(12.5,)]
            )
        else:
            result = table.select(
                price=table.one.two.three,
                content=table.one.two.content,
                at=table.one.two.at,
                span=table.one.two.span,
                currency=table.one.two.currency,
            )
            expected = pw.debug.table_from_rows(
                pw.schema_from_types(
                    price=float,
                    content=bytes,
                    at=pw.DateTimeUtc,
                    span=pw.Duration,
                    currency=str,
                ),
                [
                    (
                        12.5,
                        b"ok",
                        pd.Timestamp("2024-01-02T00:00:00Z"),
                        pd.Timedelta(123, unit="us"),
                        "USD",
                    )
                ],
            )
        assert result.schema.typehints() == expected.schema.typehints()
        assert_table_equality_wo_index(result, expected)
    finally:
        catalog.drop_table(f"{namespace}.events")
        catalog.drop_namespace(namespace)


def test_nested_iceberg_docstring_example():
    import doctest
    from typing import Any

    uri = os.environ.get("ICEBERG_CATALOG_URI", "http://iceberg:8181")
    catalog = load_catalog(name="default", uri=uri)
    namespace = f"nested_doc_{uuid4().hex}"
    catalog.create_namespace(namespace)
    data = pa.Table.from_pylist(
        [{"key": 1, "payload": {"price": 12.5, "content": b"\x01\x02"}}]
    )
    source = catalog.create_table(f"{namespace}.orders", schema=data.schema)
    try:
        source.append(data)
        doc = pw.io.iceberg.read.__doc__
        assert doc is not None
        examples = doctest.DocTestParser().get_examples(doc)
        start = next(
            i
            for i, example in enumerate(examples)
            if example.source.startswith("class Payload")
        )
        values: dict[str, Any] = {
            "pw": pw,
            "__name__": __name__,
            "catalog": pw.io.iceberg.RestCatalog(uri=uri),
            "namespace": [namespace],
        }
        for example in examples[start:]:
            exec(example.source, values)
            if example.source.startswith("result ="):
                break
        result = values["result"]
        expected = pw.schema_from_types(price=float, content=bytes, currency=str)
        assert result.schema.typehints() == expected.typehints()
        assert_table_equality_wo_index(
            result,
            pw.debug.table_from_rows(expected, [(12.5, b"\x01\x02", "USD")]),
        )
    finally:
        catalog.drop_table(f"{namespace}.orders")
        catalog.drop_namespace(namespace)
