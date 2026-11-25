# Copyright © 2026 Pathway

import doctest
import inspect
import re
import textwrap
from typing import Any

import pytest

import pathway as pw
from pathway.tests.utils import assert_table_equality_wo_index


@pytest.mark.parametrize(
    "connector",
    [
        "csv",
        "jsonlines",
        "fs",
        "kafka",
        "python",
        "debezium",
        "deltalake",
        "s3",
        "nats",
        "mqtt",
        "rabbitmq",
        "kinesis",
        "http",
        "elasticsearch",
        "sqlite",
        "postgres",
        "mysql",
        "mssql",
    ],
)
@pytest.mark.parametrize("clean_docstring", [False, True])
def test_nested_schema_read_docstring_example(
    connector, clean_docstring, tmp_path, monkeypatch
):
    reader = getattr(pw.io, connector).read
    # Python 3.13+ can strip the common indentation when compiling docstrings.
    doc = inspect.cleandoc(reader.__doc__) if clean_docstring else reader.__doc__
    examples = doctest.DocTestParser().get_examples(doc)
    start = next(
        i
        for i, example in enumerate(examples)
        if example.source.startswith("class Payload")
    )
    namespace: dict[str, Any] = {
        "pw": pw,
        "__name__": __name__,
        "rdkafka_settings": {
            "bootstrap.servers": "localhost:9092",
            "group.id": "nested-doc-test",
        },
    }
    monkeypatch.chdir(tmp_path)
    if connector in ("csv", "jsonlines"):
        match = re.search(r"\.\. code-block:: (?:json|text)\n\n((?: +.+\n)+)", doc)
        assert match is not None
        suffix = "csv" if connector == "csv" else "jsonl"
        (tmp_path / f"nested.{suffix}").write_text(textwrap.dedent(match.group(1)))
    elif connector == "fs":
        (tmp_path / "nested.jsonl").write_text('{"payload": {"price": 12.5}}\n')
    elif connector == "deltalake":
        import pyarrow as pa
        from deltalake import write_deltalake

        write_deltalake(
            "nested_delta",
            pa.Table.from_pylist(
                [{"key": 1, "payload": {"price": 12.5, "content": b"\x01\x02"}}]
            ),
        )
    for example in examples[start:]:
        exec(example.source, namespace)
        if example.source.startswith("result ="):
            break
    result = namespace["result"]
    rows: list[tuple[Any, ...]]
    assert namespace["NestedInput"].typehints()["payload"] is pw.Json
    if connector not in ("fs", "kafka"):
        schema = pw.schema_from_types(price=float, content=bytes, currency=str)
        rows = [(12.5, b"\x01\x02", "USD")]
    else:
        schema = pw.schema_from_types(price=float, currency=str)
        rows = [(12.5, "USD")]
    assert result.schema.typehints() == schema.typehints()
    if connector in ("csv", "jsonlines", "python", "deltalake", "fs"):
        assert_table_equality_wo_index(result, pw.debug.table_from_rows(schema, rows))
