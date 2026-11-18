# Copyright © 2026 Pathway

import json
import uuid

import pytest
import requests

import pathway as pw
from pathway.internals.parse_graph import G
from pathway.tests.utils import read_jsonlines

from .utils import SCHEMA_REGISTRY_BASE_ROUTE


@pytest.mark.parametrize("valid", [True, False])
def test_json_schema_registry_external_reference(valid, kafka_context, tmp_path):
    referenced_subject = f"value-{uuid.uuid4()}"
    subject = f"row-{uuid.uuid4()}"
    reference_url = "https://schemas.example.test/value.json"
    value_schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "$id": reference_url,
        "type": "object",
        "properties": {"name": {"type": "string"}},
        "required": ["name"],
        "additionalProperties": False,
    }
    row_schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "$id": "https://schemas.example.test/row.json",
        "type": "object",
        "properties": {
            "payload": {"$ref": reference_url},
            "time": {"type": "integer"},
            "diff": {"type": "integer"},
        },
        "required": ["payload", "time", "diff"],
        "additionalProperties": False,
    }
    for name, schema, references in [
        (referenced_subject, value_schema, []),
        (
            subject,
            row_schema,
            [{"name": reference_url, "subject": referenced_subject, "version": 1}],
        ),
    ]:
        requests.post(
            f"{SCHEMA_REGISTRY_BASE_ROUTE}/subjects/{name}/versions",
            json={
                "schemaType": "JSON",
                "schema": json.dumps(schema),
                "references": references,
            },
            timeout=10,
        ).raise_for_status()

    class Schema(pw.Schema):
        payload: pw.Json

    payloads = [pw.Json({"name": str(i) if valid else i}) for i in range(3)]
    table = pw.debug.table_from_rows(Schema, [(payload,) for payload in payloads])
    settings = pw.io.kafka.SchemaRegistrySettings(urls=[SCHEMA_REGISTRY_BASE_ROUTE])
    try:
        pw.io.kafka.write(
            table,
            rdkafka_settings=kafka_context.default_rdkafka_settings(),
            topic_name=kafka_context.output_topic,
            format="json",
            schema_registry_settings=settings,
            subject=subject,
        )
        if not valid:
            # Validation must follow the referenced schema, not just validate
            # the outer record or silently accept an unresolved reference.
            with pytest.raises(pw.engine.EngineError, match="(?i)valid|schema"):
                pw.run()
            return
        pw.run()

        G.clear()
        table = pw.io.kafka.read(
            rdkafka_settings=kafka_context.default_rdkafka_settings(),
            topic=kafka_context.output_topic,
            format="json",
            schema=Schema,
            mode="static",
            schema_registry_settings=settings,
        )
        output_path = tmp_path / "output.jsonl"
        pw.io.jsonlines.write(table, output_path)
        pw.run()
        assert sorted(
            row["payload"]["name"] for row in read_jsonlines(output_path)
        ) == ["0", "1", "2"]
    finally:
        for name in [subject, referenced_subject]:
            requests.delete(
                f"{SCHEMA_REGISTRY_BASE_ROUTE}/subjects/{name}", timeout=10
            ).raise_for_status()
