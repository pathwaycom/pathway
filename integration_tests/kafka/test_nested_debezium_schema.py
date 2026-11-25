# Copyright © 2026 Pathway

import json
from uuid import uuid4

import pytest

import pathway as pw
from pathway.tests.utils import FileLinesNumberChecker, wait_result_with_checker


@pytest.mark.parametrize("mongodb", [False, True])
@pytest.mark.parametrize("json_text", [False, True])
def test_nested_debezium_changes(kafka_context, tmp_path, mongodb, json_text):
    class Payload(pw.Schema):
        price: float
        content: bytes
        currency: str = pw.column_definition(default_value="USD")

    class Input(pw.Schema):
        key: int = pw.column_definition(primary_key=True)
        payload: Payload

    def image(key, price):
        payload = {"price": price, "content": "AQI="}
        row = {"key": key, "payload": json.dumps(payload) if json_text else payload}
        return json.dumps(row) if mongodb else row

    for key, op, before, after in [
        (1, "r", None, image(1, 1.5)),
        (1, "u", image(1, 1.5), image(1, 2.5)),
        (1, "d", image(1, 2.5), None),
        (3, "c", None, image(3, "bad")),
        (2, "c", None, image(2, 3.5)),
    ]:
        kafka_context.send(
            (
                json.dumps({"payload": {"key": key}}),
                json.dumps({"payload": {"op": op, "before": before, "after": after}}),
            )
        )
    kafka_context.flush()
    table = pw.io.debezium.read(
        {
            "bootstrap.servers": "kafka:9092",
            "group.id": str(uuid4()),
            "auto.offset.reset": "earliest",
        },
        kafka_context.input_topic,
        db_type=(
            pw.io.debezium.DebeziumDBType.MONGO_DB
            if mongodb
            else pw.io.debezium.DebeziumDBType.POSTGRES
        ),
        schema=Input,
        autocommit_duration_ms=20,
    ).remove_errors()
    output = tmp_path / "output.jsonl"
    pw.io.jsonlines.write(
        table.select(
            key=table.key,
            price=table.payload.price,
            content=table.payload.content,
            currency=table.payload.currency,
        ),
        output,
    )

    def complete():
        if not FileLinesNumberChecker(output, 1)():
            return False
        rows = [json.loads(line) for line in output.read_text().splitlines()]
        balance: dict[tuple[int, float, str, str], int] = {}
        for row in rows:
            value = (row["key"], row["price"], row["content"], row["currency"])
            balance[value] = balance.get(value, 0) + row["diff"]
        return {key: diff for key, diff in balance.items() if diff} == {
            (2, 3.5, "AQI=", "USD"): 1
        }

    wait_result_with_checker(complete, 60, kwargs={"terminate_on_error": False})
