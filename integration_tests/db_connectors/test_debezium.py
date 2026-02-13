import functools
import json
import pathlib
import threading
import time

import pytest
import requests
from utils import DEBEZIUM_CONNECTOR_URL, KAFKA_SETTINGS

import pathway as pw
from pathway.tests.utils import wait_result_with_checker

INPUT_COLLECTION_SIZE = 30


@functools.cache
def is_debezium_reachable() -> bool:
    for _ in range(300):
        try:
            r = requests.get(DEBEZIUM_CONNECTOR_URL, timeout=60)
            is_ok = r.status_code is not None
        except Exception as e:
            print(f"Debezium is unreachable: {e}")
            time.sleep(1.0)
            continue
        if is_ok:
            return True
    return False


# FIXME: the debezium Docker container used in the integration tests is unstable and
# sometimes is unavailable, even though the healthcheck passes
xfail_if_debezium_failed_to_start = pytest.mark.xfail(
    not is_debezium_reachable(), reason="debezium has failed to start"
)


class SumChecker:
    def __init__(self, output_path: pathlib.Path, expected_sum: int):
        self.output_path = output_path
        self.expected_sum = expected_sum

    def __call__(self) -> bool:
        with open(self.output_path, "r") as f:
            try:
                last_row = None
                for row in f:
                    data = json.loads(row)
                    if data["sum"] == self.expected_sum:
                        return True
                    last_row = data
                print(f"Expected output not found. Last row: {last_row}")
                return False
            except Exception as e:
                print(f"Expected output not found: {e}")
            return False


@xfail_if_debezium_failed_to_start
@pytest.mark.flaky(reruns=5)
def test_debezium_mongodb(tmp_path, mongodb, debezium):
    topic_prefix = debezium.register_mongodb()
    collection = mongodb.generate_collection_name()
    output_path = tmp_path / "output.jsonl"
    expected_sum = sum([x for x in range(INPUT_COLLECTION_SIZE)])

    def stream_data():
        time.sleep(10)  # Allow Kafka to start in full
        for value in range(INPUT_COLLECTION_SIZE):
            mongodb.insert_document(collection, {"value": value})
            time.sleep(0.5)

    class InputSchema(pw.Schema):
        value: int

    table = pw.io.debezium.read(
        KAFKA_SETTINGS,
        topic_name=f"{topic_prefix}{collection}",
        schema=InputSchema,
        autocommit_duration_ms=100,
    )
    table = table.reduce(sum=pw.reducers.sum(table.value))
    pw.io.jsonlines.write(table, output_path)

    inputs_thread = threading.Thread(target=stream_data, daemon=True)
    inputs_thread.start()
    wait_result_with_checker(SumChecker(output_path, expected_sum), 180, step=1.0)


@xfail_if_debezium_failed_to_start
@pytest.mark.xfail(reason="needs investigation")
def test_debezium_postgres(tmp_path, postgres, debezium):
    class InputSchema(pw.Schema):
        value: int

    table_name = postgres.create_table(InputSchema, add_special_fields=False)
    topic_name = debezium.register_postgres(table_name)
    output_path = tmp_path / "output.jsonl"
    expected_sum = sum([x for x in range(INPUT_COLLECTION_SIZE)])

    def stream_data():
        time.sleep(10)  # Allow Kafka to start in full
        for value in range(INPUT_COLLECTION_SIZE):
            postgres.insert_row(table_name, {"value": value})
            time.sleep(0.5)

    table = pw.io.debezium.read(
        KAFKA_SETTINGS,
        topic_name=topic_name,
        schema=InputSchema,
        autocommit_duration_ms=100,
    )
    table = table.reduce(sum=pw.reducers.sum(table.value))
    pw.io.jsonlines.write(table, output_path)

    inputs_thread = threading.Thread(target=stream_data, daemon=True)
    inputs_thread.start()
    wait_result_with_checker(SumChecker(output_path, expected_sum), 180, step=1.0)
