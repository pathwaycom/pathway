# Copyright © 2024 Pathway

from uuid import uuid4

import pathway as pw

rdkafka_settings = {
    "bootstrap.servers": "kafka:9092",
    "security.protocol": "plaintext",
    "group.id": str(uuid4()),
    "session.timeout.ms": "6000",
    "auto.offset.reset": "earliest",
}
topic_name = "unified_timestamps"


class InputStreamSchema(pw.Schema):
    timestamp: float
    message: str


def read_results():
    table = pw.io.kafka.read(
        rdkafka_settings,
        topic=topic_name,
        schema=InputStreamSchema,
        format="json",
        autocommit_duration_ms=100,
    )
    pw.io.csv.write(table, "./results.csv")
    pw.run()


if __name__ == "__main__":
    read_results()
