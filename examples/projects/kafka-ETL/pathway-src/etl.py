# Copyright © 2024 Pathway

import time

import pathway as pw

rdkafka_settings = {
    "bootstrap.servers": "kafka:9092",
    "security.protocol": "plaintext",
    "group.id": "0",
    "session.timeout.ms": "6000",
    "auto.offset.reset": "earliest",
}

str_repr = "%Y-%m-%d %H:%M:%S.%f %z"


class InputStreamSchema(pw.Schema):
    date: str
    message: str


timestamps_timezone_1 = pw.io.kafka.read(
    rdkafka_settings,
    topic="timezone1",
    format="json",
    schema=InputStreamSchema,
    autocommit_duration_ms=100,
)

timestamps_timezone_2 = pw.io.kafka.read(
    rdkafka_settings,
    topic="timezone2",
    format="json",
    schema=InputStreamSchema,
    autocommit_duration_ms=100,
)


def convert_to_timestamp(table: pw.Table) -> pw.Table:
    table = table.select(
        date=pw.this.date.dt.strptime(fmt=str_repr, contains_timezone=True),
        message=pw.this.message,
    )
    table_timestamp = table.select(
        timestamp=pw.this.date.dt.timestamp(unit="ms"),
        message=pw.this.message,
    )
    return table_timestamp


timestamps_timezone_1 = convert_to_timestamp(timestamps_timezone_1)
timestamps_timezone_2 = convert_to_timestamp(timestamps_timezone_2)

timestamps_unified = timestamps_timezone_1.concat_reindex(timestamps_timezone_2)

pw.io.kafka.write(
    timestamps_unified, rdkafka_settings, topic_name="unified_timestamps", format="json"
)

# We wait for Kafka to be ready.
time.sleep(20)

# We launch the computation.
pw.run()
