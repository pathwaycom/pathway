# Copyright © 2024 Pathway

import time
from datetime import timedelta

import pathway as pw

# To use advanced features with Pathway Scale, get your free license key from
# https://pathway.com/features and paste it below.
# To use Pathway Community, comment out the line below.
pw.set_license_key("demo-license-key-with-telemetry")

alert_threshold = 5
sliding_window_duration = timedelta(seconds=1)

rdkafka_settings = {
    "bootstrap.servers": "kafka:9092",
    "security.protocol": "plaintext",
    "group.id": "0",
    "session.timeout.ms": "6000",
}

inputSchema = pw.schema_builder(
    columns={
        "@timestamp": pw.column_definition(dtype=str),
        "message": pw.column_definition(dtype=str),
    }
)

# We use the Kafka connector to listen to the "logs" topic
# We only need the timestamp and the message
log_table = pw.io.kafka.read(
    rdkafka_settings,
    topic="logs",
    format="json",
    schema=inputSchema,
    autocommit_duration_ms=100,
)
log_table = log_table.select(timestamp=pw.this["@timestamp"], log=pw.this.message)
log_table = log_table.select(
    pw.this.log,
    timestamp=pw.this.timestamp.dt.strptime("%Y-%m-%dT%H:%M:%S.%fZ"),
)

t_sliding_window = log_table.windowby(
    log_table.timestamp,
    window=pw.temporal.sliding(
        hop=timedelta(milliseconds=10), duration=sliding_window_duration
    ),
    behavior=pw.temporal.common_behavior(
        cutoff=timedelta(seconds=0.1),
        keep_results=False,
    ),
).reduce(timestamp=pw.this._pw_window_end, count=pw.reducers.count())

t_alert = t_sliding_window.reduce(count=pw.reducers.max(pw.this.count)).select(
    alert=pw.this.count >= alert_threshold
)

pw.io.elasticsearch.write(
    t_alert,
    "http://elasticsearch:9200",
    auth=pw.io.elasticsearch.ElasticSearchAuth.basic("elastic", "password"),
    index_name="alerts",
)

time.sleep(5)
# We launch the computation.
pw.run()
