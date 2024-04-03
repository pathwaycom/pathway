# Copyright © 2024 Pathway

import time
from datetime import timedelta

import requests

import pathway as pw

alert_threshold = 5
sliding_window_duration = timedelta(seconds=1)


SLACK_ALERT_CHANNEL_ID = "XXX"
SLACK_ALERT_TOKEN = "XXX"

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


def on_alert_event(key, row, time, is_addition):
    alert_message = "Alert '{}' changed state to {}".format(
        row["alert"],
        "ACTIVE" if is_addition else "INACTIVE",
    )
    requests.post(
        "https://slack.com/api/chat.postMessage",
        data="text={}&channel={}".format(alert_message, SLACK_ALERT_CHANNEL_ID),
        headers={
            "Authorization": "Bearer {}".format(SLACK_ALERT_TOKEN),
            "Content-Type": "application/x-www-form-urlencoded",
        },
    ).raise_for_status()


pw.io.subscribe(t_alert, on_alert_event)

time.sleep(5)
# We launch the computation.
pw.run()
