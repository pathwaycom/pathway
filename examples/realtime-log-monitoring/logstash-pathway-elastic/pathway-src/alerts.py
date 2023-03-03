import time
from datetime import datetime

from dateutil import parser

import pathway as pw

alert_threshold = 5
sliding_window_duration = 1

rdkafka_settings = {
    "bootstrap.servers": "kafka:9092",
    "security.protocol": "plaintext",
    "group.id": "0",
    "session.timeout.ms": "6000",
}


def convert_timestamp(datestring):
    yourdate = parser.parse(datestring)
    return datetime.timestamp(yourdate)


# We use the Kafka connector to listen to the "logs" topic
# We only need the timestamp and the message
t_logs = pw.kafka.read(
    rdkafka_settings,
    topic_names=["logs"],
    format="json",
    value_columns=[
        "@timestamp",
        "message",
    ],
    autocommit_duration_ms=100,
)
t_logs = t_logs.select(timestamp=pw.this["@timestamp"], log=pw.this.message)
t_logs = t_logs.select(
    pw.this.log,
    timestamp=pw.apply_with_type(convert_timestamp, float, pw.this.timestamp),
)

t_latest_log = t_logs.reduce(last_log=pw.reducers.max(pw.this.timestamp))


t_sliding_window = t_logs.filter(
    pw.this.timestamp >= t_latest_log.ix_ref().last_log - sliding_window_duration
)
t_alert = t_sliding_window.reduce(count=pw.reducers.count())
t_alert = t_alert.select(
    alert=pw.this.count >= alert_threshold, latest_update=t_latest_log.ix_ref().last_log
)
t_alert = t_alert.select(pw.this.alert)

time.sleep(10)

pw.elasticsearch.write(
    t_alert,
    "http://elasticsearch:9200",
    auth=pw.elasticsearch.ElasticSearchAuth.basic("elastic", "password"),
    index_name="alerts",
)

# We launch the computation.
pw.run()
