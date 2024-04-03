# Copyright © 2024 Pathway

import pathway as pw


# DO NOT MODIFY WITHOUT MODIFYING THE EXAMPLE AT:
# public/pathway/examples/projects/realtime-log-monitoring/filebeat-pathway-slack/pathway-src/alerts.py
def test():
    alert_threshold = 5
    sliding_window_duration = 1_000_000_000

    inputSchema = pw.schema_builder(
        columns={
            "@timestamp": pw.column_definition(dtype=str),
            "message": pw.column_definition(dtype=str),
        }
    )

    log_table = pw.debug.table_from_markdown(
        """
        @timestamp | message
        2023-12-04T07:00:01.000000Z          | "1"
        2023-12-04T07:00:02.000000Z          | "2"
        2023-12-04T07:00:03.000000Z          | "3"
        2023-12-04T07:00:04.000000Z          | "4"
        2023-12-04T07:00:05.000000Z          | "5"
        2023-12-04T07:00:05.100000Z          | "6"
        2023-12-04T07:00:05.200000Z          | "7"
        2023-12-04T07:00:05.300000Z          | "8"
        2023-12-04T07:00:05.400000Z          | "9"
        2023-12-04T07:00:05.500000Z          | "10"
""",
        schema=inputSchema,
    )

    log_table = log_table.select(timestamp=pw.this["@timestamp"], log=pw.this.message)
    log_table = log_table.select(
        pw.this.log,
        timestamp=pw.this.timestamp.dt.strptime("%Y-%m-%dT%H:%M:%S.%fZ").dt.timestamp(),
    )

    t_latest_log = log_table.reduce(last_log=pw.reducers.max(pw.this.timestamp))

    t_sliding_window = log_table.filter(
        pw.this.timestamp >= t_latest_log.ix_ref().last_log - sliding_window_duration
    )
    t_alert = t_sliding_window.reduce(count=pw.reducers.count())
    t_alert = t_alert.select(
        alert=pw.this.count >= alert_threshold,
        latest_update=t_latest_log.ix_ref().last_log,
    )
    t_alert = t_alert.select(pw.this.alert)

    results = []

    def on_alert_event(key, row, time, is_addition):
        alert_message = "Alert '{}' changed state to {}".format(
            row["alert"],
            "ACTIVE" if is_addition else "INACTIVE",
        )
        results.append(alert_message)

    pw.io.subscribe(t_alert, on_alert_event)
    pw.run()

    assert results == ["Alert 'True' changed state to ACTIVE"]
