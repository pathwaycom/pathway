# Copyright © 2024 Pathway

from base import base_path, s3_connection_settings

import pathway as pw

topic_name = "timezone_unified"


class InputStreamSchema(pw.Schema):
    timestamp: float
    message: str


def read_results():
    table = pw.io.deltalake.read(
        base_path + "timezone_unified",
        schema=InputStreamSchema,
        autocommit_duration_ms=100,
        s3_connection_settings=s3_connection_settings,
    )
    pw.io.csv.write(table, "./results.csv")
    pw.run(monitoring_level=pw.MonitoringLevel.NONE)


if __name__ == "__main__":
    read_results()
