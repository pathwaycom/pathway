# Copyright © 2024 Pathway

from base import base_path, s3_connection_settings, str_repr

import pathway as pw


class InputStreamSchema(pw.Schema):
    date: str
    message: str


timestamps_timezone_1 = pw.io.deltalake.read(
    base_path + "timezone1",
    schema=InputStreamSchema,
    s3_connection_settings=s3_connection_settings,
    autocommit_duration_ms=100,
)

timestamps_timezone_2 = pw.io.deltalake.read(
    base_path + "timezone2",
    schema=InputStreamSchema,
    s3_connection_settings=s3_connection_settings,
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

pw.io.deltalake.write(
    timestamps_unified,
    base_path + "timezone_unified",
    s3_connection_settings=s3_connection_settings,
    min_commit_frequency=100,
)

# We launch the computation.
pw.run(monitoring_level=pw.MonitoringLevel.NONE)
