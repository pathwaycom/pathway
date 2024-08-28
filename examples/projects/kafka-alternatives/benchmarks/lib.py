import os
import time

import boto3

import pathway as pw

TEST_BUCKET_NAME = "your-bucket"  # Insert your test S3/Min.io bucket path name
TEST_ENDPOINT = "https://your-endpoint.com"
TEST_REGION = "eu-central-1"


class _MessageTableSubject(pw.io.python.ConnectorSubject):

    def __init__(self, produce_messages):
        super().__init__()
        self.produce_messages = produce_messages

    def send_message(self, value):
        self.next_bytes(value)

    def run(self):
        self.produce_messages(self)


class AdhocProducer:

    def __init__(
        self,
        base_path: str,
        connection_options: pw.io.minio.MinIOSettings,
        produce_messages,
    ):
        self.base_path = base_path
        self.connection_options = connection_options

        class _MessageTableSchema(pw.Schema):
            data: bytes

        self.message_table_subject = _MessageTableSubject(produce_messages)
        self.message_table = pw.io.python.read(
            self.message_table_subject,
            schema=_MessageTableSchema,
            autocommit_duration_ms=1000,
        )
        pw.io.deltalake.write(
            table=self.message_table,
            uri=base_path,
            s3_connection_settings=connection_options,
            min_commit_frequency=1000,
        )

    def start(self):
        pw.run(monitoring_level=pw.MonitoringLevel.NONE)


class AdhocConsumer:

    def __init__(
        self, base_path: str, connection_options: pw.io.minio.MinIOSettings, callback
    ):
        if connection_options.bucket_name is not None:
            self.bucket_name = connection_options.bucket_name
        if base_path.startswith("s3://"):
            bucket_path = base_path.lstrip("s3://")
            self.bucket_name, self.path_within_bucket = bucket_path.split("/", 1)
        else:
            self.path_within_bucket = base_path

        class _MessageTableSchema(pw.Schema):
            data: bytes

        table = pw.io.deltalake.read(
            uri=base_path,
            schema=_MessageTableSchema,
            mode="streaming",
            s3_connection_settings=connection_options,
            autocommit_duration_ms=100,
        )

        def on_change(key, row, time, is_addition):
            message = row["data"]
            callback(message)

        pw.io.subscribe(table, on_change=on_change)

    def start(self):
        self._wait_for_delta_table_creation()
        pw.run(monitoring_level=pw.MonitoringLevel.NONE)

    def _wait_for_delta_table_creation(self):
        s3 = boto3.client(
            "s3",
            aws_access_key_id=os.environ["MINIO_S3_ACCESS_KEY"],
            aws_secret_access_key=os.environ["MINIO_S3_SECRET_ACCESS_KEY"],
            endpoint_url=TEST_ENDPOINT,
        )
        while True:
            response = s3.list_objects_v2(
                Bucket=self.bucket_name, Prefix=self.path_within_bucket
            )
            if "Contents" in response and response["KeyCount"] > 0:
                break
            time.sleep(0.1)
