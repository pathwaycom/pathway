import os
import time

import boto3

import pathway as pw

TEST_REGION = "eu-central-1"
TEST_MINIO_BUCKET_NAME = "your-bucket"
TEST_MINIO_ENDPOINT = "https://your-endpoint.com"
TEST_MINIO_SETTINGS = pw.io.minio.MinIOSettings(
    access_key=os.environ["MINIO_S3_ACCESS_KEY"],
    secret_access_key=os.environ["MINIO_S3_SECRET_ACCESS_KEY"],
    bucket_name=TEST_MINIO_BUCKET_NAME,
    region=TEST_REGION,
    endpoint=TEST_MINIO_ENDPOINT,
)

TEST_S3_BUCKET_NAME = "your-bucket"
TEST_S3_SETTINGS = pw.io.s3.AwsS3Settings(
    access_key=os.environ["AWS_S3_ACCESS_KEY"],
    secret_access_key=os.environ["AWS_S3_SECRET_ACCESS_KEY"],
    bucket_name=TEST_S3_BUCKET_NAME,
    region=TEST_REGION,
)


def get_s3_backend_settings(backend_type: str):
    if backend_type == "minio":
        return TEST_MINIO_SETTINGS.create_aws_settings()
    elif backend_type == "s3":
        return TEST_S3_SETTINGS
    else:
        raise RuntimeError(f"unknown S3 backend type: {backend_type}")


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
        connection_options: pw.io.s3.AwsS3Settings,
        produce_messages,
        autocommit_duration_ms,
    ):
        self.base_path = base_path
        self.connection_options = connection_options

        class _MessageTableSchema(pw.Schema):
            data: bytes

        self.message_table_subject = _MessageTableSubject(produce_messages)
        self.message_table = pw.io.python.read(
            self.message_table_subject,
            schema=_MessageTableSchema,
            autocommit_duration_ms=autocommit_duration_ms,
        )
        pw.io.deltalake.write(
            table=self.message_table,
            uri=base_path,
            s3_connection_settings=connection_options,
            min_commit_frequency=autocommit_duration_ms,
        )

    def start(self):
        pw.run(monitoring_level=pw.MonitoringLevel.NONE)


class AdhocConsumer:

    def __init__(
        self,
        base_path: str,
        connection_options: pw.io.s3.AwsS3Settings,
        callback,
        autocommit_duration_ms,
    ):
        self.connection_options = connection_options
        if connection_options._bucket_name is not None:
            self.bucket_name = connection_options._bucket_name
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
            autocommit_duration_ms=autocommit_duration_ms,
        )

        def on_change(key, row, time, is_addition):
            message = row["data"]
            callback(message)

        pw.io.subscribe(table, on_change=on_change)

    def start(self):
        self._wait_for_delta_table_creation()
        pw.run(monitoring_level=pw.MonitoringLevel.NONE)

    def _wait_for_delta_table_creation(self):
        s3 = self._create_boto3_s3_client()
        while True:
            response = s3.list_objects_v2(
                Bucket=self.bucket_name, Prefix=self.path_within_bucket
            )
            if "Contents" in response and response["KeyCount"] > 0:
                break
            time.sleep(0.1)

    def _create_boto3_s3_client(self):
        client_kwargs = {
            "aws_access_key_id": self.connection_options._access_key,
            "aws_secret_access_key": self.connection_options._secret_access_key,
        }
        if self.connection_options._endpoint:
            client_kwargs["endpoint_url"] = self.connection_options._endpoint
        return boto3.client("s3", **client_kwargs)
