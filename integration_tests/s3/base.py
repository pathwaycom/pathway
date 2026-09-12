import json
import os
import time

import boto3
from botocore.exceptions import ClientError

import pathway as pw
from pathway.tests.utils import get_aws_s3_settings, get_minio_settings

# HTTP statuses returned when the S3/MinIO backend (or the proxy in front of it)
# is momentarily unavailable, as opposed to the request itself being wrong.
_TRANSIENT_S3_HTTP_STATUSES = frozenset({500, 502, 503, 504})

# Substrings marking a transient connectivity failure in an exception message,
# used when the error carries no structured HTTP status — e.g. the deltalake
# bindings surface the underlying object-store failure as a plain string.
_TRANSIENT_S3_MESSAGE_MARKERS = (
    "bad gateway",
    "service unavailable",
    "gateway timeout",
    "internal error",
    "slowdown",
    "could not connect to the endpoint",
    "connection reset",
    "connection refused",
    "connection closed",
    "connection aborted",
    "broken pipe",
    "timed out",
    "temporarily unavailable",
)


def _is_transient_s3_error(exc: BaseException) -> bool:
    if isinstance(exc, ClientError):
        status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status in _TRANSIENT_S3_HTTP_STATUSES:
            return True
    message = str(exc).lower()
    return any(marker in message for marker in _TRANSIENT_S3_MESSAGE_MARKERS)


def retry_on_transient_s3_error(op, *, attempts: int = 6, base_delay: float = 0.5):
    """Run an S3/MinIO operation, retrying only transient backend failures with
    exponential backoff.

    The shared MinIO deployment these tests run against intermittently answers
    ``502 Bad Gateway`` (and other 5xx / connectivity errors) when its proxy
    briefly loses the backend — the very same request succeeds moments later.
    These blips have nothing to do with what a test asserts, so we retry them.
    Every other failure (a missing object, bad credentials, an
    assertion-relevant 4xx) is not transient and is re-raised on the first
    attempt instead of being masked by backoff."""
    for attempt in range(attempts):
        try:
            return op()
        except Exception as exc:
            if attempt == attempts - 1 or not _is_transient_s3_error(exc):
                raise
            time.sleep(base_delay * (2**attempt))


MINIO_BUCKET_NAME = "minio-integrationtest"
S3_BUCKET_NAME = "aws-integrationtest"
# Full URL of the MinIO endpoint, e.g. ``http://minio:9000``. The default points
# at the ``minio`` service defined in ``docker-compose-integration.yml``; override
# this env var to point the suite at a MinIO running elsewhere (local dev, CI
# debugging, etc.).
MINIO_S3_ENDPOINT_URL = os.environ.get("MINIO_S3_ENDPOINT_URL", "http://minio:9000")
AWS_S3_SETTINGS = pw.io.s3.AwsS3Settings(
    access_key=os.environ["AWS_S3_ACCESS_KEY"],
    secret_access_key=os.environ["AWS_S3_SECRET_ACCESS_KEY"],
    bucket_name=S3_BUCKET_NAME,
    region="eu-central-1",
)
MINIO_S3_SETTINGS = pw.io.minio.MinIOSettings(
    bucket_name=MINIO_BUCKET_NAME,
    access_key=os.environ["MINIO_S3_ACCESS_KEY"],
    secret_access_key=os.environ["MINIO_S3_SECRET_ACCESS_KEY"],
    endpoint=MINIO_S3_ENDPOINT_URL,
    # Without an explicit region the engine falls back to the endpoint string,
    # whose ``http://`` slashes then corrupt the ``KEY/yyyyMMdd/REGION/s3/...``
    # X-Amz-Credential layout and trigger an AuthorizationQueryParametersError
    # from MinIO. Any non-empty token works; MinIO ignores the value otherwise.
    region="us-east-1",
)


def put_aws_object(path, contents):
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=os.environ["AWS_S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["AWS_S3_SECRET_ACCESS_KEY"],
    )
    retry_on_transient_s3_error(
        lambda: s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=path,
            Body=contents,
        )
    )


def put_minio_object(path, contents):
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=os.environ["MINIO_S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["MINIO_S3_SECRET_ACCESS_KEY"],
        endpoint_url=MINIO_S3_ENDPOINT_URL,
    )
    retry_on_transient_s3_error(
        lambda: s3_client.put_object(
            Bucket=MINIO_BUCKET_NAME,
            Key=path,
            Body=contents,
        )
    )


def delete_aws_object(path):
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=os.environ["AWS_S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["AWS_S3_SECRET_ACCESS_KEY"],
    )
    retry_on_transient_s3_error(
        lambda: s3_client.delete_objects(
            Bucket=S3_BUCKET_NAME,
            Delete={"Objects": [{"Key": path}]},
        )
    )


def delete_minio_object(path):
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=os.environ["MINIO_S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["MINIO_S3_SECRET_ACCESS_KEY"],
        endpoint_url=MINIO_S3_ENDPOINT_URL,
    )
    retry_on_transient_s3_error(
        lambda: s3_client.delete_object(
            Bucket=MINIO_BUCKET_NAME,
            Key=path,
        )
    )


def put_object_into_storage(storage, path, contents):
    put_object_methods = {
        "s3": put_aws_object,
        "minio": put_minio_object,
    }
    if storage not in put_object_methods:
        raise ValueError(f"Storage type '{storage}' unsupported in tests")
    return put_object_methods[storage](path, contents)


def delete_object_from_storage(storage, path):
    delete_object_methods = {
        "s3": delete_aws_object,
        "minio": delete_minio_object,
    }
    if storage not in delete_object_methods:
        raise ValueError(f"Storage type '{storage}' unsupported in tests")
    return delete_object_methods[storage](path)


def create_jsonlines(input_dicts):
    return "\n".join([json.dumps(value) for value in input_dicts]) + "\n"


def read_jsonlines_fields(path, keys_to_extract):
    result = []
    with open(path) as f:
        for row in f:
            original_json = json.loads(row)
            processed_json = {}
            for key in keys_to_extract:
                processed_json[key] = original_json[key]
            result.append(processed_json)
    return result


def create_table_for_storage(storage_type, path, format, **kwargs):
    read_methods = {
        "s3": pw.io.s3.read,
        "minio": pw.io.minio.read,
    }
    connector_kwargs = {
        "path": path,
        "format": format,
        "mode": "static",
        "autocommit_duration_ms": 1000,
    }
    for extra_key, extra_value in kwargs.items():
        connector_kwargs[extra_key] = extra_value

    if storage_type == "s3":
        connector_kwargs["aws_s3_settings"] = get_aws_s3_settings()
    elif storage_type == "minio":
        connector_kwargs["minio_settings"] = get_minio_settings()
    else:
        raise ValueError(f"Storage type '{storage_type}' unsupported in tests")

    return read_methods[storage_type](**connector_kwargs)
