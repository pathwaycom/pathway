import os

import boto3
import pytest
from botocore.exceptions import ClientError

from .base import MINIO_BUCKET_NAME, MINIO_S3_ENDPOINT_URL

# Env vars that the deltalake S3 backend copies from `storage_options` into the
# process environment when a Delta table is opened (S3StorageOptions::from_map
# calls ensure_env_var for each of these) and never removes. After a MinIO
# Delta test runs in a worker, AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY hold the
# MinIO keys, and any later test in the same worker that lets `pw.io.s3.read`
# autodetect its credentials sends those keys to real AWS and fails with
# 403 InvalidAccessKeyId. The endpoint is not among the copied vars, so the
# request goes to AWS itself, not to MinIO.
_LEAKABLE_AWS_ENV_VARS = (
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "AWS_REGION",
    "AWS_PROFILE",
    "AWS_WEB_IDENTITY_TOKEN_FILE",
    "AWS_ROLE_ARN",
    "AWS_ROLE_SESSION_NAME",
)


@pytest.fixture(autouse=True)
def _clean_aws_credential_env(monkeypatch):
    # Start every test with a clean credential environment so that credential
    # autodetection resolves deterministically (from the mounted ~/.aws
    # profile) regardless of which Delta tests ran earlier in this worker.
    for var in _LEAKABLE_AWS_ENV_VARS:
        monkeypatch.delenv(var, raising=False)


@pytest.fixture(scope="session", autouse=True)
def _ensure_minio_bucket():
    # The MinIO container started by docker-compose comes up empty; the test
    # suite owns the bucket it writes into and (re)creates it once per session.
    client = boto3.client(
        "s3",
        aws_access_key_id=os.environ["MINIO_S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["MINIO_S3_SECRET_ACCESS_KEY"],
        endpoint_url=MINIO_S3_ENDPOINT_URL,
    )
    try:
        client.head_bucket(Bucket=MINIO_BUCKET_NAME)
    except ClientError as exc:
        status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status != 404:
            raise
        try:
            client.create_bucket(Bucket=MINIO_BUCKET_NAME)
        except ClientError as create_exc:
            # Under pytest-xdist every worker has its own session, so they
            # race ``head_bucket`` -> ``create_bucket`` together. The first
            # worker wins; the rest see ``BucketAlreadyOwnedByYou`` (or, on
            # some S3 clones, ``BucketAlreadyExists``). Both mean the bucket
            # exists now, which is the post-condition this fixture promises,
            # so we treat them as success rather than as a setup failure.
            if create_exc.response.get("Error", {}).get("Code") not in (
                "BucketAlreadyOwnedByYou",
                "BucketAlreadyExists",
            ):
                raise
