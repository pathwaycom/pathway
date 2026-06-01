import os

import boto3
import pytest
from botocore.exceptions import ClientError

from .base import MINIO_BUCKET_NAME, MINIO_S3_ENDPOINT_URL


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
