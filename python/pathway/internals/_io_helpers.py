# Copyright © 2024 Pathway

from __future__ import annotations

import boto3
import boto3.session

from pathway.internals import api, schema
from pathway.internals.table import Table
from pathway.internals.trace import trace_user_frame

S3_PATH_PREFIXES = ["s3://", "s3a://"]
S3_DEFAULT_REGION = "us-east-1"
S3_LOCATION_FIELD = "LocationConstraint"


class AwsS3Settings:
    """Stores Amazon S3 connection settings. You may also use this class to store
    configuration settings for any custom S3 installation, however you will need to
    specify the region and the endpoint.

    Args:
        bucket_name: Name of S3 bucket.
        access_key: Access key for the bucket.
        secret_access_key: Secret access key for the bucket.
        with_path_style: Whether to use path-style requests.
        region: Region of the bucket.
        endpoint: Custom endpoint in case of self-hosted storage.
        session_token: Session token, an alternative way to authenticate to S3.
    """

    @trace_user_frame
    def __init__(
        self,
        *,
        bucket_name=None,
        access_key=None,
        secret_access_key=None,
        with_path_style=False,
        region=None,
        endpoint=None,
        session_token=None,
    ):
        self._bucket_name = bucket_name
        self._access_key = access_key
        self._secret_access_key = secret_access_key
        self._session_token = session_token
        self._with_path_style = with_path_style
        self._region = region
        self._endpoint = endpoint

    @property
    def settings(self) -> api.AwsS3Settings:
        return api.AwsS3Settings(
            self._bucket_name,
            self._access_key,
            self._secret_access_key,
            self._with_path_style,
            self._region,
            self._endpoint,
            self._session_token,
        )

    @classmethod
    def new_from_path(cls, s3_path: str):
        """
        Constructs settings from S3 path. The engine will look for the credentials in
        environment variables and in local AWS profiles. It will also automatically
        detect the region of the bucket.

        This method may fail if there are no credentials or they are incorrect. It may
        also fail if the bucket does not exist.

        Args:
            s3_path: full path to the object in the form ``s3://<bucket_name>/<path>``.

        Returns:
            Configuration object.
        """
        for s3_path_prefix in S3_PATH_PREFIXES:
            starts_with_prefix = s3_path.startswith(s3_path_prefix)
            has_extra_chars = len(s3_path) > len(s3_path_prefix)
            if not starts_with_prefix or not has_extra_chars:
                continue
            bucket = s3_path[len(s3_path_prefix) :].split("/")[0]

            # the crate we use on the Rust-engine side can't detect the location
            # of a bucket, so it's done on the Python side
            s3_client = boto3.client("s3")
            location_response = s3_client.get_bucket_location(Bucket=bucket)

            # Buckets in Region us-east-1 have a LocationConstraint of None
            location_constraint = location_response[S3_LOCATION_FIELD]
            if location_constraint is None:
                region = S3_DEFAULT_REGION
            else:
                region = location_constraint.split("|")[0]

            return cls(
                bucket_name=bucket,
                region=region,
            )

        # If it doesn't start with a valid S3 prefix, it's not a full S3 path
        raise ValueError(f"Incorrect S3 path: {s3_path}")

    def authorize(self):
        if self._access_key is not None and self._secret_access_key is not None:
            return

        # DeltaLake underlying AWS S3 library may fail to deduce the credentials, so
        # we use boto3 to do that, which is more reliable
        # Related github issue: https://github.com/delta-io/delta-rs/issues/854
        session = boto3.session.Session()
        creds = session.get_credentials()
        if creds.access_key is not None and creds.secret_key is not None:
            self._access_key = creds.access_key
            self._secret_access_key = creds.secret_key
        elif creds.token is not None:
            self._session_token = creds.token


def is_s3_path(path: str) -> bool:
    for s3_path_prefix in S3_PATH_PREFIXES:
        if path.startswith(s3_path_prefix):
            return True
    return False


def _format_output_value_fields(table: Table) -> list[api.ValueField]:
    value_fields = []
    for column_name, column_data in table._columns.items():
        value_fields.append(
            api.ValueField(
                column_name,
                column_data.dtype.to_engine(),
            )
        )

    return value_fields


def _form_value_fields(schema: type[schema.Schema]) -> list[api.ValueField]:
    schema.default_values()
    default_values = schema.default_values()
    result = []

    types = {name: dtype.to_engine() for name, dtype in schema._dtypes().items()}

    for f in schema.column_names():
        dtype = types.get(f, api.PathwayType.ANY)
        value_field = api.ValueField(f, dtype)
        if f in default_values:
            value_field.set_default(default_values[f])
        result.append(value_field)

    return result
