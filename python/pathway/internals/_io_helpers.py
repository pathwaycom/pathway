# Copyright © 2023 Pathway

from __future__ import annotations

from pathway.internals import api
from pathway.internals import dtype as dt
from pathway.internals import schema
from pathway.internals.table import Table
from pathway.internals.trace import trace_user_frame


class AwsS3Settings:
    @trace_user_frame
    def __init__(
        self,
        bucket_name,
        *,
        access_key=None,
        secret_access_key=None,
        with_path_style=False,
        region=None,
        endpoint=None,
    ):
        """Constructs Amazon S3 connection settings.

        Args:
            bucket_name: Name of S3 bucket.
            access_key: Access key for the bucket.
            secret_access_key: Secret access key for the bucket.
            with_path_style: Whether to use path-style requests for the bucket.
            region: Region of the bucket.
            endpoint: Custom endpoint in case of self-hosted storage.
        """
        self.settings = api.AwsS3Settings(
            bucket_name,
            access_key,
            secret_access_key,
            with_path_style,
            region,
            endpoint,
        )


def _format_output_value_fields(table: Table) -> list[api.ValueField]:
    value_fields = []
    for column_name in table._columns.keys():
        value_fields.append(api.ValueField(column_name, api.PathwayType.ANY))

    return value_fields


def _form_value_fields(schema: type[schema.Schema]) -> list[api.ValueField]:
    schema.default_values()
    default_values = schema.default_values()
    result = []

    # XXX fix mapping schema types to PathwayType
    types = {
        name: dt.unoptionalize(dtype).to_engine()
        for name, dtype in schema._dtypes().items()
    }

    for f in schema.column_names():
        simple_type = types.get(f, api.PathwayType.ANY)
        value_field = api.ValueField(f, simple_type)
        if f in default_values:
            value_field.set_default(default_values[f])
        result.append(value_field)

    return result
