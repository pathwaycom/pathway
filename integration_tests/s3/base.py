import json
import os

import boto3

import pathway as pw
from pathway.tests.utils import get_aws_s3_settings, get_minio_settings


def put_aws_object(path, contents):
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=os.environ["AWS_S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["AWS_S3_SECRET_ACCESS_KEY"],
    )
    s3_client.put_object(
        Bucket="aws-integrationtest",
        Key=path,
        Body=contents,
    )


def put_minio_object(path, contents):
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=os.environ["MINIO_S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["MINIO_S3_SECRET_ACCESS_KEY"],
        endpoint_url="https://minio-api.deploys.pathway.com",
    )
    s3_client.put_object(
        Bucket="minio-integrationtest",
        Key=path,
        Body=contents,
    )


def put_object_into_storage(storage, path, contents):
    put_object_methods = {
        "s3": put_aws_object,
        "minio": put_minio_object,
    }
    return put_object_methods[storage](path, contents)


def create_jsonlines(input_dicts):
    return "\n".join([json.dumps(value) for value in input_dicts])


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
