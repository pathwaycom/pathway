import argparse
import subprocess

import boto3
from lib import get_s3_backend_settings


def cleanup_files_under_prefix(path, s3_backend):
    backend_settings = get_s3_backend_settings(s3_backend)
    client_kwargs = {
        "aws_access_key_id": backend_settings._access_key,
        "aws_secret_access_key": backend_settings._secret_access_key,
    }
    if backend_settings._endpoint:
        client_kwargs["endpoint_url"] = backend_settings._endpoint

    s3 = boto3.client("s3", **client_kwargs)
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=backend_settings._bucket_name, Prefix=path):
        if "Contents" not in page:
            continue
        objects_to_delete = [{"Key": obj["Key"]} for obj in page["Contents"]]
        s3.delete_objects(
            Bucket=backend_settings._bucket_name, Delete={"Objects": objects_to_delete}
        )


def start_consumer(
    lake_path,
    output_path,
    expected_messages,
    log_frequency,
    s3_backend,
    autocommit_duration_ms,
):
    command = [
        "python",
        "consumer.py",
        "--lake-path",
        lake_path,
        "--output-path",
        output_path,
        "--expected-messages",
        str(expected_messages),
        "--log-frequency",
        str(log_frequency),
        "--s3-backend",
        s3_backend,
        "--autocommit-duration-ms",
        str(autocommit_duration_ms),
    ]
    return subprocess.Popen(command)


def start_producer(
    lake_path, rate, seconds_to_stream, s3_backend, autocommit_duration_ms
):
    command = [
        "python",
        "producer.py",
        "--lake-path",
        lake_path,
        "--rate",
        str(rate),
        "--seconds-to-stream",
        str(seconds_to_stream),
        "--s3-backend",
        s3_backend,
        "--autocommit-duration-ms",
        str(autocommit_duration_ms),
    ]
    return subprocess.Popen(command)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--range-start", type=int, required=True)
    parser.add_argument("--range-end", type=int, required=True)
    parser.add_argument("--range-step", type=int, required=True)
    parser.add_argument("--seconds-to-stream", type=int, default=300)
    parser.add_argument(
        "--s3-backend", type=str, choices=["s3", "minio"], default="minio"
    )
    parser.add_argument("--autocommit-duration-ms", type=int, default=1000)
    args = parser.parse_args()

    bucket_name = get_s3_backend_settings(args.s3_backend)._bucket_name
    for rate in range(args.range_start, args.range_end + 1, args.range_step):
        print(f"Running with streaming rate = {rate}")
        tmp_s3_path = f"adhoc/s3-messaging/{rate}"
        tmp_s3_path_full = f"s3://{bucket_name}/{tmp_s3_path}"
        local_output_path = f"benchmark-results/{rate}.csv"
        cleanup_files_under_prefix(tmp_s3_path, args.s3_backend)

        consumer = start_consumer(
            lake_path=tmp_s3_path_full,
            output_path=local_output_path,
            expected_messages=rate * args.seconds_to_stream,
            log_frequency=rate // 2,
            s3_backend=args.s3_backend,
            autocommit_duration_ms=args.autocommit_duration_ms,
        )
        print("Consumer started")
        producer = start_producer(
            lake_path=tmp_s3_path_full,
            rate=rate,
            seconds_to_stream=args.seconds_to_stream,
            s3_backend=args.s3_backend,
            autocommit_duration_ms=args.autocommit_duration_ms,
        )
        print("Producer started")
        assert producer.wait() == 0
        assert consumer.wait() == 0

        cleanup_files_under_prefix(tmp_s3_path, args.s3_backend)
