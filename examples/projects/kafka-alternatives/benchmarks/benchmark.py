import argparse
import os
import subprocess

import boto3
from lib import TEST_BUCKET_NAME, TEST_ENDPOINT


def cleanup_files_under_prefix(path):
    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.environ["MINIO_S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["MINIO_S3_SECRET_ACCESS_KEY"],
        endpoint_url=TEST_ENDPOINT,
    )
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=TEST_BUCKET_NAME, Prefix=path):
        if "Contents" not in page:
            continue
        objects_to_delete = [{"Key": obj["Key"]} for obj in page["Contents"]]
        s3.delete_objects(
            Bucket=TEST_BUCKET_NAME, Delete={"Objects": objects_to_delete}
        )


def start_consumer(lake_path, output_path, expected_messages, log_frequency):
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
    ]
    return subprocess.Popen(command)


def start_producer(lake_path, rate, seconds_to_stream):
    command = [
        "python",
        "producer.py",
        "--lake-path",
        lake_path,
        "--rate",
        str(rate),
        "--seconds-to-stream",
        str(seconds_to_stream),
    ]
    return subprocess.Popen(command)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--range-start", type=int, required=True)
    parser.add_argument("--range-end", type=int, required=True)
    parser.add_argument("--range-step", type=int, required=True)
    parser.add_argument("--seconds-to-stream", type=int, default=300)
    args = parser.parse_args()

    for rate in range(args.range_start, args.range_end + 1, args.range_step):
        print(f"Running with streaming rate = {rate}")
        tmp_s3_path = f"adhoc/s3-messaging/{rate}"
        tmp_s3_path_full = f"s3://{TEST_BUCKET_NAME}/{tmp_s3_path}"
        local_output_path = f"benchmark-results/{rate}.csv"
        cleanup_files_under_prefix(tmp_s3_path)

        consumer = start_consumer(
            lake_path=tmp_s3_path_full,
            output_path=local_output_path,
            expected_messages=rate * args.seconds_to_stream,
            log_frequency=rate // 2,
        )
        print("Consumer started")
        producer = start_producer(
            lake_path=tmp_s3_path_full,
            rate=rate,
            seconds_to_stream=args.seconds_to_stream,
        )
        print("Producer started")
        assert producer.wait() == 0
        assert consumer.wait() == 0

        cleanup_files_under_prefix(tmp_s3_path)
