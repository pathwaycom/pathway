# Copyright © 2024 Pathway

import argparse
import os

import pathway as pw
from pathway.internals.monitoring import MonitoringLevel

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pathway wordcount program")
    parser.add_argument("--input", type=str)
    parser.add_argument("--output", type=str)
    parser.add_argument("--pstorage", type=str)
    parser.add_argument("--mode", type=str)
    parser.add_argument("--pstorage-type", type=str)
    args = parser.parse_args()

    if args.pstorage_type == "fs":
        pstorage_config = pw.persistence.Config.simple_config(
            pw.persistence.Backend.filesystem(path=args.pstorage),
            snapshot_interval_ms=5000,
        )
    elif args.pstorage_type == "s3":
        aws_s3_settings = pw.io.s3.AwsS3Settings(
            bucket_name="aws-integrationtest",
            access_key=os.environ["AWS_S3_ACCESS_KEY"],
            secret_access_key=os.environ["AWS_S3_SECRET_ACCESS_KEY"],
            region="eu-central-1",
        )
        pstorage_config = pw.persistence.Config.simple_config(
            pw.persistence.Backend.s3(
                root_path=args.pstorage,
                bucket_settings=aws_s3_settings,
            ),
            snapshot_interval_ms=5000,
        )
    else:
        raise ValueError(f"Unknown persistent storage type: {args.pstorage_type}")

    class InputSchema(pw.Schema):
        word: str

    words = pw.io.fs.read(
        path=args.input,
        schema=InputSchema,
        format="json",
        mode=args.mode,
        persistent_id="1",
        autocommit_duration_ms=10,
    )
    result = words.groupby(words.word).reduce(
        words.word,
        count=pw.reducers.count(),
    )
    pw.io.csv.write(result, args.output)
    pw.run(
        monitoring_level=MonitoringLevel.NONE,
        persistence_config=pstorage_config,
    )
