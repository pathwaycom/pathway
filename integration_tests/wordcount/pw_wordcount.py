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
    parser.add_argument("--persistence_mode", type=str)
    args = parser.parse_args()

    if args.persistence_mode == "PERSISTING":
        persistence_mode = pw.PersistenceMode.PERSISTING
    elif args.persistence_mode == "OPERATOR_PERSISTING":
        persistence_mode = pw.PersistenceMode.OPERATOR_PERSISTING
    else:
        ValueError(f"Unsupported persistence mode: {args.persistence_mode}")

    if args.pstorage_type == "fs":
        pstorage_config = pw.persistence.Config(
            pw.persistence.Backend.filesystem(path=args.pstorage),
            snapshot_interval_ms=5000,
            persistence_mode=persistence_mode,
        )
    elif args.pstorage_type == "s3":
        aws_s3_settings = pw.io.s3.AwsS3Settings(
            bucket_name="aws-integrationtest",
            access_key=os.environ["AWS_S3_ACCESS_KEY"],
            secret_access_key=os.environ["AWS_S3_SECRET_ACCESS_KEY"],
            region="eu-central-1",
        )
        pstorage_config = pw.persistence.Config(
            pw.persistence.Backend.s3(
                root_path=args.pstorage,
                bucket_settings=aws_s3_settings,
            ),
            snapshot_interval_ms=5000,
            persistence_mode=persistence_mode,
        )
    elif args.pstorage_type == "azure":
        pstorage_config = pw.persistence.Config(
            pw.persistence.Backend.azure(
                root_path=args.pstorage,
                account=os.environ["AZURE_BLOB_STORAGE_ACCOUNT"],
                password=os.environ["AZURE_BLOB_STORAGE_PASSWORD"],
                container=os.environ["AZURE_BLOB_STORAGE_CONTAINER"],
            ),
            snapshot_interval_ms=5000,
            persistence_mode=persistence_mode,
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
        name="1",
        autocommit_duration_ms=100,
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
