import os

from dotenv import load_dotenv

import pathway as pw

load_dotenv()


bucket = "your-bucket"
base_path = f"s3://{bucket}/"
str_repr = "%Y-%m-%d %H:%M:%S.%f %z"

s3_connection_settings = pw.io.minio.MinIOSettings(
    bucket_name=bucket,
    access_key=os.environ["MINIO_S3_ACCESS_KEY"],
    secret_access_key=os.environ["MINIO_S3_SECRET_ACCESS_KEY"],
    endpoint="your-url-endpoint.com",
)
