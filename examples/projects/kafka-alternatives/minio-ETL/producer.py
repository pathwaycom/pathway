import datetime
import os
import random
import time
from zoneinfo import ZoneInfo

import pandas as pd
from base import base_path, str_repr
from deltalake import write_deltalake
from dotenv import load_dotenv

load_dotenv()


def send_message(timezone: ZoneInfo, topic: str, i: int):
    timestamp = datetime.datetime.now(timezone)
    lake_path = base_path + topic
    message_json = {"date": timestamp.strftime(str_repr), "message": str(i)}

    additional_data = [message_json]
    df = pd.DataFrame(additional_data)
    write_deltalake(
        lake_path,
        df,
        mode="append",
        storage_options={
            "AWS_ACCESS_KEY_ID": os.environ["MINIO_S3_ACCESS_KEY"],
            "AWS_SECRET_ACCESS_KEY": os.environ["MINIO_S3_SECRET_ACCESS_KEY"],
            "AWS_REGION": "eu-central-1",
            "AWS_S3_ALLOW_UNSAFE_RENAME": "True",
            "AWS_BUCKET_NAME": "your-bucket",
            "endpoint": "your-url-endpoint.com",
            "AWS_STORAGE_ALLOW_HTTP": "true",
        },
    )


if __name__ == "__main__":
    random.seed(0)
    topic1 = "timezone1"
    topic2 = "timezone2"

    timezone1 = ZoneInfo("America/New_York")
    timezone2 = ZoneInfo("Europe/Paris")
    input_size = 10

    for i in range(input_size):
        if random.choice([True, False]):
            send_message(timezone1, topic1, i)
        else:
            send_message(timezone2, topic2, i)
        time.sleep(1)
