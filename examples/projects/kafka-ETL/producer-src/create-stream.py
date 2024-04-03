# Copyright © 2024 Pathway

import json
import random
import time
from datetime import datetime
from zoneinfo import ZoneInfo

from kafka import KafkaProducer

input_size = 100
random.seed(0)

topic1 = "timezone1"
topic2 = "timezone2"

timezone1 = ZoneInfo("America/New_York")
timezone2 = ZoneInfo("Europe/Paris")

str_repr = "%Y-%m-%d %H:%M:%S.%f %z"
api_version = (0, 10, 2)


def generate_stream():
    time.sleep(30)
    producer1 = KafkaProducer(
        bootstrap_servers=["kafka:9092"],
        security_protocol="PLAINTEXT",
        api_version=api_version,
    )
    producer2 = KafkaProducer(
        bootstrap_servers=["kafka:9092"],
        security_protocol="PLAINTEXT",
        api_version=api_version,
    )

    def send_message(timezone: ZoneInfo, producer: KafkaProducer, i: int):
        timestamp = datetime.now(timezone)
        message_json = {"date": timestamp.strftime(str_repr), "message": str(i)}
        producer.send(topic1, (json.dumps(message_json)).encode("utf-8"))

    for i in range(input_size):
        if random.choice([True, False]):
            send_message(timezone1, producer1, i)
        else:
            send_message(timezone2, producer2, i)
        time.sleep(1)

    time.sleep(2)
    producer1.close()
    producer2.close()


if __name__ == "__main__":
    generate_stream()
