# Copyright © 2024 Pathway

import csv
import json
import time

from kafka import KafkaProducer

topic = "ratings"

time.sleep(30)
producer = KafkaProducer(
    bootstrap_servers=["kafka:9092"],
    security_protocol="PLAINTEXT",
    api_version=(0, 10, 2),
)

with open("./dataset.csv", newline="") as csvfile:
    dataset_reader = csv.reader(csvfile, delimiter=",")
    first_line = True
    for row in dataset_reader:
        # We skip the header
        if first_line:
            first_line = False
            continue
        message_json = {
            "userId": int(row[0]),
            "movieId": int(row[1]),
            "rating": float(row[2]),
            "timestamp": int(row[3]),
        }
        producer.send(topic, (json.dumps(message_json)).encode("utf-8"))
        time.sleep(0.1)

producer.send(topic, "*COMMIT*".encode("utf-8"))
time.sleep(2)
producer.close()
