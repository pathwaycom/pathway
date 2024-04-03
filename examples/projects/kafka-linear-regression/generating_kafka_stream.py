# Copyright © 2024 Pathway

import json
import os
import random
import time

from kafka import KafkaProducer

topic = "linear-regression"


random.seed(0)


# function that creates a floating number close in value to i
def get_value(i):
    return i + (2 * random.random() - 1) / 10


# set kafka credentials
kafka_endpoint = "talented-cow-10356-eu1-kafka.upstash.io:9092"
kafka_user = os.environ["UPSTASH_KAFKA_USER"]
kafka_pass = os.environ["UPSTASH_KAFKA_PASS"]


# generate input stream using creds from upstash
producer = KafkaProducer(
    bootstrap_servers=[kafka_endpoint],
    sasl_mechanism="SCRAM-SHA-256",
    security_protocol="SASL_SSL",
    sasl_plain_username=kafka_user,
    sasl_plain_password=kafka_pass,
    api_version=(0, 10, 2),
)

# send Kafka messages with i (x) and float close to i (y)
for i in range(10):
    time.sleep(1)
    payload = {
        "x": i,
        "y": get_value(i),
    }
    producer.send(topic, json.dumps(payload).encode("utf-8"))

# close stream
producer.close()
