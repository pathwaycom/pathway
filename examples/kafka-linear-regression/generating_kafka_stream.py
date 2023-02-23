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


# generate input stream using creds from upstash
producer = KafkaProducer(
    bootstrap_servers=["talented-cow-10356-eu1-kafka.upstash.io:9092"],
    sasl_mechanism="SCRAM-SHA-256",
    security_protocol="SASL_SSL",
    sasl_plain_username=os.environ["UPSTASH_KAFKA_USER"],
    sasl_plain_password=os.environ["UPSTASH_KAFKA_PASS"],
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
