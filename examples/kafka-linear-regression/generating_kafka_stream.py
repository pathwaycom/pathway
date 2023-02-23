from kafka import KafkaProducer
import time
import random
import os

topic = "linear-regression"

# function that creates a floating number close in value to i
random.seed(0)
def get_value(i):
    return i + (2 * random.random() - 1)/10

# generate input stream using creds from upstash
producer = KafkaProducer(
    bootstrap_servers=["talented-cow-10356-eu1-kafka.upstash.io:9092"],
    sasl_mechanism="SCRAM-SHA-256",
    security_protocol="SASL_SSL",
    sasl_plain_username= os.environ['UPSTASH_KAFKA_USER'],
    sasl_plain_password= os.environ['UPSTASH_KAFKA_PASS'],
)

# first Kafka message contains the column names
producer.send(topic, ("x,y").encode("utf-8"))

# send Kafka messages with i (x) and float close to i (y)
for i in range(10):
    time.sleep(1)
    producer.send(
        topic, (str(i) + "," + str(get_value(i))).encode("utf-8"),
    )

# close stream
producer.close()