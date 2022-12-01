import argparse
import datetime
import json
import time

from kafka import KafkaProducer

COMMIT_COMMAND = "*COMMIT*"
FINISH_COMMAND = "*FINISH*"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Twitter dataset streamer")
    parser.add_argument("--dataset-path", type=str, required=True)
    parser.add_argument("--speed", type=float, default=1)
    parser.add_argument("--batch-size", type=float, default=500)
    args = parser.parse_args()

    dataset = []
    with open(args.dataset_path, "r") as data_input:
        for row in data_input:
            created_at = json.loads(row)["tweet"]["created_at"]
            timestamp = datetime.datetime.strptime(created_at, "%Y-%m-%dT%H:%M:%S.%fZ")
            dataset.append([timestamp, row])

    dataset.sort(key=lambda x: x[0])

    last_streamed_timestamp = None
    producer = KafkaProducer(bootstrap_servers=["kafka:9092"])
    current_batch_size = 0
    for timestamp, row in dataset:
        if last_streamed_timestamp:
            delta = (timestamp - last_streamed_timestamp).total_seconds() / args.speed
            if delta > 0:
                time.sleep(delta)
                last_streamed_timestamp = timestamp
        else:
            last_streamed_timestamp = timestamp
        producer.send("test_0", row.encode("utf-8", "ignore"), partition=0)
        current_batch_size += 1
        if current_batch_size >= args.batch_size:
            producer.send("test_0", COMMIT_COMMAND.encode("utf-8"), partition=0)
            current_batch_size = 0
    producer.send("test_0", COMMIT_COMMAND.encode("utf-8"), partition=0)
    producer.send("test_0", FINISH_COMMAND.encode("utf-8"), partition=0)
    producer.close()
