import argparse
import datetime
import json
import os
import shutil
import time

COMMIT_COMMAND = "*COMMIT*\n"
FINISH_COMMAND = "*FINISH*\n"

DATASET_DIR = "/shared-volume"


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

    temp_storage_dir = os.path.join(DATASET_DIR, ".temp")
    try:
        os.mkdir(temp_storage_dir)
    except FileExistsError:
        print("Directory for temp files already exists")

    temp_storage_file = os.path.join(temp_storage_dir, ".buffer")

    last_streamed_timestamp = None
    current_batch_size = 0
    file_sequential_number = 0
    current_buffer = open(temp_storage_file, "w")

    for timestamp, row in dataset:
        if last_streamed_timestamp:
            delta = (timestamp - last_streamed_timestamp).total_seconds() / args.speed
            if delta > 0:
                time.sleep(delta)
                last_streamed_timestamp = timestamp
        else:
            last_streamed_timestamp = timestamp
        current_buffer.write(row)
        current_batch_size += 1
        if current_batch_size >= args.batch_size:
            # Add commit command to the end of the chunk
            current_buffer.write(COMMIT_COMMAND)

            # Close the current chunk and move it for processing
            current_buffer.close()
            file_sequential_number += 1
            filename = os.path.join(DATASET_DIR, str(file_sequential_number) + ".txt")
            shutil.move(temp_storage_file, filename)

            # Reopen
            current_buffer = open(temp_storage_file, "w")
            current_batch_size = 0

    current_buffer.write(FINISH_COMMAND)
    current_buffer.close()

    filename = os.path.join(DATASET_DIR, "final.txt")
    shutil.move(temp_storage_file, filename)
