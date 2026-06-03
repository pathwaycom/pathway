# Benchmark for Delta Lake S3 messaging as a Kafka replacement

This repository contains the necessary files to benchmark Delta Lake S3 messaging describe in [this article](https://pathway.com/developers/templates/kafka-alternatives).
Below is a description of each file:
* `lib.py`: Contains Python wrappers for Pathway classes, which are used to build a message queue on top of Delta Lake.
* `producer.py`: Implements the producer logic, which generates messages and adds them to the Delta Lake-based message queue at a specified streaming rate.
* `consumer.py`: Implements the consumer logic, which retrieves messages from the message queue and tracks latencies across various percentiles (50th, 75th, 85th, 95th, and 99th).
* `benchmark.py`: Coordinates the execution of both the producer and consumer, and saves the benchmark results in the `benchmark-results/` directory.

## Setting Up the Benchmark

To set up this benchmark, you'll need to specify the name of the S3 bucket and provide two S3 access keys.

1. Set the bucket name by assigning the appropriate value to the `TEST_BUCKET_NAME`, `TEST_ENDPOINT`, and `TEST_REGION` constants in `lib.py`.
2. The access key and secret access key will be retrieved from the environment variables `MINIO_S3_ACCESS_KEY` and `MINIO_S3_SECRET_ACCESS_KEY`.

## Usage Instructions

To run the streaming benchmark, use the `benchmark.py` script. This command-line tool accepts the following parameters:
* `--range-start`: The starting value of the "messages-per-second" range to be tested.
* `--range-end`: The ending value (inclusive) of the "messages-per-second" range to be tested.
* `--range-step`: The increment between values in the tested range.
* `--seconds-to-stream`: The duration for each streaming test.

### Example

If you want to benchmark rates of 10,000, 20,000, and 30,000 messages per second, each running for 10 minutes, use the following command:

```bash
python benchmark.py --range-start 10000 --range-end 30000 --range-step 10000 --seconds-to-stream 600
```

The script will generate several CSV files in the `benchmark-results/` directory, with one file per tested rate. Each file will include a report showing the latency percentiles for different points in the streaming process.
