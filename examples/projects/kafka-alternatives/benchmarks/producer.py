import argparse
import time

from lib import AdhocProducer, get_s3_backend_settings


def create_producer(rate_per_second, streaming_time, delay_check_per_iterations=10000):

    def produce_messages(sender):
        start = time.time()
        duration_per_message = 1.0 / rate_per_second
        current_time = None
        for i in range(rate_per_second * streaming_time):
            if i % 100 == 0:
                current_time = time.time()
            message = str(current_time).encode("utf-8")
            sender.next(data=message)
            if i % delay_check_per_iterations == 0:
                time_needed_to_pass = duration_per_message * i
                time_actually_passed = current_time - start
                if time_actually_passed < time_needed_to_pass:
                    print(
                        f"Streaming is faster than target by {time_needed_to_pass - time_actually_passed}s"
                    )
                    time.sleep(time_needed_to_pass - time_actually_passed)
                elif time_actually_passed > time_needed_to_pass:
                    print(
                        f"Streaming falls behind by {time_actually_passed - time_needed_to_pass}s"
                    )
        print("Streaming done")

    return produce_messages


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--lake-path", type=str, required=True)
    parser.add_argument("--rate", type=int, required=True)
    parser.add_argument("--seconds-to-stream", type=int, required=True)
    parser.add_argument(
        "--s3-backend", type=str, choices=["minio", "s3"], required=True
    )
    parser.add_argument("--autocommit-duration-ms", type=int, required=True)
    args = parser.parse_args()

    produce_messages = create_producer(args.rate, args.seconds_to_stream)
    producer = AdhocProducer(
        args.lake_path,
        get_s3_backend_settings(args.s3_backend),
        produce_messages,
        args.autocommit_duration_ms,
    )
    producer.start()
