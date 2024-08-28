import argparse
import os
import time

from lib import TEST_BUCKET_NAME, TEST_ENDPOINT, TEST_REGION, AdhocConsumer

import pathway as pw

PERCENTILES = [50, 75, 85, 95, 99]
MAX_LATENCY = 100


def create_consumer(output_path, expected_messages, log_frequency):
    timeline = []
    started_at = time.time()

    def create_report_header():
        components = ["time_from_start"]
        for percentile in PERCENTILES:
            components.append(f"p{percentile}")
        return ",".join(components)

    def get_percentile_report(time_from_start, latencies, total_processed):
        total_seen = 0
        sought_percentile_idx = 0
        report = [time_from_start]
        for index, value in enumerate(latencies):
            total_seen += value
            while (
                total_seen
                >= total_processed * PERCENTILES[sought_percentile_idx] // 100
            ):
                report.append(index / 100.0)
                sought_percentile_idx += 1
                if sought_percentile_idx == len(PERCENTILES):
                    break
            if sought_percentile_idx == len(PERCENTILES):
                break
        return ",".join(["{:.2f}".format(p) for p in report])

    def render_latencies_report():
        reporting_started_at = time.time()
        print("Rendering latencies report...")
        latencies = [0] * (MAX_LATENCY * 100 + 1)
        report = [create_report_header()]
        total_processed = 0
        for time_from_start, latency in timeline:
            if latency > MAX_LATENCY:
                latency = MAX_LATENCY
            latencies[int(latency * 100)] += 1
            total_processed += 1
            if total_processed % log_frequency == 0:
                report.append(
                    get_percentile_report(time_from_start, latencies, total_processed)
                )
        report.append(
            get_percentile_report(time_from_start, latencies, total_processed)
        )
        with open(output_path, "w") as f:
            f.write("\n".join(report))
        print(f"Report creation done in {time.time() - reporting_started_at}s")

    def consume_messages(message: bytes):
        decoded = message.decode("utf-8")
        submission_timestamp = float(decoded)
        current_timestamp = time.time()
        latency = current_timestamp - submission_timestamp
        timeline.append([current_timestamp - started_at, latency])
        if len(timeline) == expected_messages:
            render_latencies_report()
            exit(0)

    return consume_messages


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--lake-path", type=str, required=True)
    parser.add_argument("--output-path", type=str, required=True)
    parser.add_argument("--expected-messages", type=int, required=True)
    parser.add_argument("--log-frequency", type=int, required=True)
    args = parser.parse_args()

    consumer = AdhocConsumer(
        args.lake_path,
        pw.io.minio.MinIOSettings(
            access_key=os.environ["MINIO_S3_ACCESS_KEY"],
            secret_access_key=os.environ["MINIO_S3_SECRET_ACCESS_KEY"],
            bucket_name=TEST_BUCKET_NAME,
            region=TEST_REGION,
            endpoint=TEST_ENDPOINT,
        ),
        create_consumer(args.output_path, args.expected_messages, args.log_frequency),
    )
    consumer.start()
