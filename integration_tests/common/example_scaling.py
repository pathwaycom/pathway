import argparse
import json
import logging
import time

import pathway as pw
from pathway.internals import api


class StreamerSubject(pw.io.python.ConnectorSubject):

    def __init__(self, rate: int, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.rate = rate
        self.current_number = 10000000000000

    def run(self):
        while True:
            second_started_at = time.time()
            for index in range(self.rate):
                event_time = time.time()
                next_json = {
                    "number": self.current_number,
                }
                self.current_number += 1

                self.next_json(next_json)
                if index > 0 or index == self.rate - 1:
                    expected_duration = index * 1.0 / self.rate
                    actual_duration = event_time - second_started_at
                    if actual_duration > expected_duration + 0.1:
                        logging.warning(
                            "The streaming severely falls behind the target frequency"
                        )
                    elif actual_duration < expected_duration:
                        time.sleep(expected_duration - actual_duration)


@pw.udf(deterministic=True)
def is_prime(event_json) -> bool:
    number = json.loads(event_json)["number"]
    if number < 2:
        return False

    is_prime_flag = number % 2 != 0
    i = 3
    while i * i <= number and is_prime_flag:
        if number % i == 0:
            is_prime_flag = False
            break
        i += 2

    return is_prime_flag


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--rate", type=int, required=True)
    parser.add_argument("--persistent-storage-path", type=str, required=True)
    args = parser.parse_args()

    table = pw.io.python.read(subject=StreamerSubject(rate=args.rate), format="raw")
    table = table.select(prime=is_prime(pw.this.data))
    pw.io.null.write(table)

    pw.run(
        persistence_config=pw.persistence.Config(
            backend=pw.persistence.Backend.filesystem(args.persistent_storage_path),
            persistence_mode=api.PersistenceMode.OPERATOR_PERSISTING,
            worker_scaling_enabled=True,
            workload_tracking_window_ms=60000,
        ),
        monitoring_level=pw.MonitoringLevel.NONE,
    )
