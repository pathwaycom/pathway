# Copyright © 2026 Pathway

"""Backpressure must not starve the Kafka poll loop.

When ``max_backlog_size`` backpressure fills up, the reader thread blocks
inside the bounded channel send (``src/connectors/mod.rs``) and stops calling
``consumer.poll()``. Once the blockage outlasts ``max.poll.interval.ms``,
librdkafka evicts the consumer from its group. On the next poll the consumer
silently rejoins and resumes every partition from the group's committed
offset — but Pathway never commits consumer offsets itself, so the committed
position trails what has already been ingested (with librdkafka's default
auto-commit it lags by up to ``auto.commit.interval.ms``; with auto-commit
disabled, a legitimate user setting, there is none at all and the consumer
falls back to ``auto.offset.reset``). Everything between that position and
the true read position is then delivered a second time, and the engine
records the duplicates as new insertions into a table it itself announced as
append-only — silently inflating every downstream aggregate.

The correct behavior, which this test guards: backpressure may slow reading
down, but the consumer must stay in its group (e.g. by keeping the poll loop
alive and pausing/resuming the assigned partitions), so no message is ever
re-delivered.
"""

import json
import pathlib
import time
import uuid
from collections import Counter

import pytest

import pathway as pw
from pathway.tests.utils import wait_result_with_checker

MESSAGE_COUNT = 1000
MAX_BACKLOG_SIZE = 100
MAX_POLL_INTERVAL_MS = 8_000
# How long the first row stalls the worker. While the worker sleeps, the
# output frontier cannot advance, so the backlog is never released and the
# reader thread stays blocked well past ``max.poll.interval.ms``.
STALL_SECONDS = 15.0


class AllValuesDeliveredChecker:
    """Passes once every expected payload appears in the output file at least
    once. Duplicates delivered after a group eviction re-read the topic from
    the beginning, so by the time the tail payloads arrive, any duplicates of
    the head are already in the file — the final exactly-once assertion below
    does not race with this checker."""

    def __init__(self, path: pathlib.Path, expected_values: set[str]):
        self.path = path
        self.expected_values = expected_values

    def _delivered(self) -> Counter:
        if not self.path.exists():
            return Counter()
        with open(self.path) as f:
            return Counter(json.loads(line)["data"] for line in f)

    def __call__(self) -> bool:
        return self.expected_values <= self._delivered().keys()

    def provide_information_on_failure(self) -> str:
        delivered = self._delivered()
        missing = self.expected_values - delivered.keys()
        return (
            f"{len(missing)} of {len(self.expected_values)} payloads never "
            f"delivered (e.g. {sorted(missing)[:5]}); "
            f"{sum(delivered.values())} rows written in total"
        )


@pytest.mark.flaky(reruns=3)
def test_kafka_full_backlog_does_not_redeliver_messages(tmp_path, kafka_context):
    expected_values = {f"message-{i:05d}" for i in range(MESSAGE_COUNT)}
    kafka_context.fill(sorted(expected_values))

    stalled = [False]

    @pw.udf(deterministic=True)
    def stall_once(data: str) -> str:
        # Stall the worker on the very first processed row. At this point the
        # reader thread has already filled the backlog and the bounded channel
        # (MESSAGE_COUNT is far above both) and is blocked in send(), so it
        # cannot poll the consumer until the frontier moves again — which is
        # longer than max.poll.interval.ms.
        if not stalled[0]:
            stalled[0] = True
            time.sleep(STALL_SECONDS)
        return data

    rdkafka_settings = {
        **kafka_context.default_rdkafka_settings(),
        "group.id": str(uuid.uuid4()),
        # Deterministic variant of the redelivery: with no committed offsets
        # at all, the rejoined consumer restarts from auto.offset.reset
        # ("beginning" from default_rdkafka_settings). Disabling auto-commit
        # is a legitimate configuration — Pathway never commits consumer
        # offsets and does not document a requirement for auto-commit; with
        # the default auto-commit the same eviction still re-delivers the
        # last auto.commit.interval.ms worth of messages, just fewer of them.
        "enable.auto.commit": "false",
        # Shortened from the 300 s default so the eviction fits in test time.
        # librdkafka requires max.poll.interval.ms >= session.timeout.ms, and
        # the broker's default group.min.session.timeout.ms is 6000.
        "max.poll.interval.ms": str(MAX_POLL_INTERVAL_MS),
        "session.timeout.ms": "6000",
    }

    table = pw.io.kafka.read(
        rdkafka_settings=rdkafka_settings,
        topic=kafka_context.input_topic,
        format="plaintext",
        autocommit_duration_ms=1000,
        max_backlog_size=MAX_BACKLOG_SIZE,
    )
    table = table.select(data=stall_once(pw.this.data))

    output_path = tmp_path / "output.jsonl"
    pw.io.jsonlines.write(table, output_path)

    wait_result_with_checker(
        AllValuesDeliveredChecker(output_path, expected_values), 90
    )

    with open(output_path) as f:
        delivered = Counter(json.loads(line)["data"] for line in f)

    missing = expected_values - delivered.keys()
    assert not missing, f"{len(missing)} payloads lost, e.g. {sorted(missing)[:5]}"

    duplicates = {value: count for value, count in delivered.items() if count > 1}
    assert not duplicates, (
        f"{len(duplicates)} of {MESSAGE_COUNT} messages were delivered more "
        f"than once (total rows: {sum(delivered.values())}). The reader "
        f"thread stopped polling while blocked on max_backlog_size "
        f"backpressure, the consumer was evicted from its group after "
        f"max.poll.interval.ms, and after rejoining it re-read messages "
        f"that were already ingested. Examples: "
        f"{dict(sorted(duplicates.items())[:5])}"
    )
