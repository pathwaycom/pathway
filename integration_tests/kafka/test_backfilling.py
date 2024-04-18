# Copyright © 2024 Pathway

import json
import os
import pathlib
import random

import pytest

import pathway as pw
from pathway.internals.parse_graph import G
from pathway.tests.utils import get_aws_s3_settings, wait_result_with_checker

from .utils import KafkaTestContext


def generate_wordcount_input(n_words, n_word_repetitions):
    input = []
    for word_idx in range(n_words):
        word = f"word_{word_idx}"
        input += [word] * n_word_repetitions
    random.shuffle(input)
    return input


class WordcountChecker:
    def __init__(self, kafka_context, output_file_path, n_words, n_word_repetitions):
        self.kafka_context = kafka_context
        self.output_file_path = output_file_path
        self.n_words = n_words
        self.n_word_repetitions = n_word_repetitions

    def __call__(self):
        try:
            return self.topic_stats()["completed_words"] == self.n_words
        except Exception:
            return False

    def expected_word_counts(self):
        # workaround for some messages being lost in kafka
        required_counts = {}
        for word in self.kafka_context.read_input_topic():
            value = word.value.decode("utf-8")
            if value not in required_counts:
                required_counts[value] = 0
            required_counts[value] += 1
        return required_counts

    def topic_stats(self):
        completed_words = set()
        total_messages = 0
        total_incorrect_counts = 0
        expected_word_counts = self.expected_word_counts()

        with open(self.output_file_path) as f:
            for raw_entry in f.readlines():
                if not raw_entry:
                    continue
                entry = json.loads(raw_entry)
                expected_count = expected_word_counts[entry["data"]]
                assert 0 <= entry["count"] <= expected_count
                if entry["count"] == expected_count:
                    completed_words.add(entry["data"])
                total_messages += 1

        result = {
            "n_words": self.n_words,
            "n_word_repetitions": self.n_word_repetitions,
            "completed_words": len(completed_words),
            "total_messages": total_messages,
            "total_incorrect_counts": total_incorrect_counts,
        }

        return result

    def provide_information_on_failure(self) -> str:
        return repr(self.topic_stats())


class WordcountProgram:
    def __init__(self, output_file_path, reader_method, *reader_args, **reader_kwargs):
        self.output_file_path = output_file_path
        self.reader_method = reader_method
        self.reader_args = reader_args
        self.reader_kwargs = reader_kwargs

    def __call__(self, *args, **kwargs):
        G.clear()
        words = self.reader_method(*self.reader_args, **self.reader_kwargs)
        result = words.groupby(words.data).reduce(
            words.data,
            count=pw.reducers.count(),
        )
        pw.io.jsonlines.write(
            result,
            self.output_file_path,
        )
        pw.run(*args, **kwargs)


def run_backfilling_program(
    persistence_config, tmp_path: pathlib.Path, kafka_context: KafkaTestContext
):
    """
    Note: this is the light version of more comprehensive persistent computation tests,
    which are in wordcount.

    The main purpose of this test is to check that the recovery process from Kafka
    also works and does not produce any weird effects.
    """

    output_file_path = tmp_path / "output.jsonlines"
    try:
        kafka_context.set_input_topic_partitions(24)
        n_kafka_runs = 5
        for run_seq_id in range(n_kafka_runs):
            if run_seq_id % 2 == 1:
                os.environ["PATHWAY_THREADS"] = str(4)
            else:
                os.environ["PATHWAY_THREADS"] = str(8)
            kafka_context.fill(generate_wordcount_input(1000, 50))
            checker = WordcountChecker(
                kafka_context, output_file_path, 1000, 50 * (run_seq_id + 1)
            )
            wait_result_with_checker(
                checker=checker,
                timeout_sec=60,
                target=WordcountProgram(
                    output_file_path,
                    pw.io.kafka.read,
                    rdkafka_settings=kafka_context.default_rdkafka_settings(),
                    topic=kafka_context.input_topic,
                    format="plaintext",
                    autocommit_duration_ms=5,
                    persistent_id="1",
                ),
                kwargs={"persistence_config": persistence_config},
            )
    finally:
        del os.environ["PATHWAY_THREADS"]


@pytest.mark.flaky(reruns=5)
def test_backfilling_fs_storage(
    tmp_path: pathlib.Path, kafka_context: KafkaTestContext
):
    fs_persistence_config = pw.persistence.Config.simple_config(
        pw.persistence.Backend.filesystem(tmp_path / "PStorage"),
        snapshot_interval_ms=5000,
    )
    run_backfilling_program(fs_persistence_config, tmp_path, kafka_context)


@pytest.mark.flaky(reruns=5)
def test_backfilling_s3_storage(
    tmp_path: pathlib.Path, kafka_context: KafkaTestContext, s3_path: str
):
    pstorage_s3_path = f"{s3_path}/PStorage"
    s3_persistence_config = pw.persistence.Config.simple_config(
        pw.persistence.Backend.s3(
            root_path=pstorage_s3_path,
            bucket_settings=get_aws_s3_settings(),
        ),
        snapshot_interval_ms=2000,
    )
    run_backfilling_program(s3_persistence_config, tmp_path, kafka_context)
