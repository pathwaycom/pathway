# Copyright © 2023 Pathway

import json
import os
import pathlib
import random

from utils import KafkaTestContext

import pathway as pw
from pathway.internals.parse_graph import G
from pathway.tests.utils import (
    expect_csv_checker,
    wait_result_with_checker,
    write_lines,
)


def test_kafka_raw(tmp_path: pathlib.Path, kafka_context: KafkaTestContext):
    kafka_context.fill(["foo", "bar"])

    table = pw.io.kafka.read(
        rdkafka_settings=kafka_context.default_rdkafka_settings(),
        topic=kafka_context.input_topic,
        format="raw",
        autocommit_duration_ms=100,
    )

    pw.io.csv.write(table, str(tmp_path / "output.csv"))

    assert wait_result_with_checker(
        expect_csv_checker(
            """
            data
            foo
            bar
            """,
            tmp_path / "output.csv",
            usecols=["data"],
            index_col=["data"],
        ),
        10,
    )


def test_kafka_json(tmp_path: pathlib.Path, kafka_context: KafkaTestContext):
    kafka_context.fill(
        [
            json.dumps({"k": 0, "v": "foo"}),
            json.dumps({"k": 1, "v": "bar"}),
            json.dumps({"k": 2, "v": "baz"}),
        ]
    )

    table = pw.io.kafka.read(
        rdkafka_settings=kafka_context.default_rdkafka_settings(),
        topic=kafka_context.input_topic,
        format="json",
        value_columns=["v"],
        primary_key=["k"],
        autocommit_duration_ms=100,
    )

    pw.io.csv.write(table, str(tmp_path / "output.csv"))

    assert wait_result_with_checker(
        expect_csv_checker(
            """
            k    | v
            0    | foo
            1    | bar
            2    | baz
            """,
            tmp_path / "output.csv",
            usecols=["v"],
            index_col=["k"],
        ),
        10,
    )


def test_kafka_csv(tmp_path: pathlib.Path, kafka_context: KafkaTestContext):
    kafka_context.fill(
        [
            "k,v",
            "0,foo",
            "1,bar",
            "2,baz",
        ]
    )

    table = pw.io.kafka.read(
        rdkafka_settings=kafka_context.default_rdkafka_settings(),
        topic=kafka_context.input_topic,
        format="csv",
        value_columns=["v"],
        primary_key=["k"],
        autocommit_duration_ms=100,
    )

    pw.io.csv.write(table, str(tmp_path / "output.csv"))

    assert wait_result_with_checker(
        expect_csv_checker(
            """
            k    | v
            0    | foo
            1    | bar
            2    | baz
            """,
            tmp_path / "output.csv",
            usecols=["v"],
            index_col=["k"],
        ),
        10,
    )


def test_kafka_simple_wrapper(tmp_path: pathlib.Path, kafka_context: KafkaTestContext):
    kafka_context.fill(["foo", "bar"])

    table = pw.io.kafka.simple_read(
        "kafka:9092",
        kafka_context.input_topic,
    )
    pw.io.csv.write(table, str(tmp_path / "output.csv"))

    assert wait_result_with_checker(
        expect_csv_checker(
            """
            data
            foo
            bar
            """,
            tmp_path / "output.csv",
            usecols=["data"],
            index_col=["data"],
        ),
        10,
    )

    # check that reread will have all these messages again
    G.clear()
    table = pw.io.kafka.simple_read(
        "kafka:9092",
        kafka_context.input_topic,
    )
    pw.io.csv.write(table, str(tmp_path / "output.csv"))

    assert wait_result_with_checker(
        expect_csv_checker(
            """
            data
            foo
            bar
            """,
            tmp_path / "output.csv",
            usecols=["data"],
            index_col=["data"],
        ),
        10,
    )


def test_kafka_output(tmp_path: pathlib.Path, kafka_context: KafkaTestContext):
    input_path = tmp_path / "input"
    with open(input_path, "w") as f:
        f.write("foo\nbar\n")

    table = pw.io.plaintext.read(
        str(input_path),
        mode="static",
    )
    pw.io.kafka.write(
        table,
        rdkafka_settings=kafka_context.default_rdkafka_settings(),
        topic_name=kafka_context.output_topic,
    )
    pw.run()

    output_topic_contents = kafka_context.read_output_topic()
    assert len(output_topic_contents) == 2, output_topic_contents


def test_kafka_recovery(tmp_path: pathlib.Path, kafka_context: KafkaTestContext):
    persistent_storage_path = tmp_path / "PStorage"

    kafka_context.fill(
        [
            json.dumps({"k": 0, "v": "foo"}),
            json.dumps({"k": 1, "v": "bar"}),
            json.dumps({"k": 2, "v": "baz"}),
        ]
    )

    table = pw.io.kafka.read(
        rdkafka_settings=kafka_context.default_rdkafka_settings(),
        topic=kafka_context.input_topic,
        format="json",
        value_columns=["v"],
        primary_key=["k"],
        autocommit_duration_ms=100,
        persistent_id="1",
    )

    pw.io.csv.write(table, str(tmp_path / "output.csv"))

    assert wait_result_with_checker(
        expect_csv_checker(
            """
            k    | v
            0    | foo
            1    | bar
            2    | baz
            """,
            tmp_path / "output.csv",
            usecols=["v"],
            index_col=["k"],
        ),
        10,
        kwargs={
            "persistence_config": pw.io.PersistenceConfig.single_backend(
                pw.io.PersistentStorageBackend.filesystem(persistent_storage_path),
            ),
        },
    )
    G.clear()

    # fill doesn't replace the messages, so we append 3 new ones
    kafka_context.fill(
        [
            json.dumps({"k": 3, "v": "foofoo"}),
            json.dumps({"k": 4, "v": "barbar"}),
            json.dumps({"k": 5, "v": "bazbaz"}),
        ]
    )

    table = pw.io.kafka.read(
        rdkafka_settings=kafka_context.default_rdkafka_settings(),
        topic=kafka_context.input_topic,
        format="json",
        value_columns=["v"],
        primary_key=["k"],
        autocommit_duration_ms=100,
        persistent_id="1",
    )

    pw.io.csv.write(table, str(tmp_path / "output_backfilled.csv"))
    assert wait_result_with_checker(
        expect_csv_checker(
            """
            k    | v
            3    | foofoo
            4    | barbar
            5    | bazbaz
            """,
            tmp_path / "output_backfilled.csv",
            usecols=["v"],
            index_col=["k"],
        ),
        10,
        target=pw.run,
        kwargs={
            "persistence_config": pw.io.PersistenceConfig.single_backend(
                pw.io.PersistentStorageBackend.filesystem(persistent_storage_path),
            ),
        },
    )


def test_kafka_backfilling(tmp_path: pathlib.Path, kafka_context: KafkaTestContext):
    """
    Note: this is the light version of more comprehensive persistent computation tests,
    which are in wordcount.

    The main purpose of this test is to check that the recovery process from Kafka
    also works and does not produce any weird effects.
    """

    def generate_wordcount_input(n_words, n_word_repetitions):
        input = []
        for word_idx in range(n_words):
            word = "word_{}".format(word_idx)
            input += [word] * n_word_repetitions
        random.shuffle(input)
        return input

    class WordcountChecker:
        def __init__(self, n_words, n_word_repetitions):
            self.n_words = n_words
            self.n_word_repetitions = n_word_repetitions

        def __call__(self):
            return self.topic_stats()["completed_words"] == self.n_words

        def topic_stats(self):
            completed_words = set()
            nearly_completed_words = set()
            total_messages = 0
            for raw_entry in kafka_context.read_output_topic():
                entry = json.loads(raw_entry.value)
                assert 0 <= entry["count"] <= self.n_word_repetitions
                if entry["count"] == self.n_word_repetitions:
                    completed_words.add(entry["data"])
                if entry["count"] >= self.n_word_repetitions * 0.95:
                    nearly_completed_words.add(entry["data"])
                total_messages += 1

            return {
                "n_words": self.n_words,
                "n_word_repetitions": self.n_word_repetitions,
                "completed_words": len(completed_words),
                "nearly_completed_words": len(nearly_completed_words),
                "total_messages": total_messages,
            }

    class WordcountProgram:
        def __init__(self, reader_method, *reader_args, **reader_kwargs):
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
            pw.io.kafka.write(
                result,
                rdkafka_settings=kafka_context.default_rdkafka_settings(),
                topic_name=kafka_context.output_topic,
            )
            pw.run(*args, **kwargs)

    persistent_storage_path = tmp_path / "PStorage"
    try:
        kafka_context.set_input_topic_partitions(24)
        n_kafka_runs = 5
        for run_seq_id in range(n_kafka_runs):
            if run_seq_id % 2 == 1:
                os.environ["PATHWAY_THREADS"] = str(4)
            else:
                os.environ["PATHWAY_THREADS"] = str(8)
            kafka_context.fill(generate_wordcount_input(1000, 50))
            checker = WordcountChecker(1000, 50 * (run_seq_id + 1))
            assert wait_result_with_checker(
                checker=checker,
                timeout_sec=60,
                target=WordcountProgram(
                    pw.io.kafka.read,
                    rdkafka_settings=kafka_context.default_rdkafka_settings(),
                    topic=kafka_context.input_topic,
                    format="raw",
                    autocommit_duration_ms=5,
                    persistent_id="1",
                ),
                kwargs={
                    "persistence_config": pw.io.PersistenceConfig.single_backend(
                        pw.io.PersistentStorageBackend.filesystem(
                            persistent_storage_path
                        ),
                    ),
                },
            ), str(checker.topic_stats())

        # Change the data format: it used to be Kafka, but now we can switch to a file
        input_file_path = tmp_path / "file_input"
        write_lines(input_file_path, "\n".join(generate_wordcount_input(1000, 50)))
        checker = WordcountChecker(1000, 50 * (n_kafka_runs + 1))
        assert wait_result_with_checker(
            checker=checker,
            timeout_sec=60,
            target=WordcountProgram(
                pw.io.plaintext.read,
                str(input_file_path),
                autocommit_duration_ms=5,
                persistent_id="1",
            ),
            kwargs={
                "persistence_config": pw.io.PersistenceConfig.single_backend(
                    pw.io.PersistentStorageBackend.filesystem(persistent_storage_path),
                ),
            },
        ), str(checker.topic_stats())
    finally:
        del os.environ["PATHWAY_THREADS"]
