# Copyright © 2024 Pathway

import argparse
import datetime
import json
import os

import tweepy
from kafka import KafkaProducer

BEARER_TOKEN = os.environ["TWITTER_API_TOKEN"]

COMMIT_MESSAGE = "*COMMIT*"
FINISH_MESSAGE = "*FINISH*"


class KafkaWriter:
    def __init__(self, max_batch_size, max_batch_lifetime_sec, limit):
        self._max_batch_size = max_batch_size
        self._max_batch_lifetime = datetime.timedelta(max_batch_lifetime_sec)
        self._limit = limit

        self._total_written = 0
        self._total_written_after_commit = 0
        self._last_commit_at = None
        self._current_seq_id = 0

        self._producer = KafkaProducer(bootstrap_servers=["kafka:9092"])

    def write(self, json_data):
        self._producer.send(
            "test_0",
            json.dumps(json_data).encode("utf-8"),
            partition=0,
        )

        if not self._last_commit_at:
            self._last_commit_at = datetime.datetime.now()

        self._total_written += 1
        self._total_written_after_commit += 1
        batch_exists_for = datetime.datetime.now() - self._last_commit_at

        size_limit_reached = self._total_written_after_commit == self._max_batch_size
        duration_limit_reached = batch_exists_for >= self._max_batch_lifetime

        if size_limit_reached or duration_limit_reached:
            self._last_commit_at = datetime.datetime.now()
            print("Sending a commit message. Sequential No:", self._current_seq_id)
            self._producer.send(
                "test_0",
                COMMIT_MESSAGE.encode("utf-8"),
                partition=0,
            )
            self._total_written_after_commit = 0
            self._current_seq_id += 1

        if self._total_written == self._limit:
            print("Sending finish command")
            self._producer.send(
                "test_0",
                FINISH_MESSAGE.encode("utf-8"),
                partition=0,
            )
            self._producer.close()
            exit(0)

    def close(self):
        self._producer.close()


class IDPrinter(tweepy.StreamingClient):
    def __init__(self, writer):
        super().__init__(BEARER_TOKEN)
        self._writer = writer

    def on_response(self, response):
        if response.data.geo:
            self._writer.write(
                {
                    "type": "geo-tagged",
                    "tweet": response.data.data,
                    "includes": {
                        key: [value.data for value in val_l]
                        for (key, val_l) in response.includes.items()
                    },
                },
            )
        elif (
            len(response.includes["users"]) > 0
            and response.includes["users"][0].location
        ):
            self._writer.write(
                {
                    "type": "user-location",
                    "tweet": response.data.data,
                    "includes": {
                        key: [value.data for value in val_l]
                        for (key, val_l) in response.includes.items()
                    },
                },
            )
        else:
            self._writer.write(
                {
                    "type": "no-location",
                    "tweet": response.data.data,
                    "includes": {
                        key: [value.data for value in val_l]
                        for (key, val_l) in response.includes.items()
                    },
                },
            )

    def on_errors(self, errors):
        print(errors)


def construct_writer(args):
    return KafkaWriter(
        max_batch_size=args.max_batch_size,
        max_batch_lifetime_sec=args.max_batch_lifetime_seconds,
        limit=args.tweets_limit,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Twitter showcase tweets downloader")
    parser.add_argument("--rule", type=str, required=True)
    parser.add_argument("--max-batch-size", type=int, default=100)
    parser.add_argument("--max-batch-lifetime-seconds", type=int, default=90)
    parser.add_argument("--tweets-limit", type=int, default=1000)
    args = parser.parse_args()

    writer = construct_writer(args)

    printer = IDPrinter(writer)
    print("Starting streaming to files...")
    try:
        rules = printer.get_rules()
        if rules and rules.data:
            printer.delete_rules([rule.id for rule in rules.data])
            print("deleted")
        printer.add_rules([tweepy.StreamRule(args.rule)])
        printer.filter(
            tweet_fields=[
                "id",
                "text",
                "author_id",
                "context_annotations",
                "conversation_id",
                "entities",
                "geo",
                "lang",
                "source",
                "created_at",
                "in_reply_to_user_id",
                "possibly_sensitive",
                "public_metrics",
                "referenced_tweets",
                "reply_settings",
                "withheld",
            ],
            user_fields=[
                "id",
                "name",
                "username",
                "created_at",
                "description",
                "entities",
                "location",
                "pinned_tweet_id",
                "profile_image_url",
                "protected",
                "public_metrics",
                "url",
                "verified",
                "withheld",
            ],
            place_fields=[
                "id",
                "country",
                "country_code",
                "geo",
                "name",
                "place_type",
                "full_name",
                "contained_within",
            ],
            expansions=[
                "geo.place_id",
                "author_id",
                "referenced_tweets.id",
                "in_reply_to_user_id",
                "entities.mentions.username",
                "referenced_tweets.id.author_id",
            ],
        )
    except Exception:
        writer.close()
        raise
