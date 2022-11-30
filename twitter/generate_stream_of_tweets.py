import argparse
import json
import os
import shutil

import tweepy
from kafka import KafkaProducer

bearer_token = os.environ["TWITTER_API_TOKEN"]


class Writer:
    """
    The class receives data from Twitter and accumulates this data in a temporary
    buffer. When the buffer reaches capacity, the data is written to the specific
    dataset folder.
    """

    def __init__(self, folder_name, capacity, limit):
        self._folder_name = folder_name
        self._capacity = capacity
        self._limit = limit

        self._total_written = 0
        self._total_written_to_file = 0
        self._current_seq_id = 0

        if os.path.exists(folder_name):
            shutil.rmtree(folder_name)
        os.mkdir(folder_name)

        self._file = open(".tempfile", "w")

    def write(self, json_data):
        json.dump(json_data, self._file)
        self._file.write("\n")

        self._total_written += 1
        self._total_written_to_file += 1
        if self._total_written_to_file == self._capacity:
            # Close file
            self._file.write("*COMMIT*\n")
            self._file.close()
            self._total_written_to_file = 0
            self._current_seq_id += 1

            # Copy to folder
            filename = os.path.join(
                self._folder_name, str(self._current_seq_id) + ".txt"
            )
            print("Starting a new file:", filename)
            shutil.move(".tempfile", filename)

            # Reopen file for writing
            self._file = open(".tempfile", "w")

        if self._total_written == self._limit:
            self._file.write("*FINISH*")
            self._file.close()

            self._current_seq_id += 1
            filename = os.path.join(
                self._folder_name, str(self._current_seq_id) + ".txt"
            )
            print("Copying the final part:", filename)
            shutil.move(".tempfile", filename)
            exit(0)

    def close(self):
        self._file.close()


class KafkaWriter:
    def __init__(self, capacity, limit):
        self._capacity = capacity
        self._limit = limit

        self._total_written = 0
        self._total_written_after_commit = 0
        self._current_seq_id = 0

        self._producer = KafkaProducer(
            bootstrap_servers=["kafka:" + os.environ["KAFKA_PORT"]]
        )

    def write(self, json_data):
        self._producer.send(
            "test_0",
            json.dumps(json_data).encode("utf-8"),
            partition=0,
        )

        self._total_written += 1
        self._total_written_after_commit += 1
        if self._total_written_after_commit == self._capacity:
            print("Sending a commit message. Sequential No: ", self._current_seq_id)
            self._producer.send(
                "test_0",
                "*COMMIT*".encode("utf-8"),
                partition=0,
            )
            self._total_written_after_commit = 0
            self._current_seq_id += 1

        if self._total_written == self._limit:
            print("Sending finish command")
            self._producer.send(
                "test_0",
                "*FINISH*".encode("utf-8"),
                partition=0,
            )
            self._producer.close()
            exit(0)

    def close(self):
        self._producer.close()


writer = KafkaWriter(20, 501)


class IDPrinter(tweepy.StreamingClient):
    def on_response(self, response):
        if response.data.geo:
            writer.write(
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
            writer.write(
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
            writer.write(
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


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Twitter showcase tweets downloader")
    parser.add_argument("--rule", type=str, required=True)
    args = parser.parse_args()

    printer = IDPrinter(bearer_token)
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
