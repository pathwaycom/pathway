# Copyright © 2024 Pathway

"""Pathway code for twitter showcase.

The code is responsible for parsing and processing the tweets from the Twitter API.
See public/website3/content/2.developers/7.showcases/1.twitter.md
or https://pathway.com/developers/showcases/twitter for the detailed description.
"""

import argparse

from geocoding import add_geolocation
from preprocessing import parse_and_prefilter_tweets
from processing import (
    add_distance_and_buckets,
    add_magic_influence,
    add_sentiment,
    compute_aggs_per_author_and_timewindow,
    compute_metadata_for_authors,
    enrich_metadata_with_total_influence,
    filter_out_locations_by_closeness,
)
from schemas import TweetUnparsed

import pathway as pw


def process_tweets(tweets: pw.Table[TweetUnparsed]):
    """Processes raw tweets from twitter api and prepares dynamic tables to be used by frontend."""

    tweet_pairs = parse_and_prefilter_tweets(tweets)
    tweet_pairs = add_geolocation(tweet_pairs)
    author_meta = compute_metadata_for_authors(tweet_pairs)
    tweet_pairs = add_distance_and_buckets(tweet_pairs)
    tweet_pairs = add_sentiment(tweet_pairs)
    tweet_pairs = add_magic_influence(tweet_pairs)
    tweet_pairs = filter_out_locations_by_closeness(tweet_pairs, author_meta)
    grouped = compute_aggs_per_author_and_timewindow(
        tweet_pairs.filter(tweet_pairs.is_good)
    )
    author_meta = enrich_metadata_with_total_influence(grouped, author_meta)

    author_meta = author_meta.with_columns(
        coord_to=pw.apply(str, pw.this.coord_to),
        coord_shifted=pw.apply(str, pw.this.coord_to),
    )

    tweet_pairs = tweet_pairs.with_columns(
        coord_from=pw.apply(str, pw.this.coord_from),
        coord_to=pw.apply(str, pw.this.coord_to),
    )
    return tweet_pairs, grouped, author_meta


def get_data_table(dataset_path, poll_new_objects):
    if dataset_path:
        print("Using FS path {} as a source of tweets".format(dataset_path))
        return pw.io.plaintext.read(
            path=dataset_path, mode="streaming" if poll_new_objects else "static"
        )
    else:
        print("Using Kafka as a source of tweets")
        rdkafka_settings = {
            "group.id": "$GROUP_NAME",
            "bootstrap.servers": "kafka:9092",
            "enable.partition.eof": "false",
            "session.timeout.ms": "60000",
            "enable.auto.commit": "true",
        }
        return pw.io.kafka.read(rdkafka_settings, ["test_0"], format="raw")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Twitter showcase application")
    parser.add_argument("--dataset-path", type=str)
    parser.add_argument("--poll-new-objects", type=bool, default=False)
    args = parser.parse_args()
    data = get_data_table(args.dataset_path, args.poll_new_objects)

    tweet_pairs, grouped, author_meta = process_tweets(data)

    postgres_settings = {
        "host": "postgres",
        "port": "5432",
        "dbname": "postgres",
        "user": "postgres",
        "password": "changeme",
    }
    pw.io.postgres.write_snapshot(
        author_meta,
        postgres_settings=postgres_settings,
        table_name="author_meta",
        primary_key=["tweet_to_author_id"],
    )
    pw.io.postgres.write_snapshot(
        grouped,
        postgres_settings=postgres_settings,
        table_name="grouped",
        primary_key=["tweet_to_author_id", "time_bucket"],
    )
    pw.io.postgres.write_snapshot(
        tweet_pairs,
        postgres_settings=postgres_settings,
        table_name="tweet_pairs",
        primary_key=["tweet_from_id", "tweet_to_id"],
    )

    pw.run()
