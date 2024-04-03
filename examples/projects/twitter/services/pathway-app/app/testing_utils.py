# Copyright © 2024 Pathway

"""Factories for tweets, helpful for writing unittests."""

import datetime
import json

from pathway.debug import table_to_pandas
from pathway.tests.utils import T


class Factory(object):
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def _get_kwargs(self, override_kwargs):
        kwargs = self.kwargs.copy()
        kwargs.update(override_kwargs)

        for key, arg in kwargs.items():
            if callable(arg):
                kwargs[key] = arg()

        return kwargs

    def create(self, **override_kwargs):
        obj = self._get_kwargs(override_kwargs)
        return obj


class Sequence(object):
    def __init__(self, string):
        self.sequence = 0
        self.string = string

    def __call__(self):
        self.sequence += 1

        return self.string.format(self.sequence)


class TimeSequence(object):
    def __init__(self):
        self.timestamp = 0

    def __call__(self):
        self.timestamp += 1
        return datetime.datetime.fromtimestamp(self.timestamp).isoformat() + ".000Z"


user_factory = Factory(
    id=Sequence("{}"),
    username=Sequence("Username {}"),
    location="Wrocław, Poland",
    public_metrics={"followers_count": 111, "following_count": 111, "tweet_count": 111},
)

tweet_factory = Factory(id=Sequence("{}"), created_at=TimeSequence(), text="good")


def create_tweet_pair(tweet, referenced_tweet, users):
    return json.dumps(
        {
            "tweet": {
                **tweet,
                "referenced_tweets": [
                    {"type": "retweet", "id": referenced_tweet["id"]}
                ],
            },
            "includes": {"users": users, "tweets": [referenced_tweet]},
        }
    )


def star_around_author(location, location_close, location_far, close_count, far_count):
    """Creates a tweet at given location with a given number of close/far retweets."""
    author = user_factory.create(location=location)
    tweet = tweet_factory.create(author_id=author["id"])
    tweet_pairs = []
    for _ in range(close_count):
        referencing_author = user_factory.create(location=location_close)
        retweet = tweet_factory.create(author_id=referencing_author["id"])
        tweet_pair = create_tweet_pair(
            tweet=retweet, referenced_tweet=tweet, users=[author, referencing_author]
        )
        tweet_pairs.append(tweet_pair)
    for _ in range(far_count):
        referencing_author = user_factory.create(location=location_far)
        retweet = tweet_factory.create(author_id=referencing_author["id"])
        tweet_pair = create_tweet_pair(
            tweet=retweet, referenced_tweet=tweet, users=[author, referencing_author]
        )
        tweet_pairs.append(tweet_pair)
    return tweet_pairs


def run_convert_pandas(transformer, *input_tables):
    pwTables = [T(table, format="pandas") for table in input_tables]
    outputs = transformer(*pwTables)
    return (table_to_pandas(out_table) for out_table in outputs)
