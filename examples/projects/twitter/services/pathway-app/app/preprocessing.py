# Copyright © 2024 Pathway

import json

from schemas import TweetPairs, TweetUnparsed

import pathway as pw
from pathway.stdlib.utils.col import unpack_col


def parse_and_prefilter_tweets(tweets: pw.Table[TweetUnparsed]) -> pw.Table[TweetPairs]:
    """Processes input table containing raw tweets in a JSON format.

    It filters tweets to the tweets usable in the showcase, i.e.:
        * the tweet has to have a referenced tweet (e.g. be a retweet)
        * both tweet and referenced tweet are made by users which specified their location.
    Finally it prepares needed columns out of JSONs.
    """

    def _prepare_pairs(data: str):
        """Process JSON to tweet of interest (vanilla Python)"""
        data_parsed = json.loads(data, strict=False)
        # we care only about tweets that have reference to another tweet
        if "referenced_tweets" not in data_parsed["tweet"]:
            return None
        if data_parsed["tweet"]["referenced_tweets"] == 0:
            return None
        ref_tweet_id = data_parsed["tweet"]["referenced_tweets"][0][
            "id"
        ]  # usually there is a single referenced tweet. we disregard rest of them.
        ref_tweet = None
        if "tweets" in data_parsed["includes"]:
            ref_tweet = next(
                filter(
                    lambda t: t["id"] == ref_tweet_id, data_parsed["includes"]["tweets"]
                ),
                None,
            )
        author_user = next(
            filter(
                lambda u: u["id"] == data_parsed["tweet"]["author_id"],
                data_parsed["includes"]["users"],
            ),
            None,
        )
        other_user = None
        # sometimes referenced tweet is not there, due to permission
        if ref_tweet is not None:
            other_author_id = ref_tweet["author_id"]
            other_user = next(
                filter(
                    lambda u: u["id"] == other_author_id,
                    data_parsed["includes"]["users"],
                ),
                None,
            )
        if author_user is None or other_user is None:
            return None
        if "location" not in author_user or "location" not in other_user:
            return None
        return (
            data_parsed["tweet"]["id"],
            data_parsed["tweet"]["created_at"],
            data_parsed["tweet"]["text"],
            author_user["id"],
            author_user["username"],
            author_user["location"],
            json.dumps(author_user["public_metrics"]),
            ref_tweet["id"],
            ref_tweet["created_at"],
            ref_tweet["text"],
            other_user["id"],
            other_user["username"],
            other_user["location"],
        )

    processed = tweets.select(processed=pw.apply(_prepare_pairs, tweets.data))
    processed = processed.filter(processed.processed.is_not_none())
    result = unpack_col(processed.processed, schema=TweetPairs)
    return result
