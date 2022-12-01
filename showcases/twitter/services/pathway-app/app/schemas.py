from typing import Any

import pathway as pw


class TweetUnparsed(pw.Schema):
    data: str


class TweetPairs(pw.Schema):
    tweet_from_id: int
    tweet_from_created_at: Any  # todo: datetime type?
    tweet_from_text: str
    tweet_from_author_id: int
    tweet_from_author_username: str
    tweet_from_author_location: str
    tweet_from_author_public_metrics: Any
    tweet_to_id: int
    tweet_to_created_at: Any  # todo: datetime type?
    tweet_to_text: str
    tweet_to_author_id: int
    tweet_to_author_username: str
    tweet_to_author_location: str


class TweetPairsSentiment(pw.Schema):
    tweet_from_sentiment: float
    tweet_to_sentiment: float
