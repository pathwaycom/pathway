# Copyright © 2024 Pathway

from __future__ import annotations

import os

import tweepy

import pathway as pw

BEARER_TOKEN = os.environ["TWITTER_API_TOKEN"]


class TwitterClient(tweepy.StreamingClient):
    _subject: TwitterSubject

    def __init__(self, subject: TwitterSubject) -> None:
        super().__init__(BEARER_TOKEN)
        self._subject = subject

    def on_response(self, response) -> None:
        self._subject.next_json(
            {
                "id": response.data.id,
                "text": response.data.text,
            }
        )


class TwitterSubject(pw.io.python.ConnectorSubject):
    _twitter_client: TwitterClient

    def __init__(self) -> None:
        super().__init__()
        self._twitter_client = TwitterClient(self)

    def run(self) -> None:
        self._twitter_client.sample()

    def on_stop(self) -> None:
        self._twitter_client.disconnect()


if __name__ == "__main__":

    class InputSchema(pw.Schema):
        key: int = pw.column_definition(primary_key=True)
        text: str

    input = pw.io.python.read(
        TwitterSubject(),
        schema=InputSchema,
        autocommit_duration_ms=1000,
    )

    pw.io.csv.write(input, "output.csv")

    try:
        pw.run()
    except KeyboardInterrupt:
        print("Done.")
