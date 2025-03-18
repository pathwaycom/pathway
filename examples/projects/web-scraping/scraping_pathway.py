# Copyright © 2025 Pathway

from __future__ import annotations

import logging

from scraping_python import scrape_articles

import pathway as pw
from pathway.io.python import ConnectorSubject

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


class NewsScraperSubject(ConnectorSubject):
    _website_urls: list[str]
    _refresh_interval: int

    def __init__(
        self,
        *,
        website_urls: list[str],
        refresh_interval: int,
    ) -> None:
        super().__init__()
        self._website_urls = website_urls
        self._refresh_interval = refresh_interval

    def run(self) -> None:
        for article in scrape_articles(
            self._website_urls,
            refresh_interval=self._refresh_interval,
        ):
            url = article["url"]
            text = article["text"]
            metadata = article["metadata"]

            self.next(url=url, text=text, metadata=metadata)


class ConnectorSchema(pw.Schema):
    url: str = pw.column_definition(primary_key=True)
    text: str
    metadata: dict


if __name__ == "__main__":
    websites = ["https://www.bbc.com/"]

    subject = NewsScraperSubject(
        website_urls=websites,
        refresh_interval=25,
    )

    web_articles = pw.io.python.read(subject, schema=ConnectorSchema)

    pw.io.jsonlines.write(web_articles, "scraped_web_articles.jsonl")

    pw.run(monitoring_level=pw.MonitoringLevel.NONE)
