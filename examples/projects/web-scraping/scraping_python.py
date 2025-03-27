# Copyright © 2025 Pathway

from __future__ import annotations

import json
import logging
import time
import warnings
from dataclasses import dataclass
from typing import Generator, Protocol, TypedDict

import newspaper
from newspaper.exceptions import ArticleBinaryDataException, ArticleException
from newsplease import NewsPlease
from typing_extensions import NotRequired

DEFAULT_REQUEST_PARAMS = {
    "timeout": 25,
    "proxies": {},
    "headers": {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.101.76 Safari/537.36",  # noqa: E501
    },
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


@dataclass
class ExtractedPage:
    url: str
    description: str
    article_urls: list[str]


class BasicNewsArticle(TypedDict):
    text: NotRequired[str]


class NewsArticle(TypedDict):
    image_url: NotRequired[str]
    localpath: NotRequired[str]
    text: NotRequired[str]
    maintext: NotRequired[str]
    authors: list[str]
    date_download: str
    date_modify: str
    date_publish: str
    description: str
    filename: str
    language: str
    source_domain: str
    title: str
    title_page: str
    title_rss: str
    url: str


def _remove_article_metadata(article: NewsArticle) -> BasicNewsArticle:
    """Only keep the text field in the article dict."""
    return BasicNewsArticle(text=article["text"])


def _clean_article_metadata(article: NewsArticle) -> NewsArticle:
    """Remove empty metadata fields."""
    article.pop("image_url")
    article.pop("localpath")

    return article


def _locate_set_text(article: NewsArticle) -> NewsArticle:
    if "maintext" in article and article["maintext"]:
        if not article["text"]:
            article["text"] = article["maintext"]

        article.pop("maintext")

    return article


class ArticleListProvider(Protocol):
    """Protocol defining how to get a list of article URLs from a website."""

    def list_articles(self, website: str) -> list[str]:
        """Returns a list of article URLs for the given website."""
        ...


@dataclass
class NewspaperListArticles(ArticleListProvider):
    request_params: dict | None = None
    thread_timeout_seconds: int = 30
    number_threads: int = 3

    def __post_init__(self):
        if self.request_params is None:
            self.request_params = DEFAULT_REQUEST_PARAMS

    def _build_site_map(self, url) -> ExtractedPage:
        page = newspaper.build(
            url,
            number_threads=self.number_threads,
            thread_timeout_seconds=self.thread_timeout_seconds,
            requests_params=self.request_params,
        )
        return ExtractedPage(page.url, page.description, page.article_urls())

    def list_articles(self, website: str) -> list[str]:
        try:
            web_pages = self._build_site_map(website)
            return web_pages.article_urls
        except (ArticleBinaryDataException, ArticleException, AttributeError) as e:
            warnings.warn(f"cannot fetch articles for {website}, {e}")
            return []


def scrape_articles(
    website_urls: list[str],
    refresh_interval: int = 600,
) -> Generator[dict[str, str | dict], None, None]:
    indexed_articles: set[str] = set()
    expand_articles: list[str] = []

    logging.info(f"Starting webscraper with number of urls: {len(website_urls)}")

    while True:
        for website in website_urls:
            logging.info(f"Extracting articles from: {website}")
            try:
                article_fetcher = NewspaperListArticles()
                page_article_urls = article_fetcher.list_articles(website)
            except ArticleBinaryDataException as e:
                warnings.warn(f"cannot fetch articles for {website}, {e}")
                continue
            else:
                logging.info(f"{website} found {len(page_article_urls)} articles.")

                expand_articles.extend(page_article_urls)

        logging.info(f"Total number of articles: {len(expand_articles)}")

        expand_articles = [i for i in expand_articles if i not in indexed_articles]

        article_ls: list = list(
            NewsPlease.from_urls(expand_articles, request_args={"timeout": 60}).values()
        )  # key: url, value: article. May have None entries

        articles: list[NewsArticle] = [
            article.get_serializable_dict() for article in article_ls if article
        ]

        logging.info(f"Number of fetched articles: {len(articles)}")

        for article in articles:
            url = article["url"]

            if url in indexed_articles:
                continue

            article = _locate_set_text(article)

            clean_article: NewsArticle | BasicNewsArticle = _clean_article_metadata(
                article
            )

            text = clean_article.pop("text", url)

            if text is None:
                continue

            metadata = clean_article

            indexed_articles.add(url)
            yield {"url": url, "text": text, "metadata": dict(metadata)}

        time.sleep(refresh_interval)


if __name__ == "__main__":
    websites = ["https://www.bbc.com/"]

    for article in scrape_articles(websites):
        print("Scraped article: ", json.dumps(article, indent=2))
