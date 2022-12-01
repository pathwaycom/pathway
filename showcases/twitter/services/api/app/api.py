"""Thin fastapi endpoint querying postgres database and answering requests from frontend."""

import psycopg2
import psycopg2.extras
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

ctx = {}


def run_sql(sql):
    """Runs sql against database."""
    cursor = ctx["connection"].cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cursor.execute(sql)
    results = cursor.fetchall()
    return results


@app.on_event("startup")
async def startup_event():
    """Inits application.

    Establishes a connection to the database.
    """
    ctx["connection"] = psycopg2.connect(
        database="postgres",
        user="postgres",
        password="changeme",
        host="postgres",
        port="5432",
    )
    ctx["connection"].autocommit = True


@app.get("/impact", tags=["impact"])
async def read_impact(start: int, end: int) -> dict:
    """Read aggregated statistics regarding impact of popular twitter authors."""
    sql = f"""
    SELECT
        author_meta.tweet_to_author_id,
        author_meta.author_username,
        author_meta.author_location,
        author_meta.coord_to,
        author_meta.coord_shifted,
        author_meta.total_responses,
        author_meta.total_magic_influence,
        coalesce(agg.responses_count, 0) as responses_count,
        coalesce(agg.mean_sentiment, 0) as mean_sentiment,
        coalesce(agg.magic_influence, 0) as magic_influence,
        coalesce(agg.far_count, 0) as far_count,
        coalesce(agg.medium_count, 0) as medium_count,
        coalesce(agg.close_count, 0) as close_count
    FROM author_meta
    LEFT JOIN (
        SELECT
            tweet_to_author_id,
            SUM(responses_count) as responses_count,
            SUM(tweet_from_sentiment) / SUM(responses_count) as mean_sentiment,
            SUM(author_from_magic_influence) as magic_influence,
            SUM(far_count) as far_count,
            SUM(medium_count) as medium_count,
            SUM(close_count) as close_count
        FROM grouped
        WHERE
            time_bucket >= {start} AND time_bucket < {end}
            AND diff = 1
        GROUP BY tweet_to_author_id
    ) as agg
    ON agg.tweet_to_author_id = author_meta.tweet_to_author_id
    WHERE
        author_meta.diff = 1
    ORDER BY author_meta.tweet_to_author_id

    """

    return {"data": run_sql(sql)}


@app.get("/stats", tags=["stats"])
async def read_stats() -> dict:
    """Read aggregated statistics regarding impact of popular twitter authors."""
    sql = """
    WITH grouped_all_stats AS (
            SELECT
                min(time_bucket) as min_time_bucket,
                max(time_bucket) as max_time_bucket,
                SUM(responses_count) as total_tweetpairs_good
            FROM grouped
            WHERE diff = 1
        ),
        all_tweets_stats AS (
            SELECT
                count(tweet_from_id) as total_tweets
            FROM tweet_pairs
            WHERE diff = 1
        )
    SELECT * FROM grouped_all_stats, all_tweets_stats
    """

    return run_sql(sql)[0]
