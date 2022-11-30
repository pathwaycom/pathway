import json
import math

import dateutil.parser
import numpy as np
from geopy import distance as geo_distance
from textblob import TextBlob

import pathway as pw
from pathway.stdlib.utils.col import groupby_reduce_majority


def compute_metadata_for_authors(tweet_pairs: pw.Table):
    unique_authors = tweet_pairs.groupby(tweet_pairs.tweet_to_author_id).reduce(
        tweet_pairs.tweet_to_author_id
    )
    # unique_authors = tweet_pairs.groupby(id=tweet_pairs.tweet_to_author_id).reduce()  # fails - why?!
    author_coords = groupby_reduce_majority(
        tweet_pairs.tweet_to_author_id, tweet_pairs.coord_to
    )
    author_coords = (
        author_coords.select(coord_to=author_coords.majority)
        .with_id_from(author_coords.tweet_to_author_id)
        .unsafe_promise_same_universe_as(unique_authors)
    )
    author_username = groupby_reduce_majority(
        tweet_pairs.tweet_to_author_id, tweet_pairs.tweet_to_author_username
    )
    author_username = (
        author_username.select(author_username=author_username.majority)
        .with_id_from(author_username.tweet_to_author_id)
        .unsafe_promise_same_universe_as(unique_authors)
    )
    author_location = groupby_reduce_majority(
        tweet_pairs.tweet_to_author_id, tweet_pairs.tweet_to_author_location
    )
    author_location = (
        author_location.select(author_location=author_location.majority)
        .with_id_from(author_location.tweet_to_author_id)
        .unsafe_promise_same_universe_as(unique_authors)
    )

    def add_random_shift(coords, id):
        if isinstance(coords, str):
            return coords

        gen = np.random.default_rng(seed=int(id))
        randomAngle = gen.random() * math.pi * 2
        radius = 0.02
        dx = math.cos(randomAngle) * radius
        dy = math.sin(randomAngle) * radius
        return [coords[0] + dx, coords[1] + dy]

    author_coords_shifted = author_coords.select(
        coord_shifted=pw.apply(
            add_random_shift, author_coords.coord_to, author_coords.id
        )
    )
    return (
        unique_authors
        + author_coords
        + author_location
        + author_username
        + author_coords_shifted
    )


def add_distance_and_buckets(tweet_pairs, distance_thresholds=[200, 2000]):
    def _compute_dist(coord_from, coord_to):
        try:
            dist = geo_distance.distance(
                list(reversed(coord_from)), list(reversed(coord_to))
            ).kilometers
        except Exception:
            return 1e9
        return dist

    tweet_pairs += tweet_pairs.select(
        distance=pw.apply(_compute_dist, tweet_pairs.coord_from, tweet_pairs.coord_to)
    )

    def _bucketize_distance(x):
        labels = {0: "close", 1: "medium", 2: "far"}
        for i in range(len(distance_thresholds)):
            if x < distance_thresholds[i]:
                return labels[i]
        return labels[len(distance_thresholds)]

    tweet_pairs += tweet_pairs.select(
        bucketed_distance=pw.apply(_bucketize_distance, tweet_pairs.distance)
    )
    return tweet_pairs


def add_sentiment(tweet_pairs):
    def _compute_sentiment(txt):
        return TextBlob(txt).sentiment.polarity

    tweet_pairs += tweet_pairs.select(
        tweet_from_sentiment=pw.apply(_compute_sentiment, tweet_pairs.tweet_from_text)
    )
    tweet_pairs += tweet_pairs.select(
        tweet_to_sentiment=pw.apply(_compute_sentiment, tweet_pairs.tweet_to_text)
    )

    return tweet_pairs


def add_magic_influence(tweet_pairs: pw.Table):
    def _compute_magic_influence(public_metrics):
        public_metrics_dict = json.loads(public_metrics)
        return (
            public_metrics_dict["followers_count"] ** 0.5
            / public_metrics_dict["tweet_count"] ** 0.25
        )

    tweet_pairs += tweet_pairs.select(
        author_from_magic_influence=pw.apply(
            _compute_magic_influence, tweet_pairs.tweet_from_author_public_metrics
        )
    )

    return tweet_pairs


def compute_aggs_per_author_and_timewindow(tweet_pairs):
    """Computes aggregate statistics for every author_id and date window."""

    def _bucketize_datetime(x):
        return int(dateutil.parser.parse(x).replace(second=0).timestamp())

    tweet_pairs += tweet_pairs.select(
        time_bucket=pw.apply(_bucketize_datetime, tweet_pairs.tweet_from_created_at)
    )

    total_stats = tweet_pairs.groupby(
        tweet_pairs.tweet_to_author_id,
        tweet_pairs.time_bucket,
    ).reduce(
        tweet_to_author_id=tweet_pairs.tweet_to_author_id,
        time_bucket=tweet_pairs.time_bucket,
        tweet_to_sentiment=pw.reducers.sum(tweet_pairs.tweet_to_sentiment),
        author_from_magic_influence=pw.reducers.sum(
            tweet_pairs.author_from_magic_influence
        ),
        responses_count=pw.reducers.count(),
        close_count=0,
        medium_count=0,
        far_count=0,
    )

    for distance_bucket in ["close", "medium", "far"]:
        pairs = tweet_pairs.filter(tweet_pairs.bucketed_distance == distance_bucket)
        stats = (
            pairs.groupby(pairs.tweet_to_author_id, pairs.time_bucket)
            .reduce(**{distance_bucket + "_count": pw.reducers.count()})
            .unsafe_promise_universe_is_subset_of(total_stats)
        )
        total_stats = total_stats.update_cells(stats)
    return total_stats


def _mark_coords_in_set(tweet_pairs: pw.Table, bad_coords: pw.Table):
    """Mark all tweet pairs using a coord from bad_coords as bad."""
    is_good_update = tweet_pairs.join(
        bad_coords, tweet_pairs.coord_to == bad_coords.coord, id=tweet_pairs.id
    ).select(is_good=False)
    is_good_update = is_good_update.unsafe_promise_universe_is_subset_of(tweet_pairs)
    tweet_pairs = tweet_pairs.update_cells(is_good_update)

    is_good_update = tweet_pairs.join(
        bad_coords, tweet_pairs.coord_from == bad_coords.coord, id=tweet_pairs.id
    ).select(is_good=False)
    is_good_update = is_good_update.unsafe_promise_universe_is_subset_of(tweet_pairs)
    tweet_pairs = tweet_pairs.update_cells(is_good_update)

    return tweet_pairs


def _mark_bad_coords(
    tweet_pairs: pw.Table,
    total: pw.Table,
    TOTAL_RESPONSES_NEEDED,
    CLOSE_FRACTION_CUTOFF,
):
    """Find bad locations: a coord is bad if all reliable authors
    with this coord have insufficient fraction of close responses."""

    # find reliable authors
    reliable_authors = total.filter(total.responses_count >= TOTAL_RESPONSES_NEEDED)
    # compute fraction of close responses for each reliable author
    reliable_authors += reliable_authors.select(
        close_fraction=reliable_authors.close_count / reliable_authors.responses_count
    )
    reliable_authors += reliable_authors.select(
        is_good=reliable_authors.close_fraction > CLOSE_FRACTION_CUTOFF
    )
    coords_goodness = reliable_authors.groupby(reliable_authors.coord_to).reduce(
        coord=reliable_authors.coord_to,
        is_coord_good=pw.reducers.max(reliable_authors.is_good),
    )
    bad_coords = coords_goodness.filter(
        coords_goodness.is_coord_good == False  # noqa: E712
    )
    tweet_pairs = _mark_coords_in_set(tweet_pairs, bad_coords)
    return tweet_pairs


def _filter_out_locations_by_closeness_step(
    tweet_pairs: pw.Table, author_meta: pw.Table
):
    TOTAL_RESPONSES_NEEDED = 80  # we need enough responses for an author to reliably test validness of his location
    CLOSE_FRACTION_CUTOFF = (
        0.005  # fraction of close responses needed to consider author's location valid
    )
    good_tweet_pairs = tweet_pairs.filter(tweet_pairs.is_good == True)  # noqa: E712
    grouped = compute_aggs_per_author_and_timewindow(good_tweet_pairs)

    # sum all statistics for referenced authors (ignore time)
    referenced_authors_total_stats = grouped.groupby(grouped.tweet_to_author_id).reduce(
        tweet_to_author_id=grouped.tweet_to_author_id,
        tweet_to_sentiment=pw.reducers.sum(grouped.tweet_to_sentiment),
        author_from_magic_influence=pw.reducers.sum(
            grouped.author_from_magic_influence
        ),
        responses_count=pw.reducers.sum(grouped.responses_count),
        close_count=pw.reducers.sum(grouped.close_count),
        medium_count=pw.reducers.sum(grouped.medium_count),
        far_count=pw.reducers.sum(grouped.far_count),
    )
    referenced_authors_total_stats = referenced_authors_total_stats.join(
        author_meta,
        referenced_authors_total_stats.tweet_to_author_id
        == author_meta.tweet_to_author_id,
    ).select(
        referenced_authors_total_stats.tweet_to_author_id,
        referenced_authors_total_stats.tweet_to_sentiment,
        referenced_authors_total_stats.author_from_magic_influence,
        referenced_authors_total_stats.responses_count,
        referenced_authors_total_stats.close_count,
        referenced_authors_total_stats.medium_count,
        referenced_authors_total_stats.far_count,
        author_meta.author_username,
        author_meta.coord_to,
    )
    tweet_pairs = _mark_bad_coords(
        tweet_pairs,
        referenced_authors_total_stats,
        TOTAL_RESPONSES_NEEDED,
        CLOSE_FRACTION_CUTOFF,
    )
    return tweet_pairs


def filter_out_locations_by_closeness(tweet_pairs: pw.Table, author_meta: pw.Table):
    """Iteratively filter out bad locations using distance to referenced tweets.

    Underlying idea is that if the user does not have retweets in a close area, then his location is probably faulty.
    """
    tweet_pairs += tweet_pairs.select(is_good=True)
    return pw.iterate(
        lambda tweet_pairs, author_meta: dict(
            tweet_pairs=_filter_out_locations_by_closeness_step(
                tweet_pairs=tweet_pairs, author_meta=author_meta
            ),
            author_meta=author_meta,
        ),
        # we don't need iterate_universe - tweet_pairs use marking to disable some rows
        tweet_pairs=tweet_pairs,
        author_meta=author_meta,
    ).tweet_pairs


def enrich_metadata_with_total_influence(grouped: pw.Table, author_meta: pw.Table):
    """Adds total statistics to authors data."""

    enrichment = (
        grouped.groupby(grouped.tweet_to_author_id)
        .reduce(
            tweet_to_author_id=grouped.tweet_to_author_id,
            total_responses=pw.reducers.sum(grouped.responses_count),
            total_magic_influence=pw.reducers.sum(grouped.author_from_magic_influence),
        )
        .with_id_from(pw.this.tweet_to_author_id)
        .select(
            pw.this.tweet_to_author_id,
            pw.this.total_responses,
            pw.this.total_magic_influence,
        )
    )
    return enrichment.join(
        author_meta, enrichment.tweet_to_author_id == author_meta.tweet_to_author_id
    ).select(
        enrichment.tweet_to_author_id,
        enrichment.total_responses,
        enrichment.total_magic_influence,
        author_meta.coord_to,
        author_meta.author_username,
        author_meta.author_location,
        author_meta.coord_shifted,
    )
