# Copyright © 2024 Pathway

import os
import sys
from unittest.mock import patch

import pandas as pd

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
from main import process_tweets  # noqa: E402
from testing_utils import run_convert_pandas, star_around_author  # noqa: E402


def mock_geocoder(location):
    locations = {
        "Wrocław, Poland": [17.023978, 51.097349],
        "Tokyo, Japan": [139.393288, 35.675099],
        "Trzebnica, Poland": [17.063542, 51.309959],
    }
    coords = locations.get(location, [0, 0])
    return {"geom": {"area": 1, "lon": coords[0], "lat": coords[1]}}


@patch("geocoding.geolocate_coarse_pelias")
def test_iterative_filtering_by_closeness_removes_all(geolocate_coarse_pelias_mock):
    """Test checking that the authors get iteratively removed.

    Scenario: one author in Wrocław and one in Trzebnica.
    First, the Trzebnica location will be filtered out as there are no close references.
    Then, Wrocław location will be filtered out as well,
    since Trzebnica was the only close location
    (and it was filtered out in the previous iteration).
    """
    geolocate_coarse_pelias_mock.side_effect = mock_geocoder
    tweet_pairs = []
    tweet_pairs += star_around_author(
        location="Wrocław, Poland",
        location_close="Trzebnica, Poland",
        location_far="Tokyo, Japan",
        close_count=100,
        far_count=100,
    )
    tweet_pairs += star_around_author(
        location="Trzebnica, Poland",
        location_close="Wrocław, Poland",
        location_far="Tokyo, Japan",
        close_count=0,
        far_count=100,
    )
    data = pd.DataFrame({"data": tweet_pairs})

    tweet_pairs, grouped, author_meta = run_convert_pandas(process_tweets, data)

    assert len(author_meta) == 0
    assert len(grouped) == 0
    assert (tweet_pairs["is_good"] == False).all()  # noqa


@patch("geocoding.geolocate_coarse_pelias")
def test_iterative_filtering_by_closeness_does_not_remove_good(
    geolocate_coarse_pelias_mock,
):
    geolocate_coarse_pelias_mock.side_effect = mock_geocoder
    tweet_pairs = []
    tweet_pairs += star_around_author(
        location="Wrocław, Poland",
        location_close="Trzebnica, Poland",
        location_far="Tokyo, Japan",
        close_count=100,
        far_count=100,
    )
    data = pd.DataFrame({"data": tweet_pairs})
    tweet_pairs, grouped, author_meta = run_convert_pandas(process_tweets, data)
    assert len(author_meta) == 1
    assert tweet_pairs["is_good"].all()
