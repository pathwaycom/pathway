import gzip
import json
import pickle
import re
import urllib
from enum import Enum
from functools import partial
from urllib.error import HTTPError

from schemas import TweetPairs

import pathway as pw

# Threshold used to disregard too coarse locations (e.g. "United States")
AREA_THRESHOLD = 10

pelias_url = "http://pelias_geocoder:3000"

# Reading geocoding results from cache instead of querying pelias. Set None to not use cache.
# CACHE_FILE_URL = None
CACHE_FILE_URL = "geolocator_cache.pkl.gz"


class GeolocatorInvalidFlags(Enum):
    NORESULT = "NORESULT"
    TOO_LARGE_AREA = "TOO_LARGE_AREA"
    FORBIDDEN = "FORBIDDEN"


def geolocate_coarse_pelias(location):
    url = f"{pelias_url}/parser/search?text={urllib.parse.quote(location)}"
    try:
        with urllib.request.urlopen(url) as response:
            if response.getcode() == 200:
                response_body = response.read().decode()
                data = json.loads(response_body)
                if len(data) > 0:
                    return data[0]
        return GeolocatorInvalidFlags.NORESULT.value
    except HTTPError:
        return GeolocatorInvalidFlags.NORESULT.value


def geolocate_external(location: str, forbidden_list=[], cache=None):
    for forbidden in forbidden_list:
        if re.search(forbidden, location, re.IGNORECASE):
            return GeolocatorInvalidFlags.FORBIDDEN.value

    geolocated = (
        cache[location]
        if cache is not None and location in cache
        else geolocate_coarse_pelias(location)
    )
    if geolocated == GeolocatorInvalidFlags.NORESULT.value:
        return geolocated
    if "area" in geolocated["geom"] and geolocated["geom"]["area"] > AREA_THRESHOLD:
        return GeolocatorInvalidFlags.TOO_LARGE_AREA.value
    return (geolocated["geom"]["lon"], geolocated["geom"]["lat"])


def load_pickle(path):
    try:
        with gzip.open(path, "rb") as cache_f:
            df = pickle.load(cache_f)
        return df
    except Exception:
        return {}


def get_cached_geolocation_fun():
    cache = None
    if CACHE_FILE_URL is not None:
        cache = load_pickle(CACHE_FILE_URL)
    return partial(geolocate_external, cache=cache)


def add_geolocation(tweet_pairs: pw.Table[TweetPairs]) -> pw.Table:
    locations = pw.Table.concat(
        tweet_pairs.select(location=tweet_pairs.tweet_from_author_location),
        tweet_pairs.select(location=tweet_pairs.tweet_to_author_location),
    )
    unique_locations = locations.groupby(locations.location).reduce(locations.location)

    geocoded_locations = unique_locations + unique_locations.select(
        geocoded=pw.apply_async(get_cached_geolocation_fun(), unique_locations.location)
    )

    coord_from = (
        tweet_pairs.join(
            geocoded_locations,
            tweet_pairs.tweet_from_author_location == geocoded_locations.location,
            id=tweet_pairs.id,
        )
        .select(coord_from=geocoded_locations.geocoded)
        .unsafe_promise_same_universe_as(tweet_pairs)
    )
    coord_to = (
        tweet_pairs.join(
            geocoded_locations,
            tweet_pairs.tweet_to_author_location == geocoded_locations.location,
            id=tweet_pairs.id,
        )
        .select(coord_to=geocoded_locations.geocoded)
        .unsafe_promise_same_universe_as(tweet_pairs)
    )

    def is_good_location(geolocated) -> bool:
        return geolocated not in [e.value for e in GeolocatorInvalidFlags]

    coord_from_is_good = coord_from.select(
        is_good=pw.apply(is_good_location, coord_from.coord_from)
    )
    coord_to_is_good = coord_to.select(
        is_good=pw.apply(is_good_location, coord_to.coord_to)
    )
    tweet_pair_is_good = tweet_pairs.select(
        is_good=coord_from_is_good.is_good & coord_to_is_good.is_good
    )
    return tweet_pairs + coord_from + coord_to + tweet_pair_is_good
