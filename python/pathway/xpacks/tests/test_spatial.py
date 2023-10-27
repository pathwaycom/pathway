# Copyright © 2023 Pathway

from __future__ import annotations

import pytest

import pathway as pw
from pathway.tests.utils import T, assert_table_equality_wo_index
from pathway.xpacks.spatial.geofencing import GeofenceIndex, is_in_geofence
from pathway.xpacks.spatial.h3 import h3_cover_geojson
from pathway.xpacks.spatial.index import H3Index


def test_H3_index():
    queries = T(
        """
          | lat     | lon     | sample_data
        1 | 51.1000 | 17.0300 | foo
        2 | 51.1010 | 17.0310 | bar
        3 | 10.0000 | 10.0000 | baz
        """
    )
    data = T(
        """
            | lat      | lon      | other_data
        111 | 51.0990  | 17.0290  | AAA
        112 | 51.1000  | 17.0300  | BBB
        113 | 51.1010  | 17.0310  | CCC
        114 | 51.1020  | 17.0320  | DDD
        """
    )
    index = H3Index(
        data,
        data.lat,
        data.lon,
        radius_meters=200,
    )
    result = index.join_on_distance(
        queries,
        queries.lat,
        queries.lon,
    ).select(
        sample_data=queries.sample_data,
        other_data=pw.right.other_data,
        dist_meters=pw.left.dist_meters.num.fill_na(-1).num.round(1),
    )
    assert_table_equality_wo_index(
        result,
        T(
            """
            sample_data | other_data | dist_meters
                    foo |        AAA | 131.5
                    foo |        BBB | 0.0
                    foo |        CCC | 131.5
                    bar |        BBB | 131.5
                    bar |        CCC | 0.0
                    bar |        DDD | 131.5
                    baz |       None | -1
        """
        ),
    )


def test_H3_index_bad_instance_coltypes():
    queries = T(
        """
          | instance | lat     | lon     | sample_data
        1 |        1 | 51.1000 | 17.0300 | foo
        2 |        1 | 51.1010 | 17.0310 | bar
        3 |        1 | 10.0000 | 10.0000 | baz
        """
    )
    data = T(
        """
            | instance | lat      | lon      | other_data
        111 |        A | 51.0990  | 17.0290  | AAA
        112 |        A | 51.1000  | 17.0300  | BBB
        113 |        A | 51.1010  | 17.0310  | CCC
        114 |        A | 51.1020  | 17.0320  | DDD
        """
    )
    index = H3Index(
        data,
        data.lat,
        data.lon,
        instance=data.instance,
        radius_meters=200,
    )
    with pytest.raises(ValueError):
        index.join_on_distance(
            queries,
            queries.lat,
            queries.lon,
            instance=queries.instance,
        )


def test_is_in_geofence():
    geojson = """
        {"coordinates":[[[10.0, 0.0],[12.0,0.0],[12.0,2.0],[10.0,2.0]]],"type":"Polygon"}
    """
    lon, lat = 11.0, 1.0
    assert is_in_geofence.__wrapped__(lat, lon, pw.Json.parse(geojson))

    lon, lat = 1.0, 11.0
    assert not is_in_geofence.__wrapped__(lat, lon, pw.Json.parse(geojson))


def test_assert_no_support_antimeridian():
    geojson = """
        {"coordinates":[[[-170.0, 0.0],[170,0.0],[170,2.0],[-170,2.0]]],"type":"Polygon"}
    """
    lon, lat = 175, 1.0
    with pytest.raises(AssertionError):
        is_in_geofence.__wrapped__(lat, lon, pw.Json.parse(geojson))


def test_h3_cover_geojson():
    geojson = """
        {"coordinates":[[
            [17.03, 51.10],
            [16.16, 51.21],
            [16.57, 51.84]
            ]],"type":"Polygon"}
    """
    result = h3_cover_geojson.__wrapped__(pw.Json.parse(geojson), 4)
    assert set(result) == (
        set(
            [
                595005175547035647,
                595005579273961471,
                595005424655138815,
                595005544914223103,
                595005141187297279,
            ]
        )
    )


def test_geofence_index():
    @pw.udf
    def json_parse(col: str) -> pw.Json:
        return pw.Json.parse(col)

    queries = pw.debug.table_from_markdown(
        """
          |  lon  | lat     | sample_data
        1 |  11.0 | 1.0     | foo
        2 |  11.0 | 21.0    | bar
        3 |  31.0 | 31.0    | baz
    """
    )
    data = pw.debug.table_from_markdown(
        """
            | other_data  | geometry
        111 |         AAA | {"coordinates":[[[10.0,0.0],[12.0,0.0],[12.0,2.0],[10.0,2.0]]],"type":"Polygon"}
        222 |         BBB | {"coordinates":[[[10.0,20.0],[12.0,20.0],[12.0,22.0],[10.0,22.0]]],"type":"Polygon"}
    """
    ).select(pw.this.other_data, geometry=json_parse(pw.this.geometry))
    index = GeofenceIndex(
        data,
        data.geometry,
        resolution_meters=100_000,
    )
    res = index.join_enclosing_geofences(
        queries,
        lat=queries.lat,
        lon=queries.lon,
    ).select(
        sample_data=queries.sample_data,
        other_data=pw.right.other_data,
    )
    expected = T(
        """
        sample_data | other_data
        foo         | AAA
        bar         | BBB
        baz         |
    """
    )
    assert_table_equality_wo_index(res, expected)
