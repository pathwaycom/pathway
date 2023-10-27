import bisect
from itertools import pairwise

import h3.api.basic_int as h3
from shapely import Polygon, geometry

import pathway as pw


@pw.udf
def cell_id(lat: float, lon: float, h3_level: int) -> int:
    return h3.geo_to_h3(lat, lon, h3_level)


@pw.udf
def neighboring_cells(cell_id: int) -> tuple[int, ...]:
    return tuple(h3.k_ring(cell_id))


def _get_level_from_radius(radius_meters):
    """Calculate H3 level which should be used for spatial index.

    As we take points in the same cell and neighboring cells,
    the radius_meters should be larger than the minimum edge length.
    """
    # https://h3geo.org/docs/core-library/restable#edge-lengths
    h3_avg_edge_lengths_meters = {
        0: 1281256.011,
        1: 483056.8391,
        2: 182512.9565,
        3: 68979.22179,
        4: 26071.75968,
        5: 9854.090990,
        6: 3724.532667,
        7: 1406.475763,
        8: 531.414010,
        9: 200.786148,
        10: 75.863783,
        11: 28.663897,
        12: 10.830188,
        13: 4.092010,
        14: 1.546100,
        15: 0.584169,
    }
    SAFETY_FACTOR = 1.5  # minimum edge length is smaller than the average one.
    # optional todo: more precise safety factor

    lvl = len(h3_avg_edge_lengths_meters) - bisect.bisect_left(
        sorted(h3_avg_edge_lengths_meters.values()), radius_meters * SAFETY_FACTOR
    )
    assert 0 <= lvl <= 15
    return lvl


@pw.udf
def h3_cover_geojson(geojson: pw.Json, h3_level: int) -> tuple[int, ...]:
    """Covers geojson with H3 cells at the given level.

    Built-in h3.polyfill is not enough as it outputs H3 cells for which their centroids fall into geojson.
    """
    shape = geometry.shape(geojson.value)
    assert isinstance(shape, Polygon)
    assert not _shape_crosses_antimeridan(shape)

    interior_h3 = h3.polyfill(
        geometry.mapping(shape), h3_level, geo_json_conformant=True
    )
    vertices = shape.exterior.coords
    vertices_h3 = [h3.geo_to_h3(v[1], v[0], h3_level) for v in vertices]
    exterior_h3 = set.union(
        *[set(h3.h3_line(start, end)) for start, end in pairwise(vertices_h3)]
    )

    return tuple(set.union(interior_h3, exterior_h3))


def _shape_crosses_antimeridan(shape):
    lons, _ = zip(*shape.exterior.coords)
    return max(lons) - min(lons) > 180.0
