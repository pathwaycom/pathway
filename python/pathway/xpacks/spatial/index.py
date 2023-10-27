import math

from geopy import distance

import pathway as pw
from pathway.internals.dtype import unoptionalize
from pathway.internals.runtime_type_check import runtime_type_check
from pathway.internals.trace import trace_user_frame
from pathway.xpacks.spatial.h3 import _get_level_from_radius, cell_id, neighboring_cells


@pw.udf
def dist(
    lat: float, lon: float, lat2: float | None, lon2: float | None
) -> float | None:
    if lat2 is None or lon2 is None:
        return None
    return distance.distance([lat, lon], [lat2, lon2]).meters


class H3Index:
    """
    H3-based geospatial index allowing for finding nearby lat lon points.

    Lat lon points are mapped to the corresponding cell id at a fixed hierarchy level.
    They are also mapped to the neighboring cells for fast closeby points retrieval.

    See https://h3geo.org/docs/highlights/indexing/ for the description of H3 index structure.

    Parameters:

    data (pw.Table): The table containing the data to be indexed.
    lat (pw.ColumnExpression): The column expression representing latitudes (degrees) in the data.
    lon (pw.ColumnExpression): The column expression representing longitudes (degrees) in the data.
    radius_meters (float): maximum distance supported
    instance (pw.ColumnExpression or None): The column expression representing the instance of the index
        allowing for creating multiple indexes at once.
    """

    class _LatLonSchema(pw.Schema):
        lat: float
        lon: float

    @runtime_type_check
    def __init__(
        self,
        data: pw.Table,
        lat: pw.ColumnExpression,
        lon: pw.ColumnExpression,
        radius_meters: int | float,
        instance: pw.ColumnExpression | None = None,
    ):
        self.data = data
        self.radius_meters = radius_meters
        self.h3_level = _get_level_from_radius(radius_meters)
        if instance is not None:
            self.instance = data.select(instance=instance).instance

        self.index = self._build_index(
            data.select(lat=lat, lon=lon, instance=instance, data_id=data.id)
        )

    def _build_index(self, t: pw.Table[_LatLonSchema]):
        t += t.select(true_cell_id=cell_id(t.lat, t.lon, self.h3_level))
        t += t.select(ring_cell_id=neighboring_cells(t.true_cell_id))
        index = t.flatten(pw.this.ring_cell_id, *t.without(t.ring_cell_id))
        return index

    @runtime_type_check
    @trace_user_frame
    def join_on_distance(
        self,
        query_table: pw.Table,
        query_lat: pw.ColumnExpression,
        query_lon: pw.ColumnExpression,
        distance_meters: int | float | None = None,
        instance: pw.ColumnExpression | None = None,
    ) -> pw.JoinResult:
        """
        This method efficiently joins (via left_join) rows of query table with rows of indexed data
        such that two points are within a certain distance.

        Parameters:
        query_table (pw.Table): The table containing the queries.
        lat (pw.ColumnExpression): The column expression representing latitudes (degrees) in the query_table.
        lon (pw.ColumnExpression): The column expression representing longitudes (degrees) in the query_table.
        instance (pw.ColumnExpression or None): The column expression representing the instance of the index
            allowing for parallel queries to multiple indexes at once.

        Returns:
            pw.JoinResult: result of a (distance-limited) join between query_table and indexed data table

        Example:

        >>> import pathway as pw
        >>> queries = pw.debug.table_from_markdown('''
        ...   | instance | lat     | lon     | sample_data
        ... 1 |        1 | 51.1000 | 17.0300 | foo
        ... 2 |        1 | 51.1010 | 17.0310 | bar
        ... 3 |        2 | 40.0000 | 179.999 | baz
        ... 4 |        2 | 10.0000 | 10.0000 | zzz
        ... ''')
        >>> data = pw.debug.table_from_markdown('''
        ...     | instance | lat      | lon      | other_data
        ... 111 |        1 | 51.0990  | 17.0290  | AAA
        ... 112 |        1 | 51.1000  | 17.0300  | BBB
        ... 113 |        1 | 51.1010  | 17.0310  | CCC
        ... 114 |        1 | 51.1020  | 17.0320  | DDD
        ... 311 |        2 | 40.0000  | 179.999  | EEE
        ... 313 |        2 | 40.0000  | -179.999 | FFF
        ... 314 |        2 | 40.0000  | -179.980 | GGG
        ... 412 |        2 | 51.1000  | 17.0300  | HHH
        ... ''')
        >>> index = pw.xpacks.spatial.index.H3Index(
        ...     data, data.lat, data.lon, instance=data.instance, radius_meters=200,
        ... )
        >>> res = index.join_on_distance(
        ...     queries,
        ...     queries.lat,
        ...     queries.lon,
        ...     instance=queries.instance,
        ... ).select(
        ...     instance=queries.instance,
        ...     sample_data=queries.sample_data,
        ...     other_data=pw.right.other_data,
        ...     dist_meters=pw.left.dist_meters.num.fill_na(-1).num.round(1),
        ... )
        >>> pw.debug.compute_and_print(res, include_id=False)
        instance | sample_data | other_data | dist_meters
        1        | bar         | BBB        | 131.5
        1        | bar         | CCC        | 0.0
        1        | bar         | DDD        | 131.5
        1        | foo         | AAA        | 131.5
        1        | foo         | BBB        | 0.0
        1        | foo         | CCC        | 131.5
        2        | baz         | EEE        | 0.0
        2        | baz         | FFF        | 170.8
        2        | zzz         |            | -1.0
        """
        index = self.index
        if distance_meters is None:
            distance_meters = self.radius_meters
        assert (
            distance_meters <= self.radius_meters
        ), f"Index was built to support queries with distance at most {self.radius_meters} meters."

        if instance is not None:
            assert self.instance is not None
            dtype_instance = (
                instance._to_internal()._column.dtype
                if isinstance(instance, pw.ColumnReference)
                else instance._dtype
            )
            dtype_index_instance = self.instance._to_internal()._column.dtype
            if unoptionalize(dtype_instance) != unoptionalize(dtype_index_instance):
                raise ValueError(
                    f"Instance columns do not have the same type: {dtype_instance} != {dtype_index_instance}"
                )

        queries = query_table.select(lat=query_lat, lon=query_lon, instance=instance)

        queries += queries.select(
            cell_id=cell_id(queries.lat, queries.lon, self.h3_level)
        )

        if instance is not None:
            joined = queries.join_left(
                index,
                queries.instance == index.instance,
                queries.cell_id == index.ring_cell_id,
            )
        else:
            joined = queries.join_left(
                index,
                queries.cell_id == index.ring_cell_id,
            )
        query_candidates = (
            joined.select(
                pw.left.instance,
                queries.lat,
                queries.lon,
                index.data_id,
                index_lat=index.lat,
                index_lon=index.lon,
                query_id=queries.id,
                dist_meters=dist(queries.lat, queries.lon, index.lat, index.lon),
            )
            .groupby(pw.this.query_id, pw.this.data_id)
            .reduce(
                pw.this.query_id,
                pw.this.data_id,
                pw.reducers.unique(pw.this.instance),
                pw.reducers.unique(pw.this.lat),
                pw.reducers.unique(pw.this.lon),
                pw.reducers.unique(pw.this.index_lat),
                pw.reducers.unique(pw.this.index_lon),
                pw.reducers.unique(pw.this.dist_meters),
            )
        )
        query_result = query_candidates.filter(
            query_candidates.dist_meters.num.fill_na(math.inf) < distance_meters
        )

        return query_table.join_left(
            query_result, query_table.id == query_result.query_id
        ).join_left(self.data, query_result.data_id == self.data.id)
