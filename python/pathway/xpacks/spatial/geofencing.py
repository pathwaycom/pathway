from shapely import Point, geometry

import pathway as pw
from pathway.internals.dtype import dtype_equivalence
from pathway.internals.runtime_type_check import runtime_type_check
from pathway.internals.trace import trace_user_frame
from pathway.internals.type_interpreter import eval_type
from pathway.xpacks.spatial.h3 import (
    _get_level_from_radius,
    _shape_crosses_antimeridan,
    cell_id,
    h3_cover_geojson,
)


@pw.udf
def is_in_geofence(lat: float, lon: float, geojson_geometry: pw.Json) -> bool:
    """Test if point is inside a geojson polygon"""
    if geojson_geometry is None:
        return False
    shape = geometry.shape(geojson_geometry.value)
    assert shape.geom_type == "Polygon"
    assert not _shape_crosses_antimeridan(
        shape
    )  # todo add support for antimeridian, e.g. via
    # https://gist.github.com/PawaritL/ec7136c0b718ca65db6df1c33fd1bb11#file-polygon_splitter-py

    return shape.contains(Point(lon, lat))


class GeofenceIndex:
    """
    H3-based geospatial index allowing for efficient point location inside geofences.

    Geofences are mapped to the corresponding cells id at a fixed hierarchy level.

    See https://h3geo.org/docs/highlights/indexing/ for the description of H3 index structure.

    Parameters:

    data (pw.Table): The table containing the data to be indexed.
    geometry (pw.ColumnExpression): The column expression representing geofences as geojsons.
    resolution_meters (float): approximately determines how large covering H3 cells should be
    instance (pw.ColumnExpression or None): The column expression representing the instance of the index
        allowing for creating multiple indexes at once.

    Caveats:

    Geofences crossing antimeridian are not yet supported.
    """

    @runtime_type_check
    def __init__(
        self,
        data: pw.Table,
        geojson_geometry: pw.ColumnExpression,
        resolution_meters: int | float,
        instance: pw.ColumnExpression | None = None,
    ):
        self.data = data
        self.resolution_meters = resolution_meters
        self.h3_level = _get_level_from_radius(resolution_meters)
        if instance is not None:
            self.instance = data.select(instance=instance).instance

        self.index = self._build_index(
            data.select(
                geojson_geometry=geojson_geometry, instance=instance, data_id=data.id
            )
        )

    def _build_index(self, t: pw.Table):
        t += t.select(cell_id=h3_cover_geojson(t.geojson_geometry, self.h3_level))
        index = t.flatten(pw.this.cell_id, *t.without(t.cell_id))
        return index

    @runtime_type_check
    @trace_user_frame
    def join_enclosing_geofences(
        self,
        query_table: pw.Table,
        *,
        lat: pw.ColumnExpression,
        lon: pw.ColumnExpression,
        instance: pw.ColumnExpression | None = None,
    ) -> pw.JoinResult:
        """
        Efficiently joins (via left_join) rows of query table with rows of indexed geofences
        for which the query point is inside a target geofence.

        Parameters:
        query_table (pw.Table): The table containing the queries.
        lat (pw.ColumnExpression): The column expression representing latitudes (degrees) in the query_table.
        lon (pw.ColumnExpression): The column expression representing longitudes (degrees) in the query_table.
        instance (pw.ColumnExpression or None): The column expression representing the instance of the index
            allowing for parallel queries to multiple indexes at once.

        Returns:
            pw.JoinResult: result of a join between query_table and indexed data table

        Example:

        >>> import pathway as pw
        >>> queries = pw.debug.table_from_markdown('''
        ...   |  lon  | lat     | sample_data
        ... 1 |  11.0 | 1.0     | foo
        ... 2 |  11.0 | 21.0    | bar
        ... 3 |  20.0 | 1.0     | baz
        ... ''')
        >>> @pw.udf
        ... def json_parse(col: str) -> pw.Json:
        ...     return pw.Json.parse(col)
        >>> data = pw.debug.table_from_markdown('''
        ...     | other_data  | geometry
        ... 111 |         AAA | {"coordinates":[[[10.0,0.0],[12.0,0.0],[12.0,2.0],[10.0,2.0]]],"type":"Polygon"}
        ... 222 |         BBB | {"coordinates":[[[10.0,20.0],[12.0,20.0],[12.0,22.0],[10.0,22.0]]],"type":"Polygon"}
        ... ''').with_columns(geometry=json_parse(pw.this.geometry))
        >>> index = pw.xpacks.spatial.geofencing.GeofenceIndex(
        ...     data, data.geometry, resolution_meters=100_000,
        ... )
        >>> res = index.join_enclosing_geofences(
        ...     queries,
        ...     lat=queries.lat,
        ...     lon=queries.lon,
        ... ).select(
        ...     queries.sample_data,
        ...     pw.right.other_data,
        ... )
        >>> pw.debug.compute_and_print(res, include_id=False)
        sample_data | other_data
        bar         | BBB
        baz         |
        foo         | AAA
        """
        index = self.index

        if instance is not None:
            assert self.instance is not None
            dtype_instance = eval_type(instance)
            dtype_index_instance = eval_type(self.instance)
            if not dtype_equivalence(dtype_instance, dtype_index_instance):
                raise ValueError("Instance columns do not have the same type.")

        queries = query_table.select(lat=lat, lon=lon, instance=instance)
        queries += queries.select(
            cell_id=cell_id(queries.lat, queries.lon, self.h3_level)
        )

        if instance is not None:
            joined = queries.join_left(
                index,
                queries.instance == index.instance,
                queries.cell_id == index.cell_id,
            )
        else:
            joined = queries.join_left(
                index,
                queries.cell_id == index.cell_id,
            )
        query_candidates = joined.select(
            pw.left.instance,
            queries.lat,
            queries.lon,
            index.data_id,
            index.geojson_geometry,
            query_id=queries.id,
        )
        query_result = query_candidates.filter(
            is_in_geofence(pw.this.lat, pw.this.lon, pw.this.geojson_geometry)
        )
        return query_table.join_left(
            query_result, query_table.id == query_result.query_id
        ).join_left(self.data, query_result.data_id == self.data.id)
