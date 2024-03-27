from dataclasses import dataclass

import pathway.internals as pw
from pathway.stdlib.ml.classifiers import DistanceTypes
from pathway.stdlib.ml.index import KNNIndex


@dataclass
class DataIndex:

    def query(
        self,
        query_column: pw.ColumnExpression,
        number_of_matches: pw.ColumnExpression | int = 3,
        collapse_rows: bool = True,
        with_distances: bool = False,
        metadata_filter: pw.ColumnExpression | None = None,
    ): ...

    def query_as_of_now(
        self,
        query_column: pw.ColumnExpression,
        number_of_matches: pw.ColumnExpression | int = 3,
        collapse_rows: bool = True,
        with_distances: bool = False,
        metadata_filter: pw.ColumnExpression | None = None,
    ): ...


class VectorDocumentIndex(DataIndex):
    def __init__(
        self,
        data_documents: pw.ColumnExpression,
        data: pw.Table,
        embedder: pw.UDF,
        n_dimensions: int,
        n_or: int = 20,
        n_and: int = 10,
        bucket_length: float = 10.0,
        distance_type: DistanceTypes = "euclidean",
        metadata: pw.ColumnExpression | None = None,
    ):
        embeddings = data.select(embeddings=embedder(data_documents))
        self.inner_index = KNNIndex(
            embeddings.embeddings,
            data,
            n_dimensions,
            n_or,
            n_and,
            bucket_length,
            distance_type,
            metadata,
        )
        self.embedder = embedder

    def query(
        self,
        query_column: pw.ColumnReference,  # type: ignore
        number_of_matches: pw.ColumnExpression | int = 3,
        collapse_rows: bool = True,
        with_distances: bool = False,
        metadata_filter: pw.ColumnExpression | None = None,
    ):
        query_embedding = query_column.table.select(
            embeddings=self.embedder(query_column)
        )
        return self.inner_index.get_nearest_items(
            query_embedding.embeddings,
            number_of_matches,
            collapse_rows,
            with_distances,
            metadata_filter,
        )

    def query_as_of_now(
        self,
        query_column: pw.ColumnReference,  # type: ignore
        number_of_matches: pw.ColumnExpression | int = 3,
        collapse_rows: bool = True,
        with_distances: bool = False,
        metadata_filter: pw.ColumnExpression | None = None,
    ):
        query_embedding = query_column.table.select(
            embeddings=self.embedder(query_column)
        )
        return self.inner_index.get_nearest_items_asof_now(
            query_embedding.embeddings,
            number_of_matches,
            collapse_rows,
            with_distances,
            metadata_filter,
        )
