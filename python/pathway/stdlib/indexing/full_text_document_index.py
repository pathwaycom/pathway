# Copyright © 2024 Pathway

import pathway as pw
from pathway.stdlib.indexing.bm25 import TantivyBM25
from pathway.stdlib.indexing.data_index import DataIndex


def default_full_text_document_index(
    data_column: pw.ColumnReference,
    data_table: pw.Table,
    *,
    metadata_column: pw.ColumnExpression | None = None,
) -> DataIndex:
    """
    Returns an instance of DataIndex
    (:py:class:`~pathway.stdlib.indexing.DataIndex`),
    with inner index (data structure) of our choosing. This method chooses an arbitrary
    implementation of :py:class:`~pathway.stdlib.indexing.InnerIndex`
    (that supports text queries), but it's not necessarily the best choice of
    index and its parameters (each usecase may need slightly different configuration).
    As such, it is meant to be used for development, demonstrations, starting point
    of larger project etc.
    """

    inner = TantivyBM25(data_column, metadata_column=metadata_column)
    return DataIndex(data_table, inner_index=inner)
