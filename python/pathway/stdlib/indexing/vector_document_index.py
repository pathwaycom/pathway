# Copyright © 2024 Pathway
from __future__ import annotations

import warnings

import pathway as pw
from pathway.engine import USearchMetricKind
from pathway.stdlib.indexing.data_index import DataIndex
from pathway.stdlib.indexing.nearest_neighbors import LshKnn, USearchKnn


def VectorDocumentIndex(
    data_column: pw.ColumnReference,
    data_table: pw.Table,
    *,
    dimensions: int,
    embedder: pw.UDF | None = None,
    metadata_column: pw.ColumnExpression | None = None,
):
    warnings.warn(
        "this part of API will be removed soon, "
        + "please use default_vector_document_index instead",
        DeprecationWarning,
    )
    return default_vector_document_index(
        data_column=data_column,
        data_table=data_table,
        embedder=embedder,
        dimensions=dimensions,
        metadata_column=metadata_column,
    )


def default_vector_document_index(
    data_column: pw.ColumnReference,
    data_table: pw.Table,
    # perhaps InnerIndex should have a class that extends it with a promise it uses
    # data of fixed dimension
    *,
    dimensions: int,
    embedder: pw.UDF | None = None,
    metadata_column: pw.ColumnExpression | None = None,
) -> DataIndex:
    """
    Returns an instance of DataIndex
    ( :py:class:`~pathway.stdlib.indexing.DataIndex`),
    with inner index (data structure) of our choosing. This method chooses an arbitrary
    implementation of :py:class:`~pathway.stdlib.indexing.InnerIndex`
    (that supports queries on vectors), but it's not necessarily the best choice of
    index and its parameters (each usecase may need slightly different configuration).
    As such, it is meant to be used for development, demonstrations, starting point
    of larger project etc.
    """

    return default_lsh_knn_document_index(
        data_column=data_column,
        data_table=data_table,
        embedder=embedder,
        dimensions=dimensions,
        metadata_column=metadata_column,
    )
    # make a default instance of vector storage data structure
    # currently LshKnn to maintain the state
    # of existing code that is public


def default_lsh_knn_document_index(
    data_column: pw.ColumnReference,
    data_table: pw.Table,
    # perhaps InnerIndex should have a class that extends it with a promise it uses
    # data of fixed dimension
    *,
    dimensions: int,
    embedder: pw.UDF | None = None,
    metadata_column: pw.ColumnExpression | None = None,
) -> DataIndex:
    """
    Returns an instance of `DataIndex`
    (:py:class:`~pathway.stdlib.indexing.DataIndex`), with inner index
    (data structure) that is an instance of
    :py:class:`~pathway.stdlib.indexing.LshKnn`.
    This method chooses some parameters of `LshKnn` arbitrarily, but it's not
    necessarily a choice that works well in any scenario (each usecase may need
    slightly different configuration). As such, it is meant to be used for development,
    demonstrations, starting point of larger project, etc.

    Remark: you can use :py:class:`~pathway.stdlib.indexing.DataIndex'
    with a parametrized instance of
    :py:class:`~pathway.stdlib.indexing.LshKnn`. Look up
    :py:class:`~pathway.stdlib.indexing.DataIndex' constructor to see
    how to make data index parametrized by custom data structure, and the constructor
    of :py:class:`~pathway.stdlib.indexing.LshKnn` to see
    the parameters that can be adjusted.
    """
    d_col = data_column
    if embedder is not None:
        d_col = data_column.table.select(
            _pw_embedded_data=embedder(data_column)
        )._pw_embedded_data

    inner_index = LshKnn(
        data_column=d_col,
        metadata_column=metadata_column,
        dimensions=dimensions,
        # maybe initialize parameters so that they make sense in
        # expected-user-interaction?
    )
    return DataIndex(
        data_table=data_table,
        inner_index=inner_index,
        embedder=embedder,
    )


def default_usearch_knn_document_index(
    data_column: pw.ColumnReference,
    data_table: pw.Table,
    # perhaps InnerIndex should have a class that extends it with a promise it uses
    # data of fixed dimension
    dimensions: int,
    *,
    embedder: pw.UDF | None = None,
    metadata_column: pw.ColumnExpression | None = None,
) -> DataIndex:
    """
    Returns an instance of `DataIndex` ( :py:class:`~pathway.stdlib.indexing.DataIndex`), with inner ~
    index (data structure) that is an instance of
    :py:class:`~pathway.stdlib.indexing.USearchKnn`. This method
    chooses some parameters of `USearchKnn` arbitrarily, but it's not necessarily a choice
    that works well in any scenario (each usecase may need slightly different
    configuration). As such, it is meant to be used for development, demonstrations,
    starting point of larger project, etc.

    Remark: you can use :py:class:`~pathway.stdlib.indexing.DataIndex'
    with a parametrized instance of
    :py:class:`~pathway.stdlib.indexing.USearchKnn`. Look up
    :py:class:`~pathway.stdlib.indexing.DataIndex' constructor to see how
    to make data index parametrized by custom data structure, and the constructor
    of :py:class:`~pathway.stdlib.indexing.USearchKnn` to see the
    parameters that can be adjusted.

    """
    if embedder is not None:
        data_column = data_column.table.select(
            _pw_embedded_data=embedder(data_column)
        )._pw_embedded_data

    inner_index = USearchKnn(
        data_column=data_column,
        metadata_column=metadata_column,
        dimensions=dimensions,
        reserved_space=1000,
        metric=USearchMetricKind.L2SQ,
        # maybe initialize parameters so that they make sense in
        # expected-user-interaction?
    )

    return DataIndex(
        data_table=data_table,
        inner_index=inner_index,
        embedder=embedder,
    )
