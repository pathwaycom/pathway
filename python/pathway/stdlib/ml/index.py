# Copyright © 2024 Pathway

import pathway.internals as pw
from pathway.stdlib.ml.classifiers import DistanceTypes, knn_lsh_classifier_train
from pathway.stdlib.ml.utils import _predict_asof_now
from pathway.stdlib.utils.col import unpack_col


class KNNIndex:
    """
    An approximate K-Nearest Neighbors (KNN) index implementation using the Locality-Sensitive Hashing (LSH)
    algorithm within Pathway. This index is designed to efficiently find the
    nearest neighbors of a given query embedding within a dataset.
    It is approximate in a sense that it might return less than k records per query or skip some closer points.
    If it returns not enough points too frequently, increase ``bucket_length`` accordingly.
    If it skips points too often, increase ``n_or`` or play with other parameters.
    Note that changing the parameters will influence the time and memory requirements.

    Args:
        data_embedding (pw.ColumnExpression): The column expression representing embeddings in the data.
        data (pw.Table): The table containing the data to be indexed.
        n_dimensions (int): number of dimensions in the data
        n_or (int): number of ORs
        n_and (int): number of ANDs
        bucket_length (float): bucket length (after projecting on a line)
        distance_type (str): "euclidean" and "cosine" metrics are supported.
        metadata (pw.ColumnExpression): optional column expression representing dict of the metadata.
    """

    def __init__(
        self,
        data_embedding: pw.ColumnExpression,
        data: pw.Table,
        n_dimensions: int,
        n_or: int = 20,
        n_and: int = 10,
        bucket_length: float = 10.0,
        distance_type: DistanceTypes = "euclidean",
        metadata: pw.ColumnExpression | None = None,
    ):
        self.data = data
        self.packed_data = data.select(row=pw.make_tuple(*self.data))

        embeddings = data.select(data=data_embedding, metadata=metadata)
        self._query = knn_lsh_classifier_train(
            embeddings,
            L=n_or,
            d=n_dimensions,
            M=n_and,
            A=bucket_length,
            type=distance_type,
        )

    def get_nearest_items(
        self,
        query_embedding: pw.ColumnReference,
        k: pw.ColumnExpression | int = 3,
        collapse_rows: bool = True,
        with_distances: bool = False,
        metadata_filter: pw.ColumnExpression | None = None,
    ):
        """
        This method queries the index with given queries and returns 'k' most relevant documents
        for each query in the stream. While using this method, documents associated with
        the queries will be updated if new more relevant documents appear.
        If you don't want queries results to get updated in the future, take a look at
        `get_nearest_items_asof_now`.

        Args:
            query_embedding: column of embedding vectors precomputed from the query.
            k: The number of most relevant documents to return for each query.
                Can be constant for all queries or set per query. If you want to set
                ``k`` per query, pass a reference to the column. Defaults to 3.
            collapse_rows: Determines the format of the output. If set to True,
                multiple rows corresponding to a single query will be collapsed into a single row,
                with each column containing a tuple of values from the original rows. If set to False,
                the output will retain the multi-row format for each query. Defaults to True.
            with_distances (bool): whether to output distances
            metadata_filter (pw.ColumnExpression): optional column expression containing evaluating to the text
                representing the metadata filtering query in the JMESPath format. The search will happen
                only for documents satisfying this filtering. Can be constant for all queries or set per query.

        Returns:
            pw.Table

        - If ``collapse_rows`` is set to True: Returns a table where each row corresponds to a unique query.
        Each column in the row contains a tuple (or list) of values, aggregating up
        to 'k' matches from the dataset.
        For example:

        .. code-block:: text

                        | name                        | age
            ^YYY4HAB... | ()                          | ()
            ^X1MXHYY... | ('bluejay', 'cat', 'eagle') | (43, 42, 41)

        - If ``collapse_rows`` is set to False: Returns a table where each row represents a match from the dataset
        for a given query. Multiple rows can correspond to the same query, up to 'k' matches.
        Example:

        .. code-block:: text

            name    | age | embedding | query_id
                    |     |           | ^YYY4HAB...
            bluejay | 43  | (4, 3, 2) | ^X1MXHYY...
            cat     | 42  | (3, 3, 2) | ^X1MXHYY...
            eagle   | 41  | (2, 3, 2) | ^X1MXHYY...


        Example:

        >>> import pathway as pw
        >>> from pathway.stdlib.ml.index import KNNIndex
        >>> import pandas as pd
        >>> class InputSchema(pw.Schema):
        ...     document: str
        ...     embeddings: list[float]
        ...     metadata: dict
        >>> documents = pw.debug.table_from_pandas(
        ...     pd.DataFrame.from_records([
        ...         {"document": "document 1", "embeddings":[1,-1, 0], "metadata":{"foo": 1}},
        ...         {"document": "document 2", "embeddings":[1, 1, 0], "metadata":{"foo": 2}},
        ...         {"document": "document 3", "embeddings":[0, 0, 1], "metadata":{"foo": 3}},
        ...     ]),
        ...     schema=InputSchema
        ... )
        >>> index = KNNIndex(documents.embeddings, documents, n_dimensions=3)
        >>> queries = pw.debug.table_from_pandas(
        ...     pd.DataFrame.from_records([
        ...         {"query": "What is doc 3 about?", "embeddings":[.1, .1, .1]},
        ...         {"query": "What is doc -5 about?", "embeddings":[-1, 10, -10]},
        ...     ])
        ... )
        >>> relevant_docs = index.get_nearest_items(queries.embeddings, k=2).without(pw.this.metadata)
        >>> pw.debug.compute_and_print(relevant_docs)
                    | document                     | embeddings
        ^YYY4HAB... | ()                           | ()
        ^X1MXHYY... | ('document 2', 'document 3') | ((1.0, 1.0, 0.0), (0.0, 0.0, 1.0))
        >>> index = KNNIndex(documents.embeddings, documents, n_dimensions=3, metadata=documents.metadata)
        >>> relevant_docs_meta = index.get_nearest_items(queries.embeddings, k=2, metadata_filter="foo >= `3`")
        >>> pw.debug.compute_and_print(relevant_docs_meta)
                    | document        | embeddings         | metadata
        ^YYY4HAB... | ()              | ()                 | ()
        ^X1MXHYY... | ('document 3',) | ((0.0, 0.0, 1.0),) | (pw.Json({'foo': 3}),)
        >>> data = pw.debug.table_from_markdown(
        ...     '''
        ...      x | y | __time__
        ...      2 | 3 |     2
        ...      0 | 0 |     2
        ...      2 | 2 |     6
        ...     -3 | 3 |    10
        ...     '''
        ... ).select(coords=pw.make_tuple(pw.this.x, pw.this.y))
        >>> queries = pw.debug.table_from_markdown(
        ...     '''
        ...      x | y | __time__ | __diff__
        ...      1 | 1 |     4    |     1
        ...     -3 | 1 |     8    |     1
        ...     '''
        ... ).select(coords=pw.make_tuple(pw.this.x, pw.this.y))
        >>> index = KNNIndex(data.coords, data, n_dimensions=2)
        >>> answers = queries + index.get_nearest_items(queries.coords, k=2).select(
        ...     nn=pw.this.coords
        ... )
        >>> pw.debug.compute_and_print_update_stream(answers, include_id=False)
        coords  | nn                | __time__ | __diff__
        (1, 1)  | ((0, 0), (2, 3))  | 4        | 1
        (1, 1)  | ((0, 0), (2, 3))  | 6        | -1
        (1, 1)  | ((0, 0), (2, 2))  | 6        | 1
        (-3, 1) | ((0, 0), (2, 2))  | 8        | 1
        (-3, 1) | ((0, 0), (2, 2))  | 10       | -1
        (-3, 1) | ((0, 0), (-3, 3)) | 10       | 1
        """
        queries = query_embedding.table.select(
            data=query_embedding, k=k, metadata_filter=metadata_filter
        )
        knns_ids = (
            self._query(queries, with_distances=True)
            .flatten(pw.this.knns_ids_with_dists)
            .select(
                pw.this.query_id,
                knn_id=pw.this.knns_ids_with_dists[0],
                knn_dist=pw.this.knns_ids_with_dists[1],
            )
            .update_types(knn_id=pw.Pointer, knn_dist=float)
        )

        if collapse_rows:
            return self._extract_data_collapsed_rows(
                knns_ids, queries, with_distances=with_distances
            )
        return self._extract_data_flat(knns_ids, queries, with_distances=with_distances)

    def get_nearest_items_asof_now(
        self,
        query_embedding: pw.ColumnReference,
        k: pw.ColumnExpression | int = 3,
        collapse_rows: bool = True,
        with_distances: bool = False,
        metadata_filter: pw.ColumnExpression | None = None,
    ):
        """
        This method queries the index with given queries and returns 'k' most relevant documents
        for each query in the stream. The already answered queries are not updated in
        the future if new documents appear.

        Args:
            query_embedding: column of embedding vectors precomputed from the query.
            k: The number of most relevant documents to return for each query.
                Can be constant for all queries or set per query. If you want to set
                ``k`` per query, pass a reference to the column. Defaults to 3.
            collapse_rows: Determines the format of the output. If set to True,
                multiple rows corresponding to a single query will be collapsed into a single row,
                with each column containing a tuple of values from the original rows. If set to False,
                the output will retain the multi-row format for each query. Defaults to True.
            metadata_filter (pw.ColumnExpression): optional column expression containing evaluating to the text
                representing the metadata filtering query in the JMESPath format. The search will happen
                only for documents satisfying this filtering. Can be constant for all queries or set per query.

        Example:

        >>> import pathway as pw
        >>> from pathway.stdlib.ml.index import KNNIndex
        >>> data = pw.debug.table_from_markdown(
        ...     '''
        ...      x | y | __time__
        ...      2 | 3 |     2
        ...      0 | 0 |     2
        ...      2 | 2 |     6
        ...     -3 | 3 |    10
        ...     '''
        ... ).select(coords=pw.make_tuple(pw.this.x, pw.this.y))
        >>> queries = pw.debug.table_from_markdown(
        ...     '''
        ...      x | y | __time__ | __diff__
        ...      1 | 1 |     4    |     1
        ...     -3 | 1 |     8    |     1
        ...     '''
        ... ).select(coords=pw.make_tuple(pw.this.x, pw.this.y))
        >>> index = KNNIndex(data.coords, data, n_dimensions=2)
        >>> answers = queries + index.get_nearest_items_asof_now(queries.coords, k=2).select(
        ...     nn=pw.this.coords
        ... )
        >>> pw.debug.compute_and_print_update_stream(answers, include_id=False)
        coords  | nn               | __time__ | __diff__
        (1, 1)  | ((0, 0), (2, 3)) | 4        | 1
        (-3, 1) | ((0, 0), (2, 2)) | 8        | 1
        """

        return _predict_asof_now(
            self.get_nearest_items, with_queries_universe=collapse_rows
        )(
            query_embedding,
            k=k,
            collapse_rows=collapse_rows,
            with_distances=with_distances,
            metadata_filter=metadata_filter,
        )

    def _extract_data_collapsed_rows(self, knns_ids, queries, with_distances=False):
        selected_data = (
            knns_ids.join_inner(self.packed_data, pw.left.knn_id == pw.right.id)
            .select(pw.left.query_id, pw.left.knn_dist, pw.right.row)
            .groupby(id=pw.this.query_id)
            .reduce(
                topk=pw.reducers.tuple(pw.this.row),
                dist=pw.reducers.tuple(pw.this.knn_dist),
            )
        )
        selected_data_rows = selected_data.select(
            transposed=pw.apply(lambda x: tuple(zip(*x)), pw.this.topk),
        )
        selected_data_rows = unpack_col(
            selected_data_rows.transposed, *self.data.keys()
        )

        if with_distances:
            selected_data_rows += selected_data.select(pw.this.dist)

        # Keep queries that didn't match with anything.
        colnames = list(self.data.keys())
        if with_distances:
            colnames.append("dist")
        result = queries.select(**{key: () for key in colnames})
        result = result.update_rows(selected_data_rows).with_universe_of(queries)
        return result

    def _extract_data_flat(self, knns_ids, queries, with_distances=False):
        joined = knns_ids.join_inner(self.data, pw.left.knn_id == pw.right.id)
        if with_distances:
            selected_data = joined.select(
                pw.left.query_id, dist=pw.left.knn_dist, *pw.right
            )
        else:
            selected_data = joined.select(pw.left.query_id, *pw.right)

        selected_data = queries.join_left(
            selected_data, pw.left.id == pw.right.query_id
        ).select(*pw.right.without(selected_data.query_id), query_id=pw.left.id)

        return selected_data
