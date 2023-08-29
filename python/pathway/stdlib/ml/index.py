# Copyright © 2023 Pathway

import pathway.internals as pw
from pathway.stdlib.ml.classifiers import knn_lsh_classifier_train
from pathway.stdlib.utils.col import unpack_col


class KNNIndex:
    """
    A K-Nearest Neighbors (KNN) index implementation using the Locality-Sensitive Hashing (LSH)
    algorithm within Pathway. This index is designed to efficiently find the
    nearest neighbors of a given query embedding within a dataset.

    Parameters:

    data_embedding (pw.ColumnExpression): The column expression representing embeddings in the data.
    data (pw.Table): The table containing the data to be indexed.
    n_dimensions (int): number of dimensions in the data
    n_or (int): number of ORS
    n_and (int): number of ANDS
    bucket_length (float): bucket length (after projecting on a line)
    distance_type (str): euclidean metric is supported.
    """

    def __init__(
        self,
        data_embedding: pw.ColumnExpression,
        data: pw.Table,
        n_dimensions: int,
        n_or: int = 20,
        n_and: int = 10,
        bucket_length: float = 10.0,
        distance_type: str = "euclidean",
    ):
        self.data = data
        self.packed_data = data.select(row=pw.make_tuple(*self.data))

        embeddings = data.select(data=data_embedding)
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
        k: int = 3,
        collapse_rows: bool = True,
    ):
        """
        This method queries the index with given queries and returns the 'k' most relevant documents
        for each query in the stream.

        Parameters:
        query_embedding (pw.ColumnReference): column of embedding vectors precomputed from the query.
        k (int, optional): The number of most relevant documents to return for each query.
                            Defaults to 3.
        collapse_rows (bool, optional): Determines the format of the output. If set to True,
            multiple rows corresponding to a single query will be collapsed into a single row,
            with each column containing a tuple of values from the original rows. If set to False,
            the output will retain the multi-row format for each query. Defaults to True.

        Returns:
        pw.Table:
            - If `collapse_rows` is set to True: Returns a table where each row corresponds to a unique query.
            Each column in the row contains a tuple (or list) of values, aggregating up
            to 'k' matches from the dataset.
            For example:
                            | name                        | age
                ^YYY4HAB... | ()                          | ()
                ^X1MXHYY... | ('bluejay', 'cat', 'eagle') | (43, 42, 41)

            - If `collapse_rows` is set to False: Returns a table where each row represents a match from the dataset
            for a given query. Multiple rows can correspond to the same query, up to 'k' matches.
            Example:
                name    | age | embedding | query_id
                        |     |           | ^YYY4HAB...
                bluejay | 43  | (4, 3, 2) | ^X1MXHYY...
                cat     | 42  | (3, 3, 2) | ^X1MXHYY...
                eagle   | 41  | (2, 3, 2) | ^X1MXHYY...


        Example:

        >>> import pathway as pw
        >>> import pandas as pd
        >>> documents = pw.debug.table_from_pandas(
        ...     pd.DataFrame.from_records([
        ...         {"document": "document 1", "embeddings":[1,-1, 0]},
        ...         {"document": "document 2", "embeddings":[1, 1, 0]},
        ...         {"document": "document 3", "embeddings":[0, 0, 1]},
        ...     ])
        ... )
        >>> index = KNNIndex(documents.embeddings, documents, n_dimensions=3)
        >>> queries = pw.debug.table_from_pandas(
        ...     pd.DataFrame.from_records([
        ...         {"query": "What is doc 3 about?", "embeddings":[.1, .1, .1]},
        ...         {"query": "What is doc -5 about?", "embeddings":[-1, 10, -10]},
        ...     ])
        ... )
        >>> relevant_docs = index.get_nearest_items(queries.embeddings, k=2)
        >>> pw.debug.compute_and_print(relevant_docs)
                    | document                     | embeddings
        ^YYY4HAB... | ()                           | ()
        ^X1MXHYY... | ('document 2', 'document 3') | ((1, 1, 0), (0, 0, 1))
        """

        queries = query_embedding.table.select(data=query_embedding)
        knns_ids = (
            self._query(queries, k)
            .flatten(pw.this.knns_ids, pw.this.query_id)
            .update_types(knns_ids=pw.Pointer)
        )

        if collapse_rows:
            return self._extract_data_collapsed_rows(knns_ids, queries)
        return self._extract_data_flat(knns_ids, queries)

    def _extract_data_collapsed_rows(self, knns_ids, queries):
        selected_data = (
            knns_ids.join_inner(self.packed_data, pw.left.knns_ids == pw.right.id)
            .select(pw.left.query_id, pw.right.row)
            .groupby(id=pw.this.query_id)
            .reduce(topk=pw.reducers.sorted_tuple(pw.this.row))
        )

        selected_data = selected_data.select(
            transposed=pw.apply(lambda x: tuple(zip(*x)), pw.this.topk)
        )
        selected_data = unpack_col(selected_data.transposed, *self.data.keys())

        # Keep queries that didn't match with anything.
        result = queries.select(**{key: () for key in self.data.keys()})
        result = result.update_rows(selected_data)
        return result

    def _extract_data_flat(self, knns_ids, queries):
        selected_data = knns_ids.join_inner(
            self.data, pw.left.knns_ids == pw.right.id
        ).select(pw.left.query_id, *pw.right)
        selected_data = queries.join_left(
            selected_data, pw.left.id == pw.right.query_id
        ).select(*pw.right.without(selected_data.query_id), query_id=pw.left.id)

        return selected_data
