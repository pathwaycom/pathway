# Copyright © 2023 Pathway

import pathway.internals as pw
from pathway.stdlib.ml.classifiers import knn_lsh_classifier_train


class KNNIndex:
    def __init__(self, embeddings: pw.Table, **kwargs):
        self.embeddings = embeddings

        self._query = knn_lsh_classifier_train(
            embeddings,
            L=kwargs.get("L", 20),
            d=kwargs.get("d"),
            M=kwargs.get("M", 10),
            A=kwargs.get("A", 10),
            type=kwargs.get("type", "euclidean"),
        )

    def query(self, queries: pw.Table, k: int = 3):
        """
        This method queries the index with given queries and returns the 'k' most relevant documents
        for each query in the stream.

        Parameters:
        queries (pw.Table): The table containing the queries to be run against the index.
                            Each row should represent a separate query, and the table should
                            include a column named 'query' which contains the query text.

        k (int, optional): The number of most relevant documents to return for each query.
                            Defaults to 3.

        Returns:
        pw.Table: A table where each row represents a query. For each query, it includes a column
                   'query' (the text of the query), and 'docs' (a sorted tuple of the 'k' most relevant documents
                  for the query, as determined by the index).

        Example:

        >>> import pathway as pw
        >>> import pandas as pd
        >>> documents = pw.debug.table_from_pandas(
        ...     pd.DataFrame.from_records([
        ...         {"doc": "document 1", "data":[1,-1, 0]},
        ...         {"doc": "document 2", "data":[1, 1, 0]},
        ...         {"doc": "document 3", "data":[0, 0, 1]},
        ...     ])
        ... )
        >>> index = KNNIndex(documents, d=3)
        >>> queries = pw.debug.table_from_pandas(
        ...     pd.DataFrame.from_records([
        ...         {"query": "What is doc 3 about?", "data":[.1, .1, .1]},
        ...     ])
        ... )
        >>> relevant_docs = index.query(queries, k=1)
        >>> #  pw.debug.compute_and_print(relevant_docs)


        """
        knns_ids = self._query(queries, k)

        docs = (
            knns_ids.flatten(pw.this.knns_ids, pw.this.query_id)
            .select(*pw.this, doc=self.embeddings.ix(pw.this.knns_ids).doc)
            .groupby(pw.this.query_id)
            .reduce(pw.this.query_id, result=pw.reducers.sorted_tuple(pw.this.doc))
        )
        #  queries + docs.promise_universe_is_equal_to(queries) gives missing key error
        output = queries.join_inner(
            docs, queries.id == docs.query_id, id=pw.left.id
        ).select(*pw.left, pw.right.result)
        return output
