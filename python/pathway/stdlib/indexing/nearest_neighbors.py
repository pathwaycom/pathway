# Copyright © 2024 Pathway

import dataclasses
from dataclasses import dataclass, field
from typing import Callable, Tuple

import pathway.internals as pw
from pathway.engine import ExternalIndexFactory, USearchMetricKind
from pathway.internals import dtype as dt
from pathway.internals.runtime_type_check import check_arg_types
from pathway.stdlib.indexing.colnames import _INDEX_REPLY, _NO_OF_MATCHES, _QUERY_ID
from pathway.stdlib.indexing.data_index import InnerIndex
from pathway.stdlib.indexing.typecheck_utils import check_column_reference_type

# TODO fix the dependency: we should have one index that under the hood is used in both classifier
# and here; current version is taken from existing impl, and fixing that is a separate task
from pathway.stdlib.ml.classifiers import DistanceTypes, knn_lsh_classifier_train
from pathway.stdlib.ml.utils import _predict_asof_now


def check_default_knn_column_types(
    data_column, query_column, number_of_matches, metadata_column, metadata_filter
):
    typecheck_list: list[Tuple[str, Tuple[pw.ColumnExpression, dt.DType]]] = [
        ("data column", (data_column, dt.List(dt.FLOAT))),
        ("query column", (query_column, dt.List(dt.FLOAT))),
    ]

    if metadata_column is not None:
        typecheck_list.append(("metadata column", (metadata_column, dt.JSON)))

    if metadata_filter is not None:
        typecheck_list.append(
            ("metadata filter", (metadata_filter, dt.Optional(dt.STR)))
        )

    if number_of_matches is not None and not isinstance(number_of_matches, int):
        typecheck_list.append(("number of matches", (number_of_matches, dt.INT)))

    check_column_reference_type(typecheck_list)


@dataclass(frozen=True, kw_only=True)
class USearchKnn(InnerIndex):
    """
    Interface for usearch nearest neighbors index, an implementation of k nearest
    neighbors based on HNSW algorithm `white paper <https://arxiv.org/abs/1603.09320>`_.

    To understand meaning of the explanation of some of the parameters, you might need
    some familiarity with either `HNSW algorithm <https://arxiv.org/abs/1603.09320>`_ or
    its implementation provided by `USearch <https://github.com/unum-cloud/usearch>`_

    Args:
        data_column ( pw.ColumnExpression [list[float]]): the column expression representing the data.
        metadata_column ( pw.ColumnExpression [str] | None): optional column expression,
            string representation of some auxiliary data, in JSON format.
        dimensions (int): number of dimensions of vectors that are used by the index and
            queries
        reserved_space (int): initial capacity (in number of entries) of the index
        metric (USearchMetricKind): metric kind that is used to determine distance
        connectivity (int): maximum number of edges for a node in the HNSW index
        expansion_add (int): indicates amount of work spent while adding elements to the index
            (higher = more accurate placement, more work)
        expansion_search (int): indicates amount of work spent while searching for elements in
            the index (higher = more accurate results, more work)

    """

    dimensions: int
    reserved_space: int
    metric: USearchMetricKind
    connectivity: int = 2
    expansion_add: int = 2
    expansion_search: int = 2

    def query(
        self,
        query_column: pw.ColumnReference,
        number_of_matches: pw.ColumnExpression | int = 3,
        metadata_filter: pw.ColumnExpression | None = None,
    ) -> pw.Table:
        raise NotImplementedError(
            "Currently, usearch knn index is supported only in the as-of-now variant"
        )

    @check_arg_types
    def query_as_of_now(
        self,
        query_column: pw.ColumnReference,
        number_of_matches: pw.ColumnExpression | int = 3,
        metadata_filter: pw.ColumnExpression | None = None,
    ) -> pw.Table:

        check_default_knn_column_types(
            self.data_column,
            query_column,
            number_of_matches,
            self.metadata_column,
            metadata_filter,
        )

        index = self.data_column.table
        queries = query_column.table

        index_factory = ExternalIndexFactory.usearch_knn_factory(
            dimensions=self.dimensions,
            reserved_space=self.reserved_space,
            metric=self.metric,
            connectivity=self.connectivity,
            expansion_add=self.expansion_add,
            expansion_search=self.expansion_search,
        )

        number_of_matches_ref = number_of_matches
        if isinstance(number_of_matches, int):
            queries = queries.with_columns(**{_NO_OF_MATCHES: number_of_matches})
            number_of_matches_ref = queries[_NO_OF_MATCHES]

        return index._external_index_as_of_now(
            queries,
            index_column=self.data_column,
            query_column=query_column,
            index_factory=index_factory,
            res_type=dt.List(dt.Tuple(dt.ANY_POINTER, float)),
            query_responses_limit_column=number_of_matches_ref,
            index_filter_data_column=self.metadata_column,
            query_filter_column=metadata_filter,
        )


@dataclass(frozen=True, kw_only=True)
class LshKnn(InnerIndex):
    """
    Interface for Pathway's implementation of KNN via LSH.

    Args:
        data_column ( pw.ColumnExpression [list[float]]): the column expression
            representing the data.
        metadata_column ( pw.ColumnExpression [str] | None): optional column expression,
            string representation of metadata as dictionary, in JSON format.
        dimensions (int): number of dimensions in the data
        n_or (int): number of ORs
        n_and (int): number of ANDs
        bucket_length (float): bucket length (after projecting on a line)
        distance_type (str): "euclidean" and "cosine" metrics are supported.
    """

    # TODO: a better description of dimensions, n_or, n_and, bucket_length
    _: dataclasses.KW_ONLY
    dimensions: int
    n_or: int = 20
    n_and: int = 10
    bucket_length: float = 10.0
    distance_type: DistanceTypes = "euclidean"
    _query: Callable[[pw.Table, int | None, bool], pw.Table] = field(init=False)
    # In case signature of callable _query is not enough: this class is an adapter that
    # allows use lsh based knn implementation with DataIndex api. As such, _query is a
    # field dedicated solely to store query function, generated by knn_lsh_classifier_train;
    # the implementation of this class should change, when we rewrite lsh based knn

    def __post_init__(self):
        columns: dict[str, pw.ColumnExpression | int] = {"data": self.data_column}
        if self.metadata_column is not None:
            columns["metadata"] = self.metadata_column

        # can't simply assign to self._query,
        _query = knn_lsh_classifier_train(
            # at very least we should eliminate the magic column names,
            # perhaps also reconsider whether we pass table, column or both
            self.data_column.table.select(**columns),
            L=self.n_or,
            d=self.dimensions,
            M=self.n_and,
            A=self.bucket_length,
            type=self.distance_type,
        )
        object.__setattr__(self, "_query", _query)

    @check_arg_types
    def query(
        self,
        query_column: pw.ColumnReference,
        number_of_matches: pw.ColumnExpression | int = 3,
        metadata_filter: pw.ColumnExpression | None = None,
    ) -> pw.Table:
        """
        Args:
            query_column ( pw.ColumnExpression [list[float]]):
                column containing data that is used to query the index;
            number_of_matches ( pw.ColumnExpression [int] | int ):
                number of nearest neighbors in the index response; defaults to 3
            metadata_filter ( pw.ColumnExpression [str] | None):
                optional, column expression evaluating to the text representation
                of a boolean JMESPath query. The index will consider only the entries
                with metadata that satisfies the condition in the filter.
        """

        check_default_knn_column_types(
            self.data_column,
            query_column,
            number_of_matches,
            self.metadata_column,
            metadata_filter,
        )

        columns = {
            "data": query_column,
            "k": number_of_matches,
            "metadata_filter": metadata_filter,
        }

        queries = query_column.table.select(**columns)

        @pw.udf
        def distance_to_score(
            reply: list[tuple[pw.Pointer, float]]
        ) -> list[tuple[pw.Pointer, float]]:
            return [(id, float(-distance)) for (id, distance) in reply]

        # I think _query returns knns_ids_with_dists and query_id magic columns
        magic_colname = "knns_ids_with_dists"

        # mypy seems not recognize that lsh_perform_query returned by knn_lsh_classifier_train
        # has the with_distances arg
        ret = self._query(queries, with_distances=True)  # type: ignore[call-arg]
        return (
            ret.select(
                **{
                    _INDEX_REPLY: distance_to_score(pw.this[magic_colname]),
                    _QUERY_ID: pw.this.query_id,
                }
            )
            .with_id(pw.this[_QUERY_ID])
            .without(pw.this[_QUERY_ID])
            .with_universe_of(queries)
        )

    @check_arg_types
    def query_as_of_now(
        self,
        query_column: pw.ColumnReference,
        number_of_matches: pw.ColumnExpression | int = 3,
        metadata_filter: pw.ColumnExpression | None = None,
    ) -> pw.Table:
        """
        Args:
            query_column (pw.ColumnExpression [list[float]]):
                column containing data that is used to query the index;
            number_of_matches (pw.ColumnExpression[int] | int ):
                number of nearest neighbors in the index response; defaults to 3
            metadata_filter (pw.ColumnExpression [str] | None): optional,
                column expression evaluating to the text representation
                of a boolean JMESPath query. The index will consider only the entries
                with metadata that satisfies the condition in the filter.
        """

        check_default_knn_column_types(
            self.data_column,
            query_column,
            number_of_matches,
            self.metadata_column,
            metadata_filter,
        )

        return _predict_asof_now(self.query, with_queries_universe=True)(
            query_column,
            number_of_matches=number_of_matches,
            metadata_filter=metadata_filter,
        )
