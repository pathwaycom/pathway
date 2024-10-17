# Copyright © 2024 Pathway

import dataclasses
from dataclasses import dataclass, field
from typing import Callable, Tuple

import pathway.internals as pw
from pathway.engine import (
    BruteForceKnnMetricKind,
    ExternalIndexFactory,
    USearchMetricKind,
)
from pathway.internals import dtype as dt
from pathway.internals.runtime_type_check import check_arg_types
from pathway.internals.udfs.utils import _coerce_sync
from pathway.stdlib.indexing.colnames import _INDEX_REPLY, _NO_OF_MATCHES, _QUERY_ID
from pathway.stdlib.indexing.data_index import InnerIndex
from pathway.stdlib.indexing.retrievers import InnerIndexFactory
from pathway.stdlib.indexing.typecheck_utils import check_column_reference_type

# TODO fix the dependency: we should have one index that under the hood is used in both classifier
# and here; current version is taken from existing impl, and fixing that is a separate task
from pathway.stdlib.ml.classifiers import DistanceTypes, knn_lsh_classifier_train
from pathway.stdlib.ml.utils import _predict_asof_now


def check_default_knn_column_types(
    data_column, query_column, number_of_matches, metadata_column, metadata_filter
):
    typecheck_list: list[
        Tuple[str, Tuple[pw.ColumnExpression, dt.DType | tuple[dt.DType, ...]]]
    ] = [
        ("data column", (data_column, (dt.ANY_ARRAY, dt.List(dt.FLOAT)))),
        ("query column", (query_column, (dt.ANY_ARRAY, dt.List(dt.FLOAT)))),
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


def _calculate_embeddings(
    column: pw.ColumnReference, embedder: pw.UDF | None
) -> pw.ColumnReference:
    if embedder is None:
        return column

    table = column.table
    table = table.with_columns(_pw_embedded_column=embedder(column))
    column = table._pw_embedded_column

    return column


@dataclass(frozen=True, kw_only=True)
class USearchKnn(InnerIndex):
    """
    Interface for usearch nearest neighbors index, an implementation of k nearest
    neighbors based on HNSW algorithm `white paper <https://arxiv.org/abs/1603.09320>`_.

    To understand meaning of the explanation of some of the parameters, you might need
    some familiarity with either `HNSW algorithm <https://arxiv.org/abs/1603.09320>`_ or
    its implementation provided by `USearch <https://github.com/unum-cloud/usearch>`_.

    Args:
        data_column (pw.ColumnExpression): the column expression representing the data.
        metadata_column (pw.ColumnExpression [str] | None): optional column expression,
            string representation of some auxiliary data, in JSON format.
        dimensions (int): number of dimensions of vectors that are used by the index and
            queries
        reserved_space (int): initial capacity (in the number of entries) of the index
        metric (USearchMetricKind): metric kind that is used to determine distance
        connectivity (int): maximum number of edges for a node in the HNSW index, setting
            this value to 0 tells usearch to configure it on its own
        expansion_add (int): indicates amount of work spent while adding elements to the index
            (higher = more accurate placement, more work), setting this value to 0 tells
            usearch to configure it on its own
        expansion_search (int): indicates amount of work spent while searching for elements in
            the index (higher = more accurate results, more work), setting this value to
            0 tells usearch to configure it on its own
        embedder: :py:class:`~pathway.UDF` used for calculating embeddings of string. It is needed, if index
            is used for indexing texts.

    """

    dimensions: int
    reserved_space: int
    metric: USearchMetricKind
    # setting values below to zero tells usearch figure those values on its own
    connectivity: int = 0
    expansion_add: int = 0
    expansion_search: int = 0
    embedder: pw.UDF | None = None

    # data column after applying embeddings. It is calculated during initialization and
    # cannot be set in the constructor.
    _data_column: pw.ColumnReference = field(init=False)

    def __post_init__(self):
        _data_column = _calculate_embeddings(self.data_column, self.embedder)
        object.__setattr__(self, "_data_column", _data_column)

    def query(
        self,
        query_column: pw.ColumnReference,
        number_of_matches: pw.ColumnExpression | int = 3,
        metadata_filter: pw.ColumnExpression | None = None,
    ) -> pw.Table:
        """Currently, usearch knn index is supported only in the as-of-now variant"""
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
        index = self._data_column.table

        query_column = _calculate_embeddings(query_column, self.embedder)
        queries = query_column.table

        check_default_knn_column_types(
            self._data_column,
            query_column,
            number_of_matches,
            self.metadata_column,
            metadata_filter,
        )

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
            index_column=self._data_column,
            query_column=query_column,
            index_factory=index_factory,
            res_type=dt.List(dt.Tuple(dt.ANY_POINTER, float)),
            query_responses_limit_column=number_of_matches_ref,
            index_filter_data_column=self.metadata_column,
            query_filter_column=metadata_filter,
        )


@dataclass(frozen=True, kw_only=True)
class BruteForceKnn(InnerIndex):
    """
    Interface for a brute force implementation of a nearest neighbors index.

    Args:
        data_column (pw.ColumnExpression): the column expression representing the data.
        metadata_column (pw.ColumnExpression [str] | None): optional column expression,
            string representation of some auxiliary data, in JSON format.
        dimensions (int): number of dimensions of vectors that are used by the index and
            queries
        reserved_space (int): initial capacity (in the number of entries) of the index
        auxiliary_space (int): auxiliary space (in the number of entries), the maximum
            number of distances that are stored in memory, while evaluating queries, in
            case ``auxiliary_space`` is set to a value smaller than the current number
            of entries in the index, it is still proportional to
            the size of the index (the value given in this parameter is ignored)
        metric (BruteForceKnnMetricKind): metric kind that is used to determine distance
        embedder: :py:class:`~pathway.UDF` used for calculating embeddings of string. It is needed, if index
            is used for indexing texts.

    """

    dimensions: int
    reserved_space: int
    auxiliary_space: int = 1024 * 128
    metric: BruteForceKnnMetricKind
    embedder: pw.UDF | None = None

    # data column after applying embeddings. It is calculated during initialization and
    # cannot be set in the constructor.
    _data_column: pw.ColumnReference = field(init=False)

    def __post_init__(self):
        _data_column = _calculate_embeddings(self.data_column, self.embedder)
        object.__setattr__(self, "_data_column", _data_column)

    def query(
        self,
        query_column: pw.ColumnReference,
        number_of_matches: pw.ColumnExpression | int = 3,
        metadata_filter: pw.ColumnExpression | None = None,
    ) -> pw.Table:
        """Currently, brute force knn index is supported only in the as-of-now variant"""
        raise NotImplementedError(
            "Currently, brute force knn index is supported only in the as-of-now variant"
        )

    @check_arg_types
    def query_as_of_now(
        self,
        query_column: pw.ColumnReference,
        number_of_matches: pw.ColumnExpression | int = 3,
        metadata_filter: pw.ColumnExpression | None = None,
    ) -> pw.Table:
        index = self._data_column.table

        query_column = _calculate_embeddings(query_column, self.embedder)
        queries = query_column.table

        check_default_knn_column_types(
            self._data_column,
            query_column,
            number_of_matches,
            self.metadata_column,
            metadata_filter,
        )

        index_factory = ExternalIndexFactory.brute_force_knn_factory(
            dimensions=self.dimensions,
            reserved_space=self.reserved_space,
            auxiliary_space=self.auxiliary_space,
            metric=self.metric,
        )

        number_of_matches_ref = number_of_matches
        if isinstance(number_of_matches, int):
            queries = queries.with_columns(**{_NO_OF_MATCHES: number_of_matches})
            number_of_matches_ref = queries[_NO_OF_MATCHES]

        return index._external_index_as_of_now(
            queries,
            index_column=self._data_column,
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
        data_column (pw.ColumnExpression): the column expression
            representing the data.
        metadata_column (pw.ColumnExpression [str] | None): optional column expression,
            string representation of metadata as dictionary, in JSON format.
        dimensions (int): number of dimensions in the data
        n_or (int): number of ORs
        n_and (int): number of ANDs
        bucket_length (float): bucket length (after projecting on a line)
        distance_type (str): "euclidean" and "cosine" metrics are supported.
        embedder: :py:class:`~pathway.UDF` used for calculating embeddings of string. It is needed, if index
            is used for indexing texts.
    """

    # TODO: a better description of dimensions, n_or, n_and, bucket_length
    _: dataclasses.KW_ONLY
    dimensions: int
    n_or: int = 20
    n_and: int = 10
    bucket_length: float = 10.0
    distance_type: DistanceTypes = "euclidean"
    embedder: pw.UDF | None = None
    _query: Callable[[pw.Table, int | None, bool], pw.Table] = field(init=False)
    # In case signature of callable _query is not enough: this class is an adapter that
    # allows use lsh based knn implementation with DataIndex api. As such, _query is a
    # field dedicated solely to store query function, generated by knn_lsh_classifier_train;
    # the implementation of this class should change, when we rewrite lsh based knn

    # data column after applying embeddings. It is calculated during initialization and
    # cannot be set in the constructor.
    _data_column: pw.ColumnReference = field(init=False)

    def __post_init__(self):
        _data_column = _calculate_embeddings(self.data_column, self.embedder)

        columns: dict[str, pw.ColumnExpression | int] = {"data": _data_column}
        if self.metadata_column is not None:
            columns["metadata"] = self.metadata_column

        # can't simply assign to self._query,
        _query = knn_lsh_classifier_train(
            # at very least we should eliminate the magic column names,
            # perhaps also reconsider whether we pass table, column or both
            _data_column.table.select(**columns),
            L=self.n_or,
            d=self.dimensions,
            M=self.n_and,
            A=self.bucket_length,
            type=self.distance_type,
        )
        object.__setattr__(self, "_query", _query)
        object.__setattr__(self, "_data_column", _data_column)

    @check_arg_types
    def query(
        self,
        query_column: pw.ColumnReference,
        number_of_matches: pw.ColumnExpression | int = 3,
        metadata_filter: pw.ColumnExpression | None = None,
    ) -> pw.Table:
        """
        Args:
            query_column (pw.ColumnExpression):
                column containing data that is used to query the index;
            number_of_matches (pw.ColumnExpression [int] | int):
                number of nearest neighbors in the index response; defaults to 3
            metadata_filter (pw.ColumnExpression [str] | None):
                optional, column expression evaluating to the text representation
                of a boolean JMESPath query. The index will consider only the entries
                with metadata that satisfies the condition in the filter.
        """

        query_column = _calculate_embeddings(query_column, self.embedder)

        check_default_knn_column_types(
            self._data_column,
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
            query_column (pw.ColumnExpression):
                column containing data that is used to query the index;
            number_of_matches (pw.ColumnExpression[int] | int):
                number of nearest neighbors in the index response; defaults to 3
            metadata_filter (pw.ColumnExpression [str] | None): optional,
                column expression evaluating to the text representation
                of a boolean JMESPath query. The index will consider only the entries
                with metadata that satisfies the condition in the filter.
        """

        return _predict_asof_now(self.query, with_queries_universe=True)(
            query_column,
            number_of_matches=number_of_matches,
            metadata_filter=metadata_filter,
        )


@dataclass(kw_only=True)
class KnnIndexFactory(InnerIndexFactory):
    dimensions: int | None = None
    embedder: pw.UDF | None = None

    def _get_embed_dimensions(self) -> int:
        if isinstance(self.embedder, pw.UDF):
            dim = len(_coerce_sync(self.embedder.__wrapped__)("."))
            return dim
        else:
            raise TypeError("Embedder is not a valid `pw.UDF`.")

    def __post_init__(self):
        if self.dimensions is None and self.embedder is not None:
            self.dimensions: int = self._get_embed_dimensions()
        elif self.dimensions is None and self.embedder is None:
            raise ValueError(
                "Either `dimensions` or `embedder` must be provided to index factory."
            )


@dataclass(kw_only=True)
class UsearchKnnFactory(KnnIndexFactory):
    """
    Factory for creating UsearchKNN indices.

    Args:
        dimensions (int): number of dimensions of vectors that are used by the index and
            queries
        reserved_space (int): initial capacity (in the number of entries) of the index
        metric (USearchMetricKind): metric kind that is used to determine distance.
            Defaults to cosine similarity.
        connectivity (int): maximum number of edges for a node in the HNSW index, setting
            this value to 0 tells usearch to configure it on its own
        expansion_add (int): indicates amount of work spent while adding elements to the index
            (higher = more accurate placement, more work), setting this value to 0 tells
            usearch to configure it on its own
        expansion_search (int): indicates amount of work spent while searching for elements in
            the index (higher = more accurate results, more work), setting this value to
            0 tells usearch to configure it on its own
        embedder: :py:class:`~pathway.UDF` used for calculating embeddings of string. It is needed, if index
            is used for indexing texts.

    """

    reserved_space: int = 400
    metric: USearchMetricKind = USearchMetricKind.COS
    # setting values below to zero tells usearch figure those values on its own
    connectivity: int = 0
    expansion_add: int = 0
    expansion_search: int = 0

    def build_inner_index(
        self,
        data_column: pw.ColumnReference,
        metadata_column: pw.ColumnExpression | None = None,
    ) -> InnerIndex:
        assert isinstance(
            self.dimensions, int
        ), "`dimensions` is not set, this may indicate something is wrong with embedder."

        inner_index = USearchKnn(
            data_column,
            metadata_column,
            dimensions=self.dimensions,
            reserved_space=self.reserved_space,
            metric=self.metric,
            connectivity=self.connectivity,
            expansion_add=self.expansion_add,
            expansion_search=self.expansion_search,
            embedder=self.embedder,
        )
        return inner_index


@dataclass(kw_only=True)
class BruteForceKnnFactory(KnnIndexFactory):
    """
    Factory for creating BruteForceKnn indices.

    Args:
        dimensions (int): number of dimensions of vectors that are used by the index and
            queries
        reserved_space (int): initial capacity (in the number of entries) of the index
        auxiliary_space (int): auxiliary space (in the number of entries), the maximum
            number of distances that are stored in memory, while evaluating queries, in
            case ``auxiliary_space`` is set to a value smaller than the current number
            of entries in the index, it is still proportional to
            the size of the index (the value given in this parameter is ignored)
        metric (BruteForceKnnMetricKind): metric kind that is used to determine distance.
            Defaults to cosine similarity.
        embedder: :py:class:`~pathway.UDF` used for calculating embeddings of string. It is needed, if index
            is used for indexing texts.

    """

    reserved_space: int = 400
    auxiliary_space: int = 1024 * 128
    metric: BruteForceKnnMetricKind = BruteForceKnnMetricKind.COS

    def build_inner_index(
        self,
        data_column: pw.ColumnReference,
        metadata_column: pw.ColumnExpression | None = None,
    ) -> InnerIndex:
        assert isinstance(
            self.dimensions, int
        ), "`dimensions` is not set, this may indicate something is wrong with embedder."

        inner_index = BruteForceKnn(
            data_column,
            metadata_column,
            dimensions=self.dimensions,
            reserved_space=self.reserved_space,
            auxiliary_space=self.auxiliary_space,
            metric=self.metric,
            embedder=self.embedder,
        )
        return inner_index


@dataclass(kw_only=True)
class LshKnnFactory(KnnIndexFactory):
    """
    Factory for creating LshKnn indices.

    Args:
        dimensions (int): number of dimensions in the data
        n_or (int): number of ORs
        n_and (int): number of ANDs
        bucket_length (float): bucket length (after projecting on a line)
        distance_type (str): "euclidean" and "cosine" metrics are supported.
        embedder: :py:class:`~pathway.UDF` used for calculating embeddings of string. It is needed, if index
            is used for indexing texts.
    """

    n_or: int = 20
    n_and: int = 10
    bucket_length: float = 10.0
    distance_type: DistanceTypes = "euclidean"

    def build_inner_index(
        self,
        data_column: pw.ColumnReference,
        metadata_column: pw.ColumnExpression | None = None,
    ) -> InnerIndex:
        assert isinstance(
            self.dimensions, int
        ), "`dimensions` is not set, this may indicate something is wrong with embedder."

        return LshKnn(
            data_column,
            metadata_column,
            dimensions=self.dimensions,
            n_or=self.n_or,
            n_and=self.n_and,
            bucket_length=self.bucket_length,
            distance_type=self.distance_type,
            embedder=self.embedder,
        )
