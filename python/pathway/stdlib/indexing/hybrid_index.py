from collections.abc import Callable

import pathway.internals as pw
from pathway.stdlib.indexing.colnames import (
    _INDEX_REPLY,
    _MATCHED_ID,
    _QUERY_ID,
    _SCORE,
)
from pathway.stdlib.indexing.data_index import InnerIndex
from pathway.stdlib.indexing.retrievers import InnerIndexFactory


class HybridIndex(InnerIndex):
    """
    Hybrid Index that composes any number of other indices and combines them using
    the Reciprocal Rank Fusion (RRF). It queries each index, and each retrieved row ``d`` is assigned
    score ``1/(k+rank(d))``, which is then summed over all indices. ``HybridIndex`` returns
    best rows from indexed data according to this score.

    Args:
        retrievers: list of indices to be used to compose the hybrid index.
        k: constant used for calculating ranking score.

    """

    def __init__(self, retrievers: list[InnerIndex], k: float = 60):
        if len(retrievers) < 2:
            raise ValueError(
                "HybridIndex requires at least two indices to be provided during initialization"
            )
        self.retrievers = retrievers
        self.k = k

    def _combine_results(
        self,
        query_retriever: Callable[[InnerIndex], pw.Table],
        query_table: pw.Table,
        number_of_matches: pw.ColumnExpression | int,
        *,
        as_of_now: bool,
    ) -> pw.Table:
        @pw.udf(deterministic=True)
        def enumerate_results(
            results: list[tuple[pw.Pointer, float]]
        ) -> list[tuple[int, pw.Pointer]]:
            return [(i, x[0]) for i, x in enumerate(results, start=1)]

        def query_single_retriever(retriever) -> pw.Table:
            results = (
                query_retriever(retriever)
                .select(
                    **{
                        _INDEX_REPLY: enumerate_results(pw.this[_INDEX_REPLY]),
                        _QUERY_ID: pw.this.id,
                    },
                )
                .flatten(pw.this[_INDEX_REPLY])
                .select(
                    **{
                        _MATCHED_ID: pw.this[_INDEX_REPLY].get(1),
                        _SCORE: 1 / (self.k + pw.this[_INDEX_REPLY].get(0)),
                        _QUERY_ID: pw.this[_QUERY_ID],
                    }
                )
            )
            if as_of_now:
                results = results._forget_immediately()

            return results

        results_list = [
            query_single_retriever(retriever) for retriever in self.retrievers
        ]
        results = pw.Table.concat_reindex(*results_list)
        removed_duplicates = results.groupby(
            results[_QUERY_ID], results[_MATCHED_ID]
        ).reduce(
            pw.this[_QUERY_ID],
            pw.this[_MATCHED_ID],
            _pw_groupby_sort_key=-pw.reducers.sum(pw.this[_SCORE]),
            **{_SCORE: pw.reducers.sum(pw.this[_SCORE])},
        )

        @pw.udf(deterministic=True)
        def limit_results(results: tuple, count: int) -> tuple:
            return results[:count]

        grouped_by_query = removed_duplicates.groupby(
            pw.this[_QUERY_ID],
            sort_by=pw.this._pw_groupby_sort_key,
            id=pw.this[_QUERY_ID],
        ).reduce(
            **{
                _INDEX_REPLY: pw.reducers.tuple(
                    pw.make_tuple(pw.this[_MATCHED_ID], pw.this[_SCORE])
                )
            }
        )

        if isinstance(number_of_matches, pw.ColumnExpression):
            number_of_matches_table = query_table.select(
                _pw_number_of_matches=number_of_matches
            )
            if as_of_now:
                number_of_matches_table = number_of_matches_table._forget_immediately()
            grouped_by_query.promise_universe_is_subset_of(number_of_matches_table)
            number_of_matches_table_restricted = number_of_matches_table.restrict(
                grouped_by_query
            )
            number_of_matches = number_of_matches_table_restricted._pw_number_of_matches

        limited_results = grouped_by_query.select(
            **{_INDEX_REPLY: limit_results(pw.this[_INDEX_REPLY], number_of_matches)}
        )

        if as_of_now:
            limited_results = limited_results._filter_out_results_of_forgetting()

        return limited_results

    def query(
        self,
        query_column: pw.ColumnReference,
        *,
        number_of_matches: pw.ColumnExpression | int = 3,
        metadata_filter: pw.ColumnExpression | None = None,
    ) -> pw.Table:
        def query_retriever(retriever: InnerIndex) -> pw.Table:
            return retriever.query(
                query_column,
                number_of_matches=number_of_matches,
                metadata_filter=metadata_filter,
            )

        return self._combine_results(
            query_retriever, query_column.table, number_of_matches, as_of_now=False
        )

    def query_as_of_now(
        self,
        query_column: pw.ColumnReference,
        *,
        number_of_matches: pw.ColumnExpression | int = 3,
        metadata_filter: pw.ColumnExpression | None = None,
    ) -> pw.Table:
        def query_retriever(retriever: InnerIndex) -> pw.Table:
            return retriever.query_as_of_now(
                query_column,
                number_of_matches=number_of_matches,
                metadata_filter=metadata_filter,
            )

        return self._combine_results(
            query_retriever, query_column.table, number_of_matches, as_of_now=True
        )


class HybridIndexFactory(InnerIndexFactory):
    """
    Factory for creating hybrid indices.

    Args:
        retriever_factories: list of factories of indices that will be used in the hybrid index
        k: constant used for calculating ranking score.
    """

    def __init__(self, retriever_factories: list[InnerIndexFactory], k: float = 60):
        if len(retriever_factories) < 2:
            raise ValueError(
                "HybridIndexFactory requires at least two retriever factories to be provided during initialization"
            )
        self.retriever_factories = retriever_factories
        self.k = k

    def build_inner_index(
        self,
        data_column: pw.ColumnReference,
        metadata_column: pw.ColumnExpression | None = None,
    ) -> InnerIndex:
        retrievers = [
            retriever_factory.build_inner_index(data_column, metadata_column)
            for retriever_factory in self.retriever_factories
        ]
        hybrid_index = HybridIndex(retrievers, self.k)
        return hybrid_index
