# Copyright © 2024 Pathway

from dataclasses import dataclass
from typing import Tuple

import pathway.internals as pw
from pathway.engine import ExternalIndexFactory
from pathway.internals import dtype as dt
from pathway.internals.expression import ColumnExpression
from pathway.internals.runtime_type_check import check_arg_types
from pathway.stdlib.indexing.data_index import InnerIndex
from pathway.stdlib.indexing.retrievers import InnerIndexFactory
from pathway.stdlib.indexing.typecheck_utils import check_column_reference_type


def check_default_bm25_column_types(
    data_column, query_column, number_of_matches, metadata_column, metadata_filter
):
    typecheck_list: list[
        Tuple[str, Tuple[pw.ColumnExpression, dt.DType | tuple[dt.DType, ...]]]
    ] = [
        ("data column", (data_column, dt.STR)),
        ("query column", (query_column, dt.STR)),
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


@dataclass(frozen=True)
class TantivyBM25(InnerIndex):
    """
    Interface for full text index based on `BM25 <https://en.wikipedia.org/wiki/Okapi_BM25>`_,
    provided via `tantivy <https://github.com/quickwit-oss/tantivy>`_.

    Args:
        data_column (pw.ColumnExpression[str]): the column expression representing the data.
        metadata_column (pw.ColumnExpression[str] | None): optional column expression,
            string representation of some auxiliary data, in JSON format.
        ram_budget (int): maximum capacity in bytes. When reached, the index moves a block of data
            to storage (hence, larger budget means faster index operations, but higher
            memory cost)
        in_memory_index (bool): indicates, whether the whole index is stored in RAM;
            if set to false, the index is stored in some default Pathway disk storage
    """

    ram_budget: int = 50 * 1024 * 1024  # 50 MB
    in_memory_index: bool = True

    def query(
        self,
        query_column: pw.ColumnReference,
        number_of_matches: pw.ColumnExpression | int = 3,
        metadata_filter: pw.ColumnExpression | None = None,
    ) -> pw.Table:
        """Currently, tantivy bm25 index is supported only in the as-of-now variant"""
        raise NotImplementedError(
            "Currently, tantivy bm25 index is supported only in the as-of-now variant"
        )

    @check_arg_types
    def query_as_of_now(
        self,
        query_column: pw.ColumnReference,
        number_of_matches: pw.ColumnExpression | int = 3,
        metadata_filter: pw.ColumnExpression | None = None,
    ) -> pw.Table:
        check_default_bm25_column_types(
            self.data_column,
            query_column,
            number_of_matches,
            self.metadata_column,
            metadata_filter,
        )

        index = self.data_column.table
        queries = query_column.table

        index_factory = ExternalIndexFactory.tantivy_factory(
            ram_budget=self.ram_budget,
            in_memory_index=self.in_memory_index,
        )

        number_of_matches_ref = ColumnExpression._wrap(number_of_matches)

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


@dataclass
class TantivyBM25Factory(InnerIndexFactory):
    """
    Factory for creating a TantivyBM25 index.

    Args:
        ram_budget (int): maximum capacity in bytes. When reached, the index moves a block of data
            to storage (hence, larger budget means faster index operations, but higher
            memory cost)
        in_memory_index (bool): indicates, whether the whole index is stored in RAM;
            if set to false, the index is stored in some default Pathway disk storage
    """

    ram_budget: int = 50 * 1024 * 1024  # 50 MB
    in_memory_index: bool = True

    def build_inner_index(
        self,
        data_column: pw.ColumnReference,
        metadata_column: pw.ColumnExpression | None = None,
    ) -> InnerIndex:
        inner_index = TantivyBM25(
            data_column,
            metadata_column,
            ram_budget=self.ram_budget,
            in_memory_index=self.in_memory_index,
        )
        return inner_index
