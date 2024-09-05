from abc import abstractmethod

import pathway.internals as pw
from pathway.stdlib.indexing.data_index import DataIndex, InnerIndex


class AbstractRetrieverFactory:
    @abstractmethod
    def build_index(
        self,
        data_column: pw.ColumnReference,
        data_table: pw.Table,
        metadata_column: pw.ColumnExpression | None = None,
    ) -> DataIndex: ...


class InnerIndexFactory(AbstractRetrieverFactory):
    @abstractmethod
    def build_inner_index(
        self,
        data_column: pw.ColumnReference,
        metadata_column: pw.ColumnExpression | None = None,
    ) -> InnerIndex: ...

    def build_index(
        self,
        data_column: pw.ColumnReference,
        data_table: pw.Table,
        metadata_column: pw.ColumnExpression | None = None,
    ) -> DataIndex:
        inner_index = self.build_inner_index(data_column, metadata_column)
        return DataIndex(data_table, inner_index)
