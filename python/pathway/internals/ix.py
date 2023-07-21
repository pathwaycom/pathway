# Copyright © 2023 Pathway

from __future__ import annotations

from typing import TYPE_CHECKING, Iterator

import pathway.internals.expression as expr

if TYPE_CHECKING:
    from pathway.internals.table import Table


class IxIndexer:
    """An object that when indexed by some column returns a table with rows specified by that column.
    Accepts both `[]` and `()` indexing.

    Example:

    >>> import pathway as pw
    >>> t_animals = pw.debug.parse_to_table('''
    ...   | epithet    | genus
    ... 1 | upupa      | epops
    ... 2 | acherontia | atropos
    ... 3 | bubo       | scandiacus
    ... 4 | dynastes   | hercules
    ... ''')
    >>> t_birds = pw.debug.parse_to_table('''
    ...   | desc
    ... 2 | hoopoe
    ... 4 | owl
    ... ''')
    >>> ret = t_birds.select(t_birds.desc, latin=t_animals.ix[t_birds.id].genus)
    >>> pw.debug.compute_and_print(ret, include_id=False)
    desc   | latin
    hoopoe | atropos
    owl    | hercules
    """

    _table: Table

    def __init__(self, table):
        self._table = table

    def __getitem__(self, keys_expression: expr.ColumnExpression) -> IxAppliedIndexer:
        return IxAppliedIndexer(self._table, keys_expression, False)

    def __call__(
        self, keys_expression: expr.ColumnExpression, *, optional: bool = False
    ) -> IxAppliedIndexer:
        return IxAppliedIndexer(self._table, keys_expression, optional)


class IxAppliedIndexer:
    _table: Table
    _keys_expression: expr.ColumnExpression
    _optional: bool

    def __init__(self, table, keys_expression, optional):
        self._table = table
        self._keys_expression = keys_expression
        self._optional = optional

    def _get_colref_by_name(self, name, exception_type) -> expr.ColumnIxExpression:
        expression = getattr(self._table, name)
        if not isinstance(expression, expr.ColumnReference):
            raise exception_type("f{name} is not a column")
        return expr.ColumnIxExpression(
            keys_expression=self._keys_expression,
            column_expression=expression,
            optional=self._optional,
        )

    def __getattr__(self, name: str) -> expr.ColumnIxExpression:
        return self._get_colref_by_name(name, AttributeError)

    def __getitem__(self, name: str) -> expr.ColumnIxExpression:
        return self._get_colref_by_name(name, KeyError)

    def keys(self):
        return self._table.keys()

    def __iter__(self) -> Iterator[expr.ColumnIxExpression]:
        return (self[name] for name in self.keys())
