from typing import Any, Callable, TypeVar

import pathway as pw
from pathway.internals import api
from pathway.internals.schema import Schema
from pathway.stdlib.utils.col import unpack_col

TDedupe = TypeVar("TDedupe", bound=api.Value)
TSchema = TypeVar("TSchema", bound=Schema)


def deduplicate(
    table: pw.Table[TSchema],
    *,
    col: pw.ColumnReference,
    instance: pw.ColumnExpression | None = None,
    acceptor: Callable[[TDedupe, TDedupe], bool],
) -> pw.Table[TSchema]:
    """Deduplicates rows in `table` on `col` column using acceptor function.

    It keeps rows which where accepted by the acceptor function.
    Acceptor operates on two arguments - current value and the previous accepted value.

    Args:
        table (pw.Table[TSchema]): table to deduplicate
        col (pw.ColumnReference): column used for deduplication
        acceptor (Callable[[TDedupe, TDedupe], bool]): callback telling whether two values are different
        instance (pw.ColumnExpression, optional): Group column for which deduplication will be performed separately.
            Defaults to None.

    Returns:
        pw.Table[TSchema]:
    """
    assert col.table == table

    @pw.reducers.stateful_many
    def is_different_with_state(
        state: tuple[Any, ...] | None, rows
    ) -> tuple[Any, ...] | None:
        for [col, *cols], diff in rows:
            if diff <= 0:
                continue
            state_val = state[0] if state is not None else None
            if state_val is None or acceptor(col, state_val):
                state = (col, *cols)
        return state

    _table = table.select(*table, _instance=instance)
    res = _table.groupby(_table._instance).reduce(
        _table._instance,
        res=is_different_with_state(col, *_table.without(_table._instance)),
    )
    res = res.select(res=pw.apply(lambda x: x[1:], res.res))
    return unpack_col(res.res, *_table.without(_table._instance))
