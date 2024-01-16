# Copyright © 2024 Pathway

from collections.abc import Callable

import pathway as pw
from pathway.internals.table import T, TSchema


def deduplicate(
    table: pw.Table[TSchema],
    *,
    col: pw.ColumnReference,
    instance: pw.ColumnExpression | None = None,
    acceptor: Callable[[T, T], bool],
) -> pw.Table[TSchema]:
    """Deduplicates rows in `table` on `col` column using acceptor function.

    It keeps rows which where accepted by the acceptor function.
    Acceptor operates on two arguments - current value and the previous accepted value.

    Args:
        table (pw.Table[TSchema]): table to deduplicate
        col (pw.ColumnReference): column used for deduplication
        acceptor (Callable[[T, T], bool]): callback telling whether two values are different
        instance (pw.ColumnExpression, optional): Group column for which deduplication will be performed separately.
            Defaults to None.

    Returns:
        pw.Table[TSchema]:
    """
    return table.deduplicate(value=col, instance=instance, acceptor=acceptor)
