# Copyright © 2024 Pathway

from __future__ import annotations

import pathway.internals as pw


def argmax_rows(
    table: pw.Table, *on: pw.ColumnReference, what: pw.ColumnExpression
) -> pw.Table:
    filter = table.groupby(*on).reduce(argmax_id=pw.reducers.argmax(what))
    return table.join(filter, table.id == filter.argmax_id, id=table.id).select(*table)


def argmin_rows(
    table: pw.Table, *on: pw.ColumnReference, what: pw.ColumnReference
) -> pw.Table:
    filter = table.groupby(*on).reduce(argmin_id=pw.reducers.argmin(what))
    return table.join(filter, table.id == filter.argmin_id, id=table.id).select(*table)
