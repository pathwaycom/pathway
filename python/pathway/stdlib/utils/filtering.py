# Copyright © 2024 Pathway

from __future__ import annotations

import pathway.internals as pw


def argmax_rows(
    table: pw.Table, *on: pw.ColumnReference, what: pw.ColumnExpression
) -> pw.Table:
    filter = (
        table.groupby(*on)
        .reduce(argmax_id=pw.reducers.argmax(what))
        .with_id(pw.this.argmax_id)
        .promise_universe_is_subset_of(table)
    )
    return table.restrict(filter)


def argmin_rows(
    table: pw.Table, *on: pw.ColumnReference, what: pw.ColumnReference
) -> pw.Table:
    filter = (
        table.groupby(*on)
        .reduce(argmin_id=pw.reducers.argmin(what))
        .with_id(pw.this.argmin_id)
        .promise_universe_is_subset_of(table)
    )
    return table.restrict(filter)
