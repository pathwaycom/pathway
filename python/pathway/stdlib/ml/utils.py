# Copyright © 2024 Pathway

from __future__ import annotations

from collections.abc import Callable

import pathway.internals as pw


def classifier_accuracy(predicted_labels, exact_labels):
    pw.universes.promise_is_subset_of(predicted_labels, exact_labels)
    comparative_results = predicted_labels.select(
        predicted_label=predicted_labels.predicted_label,
        label=exact_labels.restrict(predicted_labels).label,
    )
    comparative_results = comparative_results + comparative_results.select(
        match=comparative_results.label == comparative_results.predicted_label
    )
    accuracy = comparative_results.groupby(comparative_results.match).reduce(
        cnt=pw.reducers.count(),
        value=comparative_results.match,
    )
    pw.universes.promise_is_subset_of(predicted_labels, accuracy)
    return accuracy


def _predict_asof_now(
    prediction_function: Callable[..., pw.Table],
    *queries: pw.ColumnReference,
    with_queries_universe: bool = False,
) -> pw.Table:
    """
    A helper function used to predict answers to queries without updating them in the future.
    It passes a query and its forgetting counterpart through the prediction function.

    Parameters:
        prediction_function: A function that is called with transformed column reference `query`.
        queries: References to a column/columns with query data.
        with_queries_universe: Whether the result should have the same universe (set of keys)
            as the table with queries.

    Returns:
        pw.Table: A table created by applying `prediction_function` on `query`.
    """
    assert len(queries) > 0
    cols = {f"_pw_{i}": q for i, q in enumerate(queries)}
    queries_table = queries[0].table.select(**cols)
    queries_table = queries_table._forget_immediately()
    results = prediction_function(*(queries_table[name] for name in cols))
    results = results._filter_out_results_of_forgetting()
    if with_queries_universe:
        # FIXME assert that query.table is append-only,
        # then results should also be append-only (promise that)
        # then we should have a version of with_universe_of for both append only tables
        # that frees memory when records are joined
        results = results.with_universe_of(queries[0].table)
    return results
