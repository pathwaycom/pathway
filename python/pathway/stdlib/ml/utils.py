# Copyright © 2023 Pathway

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
    query: pw.ColumnReference,
    prediction_function: Callable[[pw.ColumnReference], pw.Table],
    with_queries_universe: bool = False,
) -> pw.Table:
    """
    Helper function used to predict answers to queries without updating them in the future.
    It passes query and its forgetting counterpart through prediction function.

    Parameters:
        query: A reference to column with queries.
        prediction_function: Function that is called with transformed column reference `query`.
        with_queries_universe: Whether result should have the same universe (set of keys)
            as table with queries.

    Returns:
        pw.Table: A table created by applying `prediction_function` on `query`.
    """
    queries = query.table.select(_pw_query=query)
    queries = queries._forget_immediately()
    results = prediction_function(queries._pw_query)
    results = results._filter_out_results_of_forgetting()
    if with_queries_universe:
        # FIXME assert that query.table is append-only,
        # then results should also be append-only (promise that)
        # then we should have a version of with_universe_of for both append only tables
        # that frees memory when records are joined
        results = results.with_universe_of(query.table)
    return results
