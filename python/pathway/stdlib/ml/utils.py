# Copyright © 2024 Pathway

from __future__ import annotations

import functools
import itertools
from collections.abc import Callable
from typing import ParamSpec

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


P = ParamSpec("P")


def _predict_asof_now(
    prediction_function: Callable[P, pw.Table],
    with_queries_universe: bool = False,
) -> Callable[P, pw.Table]:
    """
    A helper function used to get the results of ``prediction_function` as of now. It wraps
    the ``prediction_function`` to prevent updating answers to queries in the future.
    It passes a query and its forgetting counterpart through the ``prediction_function``.

    Parameters:
        prediction_function: A function that is called with transformed arguments.
        with_queries_universe: Whether the result should have the same universe (set of keys)
            as the table with queries.

    Returns:
        Callable[..., pw.Table]: A callable accepting the same arguments as prediction_function
            but allowing forgetting of the inputs. At least one of the arguments has to be
            of type ``pw.ColumnReference``. The callable returns a table created by applying
            ``prediction_function`` on the arguments but without updating the results in the future.
    """

    @functools.wraps(prediction_function)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> pw.Table:
        cols = {}
        i = itertools.count()
        table: pw.Table | None = None
        for arg in itertools.chain(args, kwargs.values()):
            if isinstance(arg, pw.ColumnReference):
                table = arg.table
                cols[f"_pw_{next(i)}"] = arg

        assert (
            table is not None
        ), "at least one argument to function wrapped by _predict_asof_now should be a ColumnReference"

        queries_table = table.select(**cols)
        queries_table = queries_table._forget_immediately()

        i = itertools.count()
        new_args = []
        new_kwargs = {}
        for arg in args:
            if isinstance(arg, pw.ColumnReference):
                new_args.append(queries_table[f"_pw_{next(i)}"])
            else:
                new_args.append(arg)
        for name, kwarg in kwargs.items():
            if isinstance(kwarg, pw.ColumnReference):
                new_kwargs[name] = queries_table[f"_pw_{next(i)}"]
            else:
                new_kwargs[name] = kwarg

        result = prediction_function(*new_args, **new_kwargs)
        result = result._filter_out_results_of_forgetting()
        if with_queries_universe:
            # FIXME assert that table is append-only,
            # then results should also be append-only (promise that)
            # then we should have a version of with_universe_of for both append only tables
            # that frees memory when records are joined
            result = result.with_universe_of(table)
        return result

    return wrapper
