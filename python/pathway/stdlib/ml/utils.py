# Copyright © 2023 Pathway

from __future__ import annotations

import pathway.internals as pw


def classifier_accuracy(predicted_labels, exact_labels):
    pw.universes.promise_is_subset_of(predicted_labels, exact_labels)
    comparative_results = predicted_labels.select(
        predicted_label=predicted_labels.predicted_label, label=exact_labels.label
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
