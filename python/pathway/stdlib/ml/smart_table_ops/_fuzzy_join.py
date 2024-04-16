# Copyright © 2024 Pathway

from __future__ import annotations

import math
from collections.abc import Callable
from enum import IntEnum, auto
from typing import Any

import pathway.internals as pw
from pathway.internals.helpers import StableSet


class Node(pw.Schema):
    pass


class Feature(pw.Schema):
    weight: float
    normalization_type: int


class Edge(pw.Schema):
    node: pw.Pointer[Node]
    feature: pw.Pointer[Feature]
    weight: float


class JoinResult(pw.Schema):
    left: pw.Pointer[Node]
    right: pw.Pointer[Node]
    weight: float


def _tokenize(obj: Any) -> Any:
    return str(obj).split()


def _letters(obj: Any) -> Any:
    return [c.lower() for c in str(obj) if c.isalnum()]


class FuzzyJoinFeatureGeneration(IntEnum):
    AUTO = auto()
    TOKENIZE = auto()
    LETTERS = auto()

    @property
    def generate(self) -> Callable[[Any], Any]:
        cls = type(self)
        if self == cls.AUTO:
            return _tokenize  # TODO add some magic autoguessing
        if self == cls.TOKENIZE:
            return _tokenize
        if self == cls.LETTERS:
            return _letters
        raise AssertionError


def _discrete_weight(cnt: float) -> float:
    if cnt == 0:
        return 0.0
    else:
        return 1 / (2 ** math.ceil(math.log2(cnt)))


def _discrete_logweight(cnt: float) -> float:
    if cnt == 0:
        return 0.0
    else:
        return 1 / (math.ceil(math.log2(cnt + 1)))


def _none(cnt: float) -> float:
    return cnt


class FuzzyJoinNormalization(IntEnum):
    WEIGHT = auto()
    LOGWEIGHT = auto()
    NONE = auto()

    @property
    def normalize(self) -> Callable[[Any], Any]:
        cls = type(self)
        if self == cls.WEIGHT:
            return _discrete_weight
        if self == cls.LOGWEIGHT:
            return _discrete_logweight
        if self == cls.NONE:
            return _none
        raise AssertionError


def _concatenate_columns(table: pw.Table):
    def concat_columns(*args) -> str:
        return " ".join([str(arg) for arg in args])

    return table.select(
        desc=pw.apply(
            concat_columns, *[table[name] for name in table._columns.keys()]
        )  # TODO add to api
    )


def fuzzy_match_tables(
    left_table: pw.Table,
    right_table: pw.Table,
    *,
    by_hand_match: pw.Table[JoinResult] | None = None,
    normalization=FuzzyJoinNormalization.LOGWEIGHT,
    feature_generation=FuzzyJoinFeatureGeneration.AUTO,
    left_projection: dict[str, str] = {},
    right_projection: dict[str, str] = {},
) -> pw.Table[JoinResult]:
    # If one projection is empty, we don't do any projection fuzzy_match_tables
    if left_projection == {} or right_projection == {}:
        return _fuzzy_match_tables(
            left_table=left_table,
            right_table=right_table,
            by_hand_match=by_hand_match,
            normalization=normalization,
            feature_generation=feature_generation,
        )

    # We compute the projections spaces and for each bucket b we keep track of the
    # corresponding columns which are projected into b.
    set_buckets: StableSet[str] = StableSet()
    buckets_left: dict[str, list] = {}
    buckets_right: dict[str, list] = {}

    for col_name in left_table._columns.keys():
        if col_name not in left_projection:
            continue
        bucket_id = left_projection[col_name]
        set_buckets.add(bucket_id)
        if bucket_id not in buckets_left:
            buckets_left[bucket_id] = []
        buckets_left[bucket_id].append(col_name)

    for col_name in right_table._columns.keys():
        if col_name not in right_projection:
            continue
        bucket_id = right_projection[col_name]
        set_buckets.add(bucket_id)
        if bucket_id not in buckets_right:
            buckets_right[bucket_id] = []
        buckets_right[bucket_id].append(col_name)

    # For each bucket, we compute the fuzzy_match_table on the table only with
    # the columns associated to the bucket.
    # The corresponding matches are then added in a common 'matchings' columns
    fuzzy_match_bucket_list = []
    for bucket_id in set_buckets:
        left_table_bucket = left_table[buckets_left[bucket_id]]

        right_table_bucket = right_table[buckets_right[bucket_id]]

        fuzzy_match_bucket = _fuzzy_match_tables(
            left_table=left_table_bucket,
            right_table=right_table_bucket,
            by_hand_match=by_hand_match,
            normalization=normalization,
            feature_generation=feature_generation,
        )
        fuzzy_match_bucket_list.append(fuzzy_match_bucket)
    matchings = pw.Table.concat_reindex(*fuzzy_match_bucket_list)

    # Matchings are grouped by left/right pairs and the weights are summed.
    matchings = matchings.groupby(matchings.left, matchings.right).reduce(
        matchings.left,
        matchings.right,
        weight=pw.reducers.sum(matchings.weight),
    )
    return matchings


def _fuzzy_match_tables(
    left_table: pw.Table,
    right_table: pw.Table,
    *,
    by_hand_match: pw.Table[JoinResult] | None = None,
    normalization=FuzzyJoinNormalization.LOGWEIGHT,
    feature_generation=FuzzyJoinFeatureGeneration.AUTO,
) -> pw.Table[JoinResult]:
    left = _concatenate_columns(left_table)
    right = _concatenate_columns(right_table)
    return smart_fuzzy_match(
        left.desc,
        right.desc,
        by_hand_match=by_hand_match,
        normalization=normalization,
        feature_generation=feature_generation,
    )


# TODO we may want to have different normalization for different features, as specified
# "manually" by the user in the feature generation function
def smart_fuzzy_match(
    left_col: pw.ColumnReference,
    right_col: pw.ColumnReference,
    *,
    by_hand_match: pw.Table[JoinResult] | None = None,
    normalization=FuzzyJoinNormalization.LOGWEIGHT,
    feature_generation=FuzzyJoinFeatureGeneration.AUTO,
    HEAVY_LIGHT_THRESHOLD=100,
) -> pw.Table:
    left = left_col.table
    right = right_col.table
    self_match = left is right and left_col.name == right_col.name

    if self_match:
        tabs = [left]
    else:
        tabs = [left, right]
    processed = []
    features: pw.Table | None = None
    for tab, col in zip(tabs, [left_col, right_col]):
        edges = tab.select(feature=pw.apply(feature_generation.generate, col)).flatten(
            pw.this.feature, origin_id="origin_id"
        )
        features_tmp = edges.groupby(edges.feature).reduce(
            normalization_type=int(normalization),
            # TODO add some row normalization
            weight=1.0,
        )

        edges = edges.select(
            node=edges.origin_id,
            feature=features_tmp.pointer_from(edges.feature),
            weight=1.0,
        )
        if features is None:
            features = features_tmp
        else:
            features = features.update_rows(features_tmp)
        processed.append(edges)
    assert features is not None
    if self_match:
        [edges] = processed
        return fuzzy_self_match(edges, features, by_hand_match, HEAVY_LIGHT_THRESHOLD)
    else:
        [edges_left, edges_right] = processed
        return fuzzy_match(
            edges_left, edges_right, features, by_hand_match, HEAVY_LIGHT_THRESHOLD
        )


def fuzzy_self_match(
    edges: pw.Table[Edge],
    features: pw.Table[Feature],
    by_hand_match: pw.Table[JoinResult] | None = None,
    HEAVY_LIGHT_THRESHOLD=100,
) -> pw.Table[JoinResult]:
    return _fuzzy_match(
        edges,
        edges,
        features,
        symmetric=True,
        HEAVY_LIGHT_THRESHOLD=HEAVY_LIGHT_THRESHOLD,
        by_hand_match=by_hand_match,
    )


def fuzzy_match(
    edges_left: pw.Table[Edge],
    edges_right: pw.Table[Edge],
    features: pw.Table[Feature],
    by_hand_match: pw.Table[JoinResult] | None = None,
    HEAVY_LIGHT_THRESHOLD=100,
) -> pw.Table[JoinResult]:
    return _fuzzy_match(
        edges_left,
        edges_right,
        features,
        symmetric=False,
        HEAVY_LIGHT_THRESHOLD=HEAVY_LIGHT_THRESHOLD,
        by_hand_match=by_hand_match,
    )


def fuzzy_match_with_hint(
    edges_left: pw.Table[Edge],
    edges_right: pw.Table[Edge],
    features: pw.Table[Feature],
    by_hand_match: pw.Table[JoinResult],
    HEAVY_LIGHT_THRESHOLD=100,
) -> pw.Table[JoinResult]:
    return _fuzzy_match(
        edges_left,
        edges_right,
        features,
        symmetric=False,
        HEAVY_LIGHT_THRESHOLD=HEAVY_LIGHT_THRESHOLD,
        by_hand_match=by_hand_match,
    )


def _filter_out_matched_by_hand(
    edges_left: pw.Table[Edge],
    edges_right: pw.Table[Edge],
    symmetric: bool,
    by_hand_match: pw.Table[JoinResult],
):
    edges_left = edges_left.difference(
        edges_left.join(
            by_hand_match, edges_left.node == by_hand_match.left, id=edges_left.id
        ).select()
    )
    if not symmetric:
        edges_right = edges_right.difference(
            edges_right.join(
                by_hand_match,
                edges_right.node == by_hand_match.right,
                id=edges_right.id,
            ).select()
        )
    return edges_left, edges_right


def _fuzzy_match(
    edges_left: pw.Table[Edge],
    edges_right: pw.Table[Edge],
    features: pw.Table[Feature],
    symmetric: bool,
    HEAVY_LIGHT_THRESHOLD,
    by_hand_match: pw.Table[JoinResult] | None = None,
) -> pw.Table[JoinResult]:
    if symmetric:
        assert edges_left is edges_right

    edges_left = edges_left.update_types(
        node=pw.Pointer[Node], feature=pw.Pointer[Feature]
    )
    if not symmetric:
        edges_right = edges_right.update_types(
            node=pw.Pointer[Node], feature=pw.Pointer[Feature]
        )

    if by_hand_match is not None:
        by_hand_match = by_hand_match.update_types(
            left=pw.Pointer[Node], right=pw.Pointer[Node]
        )

    # TODO do a more integrated approach for accommodating by_hand_match.
    if by_hand_match is not None:
        edges_left, edges_right = _filter_out_matched_by_hand(
            edges_left, edges_right, symmetric, by_hand_match
        )

    if symmetric:
        edges = edges_left
        edges_right = edges_right.copy()
    else:
        edges = pw.Table.concat_reindex(edges_left, edges_right)
    features_cnt = features.select(cnt=0).update_rows(
        edges.groupby(id=edges.feature).reduce(cnt=pw.reducers.count())
    )
    del edges

    edges_left_heavy = edges_left.filter(
        features_cnt.ix(edges_left.feature).cnt >= HEAVY_LIGHT_THRESHOLD
    )
    edges_left_light = edges_left.filter(
        features_cnt.ix(edges_left.feature).cnt < HEAVY_LIGHT_THRESHOLD
    )

    if symmetric:
        edges_right_heavy = edges_left_heavy.copy()
        edges_right_light = edges_left_light.copy()
    else:
        edges_right_heavy = edges_right.filter(
            features_cnt.ix(edges_right.feature).cnt >= HEAVY_LIGHT_THRESHOLD
        )
        edges_right_light = edges_right.filter(
            features_cnt.ix(edges_right.feature).cnt < HEAVY_LIGHT_THRESHOLD
        )

    def _normalize_weight(cnt: float, normalization_type: int) -> float:
        return FuzzyJoinNormalization(normalization_type).normalize(cnt)

    features_normalized = features.select(
        weight=features.weight
        * pw.apply(
            _normalize_weight,
            features_cnt.restrict(features).cnt,
            features.normalization_type,
        )
    )
    node_node_light: pw.Table[JoinResult] = edges_left_light.join(
        edges_right_light, edges_left_light.feature == edges_right_light.feature
    ).select(
        weight=edges_left_light.weight
        * edges_right_light.weight
        * features_normalized.ix(pw.this.feature).weight,
        left=edges_left_light.node,
        right=edges_right_light.node,
    )
    if symmetric:
        node_node_light = node_node_light.filter(
            node_node_light.left != node_node_light.right
        )

    node_node_light = node_node_light.groupby(
        node_node_light.left, node_node_light.right
    ).reduce(
        node_node_light.left,
        node_node_light.right,
        weight=pw.reducers.sum(node_node_light.weight),
    )

    node_node_heavy = (
        node_node_light.join(edges_left_heavy, pw.left.left == pw.right.node)
        .join(
            edges_right_heavy,
            pw.left.right == pw.right.node,
            pw.left.feature == pw.right.feature,
        )
        .select(
            pw.this.left,
            pw.this.right,
            weight=edges_left_heavy.weight
            * edges_right_heavy.weight
            * features_normalized.ix(pw.this.feature).weight,
        )
    )

    def weight_to_pseudoweight(weight, left_id, right_id):
        return pw.if_else(
            left_id < right_id,
            pw.make_tuple(weight, left_id, right_id),
            pw.make_tuple(weight, right_id, left_id),
        )

    node_node = (
        pw.Table.concat_reindex(node_node_light, node_node_heavy)
        .groupby(pw.this.left, pw.this.right)
        .reduce(pw.this.left, pw.this.right, weight=pw.reducers.sum(pw.this.weight))
        .with_columns(
            weight=weight_to_pseudoweight(
                pw.this.weight,
                pw.this.left,
                pw.this.right,
            ),
        )
        .groupby(pw.this.left)
        .reduce(
            pw.this.left,
            pw.this.ix(pw.reducers.argmax(pw.this.weight)).right,
            weight=pw.reducers.max(pw.this.weight),
        )
        .groupby(pw.this.right)
        .reduce(
            pw.this.right,
            pw.this.ix(pw.reducers.argmax(pw.this.weight)).left,
            weight=pw.reducers.max(pw.this.weight),
        )
    )

    if symmetric:
        node_node = node_node.filter(node_node.left < node_node.right)

    node_node = node_node.with_columns(
        weight=pw.this.weight[0],
    )

    if by_hand_match is not None:
        node_node = node_node.update_rows(by_hand_match)

    return node_node
