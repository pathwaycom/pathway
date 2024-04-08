# Copyright © 2024 Pathway

from __future__ import annotations

import pathway as pw
from pathway import Table, apply_with_type
from pathway.stdlib.ml.smart_table_ops import (
    FuzzyJoinFeatureGeneration,
    FuzzyJoinNormalization,
    fuzzy_match,
    fuzzy_match_tables,
    fuzzy_match_with_hint,
    fuzzy_self_match,
    smart_fuzzy_match,
)
from pathway.tests.utils import (
    T,
    assert_table_equality_wo_index,
    assert_table_equality_wo_index_types,
)


def test_fuzzy_match_simple():
    nodes = T(
        """
        name
        a
        b
        c
        AA
        BB
        CC
        """,
        id_from=["name"],
    )
    features = T(
        """
      | weight
    1 | 1.0
    2 | 1.0
    3 | 1.0
    """
    ).with_columns(weight=pw.cast(float, pw.this.weight))
    features += features.select(normalization_type=FuzzyJoinNormalization.WEIGHT)
    node_feature_left = T(
        """
     node | feature | weight
        a |       1 |    1.0
        b |       2 |    1.0
        c |       3 |    1.0

    """,
    ).with_columns(
        node=nodes.pointer_from(pw.this.node),
        feature=features.pointer_from(pw.this.feature),
        weight=pw.cast(float, pw.this.weight),
    )
    node_feature_right = T(
        """
     node | feature | weight
       AA |       1 |    1.0
       BB |       2 |    1.0
       CC |       3 |    1.0

    """,
    ).with_columns(
        node=nodes.pointer_from(pw.this.node),
        feature=features.pointer_from(pw.this.feature),
        weight=pw.cast(float, pw.this.weight),
    )
    assert_table_equality_wo_index(
        fuzzy_match(node_feature_left, node_feature_right, features),
        T(
            """
        right | left | weight
           AA |   a  |    0.5
           BB |   b  |    0.5
           CC |   c  |    0.5
    """,
        ).with_columns(
            right=nodes.pointer_from(pw.this.right),
            left=nodes.pointer_from(pw.this.left),
        ),
    )


def test_fuzzy_match_same_features():
    nodes = T(
        """
        name
        a
        b
        c
        AA
        BB
        CC
        """,
        id_from=["name"],
    )
    features = T(
        """
      | weight
    1 | 1.0
    2 | 1.0
    3 | 1.0
    """
    ).with_columns(weight=pw.cast(float, pw.this.weight))
    features += features.select(normalization_type=FuzzyJoinNormalization.WEIGHT)
    node_feature = T(
        """
     node | feature | weight
        a |       1 |    1.0
        b |       2 |    1.0
        c |       3 |    1.0

    """,
    ).with_columns(
        node=nodes.pointer_from(pw.this.node),
        feature=features.pointer_from(pw.this.feature),
        weight=pw.cast(float, pw.this.weight),
    )
    assert_table_equality_wo_index(
        fuzzy_match(node_feature, node_feature, features),
        T(
            """
    right | left | weight
       a |    a  |   0.5
       b |    b  |   0.5
       c |    c  |   0.5
    """,
        ).with_columns(
            right=nodes.pointer_from(pw.this.right),
            left=nodes.pointer_from(pw.this.left),
        ),
    )


def test_fuzzy_match_many_to_many():
    nodes = T(
        """
        name
        0
        1
        2
        3
        4
        5
        6
        7
        8
        9
        10
        11
        12
        13
        14
        15
        16
        17
        18
        19
        """,
        id_from=["name"],
    )
    features = T(
        """
      | weight
    1 | 1.0
    """
    ).with_columns(weight=pw.cast(float, pw.this.weight))
    features += features.select(normalization_type=FuzzyJoinNormalization.WEIGHT)
    node_feature_left = T(
        """
     node | feature | weight
        0 |       1 |    1.0
        1 |       1 |    1.0
        2 |       1 |    1.0
        3 |       1 |    1.0
        4 |       1 |    1.0
        5 |       1 |    1.0
        6 |       1 |    1.0
        7 |       1 |    1.0
        8 |       1 |    1.0
        9 |       1 |    1.0
    """,
    ).with_columns(
        node=nodes.pointer_from(pw.this.node),
        feature=features.pointer_from(pw.this.feature),
        weight=pw.cast(float, pw.this.weight),
    )
    node_feature_right = T(
        """
     node | feature | weight
       10 |       1 |    1.0
       11 |       1 |    1.0
       12 |       1 |    1.0
       13 |       1 |    1.0
       14 |       1 |    1.0
       15 |       1 |    1.0
       16 |       1 |    1.0
       17 |       1 |    1.0
       18 |       1 |    1.0
       19 |       1 |    1.0
    """,
    ).with_columns(
        node=nodes.pointer_from(pw.this.node),
        feature=features.pointer_from(pw.this.feature),
        weight=pw.cast(float, pw.this.weight),
    )

    assert_table_equality_wo_index(
        fuzzy_match(node_feature_left, node_feature_right, features),
        T(
            """
    right | left | weight
       15 |    6 | 0.03125
    """,
        ).with_columns(
            right=nodes.pointer_from(pw.this.right),
            left=nodes.pointer_from(pw.this.left),
        ),
    )


def test_fuzzy_self_match_simple():
    nodes = T(
        """
    name
    a
    b
    c
    """,
        id_from=["name"],
    )
    features = T(
        """
      | weight
    1 | 1.0
    2 | 1.0
    3 | 1.0
    """
    ).with_columns(weight=pw.cast(float, pw.this.weight))
    features += features.select(normalization_type=FuzzyJoinNormalization.WEIGHT)
    node_feature = T(
        """
     node | feature | weight
        a |       1 |    1.0
        a |       2 |    1.0
        b |       2 |    1.0
        c |       3 |    1.0
    """,
    ).with_columns(
        node=nodes.pointer_from(pw.this.node),
        feature=features.pointer_from(pw.this.feature),
        weight=pw.cast(float, pw.this.weight),
    )
    assert_table_equality_wo_index(
        fuzzy_self_match(node_feature, features),
        T(
            """
    right | left | weight
        a |    b | 0.5
    """,
        ).with_columns(
            right=nodes.pointer_from(pw.this.right),
            left=nodes.pointer_from(pw.this.left),
        ),
    )


def test_fuzzy_self_match_many_to_many():
    nodes = T(
        """
        name
        0
        1
        2
        3
        4
        5
        6
        7
        8
        9
        10
        11
        12
        13
        14
        15
        16
        17
        18
        19
        """
    )
    features = T(
        """
      | weight
    1 | 1.0
    """
    ).with_columns(weight=pw.cast(float, pw.this.weight))
    features += features.select(normalization_type=FuzzyJoinNormalization.WEIGHT)
    node_feature = T(
        """
     node | feature | weight
        0 |       1 |    1.0
        1 |       1 |    1.0
        2 |       1 |    1.0
        3 |       1 |    1.0
        4 |       1 |    1.0
        5 |       1 |    1.0
        6 |       1 |    1.0
        7 |       1 |    1.0
        8 |       1 |    1.0
        9 |       1 |    1.0
    """,
    ).with_columns(
        node=nodes.pointer_from(pw.this.node),
        feature=features.pointer_from(pw.this.feature),
        weight=pw.cast(float, pw.this.weight),
    )
    assert_table_equality_wo_index(
        fuzzy_self_match(node_feature, features),
        T(
            """
    right |left |  weight
       6 |    5 | 0.0625
    """,
        ).with_columns(
            right=nodes.pointer_from(pw.this.right),
            left=nodes.pointer_from(pw.this.left),
        ),
    )


# FIXME: compares pointers with <, fix this
def test_smart():
    tab = T(
        """
    name
    bbbdd
    aabbb
    ccddd
    ccc
    aaccc
    ddd
    """,
    )
    ret = smart_fuzzy_match(
        tab.name, tab.name, feature_generation=FuzzyJoinFeatureGeneration.LETTERS
    )

    expected = T(
        """
    left   | right | weight
    aabbb  | bbbdd | 3.0
    aaccc  | ccc   | 2.25
    ccddd  | ddd   | 2.25
    """,
    )

    assert_table_equality_wo_index(
        ret.select(
            left=tab.ix(ret.left).name, right=tab.ix(ret.right).name, weight=ret.weight
        ).with_columns(
            left=apply_with_type(min, str, pw.this.left, pw.this.right),
            right=apply_with_type(max, str, pw.this.left, pw.this.right),
        ),
        expected,
    )


# FIXME: compares pointers with <, fix this
def test_smart_two_tables():
    left = T(
        """
    name
    aabbb
    ccddd
    """
    )
    right = T(
        """
    name
    aaddd
    bbb
    cc
    """
    )
    ret = smart_fuzzy_match(
        left.name, right.name, feature_generation=FuzzyJoinFeatureGeneration.LETTERS
    )
    expected = T(
        """
    left  | right    | weight
    aabbb | bbb      | 3.0
    ccddd | aaddd    | 3.0
    """,
    )
    assert_table_equality_wo_index_types(
        ret.select(
            left=left.ix(ret.left).name,
            right=right.ix(ret.right).name,
            weight=ret.weight,
        ),
        expected,
    )


# FIXME: compares pointers with <, fix this
def test_smart_large_light():
    n = 10000
    tab = T(
        "\n".join(["name"] + [str(i // 2) for i in range(n)]),
        unsafe_trusted_ids=True,
    )
    ret = smart_fuzzy_match(
        tab.name, tab.name, feature_generation=FuzzyJoinFeatureGeneration.TOKENIZE
    )

    assert_table_equality_wo_index(
        ret.select(
            left=tab.ix(ret.left).name, right=tab.ix(ret.right).name, weight=ret.weight
        ),
        T(
            "\n".join(
                ["left | right | weight"] + [f"{i} | {i} | 0.5" for i in range(n // 2)]
            ),
            unsafe_trusted_ids=True,
        ),
    )


# FIXME: compares pointers with <, fix this
def test_smart_large_heavy():
    n = 10000
    tab = T(
        "\n".join(["name"] + ["a"] * n),
        unsafe_trusted_ids=True,
    )
    ret = smart_fuzzy_match(
        tab.name, tab.name, feature_generation=FuzzyJoinFeatureGeneration.LETTERS
    )

    assert_table_equality_wo_index(
        ret.select(
            left=tab.ix(ret.left).name, right=tab.ix(ret.right).name, weight=ret.weight
        ),
        Table.empty(left=str, right=str, weight=float),
    )


# FIXME: compares pointers with <, fix this
def test_smart_large_heavy_light():
    n = 10000
    tab = T(
        "\n".join(["name"] + ["ab", "ab", "b", "b"] + ["a"] * n),
        unsafe_trusted_ids=True,
    )
    ret = smart_fuzzy_match(
        tab.name,
        tab.name,
        feature_generation=FuzzyJoinFeatureGeneration.LETTERS,
    )

    expected = T(
        """
    left | right | weight
       b |  b    | 0.333333
      ab | ab    | 0.404762
    """,
    )
    assert_table_equality_wo_index(
        ret.select(
            left=tab.ix(ret.left).name,
            right=tab.ix(ret.right).name,
            weight=apply_with_type(lambda x: round(x, 6), float, ret.weight),
        ),
        expected,
    )


def test_fuzzy_match_tables():
    left = T(
        """
    amount  | currency
       10   |     EUR
       20   |     PLN
    """,
    )
    right = T(
        """
    amount | currency
    ten    |    EUR
    twenty |    PLN
    thirty |    USD
    """,
    )

    matching = fuzzy_match_tables(left, right)
    ret = matching.select(
        left=left.ix(matching.left).amount,
        right=right.ix(matching.right).amount,
        weight=matching.weight,
    )

    assert_table_equality_wo_index(
        ret,
        T(
            """
   left |  right | weight
     20 | twenty |    0.5
     10 |    ten |    0.5
    """,
        ),
    )


def test_fuzzy_projection_different_columns():
    left = T(
        """
    sender  | receiver
       A    |    B
       C    |    D
    """,
    )
    right = T(
        """
    sender  | receiver
       A    |    B
       D    |    C
    """,
    )

    left_projection = {"sender": "C1", "receiver": "C2"}
    right_projection = {"sender": "C2", "receiver": "C1"}

    matching = fuzzy_match_tables(
        left, right, left_projection=left_projection, right_projection=right_projection
    )
    ret = matching.select(
        left_sender=left.ix(matching.left).sender,
        left_receiver=left.ix(matching.left).receiver,
        right_sender=right.ix(matching.right).sender,
        right_receiver=right.ix(matching.right).receiver,
        weight=matching.weight,
    )

    assert_table_equality_wo_index_types(
        ret,
        T(
            """
    left_sender  | left_receiver | right_sender | right_receiver | weight
       C         |    D          |      D       |      C         | 1.0
    """,
        ),
    )


def test_fuzzy_projection_same_columns():
    left = T(
        """
    sender  | receiver
       A    |    B
       C    |    D
    """,
    )
    right = T(
        """
    sender  | receiver
       A    |    B
       D    |    C
    """,
    )

    left_projection = {"sender": "C1", "receiver": "C2"}
    right_projection = {"sender": "C1", "receiver": "C2"}

    matching = fuzzy_match_tables(
        left, right, left_projection=left_projection, right_projection=right_projection
    )
    ret = matching.select(
        left_sender=left.ix(matching.left).sender,
        left_receiver=left.ix(matching.left).receiver,
        right_sender=right.ix(matching.right).sender,
        right_receiver=right.ix(matching.right).receiver,
        weight=matching.weight,
    )

    assert_table_equality_wo_index_types(
        ret,
        T(
            """
    left_sender  | left_receiver | right_sender | right_receiver | weight
       A         |    B          |      A       |      B         | 1.0
    """,
        ),
    )


def test_fuzzy_match_tables_concat_columns():
    left = T(
        """
    sender  | receiver
       A    |    B
       C    |    D
    """,
    )
    right = T(
        """
    sender  | receiver
       A    |    B
       D    |    C
    """,
    )

    matching = fuzzy_match_tables(left, right)
    ret = matching.select(
        left_sender=left.ix(matching.left).sender,
        left_receiver=left.ix(matching.left).receiver,
        right_sender=right.ix(matching.right).sender,
        right_receiver=right.ix(matching.right).receiver,
        weight=matching.weight,
    )

    assert_table_equality_wo_index_types(
        ret,
        T(
            """
    left_sender  | left_receiver | right_sender | right_receiver | weight
       A         |    B          |      A       |      B         | 1.0
       C         |    D          |      D       |      C         | 1.0
    """,
        ),
    )


def test_fuzzy_match_with_hint():
    nodes = T(
        """
    name
    a
    b
    c
    AA
    BB
    CC""",
        id_from=["name"],
    )
    features = T(
        """
      | weight
    1 | 1.0
    2 | 1.0
    3 | 1.0
    """
    ).with_columns(weight=pw.cast(float, pw.this.weight))
    features += features.select(normalization_type=FuzzyJoinNormalization.WEIGHT)
    node_feature_left = T(
        """
     node | feature | weight
        a |       1 |    1.0
        b |       2 |    1.0
        c |       3 |    1.0

    """,
    ).with_columns(
        node=nodes.pointer_from(pw.this.node),
        feature=features.pointer_from(pw.this.feature),
        weight=pw.cast(float, pw.this.weight),
    )
    node_feature_right = T(
        """
     node | feature | weight
       AA |       1 |    1.0
       BB |       2 |    1.0
       CC |       3 |    1.0

    """,
    ).with_columns(
        node=nodes.pointer_from(pw.this.node),
        feature=features.pointer_from(pw.this.feature),
        weight=pw.cast(float, pw.this.weight),
    )
    by_hand_match = T(
        """
    left | right | weight
      a  |    BB |    1.0

    """,
    ).with_columns(
        weight=pw.cast(float, pw.this.weight),
        left=nodes.pointer_from(pw.this.left),
        right=nodes.pointer_from(pw.this.right),
    )
    assert_table_equality_wo_index(
        fuzzy_match_with_hint(
            node_feature_left, node_feature_right, features, by_hand_match
        ),
        T(
            """
    right | left | weight
       BB |   a  |    1.0
       CC |   c  |    0.5
    """,
        ).with_columns(
            left=nodes.pointer_from(pw.this.left),
            right=nodes.pointer_from(pw.this.right),
        ),
    )
