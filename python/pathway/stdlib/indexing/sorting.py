# Copyright © 2024 Pathway

from __future__ import annotations

from typing import Any, Optional, TypedDict

import pathway.internals as pw
from pathway.internals.arg_tuple import wrap_arg_tuple
from pathway.internals.fingerprints import fingerprint
from pathway.internals.runtime_type_check import check_arg_types
from pathway.internals.trace import trace_user_frame


def hash(val) -> int:
    return fingerprint(int(val), format="i64", seed=0)


class Hash(pw.Schema):
    hash: int


class Node(pw.Schema):
    pass


class Key(pw.Schema):
    key: float  # TODO: generic type


class LeftRight(pw.Schema):
    left: pw.Pointer[Node] | None
    right: pw.Pointer[Node] | None


class Parent(pw.Schema):
    parent: pw.Pointer[Node] | None


class Candidate(pw.Schema):
    candidate: pw.Pointer[Node]


class Instance(pw.Schema):
    instance: Any


class PrevNext(pw.Schema):
    prev: pw.Pointer[Node] | None
    next: pw.Pointer[Node] | None


@wrap_arg_tuple
def _build_tree_step(
    result: pw.Table[Key | LeftRight | Candidate | Hash | Instance],
) -> pw.Table[Key | LeftRight | Candidate | Hash | Instance]:
    """Helper transformer that performs single step of treap building.
    Each node has 'candidate' for its parent.
    Each candidate accepts a single node as its left child (smaller key, maximal hash)
    and a single node as its right child (larger key, maximal hash)."""
    left_side: pw.Table = result.filter(result.key < result.ix(result.candidate).key)
    right_side: pw.Table = result.filter(result.key > result.ix(result.candidate).key)

    left_side_by_candidate = (
        left_side.groupby(id=left_side.candidate)
        .reduce(left=pw.reducers.argmin(left_side.hash))
        .promise_universe_is_subset_of(result)
    )
    result <<= left_side_by_candidate
    result <<= left_side.select(
        candidate=left_side_by_candidate.ix(left_side.candidate).left
    )
    right_side_by_candidate = (
        right_side.groupby(id=right_side.candidate)
        .reduce(right=pw.reducers.argmin(right_side.hash))
        .promise_universe_is_subset_of(result)
    )
    result <<= right_side_by_candidate
    result <<= right_side.select(
        candidate=right_side_by_candidate.ix(right_side.candidate).right
    )

    return result


class SortedIndex(TypedDict):
    index: pw.Table[Key | LeftRight | Parent | Instance]
    oracle: pw.Table[Node | Instance]
    # oracle (root) pw.Table indexed by Instance


@wrap_arg_tuple
def build_sorted_index(nodes: pw.Table[Key | Instance]) -> SortedIndex:
    """Treap built with priorities being hashes (minhash is the root, and recursively),
    sorted according to key column."""
    orig_id_type = nodes.schema.id_type
    nodes = nodes.update_id_type(pw.Pointer[Node])
    result: pw.Table = nodes.with_columns(hash=pw.apply(hash, nodes.id))
    root = result.groupby(result.instance).reduce(
        result.instance, root=pw.reducers.argmin(result.hash)
    )
    result = result.with_columns(
        candidate=root.ix_ref(result.instance).root,
        left=pw.declare_type(Optional[pw.Pointer], None),
        right=pw.declare_type(Optional[pw.Pointer], None),
    )

    result = pw.iterate(_build_tree_step, result=result)
    root = root.update_types(root=pw.Pointer[Any])
    result = result.select(
        result.key,
        result.left,
        result.right,
        result.instance,
        parent=None,
    ).update_types(left=orig_id_type | None, right=orig_id_type | None)

    result_nonull_left = result.filter(result.left.is_not_none())
    result <<= (
        result_nonull_left.select(parent=result_nonull_left.id)
        .with_id(result_nonull_left.left)
        .promise_universe_is_subset_of(result)
    )

    result_nonull_right = result.filter(result.right.is_not_none())
    result <<= (
        result_nonull_right.select(parent=result_nonull_right.id)
        .with_id(result_nonull_right.right)
        .promise_universe_is_subset_of(result)
    )

    return dict(index=result, oracle=root)


@check_arg_types
@trace_user_frame
@pw.table_transformer
def sort_from_index(
    index: pw.Table[LeftRight | Parent], oracle=None
) -> pw.Table[PrevNext]:

    index = index.with_columns(
        leftmost=pw.coalesce(pw.this.left, pw.this.id),
        rightmost=pw.coalesce(pw.this.right, pw.this.id),
        inverse_rightmost=pw.unwrap(
            pw.if_else(
                pw.this.ix(pw.this.parent, optional=True).right == pw.this.id,
                pw.this.parent,
                pw.this.id,
            )
        ),
        inverse_leftmost=pw.unwrap(
            pw.if_else(
                pw.this.ix(pw.this.parent, optional=True).left == pw.this.id,
                pw.this.parent,
                pw.this.id,
            )
        ),
    )
    index = pw.iterate(_treesort, tab=index)

    return index.with_columns(
        next=pw.coalesce(
            pw.this.ix(pw.this.right, optional=True).leftmost,
            pw.this.ix(pw.this.inverse_rightmost).parent,
        ),
        prev=pw.coalesce(
            pw.this.ix(pw.this.left, optional=True).rightmost,
            pw.this.ix(pw.this.inverse_leftmost).parent,
        ),
    )[list(PrevNext.keys())]


def _treesort(tab: pw.Table) -> pw.Table:
    return tab.with_columns(
        leftmost=tab.ix(pw.this.leftmost).leftmost,
        rightmost=tab.ix(pw.this.rightmost).rightmost,
        inverse_rightmost=tab.ix(pw.this.inverse_rightmost).inverse_rightmost,
        inverse_leftmost=tab.ix(pw.this.inverse_leftmost).inverse_leftmost,
    )


def _retrieving_prev_next_value(tab: pw.Table) -> pw.Table:
    return tab.with_columns(
        prev_value=pw.coalesce(
            pw.this.prev_value, tab.ix(pw.this.prev, optional=True).prev_value
        ),
        next_value=pw.coalesce(
            pw.this.next_value, tab.ix(pw.this.next, optional=True).next_value
        ),
    )


@check_arg_types
@trace_user_frame
def retrieve_prev_next_values(
    ordered_table: pw.Table, value: pw.ColumnReference | None = None
) -> pw.Table:
    """
    Retrieve, for each row, a pointer to the first row in the ordered_table that \
         contains a non-"None" value, based on the orders defined by the prev and next columns.

    Args:
        ordered_table (pw.Table): Table with three columns: value, prev, next.
                                 The prev and next columns contain pointers to other rows.
        value (Optional[pw.ColumnReference]): Column reference pointing to the column containing values.
                                              If not provided, assumes the column name is "value".

    Returns:
        pw.Table: Table with two columns: prev_value and next_value.
                  The prev_value column contains the values of the first row, according \
                  to the order defined by the column next, with a value different from None.
                  The next_value column contains the values of the first row, according \
                  to the order defined by the column prev, with a value different from None.

    """
    import pathway.internals as pw

    if value is None:
        value = ordered_table.value
    else:
        value = ordered_table[value]

    ordered_table = ordered_table.select(pw.this.prev, pw.this.next, value=value)
    ordered_table = ordered_table.with_columns(
        prev_value=pw.require(pw.this.id, pw.this.value),
        next_value=pw.require(pw.this.id, pw.this.value),
    )
    return pw.iterate(_retrieving_prev_next_value, tab=ordered_table)[
        ["prev_value", "next_value"]
    ]
