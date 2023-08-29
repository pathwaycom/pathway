# Copyright © 2023 Pathway

from __future__ import annotations

import math
from typing import Callable, Optional, TypedDict

import pathway.internals as pw
from pathway.internals.arg_tuple import wrap_arg_tuple
from pathway.internals.fingerprints import fingerprint
from pathway.internals.runtime_type_check import runtime_type_check
from pathway.internals.trace import trace_user_frame


def hash(val) -> int:
    return fingerprint(int(val), format="i64", seed=0)


class Hash(pw.Schema):
    hash: int


class Node(pw.Schema):
    pass


class Key(pw.Schema):
    key: float  # TODO: generic type


class Value(pw.Schema):
    val: float


class Aggregate(pw.Schema):
    agg: float


class LeftRight(pw.Schema):
    left: Optional[pw.Pointer[Node]]
    right: Optional[pw.Pointer[Node]]


class Parent(pw.Schema):
    parent: Optional[pw.Pointer[Node]]


class Candidate(pw.Schema):
    candidate: pw.Pointer[Node]


class Instance(pw.Schema):
    instance: float


class PrevNext(pw.Schema):
    prev: Optional[pw.Pointer[Node]]
    next: Optional[pw.Pointer[Node]]


_TreeInPreparation = TypedDict(
    "_TreeInPreparation",
    {"result": pw.Table[Key | LeftRight | Candidate | Hash | Instance]},
)


@wrap_arg_tuple
def _build_tree_step(
    result: pw.Table[Key | LeftRight | Candidate | Hash | Instance],
) -> _TreeInPreparation:
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

    return dict(result=result)


SortedIndex = TypedDict(
    "SortedIndex",
    {
        "index": pw.Table[Key | LeftRight | Parent | Instance],
        "oracle": pw.Table[Node | Instance],
    },
)  # oracle (root) pw.Table indexed by Instance


@wrap_arg_tuple
def build_sorted_index(nodes: pw.Table[Key | Instance]) -> SortedIndex:
    """Treap built with priorities being hashes (minhash is the root, and recursively),
    sorted according to key column."""
    result: pw.Table = nodes + nodes.select(hash=pw.apply(hash, nodes.id))
    root = result.groupby(result.instance).reduce(
        result.instance, root=pw.reducers.argmin(result.hash)
    )
    result += result.select(
        candidate=root.ix_ref(result.instance).root,
        left=pw.declare_type(Optional[pw.Pointer], None),
        right=pw.declare_type(Optional[pw.Pointer], None),
    )

    result = pw.iterate(_build_tree_step, result=result).result
    result = result.select(
        result.key,
        result.left,
        result.right,
        result.instance,
        parent=None,
    )

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


@runtime_type_check
@trace_user_frame
def sort_from_index(
    index: pw.Table[LeftRight | Parent], oracle=None
) -> pw.Table[PrevNext]:
    return _treesort(index=index).index  # type: ignore


@runtime_type_check
@trace_user_frame
def sort(
    table: pw.Table,
    key: Optional[pw.ColumnReference] = None,
    instance: Optional[pw.ColumnReference] = None,
) -> pw.Table[PrevNext]:
    """
    Sorts a table by the specified keys.

    Args:
        table : pw.Table
            The table to be sorted.
        key : ColumnReference or None
            The name of the primary key to sort by. If None, the table is sorted
            based on the `key` column as primary key.
        instance : ColumnReference or None
            The name of the secondary key to sort by. If None, the field "instance" is chosen
            if it exists, otherwise only the primary key is used.

    Returns:
        pw.Table: The sorted table. Contains two columns: ``prev`` and ``next``, containing the pointers
        to the previous and next rows.

    Example:

    >>> import pathway as pw
    >>> table = pw.debug.table_from_markdown('''
    ... name     | age | score
    ... Alice    | 25  | 80
    ... Bob      | 20  | 90
    ... Charlie  | 30  | 80
    ... ''')
    >>> table = table.with_id_from(pw.this.name)
    >>> table += sort(table, key=pw.this.age)
    >>> pw.debug.compute_and_print(table, include_id=False)
    name    | age | score | next        | prev
    Alice   | 25  | 80    | ^DS9AT95... | ^EDPSSB1...
    Bob     | 20  | 90    | ^GBSDEEW... |
    Charlie | 30  | 80    |             | ^GBSDEEW...
    """
    if key is None:
        key = table.key
    else:
        if isinstance(key, pw.ColumnReference):
            key = table[key]
        else:
            if isinstance(key, str):
                raise ValueError(
                    f"sorting.sort(): Invalid column reference for the parameter key, found a string. Did you mean this.{key} instead of {repr(key)}?"  # noqa: E501
                )
            raise ValueError(
                "sorting.sort(): Invalid column reference for the parameter key."  # noqa: E501
            )
    if instance is None:
        table = table.select(key=key, instance=0)
    else:
        if isinstance(instance, pw.ColumnReference):
            instance = table[instance]
        else:
            if isinstance(instance, str):
                raise ValueError(
                    f"sorting.sort(): Invalid column reference for the parameter instance. Did you mean this.{instance} instead of {repr(instance)}?"  # noqa: E501
                )
            raise ValueError(
                "sorting.sort(): Invalid column reference for the parameter instance."  # noqa: E501
            )
        table = table.select(
            key=key,
            instance=instance,
        )
    return sort_from_index(**build_sorted_index(table))


@pw.transformer
class _treesort:
    class index(pw.ClassArg, input=LeftRight | Parent, output=PrevNext):
        parent = pw.input_attribute()
        left = pw.input_attribute()
        right = pw.input_attribute()

        @pw.attribute
        def leftmost(self) -> pw.Pointer[Node]:
            if self.left is None:
                return self.id
            else:
                return self.transformer.index[self.left].leftmost

        @pw.attribute
        def rightmost(self) -> pw.Pointer[Node]:
            if self.right is None:
                return self.id
            else:
                return self.transformer.index[self.right].rightmost

        @pw.attribute
        def inverse_rightmost(self) -> pw.Pointer[Node]:
            """Lowest ancestor that is not a right son."""
            if self.parent is None:
                return self.id
            elif self.transformer.index[self.parent].right != self.id:
                return self.id
            else:
                return self.transformer.index[self.parent].inverse_rightmost

        @pw.attribute
        def inverse_leftmost(self) -> pw.Pointer[Node]:
            """Lowest ancestor that is not a right son."""
            if self.parent is None:
                return self.id
            elif self.transformer.index[self.parent].left != self.id:
                return self.id
            else:
                return self.transformer.index[self.parent].inverse_leftmost

        @pw.output_attribute
        def next(self) -> Optional[pw.Pointer[Node]]:
            if self.right is not None:
                return self.transformer.index[self.right].leftmost
            return self.transformer.index[self.inverse_rightmost].parent

        @pw.output_attribute
        def prev(self) -> Optional[pw.Pointer[Node]]:
            if self.left is not None:
                return self.transformer.index[self.left].rightmost
            return self.transformer.index[self.inverse_leftmost].parent


class ComparisonRet(pw.Schema):
    comparison_ret: int


@runtime_type_check
@trace_user_frame
def filter_cmp_helper(filter_val, index, oracle=None) -> pw.Table[ComparisonRet]:
    return _filter_cmp_helper(filter_val=filter_val, index=index).index  # type: ignore


@pw.transformer
class _filter_cmp_helper:
    """Computes column helping with value filtering.
    Uses small number of deps per filtering."""

    class filter_val(pw.ClassArg, input=Value):  # indexed by ref(Instance)
        val = pw.input_attribute()

    class index(pw.ClassArg, input=Parent | Key | Instance, output=ComparisonRet):
        parent = pw.input_attribute()
        key = pw.input_attribute()
        instance = pw.input_attribute()

        @pw.attribute
        def filter_column(self):
            """INVARIANT: cmp(self.key, filter_val) == cmp(self.key, self.filter_column)"""
            if self.parent is None:
                return self.transformer.filter_val[self.pointer_from(self.instance)].val
            parent = self.transformer.index[self.parent]
            if parent.key > self.key:
                if parent.filter_column >= parent.key:
                    return math.inf
                else:
                    return parent.filter_column
            else:
                if parent.filter_column <= parent.key:
                    return -math.inf
                else:
                    return parent.filter_column

        @pw.output_attribute
        def comparison_ret(self) -> int:
            if self.key < self.filter_column:
                return -1
            if self.key == self.filter_column:
                return 0
            return 1


class PrefixSumOracle(pw.Schema):
    prefix_sum_upperbound: Callable[..., float]
    prefix_sum_upperbound_key: Callable[..., float]


@runtime_type_check
@trace_user_frame
def prefix_sum_oracle(oracle, index) -> pw.Table[PrefixSumOracle]:
    return _prefix_sum_oracle(oracle=oracle, index=index).oracle  # type: ignore


@pw.transformer
class _prefix_sum_oracle:
    """Oracle for range queries."""

    class oracle(pw.ClassArg, output=PrefixSumOracle):  # indexed by Instance
        root = pw.input_attribute()

        @pw.method
        def prefix_sum_upperbound(self, value) -> Optional[pw.Pointer]:
            """Returns id of minimum key k such that:
            sum{i.val : i.key <= k} > value
            (i.e. returns id of a first row that does not fit in the knapsack)
            Returns None if not such k exists.
            """
            return self.transformer.index[self.root].prefix_sum_upperbound(value)

        @pw.method
        def prefix_sum_upperbound_key(self, value) -> float:
            """Returns minimum key k such that:
            sum{i.val : i.key <= k} > value
            (i.e. returns id of a first row that does not fit in the knapsack)
            Returns inf if not such k exists.
            """
            upperbound = self.prefix_sum_upperbound(value)
            if upperbound is None:
                return math.inf
            else:
                return self.transformer.index[upperbound].key

    # TODO: method for lowerbound key, not id

    class index(pw.ClassArg, input=LeftRight | Value):
        left = pw.input_attribute()
        right = pw.input_attribute()
        key = pw.input_attribute()
        val = pw.input_attribute()

        @pw.output_attribute  # TODO: make private
        def agg(self) -> float:
            ret = self.val
            if self.left is not None:
                ret += self.transformer.index[self.left].agg
            if self.right is not None:
                ret += self.transformer.index[self.right].agg
            return ret

        @pw.method
        def prefix_sum_upperbound(self, value) -> Optional[pw.Pointer]:
            if self.left is not None:
                lagg = self.transformer.index[self.left].agg
                if value < lagg:
                    return self.transformer.index[self.left].prefix_sum_upperbound(
                        value
                    )
                else:
                    value -= lagg
            if value < self.val:
                return self.id
            else:
                value -= self.val
            if self.right is not None:
                ragg = self.transformer.index[self.right].agg
                if value < ragg:
                    return self.transformer.index[self.right].prefix_sum_upperbound(
                        value
                    )
                else:
                    return None
            return None


class BinsearchOracle(pw.Schema):
    lowerbound: Callable[..., Optional[pw.Pointer]]
    upperbound: Callable[..., Optional[pw.Pointer]]


@runtime_type_check
@trace_user_frame
def binsearch_oracle(oracle, index) -> pw.Table[BinsearchOracle]:
    return _binsearch_oracle(oracle=oracle, index=index).oracle  # type: ignore


@pw.transformer
class _binsearch_oracle:
    class oracle(pw.ClassArg):  # indexed by Instance
        root = pw.input_attribute()

        @pw.method
        def lowerbound(self, value) -> Optional[pw.Pointer]:
            """Returns id of item such that item.key <= value and item.key is maximal."""
            return self.transformer.index[self.root].lowerbound(value)

        @pw.method
        def upperbound(self, value) -> Optional[pw.Pointer]:
            """Returns id of item such that item.key >= value and item.key is minimal."""
            return self.transformer.index[self.root].upperbound(value)

    class index(pw.ClassArg, input=LeftRight | Key):
        key = pw.input_attribute()
        left = pw.input_attribute()
        right = pw.input_attribute()

        @pw.method
        def lowerbound(self, value) -> Optional[pw.Pointer]:
            if self.key <= value:
                if self.right is not None:
                    right_lowerbound = self.transformer.index[self.right].lowerbound(
                        value
                    )
                    if right_lowerbound is not None:
                        return right_lowerbound
                return self.id
            elif self.left is not None:
                return self.transformer.index[self.left].lowerbound(value)
            else:
                return None

        @pw.method
        def upperbound(self, value) -> Optional[pw.Pointer]:
            if self.key >= value:
                if self.left is not None:
                    left_upperbound = self.transformer.index[self.left].upperbound(
                        value
                    )
                    if left_upperbound is not None:
                        return left_upperbound
                return self.id
            elif self.right is not None:
                return self.transformer.index[self.right].upperbound(value)
            else:
                return None


# This has O(k) complexity. TODO: write version that has O(log n) complexity.
@runtime_type_check
@trace_user_frame
def filter_smallest_k(
    column: pw.ColumnReference, instance: pw.ColumnReference, ks: pw.Table
) -> pw.Table:
    ks = ks.with_id_from(ks.instance)
    table = column.table
    colname = column.name
    sorted_index = build_sorted_index(nodes=table.select(instance=instance, key=column))
    sorted_index.index += table.select(val=1)
    oracle = prefix_sum_oracle(**sorted_index)
    pw.universes.promise_is_subset_of(ks, oracle)
    # root is pked with instance, ks also
    res = ks.select(res=oracle.prefix_sum_upperbound(ks.k))
    validres = res.filter(res.res.is_not_none()).update_types(res=pw.Pointer)
    validres = validres.select(res=getattr(table.ix(validres.res), colname))
    res <<= res.filter(res.res.is_none()).select(res=math.inf)
    res <<= validres

    selector = filter_cmp_helper(filter_val=res.select(val=res.res), **sorted_index)
    # todo drop agg
    return table.filter(selector.comparison_ret < 0)


@pw.transformer
class _retrieving_prev_next_value:
    class ordered_table(pw.ClassArg):
        value = pw.input_attribute()
        prev = pw.input_attribute()
        next = pw.input_attribute()

        @pw.output_attribute
        def prev_value(self):
            if self.value is not None:
                return None
            if self.prev is None:
                return None
            if self.transformer.ordered_table[self.prev].value is not None:
                return self.prev
            return self.transformer.ordered_table[self.prev].prev_value

        @pw.output_attribute
        def next_value(self):
            if self.value is not None:
                return None
            if self.next is None:
                return None
            if self.transformer.ordered_table[self.next].value is not None:
                return self.next
            return self.transformer.ordered_table[self.next].next_value


@runtime_type_check
@trace_user_frame
def retrieve_prev_next_values(
    ordered_table: pw.Table, value: Optional[pw.ColumnReference] = None
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
    if value is None:
        value = ordered_table.value
    else:
        if isinstance(value, pw.ColumnReference):
            value = ordered_table[value]
        else:
            if isinstance(value, str):
                raise ValueError(
                    "sorting.retrieving_prev_next_values():"
                    + "Invalid column reference for the parameter value"
                    + f", found a string. Did you mean this.{value} instead of {repr(value)}?"
                )
            raise ValueError(
                "sorting.retrieving_prev_next_values():"
                + "Invalid column reference for the parameter value."
            )

    ordered_table = ordered_table.select(pw.this.prev, pw.this.next, value=value)

    return _retrieving_prev_next_value(
        ordered_table=ordered_table
    ).ordered_table  # type: ignore
