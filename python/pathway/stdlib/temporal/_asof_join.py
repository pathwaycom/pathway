# Copyright © 2023 Pathway


from __future__ import annotations

import dataclasses
import enum
from typing import Any, Dict, List

import pathway.internals as pw
import pathway.internals.expression as expr
from pathway.internals.arg_handlers import (
    arg_handler,
    join_kwargs_handler,
    select_args_handler,
)
from pathway.internals.desugaring import (
    DesugaringContext,
    SubstitutionDesugaring,
    desugar,
)
from pathway.internals.join import validate_join_condition
from pathway.internals.runtime_type_check import runtime_type_check
from pathway.internals.trace import trace_user_frame

from .utils import TimeEventType, check_joint_types


class Direction(enum.Enum):
    BACKWARD = 0
    FORWARD = 1
    NEAREST = 2


def _build_groups(t: pw.Table, dir_next: bool) -> pw.Table:
    """
    Inputs:
        - t: ordered table
            - key: tuple where the last element indicate the group
            - next/prev pointers
        - dir_next: boolean

    Outputs a table with the same number elements with:
        - peer: next if dir_next else prev
        - peer_key: t(peer).key
        - peer_same: id of next/prev element in the table with the same group
        - peer_diff: id of next/prev element in the table with a different group
    """

    def proc(cur_id, cur, peer_id, peer) -> pw.Pointer:
        if peer is None:
            return cur_id
        if cur[-1] != peer[-1]:  # check if the same side of a join
            return cur_id
        return peer_id

    succ_table = t.select(
        orig_id=t.orig_id,
        key=t.key,
        peer=t.next if dir_next else t.prev,
    )

    succ_table += succ_table.select(
        peer_key=succ_table.ix(succ_table.peer, optional=True).key,
    )
    succ_table += succ_table.select(
        group_repr=pw.apply(
            proc,
            succ_table.id,
            succ_table.key,
            succ_table.peer,
            succ_table.peer_key,
        )
    )

    def merge_ccs(data):
        data <<= data.select(data.ix(data.group_repr).group_repr)
        return dict(data=data)

    group_table = pw.iterate(merge_ccs, data=succ_table).data
    # At the end of the iterative merge_ccs, we have:
    # group_repr = last element of each consecutive group with the same `key`
    # We want to compute two things:
    #   - `next_same`: the next element with the same key
    #   - `next_diff`: the next element with a different key
    # To do so,
    # let reprs = elements which are the last elements of each consecutive group
    # next_diff(x) = group_repr(x).peer
    # next_same(x) = is_repr ? next_diff(x).peer : x.peer
    #

    reprs = group_table.filter(group_table.id == group_table.group_repr)
    group_table += group_table.select(
        peer_diff=group_table.ix(group_table.group_repr, optional=True).peer
    )
    group_table += group_table.select(peer_same=group_table.peer)
    group_table <<= reprs.select(peer_same=group_table.ix(reprs.id, optional=True).peer)
    return group_table


@dataclasses.dataclass
class _SelectColumn:
    source_column: pw.ColumnReference
    internal_name: str
    output_name: str
    side: int
    default: Any


@dataclasses.dataclass
class _SideData:
    side: bool
    table: pw.Table
    conds: List[pw.ColumnExpression]
    t: pw.ColumnExpression

    def make_sort_key(self):
        return pw.make_tuple(self.t, self.side)

    def make_shard_key(self):
        if len(self.conds) == 0:
            return None
        if len(self.conds) == 1:
            return self.conds[0]
        return pw.make_tuple(*self.conds)


class AsofJoinResult(DesugaringContext):
    """Result of an ASOF join of two tables

    Example:

    >>> import pathway as pw
    >>>
    >>> t1 = pw.debug.table_from_markdown(
    ...     '''
    ...         | K | val |  t
    ...     1   | 0 | 1   |  1
    ...     2   | 0 | 2   |  4
    ...     3   | 0 | 3   |  5
    ...     4   | 0 | 4   |  6
    ...     5   | 0 | 5   |  7
    ...     6   | 0 | 6   |  11
    ...     7   | 0 | 7   |  12
    ...     8   | 1 | 8   |  5
    ...     9   | 1 | 9   |  7
    ... '''
    ... )
    >>>
    >>> t2 = pw.debug.table_from_markdown(
    ...     '''
    ...          | K | val | t
    ...     21   | 1 | 7  | 2
    ...     22   | 1 | 3  | 8
    ...     23   | 0 | 0  | 2
    ...     24   | 0 | 6  | 3
    ...     25   | 0 | 2  | 7
    ...     26   | 0 | 3  | 8
    ...     27   | 0 | 9  | 9
    ...     28   | 0 | 7  | 13
    ...     29   | 0 | 4  | 14
    ...     '''
    ... )
    >>> res = t1.asof_join(
    ...     t2,
    ...     t1.t,
    ...     t2.t,
    ...     t1.K == t2.K,
    ...     how=pw.JoinMode.LEFT,
    ...     defaults={t2.val: -1},
    ... ).select(
    ...     pw.this.shard_key,
    ...     pw.this.t,
    ...     val_left=t1.val,
    ...     val_right=t2.val,
    ...     sum=t1.val + t2.val,
    ... )
    >>> pw.debug.compute_and_print(res, include_id=False)
    shard_key | t  | val_left | val_right | sum
    0         | 1  | 1        | -1        | 0
    0         | 4  | 2        | 6         | 8
    0         | 5  | 3        | 6         | 9
    0         | 6  | 4        | 6         | 10
    0         | 7  | 5        | 6         | 11
    0         | 11 | 6        | 9         | 15
    0         | 12 | 7        | 9         | 16
    1         | 5  | 8        | 7         | 15
    1         | 7  | 9        | 7         | 16
    """

    _side_data: Dict[bool, _SideData]
    _mode: pw.JoinMode
    _direction: Direction
    _sub_desugaring: SubstitutionDesugaring
    _defaults: Dict[expr.InternalColRef, Any]
    _all_cols: List[_SelectColumn]

    def __init__(self, side_data, mode, defaults, direction):
        super().__init__()
        self._side_data = side_data
        self._mode = mode
        self._defaults = defaults
        self._direction = direction
        all_cols: list[_SelectColumn] = []
        for sd in self._side_data.values():
            for k in sd.table.column_names():
                col = sd.table[k]
                name = f"_c{int(sd.side)}_{col.name}"
                all_cols.append(
                    _SelectColumn(
                        internal_name=name,
                        source_column=col,
                        output_name=name,
                        side=sd.side,
                        default=defaults.get(col._to_internal()),
                    )
                )

        self._all_cols = all_cols
        self._merge_result = self._merge()

        self._sub_desugaring = SubstitutionDesugaring(
            {
                sc.source_column._to_internal(): self._merge_result[sc.internal_name]
                for sc in all_cols
            }
        )
        self._substitution = {
            pw.this: self._merge_result,
            pw.left: self._side_data[False].table,
            pw.right: self._side_data[True].table,
        }

    @property
    def _desugaring(self) -> SubstitutionDesugaring:
        return self._sub_desugaring

    def _merge(self) -> pw.Table:
        orig_data = {
            k: data.table.select(
                side=data.side,
                shard_key=data.make_shard_key(),
                orig_id=data.table.id,
                key=data.make_sort_key(),
                t=data.table.t,
                **{
                    req_col.internal_name: req_col.source_column
                    if data.side == req_col.side
                    else req_col.default
                    for req_col in self._all_cols
                },
            )
            for k, data in self._side_data.items()
        }
        target = pw.Table.concat_reindex(*orig_data.values())
        target += target.select(instance=target.shard_key)
        import pathway.stdlib.indexing

        target += pathway.stdlib.indexing.sort_from_index(
            **pathway.stdlib.indexing.build_sorted_index(target)
        )
        target = target.without(target.instance)

        next_table = _build_groups(target, dir_next=True)
        prev_table = _build_groups(target, dir_next=False)
        m = target + target.select(
            next_same=next_table.peer_same,
            next_diff=next_table.peer_diff,
            prev_same=prev_table.peer_same,
            prev_diff=prev_table.peer_diff,
        )
        peer_elem = None
        if self._direction == Direction.BACKWARD:
            peer_elem = m.prev_diff
        elif self._direction == Direction.FORWARD:
            peer_elem = m.next_diff
        elif self._direction == Direction.NEAREST:

            def select_nearest(
                cur_t: expr.ColumnReference,
                prev_id: expr.ColumnReference,
                next_id: expr.ColumnReference,
                prev_t: expr.ColumnReference,
                next_t: expr.ColumnReference,
            ):
                return pw.if_else(
                    prev_id.is_none(),
                    next_id,
                    pw.if_else(
                        next_id.is_none(),
                        prev_id,
                        pw.if_else(
                            cur_t - pw.unwrap(prev_t) < pw.unwrap(next_t) - cur_t,
                            prev_id,
                            next_id,
                        ),
                    ),
                )

            peer_elem = select_nearest(
                m.t,
                m.prev_diff,
                m.next_diff,
                m.ix(m.prev_diff, optional=True).t,
                m.ix(m.next_diff, optional=True).t,
            )

        else:
            raise ValueError(f"Unsupported direction: {self._direction}")
        m += m.select(peer_elem=peer_elem)

        def fill_self(m_self: pw.Table, side: bool):
            return {
                req.output_name: m_self[req.internal_name]
                for req in self._all_cols
                if req.side == side
            }

        def fill_peer(m_self: pw.Table, side: bool):
            reqs = [req for req in self._all_cols if req.side != side]

            reqs_with_default = [req for req in reqs if req.default is not None]
            reqs_wo_default = [req for req in reqs if req.default is None]
            m_with_peer = m_self.filter(m_self.peer_elem.is_not_none())

            res_default = m_self.select(
                **{req.output_name: req.default for req in reqs_with_default}
            )
            res_default <<= m_with_peer.select(
                **{
                    req.output_name: m.ix(m_with_peer.peer_elem)[req.internal_name]
                    for req in reqs_with_default
                }
            )

            res = {
                req.output_name: m.ix(m_self.peer_elem, optional=True)[
                    req.internal_name
                ]
                for req in reqs_wo_default
            }
            res.update(dict(res_default))
            return res

        if self._mode in [pw.JoinMode.LEFT, pw.JoinMode.OUTER]:
            m0 = m.filter(~m.side)
            m0 = m0.update_types(**orig_data[False].schema)

        if self._mode in [pw.JoinMode.RIGHT, pw.JoinMode.OUTER]:
            m1 = m.filter(m.side)
            m1 = m1.update_types(**orig_data[True].schema)

        if self._mode == pw.JoinMode.LEFT:
            res = m0.select(
                m.shard_key,
                m.t,
                m.key,
                m.side,
                **{sel_col.output_name: sel_col.default for sel_col in self._all_cols},
            )
            res = res.with_columns(**fill_self(m0, False))
            res <<= m0.select(**fill_peer(m0, False))

        if self._mode == pw.JoinMode.RIGHT:
            res = m1.select(
                m.shard_key,
                m.t,
                m.key,
                m.side,
                **{sel_col.output_name: sel_col.default for sel_col in self._all_cols},
            )
            res = res.with_columns(**fill_self(m1, True))
            res <<= m1.select(**fill_peer(m1, True))

        if self._mode == pw.JoinMode.OUTER:
            res = m.select(
                m.shard_key,
                m.t,
                m.key,
                m.side,
                **{sel_col.output_name: sel_col.default for sel_col in self._all_cols},
            )
            res <<= m0.select(**fill_self(m0, False), **fill_peer(m0, False))
            res <<= m1.select(**fill_self(m1, True), **fill_peer(m1, True))

        return res

    @desugar
    @arg_handler(handler=select_args_handler)
    @trace_user_frame
    def select(self, *args: pw.ColumnReference, **kwargs: Any) -> pw.Table:
        if self._mode not in [pw.JoinMode.LEFT, pw.JoinMode.RIGHT, pw.JoinMode.OUTER]:
            raise ValueError(f"Unsupported asof join mode: {self._mode}")

        return self._merge_result.select(*args, **kwargs)


def _asof_join(
    self: pw.Table,
    other: pw.Table,
    t_left: pw.ColumnExpression,
    t_right: pw.ColumnExpression,
    *on: pw.ColumnExpression,
    how: pw.JoinMode,
    defaults: Dict[pw.ColumnReference, Any],
    direction: Direction,
):
    check_joint_types(
        {"t_left": (t_left, TimeEventType), "t_right": (t_right, TimeEventType)}
    )
    side_data = {
        False: _SideData(side=False, table=self, conds=[], t=t_left),
        True: _SideData(side=True, table=other, conds=[], t=t_right),
    }

    for cond in on:
        cond_left, cond_right, _ = validate_join_condition(cond, self, other)
        side_data[False].conds.append(cond_left)
        side_data[True].conds.append(cond_right)

    return AsofJoinResult(
        side_data=side_data,
        mode=how,
        defaults={c._to_internal(): v for c, v in defaults.items()},
        direction=direction,
    )


@desugar(substitution={pw.left: "self", pw.right: "other"})
@arg_handler(handler=join_kwargs_handler(allow_how=True, allow_id=False))
@runtime_type_check
@trace_user_frame
def asof_join(
    self: pw.Table,
    other: pw.Table,
    self_time: pw.ColumnExpression,
    other_time: pw.ColumnExpression,
    *on: pw.ColumnExpression,
    how: pw.JoinMode,
    defaults: Dict[pw.ColumnReference, Any] = {},
    direction: Direction = Direction.BACKWARD,
):
    """Perform an ASOF join of two tables.

    Args:
        other: Table to join with self, both must contain a column `val`
        self_time, other_time: time-like column expression to do the join against
        on:  a list of column expressions. Each must have == as the top level operation
            and be of the form LHS: ColumnReference == RHS: ColumnReference.
        how: mode of the join (LEFT, RIGHT, FULL)
        defaults: dictionary column-> default value. Entries in the resulting table that
            not have a predecessor in the join will be set to this default value. If no
            default is provided, None will be used.


    Example:

    >>> import pathway as pw
    >>> t1 = pw.debug.table_from_markdown(
    ...     '''
    ...         | K | val |  t
    ...     1   | 0 | 1   |  1
    ...     2   | 0 | 2   |  4
    ...     3   | 0 | 3   |  5
    ...     4   | 0 | 4   |  6
    ...     5   | 0 | 5   |  7
    ...     6   | 0 | 6   |  11
    ...     7   | 0 | 7   |  12
    ...     8   | 1 | 8   |  5
    ...     9   | 1 | 9   |  7
    ... '''
    ... )
    >>> t2 = pw.debug.table_from_markdown(
    ...     '''
    ...          | K | val | t
    ...     21   | 1 | 7  | 2
    ...     22   | 1 | 3  | 8
    ...     23   | 0 | 0  | 2
    ...     24   | 0 | 6  | 3
    ...     25   | 0 | 2  | 7
    ...     26   | 0 | 3  | 8
    ...     27   | 0 | 9  | 9
    ...     28   | 0 | 7  | 13
    ...     29   | 0 | 4  | 14
    ...     '''
    ... )
    >>> res = t1.asof_join(
    ...     t2,
    ...     t1.t,
    ...     t2.t,
    ...     t1.K == t2.K,
    ...     how=pw.JoinMode.LEFT,
    ...     defaults={t2.val: -1},
    ... ).select(
    ...     pw.this.shard_key,
    ...     pw.this.t,
    ...     val_left=t1.val,
    ...     val_right=t2.val,
    ...     sum=t1.val + t2.val,
    ... )
    >>> pw.debug.compute_and_print(res, include_id=False)
    shard_key | t  | val_left | val_right | sum
    0         | 1  | 1        | -1        | 0
    0         | 4  | 2        | 6         | 8
    0         | 5  | 3        | 6         | 9
    0         | 6  | 4        | 6         | 10
    0         | 7  | 5        | 6         | 11
    0         | 11 | 6        | 9         | 15
    0         | 12 | 7        | 9         | 16
    1         | 5  | 8        | 7         | 15
    1         | 7  | 9        | 7         | 16
    """
    return _asof_join(
        self,
        other,
        self_time,
        other_time,
        *on,
        how=how,
        defaults=defaults,
        direction=direction,
    )


@desugar(substitution={pw.left: "self", pw.right: "other"})
@arg_handler(handler=join_kwargs_handler(allow_how=False, allow_id=False))
@runtime_type_check
@trace_user_frame
def asof_join_left(
    self: pw.Table,
    other: pw.Table,
    self_time: pw.ColumnExpression,
    other_time: pw.ColumnExpression,
    *on: pw.ColumnExpression,
    defaults: Dict[pw.ColumnReference, Any] = {},
    direction: Direction = Direction.BACKWARD,
):
    """Perform a left ASOF join of two tables.

    Args:
        other: Table to join with self, both must contain a column `val`
        self_time, other_time: time-like column expression to do the join against
        on:  a list of column expressions. Each must have == as the top level operation
            and be of the form LHS: ColumnReference == RHS: ColumnReference.
        defaults: dictionary column-> default value. Entries in the resulting table that
            not have a predecessor in the join will be set to this default value. If no
            default is provided, None will be used.
        direction: direction of the join, accepted values: Direction.BACKWARD,
            Direction.FORWARD, Direction.NEAREST


    Example:

    >>> import pathway as pw
    >>> t1 = pw.debug.table_from_markdown(
    ...     '''
    ...         | K | val |  t
    ...     1   | 0 | 1   |  1
    ...     2   | 0 | 2   |  4
    ...     3   | 0 | 3   |  5
    ...     4   | 0 | 4   |  6
    ...     5   | 0 | 5   |  7
    ...     6   | 0 | 6   |  11
    ...     7   | 0 | 7   |  12
    ...     8   | 1 | 8   |  5
    ...     9   | 1 | 9   |  7
    ... '''
    ... )
    >>> t2 = pw.debug.table_from_markdown(
    ...     '''
    ...          | K | val | t
    ...     21   | 1 | 7  | 2
    ...     22   | 1 | 3  | 8
    ...     23   | 0 | 0  | 2
    ...     24   | 0 | 6  | 3
    ...     25   | 0 | 2  | 7
    ...     26   | 0 | 3  | 8
    ...     27   | 0 | 9  | 9
    ...     28   | 0 | 7  | 13
    ...     29   | 0 | 4  | 14
    ...     '''
    ... )
    >>> res = t1.asof_join_left(
    ...     t2,
    ...     t1.t,
    ...     t2.t,
    ...     t1.K == t2.K,
    ...     defaults={t2.val: -1},
    ... ).select(
    ...     pw.this.shard_key,
    ...     pw.this.t,
    ...     val_left=t1.val,
    ...     val_right=t2.val,
    ...     sum=t1.val + t2.val,
    ... )
    >>> pw.debug.compute_and_print(res, include_id=False)
    shard_key | t  | val_left | val_right | sum
    0         | 1  | 1        | -1        | 0
    0         | 4  | 2        | 6         | 8
    0         | 5  | 3        | 6         | 9
    0         | 6  | 4        | 6         | 10
    0         | 7  | 5        | 6         | 11
    0         | 11 | 6        | 9         | 15
    0         | 12 | 7        | 9         | 16
    1         | 5  | 8        | 7         | 15
    1         | 7  | 9        | 7         | 16
    """
    return _asof_join(
        self,
        other,
        self_time,
        other_time,
        *on,
        how=pw.JoinMode.LEFT,
        defaults=defaults,
        direction=direction,
    )


@desugar(substitution={pw.left: "self", pw.right: "other"})
@arg_handler(handler=join_kwargs_handler(allow_how=False, allow_id=False))
@runtime_type_check
@trace_user_frame
def asof_join_right(
    self: pw.Table,
    other: pw.Table,
    self_time: pw.ColumnExpression,
    other_time: pw.ColumnExpression,
    *on: pw.ColumnExpression,
    defaults: Dict[pw.ColumnReference, Any] = {},
    direction: Direction = Direction.BACKWARD,
):
    """Perform a right ASOF join of two tables.

    Args:
        other: Table to join with self, both must contain a column `val`
        self_time, other_time: time-like column expression to do the join against
        on:  a list of column expressions. Each must have == as the top level operation
            and be of the form LHS: ColumnReference == RHS: ColumnReference.
        defaults: dictionary column-> default value. Entries in the resulting table that
            not have a predecessor in the join will be set to this default value. If no
            default is provided, None will be used.
        direction: direction of the join, accepted values: Direction.BACKWARD,
            Direction.FORWARD, Direction.NEAREST


    Example:

    >>> import pathway as pw
    >>> t1 = pw.debug.table_from_markdown(
    ...     '''
    ...         | K | val |  t
    ...     1   | 0 | 1   |  1
    ...     2   | 0 | 2   |  4
    ...     3   | 0 | 3   |  5
    ...     4   | 0 | 4   |  6
    ...     5   | 0 | 5   |  7
    ...     6   | 0 | 6   |  11
    ...     7   | 0 | 7   |  12
    ...     8   | 1 | 8   |  5
    ...     9   | 1 | 9   |  7
    ... '''
    ... )
    >>> t2 = pw.debug.table_from_markdown(
    ...     '''
    ...          | K | val | t
    ...     21   | 1 | 7  | 2
    ...     22   | 1 | 3  | 8
    ...     23   | 0 | 0  | 2
    ...     24   | 0 | 6  | 3
    ...     25   | 0 | 2  | 7
    ...     26   | 0 | 3  | 8
    ...     27   | 0 | 9  | 9
    ...     28   | 0 | 7  | 13
    ...     29   | 0 | 4  | 14
    ...     '''
    ... )
    >>> res = t1.asof_join_right(
    ...     t2,
    ...     t1.t,
    ...     t2.t,
    ...     t1.K == t2.K,
    ...     defaults={t1.val: -1},
    ... ).select(
    ...     pw.this.shard_key,
    ...     pw.this.t,
    ...     val_left=t1.val,
    ...     val_right=t2.val,
    ...     sum=t1.val + t2.val,
    ... )
    >>> pw.debug.compute_and_print(res, include_id=False)
    shard_key | t  | val_left | val_right | sum
    0         | 2  | 1        | 0         | 1
    0         | 3  | 1        | 6         | 7
    0         | 7  | 5        | 2         | 7
    0         | 8  | 5        | 3         | 8
    0         | 9  | 5        | 9         | 14
    0         | 13 | 7        | 7         | 14
    0         | 14 | 7        | 4         | 11
    1         | 2  | -1       | 7         | 6
    1         | 8  | 9        | 3         | 12
    """
    return _asof_join(
        self,
        other,
        self_time,
        other_time,
        *on,
        how=pw.JoinMode.RIGHT,
        defaults=defaults,
        direction=direction,
    )


@desugar(substitution={pw.left: "self", pw.right: "other"})
@arg_handler(handler=join_kwargs_handler(allow_how=False, allow_id=False))
@runtime_type_check
@trace_user_frame
def asof_join_outer(
    self: pw.Table,
    other: pw.Table,
    self_time: pw.ColumnExpression,
    other_time: pw.ColumnExpression,
    *on: pw.ColumnExpression,
    defaults: Dict[pw.ColumnReference, Any] = {},
    direction: Direction = Direction.BACKWARD,
):
    """Perform an outer ASOF join of two tables.

    Args:
        other: Table to join with self, both must contain a column `val`
        self_time, other_time: time-like column expression to do the join against
        on:  a list of column expressions. Each must have == as the top level operation
            and be of the form LHS: ColumnReference == RHS: ColumnReference.
        defaults: dictionary column-> default value. Entries in the resulting table that
            not have a predecessor in the join will be set to this default value. If no
            default is provided, None will be used.
        direction: direction of the join, accepted values: Direction.BACKWARD,
            Direction.FORWARD, Direction.NEAREST


    Example:

    >>> import pathway as pw
    >>> t1 = pw.debug.table_from_markdown(
    ...     '''
    ...         | K | val |  t
    ...     1   | 0 | 1   |  1
    ...     2   | 0 | 2   |  4
    ...     3   | 0 | 3   |  5
    ...     4   | 0 | 4   |  6
    ...     5   | 0 | 5   |  7
    ...     6   | 0 | 6   |  11
    ...     7   | 0 | 7   |  12
    ...     8   | 1 | 8   |  5
    ...     9   | 1 | 9   |  7
    ... '''
    ... )
    >>> t2 = pw.debug.table_from_markdown(
    ...     '''
    ...          | K | val | t
    ...     21   | 1 | 7  | 2
    ...     22   | 1 | 3  | 8
    ...     23   | 0 | 0  | 2
    ...     24   | 0 | 6  | 3
    ...     25   | 0 | 2  | 7
    ...     26   | 0 | 3  | 8
    ...     27   | 0 | 9  | 9
    ...     28   | 0 | 7  | 13
    ...     29   | 0 | 4  | 14
    ...     '''
    ... )
    >>> res = t1.asof_join_outer(
    ...     t2,
    ...     t1.t,
    ...     t2.t,
    ...     t1.K == t2.K,
    ...     defaults={t1.val: -1, t2.val: -1},
    ... ).select(
    ...     pw.this.shard_key,
    ...     pw.this.t,
    ...     val_left=t1.val,
    ...     val_right=t2.val,
    ...     sum=t1.val + t2.val,
    ... )
    >>> pw.debug.compute_and_print(res, include_id=False)
    shard_key | t  | val_left | val_right | sum
    0         | 1  | 1        | -1        | 0
    0         | 2  | 1        | 0         | 1
    0         | 3  | 1        | 6         | 7
    0         | 4  | 2        | 6         | 8
    0         | 5  | 3        | 6         | 9
    0         | 6  | 4        | 6         | 10
    0         | 7  | 5        | 2         | 7
    0         | 7  | 5        | 6         | 11
    0         | 8  | 5        | 3         | 8
    0         | 9  | 5        | 9         | 14
    0         | 11 | 6        | 9         | 15
    0         | 12 | 7        | 9         | 16
    0         | 13 | 7        | 7         | 14
    0         | 14 | 7        | 4         | 11
    1         | 2  | -1       | 7         | 6
    1         | 5  | 8        | 7         | 15
    1         | 7  | 9        | 7         | 16
    1         | 8  | 9        | 3         | 12
    """
    return _asof_join(
        self,
        other,
        self_time,
        other_time,
        *on,
        how=pw.JoinMode.OUTER,
        defaults=defaults,
        direction=direction,
    )
