# Copyright © 2024 Pathway

from __future__ import annotations

from typing import TypeVar

from pathway.internals import column as clmn, universes
from pathway.internals.deprecation_meta import DeprecationSuperclass
from pathway.internals.runtime_type_check import check_arg_types
from pathway.internals.universe import Universe

SelfTableLike = TypeVar("SelfTableLike", bound="TableLike")


class TableLike(DeprecationSuperclass):
    """Interface class for table-likes: Table, GroupedTable and JoinResult.
    All of those contain universe info, and thus support universe-related asserts.

    Example:

    >>> import pathway as pw
    >>> t1 = pw.debug.table_from_markdown('''
    ... age | owner | pet
    ... 10  | Alice | dog
    ... 9   | Bob   | dog
    ... 8   | Alice | cat
    ... 7   | Bob   | dog
    ... ''')
    >>> g1 = t1.groupby(t1.owner)
    >>> t2 = t1.filter(t1.age >= 9)
    >>> pw.debug.compute_and_print(t2, include_id=False)
    age | owner | pet
    9   | Bob   | dog
    10  | Alice | dog
    >>> g2 = t2.groupby(t2.owner)
    >>> pw.universes.promise_is_subset_of(g2, g1) # t2 is a subset of t1, so this is safe
    """

    _universe: Universe
    _context: clmn.Context
    _id_column: clmn.IdColumn

    def __init__(self, context: clmn.Context):
        self._context = context
        self._universe = context.universe
        self._id_column = context.id_column

    @check_arg_types
    def promise_universes_are_disjoint(
        self: SelfTableLike, other: TableLike
    ) -> SelfTableLike:
        """Asserts to Pathway that an universe of self is disjoint from universe of other.

        Semantics: Used in situations where Pathway cannot deduce universes are disjoint.

        Returns:
            self

        Note:
            The assertion works in place.

        Example:

        >>> import pathway as pw
        >>> t1 = pw.debug.table_from_markdown('''
        ...   | age | owner | pet
        ... 1 | 10  | Alice | 1
        ... 2 | 9   | Bob   | 1
        ... 3 | 8   | Alice | 2
        ... ''')
        >>> t2 = pw.debug.table_from_markdown('''
        ...    | age | owner | pet
        ... 11 | 11  | Alice | 30
        ... 12 | 12  | Tom   | 40
        ... ''').promise_universes_are_disjoint(t1)
        >>> t3 = t1.concat(t2)
        >>> pw.debug.compute_and_print(t3, include_id=False)
        age | owner | pet
        8   | Alice | 2
        9   | Bob   | 1
        10  | Alice | 1
        11  | Alice | 30
        12  | Tom   | 40
        """
        universes.promise_are_pairwise_disjoint(self, other)
        return self

    @check_arg_types
    def promise_universe_is_subset_of(
        self: SelfTableLike, other: TableLike
    ) -> SelfTableLike:
        """Asserts to Pathway that an universe of self is a subset of universe of each of the other.

        Semantics: Used in situations where Pathway cannot deduce one universe being a subset of another.

        Returns:
            self

        Note:
            The assertion works in place.

        Example:

        >>> import pathway as pw
        >>> t1 = pw.debug.table_from_markdown('''
        ...   | age | owner | pet
        ... 1 | 10  | Alice | 1
        ... 2 | 9   | Bob   | 1
        ... 3 | 8   | Alice | 2
        ... ''')
        >>> t2 = pw.debug.table_from_markdown('''
        ...   | age | owner | pet
        ... 1 | 10  | Alice | 30
        ... ''').promise_universe_is_subset_of(t1)
        >>> t3 = t1 << t2
        >>> pw.debug.compute_and_print(t3, include_id=False)
        age | owner | pet
        8   | Alice | 2
        9   | Bob   | 1
        10  | Alice | 30
        """
        universes.promise_is_subset_of(self, other)
        return self

    @check_arg_types
    def promise_universe_is_equal_to(
        self: SelfTableLike, other: TableLike
    ) -> SelfTableLike:
        """Asserts to Pathway that an universe of self is a subset of universe of each of the others.

        Semantics: Used in situations where Pathway cannot deduce one universe being a subset of another.

        Returns:
            None

        Note:
            The assertion works in place.

        Example:

        >>> import pathway as pw
        >>> import pytest
        >>> t1 = pw.debug.table_from_markdown(
        ...     '''
        ...   | age | owner | pet
        ... 1 | 8   | Alice | cat
        ... 2 | 9   | Bob   | dog
        ... 3 | 15  | Alice | tortoise
        ... 4 | 99  | Bob   | seahorse
        ... '''
        ... ).filter(pw.this.age<30)
        >>> t2 = pw.debug.table_from_markdown(
        ...     '''
        ...   | age | owner
        ... 1 | 11  | Alice
        ... 2 | 12  | Tom
        ... 3 | 7   | Eve
        ... '''
        ... )
        >>> t3 = t2.filter(pw.this.age > 10)
        >>> with pytest.raises(ValueError):
        ...     t1.update_cells(t3)
        >>> t1 = t1.promise_universe_is_equal_to(t2)
        >>> result = t1.update_cells(t3)
        >>> pw.debug.compute_and_print(result, include_id=False)
        age | owner | pet
        11  | Alice | cat
        12  | Tom   | dog
        15  | Alice | tortoise
        """
        universes.promise_are_equal(self, other)
        return self
