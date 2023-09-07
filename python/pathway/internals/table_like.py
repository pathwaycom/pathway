# Copyright © 2023 Pathway

from __future__ import annotations

from typing import TypeVar

from pathway.internals import universes
from pathway.internals.deprecation_meta import DeprecationSuperclass
from pathway.internals.runtime_type_check import runtime_type_check
from pathway.internals.universe import Universe

SelfTableLike = TypeVar("SelfTableLike", bound="TableLike")


class TableLike(DeprecationSuperclass):
    """Interface class for table-likes: Table, GroupedTable and JoinResult.
    All of those contain universe info, and thus support universe-related asserts.

    Example:

    >>> import pathway as pw
    >>> t1 = pw.debug.parse_to_table('''
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

    def __init__(self, universe: Universe):
        self._universe = universe

    @runtime_type_check
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
        >>> t1 = pw.debug.parse_to_table('''
        ...   | age | owner | pet
        ... 1 | 10  | Alice | 1
        ... 2 | 9   | Bob   | 1
        ... 3 | 8   | Alice | 2
        ... ''')
        >>> t2 = pw.debug.parse_to_table('''
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

    @runtime_type_check
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
        >>> t1 = pw.debug.parse_to_table('''
        ...   | age | owner | pet
        ... 1 | 10  | Alice | 1
        ... 2 | 9   | Bob   | 1
        ... 3 | 8   | Alice | 2
        ... ''')
        >>> t2 = pw.debug.parse_to_table('''
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

    @runtime_type_check
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
        >>> t1 = pw.debug.parse_to_table('''
        ...   | pet
        ... 1 | Dog
        ... 7 | Cat
        ... ''')
        >>> t2 = pw.debug.parse_to_table('''
        ...   | age
        ... 1 | 10
        ... 7 | 3
        ... ''')
        >>> t1 = t1.promise_universe_is_equal_to(t2)
        >>> t3 = t1 + t2
        >>> pw.debug.compute_and_print(t3, include_id=False)
        pet | age
        Cat | 3
        Dog | 10
        """
        universes.promise_are_equal(self, other)
        return self
