# Copyright © 2024 Pathway

from __future__ import annotations

from typing import TYPE_CHECKING

from pathway.internals.parse_graph import G

if TYPE_CHECKING:
    from pathway.internals import TableLike


def promise_are_pairwise_disjoint(self: TableLike, *others: TableLike) -> None:
    """Asserts to Pathway that an universe of self is a subset of universe of each of the others.

    Semantics: Used in situations where Pathway cannot deduce universes are disjoint.

    Returns: None

    Note: The assertion works in place.

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
    ... ''')
    >>> pw.universes.promise_are_pairwise_disjoint(t1, t2)
    >>> t3 = t1.concat(t2)
    >>> pw.debug.compute_and_print(t3, include_id=False)
    age | owner | pet
    8   | Alice | 2
    9   | Bob   | 1
    10  | Alice | 1
    11  | Alice | 30
    12  | Tom   | 40
    """
    G.universe_solver.register_as_disjoint(
        self._universe, *(other._universe for other in others)
    )


def promise_is_subset_of(self: TableLike, *others: TableLike) -> None:
    """Asserts to Pathway that an universe of self is a subset of universe of each of the others.

    Semantics: Used in situations where Pathway cannot deduce one universe being a subset of another.

    Returns: None

    Note: The assertion works in place.

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
    ... ''')
    >>> pw.universes.promise_is_subset_of(t2, t1)
    >>> t3 = t1 << t2
    >>> pw.debug.compute_and_print(t3, include_id=False)
    age | owner | pet
    8   | Alice | 2
    9   | Bob   | 1
    10  | Alice | 30
    """
    for other in others:
        G.universe_solver.register_as_subset(self._universe, other._universe)


def promise_are_equal(self: TableLike, *others: TableLike) -> None:
    r"""Asserts to Pathway that an universe of self is equal to each of the others universes.

    Semantics: Used in situations where Pathway cannot deduce one universe being equal to another universe.

    Returns: None

    Note: The assertion works in place.

    Example:

    >>> import pathway as pw
    >>> import pytest
    >>> t1 = pw.debug.table_from_markdown(
    ...     '''
    ...   | age | owner | pet
    ... 1 | 8   | Alice | cat
    ... 2 | 9   | Bob   | dog
    ... 3 | 15  | Alice | tortoise
    ... '''
    ... )
    >>> t2 = pw.debug.table_from_markdown(
    ...     '''
    ...   | age | owner
    ... 1 | 11  | Alice
    ... 2 | 12  | Tom
    ... 3 | 7   | Eve
    ... 4 | 99  | Papa
    ... '''
    ... ).filter(pw.this.age<20)
    >>> t3 = t2.filter(pw.this.age > 10)
    >>> with pytest.raises(ValueError):
    ...     t1.update_cells(t3)
    >>> pw.universes.promise_are_equal(t1, t2)
    >>> result = t1.update_cells(t3)
    >>> pw.debug.compute_and_print(result, include_id=False)
    age | owner | pet
    11  | Alice | cat
    12  | Tom   | dog
    15  | Alice | tortoise
    """
    for other in others:
        G.universe_solver.register_as_equal(self._universe, other._universe)
