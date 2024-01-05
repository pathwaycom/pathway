# Copyright © 2024 Pathway


from __future__ import annotations

from enum import Enum


class JoinMode(Enum):
    """Enum used for controlling type of a join when passed to a generic join function.
    Consists of values: JoinMode.INNER, JoinMode.LEFT, JoinMode.RIGHT, JoinMode.OUTER

    >>> import pathway as pw
    >>> t1 = pw.debug.table_from_markdown('''
    ... age  | owner  | pet
    ...  10  | Alice  | 1
    ...   9  | Bob    | 1
    ...   8  | Alice  | 2
    ... ''')
    >>> t2 = pw.debug.table_from_markdown('''
    ... age  | owner  | pet | size
    ...  10  | Alice  | 3   | M
    ...  9   | Bob    | 1   | L
    ...  8   | Tom    | 1   | XL
    ... ''')
    >>> inner_join = t1.join(
    ...     t2, t1.pet == t2.pet, t1.owner == t2.owner, how=pw.JoinMode.INNER
    ... ).select(age=t1.age, owner_name=t2.owner, size=t2.size)
    >>> pw.debug.compute_and_print(inner_join, include_id = False)
    age | owner_name | size
    9   | Bob        | L
    >>> outer_join = t1.join(
    ...     t2, t1.pet == t2.pet, t1.owner == t2.owner, how=pw.JoinMode.OUTER
    ... ).select(age=t1.age, owner_name=t2.owner, size=t2.size)
    >>> pw.debug.compute_and_print(outer_join, include_id = False)
    age | owner_name | size
        | Alice      | M
        | Tom        | XL
    8   |            |
    9   | Bob        | L
    10  |            |
    """

    INNER = 0
    """Use inner join."""
    LEFT = 1
    """Use left join."""
    RIGHT = 2
    """Use right join."""
    OUTER = 3
    """Use outer join."""
