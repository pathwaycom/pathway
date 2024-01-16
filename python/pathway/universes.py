# Copyright © 2024 Pathway
"""Methods and classes for testing and declaring relations between keysets (universes).

Typical use:

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
>>> pw.universes.promise_are_equal(t1, t2)
>>> result = t1.update_cells(t3)
>>> pw.debug.compute_and_print(result, include_id=False)
age | owner | pet
11  | Alice | cat
12  | Tom   | dog
15  | Alice | tortoise
"""
from pathway.internals.universes import (
    promise_are_equal,
    promise_are_pairwise_disjoint,
    promise_is_subset_of,
)

__all__ = [
    "promise_are_equal",
    "promise_are_pairwise_disjoint",
    "promise_is_subset_of",
]
