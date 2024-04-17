# ---
# title: Manipulating Linked Lists with transformers and transformer classes
# description: A guide to simple linked list operations
# aside: true
# article:
#   date: '2023-01-09'
#   thumbnail: ''
#   tags: ['tutorial', 'connectors']
# keywords: ['linked list', 'data structure', 'transformer classes']
# author: 'pathway'
# jupyter:
#   jupytext:
#     formats: py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.15.2
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Manipulating Linked Lists with transformers and transformer classes
#
# ## Prerequisites
#
# Understanding this recipe requires some familiarity with [transformer classes](/developers/user-guide/diving-deeper/transformer-introduction).
#
# ## Introduction
#
# In this recipe, we are going to use Pathway to implement one of the most common data structure: a [linked list](https://en.wikipedia.org/w/index.php?title=Linked_list&oldid=1113983256).
# We will discuss how to model the list itself, and how to write algorithms operating on lists.
#
# ## Code
# First things first - imports and constants.

# %%
from __future__ import annotations

from typing import Callable, Optional

# %%
import pathway as pw
from pathway.tests.utils import T, assert_table_equality

# %% [markdown]
# ## Linked list
#
# As a quick reminder, a linked list is a set of nodes which are connected by unidirectional links.
# Each node contains a pointer to the next one, the first node being called the *head* and the last one the *final node* or the tail.


# %% [markdown]
# ### I/O Data
# We define a simple schema where each node of the list points to either a next one, or is None if it's a final node.
# We also define an output schema for our transformer.
#
# The output of the transformer describes two algorithms useful for us.
# One is a numerical column named `len`, computing the number of next pointers until the end of the list.
# The second is a callable column, which for a particular node `X` and an int value `steps`, returns which node is `steps` next-jumps in front of node `X`.
# %%
class Node(pw.Schema):
    next: Optional[pw.Pointer[Node]]


class Output(pw.Schema):
    len: int
    forward: Callable[..., Optional[pw.Pointer[Node]]]


# %% [markdown]
# ### Transformer logic
# We build a transformer class that operates on linked lists, named `linked_list_transformer`. Note the use of the keyword `class`.
# This transformer takes one table as the input, named `linked_list` below. Note that it is also defined using the nested `class` keyword.
# Since we plan the input table to be of schema `Node` (e.g. single column named `next` of proper type), we reflect this in two places:
#  * in `input` parameter for `class linked_list`
#  * in `next` attribute capturing a single column of the input table.
#
# The output attributes `len` and `forward` are defined as methods of the appropriate class,
# and are defined using recursive logic.
# %%


@pw.transformer
class linked_list_transformer:
    class linked_list(pw.ClassArg, input=Node):
        next = pw.input_attribute()

        @pw.output_attribute
        def len(self) -> int:
            if self.next is None:
                return 1
            else:
                return 1 + self.transformer.linked_list[self.next].len

        @pw.method
        def forward(self, steps) -> Optional[pw.Pointer[Node]]:
            if steps == 0:
                return self.id
            elif self.next is not None:
                return self.transformer.linked_list[self.next].forward(steps - 1)
            else:
                return None


# %% [markdown]
# ### Inverting list
# Below we build a transformer that takes a linked list as the input, and returns a reversed list.
# This is effectively achieved by swapping `id` and `next` columns. But extra care is required
# in dealing with `Node` values: first we need to filter out rows with `next` being `None`, and
# then a proper `None` needs to be added to the output.
# %%


def reverse_linked_list(nodes: pw.Table[Node]) -> pw.Table[Node]:
    reversed = (
        (filtered := nodes.filter(nodes.next.is_not_none()))
        .select(next=filtered.id)
        .with_id(filtered.next)
    )
    return nodes.select(next=None).update_rows(reversed)


# %% [markdown]
# ### Tests
# We present some easy test cases here.
# %%
def test_linked_list_len():
    nodes = T(
        """
    label | next_label
    1     | 2
    2     | 3
    3     | 4
    4     | 5
    5     | 6
    6     | 7
    7     | 8
    8     |
    """,
        id_from=["label"],
    )
    nodes += nodes.select(next=nodes.pointer_from(nodes.next_label, optional=True))
    expected = T(
        """
      | len
    1 | 8
    2 | 7
    3 | 6
    4 | 5
    5 | 4
    6 | 3
    7 | 2
    8 | 1
    """
    )

    ret = linked_list_transformer(linked_list=nodes).linked_list

    assert_table_equality(ret.select(ret.len), expected)


# %%
test_linked_list_len()


# %%


def test_linked_list_forward():
    nodes = T(
        """
    label | next_label
    1     | 2
    2     | 3
    3     | 4
    4     | 5
    5     | 6
    6     | 7
    7     | 8
    8     |
    """,
        id_from=["label"],
    )
    nodes += nodes.select(next=nodes.pointer_from(nodes.next_label, optional=True))

    linked_list = linked_list_transformer(linked_list=nodes).linked_list

    queries = T(
        """
    node_label | steps
    1         | 0
    2         | 1
    6         | 2
    6         | 3
    8         | 0
    8         | 2
        """
    )
    ret = queries.select(
        result=linked_list.ix_ref(queries.node_label).forward(queries.steps)
    )
    expected = T(
        """
    result_label
    1
    3
    8
    None
    8
    None
    """
    )
    expected = expected.select(
        result=nodes.pointer_from(expected.result_label, optional=True)
    )
    assert_table_equality(ret, expected)


# %%
test_linked_list_forward()

# %%


def test_linked_list_reversal():
    nodes = T(
        """
    label | next_label
    1     | 2
    2     | 3
    3     | 4
    4     | 5
    5     | 6
    6     | 7
    7     | 8
    8     |
    """,
        id_from=["label"],
    )
    nodes += nodes.select(next=nodes.pointer_from(nodes.next_label, optional=True))

    expected = T(
        """
    label | next_label
    1     |
    2     | 1
    3     | 2
    4     | 3
    5     | 4
    6     | 5
    7     | 6
    8     | 7
    """,
        id_from=["label"],
    )
    expected = expected.select(
        next=expected.pointer_from(expected.next_label, optional=True)
    )

    ret = reverse_linked_list(nodes)

    assert_table_equality(ret, expected)


# %%
test_linked_list_reversal()

# %% [markdown]
# ## Summary
# As an added bonus, observe that all transformers actually work even when on the input we provide multiple linked lists at once.
# For example, let's consider the following table:
#
# |   | next |
# |---|------|
# | 1 | 2    |
# | 2 | 3    |
# | 3 | None |
# | 4 | 5    |
# | 5 | 6    |
# | 6 | None |
# | 7 | 8    |
# | 8 | 9    |
# | 9 | None |
#
# This table is a valid input and would be properly reversed by a `reverse_linked_list` transformer.
# As always, feel free to play and experiment with this code or head to the next section.
