# ---
# title: 'Building a tree with transformer classes'
# description: 'Example on how transformer classes work by implementing a tree'
# notebook_export_path: notebooks/tutorials/transformer_tree_example.ipynb
# ---

# # Implementing a tree using transformer classes
# A more advanced example on how transformer classes work by implementing a tree.
#
# Pathway's transformer class is a powerful tool to perform advanced operations on tables.
# In the following, we are going to show you how to use a transformer class to implement a tree and compute recursive operations on it.
#
# We strongly advise you to read our [introduction on transformer classes](/developers/user-guide/diving-deeper/transformer-introduction/) and the [simple examples](/developers/user-guide/diving-deeper/transformer-example/) before reading further.
#
# ## Pathway Data Structures \& Algorithms 101: How to represent a Tree?
#
# Let's take a look at one of the simplest graph-like data structures: a tree. Let's encode tree nodes into a table with columns:
#
# 1. Node ID
# 2. A value `val` of integer type, stored in nodes of the tree.
# 3. The node's parent ID - which can be Null for the root.
#
# To do this, in Pathway you can write the following schema for the considered table (ID's are implicit and don't need to be defined).

# +
from __future__ import annotations

from typing import Optional

import pathway as pw


class Nodes(pw.Schema):
    val: int
    parent: Optional[pw.Pointer[Nodes]]


# -

# ## Transformer Classes acting on a single row
#
# You would now like to compute some basic statistics on the tree. For example, is a given node the root? In Python, this would follow through a simple row operation:
#
# ```py
#     # We would want to add this logic as a "method" to the `nodes` schema
#
#     def is_root(self):
#         return self.parent is None
# ```
#
# How to make a transformer which takes a table following the schema `nodes` and "gives it" the method above? The answer is a Transformer Class which acts on a single table argument called `nodes`, and adds the `is_root` logic as an output argument. We call our transformer `tree_node_roots`:


@pw.transformer
class tree_node_roots:
    class nodes(pw.ClassArg, input=Nodes):
        val = pw.input_attribute()
        parent = pw.input_attribute()

        @pw.output_attribute
        def is_root(self):
            return self.parent is None


# Let's provide a quick explanation of what happens here.
# You can specify `Nodes` as input for the class `nodes` to enforce that the rows of the table are of type `Nodes`.
# You link the parameters of the class `nodes` to the ones of `Nodes` with the `pw.input_attribute()` function. Note that the names of the parameters (`val` and `parent` in the example) must be exactly the same as the column names of the input table.
# Finally, you declare the different columns of the resulting table using the annotation `pw.output_attribute` on different functions. Each function defines a column in the output table and the value of the function is going to be used to as the value: the name of the function defines the name of the column.
#
# You can now use `tree_node_roots` as a transformer, and call `tree_node_roots(TN)` for a table `TN` of nodes to get the required output columns, just as you would for any other transformer.

# +
tree = pw.debug.table_from_markdown(
    """
    | val | parent_label
 0  | 0    |
 1  | 1    | 0
 2  | 2    | 0
 3  | 3    | 1
 4  | 4    | 1
 5  | 5    | 2
 6  | 6    | 2
 """
)
tree += tree.select(parent=tree.pointer_from(tree.parent_label, optional=True))
pw.debug.compute_and_print(tree)

result = tree_node_roots(tree).nodes
pw.debug.compute_and_print(result)


# -

# ## Transformer Classes acting on multiple rows
#
# Now, let's try something which shows the power of Pathway a bit more. Suppose you would like to see how many steps away a node is from its root. Let's call this the `level` of a node. How would you compute this?
#
# Logically, the `level` of a node is higher by 1 unit than the `level` of its parent. So, the solution can be obtained by recursion.
#
# Recursion is perhaps something you would think twice about before [attempting in SQL](https://medium.com/swlh/recursion-in-sql-explained-graphically-679f6a0f143b). In Pathway, recursion is natively supported, and efficient to use where the "recursion stack" does not change much for old data rows as new data arrives.
#
# The transformer which does just what we want is provided below.


@pw.transformer
class tree_node_roots_and_levels:
    class nodes(pw.ClassArg, input=Nodes):
        val = pw.input_attribute()
        parent = pw.input_attribute()

        @pw.output_attribute
        def is_root(self):
            return self.parent is None

        @pw.output_attribute
        def level(self):
            if self.is_root:
                return 0
            else:
                return 1 + self.transformer.nodes[self.parent].level


# Most of the logic is contained in the final line, `1 + self.transformer.nodes[self.parent].level`.
#
# You obtain the following table:

result = tree_node_roots_and_levels(tree).nodes
pw.debug.compute_and_print(result)

# A small side note: you might simply have wanted to write here `1 + self.parent.level` instead, however, this would be missing information about the table that `self.parent` lives in. This table is identified through `self.transformer.nodes`.
#
# Though making the syntax a bit more verbose, identifying objects through both a table, and a row identifier, helps to avoid confusion.
#
# You will see why this is useful in this [article](/developers/user-guide/diving-deeper/transformer-example/) where we introduce Transformer Classes that use not just one, but two or more arguments. These will allow us to work with a `matchings` table and a `profiles` table, indicating a pair of nodes for which the required computation should be performed.
#
#
# ## Conclusion
#
# In this guide, you learned how to write transformer classes building a tree and computing some basic operations on that tree. This is useful for defining row-based logic for tables, oblivious of the fact that we are operating on top of data streams.
# You can take a look at our [tour of Pathway's transformers](/developers/user-guide/diving-deeper/transformer-example/) in which you will find a list of examples of transformers.
#
# You can also check our [connectors](/developers/user-guide/connect/pathway-connectors/) to connect your data into Pathway.
