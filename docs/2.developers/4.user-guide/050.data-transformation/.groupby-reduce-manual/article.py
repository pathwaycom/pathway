# ---
# title: "Groupby Reduce"
# description: 'Groupby Reduce manu[a]l'
# notebook_export_path: notebooks/tutorials/groupby_reduce_manual.ipynb
# ---

# # Groupby - Reduce
# In this manu\[a\]l, you will learn how to aggregate data with the groupby-reduce scheme.
#
# Together, the [`groupby`](/developers/api-docs/pathway-table#pathway.Table.groupby) and [`reduce`](/developers/api-docs/pathway-table#pathway.Table.reduce) operations can be used
# to **aggregate data across the rows of the table**. In this guide,
# we expand upon a simple demonstration from the
# [First-steps Guide](/developers/user-guide/data-transformation/table-operations/)
# and:
# * explain syntax of [groupby](#groupby-syntax) and [reduce](#reduce-syntax)
# * explain [two kinds of columns we get from groupby](#groupby-reduce-column-constrains)
# * explain [automatic id generation](#auto-generated-id)
# * show [a few simple applications](#more-examples)

# ## Prerequisites
# The assumption is that we are familiar with some basic operations
# explained in the [First-steps Guide](/developers/user-guide/data-transformation/table-operations/).
# As usual, we begin with importing Pathway.

import pathway as pw

# To demonstrate the capabilities of groupby and reduce operations,
# let us consider a made up scenario.
#
# **Storyline:** let's assume that you made a poll, asking whether a particular food item is a fruit or a vegetable.
#
# An answer to such a question is a tuple `(food_item, label, vote, fractional_vote)`.
# That is, if someone could tell that tomato is a fruit, but they are not really sure,
# it could be registered as two tuples:
#
# * (tomato, fruit, 1, 0.5),
#
# * (tomato, vegetable, 0, 0.5)
#
# Below, we have the results of the poll, stored in the table *poll*.


poll = pw.debug.table_from_markdown(
    """
    | food_item | label     | vote  | fractional_vote   | time
0   | tomato    | fruit     | 1     | 0.5               | 1669882728
1   | tomato    | vegetable | 0     | 0.5               | 1669882728
2   | apple     | fruit     | 1     | 1                 | 1669883612
3   | apple     | vegetable | 0     | 0                 | 1669883612
4   | pepper    | vegetable | 1     | 0.5               | 1669883059
5   | pepper    | fruit     | 0     | 0.5               | 1669883059
6   | tomato    | fruit     | 0     | 0.3               | 1669880159
7   | tomato    | vegetable | 1     | 0.7               | 1669880159
8   | corn      | fruit     | 0     | 0.3               | 1669876829
9   | corn      | vegetable | 1     | 0.7               | 1669876829
10  | tomato    | fruit     | 0     | 0.4               | 1669874325
11  | tomato    | vegetable | 1     | 0.6               | 1669874325
12  | pepper    | fruit     | 0     | 0.45              | 1669887207
13  | pepper    | vegetable | 1     | 0.55              | 1669887207
14  | apple     | fruit     | 1     | 1                 | 1669874325
15  | apple     | vegetable | 0     | 0                 | 1669874325

"""
)

# To demonstrate a simple groupby-reduce application, let's ask about
# the total `fractional_vote` that was assigned to any combination of `foot_item`, `label`.
#
#
# First we explain the syntax of both [`groupby`](#groupby-syntax) and [`reduce`](#reduce-syntax).
# Then, we show groupby-reduce code in [action](#groupby-reduce-simple-example).

# ## Groupby Syntax
# The syntax of the [`groupby`](/developers/api-docs/pathway-table#pathway.Table.groupby) operation is fairly simple:

# +
# _MD_SHOW_table.groupby(*C)
# -

#
# It takes a list of columns `*C` as argument and groups the row according to their values in those columns.
# In other words, all the rows with the same values, column-wise, in each column of `*C` are put into the same group.
#
# As a result, it returns a [`GroupedTable`](/developers/api-docs/pathway#pathway.GroupedTable) object, which stores
# a single row for each unique tuple from columns in `*C` and a collection
# of grouped items corresponding to each column that is not in `*C`.
#
# In the example above, if we groupby over a pair of columns `food_item`, `label`,
# the groupby computes a collection of `votes` and a collection of `fractional_votes`
# for each unique combination `food_item`, `label`.
#
# In order to use this object, we need to process those collections
# with the `reduce` operation.
#
# ## Reduce Syntax
# The [`reduce`](/developers/api-docs/pathway#pathway.GroupedTable.reduce) function behaves a little bit like [`select`](/developers/api-docs/pathway-table#pathway.Table.select), and it also takes
# two kinds of arguments:

# +
# _MD_SHOW_grouped_table.reduce(*SC, *NC)
# -

# * *SC is simply a list of columns that were present in the table before the groupby
# operation,
# * *NC is a list of new columns (i.e. columns with new name), each defined as
# some function of cells in the row of grouped_table.
#
# It can be used together with groupby, as follows:

# +
# _MD_SHOW_table.groupby(*C).reduce(*SC, *NC)
# -

# The main difference between `reduce` and `select` is that each row of `grouped_table`
# has two kinds of entries, simple cells and groups.
#
# The `reduce` operation allows us to apply a reducer to transform a group into a value.
#
# ## Counting Votes With Groupby-Reduce
# Below, you can see an example that uses the [`sum`](/developers/api-docs/reducers#pathway.reducers.sum) reducer to compute the sum of all
# votes and the sum of all fractional votes.

# +
aggregated_results = poll.groupby(poll.food_item, poll.label).reduce(
    poll.food_item,
    poll.label,
    total_votes=pw.reducers.sum(poll.vote),
    total_fractional_vote=pw.reducers.sum(poll.fractional_vote),
)

pw.debug.compute_and_print(aggregated_results)

# -

# ## Groupby-Reduce Column Constraints
# To briefly summarize, if $T$ is the set of all columns of a table
# * the columns from $C$ (used as comparison key) can be used as regular columns in the reduce function
# * for all the remaining columns (in $T \setminus C$), we need to apply a reducer, before we use them in expressions
#
# In particular, we can mix columns from $C$ and reduced columns from $T \setminus C$ in column expressions.


# +
def make_a_note(label: str, tot_votes: int, tot_fractional_vote: float):
    return f"{label} got {tot_votes} votes, with total fractional_vote of {round(tot_fractional_vote, 2)}"


aggregated_results_note = poll.groupby(poll.food_item, poll.label).reduce(
    poll.food_item,
    note=pw.apply(
        make_a_note,
        poll.label,
        pw.reducers.sum(poll.vote),
        pw.reducers.sum(poll.fractional_vote),
    ),
)

pw.debug.compute_and_print(aggregated_results_note)
# -

# ## Auto generated id
# The `groupby(*C).reduce(...)` operation guarantees that each row of the
# output corresponds to a unique tuple of values from *C. Therefore *C
# can be used to generate a unique id for the resulting table.
#
# In fact that is the default behavior of Pathway' groupby operation and can be used e.g. to
# join the results of this table with some other table that has this id as a foreign key.

# +
queries = pw.debug.table_from_markdown(
    """
    | food_item | label
1   | tomato    | fruit
2   | pepper    | vegetable
3   | corn      | vegetable

"""
).with_id_from(pw.this.food_item, pw.this.label)

pw.debug.compute_and_print(
    queries.join(
        aggregated_results_note, queries.id == aggregated_results_note.id, id=queries.id
    ).select(queries.food_item, aggregated_results_note.note)
)
# -

# More examples of joins (including another example of a join over a foreign key)
# can be found in the join manual ([full article](/developers/user-guide/data-transformation/join-manual/), [foreign key example](/developers/user-guide/data-transformation/join-manual#joins-on-a-foreign-key)).
#

# ## More Examples
# ### Recent activity with max reducer
# Below, you can see a piece of code that finds the latest votes that were submitted to the poll.
# It is done with `groupby`-`reduce` operations chained with [`join`](/developers/api-docs/pathway-table#pathway.Table.join) and [`filter`](/developers/api-docs/pathway-table#pathway.Table.filter), using [`pw.this`](/developers/api-docs/pathway#pathway.this).
#
#

hour = 3600
pw.debug.compute_and_print(
    poll.groupby()
    .reduce(time=pw.reducers.max(poll.time))
    .join(poll, id=poll.id)
    .select(
        poll.food_item,
        poll.label,
        poll.vote,
        poll.fractional_vote,
        poll.time,
        latest=pw.left.time,
    )
    .filter(pw.this.time >= pw.this.latest - 2 * hour)
    .select(
        pw.this.food_item,
        pw.this.label,
        pw.this.vote,
        pw.this.fractional_vote,
    )
)

# ### Removing duplicates
# Below, duplicates are removed from the table with groupby-reduce.
# On its own, selecting `food_item` and `label` from poll returns duplicate rows:

pw.debug.compute_and_print(poll.select(poll.food_item, poll.label))

# However, we can apply groupby-reduce to select a set of unique rows:

pw.debug.compute_and_print(
    poll.groupby(poll.food_item, poll.label).reduce(poll.food_item, poll.label)
)


# ### Chained groupby-reduce-join-select
# Below, you can find an example of groupby - reduce chained with join -select,
# using `pw.this`.
#
# To demonstrate that, we can ask our poll about total fractional vote for each pair
# `food_item`, `label` and total fractional vote assigned to rows for each `food_item`.

# +
relative_score = (
    poll.groupby(poll.food_item)
    .reduce(
        poll.food_item,
        total_fractional_vote=pw.reducers.sum(poll.fractional_vote),
    )
    .join(aggregated_results, pw.left.food_item == pw.right.food_item)
    .select(
        pw.left.food_item,
        pw.right.label,
        label_fractional_vote=pw.right.total_fractional_vote,
        total_fractional_vote=pw.left.total_fractional_vote,
    )
)

pw.debug.compute_and_print(relative_score)
# -

# ### Election using argmax reducer
# Below, we present a snippet of code, that in the context of a poll,
# finds the most obvious information: which label got the most votes.

# Let's take a look on what exactly is the result of [`argmax`](/developers/api-docs/reducers#pathway.reducers.argmax) reducer:

pw.debug.compute_and_print(
    relative_score.groupby(relative_score.food_item).reduce(
        argmax_id=pw.reducers.argmax(relative_score.label_fractional_vote)
    )
)

# As you can see, it returns an ID of the row that maximizes `label_fractional_vote`
# for a fixed `food_item`.
# You can filter interesting rows using those ID-s as follows:

pw.debug.compute_and_print(
    relative_score.groupby(relative_score.food_item)
    .reduce(argmax_id=pw.reducers.argmax(relative_score.label_fractional_vote))
    .join(relative_score, pw.left.argmax_id == relative_score.id)
    .select(
        relative_score.food_item,
        relative_score.label,
        relative_score.label_fractional_vote,
    )
)

# *Remark:* the code snippet above is equivalent to:

pw.debug.compute_and_print(
    relative_score.groupby(relative_score.food_item)
    .reduce(argmax_id=pw.reducers.argmax(relative_score.label_fractional_vote))
    .select(
        relative_score.ix(pw.this.argmax_id).food_item,
        relative_score.ix(pw.this.argmax_id).label,
        relative_score.ix(pw.this.argmax_id).label_fractional_vote,
    )
)

# You can read more about [joins](/developers/user-guide/data-transformation/join-manual/), [*.ix](/developers/api-docs/pathway#property-ix) and [ID-s](/developers/user-guide/data-transformation/table-operations#manipulating-the-table) in other places.
