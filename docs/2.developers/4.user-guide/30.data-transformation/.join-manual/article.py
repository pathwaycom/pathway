# ---
# title: Simple Join
# description: Simple join manu[a]l
# notebook_export_path: notebooks/tutorials/join_manual.ipynb
# ---

# # Playing with Joins.
# A brief explanation on how to perform joins with Pathway.
#
# Join is one of the basic table operations provided in Pathway.
# A join operation combines columns from two different tables by associating rows from both tables wich are matching on some given values.
#
# This guide presents several samples of code using the joins:
# *  [join (inner join)](#simple-inner-join)
# *  [join_left, join_right, join_outer (outer_joins)](#outer-joins)
#
#
# The examples demonstrate usual use-cases, explain the behavior of
# outer joins, and point out some peculiarities you may encounter
# while using Pathway. In particular, they show how to:
# * [inherit id from left or right table](#id-inheritance-in-join)
# * [join tables using foreign keys](#joins-on-a-foreign-key)
# * [use joins in a chain](#chaining-joins)

# ## Prerequisites
#
# Be sure to import Pathway.

import pathway as pw

#
# Also, you need some tables to play with. For the sake of presentation,
# let us consider the following made up scenario: a group of four friends
# (table friends) goes to grab some breakfast.
#

friends = pw.debug.table_from_markdown(
    """
    | name  |budget
  1 | Alice | 13
  2 | Bob   | 10
  3 | Carol | 11
  4 | Dan   | 12
 """
)
menu = pw.debug.table_from_markdown(
    """
     | dish                              | price
  11 | pancakes_with_jam                 | 11
  12 | pb_jam_sandwich                   | 9
  13 | jam_pb_pancakes                   | 12
  14 | scrambled_egg                     | 11
 """
)


# #
# However, some of them have allergies (table allergies),
# and cannot eat everything. Luckily, the restaurant has a list
# of allergens contained in each dish (table allergens_in_menu).
#

# +
allergies = pw.debug.table_from_markdown(
    """
     | person| allergy
  21 | Alice | milk
  22 | Bob   | eggs
  23 | Carol | peanuts
  24 | Carol | milk
 """
)

allergens_in_menu = pw.debug.table_from_markdown(
    """
     | dish                              | allergen
  31 | pancakes_with_jam                 | milk
  32 | pancakes_with_jam                 | eggs
  33 | pb_jam_sandwich                   | peanuts
  34 | jam_pb_pancakes                   | eggs
  35 | jam_pb_pancakes                   | peanuts
  36 | jam_pb_pancakes                   | milk
  37 | scrambled_egg                     | eggs
 """
)
# -

# ## Simple inner join
# ### Syntax
# Putting it simply, the syntax of [`join`](/developers/api-docs/pathway-table#pathway.Table.join) is:

# +
# _MD_SHOW_table.join(other, *on)
# -

# where
# * `table` and `other` are tables to be joined,
# * `*on` is a list of conditions of form:

# +
# _MD_SHOW_table.some_column == other.some_other_column
# -

# **Remark:** the order of tables in the condition matters. That is,
# a condition of the following form won't be accepted:

# +
# _MD_SHOW_other.some_other_column == table.some_column
# -

# ### Examples
# As a warm-up, let us see how to make simple join-queries.
# Let's begin with a simple query that finds all pairs person-dish
# that can put someone in a hospital.

bad_match = allergies.join(
    allergens_in_menu, allergies.allergy == allergens_in_menu.allergen
).select(allergens_in_menu.dish, allergies.person)
pw.debug.compute_and_print(bad_match)

# #
# As you can see, the result is a table of life-threatening pairs.
#
# The [`select`](/developers/api-docs/pathway#pathway.JoinResult.select) function works here similarly as [`select`](/developers/api-docs/pathway-table#pathway.Table.select) on a table. The difference is
# that here, you can use columns of both tables as arguments, e.g.


# +
def make_food_constraint_note(name, dish):
    return f"{name} can't eat {dish}."


bad_match_note = allergies.join(
    allergens_in_menu, allergies.allergy == allergens_in_menu.allergen
).select(
    note=pw.apply(make_food_constraint_note, allergies.person, allergens_in_menu.dish)
)
pw.debug.compute_and_print(bad_match_note)
# -

# *Remark:* note that id is now some auto generated number, which is the usual behavior of join.

# ### On self joins
# In order to perform a self join (a join of table with itself),
# you need to create a copy with [`.copy`](/developers/api-docs/pathway-table#pathway.Table.copy).

# +
same_allergies = (
    allergies.join(
        allergies_copy := allergies.copy(), allergies.allergy == allergies_copy.allergy
    )
    .select(
        l_name=allergies.person,
        r_name=allergies_copy.person,
    )
    .filter(pw.this.l_name != pw.this.r_name)
)

pw.debug.compute_and_print(same_allergies)
# -

# ## Outer joins
# The difference between outer joins and joins is that the outer join adds to the result
# table also entries that didn't match:

# +
# _MD_SHOW_left.join_left(right, conditions)
# -

# includes entries from the *left* table and

# +
# _MD_SHOW_left.join_right(right, conditions)
# -

# includes entries from the *right* table.

# ### Simple example
# To demonstrate outer-joins, you can consider a question about forbidden
# breakfast configurations (i.e. pairs of person-dish, such that a person is
# allergic to some ingredients).

# +
people_allergies = friends.join_left(
    allergies, friends.name == allergies.person
).select(friends.name, allergies.allergy)
pw.debug.compute_and_print(people_allergies)

forbidden_breakfast = people_allergies.join_left(
    allergens_in_menu, people_allergies.allergy == allergens_in_menu.allergen
).select(people_allergies.name, allergens_in_menu.dish)
pw.debug.compute_and_print(forbidden_breakfast)
# -

# Now, as you can see, the table has duplicate rows. This is the intended behavior, as
# a particular person can be allergic to more than one ingredient. In other words,
# when you add a reason column to your result table, you can see that each row was
# included in the table above for a different reason.

forbidden_breakfast = people_allergies.join_left(
    allergens_in_menu, people_allergies.allergy == allergens_in_menu.allergen
).select(
    people_allergies.name, allergens_in_menu.dish, reason=allergens_in_menu.allergen
)
pw.debug.compute_and_print(forbidden_breakfast)


# ### Removing duplicates
# If you really want to have a table without the 'reason' column and without
# duplicates, you can achieve that with extra [`groupby`](/developers/api-docs/pathway-table#pathway.Table.groupby) and [`reduce`](/developers/api-docs/pathway#pathway.GroupedTable.reduce) :

# +
forbidden_breakfast = people_allergies.join_left(
    allergens_in_menu, people_allergies.allergy == allergens_in_menu.allergen
).select(people_allergies.name, allergens_in_menu.dish)

forbidden_breakfast_no_duplicates = forbidden_breakfast.groupby(
    forbidden_breakfast.name, forbidden_breakfast.dish
).reduce(forbidden_breakfast.name, forbidden_breakfast.dish)

pw.debug.compute_and_print(forbidden_breakfast_no_duplicates)
# -

# That simple application of groupby-reduce combination essentially selects a
# unique set of pairs (name, dish) from our table. More on the reduce-groupby operations
# can be found in the reduce-groupby [manual](/developers/user-guide/data-transformation/groupby-reduce-manual/).

# ### Expressions for unmatched rows
# Because [`join_left`](/developers/api-docs/pathway-table#pathway.Table.join_left) operation returns rows from the left table, even if there is no
# match in the right column, some input columns for functions might have no defined value.
#
# **The left join sets the undefined input cells to `None` and the function needs to
# explicitly define how to handle such cases.**
#
# As an example, you can consider a modified variant of the `bad_match_note` table.
# The modification is that you want to include all the people, and additionally you want to display their budget.
#
# To that end, it is enough that you join the `friends` table with the `bad_match` table, as
# together they include all necessary information.

basic_customer_info = friends.join_left(
    bad_match, friends.name == bad_match.person
).select(
    friends.name,
    friends.budget,
    note=pw.apply(make_food_constraint_note, friends.name, bad_match.dish),
)
pw.debug.compute_and_print(basic_customer_info)

# As you can see, the behavior of the original `make_food_constraint_note` generates a
# little bit of an odd entry for Dan. To fix that problem, you can redefine the `make_food_constraint_note`.


# +
def make_food_constraint_note_none_tolerant(name, dish):
    if dish is None:
        return f"{name} has no food restrictions."
    else:
        return make_food_constraint_note(name, dish)


basic_customer_info = friends.join_left(
    bad_match, friends.name == bad_match.person
).select(
    friends.name,
    friends.budget,
    note=pw.apply(
        make_food_constraint_note_none_tolerant,
        friends.name,
        bad_match.dish,
    ),
)
pw.debug.compute_and_print(basic_customer_info)
# -

# To once again demonstrate the fact that the arguments for expressions are replaced with
# `None`, let us change `friends.name` to `bad_match.person` in the select part of our code.

basic_customer_info = friends.join_left(
    bad_match, friends.name == bad_match.person
).select(
    bad_match.person,
    friends.budget,
    note=pw.apply(
        make_food_constraint_note_none_tolerant, friends.name, bad_match.dish
    ),
)
pw.debug.compute_and_print(basic_customer_info)

# Note that, the cell that earlier included `Dan` is empty, even though `Dan` is the
# only value that could be included in this column (if there would be a match).
# The reason is that the expression for this column simply shows the value
# from `bad match.person`; since this row was not matched, this value is undefined and
# replaced by `None`.
#

# ### On right joins
# In the examples above, you only relied on `join_left`. The [`join_right`](/developers/api-docs/pathway-table#pathway.Table.join_right) operation is quite
# similar in its behavior. Namely,

# +
people_allergies = friends.join_left(
    allergies, friends.name == allergies.person
).select(friends.name, allergies.allergy)
pw.debug.compute_and_print(people_allergies)

# is almost (except for auto-generated IDs) equivalent to

people_allergies = allergies.join_right(
    friends, allergies.person == friends.name
).select(friends.name, allergies.allergy)
pw.debug.compute_and_print(people_allergies)
# -

# When you join two tables, the only difference is in syntax - since allergies is
# the table on which you call `join_right`, it must be first argument
# in the join condition, i.e. it is

# +
# _MD_SHOW_allergies.person == friends.name
# -

# as opposed to

# +
# _MD_SHOW_friends.name == allergies.person
# -

# used in the `join_left`.

# ### On full outer joins
# The [`join_outer`](/developers/api-docs/pathway-table#pathway.Table.join_outer) operation is a full outer join, which means that

# +
# _MD_SHOW_left.join_outer(right, *on)
# -

# not only show the pairs of rows from left and right that meet the condition
# in `*on`, but also rows that didn't get matched with any other row,
# from both the `left` and `right` tables. To demonstrate this operation, let
# us introduce another group of friends and find out, for each person in a group,
# whether the other group has any people with the same name.

other_group = pw.debug.table_from_markdown(
    """
    | name  |budget
  5 | Bob   | 12
  6 | Carol | 14
  7 | Eve   | 12
 """
)

pw.debug.compute_and_print(
    friends.join_outer(other_group, friends.name == other_group.name).select(
        l_name=friends.name,
        l_id=friends.id,
        r_name=other_group.name,
        r_id=other_group.id,
    )
)


# ## ID inheritance in join
# Full (yet still informal) syntax of join is:

# +
# _MD_SHOW_table.join(other, *on, id = None)
# -

# where
# * `table` and `other` are tables to be joined,
# * `*on` is a list of conditions of form:

# +
# _MD_SHOW_table.some_column == other.some_other_column
# -

# * optional `id` can be set to either table.id or other.id
#
# Whenever the `id` argument is not none, join will try to use the column passed in
# the id argument as the new id in the result of join.
# This operation will succeed only when there is a guarantee that the resulting joined table
# has no multiple rows with the same id-to-inherit.
#
# Below you can find three examples - one successful and two failed id inheritance.
# First, let us see what are the id-s of the original tables.

pw.debug.compute_and_print(friends)
pw.debug.compute_and_print(allergies)

# ### Successful id inheritance
# Let us try a join that inherits id-s from table `allergies`.

pw.debug.compute_and_print(
    allergies.join(friends, allergies.person == friends.name, id=allergies.id).select(
        friends.name, allergies.allergy, friends.budget
    )
)

# As you can see, the id column is the same as in the `friends` table,
# which is not the case when the id parameter is not set.

pw.debug.compute_and_print(
    allergies.join(friends, allergies.person == friends.name, id=allergies.id).select(
        friends.name, allergies.allergy, friends.budget
    )
)

# ### Failed id inheritance: duplicate id-s
# The first possible problem with inheriting id is that one row of the source table
# could be matched with several entries of the other table. The code below will
# cause such a problem, and will raise a `KeyError` error.

# +
# _MD_SHOW_pw.debug.compute_and_print(
# _MD_SHOW_    allergies.join(friends, allergies.person == friends.name, id=friends.id).select(
# _MD_SHOW_        friends.name, allergies.allergy
# _MD_SHOW_    )
# _MD_SHOW_)
# _MD_SHOW_[stacktrace...]
# _MD_SHOW_KeyError: 'duplicate key: ^SERVYWW6KDGEQ2WVZ3ZZB86VSR'
# -

# ### Failed id inheritance: empty id-s
# Finally, if you consider outer joins between those two tables, you
# may encounter a situation in which you need to assign an id that is empty in the join result:


pw.debug.compute_and_print(
    allergies.join_right(friends, allergies.person == friends.name).select(
        friends.name, allergies.allergy, allergies_id=allergies.id
    )
)

# As you can see, the `allergies_id` field is not set for Dan's entry. If you try to use
# `allergies.id` as the `id` parameter, you will encounter a `TypeError` error.

# +
# _MD_SHOW_pw.debug.compute_and_print(
# _MD_SHOW_    allergies.join_right(
# _MD_SHOW_        friends, allergies.person == friends.name, id=allergies.id
# _MD_SHOW_    ).select(friends.name, allergies.allergy, allergies_id=allergies.id)
# _MD_SHOW_)
# _MD_SHOW_[stacktrace...]
# _MD_SHOW_TypeError: type mismatch: expected a pointer, got None
# -

# ## Joins on a foreign key
# In Pathway, the id column is auto-generated, and as such joining over a foreign key kept in
# some other table requires extra care. Let's assume that you have another table `likes`
# that indicates that a friend (row in `friends`) likes some particular dish (row in `menu`).

likes = pw.debug.table_from_markdown(
    """
    | f_id  | m_id
100 | 1     | 11
101 | 1     | 13
102 | 2     | 12
103 | 2     | 13
104 | 3     | 11
105 | 3     | 14
106 | 3     | 13
107 | 4     | 12
108 | 4     | 14
"""
)

# Without further specification, Pathway treats columns `f_id` and `m_id` as numbers:

pw.debug.compute_and_print(likes)

# while the id of tables `friends` and `menu` was converted to Pointer.
#

pw.debug.compute_and_print(friends)
pw.debug.compute_and_print(menu)

#
# To handle joins using those columns, you can use [`pointer_from`](/developers/api-docs/pathway-table#pathway.Table.pointer_from) function

likes += likes.select(
    f_id_ptr=friends.pointer_from(likes.f_id),
    m_id_ptr=menu.pointer_from(likes.m_id),
)
pw.debug.compute_and_print(likes)

# An [example of code](#long-chain-example) joining `friends` with `menu` using `likes`
# is presented the next section on chaining joins.

# ## Chaining joins:
# Pathway provides two ways of chaining joins. The first relies on usage of [`pw.this`](/developers/api-docs/pathway#pathway.this),
# the second allows for slightly more compact code. Below, let's focus on chaining joins
# using `pw.left`.
#
#  ### Simple join chaining
# Below, you will do chain joins using [`pw.left`](/developers/api-docs/pathway#pathway.left) and [`pw.right`](/developers/api-docs/pathway#pathway.right). To show how it can be used, let's revisit
# the first example of `join_left`, in which you computed a `join_left` on a table
# that was obtained by another `join_left`. Instead of storing the result of the first
# `join_left` in `people_allergies`, you can use the following:

pw.debug.compute_and_print(
    friends.join_left(allergies, friends.name == allergies.person)
    .select(friends.name, allergies.allergy, friends.budget)
    .join_left(allergens_in_menu, pw.left.allergy == pw.right.allergen)
    .select(pw.this.name, allergens_in_menu.dish)
)

# Essentially, for a join `left.join(right, *on)`, `pw.left` allows us to address the `left` table
# and `pw.right` allows us to address the `right` table. In this particular example, `pw.left` allows us to address the table computed by
#
#       friends.join_left(allergies, friends.name == allergies.person
#           ).select(friends.name, allergies.allergy)
#
# without breaking the chain and storing intermediate results in a temporary
# variable.
#
# More generally, given a chain:
#
# _MD_SHOW_table.join(...).select(...).join(...).select(...)...
#
# `pw.left` can be used to address the result of the latest select.
#
# While in the example above `pw.right` is essentially a replacement for `allergens_in_menu`,
# it can be also used to address a table that is passed as an argument of a join, but is
# not assigned to any variable.
#
# To show `pw.right` in action, you can go back to our [example](#expressions-using-unmatched-rows) showing basic consumer information,
# and compute it directly from tables `friends`, `allergies`, and `allergens_in_menu`.

basic_customer_info = friends.join_left(
    allergies.join(
        allergens_in_menu, allergies.allergy == allergens_in_menu.allergen
    ).select(
        allergens_in_menu.dish,
        allergies.person,
    ),
    friends.name == pw.right.person,
).select(
    friends.name,
    friends.budget,
    note=pw.apply(make_food_constraint_note_none_tolerant, friends.name, pw.right.dish),
)
pw.debug.compute_and_print(basic_customer_info)

# ### Long chain example
# To demonstrate longer chains in action, let's go back to the table `likes` you used to
# show how to handle (generate) foreign keys.

feasible_choice = (
    friends.join(likes, friends.id == likes.f_id_ptr)
    .select(friends.name, friends.budget, likes.m_id_ptr)
    .join(menu, pw.left.m_id_ptr == menu.id)
    .select(pw.left.name, pw.left.budget, menu.dish, menu.price)
    .join_left(allergies, pw.left.name == pw.right.person)
    .select(*pw.left[["name", "budget", "dish", "price"]], allergies.allergy)
    .join(allergens_in_menu, pw.left.dish == pw.right.dish)
    .select(
        *pw.left[["name", "budget", "dish", "price", "allergy"]],
        allergens_in_menu.allergen,
    )
    .filter((pw.this.price <= pw.this.budget) & (pw.this.allergy != pw.this.allergen))
)
pw.debug.compute_and_print(feasible_choice)

# As you can see, this table contains all choices of person and dish, such that a person likes
# a particular dish, is not allergic to it, and can afford it. You can further simplify
# the result by adding another groupby-reduce at the end of the chain.

pw.debug.compute_and_print(
    feasible_choice.groupby(
        *pw.this[["name", "dish", "budget", "price"]],
    ).reduce(*pw.this[["name", "dish", "budget", "price"]])
)

# Furthermore, one can make this piece of code more compact, using the [* notation](/developers/user-guide/data-transformation/table-operations#select-and-notations).

pw.debug.compute_and_print(
    friends.join(likes, friends.id == likes.f_id_ptr)
    .select(*friends, likes.m_id_ptr)
    .join(menu, pw.left.m_id_ptr == menu.id)
    .select(*pw.left.without("m_id_ptr"), *menu)
    .join_left(allergies, pw.left.name == pw.right.person)
    .select(*pw.left, allergies.allergy)
    .join(allergens_in_menu, pw.left.dish == pw.right.dish)
    .select(*pw.left, allergens_in_menu.allergen)
    .filter((pw.this.price <= pw.this.budget) & (pw.this.allergy != pw.this.allergen))
    .groupby(*pw.this)
    .reduce(*pw.this)
)
