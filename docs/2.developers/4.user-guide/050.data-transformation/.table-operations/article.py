# ---
# title: "Basic Operations"
# description: ''
# notebook_export_path: notebooks/tutorials/survival_guide.ipynb
# ---

# # Basic Operations
# Must-read for first-timers and veterans alike, this guide gathers the most commonly used basic elements of Pathway.
#
#
# While the Pathway programming framework comes with advanced functionalities such as [classifiers](/developers/showcases/lsh/lsh_chapter1) or [fuzzy-joins](/developers/showcases/fuzzy_join/fuzzy_join_chapter1), it is essential to master the basic operations at the core of the framework.
# As part of this basic operations guide, we are going to walk through the following topics:
# * [Streaming and static modes](#streaming-and-static-modes)
# * [Starting with data](#starting-with-data)
# * [Select and notations](#select-and-notations)
# * [Manipulating the table](#manipulating-the-table)
# * [Working with multiples tables: union, concatenation, join](#working-with-multiple-tables-union-concatenation-join)
# * [Updating](#updating)
# * [Computing](#operations)
#
# If you want more information you can review our complete [API docs](/developers/api-docs/pathway) or some of our [tutorials](/developers/showcases/suspicious_activity_tumbling_window).

# ## Streaming and static modes
#
# The first thing to keep in mind is that Pathway is made for streaming data.
# In this **streaming mode**, Pathway assumes unbounded incoming updates.
# To process the incoming data, Pathway maintains a [dataflow](/developers/user-guide/introduction/concepts#dataflow).
# This mode requires input connectors listening to streaming data sources.
# The computation runs indefinitely until the process is killed: the computation starts when [`pw.run`](/developers/api-docs/pathway#pathway.run) is called and everything afterwards is unreachable code.
#
# However, the streaming mode may not be the most convenient when testing or debugging.
# For that purpose, Pathway provides a **static mode** in which static data may be attached to the connectors.
# In that mode, finite and static data can be loaded, e.g. from a table written in a static CSV file or from a markdown table.
#
# When the computation is run in the static mode, all the data is loaded and processed at once.
# While the static mode does not fully benefit from the dataflow, it allows checking if the graph is correctly built.
# To ease the debugging, Pathway provides a function called [`compute_and_print`](/developers/api-docs/debug#pathway.debug.compute_and_print).
# When calling `pw.debug.compute_and_print(t)`, Pathway builds the whole graph, ingests all the available static data, prints the obtained table `t`, and then discards the data.
# Calling twice `compute_and_print` will result in ingesting the whole data twice.
# In the streaming mode, the building of the graph and the ingestion of the data is done only once when `pw.run()` is called and the data is maintained in the graph until the computation is over: results cannot be printed but should be accessed using output connectors.
#
# The processing pipeline should be designed in the same way no matter what mode is used in the end. The only difference is how the data is considered.
#
# To learn more about both modes, you can read our [article](/developers/user-guide/introduction/streaming-and-static-modes/) about it.
#
# In most of our examples, we use the static mode since it avoids setting external streaming data sources such as Kafka.

# ## Starting with data
#
# Be sure to always import Pathway.

import pathway as pw

# Now you need tables to manipulate.
# The way tables are obtained depends on whether you are on streaming or static mode.
# In this article, you will be using the static mode to be able to show the behavior of each processing step, but keep in mind that Pathway is made for the streaming mode.
#
# ### Static mode
# In the static mode, you can manually enter a table using a markdown connector. Here are the (static) tables you will be using:

t_name = pw.debug.table_from_markdown(
    """
    | name
 1  | Alice
 2  | Bob
 3  | Carole
 """
)
t_age = pw.debug.table_from_markdown(
    """
    | age
 1  | 25
 2  | 32
 3  | 28
 """
)
t_david = pw.debug.table_from_markdown(
    """
    | name  | age
 4  | David | 25
 """
)

# You can display a snapshot of our table (for debugging purposes) using [`pw.debug.compute_and_print`](/developers/api-docs/debug#pathway.debug.compute_and_print):

pw.debug.compute_and_print(t_name)

# In the following, let's omit the `pw.debug.compute_and_print()` for clarity reasons but keep in mind that it is required to print the actual *static* data at a given time.
#
#
# ### Streaming mode
# In the streaming mode, those tables would be obtained using one of the [connectors](/developers/user-guide/connect/supported-data-sources/) provided by Pathway.
# For example, we could use [`pw.io.kafka.read`](/developers/api-docs/pathway-io/kafka#pathway.io.kafka.read) to obtain the table from Kafka.
# The results should be taken out of Pathway using an output connector: we could send the data to postgresql using [`pw.io.progresql.write`](/developers/api-docs/pathway-io/postgres#pathway.io.postgres.write) for instance.
# Otherwise, the way the data is manipulated is exactly the same in both modes.

# ## Select and notations
#
#  The main way to manipulate a table in Pathway is by using the [`select`](/developers/api-docs/pathway-table#pathway.Table.select) operation.
#
#  * **The dot notation**: you can use `select` to select a particular column and you can use the dot notation to specify the name of the column. For example, you can access the column "name" of your `t_david` table:

# _MD_COMMENT_START_
pw.debug.compute_and_print(t_david.select(t_david.name))
# _MD_COMMENT_END_
# _MD_SHOW_t_david.select(t_david.name)

#  * **The bracket notation**: you can also use string to access the column **the bracket notation**. The previous example is equivalent to ```t_david.select(t_david["name"])```.

#  * The **this notation**: to refer to the table currently manipulated you can use [`pw.this`](/developers/api-docs/pathway#pathway.this). Our example becomes `t_david.select(pw.this.name)`.
# This notation works for all standard transformations.
#     > It can be used to refer to the table, even if it has not been given a name, for example in successive operations:

t_new_age = t_david.select(new_age=pw.this.age).select(
    new_age_plus_7=pw.this.new_age + 7
)
# _MD_COMMENT_START_
pw.debug.compute_and_print(t_new_age)
# _MD_COMMENT_END_
# _MD_SHOW_t_new_age


# In this example, it would be impossible to refer to the table obtained after the first select (with the column `new_age`) without using `pw.this` as `t_david` still refers to the initial and unmodified table.

#  * **left and right notations**: similarly to the this notation, [`pw.left`](/developers/api-docs/pathway#pathway.left) and [`pw.right`](/developers/api-docs/pathway#pathway.right) can be used to manipulate the different tables used in a [join](#working-with-multiples-tables-union-concatenation-join) ([`.join`](/developers/api-docs/pathway-table#pathway.Table.join)).
#     > `left_table.join(right_table, pw.left.C1==pw.right.C2).select(pw.left.C3, pw.right.C4)`
#
# For more information about the join and the use of `pw.left` and `pw.right`, you can see the dedicated [section](#working-with-multiples-tables-union-concatenation-join) and [manual](/developers/user-guide/data-transformation/join-manual/).

#  * The **star * notation**: this notation is used to select all the columns of the manipulated table. `table.select(*pw.this)` will return the full table.
# It can be combined with [`.without`](/developers/api-docs/pathway-table#pathway.Table.without) to remove the unwanted columns:
#
#     > In our example, instead of selecting the "name" column, you could want to select all the columns except the "age" one. This is obtained as follows:

# _MD_COMMENT_START_
pw.debug.compute_and_print(t_david.without("age"))
# _MD_COMMENT_END_
# _MD_SHOW_t_david.without("age")

# ⚠️ Note on **column names**: column names are **case sensitive** and *any* string can be used as column name using the bracket notation.
# However it is not the case for the dot notation which is restricted by standard Python syntax.
# For instance, the dot notation does not allow names with space: using `t.select(t["my name"])` works while `t.select(t.my name)` will produce an error.
# By convention, we advocate to follow the Python variable naming convention which is to use only the special character "\_" in addition to alphanumeric characters, and not to start with a number.

# ## Manipulating the table
#
# In addition to `select`, Pathway provides more operators to manipulate and index the tables.

#  * **Filtering**: we can use [`filter`](/developers/api-docs/pathway-table#pathway.Table.filter) to keep rows following a given property.

t_adult = pw.debug.table_from_markdown(
    """
    | is_adult
 5  | True
 6  | False
 7  | True
 """
)
# _MD_COMMENT_START_
pw.debug.compute_and_print(t_adult.filter(pw.this.is_adult))
# _MD_COMMENT_END_
# _MD_SHOW_t_adult.filter(pw.this.is_adult)

# You can use standard python operators, including arithmetic operators such as `+`, `-`, `*`, `/`, `//`, `<`, `>`, or `~`:

# _MD_COMMENT_START_
pw.debug.compute_and_print(t_age.filter(pw.this.age > 30))
# _MD_COMMENT_END_
# _MD_SHOW_t_age.filter(pw.this.age>30)

# You can also use the logical operations AND (`&`), OR (`|`) and NOT (`~`):

# _MD_COMMENT_START_
pw.debug.compute_and_print(t_adult.filter(~pw.this.is_adult))
# _MD_COMMENT_END_
# _MD_SHOW_t_adult.filter(~pw.this.is_adult)

#  * **Reindexing**: you can change the ids (accessible by `table.id`) by using [`.with_id_from()`](/developers/api-docs/pathway-table#pathway.Table.with_id_from).
# You need a table with new ids:

t_new_ids = pw.debug.table_from_markdown(
    """
    | new_id_source
 1  | 4
 2  | 5
 3  | 6
 """
)

# _MD_COMMENT_START_
pw.debug.compute_and_print(t_name.with_id_from(t_new_ids.new_id_source))
# _MD_COMMENT_END_
# _MD_SHOW_t_name.with_id_from(t_new_ids.new_id_source)

# * **ix_ref**: uses a column's values as indexes.
# As an example, if you have a table containing indexes pointing to another table, you can use this [`ix_ref`](/developers/api-docs/pathway-table#pathway.Table.ix_ref) to obtain those lines:

t_selected_ids = pw.debug.table_from_markdown(
    """
      | selected_id
 100  | 1
 200  | 3
 """
)
# _MD_COMMENT_START_
pw.debug.compute_and_print(
    t_selected_ids.select(selected=t_name.ix_ref(t_selected_ids.selected_id).name)
)
# _MD_COMMENT_END_
# _MD_SHOW_t_selected_ids.select(selected=t_name.ix_ref(t_selected_ids.selected_id).name)

# * **Group-by**: we can use [`groupby`](/developers/api-docs/pathway-table#pathway.Table.groupby) to aggregate data sharing a common property and then use a reducer to compute an aggregated value.

t_spending = pw.debug.table_from_markdown(
    """
    | name  | amount
 1  | Bob   | 100
 2  | Alice | 50
 3  | Alice | 125
 4  | Bob   | 200
 """
)
# _MD_COMMENT_START_
pw.debug.compute_and_print(
    t_spending.groupby(pw.this.name).reduce(
        pw.this.name, sum=pw.reducers.sum(pw.this.amount)
    )
)
# _MD_COMMENT_END_
# _MD_SHOW_t_spending.groupby(pw.this.name).reduce(pw.this.name, sum=pw.reducers.sum(pw.this.amount))

# You can do groupbys on multiple columns at once (e.g. `.groupby(t.colA, t.colB)`).
# The list of all the available reducers can be found [here](/developers/api-docs/reducers).
#
# If you want to find out more about the [`groupby`](/developers/api-docs/pathway-table#pathway.Table.groupby) and [`reduce`](/developers/api-docs/pathway#pathway.GroupedTable.reduce) operations, you can read our [article](/developers/user-guide/data-transformation/groupby-reduce-manual/) about it.
#
# ## Working with multiples tables: union, concatenation, join
#
#  * **Union**: we can use the operator `+` or `+=` to compute the union of two tables sharing the same ids.

t_union = t_name + t_age
# _MD_COMMENT_START_
pw.debug.compute_and_print(t_union)
# _MD_COMMENT_END_
# _MD_SHOW_

# * **Concatenation**: you can use [`concat_reindex`](/developers/api-docs/pathway-table#pathway.Table.concat_reindex) to concatenate two tables:

# _MD_COMMENT_START_
pw.debug.compute_and_print(pw.Table.concat_reindex(t_union, t_david))
# _MD_COMMENT_END_
# _MD_SHOW_pw.Table.concat_reindex(t_union,t_david)

# > **Info for Databricks Delta users**: Concatenation is highly similar to the SQL [`MERGE INTO`](https://docs.databricks.com/sql/language-manual/delta-merge-into.html).
#
# * **Join**: you can do all usual types of joins in Pathway (inner, outer, left, right). The example below presents an inner join:

# _MD_COMMENT_START_
pw.debug.compute_and_print(
    t_age.join(t_name, t_age.id == t_name.id).select(t_age.age, t_name.name)
)
# _MD_COMMENT_END_
# _MD_SHOW_t_age.join(t_name, t_age.id==t_name.id).select(t_age.age, t_name.name)

# Note that in the equality `t_age.id==t_name.id` the left part must be a column of the table on which the join is done, namely `t_name` in our example. Doing `t_name.id==t_age.id` would throw an error.
#
# For more visibility, the `pw.left` and `pw.right` notations should be used:

# _MD_COMMENT_START_
pw.debug.compute_and_print(
    t_age.join(t_name, pw.left.id == pw.right.id).select(pw.left.age, pw.right.name)
)
# _MD_COMMENT_END_
# _MD_SHOW_t_age.join(t_name, pw.left.id == pw.right.id).select(pw.left.age, pw.right.name)

# If you want more info about joins, we have an entire [manu\[a\]l](/developers/user-guide/data-transformation/join-manual/) about it!
#
# ## Updating
#
# * **Adding a new column with a default value** with `select`:

# _MD_COMMENT_START_
pw.debug.compute_and_print(t_age.select(*pw.this, adult=True))
# _MD_COMMENT_END_
# _MD_SHOW_t_age.select(*pw.this, adult=True)

# The value can be a basic operation on the columns:

# _MD_COMMENT_START_
pw.debug.compute_and_print(t_age.select(*pw.this, thirties=pw.this.age >= 30))
# _MD_COMMENT_END_
# _MD_SHOW_t_age.select(*pw.this, thirties=pw.this.age>=30)

# * **Renaming** with `select`:

# _MD_COMMENT_START_
pw.debug.compute_and_print(t_name.select(surname=pw.this.name))
# _MD_COMMENT_END_
# _MD_SHOW_t_name.select(surname=pw.this.name)

#  * **Renaming** with [`rename`](/developers/api-docs/pathway-table#pathway.Table.rename):


# _MD_COMMENT_START_
pw.debug.compute_and_print(t_name.rename(surname=pw.this.name))
# _MD_COMMENT_END_
# _MD_SHOW_t_name.rename(surname=pw.this.name)

#  * **Updating cells**: you can update the values of cells using [`update_cells`](/developers/api-docs/pathway-table#pathway.Table.update_cells) which can be also done using the binary operator `<<`. The ids and column names should be the same.

t_updated_names = pw.debug.table_from_markdown(
    """
    | name
 1  | Alicia
 2  | Bobby
 3  | Caro
 """
)
# _MD_COMMENT_START_
pw.debug.compute_and_print(t_name.update_cells(t_updated_names))
# _MD_COMMENT_END_
# _MD_SHOW_t_name.update_cells(t_updated_names)

# ## Operations
#
# * **Row-centered operations** with [`pw.apply`](/developers/api-docs/pathway#pathway.apply): you can apply a function to each value of a column (or more) by using `pw.apply` in a `select`.

# _MD_COMMENT_START_
pw.debug.compute_and_print(
    t_age.select(thirties=pw.apply(lambda x: x > 30, pw.this.age))
)
# _MD_COMMENT_END_
# _MD_SHOW_t_age.select(thirties=pw.apply(lambda x: x>30, pw.this.age))

# Operations on multiples values of a single row can be easily done this way:

t_multiples_values = pw.debug.table_from_markdown(
    """
    | valA    | valB
 1  | 1       | 10
 2  | 100     | 1000
 """
)
# _MD_COMMENT_START_
pw.debug.compute_and_print(
    t_multiples_values.select(
        sum=pw.apply(lambda x, y: x + y, pw.this.valA, pw.this.valB)
    )
)
# _MD_COMMENT_END_
# _MD_SHOW_t_multiples_values.select(sum=pw.apply(lambda x,y: x+y, pw.this.valA, pw.this.valB))
