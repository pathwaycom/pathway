# ---
# title: pw.sql
# description: 'Using SQL commands with Pathway using pw.sql function.'
# notebook_export_path: notebooks/tutorials/sql_api.ipynb
# ---

# # Using SQL with Pathway
# Perform SQL commands using Pathway's `pw.sql` function.
#
# ---
#
# Pathway provides a very simple way to use SQL commands directly in your Pathway application: the use of `pw.sql`.
# Pathway is significantly different from a usual SQL database, and not all SQL operations are available in Pathway.
# In the following, we present the SQL operations which are compatible with Pathway and how to use `pw.sql`.
#
# **This article is a summary of dos and don'ts on how to use Pathway to execute SQL queries, this is not an introduction to SQL.**
#
# ## Usage
# You can very easily execute a SQL command by doing the following:
#
# ```python
# pw.sql(query, tab=t)
# ```
#
# This will execute the SQL command `query` where the Pathway table `t` (Python local variable) can be referred to as  `tab` (SQL table name) inside `query`.
# More generally, you can pass an arbitrary number of tables associations `name, table` using `**kwargs`: `pw.sql(query, tab1=t1, tab2=t2,.., tabn=tn)`.
#
# ## Example

# +
import pathway as pw

t = pw.debug.table_from_markdown(
    """
    | a | b
 1  | 1 | 2
 2  | 4 | 3
 3  | 4 | 7
 """
)

ret = pw.sql("SELECT * FROM tab WHERE a<b", tab=t)
pw.debug.compute_and_print(ret)
# -

# ## Column names
# Unlike in Pathway, column names are not case sensitive in SQL so **column names are NOT case sensitive when using `pw.sql`**.
# To improve the compatibility with Pathway, we encourage the use of the standard Python naming convention for column and table names: no special character other than "\_", alphanumeric characters, and not starting with a number.
# Using a space will work in the SQL layer (e.g. returning a column called `"a column"`), though it may be impractical afterwards, when using the dot notation in Pathway: `t.a column` will raise an error. You can still use the `t["a column"]` syntax in Pathway.
#
# ## List of Available SQL Operations
#
# ### `SELECT`
# Using select, you can access the different columns of a table:

result_select = pw.sql("SELECT a FROM tab", tab=t)
pw.debug.compute_and_print(result_select)

# ⚠️ Pathway does not preserve the order of columns!

# #### Star notation
# Pathway supports the star notation `*` to select all the columns:

result_star = pw.sql("SELECT * FROM tab", tab=t)
pw.debug.compute_and_print(result_star)

# ⚠️ Every Pathway table has a special column `id`: this column is NOT captured by `*` expressions in SQL.

# ### `WHERE`
# In a `SELECT` query, the `WHERE` clause can be used to select rows satisfying a given condition:

result_where = pw.sql("SELECT a,b FROM tab WHERE b>2", tab=t)
pw.debug.compute_and_print(result_where)

# ### Boolean and Arithmetic Expressions
# With the `SELECT ...` and `WHERE ...` clauses, you can use the following operators:
# - boolean operators: `AND`, `OR`, `NOT`
# - arithmetic operators: `+`, `-`, `*`,  `/`, `DIV`, `MOD`, `==`, `!=`, `<`, `>`, `<=`, `>=`, `<>`
# - NULL

result_bool = pw.sql("SELECT a,b FROM tab WHERE b-a>0 AND a>3", tab=t)
pw.debug.compute_and_print(result_bool)

# Both `!=` and `<>` can be used to check non-equality.

result_neq = pw.sql("SELECT a,b FROM tab WHERE a != 4 OR b <> 3", tab=t)
pw.debug.compute_and_print(result_neq)

# `NULL` can be used to filter out rows with missing values:

# +
t_null = pw.debug.table_from_markdown(
    """
    | a | b
 1  | 1 | 2
 2  | 4 |
 3  | 4 | 7
 """
)

result_null = pw.sql("SELECT a, b FROM tab WHERE b IS NOT NULL ", tab=t_null)
pw.debug.compute_and_print(result_null)
# -

# You can use single row result subqueries in the `WHERE` clause to filter a table based on the subquery results:

t_subqueries = pw.debug.table_from_markdown(
    """
    | employee | salary
 1  | 1        | 10
 2  | 2        | 11
 3  | 3        | 12
 """
)
result_subqueries = pw.sql(
    "SELECT employee, salary FROM t WHERE salary >= (SELECT AVG(salary) FROM t)",
    t=t_subqueries,
)
pw.debug.compute_and_print(result_subqueries)

# ⚠️ For now, only single row result subqueries are supported.
# Correlated subqueries and the associated operations `ANY`, `NONE`, and `EVERY` (or its alias `ALL`) are currently not supported.

# ### `GROUP BY`
# You can use `GROUP BY` to group rows with the same value for a given column, and to use an aggregate function over the grouped rows.

result_groupby = pw.sql("SELECT a, SUM(b) FROM tab GROUP BY a", tab=t)
pw.debug.compute_and_print(result_groupby)

# ⚠️ `GROUP BY` and `JOIN` should not be used together in a single `SELECT`.

# #### Aggregation functions
# With `GROUP BY`, you can use the following aggregation functions:
# - `AVG`
# - `COUNT`
# - `MAX`
# - `MIN`
# - `SUM`
#
# ⚠️ Pathway reducers (`pw.count`, `pw.sum`, etc.) aggregate over `None` values, while traditional SQL aggregate functions skip `NULL` values: be careful to remove all the undefined values before using an aggregate function.

# ### `HAVING`

result_having = pw.sql("SELECT a, SUM(b) FROM tab GROUP BY a HAVING SUM(b)>5", tab=t)
pw.debug.compute_and_print(result_having)

# ### `AS` (alias)
# Pathway supports both notations: `old_name as new_name` and `old_name new_name`.

result_alias = pw.sql("SELECT b, a AS c FROM tab", tab=t)
pw.debug.compute_and_print(result_alias)

result_alias = pw.sql("SELECT b, a c FROM tab", tab=t)
pw.debug.compute_and_print(result_alias)

# ### `UNION`
# Pathway provides the standard `UNION` SQL operator.
# Note that `UNION` requires matching column names.

# +
t_union = pw.debug.table_from_markdown(
    """
    | a | b
 4  | 9 | 3
 5  | 2 | 7
 """
)

result_union = pw.sql("SELECT * FROM tab UNION SELECT * FROM tab2", tab=t, tab2=t_union)
pw.debug.compute_and_print(result_union)
# -

# ### `INTERSECT`
# Pathway provides the standard `INTERSECT` SQL operator.
# Note that `INTERSECT` requires matching column names.

# +
t_inter = pw.debug.table_from_markdown(
    """
    | a | b
 4  | 9 | 3
 5  | 2 | 7
 6  | 1 | 2
 """
)

result_inter = pw.sql(
    "SELECT * FROM tab INTERSECT SELECT * FROM tab2", tab=t, tab2=t_inter
)
pw.debug.compute_and_print(result_inter)
# -

# ⚠️ `INTERSECT` does not support `INTERSECT ALL` (coming soon).

# ### `JOIN`
# Pathway provides different join operations: `INNER JOIN`, `LEFT JOIN` (or `LEFT OUTER JOIN`), `RIGHT JOIN` (or `RIGHT OUTER JOIN`), `SELF JOIN`, and `CROSS JOIN`.

t_join = pw.debug.table_from_markdown(
    """
    | b | c
 4  | 4 | 9
 5  | 3 | 4
 6  | 7 | 5
 """
)
result_join = pw.sql(
    "SELECT * FROM left_table INNER JOIN right_table ON left_table.b==right_table.b",
    left_table=t,
    right_table=t_join,
)
pw.debug.compute_and_print(result_join)

# ⚠️ `GROUP BY` and `JOIN` should not be used together in a single `SELECT`.

# ⚠️ `NATURAL JOIN` and `FULL JOIN` are not supported (coming soon).

# ### `WITH`
# In addition to being placed inside a `WHERE` clause, subqueries can also be performed using the `WITH` keyword:

result_with = pw.sql(
    "WITH group_table (a, sumB) AS (SELECT a, SUM(b) FROM tab GROUP BY a) SELECT sumB FROM group_table",
    tab=t,
)
pw.debug.compute_and_print(result_with)

# ## Differences from the SQL standard
# First of all, not all SQL queries can be executed in Pathway.
# This stems mainly from the fact that Pathway is built to process streaming and dynamic data efficiently.
#
# ### No ordering
# In Pathway, indexes are separately generated and maintained by the engine, which does not guarantee any row order: SQL operations like `LIMIT`, `ORDER BY` or `SELECT TOP` don't always make sense in this context. In the future, we will support an `ORDER BY ... LIMIT ...` keyword combination, which is typically meaningful in Pathway.
# The column `id` is reserved and should not be used as a column name, this column is not captured by `*` expressions.
#
# Furthermore, there is no order on the columns and the column order used in a `SELECT` query need not be preserved.
#
# ### Immutability
# Pathway tables are immutable: operations such as `INSERT INTO` are not supported.
#
# ### Limits
# Correlated subqueries are currently not supported and keywords such as `LIKE`, `ANY`, `ALL`, or `EXISTS` are not supported.
# `COALESCE` and`IFNULL` are not supported but should be soon.
# We strongly suggest not to use anonymous columns: they might work but we cannot guarantee their behavior.
#
# ## Conclusion
# Pathway provides a powerful API to ease the transition of SQL data transformations and pipelines into Pathway.
# However, Pathway and SQL serve different purposes. To benefit from all the possibilities Pathway has to offer we strongly encourage you to use the Python syntax directly, as much as you can. Most of the time, this syntax is at least as easy to follow as SQL - see for example our [join](/developers/user-guide/data-transformation/join-manual/) and [groupby](/developers/user-guide/data-transformation/groupby-reduce-manual/) manu[a]ls.
