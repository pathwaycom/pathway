# ---
# title: "Indexing from 'groupby' and Single-row Tables"
# description:
# date: '2023-03-31'
# thumbnail: '/assets/content/blog/th-computing-pagerank.png'
# tags: ['tutorial', 'table']
# keywords: ['groupby', 'ix_ref', 'single-row table']
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
# # Indexing from a `groupby` and Single-row Tables using `ix_ref`
# In this tutorial, you will learn how to use `ix_ref` in a grouped table to access a row from the grouped table and how to manipulate single-row tables.
#
# ## Accessing a grouped row by value using `ix_ref`
#
# Imagine you've just been assigned a new project: analyzing salary statistics in your company.
# In particular, your goal is to determine the number of employees who earn more than the average salary in each department.
#
# Let's consider the following table:

# %%
import pathway as pw

salaries = pw.debug.table_from_markdown(
    """
 salary   | department
 1800   | Sales
 2000   | Finance
 2300   | Sales
 2700   | Finance
 1900   | Finance
 """
)
pw.debug.compute_and_print(salaries)

# %% [markdown]
# You can easily compute the average the salary in using a `groupby` on the `department` column:

# %%
statistics = salaries.groupby(pw.this.department).reduce(
    pw.this.department, average_salary=pw.reducers.avg(pw.this.salary)
)
pw.debug.compute_and_print(statistics)

# %% [markdown]
# Now, you could do a join to add the average salary column to each entry in the `salary` table:

# %%
salaries_join = salaries.join(
    statistics, pw.left.department == pw.right.department
).select(*pw.left, pw.right.average_salary)
pw.debug.compute_and_print(salaries_join)

# %% [markdown]
# And then, you can filter whether the salary is higher than the `average_salary` column.
#
# However, this is very tedious.
#
# **In Pathway, you can directly access the row of a grouped table from the corresponding value using `ix_ref`**
#
# `ix_ref` is a value-based indexer: you can access the row corresponding to the value `Sales` with `statistics.ix_ref("Sales")`.
#
# ![ix_ref](/assets/content/blog/ix_ref.svg)
#
# With `ix_ref(value)`, you access the row associated with `value` by the `groupby`, and you can directly access the wanted column with the usual dot notation:
# to access the average salary of a given department, you can do `statistics.ix_ref(pw.this.department).average_salary`

# %%
statistics = statistics.select(
    average_salary=pw.cast(int, pw.this.average_salary),
    department=pw.this.department,
)
higher_than_average_salaries = salaries.filter(
    pw.this.salary >= statistics.ix_ref(pw.this.department).average_salary
)
pw.debug.compute_and_print(higher_than_average_salaries)

# %% [markdown]
# Now you can count the number of entries in each department with a simple `groupby`:

# %%
number_employees = higher_than_average_salaries.groupby(pw.this.department).reduce(
    count=pw.reducers.count()
)
pw.debug.compute_and_print(number_employees)

# %% [markdown]
# ## Multi-values indexing
#
# We can also use `ix_ref` to index using tuples of values by using `ix_ref(*args)`.
#
# For example, let's say we have an extra column `position`:

# %%
salaries_with_position = pw.debug.table_from_markdown(
    """
 salary | department | position
 1800   | Sales | junior
 2000   | Finance | junior
 2300   | Sales | senior
 2700   | Finance | senior
 1900   | Finance | junior
 """
)

# %% [markdown]
# We can now make a groupby using both the `department` and `position` columns:

# %%
grouped_table_with_position = salaries_with_position.groupby(
    pw.this.department, pw.this.position
).reduce(
    pw.this.department, pw.this.position, average_salary=pw.reducers.avg(pw.this.salary)
)
pw.debug.compute_and_print(grouped_table_with_position)

# %% [markdown]
# Now we can access the row using both `department` and `position` columns:

# %%
pw.debug.compute_and_print(
    salaries_with_position.select(
        *pw.this,
        average_salary=grouped_table_with_position.ix_ref(
            pw.this.department, pw.this.position
        ).average_salary,
    )
)

# %% [markdown]
# You can also use the values directly:

# %%
pw.debug.compute_and_print(
    salaries_with_position.select(
        *pw.this,
        average_salary=grouped_table_with_position.ix_ref(
            "Sales", "junior"
        ).average_salary,
    )
)

# %% [markdown]
# ## Tables with primary keys
#
# `ix_ref` is not limited to tables obtained by a groupby/reduce scheme: it works with any table with **primary keys**.
# Primary keys are the columns chosen to index the table.
#
# By default, Pathway indexes the table with uuid indexes, except when doing a groupby/reduce where the columns used for the groupby are used to index the table.
#
# You can reindex the table by manually choosing primary keys, using `.with_id_from`:

# %%
indexed_table = pw.debug.table_from_markdown(
    """
colA   | colB
10     | A
20     | B
"""
)
pw.debug.compute_and_print(indexed_table)
reindexed_table = indexed_table.with_id_from(pw.this.colB)
pw.debug.compute_and_print(reindexed_table)

# %% [markdown]
# You can see that indexes have been updated.
#
# With primary keys, the rows can now be accessed using `ix_ref`:

# %%
pw.debug.compute_and_print(
    indexed_table.select(new_val=reindexed_table.ix_ref("A").colA)
)

# %% [markdown]
# ## Single-Row Tables
#
# A special case is an empty `groupby`: all the entries are associated to the same group.
#
# It is the case when you consider global statistics such as the average salary on the entire company.
# In Pathway, it can be computed with a simple `reduce()`.
#
# Let's compute the sum of all the salaries in the company:

# %%
pw.debug.compute_and_print(salaries.reduce(sum_salary=pw.reducers.sum(pw.this.salary)))

# %% [markdown]
# As you can see, **Pathway returns a single-row table** and not the single value.
#
# As tempting as it is, in Pathway, you cannot use the value directly and do:
# ```python
# nb_employees = employee_salary.reduce(pw.reducers.avg(pw.this.salary))
# ```
#
# In Pathway, you cannot obtain the value as an int or a float as you could in SQL: you need to use `.ix_ref()`
#
# **In Pathway, we access the value of a single-row table using `singlerowtable.ix_ref()`.**
#
# As previously, `.ix_ref()` access the entire (single) row so you still need to specify the column.
# You access the average with `average_table.ix_ref().average_salary`.
#
# Let's see how it goes:

# %%
global_statistics = salaries.reduce(average_salary=pw.reducers.avg(pw.this.salary))
pw.debug.compute_and_print(global_statistics)
global_statistics = global_statistics.cast_to_types(average_salary=int)
results = (
    salaries.filter(pw.this.salary >= global_statistics.ix_ref().average_salary)
).reduce(count=pw.reducers.count())
pw.debug.compute_and_print(results)

# %% [markdown]
# That's it!
#
# You now have the number of employees with a higher salary than average, also contained in a single-row table.
#
# `ix_ref()` can be used to copy the value in all the rows of the table:

# %%
salaries_with_average = salaries.select(
    *pw.this, global_statistics.ix_ref().average_salary
)
pw.debug.compute_and_print(salaries_with_average)

# %% [markdown]
# ## Bonus: SQL version
# With Pathway's SQL API, you can directly query tables using SQL queries:

# %%
sql_result = pw.sql(
    "SELECT COUNT(*) AS count FROM salaries WHERE salary > (SELECT AVG(salary) FROM salaries)",
    salaries=salaries,
)

# %%
pw.debug.compute_and_print(sql_result)

# %% [markdown]
# Be careful when using the SQL API: it only supports subqueries on single-row tables.
# This subquery usage is an exception, and we strongly encourage you to use the Python syntax as much as possible!
