# ---
# title: Basic Data Types
# description: An article explaining how to use basic data types
# date: '2024-01-26'
# tags: ['tutorial', 'table']
# keywords: ['type', 'schema']
# notebook_export_path: notebooks/tutorials/basic_datatypes.ipynb
# jupyter:
#   jupytext:
#     formats: py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.16.0
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Basic Data Types
# This guide is about basic data types in Pathway: it covers the list of basic data types that can be used in Pathway, explores several available conversion methods, and wraps up with examples of operators that require a column of specific data type as input.

# Currently, Pathway allows using the following basic Python types: `bool`, `str`, `int`, `float`, and `bytes`. Additionally, there is support for types `datetime` and `duration` from `datetime` module, distinguishing between `utc datetimes` and `naive datetimes`. Finally, Pathway also introduces an additional type for columns representing pointers, called `Pointer`. Below, you can find an example table with six columns, one example column for each of the basic Python types, and one column of type Pointer. The complex types (as datetime) need some conversion, and they are covered in [the later part of the article](/developers/user-guide/connect/datatypes#temporal-data-types).
# The standard way to define a type in Pathway is to use a [`schema`](/developers/api-docs/pathway#pathway.Schema) (you can learn more about schemas in this [article](/developers/user-guide/connect/schema/)):
# %%
import datetime

import pathway as pw


class SimpleTypesInputSchema(pw.Schema):
    bool_column: bool
    str_column: str
    bytes_column: bytes
    int_column: int
    float_column: float

example_table = pw.debug.table_from_markdown(
    '''
      | bool_column | str_column | bytes_column | int_column | float_column
    1 | True        | example    | example      | 42         | 42.16
    2 | False       | text       | text         | 16         | -16.42
    ''', schema = SimpleTypesInputSchema
).with_columns(id_in_column = pw.this.id)

pw.debug.compute_and_print(example_table, include_id = False)
# %% [markdown]

# %%
print(example_table.schema)

# %% [markdown]
# ## Implicit Typing
# By default, you don't need to worry about they types of columns created with `select` or `with_columns`. The expressions used in those operators have defined output type, Pathway knows it, and assigns the types of new columns automatically.
# In the example below, the new column is of type `float`, as it is a result of multiplication of
# `int` column with a `float` constant.

# %%
example_table = pw.debug.table_from_markdown(
    '''
      | int_number
    1 | 16
    2 | 42
    '''
)
example_table += example_table.select(should_be_float_number = example_table.int_number*0.5)
pw.debug.compute_and_print(example_table, include_id=False)
# %% [markdown]
# As you can see, the type of int_number is `int`, and the new column is of type `float`.
# %%
print(example_table.schema)

# %% [markdown]
# Similarly, the special columns produced by some of the Pathway operators (examples in the later part of the article) have fixed types and as such, you don't need to bother with the types of those columns.

# ## Apply With Type
# Sometimes you may want to compute a value of a column, using e.g. a function from an external library, that does not define the output type explicitly. In this case, you can use either [`pw.apply`](/developers/api-docs/pathway#pathway.apply) or [`pw.apply_with_type`](/developers/api-docs/pathway#pathway.apply_with_type). The first creates a new column of type `any` and the other requires you to specify the type of the output of function that is applied. <!-- You can find examples of `pw.apply` or `pw.apply_with_type` in the remainder of this article, and in other dedicated article [link needed]. -->

# ## Data types for columns storing text and unstructured data
# In Pathway you can store unstructured data either as `str` or as `bytes`. Both can be converted to other data types, either by built in methods (some examples in this article) or by user defined functions (i.e. via [`pw.apply`](/developers/api-docs/pathway#pathway.apply) or [`pw.apply_with_type`](/developers/api-docs/pathway#pathway.apply_with_type)).

# ### Type `str`
# %%
class StrExampleInputSchema(pw.Schema):
    text: str

str_table = pw.debug.table_from_markdown(
    '''
      | text
    1 | cd
    2 | dd
    ''', schema = StrExampleInputSchema
)
# %% [markdown]
# Below is an example of conversion from `str` to `bytes`. Currently, there is no built-in conversion method. The recommended way is to use `apply_with_type`.
# %%
str_table = str_table.with_columns(text_as_bytes = pw.apply_with_type(lambda x: x.encode("utf8"), bytes, str_table.text))

pw.debug.compute_and_print(str_table, include_id=False)
# %% [markdown]

# %%
print(str_table.schema)
# %% [markdown]
# ### Module `str`
# Furthermore, Pathway provides a [string module](/developers/api-docs/pathway#pathway.internals.expressions.StringNamespace) containing string operations. Among other things, it provides several methods that allow parsing converting `str` to other simple types, accessible via the `str` namespace of column (e.g. `table_name.column_name.str.parse_*`). You can find examples of usage of those methods in the [remaining part of this article](/developers/user-guide/connect/datatypes#parse-numbers-from-str).

# ### Type `bytes`
# %%
class BytesExampleInputSchema(pw.Schema):
    bytes_from_markdown: bytes

bytes_table = pw.debug.table_from_markdown(
    '''
      | bytes_from_markdown
    1 | cd
    2 | dd
    ''', schema = BytesExampleInputSchema
)

# %% [markdown]
# Below is an example of conversion from `bytes` to `str`. Currently, there is no built-in conversion method. The recommended way is to use `apply_with_type`. Remark: the `to_string` function does not decode the bytes, but shows a string representation of byte numbers.
# %%
bytes_table = bytes_table.with_columns(
    text_from_bytes = pw.apply_with_type(lambda x: x.decode("utf8"), str, bytes_table.bytes_from_markdown),
    text_representation_of_bytes = bytes_table.bytes_from_markdown.to_string()
)

pw.debug.compute_and_print(bytes_table, include_id=False)
# %% [markdown]
#
# %%
print(bytes_table.schema)

# %% [markdown]
# ## Numerical Data Types
# Pathway supports operations on Python `int` and `float` types, and on their `numpy` counterparts. Below, you can find a few short examples that read and convert numbers in Pathway.
#
# ### Type `int`
# %%
class IntExampleInputSchema(pw.Schema):
    int_number: int

int_table = pw.debug.table_from_markdown(
        '''
    | int_number
  1 | 2
  2 | 3
  ''', schema = IntExampleInputSchema
)

# %% [markdown]
# Similarly, as in the conversion between `str` and `bytes`, you can use `apply_with_type` to convert a column of type `int` into a column of type `float`. Furthermore, it can be expressed in a more concise way, with `apply`. Moreover, in this case you can also use the built-in `cast` function. All mentioned examples can be found in the code snippet below:
# %%
int_table = int_table.with_columns(
    int_as_float = pw.apply_with_type(lambda x: float(x), float, int_table.int_number),
    int_as_float_via_constructor = pw.apply(float, int_table.int_number),
    int_as_float_casted = pw.cast(float, int_table.int_number)
)


pw.debug.compute_and_print(int_table, include_id=False)
# %% [markdown]

# %%
print(int_table.schema)

# %% [markdown]
# ### Type `float`
# %%
class FloatExampleInputSchema(pw.Schema):
    float_number: float
    another_float_number: float

float_table = pw.debug.table_from_markdown(
        '''
    | float_number | another_float_number
  1 | 2            | -5.7
  2 | 3            | 6.6
  ''', schema = FloatExampleInputSchema
)

# %% [markdown]
# As in the case of conversion from `int` to `float`, you can use `pw.cast` to convert data from type `float` to `int`.
# %%
float_table = float_table.with_columns(another_number_as_int = pw.cast(int, float_table.another_float_number))
print(float_table.schema)

# %% [markdown]
# ### Parse numbers from `str`
# Below, you can find an application of the parsing methods from the `str` namespace ([`parse_int`](/developers/api-docs/pathway#pathway.internals.expressions.StringNamespace.parse_int) and [`parse_float`](/developers/api-docs/pathway#pathway.internals.expressions.StringNamespace.parse_float)) to parse ints and floats for columns of type `str`.

# %%
class StrNumberExampleInputSchema(pw.Schema):
    number: str

str_number_table = pw.debug.table_from_markdown(
        '''
    | number
  1 | 2
  2 | 3
  ''', schema = StrNumberExampleInputSchema
)

str_number_table = str_number_table.with_columns(
    number_as_int = str_number_table.number.str.parse_int(),
    number_as_float = str_number_table.number.str.parse_float(),
    number_with_extra_text = str_number_table.number + "a"
)

pw.debug.compute_and_print(str_number_table)

# %% [markdown]
# As you can see, the schema shows that the original column was of type `str`, and each new column has a different type, as expected.

# %%
print(str_number_table.schema)

# %% [markdown]
# ### Numerical Module
# In case you need to use some basic operations on columns of numerical type, Pathway provides a [module](/developers/api-docs/pathway#pathway.internals.expressions.NumericalNamespace) containing functions over numerical data types such as [`abs`](/developers/api-docs/pathway#pathway.internals.expressions.NumericalNamespace.abs) or [`round`](/developers/api-docs/pathway#pathway.internals.expressions.NumericalNamespace.round).

# %% [markdown]
# ## Temporal Data Types
# In Pathway, temporal data types (`datetime.datetime`) are complex data types with some representation as some simple type (as `int` or `str`). As such, you first need to load the input as simple type, and only then convert it to temporal type.
# Similarly to Python, Pathway distinguishes between [naive datetime](/developers/api-docs/pathway#pathway.DateTimeNaive) (not aware of timezones) and [UTC datetime](/developers/api-docs/pathway#pathway.DateTimeUtc) (aware of time zones).
# Below, you can find examples of reading both kinds of datetime, initially provided as `str` and `int`, using methods from the Pathway [`dt` module](/developers/api-docs/pathway#pathway.internals.expressions.DateTimeNamespace):

# %%
class DatetimeNaiveExampleInputSchema(pw.Schema):
    t1: str
    t2: int

naive_datetime = pw.debug.table_from_markdown(
        """
      |         t1          |      t2
    0 | 2023-05-15T10:13:00 | 1684138380000
    """, schema = DatetimeNaiveExampleInputSchema
)
fmt = "%Y-%m-%dT%H:%M:%S"
naive_datetime = naive_datetime.with_columns(
    dt1 = naive_datetime.t1.dt.strptime(fmt=fmt),
    dt2 = naive_datetime.t2.dt.from_timestamp("ms")
)

naive_datetime = naive_datetime.with_columns(
    difference = naive_datetime.dt1 - naive_datetime.dt2
)

pw.debug.compute_and_print(naive_datetime)

print(naive_datetime.schema)
# %% [markdown]

# %%
utc_datetime = pw.debug.table_from_markdown(
        """
      |         t1                |      t2
    0 | 2023-05-15T10:13:00+01:00 | 1684138380000
    """, schema = DatetimeNaiveExampleInputSchema
)

fmt = "%Y-%m-%dT%H:%M:%S%z"
utc_datetime = utc_datetime.with_columns(
    dt1 = utc_datetime.t1.dt.strptime(fmt=fmt),
    dt2 = utc_datetime.t2.dt.utc_from_timestamp("ms")
)

utc_datetime = utc_datetime.with_columns(
    difference = utc_datetime.dt1 - utc_datetime.dt2
)

pw.debug.compute_and_print(utc_datetime)

print(utc_datetime.schema)

# %% [markdown]
# ## Type `bool`
# Below, you can find a piece of code reading and converting boolean data.
# %%
class BoolExampleInputSchema(pw.Schema):
    boolean_column: bool

bool_table = pw.debug.table_from_markdown(
        '''
    | boolean_column
  1 | True
  2 | False
  ''', schema = BoolExampleInputSchema
)


bool_table = bool_table.with_columns(bool_as_str = bool_table.boolean_column.to_string())
bool_table = bool_table.with_columns(bool_as_str_as_bool_parse = bool_table.bool_as_str.str.parse_bool())
pw.debug.compute_and_print(bool_table, include_id=False)
print(bool_table.schema)

# %% [markdown]
# Warning: please do not use cast to convert boolean data type. While it is possible to call it, its behavior is counterintuitive and will be deprecated. Below, we demonstrate the odd behavior.
# %%
bool_table = bool_table.with_columns(bool_as_str_as_bool_cast = pw.cast(bool, bool_table.bool_as_str))
pw.debug.compute_and_print(bool_table, include_id=False)
print(bool_table.schema)

# %% [markdown]
# ## Optional Data Types
# Sometimes, you don't have a guarantee that the data is always present. To accommodate for such columns, Pathway provides support for the `Optional` data type. More precisely, whenever you expect the column to have values of type `T`, but not necessarily always present, the type of Pathway column to store this data should be `Optional[T]` which can also be denoted as `T | None`. Below, you can find a short example of the column with optional floats and two conversion methods.
# %%
class OptInputSchema(pw.Schema):
    opt_float_num: float | None

t = pw.debug.table_from_markdown(
    """
    | opt_float_num
1   | 1
2   | 2
3   | None
""",
    schema=OptInputSchema,
)

pw.debug.compute_and_print(t, include_id=False)
print(t.schema)

# %% [markdown]
# To obtain a column with a non-optional type, you can filter the non-empty values using `filter` and `is_not_none`:
# %%
t1 = t.filter(t.opt_float_num.is_not_none()).rename_columns(float_num = t.opt_float_num)
pw.debug.compute_and_print(t1, include_id=False)
print(t1.schema)
# %% [markdown]
# The more general way of making the type non-optional is via `unwrap`. The code below is equivalent to the application of `filter` and `is_not_none()` above.
# %%

t2 = t.filter(t.opt_float_num != None)
t2 = t2.with_columns(float_num = pw.unwrap(t2.opt_float_num)).without(t2.opt_float_num)
pw.debug.compute_and_print(t2, include_id=False)
print(t2.schema)

# %% [markdown]
# ## Operators with Type Constraints
# Pathway provides several operators requiring input columns to have specific types. The input types are constrained because the functions are not defined for all types, e.g., temporal operators require time-like input columns, sort operator requires data to be sortable, and `diff` requires that we can subtract two elements of considered type.

# ### Temporal operators
# An example of a temporal operator is the `windowby` operator. Its first argument is `time_expr` - the operator uses this column to store time associated with each row and then uses it according to window type and temporal behavior defined in other parameters. Since this column is supposed to represent time, we accept the types `int`, `float`, `datetime`, as they can be reasonably used to do so. In the example below, the `windowby` operator uses a column with naive `datetime`.
# %%
fmt = "%Y-%m-%dT%H:%M:%S"

table = pw.debug.table_from_markdown(
    """
    | time                  | number
 0  | 2023-06-22T09:12:34   | 2
 1  | 2023-06-22T09:23:56   | 2
 2  | 2023-06-22T09:45:20   | 1
 3  | 2023-06-22T09:06:30   | 1
 4  | 2023-06-22T10:11:42   | 2
"""
).with_columns(time=pw.this.time.dt.strptime(fmt))

result = table.windowby(
    table.time,
    window=pw.temporal.tumbling(duration=datetime.timedelta(minutes=30)),
).reduce(
    window_start = pw.this._pw_window_start,
    chocolate_bars=pw.reducers.sum(pw.this.number),
)

pw.debug.compute_and_print(result, include_id=False)

# %% [markdown]
# ### Sorting Operator
# Another example of an operator that accepts type-constrained columns is `sort`. It requires that the values in the column can be sorted (i.e., the column has type with total order). Currently, it can be used with all simple types, however please take into account that comparing elements of type `str` or `bytes` may be slow, so it's generally not recommended.
# %%
table_to_sort = pw.debug.table_from_markdown('''
    value  | value_str
    1      | de
    2      | fg
    3      | cd
    4      | ab
    5      | ef
    6      | bc
''')

sorted_by_value = table_to_sort.sort(table_to_sort.value) + table_to_sort
print(sorted_by_value.schema)

# %% [markdown]

# %%
sorted_by_value_str = table_to_sort.sort(table_to_sort.value_str) + table_to_sort
print(sorted_by_value_str.schema)

# %% [markdown]
# ### Diff
# Below are a few examples demonstrating the `diff` operator. Essentially, it sorts the table with respect to one column, and then, for each row and some other column, it subtracts the previous value from the current value. As such, it has two types of constrained columns, one with constraints for the `sort` operator, and the other requires that we can subtract the elements. Currently, among simple types, the subtraction can be done on elements of type `int`, `float` and `datetime`.

# %%
table = pw.debug.table_from_markdown('''
    timestamp | values | values_str
    1         | 1      | fg
    2         | 2      | ef
    3         | 4      | de
    4         | 7      | cd
    5         | 11     | bc
    6         | 16     | ab
    ''')
table1 = table + table.diff(pw.this.timestamp, pw.this.values)
print(table1.schema)
pw.debug.compute_and_print(table1, include_id=False)


table = table.with_columns(date = table.values.dt.from_timestamp("ms"))

table2 = table + table.diff(pw.this.timestamp, pw.this.date)
print(table2.schema)
pw.debug.compute_and_print(table2, include_id=False)

table3 = table + table.diff(pw.this.values_str, pw.this.values)
print(table3.schema)
pw.debug.compute_and_print(table3, include_id=False)

# %% [markdown]
# In particular, calling `diff` on elements from `values_str`, which cannot be subtracted, causes the following error:
# ```
# TypeError: Pathway does not support using binary operator sub on columns of types <class 'str'>, <class 'str'>.
# ```



