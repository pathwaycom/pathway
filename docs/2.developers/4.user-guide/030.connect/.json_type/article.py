# ---
# title: Dealing with JSON Data Type
# description: An article explaining how to use JSON in Pathway
# date: '2023-12-22'
# thumbnail: '/assets/content/blog/th-json.png'
# tags: ['tutorial', 'table']
# keywords: ['JSON', 'type', 'schema']
# notebook_export_path: notebooks/tutorials/json_type.ipynb
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
# # Handling JSON in Pathway
#
# JSON is a widely used format for data interchange due to its simplicity and readability. Upon finishing this article, managing JSON in Pathway should become effortlessly intuitive.
#
# As an example, we'll use JSON objects loaded directly from python list. However, JSON data can come from various sources that support this format, such as [Kafka](/developers/user-guide/connect/connectors/kafka_connectors) or an [HTTP connector](/developers/api-docs/pathway-io/http#pathway.io.http.rest_connector).
# %%
rows = [
    (
        1,
        {
            "author": {"id": 1, "name": "Haruki Murakami"},
            "books": [
                {"title": "Norwegian Wood", "year": 1987},
                {
                    "title": "Kafka on the Shore",
                    "year": 2002,
                    "category": "Literary Fiction",
                },
            ],
        },
    ),
    (
        2,
        {
            "author": {"id": 2, "name": "Stanisław Lem"},
            "books": [
                {"title": "Solaris", "year": 1961, "category": "Science Fiction"},
                {"title": "The Cyberiad", "year": 1967, "category": "Science Fiction"},
            ],
        },
    ),
    (
        3,
        {
            "author": {"id": 3, "name": "William Shakespeare"},
            "books": [
                {"title": "Hamlet", "year": 1603, "category": "Tragedy"},
                {"title": "Macbeth", "year": 1623, "category": "Tragedy"},
            ],
        },
    ),
]

# _MD_COMMENT_START_
import logging

# %% [markdown]
# Each JSON object carries information about an author and their associated books. To load it, let's establish a [schema](/developers/user-guide/connect/schema#understanding-data-types-and-schemas) reflecting the data's structure and then proceed to load this data into a table.
# %%
import pathway as pw

logging.basicConfig(level=logging.CRITICAL)
# _MD_COMMENT_END_


class InputSchema(pw.Schema):
    key: int
    data: pw.Json


table = pw.debug.table_from_rows(schema=InputSchema, rows=rows)

# _MD_COMMENT_START_
pw.debug.compute_and_print(table)
# _MD_COMMENT_END_
# _MD_SHOW_table


# %% [markdown]
# Pathway enables manipulation of JSON from two perspectives: expressions and [user-defined functions](/developers/api-docs/pathway#pathway.udf). Let's examine each one separately.

# %% [markdown]
# ## Working with JSONs using expressions
#
# ### Accessing JSON fields
#
# A column of type [`pw.Json`](/developers/api-docs/pathway#pathway.Json) enables access to its attributes using the index operator (`[]`). This operator accepts an index in the form of a string for JSON objects, an integer for JSON arrays, or an expression evaluating to one of these types. If there's no element at the index or if the value is `pw.Json.NULL`, it returns `pw.Json.NULL`, making this operator convenient for chaining.

# %%
books = table.select(author=pw.this.data["author"]["name"], books=pw.this.data["books"])
# _MD_COMMENT_START_
pw.debug.compute_and_print(books)
# _MD_COMMENT_END_
# _MD_SHOW_books


# %% [markdown]
# Alternatively, a `get()` method can be used to access `JSON` attributes. This method allows defining a custom default value.

# %%
sample = table.select(
    author=pw.this.data["author"]["name"],
    title=pw.this.data["books"][0]["title"],
    category=pw.this.data["books"][0].get("category", default=pw.Json("Uncategorized")),
)
# _MD_COMMENT_START_
pw.debug.compute_and_print(sample)
# _MD_COMMENT_END_
# _MD_SHOW_sample


# %% [markdown]
# ### Converting to simple types
#
# `JSON` column can be converted into `Optional[T]` where `T` is one of the simple types, using methods: [`as_int()`](/developers/api-docs/pathway#pathway.ColumnExpression.as_int), [`as_str()`](/developers/api-docs/pathway#pathway.ColumnExpression.as_str), [`as_float()`](/developers/api-docs/pathway#pathway.ColumnExpression.as_float), [`as_bool()`](/developers/api-docs/pathway#pathway.ColumnExpression.as_bool).

# %%
books.select(author=pw.unwrap(pw.this.author.as_str()).str.upper())
# _MD_COMMENT_START_
pw.debug.compute_and_print(
    books.select(author=pw.unwrap(pw.this.author.as_str()).str.upper())
)
# _MD_COMMENT_END_

# %% [markdown]
# ### Flatten
#
# You can utilize the [`flatten()`](/developers/api-docs/pathway-table#pathway.internals.table.Table.flatten) operator specifically on columns that contain JSON arrays. It's a useful tool when working with complex JSON structures.

# %%
flat_list = books.flatten(pw.this.books)
# _MD_COMMENT_START_
pw.debug.compute_and_print(flat_list)
# _MD_COMMENT_END_
# _MD_SHOW_flat_list

# %% [markdown]
# ## JSON in UDFs


# %% [markdown]
# Pathway enables manipulation of JSON using [user-defined functions](/developers/api-docs/pathway#pathway.udf). Just like with expressions, the index operator (`[]`) and methods allowing conversion into specific types are available.  It's crucial to note that this conversion is strict— attempting to convert incompatible data will result in an exception.


# %%
@pw.udf
def transform(data: pw.Json) -> pw.Json:
    return {"century": (data["year"].as_int()) // 100 + 1, **data.as_dict()}


# _MD_COMMENT_START_
pw.debug.compute_and_print(
    flat_list.select(title=pw.this.books["title"], metadata=transform(pw.this.books))
)
# _MD_COMMENT_END_
# _MD_SHOW_flat_list.select(title=pw.this.books["title"], metadata=transform(pw.this.books))

# %% [markdown]
# Further details about `pw.Json` functionality are available in the dedicated [API documentation](/developers/api-docs/pathway#pathway.Json).
