# ---
# title: Indexes in Pathway
# description: An article explaining ways of indexing data in Pathway.
# date: '2023-11-15'
# thumbnail: '/assets/content/blog/th-computing-pagerank.png'
# tags: ['tutorial', 'engineering']
# keywords: ['index', 'indexing', 'join', 'asof join', 'asof_now', 'KNN']
# notebook_export_path: notebooks/tutorials/indexes.ipynb
# ---

# %% [markdown]
# # Indexes in Pathway
# In this article, you'll learn about reactive indexes in Pathway and how they differ from conventional indexes used in databases. You'll also see how to use them to respond to a stream of queries in real time.
#
# Indexes are data structures that improve the speed of queries. They are often used in databases. They are helpful if you want to retrieve records with a specific value in a given column (then you need an index based on this column). An example of this is answering a stream of queries using the contents of a database.
#
# ![LSM index context drawing](assets/content/tutorials/indexes/index.svg)
#
# Indexes can also speed up joins - an existing index can be used if it is built on appropriate columns but also an index can be built ad-hoc, during query execution.
# Pathway offers indexes, but because it operates on streams, there are some differences as compared to database indexes. To learn about them, continue reading the article.

# %% [markdown]
# ## Joins
# Pathway operates on streams. Unless it is informed otherwise, it assumes that new data can arrive from any stream. Thus, when joining two streams, Pathway has to keep these streams in memory. It builds [LSM trees](https://en.wikipedia.org/wiki/Log-structured_merge-tree) on both sides of a join. Thanks to that, new records arriving in any of the two streams can be joined quickly - it is enough to look them up in the index of the other table and no costly scans are needed.
# In contrast, normal databases only use an index on one side of a join because once the query is processed the join results are not updated.
#
# Let's consider a simple example in which you join two tables in Pathway. Here, a table is built from a simulated stream of changes to its rows. The value in the `__time__` column represents the arrival time of the record to the engine. Rows with the same value in the `__time__` column belong to a single batch.
#
# To use an example with a real streaming source it is enough to replace [`pw.debug.table_from_markdown`](/developers/api-docs/debug#pathway.debug.table_from_markdown) with an appropriate [connector](/developers/user-guide/connect/supported-data-sources/) (like Redpanda or Kafka connector).
#
# The tables are joined on the `instance` column.

# %%
import pathway as pw

table_a = pw.debug.table_from_markdown(
    """
    value | instance | __time__
      1   |    1     |     2
      2   |    1     |     6
      3   |    2     |     8
      4   |    2     |    12
    """
)
table_b = pw.debug.table_from_markdown(
    """
    value | instance | __time__
      11  |    1     |     4
      12  |    2     |     6
      13  |    1     |     8
    """
)

result = table_a.join(table_b, pw.left.instance == pw.right.instance).select(
    left_value=pw.left.value, right_value=pw.right.value, instance=pw.this.instance
)

pw.debug.compute_and_print(result)

# %% [markdown]
# As you can see, the records from both sides get joined with the future records. It is expected, as Pathway incrementally updates all results to match the input data changes. However, if `table_a` would be `queries` on a `table_b` representing the `data` you want to query, you'd be surprised to see that answers to your queries are updated in the future when `data` changes. Let's say, you want to query the number of your website visits by location:

# %%
import pathway as pw

queries = pw.debug.table_from_markdown(
    """
    query_id |  country  | __time__
        1    |   France  |     4
        2    |   Poland  |     6
        3    |  Germany  |     8
        4    |      USA  |    14
    """
)
visits = pw.debug.table_from_markdown(
    """
     country | __time__
      Poland |    2
      France |    2
      Spain  |    2
      Poland |    2
      France |    4
         USA |    4
         USA |    4
     Germany |    6
         USA |    6
         USA |    8
      Poland |    8
      France |    8
      France |   12
     Germany |   14
    """
)
total_visits_by_country = visits.groupby(pw.this.country).reduce(
    pw.this.country, visits=pw.reducers.count()
)

answers = queries.join(
    total_visits_by_country, pw.left.country == pw.right.country
).select(pw.left.query_id, pw.this.country, pw.right.visits)

pw.debug.compute_and_print(answers)

# %% [markdown]
# Please note how the answer to your query with `query_no=1` is updated a few times. At first, it is equal to `2`. At time `8`, it changes to `3` and finally is equal to `4` (starting from time `12`). It may be a bit surprising if you're new to Pathway. It turns out, the `join` allows you to keep track of the updates! And it has many cool uses, for instance alerting. You can use it to set up a real-time alerting system. However, if that is not what you want and you'd like to get an answer to your query once, at its processing time, Pathway supports it as well!

# %% [markdown]
# ## Asof now join
# Monitoring changes of answers to your queries might not be what you want. Especially if you have **a lot of** queries. If you want to get an answer for a query once, and then forget it, you can use [`asof_now_join`](/developers/api-docs/temporal#pathway.stdlib.temporal.asof_now_join). Its left side is a queries table and the right side is the data you want to query. Note that the right side is still a table dynamically streaming row changes. You can update it but the updates will only affect future queries - no old answers will be updated.
#
# Let's see what `asof_now_join` would return in the example above:

# %%
import pathway as pw

queries = pw.debug.table_from_markdown(
    """
    query_id |  country  | __time__
        1    |   France  |     4
        2    |   Poland  |     6
        3    |  Germany  |     8
        4    |      USA  |    14
    """
)
visits = pw.debug.table_from_markdown(
    """
     country | __time__
      Poland |    2
      France |    2
      Spain  |    2
      Poland |    2
      France |    4
         USA |    4
         USA |    4
     Germany |    6
         USA |    6
         USA |    8
      Poland |    8
      France |    8
      France |   12
     Germany |   14
    """
)
total_visits_by_country = visits.groupby(pw.this.country).reduce(
    pw.this.country, visits=pw.reducers.count()
)

answers = queries.asof_now_join(
    total_visits_by_country, pw.left.country == pw.right.country
).select(pw.left.query_id, pw.this.country, pw.right.visits)

pw.debug.compute_and_print(answers)

# %% [markdown]
# This time the answers to queries are not updated. In particular, the answer to the query with `query_no=1` is set to `2` at time `4` and remains equal to `2` until the end of the program.
#
# In contrast to an ordinary [`join`](/developers/user-guide/data-transformation/join-manual), `asof_now_join` is not symmetric. New rows on the left side of the join will produce a result under the condition they can be joined with at least one row from the right side. If you want to produce at least one result from every query, you can use `asof_now_join_left` - then all columns from the right side in the output row will be set to `None`. On the other hand, new rows on the right side of the join won't immediately produce any new rows in the output but will update the index and if they're matched with new records from the left side later, they will appear in the output.
#
# Please note that for a correct operation, the left table of the `asof_now_join` (`queries`) can only be extended with new queries. Pathway verifies it for you. You can't delete or update the queries. It is quite reasonable. Instead of updating the query, you can just send a new query because your previous query has been already forgotten anyway.
#
# Another important thing is that `asof_now_join` depends on the processing time and because of that is non-deterministic. When sending data to a distributed system like pathway, you don't have a guarantee which data will enter the system first (network latencies, etc.). If both queries and data streams are updated at the same time, the amount of data that is already present in the system when a given query is answered is non-deterministic. Thus if you repeat the same experiment with real connectors (like Redpanda or Kafka), you may get different answers in different runs. If you used an ordinary `join`, you would always get the same answers in the end because the answers would be updated with the arrival of the new data.
#
# The differences between an ordinary `join` and `asof_now_join` used to answer the queries are summarized in the table below. Let's denote the left side of the join as "queries" and the right side as "data".
#
# | `join` | `asof_now_join` |
# |--------|-----------------|
# | updates the answers to old queries | doesn't update the answers to old queries |
# | keeps the queries | forgets the queries |
# | consumes memory (approx.) proportional to the sum of sizes of the queries and data tables | consumes memory (approx.) proportional to the size of the data table |
# | final output deterministic | final output non-deterministic |


# %% [markdown]
# ## KNN Index
# An approximate [K Nearest Neighbors (KNN) Index](/developers/templates/etl/lsh/lsh_chapter1) behaves similarly to a join. The default method [`get_nearest_items`](/developers/api-docs/ml#pathway.stdlib.ml.index.KNNIndex.get_nearest_items) maintains always up-to-date answers to all queries when the set of indexed documents changes. In fact, it uses a join under the hood.
#
# If you don't want answers to your queries to be updated, you can use [`get_nearest_items_asof_now`](/developers/api-docs/ml#pathway.stdlib.ml.index.KNNIndex.get_nearest_items_asof_now) (experimental). It'll return the closest points once and will forget the query. However, it'll monitor the stream containing index data and update the index if new data arrives (but won't update old queries). As a result, if you ask the same query again and the index has changed in the meantime, you can get a different answer. This behavior is used in our [llm-app](/developers/user-guide/llm-xpack/llm-app-pathway/) to answer queries using an always up-to-date index of documents.
#
# To understand better the differences between the methods, you can analyze the examples below. In the first example, `index.get_nearest_items` is used which leads to updating the answers. In the second example, `index.get_nearest_items_asof_now` is used and thus the answers are not updated.
# Streams of changes to tables are simulated using `pw.debug.table_from_markdown` which, as earlier, uses `__time__` column to split the data into batches.

# %%
import pathway as pw
from pathway.stdlib.ml.index import KNNIndex

queries = pw.debug.table_from_markdown(
    """
    query_id |  x |  y | __time__
        1    |  0 |  0 |    4
        2    |  2 | -2 |    6
        3    | -1 |  1 |    8
        4    | -2 | -3 |    10
    """
).select(pw.this.query_id, coords=pw.make_tuple(pw.this.x, pw.this.y))

data = pw.debug.table_from_markdown(
    """
    point_id |  x |  y | __time__
        A    |  2 |  2 |    2
        B    |  3 | -2 |    2
        C    | -1 |  0 |    6
        D    |  1 |  2 |    8
        E    | -3 |  1 |   10
        F    |  1 | -4 |   12
    """
).select(pw.this.point_id, coords=pw.make_tuple(pw.this.x, pw.this.y))

index = KNNIndex(data.coords, data, n_dimensions=2, n_and=5)
result_with_updates = queries + index.get_nearest_items(queries.coords, k=2).select(
    nn_ids=pw.this.point_id, nns=pw.this.coords
)
pw.debug.compute_and_print(result_with_updates)

# %% [markdown]
# The example below uses the same streams but does not update the answers to the queries. Note the difference between the methods used - `get_nearest_items` vs `get_nearest_items_asof_now`.
# %%
index = KNNIndex(data.coords, data, n_dimensions=2, n_and=5)
result_asof_now = queries + index.get_nearest_items_asof_now(
    queries.coords, k=2
).select(nn_ids=pw.this.point_id, nns=pw.this.coords)
pw.debug.compute_and_print(result_asof_now)

# %% [markdown]
# ![Points and queries in knn example](assets/content/tutorials/indexes/knn.svg)
# Note the differences between two cases. For instance, for query with `query_id=2`, points returned by `get_nearest_items_asof_now` were B and C because other closer points were not available at the query time. On the other hand, `get_nearest_items` updated the results and they have changed to B, F.
#
# In the example above, 2-dimensional vectors were used to make the analysis simpler. The **llm-app** uses n-dimensional vectors but the general principle doesn't change.

# %% [markdown]
# ## Applications of `asof_now` indexes to data read using HTTP REST connector
#
# If you want a more practical example, we can set up a webserver that answers queries about `k` nearest neighbors. The architecture of the app is presented on the diagram below:
#
# ![KNN app architecture](assets/content/tutorials/indexes/rest_knn.svg)
#
# First you have to make necessary imports and define a schema of the data (we will use the same schema for data na queries).
# %%
import pathway as pw
from pathway.stdlib.ml.index import KNNIndex


class PointSchema(pw.Schema):
    x: int
    y: int


# To receive the queries, you can use the [`rest_connector`](/developers/api-docs/pathway-io/http#pathway.io.http.rest_connector).
# %%
host = "0.0.0.0"  # set as needed
port = 8080  # set as needed
queries, response_writer = pw.io.http.rest_connector(
    host=host,
    port=port,
    schema=PointSchema,
    autocommit_duration_ms=50,
    delete_completed_queries=False,
)
# %% [markdown]
# The database is represented as a directory containing CSV files with points from which you want to find closest ones. You can use a [CSV connector](/developers/user-guide/connect/connectors/csv_connectors) to read a directory of CSV files. The `mode` parameter is set to `streaming`. If new files appear in the directory, their entries will be appended to the stream and hence will be added to the KNN index.
# %%
data_dir = "points"
data = pw.io.csv.read(
    data_dir,
    schema=PointSchema,
    mode="streaming",
    autocommit_duration_ms=500,
)
# %% [markdown]
# To build the index you need to turn point coordinates into a vector:
# %%
data = data.select(coords=pw.make_tuple(pw.this.x, pw.this.y))
index = KNNIndex(data.coords, data, n_dimensions=2, n_and=5)
# %% [markdown]
# To answer the queries, you can use the `get_nearest_items_asof_now` method of the index. It'll find the nearest points but won't update the queries in the future.
# %%
queries = queries.select(coords=pw.make_tuple(pw.this.x, pw.this.y))
result = queries + index.get_nearest_items_asof_now(queries.coords, k=2).select(
    result=pw.this.coords
)
# %% [markdown]
# To send back query response to the user, you should use `response_writer` returned by the [`rest_connector`](/developers/api-docs/pathway-io/http#pathway.io.http.rest_connector)
# %%
response_writer(result)
# %% [markdown]
# Firstly, let's populate the directory with a file containing some points. To do that you can run in your terminal:
# %% [markdown]
# ```shell script
# mkdir points && echo -e "x,y\n2,2\n-2,2\n0,6\n2,8" > points/a.csv
# ```
# %% [markdown]
# Now you can start the computation:
# %% [markdown]
# ```python
# pw.run()
# ```
# %% [markdown]
# It is most convenient to copy the whole code and run it as a standalone script:
# %% [markdown]
# ```python
# import pathway as pw
# from pathway.stdlib.ml.index import KNNIndex
# import argparse
#
#
# class PointSchema(pw.Schema):
#     x: int
#     y: int
#
#
# def run(data_dir: str, host: str, port: int):
#     queries, response_writer = pw.io.http.rest_connector(
#         host=host,
#         port=port,
#         schema=PointSchema,
#         autocommit_duration_ms=50,
#         delete_completed_queries=False,
#     )
#     data = pw.io.csv.read(
#         data_dir,
#         schema=PointSchema,
#         mode="streaming",
#         autocommit_duration_ms=500,
#     )
#     data = data.select(coords=pw.make_tuple(pw.this.x, pw.this.y))
#
#     index = KNNIndex(data.coords, data, n_dimensions=2, n_and=5)
#
#     queries = queries.select(coords=pw.make_tuple(pw.this.x, pw.this.y))
#
#     result = queries + index.get_nearest_items_asof_now(queries.coords, k=2).select(
#         result=pw.this.coords
#     )
#
#     response_writer(result)
#
#     pw.run()
#
#
# if __name__ == "__main__":
#     parser = argparse.ArgumentParser()
#     parser.add_argument("data_dir")
#     parser.add_argument("--host", default="0.0.0.0")
#     parser.add_argument("--port", type=int, default=8080)
#     args = parser.parse_args()
#     run(data_dir=args.data_dir, host=args.host, port=args.port)
# ```
# %% [markdown]
# Now you can send a request to the webserver. You can play with it and try different points. You can use
# %% [markdown]
# ```shell script
# curl --data '{"x": 2, "y": -2}' http://localhost:8080/
# ```

# %% [markdown]
# You can also add new points to the index by adding a new file to a directory:
# %% [markdown]
# ```shell script
# echo -e "x,y\n-3,1\n1,-4" > points/b.csv
# ````
# %% [markdown]
# Let's repeat the first query.
# %% [markdown]
# ```shell script
# curl --data '{"x": 2, "y": -2}' http://localhost:8080/
# ```
# %% [markdown]
# The result has changed which indicates that the index has been updated.
#
# In this case, you didn't need to keep the queries. However, if you wanted to alert your users (for example via email) that answers to their queries have changed, you could use the normal `get_nearest_items` method instead of `get_nearest_items_asof_now` and keep watching the changes in the output stream. To not complicate the example, instead of sending an email, we print a message to the console.
# To intercept messages in a stream, you can use the [`pw.io.subscribe`](/developers/api-docs/pathway-io#pathway.io.subscribe) method. The function you provide, is called on every row of the stream. It'll also include deletion rows and that's why we check if `is_addition` argument is true before printing (an update in a stream consists of two records - a deletion record with the content of the old row and an addition record with a content of the new row).
# The code below is a slight modification of the code answering queries. You can run and experiment with it in the same way as you did with the code above.
# %% [markdown]
# ```python
# import pathway as pw
# from pathway.stdlib.ml.index import KNNIndex
# import argparse
# import logging
#
#
# class PointSchema(pw.Schema):
#     x: int
#     y: int
#
#
# def run(data_dir: str, host: str, port: int):
#     queries, response_writer = pw.io.http.rest_connector(
#         host=host,
#         port=port,
#         schema=PointSchema,
#         autocommit_duration_ms=50,
#         delete_completed_queries=False,
#     )
#     data = pw.io.csv.read(
#         data_dir,
#         schema=PointSchema,
#         mode="streaming",
#         autocommit_duration_ms=500,
#     )
#     data = data.select(coords=pw.make_tuple(pw.this.x, pw.this.y))
#
#     index = KNNIndex(data.coords, data, n_dimensions=2, n_and=5)
#
#     queries = queries.select(coords=pw.make_tuple(pw.this.x, pw.this.y))
#
#     result = queries + index.get_nearest_items(queries.coords, k=2).select(
#         result=pw.this.coords
#     )
#
#     response_writer(result)
#
#     def on_change(key, row, time, is_addition):
#         if is_addition:
#             query_coords = row["coords"]
#             result_coords = row["result"]
#             logging.info(
#                 f"The current answer to query {query_coords} is {result_coords}."
#             )
#
#     pw.io.subscribe(result, on_change)
#
#     pw.run()
#
#
# if __name__ == "__main__":
#     parser = argparse.ArgumentParser()
#     parser.add_argument("data_dir")
#     parser.add_argument("--host", default="0.0.0.0")
#     parser.add_argument("--port", type=int, default=8080)
#     args = parser.parse_args()
#     run(data_dir=args.data_dir, host=args.host, port=args.port)
# ```
# %% [markdown]
# A similar approach was taken in our [alerting example](https://github.com/pathwaycom/llm-app/tree/main/examples/pipelines/drive_alert).
# It is an LLM app that can send you alerts on slack when the response to your query has changed significantly.
# %% [markdown]
# ## Summary
# In this article you learned about the differences in indexing between databases and Pathway. You can see that both approaches - keeping the queries to update them in the future or forgetting queries immediately after answering, are useful. It depends on your objective which approach should be used. Pathway provides methods to handle both variants.
