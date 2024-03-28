# Changelog

All notable changes to this project will be documented in this file.

This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
## [Unreleased]

### Added
- `pw.io.bigquery.write` connector for writing Pathway tables into Google BigQuery.

### Changed
- `pw.io.postgres.write` can now parallelize writes to several threads if several workers are configured.

## [0.8.5] - 2024-03-27

### Added
- New function `answer_with_geometric_rag_strategy_from_index`, which allows to use `answer_with_geometric_rag_strategy` without the need to first retrieve documents from index.
- Added support for custom state serialization to `udf_reducer`.
- Introduced `instance` parameter in `AsyncTransformer`. All calls with a given `(instance, processing_time)` pair are returned at the same processing time. Ordering is preserved within a single instance.
- Added `successful`, `failed`, `finished` properties to `AsyncTransformer`. They return tables with successful calls, failed calls and all finished calls, respectively.

### Changed
- Property `result` of `AsyncTransformer` is deprecated. Property `successful` should be used instead.
- `pw.io.csv.read`, `pw.io.jsonlines.read`, `pw.io.fs.read`, `pw.io.plaintext.read` now handle `path` as a glob pattern and read all matched files and directories recursively.

## [0.8.4] - 2024-03-18

### Fixed
- Pathway will only require `LiteLLM` package, if you use one of the wrappers for `LiteLLM`.
- Retries are implemented in `pw.io.airbyte.read`.
- State processing protocol is updated in `pw.io.airbyte.read`.

## [0.8.3] - 2024-03-13

### Added
- New parameters of `pw.UDF` class and `pw.udf` decorator: `return_type`, `deterministic`, `propagate_none`, `executor`, `cache_strategy`.
- The LLM Xpack now provides integrations with LlamaIndex and LangChain for running the Pathway VectorStore server.

### Changed
- Subclassing `UDFSync` and `UDFAsync` is deprecated. `UDF` should be subclassed to create a new UDF.
- Passing keyword arguments to `pw.apply`, `pw.apply_with_type`, `pw.apply_async` is deprecated. In the future, they'll be used for configuration, not passing data to the function.

### Fixed
- Fixed a minor bug with `Table.groupby()` method which sometimes prevented of accessing certain columns in the following `reduce()`.
- Fixed warnings from using OpenAI Async embedding model in the VectorStore in Colab.

## [0.8.2] - 2024-02-28

### Added
- `%:z` timezone format code to `strptime`.
- Support for Airbyte connectors `pw.io.airbyte`.

## [0.8.1] - 2024-02-15

### Added
- Introduced the `send_alerts` function in the `pw.io.slack` namespace, enabling users to send messages from a specified column directly to a Slack channel.
- Enhanced the `pw.io.http.rest_connector` by introducing an additional argument called `request_validator`. This feature empowers users to validate payloads and raise an `HTTP 400` error if necessary.

### Fixed
- Addressed an issue in `pw.io.xpacks.llm.VectorStoreServer` where the computation of the last modification timestamp for an indexed document was incorrect.

### Changed
- Improved the behavior of `pw.io.kafka.write`. It now includes retries when sending data to the output topic encounters failures.

## [0.8.0] - 2024-02-01

### Added
- `pw.io.http.rest_connector` now supports multiple HTTP request types.
- `pw.io.http.PathwayWebserver` now allows Cross-Origin Resource Sharing (CORS) to be enabled on newly added endpoints
- Wrappers for LiteLLM and HuggingFace chat services and SentenceTransformers embedding service are now added to Pathway xpack for LLMs.

### Changed
- `pw.run` now includes an additional parameter `runtime_typechecking` that enables strict type checking at runtime.
- Embedders in pathway.xpacks.llm.embedders now correctly process empty strings as queries.
- **BREAKING**: `pw.run` and `pw.run_all` now only accept keyword arguments.

### Fixed
- `pw.Duration` can now be returned from User-Defined Functions (UDFs) or used as a constant value without resulting in errors.
- `pw.io.debezium.read` now correctly handles tables that do not have a primary key.

## [0.7.10] - 2024-01-26

### Added
- `pw.io.http.rest_connector` can now generate Open API 3.0.3 schema that will be returned by the route ``/_schema``.
- Wrappers for OpenAI Chat and Embedding services are now added to Pathway xpack for LLMs. 
- A vector indexing pipeline that allows querying for the most similar documents. It is available as class `VectorStore` as part of Pathway xpack for LLMs.

### Fixed
- `pw.debug.table_from_markdown` now uses schema parameter (when set) to properly assign _simple types_ (`int, bool, float, str, bytes`) and optional _simple types_ to columns. 

## [0.7.9] - 2024-01-18

### Changed
- `pw.io.http.rest_connector` now also accepts port as a string for backwards compatibility.
- `pw.stdlib.ml.index.KNNIndex` now sorts by distance by default.

## [0.7.8] - 2024-01-18

### Added
- Support for comparisons of tuples has been added.
- Standalone versions of methods such as `pw.groupby`, `pw.join`, `pw.join_inner`, `pw.join_left`, `pw.join_right`, and `pw.join_outer` are now available.
- The `abs` function from Python can now be used on Pathway expressions.
- The `asof_join` method now has configurable temporal behavior. The `behavior` parameter can be used to pass the configuration.
- The state of the `deduplicate` operator can now be persisted.

### Changed
- `interval_join` can now work with intervals of zero length.
- The `pw.io.http.rest_connector` can now open multiple endpoints on the same port using a new `pw.io.http.PathwayWebserver` class.
- The `pw.xpacks.connectors.sharepoint.read` and `pw.io.gdrive.read` methods now support the size limit for a single object. If set, it will exclude too large files and won't read them.

## [0.7.7] - 2023-12-27

### Added
- pathway.xpacks.llm.splitter.TokenCountSplitter.

## [0.7.6] - 2023-12-22

## New Features

### Conversion Methods in `pw.Json`
- Introducing new methods for strict conversion of `pw.Json` to desired types within a UDF body:
  - `as_int()`
  - `as_float()`
  - `as_str()`
  - `as_bool()`
  - `as_list()`
  - `as_dict()`

### DateTime Functionality
- Added `table.col.dt.utc_from_timestamp` method: Creates `DateTimeUtc` from timestamps represented as `int`s or `float`s.
- Enhanced the `table.col.dt.timestamp` method with a new `unit` argument to specify the unit of the returned timestamp.

### Experimental Features
- Introduced an experimental xpack with a Microsoft SharePoint input connector.

## Enhancements

### Improved JSON Handling
- Index operator (`[]`) can now be directly applied to `pw.Json` within UDFs to access elements of JSON objects, arrays, and strings.

### Expanded Timestamp Functionality
- Enhanced the `table.col.dt.from_timestamp` method to create `DateTimeNaive` from timestamps represented as `int`s or `float`s.
- Deprecated not specifying the `unit` argument of the `table.col.dt.timestamp` method.

### KNNIndex Enhancements
- `KNNIndex` now supports returning computed distances.
- Added support for cosine similarity in `KNNIndex`.

### Deprecated Features
- The `offset` argument of `pw.stdlib.temporal.sliding` and `pw.stdlib.temporal.tumbling` is deprecated. Use `origin` instead, as it represents a point in time, not a duration.

## Bug Fixes

### DateTime Fixes
- Sliding window now works correctly with UTC Datetimes.

### `asof_join` Improvements
- Temporal column in `asof_join` no longer has to be named `t`.
- `asof_join` includes rows with equal times for all values of the `direction` parameter.

### Fixed Issues

- Fixed an issue with `pw.io.gdrive.read`: Shared folders support is now working seamlessly.

## [0.7.5] - 2023-12-15

### Added
- Added Table.split() method for splitting table based on an expression into two tables.
- Columns with datatype duration can now be multiplied and divided by floats.
- Columns with datatype duration now support both true and floor division (`/` and `//`) by integers.

### Changed
- Pathway is better at typing if_else expressions when optional types are involved.
- `table.flatten()` operator now supports Json array.
- Buffers (used to delay outputs, configured via delay in `common_behavior`) now flush the data when the computation is finished. The effect of this change can be seen when run in bounded (batch / multi-revision) mode.
- `pw.io.subscribe()` takes additional argument `on_time_end` - the callback function to be called on each closed time of computation.
- `pw.io.subscribe()` is now a single-worker operator, guaranteeing that `on_end` is triggered at most once.
- `KNNIndex` supports now metadata filtering. Each query can specify it's own filter in the JMESPath format.

### Fixed
- Resolved an optimization bug causing `pw.iterate` to malfunction when handling columns effectively pointing to the same data.

## [0.7.4] - 2023-12-05

### Changed
- Pathway now keeps track of `array` columntype better - it is able to keep track of Array dtype and number of dimensions, wherever applicable.

### Fixed
- Fixed issues with standalone panel+Bokeh dashboards to ensure optimal functionality and performance.

## [0.7.3] - 2023-11-30

### Added
- A method `weekday` has been added to the `dt` namespace, that can be called on column expressions containing datetime data. This method returns an integer that represents the day of the week.
- **EXPERIMENTAL**: Methods `show` and `plot` on Tables, providing visualizations of data using HoloViz Panel.
- Added support for `instance` parameter to `groupby`, `join`, `windowby` and temporal join methods.
- `pw.PersistenceMode.UDF_CACHING` persistence mode enabling automatic caching of `AsyncTransformer` invocations.

### Changed
- Methods `round` and `floor` on columns with datetimes now accept duration argument to be a string.
- `pw.debug.compute_and_print` and `pw.debug.compute_and_print_update_stream` have a new argument `n_rows` that limits the number of rows printed.
- `pw.debug.table_to_pandas` has a new argument `include_id` (by default `True`). If set to `False`, creates a new index for the Pandas DataFrame, rather than using the keys of the Pathway Table.
- `windowby` function `shard` argument is now deprecated and `instance` should be used.
- Special column name `_pw_shard` is now deprecated, and `_pw_instance` should be used.
- `pw.ReplayMode` now can be accessed as `pw.PersistenceMode`, while the `SPEEDRUN` and `REALTIME` variants are now accessible as `SPEEDRUN_REPLAY` and `REALTIME_REPLAY`.
- **EXPERIMENTAL**: `pw.io.gdrive.read` has a new argument `with_metadata` (by default `False`). If set to `True`, adds a `_metadata` column containing file metadata to the resulting table.
- Methods `get_nearest_items` and `get_nearest_items_asof_now` of `KNNIndex` allow to specify `k` (number of returned elements) separately in each query.

## [0.7.2] - 2023-11-24

### Added
- Added ability of creating custom reducers using `pw.reducers.udf_reducer` decorator. Use `pw.BaseCustomAccumulator` as a base class
  for creating accumulators. Decorating accumulator returns reducer following custom logic.
- A function `pw.debug.compute_and_print_update_stream` that computes and prints the update stream of the table.
- SQLite input connector (`pw.io.sqlite`).

### Changed
- `pw.debug.parse_to_table` is now deprecated, `pw.debug.table_from_markdown` should be used instead.
- `pw.schema_from_csv` now has `quote` and `double_quote_escapes` arguments.

### Fixed
- Schema returned from `pw.schema_from_csv` will have quotes removed from column names, so it will now work properly with `pw.io.csv.read`.

## [0.7.1] - 2023-11-17

### Added

- Experimental Google Drive input connector.
- Stateful deduplication function (`pw.stateful.deduplicate`) allowing alerting on significant changes.
- The ability to split data into batches in `pw.debug.table_from_markdown` and `pw.debug.table_from_pandas`.

## [0.7.0] - 2023-11-16

### Added
- class `Behavior`, a superclass of all behavior classes.
- class `ExactlyOnceBehavior` indicating we want to create a `CommonBehavior` that results in each window producing exactly one output (shifted in time by an optional `shift` parameter).
- function `exactly_once_behavior` creating an instance of `ExactlyOnceBehavior`.

### Changed
- **BREAKING**: `WindowBehavior` is now called `CommonBehavior`, as it can be also used with interval joins.
- **BREAKING**: `window_behavior` is now called `common_behavior`, as it can be also used with interval joins.
- Deprecating parameter `keep_queries` in `pw.io.http.rest_connector`. Now `delete_completed_queries` with an opposite meaning should be used instead. The default is still `delete_completed_queries=True` (equivalent to `keep_queries=False`) but it will soon be required to be set explicitly.

## [0.6.0] - 2023-11-10

### Added
- A flag `with_metadata` for the filesystem-based connectors to attach the source file metadata to the table entries.
- Methods `pw.debug.table_from_list_of_batches` and `pw.debug.table_from_list_of_batches_by_workers` for creating tables with defined data being inserted over time.

### Changed
- **BREAKING**: `pw.debug.table_from_pandas` and `pw.debug.table_from_markdown` now will create tables in the streaming mode, instead of static, if given table definition contains `_time` column.
- **BREAKING**: Renamed the parameter `keep_queries` in `pw.io.http.rest_connector` to `delete_queries` with the opposite meaning. It changes the default behavior - it was `keep_queries=False`, now it is `delete_queries=False`.

## [0.5.3] - 2023-10-27

### Added
- A method `get_nearest_items_asof_now` in `KNNIndex` that allows to get nearest neighbors without updating old queries in the future.
- A method `asof_now_join` in `Table` to join rows from left side of the join with right side of the join at their processing time. Past rows from left side are not used when new data appears on the right side.

## [0.5.2] - 2023-10-19

### Added
- `interval_join` now supports forgetting old entries. The configuration can be passed using `behavior` parameter of `interval_join` method.
- Decorator `@table_transformer` for marking that functions take Tables as arguments.
- Namespace for all columns `Table.C.*`.
- Output connectors now provide logs about the number of entries written and time taken.
- Filesystem connectors now support reading whole files as rows.
- Command line option for `pathway spawn` to record data and `pathway replay` command to replay data.

## [0.5.1] - 2023-10-04

### Fixed
- `select` operates only on consistent states.

## [0.5.0] - 2023-10-04

### Added
- `Schema` method `typehints` that returns dict of mypy-compatible typehints.
- Support for JSON parsing from CSV sources.
- `restrict` method in `Table` to restrict table universe to the universe of the other table.
- Better support for postgresql types in the output connector.

### Changed
- **BREAKING**: renamed `Table` method `dtypes` to `typehints`. It now returns a `dict` of mypy-compatible typehints.
- **BREAKING**: `Schema.__getitem__` returns a data class `ColumnSchema` containing all related information on particular column.
- **BREAKING**: `tuple` reducer used after intervals_over window now sorts values by time.
- **BREAKING**: expressions used in `select`, `filter`, `flatten`, `with_columns`, `with_id`, `with_id_from` have to have the same universe as the table. Earlier it was possible to use an expression from a superset of a table universe. To use expressions from wider universes, one can use `restrict` on the expression source table.
- **BREAKING**: `pw.universes.promise_are_equal(t1, t2)` no longer allows to use references from `t1` and `t2` in a single expression. To change the universe of a table, use `with_universe_of`.
- **BREAKING**: `ix` and `ix_ref` are temporarily broken inside joins (both temporal and ordinary).
- `select`, `filter`, `concat` keep columns as a single stream. The work for other operators is ongoing.

### Fixed
- Optional types other than string correctly output to PostgreSQL.

## [0.4.1] - 2023-09-25

### Added
- Support for messages compressed with zstd in the Kafka connector.

## [0.4.0] - 2023-09-21

### Added
- Support for JSON data format, including `pw.Json` type.
- Methods `as_int()`, `as_float()`, `as_str()`, `as_bool()` to convert values from `Json`.
- New argument `skip_nones` for `tuple` and `sorted_tuple` reducers.
- New argument `is_outer` for `intervals_over` window.
- `pw.schema_from_dict` and `pw.schema_from_csv` for generating schema based, respectively, on provided definition as a dictionary and CSV file with sample data.
- `generate_class` method in `Schema` class for generating schema class code.

### Changed
- Method `get()` and `[]` to support accessing elements in Jsons.
- Function `pw.assert_table_has_schema` for writing asserts checking, whether given table has the same schema as the one that is given as an argument.
- **BREAKING**: `ix` and `ix_ref` operations are now standalone transformations of `pw.Table` into `pw.Table`. Most of the usages remain the same, but sometimes user needs to provide a context (when e.g. using them inside `join` or `groupby` operations). `ix` and `ix_ref` are temporarily broken inside temporal joins.

### Fixed
- Fixed a bug where new-style optional types (e.g. `int | None`) were translated to `Any` dtype.

## [0.3.4] - 2023-09-18

### Fixed
- Incompatible `beartype` version is now excluded from dependencies.

## [0.3.3] - 2023-09-14

### Added
- Module `pathway.dt` to construct and manipulate DTypes.
- New argument `keep_queries` in `pw.io.http.rest_connector`.

### Changed
- Internal representation of DTypes. Inputting types is compatible backwards.
- Temporal functions now accept arguments of mixed types (ints and floats). For example, `pw.temporal.interval` can use ints while columns it interacts with are floats.
- Single-element arrays are now treated as arrays, not as scalars.

### Fixed
- `to_string()` method on datetimes always prints 9 fractional digits.
- `%f` format code in `strptime()` parses fractional part of a second correctly regardless of the number of digits.

## [0.3.2] - 2023-09-07

### Added

- `Table.cast_to_types()` function that can perform `pathway.cast` on multiple columns.
- `intervals_over` window, which allows to get temporally close data to given times.
- `demo.replay_csv_with_time` function that can replay a CSV file following the timestamps of a given column.

### Fixed

- Static data is now copied to ensure immutability.
- Improved error tracing mechanism to work with any type of error.

## [0.3.1] - 2023-08-29

### Added

- `tuple` reducer, that returns a tuple with values.
- `ndarray` reducer, that returns an array with values.

### Changed
- `numpy` arrays of `int32`, `uint32` and `float32` are now converted to their 64-bit variants instead of tuples.
- KNNIndex interface to take columns as inputs.
- Reducers now check types of their arguments.

### Fixed

- Fixed delayed reporting of output connector errors.
- Python objects are now freed more often, reducing peak memory usage.

## [0.3.0] - 2023-08-07

### Added

- `@` (matrix multiplication) operator.

### Changed

- Python version 3.10 or later is now required.
- Type checking is now more strict.

## [0.2.1] - 2023-07-31

### Changed

- Immediately forget queries in REST connector.
- Make type annotations mandatory in `Schema`.

### Fixed

- Fixed IDs coming from CSV source.
- Fixed indices of dataframes from pandas transformer.

## [0.2.0] - 2023-07-20

### Added

<img src="https://d14l3brkh44201.cloudfront.net/PathwayManul.svg"  alt="manul" width="50px"></img>
