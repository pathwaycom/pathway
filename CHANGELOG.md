# Changelog

All notable changes to this project will be documented in this file.

This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
## [Unreleased]

### Added
- Data persistence can now be configured to use Azure Blob Storage as a backend. An Azure backend instance can be created using `pw.persistence.Backend.azure` and included in the persistence config.

### Changed
- **BREAKING**: when creating `pw.DateTimeUtc` it is now obligatory to pass the time zone information.
- **BREAKING**: when creating `pw.DateTimeNaive` passing time zone information is not allowed.

### Fixed
- Synchronization groups now correctly handle cases where the source file-like object is updated during the reading process.

## [0.21.6] - 2025-05-29

### Added
- `sort_by` method to `pw.BaseCustomAccumulator` that allows to sort rows within a single batch. When `sort_by` is defined the rows are reduced in the order specified by the `sort_by` method. It can for example be used to process entries in the order of event time.

### Changed
- `pw.Table.debug` now prints a whole row in a single line instead of printing each cell separately.
- Calling functions without arguments in YAML configurations files is now deprecated in `pw.load_yaml`. To call the function a mapping should be passed, e.g. empty mapping as `{}`. In the future `!` syntax without any mapping will be used to pass function objects without calling them.
- The license check error message now provides a more detailed explanation of the failure.
- When code is run using `pathway spawn` with multiple processes, if one process terminates with an error, all other processes will also be terminated.
- `pw.xpacks.llm.vector_store.VectorStoreServer` is being deprecated, and it is now subclass of `pw.xpacks.llm.document_store.DocumentStore`. Public API is being kept the same, however users are encouraged to switch to using `DocumentStore` from now on. 
- `pw.xpacks.llm.vector_store.VectorStoreClient` is being deprecated in favor of `pw.xpacks.llm.document_store.DocumentStoreClient`.
- `pw.io.deltalake.write` can now maintain the target table's snapshot on the output.

## [0.21.5] - 2025-05-09

### Changed
- `pw.io.deltalake.read` now processes Delta table version updates atomically, applying all changes together in a single minibatch.
- The panel widget for table visualization now has a horizontal scroll bar for large tables.
- Added the possibility to return value from any column from `pw.reducers.argmax` and `pw.reducers.argmin`, not only `id`.

### Fixed
- `pw.reducers.argmax` and `pw.reducers.argmin` work correctly with the result of `pw.Table.windowby`.

## [0.21.4] - 2025-04-24

### Added
- `pw.io.kafka.read` and `pw.io.redpanda.read` now support static mode.

### Changed
- The `inactivity_detection` function is now a method for append only tables. It no longer relies on an event timestamp column but now uses table processing times to detect inactivity periods.

## [0.21.3] - 2025-04-16

### Fixed
- The performance of input connectors is optimized in certain cases.
- The panel widget for table visualization does now a better formatting for timestamps and missing values. The pagination was also updated to better fit the widget and the default sorters in snapshot mode have been fixed.

## [0.21.2] - 2025-04-10

### Added
- Added synchronization group mechanism to align multiple data sources based on selected columns. It can be accessed with `pw.io.register_input_synchronization_group`.
- `pw.io.register_input_synchronization_group` now supports the following types of columns: `pw.DateTimeUtc`, `pw.DateTimeNaive`, `pw.DateTimeDuration`, and `int`.

### Changed
- Enhanced error reporting for runtime errors across most operators, providing a trace that simplifies identifying the root cause.

### Fixed
- Bugfix for problem with list_documents() when no documents present in store.
- The append-only property of tables created by `pw.io.kafka.read` is now set correctly.

## [0.21.1] - 2025-03-28

### Changed
- Input connectors now throttle parsing error messages if their share is more than 10% of the parsing attempts.
- New flag `return_status` for `inputs_query` method in `pw.xpacks.llm.DocumentStore`. If set to True, DocumentStore returns the status of indexing for each file.

## [0.21.0] - 2025-03-19

### Added
- All Pathway types can now be serialized to CSV using `pw.io.csv.write` and deserialized back using `pw.io.csv.read`.
- `pw.io.csv.read` now parses null-values in data when it can be done unambiguously.

### Changed
- **BREAKING**: Updated endpoints in `pw.xpacks.llm.question_answering.BaseRAGQuestionAnswerer`:
  - Deprecated: `/v1/pw_list_documents`, `/v1/pw_ai_answer`
  - New: `/v2/list_documents`, `/v2/answer`
- RAG methods under the `pw.xpacks.llm.question_answering.RAGClient` are re-named, and they now use the new endpoints. Old methods are deprecated and will be removed in the future.
  - `pw_ai_summary` -> `summarize`
  - `pw_ai_answer` -> `answer`
  - `pw_list_documents` -> `list_documents`
- When `pw.io.deltalake.write` creates a table, it also stores its metadata in the columns of the created Delta table. This metadata can be used by Pathway when reading the table with `pw.io.deltalake.read` if no `schema` is specified.
- The `schema` parameter is now optional for `pw.io.deltalake.read`. If the table was created by Pathway and the `schema` was not specified by user, it is read from the table metadata.
- `pw.io.deltalake.write` now aligns the output metadata with the existing table's metadata, preserving any custom metadata in the sink.
- **BREAKING**: The `Bytes` type is now serialized and deserialized with base64 encoding and decoding when the CSV format is used.
- **BREAKING**: The `Duration` type is now serialized and deserialized as a number of nanoseconds when the CSV format is used.
- **BREAKING**: The `tuple` and `np.ndarray` types are now serialized and deserialized as their JSON representations when the CSV format is used.

### Fixed
- `pw.io.csv.write` now correctly escapes quote characters.
- `table_parsing_strategy="llm"` in `DoclingParser` now works correctly

## [0.20.1] - 2025-03-07

### Added
- Added `RecursiveSplitter`
- `pw.io.deltalake.write` now checks that the schema of the target table Delta Table corresponds to the schema of the Pathway table that is sent for the output. If the schemas differ, a human-readable error message is produced.

## [0.20.0] - 2025-02-25

### Added
- Added structure-aware chunking for `DoclingParser`.
- Added `table_parsing_strategy` for `DoclingParser`.
- Column expressions `as_int()`, `as_float()`, `as_str()`, and `as_bool()` now accept additional arguments, `unwrap` and `default`, to simplify null handling.
- Support for python tuples in expressions.

### Changed
- **BREAKING**: Changed the argument in `DoclingParser` from `parse_images` (bool) into `image_parsing_strategy` (Literal["llm"] | None).
- **BREAKING**: `doc_post_processors` argument in the `pw.xpacks.llm.document_store.DocumentStore` now longer accepts `pw.UDF`.
- Better error messages when using `pathway spawn` with multiple workers. Now error messages are printed only from the worker experiencing the error directly.

### Fixed
- `doc_post_processors` argument in the `pw.xpacks.llm.document_store.DocumentStore` had no effect. This is now fixed.

## [0.19.0] - 2025-02-20

### Added
- `LLMReranker` now supports custom prompts as well as custom response parsers allowing for other ranking scales apart from default 1-5.
- `pw.io.kafka.write` and `pw.io.nats.write` now support `ColumnReference` as a topic name. When a `ColumnReference` is provided, each message's topic is determined by the corresponding column value.
- `pw.io.python.write` accepting `ConnectorObserver` as an alternative to `pw.io.subscribe`.
- `pw.io.iceberg.read` and `pw.io.iceberg.write` now support S3 as data backend and AWS Glue catalog implementations.
- All output connectors now support the `sort_by` field for ordering output within a single minibatch.
- A new UDF executor `pw.udfs.fully_async_executor`. It allows for creation of non-blocking asynchronous UDFs which results can be returned in the future processing time.
- A Future data type to represent results of fully asynchronous UDFs.
- `pw.Table.await_futures` method to wait for results of fully asynchronous UDFs.
- `pw.io.deltalake.write` now supports partition columns specification.

### Changed
- **BREAKING**: Changed the interface of `LLMReranker`, the `use_logit_bias`, `cache_strategy`, `retry_strategy` and `kwargs` arguments are no longer supported.
- **BREAKING**: LLMReranker no longer inherits from pw.UDF
- **BREAKING**: `pw.stdlib.utils.AsyncTransformer.output_table` now returns a table with columns with Future data type.
- `pw.io.deltalake.read` can now read append-only tables without requiring explicit specification of primary key fields.


## [0.18.0] - 2025-02-07

### Added
- `pw.io.postgres.write` and `pw.io.postgres.write_snapshot` now handle serialization of `PyObjectWrapper` and `Timedelta` properly.
- New chunking options in `pathway.xpacks.llm.parsers.UnstructuredParser`
- Now all Pathway types can be serialized into JSON and consistently deserialized back.
- `table.col.dt.to_duration` converting an integer into a `pw.Duration`.
- `pw.Json` now supports storing datetime and duration type values in ISO format.

### Changed
- **BREAKING**: Changed the interface of `UnstructuredParser`
- **BREAKING**: The `Pointer` type is now serialized and deserialized as a string field in Iceberg and Delta Lake.
- **BREAKING**: The `Bytes` type is now serialized and deserialized with base64 encoding and decoding when the JSON format is used. A string field is used to store the encoded contents.
- **BREAKING**: The `Array` type is now serialized and deserialized as an object with two fields: `shape` denoting the shape of the stored multi-dimensional array and `elements` denoting the elements of the flattened array.
- **BREAKING**: Marked package as **py.typed** to indicate support for type hints.

### Removed
- **BREAKING**: Removed undocumented `license_key` argument from `pw.run` and `pw.run_all` methods. Instead, `pw.set_license_key` should be used.


## [0.17.0] - 2025-01-30

### Added
- `pw.io.iceberg.read` method for reading Apache Iceberg tables into Pathway.
- methods `pw.io.postgres.write` and `pw.io.postgres.write_snapshot` now accept an additional argument `init_mode`, which allows initializing the table before writing.
- `pw.io.deltalake.read` now supports serialization and deserialization for all Pathway data types.
- New parser `pathway.xpacks.llm.parsers.DoclingParser` supporting parsing of pdfs with tables and images.
- Output connectors now include an optional `name` parameter. If provided, this name will appear in logs and monitoring dashboards.
- Automatic naming for input and output connectors has been enhanced.

### Changed
- **BREAKING**: `pw.io.deltalake.read` now requires explicit specification of primary key fields.
- **BREAKING**: `pw.xpacks.llm.question_answering.BaseRAGQuestionAnswerer` now returns a dictionary from `pw_ai_answer` endpoint.
- `pw.xpacks.llm.question_answering.BaseRAGQuestionAnswerer` allows optionally returning context documents from `pw_ai_answer` endpoint.
- **BREAKING**: When using delay in temporal behavior, current time is updated immediately, not in the next batch.
- **BREAKING**: The `Pointer` type is now serialized to Delta Tables as raw bytes.
- `pw.io.kafka.write` now allows to specify `key` and `headers` for JSON and CSV data formats.
- `persistent_id` parameter in connectors has been renamed to `name`. This new `name` parameter allows you to assign names to connectors, which will appear in logs and monitoring dashboards.
- Changed names of parsers to be more consistent: `ParseUnstrutured` -> `UnstructuredParser`, `ParseUtf8` -> `Utf8Parser`. `ParseUnstrutured` and `ParseUtf8` are now deprecated.

### Fixed
- `generate_class` method in `Schema` now correctly renders columns of `UnionType` and `None` types.
- a bug in delay in temporal behavior. It was possible to emit a single entry twice in a specific situation.
- `pw.io.postgres.write_snapshot` now correctly handles tables that only have primary key columns.

### Removed
- **BREAKING**: `pw.indexing.build_sorted_index`, `pw.indexing.retrieve_prev_next_values`, `pw.indexing.sort_from_index` and `pw.indexing.SortedIndex` are removed. Sorting is now done with `pw.Table.sort`.
- **BREAKING**: Removed deprecated methods `pw.Table.unsafe_promise_same_universe_as`, `pw.Table.unsafe_promise_universes_are_pairwise_disjoint`, `pw.Table.unsafe_promise_universe_is_subset_of`, `pw.Table.left_join`, `pw.Table.right_join`, `pw.Table.outer_join`, `pw.stdlib.utils.AsyncTransformer.result`.
- **BREAKING**: Removed deprecated column `_pw_shard` in the result of `windowby`.
- **BREAKING**: Removed deprecated functions `pw.debug.parse_to_table`, `pw.udf_async`, `pw.reducers.npsum`, `pw.reducers.int_sum`, `pw.stdlib.utils.col.flatten_column`.
- **BREAKING**: Removed deprecated module `pw.asynchronous`.
- **BREAKING**: Removed deprecated access to functions from `pw.io` in `pw`.
- **BREAKING**: Removed deprecated classes `pw.UDFSync`, `pw.UDFAsync`.
- **BREAKING**: Removed class `pw.xpack.llm.parsers.OpenParse`. It's functionality has been replaced with `pw.xpack.llm.parsers.DoclingParser`.
- **BREAKING**: Removed deprecated arguments from input connectors: `value_columns`, `primary_key`, `types`, `default_values`. Schema should be used instead.

## [0.16.4] - 2025-01-09

### Fixed
- Google Drive connector in static mode now correctly displays in jupyter visualizations.

## [0.16.3] - 2025-01-02

### Added
- `pw.io.iceberg.write` method for writing Pathway tables into Apache Iceberg.

### Changed
- values of non-deterministic UDFs are not stored in tables that are `append_only`.
- `pw.Table.ix` has better runtime error message that includes id of the missing row.

### Fixed
- temporal behaviors in temporal operators (`windowby`, `interval_join`) now consume no CPU when no data passes through them.

## [0.16.2] - 2024-12-19

### Added
- `pw.xpacks.llm.prompts.RAGPromptTemplate`, set of prompt utilities that enable verifying templates and creating UDFs from prompt strings or callables.
- `pw.xpacks.llm.question_answering.BaseContextProcessor` streamlines development and tuning of representing retrieved context documents to the LLM.
- `pw.io.kafka.read` now supports `with_metadata` flag, which makes it possible to attach the metadata of the Kafka messages to the table entries.
- `pw.io.deltalake.read` can now stream the tables with deletions, if no deletion vectors were used.

### Changed
- `pw.io.sharepoint.read` now explicitly terminates with an error if it fails to read the data the specified number of times per row (the default is `8`).
- `pw.xpacks.llm.prompts.prompt_qa`, and other prompts expect 'context' and 'query' fields instead of 'docs'. 
- Removed support for `short_prompt_template` and `long_prompt_template` in `pw.xpacks.llm.question_answering.BaseRAGQuestionAnswerer`. These prompt variants are no longer accepted during construction or in requests.
- `pw.xpacks.llm.question_answering.BaseRAGQuestionAnswerer` allows setting user created prompts. Templates are verified to include 'context' and 'query' placeholders.
- `pw.xpacks.llm.question_answering.BaseRAGQuestionAnswerer` can take a `BaseContextProcessor` that represents context documents to the LLM. Defaults to `pw.xpacks.llm.question_answering.SimpleContextProcessor` which filters metadata fields and joins the documents with new lines.

### Fixed
- The input of `pw.io.fs.read` and `pw.io.s3.read` is now correctly persisted in case deletions or modifications of already processed objects take place.

## [0.16.1] - 2024-12-12

### Changed
- `pw.io.s3.read` now monitors object deletions and modifications in the S3 source, when ran in streaming mode. When an object is deleted in S3, it is also removed from the engine. Similarly, if an object is modified in S3, the engine updates its state to reflect those changes.
- `pw.io.s3.read` now supports `with_metadata` flag, which makes it possible to attach the metadata of the source object to the table entries.

### Fixed
- `pw.xpacks.llm.document_store.DocumentStore` no longer requires `_metadata` column in the input table.

## [0.16.0] - 2024-11-29

### Added
- `pw.xpacks.llm.document_store.SlidesDocumentStore`, which is a subclass of `pw.xpacks.llm.document_store.DocumentStore` customized for retrieving slides from presentations.
- `pw.temporal.inactivity_detection` and `pw.temporal.utc_now` functions allowing for alerting and other time dependent usecases

### Changed
- `pw.Table.concat`, `pw.Table.with_id`, `pw.Table.with_id_from` no longer perform checks if ids are unique. It improves memory usage.
- table operations that store values (like `pw.Table.join`, `pw.Table.update_cells`) no longer store columns that are not used downstream.
- `append_only` column property is now propagated better (there are more places where we can infer it).
- **BREAKING**: Parsers and parser utilities including `OpenParse`, `ParseUnstructured`, `ParseUtf8`, `parse_images` are now async. Parser interface in the `VectorStore` and `DocumentStore` remains unchanged.
- **BREAKING**: Unused arguments from the constructor `pw.xpacks.llm.question_answering.DeckRetriever` are no longer accepted. 

### Fixed
- `query_as_of_now` of `pw.stdlib.indexing.DataIndex` and `pw.stdlib.indexing.HybridIndex` now work in constant memory for infinite query stream (no query-related data is kept after query is answered).

## [0.15.4] - 2024-11-18

### Added
- `pw.io.kafka.read` now supports reading entries starting from a specified timestamp.
- `pw.io.nats.read` and `pw.io.nats.write` methods for reading from and writing Pathway tables to NATS.

### Changed
- `pw.Table.diff` now supports setting `instance` parameter that allows computing differences for multiple groups.
- `pw.io.postgres.write_snapshot` now keeps the Postgres table fully in sync with the current state of the table in Pathway. This means that if an entry is deleted in Pathway, the same entry will also be deleted from the Postgres table managed by the output connector.

### Fixed
- `pw.PyObjectWrapper` is now picklable.
- `query_as_of_now` of `pw.stdlib.indexing.DataIndex` and `pw.stdlib.indexing.HybridIndex` now work in constant memory for infinite query stream (no query-related data is kept after query is answered).

## [0.15.3] - 2024-11-07

### Added
- `pw.io.mongodb.write` connector for writing Pathway tables in MongoDB.
- `pw.io.s3.read` now supports downloading objects from an S3 bucket in parallel.

### Changed
- `pw.io.fs.read` performance has been improved for directories containing a large number of files.

## [0.15.2] - 2024-10-24

### Added
- `pw.io.deltalake.read` now supports custom S3 Delta Lakes with HTTP endpoints.
- `pw.io.deltalake.read` now supports specifying both a custom endpoint and a custom region for Delta Lakes via `pw.io.s3.AwsS3Settings`.

### Changed
- Indices in `pathway.stdlib.indexing.nearest_neighbors` can now work also on numpy arrays. Previously they only accepted `list[float]`. Working with numpy arrays improves memory efficiency.
- `pw.io.s3.read` has been optimized to minimize new object requests whenever possible.
- It is now possible to set the size limit of cache in `pw.udfs.DiskCache`.
- State persistence now uses a single backend for both metadata and stream storage. The `pw.persistence.Config.simple_config` method is therefore deprecated. Now you can use the `pw.persistence.Config` constructor with the same parameters that were previously used in `simple_config`.

### Fixed
- `pw.io.bigquery.write` connector now correctly handles `pw.Json` columns.

## [0.15.1] - 2024-10-04

### Fixed
- `pw.temporal.session` and `pw.temporal.asof_join` now correctly works with multiple entries with the same time.
- Fixed an issue in `pw.stdlib.indexing` where filters would cause runtime errors while using `HybridIndexFactory`.


## [0.15.0] - 2024-09-12

### Added
- **Experimental** A ``pw.xpacks.llm.document_store.DocumentStore`` to process and index documents.
- ``pw.xpacks.llm.servers.DocumentStoreServer`` used to expose REST server for retrieving documents from ``pw.xpacks.llm.document_store.DocumentStore``.
- `pw.xpacks.stdlib.indexing.HybridIndex` used for querying multiple indices and combining their results.
- `pw.io.airbyte.read` now also supports streams that only operate in `full_refresh` mode.

### Changed
- Running servers for answering queries is extracted from `pw.xpacks.llm.question_answering.BaseRAGQuestionAnswerer` into `pw.xpacks.llm.servers.QARestServer` and `pw.xpacks.llm.servers.QASummaryRestServer`.
- **BREAKING**: `query` and `query_as_of_now` of `pathway.stdlib.indexing.data_index.DataIndex` now produce an empty list instead of `None` if no match is found.

## [0.14.3] - 2024-08-22

### Fixed
- `pw.io.deltalake.read` and `pw.io.deltalake.write` now correctly work with lakes hosted in S3 over min.io, Wasabi and Digital Ocean.

### Added
- The Pathway CLI command `spawn` can now execute code directly from a specified GitHub repository.
- A new CLI command, `spawn-from-env`, has been added. This command runs the Pathway CLI `spawn` command using arguments provided in the `PATHWAY_SPAWN_ARGS` environment variable.

## [0.14.2] - 2024-08-06

### Fixed
- Switched `pw.xpacks.llm.embedders.GeminiEmbedder` to be sync to resolve compatibility issues with the Google Colab runs.
- Pinned `surya-ocr` module version for stability.

## [0.14.1] - 2024-08-05

### Added
- `pw.xpacks.llm.embedders.GeminiEmbedder` which is a wrapper for Google Gemini Embedding services.

## [0.14.0] - 2024-07-25

### Fixed
- `pw.debug.table_to_pandas` now exports `int | None` columns correctly.

### Changed
- `pw.io.airbyte.read` can now be used with Airbyte connectors implemented in Python without requiring Docker.
- **BREAKING**: UDFs now verify the type of returned values at runtime. If it is possible to cast a returned value to a proper type, the values is cast. If the value does not match the expected type and can't be cast, an error is raised.
- **BREAKING**: `pw.reducers.ndarray` reducer requires input column to either have type `float`, `int` or `Array`.
- `pw.xpacks.llm.parsers.OpenParse` can now extract and parse images & diagrams from PDFs. This can be enabled by setting the `parse_images`. `processing_pipeline` can be also set to customize the post processing of doc elements.

## [0.13.2] - 2024-07-08

### Added
- `pw.io.deltalake.read` now supports S3 data sources.
- `pw.xpacks.llm.parsers.ImageParser` which allows parsing images with the vision LMs.
- `pw.xpacks.llm.parsers.SlideParser` that enables parsing PDF and PPTX slides with the vision LMs.
- `pw.xpacks.llm.parsers.question_answering.RAGClient`, Python client for Pathway hosted RAG apps.
- `pw.xpacks.llm.parsers.question_answeringDeckRetriever`, a RAG app that enables searching through slide decks with visual-heavy elements.

### Fixed
- `pw.xpacks.llm.vector_store.VectorStoreServer` now uses new indexes.

### Changed
- `pw.xpacks.llm.parsers.OpenParse` now supports any vision Language model including local and proprietary models via LiteLLM.


## [0.13.1] - 2024-06-27

### Added
- `pw.io.kafka.read` now accepts an autogenerate_key flag. This flag determines the primary key generation policy to apply when reading raw data from the source. You can either use the key from the Kafka message or have Pathway autogenerate one.
- `pw.io.deltalake.read` input connector that fetches changes from DeltaLake into a Pathway table.
- `pw.xpacks.llm.parsers.OpenParse` which allows parsing tables and images in PDFs.

### Fixed
- All S3 input connectors (including S3, Min.io, Digital Ocean, and Wasabi) now automatically retry network operations if a failure occurs.
- The issue where the connection to the S3 source fails after partially ingesting an object has been resolved by downloading the object in full first.

## [0.13.0] - 2024-06-13

### Added
- `pw.io.deltalake.write` now supports S3 destinations.

### Changed
- `pw.debug.compute_and_print` now allows passing more than one table.
- **BREAKING**: `path` parameter in `pw.io.deltalake.write` renamed to `uri`.

### Fixed
- A bug in `pw.Table.deduplicate`. If `persistent_id` is not set, it is no longer generated in `pw.PersistenceMode.SELECTIVE_PERSISTING` mode.

## [0.12.0] - 2024-06-08

### Added
- `pw.PyObjectWrapper` that enables passing python objects of any type to the engine.
- `cache_strategy` option added for `pw.io.http.rest_connector`. It enables cache configuration, which is useful for duplicated requests.
- `allow_misses` argument to `Table.ix` and `Table.ix_ref` methods which allows for filling rows with missing keys with None values.
- `pw.io.deltalake.write` output connector that streams the changes of a given table into a DeltaLake storage.
- `pw.io.airbyte.read` now supports data extraction with Google Cloud Runs.

### Removed
- **BREAKING**: Removed `Table.having` method.
- **BREAKING**: Removed `pw.DATE_TIME_UTC`, `pw.DATE_TIME_NAIVE` and `pw.DURATION` as dtype markers. Instead, `pw.DateTimeUtc`, `pw.DateTimeNaive` and `pw.Duration` should be used, which are wrappers for corresponding pandas types.
- **BREAKING**: Removed class transformers from public API: `pw.ClassArg`, `pw.attribute`, `pw.input_attribute`, `pw.input_method`, `pw.method`, `pw.output_attribute` and `pw.transformer`.
- **BREAKING**: Removed several methods from `pw.indexing` module: `binsearch_oracle`, `filter_cmp_helper`, `filter_smallest_k` and `prefix_sum_oracle`.

## [0.11.2] - 2024-05-27

### Added
- `pathway.assert_table_has_schema` and `pathway.table_transformer` now accept `allow_subtype` argument, which, if True, allows column types in the Table be subtypes of types in the Schema.
- `next` method to `pw.io.python.ConnectorSubject` (python connector) that enables passing values of any type to the engine, not only values that are json-serializable. The `next` method should be the preferred way of passing values from the python connector.

### Changed
- The `format` argument of `pw.io.python.read` is deprecated. A data format is inferred from the method used (`next_json`, `next_str`, `next_bytes`) and the provided schema.

### Removed
- Removed `pw.numba_apply` and `numba` dependency.

### Fixed
- Fixed `pw.this` desugaring bug, where `__getitem__` in `.ix` context was not working properly.
- `pw.io.sqlite.read` now checks if the data matches the passed schema.

## [0.11.1] - 2024-05-16

### Added
- `query` and `query_as_of_now` of `pathway.stdlib.indexing.data_index.DataIndex` now accept in `metadata_column` parameter a column with data of type `str | None`.
- `pathway.xpacks.connectors.sharepoint` module, available with Pathway Scale License.


## [0.11.0] - 2024-05-10

### Added
- Embedders in the LLM xpack now have method `get_embedding_dimension` that returns number of dimension used by the chosen embedder.
- `pathway.stdlib.indexing.nearest_neighbors`, with implementations of `pathway.stdlib.indexing.data_index.InnerIndex` based on k-NN via LSH (implemented in Pathway), and k-NN provided by USearch library.
- `pathway.stdlib.indexing.vector_document_index`, with a few predefined instances of `pathway.stdlib.indexing.data_index.DataIndex`.
- `pathway.stdlib.indexing.bm25`, with implementations of `pathway.stdlib.indexing.data_index.InnerIndex` based on BM25 index provided by Tantivy.
- `pathway.stdlib.indexing.full_text_document_index`, with a predefined instance of `pathway.stdlib.indexing.data_index.DataIndex`.
- Introduced the `reranker` module under `llm.xpacks`. Includes few re-ranking strategies and utility functions for RAG applications.

### Changed
- **BREAKING**: `windowby` generates IDs of produced rows differently than in the previous version.
- **BREAKING**: `pw.io.csv.write` prints printable non-ascii characters as regular text, not `\u{xxxx}`.
- **BREAKING**: Connector methods `pw.io.elasticsearch.read`, `pw.io.debezium.read`, `pw.io.fs.read`, `pw.io.jsonlines.read`, `pw.io.kafka.read`, `pw.io.python.read`, `pw.io.redpanda.read`, `pw.io.s3.read` now check the type of the input data. Previously it was not checked if the provided format was `"json"`/`"jsonlines"`. If the data is inconsistent with the provided schema, the row is skipped and the error message is emitted.
- **BREAKING**: `query` and `query_as_of_now` methods of `pathway.stdlib.indexing.data_index.DataIndex` now return `pathway.JoinResult`, to allow resolving column name conflicts (between columns in the table with queries and table with index data).
- **BREAKING**: DataIndex methods `query` and `query_as_of_now` now return score in a column named `_pw_index_reply_score` (defined as `_SCORE` variable in `pathway.stdlib.indexing.colnames.py`).

### Removed
- **BREAKING**: `pathway.stdlib.indexing.data_index.VectorDocumentIndex` class, some predefined instances are now meant to be obtained via methods provided in `pathway.stdlib.indexing.vector_document_index`.
- **BREAKING**: `with_distances` parameter of `query` and `query_as_of_now` methods in `pathway.stdlib.indexing.data_index.DataIndex`. Instead of 'distance', we now operate with a more general term 'score' (higher = better). For distance based indices score is usually defined as negative distance. Score is now always included in the answer, as long as underlying index returns something that indicates quality of a match.

## [0.10.1] - 2024-04-30

### Added
- `query` method to VectorStoreServer to enable compatible API with `DataIndex`.
- `AdaptiveRAGQuestionAnswerer` to xpacks.question_answering. End-to-end pipeline and accompanying code for `Private RAG` showcase.

## [0.10.0] - 2024-04-24

### Added
- Pathway now warns when unintentionally creating Table with empty universe.
- `pw.io.kafka.write` in `raw` and `plaintext` formats now supports output for tables with multiple columns. For such tables, it requires the specification of the column that must be used as a value of the produced Kafka messages and gives a possibility to provide column which must be used as a key.
- `pw.io.kafka.write` can now output values from the table using Kafka message headers in 'raw' and 'plaintext' output format.

### Changed
- `instance` arguments to `groupby`, `join`, `with_id_from` now determine how entries are distributed between machines.
- `flatten` results remain on the same machine as their source entries.
- `join` sends each record between machines at most once.
- **BREAKING**: `flatten`, `join`, `groupby` (if used with `instance`), `with_id_from` (if used with `instance`) generate IDs of the produced rows differently than in the previous versions.
- `pathway spawn` with multiple workers prints only output from the first worker.

## [0.9.0] - 2024-04-18

### Added
- `pw.reducers.latest` and `pw.reducers.earliest` that return the value with respectively maximal and minimal processing time assigned.
- `pw.io.kafka.write` can now produce messages containing raw bytes in case the table consists of a single binary column and `raw` mode is specified. Similarly, this method will provide plaintext messages if `plaintext` mode is chosen and the table consists of a single string-typed column.
- `pw.io.pubsub.write` connector for publishing Pathway tables into Google PubSub.
- Argument `strict_prompt` to `answer_with_geometric_rag_strategy` and `answer_with_geometric_rag_strategy_from_index` that allows optimizing prompts for smaller open-source LLM models.
- Temporarily switch LiteLLMChat's generation method to sync version due to a bug while using `json` mode with Ollama.

### Changed
- **BREAKING**: `pw.io.kafka.read` will not parse the messages from UTF-8 in case `raw` mode was specified. To preserve this behavior you can use the `plaintext` mode.
- **BREAKING**: `Table.flatten` now flattens one column and spreads every other column of the table, instead of taking other columns from the argument list.

## [0.8.6] - 2024-04-10

### Added
- `pw.io.bigquery.write` connector for writing Pathway tables into Google BigQuery.
- parameter `filepath_globpattern` to `query` method in `VectorStoreClient` for specifying which files should be considered in the query.
- Improved compatibility of `pw.Json` with standard methods such as `len()`, `int()`, `float()`, `bool()`, `iter()`, `reversed()` when feasible.

### Changed
- `pw.io.postgres.write` can now parallelize writes to several threads if several workers are configured.
- Pathway now checks types of pointers rigorously. Indexing table with mismatched number/types of columns vs what was used to create index will now result in a TypeError.
- `pw.Json.as_float()` method now supports integer JSON values.

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
