// Copyright © 2026 Pathway

use std::borrow::Cow;
use std::mem::take;
use std::time::Duration;

use crate::async_runtime::create_async_tokio_runtime;
use crate::connectors::data_format::FormatterContext;
use crate::connectors::data_storage::polling::{
    LiveState, PolledRow, PollingDataSource, PollingReader,
};
use crate::connectors::data_storage::{ConnectorMode, ReadError, StorageType};
use crate::connectors::offset::OffsetKey;
use crate::connectors::{WriteError, Writer};

use elasticsearch::indices::IndicesGetMappingParts;
use elasticsearch::{BulkParts, Elasticsearch, SearchParts};
use log::warn;
use serde_json::{json, Value as JsonValue};
use tokio::runtime::Runtime as TokioRuntime;

/// Errors specific to the Elasticsearch input connector. Exposed to the engine
/// through a single transparent [`ReadError::ElasticSearch`](crate::connectors::ReadError)
/// variant so `?` works directly on these results inside the reader.
#[derive(Debug, thiserror::Error)]
pub enum ElasticSearchError {
    #[error(transparent)]
    Client(#[from] ::elasticsearch::Error),

    #[error("ElasticSearch search response is malformed: {0}")]
    MalformedResponse(String),

    #[error("document is missing the {0:?} field declared as the polling column")]
    MissingField(String),

    #[error(
        "the timestamp column {column:?} must hold an integer (e.g. epoch milliseconds), got {value}"
    )]
    InvalidTimestamp { column: String, value: String },
}

/// Maximum number of hits requested in a single `search` call. Elasticsearch
/// caps a plain `from`/`size` window at `index.max_result_window` (10 000 by
/// default), so a fetch larger than this is assembled internally with
/// `search_after`. The generic [`PollingReader`] decides the overall page size
/// (`limit`); this constant only bounds each underlying request.
const SEARCH_PAGE_SIZE: usize = 10_000;

/// Elasticsearch field types that the connector can read as an integer timestamp
/// and that support range queries, sorting, and the `max` aggregation the
/// watermark logic relies on.
const NUMERIC_FIELD_TYPES: &[&str] = &[
    "long",
    "integer",
    "short",
    "byte",
    "double",
    "float",
    "half_float",
    "scaled_float",
    "unsigned_long",
    "date",
    "date_nanos",
];

/// Elasticsearch field types that are sortable (have `doc_values` by default), so
/// they can be used as the secondary sort key that makes `search_after`
/// pagination a total order. Notably excludes `text`, which is not sortable.
const SORTABLE_FIELD_TYPES: &[&str] = &[
    "long",
    "integer",
    "short",
    "byte",
    "double",
    "float",
    "half_float",
    "scaled_float",
    "unsigned_long",
    "date",
    "date_nanos",
    "boolean",
    "keyword",
    "constant_keyword",
    "wildcard",
    "version",
    "ip",
];

/// Resolve a (possibly dotted) field name inside a mapping `properties` object,
/// descending through object sub-fields (`properties`) and multi-fields
/// (`fields`) so that names like `user.name` or `id.keyword` resolve. Returns the
/// leaf field's mapping node, or `None` if any segment is absent.
fn resolve_field_mapping<'a>(properties: &'a JsonValue, field: &str) -> Option<&'a JsonValue> {
    let segments: Vec<&str> = field.split('.').collect();
    let mut current = properties;
    let mut leaf = None;
    for (idx, segment) in segments.iter().enumerate() {
        let node = current.get(segment)?;
        if idx == segments.len() - 1 {
            leaf = Some(node);
        } else {
            current = node.get("properties").or_else(|| node.get("fields"))?;
        }
    }
    leaf
}

/// A [`PollingDataSource`] backed by the Elasticsearch
/// [Search API](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-search.html).
///
/// Each fetch issues a `range` query on the timestamp column, sorted ascending,
/// paginating through the full result with `search_after`. The generic
/// [`PollingReader`] layer owns all watermark and deduplication bookkeeping; this
/// type only translates "rows at or after `watermark`" into Elasticsearch
/// queries and parses the hits back out.
pub struct ElasticSearchPollingSource {
    runtime: TokioRuntime,
    client: Elasticsearch,
    index_name: String,
    timestamp_field: String,
    id_field: String,
}

impl ElasticSearchPollingSource {
    pub fn new(
        client: Elasticsearch,
        index_name: String,
        timestamp_field: String,
        id_field: String,
    ) -> Result<Self, ReadError> {
        Ok(Self {
            runtime: create_async_tokio_runtime()?,
            client,
            index_name,
            timestamp_field,
            id_field,
        })
    }

    /// Pull the `id_column` value out of a document's `_source`, canonicalizing it
    /// to a string so it can be deduplicated and persisted regardless of whether
    /// the underlying field is a string or a number.
    fn extract_id(&self, source: &JsonValue) -> Result<String, ElasticSearchError> {
        match source.get(&self.id_field) {
            None | Some(JsonValue::Null) => {
                Err(ElasticSearchError::MissingField(self.id_field.clone()))
            }
            Some(JsonValue::String(s)) => Ok(s.clone()),
            Some(other) => Ok(other.to_string()),
        }
    }

    /// Pull the `timestamp_column` value out of a document's `_source` as an
    /// integer. The column is required to be numeric (epoch milliseconds or any
    /// monotonically advancing integer) so the watermark arithmetic and the
    /// `now - max_transaction_duration` comparison are well-defined.
    fn extract_timestamp(&self, source: &JsonValue) -> Result<i64, ElasticSearchError> {
        match source.get(&self.timestamp_field) {
            None | Some(JsonValue::Null) => Err(ElasticSearchError::MissingField(
                self.timestamp_field.clone(),
            )),
            Some(value) => value
                .as_i64()
                .ok_or_else(|| ElasticSearchError::InvalidTimestamp {
                    column: self.timestamp_field.clone(),
                    value: value.to_string(),
                }),
        }
    }

    /// Fetch at most `limit` rows with `timestamp_field >= watermark`, ordered by
    /// `(timestamp_field, id_field)` ascending. `limit` may exceed
    /// `index.max_result_window`, so the page is assembled across as many
    /// `search_after` requests as needed, each capped at [`SEARCH_PAGE_SIZE`].
    async fn fetch_async(
        &self,
        watermark: i64,
        limit: usize,
    ) -> Result<Vec<PolledRow>, ElasticSearchError> {
        let mut rows = Vec::with_capacity(limit.min(SEARCH_PAGE_SIZE));
        let mut search_after: Option<JsonValue> = None;

        while rows.len() < limit {
            let request_size = (limit - rows.len()).min(SEARCH_PAGE_SIZE);

            // Built field-by-field rather than with `json!` because the
            // timestamp/id column names are runtime values and `json!` only
            // accepts literal object keys.
            let range = json!({ "gte": watermark });
            let query = json!({
                "range": { self.timestamp_field.as_str(): range }
            });
            let sort = JsonValue::Array(vec![
                json!({ self.timestamp_field.as_str(): "asc" }),
                json!({ self.id_field.as_str(): "asc" }),
            ]);
            let mut body = json!({
                "size": request_size,
                "query": query,
                "sort": sort,
            });
            if let Some(after) = &search_after {
                body["search_after"] = after.clone();
            }

            let response = self
                .client
                .search(SearchParts::Index(&[&self.index_name]))
                .body(body)
                .send()
                .await?
                .error_for_status_code()?;
            let payload: JsonValue = response.json().await?;

            let hits = payload["hits"]["hits"].as_array().ok_or_else(|| {
                ElasticSearchError::MalformedResponse(
                    "missing 'hits.hits' array in search response".to_string(),
                )
            })?;
            if hits.is_empty() {
                break;
            }

            for hit in hits {
                let source = &hit["_source"];
                let id = self.extract_id(source)?;
                let timestamp = self.extract_timestamp(source)?;
                let raw = serde_json::to_vec(source).map_err(|e| {
                    ElasticSearchError::MalformedResponse(format!(
                        "failed to serialize document _source: {e}"
                    ))
                })?;
                rows.push(PolledRow { id, timestamp, raw });
            }

            // A short underlying page means the index is exhausted at or after the
            // watermark, so the assembled page is complete even if below `limit`.
            if hits.len() < request_size {
                break;
            }
            search_after = Some(hits[hits.len() - 1]["sort"].clone());
        }

        Ok(rows)
    }

    /// The largest `timestamp_field` value currently in the index, or `None` if
    /// the index is empty. Computed with a `max` aggregation so it reflects the
    /// live edge regardless of how far the paged [`fetch_async`](Self::fetch_async)
    /// has read.
    async fn max_timestamp_async(&self) -> Result<Option<i64>, ElasticSearchError> {
        let body = json!({
            "size": 0,
            "aggs": { "max_ts": { "max": { "field": self.timestamp_field.as_str() } } },
        });
        let response = self
            .client
            .search(SearchParts::Index(&[&self.index_name]))
            .body(body)
            .send()
            .await?
            .error_for_status_code()?;
        let payload: JsonValue = response.json().await?;

        let value = &payload["aggregations"]["max_ts"]["value"];
        if value.is_null() {
            return Ok(None);
        }
        // The `max` aggregation reports its result as a JSON number that may be
        // serialized as a float even for integer fields; accept either. An
        // epoch-millisecond timestamp is exactly representable in an `f64` (well
        // under 2^53), so the cast back to `i64` does not lose precision.
        #[allow(clippy::cast_possible_truncation)]
        let max_ts = value
            .as_i64()
            .or_else(|| value.as_f64().map(|v| v as i64))
            .ok_or_else(|| ElasticSearchError::InvalidTimestamp {
                column: self.timestamp_field.clone(),
                value: value.to_string(),
            })?;
        Ok(Some(max_ts))
    }

    /// The number of documents with `timestamp_field >= lower_bound`. A `size: 0`
    /// search with `track_total_hits` returns an exact total cheaply (no hits are
    /// fetched, only the count is computed from the index).
    async fn count_from_async(&self, lower_bound: i64) -> Result<u64, ElasticSearchError> {
        let body = json!({
            "size": 0,
            "track_total_hits": true,
            "query": { "range": { self.timestamp_field.as_str(): { "gte": lower_bound } } },
        });
        let response = self
            .client
            .search(SearchParts::Index(&[&self.index_name]))
            .body(body)
            .send()
            .await?
            .error_for_status_code()?;
        let payload: JsonValue = response.json().await?;

        payload["hits"]["total"]["value"].as_u64().ok_or_else(|| {
            ElasticSearchError::MalformedResponse(
                "missing 'hits.total.value' count in search response".to_string(),
            )
        })
    }

    /// The live-edge fingerprint: the maximum timestamp and the number of
    /// documents in the overlap window `[max - max_transaction_duration, max]`.
    /// `None` if the index is empty.
    async fn live_state_async(
        &self,
        max_transaction_duration: i64,
    ) -> Result<Option<LiveState>, ElasticSearchError> {
        let Some(max_timestamp) = self.max_timestamp_async().await? else {
            return Ok(None);
        };
        let lower_bound = max_timestamp.saturating_sub(max_transaction_duration);
        let overlap_count = self.count_from_async(lower_bound).await?;
        Ok(Some(LiveState {
            max_timestamp,
            overlap_count,
        }))
    }

    async fn fetch_mappings_async(&self) -> Result<JsonValue, ElasticSearchError> {
        let response = self
            .client
            .indices()
            .get_mapping(IndicesGetMappingParts::Index(&[&self.index_name]))
            .send()
            .await?
            .error_for_status_code()?;
        Ok(response.json().await?)
    }

    /// Best-effort startup check that the two polling fields are indexed in a way
    /// the connector can actually use: `timestamp_column` numeric/date with
    /// `doc_values` and `index` enabled (range, sort, `max` aggregation), and
    /// `id_column` sortable (so `search_after` pagination is a total order).
    ///
    /// Elasticsearch indexes fields by default, so a correctly-mapped index emits
    /// nothing. Problems are reported with [`warn!`] rather than raising an error:
    /// the connector still runs, but the operator is told why polling may misbehave
    /// (e.g. a string `id_column` dynamically mapped as `text`, which is not
    /// sortable). If the mapping cannot be fetched at all (e.g. the index does not
    /// exist yet) a single warning is emitted and the check is skipped.
    fn warn_on_unindexed_polling_fields(&self) {
        let mappings = match self
            .runtime
            .block_on(async { self.fetch_mappings_async().await })
        {
            Ok(mappings) => mappings,
            Err(error) => {
                warn!(
                    "Could not read the Elasticsearch mapping of index {:?} to verify that the \
                     polling fields {:?} and {:?} are indexed (the connector will still run): \
                     {error}",
                    self.index_name, self.timestamp_field, self.id_field
                );
                return;
            }
        };

        let Some(indices) = mappings.as_object() else {
            return;
        };
        for (index, body) in indices {
            let properties = &body["mappings"]["properties"];
            if !properties.is_object() {
                warn!(
                    "Elasticsearch index {index:?} has no field mappings yet (it may be empty), so \
                     the connector cannot verify that the polling fields {:?} and {:?} are indexed.",
                    self.timestamp_field, self.id_field
                );
                continue;
            }
            self.warn_on_timestamp_mapping(index, properties);
            self.warn_on_id_mapping(index, properties);
        }
    }

    fn warn_on_timestamp_mapping(&self, index: &str, properties: &JsonValue) {
        let field = &self.timestamp_field;
        let Some(node) = resolve_field_mapping(properties, field) else {
            warn!(
                "Elasticsearch index {index:?} has no mapping for the timestamp_column {field:?}; \
                 polling needs it to be a numeric or date field with doc_values enabled — range \
                 queries, sorting, and the max aggregation used for watermarking all rely on it."
            );
            return;
        };
        let field_type = node
            .get("type")
            .and_then(JsonValue::as_str)
            .unwrap_or("object");
        if !NUMERIC_FIELD_TYPES.contains(&field_type) {
            warn!(
                "Elasticsearch timestamp_column {field:?} in index {index:?} is mapped as \
                 {field_type:?}, not a numeric or date type; the connector reads it as an integer \
                 and orders and watermarks by it, so polling will not work correctly."
            );
        }
        if node.get("doc_values").and_then(JsonValue::as_bool) == Some(false) {
            warn!(
                "Elasticsearch timestamp_column {field:?} in index {index:?} has doc_values \
                 disabled; sorting and the max aggregation used for watermarking require doc_values."
            );
        }
        if node.get("index").and_then(JsonValue::as_bool) == Some(false) {
            warn!(
                "Elasticsearch timestamp_column {field:?} in index {index:?} has indexing disabled; \
                 the range queries used for polling require the field to be indexed."
            );
        }
    }

    fn warn_on_id_mapping(&self, index: &str, properties: &JsonValue) {
        let field = &self.id_field;
        let Some(node) = resolve_field_mapping(properties, field) else {
            warn!(
                "Elasticsearch index {index:?} has no mapping for the id_column {field:?}; polling \
                 sorts by it for pagination, so it must be a sortable field (keyword or numeric)."
            );
            return;
        };
        let field_type = node
            .get("type")
            .and_then(JsonValue::as_str)
            .unwrap_or("object");
        if field_type == "text" {
            warn!(
                "Elasticsearch id_column {field:?} in index {index:?} is mapped as 'text', which is \
                 not sortable; polling sorts by it for pagination. Use a keyword or numeric field \
                 (for a string id this is usually the {field:?}.keyword sub-field)."
            );
        } else if !SORTABLE_FIELD_TYPES.contains(&field_type) {
            warn!(
                "Elasticsearch id_column {field:?} in index {index:?} is mapped as {field_type:?}, \
                 which may not be sortable; polling sorts by it for pagination, so use a keyword or \
                 numeric field."
            );
        }
        if node.get("doc_values").and_then(JsonValue::as_bool) == Some(false) {
            warn!(
                "Elasticsearch id_column {field:?} in index {index:?} has doc_values disabled; the \
                 sorting used for pagination requires doc_values."
            );
        }
    }
}

impl PollingDataSource for ElasticSearchPollingSource {
    fn fetch(&mut self, min_watermark: i64, limit: usize) -> Result<Vec<PolledRow>, ReadError> {
        let rows = self
            .runtime
            .block_on(async { self.fetch_async(min_watermark, limit).await })?;
        Ok(rows)
    }

    fn live_state(
        &mut self,
        max_transaction_duration: i64,
    ) -> Result<Option<LiveState>, ReadError> {
        let state = self
            .runtime
            .block_on(async { self.live_state_async(max_transaction_duration).await })?;
        Ok(state)
    }

    fn storage_type(&self) -> StorageType {
        StorageType::ElasticSearch
    }

    fn offset_key(&self) -> OffsetKey {
        OffsetKey::ElasticSearch
    }

    fn short_description(&self) -> Cow<'static, str> {
        format!("ElasticSearch({})", self.index_name).into()
    }
}

/// A polling Elasticsearch input connector.
pub type ElasticSearchReader = PollingReader<ElasticSearchPollingSource>;

/// Build an [`ElasticSearchReader`] from a configured client and the polling
/// parameters supplied through the Python API.
#[allow(clippy::too_many_arguments)]
pub fn build_elasticsearch_reader(
    client: Elasticsearch,
    index_name: String,
    timestamp_field: String,
    id_field: String,
    mode: ConnectorMode,
    max_transaction_duration_ms: i64,
    read_batch_size: usize,
    poll_interval: Duration,
) -> Result<ElasticSearchReader, ReadError> {
    let source = ElasticSearchPollingSource::new(client, index_name, timestamp_field, id_field)?;
    source.warn_on_unindexed_polling_fields();
    Ok(PollingReader::new(
        source,
        mode,
        max_transaction_duration_ms,
        read_batch_size,
        poll_interval,
    ))
}

pub struct ElasticSearchWriter {
    runtime: TokioRuntime,
    client: Elasticsearch,
    index_name: String,
    max_batch_size: Option<usize>,

    docs_buffer: Vec<Vec<u8>>,
}

impl ElasticSearchWriter {
    pub fn new(
        client: Elasticsearch,
        index_name: String,
        max_batch_size: Option<usize>,
    ) -> Result<Self, WriteError> {
        Ok(ElasticSearchWriter {
            runtime: create_async_tokio_runtime()?,
            client,
            index_name,
            max_batch_size,
            docs_buffer: Vec::new(),
        })
    }
}

impl Writer for ElasticSearchWriter {
    fn write(&mut self, data: FormatterContext) -> Result<(), WriteError> {
        for payload in data.payloads {
            self.docs_buffer.push(b"{\"index\": {}}".to_vec());
            self.docs_buffer.push(payload.into_raw_bytes()?);
        }

        if let Some(max_batch_size) = self.max_batch_size {
            if self.docs_buffer.len() / 2 >= max_batch_size {
                self.flush(true)?;
            }
        }

        Ok(())
    }

    fn flush(&mut self, _forced: bool) -> Result<(), WriteError> {
        if self.docs_buffer.is_empty() {
            return Ok(());
        }
        self.runtime.block_on(async {
            self.client
                .bulk(BulkParts::Index(&self.index_name))
                .body(take(&mut self.docs_buffer))
                .send()
                .await
                .map_err(WriteError::Elasticsearch)?
                .error_for_status_code()
                .map_err(WriteError::Elasticsearch)?;

            Ok(())
        })
    }

    fn name(&self) -> String {
        format!("ElasticSearch({})", self.index_name)
    }

    fn single_threaded(&self) -> bool {
        false
    }
}
