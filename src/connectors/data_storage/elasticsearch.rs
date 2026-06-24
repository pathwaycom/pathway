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
use crate::retry::{execute_with_retries_async, RetryConfig};

use elasticsearch::indices::IndicesGetMappingParts;
use elasticsearch::nodes::{NodesInfoParts, NodesStatsParts};
use elasticsearch::{BulkParts, Elasticsearch, SearchParts};
use log::warn;
use serde_json::{json, Value as JsonValue};
use tokio::runtime::Runtime as TokioRuntime;

/// Errors specific to the Elasticsearch connector. Exposed to the engine through
/// single transparent [`ReadError::ElasticSearch`](crate::connectors::ReadError)
/// and [`WriteError::ElasticSearch`](crate::connectors::WriteError) variants so
/// `?` works directly on these results inside the reader and writer.
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

    #[error(
        "ElasticSearch bulk indexing into {index_name:?} still failed for {failed_count} \
         document(s) with retriable errors after {retries} retries: {reasons}"
    )]
    BulkRetriesExhausted {
        index_name: String,
        failed_count: usize,
        retries: usize,
        reasons: String,
    },
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

/// The action line prepended before every document in the NDJSON `_bulk` body.
/// Each buffered document is two NDJSON lines: this action line, then the
/// document itself.
const BULK_ACTION_LINE: &[u8] = b"{\"index\": {}}";

/// `http.max_content_length` assumed when the live value cannot be read from the
/// cluster. This matches Elasticsearch's own default (100 MB); a bulk request
/// larger than this is rejected with `413 Request Entity Too Large`, so the
/// writer splits its buffer to stay under it.
const DEFAULT_MAX_CONTENT_LENGTH: usize = 100 * 1024 * 1024;

/// `indexing_pressure.memory.limit` assumed when the live value cannot be read
/// from the cluster. Elasticsearch defaults this to 10% of the JVM heap; 50 MB
/// corresponds to a small (~512 MB-heap) node, so it is a safe conservative
/// fallback — a real cluster almost always reports a larger value, lifting the
/// cap accordingly.
const DEFAULT_INDEXING_PRESSURE_LIMIT: usize = 50 * 1024 * 1024;

/// Fraction (numerator/denominator) of `http.max_content_length` a single bulk
/// request is allowed to fill. The headroom absorbs the small difference between
/// the NDJSON body bytes we measure and whatever the server counts toward the
/// limit, so a request never tips over the hard cap on a rounding error.
const BATCH_BYTE_BUDGET_NUM: usize = 9;
const BATCH_BYTE_BUDGET_DEN: usize = 10;

/// How many times the start-up limit probes are retried (with backoff) before
/// giving up and falling back to a default. These run once at construction, so a
/// few attempts cheaply ride out a transient blip (a node briefly unreachable,
/// the cluster still forming) without making a degraded cluster wait long.
const LIMIT_PROBE_RETRIES: usize = 3;

/// How many times a bulk request is retried (with backoff) when it — or some of
/// its documents — fail with a *transient* error. Only work that the server
/// reports as not yet applied is resent, so a retry never duplicates a document.
const BULK_WRITE_RETRIES: usize = 4;

/// HTTP status codes (whether the whole bulk request was rejected, or an
/// individual document failed inside an otherwise-`200` response) that the writer
/// retries.
///
/// Both are back-pressure / transient-unavailability signals that Elasticsearch
/// raises *before* applying the affected operation, so the document is known not
/// to have been indexed and resending it cannot duplicate it. Deterministic
/// failures (`400` bad document, `401`/`403` auth, `404` missing index, `413`
/// single oversized document) are never retried — they would fail identically —
/// and ambiguous transport/gateway failures (a lost response, a proxy `502`/`504`)
/// are not retried either, since the request may have been applied and resending
/// it could duplicate data (this connector does not assign document ids, so a
/// resend is not idempotent).
fn is_retriable_bulk_status(status: u16) -> bool {
    matches!(status, 429 | 503)
}

/// Number of bytes a single buffered document contributes to the NDJSON `_bulk`
/// body: the action line and the document, each followed by a newline (the
/// `elasticsearch` client's newline-delimited body writer appends a `\n` after
/// every line).
fn bulk_entry_byte_size(payload_len: usize) -> usize {
    BULK_ACTION_LINE.len() + 1 + payload_len + 1
}

/// Read `http.max_content_length` (in bytes) from the cluster's node info.
///
/// Each node enforces its own limit on the size of an inbound HTTP body; the
/// smallest one is returned so a bulk request fits whichever node handles it.
/// Fails if the node info cannot be fetched, parsed, or contains no node
/// reporting the limit — the caller falls back to [`DEFAULT_MAX_CONTENT_LENGTH`]
/// in that case.
async fn fetch_max_content_length(client: &Elasticsearch) -> Result<usize, ElasticSearchError> {
    let response = client
        .nodes()
        .info(NodesInfoParts::Metric(&["http"]))
        .send()
        .await?
        .error_for_status_code()?;
    let payload: JsonValue = response.json().await?;
    let nodes = payload["nodes"].as_object().ok_or_else(|| {
        ElasticSearchError::MalformedResponse(
            "missing 'nodes' object in node info response".to_string(),
        )
    })?;
    let mut limit: Option<usize> = None;
    for node in nodes.values() {
        if let Some(value) = node["http"]["max_content_length_in_bytes"].as_u64() {
            let value = usize::try_from(value).unwrap_or(usize::MAX);
            limit = Some(limit.map_or(value, |current| current.min(value)));
        }
    }
    limit.ok_or_else(|| {
        ElasticSearchError::MalformedResponse(
            "no node reported http.max_content_length_in_bytes".to_string(),
        )
    })
}

/// Read `indexing_pressure.memory.limit` (in bytes) from the cluster's node
/// stats.
///
/// Independently of the HTTP body limit, Elasticsearch rejects a bulk request
/// with `429 Too Many Requests` once the in-flight indexing memory it would use
/// (counted on both the coordinating and the primary node) exceeds this limit,
/// which defaults to 10% of the JVM heap. The smallest limit across nodes is
/// returned so a request fits whichever node coordinates or holds the primary
/// shard. Fails (so the caller falls back to [`DEFAULT_INDEXING_PRESSURE_LIMIT`])
/// if the stats cannot be fetched, parsed, or report no limit.
async fn fetch_indexing_pressure_limit(
    client: &Elasticsearch,
) -> Result<usize, ElasticSearchError> {
    let response = client
        .nodes()
        .stats(NodesStatsParts::Metric(&["indexing_pressure"]))
        .send()
        .await?
        .error_for_status_code()?;
    let payload: JsonValue = response.json().await?;
    let nodes = payload["nodes"].as_object().ok_or_else(|| {
        ElasticSearchError::MalformedResponse(
            "missing 'nodes' object in node stats response".to_string(),
        )
    })?;
    let mut limit: Option<usize> = None;
    for node in nodes.values() {
        if let Some(value) = node["indexing_pressure"]["memory"]["limit_in_bytes"].as_u64() {
            let value = usize::try_from(value).unwrap_or(usize::MAX);
            limit = Some(limit.map_or(value, |current| current.min(value)));
        }
    }
    limit.ok_or_else(|| {
        ElasticSearchError::MalformedResponse(
            "no node reported indexing_pressure.memory.limit_in_bytes".to_string(),
        )
    })
}

pub struct ElasticSearchWriter {
    runtime: TokioRuntime,
    client: Elasticsearch,
    index_name: String,
    max_batch_size: Option<usize>,
    // Upper bound on the size (in bytes) of a single bulk request body, kept
    // safely below the cluster's `http.max_content_length` so the server never
    // rejects a batch as too large.
    max_batch_bytes: usize,

    docs_buffer: Vec<Vec<u8>>,
    // Running size of the NDJSON body the buffered documents would serialize to,
    // tracked incrementally so the byte budget can be checked without re-scanning
    // the buffer on every write.
    docs_buffer_bytes: usize,
}

impl ElasticSearchWriter {
    pub fn new(
        client: Elasticsearch,
        index_name: String,
        max_batch_size: Option<usize>,
    ) -> Result<Self, WriteError> {
        let runtime = create_async_tokio_runtime()?;
        // Discover the two server-side limits that bound a single bulk request
        // once at construction. Either being unreadable falls back to a
        // conservative documented default rather than failing the connector — an
        // over-large batch is still avoided.
        //
        // `http.max_content_length` is the hard cap on the HTTP body (a larger
        // request is rejected with 413). `indexing_pressure.memory.limit` is the
        // in-flight indexing memory a request may use before it is rejected with
        // 429; a bulk of B bytes counts against it on both the coordinating and
        // the primary node (~2·B at the peak), so a request must stay below half
        // of it. The batch is bounded by whichever limit is hit first.
        // A transient error (a slow or briefly-unreachable node) is retried with
        // backoff before falling back, so a momentary hiccup at start-up does not
        // needlessly drop the connector onto the conservative default for the
        // whole run.
        let max_content_length = match runtime.block_on(async {
            execute_with_retries_async(
                async || fetch_max_content_length(&client).await,
                RetryConfig::default(),
                LIMIT_PROBE_RETRIES,
            )
            .await
        }) {
            Ok(limit) => limit,
            Err(error) => {
                warn!(
                    "Could not read Elasticsearch http.max_content_length (using the \
                     {DEFAULT_MAX_CONTENT_LENGTH}-byte default to bound bulk request size): {error}"
                );
                DEFAULT_MAX_CONTENT_LENGTH
            }
        };
        let indexing_pressure_limit = match runtime.block_on(async {
            execute_with_retries_async(
                async || fetch_indexing_pressure_limit(&client).await,
                RetryConfig::default(),
                LIMIT_PROBE_RETRIES,
            )
            .await
        }) {
            Ok(limit) => limit,
            Err(error) => {
                warn!(
                    "Could not read Elasticsearch indexing_pressure.memory.limit (using the \
                     {DEFAULT_INDEXING_PRESSURE_LIMIT}-byte default to bound bulk request size): \
                     {error}"
                );
                DEFAULT_INDEXING_PRESSURE_LIMIT
            }
        };
        let content_length_budget =
            (max_content_length / BATCH_BYTE_BUDGET_DEN).saturating_mul(BATCH_BYTE_BUDGET_NUM);
        let indexing_pressure_budget = indexing_pressure_limit / 2;
        let max_batch_bytes = content_length_budget.min(indexing_pressure_budget).max(1);
        Ok(ElasticSearchWriter {
            runtime,
            client,
            index_name,
            max_batch_size,
            max_batch_bytes,
            docs_buffer: Vec::new(),
            docs_buffer_bytes: 0,
        })
    }
}

/// The per-document outcome of a bulk response whose body reported `errors: true`.
#[derive(Default)]
struct BulkItemFailures {
    /// The NDJSON lines (action line followed by document, as stored in the send
    /// buffer) of the documents that failed with a retriable status and should be
    /// resent. Empty when nothing is worth retrying.
    retriable_docs: Vec<Vec<u8>>,
    /// Reasons for the retriable failures, for diagnostics if the retries run out.
    retriable_reasons: Vec<String>,
    /// Reasons for documents that failed deterministically; these are not retried.
    permanent_reasons: Vec<String>,
}

/// Inspect a `200`-but-`errors: true` bulk response and split the failed
/// documents into those worth retrying and those that failed permanently.
///
/// `sent` is the flat NDJSON buffer that produced the request: document `i`
/// occupies `sent[2*i]` (its action line) and `sent[2*i + 1]` (its body), in the
/// same order as `items` in the response. Successful items are skipped, so only
/// the still-unwritten documents are carried into the next attempt — a retry
/// therefore never resends an already-indexed document.
fn classify_bulk_item_failures(payload: &JsonValue, sent: &[Vec<u8>]) -> BulkItemFailures {
    let mut failures = BulkItemFailures::default();
    let Some(items) = payload["items"].as_array() else {
        return failures;
    };
    for (index, item) in items.iter().enumerate() {
        // Each item is a single-key object keyed by the action (`index`), whose
        // value carries the per-document `status` and, on failure, an `error`.
        let Some(result) = item.as_object().and_then(|object| object.values().next()) else {
            continue;
        };
        let status = result["status"].as_u64().unwrap_or(0);
        if (200..300).contains(&status) {
            continue;
        }
        let reason = result["error"]["reason"]
            .as_str()
            .unwrap_or("unknown error");
        let reason = format!("status {status}: {reason}");
        #[allow(clippy::cast_possible_truncation)]
        if is_retriable_bulk_status(status as u16) {
            if let (Some(action), Some(document)) = (sent.get(2 * index), sent.get(2 * index + 1)) {
                failures.retriable_docs.push(action.clone());
                failures.retriable_docs.push(document.clone());
                failures.retriable_reasons.push(reason);
            }
        } else {
            failures.permanent_reasons.push(reason);
        }
    }
    failures
}

/// Join failure reasons into a short, bounded summary for a log line or error.
fn summarize_failure_reasons(reasons: &[String]) -> String {
    const MAX_SHOWN: usize = 5;
    let shown = reasons
        .iter()
        .take(MAX_SHOWN)
        .cloned()
        .collect::<Vec<_>>()
        .join("; ");
    if reasons.len() > MAX_SHOWN {
        format!("{shown}; … and {} more", reasons.len() - MAX_SHOWN)
    } else {
        shown
    }
}

impl Writer for ElasticSearchWriter {
    fn write(&mut self, data: FormatterContext) -> Result<(), WriteError> {
        for payload in data.payloads {
            let payload = payload.into_raw_bytes()?;
            let entry_bytes = bulk_entry_byte_size(payload.len());

            // Flush the buffered documents before this one would push the bulk
            // request body over the size the server accepts. A document larger
            // than the whole budget cannot be split, so it is still buffered and
            // sent on its own; flushing first at least keeps every other request
            // valid instead of failing the entire batch.
            if !self.docs_buffer.is_empty()
                && self.docs_buffer_bytes + entry_bytes > self.max_batch_bytes
            {
                self.flush(true)?;
            }

            self.docs_buffer.push(BULK_ACTION_LINE.to_vec());
            self.docs_buffer.push(payload);
            self.docs_buffer_bytes += entry_bytes;

            if let Some(max_batch_size) = self.max_batch_size {
                if self.docs_buffer.len() / 2 >= max_batch_size {
                    self.flush(true)?;
                }
            }
        }

        Ok(())
    }

    fn flush(&mut self, _forced: bool) -> Result<(), WriteError> {
        if self.docs_buffer.is_empty() {
            return Ok(());
        }
        self.docs_buffer_bytes = 0;
        // The set of documents still to write, narrowing on each retry to only the
        // ones the server reported as not yet applied. Borrowed by reference into
        // the request body so retrying re-sends without copying the payloads.
        let mut remaining = take(&mut self.docs_buffer);
        let result: Result<(), ElasticSearchError> = self.runtime.block_on(async {
            let mut retry_config = RetryConfig::default();
            let mut retries_left = BULK_WRITE_RETRIES;
            loop {
                let body: Vec<&[u8]> = remaining.iter().map(Vec::as_slice).collect();
                let response = self
                    .client
                    .bulk(BulkParts::Index(&self.index_name))
                    .body(body)
                    .send()
                    .await?;

                if response.status_code().is_success() {
                    let payload: JsonValue = response.json().await?;
                    if !payload["errors"].as_bool().unwrap_or(false) {
                        return Ok(());
                    }
                    let failures = classify_bulk_item_failures(&payload, &remaining);
                    if !failures.permanent_reasons.is_empty() {
                        // These documents would fail identically on every retry,
                        // so retrying (or failing the whole run, which would
                        // re-send the batch on recovery and loop forever) cannot
                        // help. Report them and move on with the rest.
                        warn!(
                            "ElasticSearch index {:?}: dropped {} document(s) rejected with \
                             non-retriable errors: {}",
                            self.index_name,
                            failures.permanent_reasons.len(),
                            summarize_failure_reasons(&failures.permanent_reasons),
                        );
                    }
                    if failures.retriable_docs.is_empty() {
                        return Ok(());
                    }
                    if retries_left == 0 {
                        return Err(ElasticSearchError::BulkRetriesExhausted {
                            index_name: self.index_name.clone(),
                            failed_count: failures.retriable_docs.len() / 2,
                            retries: BULK_WRITE_RETRIES,
                            reasons: summarize_failure_reasons(&failures.retriable_reasons),
                        });
                    }
                    remaining = failures.retriable_docs;
                } else {
                    let status = response.status_code().as_u16();
                    if !is_retriable_bulk_status(status) || retries_left == 0 {
                        // Non-retriable rejection, or out of retries. The whole
                        // request was refused before indexing, so nothing was
                        // written; surface the server's error.
                        response.error_for_status_code()?;
                        unreachable!("error_for_status_code returns Err on a non-success status");
                    }
                    // Retriable top-level rejection: the batch was refused
                    // wholesale, so `remaining` is unchanged and re-sending it
                    // duplicates nothing.
                }

                retries_left -= 1;
                retry_config.sleep_after_error_async().await;
            }
        });
        result.map_err(WriteError::from)
    }

    fn name(&self) -> String {
        format!("ElasticSearch({})", self.index_name)
    }

    fn single_threaded(&self) -> bool {
        false
    }
}
