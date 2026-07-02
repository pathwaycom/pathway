use std::collections::HashMap;
use std::mem::take;
use std::time::Duration;

use log::error;
use reqwest::blocking::Client;
use serde_json::{json, Map as JsonMap, Value as JsonValue};

use crate::connectors::data_format::FormatterContext;
use crate::connectors::{WriteError, Writer};
use crate::engine::{Key, Value};
use crate::python_api::ValueField;
use crate::retry::{execute_with_retries_if, RetryConfig};

// Pinecone REST API version pinned in the request header. Matches the version
// the official client sends; the data-plane endpoints used here
// (`/vectors/upsert`, `/vectors/delete`) are stable across recent versions.
const API_VERSION: &str = "2025-10";
const API_KEY_HEADER: &str = "Api-Key";
const API_VERSION_HEADER: &str = "X-Pinecone-Api-Version";
const DEFAULT_CONTROL_HOST: &str = "https://api.pinecone.io";

// Pinecone rejects upsert requests whose body exceeds 2 MB with HTTP 413. Keep a
// margin below that so a batch of high-dimensional vectors is split into several
// requests instead of failing the run.
const MAX_UPSERT_BYTES: usize = 1_900_000;
// Pinecone allows at most 1000 ids per `delete` call and at most 1000 vectors
// per `upsert` call on serverless indexes.
const DELETE_BATCH_SIZE: usize = 1000;
const MAX_UPSERT_COUNT: usize = 1000;
const DEFAULT_UPSERT_COUNT: usize = 100;
const N_SEND_ATTEMPTS: usize = 5;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, thiserror::Error)]
pub enum PineconeError {
    #[error("Pinecone HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),

    #[error("Pinecone API returned status {status}: {body}")]
    Api { status: u16, body: String },

    #[error("could not resolve the data-plane host for Pinecone index {0:?}: {1}")]
    HostResolution(String, String),

    #[error(
        "metadata column {column:?} contains a value of unsupported type {type_name:?}. \
         Pinecone metadata supports str, int, float, bool, and list[str] only"
    )]
    UnsupportedMetadataType { column: String, type_name: String },

    #[error("vector column {0:?} is not a list[float] or a 1-D float array")]
    InvalidVector(String),

    #[error(
        "vector column {0:?} contains a non-finite value (NaN or infinity); \
         a Pinecone vector must contain only finite numbers"
    )]
    NonFiniteVector(String),

    #[error(
        "metadata column {0:?} contains a non-finite value (NaN or infinity); \
         Pinecone metadata cannot store NaN or infinity"
    )]
    NonFiniteMetadata(String),

    #[error(
        "primary key column {0:?} cannot be used as a Pinecone id (must be int, str, or a pointer)"
    )]
    InvalidId(String),

    #[error(
        "two rows map to the same Pinecone record id {id:?} via primary_key column \
         {column:?}; the primary_key must uniquely identify rows, otherwise records \
         silently overwrite each other in the index"
    )]
    DuplicateRecordId { id: String, column: String },
}

fn is_retriable(error: &PineconeError) -> bool {
    match error {
        // Network-level blips (timeouts, dropped connections) are worth retrying.
        PineconeError::Http(e) => e.is_timeout() || e.is_connect() || e.is_request(),
        // 429 (rate limit) and 5xx (server-side) are transient; 4xx are not.
        PineconeError::Api { status, .. } => *status == 429 || (500..600).contains(status),
        _ => false,
    }
}

enum PineconeEvent {
    // The record's pre-serialized JSON object, e.g. {"id":..,"values":[..],"metadata":{..}}.
    Upsert(String),
    Delete,
}

pub struct PineconeWriter {
    client: Client,
    upsert_url: String,
    delete_url: String,
    api_key: String,
    namespace: String,
    value_fields: Vec<ValueField>,
    // Index of the user-chosen primary_key column, or `None` to use the row's
    // internal key (pointer) as the Pinecone record id.
    pk_index: Option<usize>,
    vector_index: usize,
    metadata_indices: Vec<usize>,
    upsert_count_cap: usize,
    // Snapshot buffer: keyed by Pinecone record id. Within one minibatch an
    // insertion takes precedence over a deletion of the same id (an update keeps
    // the freshly-inserted row); two insertions for the same id are a primary_key
    // collision and are rejected rather than allowed to silently overwrite.
    buffer: HashMap<String, PineconeEvent>,
}

impl PineconeWriter {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        api_key: String,
        control_host: Option<String>,
        index_name: &str,
        namespace: String,
        value_fields: Vec<ValueField>,
        pk_index: Option<usize>,
        vector_index: usize,
        metadata_indices: Vec<usize>,
        max_batch_size: Option<usize>,
    ) -> Result<Self, WriteError> {
        let client = Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()
            .map_err(PineconeError::from)?;
        let upsert_count_cap = max_batch_size
            .unwrap_or(DEFAULT_UPSERT_COUNT)
            .clamp(1, MAX_UPSERT_COUNT);
        let control_host = control_host.unwrap_or_else(|| DEFAULT_CONTROL_HOST.to_string());
        let index_host = Self::resolve_index_host(&client, &control_host, index_name, &api_key)?;
        Ok(Self {
            client,
            upsert_url: format!("{index_host}/vectors/upsert"),
            delete_url: format!("{index_host}/vectors/delete"),
            api_key,
            namespace,
            value_fields,
            pk_index,
            vector_index,
            metadata_indices,
            upsert_count_cap,
            buffer: HashMap::new(),
        })
    }

    // Resolve the per-index data-plane host via the control plane. Pinecone Local
    // serves plaintext but advertises a bare `host:port`, so the scheme is taken
    // from the control host (http:// for the local emulator, https:// otherwise).
    fn resolve_index_host(
        client: &Client,
        control_host: &str,
        index_name: &str,
        api_key: &str,
    ) -> Result<String, WriteError> {
        let url = format!("{control_host}/indexes/{index_name}");
        let response = client
            .get(&url)
            .header(API_KEY_HEADER, api_key)
            .header(API_VERSION_HEADER, API_VERSION)
            .send()
            .map_err(PineconeError::from)?;
        if !response.status().is_success() {
            let status = response.status().as_u16();
            let body = response.text().unwrap_or_default();
            return Err(PineconeError::HostResolution(
                index_name.to_string(),
                format!("status {status}: {body}"),
            )
            .into());
        }
        let body: JsonValue = response.json().map_err(PineconeError::from)?;
        let host = body["host"].as_str().ok_or_else(|| {
            PineconeError::HostResolution(
                index_name.to_string(),
                "the control plane response did not contain a host".to_string(),
            )
        })?;
        let bare = host
            .strip_prefix("https://")
            .or_else(|| host.strip_prefix("http://"))
            .unwrap_or(host);
        let scheme = if control_host.starts_with("http://") {
            "http"
        } else {
            "https"
        };
        Ok(format!("{scheme}://{bare}"))
    }

    fn record_id(&self, values: &[Value], key: &Key) -> Result<String, WriteError> {
        let Some(pk_index) = self.pk_index else {
            // No primary_key column: use the row's internal key as the record id.
            // It is unique and matches the dataflow's sharding, so writes are safe
            // to run in parallel across workers.
            return Ok(key.to_string());
        };
        match &values[pk_index] {
            Value::Int(i) => Ok(i.to_string()),
            Value::String(s) => Ok(s.to_string()),
            Value::Pointer(p) => Ok(p.to_string()),
            _ => Err(PineconeError::InvalidId(self.value_fields[pk_index].name.clone()).into()),
        }
    }

    #[allow(clippy::cast_precision_loss)]
    fn vector(&self, values: &[Value]) -> Result<Vec<f64>, WriteError> {
        let name = || self.value_fields[self.vector_index].name.clone();
        // JSON cannot represent NaN/infinity, so a non-finite component would be
        // serialized as `null` and rejected by Pinecone with an opaque error.
        // Catch it here and surface a clear message naming the column instead.
        let finite = |f: f64| -> Result<f64, WriteError> {
            if f.is_finite() {
                Ok(f)
            } else {
                Err(PineconeError::NonFiniteVector(name()).into())
            }
        };
        match &values[self.vector_index] {
            Value::Tuple(items) => items
                .iter()
                .map(|item| match item {
                    Value::Float(f) => finite(f.0),
                    Value::Int(i) => Ok(*i as f64),
                    _ => Err(PineconeError::InvalidVector(name()).into()),
                })
                .collect(),
            Value::FloatArray(array) => array.iter().copied().map(finite).collect(),
            Value::IntArray(array) => Ok(array.iter().map(|i| *i as f64).collect()),
            _ => Err(PineconeError::InvalidVector(name()).into()),
        }
    }

    fn metadata(&self, values: &[Value]) -> Result<JsonMap<String, JsonValue>, WriteError> {
        let mut metadata = JsonMap::new();
        for &index in &self.metadata_indices {
            let column = &self.value_fields[index].name;
            let json = match &values[index] {
                // None metadata is dropped: Pinecone rejects null metadata values.
                Value::None => continue,
                Value::Bool(b) => json!(b),
                Value::Int(i) => json!(i),
                Value::Float(f) => {
                    if !f.0.is_finite() {
                        return Err(PineconeError::NonFiniteMetadata(column.clone()).into());
                    }
                    json!(f.0)
                }
                Value::String(s) => json!(s),
                Value::Tuple(items) => {
                    let mut strings = Vec::with_capacity(items.len());
                    for item in items.iter() {
                        match item {
                            Value::String(s) => strings.push(s.to_string()),
                            other => {
                                return Err(PineconeError::UnsupportedMetadataType {
                                    column: column.clone(),
                                    type_name: format!("{:?}", other.kind()),
                                }
                                .into())
                            }
                        }
                    }
                    json!(strings)
                }
                other => {
                    return Err(PineconeError::UnsupportedMetadataType {
                        column: column.clone(),
                        type_name: format!("{:?}", other.kind()),
                    }
                    .into())
                }
            };
            metadata.insert(column.clone(), json);
        }
        Ok(metadata)
    }

    fn build_record(&self, values: &[Value], id: &str) -> Result<String, WriteError> {
        let mut record = JsonMap::with_capacity(3);
        record.insert("id".to_string(), json!(id));
        record.insert("values".to_string(), json!(self.vector(values)?));
        let metadata = self.metadata(values)?;
        if !metadata.is_empty() {
            record.insert("metadata".to_string(), JsonValue::Object(metadata));
        }
        Ok(JsonValue::Object(record).to_string())
    }

    fn post(&self, url: &str, body: &str) -> Result<(), WriteError> {
        let send = || -> Result<(), PineconeError> {
            let response = self
                .client
                .post(url)
                .header(API_KEY_HEADER, &self.api_key)
                .header(API_VERSION_HEADER, API_VERSION)
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .body(body.to_owned())
                .send()?;
            if response.status().is_success() {
                Ok(())
            } else {
                let status = response.status().as_u16();
                let body = response.text().unwrap_or_default();
                Err(PineconeError::Api { status, body })
            }
        };
        execute_with_retries_if(send, is_retriable, RetryConfig::default(), N_SEND_ATTEMPTS)
            .map_err(WriteError::from)
    }

    fn namespace_json(&self) -> String {
        serde_json::to_string(&self.namespace).expect("a string always serializes")
    }

    fn flush_deletes(&self, ids: &[String]) -> Result<(), WriteError> {
        let namespace = self.namespace_json();
        for chunk in ids.chunks(DELETE_BATCH_SIZE) {
            let ids_json = serde_json::to_string(chunk).expect("ids always serialize");
            let body = format!("{{\"ids\":{ids_json},\"namespace\":{namespace}}}");
            self.post(&self.delete_url, &body)?;
        }
        Ok(())
    }

    fn flush_upserts(&self, records: &[String]) -> Result<(), WriteError> {
        let namespace = self.namespace_json();
        let mut start = 0;
        while start < records.len() {
            let mut end = start;
            let mut bytes = 0;
            // Pack as many records as fit under both the request-size and count caps.
            while end < records.len() {
                let next = records[end].len() + 1; // +1 for the separating comma
                if end > start
                    && (end - start >= self.upsert_count_cap || bytes + next > MAX_UPSERT_BYTES)
                {
                    break;
                }
                bytes += next;
                end += 1;
            }
            let body = format!(
                "{{\"vectors\":[{}],\"namespace\":{}}}",
                records[start..end].join(","),
                namespace
            );
            self.post(&self.upsert_url, &body)?;
            start = end;
        }
        Ok(())
    }
}

impl Writer for PineconeWriter {
    fn write(&mut self, data: FormatterContext) -> Result<(), WriteError> {
        let id = self.record_id(&data.values, &data.key)?;
        match data.diff {
            1 => {
                // With a user-chosen primary_key column, two insertions for the
                // same id within a batch mean two distinct current rows claim the
                // same Pinecone record id (a non-unique primary_key); reject it
                // instead of silently overwriting. (Not possible when the id is the
                // row's internal key, which is unique.) An insertion still wins over
                // a pending deletion of the same id (a row being updated keeps its
                // freshly-inserted value).
                if let Some(pk_index) = self.pk_index {
                    if matches!(self.buffer.get(&id), Some(PineconeEvent::Upsert(_))) {
                        return Err(PineconeError::DuplicateRecordId {
                            id,
                            column: self.value_fields[pk_index].name.clone(),
                        }
                        .into());
                    }
                }
                let record = self.build_record(&data.values, &id)?;
                self.buffer.insert(id, PineconeEvent::Upsert(record));
            }
            -1 => {
                // Record the deletion only if no insertion for this id is already
                // pending in this batch, regardless of arrival order.
                self.buffer.entry(id).or_insert(PineconeEvent::Delete);
            }
            _ => unreachable!("diff can only be 1 or -1"),
        }
        Ok(())
    }

    fn flush(&mut self, _forced: bool) -> Result<(), WriteError> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let mut upserts = Vec::new();
        let mut deletes = Vec::new();
        for (id, event) in take(&mut self.buffer) {
            match event {
                PineconeEvent::Upsert(record) => upserts.push(record),
                PineconeEvent::Delete => deletes.push(id),
            }
        }
        // Deletes before upserts: handles update pairs whose old and new rows have
        // different ids but where ordering across the request boundary still matters.
        self.flush_deletes(&deletes)?;
        self.flush_upserts(&upserts)?;
        Ok(())
    }

    fn name(&self) -> String {
        format!("Pinecone({})", self.upsert_url)
    }

    fn single_threaded(&self) -> bool {
        // With a user-chosen `primary_key` column the Pinecone record id need not
        // coincide with the table's row key, which is what the dataflow shards by.
        // Then an update's delete(old) and upsert(new) for the same record id could
        // land on different workers and race over the network, dropping a record
        // that should survive — so serialize on one worker (which also lets us
        // detect primary_key collisions). With no `primary_key` the id IS the row
        // key the dataflow already shards by, so writes can run in parallel.
        self.pk_index.is_some()
    }
}

impl Drop for PineconeWriter {
    fn drop(&mut self) {
        if let Err(e) = self.flush(true) {
            error!("Failed to flush the Pinecone writer on drop: {e}");
        }
    }
}
