// Copyright © 2026 Pathway

use std::collections::HashMap;
use std::mem::take;
use std::thread::sleep;
use std::time::Duration;

use base64::Engine;
use log::warn;
use reqwest::blocking::Client;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue, CONTENT_TYPE};
use reqwest::StatusCode;
use serde_json::{json, Map as JsonMap, Value as JsonValue};

use crate::connectors::data_format::FormatterContext;
use crate::engine::value::Kind;
use crate::engine::{Key, Value};

use super::{WriteError, Writer};

// Used if the server's pre-flight check can't be read; it matches Chroma's own
// default and only bounds how many records go in one HTTP request.
const DEFAULT_MAX_BATCH_SIZE: usize = 5461;
// Transient HTTP failures (connection resets, 429, 5xx) are retried a few times
// with backoff before the write is surfaced as an error, so a momentary hiccup
// doesn't fail the whole run. flush() is not retried by the engine, so the
// writer does its own retrying here.
const HTTP_MAX_RETRIES: usize = 4;
const HTTP_RETRY_BASE_DELAY: Duration = Duration::from_millis(200);

#[derive(Debug, thiserror::Error)]
pub enum ChromaError {
    #[error(transparent)]
    Http(#[from] reqwest::Error),

    #[error("invalid HTTP header {0:?}: {1}")]
    InvalidHeader(String, String),

    #[error("collection {0:?} does not exist on the Chroma server")]
    CollectionNotFound(String),

    #[error("Chroma server returned an error (HTTP {status}): {message}")]
    Server { status: u16, message: String },

    #[error(
        "the embedding column {0:?} holds a value of kind {1:?}; \
         it must be a list of floats or a 1-D numeric array"
    )]
    UnsupportedEmbedding(String, Kind),

    #[error(
        "the embedding column {0:?} holds a {1}-dimensional array; only 1-D arrays are supported"
    )]
    MultidimensionalEmbedding(String, usize),

    #[error(
        "the metadata column {0:?} holds a value of kind {1:?}; \
         Chroma metadata values must be str, int, float, or bool"
    )]
    UnsupportedMetadata(String, Kind),

    #[error("the document column {0:?} holds a value of kind {1:?}; it must be a string")]
    NonStringDocument(String, Kind),

    #[error(
        "the primary-key column {0:?} holds a value of kind {1:?} that can't be used as an id"
    )]
    UnsupportedId(String, Kind),
}

/// Output writer for a Chroma collection, talking to the server over its v2
/// HTTP API. Rows added in a minibatch are upserted and rows removed are
/// deleted; within a single flush the deletes are applied before the upserts so
/// an update pair (delete of the old row + insert of the new one) that shares an
/// id lands as the new value.
pub struct ChromaWriter {
    client: Client,
    upsert_url: String,
    delete_url: String,
    collection_name: String,

    field_names: Vec<String>,
    // The column whose values are the Chroma record ids; `None` means no column
    // was given, so the row's internal Pathway key is used as the id instead.
    primary_key_index: Option<usize>,
    embedding_index: usize,
    document_index: Option<usize>,
    metadata_indices: Vec<usize>,
    max_batch_size: usize,
    // When the server advertises it, embeddings are sent as base64-encoded
    // little-endian float32 bytes (the same compact wire form the official
    // client uses) instead of verbose JSON number arrays, which keeps a
    // full-size batch of high-dimensional vectors under the server's body-size
    // limit and off the 413 path.
    use_base64_embeddings: bool,

    // Buffers filled by write() and drained by flush().
    upsert_ids: Vec<String>,
    upsert_embeddings: Vec<JsonValue>,
    upsert_documents: Vec<JsonValue>,
    upsert_metadatas: Vec<JsonValue>,
    delete_ids: Vec<String>,
}

impl ChromaWriter {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        host: &str,
        port: u16,
        ssl: bool,
        headers: Option<HashMap<String, String>>,
        tenant: &str,
        database: &str,
        collection_name: String,
        field_names: Vec<String>,
        primary_key_index: Option<usize>,
        embedding_index: usize,
        document_index: Option<usize>,
        metadata_indices: Vec<usize>,
    ) -> Result<Self, ChromaError> {
        let scheme = if ssl { "https" } else { "http" };
        let origin = format!("{scheme}://{host}:{port}");

        let mut header_map = HeaderMap::new();
        header_map.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        if let Some(headers) = headers {
            for (key, value) in headers {
                let name = HeaderName::from_bytes(key.as_bytes())
                    .map_err(|e| ChromaError::InvalidHeader(key.clone(), e.to_string()))?;
                let value = HeaderValue::from_str(&value)
                    .map_err(|e| ChromaError::InvalidHeader(key.clone(), e.to_string()))?;
                header_map.insert(name, value);
            }
        }
        let client = Client::builder().default_headers(header_map).build()?;

        let (max_batch_size, use_base64_embeddings) = Self::fetch_preflight(&client, &origin);

        let collections_url =
            format!("{origin}/api/v2/tenants/{tenant}/databases/{database}/collections");
        let collection_id =
            Self::resolve_collection_id(&client, &collections_url, &collection_name)?;

        Ok(Self {
            client,
            upsert_url: format!("{collections_url}/{collection_id}/upsert"),
            delete_url: format!("{collections_url}/{collection_id}/delete"),
            collection_name,
            field_names,
            primary_key_index,
            embedding_index,
            document_index,
            metadata_indices,
            max_batch_size,
            use_base64_embeddings,
            upsert_ids: Vec::new(),
            upsert_embeddings: Vec::new(),
            upsert_documents: Vec::new(),
            upsert_metadatas: Vec::new(),
            delete_ids: Vec::new(),
        })
    }

    /// Read the server's pre-flight check: the maximum records-per-request and
    /// whether it accepts base64-encoded embeddings. A failure here is not fatal
    /// — fall back to Chroma's documented default batch size and the safe
    /// (JSON-array) embedding encoding.
    fn fetch_preflight(client: &Client, origin: &str) -> (usize, bool) {
        let url = format!("{origin}/api/v2/pre-flight-checks");
        match client
            .get(&url)
            .send()
            .and_then(reqwest::blocking::Response::json::<JsonValue>)
        {
            Ok(payload) => {
                let max_batch_size = payload["max_batch_size"]
                    .as_u64()
                    .and_then(|v| usize::try_from(v).ok())
                    .filter(|v| *v > 0)
                    .unwrap_or(DEFAULT_MAX_BATCH_SIZE);
                let supports_base64 = payload["supports_base64_encoding"]
                    .as_bool()
                    .unwrap_or(false);
                (max_batch_size, supports_base64)
            }
            Err(error) => {
                warn!(
                    "Could not read Chroma pre-flight checks (using the \
                     {DEFAULT_MAX_BATCH_SIZE} default batch size and JSON embeddings): {error}"
                );
                (DEFAULT_MAX_BATCH_SIZE, false)
            }
        }
    }

    /// Resolve a collection name to the uuid the data-plane endpoints address it
    /// by. A `404` is reported as a clear "collection does not exist" error.
    fn resolve_collection_id(
        client: &Client,
        collections_url: &str,
        collection_name: &str,
    ) -> Result<String, ChromaError> {
        let url = format!("{collections_url}/{collection_name}");
        let response = client.get(&url).send()?;
        let status = response.status();
        if status == StatusCode::NOT_FOUND {
            return Err(ChromaError::CollectionNotFound(collection_name.to_string()));
        }
        if !status.is_success() {
            return Err(ChromaError::Server {
                status: status.as_u16(),
                message: response.text().unwrap_or_default(),
            });
        }
        let payload: JsonValue = response.json()?;
        match payload["id"].as_str() {
            Some(id) => Ok(id.to_string()),
            None => Err(ChromaError::CollectionNotFound(collection_name.to_string())),
        }
    }

    fn build_id(&self, key: Key, values: &[Value]) -> Result<String, ChromaError> {
        // No primary-key column: fall back to the row's internal Pathway key.
        let Some(index) = self.primary_key_index else {
            return Ok(key.to_string());
        };
        match &values[index] {
            Value::Int(i) => Ok(i.to_string()),
            Value::String(s) => Ok(s.to_string()),
            Value::Pointer(p) => Ok(p.to_string()),
            Value::Bool(b) => Ok(b.to_string()),
            other => Err(ChromaError::UnsupportedId(
                self.field_names[index].clone(),
                other.kind(),
            )),
        }
    }

    fn build_embedding(&self, values: &[Value]) -> Result<JsonValue, ChromaError> {
        let components = self.embedding_components(values)?;
        if self.use_base64_embeddings {
            // Pack the vector as little-endian float32 bytes (Chroma stores
            // float32) and base64-encode it — the compact wire form the server
            // advertised support for.
            let mut bytes = Vec::with_capacity(components.len() * 4);
            for component in &components {
                #[allow(clippy::cast_possible_truncation)]
                bytes.extend_from_slice(&(*component as f32).to_le_bytes());
            }
            Ok(json!(
                base64::engine::general_purpose::STANDARD.encode(bytes)
            ))
        } else {
            Ok(json!(components))
        }
    }

    /// Extract the embedding column as a flat vector of floats, accepting a
    /// Pathway `list[float]` (a tuple) or a 1-D float/int array.
    fn embedding_components(&self, values: &[Value]) -> Result<Vec<f64>, ChromaError> {
        let column = &self.field_names[self.embedding_index];
        match &values[self.embedding_index] {
            Value::Tuple(items) => {
                let mut components = Vec::with_capacity(items.len());
                for item in items.iter() {
                    components.push(match item {
                        Value::Float(f) => f.0,
                        #[allow(clippy::cast_precision_loss)]
                        Value::Int(i) => *i as f64,
                        other => {
                            return Err(ChromaError::UnsupportedEmbedding(
                                column.clone(),
                                other.kind(),
                            ))
                        }
                    });
                }
                Ok(components)
            }
            Value::FloatArray(array) => {
                if array.shape().len() != 1 {
                    return Err(ChromaError::MultidimensionalEmbedding(
                        column.clone(),
                        array.shape().len(),
                    ));
                }
                Ok(array.iter().copied().collect())
            }
            Value::IntArray(array) => {
                if array.shape().len() != 1 {
                    return Err(ChromaError::MultidimensionalEmbedding(
                        column.clone(),
                        array.shape().len(),
                    ));
                }
                #[allow(clippy::cast_precision_loss)]
                Ok(array.iter().map(|&i| i as f64).collect())
            }
            other => Err(ChromaError::UnsupportedEmbedding(
                column.clone(),
                other.kind(),
            )),
        }
    }

    fn build_document(&self, values: &[Value]) -> Result<JsonValue, ChromaError> {
        let Some(index) = self.document_index else {
            return Ok(JsonValue::Null);
        };
        match &values[index] {
            Value::String(s) => Ok(json!(s)),
            Value::None => Ok(JsonValue::Null),
            other => Err(ChromaError::NonStringDocument(
                self.field_names[index].clone(),
                other.kind(),
            )),
        }
    }

    /// Build one record's metadata object. `None`-valued columns are dropped
    /// (Chroma rejects null metadata values); a record that ends up with no
    /// metadata is sent as `null` rather than an empty object.
    fn build_metadata(&self, values: &[Value]) -> Result<JsonValue, ChromaError> {
        if self.metadata_indices.is_empty() {
            return Ok(JsonValue::Null);
        }
        let mut map = JsonMap::new();
        for &index in &self.metadata_indices {
            let column = &self.field_names[index];
            let scalar = match &values[index] {
                Value::None => continue,
                Value::Bool(b) => json!(b),
                Value::Int(i) => json!(i),
                Value::Float(f) => json!(f),
                Value::String(s) => json!(s),
                other => {
                    return Err(ChromaError::UnsupportedMetadata(
                        column.clone(),
                        other.kind(),
                    ))
                }
            };
            map.insert(column.clone(), scalar);
        }
        if map.is_empty() {
            Ok(JsonValue::Null)
        } else {
            Ok(JsonValue::Object(map))
        }
    }

    /// POST a JSON body, retrying transient failures (connection errors, 429,
    /// 5xx) with exponential backoff. A non-retriable error status (4xx other
    /// than 429) is surfaced immediately.
    fn post_json(&self, url: &str, body: &JsonValue) -> Result<(), ChromaError> {
        let mut attempt = 0;
        loop {
            let outcome = self.try_post_json(url, body);
            match outcome {
                Ok(()) => return Ok(()),
                Err(error) => {
                    attempt += 1;
                    if attempt >= HTTP_MAX_RETRIES || !Self::is_retriable(&error) {
                        return Err(error);
                    }
                    warn!(
                        "Chroma write to collection {:?} failed (attempt {attempt}/{HTTP_MAX_RETRIES}), \
                         retrying: {error}",
                        self.collection_name
                    );
                    sleep(HTTP_RETRY_BASE_DELAY * (1u32 << (attempt - 1)));
                }
            }
        }
    }

    fn try_post_json(&self, url: &str, body: &JsonValue) -> Result<(), ChromaError> {
        let response = self.client.post(url).json(body).send()?;
        let status = response.status();
        if status.is_success() {
            return Ok(());
        }
        Err(ChromaError::Server {
            status: status.as_u16(),
            message: response.text().unwrap_or_default(),
        })
    }

    fn is_retriable(error: &ChromaError) -> bool {
        match error {
            ChromaError::Http(e) => {
                e.is_timeout() || e.is_connect() || e.is_request() || e.is_body()
            }
            ChromaError::Server { status, .. } => *status == 429 || (500..600).contains(status),
            _ => false,
        }
    }

    fn flush_deletes(&mut self) -> Result<(), WriteError> {
        if self.delete_ids.is_empty() {
            return Ok(());
        }
        let ids = take(&mut self.delete_ids);
        for chunk in ids.chunks(self.max_batch_size) {
            let body = json!({ "ids": chunk });
            self.post_json(&self.delete_url, &body)?;
        }
        Ok(())
    }

    fn flush_upserts(&mut self) -> Result<(), WriteError> {
        if self.upsert_ids.is_empty() {
            return Ok(());
        }
        let ids = take(&mut self.upsert_ids);
        let embeddings = take(&mut self.upsert_embeddings);
        let documents = take(&mut self.upsert_documents);
        let metadatas = take(&mut self.upsert_metadatas);
        let has_documents = self.document_index.is_some();
        let has_metadatas = !self.metadata_indices.is_empty();

        let mut start = 0;
        while start < ids.len() {
            let stop = (start + self.max_batch_size).min(ids.len());
            let mut body = JsonMap::new();
            body.insert("ids".to_string(), json!(ids[start..stop]));
            body.insert("embeddings".to_string(), json!(embeddings[start..stop]));
            body.insert(
                "documents".to_string(),
                if has_documents {
                    json!(documents[start..stop])
                } else {
                    JsonValue::Null
                },
            );
            body.insert(
                "metadatas".to_string(),
                if has_metadatas {
                    json!(metadatas[start..stop])
                } else {
                    JsonValue::Null
                },
            );
            self.post_json(&self.upsert_url, &JsonValue::Object(body))?;
            start = stop;
        }
        Ok(())
    }
}

impl Writer for ChromaWriter {
    fn write(&mut self, data: FormatterContext) -> Result<(), WriteError> {
        let values = &data.values;
        if data.diff == 1 {
            self.upsert_ids.push(self.build_id(data.key, values)?);
            self.upsert_embeddings.push(self.build_embedding(values)?);
            self.upsert_documents.push(self.build_document(values)?);
            self.upsert_metadatas.push(self.build_metadata(values)?);
        } else {
            self.delete_ids.push(self.build_id(data.key, values)?);
        }
        Ok(())
    }

    fn flush(&mut self, _forced: bool) -> Result<(), WriteError> {
        // Deletes before upserts: an update arrives as a delete of the old row
        // followed by an insert of the new one with the same id, and applying
        // the delete first keeps the inserted value.
        self.flush_deletes()?;
        self.flush_upserts()?;
        Ok(())
    }

    fn name(&self) -> String {
        format!("Chroma({})", self.collection_name)
    }

    fn retriable(&self) -> bool {
        // upsert and delete are idempotent, so re-sending a record on engine
        // retry is safe.
        true
    }

    fn single_threaded(&self) -> bool {
        // With a user-provided primary-key column the Chroma id is that column's
        // value, so different workers can address disjoint ids in parallel.
        // Without one the id is the row's internal Pathway key, and funneling
        // every record through a single writer keeps the whole keyless stream in
        // one place — so the deletes-before-upserts flush ordering holds for an
        // update (delete of the old row + upsert of the new one) no matter how
        // the engine would otherwise distribute it.
        self.primary_key_index.is_none()
    }
}
