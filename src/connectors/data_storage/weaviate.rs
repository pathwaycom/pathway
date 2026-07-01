// Copyright © 2026 Pathway

use std::collections::{HashMap, HashSet};
use std::mem::take;

use crate::async_runtime::create_async_tokio_runtime;
use crate::connectors::data_format::{serialize_value_to_json, FormatterContext};
use crate::connectors::{WriteError, Writer};
use crate::engine::Value;

use futures::stream::{self, StreamExt, TryStreamExt};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use reqwest::Client as HttpClient;
use serde_json::{json, Map as JsonMap, Value as JsonValue};
use tokio::runtime::Runtime as TokioRuntime;
use uuid::Uuid;

// Upper bound on objects buffered before a mid-mini-batch flush, to keep the
// network continuously busy without letting a huge mini-batch grow memory
// unbounded.
const MAX_BUFFERED_OBJECTS: usize = 20_000;

/// Errors specific to the Weaviate connector. Exposed to the engine through a
/// single transparent [`WriteError::Weaviate`](crate::connectors::WriteError)
/// variant so `?` works directly on these results inside the writer.
#[derive(Debug, thiserror::Error)]
pub enum WeaviateError {
    #[error(transparent)]
    Http(#[from] reqwest::Error),

    #[error("failed to build Weaviate HTTP client: {0}")]
    ClientBuild(String),

    #[error(
        "Weaviate rejected {failed} of {total} object(s) written to collection \
         {collection:?}; first error: {first}"
    )]
    BatchInsert {
        collection: String,
        failed: usize,
        total: usize,
        first: String,
    },

    #[error("Weaviate batch delete from collection {collection:?} failed for {failed} object(s)")]
    BatchDelete { collection: String, failed: usize },

    #[error("Weaviate response is malformed: {0}")]
    MalformedResponse(String),

    #[error("the vector column {0:?} must contain a 1-D array of numbers")]
    InvalidVector(String),
}

/// Native Weaviate output connector.
///
/// Writes objects to Weaviate over its REST `/v1/batch/objects` endpoint, which
/// upserts by object UUID — so re-writing the same key replaces the object
/// rather than duplicating it, and no delete-before-insert is needed. Each
/// mini-batch is reduced to a net effect per UUID (last event wins), then the
/// buffered objects are sent in chunks of `batch_size`, with up to `concurrency`
/// requests in flight at once. Deletions (`diff < 0`) are issued as batch
/// delete-by-id requests using a `ContainsAny` filter.
///
/// The UUID is derived from the primary-key value exactly as
/// `weaviate.util.generate_uuid5` does: `uuid5(NAMESPACE_DNS, str(pk))`.
pub struct WeaviateWriter {
    runtime: TokioRuntime,
    client: HttpClient,
    batch_url: String,
    collection: String,
    pk_index: usize,
    vector_index: Option<usize>,
    // (property name, position in FormatterContext::values), excluding the
    // primary-key and vector columns.
    property_fields: Vec<(String, usize)>,
    batch_size: usize,
    concurrency: usize,

    // Net effect of the current mini-batch, keyed by object UUID.
    upserts: HashMap<String, JsonValue>,
    deletes: HashSet<String>,
}

impl WeaviateWriter {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        base_url: &str,
        collection: String,
        pk_index: usize,
        vector_index: Option<usize>,
        property_fields: Vec<(String, usize)>,
        api_key: Option<&str>,
        headers: &HashMap<String, String>,
        batch_size: usize,
        concurrency: usize,
    ) -> Result<Self, WriteError> {
        let runtime = create_async_tokio_runtime()?;

        let mut header_map = HeaderMap::new();
        header_map.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        if let Some(api_key) = api_key {
            let mut value = HeaderValue::from_str(&format!("Bearer {api_key}"))
                .map_err(|e| WeaviateError::ClientBuild(e.to_string()))?;
            value.set_sensitive(true);
            header_map.insert(AUTHORIZATION, value);
        }
        for (name, value) in headers {
            let name = HeaderName::from_bytes(name.as_bytes())
                .map_err(|e| WeaviateError::ClientBuild(e.to_string()))?;
            let value = HeaderValue::from_str(value)
                .map_err(|e| WeaviateError::ClientBuild(e.to_string()))?;
            header_map.insert(name, value);
        }
        let client = HttpClient::builder()
            .default_headers(header_map)
            .build()
            .map_err(WeaviateError::Http)?;

        let batch_url = format!("{}/v1/batch/objects", base_url.trim_end_matches('/'));
        Ok(Self {
            runtime,
            client,
            batch_url,
            collection,
            pk_index,
            vector_index,
            property_fields,
            batch_size: batch_size.max(1),
            concurrency: concurrency.max(1),
            upserts: HashMap::new(),
            deletes: HashSet::new(),
        })
    }

    // Buffer generously before a mid-mini-batch flush: each flush keeps
    // `concurrency` requests continuously in flight until its whole buffer drains,
    // so larger buffers mean fewer flush boundaries (where the network would
    // otherwise idle while the next buffer is built) — at the cost of bounded
    // memory. A mini-batch smaller than this is flushed once, at its end.
    fn flush_threshold(&self) -> usize {
        (self.batch_size * self.concurrency).max(MAX_BUFFERED_OBJECTS)
    }

    fn build_object(&self, uuid: &str, values: &[Value]) -> Result<JsonValue, WriteError> {
        let mut properties = JsonMap::with_capacity(self.property_fields.len());
        for (name, index) in &self.property_fields {
            properties.insert(name.clone(), serialize_value_to_json(&values[*index])?);
        }
        let mut object = json!({
            "class": self.collection,
            "id": uuid,
            "properties": JsonValue::Object(properties),
        });
        if let Some(vector_index) = self.vector_index {
            let value = &values[vector_index];
            if !matches!(value, Value::None) {
                let vector = value_to_vector(value, vector_index)?;
                object["vector"] = json!(vector);
            }
        }
        Ok(object)
    }
}

// Matches `weaviate.util.generate_uuid5(pk)` == `uuid5(NAMESPACE_DNS, str(pk))`,
// so the engine and the Python helper address the same object.
fn value_to_uuid(value: &Value) -> Result<String, WriteError> {
    let name = match serialize_value_to_json(value)? {
        JsonValue::String(s) => s,
        JsonValue::Number(n) => n.to_string(),
        JsonValue::Bool(b) => if b { "True" } else { "False" }.to_string(),
        other => other.to_string(),
    };
    Ok(Uuid::new_v5(&Uuid::NAMESPACE_DNS, name.as_bytes()).to_string())
}

// Vector components are embedding coordinates; the i64 -> f64 cast for an
// integer-typed vector is intentional and the values are far inside f64's exact
// integer range in practice.
#[allow(clippy::cast_precision_loss)]
fn scalar_f64(value: &Value, vector_index: usize) -> Result<f64, WeaviateError> {
    match value {
        Value::Float(f) => Ok(f.0),
        Value::Int(i) => Ok(*i as f64),
        _ => Err(WeaviateError::InvalidVector(vector_index.to_string())),
    }
}

#[allow(clippy::cast_precision_loss)]
fn value_to_vector(value: &Value, vector_index: usize) -> Result<Vec<f64>, WeaviateError> {
    match value {
        Value::Tuple(items) => items.iter().map(|v| scalar_f64(v, vector_index)).collect(),
        Value::FloatArray(array) => Ok(array.iter().copied().collect()),
        Value::IntArray(array) => Ok(array.iter().map(|x| *x as f64).collect()),
        _ => Err(WeaviateError::InvalidVector(vector_index.to_string())),
    }
}

async fn insert_chunk(
    client: &HttpClient,
    batch_url: &str,
    collection: &str,
    objects: Vec<JsonValue>,
) -> Result<(), WeaviateError> {
    let total = objects.len();
    let response = client
        .post(batch_url)
        .json(&json!({ "objects": objects }))
        .send()
        .await?
        .error_for_status()?;
    let parsed: JsonValue = response.json().await?;
    let results = parsed.as_array().ok_or_else(|| {
        WeaviateError::MalformedResponse("batch insert response is not an array".to_string())
    })?;
    let mut failed = 0;
    let mut first = String::new();
    for item in results {
        if let Some(errors) = item.get("result").and_then(|r| r.get("errors")) {
            if !errors.is_null() {
                failed += 1;
                if first.is_empty() {
                    first = errors.to_string();
                }
            }
        }
    }
    if failed > 0 {
        return Err(WeaviateError::BatchInsert {
            collection: collection.to_string(),
            failed,
            total,
            first,
        });
    }
    Ok(())
}

async fn delete_chunk(
    client: &HttpClient,
    batch_url: &str,
    collection: &str,
    ids: Vec<String>,
) -> Result<(), WeaviateError> {
    let body = json!({
        "match": {
            "class": collection,
            "where": {
                "operator": "ContainsAny",
                "path": ["id"],
                "valueTextArray": ids,
            },
        },
    });
    let response = client
        .delete(batch_url)
        .json(&body)
        .send()
        .await?
        .error_for_status()?;
    let parsed: JsonValue = response.json().await?;
    let failed = parsed
        .get("results")
        .and_then(|r| r.get("failed"))
        .and_then(JsonValue::as_u64)
        .unwrap_or(0);
    if failed > 0 {
        return Err(WeaviateError::BatchDelete {
            collection: collection.to_string(),
            failed: usize::try_from(failed).unwrap_or(usize::MAX),
        });
    }
    Ok(())
}

impl Writer for WeaviateWriter {
    fn write(&mut self, data: FormatterContext) -> Result<(), WriteError> {
        let uuid = value_to_uuid(&data.values[self.pk_index])?;
        if data.diff > 0 {
            let object = self.build_object(&uuid, &data.values)?;
            self.deletes.remove(&uuid);
            self.upserts.insert(uuid, object);
        } else {
            self.upserts.remove(&uuid);
            self.deletes.insert(uuid);
        }
        if self.upserts.len() + self.deletes.len() >= self.flush_threshold() {
            self.flush(false)?;
        }
        Ok(())
    }

    fn flush(&mut self, _forced: bool) -> Result<(), WriteError> {
        if self.upserts.is_empty() && self.deletes.is_empty() {
            return Ok(());
        }
        let deletes: Vec<String> = take(&mut self.deletes).into_iter().collect();
        let upserts: Vec<JsonValue> = take(&mut self.upserts).into_values().collect();
        let client = &self.client;
        let batch_url = self.batch_url.as_str();
        let collection = self.collection.as_str();
        let batch_size = self.batch_size;
        let concurrency = self.concurrency;

        self.runtime.block_on(async move {
            // Deletions go first: within a single flush the delete and upsert id
            // sets are disjoint (last-event-wins resolved them), so this only
            // affects keys deleted in this mini-batch.
            if !deletes.is_empty() {
                let chunks: Vec<Vec<String>> =
                    deletes.chunks(batch_size).map(<[_]>::to_vec).collect();
                stream::iter(chunks)
                    .map(|chunk| delete_chunk(client, batch_url, collection, chunk))
                    .buffer_unordered(concurrency)
                    .try_collect::<Vec<()>>()
                    .await?;
            }
            if !upserts.is_empty() {
                let chunks: Vec<Vec<JsonValue>> =
                    upserts.chunks(batch_size).map(<[_]>::to_vec).collect();
                stream::iter(chunks)
                    .map(|chunk| insert_chunk(client, batch_url, collection, chunk))
                    .buffer_unordered(concurrency)
                    .try_collect::<Vec<()>>()
                    .await?;
            }
            Ok::<(), WeaviateError>(())
        })?;
        Ok(())
    }

    // Object UUIDs make every write idempotent, so each worker can write its own
    // shard concurrently instead of funneling everything through worker 0.
    fn single_threaded(&self) -> bool {
        false
    }

    fn name(&self) -> String {
        "WeaviateWriter".to_string()
    }
}
