// Copyright © 2026 Pathway

use std::collections::HashMap;
use std::mem::take;

use qdrant_client::qdrant::{DeletePointsBuilder, PointId, PointStruct, UpsertPointsBuilder};
use qdrant_client::Qdrant;
use serde_json::{Map as JsonMap, Value as JsonValue};
use tokio::runtime::Runtime as TokioRuntime;
use uuid::Uuid;

use crate::connectors::data_format::{serialize_value_to_json, FormatterContext};
use crate::connectors::{WriteError, Writer};
use crate::engine::{Key, Value};
use crate::external_integration::qdrant_integration::ensure_collection_with_cosine;
use crate::python_api::ValueField;

/// Errors specific to the Qdrant output connector. Surfaced to the engine
/// through the single transparent [`WriteError::Qdrant`] variant, so `?` works
/// directly on these results inside the writer.
#[derive(Debug, thiserror::Error)]
pub enum QdrantWriteError {
    #[error(transparent)]
    Client(#[from] qdrant_client::QdrantError),

    #[error(
        "the column selected as the Qdrant vector holds a value of type that is not a \
         list[float] or a 1-D numeric array: {0}"
    )]
    NotAVector(Value),

    #[error("the column selected as the Qdrant vector contains a non-numeric element: {0}")]
    NonNumericVectorElement(Value),

    #[error("the column selected as the Qdrant vector holds a {0}-dimensional array; only 1-D vectors are supported")]
    MultiDimensionalVector(usize),

    #[error(
        "the column selected as the Qdrant vector contains a non-finite element \
         (NaN or infinity)"
    )]
    NonFiniteVectorElement,
}

/// A pending per-key operation within the current minibatch. Keyed by the
/// Pathway row [`Key`] so that, like the `DynamoDB` sink, an insertion always
/// wins over a deletion of the same key regardless of arrival order (an update
/// arrives as a `-1` of the old row followed by a `+1` of the new one, both
/// carrying the same key).
///
/// The upsert payload is boxed because a [`PointStruct`] (vector + payload) is
/// far larger than the bare [`PointId`] of a delete.
enum QdrantOp {
    Upsert(Box<PointStruct>),
    Delete(PointId),
}

/// Map a Pathway row key to a stable Qdrant point id.
///
/// The Pathway key is a 128-bit value, so it is encoded as a UUID point id.
/// This is deterministic and stateless — the same row always maps to the same
/// point id across restarts and workers — so deletions reliably target the
/// point a prior insertion created, without keeping an in-memory key→id table.
fn key_to_point_id(key: Key) -> PointId {
    // `KeyImpl` is u128 by default but may be narrower under a build feature, so
    // widen explicitly; the conversion is a no-op (and lint-flagged) only in the
    // default configuration.
    #[allow(clippy::useless_conversion)]
    let key_u128 = u128::from(key.0);
    Uuid::from_u128(key_u128).to_string().into()
}

pub struct QdrantWriter {
    runtime: TokioRuntime,
    client: Qdrant,
    collection_name: String,
    value_fields: Vec<ValueField>,
    vector_field_index: usize,
    batch_size: usize,
    // Vector dimension observed from the first upserted point, used to create
    // the collection on demand if it does not already exist.
    observed_dimension: Option<u64>,
    // Set once the collection is known to exist (either it already did, or it
    // was created from `observed_dimension`).
    collection_ready: bool,
    // Net effect per key for the current minibatch.
    pending: HashMap<Key, QdrantOp>,
}

impl QdrantWriter {
    pub fn new(
        runtime: TokioRuntime,
        client: Qdrant,
        collection_name: String,
        value_fields: Vec<ValueField>,
        vector_field_index: usize,
        batch_size: usize,
    ) -> Self {
        Self {
            runtime,
            client,
            collection_name,
            value_fields,
            vector_field_index,
            batch_size,
            observed_dimension: None,
            collection_ready: false,
            pending: HashMap::new(),
        }
    }

    /// Extract the vector column value as the `Vec<f32>` Qdrant stores.
    fn extract_vector(value: &Value) -> Result<Vec<f32>, QdrantWriteError> {
        let vector = Self::extract_vector_components(value)?;
        // Qdrant silently accepts NaN/infinity, but one such point makes the
        // whole collection unreadable to standard clients, so reject it here.
        if vector.iter().any(|v| !v.is_finite()) {
            return Err(QdrantWriteError::NonFiniteVectorElement);
        }
        Ok(vector)
    }

    fn extract_vector_components(value: &Value) -> Result<Vec<f32>, QdrantWriteError> {
        match value {
            Value::FloatArray(array) => {
                if array.ndim() != 1 {
                    return Err(QdrantWriteError::MultiDimensionalVector(array.ndim()));
                }
                // Iterate rather than `as_slice()`: a non-contiguous array (e.g. a
                // reversed view or a column of a 2-D array) has no backing slice,
                // so `as_slice()` returns `None` and an `unwrap_or(&[])` would
                // silently drop every element, producing a 0-dimensional vector.
                #[allow(clippy::cast_possible_truncation)]
                Ok(array.iter().map(|v| *v as f32).collect())
            }
            Value::IntArray(array) => {
                if array.ndim() != 1 {
                    return Err(QdrantWriteError::MultiDimensionalVector(array.ndim()));
                }
                #[allow(clippy::cast_precision_loss)]
                Ok(array.iter().map(|v| *v as f32).collect())
            }
            Value::Tuple(items) => items
                .iter()
                .map(|item| match item {
                    #[allow(clippy::cast_possible_truncation)]
                    Value::Float(f) => Ok(f64::from(*f) as f32),
                    #[allow(clippy::cast_precision_loss)]
                    Value::Int(i) => Ok(*i as f32),
                    other => Err(QdrantWriteError::NonNumericVectorElement(other.clone())),
                })
                .collect(),
            other => Err(QdrantWriteError::NotAVector(other.clone())),
        }
    }

    /// Build the point payload from every column except the vector column.
    fn build_payload(&self, values: &[Value]) -> Result<JsonMap<String, JsonValue>, WriteError> {
        let mut payload = JsonMap::with_capacity(self.value_fields.len().saturating_sub(1));
        for (index, (field, value)) in self.value_fields.iter().zip(values.iter()).enumerate() {
            if index == self.vector_field_index {
                continue;
            }
            payload.insert(field.name.clone(), serialize_value_to_json(value)?);
        }
        Ok(payload)
    }

    fn ensure_collection(&mut self) -> Result<(), WriteError> {
        if self.collection_ready {
            return Ok(());
        }
        // Without an observed vector the dimension is unknown, so a missing
        // collection cannot be created yet; an existing collection still
        // accepts the deletes that triggered this flush.
        if let Some(dimension) = self.observed_dimension {
            ensure_collection_with_cosine(
                &self.runtime,
                &self.client,
                &self.collection_name,
                dimension,
            )
            .map_err(QdrantWriteError::from)?;
            self.collection_ready = true;
        }
        Ok(())
    }
}

impl Writer for QdrantWriter {
    fn write(&mut self, data: FormatterContext) -> Result<(), WriteError> {
        match data.diff {
            1 => {
                let vector = Self::extract_vector(&data.values[self.vector_field_index])?;
                self.observed_dimension.get_or_insert(vector.len() as u64);
                let payload = self.build_payload(&data.values)?;
                let point = PointStruct::new(key_to_point_id(data.key), vector, payload);
                self.pending
                    .insert(data.key, QdrantOp::Upsert(Box::new(point)));
            }
            -1 => {
                // An insertion of the same key (in any order) takes precedence,
                // matching snapshot semantics: keep the freshly-inserted point.
                self.pending
                    .entry(data.key)
                    .or_insert_with(|| QdrantOp::Delete(key_to_point_id(data.key)));
            }
            other => unreachable!("diff can only be 1 or -1, got {other}"),
        }
        if self.pending.len() >= self.batch_size {
            self.flush(false)?;
        }
        Ok(())
    }

    fn flush(&mut self, _forced: bool) -> Result<(), WriteError> {
        if self.pending.is_empty() {
            return Ok(());
        }
        self.ensure_collection()?;

        let mut to_delete: Vec<PointId> = Vec::new();
        let mut to_upsert: Vec<PointStruct> = Vec::new();
        for op in take(&mut self.pending).into_values() {
            match op {
                QdrantOp::Upsert(point) => to_upsert.push(*point),
                QdrantOp::Delete(id) => to_delete.push(id),
            }
        }

        self.runtime
            .block_on(async {
                // Deletes before upserts so that, across the chunk boundaries,
                // a delete never races ahead of a re-insertion of the same key.
                // `wait(true)` makes the server apply each request before
                // responding, so an error (e.g. a vector-dimension mismatch) is
                // reported synchronously instead of being swallowed by an async
                // queue — the sink must not report success for points the server
                // later rejects.
                for chunk in to_delete.chunks(self.batch_size) {
                    self.client
                        .delete_points(
                            DeletePointsBuilder::new(&self.collection_name)
                                .points(chunk.to_vec())
                                .wait(true),
                        )
                        .await?;
                }
                for chunk in to_upsert.chunks(self.batch_size) {
                    self.client
                        .upsert_points(
                            UpsertPointsBuilder::new(&self.collection_name, chunk.to_vec())
                                .wait(true),
                        )
                        .await?;
                }
                Ok::<(), qdrant_client::QdrantError>(())
            })
            .map_err(QdrantWriteError::from)?;

        Ok(())
    }

    fn single_threaded(&self) -> bool {
        // Each Qdrant point id is derived from the (globally unique) Pathway row
        // key, and upserts/deletes are independent and idempotent, so the output
        // can be sharded across workers: every worker writes its own rows to the
        // shared collection without coordination.
        false
    }

    fn name(&self) -> String {
        format!("Qdrant({})", self.collection_name)
    }
}
