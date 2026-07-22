// Copyright © 2026 Pathway

use std::collections::{HashMap, HashSet};
use std::mem::take;

use qdrant_client::qdrant::vectors_config::Config as VectorsConfigVariant;
use qdrant_client::qdrant::{
    DeletePointsBuilder, NamedVectors, PointId, PointStruct, UpsertPointsBuilder, Vector,
};
use qdrant_client::Qdrant;
use serde_json::{Map as JsonMap, Value as JsonValue};
use tokio::runtime::Runtime as TokioRuntime;
use uuid::Uuid;

use crate::connectors::data_format::{serialize_value_to_json, FormatterContext};
use crate::connectors::data_storage::vectors::{vector_kind_of, VectorKind};
use crate::connectors::{WriteError, Writer};
use crate::engine::{Key, Value};
use crate::python_api::ValueField;

/// Errors specific to the Qdrant output connector. Surfaced to the engine
/// through the single transparent [`WriteError::Qdrant`] variant, so `?` works
/// directly on these results inside the writer.
#[derive(Debug, thiserror::Error)]
pub enum QdrantWriteError {
    #[error(transparent)]
    Client(#[from] qdrant_client::QdrantError),

    #[error(
        "Qdrant collection \"{0}\" does not exist. The connector never creates collections: \
         create it beforehand with the desired named vector configuration \
         (the collection schema defines which table columns are written as vectors)"
    )]
    CollectionNotFound(String),

    #[error("Qdrant did not return the configuration of collection \"{0}\"")]
    MissingCollectionConfig(String),

    #[error(
        "Qdrant collection \"{0}\" is configured with a single unnamed vector. \
         The connector requires named vector slots: recreate the collection with \
         a named dense (and optionally sparse) vector configuration"
    )]
    UnnamedVectorSlot(String),

    #[error(
        "Qdrant collection \"{0}\" declares no vector slots. Recreate it with at \
         least one named dense or sparse vector slot"
    )]
    NoVectorSlots(String),

    #[error(
        "Qdrant collection \"{collection}\" declares a {kind} vector slot \"{slot}\", \
         but the table has no column named \"{slot}\". Vector slots are bound to \
         table columns by name"
    )]
    SlotWithoutColumn {
        collection: String,
        slot: String,
        kind: &'static str,
    },

    #[error(
        "Qdrant collection declares a {expected_kind} vector slot \"{slot}\", which \
         requires a column \"{slot}\" of type {expected_dtype}, but the table column \
         \"{slot}\" has type {actual_dtype}"
    )]
    SlotKindMismatch {
        slot: String,
        expected_kind: &'static str,
        expected_dtype: &'static str,
        actual_dtype: String,
    },

    #[error(
        "Qdrant collection declares a multivector slot \"{0}\"; multivectors \
         (list[list[float]] columns) are not supported by the Qdrant output connector yet"
    )]
    MultivectorNotImplemented(String),

    #[error(
        "the vector in column \"{column}\" has dimension {actual}, but the Qdrant \
         dense vector slot \"{column}\" expects dimension {expected}"
    )]
    DenseDimensionMismatch {
        column: String,
        expected: u64,
        actual: usize,
    },

    #[error(
        "the column \"{0}\" bound to a Qdrant dense vector slot holds a value that \
         is not a list[float] or a 1-D numeric array: {1}"
    )]
    NotAVector(String, Value),

    #[error("the vector in column \"{0}\" contains a non-numeric element: {1}")]
    NonNumericVectorElement(String, Value),

    #[error(
        "the column \"{0}\" bound to a Qdrant dense vector slot holds a \
         {1}-dimensional array; only 1-D vectors are supported"
    )]
    MultiDimensionalVector(String, usize),

    #[error("the vector in column \"{0}\" contains a non-finite element (NaN or infinity)")]
    NonFiniteVectorElement(String),

    #[error(
        "the column \"{0}\" bound to a Qdrant sparse vector slot holds a value that \
         is not a list[tuple[int, float]]: {1}"
    )]
    NotASparseVector(String, Value),

    #[error(
        "the sparse vector in column \"{0}\" contains index {1}, which does not fit \
         the u32 range Qdrant requires for sparse vector indices"
    )]
    SparseIndexOutOfRange(String, i64),
}

/// A vector slot declared by the collection, bound to the same-named table
/// column at startup.
enum SlotBinding {
    Dense {
        name: String,
        column_index: usize,
        dimension: u64,
    },
    Sparse {
        name: String,
        column_index: usize,
    },
}

impl SlotBinding {
    fn column_index(&self) -> usize {
        match self {
            Self::Dense { column_index, .. } | Self::Sparse { column_index, .. } => *column_index,
        }
    }
}

/// A pending per-key operation within the current minibatch. Keyed by the
/// Pathway row [`Key`] so that, like the `DynamoDB` sink, an insertion always
/// wins over a deletion of the same key regardless of arrival order (an update
/// arrives as a `-1` of the old row followed by a `+1` of the new one, both
/// carrying the same key).
///
/// The upsert payload is boxed because a [`PointStruct`] (vectors + payload) is
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
    // Vector slots declared by the collection, bound to same-named columns.
    slots: Vec<SlotBinding>,
    // Column indices not bound to any vector slot; these form the payload.
    payload_indices: Vec<usize>,
    batch_size: usize,
    // Net effect per key for the current minibatch.
    pending: HashMap<Key, QdrantOp>,
}

impl QdrantWriter {
    /// Introspect the collection and bind its vector slots to table columns.
    ///
    /// The collection schema is the single source of truth: every declared slot
    /// must have a same-named column of the matching kind, and any violation
    /// fails loudly here — at startup — rather than degrading at write time.
    pub fn new(
        runtime: TokioRuntime,
        client: Qdrant,
        collection_name: String,
        value_fields: Vec<ValueField>,
        batch_size: usize,
    ) -> Result<Self, QdrantWriteError> {
        let slots = Self::bind_slots(&runtime, &client, &collection_name, &value_fields)?;
        let bound_indices: HashSet<usize> = slots.iter().map(SlotBinding::column_index).collect();
        let payload_indices = (0..value_fields.len())
            .filter(|index| !bound_indices.contains(index))
            .collect();
        Ok(Self {
            runtime,
            client,
            collection_name,
            value_fields,
            slots,
            payload_indices,
            batch_size,
            pending: HashMap::new(),
        })
    }

    fn bind_slots(
        runtime: &TokioRuntime,
        client: &Qdrant,
        collection_name: &str,
        value_fields: &[ValueField],
    ) -> Result<Vec<SlotBinding>, QdrantWriteError> {
        if !runtime.block_on(client.collection_exists(collection_name))? {
            return Err(QdrantWriteError::CollectionNotFound(
                collection_name.to_string(),
            ));
        }
        let info = runtime.block_on(client.collection_info(collection_name))?;
        let params = info
            .result
            .and_then(|result| result.config)
            .and_then(|config| config.params)
            .ok_or_else(|| {
                QdrantWriteError::MissingCollectionConfig(collection_name.to_string())
            })?;

        let mut dense_slots = match params.vectors_config.and_then(|config| config.config) {
            None => Vec::new(),
            Some(VectorsConfigVariant::Params(_)) => {
                return Err(QdrantWriteError::UnnamedVectorSlot(
                    collection_name.to_string(),
                ));
            }
            Some(VectorsConfigVariant::ParamsMap(map)) => map.map.into_iter().collect::<Vec<_>>(),
        };
        let mut sparse_slots: Vec<String> = params
            .sparse_vectors_config
            .map(|config| config.map.into_keys().collect())
            .unwrap_or_default();
        // The maps arrive in random order; sort so validation reports the same
        // slot first on every run.
        dense_slots.sort_by(|(left, _), (right, _)| left.cmp(right));
        sparse_slots.sort_unstable();

        if dense_slots.is_empty() && sparse_slots.is_empty() {
            return Err(QdrantWriteError::NoVectorSlots(collection_name.to_string()));
        }

        let mut slots = Vec::with_capacity(dense_slots.len() + sparse_slots.len());
        for (slot_name, vector_params) in dense_slots {
            if vector_params.multivector_config.is_some() {
                return Err(QdrantWriteError::MultivectorNotImplemented(slot_name));
            }
            let column_index =
                Self::bind_column(value_fields, collection_name, &slot_name, VectorKind::Dense)?;
            slots.push(SlotBinding::Dense {
                name: slot_name,
                column_index,
                dimension: vector_params.size,
            });
        }
        for slot_name in sparse_slots {
            let column_index = Self::bind_column(
                value_fields,
                collection_name,
                &slot_name,
                VectorKind::Sparse,
            )?;
            slots.push(SlotBinding::Sparse {
                name: slot_name,
                column_index,
            });
        }
        Ok(slots)
    }

    /// Find the column with the slot's name and check its dtype kind.
    fn bind_column(
        value_fields: &[ValueField],
        collection_name: &str,
        slot_name: &str,
        expected_kind: VectorKind,
    ) -> Result<usize, QdrantWriteError> {
        let column_index = value_fields
            .iter()
            .position(|field| field.name == slot_name)
            .ok_or_else(|| QdrantWriteError::SlotWithoutColumn {
                collection: collection_name.to_string(),
                slot: slot_name.to_string(),
                kind: expected_kind.name(),
            })?;
        let dtype = &value_fields[column_index].type_;
        if vector_kind_of(dtype) != Some(expected_kind) {
            return Err(QdrantWriteError::SlotKindMismatch {
                slot: slot_name.to_string(),
                expected_kind: expected_kind.name(),
                expected_dtype: expected_kind.expected_dtype(),
                actual_dtype: dtype.to_string(),
            });
        }
        Ok(column_index)
    }

    /// Extract a dense vector column value as the `Vec<f32>` Qdrant stores.
    fn extract_dense(value: &Value, column: &str) -> Result<Vec<f32>, QdrantWriteError> {
        let vector = Self::extract_dense_components(value, column)?;
        // Qdrant silently accepts NaN/infinity, but one such point makes the
        // whole collection unreadable to standard clients, so reject it here.
        if vector.iter().any(|component| !component.is_finite()) {
            return Err(QdrantWriteError::NonFiniteVectorElement(column.to_string()));
        }
        Ok(vector)
    }

    fn extract_dense_components(value: &Value, column: &str) -> Result<Vec<f32>, QdrantWriteError> {
        match value {
            Value::FloatArray(array) => {
                if array.ndim() != 1 {
                    return Err(QdrantWriteError::MultiDimensionalVector(
                        column.to_string(),
                        array.ndim(),
                    ));
                }
                // Iterate rather than `as_slice()`: a non-contiguous array (e.g. a
                // reversed view or a column of a 2-D array) has no backing slice,
                // so `as_slice()` returns `None` and an `unwrap_or(&[])` would
                // silently drop every element, producing a 0-dimensional vector.
                #[allow(clippy::cast_possible_truncation)]
                Ok(array.iter().map(|component| *component as f32).collect())
            }
            Value::IntArray(array) => {
                if array.ndim() != 1 {
                    return Err(QdrantWriteError::MultiDimensionalVector(
                        column.to_string(),
                        array.ndim(),
                    ));
                }
                #[allow(clippy::cast_precision_loss)]
                Ok(array.iter().map(|component| *component as f32).collect())
            }
            Value::Tuple(items) => items
                .iter()
                .map(|item| match item {
                    #[allow(clippy::cast_possible_truncation)]
                    Value::Float(f) => Ok(f64::from(*f) as f32),
                    #[allow(clippy::cast_precision_loss)]
                    Value::Int(i) => Ok(*i as f32),
                    other => Err(QdrantWriteError::NonNumericVectorElement(
                        column.to_string(),
                        other.clone(),
                    )),
                })
                .collect(),
            other => Err(QdrantWriteError::NotAVector(
                column.to_string(),
                other.clone(),
            )),
        }
    }

    /// Extract a sparse vector column value as the (indices, weights) pair
    /// Qdrant stores. Weights are sent raw (e.g. term frequencies): the IDF
    /// modifier, when configured on the slot, is applied server-side.
    fn extract_sparse(
        value: &Value,
        column: &str,
    ) -> Result<(Vec<u32>, Vec<f32>), QdrantWriteError> {
        let Value::Tuple(pairs) = value else {
            return Err(QdrantWriteError::NotASparseVector(
                column.to_string(),
                value.clone(),
            ));
        };
        let mut indices = Vec::with_capacity(pairs.len());
        let mut weights = Vec::with_capacity(pairs.len());
        for pair in pairs.iter() {
            let Value::Tuple(pair_items) = pair else {
                return Err(QdrantWriteError::NotASparseVector(
                    column.to_string(),
                    value.clone(),
                ));
            };
            let [Value::Int(index), Value::Float(weight)] = pair_items.as_ref() else {
                return Err(QdrantWriteError::NotASparseVector(
                    column.to_string(),
                    value.clone(),
                ));
            };
            let index = u32::try_from(*index)
                .map_err(|_| QdrantWriteError::SparseIndexOutOfRange(column.to_string(), *index))?;
            #[allow(clippy::cast_possible_truncation)]
            let weight = f64::from(*weight) as f32;
            if !weight.is_finite() {
                return Err(QdrantWriteError::NonFiniteVectorElement(column.to_string()));
            }
            indices.push(index);
            weights.push(weight);
        }
        Ok((indices, weights))
    }

    /// Build the named vectors of a point: one entry per declared slot, dense
    /// and sparse written atomically in the same upsert.
    fn build_vectors(&self, values: &[Value]) -> Result<NamedVectors, QdrantWriteError> {
        let mut vectors = NamedVectors::default();
        for slot in &self.slots {
            match slot {
                SlotBinding::Dense {
                    name,
                    column_index,
                    dimension,
                } => {
                    let dense = Self::extract_dense(&values[*column_index], name)?;
                    if dense.len() as u64 != *dimension {
                        return Err(QdrantWriteError::DenseDimensionMismatch {
                            column: name.clone(),
                            expected: *dimension,
                            actual: dense.len(),
                        });
                    }
                    vectors = vectors.add_vector(name.clone(), Vector::new_dense(dense));
                }
                SlotBinding::Sparse { name, column_index } => {
                    let (indices, weights) = Self::extract_sparse(&values[*column_index], name)?;
                    vectors =
                        vectors.add_vector(name.clone(), Vector::new_sparse(indices, weights));
                }
            }
        }
        Ok(vectors)
    }

    /// Build the point payload from every column not bound to a vector slot.
    fn build_payload(&self, values: &[Value]) -> Result<JsonMap<String, JsonValue>, WriteError> {
        let mut payload = JsonMap::with_capacity(self.payload_indices.len());
        for &index in &self.payload_indices {
            payload.insert(
                self.value_fields[index].name.clone(),
                serialize_value_to_json(&values[index])?,
            );
        }
        Ok(payload)
    }
}

impl Writer for QdrantWriter {
    fn write(&mut self, data: FormatterContext) -> Result<(), WriteError> {
        match data.diff {
            1 => {
                let vectors = self.build_vectors(&data.values)?;
                let payload = self.build_payload(&data.values)?;
                let point = PointStruct::new(key_to_point_id(data.key), vectors, payload);
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
                // responding, so an error (e.g. a sparse index conflict) is
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
