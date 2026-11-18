// Copyright © 2026 Pathway

use std::collections::HashMap;
use std::sync::Arc;

use crate::async_runtime::create_async_tokio_runtime;
use crate::engine::error::DynResult;
use crate::engine::{Error, Key};
use log::warn;
use qdrant_client::qdrant::point_id::PointIdOptions;
use qdrant_client::qdrant::{CreateCollectionBuilder, Value, VectorParamsBuilder};
use qdrant_client::qdrant::{
    DeletePointsBuilder, Distance, PointStruct, QueryPointsBuilder, UpsertPointsBuilder,
};
use qdrant_client::Qdrant;

use super::{
    DerivedFilteredSearchIndex, ExternalIndex, ExternalIndexFactory, IndexingError, KeyScoreMatch,
    KeyToU64IdMapper, NonFilteringExternalIndex,
};

/// Build a Qdrant client from a URL and optional API key.
///
/// Shared between the KNN index ([`QdrantIndex`]) and the output connector
/// (`QdrantWriter`) so both talk to Qdrant the exact same way.
pub fn build_qdrant_client(
    url: &str,
    api_key: Option<String>,
) -> Result<Qdrant, qdrant_client::QdrantError> {
    Qdrant::from_url(url).api_key(api_key).build()
}

/// Ensure a collection exists, creating it with a single `Cosine`-distance
/// vector of the given size if it does not. Used by the KNN index; the output
/// connector never creates collections — its collection schema is authoritative.
pub fn ensure_collection_with_cosine(
    runtime: &tokio::runtime::Runtime,
    client: &Qdrant,
    collection_name: &str,
    vector_size: u64,
) -> Result<(), qdrant_client::QdrantError> {
    runtime.block_on(async {
        if client.collection_exists(collection_name).await? {
            return Ok(());
        }
        match client
            .create_collection(
                CreateCollectionBuilder::new(collection_name)
                    .vectors_config(VectorParamsBuilder::new(vector_size, Distance::Cosine)),
            )
            .await
        {
            Ok(_) => Ok(()),
            // Several workers may create the collection concurrently (the output
            // sink runs on every worker): treat the failure as success if the
            // collection exists by the time we recheck, surfacing the original
            // error otherwise.
            Err(e) => {
                if client.collection_exists(collection_name).await? {
                    Ok(())
                } else {
                    Err(e)
                }
            }
        }
    })
}

/// Cast a slice of `f64` vector components to the `f32` Qdrant stores.
///
/// Shared so the index and the output connector narrow vectors identically.
#[allow(clippy::cast_possible_truncation)]
pub fn cast_f64_slice_to_f32(data: &[f64]) -> Vec<f32> {
    data.iter().map(|v| *v as f32).collect()
}

pub struct QdrantIndex {
    client: Qdrant,
    collection_name: String,
    key_to_id_mapper: KeyToU64IdMapper,
    vector_size: usize,
    runtime: tokio::runtime::Runtime,
}

impl QdrantIndex {
    pub fn new(
        url: &str,
        collection_name: String,
        vector_size: usize,
        api_key: Option<String>,
    ) -> Result<Self, Error> {
        let runtime = create_async_tokio_runtime().map_err(IndexingError::from)?;

        let client = build_qdrant_client(url, api_key).map_err(IndexingError::from)?;

        ensure_collection_with_cosine(&runtime, &client, &collection_name, vector_size as u64)
            .map_err(IndexingError::from)?;

        Ok(QdrantIndex {
            client,
            collection_name,
            key_to_id_mapper: KeyToU64IdMapper::new(),
            vector_size,
            runtime,
        })
    }

    #[allow(clippy::cast_possible_truncation)]
    async fn search_one_async(
        &self,
        data: &[f64],
        limit: usize,
    ) -> Result<Vec<KeyScoreMatch>, IndexingError> {
        let query_vec: Vec<f32> = cast_f64_slice_to_f32(data);
        let search_result = self
            .client
            .query(
                QueryPointsBuilder::new(&self.collection_name)
                    .query(query_vec)
                    .limit(limit as u64)
                    .with_payload(false),
            )
            .await?;

        let results = search_result
            .result
            .into_iter()
            .filter_map(|point| {
                let Some(point_id) = point.id else {
                    warn!("Qdrant returned point without ID, ignoring");
                    return None;
                };

                let Some(point_id_options) = point_id.point_id_options else {
                    warn!("Qdrant returned point ID without options, ignoring");
                    return None;
                };

                let id = match point_id_options {
                    PointIdOptions::Num(num) => num,
                    PointIdOptions::Uuid(_) => {
                        warn!("Qdrant returned UUID point ID, expected numeric ID");
                        return None;
                    }
                };

                let Some(key) = self.key_to_id_mapper.get_key_for_id(id) else {
                    warn!("Qdrant index returned a nonexistent ID {id}, ignoring");
                    return None;
                };

                Some(KeyScoreMatch {
                    key,
                    score: f64::from(point.score),
                })
            })
            .collect();

        Ok(results)
    }

    #[allow(clippy::cast_possible_truncation)]
    fn add_batch(&mut self, data: Vec<(Key, Vec<f64>)>) -> Result<(), IndexingError> {
        let points: Result<Vec<_>, IndexingError> = data
            .into_iter()
            .map(|(key, vec_data)| {
                if vec_data.len() != self.vector_size {
                    return Err(IndexingError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!(
                            "Vector size mismatch: expected {}, got {}",
                            self.vector_size,
                            vec_data.len()
                        ),
                    )));
                }

                let key_id = self.key_to_id_mapper.get_next_free_u64_id(key);
                let vec_f32: Vec<f32> = cast_f64_slice_to_f32(&vec_data);
                Ok(PointStruct::new(
                    key_id,
                    vec_f32,
                    HashMap::<String, Value>::new(),
                ))
            })
            .collect();

        self.runtime.block_on(
            self.client
                .upsert_points(UpsertPointsBuilder::new(&self.collection_name, points?)),
        )?;

        Ok(())
    }

    fn remove_batch(&mut self, keys: Vec<Key>) -> Result<Vec<u64>, IndexingError> {
        let key_ids: Vec<u64> = keys
            .into_iter()
            .filter_map(|key| self.key_to_id_mapper.remove_key(key).ok())
            .collect();

        if !key_ids.is_empty() {
            self.runtime.block_on(self.client.delete_points(
                DeletePointsBuilder::new(&self.collection_name).points(key_ids.clone()),
            ))?;
        }

        Ok(key_ids)
    }
}

impl NonFilteringExternalIndex<Vec<f64>, Vec<f64>> for QdrantIndex {
    fn add(&mut self, add_data: Vec<(Key, Vec<f64>)>) -> Vec<(Key, DynResult<()>)> {
        if add_data.is_empty() {
            return Vec::new();
        }

        let keys: Vec<Key> = add_data.iter().map(|(k, _)| *k).collect();

        match self.add_batch(add_data) {
            Ok(()) => keys.into_iter().map(|key| (key, Ok(()))).collect(),
            Err(e) => {
                let shared_error: Arc<str> = Error::from(e).to_string().into();
                keys.into_iter()
                    .map(|key| (key, Err(Error::Other(shared_error.as_ref().into()).into())))
                    .collect()
            }
        }
    }

    fn remove(&mut self, keys: Vec<Key>) -> Vec<(Key, DynResult<()>)> {
        if keys.is_empty() {
            return Vec::new();
        }

        let original_keys = keys.clone();

        match self.remove_batch(keys) {
            Ok(_) => original_keys.into_iter().map(|key| (key, Ok(()))).collect(),
            Err(e) => {
                let shared_error: Arc<str> = Error::from(e).to_string().into();
                original_keys
                    .into_iter()
                    .map(|key| (key, Err(Error::Other(shared_error.as_ref().into()).into())))
                    .collect()
            }
        }
    }

    fn search(
        &self,
        queries: &[(Key, Vec<f64>, usize)],
    ) -> Vec<(Key, DynResult<Vec<KeyScoreMatch>>)> {
        if queries.is_empty() {
            return Vec::new();
        }

        let keys: Vec<Key> = queries.iter().map(|(k, _, _)| *k).collect();

        let results = self.runtime.block_on(async {
            let futures: Vec<_> = queries
                .iter()
                .map(|(_, data, limit)| self.search_one_async(data, *limit))
                .collect();

            futures::future::join_all(futures).await
        });

        keys.into_iter()
            .zip(results)
            .map(|(key, result)| (key, result.map_err(|e| Error::from(e).into())))
            .collect()
    }
}

pub struct QdrantIndexFactory {
    url: String,
    collection_name: String,
    vector_size: usize,
    api_key: Option<String>,
}

impl QdrantIndexFactory {
    pub fn new(
        url: &str,
        collection_name: String,
        vector_size: usize,
        api_key: Option<String>,
    ) -> QdrantIndexFactory {
        QdrantIndexFactory {
            url: url.to_string(),
            collection_name,
            vector_size,
            api_key,
        }
    }
}

impl ExternalIndexFactory for QdrantIndexFactory {
    fn make_instance(&self) -> Result<Box<dyn ExternalIndex>, Error> {
        let qdrant_index = QdrantIndex::new(
            &self.url,
            self.collection_name.clone(),
            self.vector_size,
            self.api_key.clone(),
        )?;
        Ok(Box::new(DerivedFilteredSearchIndex::new(Box::new(
            qdrant_index,
        ))))
    }
}
