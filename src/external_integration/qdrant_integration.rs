// Copyright © 2024 Pathway

use std::collections::HashMap;

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
    DerivedFilteredSearchIndex, ExternalIndex, ExternalIndexFactory, KeyScoreMatch,
    KeyToU64IdMapper, NonFilteringExternalIndex,
};

pub struct QdrantIndex {
    client: Qdrant,
    collection_name: String,
    key_to_id_mapper: KeyToU64IdMapper,
    vector_size: usize,
    runtime: tokio::runtime::Runtime,
}

impl QdrantIndex {
    pub fn new(
        url: String,
        collection_name: String,
        vector_size: usize,
        api_key: Option<String>,
    ) -> DynResult<QdrantIndex> {
        let runtime = create_async_tokio_runtime()
            .map_err(|e| Error::Other(format!("Failed to create async runtime: {e}").into()))?;

        let client = Qdrant::from_url(&url)
            .api_key(api_key)
            .build()
            .map_err(|e| Error::Other(format!("Failed to create Qdrant client: {e}").into()))?;

        let collection_exists = runtime
            .block_on(client.collection_exists(&collection_name))
            .map_err(|e| {
                Error::Other(format!("Failed to check collection existence: {e}").into())
            })?;

        if !collection_exists {
            runtime
                .block_on(client.create_collection(
                    CreateCollectionBuilder::new(collection_name.clone()).vectors_config(
                        VectorParamsBuilder::new(vector_size as u64, Distance::Cosine),
                    ),
                ))
                .map_err(|e| Error::Other(format!("Failed to create collection: {e}").into()))?;
        }

        Ok(QdrantIndex {
            client,
            collection_name,
            key_to_id_mapper: KeyToU64IdMapper::new(),
            vector_size,
            runtime,
        })
    }

    fn search_one(&self, data: &[f64], limit: usize) -> DynResult<Vec<KeyScoreMatch>> {
        let query_vec: Vec<f32> = data.iter().map(|v| *v as f32).collect();
        let search_result = self
            .runtime
            .block_on(
                self.client.query(
                    QueryPointsBuilder::new(&self.collection_name)
                        .query(query_vec)
                        .limit(limit as u64)
                        .with_payload(false),
                ),
            )
            .map_err(|e| Error::Other(format!("Search failed: {e}").into()))?;
        let mut results = Vec::with_capacity(search_result.result.len());
        for point in search_result.result {
            let Some(point_id) = point.id else {
                warn!("Qdrant returned point without ID, ignoring");
                continue;
            };

            let Some(point_id_options) = point_id.point_id_options else {
                warn!("Qdrant returned point ID without options, ignoring");
                continue;
            };

            let id = match point_id_options {
                PointIdOptions::Num(num) => num,
                PointIdOptions::Uuid(_) => {
                    warn!("Qdrant returned UUID point ID, expected numeric ID");
                    continue;
                }
            };

            let Some(key) = self.key_to_id_mapper.get_key_for_id(id) else {
                warn!("Qdrant index returned a nonexistent ID {id}, ignoring");
                continue;
            };

            results.push(KeyScoreMatch {
                key,
                score: f64::from(point.score),
            });
        }

        Ok(results)
    }

    fn add_one(&mut self, key: Key, data: &[f64]) -> DynResult<()> {
        if data.len() != self.vector_size {
            return Err(format!(
                "Vector size mismatch: expected {}, got {}",
                self.vector_size,
                data.len()
            )
            .into());
        }

        let key_id = self.key_to_id_mapper.get_next_free_u64_id(key);
        let vec_f32: Vec<f32> = data.iter().map(|v| *v as f32).collect();

        self.runtime
            .block_on(self.client.upsert_points(UpsertPointsBuilder::new(
                &self.collection_name,
                vec![PointStruct::new(
                    key_id,
                    vec_f32,
                    HashMap::<String, Value>::new(),
                )],
            )))
            .map_err(|e| Error::Other(format!("Failed to add point: {e}").into()))?;

        Ok(())
    }

    fn remove_one(&mut self, key: Key) -> DynResult<()> {
        let key_id = self.key_to_id_mapper.remove_key(key)?;

        self.runtime
            .block_on(
                self.client.delete_points(
                    DeletePointsBuilder::new(&self.collection_name).points([key_id]),
                ),
            )
            .map_err(|e| Error::Other(format!("Failed to remove point: {e}").into()))?;

        Ok(())
    }
}

impl NonFilteringExternalIndex<Vec<f64>, Vec<f64>> for QdrantIndex {
    fn add(&mut self, add_data: Vec<(Key, Vec<f64>)>) -> Vec<(Key, DynResult<()>)> {
        add_data
            .into_iter()
            .map(|(key, data)| (key, self.add_one(key, &data)))
            .collect()
    }

    fn remove(&mut self, keys: Vec<Key>) -> Vec<(Key, DynResult<()>)> {
        keys.into_iter()
            .map(|key| (key, self.remove_one(key)))
            .collect()
    }

    fn search(
        &self,
        queries: &[(Key, Vec<f64>, usize)],
    ) -> Vec<(Key, DynResult<Vec<KeyScoreMatch>>)> {
        queries
            .iter()
            .map(|(key, data, limit)| (*key, self.search_one(data, *limit)))
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
        url: String,
        collection_name: String,
        vector_size: usize,
        api_key: Option<String>,
    ) -> QdrantIndexFactory {
        QdrantIndexFactory {
            url,
            collection_name,
            vector_size,
            api_key,
        }
    }
}

impl ExternalIndexFactory for QdrantIndexFactory {
    fn make_instance(&self) -> Result<Box<dyn ExternalIndex>, Error> {
        let qdrant_index = QdrantIndex::new(
            self.url.clone(),
            self.collection_name.clone(),
            self.vector_size,
            self.api_key.clone(),
        )?;
        Ok(Box::new(DerivedFilteredSearchIndex::new(Box::new(
            qdrant_index,
        ))))
    }
}
