use std::collections::hash_map::Iter;
use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use xxhash_rust::xxh3::Xxh3 as Hasher;

use crate::connectors::metadata::SourceMetadata;
use crate::persistence::backends::{Error as PersistenceError, PersistenceBackend};

const BLOB_EXTENSION: &str = ".blob";
const METADATA_EXTENSION: &str = ".metadata";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MetadataEntry {
    uri: Vec<u8>,
    metadata: SourceMetadata,
}

impl MetadataEntry {
    pub fn new(uri: Vec<u8>, metadata: SourceMetadata) -> Self {
        Self { uri, metadata }
    }

    pub fn parse(bytes: &[u8]) -> Result<Self, PersistenceError> {
        let data = std::str::from_utf8(bytes)?;
        let result = serde_json::from_str::<Self>(data.trim_end())
            .map_err(|e| PersistenceError::IncorrectMetadataFormat(data.to_string(), e))?;
        Ok(result)
    }

    pub fn serialize(&self) -> String {
        serde_json::to_string(&self).unwrap()
    }
}

#[derive(Debug)]
pub struct CachedObjectStorage {
    backend: Box<dyn PersistenceBackend>,
    known_objects: HashMap<Vec<u8>, SourceMetadata>,
}

impl CachedObjectStorage {
    pub fn new(backend: Box<dyn PersistenceBackend>) -> Result<Self, PersistenceError> {
        let mut known_objects = HashMap::new();
        let keys = backend.list_keys()?;

        // If slow, use thread pool
        for key in keys {
            if !key.ends_with(METADATA_EXTENSION) {
                continue;
            }
            let object = backend.get_value(&key)?;
            let entry = MetadataEntry::parse(&object)?;
            known_objects.insert(entry.uri, entry.metadata);
        }

        Ok(Self {
            backend,
            known_objects,
        })
    }

    pub fn clear(&mut self) -> Result<(), PersistenceError> {
        let keys: Vec<Vec<u8>> = self.known_objects.keys().cloned().collect();
        for uri in keys {
            self.remove_object_from_storage(&uri)?;
        }
        self.known_objects.clear();
        Ok(())
    }

    pub fn place_object(
        &mut self,
        uri: &[u8],
        contents: Vec<u8>,
        metadata: SourceMetadata,
    ) -> Result<(), PersistenceError> {
        let object_key = Self::cached_object_path(uri);
        let metadata_key = Self::metadata_path(uri);

        futures::executor::block_on(async {
            self.backend.put_value(&object_key, contents).await.unwrap()
        })?;

        // The binary object and the metadata must be written one by one.
        // This ensures that if there is a metadata object for a certain key,
        // it is guaranteed to be fully loaded into the storage.
        // On the contrary, if two futures are waited for simultaneously,
        // it is possible that the binary object uploading future fails,
        // while the metadata uploading future succeeds. This would lead to a situation where,
        // upon recovery, a certain object is considered present in the storage
        // but causes an error during an actual acquisition attempt.
        let metadata_entry = MetadataEntry::new(uri.to_vec(), metadata.clone());
        futures::executor::block_on(async {
            let serialized_entry = metadata_entry.serialize();
            self.backend
                .put_value(&metadata_key, serialized_entry.as_bytes().to_vec())
                .await
                .unwrap()
        })?;
        self.known_objects.insert(uri.to_vec(), metadata);

        Ok(())
    }

    pub fn remove_object(&mut self, uri: &[u8]) -> Result<(), PersistenceError> {
        self.remove_object_from_storage(uri)?;
        self.known_objects.remove(uri);
        Ok(())
    }

    pub fn contains_object(&self, uri: &[u8]) -> bool {
        self.known_objects.contains_key(uri)
    }

    pub fn get_iter(&self) -> Iter<Vec<u8>, SourceMetadata> {
        self.known_objects.iter()
    }

    pub fn stored_metadata(&self, uri: &[u8]) -> Option<&SourceMetadata> {
        self.known_objects.get(uri)
    }

    pub fn get_object(&self, uri: &[u8]) -> Result<Vec<u8>, PersistenceError> {
        let object_key = Self::cached_object_path(uri);
        self.backend.get_value(&object_key)
    }

    // Below are helper methods

    fn remove_object_from_storage(&mut self, uri: &[u8]) -> Result<(), PersistenceError> {
        let object_key = Self::cached_object_path(uri);
        let metadata_key = Self::metadata_path(uri);
        self.backend.remove_key(&metadata_key)?;
        self.backend.remove_key(&object_key)
    }

    fn uri_hash(uri: &[u8]) -> String {
        let mut hasher = Hasher::default();
        hasher.update(uri);
        format!("{}", hasher.digest128())
    }

    fn cached_object_path(uri: &[u8]) -> String {
        let uri_hash = Self::uri_hash(uri);
        format!("{uri_hash}{BLOB_EXTENSION}")
    }

    fn metadata_path(uri: &[u8]) -> String {
        let uri_hash = Self::uri_hash(uri);
        format!("{uri_hash}{METADATA_EXTENSION}")
    }
}
