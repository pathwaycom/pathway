use log::error;
use std::cmp::max;
use std::collections::hash_map::Iter;
use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::connectors::metadata::FileLikeMetadata;
use crate::persistence::backends::{Error as PersistenceError, PersistenceBackend};

pub type CachedObjectVersion = u64;
pub type Uri = Vec<u8>;

const BLOB_EXTENSION: &str = ".blob";
const METADATA_EXTENSION: &str = ".metadata";
const EMPTY_STORAGE_VERSION: CachedObjectVersion = 0;

#[derive(Debug, Clone, Serialize, Deserialize)]
enum EventType {
    Update(FileLikeMetadata),
    Delete,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MetadataEvent {
    uri: Uri,
    version: CachedObjectVersion,
    type_: EventType,
}

impl MetadataEvent {
    pub fn new(uri: Uri, version: CachedObjectVersion, type_: EventType) -> Self {
        Self {
            uri,
            version,
            type_,
        }
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

/// The `CachedObjectStorage` is a data structure that provides
/// the following interface:
/// 1. Upsert an object denoted by its URI, value, and metadata. Each upsert
///    creates a new version of the data structure.
/// 2. Remove an object by its URI. Each removal creates a new version of the
///    data structure.
/// 3. Lookup: find an object by its URI, check if an object with the given
///    URI is present, get the metadata of the object, etc. The lookups are
///    performed on the latest state of the data structure.
/// 4. Rewind to the given version of the data structure in part. The rewind
///    alters the state of the data structure: all versions that follow
///    the given one are removed and are no longer accessible. All versions that are
///    obsolete after the rewind, e.g., those that don't correspond to the latest state of
///    any URI, are also removed.
///
/// The versions are numbered with consecutive integers from 1 onwards. Rewind to
/// version 0 corresponds to the cleanup of the storage.
///
/// The implementation is as follows:
/// * There are two types of events: object addition and object removal.
///    These events are stored both in the selected durable storage and in several
///    in-memory indexes, denoting events by version; sorted event sequences by the
///    object URI and the snapshot - the actual, version-unaware state of the data
///    structure.
/// * When the data structure starts, it reads the sequence of events and
///    constructs the mappings described above.
/// * When a rewind takes place, the versions that need to be deleted are detected
///    and undone one by one, starting from the latest. Note that these events are
///    also removed from the durable storage.
/// * When a lookup takes place, the snapshot is used.
/// * When an upsert or removal takes place, a new version is created. An event
///    corresponding to this version is added to the durable storage and to the local
///    event indexes. It is also reflected in a locally stored snapshot.
#[derive(Debug)]
pub struct CachedObjectStorage {
    backend: Box<dyn PersistenceBackend>,
    event_by_version: HashMap<CachedObjectVersion, MetadataEvent>,
    versions_by_uri: HashMap<Uri, Vec<CachedObjectVersion>>,
    snapshot: HashMap<Uri, FileLikeMetadata>,
    current_version: CachedObjectVersion,
}

impl CachedObjectStorage {
    pub fn new(backend: Box<dyn PersistenceBackend>) -> Result<Self, PersistenceError> {
        let mut event_by_version = HashMap::new();
        let mut versions_by_uri = HashMap::new();
        let mut current_version = EMPTY_STORAGE_VERSION + 1;
        let keys = backend.list_keys()?;

        // If slow, use thread pool
        for key in keys {
            if !key.ends_with(METADATA_EXTENSION) {
                continue;
            }
            let object = backend.get_value(&key)?;
            let event = MetadataEvent::parse(&object)?;
            let version = event.version;

            versions_by_uri
                .entry(event.uri.clone())
                .or_insert(Vec::new())
                .push(version);
            event_by_version.insert(version, event);
            current_version = max(current_version, version + 1);
        }
        for versions in versions_by_uri.values_mut() {
            versions.sort_unstable();
        }
        let snapshot = Self::construct_snapshot(&event_by_version, &versions_by_uri);

        Ok(Self {
            backend,
            event_by_version,
            versions_by_uri,
            snapshot,
            current_version,
        })
    }

    pub fn clear(&mut self) -> Result<(), PersistenceError> {
        self.rewind(EMPTY_STORAGE_VERSION)
    }

    pub fn rewind(&mut self, target_version: CachedObjectVersion) -> Result<(), PersistenceError> {
        assert!(target_version < self.current_version, "Target version ({target_version}) can't be greater or equal to the current version ({})", self.current_version);
        let mut versions_for_removal = Vec::new();
        for version in self.event_by_version.keys() {
            if *version > target_version {
                versions_for_removal.push(*version);
            }
        }
        versions_for_removal.sort_by(|lhs, rhs| rhs.cmp(lhs));

        for version in versions_for_removal {
            self.remove_version_from_storage(version)?;
            let event = self
                .event_by_version
                .remove(&version)
                .expect("Metadata must be present for version {version}");
            let uri = event.uri;
            let versions_by_uri = self
                .versions_by_uri
                .get_mut(&uri)
                .expect("Versions by URI must be present for version {version}");
            assert_eq!(versions_by_uri.pop(), Some(version));
            if versions_by_uri.is_empty() {
                self.snapshot.remove(&uri);
                self.versions_by_uri.remove(&uri);
            } else {
                let new_actual_version = versions_by_uri[versions_by_uri.len() - 1];
                let new_actual_event = self
                    .event_by_version
                    .get(&new_actual_version)
                    .expect("Metadata must be present for {new_actual_version}");
                let _ = match &new_actual_event.type_ {
                    EventType::Delete => self.snapshot.remove(&uri),
                    EventType::Update(metadata) => self.snapshot.insert(uri, metadata.clone()),
                };
            }
        }
        self.remove_obsolete_versions(target_version)?;
        self.current_version = target_version + 1;

        Ok(())
    }

    pub fn remove_obsolete_versions(
        &mut self,
        stable_version: CachedObjectVersion,
    ) -> Result<(), PersistenceError> {
        let mut obsolete_uris = Vec::new();
        let mut obsolete_events = Vec::new();
        for (uri, versions) in &mut self.versions_by_uri {
            let mut actual_versions = Vec::new();
            let mut is_version_obsolete = false;
            for version in versions.iter().rev() {
                if is_version_obsolete {
                    let event = self
                        .event_by_version
                        .remove(version)
                        .expect("Event must be present for version {version}");
                    obsolete_events.push(event);
                } else {
                    actual_versions.push(*version);
                    if *version <= stable_version {
                        is_version_obsolete = true;
                    }
                }
            }
            assert!(!actual_versions.is_empty());
            let first_actual_version = actual_versions[actual_versions.len() - 1];
            let event = self
                .event_by_version
                .get(&first_actual_version)
                .expect("Event must be present for version {first_actual_version}");
            if matches!(event.type_, EventType::Delete) {
                obsolete_events.push(
                    self.event_by_version
                        .remove(&first_actual_version)
                        .expect("Event must be present for version {first_actual_version}"),
                );
                actual_versions.pop();
            }
            actual_versions.reverse();
            *versions = actual_versions;
            if versions.is_empty() {
                obsolete_uris.push(uri.clone());
            }
        }

        // If slow, can be parallelized since no shared state is used here.
        for event in obsolete_events {
            if let Err(e) = Self::remove_event_with_backend(self.backend.as_mut(), &event) {
                error!("Failed to remove obsolete event {event:?} from the storage: {e:?}");
            }
        }

        for uri in obsolete_uris {
            self.versions_by_uri.remove(&uri);
        }

        Ok(())
    }

    pub fn place_object(
        &mut self,
        uri: &[u8],
        contents: Vec<u8>,
        metadata: FileLikeMetadata,
    ) -> Result<(), PersistenceError> {
        let version = self.next_available_version();
        let object_key = Self::cached_object_path(version);

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
        let event = MetadataEvent::new(uri.to_vec(), version, EventType::Update(metadata));
        self.write_metadata_event(&event)?;

        Ok(())
    }

    pub fn remove_object(&mut self, uri: &[u8]) -> Result<(), PersistenceError> {
        let version = self.next_available_version();
        let event = MetadataEvent::new(uri.to_vec(), version, EventType::Delete);
        self.write_metadata_event(&event)
    }

    pub fn contains_object(&self, uri: &[u8]) -> bool {
        self.snapshot.contains_key(uri)
    }

    pub fn get_iter(&self) -> Iter<Uri, FileLikeMetadata> {
        self.snapshot.iter()
    }

    pub fn stored_metadata(&self, uri: &[u8]) -> Option<&FileLikeMetadata> {
        self.snapshot.get(uri)
    }

    pub fn get_object(&self, uri: &[u8]) -> Result<Vec<u8>, PersistenceError> {
        let last_version = self
            .versions_by_uri
            .get(uri)
            .map(|v| v[v.len() - 1])
            .ok_or(PersistenceError::NoAvailableVersions)?;
        let object_key = Self::cached_object_path(last_version);
        self.backend.get_value(&object_key)
    }

    pub fn actual_version(&self) -> CachedObjectVersion {
        self.current_version - 1
    }

    // Below are helper methods

    fn construct_snapshot(
        event_by_version: &HashMap<CachedObjectVersion, MetadataEvent>,
        versions_by_uri: &HashMap<Uri, Vec<CachedObjectVersion>>,
    ) -> HashMap<Uri, FileLikeMetadata> {
        let mut snapshot = HashMap::new();
        for versions in versions_by_uri.values() {
            let last_version = versions[versions.len() - 1];
            let event = event_by_version
                .get(&last_version)
                .expect("Event for version {last_version} must be present");
            if let EventType::Update(metadata) = &event.type_ {
                snapshot.insert(event.uri.clone(), metadata.clone());
            }
        }
        snapshot
    }

    fn write_metadata_event(&mut self, event: &MetadataEvent) -> Result<(), PersistenceError> {
        let version = event.version;
        let uri = event.uri.clone();
        let metadata_key = Self::metadata_path(version);

        futures::executor::block_on(async {
            let serialized_entry = event.serialize();
            self.backend
                .put_value(&metadata_key, serialized_entry.as_bytes().to_vec())
                .await
                .unwrap()
        })?;

        self.event_by_version.insert(version, event.clone());
        self.versions_by_uri
            .entry(uri.clone())
            .or_default()
            .push(version);

        let _ = match event.type_ {
            EventType::Update(ref metadata) => self.snapshot.insert(uri, metadata.clone()),
            EventType::Delete => self.snapshot.remove(&uri),
        };

        Ok(())
    }

    fn remove_version_from_storage(
        &mut self,
        version: CachedObjectVersion,
    ) -> Result<(), PersistenceError> {
        let metadata = self
            .event_by_version
            .get(&version)
            .ok_or(PersistenceError::NoAvailableVersions)?;
        Self::remove_event_with_backend(self.backend.as_mut(), metadata)
    }

    fn remove_event_with_backend(
        backend: &mut dyn PersistenceBackend,
        event: &MetadataEvent,
    ) -> Result<(), PersistenceError> {
        let metadata_key = Self::metadata_path(event.version);
        backend.remove_key(&metadata_key)?;
        if matches!(event.type_, EventType::Update(_)) {
            let object_key = Self::cached_object_path(event.version);
            backend.remove_key(&object_key)?;
        }
        Ok(())
    }

    fn next_available_version(&mut self) -> u64 {
        self.current_version += 1;
        self.current_version - 1
    }

    fn cached_object_path(version: CachedObjectVersion) -> String {
        format!("{version}{BLOB_EXTENSION}")
    }

    fn metadata_path(version: CachedObjectVersion) -> String {
        format!("{version}{METADATA_EXTENSION}")
    }
}
