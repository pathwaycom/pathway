use log::{debug, error, info, warn};
use std::cmp::{max, min};
use std::collections::hash_map::Iter;
use std::collections::{HashMap, HashSet};
use std::mem::take;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures::channel::oneshot::Receiver as OneShotReceiver;
use lz4_flex::block::{compress_prepend_size, decompress_size_prepended};
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use rayon::{ThreadPool, ThreadPoolBuilder};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use tempfile::TempDir;

use crate::connectors::metadata::FileLikeMetadata;
use crate::persistence::backends::{Error as PersistenceError, PersistenceBackend};

pub type CachedObjectsBatchId = u64;
pub type CachedObjectVersion = u64;
pub type Uri = Vec<u8>;
pub type UriRef<'a> = &'a [u8];
pub type SharedCachedObjectsExternalAccessor = Arc<Mutex<CachedObjectsExternalAccessor>>;

const BLOB_EXTENSION: &str = ".blob";
const METADATA_EXTENSION: &str = ".metadata";
const EMPTY_STORAGE_VERSION: CachedObjectVersion = 0;
const EMPTY_STORAGE_BATCH_ID: CachedObjectsBatchId = 0;
const BLOB_READER_POOL_SIZE: usize = 8;

/// Threshold for the number of events in a batch considered "small".
///
/// If both the number of events and the blob length of a batch aren't exceeding these values,
/// the system will reuse the existing batch for writing. This means that during the next
/// write operation, the data will be overwritten.
///
/// This behavior is intentional: for objects this small, the cost of overwriting is considered
/// negligible. The trade-off improves restart efficiency, as fewer objects need to be scanned
/// sequentially during recovery.
const SMALL_BATCH_EVENTS_COUNT: usize = 10_000;
const SMALL_BATCH_BLOB_LENGTH: usize = 262_144;

/// Upper threshold for the number of events in a batch considered "large".
///
/// As soon as either of these is exceeded, the batch is flushed automatically,
/// even if the user hasn't explicitly called `flush()`.
const LARGE_BATCH_EVENTS_COUNT: usize = 100_000;
const LARGE_BATCH_BLOB_LENGTH: usize = 200_000_000;

#[derive(Debug, Clone, Serialize, Deserialize)]
enum EventType {
    Update(FileLikeMetadata),
    Delete,
}

#[derive(Debug)]
struct BlobSegment {
    uri: Uri,
    object_blob_start: usize,
    object_blob_len: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MetadataEvent {
    uri: Uri,
    version: CachedObjectVersion,
    type_: EventType,
    batch_id: CachedObjectsBatchId,
    object_blob_start: usize,
    object_blob_len: usize,
}

impl MetadataEvent {
    pub fn new(
        uri: Uri,
        version: CachedObjectVersion,
        type_: EventType,
        batch_id: CachedObjectsBatchId,
        object_blob_start: usize,
        object_blob_len: usize,
    ) -> Self {
        Self {
            uri,
            version,
            type_,
            batch_id,
            object_blob_start,
            object_blob_len,
        }
    }

    pub fn as_blob_segment(&self) -> BlobSegment {
        BlobSegment {
            uri: self.uri.clone(),
            object_blob_start: self.object_blob_start,
            object_blob_len: self.object_blob_len,
        }
    }
}

#[derive(Debug)]
struct CurrentUpload {
    batch_id: CachedObjectsBatchId,
    blob_future: OneShotReceiver<Result<(), PersistenceError>>,
    metadata_future: OneShotReceiver<Result<(), PersistenceError>>,
}

impl CurrentUpload {
    async fn wait_for_completion(self) -> Result<(), PersistenceError> {
        let blob_result = self.blob_future.await.unwrap();
        let metadata_result = self.metadata_future.await.unwrap();
        if let Err(e) = blob_result {
            error!("Failed to save blob for batch {}: {e:?}", self.batch_id);
            return Err(e);
        }
        if let Err(e) = metadata_result {
            error!("Failed to save metadata for batch {}: {e:?}", self.batch_id);
            Err(e)
        } else {
            Ok(())
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EventsBatch {
    batch_id: CachedObjectsBatchId,
    events: Vec<MetadataEvent>,
}

impl EventsBatch {
    pub fn new(batch_id: CachedObjectsBatchId) -> Self {
        Self {
            batch_id,
            events: Vec::new(),
        }
    }

    fn add_event(&mut self, event: MetadataEvent) {
        if let Some(last_event) = self.events.last() {
            assert!(
                last_event.version < event.version,
                "Versions must monotonically increase"
            );
        }
        self.events.push(event);
    }

    fn shrink_to_version(&mut self, version: CachedObjectVersion) {
        let last_fit_index = match self
            .events
            .binary_search_by_key(&version, |event| event.version)
        {
            Ok(index) => index,
            Err(index) => {
                warn!(
                    "Requested to shrink batch {} to version {version} but it doesn't exist here",
                    self.batch_id
                );
                if index == self.events.len() {
                    // All events are below the cut version
                    warn!(
                        "Attempted to shrink the batch {} which is fully before the target version. Requested version: {version}, watermark: {:?}", 
                        self.batch_id,
                        self.watermark(),
                    );
                    return;
                }
                if index == 0 {
                    // All events are older than the cut version
                    // meaning that the whole block must be deleted
                    warn!(
                        "Attempted to shrink the batch {} which has no matching IDs. Requested version: {version}, watermark: {:?}",
                        self.batch_id,
                        self.watermark(),
                    );
                    self.events.clear();
                    return;
                }
                index - 1
            }
        };
        self.events.truncate(last_fit_index + 1);
    }

    fn watermark(&self) -> Option<(CachedObjectVersion, CachedObjectVersion)> {
        if self.events.is_empty() {
            None
        } else {
            let lowest_version = self.events[0].version;
            let highest_version = self.events[self.events.len() - 1].version;
            Some((lowest_version, highest_version))
        }
    }
}

#[derive(Debug)]
pub struct CachedObjectsExternalAccessor {
    backend: Box<dyn PersistenceBackend>,
    current_batch: EventsBatch,
    current_blobs: Vec<u8>,
    has_changes: bool,
    current_uploads: Vec<CurrentUpload>,
}

impl CachedObjectsExternalAccessor {
    pub fn new(backend: Box<dyn PersistenceBackend>, batch_id: CachedObjectsBatchId) -> Self {
        Self {
            backend,
            current_batch: EventsBatch::new(batch_id),
            current_blobs: Vec::new(),
            has_changes: false,
            current_uploads: Vec::new(),
        }
    }

    pub fn start_forced_state_upload(&mut self) -> Result<(), PersistenceError> {
        if self.has_changes {
            let current_upload = Self::start_upload_with_backend(
                self.backend.as_ref(),
                &self.current_batch,
                &self.current_blobs,
            )?;
            self.current_uploads.push(current_upload);
        }
        self.start_new_batch_after_save();
        Ok(())
    }

    fn start_upload_with_backend(
        backend: &dyn PersistenceBackend,
        batch: &EventsBatch,
        blobs: &[u8],
    ) -> Result<CurrentUpload, PersistenceError> {
        let metadata_key = Self::metadata_batch_path(batch.batch_id);
        let cached_objects_key = Self::cached_objects_path(batch.batch_id);

        let compress_started_at = Instant::now();
        let compressed = compress_prepend_size(blobs);
        debug!(
            "Saving current batch. Events count: {}. Blobs compression done: {} -> {}, time elapsed: {:?}",
            batch.events.len(),
            blobs.len(),
            compressed.len(),
            compress_started_at.elapsed()
        );

        let serialized_entry =
            bincode::serialize(batch).map_err(|err| PersistenceError::Bincode(*err))?;

        let blob_future = backend.put_value(&cached_objects_key, compressed);
        let metadata_future = backend.put_value(&metadata_key, serialized_entry);

        Ok(CurrentUpload {
            batch_id: batch.batch_id,
            blob_future,
            metadata_future,
        })
    }

    fn start_new_batch_after_save(&mut self) {
        self.has_changes = false;

        let is_small_batch = self.current_batch.events.len() <= SMALL_BATCH_EVENTS_COUNT
            && self.current_blobs.len() <= SMALL_BATCH_BLOB_LENGTH;
        if !is_small_batch {
            self.current_batch.batch_id += 1;
            self.current_batch.events.clear();
            self.current_blobs.clear();
        }
    }

    pub fn wait_for_all_uploads(&mut self) -> Result<(), PersistenceError> {
        let upload_start = Instant::now();
        futures::executor::block_on(async {
            for upload in take(&mut self.current_uploads) {
                upload.wait_for_completion().await?;
            }
            Ok::<_, PersistenceError>(())
        })?;

        let upload_elapsed = upload_start.elapsed();
        if upload_elapsed > Duration::from_secs(1) {
            info!(
                "Cached objects storage: all uploads have finished. Time elapsed: {:?}",
                upload_start.elapsed()
            );
        }

        Ok(())
    }

    fn shrink_to_version(
        &mut self,
        mut batch: EventsBatch,
        version: CachedObjectVersion,
    ) -> Result<(EventsBatch, Vec<u8>), PersistenceError> {
        batch.shrink_to_version(version);
        if batch.events.is_empty() {
            self.clean(batch.batch_id)?;
            return Ok((batch, Vec::with_capacity(0)));
        }
        let last_event = batch
            .events
            .last()
            .expect("batch can't be empty at this point");
        let last_blob_finish = last_event.object_blob_start + last_event.object_blob_len;
        let mut blobs = self.download_blobs(batch.batch_id)?;
        blobs.truncate(last_blob_finish);

        let upload = Self::start_upload_with_backend(self.backend.as_ref(), &batch, &blobs)?;
        futures::executor::block_on(async { upload.wait_for_completion().await })?;

        Ok((batch, blobs))
    }

    fn download_blobs(&self, batch_id: CachedObjectsBatchId) -> Result<Vec<u8>, PersistenceError> {
        Self::download_blobs_with_backend(self.backend.as_ref(), batch_id)
    }

    fn clean(&self, batch_id: CachedObjectsBatchId) -> Result<(), PersistenceError> {
        Self::clean_with_backend(self.backend.as_ref(), batch_id)
    }

    fn place_object(
        &mut self,
        version: CachedObjectVersion,
        uri: Uri,
        metadata: FileLikeMetadata,
        contents: Vec<u8>,
    ) -> Result<MetadataEvent, PersistenceError> {
        let event = MetadataEvent::new(
            uri,
            version,
            EventType::Update(metadata),
            self.current_batch.batch_id,
            self.current_blobs.len(),
            contents.len(),
        );
        self.add_event(event.clone(), contents)?;
        Ok(event)
    }

    fn remove_object(
        &mut self,
        version: CachedObjectVersion,
        uri: Uri,
    ) -> Result<MetadataEvent, PersistenceError> {
        let event = MetadataEvent::new(
            uri,
            version,
            EventType::Delete,
            self.current_batch.batch_id,
            self.current_blobs.len(),
            0,
        );
        self.add_event(event.clone(), Vec::with_capacity(0))?;
        Ok(event)
    }

    fn add_event(
        &mut self,
        event: MetadataEvent,
        mut blob: Vec<u8>,
    ) -> Result<(), PersistenceError> {
        self.current_batch.add_event(event);
        self.current_blobs.append(&mut blob);
        self.has_changes = true;

        let is_large_batch = self.current_batch.events.len() >= LARGE_BATCH_EVENTS_COUNT
            || self.current_blobs.len() >= LARGE_BATCH_BLOB_LENGTH;
        if is_large_batch {
            let current_upload = Self::start_upload_with_backend(
                self.backend.as_ref(),
                &self.current_batch,
                &self.current_blobs,
            )?;
            self.current_uploads.push(current_upload);
            self.start_new_batch_after_save();
        }

        Ok(())
    }

    fn clean_with_backend(
        backend: &dyn PersistenceBackend,
        batch_id: CachedObjectsBatchId,
    ) -> Result<(), PersistenceError> {
        let metadata_key = Self::metadata_batch_path(batch_id);
        let cached_objects_key = Self::cached_objects_path(batch_id);
        backend.remove_key(&metadata_key)?;
        backend.remove_key(&cached_objects_key)
    }

    fn download_blobs_with_backend(
        backend: &dyn PersistenceBackend,
        batch_id: CachedObjectsBatchId,
    ) -> Result<Vec<u8>, PersistenceError> {
        let key = Self::cached_objects_path(batch_id);
        let compressed_blobs = backend.get_value(&key)?;
        let blobs = decompress_size_prepended(compressed_blobs.as_slice())?;
        Ok(blobs)
    }

    fn load_blobs_into_snapshot(
        backend: &dyn PersistenceBackend,
        batch_id: CachedObjectsBatchId,
        segments: &[BlobSegment],
        object_snapshot: &Mutex<&mut SqliteObjectsSnapshot>,
    ) -> Result<(), PersistenceError> {
        let key = Self::cached_objects_path(batch_id);
        let compressed_blobs = backend.get_value(&key)?;

        let mut object_snapshot = object_snapshot.lock().unwrap();
        let blobs = decompress_size_prepended(compressed_blobs.as_slice())?;
        object_snapshot.insert_segments(segments, &blobs)?;
        Ok(())
    }

    fn metadata_batch_path(batch_id: CachedObjectsBatchId) -> String {
        format!("{batch_id:018}{METADATA_EXTENSION}")
    }

    fn cached_objects_path(batch_id: CachedObjectsBatchId) -> String {
        format!("{batch_id:018}{BLOB_EXTENSION}")
    }
}

const SQLITE_CREATE_CACHE_SQL: &str =
    "CREATE TABLE IF NOT EXISTS objects (uri BLOB PRIMARY KEY, contents BLOB NOT NULL);";
const SQLITE_UPSERT_SQL: &str = "INSERT OR REPLACE INTO objects (uri, contents) VALUES (?1, ?2)";
const SQLITE_DELETE_SQL: &str = "DELETE FROM objects WHERE uri = ?1";
const SQLITE_GET_SQL: &str = "SELECT contents FROM objects WHERE uri = ?1";
const SQLITE_INSERT_SQL: &str = "INSERT INTO objects (uri, contents) VALUES (?1, ?2)";

#[derive(Debug)]
pub struct SqliteObjectsSnapshot {
    _tempdir: TempDir, // The tempdir is deleted on `drop`, so we keep it
    conn: Connection,
}

impl SqliteObjectsSnapshot {
    pub fn new() -> Result<Self, PersistenceError> {
        let tempdir = tempfile::TempDir::new()?;
        let db_path = tempdir.path().join("snapshot.sqlite3");
        let conn = Connection::open(db_path)?;

        // The recovery benchmark, using ~750,000 objects with a total
        // size of 11 GB, initially took about 3m20s. By tweaking
        // the database with certain PRAGMA settings, this time can be
        // reduced to ~30s, which is on par with a
        // "single key = single file" object approach, but is much more
        // efficient in terms of inode usage: here we use only O(1) inodes.
        //
        // https://www.sqlite.org/pragma.html#pragma_journal_mode
        // The database does not maintain a rollback journal, which
        // reduces the number of disk writes. As a consequence,
        // rollbacks are not possible. But we don't use them anyway.
        conn.pragma_update(None, "journal_mode", "OFF")?;

        // https://www.sqlite.org/pragma.html#pragma_synchronous
        // The synchronous setting controls whether SQLite asks the
        // operating system to flush data to stable storage (`fsync`) at
        // critical moments.
        //
        // With `synchronous = OFF (0)`, SQLite continues as soon as it
        // has handed data off to the OS. If the SQLite process crashes,
        // that data will generally be preserved by the OS; however, the
        // database can be corrupted if the operating system crashes or
        // the machine loses power before the OS has actually written
        // the buffers to the disk surface. Commits can be orders of
        // magnitude faster with `OFF`, which is acceptable for an
        // ephemeral cache.
        conn.pragma_update(None, "synchronous", "OFF")?;

        // https://www.sqlite.org/pragma.html#pragma_temp_store
        // Temporary structures are stored in memory. They are
        // relatively small, and this still improves performance.
        conn.pragma_update(None, "temp_store", "MEMORY")?;

        // https://www.sqlite.org/pragma.html#pragma_locking_mode
        // The database is taken into exclusive locking mode, so locks are
        // not checked on every operation, avoiding that overhead.
        conn.pragma_update(None, "locking_mode", "EXCLUSIVE")?;

        conn.execute(SQLITE_CREATE_CACHE_SQL, [])?;
        Ok(Self {
            _tempdir: tempdir,
            conn,
        })
    }

    fn insert(&mut self, uri: UriRef, contents: &[u8]) -> Result<(), PersistenceError> {
        self.conn
            .execute(SQLITE_UPSERT_SQL, params![uri, contents])?;
        Ok(())
    }

    fn insert_segments(&mut self, segments: &[BlobSegment], blobs: &[u8]) -> rusqlite::Result<()> {
        let sqlite_tx = self.conn.unchecked_transaction()?;
        {
            let mut sqlite_stmt = sqlite_tx.prepare_cached(SQLITE_INSERT_SQL)?;
            for segment in segments {
                let object_slice = &blobs[segment.object_blob_start
                    ..segment.object_blob_start + segment.object_blob_len];
                sqlite_stmt.execute(rusqlite::params![segment.uri, object_slice])?;
            }
        }
        sqlite_tx.commit()?;
        Ok(())
    }

    fn remove(&mut self, uri: UriRef) -> Result<(), PersistenceError> {
        let affected = self.conn.execute(SQLITE_DELETE_SQL, params![uri])?;
        if affected == 0 {
            return Err(PersistenceError::NoCachedObject);
        }
        Ok(())
    }

    fn get(&self, uri: UriRef) -> Result<Vec<u8>, PersistenceError> {
        let result: Option<Vec<u8>> = self
            .conn
            .query_row(SQLITE_GET_SQL, params![uri], |row| row.get(0))
            .optional()?;

        result.ok_or(PersistenceError::NoCachedObject)
    }
}

pub struct CachedObjectStorage {
    external_accessor: Arc<Mutex<CachedObjectsExternalAccessor>>,
    metadata_snapshot: HashMap<Uri, FileLikeMetadata>,
    objects_snapshot: SqliteObjectsSnapshot,
    current_version: CachedObjectVersion,
}

impl CachedObjectStorage {
    pub fn new(backend: Box<dyn PersistenceBackend>) -> Result<Self, PersistenceError> {
        Ok(Self {
            external_accessor: Arc::new(Mutex::new(CachedObjectsExternalAccessor::new(
                backend,
                EMPTY_STORAGE_BATCH_ID + 1,
            ))),
            metadata_snapshot: HashMap::new(),
            objects_snapshot: SqliteObjectsSnapshot::new()?,
            current_version: EMPTY_STORAGE_VERSION + 1,
        })
    }

    pub fn clear(&mut self) -> Result<(), PersistenceError> {
        self.start_from_stable_version(EMPTY_STORAGE_VERSION)
    }

    /// Called at most once, before any object updates or removals.
    /// If not called, the storage starts clean, without any cached objects stored.
    pub fn start_from_stable_version(
        &mut self,
        target_version: CachedObjectVersion,
    ) -> Result<(), PersistenceError> {
        info!("Cached objects storage starts from the latest stable version: {target_version}");
        assert!(
            self.metadata_snapshot.is_empty(),
            "start_from_stable_version can only be called before any object operations"
        );

        // At the moment of the initialization, nobody uses the external accessor,
        // so we can acquire mutex for the whole duration of the initialization
        let mut external_accessor = self.external_accessor.lock().unwrap();

        let mut keys = external_accessor.backend.list_keys()?;
        keys.sort();

        let mut global_highest_version = EMPTY_STORAGE_VERSION;
        let mut current_batch_id = EMPTY_STORAGE_BATCH_ID + 1;
        let mut downloaded_blobs = HashMap::with_capacity(1);
        let mut latest_event_by_uri = HashMap::new();
        let mut existing_batch_ids = HashSet::new();

        for key in keys {
            if global_highest_version >= target_version {
                // The target_version has already been reached,
                // therefore the block is either obsolete or goes beyond
                // the requested version.
                info!("Global highest version {global_highest_version} is already higher than the target version {target_version}: removing the block '{key}'");
                external_accessor.backend.remove_key(&key)?;
                continue;
            }
            if !key.ends_with(METADATA_EXTENSION) {
                continue;
            }

            let object = external_accessor.backend.get_value(&key)?;
            let mut batch: EventsBatch =
                bincode::deserialize(&object).map_err(|err| PersistenceError::Bincode(*err))?;

            // The object can be removed in one of the following cases:
            // 1. All versions in the batch come after the target version
            // 2. The block is obsolete: its highest version is lower than the one already seen
            let Some((lowest_version, highest_version)) = batch.watermark() else {
                error!(
                    "Empty cached objects batch: {}. The watermark is None.",
                    batch.batch_id
                );
                external_accessor.clean(batch.batch_id)?;
                continue;
            };
            if lowest_version > target_version {
                info!(
                    "Removing the batch {} that is fully beyond the target version {target_version}. Watermark: ({lowest_version}, {highest_version})",
                    batch.batch_id
                );
                external_accessor.clean(batch.batch_id)?;
                continue;
            }

            // If some of the versions go after the target one, the block must be shrank
            if highest_version > target_version {
                info!(
                    "The batch {} must be split due to watermark ({lowest_version}, {highest_version}) being dissected by a target version {target_version}",
                    batch.batch_id
                );
                let (shrank_batch, blobs) =
                    external_accessor.shrink_to_version(batch, target_version)?;
                batch = shrank_batch;
                if !blobs.is_empty() {
                    downloaded_blobs.insert(batch.batch_id, blobs);
                }
            }

            global_highest_version = max(global_highest_version, highest_version);
            current_batch_id = max(current_batch_id, batch.batch_id + 1);
            existing_batch_ids.insert(batch.batch_id);
            for event in batch.events {
                latest_event_by_uri.insert(event.uri.clone(), event);
            }
        }

        self.current_version = target_version + 1;
        external_accessor.current_batch = EventsBatch::new(current_batch_id);

        drop(external_accessor); // Release the mutex, not to pass mutable reference in the method that mutates the state
        self.build_snapshots(latest_event_by_uri, downloaded_blobs, &existing_batch_ids)
    }

    pub fn place_object(
        &mut self,
        uri: UriRef,
        contents: &[u8],
        metadata: FileLikeMetadata,
    ) -> Result<(), PersistenceError> {
        let version = self.next_available_version();
        let event = self.external_accessor.lock().unwrap().place_object(
            version,
            uri.to_vec(),
            metadata,
            contents.to_owned(),
        )?;
        self.apply_metadata_event(event, contents)
    }

    pub fn remove_object(&mut self, uri: UriRef) -> Result<(), PersistenceError> {
        let version = self.next_available_version();
        let event = self
            .external_accessor
            .lock()
            .unwrap()
            .remove_object(version, uri.to_vec())?;
        self.apply_metadata_event(event, &[])
    }

    pub fn contains_object(&self, uri: UriRef) -> bool {
        self.metadata_snapshot.contains_key(uri)
    }

    pub fn get_iter(&self) -> Iter<Uri, FileLikeMetadata> {
        self.metadata_snapshot.iter()
    }

    pub fn stored_metadata(&self, uri: UriRef) -> Option<&FileLikeMetadata> {
        self.metadata_snapshot.get(uri)
    }

    pub fn get_object(&self, uri: UriRef) -> Result<Vec<u8>, PersistenceError> {
        self.objects_snapshot.get(uri)
    }

    pub fn actual_version(&self) -> CachedObjectVersion {
        self.current_version - 1
    }

    pub fn get_external_accessor(&self) -> SharedCachedObjectsExternalAccessor {
        self.external_accessor.clone()
    }

    // Below are helper methods

    fn build_snapshots(
        &mut self,
        latest_event_by_uri: HashMap<Uri, MetadataEvent>,
        downloaded_blobs: HashMap<CachedObjectsBatchId, Vec<u8>>,
        existing_batch_ids: &HashSet<CachedObjectsBatchId>,
    ) -> Result<(), PersistenceError> {
        let mut segments_for_download = HashMap::new();
        let mut actual_batch_ids = HashSet::new();
        for (_, event) in latest_event_by_uri {
            actual_batch_ids.insert(event.batch_id);

            let blob_segment = event.as_blob_segment();
            let EventType::Update(metadata) = event.type_ else {
                continue;
            };

            segments_for_download
                .entry(event.batch_id)
                .or_insert_with(Vec::new)
                .push(blob_segment);

            self.metadata_snapshot.insert(event.uri, metadata);
        }
        info!(
            "The metadata snapshot contains {} objects. There are {} batch blobs to download.",
            self.metadata_snapshot.len(),
            segments_for_download.len(),
        );

        let obsolete_batch_ids: Vec<_> = existing_batch_ids.difference(&actual_batch_ids).collect();

        let mut external_accessor = self.external_accessor.lock().unwrap();
        let backend = &mut external_accessor.backend;

        let max_parallel_access_requests =
            max(segments_for_download.len(), obsolete_batch_ids.len());
        let workers = ThreadPoolBuilder::new()
            .num_threads(min(BLOB_READER_POOL_SIZE, max_parallel_access_requests))
            .build()
            .expect("Failed to create downloader pool");

        Self::build_objects_snapshot(
            &workers,
            backend.as_ref(),
            segments_for_download,
            downloaded_blobs,
            &Mutex::new(&mut self.objects_snapshot),
        )?;

        Self::remove_obsolete_batches(&workers, backend.as_ref(), obsolete_batch_ids.as_slice())?;

        Ok(())
    }

    fn build_objects_snapshot(
        download_workers: &ThreadPool,
        backend: &dyn PersistenceBackend,
        mut segments_for_download: HashMap<CachedObjectsBatchId, Vec<BlobSegment>>,
        downloaded_blobs: HashMap<CachedObjectsBatchId, Vec<u8>>,
        object_snapshot: &Mutex<&mut SqliteObjectsSnapshot>,
    ) -> Result<(), PersistenceError> {
        for (batch_id, blobs) in downloaded_blobs {
            let Some(segments) = segments_for_download.remove(&batch_id) else {
                warn!("There is a predownloaded batch that is not needed for objects recovery: {batch_id}");
                continue;
            };
            let mut object_snapshot = object_snapshot.lock().unwrap();
            object_snapshot.insert_segments(&segments, &blobs)?;
        }

        let objects_snapshot_results: Vec<_> = download_workers.install(|| {
            segments_for_download
                .par_iter()
                .map(|(batch_id, batch_segments)| {
                    CachedObjectsExternalAccessor::load_blobs_into_snapshot(
                        backend,
                        *batch_id,
                        batch_segments.as_slice(),
                        object_snapshot,
                    )
                })
                .collect()
        });

        for snapshot_result in objects_snapshot_results {
            snapshot_result?;
        }

        Ok(())
    }

    fn remove_obsolete_batches(
        removal_workers: &ThreadPool,
        backend: &dyn PersistenceBackend,
        batch_ids: &[&CachedObjectsBatchId],
    ) -> Result<(), PersistenceError> {
        info!("Removing {} obsolete batches", batch_ids.len());
        let removal_results: Vec<_> = removal_workers.install(|| {
            batch_ids
                .par_iter()
                .map(|batch_id| {
                    CachedObjectsExternalAccessor::clean_with_backend(backend, **batch_id)
                })
                .collect()
        });

        for result in removal_results {
            result?;
        }
        Ok(())
    }

    fn apply_metadata_event(
        &mut self,
        event: MetadataEvent,
        contents: &[u8],
    ) -> Result<(), PersistenceError> {
        match event.type_ {
            EventType::Update(metadata) => {
                self.objects_snapshot.insert(&event.uri, contents)?;
                self.metadata_snapshot.insert(event.uri, metadata);
            }
            EventType::Delete => {
                self.objects_snapshot.remove(&event.uri)?;
                self.metadata_snapshot.remove(&event.uri);
            }
        }
        Ok(())
    }

    fn next_available_version(&mut self) -> u64 {
        self.current_version += 1;
        self.current_version - 1
    }
}
