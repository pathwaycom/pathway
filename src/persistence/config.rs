// Copyright © 2024 Pathway

#![allow(clippy::module_name_repetitions)]

use log::{error, info, warn};
use std::collections::HashMap;
use std::fs;
use std::io::Error as IoError;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use s3::bucket::Bucket as S3Bucket;

use crate::connectors::data_storage::S3CommandName;
use crate::connectors::data_storage::{ReadError, WriteError};
use crate::connectors::snapshot::{
    Event, LocalBinarySnapshotReader, LocalBinarySnapshotWriter, MockSnapshotReader,
    ReadSnapshotEvent, S3SnapshotReader, S3SnapshotWriter, SnapshotReader,
};
use crate::connectors::{PersistenceMode, SnapshotAccess};
use crate::deepcopy::DeepCopy;
use crate::engine::{Timestamp, TotalFrontier};
use crate::fs_helpers::ensure_directory;
use crate::persistence::metadata_backends::Error as MetadataBackendError;
use crate::persistence::metadata_backends::{
    FilesystemKVStorage, MetadataBackend, MockKVStorage, S3KVStorage,
};
use crate::persistence::state::MetadataAccessor;
use crate::persistence::{PersistentId, SharedSnapshotWriter};

const STREAMS_DIRECTORY_NAME: &str = "streams";

pub type ConnectorWorkerPair = (PersistentId, usize);

/// Metadata storage handles the frontier over all persisted data sources.
/// When we restart the computation, it will start from the frontier stored
/// in the metadata storage
#[derive(Debug, Clone)]
pub enum MetadataStorageConfig {
    Filesystem(PathBuf),
    S3 { bucket: S3Bucket, root_path: String },
    Mock,
}

/// Stream storage handles the snapshot, which will be loaded when Pathway
/// program restarts. It can also handle the cache for UDF calls
#[derive(Debug, Clone)]
pub enum StreamStorageConfig {
    Filesystem(PathBuf),
    S3 { bucket: S3Bucket, root_path: String },
    Mock(HashMap<ConnectorWorkerPair, Vec<Event>>),
}

/// Persistence in Pathway consists of two parts: actual frontier
/// storage and maintenance and snapshotting.
///
/// The outer config contains the configuration for these two parts,
/// which is passed from Python program by user.
#[derive(Debug, Clone)]
pub struct PersistenceManagerOuterConfig {
    snapshot_interval: Duration,
    metadata_storage: MetadataStorageConfig,
    stream_storage: StreamStorageConfig,
    snapshot_access: SnapshotAccess,
    persistence_mode: PersistenceMode,
    continue_after_replay: bool,
}

impl PersistenceManagerOuterConfig {
    pub fn new(
        snapshot_interval: Duration,
        metadata_storage: MetadataStorageConfig,
        stream_storage: StreamStorageConfig,
        snapshot_access: SnapshotAccess,
        persistence_mode: PersistenceMode,
        continue_after_replay: bool,
    ) -> Self {
        Self {
            snapshot_interval,
            metadata_storage,
            stream_storage,
            snapshot_access,
            persistence_mode,
            continue_after_replay,
        }
    }

    pub fn into_inner(self, worker_id: usize, total_workers: usize) -> PersistenceManagerConfig {
        PersistenceManagerConfig::new(self, worker_id, total_workers)
    }
}

/// The main persistent manager config, which, however can only be
/// constructed from within the worker
#[derive(Debug, Clone)]
pub struct PersistenceManagerConfig {
    metadata_storage: MetadataStorageConfig,
    stream_storage: StreamStorageConfig,
    pub snapshot_access: SnapshotAccess,
    pub persistence_mode: PersistenceMode,
    pub continue_after_replay: bool,
    pub worker_id: usize,
    pub snapshot_interval: Duration,
    total_workers: usize,
}

#[derive(Copy, Clone, Debug)]
pub enum ReadersQueryPurpose {
    ReadSnapshot,
    ReconstructFrontier,
}

impl ReadersQueryPurpose {
    pub fn includes_worker(
        &self,
        other_worker_id: usize,
        worker_id: usize,
        total_workers: usize,
    ) -> bool {
        match self {
            ReadersQueryPurpose::ReadSnapshot => {
                if other_worker_id % total_workers == worker_id {
                    if other_worker_id != worker_id {
                        info!(
                            "Assigning snapshot from the former worker {other_worker_id} to worker {} due to reduced number of worker threads",
                            worker_id
                        );
                    }
                    true
                } else {
                    false
                }
            }
            ReadersQueryPurpose::ReconstructFrontier => true,
        }
    }

    pub fn truncate_at_end(&self) -> bool {
        match self {
            ReadersQueryPurpose::ReadSnapshot => true,
            ReadersQueryPurpose::ReconstructFrontier => false,
        }
    }
}

impl PersistenceManagerConfig {
    pub fn new(
        outer_config: PersistenceManagerOuterConfig,
        worker_id: usize,
        total_workers: usize,
    ) -> Self {
        Self {
            stream_storage: outer_config.stream_storage,
            metadata_storage: outer_config.metadata_storage,
            snapshot_access: outer_config.snapshot_access,
            persistence_mode: outer_config.persistence_mode,
            continue_after_replay: outer_config.continue_after_replay,
            snapshot_interval: outer_config.snapshot_interval,
            worker_id,
            total_workers,
        }
    }

    pub fn create_metadata_storage(&self) -> Result<MetadataAccessor, MetadataBackendError> {
        let backend: Box<dyn MetadataBackend> = match &self.metadata_storage {
            MetadataStorageConfig::Filesystem(root_path) => {
                Box::new(FilesystemKVStorage::new(root_path)?)
            }
            MetadataStorageConfig::S3 { bucket, root_path } => {
                Box::new(S3KVStorage::new(bucket.deep_copy(), root_path))
            }
            MetadataStorageConfig::Mock => Box::new(MockKVStorage {}),
        };
        MetadataAccessor::new(backend, self.worker_id)
    }

    pub fn create_snapshot_readers(
        &self,
        persistent_id: PersistentId,
        threshold_times: &HashMap<usize, TotalFrontier<Timestamp>>,
        query_purpose: ReadersQueryPurpose,
    ) -> Result<Vec<SnapshotReader>, ReadError> {
        let mut result: Vec<SnapshotReader> = Vec::new();
        let reader_impls = match &self.stream_storage {
            StreamStorageConfig::Filesystem(root_path) => {
                let mut reader_impls = HashMap::<usize, Box<dyn ReadSnapshotEvent>>::new();
                let assigned_snapshot_paths =
                    self.assigned_snapshot_paths(root_path, persistent_id, query_purpose)?;
                for (worker_id, path) in assigned_snapshot_paths {
                    reader_impls.insert(worker_id, Box::new(LocalBinarySnapshotReader::new(path)?));
                }
                reader_impls
            }
            StreamStorageConfig::S3 { bucket, root_path } => {
                let mut reader_impls = HashMap::<usize, Box<dyn ReadSnapshotEvent>>::new();
                let assigned_snapshot_paths = self.assigned_s3_snapshot_paths(
                    &bucket.deep_copy(),
                    root_path,
                    persistent_id,
                    query_purpose,
                )?;
                for (worker_id, path) in assigned_snapshot_paths {
                    reader_impls.insert(
                        worker_id,
                        Box::new(S3SnapshotReader::new(bucket.deep_copy(), &path)?),
                    );
                }
                reader_impls
            }
            StreamStorageConfig::Mock(event_map) => {
                let mut reader_impls = HashMap::<usize, Box<dyn ReadSnapshotEvent>>::new();
                let events = event_map
                    .get(&(persistent_id, self.worker_id))
                    .unwrap_or(&vec![])
                    .clone();
                let reader = MockSnapshotReader::new(events);
                reader_impls.insert(self.worker_id, Box::new(reader));
                reader_impls
            }
        };

        let min_threshold_time = *threshold_times
            .values()
            .min()
            .unwrap_or(&TotalFrontier::At(Timestamp(0)));
        for (_, reader_impl) in reader_impls {
            result.push(SnapshotReader::new(
                reader_impl,
                min_threshold_time,
                query_purpose.truncate_at_end(),
            )?);
        }

        Ok(result)
    }

    pub fn create_snapshot_writer(
        &mut self,
        persistent_id: PersistentId,
    ) -> Result<SharedSnapshotWriter, WriteError> {
        match &self.stream_storage {
            StreamStorageConfig::Filesystem(root_path) => Ok(Arc::new(Mutex::new(Box::new(
                LocalBinarySnapshotWriter::new(
                    &self.snapshot_writer_path(root_path, persistent_id)?,
                )?,
            )))),
            StreamStorageConfig::S3 { bucket, root_path } => {
                let snapshot_path = self.s3_snapshot_path(root_path, persistent_id);
                Ok(Arc::new(Mutex::new(Box::new(S3SnapshotWriter::new(
                    bucket.deep_copy(),
                    &snapshot_path,
                )))))
            }
            StreamStorageConfig::Mock(_) => {
                unreachable!()
            }
        }
    }

    fn snapshot_writer_path(
        &self,
        root_path: &Path,
        persistent_id: PersistentId,
    ) -> Result<PathBuf, IoError> {
        ensure_directory(root_path)?;
        let streams_path = root_path.join(STREAMS_DIRECTORY_NAME);
        ensure_directory(&streams_path)?;
        let worker_path = streams_path.join(self.worker_id.to_string());
        ensure_directory(&worker_path)?;
        Ok(worker_path.join(persistent_id.to_string()))
    }

    fn s3_snapshot_path(&self, root_path: &str, persistent_id: PersistentId) -> String {
        format!(
            "{}/streams/{}/{}",
            root_path.strip_suffix('/').unwrap_or(root_path),
            self.worker_id,
            persistent_id
        )
    }

    fn assigned_snapshot_paths(
        &self,
        root_path: &Path,
        persistent_id: PersistentId,
        query_purpose: ReadersQueryPurpose,
    ) -> Result<HashMap<usize, PathBuf>, ReadError> {
        ensure_directory(root_path)?;

        let streams_dir = root_path.join("streams");
        ensure_directory(&streams_dir)?;

        let mut assigned_paths = HashMap::new();
        let paths = fs::read_dir(&streams_dir)?;
        for entry in paths.flatten() {
            let file_type = entry.file_type()?;
            if file_type.is_dir() {
                let dir_name = if let Some(name) = entry.file_name().to_str() {
                    name.to_string()
                } else {
                    error!("Failed to parse block folder name: {entry:?}");
                    continue;
                };

                let parsed_worker_id: usize = match dir_name.parse() {
                    Ok(worker_id) => worker_id,
                    Err(e) => {
                        error!("Could not parse worker id from snapshot directory {entry:?}. Error details: {e}");
                        continue;
                    }
                };

                if !query_purpose.includes_worker(
                    parsed_worker_id,
                    self.worker_id,
                    self.total_workers,
                ) {
                    continue;
                }

                let snapshot_path = streams_dir.join(format!("{parsed_worker_id}/{persistent_id}"));
                assigned_paths.insert(parsed_worker_id, snapshot_path);
            } else {
                warn!("Unexpected object in snapshot directory: {entry:?}");
            }
        }

        Ok(assigned_paths)
    }

    fn assigned_s3_snapshot_paths(
        &self,
        bucket: &S3Bucket,
        root_path: &str,
        persistent_id: PersistentId,
        query_purpose: ReadersQueryPurpose,
    ) -> Result<HashMap<usize, String>, ReadError> {
        let snapshots_root_path = format!(
            "{}/streams/",
            root_path.strip_suffix('/').unwrap_or(root_path)
        );
        let prefix_len = snapshots_root_path.len();
        let object_lists = bucket
            .list(snapshots_root_path.clone(), None)
            .map_err(|e| ReadError::S3(S3CommandName::ListObjectsV2, e))?;

        let mut assigned_paths = HashMap::new();

        for list in &object_lists {
            for object in &list.contents {
                let key: &str = &object.key;
                assert!(key.len() > prefix_len);
                let snapshot_path_block = key[prefix_len..].to_string();
                // snapshot_path_block has the form {worker_id}/{persistent_id}/{snapshot_block_id}
                let path_parts: Vec<&str> = snapshot_path_block.split('/').collect();
                if path_parts.len() != 3 {
                    error!("Incorrect path block format: {snapshot_path_block}");
                    continue;
                }

                let parsed_worker_id: usize = match path_parts[0].parse() {
                    Ok(worker_id) => worker_id,
                    Err(e) => {
                        error!("Could not parse worker id from snapshot directory {key:?}. Error details: {e}");
                        continue;
                    }
                };

                if !query_purpose.includes_worker(
                    parsed_worker_id,
                    self.worker_id,
                    self.total_workers,
                ) {
                    continue;
                }

                let snapshot_path =
                    format!("{snapshots_root_path}{parsed_worker_id}/{persistent_id}");
                assigned_paths.insert(parsed_worker_id, snapshot_path);
            }
        }

        Ok(assigned_paths)
    }
}
