#![allow(clippy::module_name_repetitions)]

use log::{error, info, warn};
use std::fs;
use std::io::Error as IoError;
use std::path::{Path, PathBuf};

use s3::bucket::Bucket as S3Bucket;

use crate::connectors::data_storage::{ReadError, WriteError};
use crate::connectors::snapshot::{
    LocalBinarySnapshotReader, LocalBinarySnapshotWriter, SnapshotReader, SnapshotWriter,
};
use crate::fs_helpers::ensure_directory;
use crate::persistence::metadata_backends::Error as MetadataBackendError;
use crate::persistence::metadata_backends::{FilesystemKVStorage, MetadataBackend};
use crate::persistence::state::MetadataAccessor;
use crate::persistence::PersistentId;

const STREAMS_DIRECTORY_NAME: &str = "streams";

/// Metadata storage handles the frontier over all persisted data sources.
/// When we restart the computation, it will start from the frontier stored
/// in the metadata storage
#[derive(Debug, Clone)]
pub enum MetadataStorageConfig {
    Filesystem(PathBuf),
}

/// Stream storage handles the snapshot, which will be loaded when Pathway
/// program restarts. It can also handle the cache for UDF calls
#[derive(Debug, Clone)]
pub enum StreamStorageConfig {
    Filesystem(PathBuf),
    S3 { bucket: S3Bucket, root_path: String },
}

/// Persistence in Pathway consists of two parts: actual frontier
/// storage and maintenance and snapshotting.
///
/// The outer config contains the configuration for these two parts,
/// which is passed from Python program by user.
#[derive(Debug, Clone)]
pub struct PersistenceManagerOuterConfig {
    metadata_storage: MetadataStorageConfig,
    stream_storage: StreamStorageConfig,
}

impl PersistenceManagerOuterConfig {
    pub fn new(
        metadata_storage: MetadataStorageConfig,
        stream_storage: StreamStorageConfig,
    ) -> Self {
        Self {
            metadata_storage,
            stream_storage,
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
    worker_id: usize,
    total_workers: usize,
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
            worker_id,
            total_workers,
        }
    }

    pub fn create_metadata_storage(&self) -> Result<MetadataAccessor, MetadataBackendError> {
        let backend: Box<dyn MetadataBackend> = match &self.metadata_storage {
            MetadataStorageConfig::Filesystem(root_path) => {
                Box::new(FilesystemKVStorage::new(root_path)?)
            }
        };
        MetadataAccessor::new(backend, self.worker_id)
    }

    pub fn create_snapshot_readers(
        &self,
        persistent_id: PersistentId,
    ) -> Result<Vec<Box<dyn SnapshotReader>>, ReadError> {
        match &self.stream_storage {
            StreamStorageConfig::Filesystem(root_path) => {
                let mut result: Vec<Box<dyn SnapshotReader>> = Vec::new();
                for path in self.assigned_snapshot_paths(root_path, persistent_id)? {
                    result.push(Box::new(LocalBinarySnapshotReader::new(path)?));
                }
                Ok(result)
            }
            StreamStorageConfig::S3 { .. } => todo!(), // PR 4348
        }
    }

    pub fn create_snapshot_writer(
        &mut self,
        persistent_id: PersistentId,
    ) -> Result<Box<dyn SnapshotWriter>, WriteError> {
        match &self.stream_storage {
            StreamStorageConfig::Filesystem(root_path) => {
                Ok(Box::new(LocalBinarySnapshotWriter::new(
                    &self.snapshot_writer_path(root_path, persistent_id)?,
                )?))
            }
            StreamStorageConfig::S3 { .. } => todo!(), // PR 4348
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

    fn assigned_snapshot_paths(
        &self,
        root_path: &Path,
        persistent_id: PersistentId,
    ) -> Result<Vec<PathBuf>, IoError> {
        ensure_directory(root_path)?;

        let streams_dir = root_path.join("streams");
        ensure_directory(&streams_dir)?;

        let mut assigned_paths = Vec::new();
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

                let earlier_worker_id: usize = match dir_name.parse() {
                    Ok(worker_id) => worker_id,
                    Err(e) => {
                        error!("Could not parse worker id from snapshot directory {entry:?}. Error details: {e}");
                        continue;
                    }
                };

                if earlier_worker_id % self.total_workers != self.worker_id {
                    continue;
                }
                if earlier_worker_id != self.worker_id {
                    info!("Assigning snapshot from the former worker {earlier_worker_id} to worker {} due to reduced number of worker threads", self.worker_id);
                }

                let snapshot_path =
                    streams_dir.join(format!("{earlier_worker_id}/{persistent_id}"));
                assigned_paths.push(snapshot_path);
            } else {
                warn!("Unexpected object in snapshot directory: {entry:?}");
            }
        }

        Ok(assigned_paths)
    }
}
