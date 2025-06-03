// Copyright © 2024 Pathway

#![allow(clippy::module_name_repetitions)]

use differential_dataflow::difference::Semigroup;
use differential_dataflow::ExchangeData;
use log::{error, info};
use std::collections::HashMap;
use std::io::Error as IoError;
use std::path::{Path, PathBuf};
use std::sync::{mpsc, Arc, Mutex};
use std::time::Duration;

use azure_storage::StorageCredentials as AzureStorageCredentials;
use s3::bucket::Bucket as S3Bucket;

use crate::connectors::{PersistenceMode, SnapshotAccess};
use crate::deepcopy::DeepCopy;
use crate::engine::error::DynError;
use crate::engine::license::License;
use crate::engine::{Result, Timestamp, TotalFrontier};
use crate::fs_helpers::ensure_directory;
use crate::persistence::backends::{
    AzureKVStorage, FilesystemKVStorage, MockKVStorage, PersistenceBackend, S3KVStorage,
};
use crate::persistence::cached_object_storage::CachedObjectStorage;
use crate::persistence::input_snapshot::{
    Event, InputSnapshotReader, InputSnapshotWriter, MockSnapshotReader, ReadInputSnapshot,
    SnapshotMode,
};
use crate::persistence::operator_snapshot::{
    ConcreteSnapshotMerger, ConcreteSnapshotReader, ConcreteSnapshotWriter,
    MultiConcreteSnapshotReader,
};
use crate::persistence::state::FinalizedTimeQuerier;
use crate::persistence::state::MetadataAccessor;
use crate::persistence::Error as PersistenceBackendError;
use crate::persistence::{PersistentId, SharedSnapshotWriter};

const STREAMS_DIRECTORY_NAME: &str = "streams";

pub type ConnectorWorkerPair = (PersistentId, usize);

/// The configuration for the backend that stores persisted state.
#[derive(Debug, Clone)]
pub enum PersistentStorageConfig {
    Filesystem(PathBuf),
    S3 {
        bucket: Box<S3Bucket>,
        root_path: String,
    },
    Azure {
        account: String,
        credentials: AzureStorageCredentials,
        container: String,
        root_path: String,
    },
    Mock(HashMap<ConnectorWorkerPair, Vec<Event>>),
}

impl PersistentStorageConfig {
    pub fn create(&self) -> Result<Box<dyn PersistenceBackend>, PersistenceBackendError> {
        match &self {
            Self::Filesystem(root_path) => Ok(Box::new(FilesystemKVStorage::new(root_path)?)),
            Self::S3 { bucket, root_path } => {
                Ok(Box::new(S3KVStorage::new(bucket.deep_copy(), root_path)))
            }
            Self::Azure {
                account,
                credentials,
                container,
                root_path,
            } => Ok(Box::new(AzureKVStorage::new(
                root_path,
                account.clone(),
                container.clone(),
                credentials.clone(),
            )?)),
            Self::Mock(_) => Ok(Box::new(MockKVStorage {})),
        }
    }
}

/// Persistence in Pathway consists of two parts: actual frontier
/// storage and maintenance and snapshotting.
///
/// The outer config contains the configuration for these two parts,
/// which is passed from Python program by user.
#[derive(Debug, Clone)]
pub struct PersistenceManagerOuterConfig {
    snapshot_interval: Duration,
    backend: PersistentStorageConfig,
    snapshot_access: SnapshotAccess,
    persistence_mode: PersistenceMode,
    continue_after_replay: bool,
}

impl PersistenceManagerOuterConfig {
    pub fn new(
        snapshot_interval: Duration,
        backend: PersistentStorageConfig,
        snapshot_access: SnapshotAccess,
        persistence_mode: PersistenceMode,
        continue_after_replay: bool,
    ) -> Self {
        Self {
            snapshot_interval,
            backend,
            snapshot_access,
            persistence_mode,
            continue_after_replay,
        }
    }

    pub fn into_inner(self, worker_id: usize, total_workers: usize) -> PersistenceManagerConfig {
        PersistenceManagerConfig::new(self, worker_id, total_workers)
    }

    pub fn validate(&self, license: &License) -> Result<()> {
        if matches!(self.persistence_mode, PersistenceMode::OperatorPersisting) {
            license
                .check_entitlements(["full-persistence"])
                .map_err(DynError::from)?;
        }
        Ok(())
    }
}

/// The main persistent manager config, which, however can only be
/// constructed from within the worker
#[derive(Debug, Clone)]
pub struct PersistenceManagerConfig {
    backend: PersistentStorageConfig,
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
            backend: outer_config.backend,
            snapshot_access: outer_config.snapshot_access,
            persistence_mode: outer_config.persistence_mode,
            continue_after_replay: outer_config.continue_after_replay,
            snapshot_interval: outer_config.snapshot_interval,
            worker_id,
            total_workers,
        }
    }

    pub fn create_cached_object_storage(
        &self,
        persistent_id: PersistentId,
    ) -> Result<CachedObjectStorage, PersistenceBackendError> {
        let backend: Box<dyn PersistenceBackend> = match &self.backend {
            PersistentStorageConfig::Filesystem(root_path) => {
                let storage_root_path = root_path.join(format!(
                    "cached-objects-storage/{}/{persistent_id}",
                    self.worker_id
                ));
                ensure_directory(&storage_root_path)?;
                Box::new(FilesystemKVStorage::new(&storage_root_path)?)
            }
            PersistentStorageConfig::S3 { bucket, root_path } => {
                let storage_root_path = format!(
                    "{}/cached-objects-storage/{persistent_id}",
                    root_path.strip_suffix('/').unwrap_or(root_path),
                );
                Box::new(S3KVStorage::new(bucket.deep_copy(), &storage_root_path))
            }
            PersistentStorageConfig::Azure {
                account,
                credentials,
                container,
                root_path,
            } => {
                let storage_root_path = format!(
                    "{}/cached-objects-storage/{persistent_id}",
                    root_path.strip_suffix('/').unwrap_or(root_path),
                );
                Box::new(AzureKVStorage::new(
                    &storage_root_path,
                    account.to_string(),
                    container.to_string(),
                    credentials.clone(),
                )?)
            }
            PersistentStorageConfig::Mock(_) => Box::new(MockKVStorage {}),
        };
        CachedObjectStorage::new(backend)
    }

    pub fn create_metadata_storage(&self) -> Result<MetadataAccessor, PersistenceBackendError> {
        let backend = self.backend.create()?;
        MetadataAccessor::new(backend, self.worker_id, self.total_workers)
    }

    fn get_readers_backends(
        &self,
        persistent_id: PersistentId,
        query_purpose: ReadersQueryPurpose,
    ) -> Result<Vec<Box<dyn PersistenceBackend>>, PersistenceBackendError> {
        let mut result: Vec<Box<dyn PersistenceBackend>> = Vec::new();
        match &self.backend {
            PersistentStorageConfig::Filesystem(root_path) => {
                let assigned_snapshot_paths =
                    self.assigned_local_snapshot_paths(root_path, persistent_id, query_purpose)?;
                for (_, path) in assigned_snapshot_paths {
                    let backend = FilesystemKVStorage::new(&path)?;
                    result.push(Box::new(backend));
                }
                Ok(result)
            }
            PersistentStorageConfig::S3 { bucket, root_path } => {
                let snapshots_root_path = Self::cloud_snapshots_root_path(root_path);
                let backend = Box::new(S3KVStorage::new(bucket.deep_copy(), &snapshots_root_path));
                let assigned_snapshot_paths = self.assigned_cloud_snapshot_paths(
                    backend.as_ref(),
                    &snapshots_root_path,
                    persistent_id,
                    query_purpose,
                )?;
                for (_, path) in assigned_snapshot_paths {
                    let backend = S3KVStorage::new(bucket.deep_copy(), &path);
                    result.push(Box::new(backend));
                }
                Ok(result)
            }
            PersistentStorageConfig::Azure {
                root_path,
                account,
                container,
                credentials,
            } => {
                let snapshots_root_path = Self::cloud_snapshots_root_path(root_path);
                let backend = Box::new(AzureKVStorage::new(
                    &snapshots_root_path,
                    account.to_string(),
                    container.to_string(),
                    credentials.clone(),
                )?);
                let assigned_snapshot_paths = self.assigned_cloud_snapshot_paths(
                    backend.as_ref(),
                    &snapshots_root_path,
                    persistent_id,
                    query_purpose,
                )?;
                for (_, path) in assigned_snapshot_paths {
                    let backend = AzureKVStorage::new(
                        &path,
                        account.to_string(),
                        container.to_string(),
                        credentials.clone(),
                    )?;
                    result.push(Box::new(backend));
                }
                Ok(result)
            }
            PersistentStorageConfig::Mock(_) => Ok(Vec::new()),
        }
    }

    pub fn create_snapshot_readers(
        &self,
        persistent_id: PersistentId,
        threshold_time: TotalFrontier<Timestamp>,
        query_purpose: ReadersQueryPurpose,
    ) -> Result<Vec<Box<dyn ReadInputSnapshot>>, PersistenceBackendError> {
        info!("Using threshold time: {threshold_time:?}");
        let mut result: Vec<Box<dyn ReadInputSnapshot>> = Vec::new();
        if let PersistentStorageConfig::Mock(event_map) = &self.backend {
            let events = event_map
                .get(&(persistent_id, self.worker_id))
                .unwrap_or(&vec![])
                .clone();
            let reader = MockSnapshotReader::new(events);
            result.push(Box::new(reader));
            Ok(result)
        } else {
            let backends = self.get_readers_backends(persistent_id, query_purpose)?;
            for backend in backends {
                let reader = InputSnapshotReader::new(
                    backend,
                    threshold_time,
                    query_purpose.truncate_at_end(),
                )?;
                result.push(Box::new(reader));
            }
            Ok(result)
        }
    }

    fn get_writer_backend(
        &mut self,
        persistent_id: PersistentId,
    ) -> Result<Box<dyn PersistenceBackend>, PersistenceBackendError> {
        match &self.backend {
            PersistentStorageConfig::Filesystem(root_path) => Ok(Box::new(
                FilesystemKVStorage::new(&self.snapshot_writer_path(root_path, persistent_id)?)?,
            )),
            PersistentStorageConfig::S3 { bucket, root_path } => Ok(Box::new(S3KVStorage::new(
                bucket.deep_copy(),
                &self.cloud_snapshot_path(root_path, persistent_id),
            ))),
            PersistentStorageConfig::Azure {
                root_path,
                account,
                container,
                credentials,
            } => Ok(Box::new(AzureKVStorage::new(
                &self.cloud_snapshot_path(root_path, persistent_id),
                account.to_string(),
                container.to_string(),
                credentials.clone(),
            )?)),
            PersistentStorageConfig::Mock(_) => {
                unreachable!()
            }
        }
    }

    pub fn create_snapshot_writer(
        &mut self,
        persistent_id: PersistentId,
        snapshot_mode: SnapshotMode,
    ) -> Result<SharedSnapshotWriter, PersistenceBackendError> {
        let backend = self.get_writer_backend(persistent_id)?;
        let snapshot_mode = if matches!(self.persistence_mode, PersistenceMode::OperatorPersisting)
        {
            SnapshotMode::OffsetsOnly
        } else {
            snapshot_mode
        };
        let snapshot_writer = InputSnapshotWriter::new(backend, snapshot_mode);
        Ok(Arc::new(Mutex::new(snapshot_writer?)))
    }

    fn snapshot_writer_path(
        &self,
        root_path: &Path,
        persistent_id: PersistentId,
    ) -> Result<PathBuf, IoError> {
        let streams_path = root_path.join(STREAMS_DIRECTORY_NAME);
        let worker_path = streams_path.join(self.worker_id.to_string());
        ensure_directory(&worker_path)?;
        Ok(worker_path.join(persistent_id.to_string()))
    }

    fn cloud_snapshots_root_path(root_path: &str) -> String {
        format!(
            "{}/streams",
            root_path.strip_suffix('/').unwrap_or(root_path)
        )
    }

    fn cloud_snapshot_path(&self, root_path: &str, persistent_id: PersistentId) -> String {
        format!(
            "{}/{}/{}",
            Self::cloud_snapshots_root_path(root_path),
            self.worker_id,
            persistent_id
        )
    }

    fn assigned_local_snapshot_paths(
        &self,
        root_path: &Path,
        persistent_id: PersistentId,
        query_purpose: ReadersQueryPurpose,
    ) -> Result<HashMap<usize, PathBuf>, PersistenceBackendError> {
        let streams_dir = root_path.join("streams");
        ensure_directory(&streams_dir)?;
        let backend = Box::new(FilesystemKVStorage::new(&streams_dir)?);
        let paths = self.assigned_snapshot_paths_with_backend(
            backend.as_ref(),
            persistent_id,
            query_purpose,
        )?;
        let result: HashMap<_, _> = paths
            .into_iter()
            .map(|(key, value)| (key, streams_dir.join(value)))
            .collect();
        Ok(result)
    }

    fn assigned_cloud_snapshot_paths(
        &self,
        backend: &dyn PersistenceBackend,
        snapshots_root_path: &str,
        persistent_id: PersistentId,
        query_purpose: ReadersQueryPurpose,
    ) -> Result<HashMap<usize, String>, PersistenceBackendError> {
        let paths =
            self.assigned_snapshot_paths_with_backend(backend, persistent_id, query_purpose)?;
        let result: HashMap<_, _> = paths
            .into_iter()
            .map(|(key, value)| (key, format!("{snapshots_root_path}/{value}")))
            .collect();
        Ok(result)
    }

    fn assigned_snapshot_paths_with_backend(
        &self,
        backend: &dyn PersistenceBackend,
        persistent_id: PersistentId,
        query_purpose: ReadersQueryPurpose,
    ) -> Result<HashMap<usize, String>, PersistenceBackendError> {
        let object_keys = backend.list_keys()?;
        let mut assigned_paths = HashMap::new();

        for snapshot_path_block in &object_keys {
            // snapshot_path_block has the form {worker_id}/{persistent_id}/{snapshot_block_id}
            let path_parts: Vec<&str> = snapshot_path_block.split('/').collect();
            if path_parts.len() != 3 {
                error!("Incorrect path block format: {snapshot_path_block}");
                continue;
            }

            let parsed_worker_id: usize = match path_parts[0].parse() {
                Ok(worker_id) => worker_id,
                Err(e) => {
                    error!("Could not parse worker id from snapshot directory {snapshot_path_block:?}. Error details: {e}");
                    continue;
                }
            };

            if !query_purpose.includes_worker(parsed_worker_id, self.worker_id, self.total_workers)
            {
                continue;
            }

            let snapshot_path = format!("{parsed_worker_id}/{persistent_id}");
            assigned_paths.insert(parsed_worker_id, snapshot_path);
        }

        Ok(assigned_paths)
    }

    fn create_operator_snapshot_merger<D, R>(
        &mut self,
        persistent_id: PersistentId,
        receiver: mpsc::Receiver<()>,
    ) -> Result<ConcreteSnapshotMerger, PersistenceBackendError>
    where
        D: ExchangeData,
        R: ExchangeData + Semigroup,
    {
        let merger_backend = self.get_writer_backend(persistent_id)?;
        let metadata_backend = self.backend.create()?;
        let time_querier = FinalizedTimeQuerier::new(metadata_backend, self.total_workers);
        let merger = ConcreteSnapshotMerger::new::<D, R>(
            merger_backend,
            self.snapshot_interval,
            time_querier,
            receiver,
        );
        Ok(merger)
    }

    pub fn create_operator_snapshot_readers<D, R>(
        &mut self,
        persistent_id: PersistentId,
        threshold_time: TotalFrontier<Timestamp>,
    ) -> Result<(MultiConcreteSnapshotReader, ConcreteSnapshotMerger), PersistenceBackendError>
    where
        D: ExchangeData,
        R: ExchangeData + Semigroup,
    {
        info!("Using threshold time: {threshold_time:?}");
        let mut readers: Vec<ConcreteSnapshotReader> = Vec::new();
        let backends =
            self.get_readers_backends(persistent_id, ReadersQueryPurpose::ReadSnapshot)?;
        for backend in backends {
            let reader = ConcreteSnapshotReader::new(backend, threshold_time);
            readers.push(reader);
        }
        let (sender, receiver) = mpsc::channel(); // pair used to block merger until reader finishes
        let reader = MultiConcreteSnapshotReader::new(readers, sender);
        let merger = self.create_operator_snapshot_merger::<D, R>(persistent_id, receiver)?;
        Ok((reader, merger))
    }

    pub fn create_operator_snapshot_writer<D, R>(
        &mut self,
        persistent_id: PersistentId,
    ) -> Result<ConcreteSnapshotWriter<D, R>, PersistenceBackendError>
    where
        D: ExchangeData,
        R: ExchangeData + Semigroup,
    {
        let backend = self.get_writer_backend(persistent_id)?;
        let writer = ConcreteSnapshotWriter::new(backend, self.snapshot_interval);
        Ok(writer)
    }
}
