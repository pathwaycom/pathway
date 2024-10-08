// Copyright © 2024 Pathway

use std::fmt::Debug;
use std::io::Error as IoError;
use std::str::Utf8Error;
use std::sync::{Arc, Mutex};

use crate::connectors::snapshot::WriteSnapshotEvent;
use xxhash_rust::xxh3::Xxh3 as Hasher;

use ::s3::error::S3Error;
use serde_json::Error as JsonParseError;

pub mod config;
pub mod frontier;
pub mod input_snapshot;
pub mod metadata_backends;
pub mod snapshot_backends;
pub mod state;
pub mod tracker;

pub type PersistentId = u128;
pub type ExternalPersistentId = String;
pub type SharedSnapshotWriter = Arc<Mutex<Box<dyn WriteSnapshotEvent>>>;

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum BackendError {
    #[error(transparent)]
    FileSystem(#[from] IoError),

    #[error(transparent)]
    S3(#[from] S3Error),

    #[error(transparent)]
    Utf8(#[from] Utf8Error),

    #[error("metadata entry {0:?} incorrectly formatted: {1}")]
    IncorrectMetadataFormat(String, #[source] JsonParseError),
}

pub trait IntoPersistentId {
    fn into_persistent_id(self) -> PersistentId;
}

impl IntoPersistentId for ExternalPersistentId {
    fn into_persistent_id(self) -> PersistentId {
        let mut hasher = Hasher::default();
        hasher.update(self.as_bytes());
        hasher.digest128()
    }
}
