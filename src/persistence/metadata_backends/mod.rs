// Copyright © 2024 Pathway

use std::fmt::Debug;

pub use file::FilesystemKVStorage;
pub use mock::MockKVStorage;
pub use s3::S3KVStorage;

pub mod file;
pub mod mock;
pub mod s3;
use crate::persistence::BackendError;

/// The metadata backend works with key-value (KV) storages.
///
/// It stores essential information such as data source types,
/// the last finalized Pathway timestamp, and more. This is used
/// during the second phase of the two-phase state commit: once the
/// snapshot is up-to-date, the relevant information about the
/// frontiers of the processed data is saved in the metadata storage.
pub trait MetadataBackend: Send + Debug {
    /// List all keys present in the storage.
    fn list_keys(&self) -> Result<Vec<String>, BackendError>;

    /// Get the value corresponding to the `key`.
    fn get_value(&self, key: &str) -> Result<String, BackendError>;

    /// Set the value corresponding to the `key` to `value`.
    fn put_value(&mut self, key: &str, value: &str) -> Result<(), BackendError>;

    /// Remove the value corresponding to the `key`.
    fn remove_key(&self, key: &str) -> Result<(), BackendError>;
}
