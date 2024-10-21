// Copyright © 2024 Pathway

use std::fmt::Debug;
use std::io::Error as IoError;
use std::str::Utf8Error;

use ::s3::error::S3Error;
use bincode::ErrorKind as BincodeError;
use futures::channel::oneshot::Receiver as OneShotReceiver;
use serde_json::Error as JsonParseError;

pub use file::FilesystemKVStorage;
pub use mock::MockKVStorage;
pub use s3::S3KVStorage;

pub mod file;
pub mod mock;
pub mod s3;

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    #[error(transparent)]
    Io(#[from] IoError),

    #[error(transparent)]
    S3(#[from] S3Error),

    #[error(transparent)]
    Utf8(#[from] Utf8Error),

    #[error(transparent)]
    Bincode(#[from] BincodeError),

    #[error("metadata entry {0:?} incorrectly formatted: {1}")]
    IncorrectMetadataFormat(String, #[source] JsonParseError),
}

/// The persistence backend can be implemented over a Key-Value
/// storage that implements the following interface.
pub trait PersistenceBackend: Send + Debug {
    /// List all keys present in the storage.
    fn list_keys(&self) -> Result<Vec<String>, Error>;

    /// Get the value corresponding to the `key`.
    fn get_value(&self, key: &str) -> Result<Vec<u8>, Error>;

    /// Set the value corresponding to the `key` to `value`.
    fn put_value(&mut self, key: &str, value: Vec<u8>) -> OneShotReceiver<Result<(), Error>>;

    /// Remove the value corresponding to the `key`.
    fn remove_key(&self, key: &str) -> Result<(), Error>;
}
