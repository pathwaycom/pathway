// Copyright © 2024 Pathway

use std::fmt::Debug;
use std::io::Error as IoError;
use std::str::Utf8Error;

use ::s3::error::S3Error;
use serde_json::Error as ParseError;

pub mod file;
pub mod mock;
pub mod s3;
pub use file::FilesystemKVStorage;
pub use mock::MockKVStorage;
pub use s3::S3KVStorage;

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    #[error(transparent)]
    FileSystem(#[from] IoError),

    #[error(transparent)]
    S3(#[from] S3Error),

    #[error(transparent)]
    Utf8(#[from] Utf8Error),

    #[error("metadata entry {0:?} incorrectly formatted: {1}")]
    IncorrectFormat(String, #[source] ParseError),
}

pub trait MetadataBackend: Send + Debug {
    fn list_keys(&self) -> Result<Vec<String>, Error>;
    fn get_value(&self, key: &str) -> Result<String, Error>;
    fn put_value(&mut self, key: &str, value: &str) -> Result<(), Error>;
    fn remove_key(&self, key: &str) -> Result<(), Error>;
}
