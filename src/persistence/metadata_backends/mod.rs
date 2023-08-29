use std::fmt::Debug;
use std::io::Error as IoError;

use serde_json::Error as ParseError;

pub mod file;
pub use file::FilesystemKVStorage;

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    #[error(transparent)]
    FileSystem(#[from] IoError),

    #[error("metadata entry {0:?} incorrectly formatted: {1}")]
    IncorrectFormat(String, #[source] ParseError),
}

pub trait MetadataBackend: Send + Debug {
    fn list_keys(&self) -> Result<Vec<String>, Error>;
    fn get_value(&self, key: &str) -> Result<String, Error>;
    fn put_value(&mut self, key: &str, value: &str) -> Result<(), Error>;
}
