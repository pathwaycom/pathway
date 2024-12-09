use crate::connectors::metadata::SourceMetadata;
use crate::connectors::ReadError;
use crate::persistence::cached_object_storage::CachedObjectStorage;

pub mod filesystem;
pub mod s3;

#[allow(clippy::module_name_repetitions)]
pub use filesystem::FilesystemScanner;

#[allow(clippy::module_name_repetitions)]
pub use s3::S3Scanner;

#[derive(Clone, Debug)]
pub enum QueuedAction {
    Read(Vec<u8>, SourceMetadata),
    Update(Vec<u8>, SourceMetadata),
    Delete(Vec<u8>),
}

impl QueuedAction {
    pub fn path(&self) -> &[u8] {
        match self {
            Self::Read(path, _) | Self::Update(path, _) | Self::Delete(path) => path,
        }
    }
}

#[allow(clippy::module_name_repetitions)]
pub trait PosixLikeScanner: Send {
    fn object_metadata(&mut self, object_path: &[u8]) -> Result<Option<SourceMetadata>, ReadError>;
    fn read_object(&mut self, object_path: &[u8]) -> Result<Vec<u8>, ReadError>;
    fn next_scanner_actions(
        &mut self,
        are_deletions_enabled: bool,
        cached_object_storage: &CachedObjectStorage,
    ) -> Result<Vec<QueuedAction>, ReadError>;
}
