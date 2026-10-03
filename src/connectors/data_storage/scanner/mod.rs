use crate::connectors::metadata::FileLikeMetadata;
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
    Read(Vec<u8>, FileLikeMetadata),
    Update(Vec<u8>, FileLikeMetadata),
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
    fn object_metadata(
        &mut self,
        object_path: &[u8],
    ) -> Result<Option<FileLikeMetadata>, ReadError>;
    fn read_object(&mut self, object_path: &[u8]) -> Result<Vec<u8>, ReadError>;
    fn next_scanner_actions(
        &mut self,
        are_deletions_enabled: bool,
        cached_object_storage: &CachedObjectStorage,
    ) -> Result<Vec<QueuedAction>, ReadError>;
    fn has_pending_actions(&self) -> bool;
    fn short_description(&self) -> String;

    /// Whether an object can change between the moment a scan took its
    /// metadata and the moment its contents are read. A local file is
    /// created empty and filled afterwards, so a scan may tag it before the
    /// writer is done; for such a scanner the reader takes the metadata
    /// again after the read and reads the object once more while the two
    /// disagree. An S3 object is replaced atomically, and its metadata costs
    /// a request, so the default keeps the metadata taken by the scan.
    fn can_object_change_during_read(&self) -> bool {
        false
    }
}
