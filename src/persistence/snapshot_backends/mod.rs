// Copyright © 2024 Pathway

use std::fmt::Debug;
use std::io::Read;

use futures::channel::oneshot::Receiver as OneShotReceiver;

use crate::persistence::BackendError;

/// Snapshot backend operates with chunks.
/// A chunk - is a binary large object that has the name and the contents.
///
/// Below is the interface that any backend must implement
/// to be able to store persistence chunks.
pub trait SnapshotBackend: Send + Debug {
    /// Append `contents` to the end of the current chunk.
    fn write(&self, contents: &[u8]) -> Result<(), BackendError>;

    /// Ensure that all data is written and available in the
    /// backend storage.
    fn flush(&self) -> OneShotReceiver<Result<(), BackendError>>;

    /// Finalize writing the current chunk and name it `chunk_name`.
    fn finalize_chunk(&self, chunk_name: &str) -> OneShotReceiver<Result<(), BackendError>>;

    /// List all chunks available in the storage.
    fn list_chunks(&self) -> Result<Vec<String>, BackendError>;

    /// Read the full contents of a chunk `chunk_name`.
    fn read(&self, chunk_name: &str) -> Result<Box<dyn Read>, BackendError>;
}
