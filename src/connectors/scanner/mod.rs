use std::io::Read;
use std::sync::Arc;

use crate::connectors::DataEventType;
use crate::connectors::{ReadError, ReadResult};

pub mod filesystem;
pub mod s3;

#[allow(clippy::module_name_repetitions)]
pub use filesystem::FilesystemScanner;

#[allow(clippy::module_name_repetitions)]
pub use s3::S3Scanner;

#[allow(clippy::module_name_repetitions)]
pub trait PosixLikeScanner: Send {
    /// Returns the current events type: whether it is data to be added
    /// or data to be removed.
    fn data_event_type(&self) -> Option<DataEventType>;

    /// Returns a reader for the currently selected object if there
    /// is an object selected for reading.
    fn current_object_reader(
        &mut self,
    ) -> Result<Option<Box<dyn Read + Send + 'static>>, ReadError>;

    /// Returns the name of the currently processed file in the input directory. It will
    /// be used in the offset that will be produced by a reader.
    fn current_offset_object(&self) -> Option<Arc<[u8]>>;

    /// Performs a seek on a directory-like structure to resume reading after the
    /// object with a given name.
    fn seek_to_object(&mut self, seek_object_path: &[u8]) -> Result<(), ReadError>;

    /// Finish reading the current file and find the next one to read from.
    /// If there is a file to read from, the method returns a `ReadResult`
    /// specifying the action to be provided downstream.
    ///
    /// It can either be a `NewSource` event when the new action is found or
    /// a `FinishedSource` event when we've had an ongoing action and it needs
    /// to be finalized.
    fn next_scanner_action(&mut self) -> Result<Option<ReadResult>, ReadError>;
}
