// Copyright © 2024 Pathway

use log::error;
use std::fmt::Debug;
use std::io::Error as IoError;
use std::mem::take;
use std::str::Utf8Error;
use std::sync::mpsc;
use std::sync::mpsc::Sender;
use std::thread;

use ::s3::error::S3Error;
use azure_storage::Error as AzureStorageError;
use bincode::ErrorKind as BincodeError;
use futures::channel::oneshot;
use futures::channel::oneshot::Receiver as OneShotReceiver;
use futures::channel::oneshot::Sender as OneShotSender;
use glob::PatternError as GlobPatternError;
use serde_json::Error as JsonParseError;

pub use azure::AzureKVStorage;
pub use file::FilesystemKVStorage;
pub use memory::{MemoryKVStorage, MemoryKVStorageError};
pub use mock::MockKVStorage;
pub use s3::S3KVStorage;

pub mod azure;
pub mod file;
pub mod memory;
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
    Memory(#[from] MemoryKVStorageError),

    #[error(transparent)]
    Utf8(#[from] Utf8Error),

    #[error(transparent)]
    Bincode(#[from] BincodeError),

    #[error(transparent)]
    Azure(#[from] AzureStorageError),

    #[error(transparent)]
    Glob(#[from] GlobPatternError),

    #[error("no available cached object versions")]
    NoAvailableVersions,

    #[error("path must be a valid utf-8 string")]
    PathIsNotUtf8,

    #[error("metadata entry {0:?} incorrectly formatted: {1}")]
    IncorrectMetadataFormat(String, #[source] JsonParseError),
}

pub type BackendPutFuture = OneShotReceiver<Result<(), Error>>;
/// The persistence backend can be implemented over a Key-Value
/// storage that implements the following interface.
pub trait PersistenceBackend: Send + Debug {
    /// List all keys present in the storage.
    fn list_keys(&self) -> Result<Vec<String>, Error>;

    /// Get the value corresponding to the `key`.
    fn get_value(&self, key: &str) -> Result<Vec<u8>, Error>;

    /// Set the value corresponding to the `key` to `value`.
    fn put_value(&mut self, key: &str, value: Vec<u8>) -> BackendPutFuture;

    /// Remove the value corresponding to the `key`.
    fn remove_key(&mut self, key: &str) -> Result<(), Error>;
}

#[derive(Debug)]
enum BackgroundUploaderEvent {
    UploadObject {
        key: String,
        value: Vec<u8>,
        result_sender: OneShotSender<Result<(), Error>>,
    },
    Finish,
}

#[derive(Debug)]
pub struct BackgroundObjectUploader {
    upload_event_sender: Sender<BackgroundUploaderEvent>,
    uploader_thread: Option<std::thread::JoinHandle<()>>,
}

impl BackgroundObjectUploader {
    pub fn new(
        mut upload_object: impl FnMut(String, Vec<u8>) -> Result<(), Error> + Send + 'static,
    ) -> Self {
        let (upload_event_sender, upload_event_receiver) = mpsc::channel();
        let uploader_thread = thread::Builder::new()
            .name("pathway:snapshot-bg-writer".to_string())
            .spawn(move || {
                loop {
                    let event = upload_event_receiver.recv().expect("unexpected termination for background objects sender");
                    match event {
                        BackgroundUploaderEvent::UploadObject { key, value, result_sender } => {
                            let upload_result = upload_object(key, value);
                            let send_result = result_sender.send(upload_result);
                            if let Err(unsent_flush_result) = send_result {
                                error!("The receiver no longer waits for the result of this save: {unsent_flush_result:?}");
                            }
                        },
                        BackgroundUploaderEvent::Finish => break,
                    };
                }
            })
            .expect("background uploader failed");

        Self {
            upload_event_sender,
            uploader_thread: Some(uploader_thread),
        }
    }

    fn upload_object(&mut self, key: String, value: Vec<u8>) -> BackendPutFuture {
        let (sender, receiver) = oneshot::channel();
        self.upload_event_sender
            .send(BackgroundUploaderEvent::UploadObject {
                key,
                value,
                result_sender: sender,
            })
            .expect("chunk queue submission should not fail");
        receiver
    }
}

impl Drop for BackgroundObjectUploader {
    fn drop(&mut self) {
        self.upload_event_sender
            .send(BackgroundUploaderEvent::Finish)
            .expect("failed to submit the graceful shutdown event");
        if let Some(uploader_thread) = take(&mut self.uploader_thread) {
            if let Err(e) = uploader_thread.join() {
                // there is no formatter for std::any::Any
                error!("Failed to join background snapshot uploader thread: {e:?}");
            }
        }
    }
}
