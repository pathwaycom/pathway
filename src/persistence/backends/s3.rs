// Copyright © 2024 Pathway

use log::error;
use std::mem::take;
use std::sync::mpsc;
use std::sync::mpsc::Sender;
use std::thread;

use futures::channel::oneshot;
use futures::channel::oneshot::Sender as OneShotSender;
use s3::bucket::Bucket as S3Bucket;

use crate::deepcopy::DeepCopy;
use crate::persistence::backends::PersistenceBackend;
use crate::persistence::Error;
use crate::retry::{execute_with_retries, RetryConfig};

use super::BackendPutFuture;

const MAX_S3_RETRIES: usize = 2;

#[derive(Debug)]
enum S3UploaderEvent {
    UploadObject {
        key: String,
        value: Vec<u8>,
        result_sender: OneShotSender<Result<(), Error>>,
    },
    Finish,
}

#[derive(Debug)]
#[allow(clippy::module_name_repetitions)]
pub struct S3KVStorage {
    bucket: S3Bucket,
    root_path: String,
    upload_event_sender: Sender<S3UploaderEvent>,
    uploader_thread: Option<std::thread::JoinHandle<()>>,
}

impl S3KVStorage {
    pub fn new(bucket: S3Bucket, root_path: &str) -> Self {
        let mut root_path_prepared = root_path.to_string();
        if !root_path.ends_with('/') {
            root_path_prepared += "/";
        }

        let (upload_event_sender, upload_event_receiver) = mpsc::channel();
        let uploader_bucket = bucket.deep_copy();
        let uploader_thread = thread::Builder::new()
            .name("pathway:s3_snapshot-bg-writer".to_string())
            .spawn(move || {
                loop {
                    let event = upload_event_receiver.recv().expect("unexpected termination for s3 objects sender");
                    match event {
                        S3UploaderEvent::UploadObject { key, value, result_sender } => {
                            let upload_result = execute_with_retries(
                                || uploader_bucket.put_object(&key, &value),
                                RetryConfig::default(),
                                MAX_S3_RETRIES,
                            ).map_err(Error::S3).map(|_| ());
                            let send_result = result_sender.send(upload_result);
                            if let Err(unsent_flush_result) = send_result {
                                error!("The receiver no longer waits for the result of this save: {unsent_flush_result:?}");
                            }
                        },
                        S3UploaderEvent::Finish => break,
                    };
                }
            })
            .expect("s3 uploader failed");

        Self {
            bucket,
            upload_event_sender,
            uploader_thread: Some(uploader_thread),
            root_path: root_path_prepared,
        }
    }

    fn full_key_path(&self, key: &str) -> String {
        self.root_path.clone() + key
    }
}

impl Drop for S3KVStorage {
    fn drop(&mut self) {
        self.upload_event_sender
            .send(S3UploaderEvent::Finish)
            .expect("failed to submit the graceful shutdown event");
        if let Some(uploader_thread) = take(&mut self.uploader_thread) {
            if let Err(e) = uploader_thread.join() {
                // there is no formatter for std::any::Any
                error!("Failed to join s3 snapshot uploader thread: {e:?}");
            }
        }
    }
}

impl PersistenceBackend for S3KVStorage {
    fn list_keys(&self) -> Result<Vec<String>, Error> {
        let mut keys = Vec::new();

        let object_lists = self.bucket.list(self.root_path.clone(), None)?;
        let prefix_len = self.root_path.len();

        for list in &object_lists {
            for object in &list.contents {
                let key: &str = &object.key;
                assert!(key.len() > self.root_path.len());
                let prepared_key = key[prefix_len..].to_string();
                if !prepared_key.contains('/') {
                    // Similarly to the filesystem backend,
                    // we take only files in the imaginary folder
                    keys.push(prepared_key);
                }
            }
        }

        Ok(keys)
    }

    fn get_value(&self, key: &str) -> Result<Vec<u8>, Error> {
        let full_key_path = self.full_key_path(key);
        let response_data = execute_with_retries(
            || self.bucket.get_object(&full_key_path), // returns Err on incorrect status code because fail-on-err feature is enabled
            RetryConfig::default(),
            MAX_S3_RETRIES,
        )?;
        Ok(response_data.bytes().to_vec())
    }

    fn put_value(&mut self, key: &str, value: Vec<u8>) -> BackendPutFuture {
        let (sender, receiver) = oneshot::channel();
        self.upload_event_sender
            .send(S3UploaderEvent::UploadObject {
                key: self.full_key_path(key),
                value,
                result_sender: sender,
            })
            .expect("chunk queue submission should not fail");
        receiver
    }

    fn remove_key(&mut self, key: &str) -> Result<(), Error> {
        let full_key_path = self.full_key_path(key);
        let _ = self.bucket.delete_object(full_key_path)?;
        Ok(())
    }
}
