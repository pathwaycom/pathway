// Copyright © 2024 Pathway

use s3::bucket::Bucket as S3Bucket;

use crate::deepcopy::DeepCopy;
use crate::persistence::backends::PersistenceBackend;
use crate::persistence::Error;
use crate::retry::{execute_with_retries, RetryConfig};

use super::{BackendPutFuture, BackgroundObjectUploader};

const MAX_S3_RETRIES: usize = 2;

#[derive(Debug)]
#[allow(clippy::module_name_repetitions)]
pub struct S3KVStorage {
    bucket: S3Bucket,
    root_path: String,
    background_uploader: BackgroundObjectUploader,
}

impl S3KVStorage {
    pub fn new(bucket: S3Bucket, root_path: &str) -> Self {
        let mut root_path_prepared = root_path.to_string();
        if !root_path.ends_with('/') {
            root_path_prepared += "/";
        }

        let uploader_bucket = bucket.deep_copy();
        let upload_object = move |key: String, value: Vec<u8>| {
            let _ = execute_with_retries(
                || uploader_bucket.put_object(&key, &value),
                RetryConfig::default(),
                MAX_S3_RETRIES,
            )?;
            Ok(())
        };

        Self {
            bucket,
            background_uploader: BackgroundObjectUploader::new(upload_object),
            root_path: root_path_prepared,
        }
    }

    fn full_key_path(&self, key: &str) -> String {
        self.root_path.clone() + key
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
                keys.push(prepared_key);
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
        self.background_uploader
            .upload_object(self.full_key_path(key), value)
    }

    fn remove_key(&mut self, key: &str) -> Result<(), Error> {
        let full_key_path = self.full_key_path(key);
        let _ = self.bucket.delete_object(full_key_path)?;
        Ok(())
    }
}
