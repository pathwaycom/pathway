// Copyright © 2026 Pathway

use std::sync::Arc;

use s3::bucket::Bucket as S3Bucket;
use tokio::runtime::Runtime as TokioRuntime;

use crate::deepcopy::DeepCopy;
use crate::persistence::backends::PersistenceBackend;
use crate::persistence::Error;
use crate::retry::{execute_with_retries_async, RetryConfig};
use crate::s3_runtime::build_s3_runtime;

use super::{BackendPutFuture, BackgroundObjectUploader};

const MAX_S3_RETRIES: usize = 2;

#[derive(Debug)]
#[allow(clippy::module_name_repetitions)]
pub struct S3KVStorage {
    bucket: S3Bucket,
    root_path: String,
    background_uploader: BackgroundObjectUploader,
    // Shared with the background uploader thread; dropped with the storage so
    // it never outlives the process that created it — see `s3_runtime`.
    runtime: Arc<TokioRuntime>,
}

impl S3KVStorage {
    pub fn new(bucket: S3Bucket, root_path: &str) -> Self {
        let mut root_path_prepared = root_path.to_string();
        if !root_path.ends_with('/') {
            root_path_prepared += "/";
        }

        let runtime = Arc::new(build_s3_runtime());
        let uploader_bucket = bucket.deep_copy();
        let uploader_runtime = runtime.clone();
        let upload_object = move |key: String, value: Vec<u8>| {
            let _ = uploader_runtime.block_on(execute_with_retries_async(
                async || uploader_bucket.put_object(&key, &value).await,
                RetryConfig::default(),
                MAX_S3_RETRIES,
            ))?;
            Ok(())
        };

        Self {
            bucket,
            background_uploader: BackgroundObjectUploader::new(upload_object),
            root_path: root_path_prepared,
            runtime,
        }
    }

    fn full_key_path(&self, key: &str) -> String {
        self.root_path.clone() + key
    }
}

impl PersistenceBackend for S3KVStorage {
    fn list_keys(&self) -> Result<Vec<String>, Error> {
        let prefix_len = self.root_path.len();
        let mut keys = Vec::new();

        let object_lists = self.runtime.block_on(execute_with_retries_async(
            async || self.bucket.list(self.root_path.clone(), None).await,
            RetryConfig::default(),
            MAX_S3_RETRIES,
        ))?;

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
        let value = self.runtime.block_on(execute_with_retries_async(
            async || -> Result<Vec<u8>, Error> {
                // `fail-on-err` only inspects the status code, so a body that
                // ends early still arrives as a successful response and would
                // pass for the whole object - to surface much later, as data
                // that can't be deserialized. Comparing it against the length
                // the response announced turns that into a retryable error.
                let response = self.bucket.get_object(&full_key_path).await?;
                let announced_len = response
                    .headers()
                    .get("content-length")
                    .and_then(|value| value.parse::<usize>().ok());
                let value = response.bytes().to_vec();
                if let Some(expected) = announced_len {
                    if value.len() != expected {
                        return Err(Error::TruncatedResponse {
                            key: full_key_path.clone(),
                            expected,
                            actual: value.len(),
                        });
                    }
                }
                Ok(value)
            },
            RetryConfig::default(),
            MAX_S3_RETRIES,
        ))?;
        Ok(value)
    }

    fn put_value(&self, key: &str, value: Vec<u8>) -> BackendPutFuture {
        self.background_uploader
            .upload_object(self.full_key_path(key), value)
    }

    fn remove_key(&self, key: &str) -> Result<(), Error> {
        let full_key_path = self.full_key_path(key);
        let _ = self.runtime.block_on(execute_with_retries_async(
            async || self.bucket.delete_object(full_key_path.clone()).await,
            RetryConfig::default(),
            MAX_S3_RETRIES,
        ))?;
        Ok(())
    }
}
